"""Thin source-search adapter for the existing authenticated MCP/WSS path.

Construction is explicit: this is not a deployment switch or a corpus fallback.
The queue, OAuth resolver and source gate are the existing controller owners.
"""
from __future__ import annotations

import asyncio
import base64
from dataclasses import dataclass
import json
import math
import time
from urllib.parse import quote
from uuid import UUID

from mcp.server.mcpserver.exceptions import ToolError

from .contracts import EvidenceSearchOutput, FetchOutput, ReadTrace, SearchOutput, SearchResult
from .kaggle_protocol import Query, QueryFilters, TrustedActorContext, PassageContinuation, PassageReadRequest, StoryReadRequest
from .kaggle_fact_contracts import FactReadRequest
from .query_telemetry import ReadRuntime
from .read_authority import TrustedReadDestination, VerifiedReadAuthority
from .read_issuance import retain_read_output
from .read_telemetry import ReadTelemetry


class TargetUnavailable(RuntimeError):
    pass


_PASSAGE_ID = 'rkb:passage:v1:'


def passage_fetch_id(locator: PassageContinuation) -> str:
    """Self-contained versioned locator, without a signature or capability grant."""
    return _PASSAGE_ID + base64.urlsafe_b64encode(locator.model_dump_json().encode()).decode().rstrip('=')


def parse_passage_fetch_id(value: str) -> PassageContinuation:
    if type(value) is not str or len(value) > 4096 or not value.startswith(_PASSAGE_ID):
        raise ValueError('invalid exact source locator')
    token = value[len(_PASSAGE_ID):]
    try:
        raw = base64.b64decode(token + '=' * (-len(token) % 4), altchars=b'-_', validate=True)
        locator = PassageContinuation.model_validate_json(raw)
        if (passage_fetch_id(locator) != value
                or str(UUID(locator.source.source_id)) != locator.source.source_id
                or str(UUID(locator.passage_id)) != locator.passage_id):
            raise ValueError
        return locator
    except (ValueError, TypeError):
        raise ValueError('invalid exact source locator') from None


class PublicQueryInvalid(ValueError):
    pass



@dataclass(frozen=True)
class TargetReadBinding:
    run_id: str
    runtime_incarnation: str
    connection_generation: int
    database_generation: str
    min_data_seq: int
    provider_version: str


def current_binding(queue):
    """Small read-only control snapshot; BGE-only readiness never suffices."""
    try:
        with queue.connect(write=False) as db:
            queue._shared_owner(db)
            row = db.execute('SELECT current_run FROM control WHERE id=1').fetchone()
            run = db.execute('SELECT * FROM runs WHERE id=?', (row[0],)).fetchone() if row else None
            if (run is None or run['status'] != 'ready' or not run['channel_active']
                    or run['attachment_conflict'] or queue._run_expired(run, queue.clock())
                    or run['provider_version'] is None or not run['runtime_incarnation']):
                raise TargetUnavailable('target runtime not ready')
            runtime = queue._runtime_row(db, run['id'])
            if runtime is None or runtime['accepted_generation'] != run['connection_generation']:
                raise TargetUnavailable('target runtime not verified')
            observed = json.loads(runtime['observed_json'])
            if not queue._runtime_check(db, run, run['connection_generation'], observed).ok:
                raise TargetUnavailable('target runtime proof differs')
            expected = json.loads(runtime['expected_json'])
            owner = db.execute('SELECT run_id,incarnation FROM bge_shared_admission WHERE id=1').fetchone()
            if owner is None or tuple(owner) != (run['id'], run['runtime_incarnation']):
                raise TargetUnavailable('shared compute owner differs')
            return TargetReadBinding(run['id'], run['runtime_incarnation'], run['connection_generation'],
                observed['database_generation'], expected['min_data_seq'], str(run['provider_version']))
    except TargetUnavailable:
        raise
    except Exception:
        raise TargetUnavailable('target runtime unavailable') from None


class KaggleSourceBackend:
    """Current source vertical; unsupported product operations fail explicitly."""
    current_read_output_required = True

    def __init__(self, *, worker_queue, read_authority, source_gate, destination_ref,
                 read_telemetry=None):
        self.worker_queue, self.read_authority, self.source_gate = worker_queue, read_authority, source_gate
        if worker_queue.shared_reads.authority is not read_authority or source_gate.authority is not read_authority:
            raise ValueError('one canonical controller read authority required')
        self.destination = TrustedReadDestination('mcp', 'personal', destination_ref)
        self.read_telemetry = read_telemetry or ReadTelemetry()

    def binding_current(self, binding):
        try:
            return type(binding) is TargetReadBinding and current_binding(self.worker_queue) == binding
        except TargetUnavailable:
            return False

    @staticmethod
    def _render(result, *, evidence, attempt):
        ready = result.mode == 'hybrid' and result.status in ('ok', 'degraded')
        common = dict(mode='hybrid' if ready else 'lexical_degraded',
            retrieval_mode='bge_lexical' if ready else 'lexical_only',
            main_state='ready' if ready else 'unavailable', status=result.status, reason=result.reason,
            observed_data_seq=result.observed_data_seq, observed_projection_seq=result.observed_projection_seq,
            prior_write_pending=result.prior_write_pending,
            read_trace=ReadTrace(**attempt.correlation(), backend_elapsed_ms=(time.monotonic()-attempt.started)*1000))
        def locator(item):
            return 'urn:rkb:' + ('story:' if item.kind=='story' else 'passage:') + quote(item.id, safe='')
        def fetch_id(item):
            if item.kind=='story':return item.id
            if (len(item.dependencies.source_revisions) == 1
                    and result.observed_database_generation is not None
                    and result.observed_visibility_epoch is not None):
                return passage_fetch_id(PassageContinuation(
                    database_generation=result.observed_database_generation,
                    visibility_epoch=result.observed_visibility_epoch,
                    source=item.dependencies.source_revisions[0], passage_id=item.id,
                    text_offset=0, bibliography_offset=0))
            return item.id
        if evidence:
            return EvidenceSearchOutput(evidence=[FetchOutput(id=fetch_id(item),
                title=item.metadata.title or item.id, text=item.text, url=locator(item),
                metadata={'citation': item.metadata.citation, 'locator': item.metadata.locator,
                    'text_form': item.text_form, 'continuation': item.continuation,
                    'source_continuation': item.source_continuation.model_dump(mode='json')
                        if item.source_continuation else None,
                    'continuation_complete': item.continuation_complete,
                    'next_id': passage_fetch_id(item.source_continuation)
                        if item.source_continuation and not item.continuation_complete else None,
                    'source_revisions': [source.model_dump() for source in item.dependencies.source_revisions],
                    'kind':item.kind,'exact_variant_ref':item.exact_variant_ref,
                    'variant_refs':list(item.dependencies.variant_refs)})
                for item in result.evidence], **common)
        return SearchOutput(results=[SearchResult(id=fetch_id(item), title=item.metadata.title or item.id,
            url=locator(item)) for item in result.evidence], **common)

    @staticmethod
    def _render_fetch(result, *, locator, attempt):
        common = dict(id=passage_fetch_id(locator), title=locator.passage_id,
            url='urn:rkb:passage:' + quote(locator.passage_id, safe=''),
            status=result.status, reason=result.reason,
            observed_data_seq=result.observed_data_seq, observed_projection_seq=result.observed_projection_seq,
            read_trace=ReadTrace(**attempt.correlation(), backend_elapsed_ms=(time.monotonic()-attempt.started)*1000))
        if result.status != 'ok':
            return FetchOutput(text='', **common)
        item = result.evidence[0]
        next_id = (passage_fetch_id(item.source_continuation)
            if item.source_continuation is not None and not item.continuation_complete else None)
        return FetchOutput(text=item.text, metadata={
            'citation': item.metadata.citation, 'locator': item.metadata.locator,
            'text_form': item.text_form, 'bounded_passage': True,
            'text_offset': item.source_continuation.text_offset - len(item.text)
                if item.source_continuation else locator.text_offset,
            'bibliography_offset': locator.bibliography_offset,
            'next_id': next_id, 'continuation_complete': next_id is None,
            'source_revisions': [source.model_dump() for source in item.dependencies.source_revisions]}, **common)

    @staticmethod
    def _render_story(result, *, attempt):
        return {**(result.record or {}),'status':result.status,'reason':result.reason,
            'observed_data_seq':result.observed_data_seq,'observed_projection_seq':result.observed_projection_seq,
            'read_trace':ReadTrace(**attempt.correlation(),
                backend_elapsed_ms=(time.monotonic()-attempt.started)*1000).model_dump(mode='json')}

    @staticmethod
    def _render_fact(result, *, attempt):
        return dict(status=result.status,reason=result.reason,items=list(result.records),
            has_more=result.next_cursor is not None,next_cursor=result.next_cursor,
            retrieval_mode='indexed_native_source_facts',
            observed_data_seq=result.observed_data_seq,
            observed_projection_seq=result.observed_projection_seq,
            read_trace=ReadTrace(**attempt.correlation(),
                backend_elapsed_ms=(time.monotonic()-attempt.started)*1000).model_dump(mode='json'))

    async def _search(self, query, principal, *, evidence, source_ids=None, limit=3, exact_locator=None,
                      story_request=None,fact_request=None,kinds=('source',),trusted=None):
        # One budget includes auth, admission, network, native execution and gate.
        deadline = min(time.monotonic()+2,trusted['deadline']) if trusted is not None else time.monotonic()+2
        telemetry = self.read_telemetry
        exact=exact_locator is not None or story_request is not None or fact_request is not None
        operation='fetch' if exact else 'knowledge_search' if evidence else 'search'
        attempt = telemetry.begin_attempt(operation,**({'request_id':trusted['request_id']} if trusted else {}))
        authority = trusted['authority'] if trusted is not None else None
        destination=trusted['destination'] if trusted is not None else self.destination
        ticket = permit = gated = None
        retained = False
        status, reason = 'unavailable', 'not_ready'
        branches = ()
        try:
            async with asyncio.timeout_at(deadline):
                try:
                    filters = QueryFilters(source_ids=tuple(source_ids) if source_ids is not None else None,
                        kinds=kinds)
                    if filters.source_ids is not None and any(str(UUID(value)) != value for value in filters.source_ids):
                        raise ValueError('canonical source ids required')
                except (ValueError, TypeError, AttributeError):
                    raise PublicQueryInvalid from None
                if trusted is None:
                    authority = await self.read_authority.resolve_oauth(principal.access_token,destination=destination)
                else:
                    if type(authority) is not VerifiedReadAuthority or authority.destination!=destination:
                        raise PermissionError('adapter destination differs')
                    await self.read_authority.require(authority,'public_search')
                binding = await asyncio.to_thread(current_binding, self.worker_queue)
                try:
                    identity = dict(request_id=attempt.request_id,
                        connection_generation=binding.connection_generation,
                        turn_generation=trusted['turn_generation'] if trusted else 0,
                        min_data_seq=max(binding.min_data_seq,trusted['min_data_seq']) if trusted else binding.min_data_seq,
                        actor=TrustedActorContext(person_id=authority.person_id, audience=authority.destination.key,
                            auth_epoch=authority.auth_revision, visibility_epoch=0, context_ref=authority.context_ref),
                        deadline_ms=max(1, int((deadline-time.monotonic())*1000)))
                    request = (FactReadRequest(**fact_request,**identity) if fact_request is not None else
                        StoryReadRequest(**story_request,**identity) if story_request is not None else
                        PassageReadRequest(locator=exact_locator, **identity) if exact_locator is not None else
                        Query(query=query, filters=filters, evidence_limit=limit, **identity))
                except (ValueError, TypeError):
                    raise PublicQueryInvalid from None

                permit = await self.source_gate.dispatch(authority, request, binding)
                def dispatched(resource):
                    telemetry.bind_dispatch(attempt,
                        runtime=ReadRuntime(resource.run_id, resource.runtime_incarnation),
                        connection_generation=resource.dispatch_generation,
                        turn_generation=request.turn_generation)
                ticket = await self.worker_queue.shared_reads.begin_read(authority, permit.query,
                    dialog_ref=destination.key, deadline=deadline, on_dispatch=dispatched)
                result = await asyncio.shield(ticket.result)
                branches = ('lexical', 'dense') if getattr(result, 'mode', None) == 'hybrid' else ()
                gated = await self.source_gate.finish(permit, result)
                permit = None
                status, reason = gated.result.status, gated.result.reason
                if (gated.result.status != 'ok' if exact else gated.result.mode != 'hybrid'):
                    raise ToolError(json.dumps({'status': status, 'reason': reason}))
                render = (lambda value:self._render_fact(value,attempt=attempt)) if fact_request is not None else (lambda value:self._render_story(value,attempt=attempt)) if story_request is not None else (lambda value: self._render_fetch(value, locator=exact_locator, attempt=attempt)) if exact_locator is not None else (
                    lambda value: self._render(value, evidence=evidence, attempt=attempt))
                output = render(gated.result)
                if not retain_read_output(self.source_gate, gated, render,
                        on_release=lambda: self.read_authority.release(authority),
                        on_terminal=lambda final_status, final_reason: telemetry.finish_attempt(attempt,
                            status=final_status, reason=final_reason, executed_branches=branches)):
                    raise TargetUnavailable('public read output boundary required')
                retained = True
                # Authority must live until the final output check, not merely
                # until QUERY_RESULT. The request-local guard owns its release.
                return output
        except asyncio.CancelledError:
            if not asyncio.current_task().cancelling():
                status = 'unavailable'
                reason = 'deadline_exceeded' if time.monotonic() >= deadline-.002 else 'context_gap'
                raise ToolError(json.dumps({'status': status, 'reason': reason})) from None
            status, reason = 'cancelled', 'caller_cancelled'
            raise
        except TimeoutError:
            status, reason = 'unavailable', 'deadline_exceeded'
            raise ToolError('{"status":"unavailable","reason":"deadline_exceeded"}') from None
        except ToolError:
            raise
        except PermissionError:
            status, reason = 'denied', 'access_denied'
            raise ToolError('{"status":"denied","reason":"access_denied"}') from None
        except PublicQueryInvalid:
            status, reason = 'invalid_query', 'invalid_input'
            raise ToolError('{"status":"invalid_query","reason":"invalid_input"}') from None
        except Exception:
            status, reason = 'unavailable', 'not_ready'
            raise ToolError('{"status":"unavailable","reason":"not_ready"}') from None
        finally:
            if ticket is not None:
                self.worker_queue.shared_reads.cancel_read(ticket)
            if permit is not None:
                self.source_gate.abandon(permit)
            if not retained:
                if gated is not None:
                    self.source_gate.release(gated.ticket)
                if authority is not None:
                    self.read_authority.release(authority)
            if not retained:
                telemetry.finish_attempt(attempt, status=status, reason=reason, executed_branches=branches)

    async def search(self, query, principal, *, document_ids=None, main_job_id=None, aliases=None):
        if main_job_id or aliases:
            raise ToolError('{"status":"unavailable","reason":"unsupported_target_search_option"}')
        return await self._search(query, principal, evidence=False, source_ids=document_ids)

    async def search_evidence(self, query, principal, *, max_evidence=3, document_ids=None, main_job_id=None):
        if main_job_id:
            raise ToolError('{"status":"unavailable","reason":"unsupported_target_search_option"}')
        return await self._search(query, principal, evidence=True, source_ids=document_ids, limit=max_evidence)

    async def fetch(self, id, principal):
        try:
            locator = parse_passage_fetch_id(id)
        except (ValueError, TypeError):
            raise ToolError('{"status":"invalid_query","reason":"invalid_input"}') from None
        return await self._search(None, principal, evidence=True, exact_locator=locator)

    async def search_adapter(self,query,*,authority,request_id,turn_generation,min_data_seq,
                             deadline_monotonic,destination,source_ids=None,limit=3):
        """Trusted receiver seam; the retained authority lives through final issuance.

        The receiver owns service authentication/link resolution and must use
        the same request-local output issuance guard. No bearer is synthesized.
        """
        if (type(destination) is not TrustedReadDestination or type(request_id) is not str
                or not 1<=len(request_id)<=256 or type(turn_generation) is not int or turn_generation<0
                or type(min_data_seq) is not int or min_data_seq<0
                or type(deadline_monotonic) not in (int,float) or not math.isfinite(deadline_monotonic)):
            self.read_authority.release(authority)
            raise ToolError('{"status":"invalid_query","reason":"invalid_input"}')
        return await self._search(query,None,evidence=True,source_ids=source_ids,limit=limit,trusted=dict(
            authority=authority,request_id=request_id,turn_generation=turn_generation,min_data_seq=min_data_seq,
            deadline=deadline_monotonic,destination=destination))

    async def story_get(self,story_id,principal,*,revision=None,view='compact',cursor=None,limit=8,
                        assertion_id=None,relation_id=None):
        if view not in ('compact','editorial') or cursor is not None or assertion_id is not None or relation_id is not None:
            raise ToolError('{"status":"unavailable","reason":"unsupported_target_story_view"}')
        return await self._search(None,principal,evidence=True,kinds=('story',),story_request=dict(
            story_id=story_id,revision=revision,view='get',limit=limit))

    async def story_history(self,story_id,principal,*,from_revision=None,to_revision=None,limit=10,cursor=None):
        if cursor is not None or to_revision not in (None,1) or from_revision not in (None,1) or not 1<=limit<=10:
            raise ToolError('{"status":"unavailable","reason":"unsupported_target_story_history"}')
        return await self._search(None,principal,evidence=True,kinds=('story',),story_request=dict(
            story_id=story_id,view='history',limit=limit))

    async def story_variants(self,story_id,principal,*,limit=10,cursor=None):
        if cursor is not None or not 1<=limit<=10:
            raise ToolError('{"status":"unavailable","reason":"unsupported_target_story_variants"}')
        return await self._search(None,principal,evidence=True,kinds=('story',),story_request=dict(
            story_id=story_id,view='variants',limit=limit))

    async def story_search(self,query,principal,*,filters=None,mode='hybrid',order='updated',limit=3,cursor=None):
        if filters or mode!='hybrid' or order!='updated' or cursor is not None or not 1<=limit<=5:
            raise ToolError('{"status":"unavailable","reason":"unsupported_target_story_search"}')
        return await self._search(query,principal,evidence=True,kinds=('story',),limit=limit)

    async def story_calendar(self,principal,*,month,day,calendar='gregorian',limit=3,cursor=None):
        return await self._search(None,principal,evidence=True,kinds=('fact',),fact_request=dict(
            action='calendar',month=month,day=day,calendar=calendar,limit=limit,cursor=cursor))

    async def source_fact_read(self,principal,*,source_id,limit=3,cursor=None):
        """Fixed controller route; no new public MCP tool is introduced."""
        return await self._search(None,principal,evidence=True,kinds=('fact',),fact_request=dict(
            action='assertions',source_id=source_id,limit=limit,cursor=cursor))

    async def story_observation_search(self,principal,*,metric_key,unit_code=None,limit=3,cursor=None):
        return await self._search(None,principal,evidence=True,kinds=('fact',),fact_request=dict(
            action='observations',metric_key=metric_key,unit_code=unit_code,limit=limit,cursor=cursor))
