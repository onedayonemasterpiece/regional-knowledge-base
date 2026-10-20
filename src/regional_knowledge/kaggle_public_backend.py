"""Thin source-search adapter for the existing authenticated MCP/WSS path.

Construction is explicit: this is not a deployment switch or a corpus fallback.
The queue, OAuth resolver and source gate are the existing controller owners.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
import json
import time
from urllib.parse import quote
from uuid import UUID

from mcp.server.mcpserver.exceptions import ToolError

from .contracts import EvidenceSearchOutput, FetchOutput, ReadTrace, SearchOutput, SearchResult
from .kaggle_protocol import Query, QueryFilters, TrustedActorContext
from .query_telemetry import ReadRuntime
from .read_authority import TrustedReadDestination
from .read_issuance import retain_read_output
from .read_telemetry import ReadTelemetry


class TargetUnavailable(RuntimeError):
    pass


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
            return 'urn:rkb:passage:' + quote(item.id, safe='')
        if evidence:
            return EvidenceSearchOutput(evidence=[FetchOutput(id=item.id,
                title=item.metadata.title or item.id, text=item.text, url=locator(item),
                metadata={'citation': item.metadata.citation, 'locator': item.metadata.locator,
                    'text_form': item.text_form, 'continuation': item.continuation,
                    'source_continuation': item.source_continuation.model_dump(mode='json')
                        if item.source_continuation else None,
                    'continuation_complete': item.continuation_complete,
                    'source_revisions': [source.model_dump() for source in item.dependencies.source_revisions]})
                for item in result.evidence], **common)
        return SearchOutput(results=[SearchResult(id=item.id, title=item.metadata.title or item.id,
            url=locator(item)) for item in result.evidence], **common)

    async def _search(self, query, principal, *, evidence, source_ids=None, limit=3):
        # One budget includes auth, admission, network, native execution and gate.
        deadline = time.monotonic()+2
        telemetry = self.read_telemetry
        attempt = telemetry.begin_attempt('knowledge_search' if evidence else 'search')
        authority = ticket = permit = gated = None
        retained = False
        status, reason = 'unavailable', 'not_ready'
        branches = ()
        try:
            async with asyncio.timeout_at(deadline):
                try:
                    filters = QueryFilters(source_ids=tuple(source_ids) if source_ids is not None else None,
                        kinds=('source',))
                    if filters.source_ids is not None and any(str(UUID(value)) != value for value in filters.source_ids):
                        raise ValueError('canonical source ids required')
                except (ValueError, TypeError, AttributeError):
                    raise PublicQueryInvalid from None
                authority = await self.read_authority.resolve_oauth(principal.access_token,
                    destination=self.destination)
                binding = await asyncio.to_thread(current_binding, self.worker_queue)
                try:
                    request = Query(request_id=attempt.request_id,
                        connection_generation=binding.connection_generation, turn_generation=0,
                        query=query, filters=filters, min_data_seq=binding.min_data_seq,
                        actor=TrustedActorContext(person_id=authority.person_id, audience=authority.destination.key,
                            auth_epoch=authority.auth_revision, visibility_epoch=0, context_ref=authority.context_ref),
                        deadline_ms=max(1, int((deadline-time.monotonic())*1000)), evidence_limit=limit)
                except (ValueError, TypeError):
                    raise PublicQueryInvalid from None
                permit = await self.source_gate.dispatch(authority, request, binding)
                def dispatched(resource):
                    telemetry.bind_dispatch(attempt,
                        runtime=ReadRuntime(resource.run_id, resource.runtime_incarnation),
                        connection_generation=resource.dispatch_generation,
                        turn_generation=request.turn_generation)
                ticket = await self.worker_queue.shared_reads.begin_read(authority, permit.query,
                    dialog_ref=self.destination.key, deadline=deadline, on_dispatch=dispatched)
                result = await asyncio.shield(ticket.result)
                branches = ('lexical', 'dense') if result.mode == 'hybrid' else ()
                gated = await self.source_gate.finish(permit, result)
                permit = None
                status, reason = gated.result.status, gated.result.reason
                if gated.result.mode != 'hybrid':
                    raise ToolError(json.dumps({'status': status, 'reason': reason}))
                render = lambda value: self._render(value, evidence=evidence, attempt=attempt)
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

    async def fetch(self, *args, **kwargs):
        raise ToolError('{"status":"unavailable","reason":"target_exact_fetch_not_ready"}')
