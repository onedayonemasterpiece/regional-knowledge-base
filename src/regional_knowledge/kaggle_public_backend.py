"""Thin source-search adapter for the existing authenticated MCP/WSS path.

Construction is explicit: this is not a deployment switch or a corpus fallback.
The queue, OAuth resolver and source gate are the existing controller owners.
"""
from __future__ import annotations

import asyncio
import base64
from dataclasses import dataclass
import hashlib
import json
import math
import re
import time
from urllib.parse import quote
from uuid import UUID, uuid4

from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import CallToolResult, ImageContent, TextContent, EmbeddedResource, BlobResourceContents

from .contracts import EvidenceSearchOutput, FetchOutput, ReadTrace, SearchOutput, SearchResult, BookFindOutput, StartupStatus
from .kaggle_protocol import Query, QueryFilters, TrustedActorContext, PassageContinuation, PassageReadRequest, StoryReadRequest
from .kaggle_fact_contracts import FactReadRequest
from .kaggle_source_graph_read import SourceGraphReadRequest
from .kaggle_source_read import SourceReadRequest
from .query_telemetry import ReadRuntime
from .read_authority import TrustedReadDestination, VerifiedReadAuthority
from .kaggle_editorial_contracts import EditorialReadRequest,EditorialReadResult,editorial_cursor,parse_editorial_cursor
from .read_issuance import retain_read_output
from .kaggle_public_illustration_gate import (
    PublicIllustrationOutputGate, IssuedPublicIllustration, IssuedArtifactBytes,
)
from .read_telemetry import ReadTelemetry


class TargetUnavailable(RuntimeError):
    pass


def source_unit_pdf_result(record):
    """One standard MCP embedded binary resource; no file state on Dev."""
    return CallToolResult(content=[
        TextContent(type='text',text='PDF исходной статьи: '+record['article_id']),
        EmbeddedResource(type='resource',resource=BlobResourceContents(
            uri='rkb://source/'+record['source_id']+'/article/'+record['article_id']+'.pdf',
            mimeType='application/pdf',blob=record['blob'])),
    ])



_PASSAGE_ID = 'rkb:passage:v1:'


def _evidence_title(item) -> str:
    if item.kind=='story': return (item.metadata.title or item.id)[:2048]
    catalog = (item.metadata.source_provenance or {}).get('catalog')
    if not isinstance(catalog, dict) or not isinstance(catalog.get('title'), str) or not catalog['title'].strip():
        return item.id
    authors = catalog.get('authors')
    names = ', '.join(a.strip() for a in authors if isinstance(a, str) and a.strip()) if isinstance(authors, list) else ''
    year = catalog.get('publication_year')
    title = catalog['title'].strip() + (f' ({year})' if type(year) is int and 1 <= year <= 3000 else '')
    # Explicit p./pp./с./стр. are printed pages; legacy "pages N" can be a scan.
    pattern = r'(?<!\w)(pp?\.|с\.|стр\.)\s*((?:\d+[a-z]?|[ivxlcdm]+)(?:\s*(?:,|[-–])\s*(?:\d+[a-z]?|[ivxlcdm]+))*)(?=$|[;,])'
    groups = []
    for page in re.finditer(pattern, item.metadata.locator or '', re.I):
        prefix = 'с.'
        values = [part.strip().replace('-', '–') for part in page[2].split(',')]
        if groups and groups[-1][0] == prefix:
            groups[-1][1].extend(values)
        else:
            groups.append((prefix, values))
    for prefix, values in groups:
        spans, run = [], []
        for value in values:
            if run and value.isdecimal() and run[-1].isdecimal() and int(value) == int(run[-1]) + 1:
                run.append(value)
            else:
                if run:
                    spans.append(f'{run[0]}–{run[-1]}' if len(run) > 1 else run[0])
                run = [value]
        if run:
            spans.append(f'{run[0]}–{run[-1]}' if len(run) > 1 else run[0])
        title += f', {prefix} ' + ', '.join(spans)
    return ((names + '. ' if names else '') + title)[:2048]


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


_MEDIA_ID = "rkb:illustration:v1:"


def _media_selector(value):
    """Paired untrusted IDs only; an illustration ID alone is not a source ACL."""
    if type(value) is not str or not value.startswith(_MEDIA_ID) or len(value) > 125:
        raise PublicQueryInvalid("exact paired source and illustration selector required")
    fields = value[len(_MEDIA_ID):].split(":")
    if len(fields) != 2:
        raise PublicQueryInvalid("exact paired source and illustration selector required")
    try:
        source_id, illustration_id = (str(UUID(value)) for value in fields)
    except (TypeError, ValueError):
        raise PublicQueryInvalid("canonical media selectors required") from None
    if fields != [source_id, illustration_id]:
        raise PublicQueryInvalid("canonical media selectors required")
    return source_id, illustration_id


def _illustration_caption(record: dict) -> str:
    """Model-readable credit from existing, source-verified public fields only."""
    lines = []
    catalog = record.get("_citation_catalog")
    if type(catalog) is dict:
        title = catalog.get("title")
        if type(title) is str and title.strip():
            lines.append("Книга: " + title.strip()[:2048])
        authors = catalog.get("authors")
        if type(authors) is list:
            names = ", ".join(a.strip() for a in authors if type(a) is str and a.strip())
            if names:
                lines.append("Автор: " + names[:2048])
        year = catalog.get("publication_year")
        if type(year) is int and 1 <= year <= 3000:
            lines.append(f"Год: {year}")
    page_index = record.get("physical_page_index")
    if type(page_index) is int and 0 <= page_index <= 2499:
        lines.append(f"Страница файла (не печатная нумерация): {page_index + 1}")
    caption = record.get("caption_text")
    if type(caption) is str and caption.strip():
        lines.append("Подпись под рисунком: " + caption.strip())
    return "\n".join(lines) if lines else "Иллюстрация без подписи."


@dataclass(frozen=True)
class _GatedMedia:
    """Request-local immutable bytes, never a user-returned artifact capability."""
    issued: IssuedPublicIllustration
    pixels: IssuedArtifactBytes | None = None

    @property
    def ticket(self):
        return self.issued.ticket


@dataclass(frozen=True)
class TargetReadBinding:
    run_id: str
    runtime_incarnation: str
    connection_generation: int
    database_generation: str
    min_data_seq: int
    provider_version: str


def current_binding(queue, *, allow_draining=False):
    """Serving requires READY; retained checkpoint/output guards may allow current DRAINING."""
    try:
        with queue.connect(write=False) as db:
            queue._shared_owner(db)
            row = db.execute('SELECT current_run FROM control WHERE id=1').fetchone()
            run = db.execute('SELECT * FROM runs WHERE id=?', (row[0],)).fetchone() if row else None
            if (run is None or run['status'] not in (('ready','draining') if allow_draining else ('ready',)) or not run['channel_active']
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


PENDING_STARTUP_MESSAGE_RU = ("Запускаем инфраструктуру, подождите. Первый поиск займёт дольше обычного; "
    "следующие запросы будут быстрыми.")


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
        # Only the single native/root owner can bind a trusted Media15 gate.
        # No WSS, pool, PG role or archive client is constructed here.
        self.public_media_gate = None
        self.illustration_renderer = None

    def configure_public_illustrations(self, gate: PublicIllustrationOutputGate):
        """Owner-only explicit composition; unconfigured target stays CLOSED.

        The root must supply a real current PG reader, installed SQL015,
        independently leased archive observer AND a real byte issuer returning
        IssuedArtifactBytes bound to its owner-held ArchiveOpaqueProof.
        Merely setting SQL availability='ready' never enables this route.
        """
        if (type(gate) is not PublicIllustrationOutputGate
                or gate.authority is not self.read_authority
                or gate.source_gate is not self.source_gate
                or not callable(getattr(gate.archive, "read_issued_bytes", None))
                or self.public_media_gate is not None):
            raise TypeError("current canonical media gate and real image issuer required")
        self.public_media_gate = gate

    @staticmethod
    def _render_issued_media(value: _GatedMedia) -> CallToolResult:
        if (type(value) is not _GatedMedia
                or type(value.issued) is not IssuedPublicIllustration
                or type(value.pixels) is not IssuedArtifactBytes
                or value.pixels.proof.ref != value.issued.frame.artifact.ref):
            raise PermissionError("current exact archive image bytes required")
        # Metadata is already the existing media owner's allowlisted renderer
        # and is bound to immutable canonical frame bytes in its output gate.
        return CallToolResult(content=[
            TextContent(type="text", text=value.issued.frame.model_dump_json()),
            ImageContent(type="image", data=base64.b64encode(
                value.pixels.data).decode("ascii"), mimeType=value.pixels.mime_type),
            TextContent(type="text", text=_illustration_caption(value.issued.frame.metadata)),
        ])

    async def illustration_fetch(self, locator, principal):
        """The EXISTING MCP illustration_fetch tool, target-native-only path.

        Never fall back to legacy Supabase/Vibe for an unavailable Kaggle source.
        The first stage authorizes and retains current source and archive proof,
        then emits only a harmless placeholder inside request-local capture.
        The existing ASGI final-issuance middleware owns real pixel read,
        refreshed SQL/actor/lease proof and final no-await ImageContent send.
        """
        try:
            source_id, illustration_id = _media_selector(locator)
        except PublicQueryInvalid:
            raise ToolError('{"status":"invalid_query","reason":"invalid_illustration_locator"}') from None
        if self.illustration_renderer is not None:
            return await self._search(None,principal,evidence=True,catalog_request=dict(
                operation='source_illustration',source_id=source_id,query=illustration_id))
        gate = self.public_media_gate
        if gate is None:
            raise ToolError('{"status":"unavailable","reason":"media_owner_not_registered"}')
        verified = None
        issued = None
        retained = False
        try:
            # Reuse the OAuth, current PG binding and independent native media
            # issuance owner; ordinary pixels do not require history renewal.
            verified = await self.read_authority.resolve_oauth(
                principal.access_token, destination=self.destination)
            await self.read_authority.require(verified, "public_search")
            binding = await asyncio.to_thread(current_binding, self.worker_queue)
            if self.source_gate.binding_current(binding) is not True:
                raise PermissionError("target binding changed")
            issued = await gate.issue(verified=verified, binding=binding,
                                      source_id=source_id, illustration_id=illustration_id)
            async def refresh(previous):
                # The middleware never reuses cached pixels: invalidate the
                # prior ticket BEFORE awaiting fresh native/archival read.
                gate.release(previous.ticket)
                fresh = None
                try:
                    async with asyncio.timeout(8):
                        fresh = await gate.issue(
                            verified=verified, binding=binding,
                            source_id=source_id, illustration_id=illustration_id)
                        image = await gate.read_issued_bytes(fresh)
                    if not gate.issuance_current(fresh.ticket):
                        raise PermissionError("media changed after pixel read")
                    return _GatedMedia(fresh, image)
                except BaseException:
                    if fresh is not None:
                        gate.release(fresh.ticket)
                    raise

            placeholder = [
                TextContent(type="text",
                    text='{"status":"unavailable","reason":"final_media_issuance_pending"}')
            ]
            # The placeholder never contains pixels. Only guarded ASGI rerender
            # can include ImageContent; without request-local capture, DENY.
            if not retain_read_output(gate, _GatedMedia(issued),
                   self._render_issued_media,
                   on_refresh=refresh,
                   on_release=lambda: self.read_authority.release(verified)):
                raise PermissionError("current public media output boundary required")
            retained = True
            return placeholder
        except asyncio.CancelledError:
            raise
        except ToolError:
            raise
        except PermissionError:
            raise ToolError('{"status":"denied","reason":"access_denied"}') from None
        except Exception:
            raise ToolError('{"status":"unavailable","reason":"media_not_ready"}') from None
        finally:
            if not retained:
                if issued is not None:
                    gate.release(issued.ticket)
                if verified is not None:
                    self.read_authority.release(verified)

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
                title=_evidence_title(item), text=item.text, url=locator(item),
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
        def inline_result(item):
            next_id = (passage_fetch_id(item.source_continuation)
                if item.source_continuation and not item.continuation_complete else None)
            incomplete = next_id is not None or (item.text_form == 'excerpt' and not item.continuation_complete)
            return SearchResult(id=fetch_id(item), title=_evidence_title(item), url=locator(item),
                text=item.text, incomplete=incomplete, next_id=next_id,
                metadata={'citation': item.metadata.citation, 'locator': item.metadata.locator,
                    'source_provenance': item.metadata.source_provenance,
                    'text_form': item.text_form, 'continuation_complete': not incomplete,
                    'source_revisions': [source.model_dump() for source in item.dependencies.source_revisions]})
        return SearchOutput(results=[inline_result(item) for item in result.evidence], **common)

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
        common['title'] = _evidence_title(item)
        next_id = (passage_fetch_id(item.source_continuation)
            if item.source_continuation is not None and not item.continuation_complete else None)
        return FetchOutput(text=item.text, metadata={
            'citation': item.metadata.citation, 'locator': item.metadata.locator,
            'source_provenance':item.metadata.source_provenance,
            'illustrations':list(result.illustrations),
            'illustrations_has_more':result.illustrations_has_more,
            'text_form': item.text_form, 'bounded_passage': True,
            'text_offset': item.source_continuation.text_offset - len(item.text)
                if item.source_continuation else locator.text_offset,
            'bibliography_offset': locator.bibliography_offset,
            'next_id': next_id, 'continuation_complete': next_id is None,
            'source_revisions': [source.model_dump() for source in item.dependencies.source_revisions]}, **common)

    @staticmethod
    def _render_story(result, *, attempt):
        output={**(result.record or {}),**({'next_cursor':editorial_cursor(result.record,result.observed_database_generation)}
            if type(result) is EditorialReadResult and result.record is not None else {}),'status':result.status,'reason':result.reason,
            'observed_data_seq':result.observed_data_seq,'observed_projection_seq':result.observed_projection_seq,
            'read_trace':ReadTrace(**attempt.correlation(),
                backend_elapsed_ms=(time.monotonic()-attempt.started)*1000).model_dump(mode='json')}
        # Use the SAME opaque locator and independent ACL/revocation checks as
        # ordinary fetch. The Story never confers a new passage-read grant.
        if result.record is not None and 'passage_refs' in result.record:
            if (result.observed_database_generation is None or result.observed_visibility_epoch is None
                    or len(result.evidence)!=1 or len(result.evidence[0].dependencies.source_revisions)!=1):
                raise ValueError('current Story source locator required')
            source=result.evidence[0].dependencies.source_revisions[0]
            output['passage_refs']=[{**ref,'fetch_id':passage_fetch_id(PassageContinuation(
                database_generation=result.observed_database_generation,
                source=source, passage_id=ref['passage_id'],
                visibility_epoch=result.observed_visibility_epoch,
                text_offset=0,bibliography_offset=0))}
                for ref in result.record['passage_refs']]
        return output

    @staticmethod
    def _render_fact(result, *, attempt, comparison=False):
        if comparison and result.status=='ok':
            from .observation_comparison import render_comparison
            compared=render_comparison(result)
        else:compared={}
        output=dict(status=result.status,reason=result.reason,items=list(result.records),
            has_more=result.next_cursor is not None,next_cursor=result.next_cursor,
            retrieval_mode='indexed_native_source_facts',
            observed_data_seq=result.observed_data_seq,
            observed_projection_seq=result.observed_projection_seq,
            read_trace=ReadTrace(**attempt.correlation(),
                backend_elapsed_ms=(time.monotonic()-attempt.started)*1000).model_dump(mode='json'))
        output.update(compared)
        return output

    @staticmethod
    def _render_chunks(result, *, attempt, request):
        # The native result is already bounded and source-provenance gated.
        return dict(source_id=request['source_id'],page_start=request['page_start'],
            page_end=request['page_end'],chunks=list(result.records),
            next_after_ordinal=result.next_after_ordinal,
            has_more=result.next_after_ordinal is not None,
            status=result.status,reason=result.reason,
            observed_data_seq=result.observed_data_seq,
            observed_projection_seq=result.observed_projection_seq,
            read_trace=ReadTrace(**attempt.correlation(),
                backend_elapsed_ms=(time.monotonic()-attempt.started)*1000).model_dump(mode='json'))

    @staticmethod
    def _render_graph(result,*,attempt):
        return dict(status=result.status,reason=result.reason,items=list(result.records),
            next_cursor=result.next_cursor,has_more=result.next_cursor is not None,
            coverage='accepted_source_graph',geometry_acceptance='source_claims_not_owner_geometry',
            observed_data_seq=result.observed_data_seq,observed_projection_seq=result.observed_projection_seq,
            read_trace=ReadTrace(**attempt.correlation(),backend_elapsed_ms=(time.monotonic()-attempt.started)*1000).model_dump(mode='json'))

    async def source_graph_fetch(self,entity_id,principal,*,limit=3,cursor=None,category=None,discovery_job_id=None):
        if discovery_job_id is not None:
            raise ToolError('{"status":"unavailable","reason":"target_graph_discovery_unavailable"}')
        action={'alias':'aliases','relation':'relations'}.get(category,category) or 'entity'
        return await self._search('',principal,evidence=True,graph_request=dict(entity_id=entity_id,
            action=action,limit=limit,cursor=cursor))

    async def entity_list(self,principal,*,document_ids=None,kinds=None,query='',limit=3,cursor=None):
        # Preserve the native graph's exact source bound while forwarding the
        # existing public name/kind selectors; never scan corpus on Dev.
        if not isinstance(document_ids,(list,tuple)) or len(document_ids)!=1:
            raise ToolError('{"status":"unavailable","reason":"target_graph_exact_source_scope_required"}')
        return await self._search('',principal,evidence=True,graph_request=dict(
            source_id=document_ids[0],action='list',limit=limit,cursor=cursor,
            kinds=(tuple(kinds) or None) if isinstance(kinds,(list,tuple)) else kinds,query=query))

    async def geo_queue(self, principal, *, limit=5):
        # Existing OAuth/read gate and current source-dependent output issuance.
        # The worker executes exactly one bounded, accepted-only SQL query.
        return await self._search('', principal, evidence=True,
            graph_request=dict(action='geo_queue', limit=limit))

    async def _search(self, query, principal, *, evidence, source_ids=None, limit=3, exact_locator=None,
                      story_request=None,fact_request=None,catalog_request=None,graph_request=None,kinds=('source',),trusted=None,story_cursor_generation=None):
        # Draft: search waits at most 8s for admission, then has 2s to execute.
        illustration=catalog_request is not None and catalog_request.get('operation')=='source_illustration'
        exact=exact_locator is not None or story_request is not None or fact_request is not None or catalog_request is not None or graph_request is not None
        deadline = (min(time.monotonic()+(2 if exact else 10),trusted['deadline'])
            if trusted is not None else time.monotonic()+(2 if exact else 10))
        telemetry = self.read_telemetry
        operation='illustration_fetch' if illustration else 'fetch' if exact else 'knowledge_search' if evidence else 'search'
        attempt = telemetry.begin_attempt(operation)
        authority = trusted['authority'] if trusted is not None else None
        destination=trusted['destination'] if trusted is not None else self.destination
        ticket = permit = gated = None
        admission_deadline=deadline
        retained = False
        status, reason = 'unavailable', 'not_ready'
        branches = ()
        controller_stages={};dispatched_at=None;received_at=None
        async def measured(name,awaitable):
            began=time.monotonic()
            try:return await awaitable
            finally:controller_stages[name]=(time.monotonic()-began)*1000
        async def authority_measured(prefix,awaitable):
            from .read_telemetry import authority_timing
            token=authority_timing.set((controller_stages,prefix))
            try:return await awaitable
            finally:authority_timing.reset(token)
        def terminal(final_status,final_reason):
            values=dict(controller_stages)
            if received_at is not None:
                values['result_to_terminal_ms']=(time.monotonic()-received_at)*1000
            telemetry.finish_attempt(attempt,status=final_status,reason=final_reason,
                executed_branches=branches,controller_stages_ms=values)
        def failure(final_status,final_reason,**extra):
            return ToolError(json.dumps(dict(status=final_status,reason=final_reason,
                read_trace=ReadTrace(**attempt.correlation(),
                    backend_elapsed_ms=(time.monotonic()-attempt.started)*1000).model_dump(mode='json'),
                **extra),separators=(',',':')))
        try:
            async with asyncio.timeout_at(None if illustration else min(deadline,attempt.started+2)) as timeout:
                try:
                    filters = QueryFilters(source_ids=tuple(source_ids) if source_ids is not None else None,
                        kinds=kinds)
                    if filters.source_ids is not None and any(str(UUID(value)) != value for value in filters.source_ids):
                        raise ValueError('canonical source ids required')
                except (ValueError, TypeError, AttributeError):
                    raise PublicQueryInvalid from None
                if trusted is None:
                    authority = await measured('auth_ms',authority_measured('auth',
                        self.read_authority.resolve_oauth(principal.access_token,destination=destination)))
                else:
                    if type(authority) is not VerifiedReadAuthority or authority.destination!=destination:
                        raise PermissionError('adapter destination differs')
                    await measured('auth_ms',authority_measured('auth',
                        self.read_authority.require(authority,'public_search')))
                    await measured('request_activity_ms',
                        asyncio.to_thread(self.worker_queue.record_request_activity))
                try:
                    # Authenticated actor + verified current PG/worker binding
                    # suffice for reads. History renewal belongs to writes
                    # and must never hide previously published pages.
                    binding = await measured('binding_ms',asyncio.to_thread(current_binding, self.worker_queue))
                except TargetUnavailable:
                    # An already-authorized caller records a durable demand;
                    # the existing controller owns the only remote launch.
                    demand = await measured('startup_demand_ms',
                        asyncio.to_thread(self.worker_queue.request_startup))
                    if demand['status']=='unavailable':
                        status,reason='unavailable',demand['reason']
                        raise failure(status,reason,startup_ref=demand['startup_ref']) from None
                    status,reason='pending','target_warming'
                    if exact:
                        # Exact fetch/catalogue/read DTOs do not have pending.
                        # Do not silently substitute empty source content.
                        raise failure('unavailable','target_warming',startup_ref=demand['startup_ref']) from None
                    startup_status=StartupStatus(startup_ref=demand['startup_ref'],
                        status='pending',reason='target_warming',
                        **await measured('startup_eta_ms',self._startup_wait(demand['startup_ref'])))
                    await measured('startup_final_auth_ms',authority_measured('startup_auth',
                        self.read_authority.recheck(authority)))
                    common=dict(status='pending',reason='target_warming',mode='lexical_degraded',
                        retrieval_mode='lexical_only',main_state='starting',startup=startup_status,
                        read_trace=ReadTrace(**attempt.correlation(),
                            backend_elapsed_ms=(time.monotonic()-attempt.started)*1000))
                    return EvidenceSearchOutput(evidence=[],**common) if evidence else SearchOutput(results=[],**common)
                if story_cursor_generation is not None and story_cursor_generation!=binding.database_generation:
                    raise PublicQueryInvalid
                if not exact:timeout.reschedule(deadline)
                try:
                    identity = dict(request_id=attempt.request_id,
                        connection_generation=binding.connection_generation,
                        turn_generation=trusted['turn_generation'] if trusted else 0,
                        min_data_seq=max(binding.min_data_seq,trusted['min_data_seq']) if trusted else binding.min_data_seq,
                        actor=TrustedActorContext(person_id=authority.person_id, audience=authority.destination.key,
                            auth_epoch=authority.auth_revision, visibility_epoch=0, context_ref=authority.context_ref),
                        deadline_ms=min(2000 if exact else 10000,max(1,int((deadline-time.monotonic())*1000))))
                    request = (SourceGraphReadRequest(**graph_request,**identity) if graph_request is not None else
                        SourceReadRequest(**catalog_request,**identity) if catalog_request is not None else
                        FactReadRequest(**fact_request,**identity) if fact_request is not None else
                        (EditorialReadRequest if self.source_gate.catalog_supported() else StoryReadRequest)(**story_request,**identity) if story_request is not None else
                        PassageReadRequest(locator=exact_locator, **identity) if exact_locator is not None else
                        Query(query=query, filters=filters, evidence_limit=limit, **identity))
                except (ValueError, TypeError):
                    raise PublicQueryInvalid from None

                permit = await measured('dispatch_gate_ms',self.source_gate.dispatch(authority, request, binding))
                admission_started=time.monotonic()
                def dispatched(resource):
                    nonlocal dispatched_at,deadline
                    dispatched_at=time.monotonic()
                    deadline=min(deadline,dispatched_at+2)
                    if not illustration:timeout.reschedule(deadline)
                    controller_stages['admission_ms']=(dispatched_at-admission_started)*1000
                    telemetry.bind_dispatch(attempt,
                        runtime=ReadRuntime(resource.run_id, resource.runtime_incarnation),
                        connection_generation=resource.dispatch_generation,
                        turn_generation=request.turn_generation)
                admission_deadline=min(deadline-(2 if request.deadline_ms>2000 else 0),time.monotonic()+8)
                ticket = await self.worker_queue.shared_reads.begin_read(authority, permit.query,
                    dialog_ref=destination.key, deadline=admission_deadline, on_dispatch=dispatched)
                result = await asyncio.shield(ticket.result)
                received_at=time.monotonic()
                if dispatched_at is not None:
                    controller_stages['dispatch_to_result_ms']=(received_at-dispatched_at)*1000
                branches = ('lexical', 'dense') if getattr(result, 'mode', None) == 'hybrid' else ()
                gated = await measured('finish_gate_ms',self.source_gate.finish(permit, result))
                permit = None
                status, reason = gated.result.status, gated.result.reason
                if (gated.result.status != 'ok' if exact else gated.result.mode != 'hybrid'):
                    if catalog_request is not None and catalog_request.get('operation')=='source_unit_pdf':
                        raise failure(status,reason,next_action=(
                            'continue_book_ingest' if reason=='unit_not_ready' else
                            'book_find_units' if reason=='unit_not_found' else
                            'select_shorter_complete_unit' if reason=='pdf_transport_limit' else
                            'source_structure_unsupported' if reason=='pdf_render_unsupported' else 'retry_later'))
                    raise failure(status,reason)
                if graph_request is not None:
                    render = lambda value: self._render_graph(value,attempt=attempt)
                elif catalog_request is not None:
                    operation = catalog_request.get('operation')
                    if operation == 'accepted_chunks':
                        render = lambda value: self._render_chunks(
                            value,attempt=attempt,request=catalog_request)
                    elif operation == 'source_units':
                        render = lambda value: dict(
                            status='ok',source_id=catalog_request['source_id'],
                            units=list(value.records),
                            next_after_ordinal=value.next_after_ordinal)
                    elif operation == 'source_unit_pdf':
                        render = lambda value: source_unit_pdf_result(value.records[0])
                    else:
                        render = lambda value: BookFindOutput(
                            query=catalog_request['query'],results=list(value.records))
                elif fact_request is not None:
                    render = lambda value: self._render_fact(
                        value,attempt=attempt,comparison=fact_request['action']=='observation_compare')
                elif story_request is not None:
                    render = lambda value: self._render_story(value,attempt=attempt)
                elif exact_locator is not None:
                    render = lambda value: self._render_fetch(
                        value,locator=exact_locator,attempt=attempt)
                else:
                    render = lambda value: self._render(value,evidence=evidence,attempt=attempt)
                output = None if illustration else render(gated.result)
                if illustration:
                    from .read_issuance import MAX_RESPONSE_BYTES
                    from .kaggle_protocol import QUERY_MAX_BYTES
                    pixels=b''
                    def render(value):
                        if value.status!='ok':
                            return CallToolResult(isError=True,content=[TextContent(type='text',
                                text=json.dumps(dict(status=value.status,reason=value.reason)))])
                        return CallToolResult(content=[TextContent(type='text',text=json.dumps(
                            dict(status='ready',metadata={k:v for k,v in value.records[0].items()
                                if k not in ('ingestion_id', '_citation_catalog')}),ensure_ascii=False)),
                            ImageContent(type='image',data=base64.b64encode(pixels).decode('ascii'),
                                mimeType='image/jpeg'),
                            TextContent(type='text', text=_illustration_caption(value.records[0]))])
                    empty=render(gated.result).model_dump(mode='json',by_alias=True,exclude_none=True)
                    overhead=len(json.dumps(empty,ensure_ascii=False,separators=(',',':')).encode())
                    # Existing response ceiling includes base64 and metadata.
                    # Existing control-frame bound reserves the RPC envelope.
                    budget=((MAX_RESPONSE_BYTES-overhead-QUERY_MAX_BYTES)//4)*3
                    accepted=gated.result
                    self.source_gate.release(gated.ticket)
                    pixels=await self.illustration_renderer(accepted.records[0],binding,max_bytes=budget)
                    # Rendering may outlive a metadata ticket. Issue through the
                    # ordinary gate after I/O, rechecking actor, binding and
                    # withdrawal; final middleware still refreshes it.
                    permit=await self.source_gate.dispatch(authority,request,binding)
                    gated=await self.source_gate.finish(permit,accepted)
                    permit=None
                    output=render(gated.result)
                if not retain_read_output(self.source_gate, gated, render,
                        on_release=lambda: self.read_authority.release(authority),
                        on_terminal=terminal):
                    raise TargetUnavailable('public read output boundary required')
                retained = True
                # Authority must live until the final output check, not merely
                # until QUERY_RESULT. The request-local guard owns its release.
                return output
        except asyncio.CancelledError:
            if not asyncio.current_task().cancelling():
                status = 'unavailable'
                reason = 'deadline_exceeded' if time.monotonic() >= (deadline if dispatched_at is not None else admission_deadline)-.002 else 'context_gap'
                raise failure(status,reason) from None
            status, reason = 'cancelled', 'caller_cancelled'
            raise
        except TimeoutError:
            status, reason = 'unavailable', 'deadline_exceeded'
            raise failure(status,reason) from None
        except ToolError:
            raise
        except PermissionError:
            status, reason = 'denied', 'access_denied'
            raise failure(status,reason) from None
        except PublicQueryInvalid:
            status, reason = 'invalid_query', 'invalid_input'
            raise failure(status,reason) from None
        except Exception:
            status, reason = 'unavailable', 'not_ready'
            raise failure(status,reason) from None
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
                terminal(status,reason)

    async def _startup_wait(self, startup_ref):
        eta,state=await asyncio.to_thread(self.worker_queue.startup_estimate,startup_ref)
        if state=='estimated':
            message=f' Примерное оставшееся ожидание: {eta//60} мин. Оценка по предыдущим успешным запускам.'
        elif state=='overdue':
            message=' Запуск длится дольше обычного; оставшееся время пока неизвестно.'
        else:
            message=' Надёжной оценки оставшегося времени пока нет.'
        return dict(eta_seconds=eta,message_ru=PENDING_STARTUP_MESSAGE_RU+message)

    async def startup_status(self, startup_ref, principal):
        """MCP read-only continuation: freshly authorized, never auto-retries the query."""
        attempt=self.read_telemetry.begin_attempt('startup_status')
        status,reason='unavailable','not_ready'
        if type(startup_ref) is not str:
            self.read_telemetry.finish_attempt(attempt,status='invalid_query',reason='invalid_input')
            raise ToolError('{"status":"invalid_query","reason":"invalid_input"}')
        authority=None;health_binding=None
        try:
            authority=await self.read_authority.resolve_oauth(principal.access_token,destination=self.destination)
            await self.read_authority.require(authority,'public_search')
            observed=await asyncio.to_thread(self.worker_queue.startup_status,startup_ref)
            if observed['status']=='ready':
                try:
                    await asyncio.to_thread(current_binding,self.worker_queue)
                except TargetUnavailable:
                    observed.update(status='unavailable',reason='target_not_ready')
            await self.read_authority.recheck(authority)
            from .kaggle_health import heartbeat_health, collect_health, DatabaseHealth
            try:
                if observed['status']=='ready':
                    from .kaggle_protocol import SqlHealthRequest, SqlHealthResult
                    async def native_probe(binding):
                        nonlocal health_binding
                        health_binding=binding
                        deadline=time.monotonic()+2
                        request=SqlHealthRequest(request_id=uuid4().hex,
                            connection_generation=binding.connection_generation,turn_generation=0,
                            database_generation=binding.database_generation,min_data_seq=binding.min_data_seq,
                            deadline_ms=2000,
                            actor=TrustedActorContext(person_id=authority.person_id,audience=authority.destination.key,
                                auth_epoch=authority.auth_revision,visibility_epoch=0,context_ref=authority.context_ref))
                        ticket=None
                        try:
                            ticket=await self.worker_queue.shared_reads.begin_read(authority,request,
                                dialog_ref=authority.destination.key,deadline=deadline)
                            try:result=await asyncio.shield(ticket.result)
                            except asyncio.CancelledError:
                                if asyncio.current_task().cancelling():raise
                                await self.read_authority.recheck(authority)
                                if time.monotonic()>=deadline:
                                    return DatabaseHealth(binding,reason='pg_timeout')
                                raise TargetUnavailable('SQL health request attachment changed') from None
                            if type(result) is not SqlHealthResult:
                                raise TargetUnavailable('SQL health response type differs')
                            if result.health is None:
                                return DatabaseHealth(binding,reason='pg_timeout'
                                    if result.reason=='deadline_exceeded' else 'pg_query_failed')
                            if (result.health.schema_version is not None
                                    and result.observed_database_generation!=binding.database_generation):
                                raise TargetUnavailable('SQL health generation differs')
                            return DatabaseHealth(binding,**result.health.model_dump())
                        finally:
                            if ticket is not None:self.worker_queue.shared_reads.cancel_read(ticket)
                    observed['health']=await collect_health(queue=self.worker_queue,native_probe=native_probe)
                else:
                    observed['health']=await asyncio.to_thread(heartbeat_health,self.worker_queue)
            except Exception:
                observed['health']={'codec':'rkb.health.v1','reason':'health_probe_unavailable','read_ready':False}
            await self.read_authority.recheck(authority)
            if health_binding is not None and observed['health'].get('read_ready'):
                try:still_current=current_binding(self.worker_queue)==health_binding
                except TargetUnavailable:still_current=False
                if not still_current:
                    observed['health'].update(postgres=None,read_ready=False,reason='attachment_changed')
            if observed['status']=='pending':
                observed.update(await self._startup_wait(startup_ref))
                await self.read_authority.recheck(authority)
            output=StartupStatus.model_validate(observed)
            status,reason=output.status,output.reason
            return output
        except asyncio.CancelledError:
            status,reason='cancelled','caller_cancelled'
            raise
        except (ValueError,TypeError):
            status,reason='invalid_query','invalid_input'
            raise ToolError('{"status":"invalid_query","reason":"invalid_input"}') from None
        except PermissionError:
            status,reason='denied','access_denied'
            raise ToolError('{"status":"denied","reason":"access_denied"}') from None
        finally:
            if authority is not None:self.read_authority.release(authority)
            self.read_telemetry.finish_attempt(attempt,status='ok' if status=='ready' else status,reason=reason)

    async def search(self, query, principal, *, document_ids=None, main_job_id=None, aliases=None, top_k=3):
        if main_job_id or aliases:
            raise ToolError('{"status":"unavailable","reason":"unsupported_target_search_option"}')
        return await self._search(query, principal, evidence=False, source_ids=document_ids, limit=top_k)

    async def search_evidence(self, query, principal, *, max_evidence=3, document_ids=None, main_job_id=None):
        if main_job_id:
            raise ToolError('{"status":"unavailable","reason":"unsupported_target_search_option"}')
        return await self._search(query, principal, evidence=True, source_ids=document_ids, limit=max_evidence)

    async def fetch_chunks(self, principal, *, source_id, page_start, page_end,
                           after_ordinal=None, limit=8):
        # Reuse fetch's exact OAuth / public_search / source gate / final issuance.
        return await self._search(None,principal,evidence=True,catalog_request=dict(
            operation='accepted_chunks',source_id=source_id,page_start=page_start,
            page_end=page_end,after_ordinal=after_ordinal,limit=limit))

    async def fetch(self, id, principal):
        try:
            locator = parse_passage_fetch_id(id)
        except (ValueError, TypeError):
            raise ToolError('{"status":"invalid_query","reason":"invalid_input"}') from None
        return await self._search(None, principal, evidence=True, exact_locator=locator)

    async def book_find(self,query,principal,*,limit=8):
        return await self._search(None,principal,evidence=True,catalog_request=dict(query=query,limit=limit))

    async def source_units(self,source_id,principal,*,after_ordinal=None,limit=8):
        """Existing authorized source read path; no new importer or search backend."""
        return await self._search(None,principal,evidence=True,catalog_request=dict(
            operation='source_units',source_id=source_id,after_ordinal=after_ordinal,
            limit=limit))

    async def source_unit_export(self,source_id,article_id,principal):
        """Client-neutral, authorized native PDF read; return actual MCP bytes."""
        return await self._search(None,principal,evidence=True,catalog_request=dict(
            operation='source_unit_pdf',source_id=source_id,article_id=article_id,limit=1))

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
        current=self.source_gate.catalog_supported()
        if to_revision is not None or (not current and (cursor is not None or from_revision not in (None,1))) or not 1<=limit<=10:
            raise ToolError('{"status":"unavailable","reason":"unsupported_target_story_history"}')
        params=dict(story_id=story_id,view='history',limit=limit,after_revision=max(0,(from_revision or 1)-1));generation=None
        if cursor is not None:
            if from_revision is not None:raise ToolError('{"status":"invalid_query","reason":"invalid_input"}')
            try:page,generation=parse_editorial_cursor(cursor,story_id=story_id,view='history');params.update(page)
            except (ValueError,TypeError):raise ToolError('{"status":"invalid_query","reason":"invalid_input"}') from None
        return await self._search(None,principal,evidence=True,kinds=('story',),story_request=params,story_cursor_generation=generation)

    async def story_variants(self,story_id,principal,*,limit=10,cursor=None):
        if (cursor is not None and not self.source_gate.catalog_supported()) or not 1<=limit<=10:
            raise ToolError('{"status":"unavailable","reason":"unsupported_target_story_variants"}')
        params=dict(story_id=story_id,view='variants',limit=limit);generation=None
        if cursor is not None:
            try:page,generation=parse_editorial_cursor(cursor,story_id=story_id,view='variants');params.update(page)
            except (ValueError,TypeError):raise ToolError('{"status":"invalid_query","reason":"invalid_input"}') from None
        return await self._search(None,principal,evidence=True,kinds=('story',),story_request=params,story_cursor_generation=generation)

    async def story_search(self,query,principal,*,filters=None,mode='hybrid',order='updated',limit=3,cursor=None):
        if mode!='hybrid' or order!='updated' or cursor is not None or not 1<=limit<=5:
            raise ToolError('{"status":"unavailable","reason":"unsupported_target_story_search"}')
        # Existing QueryFilters/Story SQL already scope lexical, dense and
        # hydration by source. Other legacy filters need native predicates:
        # never post-filter a global top-K or silently discard restrictions.
        if filters is not None and (type(filters) is not dict or set(filters)-{'source_id'}):
            raise ToolError('{"status":"unavailable","reason":"unsupported_target_story_filter"}')
        source_ids=None
        if filters and 'source_id' in filters:
            source_id=filters['source_id']
            try:
                if type(source_id) is not str or str(UUID(source_id))!=source_id:
                    raise ValueError('canonical source UUID required')
            except (TypeError,ValueError,AttributeError):
                raise ToolError('{"status":"invalid_query","reason":"invalid_input"}') from None
            source_ids=(source_id,)
        return await self._search(query,principal,evidence=True,kinds=('story',),
            source_ids=source_ids,limit=limit)

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

    async def story_observation_compare(self,story_id,left_id,right_id,principal):
        return await self._search(None,principal,evidence=True,kinds=('fact',),fact_request=dict(
            action='observation_compare',story_id=story_id,left_id=left_id,right_id=right_id,limit=2))
