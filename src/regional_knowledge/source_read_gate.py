"""Current source-only output barrier; capabilities are retained object identities.

The controller owns runtime bindings and final synchronous issuance. This module
does no model/provider work and keeps no corpus, catalog or independent IAM.
"""
from __future__ import annotations

from dataclasses import dataclass
import math
import time
from uuid import UUID

from .kaggle_protocol import Query, QueryResult, PassageReadRequest, PassageReadResult, StoryReadRequest, StoryReadResult, TrustedActorContext
from .kaggle_fact_contracts import FactReadRequest, FactReadResult
from .kaggle_source_read import SourceReadRequest, SourceReadResult
from .read_authority import ReadAuthority


class SourceGateDenied(PermissionError):
    pass


def _source_ids(values, *, maximum):
    if (type(values) is not tuple or not 1 <= len(values) <= maximum
            or len(set(values)) != len(values)):
        raise ValueError('bounded unique source dependencies required')
    for value in values:
        if type(value) is not str or str(UUID(value)) != value:
            raise ValueError('canonical source identity required')
    return values


def binding_key(binding):
    fields = ('run_id', 'runtime_incarnation', 'connection_generation',
              'database_generation', 'min_data_seq', 'provider_version')
    values = tuple(getattr(binding, name, None) for name in fields)
    if (any(not isinstance(values[i], str) or not 1 <= len(values[i].encode()) <= 256
            for i in (0, 1, 3, 5))
            or type(values[2]) is not int or values[2] < 1
            or type(values[4]) is not int or values[4] < 0):
        raise SourceGateDenied('trusted runtime binding required')
    return values


@dataclass(frozen=True, eq=False)
class DispatchPermit:
    query: Query | PassageReadRequest | StoryReadRequest


@dataclass(frozen=True, eq=False)
class OutputTicket:
    pass


@dataclass(frozen=True, eq=False)
class GatedResult:
    result: QueryResult | PassageReadResult | StoryReadResult
    ticket: OutputTicket


@dataclass
class _Read:
    authority: object
    query: Query | PassageReadRequest | StoryReadRequest
    binding: object
    key: tuple
    deadline: float
    facts: object = None
    result: QueryResult | PassageReadResult | StoryReadResult | None = None


class SourceReadGate:
    def __init__(self, read_authority: ReadAuthority, spool, *, binding_current,
                 ttl_seconds=30, max_retained=512, clock=time.monotonic):
        if not isinstance(read_authority, ReadAuthority) or not callable(binding_current):
            raise TypeError('canonical read authority and current binding callback required')
        if (type(ttl_seconds) not in (int, float) or not math.isfinite(ttl_seconds)
                or not 0 < ttl_seconds <= 120 or type(max_retained) is not int
                or not 1 <= max_retained <= 4096):
            raise ValueError('bounded ticket lifetime and capacity required')
        self.authority, self.spool = read_authority, spool
        self.binding_current, self.clock = binding_current, clock
        self.ttl_seconds, self.max_retained = ttl_seconds, max_retained
        self._permits, self._tickets = {}, {}

    def _expire(self):
        now = self.clock()
        for retained in (self._permits, self._tickets):
            for token, read in tuple(retained.items()):
                if read.deadline <= now:
                    del retained[token]

    def _binding(self, read):
        try:
            return (read.deadline > self.clock() and binding_key(read.binding) == read.key
                    and self.binding_current(read.binding))
        except Exception:
            return False

    def story_supported(self):
        # Semantics come from the deployment-owned immutable registry, never a
        # serialized permission flag or a caller-selected variant.
        from .source_import_v2 import SOURCE_IMPORT_V2,STORY_REVISIONS,source_import_v2_handler
        for handler in self.spool.registry.handlers.values():
            if handler.route==SOURCE_IMPORT_V2 and handler.revisions==STORY_REVISIONS:
                expected=source_import_v2_handler(revisions=STORY_REVISIONS,
                    check_sha256=handler.check.definition_sha256,apply_sha256=handler.apply.definition_sha256)
                if handler==expected:return True
        return self.facts_supported()

    def facts_supported(self):
        from .source_import_v3 import SOURCE_IMPORT_V3,FACT_REVISIONS,source_import_v3_handler
        for handler in self.spool.registry.handlers.values():
            if handler.route==SOURCE_IMPORT_V3 and handler.revisions==FACT_REVISIONS:
                expected=source_import_v3_handler(revisions=FACT_REVISIONS,
                    check_sha256=handler.check.definition_sha256,apply_sha256=handler.apply.definition_sha256)
                if handler==expected:return True
        return False

    def catalog_supported(self):
        from .source_import_v4 import SOURCE_IMPORT_V4,source_import_v4_handler
        from .story_editorial import EDITORIAL_REVISIONS
        for handler in self.spool.registry.handlers.values():
            if handler.route==SOURCE_IMPORT_V4 and handler.revisions==EDITORIAL_REVISIONS:
                if handler==source_import_v4_handler(check_sha256=handler.check.definition_sha256,
                        apply_sha256=handler.apply.definition_sha256):return True
        return False

    async def dispatch(self, authority, query: Query | PassageReadRequest | StoryReadRequest, binding):
        self._expire()
        if type(query) not in (Query, PassageReadRequest, StoryReadRequest, FactReadRequest, SourceReadRequest) or len(self._permits) + len(self._tickets) >= self.max_retained:
            raise SourceGateDenied('read capacity or contract unavailable')
        await self.authority.require(authority, 'public_search')
        key = binding_key(binding)
        if not self.binding_current(binding) or query.connection_generation != key[2]:
            raise SourceGateDenied('runtime binding changed')
        story = type(query) is StoryReadRequest or (type(query) is Query and query.filters.kinds == ('story',))
        if story and not self.story_supported():
            raise SourceGateDenied('native story semantics unavailable')
        if type(query) is FactReadRequest and not self.facts_supported():
            raise SourceGateDenied('native fact semantics unavailable')
        if type(query) is SourceReadRequest and not self.catalog_supported():
            raise SourceGateDenied('native source catalogue unavailable')
        if type(query) is Query and query.filters.kinds is not None and query.filters.kinds not in (('source',),('story',)):
            raise SourceGateDenied('independent evidence authority unavailable')
        if type(query) is Query and query.filters.source_ids is not None:
            try:
                _source_ids(query.filters.source_ids, maximum=32)
            except (ValueError, TypeError, AttributeError):
                raise SourceGateDenied('source scope contract differs') from None
        if type(query) is PassageReadRequest and query.locator.database_generation != key[3]:
            raise SourceGateDenied('source locator database changed')
        # Actor and freshness are controller projections, never caller authority.
        actor = TrustedActorContext(person_id=authority.person_id,
            audience=authority.destination.key, auth_epoch=authority.auth_revision,
            visibility_epoch=0, context_ref=authority.context_ref)
        query = query.model_copy(update={'actor': actor, 'min_data_seq': max(query.min_data_seq, key[4])})
        permit = DispatchPermit(query)
        self._permits[permit] = _Read(authority, query, binding, key,
            self.clock() + min(self.ttl_seconds, query.deadline_ms / 1000))
        return permit

    async def _gate(self, read, result):
        await self.authority.require(read.authority, 'public_search')
        if not self._binding(read):
            raise SourceGateDenied('runtime binding changed')
        q = read.query
        catalog = type(q) is SourceReadRequest
        fact = type(q) is FactReadRequest
        story = type(q) is StoryReadRequest
        exact = catalog or fact or type(q) in (PassageReadRequest,StoryReadRequest)
        if (type(result) is not (SourceReadResult if catalog else FactReadResult if fact else StoryReadResult if story else PassageReadResult if exact else QueryResult)
                or (result.request_id, result.connection_generation, result.turn_generation)
                    != (q.request_id, q.connection_generation, q.turn_generation)):
            raise SourceGateDenied('result request binding differs')
        try:
            type(result).model_validate(result.model_dump())
        except (ValueError, TypeError, AttributeError):
            closed = self._closed(result, 'internal_error')
            read.facts, read.result = None, closed
            return closed
        if result.status not in ('ok','degraded'):
            if result.evidence or (not exact and result.mode is not None):
                raise SourceGateDenied('non-search result contains evidence')
            # A native failure is already evidence-free. Preserve its actual
            # reason rather than inventing pending-write/cursor observations.
            read.facts,read.result=None,result
            return result
        try:
            # Frozen models can still be bypassed with model_copy. Validate the
            # complete native payload before inspecting its source dependency set.
            for item in result.evidence:
                ids = _source_ids(item.dependencies.source_ids, maximum=32)
                refs = item.dependencies.source_revisions
                if (len(refs) != len(ids) or {ref.source_id for ref in refs} != set(ids)
                        or len({ref.source_id for ref in refs}) != len(refs)):
                    raise ValueError('exact revisions must cover every dependency')
        except (ValueError, TypeError, AttributeError):
            closed = self._closed(result, 'internal_error')
            read.facts, read.result = None, closed
            return closed
        sources = tuple(sorted({s for item in result.evidence for s in item.dependencies.source_ids}))
        if len(sources) > 160:
            raise SourceGateDenied('dependency bound exceeded')
        scope = None if catalog else ((q.source_id,) if q.source_id is not None else None) if fact else sources if story else (q.locator.source.source_id,) if exact else q.filters.source_ids
        if catalog and (not self.catalog_supported() or len(result.records)>q.limit):
            raise SourceGateDenied('exact bounded source catalogue required')
        if fact and (not self.facts_supported() or len(result.records)>q.limit or
                any(record.get('kind')!=q.action for record in result.records) or
                q.source_id is not None and any(source!=q.source_id for source in sources)):
            raise SourceGateDenied('exact source fact provenance differs')
        if fact and any(q.action=='calendar' and any(record.get('fact',{}).get(key)!=getattr(q,key)
                    for key in ('month','day','calendar')) or q.action=='observations' and (
                    record.get('fact',{}).get('metric_key')!=q.metric_key or q.unit_code is not None and
                    record.get('fact',{}).get('unit_code')!=q.unit_code) for record in result.records):
            raise SourceGateDenied('exact source fact filter differs')
        if story and (not self.story_supported() or result.record['story_id']!=q.story_id
                or result.record['view']!=q.view or q.revision is not None and result.record['revision']!=q.revision
                or len(result.record['entries'])>q.limit
                or any(e['revision']<=q.after_revision for e in result.record['entries'])):
            raise SourceGateDenied('exact story provenance differs')
        if exact and not story and not fact and not catalog and (len(result.evidence) != 1 or result.evidence[0].id != q.locator.passage_id
                or result.evidence[0].dependencies.source_revisions != (q.locator.source,)
                or result.observed_visibility_epoch != q.locator.visibility_epoch):
            raise SourceGateDenied('exact source provenance differs')
        facts = await self.spool.read_facts(scope, sources)
        if (not self.spool.facts_current(facts) or facts.database_generation != read.key[3]
                or result.observed_database_generation != read.key[3]
                or result.observed_data_seq is None or result.observed_data_seq < q.min_data_seq
                or result.observed_projection_seq is None
                or result.observed_projection_seq < q.min_data_seq
                or result.observed_visibility_epoch is None):
            result = self._closed(result, 'prior_write_pending')
        else:
            restricted = {c.source_id for c in facts.changes if c.restriction
                or c.sequence > min(result.observed_data_seq,result.observed_projection_seq)}
            lag = facts.scope_sequence > min(result.observed_data_seq,result.observed_projection_seq)
            def supported(item):
                expected_kind='fact' if fact else 'story' if story or type(q) is Query and q.filters.kinds==('story',) else 'source'
                if item.kind!=expected_kind or item.dependencies.grant_refs:return False
                if item.kind in ('source','fact'):return not item.dependencies.variant_refs and item.exact_variant_ref is None
                if item.kind!='story' or not self.story_supported():return False
                try:
                    prefix,variant,revision=item.exact_variant_ref.split(':')
                    return (prefix=='variant' and str(UUID(variant))==variant and revision=='r1'
                        and item.dependencies.variant_refs==(item.exact_variant_ref,)
                        and item.text_form=='complete' and item.continuation is None
                        and item.source_continuation is None and len(item.dependencies.source_ids)==1)
                except (ValueError,TypeError,AttributeError):return False
            retained = tuple(item for item in result.evidence
                if supported(item)
                and (scope is None or set(item.dependencies.source_ids) <= set(scope))
                and not restricted.intersection(item.dependencies.source_ids))
            unsupported = any(not supported(item) for item in result.evidence)
            if unsupported:
                result = self._closed(result, 'access_denied')
            elif result.status in ('ok', 'degraded') and (facts.pending or facts.unknown or lag
                    or len(retained) != len(result.evidence)):
                reason = (
                    'prior_write_pending' if facts.pending or facts.unknown else
                    'projection_lag' if lag else 'evidence_withdrawn')
                result = (self._closed(result, reason) if exact else result.model_copy(update={
                    'status': 'degraded', 'reason': reason,
                    'prior_write_pending': facts.pending or facts.unknown,
                    'evidence': retained, 'continuation': None}))
        # Authority is rechecked again after the local read's await. The final
        # synchronous fence below also rejects mutations during that recheck.
        await self.authority.require(read.authority, 'public_search')
        if not self._binding(read):
            raise SourceGateDenied('runtime binding changed during output check')
        if not self.spool.facts_current(facts):
            # Missing coverage or an in-flight write is availability, not a
            # participant denial. An evidence-free response needs only the
            # current authority/runtime fence through final JSON issuance.
            result = self._closed(result, 'prior_write_pending')
            facts = None
        read.facts, read.result = facts, result
        return result

    @staticmethod
    def _closed(result, reason):
        return result.model_copy(update={'status': 'unavailable' if reason != 'access_denied' else 'denied',
            **({'mode': None, 'continuation': None} if type(result) is QueryResult else
                {'record':None} if type(result) is StoryReadResult else
                {'records':(),'next_cursor':None} if type(result) is FactReadResult else
                {'records':()} if type(result) is SourceReadResult else {}),
            'reason': reason, 'evidence': (),
            'prior_write_pending': reason == 'prior_write_pending'})

    async def finish(self, permit, result):
        self._expire()
        read = self._permits.pop(permit, None)
        if read is None:
            raise SourceGateDenied('dispatch permit expired or unknown')
        result = await self._gate(read, result)
        ticket = OutputTicket()
        self._tickets[ticket] = read
        return GatedResult(result, ticket)

    async def refresh(self, gated):
        self._expire()
        read = self._tickets.get(gated.ticket) if type(gated) is GatedResult else None
        if read is None or read.result is not gated.result:
            raise SourceGateDenied('output ticket expired or unknown')
        try:
            result = await self._gate(read, read.result)
        except BaseException:
            self.release(gated.ticket)
            raise
        self.release(gated.ticket)
        ticket = OutputTicket()
        self._tickets[ticket] = read
        return GatedResult(result, ticket)

    def issuance_current(self, ticket):
        read = self._tickets.get(ticket)
        try:
            return bool(read is not None and self._binding(read)
                and (read.facts is None or self.spool.facts_current(read.facts)))
        except Exception:
            return False

    def release(self, ticket):
        self._tickets.pop(ticket, None)

    def abandon(self, permit):
        """Controller cancellation/timeout releases the original permit immediately."""
        self._permits.pop(permit, None)

    def invalidate(self):
        self._permits.clear()
        self._tickets.clear()
        self.spool.invalidate_source_history()
