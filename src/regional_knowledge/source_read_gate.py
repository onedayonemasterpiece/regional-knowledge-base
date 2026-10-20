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
from .kaggle_editorial_contracts import EditorialReadRequest,EditorialReadResult
from .kaggle_source_graph_read import SourceGraphReadRequest, SourceGraphReadResult
from .read_authority import ReadAuthority
from .kaggle_acceptance_contracts import AcceptanceReadRequest,AcceptanceReadResult,BranchRanks


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
        return self.catalog_supported()

    def catalog_supported(self):
        from .source_import_v4 import SOURCE_IMPORT_V4,source_import_v4_handler
        from .story_editorial import EDITORIAL_REVISIONS
        from .kaggle_domain_bindings import DOMAIN_REVISIONS,current_handler
        for handler in self.spool.registry.handlers.values():
            if handler.route!=SOURCE_IMPORT_V4:continue
            if handler.revisions==EDITORIAL_REVISIONS:
                expected=source_import_v4_handler(check_sha256=handler.check.definition_sha256,apply_sha256=handler.apply.definition_sha256)
            elif handler.revisions==DOMAIN_REVISIONS:
                expected=current_handler(handler.route,check_sha256=handler.check.definition_sha256,apply_sha256=handler.apply.definition_sha256)
            else:continue
            if handler==expected:return True
        return False

    def editorial_supported(self):
        if not self.catalog_supported():return False
        from .story_editorial import EDITORIAL_REVISIONS,editorial_handler
        from .kaggle_domain_bindings import DOMAIN_REVISIONS,current_handler
        found={}
        for handler in self.spool.registry.handlers.values():
            if handler.route not in ('story.edit.v1','story.access.v1'):continue
            if handler.revisions==EDITORIAL_REVISIONS:
                expected=editorial_handler(route=handler.route,check_sha256=handler.check.definition_sha256,apply_sha256=handler.apply.definition_sha256)
            elif handler.revisions==DOMAIN_REVISIONS:
                expected=current_handler(handler.route,check_sha256=handler.check.definition_sha256,apply_sha256=handler.apply.definition_sha256)
            else:continue
            if handler==expected:found[handler.route]=handler.revisions
        return len(found)==2 and len(set(found.values()))==1

    def graph_supported(self):
        from .kaggle_domain_bindings import DOMAIN_REVISIONS
        from .source_import_v4 import IMPORT_V4_DEPENDENCY
        from .source_import import IMPORT_PRECONDITIONS
        return any(h.route=='source.import.v4' and h.revisions==DOMAIN_REVISIONS
            and h.check.name=='check_source_import_graph_v1' and h.apply.name=='apply_source_import_graph_v1'
            and h.dependency==IMPORT_V4_DEPENDENCY and h.transition is None and h.precondition_keys==IMPORT_PRECONDITIONS
            for h in self.spool.registry.handlers.values())

    async def dispatch(self, authority, query: Query | PassageReadRequest | StoryReadRequest, binding):
        self._expire()
        if type(query) not in (Query, AcceptanceReadRequest, PassageReadRequest, StoryReadRequest, FactReadRequest, SourceReadRequest,EditorialReadRequest,SourceGraphReadRequest) or len(self._permits) + len(self._tickets) >= self.max_retained:
            raise SourceGateDenied('read capacity or contract unavailable')
        await self.authority.require(authority, 'public_search')
        if type(query) is AcceptanceReadRequest:
            self._require_acceptance(authority,query,binding)
        key = binding_key(binding)
        if not self.binding_current(binding) or query.connection_generation != key[2]:
            raise SourceGateDenied('runtime binding changed')
        story = type(query) in (StoryReadRequest,EditorialReadRequest) or (type(query) is Query and query.filters.kinds == ('story',))
        if story and not self.story_supported():
            raise SourceGateDenied('native story semantics unavailable')
        if type(query) is FactReadRequest and not self.facts_supported():
            raise SourceGateDenied('native fact semantics unavailable')
        if type(query) is SourceGraphReadRequest and not self.graph_supported():
            raise SourceGateDenied('native source graph unavailable')
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
        acceptance=type(q) is AcceptanceReadRequest
        if acceptance:self._require_acceptance(read.authority,q,read.binding)
        graph = type(q) is SourceGraphReadRequest
        catalog = type(q) is SourceReadRequest
        fact = type(q) is FactReadRequest
        editorial=type(q) is EditorialReadRequest
        story = type(q) in (StoryReadRequest,EditorialReadRequest)
        exact = acceptance or graph or catalog or fact or type(q) in (PassageReadRequest,StoryReadRequest,EditorialReadRequest)
        if (type(result) is not (AcceptanceReadResult if acceptance else SourceGraphReadResult if graph else SourceReadResult if catalog else FactReadResult if fact else EditorialReadResult if editorial else StoryReadResult if story else PassageReadResult if exact else QueryResult)
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
                if acceptance and any(ref not in q.fixture_sources for ref in refs):
                    raise ValueError('observed source revision differs from frozen fixture')
        except (ValueError, TypeError, AttributeError):
            closed = self._closed(result, 'internal_error')
            read.facts, read.result = None, closed
            return closed
        sources = tuple(sorted({s for item in result.evidence for s in item.dependencies.source_ids}))
        if len(sources) > 160:
            raise SourceGateDenied('dependency bound exceeded')
        scope = q.filters.source_ids if acceptance else ((q.source_id,) if q.source_id is not None else None) if graph else None if catalog else ((q.source_id,) if q.source_id is not None else None) if fact else sources if story else (q.locator.source.source_id,) if exact else q.filters.source_ids
        if fact and q.action=='observation_compare':scope=sources
        if graph and (not self.graph_supported() or len(result.records)>q.limit or any(
                record['action']!=q.action or q.source_id is not None and record['source_id']!=q.source_id
                or q.entity_id is not None and record['entity_id']!=q.entity_id for record in result.records)):
            raise SourceGateDenied('exact source graph selector differs')
        if catalog and (not self.catalog_supported() or len(result.records)>q.limit):
            raise SourceGateDenied('exact bounded source catalogue required')
        if fact and (not self.facts_supported() or len(result.records)>q.limit or
                any(record.get('kind')!=('observations' if q.action=='observation_compare' else q.action) for record in result.records) or
                q.source_id is not None and any(source!=q.source_id for source in sources)):
            raise SourceGateDenied('exact source fact provenance differs')
        if fact and q.action=='observation_compare' and (
                len(result.records)!=2 or result.next_cursor is not None or
                tuple(record.get('id') for record in result.records)!=(q.left_id,q.right_id) or
                any(record.get('story_id')!=q.story_id for record in result.records) or
                result.records[0].get('story_revision')!=result.records[1].get('story_revision') or
                type(result.records[0].get('story_revision')) is not int or result.records[0]['story_revision']<1 or
                len(sources)!=1):
            raise SourceGateDenied('exact Story observation pair differs')
        if fact and any(q.action=='calendar' and any(record.get('fact',{}).get(key)!=getattr(q,key)
                    for key in ('month','day','calendar')) or q.action=='observations' and (
                    record.get('fact',{}).get('metric_key')!=q.metric_key or q.unit_code is not None and
                    record.get('fact',{}).get('unit_code')!=q.unit_code) for record in result.records):
            raise SourceGateDenied('exact source fact filter differs')
        if story and (not self.story_supported() or result.record['story_id']!=q.story_id
                or result.record['view']!=q.view or q.revision is not None and result.record['revision']!=q.revision
                or len(result.record['entries'])>q.limit
                or q.view=='history' and any(e['revision']<=q.after_revision for e in result.record['entries'])
                or editorial and q.view=='variants' and q.after_variant_id is not None and
                    any(e['variant_id']<=q.after_variant_id for e in result.record['entries'])):
            raise SourceGateDenied('exact story provenance differs')
        if exact and not acceptance and not graph and not story and not fact and not catalog and (len(result.evidence) != 1 or result.evidence[0].id != q.locator.passage_id
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
                expected_kind=('geo' if q.action=='poi_facts' else 'entity') if graph else 'fact' if fact else 'story' if story or type(q) is Query and q.filters.kinds==('story',) else 'source'
                if item.kind!=expected_kind or item.dependencies.grant_refs:return False
                if item.kind in ('source','fact','entity','geo'):return not item.dependencies.variant_refs and item.exact_variant_ref is None
                if item.kind!='story' or not self.story_supported():return False
                try:
                    prefix,variant,revision=item.exact_variant_ref.split(':')
                    return (prefix=='variant' and str(UUID(variant))==variant and
                        (revision=='r1' or self.catalog_supported() and revision.startswith('r') and revision[1:].isascii()
                         and revision[1:].isdecimal() and str(int(revision[1:]))==revision[1:] and 1<=int(revision[1:])<2**63)
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
            **({'mode': None, 'continuation': None, 'ranks':BranchRanks()} if type(result) is AcceptanceReadResult else
                {'mode': None, 'continuation': None} if type(result) is QueryResult else
                {'record':None} if type(result) in (StoryReadResult,EditorialReadResult) else
                {'records':(),'next_cursor':None} if type(result) in (FactReadResult,SourceGraphReadResult) else
                {'records':()} if type(result) is SourceReadResult else {}),
            'reason': reason, 'evidence': (),
            'prior_write_pending': reason == 'prior_write_pending'})

    def _require_acceptance(self,authority,request,binding):
        from .acceptance_observer import FrozenAcceptanceFixture
        fixture=getattr(self,'_acceptance_fixture',None)
        if type(fixture) is not FrozenAcceptanceFixture:
            raise SourceGateDenied('acceptance observer inactive')
        fixture.require(self.authority,authority,request,binding)

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
            if read is not None and type(read.query) is AcceptanceReadRequest:
                self._require_acceptance(read.authority,read.query,read.binding)
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
