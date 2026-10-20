"""Current source-only output barrier; capabilities are retained object identities.

The controller owns runtime bindings and final synchronous issuance. This module
does no model/provider work and keeps no corpus, catalog or independent IAM.
"""
from __future__ import annotations

from dataclasses import dataclass
import math
import time

from .kaggle_protocol import Query, QueryResult, TrustedActorContext
from .read_authority import ReadAuthority


class SourceGateDenied(PermissionError):
    pass


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
    query: Query


@dataclass(frozen=True, eq=False)
class OutputTicket:
    pass


@dataclass(frozen=True, eq=False)
class GatedResult:
    result: QueryResult
    ticket: OutputTicket


@dataclass
class _Read:
    authority: object
    query: Query
    binding: object
    key: tuple
    deadline: float
    facts: object = None
    result: QueryResult | None = None


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

    async def dispatch(self, authority, query: Query, binding):
        self._expire()
        if type(query) is not Query or len(self._permits) + len(self._tickets) >= self.max_retained:
            raise SourceGateDenied('read capacity or contract unavailable')
        await self.authority.require(authority, 'public_search')
        key = binding_key(binding)
        if not self.binding_current(binding) or query.connection_generation != key[2]:
            raise SourceGateDenied('runtime binding changed')
        if query.filters.kinds is not None and set(query.filters.kinds) != {'source'}:
            raise SourceGateDenied('independent evidence authority unavailable')
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
        if (type(result) is not QueryResult
                or (result.request_id, result.connection_generation, result.turn_generation)
                    != (q.request_id, q.connection_generation, q.turn_generation)):
            raise SourceGateDenied('result request binding differs')
        if result.status not in ('ok','degraded'):
            if result.evidence or result.mode is not None:
                raise SourceGateDenied('non-search result contains evidence')
            # A native failure is already evidence-free. Preserve its actual
            # reason rather than inventing pending-write/cursor observations.
            read.facts,read.result=None,result
            return result
        sources = tuple(sorted({s for item in result.evidence for s in item.dependencies.source_ids}))
        if len(sources) > 160:
            raise SourceGateDenied('dependency bound exceeded')
        facts = await self.spool.read_facts(q.filters.source_ids, sources)
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
            retained = tuple(item for item in result.evidence
                if item.kind == 'source' and not item.dependencies.variant_refs
                and not item.dependencies.grant_refs and item.exact_variant_ref is None
                and (q.filters.source_ids is None or set(item.dependencies.source_ids) <= set(q.filters.source_ids))
                and not restricted.intersection(item.dependencies.source_ids))
            unsupported = any(item.kind != 'source' or item.dependencies.variant_refs
                or item.dependencies.grant_refs or item.exact_variant_ref is not None
                for item in result.evidence)
            if unsupported:
                result = self._closed(result, 'access_denied')
            elif result.status in ('ok', 'degraded') and (facts.pending or facts.unknown or lag
                    or len(retained) != len(result.evidence)):
                result = result.model_copy(update={'status': 'degraded', 'reason':
                    'prior_write_pending' if facts.pending or facts.unknown else
                    'projection_lag' if lag else 'evidence_withdrawn',
                    'prior_write_pending': facts.pending or facts.unknown,
                    'evidence': retained, 'continuation': None})
        # Authority is rechecked again after the local read's await. The final
        # synchronous fence below also rejects mutations during that recheck.
        await self.authority.require(read.authority, 'public_search')
        if not self._binding(read) or not self.spool.facts_current(facts):
            raise SourceGateDenied('read authority changed during output check')
        read.facts, read.result = facts, result
        return result

    @staticmethod
    def _closed(result, reason):
        return result.model_copy(update={'status': 'unavailable' if reason != 'access_denied' else 'denied',
            'mode': None, 'reason': reason, 'evidence': (), 'continuation': None,
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
