"""Inert owner-only acceptance harness on the existing ephemeral read path.

No MCP registration or startup auto-configuration. An operator reviews and
constructs one frozen fixture only after imports and native proof verification.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
import hashlib
import json
import re
import time
from uuid import UUID

from .bge_contract import REVISION, SPACE, TOKENIZER_SHA256
from .kaggle_acceptance_contracts import AcceptanceReadRequest, AcceptanceReadResult
from .kaggle_protocol import QueryFilters, TrustedActorContext, SourceDependency
from .kaggle_public_backend import TargetReadBinding, current_binding
from .read_authority import VerifiedReadAuthority


@dataclass(frozen=True)
class FrozenAcceptanceFixture:
    owner_id: str
    source_ids: tuple[str, str]
    gold_sha256: str
    binding: TargetReadBinding
    accepted_min_data_seq: int
    native_support_sha256: str
    question_sha256s: tuple[str, ...]
    source_revisions: tuple[SourceDependency, SourceDependency]

    def __post_init__(self):
        if (str(UUID(self.owner_id)) != self.owner_id or type(self.source_ids) is not tuple
                or len(self.source_ids) != 2 or len(set(self.source_ids)) != 2
                or any(str(UUID(v)) != v for v in self.source_ids)
                or not re.fullmatch('[0-9a-f]{64}', self.gold_sha256)
                or not re.fullmatch('[0-9a-f]{64}', self.native_support_sha256)
                or type(self.question_sha256s) is not tuple or not 1<=len(self.question_sha256s)<=120
                or len(set(self.question_sha256s))!=len(self.question_sha256s)
                or any(not re.fullmatch('[0-9a-f]{64}',v) for v in self.question_sha256s)
                or type(self.source_revisions) is not tuple or len(self.source_revisions)!=2
                or any(type(s) is not SourceDependency for s in self.source_revisions)
                or tuple(s.source_id for s in self.source_revisions)!=self.source_ids
                or type(self.binding) is not TargetReadBinding
                or type(self.accepted_min_data_seq) is not int
                or self.accepted_min_data_seq < self.binding.min_data_seq):
            raise ValueError('reviewed exact fixture pins required')

    def wire(self):
        return dict(owner_id=self.owner_id, source_ids=self.source_ids,
            gold_sha256=self.gold_sha256, binding=vars(self.binding),
            accepted_min_data_seq=self.accepted_min_data_seq,
            native_support_sha256=self.native_support_sha256,question_sha256s=self.question_sha256s,
            source_revisions=[s.model_dump() for s in self.source_revisions],
            settings=dict(revision=REVISION, space=SPACE, tokenizer_sha256=TOKENIZER_SHA256,
                candidate_budget=100, rrf_k=60, hnsw_ef_search=100,
                hnsw_max_scan_tuples=1000, hnsw_iterative_scan='strict_order', rank_limit=10))

    @property
    def sha256(self):
        return hashlib.sha256(json.dumps(self.wire(),sort_keys=True,separators=(',',':')).encode()).hexdigest()

    def require(self, resolver, authority, request, binding=None):
        # This supplements, never replaces, canonical current resolver.require.
        if (type(authority) is not VerifiedReadAuthority
                or self.owner_id != resolver.participants.owner
                or authority.person_id != self.owner_id or authority.route != 'owner'
                or authority.destination.audience != 'personal'
                or type(request) is not AcceptanceReadRequest
                or request.fixture_sha256 != self.sha256
                or hashlib.sha256(request.query.encode('utf-8')).hexdigest() not in self.question_sha256s
                or request.filters.source_ids != self.source_ids
                or request.fixture_sources != self.source_revisions
                or request.min_data_seq < self.accepted_min_data_seq
                or request.connection_generation != self.binding.connection_generation
                or binding is not None and binding != self.binding):
            raise PermissionError('frozen owner acceptance fixture differs')
        AcceptanceReadRequest.model_validate(request.model_dump())


class AcceptanceObserver:
    """Trusted in-process runner; writes only through a synchronous consumer.

    Supplied authority must already belong to the actual canonical resolver.
    Ownership transfers for this attempt and is released after final issuance.
    No caller identity, credential, fixture or query flag can grant permission.
    """
    def __init__(self, *, queue, gate, fixture: FrozenAcceptanceFixture):
        if (type(fixture) is not FrozenAcceptanceFixture or queue.shared_reads.authority is not gate.authority
                or fixture.owner_id != gate.authority.participants.owner):
            raise ValueError('one canonical controller and reviewed owner fixture required')
        self.queue, self.gate, self.fixture = queue, gate, fixture
        for component in (gate,queue.shared_reads):
            previous = getattr(component,'_acceptance_fixture',None)
            if previous is not None and previous != fixture:
                raise ValueError('acceptance fixture already frozen')
        # Deliberately absent from normal production composition.
        gate._acceptance_fixture = queue.shared_reads._acceptance_fixture = fixture

    async def observe(self, query, *, authority, request_id, consume):
        if not callable(consume):
            raise TypeError('trusted synchronous private-artifact consumer required')
        deadline=time.monotonic()+2
        permit=ticket=gated=None
        try:
            async with asyncio.timeout_at(deadline):
                await self.gate.authority.require(authority,'public_search')
                binding=await asyncio.to_thread(current_binding,self.queue)
                request=AcceptanceReadRequest(query=query,request_id=request_id,
                    connection_generation=binding.connection_generation,turn_generation=0,
                    actor=TrustedActorContext(person_id=authority.person_id,audience=authority.destination.key,
                        auth_epoch=authority.auth_revision,visibility_epoch=0,context_ref=authority.context_ref),
                    min_data_seq=self.fixture.accepted_min_data_seq,
                    deadline_ms=max(1,int((deadline-time.monotonic())*1000)),
                    fixture_sha256=self.fixture.sha256,
                    fixture_sources=self.fixture.source_revisions,
                    filters=QueryFilters(source_ids=self.fixture.source_ids,kinds=('source',)))
                self.fixture.require(self.gate.authority,authority,request,binding)
                permit=await self.gate.dispatch(authority,request,binding)
                ticket=await self.queue.shared_reads.begin_read(authority,permit.query,
                    dialog_ref=authority.destination.key,deadline=deadline)
                native=await asyncio.shield(ticket.result)
                gated=await self.gate.finish(permit,native)
                permit=None
                for _ in range(2):
                    gated=await self.gate.refresh(gated)
                    value=gated.result
                    if type(value) is not AcceptanceReadResult or value.status not in ('ok','degraded') or value.mode!='hybrid':
                        raise RuntimeError('acceptance observation unavailable')
                    # No text/gold answers leave the retained source gate. The
                    # evaluator binds IDs to independently read native proofs.
                    payload=dict(fixture_sha256=self.fixture.sha256,
                        request_id=value.request_id,ranks=value.ranks.model_dump(mode='json'),
                        status=value.status,reason=value.reason,mode=value.mode,
                        observed_data_seq=value.observed_data_seq,
                        observed_projection_seq=value.observed_projection_seq,
                        observed_visibility_epoch=value.observed_visibility_epoch,
                        observed_database_generation=value.observed_database_generation,
                        items=[dict(id=item.id,source_revisions=[r.model_dump()
                            for r in item.dependencies.source_revisions]) for item in value.evidence])
                    data=json.dumps(payload,sort_keys=True,separators=(',',':')).encode()
                    if self.gate.issuance_current(gated.ticket):
                        # No await between final current fence and local issue.
                        result=consume(data)
                        if result is not None:
                            raise TypeError('consumer must synchronously persist and return None')
                        return
                raise PermissionError('acceptance changed during issuance')
        finally:
            if ticket is not None and not ticket.result.done():
                self.queue.shared_reads.cancel_read(ticket)
            if permit is not None:self.gate.abandon(permit)
            if gated is not None:self.gate.release(gated.ticket)
            self.gate.authority.release(authority)
