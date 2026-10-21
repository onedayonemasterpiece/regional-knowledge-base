"""Unwired, model-directed source-to-Case reconciliation leaf.

No background LLM, Case store, PG executor, authority implementation, or MCP
registration lives here. The future integration must inject current target
source reads, durable PG/CAS progress, existing OAuth/source authority and the
existing KaggleCasesBackend.disagreement_edit method. A source-pass marked
complete is NOT a cross-book reconciliation completion.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Literal, Protocol
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .disagreement_contracts import CaseCreate, EvidenceAdd


class ReconcileError(ValueError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def _canonical(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False).encode("utf-8")


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _uuid(value: str) -> str:
    try:
        canonical = str(UUID(value))
    except (TypeError, ValueError, AttributeError):
        raise ReconcileError("invalid_source_identity") from None
    if value != canonical:
        raise ReconcileError("invalid_source_identity")
    return value


class ExactInput(BaseModel):
    model_config = ConfigDict(extra="forbid")


class AssertionLink(ExactInput):
    source_id: UUID
    source_revision: int = Field(ge=1, strict=True)
    eligibility_revision: int = Field(ge=1, strict=True)
    assertion_id: UUID


class CasePlan(ExactInput):
    action: Literal["none", "create", "add_evidence"]
    case_id: UUID | None = None
    create: CaseCreate | None = None
    evidence: tuple[EvidenceAdd, ...] = ()

    @model_validator(mode="after")
    def valid_action(self):
        if self.action == "none":
            if self.case_id is not None or self.create is not None or self.evidence:
                raise ValueError("no-case plan must not contain effects")
        elif self.action == "create":
            if self.case_id is not None or self.create is None or len(self.evidence) < 2:
                raise ValueError("new Case needs typed question and both source proofs")
        elif self.case_id is None or self.create is not None or not self.evidence:
            raise ValueError("existing Case needs its real ID and sourced evidence")
        return self


class Comparison(ExactInput):
    comparison_key: str = Field(min_length=1, max_length=128, strict=True, pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]*$")
    anchor: AssertionLink
    peer: AssertionLink
    decision: Literal["conflict", "compatible", "needs_context"]
    rationale: str = Field(min_length=1, max_length=8000, strict=True)
    case_plan: CasePlan

    @model_validator(mode="after")
    def model_makes_decision(self):
        if self.anchor.source_id == self.peer.source_id:
            raise ValueError("compare different accepted sources")
        if (self.decision == "conflict") != (self.case_plan.action != "none"):
            raise ValueError("only a model-authored conflict can request Case effects")
        return self


@dataclass(frozen=True)
class Proof:
    assertion_id: str
    source_sha256: str
    page_id: str
    region_id: str
    text_start: int
    quote: str

    def validate(self) -> None:
        _uuid(self.assertion_id)
        _uuid(self.page_id)
        _uuid(self.region_id)
        if len(self.source_sha256) != 64 or any(x not in "0123456789abcdef" for x in self.source_sha256):
            raise ReconcileError("invalid_source_proof")
        if self.text_start < 0 or not self.quote:
            raise ReconcileError("invalid_source_proof")


@dataclass(frozen=True)
class SourceSnapshot:
    source_id: str
    source_revision: int
    eligibility_revision: int
    frontier_id: str
    accepted: bool
    withdrawn: bool
    source_pass_state: Literal["pending", "complete"]
    reconciliation_state: Literal["open", "complete"]
    proofs: tuple[Proof, ...]
    proofs_next_cursor: str | None = None


@dataclass(frozen=True)
class SourcePage:
    items: tuple[SourceSnapshot, ...]
    next_cursor: str | None


@dataclass(frozen=True)
class ReconcileActor:
    person_id: str


@dataclass(frozen=True)
class ReconcileIntent:
    actor_id: str
    operation_key: str
    request_sha256: str
    anchor_source: SourceSnapshot
    peer_source: SourceSnapshot
    comparison: Comparison


@dataclass(frozen=True)
class StoredIntent:
    intent: ReconcileIntent
    version: int = 1
    step_index: int = 0
    attempted: bool = False
    case_id: str | None = None
    done: bool = False


class SourceReader(Protocol):
    async def next_sources(self, actor_id: str, *, after: str | None, limit: int) -> SourcePage: ...
    async def source(self, actor_id: str, source_id: str, assertion_id: str) -> SourceSnapshot: ...


class ReconcileAuthority(Protocol):
    async def resolve(self, principal: Any, *, source_ids: tuple[str, ...], mutate: bool) -> ReconcileActor: ...
    async def current(self, actor: ReconcileActor, *, source_ids: tuple[str, ...], mutate: bool) -> None: ...


class ReconcileStore(Protocol):
    """Future PG implementation: all stage/begin/advance/finish operations must be atomic CAS.

    The store must reject changed request_sha256 for the same actor+operation_key
    and must return stored immutable bytes on a duplicate. It must not infer
    durable Case success from a source cursor or a local timeout.
    """
    async def get(self, actor_id: str, operation_key: str) -> StoredIntent | None: ...
    async def stage(self, intent: ReconcileIntent) -> StoredIntent: ...
    async def begin(self, actor_id: str, operation_key: str, expected_version: int) -> StoredIntent: ...
    async def advance(self, actor_id: str, operation_key: str, expected_version: int,
                      step_index: int, case_id: str | None) -> StoredIntent: ...
    async def finish(self, actor_id: str, operation_key: str, expected_version: int) -> StoredIntent: ...


class ExistingCaseBackend(Protocol):
    async def disagreement_edit(self, action: str, operation_key: str, principal: Any, *,
                                case_id: str | None = None, command: Any = None,
                                expected_revision: int | None = None) -> dict[str, Any]: ...


def _ensure_source(snapshot: SourceSnapshot, link: AssertionLink) -> Proof:
    if (_uuid(snapshot.source_id) != str(link.source_id)
            or snapshot.source_revision != link.source_revision
            or snapshot.eligibility_revision != link.eligibility_revision):
        raise ReconcileError("source_revision_changed")
    if not snapshot.accepted or snapshot.withdrawn:
        raise ReconcileError("source_withdrawn_or_unavailable")
    if snapshot.source_revision < 1 or snapshot.eligibility_revision < 1:
        raise ReconcileError("source_revision_changed")
    _uuid(snapshot.frontier_id)
    matches = [p for p in snapshot.proofs if p.assertion_id == str(link.assertion_id)]
    if len(matches) != 1:
        raise ReconcileError("source_assertion_unavailable")
    matches[0].validate()
    return matches[0]


def _ensure_evidence(comparison: Comparison, anchor: Proof, peer: Proof) -> None:
    proof_map = {
        str(comparison.anchor.source_id): (comparison.anchor, anchor),
        str(comparison.peer.source_id): (comparison.peer, peer),
    }
    found: set[str] = set()
    for evidence in comparison.case_plan.evidence:
        link_proof = proof_map.get(str(evidence.document_id))
        if link_proof is None:
            raise ReconcileError("case_evidence_outside_comparison")
        link, proof = link_proof
        if (evidence.source_revision != link.source_revision
                or evidence.source_sha256 != proof.source_sha256
                or str(evidence.page_id) != proof.page_id
                or str(evidence.region_id) != proof.region_id
                or evidence.text_start != proof.text_start or evidence.quote != proof.quote):
            raise ReconcileError("case_evidence_mismatch")
        found.add(str(evidence.document_id))
    if comparison.case_plan.action == "create" and found != set(proof_map):
        raise ReconcileError("new_case_requires_both_source_evidences")


def _effect_key(parent: str, index: int) -> str:
    # A stable child operation for the existing Case backend, NOT a Case ID.
    return "reconcile-case:" + _digest([parent, index])


def _steps(comparison: Comparison) -> tuple[tuple[str, Any], ...]:
    plan = comparison.case_plan
    if plan.action == "none":
        return ()
    evidence = tuple(("add_evidence", value) for value in plan.evidence)
    return (("create", plan.create), *evidence) if plan.action == "create" else evidence


def _saved(output: dict[str, Any]) -> bool:
    return (type(output) is dict and output.get("status") == "saved"
            and output.get("saved") is True and output.get("outcome_durability") == "DURABLE"
            and output.get("intent_remote_accepted") is True)


class TargetStoryReconcile:
    """An unregistered, independently testable product-domain leaf.

    Source/authority/cursor adapters must be instantiated by the target owner;
    no legacy StoryRegistry, new Case CRUD, generic LLM, or local PG is used.
    """

    def __init__(self, *, sources: SourceReader, authority: ReconcileAuthority,
                 store: ReconcileStore, cases: ExistingCaseBackend):
        self.sources, self.authority, self.store, self.cases = sources, authority, store, cases

    async def next(self, principal: Any, *, after: str | None = None, limit: int = 3) -> dict[str, Any]:
        if type(limit) is not int or not 1 <= limit <= 8 or (after is not None and (type(after) is not str or len(after) > 300)):
            raise ReconcileError("bounded_next_required")
        actor = await self.authority.resolve(principal, source_ids=(), mutate=False)
        page = await self.sources.next_sources(actor.person_id, after=after, limit=limit)
        if not isinstance(page, SourcePage) or len(page.items) > limit:
            raise ReconcileError("invalid_source_page")
        items = []
        for snapshot in page.items:
            if snapshot.accepted and not snapshot.withdrawn and snapshot.reconciliation_state == "open":
                if len(snapshot.proofs) > 8:
                    raise ReconcileError("unbounded_source_page")
                for proof in snapshot.proofs:
                    proof.validate()
                items.append(dict(source_id=snapshot.source_id, source_revision=snapshot.source_revision,
                    eligibility_revision=snapshot.eligibility_revision, frontier_id=snapshot.frontier_id,
                    source_pass_state=snapshot.source_pass_state, reconciliation_state=snapshot.reconciliation_state,
                    assertions=[dict(assertion_id=p.assertion_id, page_id=p.page_id, region_id=p.region_id,
                                     text_start=p.text_start, quote=p.quote, source_sha256=p.source_sha256)
                                for p in snapshot.proofs], assertions_next_cursor=snapshot.proofs_next_cursor))
        source_ids = tuple(item["source_id"] for item in items)
        await self.authority.current(actor, source_ids=source_ids, mutate=False)
        return dict(status="ok", items=items, next_cursor=page.next_cursor,
                    note="source_pass_state=complete is not cross_book_complete")

    async def _proofs(self, actor: ReconcileActor, comparison: Comparison) -> tuple[SourceSnapshot, SourceSnapshot]:
        a, b = comparison.anchor, comparison.peer
        left = await self.sources.source(actor.person_id, str(a.source_id), str(a.assertion_id))
        right = await self.sources.source(actor.person_id, str(b.source_id), str(b.assertion_id))
        first, second = _ensure_source(left, a), _ensure_source(right, b)
        _ensure_evidence(comparison, first, second)
        return left, right

    async def stage(self, principal: Any, comparison: Comparison) -> dict[str, Any]:
        if type(comparison) is not Comparison:
            raise ReconcileError("typed_comparison_required")
        source_ids = (str(comparison.anchor.source_id), str(comparison.peer.source_id))
        actor = await self.authority.resolve(principal, source_ids=source_ids, mutate=True)
        left, right = await self._proofs(actor, comparison)
        if left.reconciliation_state != "open":
            raise ReconcileError("cross_book_reconciliation_closed")
        key = "story-reconcile:" + _digest([actor.person_id, left.frontier_id, comparison.comparison_key])
        intent = ReconcileIntent(actor.person_id, key, _digest(comparison.model_dump(mode="json")),
                                 left, right, comparison)
        await self.authority.current(actor, source_ids=source_ids, mutate=True)
        stored = await self.store.stage(intent)
        if stored.intent.actor_id != actor.person_id or stored.intent.operation_key != key:
            raise ReconcileError("reconciliation_storage_mismatch")
        if stored.intent.request_sha256 != intent.request_sha256:
            raise ReconcileError("operation_key_conflict")
        return self._output(stored)

    async def _current_intent(self, principal: Any, operation_key: str, *, mutate: bool):
        if type(operation_key) is not str or not operation_key.startswith("story-reconcile:") or len(operation_key) != 80:
            raise ReconcileError("invalid_reconciliation_operation")
        actor = await self.authority.resolve(principal, source_ids=(), mutate=mutate)
        row = await self.store.get(actor.person_id, operation_key)
        if row is None or row.intent.actor_id != actor.person_id:
            raise ReconcileError("reconciliation_not_found")
        comparison = row.intent.comparison
        source_ids = (str(comparison.anchor.source_id), str(comparison.peer.source_id))
        await self.authority.current(actor, source_ids=source_ids, mutate=mutate)
        current_left, current_right = await self._proofs(actor, comparison)
        if (current_left.frontier_id != row.intent.anchor_source.frontier_id
                or current_right.frontier_id != row.intent.peer_source.frontier_id):
            raise ReconcileError("source_frontier_changed")
        await self.authority.current(actor, source_ids=source_ids, mutate=mutate)
        return actor, row, source_ids

    @staticmethod
    def _output(row: StoredIntent, *, reason: str | None = None) -> dict[str, Any]:
        total = len(_steps(row.intent.comparison))
        return dict(status="completed" if row.done else "pending" if (row.attempted or row.step_index) else "staged",
                    reason=reason, operation_key=row.intent.operation_key,
                    step_index=row.step_index, step_count=total,
                    current_case_operation_key=_effect_key(row.intent.operation_key, row.step_index)
                        if row.step_index < total else None,
                    case_id=row.case_id, source_pass_state=row.intent.anchor_source.source_pass_state,
                    cross_book_complete=False, request_sha256=row.intent.request_sha256)

    async def apply(self, principal: Any, operation_key: str) -> dict[str, Any]:
        actor, row, source_ids = await self._current_intent(principal, operation_key, mutate=True)
        if row.done:
            return self._output(row)
        steps = _steps(row.intent.comparison)
        if row.step_index == len(steps):
            await self.authority.current(actor, source_ids=source_ids, mutate=True)
            return self._output(await self.store.finish(actor.person_id, operation_key, row.version))
        action, command = steps[row.step_index]
        if not row.attempted:
            row = await self.store.begin(actor.person_id, operation_key, row.version)
        case_id = (row.case_id or (str(row.intent.comparison.case_plan.case_id)
                    if row.intent.comparison.case_plan.case_id is not None else None)) if action == "add_evidence" else None
        if action == "add_evidence" and not case_id:
            raise ReconcileError("case_creation_not_confirmed")
        child_key = _effect_key(operation_key, row.step_index)
        await self.authority.current(actor, source_ids=source_ids, mutate=True)
        try:
            # Same exact operation key and immutable typed command on every retry.
            # The existing Case backend, not this leaf, owns accepted/UNKNOWN/Saved.
            outcome = await self.cases.disagreement_edit(action, child_key, principal,
                case_id=case_id, command=command)
        except TimeoutError:
            return self._output(row, reason="case_result_unknown")
        if not _saved(outcome):
            return self._output(row, reason="case_outcome_pending")
        confirmed = case_id
        if action == "create":
            value = outcome.get("case_id")
            if type(value) is not str:
                raise ReconcileError("case_id_unconfirmed")
            confirmed = _uuid(value)
        elif outcome.get("case_id") != case_id:
            raise ReconcileError("case_id_mismatch")
        await self.authority.current(actor, source_ids=source_ids, mutate=True)
        row = await self.store.advance(actor.person_id, operation_key, row.version, row.step_index + 1, confirmed)
        if row.step_index == len(steps):
            await self.authority.current(actor, source_ids=source_ids, mutate=True)
            row = await self.store.finish(actor.person_id, operation_key, row.version)
        return self._output(row)

    async def status(self, principal: Any, operation_key: str) -> dict[str, Any]:
        actor, row, source_ids = await self._current_intent(principal, operation_key, mutate=False)
        result = self._output(row)
        steps = _steps(row.intent.comparison)
        if row.attempted and not row.done and row.step_index < len(steps):
            key = _effect_key(operation_key, row.step_index)
            try:
                observed = await self.cases.disagreement_edit("operation_status", key, principal,
                    case_id=row.case_id or (str(row.intent.comparison.case_plan.case_id)
                        if row.intent.comparison.case_plan.case_id is not None else None))
                result["case_receipt_verified"] = _saved(observed)
            except TimeoutError:
                result["case_receipt_verified"] = False
            # Read-only status never advances the reconciliation cursor.
        await self.authority.current(actor, source_ids=source_ids, mutate=False)
        return result
