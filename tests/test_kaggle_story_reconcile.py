"""Synthetic only: no PostgreSQL, provider, HTTP, Docker, model or shared runtime."""
from __future__ import annotations

from dataclasses import replace
from uuid import UUID

import pytest

from regional_knowledge.disagreement_contracts import CaseCreate, EvidenceAdd
from regional_knowledge.kaggle_story_reconcile import (
    AssertionLink, CasePlan, Comparison, Proof, ReconcileActor, ReconcileError,
    SourcePage, SourceSnapshot, StoredIntent, TargetStoryReconcile,
)

A = "00000000-0000-0000-0000-000000000001"
B = "00000000-0000-0000-0000-000000000002"
C = "00000000-0000-0000-0000-000000000003"
ACTOR = "10000000-0000-0000-0000-000000000001"
OTHER = "10000000-0000-0000-0000-000000000002"
CASE = "20000000-0000-0000-0000-000000000001"


def snapshot(source_id, *, rev=1, eligible=1, source_pass="complete",
             cross_book="open", withdrawn=False, with_proof=True):
    proofs = (Proof(
        assertion_id=source_id.replace("00000000-", "30000000-", 1),
        source_sha256="a" * 64, page_id=source_id.replace("00000000-", "40000000-", 1),
        region_id=source_id.replace("00000000-", "50000000-", 1),
        text_start=4, quote="Exact original excerpt " + source_id[-1],
    ),) if with_proof else ()
    return SourceSnapshot(
        source_id=source_id, source_revision=rev, eligibility_revision=eligible,
        frontier_id=source_id.replace("00000000-", "60000000-", 1),
        accepted=True, withdrawn=withdrawn, source_pass_state=source_pass,
        reconciliation_state=cross_book, proofs=proofs,
    )


def ref(s):
    return AssertionLink(source_id=UUID(s.source_id), source_revision=s.source_revision,
                         eligibility_revision=s.eligibility_revision,
                         assertion_id=UUID(s.proofs[0].assertion_id))


def evidence(s, *, position="left"):
    p = s.proofs[0]
    return EvidenceAdd(document_id=UUID(s.source_id), source_revision=s.source_revision,
                       source_sha256=p.source_sha256, page_id=UUID(p.page_id),
                       region_id=UUID(p.region_id), text_start=p.text_start,
                       quote=p.quote, context="Synthetic witness with bounded context",
                       position_ids=[position], relation="challenges", speaker="Source author",
                       attribution="direct_statement")


def comparison(left, right, *, key="episode-A-B", action="create", rationale="Sources differ."):
    if action == "create":
        case = CasePlan(action="create", create=CaseCreate(
            question="What actually happened at the synthetic harbour?",
            context="Two sources provide different grounded descriptions.",
            classification="conflicting_accounts", rationale="Exact positions differ.",
            positions=[dict(id="left", text="First assertion", speaker="Witness A", modality="asserted"),
                       dict(id="right", text="Second assertion", speaker="Witness B", modality="asserted")]),
            evidence=(evidence(left, position="left"), evidence(right, position="right")))
        decision = "conflict"
    elif action == "add_evidence":
        case = CasePlan(action="add_evidence", case_id=UUID(CASE),
                        evidence=(evidence(left, position="left"),))
        decision = "conflict"
    else:
        case = CasePlan(action="none")
        decision = "compatible"
    return Comparison(comparison_key=key, anchor=ref(left), peer=ref(right),
                      decision=decision, rationale=rationale, case_plan=case)


class FakeAuthority:
    def __init__(self):
        self.revoked = False
        self.revoked_sources = set()
        self.calls = []

    async def resolve(self, principal, *, source_ids, mutate):
        self.calls.append(("resolve", principal, source_ids, mutate))
        if self.revoked or any(s in self.revoked_sources for s in source_ids):
            raise PermissionError("canonical authority revoked")
        return ReconcileActor(principal)

    async def current(self, actor, *, source_ids, mutate):
        self.calls.append(("current", actor.person_id, source_ids, mutate))
        if self.revoked or any(s in self.revoked_sources for s in source_ids):
            raise PermissionError("source/current permissions revoked")


class FakeSources:
    def __init__(self, *snapshots):
        self.items = {s.source_id: s for s in snapshots}
        self.reads = []

    async def next_sources(self, actor_id, *, after, limit):
        self.reads.append(("next", actor_id, after, limit))
        keys = sorted(k for k in self.items if after is None or k > after)
        selected = keys[:limit]
        return SourcePage(tuple(self.items[k] for k in selected),
                          selected[-1] if len(keys) > limit else None)

    async def source(self, actor_id, source_id, assertion_id):
        self.reads.append(("source", actor_id, source_id, assertion_id))
        snap = self.items[source_id]
        return replace(snap, proofs=tuple(p for p in snap.proofs if p.assertion_id == assertion_id))


class FakePgCursor:
    """A shared synthetic persistent map, modelling single-writer compare-and-swap."""
    def __init__(self):
        self.rows = {}
        self.stages = 0

    async def get(self, actor_id, operation_key):
        return self.rows.get((actor_id, operation_key))

    async def stage(self, intent):
        key = (intent.actor_id, intent.operation_key)
        row = self.rows.get(key)
        if row is not None:
            if row.intent.request_sha256 != intent.request_sha256:
                raise ReconcileError("operation_key_conflict")
            return row
        row = StoredIntent(intent=intent)
        self.rows[key] = row
        self.stages += 1
        return row

    def cas(self, actor, key, version, **changes):
        from dataclasses import replace
        old = self.rows[(actor, key)]
        if version != old.version:
            raise ReconcileError("stale_progress")
        row = replace(old, version=version + 1, **changes)
        self.rows[(actor, key)] = row
        return row

    async def begin(self, actor, key, version):
        return self.cas(actor, key, version, attempted=True)

    async def advance(self, actor, key, version, step_index, case_id):
        return self.cas(actor, key, version, step_index=step_index,
                        attempted=False, case_id=case_id)

    async def finish(self, actor, key, version):
        return self.cas(actor, key, version, done=True)


class FakeExistingCaseBackend:
    """Exactly one remote effect per Case operation_key; retries return same receipt."""
    def __init__(self):
        self.effects = {}
        self.attempts = []
        self.lost_first_create_reply = False

    async def disagreement_edit(self, action, operation_key, principal, *,
                                case_id=None, command=None, expected_revision=None):
        self.attempts.append((action, operation_key, principal, case_id))
        if action == "operation_status":
            return self.effects.get((principal, operation_key), {"status": "pending", "saved": False})
        row = self.effects.get((principal, operation_key))
        if row is None:
            assert action in ("create", "add_evidence"), "must reuse existing Case methods"
            assert (action != "create" or isinstance(command, CaseCreate))
            assert (action != "add_evidence" or isinstance(command, EvidenceAdd))
            case = CASE if action == "create" else case_id
            assert case == CASE
            row = dict(status="saved", saved=True, outcome_durability="DURABLE",
                       intent_remote_accepted=True, case_id=case)
            self.effects[(principal, operation_key)] = row
            if action == "create" and self.lost_first_create_reply:
                self.lost_first_create_reply = False
                raise TimeoutError("synthetic lost response AFTER remote effect")
        return row


@pytest.fixture
def environment():
    a, b, c = snapshot(A), snapshot(B), snapshot(C, with_proof=True)
    authority = FakeAuthority()
    sources = FakeSources(a, b, c)
    store = FakePgCursor()
    cases = FakeExistingCaseBackend()
    adapter = TargetStoryReconcile(sources=sources, authority=authority, store=store, cases=cases)
    return a, b, c, authority, sources, store, cases, adapter


@pytest.mark.asyncio
async def test_bounded_next_does_not_treat_source_pass_complete_as_cross_book_complete(environment):
    a, b, c, authority, sources, store, cases, adapter = environment
    sources.items[C] = replace(c, reconciliation_state="complete")
    first = await adapter.next(ACTOR, limit=1)
    assert first["items"][0]["source_id"] == A
    assert first["items"][0]["source_pass_state"] == "complete"
    assert first["items"][0]["reconciliation_state"] == "open"
    assert first["items"][0]["assertions"][0]["quote"] == a.proofs[0].quote
    assert first["next_cursor"] == A
    second = await adapter.next(ACTOR, after=first["next_cursor"], limit=2)
    assert [item["source_id"] for item in second["items"]] == [B]
    assert second["next_cursor"] is None
    assert cases.effects == {}
    with pytest.raises(ReconcileError, match="bounded_next_required"):
        await adapter.next(ACTOR, limit=100)


@pytest.mark.asyncio
async def test_stage_is_exact_and_replay_conflict_cannot_replace_model_decision(environment):
    a, b, _, _, _, store, cases, adapter = environment
    value = comparison(a, b)
    first = await adapter.stage(ACTOR, value)
    assert first["status"] == "staged" and first["step_count"] == 3
    assert first["case_id"] is None and store.stages == 1
    assert (await adapter.stage(ACTOR, value)) == first and store.stages == 1
    with pytest.raises(ReconcileError, match="operation_key_conflict"):
        await adapter.stage(ACTOR, comparison(a, b, rationale="An altered model decision."))
    assert not cases.effects


@pytest.mark.asyncio
async def test_lost_create_result_reuses_existing_case_effect_and_finishes_source_links(environment):
    a, b, _, authority, sources, store, cases, adapter = environment
    cases.lost_first_create_reply = True
    staged = await adapter.stage(ACTOR, comparison(a, b))
    operation = staged["operation_key"]
    unknown = await adapter.apply(ACTOR, operation)
    assert unknown["status"] == "pending" and unknown["reason"] == "case_result_unknown"
    checked = await adapter.status(ACTOR, operation)
    assert checked["case_receipt_verified"] is True
    assert checked["step_index"] == 0, "read-only status never advances a cursor"
    # New adapter is a fresh process; state and remote Case effects live in injected stores.
    fresh = TargetStoryReconcile(sources=sources, authority=authority, store=store, cases=cases)
    for i in range(3):
        outcome = await fresh.apply(ACTOR, operation)
    assert outcome["status"] == "completed" and outcome["case_id"] == CASE
    assert outcome["cross_book_complete"] is False
    assert len(cases.effects) == 3  # original CREATE + exactly two source evidence ops
    assert sum(1 for action, _, _, _ in cases.attempts if action == "create") == 2  # same key
    assert len({key for _, key, _, _ in cases.attempts if key in [x[1] for x in cases.attempts if x[0] == "create"]}) == 1
    assert (await fresh.apply(ACTOR, operation))["status"] == "completed"
    assert len(cases.effects) == 3


@pytest.mark.asyncio
async def test_complete_source_pass_can_record_nonconflict_without_creating_case(environment):
    a, b, _, _, _, store, cases, adapter = environment
    staged = await adapter.stage(ACTOR, comparison(a, b, action="none", key="no-false-case"))
    outcome = await adapter.apply(ACTOR, staged["operation_key"])
    assert outcome["status"] == "completed" and outcome["step_count"] == 0
    assert outcome["source_pass_state"] == "complete"
    assert outcome["cross_book_complete"] is False
    assert cases.attempts == []
    assert len(store.rows) == 1


@pytest.mark.asyncio
async def test_third_source_extends_explicit_existing_case_not_pair_as_identity(environment):
    a, b, c, authority, sources, store, cases, adapter = environment
    staged = await adapter.stage(ACTOR, comparison(c, b, key="third-source", action="add_evidence"))
    assert staged["case_id"] is None, "not yet applied, Case ID is an explicit model selection"
    applied = await adapter.apply(ACTOR, staged["operation_key"])
    assert applied["status"] == "completed" and applied["case_id"] == CASE
    assert len(cases.effects) == 1
    assert [action for action, *_ in cases.attempts] == ["add_evidence"]


@pytest.mark.parametrize("tamper", ["source_rev", "eligibility", "withdrawn", "quote", "wrong_sha",
                                     "wrong_region", "outside_source"])
@pytest.mark.asyncio
async def test_changed_original_or_provenance_fails_closed_without_stage_or_case(environment, tamper):
    a, b, _, authority, sources, store, cases, adapter = environment
    original = comparison(a, b)
    if tamper == "source_rev":
        sources.items[A] = replace(a, source_revision=2)
    elif tamper == "eligibility":
        sources.items[A] = replace(a, eligibility_revision=2)
    elif tamper == "withdrawn":
        sources.items[B] = replace(b, withdrawn=True)
    elif tamper == "quote":
        wrong = original.case_plan.evidence[0].model_copy(update={"quote": "Edited text"})
        original = original.model_copy(update={"case_plan": original.case_plan.model_copy(
            update={"evidence": (wrong, original.case_plan.evidence[1])})})
    elif tamper == "wrong_sha":
        wrong = original.case_plan.evidence[1].model_copy(update={"source_sha256": "b" * 64})
        original = original.model_copy(update={"case_plan": original.case_plan.model_copy(
            update={"evidence": (original.case_plan.evidence[0], wrong)})})
    elif tamper == "wrong_region":
        wrong = original.case_plan.evidence[1].model_copy(update={"region_id": UUID(A)})
        original = original.model_copy(update={"case_plan": original.case_plan.model_copy(
            update={"evidence": (original.case_plan.evidence[0], wrong)})})
    else:
        wrong = original.case_plan.evidence[1].model_copy(update={"document_id": UUID(C)})
        original = original.model_copy(update={"case_plan": original.case_plan.model_copy(
            update={"evidence": (original.case_plan.evidence[0], wrong)})})
    with pytest.raises(ReconcileError):
        await adapter.stage(ACTOR, original)
    assert store.rows == {} and cases.effects == {}


@pytest.mark.asyncio
async def test_withdrawal_and_current_access_after_stage_block_case_execution(environment):
    a, b, _, authority, sources, store, cases, adapter = environment
    key = (await adapter.stage(ACTOR, comparison(a, b)))["operation_key"]
    sources.items[B] = replace(b, withdrawn=True)
    with pytest.raises(ReconcileError, match="source_withdrawn_or_unavailable"):
        await adapter.apply(ACTOR, key)
    sources.items[B] = b
    authority.revoked_sources.add(A)
    with pytest.raises(PermissionError):
        await adapter.apply(ACTOR, key)
    with pytest.raises(PermissionError):
        await adapter.status(ACTOR, key)
    assert not cases.attempts


@pytest.mark.asyncio
async def test_foreign_actor_cannot_lookup_or_replay_receipt(environment):
    a, b, _, authority, sources, store, cases, adapter = environment
    operation = (await adapter.stage(ACTOR, comparison(a, b)))["operation_key"]
    with pytest.raises(ReconcileError, match="reconciliation_not_found"):
        await adapter.status(OTHER, operation)
    assert store.stages == 1 and not cases.effects


@pytest.mark.asyncio
async def test_case_pending_is_not_saved_and_status_is_read_only(environment):
    a, b, _, authority, sources, store, cases, adapter = environment
    operation = (await adapter.stage(ACTOR, comparison(a, b)))["operation_key"]

    class PendingCase:
        calls = 0
        async def disagreement_edit(self, action, operation_key, principal, **kwargs):
            self.calls += 1
            return {"status": "pending", "saved": False}

    pending = PendingCase()
    adapter.cases = pending
    first = await adapter.apply(ACTOR, operation)
    assert first["status"] == "pending" and first["reason"] == "case_outcome_pending"
    read = await adapter.status(ACTOR, operation)
    assert read["case_receipt_verified"] is False
    assert read["step_index"] == 0 and store.rows[(ACTOR, operation)].step_index == 0
    assert pending.calls == 2


@pytest.mark.asyncio
async def test_canonical_json_input_parses_uuid_but_rejects_coerced_revisions(environment):
    from pydantic import ValidationError

    a, b, _, authority, sources, store, cases, adapter = environment
    model = comparison(a, b)
    # The public MCP receives decoded JSON objects, not Python UUID instances.
    wire = model.model_dump(mode="json")
    typed = Comparison.model_validate(wire)
    assert typed.model_dump(mode="json") == wire
    assert (await adapter.stage(ACTOR, typed))["status"] == "staged"
    wire["anchor"]["source_revision"] = "1"
    with pytest.raises(ValidationError):
        Comparison.model_validate(wire)
    wire["anchor"]["source_revision"] = 1
    wire["extra_authority"] = ACTOR
    with pytest.raises(ValidationError):
        Comparison.model_validate(wire)


@pytest.mark.asyncio
async def test_owner_revoked_after_remote_case_save_before_cursor_update_recovers_same_effect(environment):
    a, b, _, authority, sources, store, cases, adapter = environment
    key = (await adapter.stage(ACTOR, comparison(a, b)))["operation_key"]
    original = cases.disagreement_edit

    async def save_then_revoke(action, operation_key, principal, **kwargs):
        result = await original(action, operation_key, principal, **kwargs)
        if action == "create":
            authority.revoked = True
        return result

    cases.disagreement_edit = save_then_revoke
    with pytest.raises(PermissionError):
        await adapter.apply(ACTOR, key)
    assert store.rows[(ACTOR, key)].step_index == 0
    assert len(cases.effects) == 1, "Case effect happened but did not bypass current access"
    authority.revoked = False
    cases.disagreement_edit = original
    recovered = await adapter.apply(ACTOR, key)
    assert recovered["step_index"] == 1 and recovered["case_id"] == CASE
    assert len(cases.effects) == 1, "identical operation_key reconciles, never creates a second Case"


@pytest.mark.asyncio
async def test_needs_context_persists_gap_without_completing_cross_book_or_calling_case(environment):
    a, b, _, authority, sources, store, cases, adapter = environment
    original = comparison(a, b, action="none", key="pending-source-gap")
    pending = original.model_copy(update={"decision": "needs_context"})
    staged = await adapter.stage(ACTOR, pending)
    assert staged["status"] == "needs_context"
    operation = staged["operation_key"]
    after_apply = await adapter.apply(ACTOR, operation)
    assert after_apply["status"] == "needs_context"
    assert after_apply["reason"] == "model_requires_additional_source_context"
    assert not store.rows[(ACTOR, operation)].done
    assert not store.rows[(ACTOR, operation)].attempted
    assert cases.effects == {}
    fresh = TargetStoryReconcile(sources=sources, authority=authority, store=store, cases=cases)
    assert (await fresh.status(ACTOR, operation))["status"] == "needs_context"
    # New model evidence is a new proposal; original pending gap is immutable.
    new = comparison(a, b, key="resumed-with-exact-evidence", action="create")
    later = await fresh.stage(ACTOR, new)
    assert later["status"] == "staged" and later["operation_key"] != operation


@pytest.mark.parametrize("case_status", ["revoked", "unproved"])
@pytest.mark.asyncio
async def test_completed_case_receipt_is_rechecked_after_later_case_permission_change(environment, case_status):
    a, b, _, authority, sources, store, cases, adapter = environment
    operation = (await adapter.stage(ACTOR, comparison(a, b)))["operation_key"]
    for _ in range(3):
        outcome = await adapter.apply(ACTOR, operation)
    assert outcome["status"] == "completed" and outcome["case_id"] == CASE
    effects_before = len(cases.effects)
    original = cases.disagreement_edit

    async def case_revoked_or_unproved(action, key, principal, **kwargs):
        if action == "operation_status":
            if case_status == "revoked":
                raise PermissionError("current Case scope revoked")
            return {"status": "pending", "saved": False}
        return await original(action, key, principal, **kwargs)

    cases.disagreement_edit = case_revoked_or_unproved
    error = PermissionError if case_status == "revoked" else ReconcileError
    with pytest.raises(error):
        await adapter.status(ACTOR, operation)
    with pytest.raises(error):
        await adapter.apply(ACTOR, operation)
    assert len(cases.effects) == effects_before, "no Case write or new receipt on repeat"
