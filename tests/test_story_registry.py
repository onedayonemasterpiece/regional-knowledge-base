"""Synthetic end-to-end registry invariants. No real book or private quotation in Git."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from hashlib import sha256
from types import SimpleNamespace
from uuid import uuid4

import pytest

from regional_knowledge.contracts import Principal
from regional_knowledge.sqlite_corpus import SQLiteCorpus
from regional_knowledge.story_contracts import (
    AddGap, AttachEvidence, EvidenceLocator, ExtractClaim, ExtractStage, ExtractStart,
    RecordAssessment, RegisteredSource, ResolveGap, ReviewDecision, SeedInput,
    UpsertAssertion, UpsertVariant,
)
from regional_knowledge.story_registry import StoryError, StoryRegistry


def principal():
    return Principal(subject=str(uuid4()), client_id="synthetic-client", issuer="unit-test",
                     access_token="not-a-real-token")


@pytest.fixture
def setup(tmp_path):
    corpus = SQLiteCorpus(tmp_path / "corpus.sqlite3")
    return StoryRegistry(corpus), principal(), principal()


def seed(registry, actor, name="История старого фонаря", key="seed-0001"):
    return registry.create(actor, SeedInput(text=name), None, None, None, key)


def expect(code, func, *args, **kwargs):
    with pytest.raises(StoryError) as error:
        func(*args, **kwargs)
    assert error.value.code == code


def document(registry, owner):
    ident = str(uuid4())
    page, region, chunk = [str(uuid4()) for _ in range(3)]
    text = "У старого фонаря днём собирались мастера. Улица была освещена газовыми лампами."
    sig = sha256(text.encode()).hexdigest()
    registry.corpus.put("rkb_documents", [{
        "id": ident, "title": "Синтетическая книга о городе",
        "owner_user_id": owner.subject, "source_sha256": "a" * 64,
        "active_revision": 1, "content_visibility": "private",
        "source_visibility": "private", "rights_status": "restricted",
    }])
    registry.corpus.put("rkb_pages", [{
        "id": page, "document_id": ident, "revision": 1,
        "physical_page_index": 0, "printed_page_number": "7",
    }])
    registry.corpus.put("rkb_regions", [{
        "id": region, "page_id": page, "source_text": text, "reading_order": 0,
    }])
    registry.corpus.put("rkb_chunks", [{
        "id": chunk, "document_id": ident, "revision": 1, "title": "Синтетический эпизод",
        "source_text": text, "text_sha256": sig, "search_material_sha256": sig,
        "page_ids": [page], "region_ids": [region],
    }])
    return ident, page, region, chunk, text


def test_seed_idempotency_unknown_provenance_and_search(setup):
    registry, owner, _ = setup
    original = seed(registry, owner)
    assert original["committed_revision"] == 1
    assert registry.create(owner, SeedInput(text="История старого фонаря"), None,
                           None, None, "seed-0001") == original
    expect("idempotency_conflict", registry.create, owner, SeedInput(text="Другая история"),
           None, None, None, "seed-0001")
    found = registry.search(owner, query="фонаря")
    assert found["retrieval_mode"] == "lexical_only"
    assert found["results"][0]["story_id"] == original["story_id"]
    assert registry.get(owner, original["story_id"])["snapshot"]["seed"]["origin_status"] == "unknown"
    assert registry.history(owner, original["story_id"])["history"][0]["action"] == "create"


def test_atomic_revision_conflict_and_replay_after_commit(setup):
    registry, owner, _ = setup
    item = seed(registry, owner)
    sid = item["story_id"]
    a = registry.edit(owner, sid, 1, [AddGap(op="add_gap", text="Установить место")], "edit-0001")
    assert a["committed_revision"] == 2
    assert registry.edit(owner, sid, 1, [AddGap(op="add_gap", text="Установить место")],
                         "edit-0001") == a
    expect("revision_conflict", registry.edit, owner, sid, 1,
           [AddGap(op="add_gap", text="Ошибка")], "edit-0002")
    expect("validation_failed", registry.edit, owner, sid, 2,
           [AddGap(op="add_gap", text="Should roll back"),
            ResolveGap(op="resolve_gap", gap_id="missing", resolution="No")], "edit-0003")
    assert registry.get(owner, sid, view="evidence")["revision"] == 2
    assert len(registry.get(owner, sid, view="evidence")["snapshot"]["gaps"]) == 1


def test_competing_writers_no_lost_edits(setup):
    registry, owner, _ = setup
    sid = seed(registry, owner)["story_id"]
    def worker(i):
        try:
            return registry.edit(owner, sid, 1, [AddGap(op="add_gap", text=f"gap {i}")],
                                 f"parallel-{i}")["committed_revision"]
        except StoryError as exc:
            return exc.code
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(worker, range(2)))
    assert sorted(map(str, results)) == ["2", "revision_conflict"]


def test_legend_editor_approval_no_truth_proof_and_source_change(setup):
    registry, owner, _ = setup
    src = registry.register_source(owner, RegisteredSource(
        document_kind="user_note", medium="text", origin_status="partial",
        title="Запись рассказа", body="Говорят, что старый фонарь иногда светился без огня."
    ), "source-0001")
    item = seed(registry, owner)
    sid = item["story_id"]
    rev2 = registry.edit(owner, sid, 1, [UpsertAssertion(
        op="upsert_assertion", proposition="Фонарь светился без огня",
        kind="attributed_account", account_kind="legend", attributed_to="неизвестный рассказчик",
    )], "claim-0001")
    claim = registry.get(owner, sid, view="evidence")["snapshot"]["assertions"][0]
    ev = registry.edit(owner, sid, rev2["committed_revision"], [AttachEvidence(
        op="attach_evidence", assertion_id=claim["assertion_id"], assertion_revision=1,
        source_kind="external", source_id=src["source_id"], source_revision=1,
        relation="reports", original_excerpt="Говорят, что старый фонарь иногда светился без огня.",
    )], "evidence-0001")
    variants = registry.edit(owner, sid, ev["committed_revision"], [UpsertVariant(
        op="upsert_variant", audience="Жители города", format="short_video",
        body="По словам рассказчика, фонарь светился сам.",
        attributions=["По словам рассказчика, это легенда, не установленный факт."],
    )], "variant-0001")
    variant = registry.get(owner, sid, view="evidence")["snapshot"]["variants"][0]
    assert registry.validate(owner, sid)["all_passed"]
    approved = registry.transition(owner, sid, variants["committed_revision"], "publish_ready",
                                   [{"variant_id": variant["variant_id"], "revision": 1}],
                                   ReviewDecision(semantic_checked=True, attribution_checked=True,
                                                  rights_checked=True, reviewer_note="Легенда названа легендой"),
                                   "ready-0001")
    assert variant["variant_id"] in approved["readiness"]["ready_variants"]
    exported = registry.export(owner, sid, variant["variant_id"])
    assert exported["publication_state"] == "not_sent"
    assert "легенда" in exported["attributions"][0]
    assessed = registry.edit(owner, sid, approved["committed_revision"], [RecordAssessment(
        op="record_assessment", assertion_id=claim["assertion_id"],
        assertion_revision=1, evidence_ids=[registry.get(owner, sid, view="evidence")["snapshot"]["assertions"][0]["evidence_ids"][0]],
        support_status="single_source", semantic_review="supported",
        rationale="Источник свидетельствует о существовании рассказа, не о чуде"
    )], "new-assess-0001")
    assert variant["variant_id"] in assessed["readiness"]["needs_revalidation"]
    expect("validation_failed", registry.export, owner, sid, variant["variant_id"])


def test_real_quote_wrong_claim_is_not_semantic_support(setup):
    registry, owner, _ = setup
    doc, page, region, chunk, text = document(registry, owner)
    sid = seed(registry, owner)["story_id"]
    registry.edit(owner, sid, 1, [UpsertAssertion(
        op="upsert_assertion", proposition="Фонари включали каждую ночь",
        kind="historical_claim")], "historic-0001")
    claim = registry.get(owner, sid, view="evidence")["snapshot"]["assertions"][0]
    registry.edit(owner, sid, 2, [AttachEvidence(
        op="attach_evidence", assertion_id=claim["assertion_id"], assertion_revision=1,
        source_kind="document", source_id=doc, source_revision=1,
        relation="provides_context", locator=EvidenceLocator(page_id=page,
           region_id=region, chunk_id=chunk, physical_page_index=0),
        original_excerpt="У старого фонаря днём собирались мастера.",
        source_role_for_assertion="secondary")], "historic-ev-0001")
    registry.edit(owner, sid, 3, [RecordAssessment(
        op="record_assessment", assertion_id=claim["assertion_id"],
        assertion_revision=1, support_status="unsupported", semantic_review="unsupported",
        rationale="Настоящая цитата не доказывает, что фонари горели ночью",
    )], "historic-assess-0001")
    registry.edit(owner, sid, 4, [UpsertVariant(
        op="upsert_variant", audience="Город", format="post", body="Ночью светили фонари",
    )], "historic-variant-0001")
    validation = registry.validate(owner, sid)
    assert validation["variants"][0]["result"] == "failed"
    assert "historical_claim_without_semantic_review:" + claim["assertion_id"] in validation["variants"][0]["blockers"]


def test_rights_revocation_denies_story_receipts_and_export(setup):
    registry, owner, other = setup
    doc, page, region, chunk, text = document(registry, owner)
    from regional_knowledge.story_contracts import SourceRef
    sid = registry.create(owner, SeedInput(text="Зацепка из книги"), None, None,
                          [SourceRef(kind="document", source_id=doc, source_revision=1)],
                          "scoped-0001")["story_id"]
    revision = registry.access(owner, sid, 1, other.subject, "viewer", "grant-0001")
    expect("not_found_or_not_accessible", registry.get, other, sid)
    registry.corpus.put("rkb_document_grants", [{
        "document_id": doc, "grantee_user_id": other.subject, "role": "viewer",
    }])
    assert registry.get(other, sid)["story_id"] == sid
    with registry.corpus.connect() as db:
        db.execute("""DELETE FROM corpus_rows WHERE table_name='rkb_document_grants'
                      AND json_extract(payload,'$.document_id')=?""", (doc,))
    expect("not_found_or_not_accessible", registry.get, other, sid)
    expect("not_found_or_not_accessible", registry.history, other, sid)
    # Own idempotent receipt still exists and is returned without making a new revision.
    assert registry.access(owner, sid, 1, other.subject, "viewer", "grant-0001") == revision


def test_extractor_leases_and_durable_checkpoint(setup):
    registry, owner, _ = setup
    doc, page, region, chunk, text = document(registry, owner)
    start = registry.extract(owner, ExtractStart(command="start", document_id=doc,
                            source_revision=1), "start-job-0001")
    assert start["executor_state"] == "awaiting_agent" and start["commit_state"] == "queued"
    jid = start["job_id"]
    claim = registry.extract(owner, ExtractClaim(command="claim", job_id=jid,
                             expected_job_revision=1), "claim-job-0001")
    assert claim["lease_active"]
    assert claim == registry.extract(owner, ExtractClaim(command="claim", job_id=jid,
                              expected_job_revision=1), "claim-job-0001")
    expect("revision_conflict", registry.extract, owner,
           ExtractStage(command="stage", job_id=jid, expected_job_revision=2,
                        batch_id=claim["batch_id"], lease_token="wrong-lease",
                        source_revision=1), "wrong-stage-0001")
    outcome = registry.extract(owner, ExtractStage(command="stage", job_id=jid,
             expected_job_revision=2, batch_id=claim["batch_id"],
             lease_token=claim["lease_token"], source_revision=1,
             candidates=[SeedInput(text="Эпизод в книге", origin_status="partial")]), "stage-job-0001")
    assert outcome["executor_state"] == "done"
    assert len(registry.job_get(owner, jid)["result_ids"]) == 1
    expect("revision_conflict", registry.extract, owner,
           ExtractStage(command="stage", job_id=jid, expected_job_revision=2,
                        batch_id=claim["batch_id"], lease_token=claim["lease_token"],
                        source_revision=1), "late-stage-0001")


def test_online_backup_restores_story_versions_receipts_and_rights(setup, tmp_path):
    registry, owner, _ = setup
    item = seed(registry, owner)
    registry.edit(owner, item["story_id"], 1, [AddGap(op="add_gap", text="Когда?")], "backup-edit")
    backup = tmp_path / "verified.sqlite3"
    registry.corpus.backup(backup)
    restored = StoryRegistry(SQLiteCorpus(backup))
    assert restored.get(owner, item["story_id"], view="evidence")["revision"] == 2
    assert len(restored.history(owner, item["story_id"])["history"]) == 2
    assert restored.create(owner, SeedInput(text="История старого фонаря"), None,
                           None, None, "seed-0001") == item


@pytest.mark.asyncio
async def test_mcp_bundles_and_unmodified_default_live(setup, monkeypatch):
    from regional_knowledge.server import build_server
    registry, owner, _ = setup
    monkeypatch.setenv("RKB_DEV_NOAUTH", "1")
    backend = SimpleNamespace(corpus=registry.corpus)
    reader = {x.name for x in await build_server(backend=backend, profile="story_reader").list_tools()}
    contributor = {x.name for x in await build_server(backend=backend, profile="story_contributor").list_tools()}
    editor = {x.name for x in await build_server(backend=backend, profile="story_editor").list_tools()}
    live = {x.name for x in await build_server(backend=backend, profile="live").list_tools()}
    assert live == {"knowledge_search"}
    assert "story_create" not in reader and "story_search" in reader
    assert "story_create" in contributor and "story_transition" not in contributor
    assert {"story_export", "story_transition", "story_access", "story_publication_record"} <= editor
