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
    LinkEntity, RecordAssessment, RecordInterest, RegisteredSource, ResolveGap,
    ReviewDecision, SeedInput, SetContributors, UpsertAssertion, UpsertVariant,
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
    assert "Непроверенная зацепка" in found["results"][0]["spoken_summary"]
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
    spoken = registry.search(owner, "фонаря")["results"][0]["spoken_summary"]
    assert "По легенде" in spoken
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

def test_source_change_invalidates_fingerprint_and_persists_editor_decision(setup):
    registry, owner, _ = setup
    doc, page, region, chunk, text = document(registry, owner)
    sid = seed(registry, owner)["story_id"]
    registry.edit(owner, sid, 1, [UpsertAssertion(
        op="upsert_assertion", proposition="Днём у фонаря собирались мастера",
        kind="historical_claim")], "source-claim-001")
    claim = registry.get(owner, sid, view="evidence")["snapshot"]["assertions"][0]
    registry.edit(owner, sid, 2, [AttachEvidence(
        op="attach_evidence", assertion_id=claim["assertion_id"], assertion_revision=1,
        source_kind="document", source_id=doc, source_revision=1,
        relation="supports", source_role_for_assertion="secondary",
        original_excerpt="У старого фонаря днём собирались мастера.",
        locator=EvidenceLocator(page_id=page, region_id=region, chunk_id=chunk, physical_page_index=0)
    )], "source-proof-001")
    evidence_id = registry.get(owner, sid, view="evidence")["snapshot"]["assertions"][0]["evidence_ids"][0]
    registry.edit(owner, sid, 3, [RecordAssessment(
        op="record_assessment", assertion_id=claim["assertion_id"], assertion_revision=1,
        evidence_ids=[evidence_id], support_status="single_source",
        semantic_review="supported", rationale="В источнике прямо описаны дневные встречи"
    )], "source-assess-001")
    registry.edit(owner, sid, 4, [UpsertVariant(
        op="upsert_variant", audience="Жители", format="post", body="Днём у фонаря собирались мастера.",
    )], "source-variant-001")
    variant = registry.get(owner, sid, view="evidence")["snapshot"]["variants"][0]
    ready = registry.transition(owner, sid, 5, "publish_ready",
       [{"variant_id": variant["variant_id"], "revision": 1}],
       ReviewDecision(semantic_checked=True, attribution_checked=True, rights_checked=True,
                      reviewer_note="Сверено с источником"), "source-ready-001")
    assert ready["readiness"]["ready_variants"] == [variant["variant_id"]]
    with registry.corpus.connect() as db:
        decision = db.execute("SELECT * FROM story_review_decisions WHERE story_id=?", (sid,)).fetchone()
        assert decision is not None and decision["actor_id"] == owner.subject
        assert __import__("json").loads(decision["review"])["actual_executor"] == "application"
    assert registry.export(owner, sid, variant["variant_id"])["body"] == "Днём у фонаря собирались мастера."
    current = registry.corpus.one("rkb_documents", doc)
    # Rechunk: page/region IDs may change, but text and original SHA do not.
    new_page, new_region = str(uuid4()), str(uuid4())
    registry.corpus.put("rkb_pages", [{"id": new_page, "document_id": doc,
                                      "revision": 2, "physical_page_index": 0}])
    registry.corpus.put("rkb_regions", [{"id": new_region, "page_id": new_page,
                                        "source_text": text, "reading_order": 0}])
    registry.corpus.put("rkb_documents", [{**current, "active_revision": 2}])
    assert registry.get(owner, sid)["readiness"]["ready_variants"] == [variant["variant_id"]]
    assert registry.export(owner, sid, variant["variant_id"])["body"] == "Днём у фонаря собирались мастера."
    # Corrected OCR under the SAME original PDF SHA must invalidate approval.
    corrected_page, corrected_region = str(uuid4()), str(uuid4())
    registry.corpus.put("rkb_pages", [{"id": corrected_page, "document_id": doc,
                                      "revision": 3, "physical_page_index": 0}])
    registry.corpus.put("rkb_regions", [{"id": corrected_region, "page_id": corrected_page,
                "source_text": "Исправленный источник: мастера в другом месте.", "reading_order": 0}])
    registry.corpus.put("rkb_documents", [{**current, "active_revision": 3}])
    assert registry.get(owner, sid)["readiness"]["ready_variants"] == []
    assert registry.get(owner, sid)["readiness"]["needs_revalidation"] == [variant["variant_id"]]
    assert registry.validate(owner, sid)["variants"][0]["result"] == "failed"
    expect("source_changed", registry.export, owner, sid, variant["variant_id"])


def test_source_acl_applied_to_search_before_ranking_and_after_revocation(setup):
    registry, owner, other = setup
    from regional_knowledge.story_contracts import SourceRef
    doc, _, _, _, _ = document(registry, owner)
    hidden = registry.create(owner, SeedInput(text="Фонарь секретной улицы"), None, None,
            [SourceRef(kind="document", source_id=doc, source_revision=1)], "search-private-001")
    registry.access(owner, hidden["story_id"], 1, other.subject, "viewer", "search-grant-001")
    accessible = registry.create(other, SeedInput(text="Фонарь открытой улицы"), None,
                                 None, None, "search-public-001")
    before = registry.search(other, "Фонарь", mode="semantic")
    assert before["retrieval_mode"] == "lexical_degraded"
    assert [h["story_id"] for h in before["results"]] == [accessible["story_id"]]
    registry.corpus.put("rkb_document_grants", [{
       "document_id": doc, "grantee_user_id": other.subject, "role": "viewer",
    }])
    granted = registry.search(other, "Фонарь")
    assert {h["story_id"] for h in granted["results"]} == {
        accessible["story_id"], hidden["story_id"]
    }
    with registry.corpus.connect() as db:
        db.execute("""DELETE FROM corpus_rows WHERE table_name='rkb_document_grants'
                      AND json_extract(payload,'$.document_id')=?""", (doc,))
    revoked = registry.search(other, "Фонарь")
    assert [h["story_id"] for h in revoked["results"]] == [accessible["story_id"]]
    assert all("секретной" not in h["summary"] for h in revoked["results"])


def test_story_search_cursor_does_not_skip_ranked_rows(setup):
    registry, owner, _ = setup
    expected = set()
    for i in range(7):
        result = registry.create(owner, SeedInput(text=f"Фонарь и эпизод {i}"),
                                 None, None, None, f"story-page-{i:04d}")
        expected.add(result["story_id"])
    found, cursor = set(), None
    for _ in range(5):
        page = registry.search(owner, "Фонарь", limit=2, cursor=cursor)
        found.update(h["story_id"] for h in page["results"])
        cursor = page["next_cursor"]
        if not page["has_more"]:
            assert cursor is None
            break
    assert found == expected


def test_source_locator_does_not_accept_foreign_chunk(setup):
    registry, owner, _ = setup
    doc, page, region, chunk, text = document(registry, owner)
    sid = seed(registry, owner)["story_id"]
    registry.edit(owner, sid, 1, [UpsertAssertion(
        op="upsert_assertion", proposition="Мастера собирались днем",
        kind="historical_claim")], "bad-pointer-claim-001")
    a = registry.get(owner, sid, view="evidence")["snapshot"]["assertions"][0]
    expect("invalid_evidence", registry.edit, owner, sid, 2, [AttachEvidence(
        op="attach_evidence", assertion_id=a["assertion_id"], assertion_revision=1,
        source_kind="document", source_id=doc, source_revision=1, relation="supports",
        original_excerpt="У старого фонаря днём собирались мастера.",
        locator=EvidenceLocator(page_id=page, region_id=region, chunk_id=str(uuid4()),
                                physical_page_index=0)
    )], "bad-pointer-ev-001")
    assert registry.get(owner, sid, view="evidence")["revision"] == 2


def test_unverified_media_cannot_be_approved(setup):
    registry, owner, _ = setup
    sid = seed(registry, owner)["story_id"]
    registry.edit(owner, sid, 1, [UpsertAssertion(op="upsert_assertion",
        proposition="Кто-то рассказывал о фонаре", kind="attributed_account",
        account_kind="legend", attributed_to="аноним")], "media-claim-001")
    registry.edit(owner, sid, 2, [UpsertVariant(op="upsert_variant", audience="Город",
        format="short_video", body="По рассказу анонима...",
        attributions=["По рассказу анонима, это легенда."],
        media_refs=["knowledge://illustrations/unverified"])
    ], "media-variant-001")
    v = registry.validate(owner, sid)["variants"][0]
    assert v["result"] == "failed"
    assert "media_rights_not_verified" in v["blockers"]


@pytest.mark.asyncio
async def test_function_call_surface_actual_mutation_receipt_and_readback(setup, monkeypatch):
    from regional_knowledge import server as server_module
    import json
    registry, owner, _ = setup
    monkeypatch.setenv("RKB_DEV_NOAUTH", "1")
    monkeypatch.setattr(server_module, "_principal", lambda: owner)
    server = server_module.build_server(
        backend=SimpleNamespace(corpus=registry.corpus), profile="story_contributor")
    declared = {tool.name: tool for tool in await server.list_tools()}
    assert "story_create" in declared and "story_get" in declared
    assert "idempotency_key" in declared["story_create"].input_schema["required"]
    args = {"seed": {"text": "Фонарь у городской стены", "origin_status": "unknown"},
            "idempotency_key": "mcp-function-0001"}
    def unpack(value):
        # ToolManager exposes structured dicts in MCP >=2.2, while some
        # unstructured compatibility tools return text content.
        return value if isinstance(value, dict) else json.loads(value[0].text)
    result = await server._tool_manager.call_tool("story_create", args, None)
    created = unpack(result)
    assert created["commit_state"] == "saved"
    story_id = created["story_id"]
    assert unpack(await server._tool_manager.call_tool(
        "story_create", args, None)) == created
    fetched = unpack(await server._tool_manager.call_tool(
        "story_get", {"story_id": story_id, "view": "compact"}, None))
    assert fetched["snapshot"]["seed"]["origin_status"] == "unknown"
    search = unpack(await server._tool_manager.call_tool(
        "story_search", {"query": "фонарь", "limit": 3}, None))
    assert any(x["story_id"] == story_id for x in search["results"])


def test_story_interest_scoring_order_and_typed_filters(setup):
    from regional_knowledge.story_contracts import RecordInterest, ScoreCriterion
    registry, actor, _ = setup
    raw = seed(registry, actor, "Повседневность в архиве", "score-seed-001")
    scored = seed(registry, actor, "Повседневность на улицах", "score-seed-002")
    best = ScoreCriterion(value=4, rationale="Есть необычная региональная деталь")
    registry.edit(actor, scored["story_id"], 1, [RecordInterest(
        op="record_interest_assessment", unexpectedness=best, local_relevance=best,
        human_resonance=best, explanatory_value=best, visual_potential=best,
        channel_novelty=80, production_readiness=50,
    )], "score-edit-001")
    ranked = registry.search(actor, "Повседневность", order="potential")
    assert ranked["results"][0]["story_id"] == scored["story_id"]
    assert ranked["results"][0]["potential"] == 100
    assert ranked["results"][0]["score_coverage"] == 1
    assert ranked["results"][1]["potential"] is None
    selected = registry.search(actor, filters={"min_potential": "90", "material_type": "other"})
    assert [r["story_id"] for r in selected["results"]] == [scored["story_id"]]
    expect("validation_failed", registry.search, actor, filters={"min_potential": "101"})


def test_accepted_source_batch_creates_grounded_candidate_and_replays_without_duplicates(setup):
    """One model-authored batch -> exact source card, citation and checkpoint."""
    from regional_knowledge.story_contracts import (
        ExtractGroundedCandidate, ExtractGroundedEvidence,
    )
    registry, owner, foreign = setup
    doc, page, region, chunk, text = document(registry, owner)
    batch = registry.corpus_read(owner, doc, 1, limit=20)
    assert batch["chunks"][0]["source_regions"] == [{
        "page_id": page, "region_id": region, "physical_page_index": 0,
        "printed_page_number": "7", "source_text": text,
        "source_text_truncated": False,
    }]
    first = registry.extract(owner, ExtractStart(
        command="start", document_id=doc, source_revision=1, batch_size=20,
    ), "grounded-start-first")
    claim = registry.extract(owner, ExtractClaim(
        command="claim", job_id=first["job_id"], expected_job_revision=1,
    ), "grounded-claim-first")
    episode = ExtractGroundedCandidate(
        candidate_key="gas-lantern-meeting",
        title="Собрание мастеров у старого фонаря",
        summary="По книге ремесленники встречались у старого фонаря.",
        material_type="everyday_life",
        proposition="Автор описывает встречу мастеров у старого фонаря.",
        account_kind="other", attributed_to="Автор синтетической книги",
        evidence_refs=[ExtractGroundedEvidence(
            page_id=page, region_id=region,
            original_excerpt="У старого фонаря днём собирались мастера.",
        )],
    )
    staged = ExtractStage(
        command="stage", job_id=first["job_id"], expected_job_revision=claim["job_revision"],
        batch_id=claim["batch_id"], lease_token=claim["lease_token"],
        source_revision=1, grounded_candidates=[episode],
    )
    result = registry.extract(owner, staged, "grounded-stage-first")
    assert result["executor_state"] == "done"
    assert result["grounded_candidates_created"] == 1
    assert result["grounded_candidates_reused"] == 0
    assert len(result["resource_ids"]) == 1
    sid = result["resource_ids"][0]
    assert registry.extract(owner, staged, "grounded-stage-first") == result
    assert registry.job_get(owner, first["job_id"])["processed_chunks"] == 1
    story = registry.get(owner, sid, view="evidence")
    assert story["revision"] == 2 and story["snapshot"]["state"] == "candidate"
    claim_state = story["snapshot"]["assertions"][0]
    assert claim_state["kind"] == "attributed_account"
    assert claim_state["attributed_to"] == "Автор синтетической книги"
    assert len(claim_state["evidence_ids"]) == 1
    with registry.corpus.connect() as db:
        evidence = db.execute("SELECT * FROM story_evidence WHERE story_id=?", (sid,)).fetchone()
        assert evidence["text_match"] == "exact"
        assert evidence["relation"] == "reports"
        assert evidence["original_review"] == "not_checked"
        assert db.execute("SELECT count(*) FROM story_records WHERE id=?", (sid,)).fetchone()[0] == 1

    # A fresh job on the same accepted source cannot duplicate this evidence.
    later = registry.extract(owner, ExtractStart(
        command="start", document_id=doc, source_revision=1, batch_size=20,
    ), "grounded-start-again")
    later_claim = registry.extract(owner, ExtractClaim(
        command="claim", job_id=later["job_id"], expected_job_revision=1,
    ), "grounded-claim-again")
    same = registry.extract(owner, ExtractStage(
        command="stage", job_id=later["job_id"],
        expected_job_revision=later_claim["job_revision"],
        batch_id=later_claim["batch_id"], lease_token=later_claim["lease_token"],
        source_revision=1, grounded_candidates=[episode],
    ), "grounded-stage-again")
    assert same["grounded_candidates_created"] == 0
    assert same["grounded_candidates_reused"] == 1
    assert same["resource_ids"] == [sid]
    expect("not_found_or_not_accessible", registry.corpus_read, foreign, doc, 1)


def test_accepted_source_batch_rejects_false_and_out_of_batch_evidence(setup):
    """A bad proof never advances a lease or creates a phantom story."""
    from regional_knowledge.story_contracts import (
        ExtractGroundedCandidate, ExtractGroundedEvidence,
    )
    registry, owner, _ = setup
    doc, page, region, chunk, text = document(registry, owner)
    started = registry.extract(owner, ExtractStart(
        command="start", document_id=doc, source_revision=1,
    ), "grounded-bad-start")
    claim = registry.extract(owner, ExtractClaim(
        command="claim", job_id=started["job_id"], expected_job_revision=1,
    ), "grounded-bad-claim")
    def candidate(evidence):
        return ExtractGroundedCandidate(
            candidate_key="invented-episode",
            title="Проверяем достоверность выдержки",
            summary="Проверка точной цитаты без модели.",
            proposition="Автор якобы утверждает выдуманный факт",
            attributed_to="Синтетический источник",
            evidence_refs=[evidence],
        )
    common = dict(
        command="stage", job_id=started["job_id"],
        expected_job_revision=claim["job_revision"],
        batch_id=claim["batch_id"], lease_token=claim["lease_token"],
        source_revision=1,
    )
    bad = ExtractStage(**common, grounded_candidates=[candidate(
        ExtractGroundedEvidence(
            page_id=page, region_id=region,
            original_excerpt="Этого в книге нет.",
        ),
    )])
    expect("invalid_evidence", registry.extract, owner, bad, "grounded-bad-proof")
    assert registry.job_get(owner, started["job_id"])["processed_chunks"] == 0
    with registry.corpus.connect() as db:
        assert db.execute("SELECT count(*) FROM story_records").fetchone()[0] == 0
    # The same lease may be fixed without a duplicate job or accepting a lie.
    good = ExtractStage(**common, grounded_candidates=[candidate(
        ExtractGroundedEvidence(
            page_id=page, region_id=region,
            original_excerpt="У старого фонаря днём собирались мастера.",
        ),
    )])
    result = registry.extract(owner, good, "grounded-corrected-proof")
    assert result["grounded_candidates_created"] == 1
    assert registry.job_get(owner, started["job_id"])["processed_chunks"] == 1




def test_entity_list_acl_keyset_reaches_sources_beyond_first_hundred(setup):
    """The global source catalog is not truncated before filtering and paging."""
    registry, owner, other = setup
    own_docs, other_docs, entities = [], [], []
    for i in range(175):
        document_id, entity_id = str(uuid4()), str(uuid4())
        is_owner = i >= 23
        (own_docs if is_owner else other_docs).append(document_id)
        registry.corpus.put("rkb_documents", [{
            "id": document_id, "owner_user_id": owner.subject if is_owner else other.subject,
            "source_sha256": "a" * 64, "active_revision": 1, "content_visibility": "private",
            "rights_status": "restricted",
        }])
        entities.append({
            "id": entity_id, "document_id": document_id, "revision": 1,
            "kind": "place", "canonical_label": f"Место {i:03d}",
        })
    registry.corpus.put("rkb_entities", entities)
    seen, cursor, seen_cursors = set(), None, set()
    for _ in range(30):
        result = registry.entity_list(owner, kinds=["place"], cursor=cursor, limit=9)
        assert result["cursor_policy"] == "authorized_keyset_highwater_acl_rechecked"
        for item in result["items"]:
            assert item["document_id"] in own_docs
            assert item["entity_id"] not in seen
            seen.add(item["entity_id"])
        if not result["has_more"]:
            break
        assert result["next_cursor"] and result["next_cursor"] not in seen_cursors
        seen_cursors.add(result["next_cursor"])
        cursor = result["next_cursor"]
    assert len(seen) == 152
    assert registry.entity_list(other, document_ids=own_docs[:15])["items"] == []
    with pytest.raises(StoryError) as exc:
        registry.entity_list(other, cursor=cursor, limit=9)
    assert exc.value.code == "validation_failed"



def test_bounded_cross_book_evidence_reader_keeps_source_after_merge(setup):
    """Merged evidence retains its original assertion/source ownership and ACL."""
    registry, owner, other = setup
    sources = [document(registry, owner) for _ in range(2)]
    story_ids = []
    for i, (doc, page, region, chunk, text) in enumerate(sources):
        created = registry.create(
            owner, SeedInput(text=f"Синтетический эпизод {i}"), None, None,
            None, f"cross-book-seed-{i}",
        )
        sid = created["story_id"]
        registry.edit(owner, sid, 1, [UpsertAssertion(
            op="upsert_assertion", kind="attributed_account",
            account_kind="other", attributed_to=f"Автор книги {i}",
            proposition=f"Автор {i} описывает встречу ремесленников.",
        )], f"cross-book-claim-{i}")
        claim = registry.get(owner, sid, view="evidence")["snapshot"]["assertions"][0]
        registry.edit(owner, sid, 2, [AttachEvidence(
            op="attach_evidence", assertion_id=claim["assertion_id"], assertion_revision=1,
            source_kind="document", source_id=doc, source_revision=1, relation="reports",
            original_excerpt="У старого фонаря днём собирались мастера.",
            locator=EvidenceLocator(page_id=page, region_id=region, chunk_id=chunk),
        )], f"cross-book-proof-{i}")
        story_ids.append(sid)
    registry.merge(
        owner, story_ids[0], [story_ids[1]],
        {story_ids[0]: 3, story_ids[1]: 3},
        "True duplicated synthetic event only", "cross-book-merge-1",
    )
    first = registry.get(owner, story_ids[0], view="evidence_page", limit=1)
    assert len(first["items"]) == 1 and first["has_more"]
    second = registry.get(owner, story_ids[0], view="evidence_page",
                          limit=1, cursor=first["next_cursor"])
    assert len(second["items"]) == 1 and not second["has_more"]
    items = first["items"] + second["items"]
    assert {x["source_id"] for x in items} == {x[0] for x in sources}
    assert all(x["text_match"] == "exact" and x["source_state"] == "unchanged"
               and x["relation"] == "reports" for x in items)
    assert all(x["proposition"] for x in items)

    source_page = registry.get(owner, story_ids[0], view="sources_page", limit=1)
    assert source_page["has_more"] and len(source_page["items"]) == 1
    last_page = registry.get(owner, story_ids[0], view="sources_page",
                             limit=1, cursor=source_page["next_cursor"])
    assert len(last_page["items"]) == 1
    assert {x["source_id"] for x in [*source_page["items"], *last_page["items"]]} == {
        d for d, *_ in sources
    }
    assertion_page = registry.get(owner, story_ids[0], view="assertion_page", limit=1)
    assert assertion_page["has_more"]
    expect("not_found_or_not_accessible", registry.get, other, story_ids[0],
           view="evidence_page", limit=1)
    with pytest.raises(StoryError) as mismatch:
        registry.get(owner, story_ids[0], view="evidence_page", cursor="3:anything")
    assert mismatch.value.code == "validation_failed"


def test_variant_dependencies_ignore_unrelated_story_enrichment(setup):
    """Adding another episode does not invalidate a separately sourced variant."""
    from regional_knowledge.story_contracts import VariantBlock
    registry, owner, _ = setup
    sid = seed(registry, owner, key="variant-scope-seed")["story_id"]
    registry.edit(owner, sid, 1, [UpsertAssertion(
        op="upsert_assertion", kind="attributed_account",
        account_kind="other", attributed_to="Source A", proposition="Источник A сообщает об эпизоде."
    )], "variant-scope-assertion-a")
    registry.edit(owner, sid, 2, [UpsertAssertion(
        op="upsert_assertion", kind="attributed_account",
        account_kind="other", attributed_to="Source B", proposition="Источник B сообщает о другом эпизоде."
    )], "variant-scope-assertion-b")
    a, b = registry.get(owner, sid, view="evidence")["snapshot"]["assertions"]
    registry.edit(owner, sid, 3, [
        UpsertVariant(op="upsert_variant", audience="Местные жители", format="post",
                      body="Первый эпизод", attributions=["По версии источника A"],
                      blocks=[VariantBlock(
                          text="Первый эпизод", assertion_id=a["assertion_id"],
                          assertion_revision=a["revision"],
                      )]),
        UpsertVariant(op="upsert_variant", audience="Исследователи", format="article",
                      body="Второй эпизод", attributions=["По версии источника B"],
                      blocks=[VariantBlock(
                          text="Второй эпизод", assertion_id=b["assertion_id"],
                          assertion_revision=b["revision"],
                      )]),
    ], "variant-scope-two-formats")
    variants = registry.get(owner, sid, view="evidence")["snapshot"]["variants"]
    registry.transition(owner, sid, 4, "publish_ready", [
        {"variant_id": v["variant_id"], "revision": 1} for v in variants
    ], ReviewDecision(semantic_checked=True, attribution_checked=True,
                      rights_checked=True, reviewer_note="Synthetic review"),
       "variant-scope-approval")
    ids = [v["variant_id"] for v in variants]
    assert set(registry.get(owner, sid)["readiness"]["ready_variants"]) == set(ids)

    registry.edit(owner, sid, 5, [UpsertAssertion(
        op="upsert_assertion", kind="attributed_account",
        account_kind="other", attributed_to="Source C",
        proposition="Третий эпизод не входит в опубликованные варианты.",
    )], "variant-scope-new-unrelated")
    assert set(registry.get(owner, sid)["readiness"]["ready_variants"]) == set(ids)
    registry.edit(owner, sid, 6, [RecordAssessment(
        op="record_assessment", assertion_id=a["assertion_id"],
        assertion_revision=1, rationale="Review of A only",
    )], "variant-scope-a-review")
    readiness = registry.get(owner, sid)["readiness"]
    assert ids[0] in readiness["needs_revalidation"]
    assert ids[1] in readiness["ready_variants"]


def test_superseded_assessment_cannot_approve_a_historical_claim(setup):
    """One stale favorable review never overrides current adverse evidence."""
    registry, owner, _ = setup
    doc, page, region, chunk, text = document(registry, owner)
    sid = seed(registry, owner, key="review-freshness-seed")["story_id"]
    registry.edit(owner, sid, 1, [UpsertAssertion(
        op="upsert_assertion", kind="historical_claim",
        proposition="Мастера ежедневно собирались у фонаря.",
    )], "review-freshness-claim")
    assertion = registry.get(owner, sid, view="evidence")["snapshot"]["assertions"][0]
    registry.edit(owner, sid, 2, [AttachEvidence(
        op="attach_evidence", assertion_id=assertion["assertion_id"],
        assertion_revision=1, source_kind="document", source_id=doc, source_revision=1,
        relation="reports", original_excerpt="У старого фонаря днём собирались мастера.",
        locator=EvidenceLocator(page_id=page, region_id=region, chunk_id=chunk),
    )], "review-freshness-evidence")
    evidence_id = registry.get(owner, sid, view="evidence")["snapshot"]["assertions"][0]["evidence_ids"][0]
    registry.edit(owner, sid, 3, [RecordAssessment(
        op="record_assessment", assertion_id=assertion["assertion_id"],
        assertion_revision=1, evidence_ids=[evidence_id], support_status="single_source",
        semantic_review="supported", rationale="Source initially appears supporting",
    )], "review-freshness-supported")
    approved_id = registry.get(owner, sid, view="evidence")["snapshot"]["assertions"][0]["assessment_ids"][0]
    registry.edit(owner, sid, 4, [UpsertVariant(
        op="upsert_variant", audience="Жители", format="post",
        body="Мастера ежедневно собирались у фонаря.",
    )], "review-freshness-variant")
    variant = registry.get(owner, sid, view="evidence")["snapshot"]["variants"][0]
    registry.transition(owner, sid, 5, "publish_ready", [
        {"variant_id": variant["variant_id"], "revision": 1},
    ], ReviewDecision(semantic_checked=True, attribution_checked=True,
                      rights_checked=True, reviewer_note="Synthetic provisional approval"),
       "review-freshness-approve")
    registry.edit(owner, sid, 6, [RecordAssessment(
        op="record_assessment", assertion_id=assertion["assertion_id"],
        assertion_revision=1, evidence_ids=[evidence_id], support_status="contradicted",
        semantic_review="unsupported", rationale="Earlier reading was wrong",
        supersedes_assessment_id=approved_id,
    )], "review-freshness-superseded")
    detail = registry.get(owner, sid, view="assertion_page")
    reviews = detail["items"][0]["effective_assessments"]
    assert len(reviews) == 1 and reviews[0]["semantic_review"] == "unsupported"
    report = registry.validate(owner, sid)
    assert any("historical_claim_without_semantic_review" in x
               for x in report["variants"][0]["blockers"])
    assert not registry.get(owner, sid)["readiness"]["ready_variants"]
    with pytest.raises(StoryError) as exc:
        registry.edit(owner, sid, 7, [RecordAssessment(
            op="record_assessment", assertion_id=assertion["assertion_id"],
            assertion_revision=1, evidence_ids=[evidence_id],
            semantic_review="supported", rationale="Invalid stale override",
            supersedes_assessment_id=approved_id,
        )], "review-freshness-invalid-double-supersession")
    assert exc.value.code == "revision_conflict"


def test_story_search_indexes_current_claims_not_just_story_titles(setup):
    """A detail only present in assertion text is discoverable and remains ACL scoped."""
    registry, owner, stranger = setup
    sid = seed(registry, owner, name="Старая городская заметка",
               key="claim-search-001")["story_id"]
    registry.edit(owner, sid, 1, [UpsertAssertion(
        op="upsert_assertion", kind="attributed_account", account_kind="other",
        attributed_to="Синтетический автор",
        proposition="На городской площади обнаружили алебарду с необычной печатью.",
    )], "claim-search-assertion-001")
    result = registry.search(owner, query="алебарду")
    assert sid in [hit["story_id"] for hit in result["results"]]
    assert registry.search(stranger, query="алебарду")["results"] == []
    assertion = registry.get(owner, sid, view="evidence")["snapshot"]["assertions"][0]
    registry.edit(owner, sid, 2, [UpsertAssertion(
        op="upsert_assertion", kind="attributed_account", account_kind="other",
        assertion_id=assertion["assertion_id"], expected_assertion_revision=1,
        attributed_to="Синтетический автор",
        proposition="На городской площади обнаружили старинный меч с печатью.",
    )], "claim-search-assertion-002")
    assert sid not in [hit["story_id"] for hit in registry.search(owner, query="алебарду")["results"]]
    assert sid in [hit["story_id"] for hit in registry.search(owner, query="меч")["results"]]


def test_reviewed_merge_preserves_editors_links_drafts_and_old_interest_without_approval(setup):
    """A true duplicate merge must never erase authorship or inherit publish_ready."""
    registry, owner, _ = setup
    target = seed(registry, owner, name="Первый источник о фонаре",
                  key="preserve-merge-target-seed")["story_id"]
    source_id = seed(registry, owner, name="Дубль по другому источнику",
                     key="preserve-merge-source-seed")["story_id"]
    entity_id = str(uuid4())
    edited = registry.edit(owner, source_id, 1, [
        SetContributors(op="set_contributors", contributors=[
            {"name": "Редактор источника B", "role": "researcher"}]),
        LinkEntity(op="link_entity", entity_ref=entity_id, kind="place"),
        RecordInterest(op="record_interest_assessment", audience="Туристы", format="post"),
        UpsertVariant(op="upsert_variant", audience="Жители", format="post",
                      body="Источник B описывает старый фонарь."),
    ], "preserve-merge-context-edit")
    assert edited["committed_revision"] == 2
    source_snap = registry.get(owner, source_id, view="evidence")["snapshot"]
    variant_id = source_snap["variants"][0]["variant_id"]
    assert source_snap["interest_assessments"]

    merged = registry.merge(
        owner, target, [source_id], {target: 1, source_id: 2},
        "Exact duplicated episode manually identified in synthetic review",
        "preserve-merge-contents")
    assert merged["committed_revision"] == 2
    snap = registry.get(owner, target, view="evidence")["snapshot"]
    assert {"name": "Редактор источника B", "role": "researcher"} in snap["contributors"]
    assert any(x["entity_ref"] == entity_id for x in snap["links"])
    migrated = next(x for x in snap["variants"] if x["variant_id"] == variant_id)
    assert migrated["state"] == "needs_revalidation"
    assert migrated["merge_origin_story_id"] == source_id
    assert migrated["previous_editorial_state"] == "drafting"
    assert registry.get(owner, target)["readiness"]["ready_variants"] == []
    retained = snap["historical_interest_assessments"]
    assert retained[0]["origin_story_id"] == source_id
    assert retained[0]["assessment"]["audience"] == "Туристы"
    assert retained[0]["requires_reassessment"]
    with registry.corpus.connect() as db:
        owner_row = db.execute("SELECT story_id,state,approved_revision FROM story_variants WHERE id=?",
                               (variant_id,)).fetchone()
        assert owner_row["story_id"] == target
        assert owner_row["state"] == "needs_revalidation"
        assert owner_row["approved_revision"] is None
        assert db.execute("SELECT COUNT(*) FROM story_variant_versions WHERE id=?",
                          (variant_id,)).fetchone()[0] == 1
    old = registry.get(owner, source_id, view="evidence")["snapshot"]
    assert old["archived"] and old["merged_into"] == target
    assert old["contributors"]
