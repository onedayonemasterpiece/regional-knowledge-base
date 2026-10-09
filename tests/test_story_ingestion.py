"""Synthetic same-pass book stage -> accepted Story Registry candidates.

No real books, network, model quotas, original source content or publication.
"""
from __future__ import annotations

from types import SimpleNamespace
from uuid import UUID, uuid4, uuid5

import pytest

from regional_knowledge.contracts import Principal, StagePageInput, StageChunkInput
from regional_knowledge.sqlite_corpus import SQLiteCorpus
from regional_knowledge.sqlite_data import LocalContext
from regional_knowledge.stage_graph import StagedGraph, compile_model_stage, merge_stage
from regional_knowledge.story_contracts import StageStoryCandidateInput
from regional_knowledge.story_ingestion import persist_stage, activate_stories, extraction_status
from regional_knowledge.story_registry import StoryRegistry, StoryError


def owner():
    return Principal(subject=str(uuid4()), client_id="synthetic-ingesting-model",
                     issuer="unit-test", access_token="not-a-real-token")


@pytest.fixture
def fixture(tmp_path):
    corpus = SQLiteCorpus(tmp_path / "corpus.sqlite3")
    registry = StoryRegistry(corpus)
    principal = owner()
    document_id, ingestion_id = str(uuid4()), str(uuid4())
    page_id = str(uuid5(UUID(document_id), "page:1:0"))
    text = "На городском празднике собирались музыканты и ремесленники."
    doc = {
        "id": document_id, "owner_user_id": principal.subject,
        "title": "Синтетическая книга", "authors": ["Аноним"],
        "source_sha256": "a" * 64, "active_revision": 0, "page_count": 1,
        "content_visibility": "private", "source_visibility": "private",
        "rights_status": "restricted",
    }
    job = {
        "id": ingestion_id, "document_id": document_id,
        "owner_user_id": principal.subject, "staged_revision": 1,
        "state": "processing", "cursor": "",
    }
    corpus.put("rkb_documents", [doc])
    corpus.put("rkb_ingestion_jobs", [job])
    page = StagePageInput(
        page_id=page_id, physical_page_index=0, story_candidates_reviewed=True,
        source_material="visual_reviewed", source_review_note="Verified synthetic source text",
        regions=[{"region_key": "body", "kind": "body",
                  "bbox": {"left": 1, "top": 1, "right": 900, "bottom": 800},
                  "reading_order": 0, "source_text": text}],
    )
    chunks = [StageChunkInput(chunk_key="first-episode", title="Example",
                              region_refs=[{"page_id": page_id, "region_key": "body"}])]
    graph = merge_stage(StagedGraph(revision=1), compile_model_stage(
        StagedGraph(revision=1), document_id=document_id, revision=1,
        pages=[page], chunks=chunks))
    candidate = StageStoryCandidateInput(
        candidate_key="festival-episode",
        title="Музыканты на городском празднике",
        summary="Из книжного рассказа: на празднике были музыканты и мастера.",
        material_type="everyday_life",
        proposition="По книге, на празднике собирались музыканты и ремесленники.",
        account_kind="other", attributed_to="Аноним",
        evidence_refs=[{"page_id": page_id, "region_key": "body",
                        "original_excerpt": "собирались музыканты и ремесленники."}],
    )
    return corpus, registry, principal, doc, job, graph, page, candidate


def accept_source(corpus, principal, doc, job, graph):
    pages = [
        {"id": str(page.page_id), "document_id": doc["id"], "revision": 1,
         "physical_page_index": page.physical_page_index,
         "printed_page_number": page.printed_page_number}
        for page in graph.pages
    ]
    regions = [
        {"id": str(region.region_id), "page_id": str(page.page_id),
         "source_text": region.source_text, "reading_order": region.reading_order}
        for page in graph.pages for region in page.regions
    ]
    corpus.put("rkb_pages", pages)
    corpus.put("rkb_regions", regions)
    with corpus.connect() as db:
        db.execute("BEGIN IMMEDIATE")
        ctx = LocalContext(corpus, db, principal.subject)
        corpus.put("rkb_documents", [{**doc, "active_revision": 1}], connection=db)
        first = activate_stories(ctx, doc, job["id"], 1)
    return first


def test_auto_candidate_staged_with_page_and_accepted_only_when_book_activates(fixture):
    corpus, registry, principal, doc, job, graph, page, candidate = fixture
    stored = persist_stage(corpus, principal, job, graph, [page], [candidate])
    assert stored == {"candidate_count": 1, "needs_review_count": 0, "reviewed_in_batch": 1}
    assert corpus.path.exists()
    status = extraction_status(corpus, job)
    assert status["state"] == "staged"
    assert status["saved_candidates"] == 0
    with corpus.connect() as db:
        assert db.execute("SELECT count(*) FROM story_records").fetchone()[0] == 0
    activated = accept_source(corpus, principal, doc, job, graph)
    assert activated == {"accepted": 1, "reused": 0, "needs_review": 0}
    status = extraction_status(corpus, {**job, "state": "finalized"})
    assert status["state"] == "completed"
    assert status["saved_candidates"] == 1
    with corpus.connect() as db:
        row = db.execute("SELECT id,revision,state FROM story_records").fetchone()
        evidence = db.execute("SELECT * FROM story_evidence").fetchone()
    card = registry.get(principal, row["id"], view="evidence")
    assert row["revision"] == 2 and row["state"] == "candidate"
    assert card["snapshot"]["assertions"][0]["kind"] == "attributed_account"
    assert card["readiness"]["total_variants"] == 0
    assert evidence["text_match"] == "exact" and evidence["source_role"] == "unspecified"
    assert evidence["original_review"] == "not_checked"
    assert evidence["relation"] == "reports"
    assert registry.search(principal, "музыканты")["results"][0]["story_id"] == row["id"]
    # Replayed activation cannot create a second accepted story.
    with corpus.connect() as db:
        db.execute("BEGIN IMMEDIATE")
        ctx = LocalContext(corpus, db, principal.subject)
        assert activate_stories(ctx, doc, job["id"], 1)["accepted"] == 0
    with corpus.connect() as db:
        assert db.execute("SELECT count(*) FROM story_records").fetchone()[0] == 1


def test_stage_replay_and_source_edit_invalidate_candidate(fixture):
    corpus, registry, principal, doc, job, graph, page, candidate = fixture
    persist_stage(corpus, principal, job, graph, [page], [candidate])
    persist_stage(corpus, principal, job, graph, [page], [candidate])
    with corpus.connect() as db:
        assert db.execute("SELECT count(*) FROM story_ingest_candidates").fetchone()[0] == 1
    changed = page.model_copy(update={"story_candidates_reviewed": False})
    persist_stage(corpus, principal, job, graph, [changed], [])
    with corpus.connect() as db:
        assert db.execute("SELECT count(*) FROM story_ingest_candidates").fetchone()[0] == 0
    status = extraction_status(corpus, job)
    assert status["state"] == "not_reviewed"
    assert status["reviewed_pages"] == 0


def test_false_or_foreign_evidence_rejected_before_authority(fixture):
    corpus, registry, principal, doc, job, graph, page, candidate = fixture
    wrong = candidate.model_copy(update={"evidence_refs": [
        candidate.evidence_refs[0].model_copy(update={"original_excerpt": "Совершенно выдуманный текст."})]})
    result = persist_stage(corpus, principal, job, graph, [page], [wrong])
    assert result["candidate_count"] == 0 and result["needs_review_count"] == 1
    assert extraction_status(corpus, job)["saved_candidates"] == 0
    assert extraction_status(corpus, job)["needs_review_candidates"] == 1
    assert accept_source(corpus, principal, doc, job, graph)["accepted"] == 0
    stranger = owner()
    with pytest.raises(PermissionError):
        persist_stage(corpus, stranger, job, graph, [page], [candidate])


def test_legacy_import_without_model_review_is_not_misrepresented(fixture):
    corpus, registry, principal, doc, job, graph, page, candidate = fixture
    # Existing ingestion callers still activate a book when they omit stories.
    old_page = page.model_copy(update={"story_candidates_reviewed": False})
    persist_stage(corpus, principal, job, graph, [old_page], [])
    assert accept_source(corpus, principal, doc, job, graph)["accepted"] == 0
    state = extraction_status(corpus, {**job, "state": "finalized"})
    assert state["state"] == "awaiting_agent" and state["saved_candidates"] == 0


@pytest.mark.asyncio
async def test_model_facing_existing_ingest_schema_advertises_inline_candidate_stage(monkeypatch):
    from regional_knowledge.server import build_server
    monkeypatch.setenv("RKB_DEV_NOAUTH", "1")
    server = build_server()
    tools = {tool.name: tool for tool in await server.list_tools()}
    ingest = tools["book_ingest"]
    assert "story_candidates" in ingest.input_schema["properties"]
    assert "source" in ingest.description
    assert "story_candidates_reviewed" in ingest.description
    assert "story_candidates" in server.instructions
    assert "story_extraction" in ingest.output_schema["properties"]
    # No second model, source upload, chat workflow or companion app is required.
    assert tools["book_ingest"].annotations.read_only_hint is False
