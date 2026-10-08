"""Inherited-source rechunking requires exact prior accepted book evidence."""
from __future__ import annotations

import hashlib
from uuid import uuid4

import pytest

from regional_knowledge.accepted_source_reuse import certified_accepted_source_reuse
from regional_knowledge.contracts import BBox, StagePageInput
from regional_knowledge.object_store import UnavailableObjectStore
from regional_knowledge.sqlite_backend import SQLiteBackend
from regional_knowledge.sqlite_data import defaults
from regional_knowledge.stage_graph import (
    StagedGraph, StagedIllustration, StagedPage, StagedRegion, StagedRelation,
    validate_graph,
)
from regional_knowledge.supabase_backend import LexicalOnlyEmbedder


def make_source(tmp_path):
    service = SQLiteBackend(
        corpus_path=tmp_path / "corpus.sqlite3",
        embedder=LexicalOnlyEmbedder(),
        object_store=UnavailableObjectStore(),
    )
    actor, document, job, old_page, new_page = [str(uuid4()) for _ in range(5)]
    old_ids = [str(uuid4()) for _ in range(3)]
    new_ids = [str(uuid4()) for _ in range(3)]
    old_figure, old_caption, old_body = old_ids
    new_figure, new_caption, new_body = new_ids
    sha = "a" * 64
    parts = [
        ("figure", "", {"left": 15, "top": 10, "right": 650, "bottom": 500}),
        ("caption", "Diagram of an old bridge.", {"left": 20, "top": 510, "right": 650, "bottom": 580}),
        ("body", "The bridge crossed a river.", {"left": 40, "top": 600, "right": 920, "bottom": 930}),
    ]
    service.corpus.put("rkb_users", [{**defaults("rkb_users"), "id": actor}])
    service.corpus.put("rkb_documents", [{
        **defaults("rkb_documents"), "id": document, "owner_user_id": actor,
        "title": "Unrelated synthetic historical source",
        "active_revision": 1, "page_count": 1, "source_sha256": sha,
        "source_archive_status": "verified",
        "source_archive_ref": "knowledge://synthetic/source",
    }])
    service.corpus.put("rkb_ingestion_jobs", [{
        **defaults("rkb_ingestion_jobs"),
        "id": job, "document_id": document, "owner_user_id": actor,
        "source_sha256": sha, "staged_revision": 1,
        "state": "finalized",
    }])
    service.corpus.put("rkb_pages", [{
        **defaults("rkb_pages"), "id": old_page, "document_id": document,
        "revision": 1, "physical_page_index": 0,
        "printed_page_number": "I", "width": 1000, "height": 1000,
    }])
    service.corpus.put("rkb_regions", [
        {
            **defaults("rkb_regions"), "id": id_, "page_id": old_page,
            "kind": kind, "source_text": txt,
            "text_sha256": hashlib.sha256(txt.encode()).hexdigest(),
            "bbox": bounds, "reading_order": idx,
        }
        for idx, ((kind, txt, bounds), id_) in enumerate(zip(parts, old_ids, strict=True))
    ])
    service.corpus.put("rkb_illustrations", [{
        **defaults("rkb_illustrations"), "id": str(uuid4()),
        "page_id": old_page, "document_id": document,
        "source_region_id": old_figure, "kind": "drawing",
        "caption_region_ids": [old_caption],
        "nearby_region_ids": [old_body],
        "visual_description": "A diagram of an old bridge",
        "display_rotation_degrees": 0,
    }])
    service.corpus.put("rkb_region_relations", [{
        **defaults("rkb_region_relations"), "id": str(uuid4()),
        "page_id": old_page, "kind": "caption_of",
        "source_region_id": old_caption, "target_region_id": old_figure,
    }])
    staged_regions = [
        StagedRegion(
            region_key=f"r{idx}", region_id=id_, page_id=new_page,
            kind=kind, source_text=txt, normalized_text=txt,
            bbox=BBox(**bounds), reading_order=idx,
        )
        for idx, ((kind, txt, bounds), id_) in enumerate(zip(parts, new_ids, strict=True))
    ]
    staged_page = StagedPage(
        page_id=new_page, physical_page_index=0,
        printed_page_number="I", width=1000, height=1000,
        source_material="accepted_reuse",
        source_review_note="Inherited accepted source without another visual review",
        regions=staged_regions,
    )
    staged_img = StagedIllustration(
        illustration_key="img", illustration_id=uuid4(),
        page_id=new_page, source_region_id=new_figure,
        bbox=BBox(**parts[0][2]), kind="drawing",
        caption_region_ids=[new_caption], nearby_region_ids=[new_body],
        visual_description="A diagram of an old bridge",
    )
    relation = StagedRelation(
        kind="caption_of",
        source_region_id=new_caption,
        target_region_id=new_figure,
    )
    graph = StagedGraph(revision=2, pages=[staged_page],
                        illustrations=[staged_img], relations=[relation])
    ingestion = {
        "document_id": document, "owner_user_id": actor,
        "source_sha256": sha, "staged_revision": 2,
    }
    return service, ingestion, graph, job


def test_accepted_source_requires_server_proof_and_is_not_fresh_review(tmp_path):
    service, row, graph, _ = make_source(tmp_path)
    assert certified_accepted_source_reuse(service.corpus, row, graph) == (True, 1)
    untrusted = validate_graph(graph, expected_page_count=1)
    assert any("source completeness unreviewed" in x for x in untrusted.errors)
    verified = validate_graph(graph, expected_page_count=1, accepted_reuse_verified=True)
    assert not any("source completeness unreviewed" in x for x in verified.errors)


@pytest.mark.parametrize("mutation", [
    "ocr", "geometry", "picture_bbox", "picture_caption",
    "picture_removed", "relation_removed", "wrong_owner",
    "source_sha", "unverified_archive", "no_finalized_job",
    "older_staged_revision", "missing_region",
])
def test_accepted_source_fail_closed_on_any_material_change(tmp_path, mutation):
    service, row, graph, job_id = make_source(tmp_path)
    graph = graph.model_copy(deep=True)
    row = dict(row)
    if mutation == "ocr":
        graph.pages[0].regions[2].source_text = "A different river."
    elif mutation == "geometry":
        graph.pages[0].regions[2].bbox = BBox(left=41, top=600, right=920, bottom=930)
    elif mutation == "picture_bbox":
        graph.illustrations[0].bbox = BBox(left=16, top=10, right=650, bottom=500)
    elif mutation == "picture_caption":
        graph.illustrations[0].caption_region_ids = []
    elif mutation == "picture_removed":
        graph.illustrations.clear()
    elif mutation == "relation_removed":
        graph.relations.clear()
    elif mutation == "wrong_owner":
        row["owner_user_id"] = str(uuid4())
    elif mutation == "source_sha":
        row["source_sha256"] = "b" * 64
    elif mutation == "unverified_archive":
        doc = service.corpus.one("rkb_documents",row["document_id"])
        service.corpus.put("rkb_documents", [{
            **doc, "source_archive_status": "pending",
        }])
    elif mutation == "no_finalized_job":
        job = service.corpus.one("rkb_ingestion_jobs",job_id)
        service.corpus.put("rkb_ingestion_jobs", [{
            **job, "state": "failed",
        }])
    elif mutation == "older_staged_revision":
        row["staged_revision"] = 1
    elif mutation == "missing_region":
        graph.pages[0].regions.pop()
    assert certified_accepted_source_reuse(service.corpus,row,graph) == (False,None)


def test_only_explicit_accepted_reuse_is_eligible(tmp_path):
    service, row, graph, _ = make_source(tmp_path)
    graph.pages[0].source_material = "preview"
    assert certified_accepted_source_reuse(service.corpus,row,graph) == (False,None)


def test_staging_contract_accepts_distinct_reuse_status():
    item = StagePageInput.model_validate({
        "page_id":str(uuid4()),
        "physical_page_index": 0,
        "source_material": "accepted_reuse",
        "source_review_note": "Previously accepted source, no new page review",
        "regions": [],
    })
    assert item.source_material == "accepted_reuse"
