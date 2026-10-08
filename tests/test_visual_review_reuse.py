from uuid import uuid4

import pytest

import regional_knowledge.stage_service as stage_service
from regional_knowledge.object_store import UnavailableObjectStore
from regional_knowledge.sqlite_backend import SQLiteBackend
from regional_knowledge.sqlite_data import defaults
from regional_knowledge.stage_graph import StagedGraph, StagedPage, StagedRegion, StagedIllustration
from regional_knowledge.contracts import BBox
from regional_knowledge.supabase_backend import LexicalOnlyEmbedder


def backend(tmp_path):
    return SQLiteBackend(
        corpus_path=tmp_path / "corpus.sqlite3",
        embedder=LexicalOnlyEmbedder(),
        object_store=UnavailableObjectStore(),
    )


@pytest.mark.asyncio
async def test_reuses_visual_review_from_validated_same_source_revision(tmp_path, monkeypatch):
    service=backend(tmp_path)
    actor,document,old_job=[str(uuid4()) for _ in range(3)]
    source_sha="a"*64
    service.corpus.put("rkb_users",[{**defaults("rkb_users"),"id":actor}])
    service.corpus.put("rkb_documents",[{
        **defaults("rkb_documents"),
        "id":document,
        "owner_user_id":actor,
        "title":"Book",
        "source_sha256":source_sha,
        "page_count":1,
    }])
    service.corpus.put("rkb_ingestion_jobs",[{
        **defaults("rkb_ingestion_jobs"),
        "id":old_job,
        "owner_user_id":actor,
        "document_id":document,
        "source_sha256":source_sha,
        "source_file_id":"old",
        "staged_revision":1,
        "state":"processing",
        "cursor":"vectors",
        "staged_graph_object_id":str(uuid4()),
    }])
    with service.corpus.connect() as db:
        db.execute(
            "insert into revision_publication values(?,?,?,?,?,?,?)",
            (document,1,old_job,source_sha,"[]","[]","pending"),
        )

    prior=StagedGraph(
        revision=1,
        pages=[StagedPage(
            page_id=uuid4(),
            physical_page_index=0,
            source_material="visual_reviewed",
            source_review_note="Reviewed rendered source page; native text coverage checked.",
        )],
    )
    current=StagedGraph(
        revision=2,
        pages=[StagedPage(
            page_id=uuid4(),
            physical_page_index=0,
            source_material="full_native",
            source_review_note="Native extraction only.",
        )],
    )

    async def fake_load(service_arg,row):
        assert service_arg is service and row["id"]==old_job
        return prior

    monkeypatch.setattr(stage_service,"_load_graph",fake_load)
    updated,count,revision=await stage_service._reuse_prior_visual_reviews(
        service,
        {
            "document_id":document,
            "source_sha256":source_sha,
            "staged_revision":2,
        },
        current,
    )
    assert count==1 and revision==1
    assert updated.pages[0].source_material=="visual_reviewed"
    assert "same-source revision 1" in updated.pages[0].source_review_note
    assert "coverage checked" in updated.pages[0].source_review_note
    await service.aclose()


@pytest.mark.asyncio
async def test_visual_review_reuse_requires_same_source_sha(tmp_path, monkeypatch):
    service=backend(tmp_path)
    actor,document,old_job=[str(uuid4()) for _ in range(3)]
    service.corpus.put("rkb_users",[{**defaults("rkb_users"),"id":actor}])
    service.corpus.put("rkb_documents",[{
        **defaults("rkb_documents"),
        "id":document,
        "owner_user_id":actor,
        "title":"Book",
        "source_sha256":"a"*64,
        "page_count":1,
    }])
    service.corpus.put("rkb_ingestion_jobs",[{
        **defaults("rkb_ingestion_jobs"),
        "id":old_job,
        "owner_user_id":actor,
        "document_id":document,
        "source_sha256":"b"*64,
        "source_file_id":"old",
        "staged_revision":1,
        "state":"processing",
        "cursor":"vectors",
        "staged_graph_object_id":str(uuid4()),
    }])
    with service.corpus.connect() as db:
        db.execute(
            "insert into revision_publication values(?,?,?,?,?,?,?)",
            (document,1,old_job,"b"*64,"[]","[]","pending"),
        )
    current=StagedGraph(
        revision=2,
        pages=[StagedPage(
            page_id=uuid4(),
            physical_page_index=0,
            source_material="full_native",
            source_review_note="Native extraction only.",
        )],
    )

    async def must_not_load(*args,**kwargs):
        raise AssertionError("different-source graph must not be loaded")

    monkeypatch.setattr(stage_service,"_load_graph",must_not_load)
    updated,count,revision=await stage_service._reuse_prior_visual_reviews(
        service,
        {
            "document_id":document,
            "source_sha256":"a"*64,
            "staged_revision":2,
        },
        current,
    )
    assert updated is current
    assert count==0 and revision is None
    assert updated.pages[0].source_material=="full_native"
    await service.aclose()


def _review_fixture_page(text="Accepted printed text", *, left=10):
    page_id=uuid4()
    region=StagedRegion(
        region_key="body-0", region_id=uuid4(), page_id=page_id,
        kind="body", reading_order=0,
        bbox=BBox(left=left,top=10,right=900,bottom=400),
        source_text=text, normalized_text=text,
    )
    return StagedPage(
        page_id=page_id, physical_page_index=0,
        width=1000, height=1000, regions=[region],
    )


def test_source_review_reuse_fails_closed_on_changed_ocr_or_geometry():
    old_page=_review_fixture_page()
    same=_review_fixture_page()
    old=StagedGraph(revision=1,pages=[old_page])
    proposed=StagedGraph(revision=2,pages=[same])
    assert stage_service._reviewed_source_page_signature(old,old_page)==(
        stage_service._reviewed_source_page_signature(proposed,same)
    )
    different_text=_review_fixture_page("Mutated OCR despite same PDF SHA")
    changed=StagedGraph(revision=2,pages=[different_text])
    assert stage_service._reviewed_source_page_signature(old,old_page)!=(
        stage_service._reviewed_source_page_signature(changed,different_text)
    )
    shifted=_review_fixture_page(left=11)
    shifted_graph=StagedGraph(revision=2,pages=[shifted])
    assert stage_service._reviewed_source_page_signature(old,old_page)!=(
        stage_service._reviewed_source_page_signature(shifted_graph,shifted)
    )


def test_source_review_reuse_rejects_changed_or_missing_illustration():
    old_page=_review_fixture_page()
    same=_review_fixture_page()
    def image(page,description):
        region=page.regions[0]
        return StagedIllustration(
            illustration_key="drawing-0",illustration_id=uuid4(),
            page_id=page.page_id,source_region_id=region.region_id,
            bbox=region.bbox,kind="drawing",
            caption_region_ids=[region.region_id],visual_description=description,
        )
    old=StagedGraph(revision=1,pages=[old_page],
                    illustrations=[image(old_page,"Original illustration")])
    proposed=StagedGraph(revision=2,pages=[same],
                         illustrations=[image(same,"Original illustration")])
    assert stage_service._reviewed_source_page_signature(old,old_page)==(
        stage_service._reviewed_source_page_signature(proposed,same)
    )
    changed=StagedGraph(revision=2,pages=[same],
                        illustrations=[image(same,"Changed illustration")])
    assert stage_service._reviewed_source_page_signature(old,old_page)!=(
        stage_service._reviewed_source_page_signature(changed,same)
    )
    deleted=StagedGraph(revision=2,pages=[same])
    assert stage_service._reviewed_source_page_signature(old,old_page)!=(
        stage_service._reviewed_source_page_signature(deleted,same)
    )
