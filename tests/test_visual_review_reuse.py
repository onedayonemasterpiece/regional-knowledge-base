from uuid import uuid4

import pytest

import regional_knowledge.stage_service as stage_service
from regional_knowledge.object_store import UnavailableObjectStore
from regional_knowledge.sqlite_backend import SQLiteBackend
from regional_knowledge.sqlite_data import defaults
from regional_knowledge.stage_graph import StagedGraph, StagedPage
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
