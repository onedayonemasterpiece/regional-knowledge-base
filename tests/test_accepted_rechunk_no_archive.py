"""Owner-safe, idempotent source-only re-chunk startup on unrelated synthetic data."""
from uuid import uuid4
import pytest
from regional_knowledge.contracts import Principal
from regional_knowledge.object_store import UnavailableObjectStore
from regional_knowledge.sqlite_backend import SQLiteBackend
from regional_knowledge.sqlite_data import defaults
from regional_knowledge.supabase_backend import LexicalOnlyEmbedder


def setup_book(tmp_path):
    backend=SQLiteBackend(
        corpus_path=tmp_path/"accepted_source.sqlite",
        embedder=LexicalOnlyEmbedder(),
        object_store=UnavailableObjectStore(),
    )
    user,doc,source,previous,page=[str(uuid4()) for _ in range(5)]
    principal=Principal(subject=user,client_id="synthetic",issuer="test",access_token="unit-test")
    sha="a"*64
    backend.corpus.put("rkb_users",[{**defaults("rkb_users"),"id":user}])
    backend.corpus.put("rkb_documents",[{
        **defaults("rkb_documents"),"id":doc,
        "owner_user_id":user,"title":"Synthetic accepted source",
        "source_sha256":sha,"page_count":1,"active_revision":1,
        "source_archive_status":"verified","source_archive_ref":"entry_synthetic",
        "source_format":"pdf",
    }])
    backend.corpus.put("rkb_objects",[{
        **defaults("rkb_objects"),"id":source,
        "document_id":doc,"kind":"source_pdf",
        "object_key":"private/accepted/source.pdf",
        "sha256":sha,"mime_type":"application/pdf",
        "deleted_at":"2026-01-01T00:00:00Z",
    }])
    backend.corpus.put("rkb_ingestion_jobs",[{
        **defaults("rkb_ingestion_jobs"),"id":previous,
        "document_id":doc,"owner_user_id":user,
        "source_sha256":sha,"source_object_id":source,
        "source_file_id":"already-finalized",
        "staged_revision":1,"state":"finalized",
    }])
    backend.corpus.put("rkb_pages",[{
        **defaults("rkb_pages"),"id":page,"document_id":doc,
        "revision":1,"physical_page_index":0,
        "width":1000,"height":1000,
    }])
    return backend,principal,doc


async def rechunk(b,principal,doc,**extra):
    return await b.book_ingest(
        command="rechunk",principal=principal,file=None,
        document_id=doc,ingestion_id=None,cursor=None,payload=extra or None,
    )


@pytest.mark.asyncio
async def test_derived_start_without_archive_fetch_is_idempotent_and_not_active(tmp_path,monkeypatch):
    backend,principal,doc=setup_book(tmp_path)
    try:
        from regional_knowledge import source_archive
        async def forbidden(*args,**kwargs):
            raise AssertionError("already accepted source must not be downloaded")
        monkeypatch.setattr(source_archive,"download_source",forbidden)
        async def forbidden_inspect(*args,**kwargs):
            raise AssertionError("accepted material must not be reinspected")
        backend.pdf_processor.inspect_file=forbidden_inspect
        first=await rechunk(backend,principal,doc)
        assert first.state=="staged"
        assert first.document_id==doc
        assert first.next_action=="continue_pages"
        row=backend.corpus.one("rkb_ingestion_jobs",first.ingestion_id)
        assert row["source_file_id"]==f"accepted-rechunk:{doc}:after:1"
        assert row["staged_revision"]==2
        assert backend.corpus.one("rkb_documents",doc)["active_revision"]==1
        replay=await rechunk(backend,principal,doc)
        assert replay.ingestion_id==first.ingestion_id
        assert backend.corpus.one("rkb_documents",doc)["active_revision"]==1
        assert len([x for x in backend.corpus.rows("rkb_ingestion_jobs")
                    if x["document_id"]==doc])==2
    finally:
        await backend.aclose()


@pytest.mark.asyncio
async def test_rechunk_requires_owner_and_cannot_force_unrelated_revision(tmp_path):
    b,owner,doc=setup_book(tmp_path)
    try:
        other=Principal(subject=str(uuid4()),client_id="other",issuer="test",access_token="other")
        with pytest.raises((LookupError,PermissionError)):
            await rechunk(b,other,doc)
        with pytest.raises(ValueError,match="cannot force"):
            await rechunk(b,owner,doc,duplicate_policy="new_revision")
        assert len(b.corpus.rows("rkb_ingestion_jobs"))==1
    finally: await b.aclose()


@pytest.mark.asyncio
async def test_rechunk_requires_verified_active_finalized_source(tmp_path):
    b,actor,doc=setup_book(tmp_path)
    try:
        d=b.corpus.one("rkb_documents",doc)
        b.corpus.put("rkb_documents",[{**d,"source_archive_status":"pending"}])
        with pytest.raises(RuntimeError,match="archive"):
            await rechunk(b,actor,doc)
        b.corpus.put("rkb_documents",[d])
        j=b.corpus.rows("rkb_ingestion_jobs")[0]
        b.corpus.put("rkb_ingestion_jobs",[{**j,"state":"failed"}])
        with pytest.raises(RuntimeError,match="finalized"):
            await rechunk(b,actor,doc)
        assert len(b.corpus.rows("rkb_ingestion_jobs"))==1
    finally:await b.aclose()
