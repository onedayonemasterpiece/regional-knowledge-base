import hashlib
from uuid import uuid4

import pytest

from regional_knowledge.contracts import Principal
from regional_knowledge.object_store import UnavailableObjectStore
from regional_knowledge.sqlite_backend import SQLiteBackend
from regional_knowledge.sqlite_data import defaults
from regional_knowledge.supabase_backend import LexicalOnlyEmbedder


@pytest.fixture
def backend(tmp_path):
    return SQLiteBackend(
        corpus_path=tmp_path / "corpus.db",
        embedder=LexicalOnlyEmbedder(),
        object_store=UnavailableObjectStore(),
    )


@pytest.mark.asyncio
async def test_explicit_new_revision_bypasses_different_pending_ingestion(backend):
    actor, doc, old_job, new_job = [str(uuid4()) for _ in range(4)]
    backend.corpus.put("rkb_users", [{**defaults("rkb_users"), "id": actor}])
    backend.corpus.put("rkb_documents", [{
        **defaults("rkb_documents"),
        "id": doc,
        "owner_user_id": actor,
        "title": "Book",
        "source_sha256": "a" * 64,
        "page_count": 1,
        "active_revision": 0,
    }])
    backend.corpus.put("rkb_ingestion_jobs", [{
        **defaults("rkb_ingestion_jobs"),
        "id": old_job,
        "owner_user_id": actor,
        "document_id": doc,
        "source_file_id": f"archive-reprocess:{doc}:after:0",
        "source_sha256": "a" * 64,
        "state": "processing",
        "cursor": "vectors",
        "staged_revision": 1,
        "duplicate_policy": "new_revision",
    }])
    payload = {
        "p_ingestion_id": new_job,
        "p_document_id": doc,
        "p_title": "Book",
        "p_authors": [],
        "p_source_sha256": "a" * 64,
        "p_source_file_id": f"archive-reprocess:{doc}:force-after:0",
        "p_page_count": 1,
        "p_duplicate_policy": "new_revision",
        "p_force_new_revision": True,
    }
    headers = {"x-rkb-actor": actor}
    created = await backend.local_rpc("rkb_start_ingestion", payload, headers)
    replay = await backend.local_rpc(
        "rkb_start_ingestion",
        {**payload, "p_ingestion_id": str(uuid4())},
        headers,
    )
    assert created.json() == replay.json()
    assert created.json()[0]["ingestion_id"] == new_job
    row = backend.corpus.one("rkb_ingestion_jobs", new_job)
    assert row["staged_revision"] == 2
    assert backend.corpus.one("rkb_ingestion_jobs", old_job)["state"] == "processing"


@pytest.mark.asyncio
async def test_force_flag_requires_new_revision_policy(backend):
    actor, doc = str(uuid4()), str(uuid4())
    backend.corpus.put("rkb_users", [{**defaults("rkb_users"), "id": actor}])
    backend.corpus.put("rkb_documents", [{
        **defaults("rkb_documents"),
        "id": doc,
        "owner_user_id": actor,
        "title": "Book",
        "source_sha256": "a" * 64,
        "page_count": 1,
    }])
    with pytest.raises(ValueError, match="requires new_revision"):
        await backend.local_rpc("rkb_start_ingestion", {
            "p_ingestion_id": str(uuid4()),
            "p_document_id": doc,
            "p_title": "Book",
            "p_authors": [],
            "p_source_sha256": "a" * 64,
            "p_source_file_id": "forced",
            "p_page_count": 1,
            "p_duplicate_policy": "reuse",
            "p_force_new_revision": True,
        }, {"x-rkb-actor": actor})


@pytest.mark.asyncio
async def test_reprocess_metadata_new_revision_selects_explicit_restart(backend, monkeypatch):
    actor, doc = str(uuid4()), str(uuid4())
    principal = Principal(
        subject=actor,
        client_id="test",
        issuer="test",
        access_token="test-token",
    )
    calls = []

    async def reprocess(*, principal, document_id, force_new_revision=False):
        calls.append((principal.subject, document_id, force_new_revision))
        from regional_knowledge.contracts import BookIngestOutput
        return BookIngestOutput(
            ingestion_id="job",
            state="staged",
            message="ok",
            document_id=document_id,
            next_action="continue_pages",
        )

    monkeypatch.setattr(backend, "_reprocess_existing_source", reprocess)
    normal = await backend.book_ingest(
        command="reprocess",
        principal=principal,
        file=None,
        ingestion_id=None,
        cursor=None,
        payload=None,
        document_id=doc,
    )
    forced = await backend.book_ingest(
        command="reprocess",
        principal=principal,
        file=None,
        ingestion_id=None,
        cursor=None,
        payload={"duplicate_policy": "new_revision"},
        document_id=doc,
    )
    assert normal.document_id == forced.document_id == doc
    assert calls == [(actor, doc, False), (actor, doc, True)]


@pytest.mark.asyncio
async def test_reprocess_can_recover_from_byte_identical_attachment(backend, monkeypatch):
    """Optional source recovery preserves document ID, no new import identity."""
    from regional_knowledge.contracts import BookIngestOutput, ChatFile

    actor, doc = str(uuid4()), str(uuid4())
    principal = Principal(subject=actor, client_id="test", issuer="test",
                          access_token="test-token")
    identical_file = ChatFile(
        file_id="attached-existing-source",
        download_url="https://files.example.test/original.pdf",
        mime_type="application/pdf",
        file_name="original.pdf",
    )
    seen = []

    async def reprocess(*, principal, document_id, force_new_revision=False,
                        verified_file=None, derived_only=False):
        seen.append((document_id, force_new_revision, verified_file, derived_only))
        return BookIngestOutput(
            ingestion_id="existing-document-revision",
            document_id=document_id,
            state="staged",
            message="verified original source",
            next_action="continue_pages",
        )

    monkeypatch.setattr(backend, "_reprocess_existing_source", reprocess)
    result = await backend.book_ingest(
        command="reprocess", principal=principal, file=identical_file,
        ingestion_id=None, cursor=None, payload=None, document_id=doc,
    )
    assert result.document_id == doc
    assert seen == [(doc, False, identical_file, False)]
    with pytest.raises(ValueError, match="rechunk must not attach"):
        await backend.book_ingest(
            command="rechunk", principal=principal, file=identical_file,
            ingestion_id=None, cursor=None, payload=None, document_id=doc,
        )
    assert len(seen) == 1  # rechunk failure did not start another revision


