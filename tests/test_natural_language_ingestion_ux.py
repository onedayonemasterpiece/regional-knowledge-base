import hashlib
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID, uuid4

import psycopg
import pytest
from psycopg.types.json import Jsonb

from test_graph_postgres import fixture, graph_db
from regional_knowledge.postgres_backend import PostgresBackend
from regional_knowledge.supabase_backend import LexicalOnlyEmbedder


class _UnusedStore:
    async def download_file(self, *args, **kwargs):
        raise AssertionError("reprocess must prove the verified archive path")


class _OnePageProcessor:
    async def inspect_file(self, path):
        assert Path(path).exists()
        return SimpleNamespace(page_count=1, title=None)


@pytest.mark.asyncio
async def test_book_find_and_archived_reprocess_are_acl_safe_idempotent_and_same_document(
    graph_db,
    tmp_path,
    monkeypatch,
):
    doc, owner, other, _, _, _ = fixture(graph_db)
    archived = b"%PDF-1.7\narchived-gause-control"
    sha = hashlib.sha256(archived).hexdigest()
    source_object = uuid4()

    with psycopg.connect(graph_db, autocommit=True) as db:
        db.execute(
            """
            update rkb_documents
               set title=%s,
                   authors=%s,
                   source_sha256=%s,
                   source_format='pdf',
                   source_filename='gause.pdf',
                   source_archive_status='verified',
                   source_archive_ref='entry_gause_control',
                   page_count=1
             where id=%s
            """,
            (
                "Кёнигсберг. История города",
                Jsonb(["Фриц Гаузе"]),
                sha,
                doc,
            ),
        )
        db.execute(
            """
            insert into rkb_objects(
                id,document_id,kind,object_key,sha256,mime_type,size_bytes,deleted_at
            ) values(%s,%s,'source_pdf','gc/source.pdf',%s,'application/pdf',%s,now())
            """,
            (source_object, doc, sha, len(archived)),
        )

    b = PostgresBackend(
        graph_db,
        embedder=LexicalOnlyEmbedder(),
        object_store=_UnusedStore(),
    )
    b.pdf_processor = _OnePageProcessor()
    monkeypatch.setenv("RKB_WORK_DIR", str(tmp_path))
    archive_reads = []

    async def archived_download(
        backend,
        principal,
        document_id,
        obj,
        path,
        *,
        require_archive=False,
    ):
        assert backend is b
        assert principal.subject == owner.subject
        assert document_id == str(doc)
        assert obj["sha256"] == sha
        assert require_archive is True
        archive_reads.append(document_id)
        Path(path).write_bytes(archived)

    monkeypatch.setattr(
        "regional_knowledge.source_archive.download_source",
        archived_download,
    )

    try:
        found = await b.book_find("Гаузе", owner)
        assert [item.document_id for item in found.results] == [str(doc)]
        assert found.results[0].authors == ["Фриц Гаузе"]
        assert (await b.book_find("Гаузе", other)).results == []

        with psycopg.connect(graph_db, autocommit=True) as db:
            db.execute(
                "insert into rkb_document_grants(document_id,grantee_user_id,role) values(%s,%s,'viewer')",
                (doc, UUID(other.subject)),
            )
        assert (await b.book_find("Гаузе", other)).results[0].document_id == str(doc)
        with pytest.raises(PermissionError, match="book_reprocess_owner_required"):
            await b.book_ingest(
                command="reprocess",
                principal=other,
                file=None,
                ingestion_id=None,
                cursor=None,
                payload=None,
                document_id=str(doc),
            )

        first = await b.book_ingest(
            command="reprocess",
            principal=owner,
            file=None,
            ingestion_id=None,
            cursor=None,
            payload=None,
            document_id=str(doc),
        )
        assert first.document_id == str(doc)
        assert first.state == "staged"
        assert first.next_action == "continue_pages"
        assert first.source_archive_status == "verified"
        assert archive_reads == [str(doc)]

        replay = await b.book_ingest(
            command="reprocess",
            principal=owner,
            file=None,
            ingestion_id=None,
            cursor=None,
            payload=None,
            document_id=str(doc),
        )
        assert replay.ingestion_id == first.ingestion_id
        assert replay.document_id == first.document_id
        assert archive_reads == [str(doc)]

        status = await b.book_ingest(
            command="status",
            principal=owner,
            file=None,
            ingestion_id=first.ingestion_id,
            cursor=None,
            payload=None,
        )
        assert status.next_action == "continue_pages"

        with psycopg.connect(graph_db) as db:
            job = db.execute(
                "select document_id,staged_revision,duplicate_policy,source_object_id from rkb_ingestion_jobs where id=%s",
                (UUID(first.ingestion_id),),
            ).fetchone()
            assert job == (doc, 2, "new_revision", source_object)
            assert db.execute(
                "select count(*) from rkb_documents where owner_user_id=%s",
                (UUID(owner.subject),),
            ).fetchone()[0] == 1

        with psycopg.connect(graph_db, autocommit=True) as db:
            db.execute(
                "update rkb_documents set source_archive_status='pending',source_archive_ref=null where id=%s",
                (doc,),
            )
        with pytest.raises(RuntimeError, match="attach the original PDF/DjVu again"):
            await b.book_ingest(
                command="reprocess",
                principal=owner,
                file=None,
                ingestion_id=None,
                cursor=None,
                payload=None,
                document_id=str(doc),
            )
    finally:
        await b.aclose()


def test_ingestion_next_action_is_model_actionable():
    output = PostgresBackend._ingestion_output(
        {"id": str(uuid4()), "document_id": str(uuid4()), "state": "ready", "cursor": None},
        "ready",
    )
    assert output.next_action == "finalize"
    output = PostgresBackend._ingestion_output(
        {"id": str(uuid4()), "document_id": str(uuid4()), "state": "staged", "cursor": None},
        "complete",
    )
    assert output.next_action == "validate"
    output = PostgresBackend._ingestion_output(
        {"id": str(uuid4()), "document_id": str(uuid4()), "state": "failed", "cursor": None},
        "failed",
    )
    assert output.next_action == "blocker"
