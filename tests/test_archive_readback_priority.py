"""Archive selection must prioritize already verified provider readbacks."""
from datetime import datetime, timedelta, timezone
import sqlite3
from uuid import uuid4

from regional_knowledge.source_archive import _PENDING_SOURCES_SQL
from regional_knowledge.sqlite_data import translate


def test_pending_source_queue_prioritizes_provider_readbacks_without_duplicates():
    conn=sqlite3.connect(":memory:")
    conn.row_factory=sqlite3.Row
    try:
        conn.execute("""CREATE TABLE rkb_documents (
            id TEXT PRIMARY KEY, owner_user_id TEXT, source_sha256 TEXT,
            source_archive_status TEXT, source_archive_attempt_at TEXT,
            source_archive_error TEXT, source_archive_read_id TEXT,
            source_archive_operation_id TEXT)""")
        conn.execute("""CREATE TABLE rkb_objects (
            id TEXT PRIMARY KEY, document_id TEXT, kind TEXT,
            sha256 TEXT, deleted_at TEXT, object_key TEXT, mime_type TEXT)""")
        owner=str(uuid4())
        attempted=(datetime.now(timezone.utc)-timedelta(days=2)).isoformat()
        for n in range(8):
            ident=str(uuid4())
            digest=("%064x"%n)
            readback="op_read_"+str(n) if n>=4 else None
            sent="op_put_"+str(n) if n>=2 else None
            conn.execute(
                "INSERT INTO rkb_documents VALUES(?,?,?,?,?,?,?,?)",
                (ident,owner,digest,"pending",attempted,
                 "HTTPStatusError",readback,sent))
            conn.execute(
                "INSERT INTO rkb_objects VALUES(?,?,?,?,?,?,?)",
                (str(uuid4()),ident,"source_pdf",digest,None,
                 "private/object", "application/pdf"))
        rows=conn.execute(translate(_PENDING_SOURCES_SQL),(owner,)).fetchall()
        assert len(rows)==4
        assert all(row["source_archive_read_id"] for row in rows)
        assert all(row["source_archive_operation_id"] for row in rows)
        assert len({row["id"] for row in rows})==4
    finally:
        conn.close()
