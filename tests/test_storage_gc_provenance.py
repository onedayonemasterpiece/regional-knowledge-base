"""PostgreSQL acceptance: GC must not destroy validated finalized source graphs.

Uses session-local temporary tables in the CI PostgreSQL fixture only. No
production secrets, user documents, live storage, or permanent DB mutations.
"""
import os
from uuid import uuid4

import psycopg
import pytest

from regional_knowledge.storage_gc import CANDIDATES


def test_retains_finalized_source_proof_while_collecting_orphans():
    dsn=os.environ.get("RKB_GRAPH_TEST_DSN")
    if not dsn:
        pytest.skip("isolated PostgreSQL fixture not configured")
    # Only the dedicated test service may run this fixture.
    assert "127.0.0.1:54329/rkb_graph_test" in dsn
    doc=uuid4()
    objects={name:uuid4() for name in ("finalized","processing","failed","orphan")}
    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cur:
            cur.execute("""create temp table rkb_documents(
                id uuid, source_archive_status text, source_archive_ref text,
                source_sha256 text)""")
            cur.execute("""create temp table rkb_objects(
                id uuid,document_id uuid,kind text,deleted_at timestamptz,
                created_at timestamptz,size_bytes bigint,sha256 text)""")
            cur.execute("""create temp table rkb_ingestion_jobs(
                id uuid,document_id uuid,staged_graph_object_id uuid,state text)""")
            cur.execute("create temp table rkb_chunks(text_object_id uuid,source_text text)")
            cur.execute("create temp table rkb_pages(id uuid,document_id uuid)")
            cur.execute("create temp table rkb_regions(page_id uuid,text_sha256 text,source_text text)")
            cur.execute("create temp table rkb_illustrations(crop_object_id uuid,vibepublish_entry_ref text)")
            cur.execute("insert into rkb_documents values(%s,'verified','archive',%s)",
                        (doc,"a"*64))
            for name,oid in objects.items():
                cur.execute("""insert into rkb_objects
                    (id,document_id,kind,deleted_at,created_at,size_bytes,sha256)
                    values(%s,%s,'document_graph',null,now()-interval '2 hours',1024,%s)""",
                    (oid,doc,"b"*64))
                if name!="orphan":
                    cur.execute("""insert into rkb_ingestion_jobs
                        (id,document_id,staged_graph_object_id,state)
                        values(%s,%s,%s,%s)""",(uuid4(),doc,oid,name))
            cur.execute(CANDIDATES)
            collected={row[0] for row in cur.fetchall()}
    assert collected=={objects["failed"],objects["orphan"]}
