"""Real PostgreSQL RLS v5 acceptance in an isolated rollback-safe schema.

CI supplies a disposable pgvector PostgreSQL service, never the production
Supabase database. Actor, scope and revision tests are synthetic. These tests
do not equate user-provided GUCs to a trusted client: the application server
must derive and validate them against SQLite ACL before opening the DB context.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from uuid import uuid4

import pytest
import psycopg
from psycopg.rows import dict_row

from regional_knowledge.bge_contract import SPACE as BGE, REVISION
from regional_knowledge.e5_contract import SPACE as E5

ROOT=Path(__file__).resolve().parents[1]
CI_DSN="postgresql://postgres:graph_fixture_only@127.0.0.1:54329/rkb_graph_test"
TABLES=("rkb_vector_items","rkb_chunk_embeddings_e5","rkb_chunk_embeddings_bge")


def sql_for_schema(sql_path: str, schema: str) -> str:
    source=(ROOT/sql_path).read_text()
    assert source.startswith("begin;") and source.strip().endswith("commit;")
    for name in (
        "rkb_vector_readable_documents",
        "rkb_vector_candidates_v5",
        "rkb_vector_validate_scope_v5",
        "rkb_vector_candidates_v4",
        "rkb_current_actor_id",
        *TABLES,
    ):
        source=source.replace("public."+name, schema+"."+name)
    return source.removeprefix("begin;").strip().removesuffix("commit;").strip()


def vector(size: int, x: float, y: float) -> str:
    return "["+",".join(str(z) for z in [x,y]+[0.0]*(size-2))+"]"


@pytest.mark.skipif(not os.getenv("RKB_GRAPH_TEST_DSN"),reason="isolated pgvector PostgreSQL not provisioned")
def test_rls_v5_matches_v4_and_rejects_unauthorized_context():
    assert os.environ["RKB_GRAPH_TEST_DSN"]==CI_DSN, "refusing non-CI PostgreSQL"
    schema="rkb_v5_test_"+uuid4().hex[:12]
    owner,viewer,unknown=uuid4(),uuid4(),uuid4()
    doc_a,doc_b=uuid4(),uuid4()
    a1,a2,b1=uuid4(),uuid4(),uuid4()
    sha1,sha2="a"*64,"b"*64
    with psycopg.connect(CI_DSN,autocommit=True,row_factory=dict_row) as db:
        try:
            db.execute("create extension if not exists vector")
            db.execute("do $$begin if not exists(select 1 from pg_roles where rolname='rkb_app') then create role rkb_app;end if;end$$")
            db.execute("create schema "+schema)
            db.execute("create table "+schema+".actors(id uuid primary key)")
            db.execute("insert into "+schema+".actors(id) values(%s),(%s)",(owner,viewer))
            db.execute(
                "create function "+schema+".rkb_current_actor_id() returns uuid "
                "language sql stable security definer set search_path='' as "
                "$actor$ select id from "+schema+".actors where id="
                "nullif(pg_catalog.current_setting('rkb.actor_id',true),'')::uuid $actor$"
            )
            db.execute(
                "create table "+schema+".rkb_vector_items("
                "chunk_id uuid primary key,document_id uuid not null,revision bigint not null,"
                "text_sha256 text not null,search_material_sha256 text not null)"
            )
            for kind,width in (("e5",384),("bge",1024)):
                db.execute(
                    "create table "+schema+".rkb_chunk_embeddings_"+kind+"("
                    "chunk_id uuid primary key,embedding_space text not null,"
                    "model_revision text,revision bigint not null,text_sha256 text not null,"
                    "search_material_sha256 text not null,embedding public.vector("
                    +str(width)+"))"
                )
            for table in TABLES:
                db.execute("alter table "+schema+"."+table+" enable row level security")
                db.execute("grant select on "+schema+"."+table+" to rkb_app")
            db.execute("grant usage on schema "+schema+" to rkb_app")
            db.execute(sql_for_schema("sql/022_vector_candidate_hotpath.sql",schema))
            db.execute(sql_for_schema("sql/023_vector_rls_v5.sql",schema))
            for item,doc,revision,bx,by in (
                (a1,doc_a,1,1,0),(a2,doc_a,2,0,1),(b1,doc_b,1,-1,0),
            ):
                db.execute(
                    "insert into "+schema+".rkb_vector_items values(%s,%s,%s,%s,%s)",
                    (item,doc,revision,sha1,sha2),
                )
                for kind,width,space in (("e5",384,E5),("bge",1024,BGE)):
                    db.execute(
                        "insert into "+schema+".rkb_chunk_embeddings_"+kind
                        +"(chunk_id,embedding_space,model_revision,revision,text_sha256,"
                        "search_material_sha256,embedding) values(%s,%s,%s,%s,%s,%s,%s::vector)",
                        (item,space,REVISION if kind=="bge" else None,revision,
                         sha1,sha2,vector(width,bx,by)),
                    )
            def compare(actor,documents,revisions,*,kind):
                size=384 if kind=="e5" else 1024
                args=(vector(size,1,0),E5,None,None,10) if kind=="e5" else (
                    None,None,vector(size,1,0),BGE,10
                )
                with db.transaction():
                    db.execute("set local role rkb_app")
                    db.execute(
                        "select set_config('rkb.actor_id',%s,true),"
                        "set_config('rkb.vector_documents',%s,true),"
                        "set_config('rkb.vector_revisions',%s,true)",
                        (str(actor),",".join(str(v) for v in documents),json.dumps(revisions)),
                    )
                    role=db.execute("select current_user as name").fetchone()["name"]
                    assert role=="rkb_app"
                    old=db.execute(
                        "select * from "+schema+".rkb_vector_candidates_v4(%s,%s,%s,%s,%s)",
                        args,
                    ).fetchall()
                    new=db.execute(
                        "select * from "+schema+".rkb_vector_candidates_v5(%s,%s,%s,%s,%s)",
                        args,
                    ).fetchall()
                    assert new==old, "v5 changed candidate IDs/order/hashes"
                    return [row["chunk_id"] for row in new]

            for kind in ("bge","e5"):
                assert compare(owner,[doc_a],{str(doc_a):1},kind=kind)==[a1]
                assert compare(viewer,[doc_b],{str(doc_b):1},kind=kind)==[b1]
                assert compare(owner,[doc_a,doc_b],{str(doc_a):1,str(doc_b):1},kind=kind)==[a1,b1]
                assert compare(owner,[doc_a],{str(doc_a):2},kind=kind)==[a2]
                assert compare(owner,[doc_b],{str(doc_b):2},kind=kind)==[]
            # Missing/unknown actor fails both the v5 routine and the RLS
            # policy. The DB does not independently determine SQLite grants.
            for actor in (None,unknown):
                with db.transaction():
                    db.execute("set local role rkb_app")
                    db.execute(
                        "select set_config('rkb.actor_id',%s,true),"
                        "set_config('rkb.vector_documents',%s,true),"
                        "set_config('rkb.vector_revisions',%s,true)",
                        (str(actor) if actor else "",str(doc_a),json.dumps({str(doc_a):1})),
                    )
                    for table in TABLES:
                        assert db.execute("select count(*) n from "+schema+"."+table).fetchone()["n"]==0
                    with pytest.raises(psycopg.Error):
                        with db.transaction():
                            db.execute("select * from "+schema+".rkb_vector_candidates_v5(%s,%s,%s,%s,%s)",
                                       (None,None,vector(1024,1,0),BGE,5)).fetchall()
            # Actor is registered, but a mismatched document/revision
            # transaction scope must be rejected before candidate retrieval.
            with db.transaction():
                db.execute("set local role rkb_app")
                db.execute("select set_config('rkb.actor_id',%s,true),"
                           "set_config('rkb.vector_documents',%s,true),"
                           "set_config('rkb.vector_revisions',%s,true)",
                           (str(owner),str(doc_b),json.dumps({str(doc_a):1})))
                with pytest.raises(psycopg.Error):
                    with db.transaction():
                        db.execute("select * from "+schema+".rkb_vector_candidates_v5(%s,%s,%s,%s,%s)",
                                   (None,None,vector(1024,1,0),BGE,5))
            policies=db.execute(
                "select tablename,policyname,qual from pg_policies where schemaname=%s",
                (schema,),
            ).fetchall()
            assert len(policies)==3
            acl={r["tablename"]:r["qual"] for r in policies}
            assert "rkb_vector_readable_documents" in acl["rkb_vector_items"]
            assert all("rkb_vector_items" in acl[name] for name in TABLES[1:])
            routine=db.execute(
                "select p.prosecdef as definer from pg_proc p "
                "where p.oid=to_regprocedure(%s)",
                (schema+".rkb_vector_candidates_v5(text,text,text,text,integer)",),
            ).fetchone()
            assert routine=={"definer":False}
            # Rollback restores v4 policy paths without dropping data.
            db.execute(sql_for_schema("sql/023_vector_rls_v5.rollback.sql",schema))
            remaining=db.execute(
                "select count(*) n from pg_policies where schemaname=%s",(schema,)
            ).fetchone()["n"]
            assert remaining==3
            assert db.execute(
                "select to_regprocedure(%s) is null as gone",
                (schema+".rkb_vector_candidates_v5(text,text,text,text,integer)",),
            ).fetchone()["gone"]
        finally:
            db.execute("drop schema if exists "+schema+" cascade")
