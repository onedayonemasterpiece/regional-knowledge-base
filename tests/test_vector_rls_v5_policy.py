"""Policy and dispatch contracts for independent PostgreSQL vector RLS v5."""
from pathlib import Path
from contextlib import asynccontextmanager
from uuid import uuid4
import pytest

from regional_knowledge.vector_plane import RemoteVectorClient

ROOT=Path(__file__).resolve().parents[1]


def test_v5_invoker_and_policies_keep_independent_actor_scope():
    sql=(ROOT/"sql/023_vector_rls_v5.sql").read_text()
    assert "language plpgsql stable security invoker" in sql
    v5=sql.split("create or replace function public.rkb_vector_candidates_v5(",1)[1]
    assert "language sql stable security invoker" in v5
    assert "language plpgsql stable security definer" not in v5
    assert "rkb_vector_validate_scope_v5" in sql
    assert "rkb_current_actor_id()" in sql
    assert "rkb_vector_readable_documents" in sql
    assert "document_id = any(array(select pg_catalog.unnest(public.rkb_vector_readable_documents())))" in sql
    for table in ("rkb_chunk_embeddings_e5","rkb_chunk_embeddings_bge"):
        assert f"create policy vector_read on public.{table}" in sql
    assert sql.count("chunk_id in (select a.chunk_id from public.rkb_vector_items a)") == 2
    assert "alter table" not in sql.lower() or "disable row level security" not in sql.lower()
    assert "vector document/revision scope mismatch" in sql
    assert "grant execute on function public.rkb_vector_candidates_v5" in sql


def test_v5_rollback_restores_individual_rls_without_data_changes():
    sql=(ROOT/"sql/023_vector_rls_v5.rollback.sql").read_text()
    assert "public.rkb_vector_scope(document_id)" in sql
    assert sql.count("public.rkb_vector_scope(a.document_id)") == 2
    assert "drop function if exists public.rkb_vector_candidates_v5" in sql
    assert "truncate" not in sql.lower()
    assert "delete from" not in sql.lower()
    assert "drop table" not in sql.lower()


@pytest.mark.asyncio
async def test_candidate_version_allowlist_and_authorized_scope(monkeypatch):
    class Database:
        def __init__(self):
            self.query=None
            self.params=None
        async def execute(self,query,params):
            self.query=query
            self.params=params
            return self
        async def fetchall(self):
            return [{"chunk_id":uuid4()}]

    db=Database()
    captured=[]
    @asynccontextmanager
    async def fake_conn(headers):
        captured.append(headers)
        yield db

    client=RemoteVectorClient.__new__(RemoteVectorClient)
    monkeypatch.setattr(client,"_connection",fake_conn)
    doc=str(uuid4())
    actor=str(uuid4())
    for version in ("v4","v5"):
        monkeypatch.setenv("RKB_VECTOR_CANDIDATE_VERSION",version)
        results=await client.candidates(actor,{doc:2},None,None,None,None,10)
        assert len(results)==1
        assert "rkb_vector_candidates_"+version in db.query
        assert captured[-1]["x-rkb-vector-revisions"].startswith("{")
        assert db.params[-1]==10
    monkeypatch.setenv("RKB_VECTOR_CANDIDATE_VERSION","security_definer")
    with pytest.raises(ValueError,match="unsupported"):
        await client.candidates(actor,{doc:2},None,None,None,None,10)
    assert len(captured)==2
    # Empty scope never even opens a vector connection.
    assert await client.candidates(actor,{},None,None,None,None,10)==[]
