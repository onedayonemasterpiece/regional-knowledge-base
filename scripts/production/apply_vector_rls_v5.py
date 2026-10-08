"""Apply or roll back vector-only RLS v5 without touching corpus/vector rows.

The SQL is idempotent and transactional. The default runtime stays on v4 until
an explicit shadow comparison authorizes v5 via RKB_VECTOR_CANDIDATE_VERSION.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path

from operator_env import load_service_env
from regional_knowledge.vector_plane import RemoteVectorClient

ROOT=Path(__file__).resolve().parents[2]
INSTALL=ROOT/"sql/023_vector_rls_v5.sql"
ROLLBACK=ROOT/"sql/023_vector_rls_v5.rollback.sql"

async def apply(*,rollback:bool) -> None:
    load_service_env()
    if rollback and os.getenv("RKB_VECTOR_CANDIDATE_VERSION","v4")=="v5":
        raise RuntimeError("switch runtime candidate version back to v4 before RLS rollback")
    client=RemoteVectorClient(os.environ["KB_SUPABASE_SESSION_CONNECTION"],min_size=0,max_size=1)
    try:
        selected=ROLLBACK if rollback else INSTALL
        source=selected.read_text().strip()
        if not source.startswith("begin;") or not source.endswith("commit;"):
            raise ValueError("RLS migration must have an explicit transaction boundary")
        statements=source.removeprefix("begin;").removesuffix("commit;").strip()
        async with client._connection({"x-rkb-service":"1"}) as db:
            await db.execute(statements)
            rows=await(await db.execute("""
                select tablename,policyname,qual
                  from pg_catalog.pg_policies
                 where schemaname='public'
                   and tablename in (
                     'rkb_vector_items','rkb_chunk_embeddings_e5',
                     'rkb_chunk_embeddings_bge'
                   )
                   and policyname in ('rkb_vector_items_read','vector_read')
                 order by tablename
            """)).fetchall()
            policy={row["tablename"]:row["qual"] for row in rows}
            if len(policy)!=3:
                raise RuntimeError("missing independent vector RLS policies")
            if rollback:
                if "rkb_vector_scope" not in policy["rkb_vector_items"]:
                    raise RuntimeError("anchor RLS rollback mismatched")
            else:
                if "rkb_vector_readable_documents" not in policy["rkb_vector_items"]:
                    raise RuntimeError("anchor statement-scope policy not installed")
                for table in ("rkb_chunk_embeddings_e5","rkb_chunk_embeddings_bge"):
                    if "rkb_vector_items" not in policy[table]:
                        raise RuntimeError("embedding membership RLS policy not installed")
            rpc=await(await db.execute("""
                select p.prosecdef as definer
                  from pg_catalog.pg_proc p
                 where p.oid=to_regprocedure(
                   'public.rkb_vector_candidates_v5(text,text,text,text,integer)'
                 )
            """)).fetchone()
            if not rollback and (rpc is None or rpc["definer"]):
                raise RuntimeError("v5 must be installed with SECURITY INVOKER")
            enabled=await(await db.execute("""
                select relname,relrowsecurity from pg_catalog.pg_class
                where oid in (
                 'public.rkb_vector_items'::regclass,
                 'public.rkb_chunk_embeddings_e5'::regclass,
                 'public.rkb_chunk_embeddings_bge'::regclass
                )
            """)).fetchall()
            if len(enabled)!=3 or any(not row["relrowsecurity"] for row in enabled):
                raise RuntimeError("vector RLS disabled unexpectedly")
        print(json.dumps({
            "phase":"rollback" if rollback else "install",
            "v5_invoker_verified":not rollback,
            "rls_tables_verified":3,
            "policy_checks":"pass",
        }))
    finally:
        await client.aclose()

def main() -> None:
    parser=argparse.ArgumentParser()
    parser.add_argument("--rollback",action="store_true")
    args=parser.parse_args()
    asyncio.run(apply(rollback=args.rollback))

if __name__=="__main__":
    main()
