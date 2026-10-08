"""Read-only production actor-scope v4/v5 candidate parity and timing A/B.

Samples accepted BGE/E5 vectors already stored in the user's authorized
vector plane. No vectors, document IDs, query text or source material are
printed or written. Both versions run as rkb_app under the same transaction
scope, with alternating execution order and exact ranked-row parity.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import statistics
import time
from pathlib import Path

from operator_env import load_service_env
from regional_knowledge.supabase_backend import backend_from_env
from regional_knowledge.indexing import IndexReconciler
from regional_knowledge.bge_queue import BgeQueue
from regional_knowledge.bge_contract import SPACE as BGE
from regional_knowledge.e5_contract import SPACE as E5


def stats(values):
    values=sorted(values)
    if not values:return {"n":0}
    def percentile(x):
        at=(len(values)-1)*x
        lower=int(at)
        high=min(lower+1,len(values)-1)
        return round(1000*(values[lower]+(values[high]-values[lower])*(at-lower)),2)
    return {"n":len(values),"p50_ms":percentile(.5),"p95_ms":percentile(.95),"max_ms":percentile(1)}


def canonical(rows):
    return [
        (
            str(row["chunk_id"]),str(row["branch"]),int(row["rank"]),int(row["revision"]),
            str(row["text_sha256"]),str(row["search_material_sha256"]),
        )
        for row in rows
    ]


async def run(samples_per_space: int):
    load_service_env()
    backend=backend_from_env()
    actor=IndexReconciler(backend,BgeQueue(os.environ["RKB_BGE_QUEUE_PATH"])).actor(
        os.environ["RKB_OWNER_SUBJECT"]
    )
    client=backend.vector_client
    try:
        async with backend.data_client._connection(backend._headers(actor)) as db:
            rows=await(await db.execute(
                "select id,active_revision from rkb_documents"
            )).fetchall()
        scope={
            str(row["id"]):int(row["active_revision"])
            for row in rows
            if int(row["active_revision"] or 0)>0 and backend.search_visible(row["id"])
        }
        if not scope or len(scope)>100:
            raise RuntimeError("unexpected actor-authorized document scope")
        headers={"x-rkb-actor":actor.subject,"x-rkb-vector-revisions":json.dumps(scope)}
        async with client._connection(headers) as db:
            current=(await(await db.execute("select current_user::text name")).fetchone())["name"]
            if current!="rkb_app":raise PermissionError("not in RLS application role")
            installed=await(await db.execute("""
                select to_regprocedure(
                  'public.rkb_vector_candidates_v5(text,text,text,text,integer)'
                )::text as name
            """)).fetchone()
            if not installed or not installed["name"]:raise RuntimeError("v5 not installed")
            samples={}
            for kind,table,space in (
                ("bge","public.rkb_chunk_embeddings_bge",BGE),
                ("e5","public.rkb_chunk_embeddings_e5",E5),
            ):
                rows=await(await db.execute(
                    f"""select e.embedding::text v from {table} e
                        join public.rkb_vector_items a on a.chunk_id=e.chunk_id
                        where a.document_id=any(%s::uuid[]) and
                            a.revision=(%s::jsonb->>a.document_id::text)::bigint
                            and e.revision=a.revision
                            and e.text_sha256=a.text_sha256
                            and e.search_material_sha256=a.search_material_sha256
                            and e.embedding_space=%s
                        order by a.chunk_id limit %s""",
                    (list(scope),json.dumps(scope),space,samples_per_space)
                )).fetchall()
                samples[kind]=[row["v"] for row in rows]
        if not samples["bge"] or not samples["e5"]:
            raise RuntimeError("both BGE and E5 active-vector samples required")
        report={"role":"rkb_app","active_documents":len(scope),"samples":{},
                "candidate_id_rank_sha_parity":True,"timing_ms":{}}
        for kind,queries in samples.items():
            bge=kind=="bge"
            timings={"v4":[],"v5":[]}
            # Two rounds, alternating order, to reduce cache and ordering bias.
            for index,vector in enumerate(queries*2):
                if bge:arguments=(None,None,vector,BGE,100)
                else:arguments=(vector,E5,None,None,100)
                answers={}
                order=("v4","v5") if index%2==0 else ("v5","v4")
                for version in order:
                    async with client._connection(headers) as db:
                        t=time.monotonic()
                        rows=await(await db.execute(
                            f"select * from public.rkb_vector_candidates_{version}(%s,%s,%s,%s,%s)",
                            arguments
                        )).fetchall()
                        timings[version].append(time.monotonic()-t)
                        answers[version]=canonical(rows)
                if answers["v4"]!=answers["v5"]:
                    report["candidate_id_rank_sha_parity"]=False
                    raise AssertionError(f"{kind} v4/v5 candidate mismatch in sample {index}")
            report["samples"][kind]=len(queries)
            report["timing_ms"][kind]={version:stats(times) for version,times in timings.items()}
        return report
    finally:
        await backend.aclose()


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--samples",type=int,default=8)
    parser.add_argument("--output",type=Path)
    args=parser.parse_args()
    if not 1<=args.samples<=32:parser.error("samples must be between 1 and 32")
    report=asyncio.run(run(args.samples))
    rendered=json.dumps(report,indent=2)
    if args.output:
        # Avoid public workspace output for operational details.
        path=args.output.resolve()
        git=Path(__file__).resolve().parents[2]
        if path==git or git in path.parents:
            raise RuntimeError("benchmark report must be outside public checkout")
        path.parent.mkdir(mode=0o700,parents=True,exist_ok=True)
        path.write_text(rendered+"\n")
        path.chmod(0o600)
    print(rendered)


if __name__=="__main__":
    main()
