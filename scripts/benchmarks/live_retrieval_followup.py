"""Read-only live two-book retrieval follow-up with explicit latency numbers.

Does not ingest, modify, delete or re-embed the corpus. Uses the same production
backend and owner ACL as the public MCP service. Run in the registered project
environment with PYTHONPATH=src:scripts/production.
"""
from __future__ import annotations
import argparse
import asyncio
import json
import os
import statistics
import sys
from pathlib import Path

sys.path[:0]=[str(Path(__file__).resolve().parents[2]/"src"),
              str(Path(__file__).resolve().parents[1]/"production")]
from operator_env import load_service_env
from regional_knowledge.supabase_backend import backend_from_env
from regional_knowledge.bge_queue import BgeQueue
from regional_knowledge.indexing import IndexReconciler

GAUSE="7ce738b0-d3d3-4fc2-9a61-aa58b537a0e9"
BRUNNECK="432bbc6a-ec04-4936-aed9-950f67f25230"
POSITIVES=[
    ("kulmisches Recht",BRUNNECK),
    ("Kulmer Handfeste",BRUNNECK),
    ("flämisches Erbe",BRUNNECK),
    ("iure Culmensi",BRUNNECK),
    ("Reiterdienst Recognitionszins",BRUNNECK),
    ("kölmischen Güter Allodisation",BRUNNECK),
    ("1233 Kulmer Handfeste",BRUNNECK),
    ("Landrecht von 1620",BRUNNECK),
    ("Кёнигсберг основание крепости",GAUSE),
    ("Твангсте",GAUSE),
    ("Twanste",GAUSE),
    ("Twangste",GAUSE),
    ("Альтштадт Лёбенихт Кнайпхоф",GAUSE),
    ("Оттокар",GAUSE),
    ("Mont Royal Königliche Berg",GAUSE),
    ("Геркус Монте",GAUSE),
]
NEGATIVES=("xyzzy12345","NoSuchBook999999")
SCOPED=[
    ("Оттокар",[GAUSE]),
    ("Kulmer Handfeste",[BRUNNECK]),
    ("iure Culmensi",[BRUNNECK]),
]

def percentile(data,p):
    data=sorted(data)
    if not data:return None
    x=(len(data)-1)*p
    lo=int(x);hi=min(lo+1,len(data)-1)
    return round(data[lo]+(data[hi]-data[lo])*(x-lo),2)

async def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--output",type=Path)
    parser.add_argument("--strict",action="store_true")
    args=parser.parse_args()
    load_service_env()
    backend=backend_from_env()
    actor=IndexReconciler(backend,BgeQueue(os.environ["RKB_BGE_QUEUE_PATH"])).actor(os.environ["RKB_OWNER_SUBJECT"])
    checks=[]
    try:
        # A readiness request avoids measuring the first model warm-up as a warm-path result.
        await backend.search("Твангсте",actor,match_count=8)
        for query,expected in POSITIVES:
            result=await backend.search(query,actor,match_count=8)
            top=result.results[0] if result.results else None
            doc=(backend.corpus.one("rkb_chunks",top.id) or {}).get("document_id") if top else None
            checks.append({
                "query":query,"type":"positive","expected_doc":expected,
                "actual_top1_doc":doc,"top1_title":top.title if top else None,
                "mode":result.retrieval_mode,"state":result.main_state,
                "latency_ms":result.latency_ms,
                "pass":bool(result.main_state=="ready" and result.retrieval_mode=="bge"
                            and doc==expected and top and top.ranking_signals),
            })
        for query in NEGATIVES:
            result=await backend.search(query,actor,match_count=8)
            checks.append({
                "query":query,"type":"negative_identifier",
                "mode":result.retrieval_mode,"state":result.main_state,
                "policy":result.retrieval_policy,
                "latency_ms":result.latency_ms,
                "count":len(result.results),
                "pass":bool(result.main_state=="ready" and not result.results
                            and result.retrieval_policy=="exact_identifier"),
            })
        for query,scope in SCOPED:
            result=await backend.search(query,actor,match_count=8,document_ids=scope)
            found=set()
            for item in result.results:
                row=backend.corpus.one("rkb_chunks",item.id)
                if row:found.add(row["document_id"])
            checks.append({
                "query":query,"type":"scoped","scope":scope,
                "mode":result.retrieval_mode,"state":result.main_state,
                "latency_ms":result.latency_ms,
                "found_docs":sorted(found),
                "pass":bool(result.main_state=="ready" and result.retrieval_mode=="bge"
                            and result.results and found.issubset(set(scope))),
            })
        try:
            await backend.search("Оттокар",actor,document_ids=["00000000-0000-4000-8000-000000000000"])
        except PermissionError:
            checks.append({"type":"unauthorized_scope","pass":True})
        else:
            checks.append({"type":"unauthorized_scope","pass":False})

        timings=[r["latency_ms"] for r in checks if r.get("latency_ms") is not None
                 and r.get("type") in ("positive","scoped")]
        quality=all(r["pass"] for r in checks)
        latency=bool(timings and percentile(timings,.95)<=1000 and max(timings)<=2000)
        report={
            "version":"rkb.retrieval_followup.v1",
            "measurement":"backend latency_ms (includes continuation; excludes MCP transport)",
            "corpus":"real active books and other authorized corpus, no synthetic search controls",
            "required_bge":"production policy",
            "positive_cases":len(POSITIVES),
            "scoped_cases":len(SCOPED),
            "negative_identifier_cases":len(NEGATIVES),
            "quality_pass":quality,
            "latency_gate":{"warm_backend_p95_ms_max":1000,"hard_per_request_ms_max":2000,
                            "observed_p50_ms":percentile(timings,.5),
                            "observed_p95_ms":percentile(timings,.95),
                            "observed_max_ms":max(timings) if timings else None,
                            "pass":latency},
            "all_pass":quality and latency,
            "checks":checks,
        }
        rendered=json.dumps(report,ensure_ascii=False,indent=2)
        if args.output:
            args.output.parent.mkdir(parents=True,exist_ok=True)
            args.output.write_text(rendered+"\n")
        print(rendered)
        if args.strict and not report["all_pass"]:raise SystemExit(1)
    finally:
        await backend.aclose()

if __name__=="__main__":asyncio.run(main())
