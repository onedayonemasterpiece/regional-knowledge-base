"""Read-only live two-book retrieval follow-up with explicit latency numbers.

Does not ingest, modify, delete or re-embed the corpus. Uses the same production
backend and owner ACL as the public MCP service. Run in the registered project
environment with PYTHONPATH=src:scripts/production.
"""
from __future__ import annotations
import argparse
import asyncio
import json
import hashlib
from uuid import UUID
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

def load_fixture(path: Path):
    """Load source-grounded acceptance cases outside the public Git checkout."""
    source=path.resolve()
    git_root=Path(__file__).resolve().parents[2]
    if source==git_root or git_root in source.parents:
        raise ValueError("private retrieval fixture must live outside the public repository")
    raw=source.read_bytes()
    fixture=json.loads(raw)
    if not isinstance(fixture,dict) or fixture.get("version")!=1:
        raise ValueError("unsupported private retrieval fixture version")
    def query(value):
        if not isinstance(value,str) or not 1<=len(value.strip())<=1024:
            raise ValueError("invalid query")
        return value.strip()
    def document(value):
        return str(UUID(str(value)))
    positives=[
        (query(row["query"]),document(row["document_id"]))
        for row in fixture["positives"]
    ]
    negatives=[query(value) for value in fixture["negative_identifiers"]]
    scoped=[
        (query(row["query"]),[document(value) for value in row["document_ids"]])
        for row in fixture["scoped"]
    ]
    if not 1<=len(positives)<=300 or len(negatives)>100 or len(scoped)>100:
        raise ValueError("retrieval fixture outside bounded acceptance size")
    if any(not 1<=len(documents)<=20 or len(documents)!=len(set(documents))
           for _,documents in scoped):
        raise ValueError("invalid scoped fixture")
    unauthorized=document(fixture["unauthorized_doc_id"])
    return positives,negatives,scoped,unauthorized,hashlib.sha256(raw).hexdigest()

def percentile(data,p):
    data=sorted(data)
    if not data:return None
    x=(len(data)-1)*p
    lo=int(x);hi=min(lo+1,len(data)-1)
    return round(data[lo]+(data[hi]-data[lo])*(x-lo),2)

async def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--cases",type=Path,required=True,help="Private fixture JSON located outside the public repository")
    parser.add_argument("--output",type=Path)
    parser.add_argument("--strict",action="store_true")
    parser.add_argument("--concurrency",type=int,default=1,help="Maximum concurrent positive retrievals, 1-8")
    args=parser.parse_args()
    if not 1<=args.concurrency<=8:parser.error("concurrency must be 1..8")
    positives,negatives,scoped,unauthorized,fixture_sha256=load_fixture(args.cases)
    load_service_env()
    backend=backend_from_env()
    actor=IndexReconciler(backend,BgeQueue(os.environ["RKB_BGE_QUEUE_PATH"])).actor(os.environ["RKB_OWNER_SUBJECT"])
    checks=[]
    try:
        # A readiness request avoids measuring the first model warm-up as a warm-path result.
        await backend.search(positives[0][0],actor,match_count=8)
        semaphore=asyncio.Semaphore(args.concurrency)
        async def check_positive(query,expected):
            async with semaphore:
                result=await backend.search(query,actor,match_count=8)
                top=result.results[0] if result.results else None
                doc=(backend.corpus.one("rkb_chunks",top.id) or {}).get("document_id") if top else None
                return {
                    "query":query,"type":"positive","expected_doc":expected,
                    "actual_top1_doc":doc,"top1_title":top.title if top else None,
                    "mode":result.retrieval_mode,"state":result.main_state,
                    "latency_ms":result.latency_ms,
                    "pass":bool(result.main_state=="ready" and result.retrieval_mode=="bge"
                                and doc==expected and top and top.ranking_signals),
                }
        checks.extend(await asyncio.gather(
            *(check_positive(query,expected) for query,expected in positives)
        ))
        for query in negatives:
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
        for query,scope in scoped:
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
            await backend.search(positives[0][0],actor,document_ids=[unauthorized])
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
            "fixture_sha256":fixture_sha256,
            "concurrency":args.concurrency,
            "positive_cases":len(positives),
            "scoped_cases":len(scoped),
            "negative_identifier_cases":len(negatives),
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
