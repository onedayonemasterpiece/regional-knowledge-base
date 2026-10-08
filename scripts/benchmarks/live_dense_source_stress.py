"""BGE-only live source-proof stress with private frozen questions and vectors.

Run from an exact immutable Regional Knowledge release. The private --lab folder
contains stress-cases.json, negative-cases.json, boundary-buckets.json, and
stress-queries-bge.npy. Does not create documents, edit indexes or re-embed
passages. Output with case-level details must stay outside the public repo.

Example:
  python scripts/benchmarks/live_dense_source_stress.py \
    --lab /private/frozen-stress --query-mode fp32-frozen \
    --expected-sha <release-sha> --concurrency 2 --output /private/report.json
"""
from __future__ import annotations
import argparse
import ast
import asyncio
import collections
import hashlib
import json
import os
import struct
import sys
import time
from pathlib import Path

ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/"src"),str(ROOT/"scripts/production"),str(ROOT/"scripts/benchmarks")]
from operator_env import load_service_env
from chunk_size_audit import norm
from regional_knowledge.supabase_backend import backend_from_env
from regional_knowledge.indexing import IndexReconciler
from regional_knowledge.bge_queue import BgeQueue
from regional_knowledge.bge_contract import SPACE as BGE_SPACE, validate_vector


def percentile(values:list[float],fraction:float)->float|None:
    ordered=sorted(values)
    if not ordered:return None
    x=(len(ordered)-1)*fraction
    a=int(x);b=min(a+1,len(ordered)-1)
    return round(ordered[a]+(ordered[b]-ordered[a])*(x-a),2)


def hits(rows:list[dict])->dict:
    n=len(rows)
    if not n:return {"n":0}
    def hit(k:int)->float:
        return round(sum(r["rank"] is not None and r["rank"]<=k for r in rows)/n,5)
    return {
        "n":n,"hit1":hit(1),"hit5":hit(5),"hit10":hit(10),
        "ready_fraction":round(sum(r["ready"] for r in rows)/n,5),
        "wrong_document_top1":round(sum(r["wrong_doc"] for r in rows)/n,5),
    }


def frozen_matrix(path:Path,expected_rows:int)->tuple[bytes,int,str]:
    data=path.read_bytes()
    if not data.startswith(b"\x93NUMPY"):raise ValueError("invalid frozen matrix header")
    major=data[6]
    if major==1:header_size=struct.unpack_from("<H",data,8)[0];begin=10
    elif major in (2,3):header_size=struct.unpack_from("<I",data,8)[0];begin=12
    else:raise ValueError("unsupported matrix format")
    header=ast.literal_eval(data[begin:begin+header_size].decode("latin1"))
    if (header.get("shape")!=(expected_rows,1024)
        or header.get("fortran_order")
        or header.get("descr") not in ("<f4","|f4")):
        raise ValueError("frozen matrix shape or dtype mismatch")
    offset=begin+header_size
    if len(data)!=offset+expected_rows*4096:raise ValueError("incomplete frozen matrix")
    return data,offset,hashlib.sha256(data).hexdigest()


def authorized_private_path(path:Path)->Path:
    candidate=path.resolve()
    if candidate==ROOT or ROOT in candidate.parents:
        raise ValueError("source-grounded private fixture/report may not be stored in Git checkout")
    return candidate


async def benchmark(args):
    release_marker=ROOT/".rkb-release-sha"
    if not release_marker.is_file() or release_marker.read_text().strip()!=args.expected_sha:
        raise RuntimeError("benchmark must run from pinned immutable release")
    import regional_knowledge.vector_plane as vector_plane
    if not Path(vector_plane.__file__).resolve().is_relative_to(ROOT/"src"):
        raise RuntimeError("the vector candidate client is not sourced from this release")
    load_service_env()
    if os.getenv("RKB_VECTOR_CANDIDATE_VERSION")!="v5":
        raise RuntimeError("production vector candidate version must be v5")

    lab=authorized_private_path(args.lab)
    raw_cases=(lab/"stress-cases.json").read_bytes()
    full=json.loads(raw_cases)
    negatives=json.loads((lab/"negative-cases.json").read_text())
    buckets=json.loads((lab/"boundary-buckets.json").read_text())
    if not isinstance(full,list) or len(full)>1000 or not full:
        raise ValueError("unsupported case count")
    chosen=[
        (index,c) for index,c in enumerate(full)
        if (args.variant=="all" or c["variant"] in args.variant.split(","))
        and (args.split=="all" or c["split"]==args.split)
    ][:args.limit]
    if not chosen:raise ValueError("no cases selected")
    matrix,offset,matrix_sha=(None,None,None)
    if args.query_mode=="fp32-frozen":
        matrix,offset,matrix_sha=frozen_matrix(
            lab/"stress-queries-bge.npy",len(full)+len(negatives)
        )
    backend=backend_from_env()
    actor=IndexReconciler(
        backend,BgeQueue(os.environ["RKB_BGE_QUEUE_PATH"])
    ).actor(os.environ["RKB_OWNER_SUBJECT"])
    try:
        async with backend.data_client._connection(backend._headers(actor)) as db:
            visible=await(await db.execute("select id,active_revision from rkb_documents")).fetchall()
        allowed={
            str(row["id"]):int(row["active_revision"])
            for row in visible
            if int(row["active_revision"] or 0)>0
            and backend.search_visible(str(row["id"]))
        }
        selected_documents={c["doc"] for _,c in chosen}
        if not selected_documents.issubset(allowed):
            raise PermissionError("source fixture contains unauthorized documents")
        if args.scope=="selected_sources":
            allowed={d:allowed[d] for d in selected_documents}

        active={
            str(c["id"]):c
            for c in backend.corpus.rows("rkb_chunks")
            if c["document_id"] in allowed
            and int(c["revision"])==allowed[c["document_id"]]
        }
        source_normalized={
            ident:norm(c["source_text"])
            for ident,c in active.items() if c["document_id"] in selected_documents
        }
        gold={}
        for _,case in chosen:
            key=case["base_id"]
            if key not in gold:
                proof=norm(case["proof"])
                gold[key]={
                    ident for ident,txt in source_normalized.items()
                    if txt.find(proof)>=0
                    and active[ident]["document_id"]==case["doc"]
                }
                if not gold[key]:
                    raise RuntimeError("frozen proof absent from active source corpus")

        sem=asyncio.Semaphore(args.concurrency)
        async def one(index,case):
            async with sem:
                started=time.monotonic();encode_ms=None
                try:
                    if args.query_mode=="fp32-frozen":
                        vector=validate_vector(list(struct.unpack_from(
                            "<1024f",matrix,offset+index*4096
                        )))
                        encode_ms=0.0
                    else:
                        vector=await backend.bge_query_embedder.embed(case["query"])
                        encode_ms=1000*(time.monotonic()-started)
                    literal="["+",".join(format(value,".9g") for value in vector)+"]"
                    candidates=await backend.vector_client.candidates(
                        actor.subject,allowed,None,None,literal,BGE_SPACE,10
                    )
                    ids=[]
                    for candidate in candidates:
                        key=str(candidate["chunk_id"])
                        row=active.get(key)
                        if row is None:raise PermissionError("out-of-scope candidate")
                        if any(str(row[k])!=str(candidate[k]) for k in (
                            "revision","text_sha256","search_material_sha256"
                        )):
                            raise RuntimeError("candidate/source hash mismatch")
                        ids.append(key)
                    top_doc=active[ids[0]]["document_id"] if ids else None
                    matched=gold[case["base_id"]]
                    rank=next((position+1 for position,ident in enumerate(ids)
                               if ident in matched),None)
                    return {
                        "case_id":case["id"],"variant":case["variant"],
                        "split":case["split"],"language":case["language"],
                        "boundary":buckets[case["base_id"]],
                        "rank":rank,"ready":bool(ids),"wrong_doc":top_doc!=case["doc"],
                        "elapsed_ms":round(1000*(time.monotonic()-started),2),
                        "encode_ms":round(encode_ms,2),
                    }
                except Exception as err:
                    return {
                        "case_id":case["id"],"variant":case["variant"],
                        "split":case["split"],"language":case["language"],
                        "boundary":buckets[case["base_id"]],
                        "rank":None,"ready":False,"wrong_doc":False,
                        "error_type":type(err).__name__,
                        "http_status":getattr(getattr(err,"response",None),"status_code",None),
                        "elapsed_ms":round(1000*(time.monotonic()-started),2),
                        "encode_ms":round(encode_ms,2) if encode_ms is not None else None,
                    }
        started=time.monotonic()
        rows=[];aborted=False
        for batch_start in range(0,len(chosen),32):
            batch=await asyncio.gather(*(one(index,case) for index,case in chosen[batch_start:batch_start+32]))
            rows.extend(batch)
            if sum(not r["ready"] for r in batch)>=args.max_failed_per_batch:
                aborted=True;break
        held=[r for r in rows if r["variant"]=="original" and r["split"]=="test"]
        agg={
            "all":hits(rows),
            "heldout_original":hits(held),
            "by_variant":{k:hits([r for r in rows if r["variant"]==k]) for k in (
                "original","orthography","terse","typo","verbose","cross_book_noise"
            )},
            "by_language":{k:hits([r for r in rows if r["language"]==k]) for k in ("ru","de")},
            "by_boundary":{k:hits([r for r in rows if r["boundary"]==k]) for k in sorted(set(r["boundary"] for r in rows))},
            "errors_by_type":dict(collections.Counter(r.get("error_type","no_hits") for r in rows if not r["ready"])),
            "time_ms":{
                "p50":percentile([r["elapsed_ms"] for r in rows],.5),
                "p95":percentile([r["elapsed_ms"] for r in rows],.95),
                "max":max(r["elapsed_ms"] for r in rows),
                "encoder_p95":percentile([r["encode_ms"] for r in rows if r["encode_ms"] is not None],.95),
                "wall_seconds":round(time.monotonic()-started,2),
            },
        }
        data={
            "schema":"rkb.live_dense_source_stress.v1",
            "release_sha":args.expected_sha,
            "source_fixture_sha256":hashlib.sha256(raw_cases).hexdigest(),
            "query_matrix_sha256":matrix_sha,
            "query_mode":args.query_mode,"document_scope":args.scope,
            "dense_only":True,"continuation":False,"aliases":False,
            "lexical":False,"query_concurrency":args.concurrency,
            "planned_cases":len(chosen),"finished_cases":len(rows),
            "aborted":aborted,"corpus_source_proofs_checked":len(gold),
            "quality_gate_passed":len(held)>=32 and len(rows)==len(chosen) and
                 agg["heldout_original"].get("hit5",0)>=.85
                 and agg["heldout_original"].get("hit10",0)>=.90,
            "stats":agg,"private_cases":rows,
        }
        dest=authorized_private_path(args.output)
        if dest.exists():raise FileExistsError("refuse to overwrite previous stress evidence")
        dest.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
        dest.write_text(json.dumps(data,ensure_ascii=False,indent=2)+"\n")
        dest.chmod(0o600)
        print(json.dumps({k:v for k,v in data.items() if k!="private_cases"},ensure_ascii=False,indent=2))
        if args.strict and (aborted or not data["quality_gate_passed"]):
            raise SystemExit("strict source-grounded dense retrieval gate did not pass")
    finally:
        await backend.aclose()


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--lab",type=Path,required=True)
    parser.add_argument("--expected-sha",required=True)
    parser.add_argument("--query-mode",choices=("fp32-frozen","int8-live"),required=True)
    parser.add_argument("--scope",choices=("all_authorized","selected_sources"),default="all_authorized")
    parser.add_argument("--variant",default="all")
    parser.add_argument("--split",choices=("all","dev","test"),default="all")
    parser.add_argument("--concurrency",type=int,default=2)
    parser.add_argument("--max-failed-per-batch",type=int,default=4)
    parser.add_argument("--limit",type=int,default=384)
    parser.add_argument("--output",type=Path,required=True)
    parser.add_argument("--strict",action="store_true")
    args=parser.parse_args()
    if not 1<=args.concurrency<=4 or not 1<=args.max_failed_per_batch<=32 or not 1<=args.limit<=1000:
        parser.error("unsafe benchmark bounds")
    if args.variant!="all" and not set(args.variant.split(",")).issubset({
        "original","orthography","terse","typo","verbose","cross_book_noise"
    }):
        parser.error("unknown query variant")
    asyncio.run(benchmark(args))

if __name__=="__main__":
    main()
