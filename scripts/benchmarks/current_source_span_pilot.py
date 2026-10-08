"""Non-destructive exact-source span re-chunk pilot for already authorized books.

Reads accepted active source regions + figure references from SQLite authority.
Builds a deterministic staged *proposal*, not a reviewed/accepted revision.
No Supabase mutations, no source-object writes, no active-revision changes.
Private case/query/source output stays outside public Git checkout.
"""
from __future__ import annotations

import argparse
import asyncio
import collections
import hashlib
import json
import os
import sys
import time
from pathlib import Path
from uuid import UUID,uuid5

from tokenizers import Tokenizer

ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/"src"),str(ROOT/"scripts/production")]
from operator_env import load_service_env
from regional_knowledge.supabase_backend import backend_from_env
from regional_knowledge.bge_queue import BgeQueue
from regional_knowledge.indexing import IndexReconciler
from regional_knowledge.source_span_planner import plan_reviewed_page
from regional_knowledge.contracts import StagePageInput
from regional_knowledge.stage_graph import StagedGraph,compile_model_stage,merge_stage,validate_graph
from regional_knowledge.bge_contract import MAX_TOKENS as BGE_MAX
from regional_knowledge.e5_contract import MAX_TOKENS as E5_MAX

BGE_TOKENIZER=Path("/home/dev/.local/share/regional-knowledge-base/bge-m3-int8/tokenizer.json")
E5_TOKENIZER=Path("/home/dev/.local/share/regional-knowledge-base/fast-e5/models/tokenizer.json")
TEXT_KINDS=frozenset(("body","heading","caption","footnote","table","marginalia"))


def private_path(path:Path)->Path:
    resolved=path.resolve()
    if resolved==ROOT or ROOT in resolved.parents:
        raise RuntimeError("provenance and source text may not be written to public Git")
    return resolved


def source_hash(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def page_from_active(doc,revision,page,regions,illustrations):
    # A previously accepted active page is not automatically a newly reviewed
    # page. Explicitly keep the new proposed revision in 'preview' state.
    actual_doc=UUID(str(doc["id"]))
    page_id=str(uuid5(actual_doc,f"page:{revision}:{page['physical_page_index']}"))
    ordered=sorted(regions,key=lambda r:(int(r.get("reading_order") or 0),str(r["id"])))
    key_map={str(r["id"]):f"rg-{int(r.get('reading_order') or 0):03d}-{str(r['id'])[:8]}"
             for r in ordered}
    staged_regions=[]
    for region in ordered:
        data={
            "region_key":key_map[str(region["id"])],
            "kind":region["kind"],
            "bbox":region["bbox"],
            "reading_order":int(region.get("reading_order") or 0),
            "source_text":region.get("source_text") or "",
            "normalized_text":region.get("source_text") or "",
            "needs_review":bool(region.get("needs_review",False)),
        }
        if region.get("column_id"):data["column_id"]=region["column_id"]
        if region.get("confidence") is not None:data["confidence"]=region["confidence"]
        staged_regions.append(data)
    staged_images=[]
    represented=set()
    for n,item in enumerate(illustrations):
        source=str(item["source_region_id"])
        if source not in key_map:raise RuntimeError("source illustration region missing")
        represented.add(source)
        cids=[key_map[str(value)] for value in item.get("caption_region_ids",[]) if str(value) in key_map]
        if len(cids)!=len(item.get("caption_region_ids",[])):
            raise RuntimeError("caption source link missing")
        near=[key_map[str(value)] for value in item.get("nearby_region_ids",[]) if str(value) in key_map]
        if len(near)!=len(item.get("nearby_region_ids",[])):
            raise RuntimeError("nearby source link missing")
        staged_images.append({
            "illustration_key":f"figure-{n:03d}",
            "source_region_key":key_map[source],
            "kind":item["kind"],
            "caption_region_keys":cids,
            "nearby_region_keys":near,
            "visual_description":item.get("visual_description"),
            "visual_description_provenance":"model_observation",
            "visual_description_language":item.get("visual_description_language"),
            "display_rotation_degrees":item.get("display_rotation_degrees") or 0,
        })
    figures={str(r["id"]) for r in ordered if r["kind"]=="figure"}
    if figures!=represented:
        raise RuntimeError("figure regions lack authoritative accepted illustration references")
    params={
        "page_id":page_id,
        "physical_page_index":int(page["physical_page_index"]),
        "printed_page_number":page.get("printed_page_number"),
        "width":page["width"],"height":page["height"],
        "layout_kind":page.get("layout_kind"),
        "source_material":"preview",
        "source_review_note":None,
        "regions":staged_regions,
        "illustrations":staged_images,
    }
    return StagePageInput.model_validate(params)


def measure(values):
    if not values:return {"count":0}
    ordered=sorted(values)
    return {"count":len(values),"p50":ordered[(len(ordered)-1)//2],
            "p95":ordered[int(.95*(len(ordered)-1))],"max":ordered[-1]}


async def pilot(args):
    marker=ROOT/".rkb-release-sha"
    if not marker.exists() or marker.read_text().strip()!=args.expected_sha:
        raise RuntimeError("use exact immutable release to run non-destructive pilot")
    load_service_env()
    fixture=private_path(args.cases)
    source=json.loads(fixture.read_text())
    if not isinstance(source,list) or len(source)>1000:
        raise ValueError("invalid bounded fixture")
    target_documents={row["doc"] for row in source}
    if not 1<=len(target_documents)<=3:raise ValueError("unexpected document scope")
    backend=backend_from_env()
    actor=IndexReconciler(
        backend,BgeQueue(os.environ["RKB_BGE_QUEUE_PATH"])
    ).actor(os.environ["RKB_OWNER_SUBJECT"])
    try:
        docs={r["id"]:r for r in backend.corpus.rows("rkb_documents")
              if r["id"] in target_documents}
        if set(docs)!=target_documents:
            raise PermissionError("selected document missing")
        for doc in docs.values():
            if doc.get("owner_user_id")!=actor.subject or not backend.search_visible(doc["id"]):
                raise PermissionError("private source authorization failed")
            if not doc.get("source_sha256") or not doc.get("active_revision"):
                raise RuntimeError("source authority missing")
        original_pages={
            r["id"]:r for r in backend.corpus.rows("rkb_pages")
            if r.get("document_id") in docs
            and int(r.get("revision") or 0)==int(docs[r["document_id"]]["active_revision"])
        }
        regions=collections.defaultdict(list)
        for region in backend.corpus.rows("rkb_regions"):
            if region.get("page_id") in original_pages:
                regions[region["page_id"]].append(region)
        images=collections.defaultdict(list)
        for image in backend.corpus.rows("rkb_illustrations"):
            if image.get("page_id") in original_pages:
                images[image["page_id"]].append(image)

        bge_tok=Tokenizer.from_file(str(BGE_TOKENIZER))
        e5_tok=Tokenizer.from_file(str(E5_TOKENIZER))
        bge_sha=source_hash(BGE_TOKENIZER)
        e5_sha=source_hash(E5_TOKENIZER)
        def token_counts(text):
            return len(bge_tok.encode(text).ids),len(e5_tok.encode("passage: "+text).ids)
        def count_max(text):
            return max(token_counts(text))

        out_books=[]
        material_all={}
        for did,doc in docs.items():
            active_rev=int(doc["active_revision"])
            next_rev=active_rev+1
            prows=sorted(
                (p for p in original_pages.values() if p["document_id"]==did),
                key=lambda item:item["physical_page_index"],
            )
            if len(prows)!=int(doc["page_count"]):
                raise RuntimeError("active page count does not match accepted source")
            # Figure/review provenance must be checked again before activation.
            # This pipeline is deliberately not a silent source review bypass.
            graph=StagedGraph(revision=next_rev)
            specs=[];body_segments=0
            for page in prows:
                stage_page=page_from_active(
                    doc,next_rev,page,regions[page["id"]],images[page["id"]]
                )
                planned=plan_reviewed_page(
                    stage_page,
                    title=str(doc.get("title") or "Source")[:400],
                    token_count=count_max,
                    target_tokens=args.target_tokens,
                    max_chars=args.max_chars,
                    chunk_prefix=f"p{int(page['physical_page_index']):04d}",
                )
                body_segments+=len(planned)
                specs.append((stage_page,planned))
            # Staged API batches at most eight pages. Reuse the existing
            # deterministic model graph assembly without finalizing it.
            for offset in range(0,len(specs),8):
                pages=[s[0] for s in specs[offset:offset+8]]
                chunks=[c for _,cc in specs[offset:offset+8] for c in cc]
                result=compile_model_stage(
                    graph,document_id=did,revision=next_rev,
                    pages=pages,chunks=chunks,
                )
                graph=merge_stage(graph,result)
            verified=validate_graph(graph,expected_page_count=int(doc["page_count"]))
            completeness=[e for e in verified.errors if "source completeness unreviewed" in e]
            other_errors=[e for e in verified.errors if "source completeness unreviewed" not in e]
            if other_errors:
                raise RuntimeError("candidate graph failed integrity validation: "
                                   +";".join(other_errors[:4]))
            if len(completeness)!=int(doc["page_count"]):
                raise RuntimeError("pilot unexpectedly bypassed page source review")
            materials=[]
            for chunk in graph.chunks:
                # All indexed material will be independently checked with
                # the same actual BGE and E5 tokenizer input conventions.
                material=chunk.text
                b_count,e_count=token_counts(material)
                if b_count>BGE_MAX or e_count>E5_MAX:
                    raise ValueError("pilot chunk violates required encoder budget")
                materials.append({
                    "id":str(chunk.chunk_id),"doc":did,"pages":[str(x) for x in chunk.page_ids],
                    "text":material,"source_spans":[x.model_dump(mode="json") for x in chunk.source_spans],
                    "bge_tokens":b_count,"e5_tokens":e_count,
                })
            material_all[did]=materials
            out_books.append({
                "book_slot":len(out_books)+1,
                "active_revision_untouched":active_rev,
                "proposal_only_revision":next_rev,
                "source_sha256":doc["source_sha256"],
                "pages":len(prows),"original_text_regions":sum(
                    len(regions[p["id"]]) for p in prows),
                "original_illustrations":sum(len(images[p["id"]]) for p in prows),
                "planned_chunks":len(materials),
                "chars":measure([len(m["text"]) for m in materials]),
                "bge_tokens":measure([m["bge_tokens"] for m in materials]),
                "e5_tokens":measure([m["e5_tokens"] for m in materials]),
                "requires_reverification_pages":len(completeness),
                "validated_no_nonreview_errors":not other_errors,
            })
        # The 64 original gold sources were frozen before the ranking
        # experiment, not selected after observing new chunk boundaries.
        from chunk_size_audit import norm
        originals=[r for r in source if r["variant"]=="original"]
        evidence_found=0;no_hit=[]
        for case in originals:
            matches=material_all.get(case["doc"],[])
            gold=norm(case["proof"])
            if any(gold in norm(m["text"]) for m in matches):
                evidence_found+=1
            else:
                no_hit.append(case["id"])
        result={
            "status":"proposal_not_staged_not_activated",
            "installed_sha":args.expected_sha,"target_tokens":args.target_tokens,
            "max_chars":args.max_chars,"bge_tokenizer_sha256":bge_sha,
            "e5_tokenizer_sha256":e5_sha,
            "books":out_books,"source_grounded_original_cases":len(originals),
            "original_gold_proofs_contained_in_one_proposed_chunk":evidence_found,
            "original_gold_proofs_across_chunk_boundaries":len(no_hit),
            "pending_visual_reverification":sum(b["requires_reverification_pages"] for b in out_books),
        }
        data={"report":result,
              "private_materials":material_all,
              "private_golds_not_in_single_chunk":no_hit}
        dest=private_path(args.output)
        if dest.exists():raise RuntimeError("pilot output exists; do not overwrite")
        dest.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
        dest.write_text(json.dumps(data,ensure_ascii=False,indent=2)+"\n")
        dest.chmod(0o600)
        print(json.dumps(result,ensure_ascii=False,indent=2))
    finally:
        await backend.aclose()


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--expected-sha",required=True)
    p.add_argument("--cases",type=Path,required=True)
    p.add_argument("--target-tokens",type=int,default=256)
    p.add_argument("--max-chars",type=int,default=1100)
    p.add_argument("--output",type=Path,required=True)
    args=p.parse_args()
    if not 64<=args.target_tokens<=384 or not 500<=args.max_chars<=1500:
        p.error("unsafe target/token limits")
    asyncio.run(pilot(args))


if __name__=="__main__":
    main()
