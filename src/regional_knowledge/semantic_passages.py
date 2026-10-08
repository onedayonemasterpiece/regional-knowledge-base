"""Bounded semantic passage draft from fully reviewed source regions.

Only proposes stage chunk inputs: no source mutation, no revision activation,
no model inference. Every excerpt retains a SHA-256-anchored codepoint span of
the source region. Exact model token counters are injected by the caller, so
the proposal is not allowed to silently truncate at the embedding service.
"""
from __future__ import annotations

import hashlib
import re
from collections.abc import Callable,Sequence

from .contracts import StageChunkInput,StagePageInput,StageRegionSpanRef


TEXT_KINDS={"body","heading","table","marginalia","caption"}
SENTENCE_BREAK=re.compile(r"[.!?;:]\s+",re.UNICODE)
ABBREVIATION=re.compile(r"(?:\b(?:S|No|Nr|Art|Bd|Th|St|Dr|vgl|resp|bzw)|\d)\.$",re.I)


def _choose_cut(text:str,start:int,soft_end:int)->int:
    """Prefer sentence boundaries after halfway, then whitespace; no word cuts."""
    if soft_end>=len(text):return len(text)
    halfway=start+max(1,int((soft_end-start)*.55))
    sentences=[]
    for match in SENTENCE_BREAK.finditer(text,start,soft_end):
        end=match.end()
        if end<halfway:continue
        if ABBREVIATION.search(text[start:match.start()+1]):continue
        sentences.append(end)
    if sentences:return sentences[-1]
    spaces=[j+1 for j in range(halfway,soft_end) if text[j].isspace()]
    if spaces:return spaces[-1]
    for j in range(soft_end-1,start,-1):
        if text[j].isspace():return j+1
    raise ValueError("unbreakable text exceeds the semantic token target")


def _source_pieces(
    text:str,count_tokens:Callable[[str],int],target_tokens:int,
)->list[tuple[int,int]]:
    """Exact offsets; concatenate the extracted spans to recover source words."""
    start=0
    result=[]
    while start<len(text):
        if not text[start:].strip():break
        # Token length is approximately monotone for these tokenizer families.
        # Always verify selected chunks after the binary search.
        low=start+1
        high=len(text)
        best=start
        while low<=high:
            mid=(low+high)//2
            if count_tokens(text[start:mid])<=target_tokens:
                best=mid
                low=mid+1
            else:high=mid-1
        if best<=start:raise ValueError("single source token exceeds target budget")
        end=_choose_cut(text,start,best) if best<len(text) else len(text)
        if not text[start:end].strip():
            raise ValueError("empty draft source span")
        if count_tokens(text[start:end])>target_tokens:
            raise ValueError("source passage token counter was not monotone")
        if end<len(text) and text[end-1].isalnum() and text[end].isalnum():
            raise ValueError("draft source span would split a word")
        result.append((start,end))
        start=end
    if any(ch.strip() and not any(a<=i<b for a,b in result)
           for i,ch in enumerate(text)):
        raise ValueError("source text lost during passage draft")
    return result


def draft_semantic_passages(
    pages:Sequence[StagePageInput],
    count_tokens:Callable[[str],int],
    *,
    target_tokens:int=256,
    hard_max_tokens:int=512,
    title_prefix:str="Source passage",
)->list[StageChunkInput]:
    """Produce provenance-preserving draft chunks for a subsequent staged revision.

    Requires prior visual review; it neither declares pages visually reviewed
    nor silently resolves unfinished review. Footnotes remain own full-region
    passages and must independently fit the strict token ceiling.
    """
    if not 32<=target_tokens<=hard_max_tokens<=512:
        raise ValueError("semantic token target or hard limit invalid")
    if not 1<=len(pages)<=2500:
        raise ValueError("one to 2500 reviewed pages required")
    if len(title_prefix)>350 or not title_prefix.strip():
        raise ValueError("invalid draft title")
    output=[]
    seen_pages=set()
    for page in sorted(pages,key=lambda p:p.physical_page_index):
        if page.page_id in seen_pages:raise ValueError("duplicate source page")
        seen_pages.add(page.page_id)
        if page.source_material!="visual_reviewed" or not (
            page.source_review_note or ""
        ).strip():
            raise ValueError("cannot propose passages from unreviewed page")
        if any(region.needs_review for region in page.regions):
            raise ValueError("cannot propose passages from unreviewed region")
        current=[]
        current_text=[]
        serial=0
        def flush():
            nonlocal serial
            if not current:return
            if len(current)>100:raise ValueError("too many referenced source spans")
            text="\n".join(current_text)
            if count_tokens(text)>hard_max_tokens:
                raise ValueError("draft search input exceeds 512-token ceiling")
            serial+=1
            output.append(StageChunkInput(
                chunk_key=f"semantic-p{page.physical_page_index:05d}-{serial:04d}",
                title=f"{title_prefix[:350]} p.{page.physical_page_index+1}, {serial}",
                span_refs=list(current),
            ))
            current.clear()
            current_text.clear()
        for region in sorted(page.regions,key=lambda r:r.reading_order):
            source=region.source_text
            if not source.strip():continue
            kind=region.kind.value if hasattr(region.kind,"value") else str(region.kind)
            if kind=="footnote":
                flush()
                if count_tokens("[Footnote] "+source)>hard_max_tokens:
                    raise ValueError("oversized footnote needs explicit source review")
                serial+=1
                output.append(StageChunkInput(
                    chunk_key=f"semantic-p{page.physical_page_index:05d}-{serial:04d}",
                    title=f"{title_prefix[:350]} p.{page.physical_page_index+1}, {serial}",
                    region_refs=[{"page_id":page.page_id,"region_key":region.region_key}],
                ))
                continue
            if kind not in TEXT_KINDS:continue
            digest=hashlib.sha256(source.encode("utf-8")).hexdigest()
            for start,end in _source_pieces(source,count_tokens,target_tokens):
                value=source[start:end].strip()
                proposed="\n".join([*current_text,value]) if current_text else value
                if current and (count_tokens(proposed)>target_tokens or len(current)>=100):
                    flush()
                current.append(StageRegionSpanRef(
                    page_id=page.page_id,region_key=region.region_key,
                    start=start,end=end,source_text_sha256=digest,
                ))
                current_text.append(value)
        flush()
    if not output:raise ValueError("no nonempty source passages")
    if len({chunk.chunk_key for chunk in output})!=len(output):
        raise RuntimeError("draft chunk keys are not unique")
    return output
