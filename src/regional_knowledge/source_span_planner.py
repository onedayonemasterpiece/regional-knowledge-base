"""Deterministic, token-bounded source-span planner for a reviewed source page.

This is a non-authoritative staging helper: it never changes a revision or
claims source verification. A reviewer still supplies actual reviewed pages,
figures, footnotes and semantic judgment. Every covered source-codepoint is
anchored to the original region text SHA-256 and exact [start,end) offsets.
"""
from __future__ import annotations

import hashlib
import re
from collections.abc import Callable

from .contracts import StageChunkInput, StagePageInput, StageRegionSpanRef

TEXT_KINDS=frozenset(("body","heading","caption","table","marginalia"))
_FOOTNOTE="footnote"
_SENTENCE_END=re.compile(r"[.!?;:](?=\s|$)")
_ABBR=re.compile(r"(?:\b(?:S|No|Nr|Art|Bd|Th|St|Dr|vgl|resp|bzw)|\d)\.$",re.I)


def _split_text(text:str,count_tokens:Callable[[str],int],*,target:int,max_chars:int):
    """Contiguous lossless span pairs; stops at semantic clauses if feasible."""
    if not isinstance(text,str) or not text.strip():
        return []
    if target<32 or max_chars<300:
        raise ValueError("unsafe source planner limits")
    pos=0
    spans=[]
    while pos<len(text):
        limit=min(len(text),pos+max_chars)
        if count_tokens(text[pos:limit])>target:
            lo=pos
            hi=limit
            while lo+1<hi:
                mid=(lo+hi)//2
                if count_tokens(text[pos:mid])<=target:lo=mid
                else:hi=mid
            limit=lo
        if limit<=pos:
            raise ValueError("a source unit cannot fit within the token budget")
        if limit==len(text):
            end=limit
        else:
            floor=pos+max(1,int((limit-pos)*.55))
            boundary=None
            for match in _SENTENCE_END.finditer(text,floor,limit):
                upto=match.end()
                if _ABBR.search(text[max(pos,upto-12):upto]):
                    continue
                if upto<len(text) and text[upto].isspace():
                    boundary=upto
            if boundary is not None:
                end=boundary
            else:
                # Do not allow an arbitrary tokenizer/codepoint limit to
                # split a source word or change its spelling.
                cut=max(text.rfind(" ",floor,limit+1),
                        text.rfind("\n",floor,limit+1),
                        text.rfind("\t",floor,limit+1))
                if cut<floor:
                    raise ValueError("source has no safe textual split point")
                end=cut
        if end<=pos or (end<len(text) and text[end-1].isalnum() and text[end].isalnum()):
            raise ValueError("source split would cut through a word")
        spans.append((pos,end))
        pos=end
    if spans[0][0]!=0 or spans[-1][1]!=len(text) or any(a[1]!=b[0] for a,b in zip(spans,spans[1:])):
        raise RuntimeError("planner failed to preserve contiguous region text")
    return spans


def plan_reviewed_page(
    page:StagePageInput,
    *,
    title:str,
    token_count:Callable[[str],int],
    target_tokens:int=256,
    max_chars:int=1100,
    chunk_prefix:str="source",
)->list[StageChunkInput]:
    """Build compact passages using source spans with exact byte provenance.

    Figures remain the responsibility of the page's illustration graph.
    A footnote is preserved via a whole-region passage; oversized footnotes
    fail closed pending manual staging. No page review status is modified.
    """
    pieces=[]
    for region in sorted(page.regions,key=lambda item:(item.reading_order,item.region_key)):
        kind=region.kind.value if hasattr(region.kind,"value") else str(region.kind)
        if kind=="figure":
            continue
        text=region.source_text
        if not text or not text.strip():
            continue
        if kind not in TEXT_KINDS and kind!=_FOOTNOTE:
            raise ValueError("unsupported source textual region category")
        if kind==_FOOTNOTE:
            if len(text)>max_chars or token_count(text)>target_tokens:
                raise ValueError("oversized footnote requires reviewed manual passage")
            pieces.append(("footnote",region,None,None,text))
            continue
        for a,b in _split_text(text,token_count,target=target_tokens,max_chars=max_chars):
            pieces.append(("span",region,a,b,text[a:b].strip()))

    output=[]
    pending=[]
    def flush():
        if not pending:return
        index=len(output)
        if pending[0][0]=="footnote":
            assert len(pending)==1
            item=pending[0]
            out=StageChunkInput(
                chunk_key=f"{chunk_prefix}-{index:05d}",
                title=title,
                region_refs=[{"page_id":page.page_id,"region_key":item[1].region_key}],
            )
        else:
            refs=[]
            for _,region,start,end,_ in pending:
                refs.append(StageRegionSpanRef(
                    page_id=page.page_id,
                    region_key=region.region_key,
                    start=start,end=end,
                    source_text_sha256=hashlib.sha256(region.source_text.encode("utf-8")).hexdigest(),
                ))
            out=StageChunkInput(
                chunk_key=f"{chunk_prefix}-{index:05d}",title=title,
                span_refs=refs,
            )
        output.append(out)
        pending.clear()

    for item in pieces:
        kind,region,start,end,fragment=item
        if kind=="footnote":
            flush()
            pending.append(item)
            flush()
            continue
        possible="\n".join([r[4] for r in [*pending,item]])
        if pending and (len(possible)>max_chars or token_count(possible)>target_tokens):
            flush()
            possible=fragment
        if len(possible)>max_chars or token_count(possible)>target_tokens:
            raise ValueError("planner emitted a passage beyond the token budget")
        pending.append(item)
    flush()
    return output
