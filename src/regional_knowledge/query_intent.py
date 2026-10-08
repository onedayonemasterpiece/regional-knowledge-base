"""Conservative intent recognition for literal identifiers.

A lone opaque alphanumeric identifier is an exact-match question, not a
natural-language similarity question. Do not generalize to names, years,
transliterations, or negative historical premises.
"""
import re
import time

_OPAQUE_IDENTIFIER=re.compile(r"(?=.{9,64}$)(?=[A-Za-z0-9._:-]*[0-9]{5})[A-Za-z][A-Za-z0-9._:-]*$",re.ASCII)

def is_opaque_identifier(query:str)->bool:
    return bool(_OPAQUE_IDENTIFIER.fullmatch(query.strip()))

async def exact_identifier_search(backend,query,principal,*,match_count,document_ids):
    """Return source-anchored literal hits without inventing dense neighbors."""
    from .contracts import SearchOutput,SearchResult
    from .rank_fusion import fuse
    started=time.monotonic()
    response=await backend.client.post(
        f'{backend.config.url.rstrip("/")}/rest/v1/rpc/rkb_multilingual_rankings',
        headers=backend._headers(principal),
        json={'query_text':query,'bge_vector':None,'e5_vector':None,
              'aliases':[],'document_ids':document_ids,
              'depth':max(1,min(int(match_count),20)),
              'include_lexical':True},
    )
    response.raise_for_status()
    ids,diagnostics=fuse(response.json(),['lexical'],limit=max(1,min(int(match_count),20)))
    results=[]
    for ident in ids:
        row=backend.corpus.one('rkb_chunks',ident)
        if row:
            results.append(SearchResult(id=ident,title=str(row['title']),
                url=backend._evidence_url(ident),ranking_signals=diagnostics[ident]))
    return SearchOutput(results=results,mode='lexical_degraded',retrieval_mode='lexical_only',
        retrieval_policy='exact_identifier',main_state='ready',
        latency_ms=round(1000*(time.monotonic()-started),1),
        timings={'search_seconds':time.monotonic()-started})
