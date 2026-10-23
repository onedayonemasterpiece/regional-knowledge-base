"""Packet-end coverage and a bounded query over existing native projections."""
import hashlib
from uuid import UUID, uuid5


def deferred_tail_ranges(graph, current_pages, fully_covered, intervals):
    """Recognize only coverage geometry; never choose a semantic boundary."""
    if not current_pages:
        return {}
    prose=sorted(((page.physical_page_index,region.reading_order,region)
        for page in graph.pages if page.physical_page_index in current_pages
        for region in page.regions if region.kind.value=='body' and region.source_text.strip()),
        key=lambda item:item[:2])
    missing={}
    for page,_,region in prose:
        ident=str(region.region_id);text=region.source_text
        if ident in fully_covered:continue
        cursor=0;gaps=[]
        for start,end in sorted(intervals.get(ident,[])):
            if start>cursor and text[cursor:start].strip():gaps.append((cursor,start))
            cursor=max(cursor,end)
        if text[cursor:].strip():gaps.append((cursor,len(text)))
        if not gaps:continue
        if page!=max(current_pages) or len(gaps)!=1 or gaps[0][1]!=len(text):return {}
        missing[ident]=gaps[0]
    if not missing:return {}
    first=next(i for i,(_,_,r) in enumerate(prose) if str(r.region_id) in missing)
    # After the first omitted span every following body region must also be
    # omitted in full. A covered later region would make this a middle gap.
    for _,_,region in prose[first+1:]:
        gap=missing.get(str(region.region_id))
        if gap is None or region.source_text[:gap[0]].strip():return {}
    return missing


async def pending_tail_refs(connection, source_id, ingestion_id):
    """Only pointers leave PG; no tail journal, cache or full-book Python scan."""
    if await (await connection.execute('''SELECT 1 FROM rkb_v4.source_stage_sessions st
        JOIN rkb_v4.sources s USING(source_id)
        WHERE st.source_id=%s AND st.ingestion_id=%s AND s.accepted AND NOT s.withdrawn''',
        (source_id,ingestion_id))).fetchone() is None:
        raise ValueError('source_tail_scope_unavailable')
    rows=await (await connection.execute('''SELECT r.region_id,r.page_id,p.physical_page_index,
        p.model_page->'regions',r.source_text_sha256,sp.covered_end
        FROM rkb_v4.source_regions r JOIN rkb_v4.source_stage_pages p
          ON p.source_id=r.source_id AND p.page_id=r.page_id
        CROSS JOIN LATERAL(SELECT coalesce(max((span->>'end')::integer),0) covered_end
          FROM rkb_v4.source_passage_provenance z
          CROSS JOIN LATERAL jsonb_array_elements(z.data->'source_spans') span
          WHERE z.source_id=r.source_id
            AND z.data @> jsonb_build_object('page_ids',jsonb_build_array(r.page_id::text))
            AND span->>'region_id'=r.region_id::text) sp
        WHERE r.source_id=%s AND btrim(r.source_text)<>''
          AND btrim(substr(r.source_text,sp.covered_end+1),E' \\t\\n\\r')<>''
          AND EXISTS(SELECT 1 FROM jsonb_array_elements(p.model_page->'regions') m
              WHERE m->>'kind'='body' AND m->>'source_text'=r.source_text)
          AND NOT EXISTS(SELECT 1 FROM rkb_v4.source_passage_provenance z
              WHERE z.source_id=r.source_id AND z.data @> jsonb_build_object('page_ids',jsonb_build_array(r.page_id::text))
                AND ((z.data->'source_spans'='[]'::jsonb AND z.data->'region_ids' @> jsonb_build_array(r.region_id::text))
                  OR z.data->'footnote_region_ids' @> jsonb_build_array(r.region_id::text)))
        ORDER BY p.physical_page_index,r.region_id LIMIT 5''',(source_id,))).fetchall()
    refs=[]
    for ident,page_id,index,regions,sha,start in rows:
        region=next((r for r in regions if str(uuid5(UUID(str(page_id)),'region:'+r['region_key']))==str(ident)),None)
        if (region is None or region['kind']!='body'
                or hashlib.sha256(region['source_text'].encode()).hexdigest()!=sha):
            raise ValueError('source_tail_projection_changed')
        refs.append(dict(page_id=str(page_id),physical_page_index=index,
            region_id=str(ident),region_key=region['region_key'],source_text_sha256=sha,
            start=start,end=len(region['source_text'])))
    return dict(tail_refs=refs[:4],has_more=len(refs)>4)
