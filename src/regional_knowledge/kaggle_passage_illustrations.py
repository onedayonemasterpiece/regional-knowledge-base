"""Accepted figures linked to one passage in its existing native read snapshot."""
from __future__ import annotations


async def read_source_illustration(request,cursor,execute):
    from .kaggle_protocol import EvidenceItem,EvidenceMetadata,ItemDependencies,SourceDependency
    from .kaggle_query import ReadUnavailable
    await execute('''SELECT i.data,pg.physical_page_index,v.eligibility_revision,session.ingestion_id,
            jsonb_build_object('title',picked.metadata->>'title',
                'authors',picked.metadata->'authors',
                'publication_year',picked.metadata->'publication_year')
        FROM rkb_v4.source_illustrations i
        JOIN rkb_v4.sources s USING(source_id)
        JOIN rkb_v4.active_revision a USING(source_id)
        JOIN rkb_v4.source_visibility v USING(source_id)
        JOIN rkb_v4.source_stage_sessions session ON session.source_id=i.source_id
        JOIN rkb_v4.source_stage_pages pg ON pg.source_id=i.source_id
            AND pg.page_id=(i.data->>'page_id')::uuid
        LEFT JOIN rkb_v4.source_catalog catalog ON catalog.source_id=i.source_id
            AND catalog.source_revision=a.revision
        LEFT JOIN LATERAL (
            SELECT k.metadata FROM rkb_v4.source_catalog_corrections k
            WHERE k.source_id=i.source_id ORDER BY k.accepted_sequence DESC LIMIT 1
        ) corrected ON true
        CROSS JOIN LATERAL (
            SELECT coalesce(corrected.metadata,catalog.metadata) AS metadata
        ) picked
        WHERE i.source_id=%s::uuid AND i.illustration_id=%s::uuid
            AND s.accepted AND NOT s.withdrawn AND a.revision=1 LIMIT 1''',
        (request.source_id,request.query))
    row=await cursor.fetchone()
    if row is None:raise ReadUnavailable('evidence_withdrawn')
    data,physical,eligibility,ingestion,catalog=row
    figure={key:value for key,value in data.items() if key!='crop_binding'}
    figure.update(source_id=request.source_id,physical_page_index=physical,
                  ingestion_id=str(ingestion),_citation_catalog=catalog)
    evidence=EvidenceItem(id=request.query,kind='source',
        text=(data.get('caption_text') or data.get('visual_description') or data['illustration_key'])[:2000],
        metadata=EvidenceMetadata(locator='source:'+request.source_id),
        dependencies=ItemDependencies(source_ids=(request.source_id,),variant_refs=(),grant_refs=(),
            source_revisions=(SourceDependency(source_id=request.source_id,content_revision=1,
                eligibility_revision=eligibility),)))
    return (figure,),(evidence,),None


async def read_passage_illustrations(cursor, execute, *, source_id, passage_id):
    linked = await read_passage_illustrations_batch(
        cursor, execute, passage_ids=(passage_id,), source_ids=(source_id,))
    return linked.get(passage_id, ((), False))


async def read_passage_illustrations_batch(cursor, execute, *, passage_ids, source_ids, limit=200):
    # One accepted-link lookup for the entire searched hit set, including the exact fetch case.
    await execute('''SELECT passage_id,source_id,illustration_id,data,physical_page_index,basis
        FROM (
            SELECT p.passage_id,p.source_id,i.illustration_id,i.data,pg.physical_page_index,
                CASE WHEN linked.explicit THEN 'accepted_chunk_link' ELSE 'exact_shared_region' END AS basis,
                row_number() OVER (PARTITION BY p.passage_id
                    ORDER BY pg.physical_page_index,i.illustration_id) AS figure_rank
            FROM rkb_v4.source_passage_provenance p
            JOIN rkb_v4.source_illustrations i ON i.source_id=p.source_id
            JOIN LATERAL (SELECT b.media FROM rkb_v4.source_batch_publications b
                WHERE b.source_id=i.source_id AND b.component='semantic'
                    AND b.figure_ids @> ARRAY[i.illustration_id]
                ORDER BY b.batch_index DESC LIMIT 1) publication ON true
            CROSS JOIN LATERAL (SELECT EXISTS (
                SELECT 1 FROM jsonb_array_elements(publication.media->'passage_links') link
                WHERE link->>'passage_id'=p.passage_id::text
                    AND link->'illustration_ids' @> jsonb_build_array(i.illustration_id::text)
            ) AS explicit) linked
            JOIN rkb_v4.source_stage_pages pg ON pg.source_id=i.source_id
                AND pg.page_id=(i.data->>'page_id')::uuid
            JOIN rkb_v4.sources s ON s.source_id=p.source_id
            JOIN rkb_v4.active_revision a ON a.source_id=p.source_id
                AND a.revision=p.source_revision
            WHERE p.passage_id=ANY(%s::uuid[]) AND p.source_id=ANY(%s::uuid[])
                AND s.accepted AND NOT s.withdrawn
                AND (linked.explicit
                    OR p.data->'region_ids' @> jsonb_build_array(i.data->'source_region_id')
                    OR i.data->'caption_region_ids' ?| ARRAY(
                        SELECT jsonb_array_elements_text(p.data->'region_ids')))
        ) ranked WHERE figure_rank<=%s ORDER BY passage_id,figure_rank''',
        (list(passage_ids), list(source_ids), limit+1))
    linked = {}
    for passage, source, illustration, data, physical, basis in await cursor.fetchall():
        linked.setdefault(str(passage), []).append(dict(
            id=f'rkb:illustration:v1:{source}:{illustration}', page_id=data['page_id'],
            physical_page_index=physical, association_basis=basis))
    return {key: (tuple(items[:limit]), len(items)>limit) for key, items in linked.items()}
