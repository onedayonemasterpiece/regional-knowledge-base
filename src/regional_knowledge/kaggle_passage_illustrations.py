"""Accepted figures linked to one passage in its existing native read snapshot."""
from __future__ import annotations


async def read_source_illustration(request,cursor,execute):
    from .kaggle_protocol import EvidenceItem,EvidenceMetadata,ItemDependencies,SourceDependency
    from .kaggle_query import ReadUnavailable
    await execute('''SELECT i.data,pg.physical_page_index,v.eligibility_revision,session.ingestion_id
        FROM rkb_v4.source_illustrations i
        JOIN rkb_v4.sources s USING(source_id)
        JOIN rkb_v4.active_revision a USING(source_id)
        JOIN rkb_v4.source_visibility v USING(source_id)
        JOIN rkb_v4.source_stage_sessions session ON session.source_id=i.source_id
        JOIN rkb_v4.source_stage_pages pg ON pg.source_id=i.source_id
            AND pg.page_id=(i.data->>'page_id')::uuid
        WHERE i.source_id=%s::uuid AND i.illustration_id=%s::uuid
            AND s.accepted AND NOT s.withdrawn AND a.revision=1 LIMIT 1''',
        (request.source_id,request.query))
    row=await cursor.fetchone()
    if row is None:raise ReadUnavailable('evidence_withdrawn')
    data,physical,eligibility,ingestion=row
    figure={key:value for key,value in data.items() if key!='crop_binding'}
    figure.update(source_id=request.source_id,physical_page_index=physical,ingestion_id=str(ingestion))
    evidence=EvidenceItem(id=request.query,kind='source',
        text=(data.get('caption_text') or data.get('visual_description') or data['illustration_key'])[:2000],
        metadata=EvidenceMetadata(locator='source:'+request.source_id),
        dependencies=ItemDependencies(source_ids=(request.source_id,),variant_refs=(),grant_refs=(),
            source_revisions=(SourceDependency(source_id=request.source_id,content_revision=1,
                eligibility_revision=eligibility),)))
    return (figure,),(evidence,),None


async def read_passage_illustrations(cursor, execute, *, source_id, passage_id):
    # Select the accepted figure relation, not every picture on a nearby page.
    # Representation hashes and archive readiness do not decide discoverability.
    await execute('''SELECT i.illustration_id,i.data,pg.physical_page_index,
            CASE WHEN linked.explicit THEN 'accepted_chunk_link' ELSE 'exact_shared_region' END
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
        WHERE p.source_id=%s::uuid AND p.passage_id=%s::uuid
            AND s.accepted AND NOT s.withdrawn
            AND (linked.explicit
                OR p.data->'region_ids' @> jsonb_build_array(i.data->'source_region_id')
                OR i.data->'caption_region_ids' ?| ARRAY(
                    SELECT jsonb_array_elements_text(p.data->'region_ids')))
        ORDER BY pg.physical_page_index,i.illustration_id LIMIT 200''',
        (source_id, passage_id))
    rows = await cursor.fetchall()
    fields = ('illustration_key', 'page_id', 'source_region_id', 'bbox', 'kind',
              'caption_text', 'caption_region_ids', 'visual_description',
              'visual_description_provenance', 'display_rotation_degrees')
    return [dict(id=f'rkb:illustration:v1:{source_id}:{illustration_id}',
                 illustration_id=str(illustration_id), source_id=source_id,
                 association_basis=basis,
                 physical_page_index=physical,
                 **{key: data[key] for key in fields if key in data})
            for illustration_id, data, physical, basis in rows]
