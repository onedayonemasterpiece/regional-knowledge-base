"""Resolve explicit prior references in working PG projections, without a cache."""
import base64
import copy
from uuid import UUID,uuid5

from .source_stage import validate_stage_payload,canonical


def chunk_pages(chunk):
    return {r['page_id'] for field in ('region_refs','span_refs','footnote_refs','illustration_refs')
            for r in chunk.get(field,[])}


async def load_current(connection,raw,source_id,text_only):
    doc=validate_stage_payload(raw,source_id=source_id)
    result=copy.deepcopy(doc);model=result['model_payload']
    if text_only:
        # Transcription can depend on an accepted page's trailing region.
        # Optional semantic input still never causes a text-phase lookup.
        model.update(entity_candidates=None,story_candidates=[],poi_facts=[],poi_media_links=[])
    graph=model.get('entity_candidates') or {}
    nodes={e['key']:e for e in graph.get('entities',[])}
    refs=dict(graph.get('entity_refs',{}));relations=graph.get('relations',[])
    stories=model.get('story_candidates',[])
    needed={k for s in stories for k in s.get('entity_keys',[])}
    needed.update(k for edge in relations for k in (edge['source_key'],edge['target_key']))
    missing=needed-nodes.keys()-refs.keys()
    if missing:
        rows=await (await connection.execute('''SELECT entity_key,entity_id FROM rkb_v4.source_entity_representations
            WHERE source_id=%s AND entity_key=ANY(%s::text[])
            UNION SELECT entity_key,entity_id FROM rkb_v4.source_entity_references
            WHERE source_id=%s AND entity_key=ANY(%s::text[])''',
            (source_id,sorted(missing),source_id,sorted(missing)))).fetchall()
        refs.update((key,str(ident)) for key,ident in rows)
        if needed-nodes.keys()-refs.keys():raise ValueError('native_source_context_entity_missing')
    proofs=[p for n in nodes.values() for p in [n['evidence'],*[a['evidence'] for a in n.get('aliases',[])]]]
    proofs.extend(p for edge in relations for p in edge['evidence'])
    current_chunks={str(uuid5(UUID(source_id),'chunk:1:'+c['chunk_key'])) for c in model['chunks']}
    missing_chunks={p['chunk_id'] for p in proofs}-current_chunks
    evidence=[r for s in stories for r in s['evidence_refs']]
    missing_chunks.update(r['chunk_id'] for r in evidence if r.get('chunk_id') and r['chunk_id'] not in current_chunks)
    pages={p['page_id']:(p,img) for p,img in zip(model['pages'],doc['issued_images_b64'],strict=True)}
    old_pages={r['page_id'] for r in evidence if not r.get('chunk_id')}-pages.keys()
    selections={}
    # Each predicate addresses an explicit passage UUID or one physical page;
    # provenance has its existing PK plus a GIN index, not a book-history scan.
    if missing_chunks:
        rows=await (await connection.execute('''SELECT passage_id,data FROM rkb_v4.source_passage_provenance
            WHERE source_id=%s AND passage_id=ANY(%s::uuid[])''',(source_id,sorted(missing_chunks)))).fetchall()
        selections.update((str(ident),data) for ident,data in rows)
        if missing_chunks-selections.keys():raise ValueError('native_source_context_chunk_missing')
    for page in sorted(old_pages):
        rows=await (await connection.execute('''SELECT passage_id,data FROM rkb_v4.source_passage_provenance
            WHERE source_id=%s AND data @> %s::jsonb LIMIT 501''',
            (source_id,canonical({'page_ids':[page]}).decode()))).fetchall()
        if len(rows)>500:raise ValueError('native_source_context_page_capacity')
        selections.update((str(ident),data) for ident,data in rows)
    required={p for selection in selections.values() for p in selection['page_ids']}
    required.update(r['page_id'] for r in evidence)
    required.update(p['page_id'] for p in proofs)
    for item in model.get('poi_facts',[]):required.update(r['page_id'] for r in item['evidence_refs'])
    for item in model.get('poi_media_links',[]):required.add(item['illustration_ref']['page_id'])
    required.update(p for c in model['chunks'] for p in chunk_pages(c))
    missing_pages=required-pages.keys()
    if missing_pages:
        rows=await (await connection.execute('''SELECT p.page_id,p.model_page,p.image_receipt FROM rkb_v4.source_stage_pages p
            WHERE p.source_id=%s AND p.page_id=ANY(%s::uuid[])
            AND EXISTS(SELECT 1 FROM rkb_v4.source_regions r WHERE r.source_id=p.source_id AND r.page_id=p.page_id)''',
            (source_id,sorted(missing_pages)))).fetchall()
        pages.update((str(ident),(page,base64.b64encode(bytes(image)).decode())) for ident,page,image in rows)
        if required-pages.keys():raise ValueError('native_source_context_page_missing')
    region_refs={str(uuid5(UUID(p['page_id']),'region:'+r['region_key'])):
        dict(page_id=p['page_id'],region_key=r['region_key']) for p,_ in pages.values() for r in p['regions']}
    chunks=copy.deepcopy(model['chunks']);context_ids={}
    for ident,selection in selections.items():
        if ident in current_chunks:continue
        key='prior:'+UUID(ident).hex
        chunk=dict(chunk_key=key,title='Previously accepted passage',
            footnote_refs=[region_refs[r] for r in selection['footnote_region_ids']])
        if selection['source_spans']:
            chunk['span_refs']=[dict(region_refs[s['region_id']],**{k:s[k] for k in ('start','end','source_text_sha256')})
                                for s in selection['source_spans']]
        else:chunk['region_refs']=[region_refs[r] for r in selection['region_ids']]
        chunks.append(chunk);context_ids[key]=ident
    ordered=sorted(pages.values(),key=lambda row:row[0]['physical_page_index'])
    result['issued_images_b64']=[img for _,img in ordered]
    result['model_payload']['pages']=[p for p,_ in ordered]
    result['model_payload']['chunks']=chunks
    result['model_payload']['entity_candidates']=dict(entities=list(nodes.values()),entity_refs=refs,relations=relations)
    result['context_chunk_ids']=context_ids
    return result
