"""Local ingestion identity, provenance validation and two-plane activation."""
import hashlib
import json
from uuid import UUID,uuid4
from .sqlite_corpus import canonical
from .sqlite_data import defaults
from .e5_contract import SPACE as E5_SPACE
from .bge_contract import SPACE as BGE_SPACE,REVISION


def start(ctx,p):
    actor=ctx.actor
    if actor is None:raise PermissionError('actor required')
    policy=p.get('p_duplicate_policy','reuse')
    if policy not in ('reuse','new_revision') or p['p_page_count']<1 or not p['p_source_file_id'].strip():raise ValueError('invalid ingestion start')
    jobs=[j for j in ctx.rows('rkb_ingestion_jobs') if j['owner_user_id']==actor]
    same=[j for j in jobs if j['source_file_id']==p['p_source_file_id'] and j['duplicate_policy']==policy]
    if same:
        j=same[0]
        if j['source_sha256']!=p['p_source_sha256']:raise ValueError('source_file_id bound to different bytes')
        return [{'ingestion_id':j['id'],'document_id':j['document_id']}]
    roots=[d for d in ctx.rows('rkb_documents') if d['owner_user_id']==actor and d['source_sha256']==p['p_source_sha256']]
    named=next((d for d in roots if d['id']==p['p_document_id']),None) if policy=='new_revision' else None
    if not named and len(roots)>1:raise ValueError('historical_source_identity_ambiguous')
    doc=named or (roots[0] if roots else None)
    if doc:
        previous=sorted([j for j in jobs if j['document_id']==doc['id']],key=lambda j:(j['staged_revision'],j['created_at'],j['id']),reverse=True)
        if previous and (policy=='reuse' or previous[0]['state']!='finalized'):
            return [{'ingestion_id':previous[0]['id'],'document_id':doc['id']}]
        rev=max([doc['active_revision'],*[j['staged_revision'] for j in previous]])+1
    else:
        doc={**defaults('rkb_documents'),'id':str(UUID(p['p_document_id'])),'owner_user_id':actor,'title':p['p_title'],'authors':p.get('p_authors') or [],'publication_year':p.get('p_publication_year'),'language':p.get('p_language'),'source_sha256':p['p_source_sha256'],'page_count':p['p_page_count']}
        ctx.check_write('rkb_documents',None,doc);ctx.corpus.put('rkb_documents',[doc],connection=ctx.db);rev=1
    job={**defaults('rkb_ingestion_jobs'),'id':str(UUID(p['p_ingestion_id'])),'owner_user_id':actor,'document_id':doc['id'],'source_file_id':p['p_source_file_id'],'source_sha256':p['p_source_sha256'],'state':'processing','cursor':'0','staged_revision':rev,'duplicate_policy':policy}
    ctx.corpus.put('rkb_ingestion_jobs',[job],connection=ctx.db)
    return [{'ingestion_id':job['id'],'document_id':doc['id']}]


def job_guard(ctx,p):
    d=ctx.one('rkb_documents',p['p_document_id']);j=ctx.one('rkb_ingestion_jobs',p['p_ingestion_id'])
    if not d or not j or ctx.actor!=d['owner_user_id'] or ctx.actor!=j['owner_user_id'] or j['document_id']!=d['id']:raise PermissionError('ingestion ownership required')
    if j['staged_revision']!=p['p_revision'] or not (j['state']=='ready' or j['state']=='processing' and j.get('cursor') in ('finalize','vectors')):raise ValueError('ingestion revision is not ready')
    if p['p_revision']<=d['active_revision']:raise ValueError('staged revision is not newer than selected revision')
    if d['source_sha256']!=j['source_sha256']:raise ValueError('source mismatch')
    return d,j


def insert_chunks(ctx,p):
    d,j=job_guard(ctx,p)
    if not isinstance(p['p_chunks'],list) or len(p['p_chunks'])>100:raise ValueError('invalid chunk batch')
    if p.get('p_text_object_id'):
        obj=ctx.one('rkb_objects',p['p_text_object_id'])
        if not obj or obj['document_id']!=d['id'] or obj['kind']!='text_projection':raise ValueError('text projection mismatch')
    for item in p['p_chunks']:
        c={**defaults('rkb_chunks'),**{k:v for k,v in item.items() if k not in ('embedding','normalized_text','fts')},'document_id':d['id'],'revision':p['p_revision'],'text_object_id':p.get('p_text_object_id')}
        for field,table in (('page_ids','rkb_pages'),('region_ids','rkb_regions'),('footnote_region_ids','rkb_regions'),('illustration_ids','rkb_illustrations')):
            for ident in c[field]:
                ref=ctx.one(table,ident);page=ctx.one('rkb_pages',ref['page_id']) if ref and table!='rkb_pages' else ref
                if not page or page['document_id']!=d['id'] or page['revision']!=p['p_revision']:raise ValueError('chunk reference escapes revision')
        old=ctx.one('rkb_chunks',c['id'])
        ctx.check_write('rkb_chunks',old,c)
        if old and any(old[k]!=c[k] for k in ('document_id','revision','text_sha256','search_material_sha256')):raise ValueError('chunk identity mismatch')
        ctx.corpus.put('rkb_chunks',[c],connection=ctx.db)
    return [{'inserted_count':len(p['p_chunks'])}]


def validate(ctx,d,rev,events):
    pages={p['id']:p for p in ctx.rows('rkb_pages',{'document_id':d['id'],'revision':rev})}
    regions={r['id']:r for r in ctx.rows('rkb_regions',{'page_id':list(pages)})}
    chunks=[c for c in ctx.rows('rkb_chunks',{'document_id':d['id'],'revision':rev})]
    illustrations={i['id']:i for i in ctx.rows('rkb_illustrations',{'document_id':d['id'],'page_id':list(pages)})}
    if len(pages)!=d['page_count'] or sorted(p['physical_page_index'] for p in pages.values())!=list(range(d['page_count'])):raise ValueError('revision page coverage incomplete')
    if not chunks or any(r.get('needs_review') for r in regions.values()):raise ValueError('revision is not accepted')
    covered=set()
    for c in chunks:
        refs=c['region_ids']+c['footnote_region_ids'];covered.update(refs)
        if not set(c['page_ids'])<=pages.keys() or not set(refs)<=regions.keys() or not set(c['illustration_ids'])<=illustrations.keys():raise ValueError('chunk provenance escapes revision')
        if any(regions[r]['page_id'] not in c['page_ids'] for r in refs):raise ValueError('chunk region/page mismatch')
    if any(r.get('text_sha256') and r['id'] not in covered for r in regions.values()):raise ValueError('textual region not covered')
    for i in illustrations.values():
        r=regions.get(i['source_region_id'])
        if not r or r['kind']!='figure' or r['page_id']!=i['page_id'] or any(regions.get(x,{}).get('page_id')!=i['page_id'] for x in i['caption_region_ids']+i['nearby_region_ids']):raise ValueError('illustration provenance escapes page')
    if not isinstance(events,list) or len(events)>500:raise ValueError('invalid POI event batch')
    for e in events:
        kind=e.get('contract_version');evidence=e.get('evidence',{});scope=e.get('scope',{});source=e.get('source',{})
        UUID(e['event_id'])
        if kind not in ('poi.fact_evidence.v1','poi.media_evidence.v1') or e.get('producer')!='regional_knowledge' or source.get('document_ref')!='knowledge://documents/'+d['id'] or source.get('revision')!=rev:raise ValueError('POI source mismatch')
        if scope.get('owner_sub')!=d['owner_user_id']:raise ValueError('POI owner mismatch')
        if kind=='poi.fact_evidence.v1' and (scope.get('visibility')!=d['content_visibility'] or (scope.get('workspace_id') or None)!=d.get('workspace_id')):raise ValueError('POI scope mismatch')
        eps=evidence.get('page_ids') or ([evidence['page_id']] if evidence.get('page_id') else [])
        ers=evidence.get('region_ids') or []
        if not eps or not ers or not set(eps)<=pages.keys() or any(regions.get(r,{}).get('page_id') not in eps for r in ers):raise ValueError('POI provenance mismatch')
        for name in ('provenance_precision_score','author_subject_authority','publication_method_score','evidence_verification_score'):
            value=evidence.get(name)
            if value is not None and (type(value)!=int or not 0<=value<=100):raise ValueError('POI score invalid')
        if kind=='poi.fact_evidence.v1':
            if type(evidence.get('provenance_precision_score'))!=int:raise ValueError('POI provenance score required')
            candidate=str(UUID(e['claim']['candidate_id']))
            if evidence.get('evidence_ref')!='knowledge://evidence/'+candidate:raise ValueError('POI evidence ref mismatch')
        else:
            media=e.get('media',{});iid=media.get('illustration_id');i=illustrations.get(iid)
            if not i or media.get('illustration_ref')!='knowledge://illustrations/'+iid or media.get('relation') not in ('depicts','illustrates','map_of','detail_of'):raise ValueError('POI illustration mismatch')
            if any(media.get(k)!=i.get(k) for k in ('page_id','source_region_id','source_crop_sha256','rights_status','visibility','caption_region_ids')) or scope.get('visibility')!=i['visibility'] or scope.get('visibility')=='workspace' and (scope.get('workspace_id') or None)!=d.get('workspace_id'):raise ValueError('POI media scope/provenance mismatch')
            if any(regions[r]['page_id']!=i['page_id'] for r in ers):raise ValueError('POI media evidence escapes page')
        family=(evidence.get('source_family_id') or '').strip()
        if not family or family.startswith('unresolved:'):evidence['source_family_id']='unknown'
        if not 1<=len(e.get('idempotency_key',''))<=300:raise ValueError('POI idempotency key required')
        for old in ctx.rows('rkb_integration_outbox'):
            if old['event_id']==e['event_id'] or old['idempotency_key']==e['idempotency_key']:
                if old['payload']!=e:raise ValueError('POI event identity conflict')
    return chunks


def manifest(chunks):return sorted([(c['id'],c['revision'],c['text_sha256'],c['search_material_sha256']) for c in chunks])

def ready(ctx,c):
    for table,space in (('rkb_chunk_embeddings_e5',E5_SPACE),('rkb_chunk_embeddings_bge',BGE_SPACE)):
        e=ctx.one(table,c['id'])
        if not e or e['embedding_space']!=space or any(e[k]!=c[k] for k in ('revision','text_sha256','search_material_sha256')) or space==BGE_SPACE and e.get('model_revision')!=REVISION:return False
    return True


def activate(ctx,p):
    existing=ctx.one('rkb_ingestion_jobs',p['p_ingestion_id']);doc=ctx.one('rkb_documents',p['p_document_id'])
    if existing and doc and existing['document_id']==doc['id'] and existing['owner_user_id']==ctx.actor==doc['owner_user_id'] and existing['staged_revision']==p['p_revision'] and existing['state']=='finalized':return [{'document_id':doc['id'],'active_revision':doc['active_revision'],'ingestion_state':'finalized'}]
    d,j=job_guard(ctx,p);rev=p['p_revision'];events=p.get('p_poi_events') or []
    chunks=validate(ctx,d,rev,events);frozen=canonical(manifest(chunks))
    previous=ctx.db.execute('select * from revision_publication where document_id=? and revision=?',(d['id'],rev)).fetchone()
    if previous and (previous['manifest']!=frozen or previous['source_sha256']!=d['source_sha256'] or previous['ingestion_id']!=j['id']):raise ValueError('publication identity changed')
    ctx.db.execute('insert into revision_publication values(?,?,?,?,?,?,?) on conflict(document_id,revision) do nothing',(d['id'],rev,j['id'],d['source_sha256'],frozen,canonical(events),'pending'))
    if not all(ready(ctx,c) for c in chunks):
        ctx.corpus.put('rkb_ingestion_jobs',[{**j,'state':'processing','cursor':'vectors'}],connection=ctx.db)
        return [{'document_id':d['id'],'active_revision':d['active_revision'],'ingestion_state':'processing','pending_vectors':True}]
    if previous:events=json.loads(previous['poi_events'])
    ctx.activation=True
    ctx.corpus.put('rkb_documents',[{**d,'active_revision':rev}],connection=ctx.db)
    ctx.corpus.put('rkb_ingestion_jobs',[{**j,'state':'finalized','cursor':None,'error_code':None}],connection=ctx.db)
    for node in ctx.rows('rkb_entities'):
        m=node.get('metadata') or {}
        if node['document_id']==d['id'] and m.get('_next_revision')==rev and any(x['entity_id']==node['id'] and x['document_id']==d['id'] and x['revision']==rev for x in ctx.rows('rkb_entity_mentions')):
            ctx.corpus.put('rkb_entities',[{**node,'revision':rev,'state':m.get('_next_state',node['state']),'external_ref':m.get('_next_external_ref',node.get('external_ref')),'metadata':m.get('_next_metadata',m)}],connection=ctx.db)
    key='document:'+d['id']+':'+str(rev)
    if not any(x['job_key']==key for x in ctx.rows('rkb_graph_discovery_jobs')):
        ctx.corpus.put('rkb_graph_discovery_jobs',[{**defaults('rkb_graph_discovery_jobs'),'actor_id':d['owner_user_id'],'document_id':d['id'],'revision':rev,'job_key':key,'payload':{'kind':'document','offset':0}}],connection=ctx.db)
    for e in events:
        if not any(x['event_id']==e['event_id'] for x in ctx.rows('rkb_integration_outbox')):
            ctx.corpus.put('rkb_integration_outbox',[{**defaults('rkb_integration_outbox'),'owner_user_id':d['owner_user_id'],'document_id':d['id'],'revision':rev,'target_service':'street_story','event_type':e['contract_version'],'event_id':e['event_id'],'idempotency_key':e['idempotency_key'],'visibility':e['scope']['visibility'],'workspace_id':e['scope'].get('workspace_id'),'payload':e,'state':'pending_delivery' if e['scope']['visibility']=='public' else 'pending_authorization'}],connection=ctx.db)
    ctx.db.execute("update revision_publication set state='ready' where document_id=? and revision=?",(d['id'],rev))
    return [{'document_id':d['id'],'active_revision':rev,'ingestion_state':'finalized'}]
