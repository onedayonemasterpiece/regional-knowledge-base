"""Evidence-scoped graph persistence/navigation over the existing Postgres plane."""
from __future__ import annotations
import asyncio,json,logging,hashlib
from uuid import UUID,uuid5
from psycopg.types.json import Jsonb
from .entity_graph import GraphBundle,GraphAlias,entity_id,normalize_alias,digest
from .poi_reference import StreetStoryPoiResolver,canonical_poi_key
log=logging.getLogger(__name__)

def locator(e):
    return {**e.model_dump(mode='json'),'evidence_ref':'knowledge://chunks/'+str(e.chunk_id)}

class GraphService:
    def __init__(self,backend,resolver=None):
        if not hasattr(backend,'data_client'):raise RuntimeError('graph requires the production Postgres data plane')
        self.backend=backend;self.resolver=resolver or StreetStoryPoiResolver();self._regions={}
    def connection(self,principal):return self.backend.data_client._connection(self.backend._headers(principal))

    async def region_text(self,principal,document_id,revision,chunk_id,region_id):
        # The actor authorizes the exact chunk/region before internal object lookup.
        async with self.connection(principal) as db:
            row=await(await db.execute('select r.text_sha256,r.source_text from rkb_regions r join rkb_chunks c on r.id=any(c.region_ids) where c.id=%s and c.document_id=%s and c.revision=%s and r.id=%s',(UUID(str(chunk_id)),UUID(str(document_id)),revision,UUID(str(region_id))))).fetchone()
            if not row:raise LookupError('region evidence not found')
        if row.get('source_text') is not None:
            text=row['source_text']
            if hashlib.sha256(text.encode()).hexdigest()!=row['text_sha256']:raise RuntimeError('region text integrity failure')
            return text
        key=(str(document_id),revision)
        if key not in self._regions:
            async with self.backend.data_client._connection({'x-rkb-service':'1'}) as db:
                objects=await(await db.execute("select o.object_key,o.sha256 from rkb_ingestion_jobs j join rkb_objects o on o.id=j.staged_graph_object_id and o.document_id=j.document_id where j.document_id=%s and j.staged_revision=%s order by j.created_at desc limit 1",(UUID(str(document_id)),revision))).fetchall()
            texts={}
            for obj in objects:
                data=await self.backend.object_store.get_bytes(obj['object_key'])
                if hashlib.sha256(data).hexdigest()!=obj['sha256']:raise RuntimeError('source graph integrity failure')
                g=json.loads(data)
                for page in g.get('pages',[]):
                    for region in page.get('regions',[]):texts[str(region['region_id'])]=region.get('source_text','')
            if len(self._regions)>=8:self._regions.pop(next(iter(self._regions)))
            self._regions[key]=texts
        text=self._regions[key].get(str(region_id))
        if text is not None and hashlib.sha256(text.encode()).hexdigest()==row['text_sha256']:return text
        # Legacy projections can have one region exactly equal to the source chunk.
        chunk=(await self.backend.fetch(str(chunk_id),principal)).text
        if hashlib.sha256(chunk.encode()).hexdigest()==row['text_sha256']:return chunk
        raise RuntimeError('exact source region material unavailable')

    async def evidence(self,principal,document_id,revision,e,*,staged_texts=None):
        async with self.connection(principal) as db:
            row=await(await db.execute('select rkb_graph_check_evidence(%s,%s,%s) ok',(UUID(str(document_id)),revision,Jsonb(e.model_dump(mode='json'))))).fetchone()
            if not row['ok']:raise ValueError('graph evidence outside authorized document/revision/chunk/page/region')
        text=staged_texts.get(str(e.chunk_id)) if staged_texts is not None else (await self.backend.fetch(str(e.chunk_id),principal)).text
        if staged_texts is None:
            region=await self.region_text(principal,document_id,revision,e.chunk_id,e.region_id)
            if e.exact_quote not in region:raise ValueError("graph quote not in referenced source region")
        if text is None or e.exact_quote not in text:raise ValueError('graph exact quote does not match source bytes')

    async def stage(self,principal,document_id,revision,bundle,*,staged_texts=None):
        bundle=GraphBundle.model_validate(bundle) if not isinstance(bundle,GraphBundle) else bundle
        async with self.connection(principal) as db:
            row=await(await db.execute('select id,active_revision from rkb_documents where id=%s and owner_user_id=rkb_current_actor_id() for update',(UUID(str(document_id)),))).fetchone()
            if not row:raise PermissionError('graph writes require source ownership')
            if revision<1 or (staged_texts is None and row['active_revision']!=revision):raise ValueError('active revision required')
        evidence={digest(e.model_dump(mode='json')):e for n in bundle.entities for e in [n.evidence,*[a.evidence for a in n.aliases]]}
        evidence.update({digest(e.model_dump(mode='json')):e for r in bundle.relations for e in r.evidence})
        for e in evidence.values():await self.evidence(principal,document_id,revision,e,staged_texts=staged_texts)
        nodes={n.key:n for n in bundle.entities};ids={n.key:n.entity_id or entity_id(UUID(str(document_id)),n.key) for n in bundle.entities}
        resolved={}
        for n in bundle.entities:
            if n.exact_source_spelling not in n.evidence.exact_quote:raise ValueError('source spelling absent from exact quote')
            if n.kind=='poi_ref':
                try:resolved[n.key]=await asyncio.to_thread(self.resolver.resolve,n.poi_locator)
                except Exception as error:
                    # Materialization must not depend on external POI availability.
                    resolved[n.key]={'state':'unavailable','external_ref':None}
                    log.info(json.dumps({'event':'graph_poi_resolution_deferred','error_type':type(error).__name__}))
        async with self.connection(principal) as db:
            # Recheck exact locators and owner inside the committing transaction.
            doc=await(await db.execute('select id,active_revision from rkb_documents where id=%s and owner_user_id=rkb_current_actor_id() for update',(UUID(str(document_id)),))).fetchone()
            if not doc or (staged_texts is None and doc['active_revision']!=revision):raise ValueError('document revision changed')
            for n in bundle.entities:
                nid=ids[n.key];old=await(await db.execute('select * from rkb_entities where id=%s',(nid,))).fetchone()
                if old and (old['owner_user_id']!=UUID(principal.subject) or old['kind']!=n.kind):raise PermissionError('explicit identity reuse requires same owner/kind')
                if n.entity_id and not old:raise LookupError('entity not found')
                result=resolved.get(n.key,{});ref=result.get('external_ref');state='unresolved' if n.kind=='poi_ref' and not ref else n.state
                metadata={'review_note':n.review_note,'poi_locator':n.poi_locator.model_dump(mode='json') if n.poi_locator else None,'resolution':result.get('state')}
                if old and n.kind=='poi_ref' and ref and old['external_ref'] and str(old['external_ref'])!=ref:
                    raise ValueError('canonical POI identity conflict; explicit editorial review required')
                if old is None:
                    await db.execute('insert into rkb_entities(id,owner_user_id,kind,canonical_label,external_ref,document_id,revision,state,metadata) values(%s,%s,%s,%s,%s,%s,%s,%s,%s)',(nid,UUID(principal.subject),n.kind,n.canonical_label,ref,UUID(str(document_id)),revision,state,Jsonb(metadata)))
                elif not n.entity_id:
                    if old['canonical_label']!=n.canonical_label or old['document_id']!=UUID(str(document_id)):raise ValueError('entity key payload conflict')
                    if old['revision']!=revision:
                        if staged_texts is not None:
                            pending={**old['metadata'],'_next_revision':revision,'_next_metadata':metadata,'_next_state':state,'_next_external_ref':ref or old['external_ref']}
                            await db.execute('update rkb_entities set metadata=%s where id=%s',(Jsonb(pending),nid))
                        else:
                            await db.execute('update rkb_entities set revision=%s,external_ref=%s,state=%s,metadata=%s where id=%s',(revision,ref,state,Jsonb(metadata),nid))
                    elif n.kind=='poi_ref' and ref and not old['external_ref']:
                        # A POI missing during the first source stage can become
                        # known to Street Story after asynchronous owner review.
                        # Promote the SAME exact source-backed node and revision;
                        # do not replace/merge its identity, citations or edges.
                        if old['state'] not in ('unresolved','candidate'):
                            raise ValueError('Only unresolved POI references support identity refresh')
                        await db.execute('update rkb_entities set external_ref=%s,state=%s,metadata=%s where id=%s and external_ref is null',
                                         (ref,state,Jsonb(metadata),nid))
                # Explicit reuse does not overwrite seed identity/metadata from another book.
                mid=uuid5(nid,f'mention:{document_id}:{revision}:{digest(locator(n.evidence))}')
                await db.execute('insert into rkb_entity_mentions(id,entity_id,document_id,revision,chunk_id,page_id,region_id,exact_source_spelling,evidence,state) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) on conflict(id) do nothing',(mid,nid,UUID(str(document_id)),revision,n.evidence.chunk_id,n.evidence.page_id,n.evidence.region_id,n.exact_source_spelling,Jsonb(locator(n.evidence)),n.state))
                for a in n.aliases:await self._alias(db,nid,document_id,revision,a)
                await self.enqueue(db,principal.subject,nid,{'kind':'entity','version':digest(n.model_dump(mode='json'))})
            for r in bundle.relations:
                rid=uuid5(ids[r.source_key],f'edge:{ids[r.target_key]}:{r.kind}:{document_id}:{revision}:{digest(r.model_dump(mode="json"))}')
                await db.execute('insert into rkb_entity_relations(id,source_id,target_id,kind,document_id,revision,evidence,time_scope,state,review_note) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) on conflict(id) do nothing',(rid,ids[r.source_key],ids[r.target_key],r.kind,UUID(str(document_id)),revision,Jsonb([locator(e) for e in r.evidence]),r.time_scope,r.state,r.review_note))
        log.info(json.dumps({'event':'graph_staged','entities':len(ids),'relations':len(bundle.relations),'unresolved_pois':sum(not r.get('external_ref') for r in resolved.values())}))
        return {'entities':{k:str(v) for k,v in ids.items()},'relations':len(bundle.relations),'unresolved_pois':sum(not r.get('external_ref') for r in resolved.values())}

    async def _alias(self,db,nid,doc,rev,a):
        normal=normalize_alias(a.value)
        if not normal:raise ValueError('empty alias')
        aid=uuid5(nid,f'alias:{doc}:{rev}:{normal}')
        await db.execute('insert into rkb_entity_aliases(id,entity_id,value,normalized_value,language,alias_type,time_scope,document_id,revision,evidence) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) on conflict(entity_id,normalized_value,document_id,revision) do nothing',(aid,nid,a.value,normal,a.language,a.alias_type,a.time_scope,UUID(str(doc)),rev,Jsonb(locator(a.evidence))))

    async def enqueue(self,db,actor,nid,payload,document_id=None,revision=None):
        boundary=await(await db.execute("select md5(coalesce(string_agg(id::text||active_revision::text,',' order by id),'')) version from rkb_documents where active_revision>0")).fetchone()
        key=digest({'actor':actor,'entity':str(nid),'payload':payload,'boundary':boundary['version'],'doc':str(document_id),'revision':revision})
        jid=uuid5(UUID(actor),key)
        await db.execute('insert into rkb_graph_discovery_jobs(id,actor_id,entity_id,document_id,revision,job_key,payload) values(%s,%s,%s,%s,%s,%s,%s) on conflict(job_key) do nothing',(jid,UUID(actor),nid,document_id,revision,key,Jsonb(payload)))
        return str(jid)

    async def add_alias(self,principal,nid,alias):
        a=GraphAlias.model_validate(alias);nid=UUID(str(nid))
        async with self.connection(principal) as db:
            node=await(await db.execute('select * from rkb_entities where id=%s and owner_user_id=rkb_current_actor_id()',(nid,))).fetchone()
            if not node:raise LookupError('entity not found')
            row=await(await db.execute('select c.document_id,c.revision from rkb_chunks c join rkb_documents d on d.id=c.document_id where c.id=%s and c.revision=d.active_revision',(a.evidence.chunk_id,))).fetchone()
            if not row:raise LookupError('active evidence not found')
        await self.evidence(principal,row['document_id'],row['revision'],a.evidence)
        async with self.connection(principal) as db:
            await self._alias(db,nid,row['document_id'],row['revision'],a)
            jid=await self.enqueue(db,principal.subject,nid,{'kind':'entity','version':digest(a.model_dump(mode='json'))})
        return {'job_id':jid,'state':'candidate_discovery'}

    async def discover_poi(self,principal,external_ref):
        canonical_poi_key(external_ref)
        canonical=await asyncio.to_thread(self.resolver.version,external_ref)
        if not canonical:raise LookupError('canonical POI not found')
        async with self.connection(principal) as db:
            jid=await self.enqueue(db,principal.subject,None,{'kind':'poi','external_ref':external_ref,'version':canonical['version'],'names':canonical['names']})
        return {'job_id':jid,'state':'candidate_discovery',
                'canonical_poi_ref':external_ref,
                'identity_state':canonical.get('identity_state','candidate'),
                'historical_geometry':canonical.get('historical_geometry','not_verified')}

    async def job_read(self,principal,jid,limit=20):
        async with self.connection(principal) as db:
            job=await(await db.execute('select id,state,error_code,payload from rkb_graph_discovery_jobs where id=%s',(UUID(str(jid)),))).fetchone()
            if not job:raise LookupError('discovery job not found')
            candidates=[]
            for candidate in job['payload'].get('candidates',[])[:max(1,min(limit,20))]:
                e=candidate['evidence']
                row=await(await db.execute('select c.id from rkb_chunks c join rkb_documents d on d.id=c.document_id where c.id=%s and c.revision=d.active_revision',(UUID(e['chunk_id']),))).fetchone()
                if row:candidates.append(candidate)
        return {'job_id':job['id'],'state':job['state'],'error_code':job['error_code'],'candidates':candidates,'automatic_merges':0}

    async def read(self,principal,nid,limit=20):
        nid=UUID(str(nid));limit=max(1,min(int(limit),20))
        async with self.connection(principal) as db:
            node=await(await db.execute('select id,kind,canonical_label,external_ref,state from rkb_entities where id=%s and rkb_graph_active(document_id,revision)',(nid,))).fetchone()
            if not node:raise LookupError('entity not found')
            aliases=await(await db.execute('select value,language,alias_type,time_scope,evidence from rkb_entity_aliases where entity_id=%s and rkb_graph_active(document_id,revision) order by id limit %s',(nid,limit))).fetchall()
            mentions=await(await db.execute('select state,exact_source_spelling,evidence,signals from rkb_entity_mentions where entity_id=%s and rkb_graph_active(document_id,revision) order by id limit %s',(nid,limit))).fetchall()
            jobs=await(await db.execute('select id,state,attempts,error_code from rkb_graph_discovery_jobs where entity_id=%s order by created_at desc limit 5',(nid,))).fetchall()
            edges=await(await db.execute('''select e.id,e.source_id,e.target_id,e.kind,e.state,e.time_scope,e.evidence,
             n.id neighbor_id,n.kind neighbor_kind,n.canonical_label neighbor_label,n.external_ref
             from rkb_entity_relations e join rkb_entities n on n.id=case when e.source_id=%s then e.target_id else e.source_id end
             where (e.source_id=%s or e.target_id=%s) and rkb_graph_active(e.document_id,e.revision) and rkb_graph_active(n.document_id,n.revision)
             order by e.id limit %s''',(nid,nid,nid,limit+1))).fetchall()
        if node['external_ref']:
            try:
                identity=await asyncio.to_thread(self.resolver.version,node['external_ref'])
                node['external_identity_state']=identity.get('identity_state','candidate') if identity else 'unavailable'
            except Exception:node['external_identity_state']='unavailable'
        return {'entity':node,'aliases':[{**a,'state':'candidate'} for a in aliases],'mentions':mentions,'neighbors':edges[:limit],'truncated':len(edges)>limit,'max_hops':1,'discovery_jobs':jobs}

    async def related(self,principal,nid,query=None,limit=8):
        graph=await self.read(principal,nid,20)
        names=[{'name':a['value'],'kind':a['alias_type']} for a in graph['aliases']][:20]
        if graph['entity']['external_ref']:
            try:
                canonical=await asyncio.to_thread(self.resolver.version,graph['entity']['external_ref'])
                if canonical:names += [{'name':name,'kind':'historical'} for name in canonical['names']]
            except Exception as error:log.info(json.dumps({'event':'graph_related_poi_alias_deferred','error_type':type(error).__name__}))
        names=list({a['name']:a for a in names}.values())[:20]
        result=await self.backend.search(query or graph['entity']['canonical_label'],principal,match_count=max(1,min(limit,20)),aliases=names)
        return {'graph':graph,'retrieval':result.model_dump(mode='json')}
