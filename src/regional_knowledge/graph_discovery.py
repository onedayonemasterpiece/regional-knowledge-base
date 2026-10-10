"""Bounded durable reverse discovery; every vector hit stays a candidate."""
import asyncio,json,logging,os,time
from uuid import UUID,uuid4,uuid5
from psycopg.types.json import Jsonb
from .contracts import Principal,PoiLocatorInput
from .entity_graph import GraphEvidence,normalize_alias,digest
from .graph_service import GraphService,locator
from .poi_reference import StreetStoryPoiResolver
log=logging.getLogger(__name__)

class GraphDiscoveryWorker:
    def __init__(self,backend):self.backend=backend;self.graph=GraphService(backend);self.poi_cursor=None
    async def claim(self):
        async with self.backend.data_client._connection({'x-rkb-service':'1'}) as db:
            await db.execute("update rkb_graph_discovery_jobs set state='failed',error_code='lease_attempts_exhausted' where state='running' and lease_until<now() and attempts>=5")
            row=await(await db.execute("""select * from rkb_graph_discovery_jobs where attempts<5 and
             ((state='pending' and available_at<=now()) or (state='running' and lease_until<now()))
             order by created_at,id for update skip locked limit 1""")).fetchone()
            if not row:return None
            claim=uuid4();await db.execute("update rkb_graph_discovery_jobs set state='running',claim=%s,attempts=attempts+1,lease_until=now()+interval '240 seconds' where id=%s",(claim,row['id']))
            return {**row,'claim':claim}
    def actor(self,job):
        return Principal(subject=str(job['actor_id'] or UUID(int=0)),client_id='graph-discovery',issuer='internal-actor-bridge',access_token='internal-actor-bridge')
    async def finish(self,job,*,error=None):
        async with self.backend.data_client._connection({'x-rkb-service':'1'}) as db:
            await db.execute("update rkb_graph_discovery_jobs set state=%s,available_at=now()+interval '30 seconds',error_code=%s,lease_until=null where id=%s and claim=%s",('pending' if error and job['attempts']<4 else 'failed' if error else 'done',error,job['id'],job['claim']))
    async def document(self,job,actor):
        offset=int(job['payload'].get('offset',0))
        async with self.graph.connection(actor) as db:
            nodes=await(await db.execute('select id from rkb_entities where owner_user_id=rkb_current_actor_id() and rkb_graph_active(document_id,revision) order by id offset %s limit 32',(offset,))).fetchall()
            for node in nodes:await self.graph.enqueue(db,actor.subject,node['id'],{'kind':'entity','version':f"document:{job['document_id']}:{job['revision']}"},job['document_id'],job['revision'])
            if len(nodes)==32:
                await db.execute("update rkb_graph_discovery_jobs set state='pending',claim=null,lease_until=null,payload=%s,attempts=0 where id=%s and claim=%s",(Jsonb({'kind':'document','offset':offset+32}),job['id'],job['claim']))
                return False
        return True
    async def entity(self,job,actor):
        try:
            graph=await self.graph.read(actor,job['entity_id'],20) if job['entity_id'] else {'entity':{'canonical_label':job['payload']['names'][0]},'aliases':[]}
        except LookupError:return # Revoked/replaced source: no authorized projection.
        names=list(dict.fromkeys([graph['entity']['canonical_label'],*[a['value'] for a in graph['aliases']],*job['payload'].get('names',[])]))[:20]
        if graph['entity'].get('external_ref'):
            try:
                canonical=await asyncio.to_thread(self.graph.resolver.version,graph['entity']['external_ref'])
                if canonical:names=list(dict.fromkeys([*names,*canonical['names']]))[:20]
            except Exception as error:log.info(json.dumps({'event':'graph_discovery_poi_alias_deferred','error_type':type(error).__name__}))
        aliases=[{'name':n,'kind':'historical'} for n in names]
        exact=await self.backend.client.post(self.backend.config.url.rstrip('/')+'/rest/v1/rpc/rkb_multilingual_rankings',headers=self.backend._headers(actor),json={'query_text':names[0],'bge_vector':None,'bge_space':None,'e5_vector':None,'e5_space':None,'aliases':aliases,'depth':100})
        exact.raise_for_status();signals={}
        for row in exact.json():signals.setdefault(str(row['chunk_id']),[]).append({k:row[k] for k in ('branch','rank','matched_alias')})
        result=await self.backend.search(names[0],actor,match_count=20,aliases=aliases,**({'main_job_id':job['payload']['main_job_id']} if job['payload'].get('main_job_id') else {}))
        if result.main_job_id:
            async with self.graph.connection(actor) as db:
                await db.execute('update rkb_graph_discovery_jobs set payload=%s where id=%s and claim=%s',(Jsonb({**job['payload'],'main_job_id':result.main_job_id}),job['id'],job['claim']))
        # Poll the SAME durable main encoding without repeated fast-tier inference/SQL.
        if result.main_state!='ready' and result.main_job_id:
            from .bge_queue import BgeQueue
            from .multilingual_retrieval import wait_result
            ready=await wait_result(BgeQueue(os.environ['RKB_BGE_QUEUE_PATH']),actor.subject,result.main_job_id,90)
            if ready:result=await self.backend.search(names[0],actor,match_count=20,aliases=aliases,main_job_id=result.main_job_id)
        for item in result.results:signals.setdefault(item.id,[]).extend([s.model_dump(mode='json') if hasattr(s,'model_dump') else s for s in item.ranking_signals])
        # Exact alias candidates precede vector-only neighbors; never semantic merge.
        ids=list(signals)[:40]
        async with self.graph.connection(actor) as db:
            rows=await(await db.execute('''select c.id,c.document_id,c.revision,c.page_ids,c.region_ids from rkb_chunks c join rkb_documents d on d.id=c.document_id
              where c.id=any(%s::uuid[]) and c.revision=d.active_revision and (%s::uuid is null or c.document_id=%s) and (%s::bigint is null or c.revision=%s)''',(ids,job['document_id'],job['document_id'],job['revision'],job['revision']))).fetchall()
        written=0;poi_candidates=[]
        for row in rows:
            if not row['region_ids'] or not row['page_ids']:continue
            text=(await self.backend.fetch(str(row['id']),actor)).text
            # Locate the quote in a verified source region, not an arbitrary region in a multi-region chunk.
            async with self.graph.connection(actor) as db:
                pairs=await(await db.execute('select r.id region_id,r.page_id from rkb_regions r where r.id=any(%s::uuid[]) and r.page_id=any(%s::uuid[]) order by r.id limit 32',(row['region_ids'],row['page_ids']))).fetchall()
            options=[]
            for pair in pairs:
                try:region=await self.graph.region_text(actor,row['document_id'],row['revision'],row['id'],pair['region_id'])
                except RuntimeError:continue
                matches=[n for n in names if normalize_alias(n) in normalize_alias(region)]
                options.append((bool(matches),pair,region,matches))
            if not options:continue
            _,pair,region,matching=max(options,key=lambda p:p[0])
            spelling='';quote=region[:1000].strip()
            if matching:
                import re
                m=re.search(r'\s+'.join(re.escape(part) for part in matching[0].split()),region,re.I)
                if m:spelling=m.group();quote=region[max(0,m.start()-100):min(len(region),m.end()+500)].strip()
            if not quote or quote not in text:continue
            async with self.graph.connection(actor) as db:
                e=GraphEvidence(chunk_id=row['id'],page_id=pair['page_id'],region_id=pair['region_id'],exact_quote=quote)
                # Retrieval does not establish that this passage mentions this
                # identity. Keep suggestions on the existing discovery job;
                # only model-authored graph_stage creates source mentions.
                poi_candidates.append({'evidence':locator(e),'exact_source_spelling':spelling,
                    'state':'candidate','ranking':signals[str(row['id'])],
                    'identity_unresolved':True,'entity_id':str(job['entity_id']) if job['entity_id'] else None,
                    'external_ref':job['payload'].get('external_ref')})
                written+=1
        async with self.graph.connection(actor) as db:
            current=await(await db.execute('select payload from rkb_graph_discovery_jobs where id=%s and claim=%s',(job['id'],job['claim']))).fetchone()
            if current:await db.execute('update rkb_graph_discovery_jobs set payload=%s where id=%s and claim=%s',(Jsonb({**current['payload'],'candidates':poi_candidates}),job['id'],job['claim']))
        log.info(json.dumps({'event':'graph_discovery_complete','job_id':str(job['id']),'candidates':written,'retrieval_mode':result.retrieval_mode,'automatic_merges':0}))

    async def sync_pois(self):
        # Bounded round-robin over references. Street Story remains alias owner.
        async with self.backend.data_client._connection({'x-rkb-service':'1'}) as db:
            nodes=await(await db.execute("select * from rkb_entities where kind='poi_ref' and (%s::uuid is null or id>%s) order by id limit 16",(self.poi_cursor,self.poi_cursor))).fetchall()
        if not nodes:self.poi_cursor=None;return
        self.poi_cursor=nodes[-1]['id']
        for node in nodes:
            try:
                ref=node['external_ref']
                if not ref:continue # Only the model may select a physical identity.
                version=await asyncio.to_thread(self.graph.resolver.version,ref)
                if not version or version['version']==node['metadata'].get('poi_alias_version'):continue
                actor=self.actor({'actor_id':node['owner_user_id']})
                async with self.graph.connection(actor) as db:
                    metadata={**node['metadata'],'poi_alias_version':version['version'],'resolution':'resolved'}
                    await db.execute('update rkb_entities set external_ref=%s,state=case when state=\'unresolved\' then \'candidate\' else state end,metadata=%s where id=%s',(ref,Jsonb(metadata),node['id']))
                    await self.graph.enqueue(db,actor.subject,node['id'],{'kind':'entity','version':'poi:'+version['version'],'names':version['names']})
                log.info(json.dumps({'event':'graph_poi_version_scheduled','entity_id':str(node['id'])}))
            except Exception as error:log.info(json.dumps({'event':'graph_poi_sync_deferred','error_type':type(error).__name__}))
    async def tick(self):
        job=await self.claim()
        if not job:return False
        try:
            actor=self.actor(job)
            if job['payload'].get('kind')=='document':
                if not await self.document(job,actor):return True
            else:await self.entity(job,actor)
            await self.finish(job)
        except Exception as error:
            await self.finish(job,error=type(error).__name__)
            log.warning(json.dumps({'event':'graph_discovery_retry','job_id':str(job['id']),'error_type':type(error).__name__}))
        return True

async def main():
    from .supabase_backend import backend_from_env
    from .geo_resolution import GeoQueue
    logging.basicConfig(level=logging.INFO);backend=backend_from_env();worker=GraphDiscoveryWorker(backend);last=0
    geo=GeoQueue(backend.corpus) if hasattr(backend,'corpus') else None
    last_geo=0
    try:
        while True:
            if time.monotonic()-last>30:
                if geo is not None:
                    try:await asyncio.to_thread(geo.poll_owner_updates,24)
                    except Exception as error:
                        log.warning(json.dumps({'event':'geo_owner_delta_degraded',
                                                'error_type':type(error).__name__}))
                await worker.sync_pois()
                last=time.monotonic()
            # Bounded catch-up of old graph mentions and one fenced owner
            # resolution; source ingest and BGE are never held behind geography.
            if geo is not None and time.monotonic()-last_geo>5:
                try:
                    await asyncio.to_thread(geo.backfill_batch,12)
                    await asyncio.to_thread(geo.worker_tick,2)
                except Exception as error:
                    log.warning(json.dumps({'event':'geo_queue_worker_degraded',
                                            'error_type':type(error).__name__}))
                last_geo=time.monotonic()
            if not await worker.tick():await asyncio.sleep(2)
    finally:await backend.aclose()
if __name__=='__main__':asyncio.run(main())
