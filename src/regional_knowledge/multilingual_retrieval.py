"""Main-tier query jobs, explicit rank fusion and ACL-bound result hydration."""
import asyncio,hashlib,json,logging,os,time,uuid
from .bge_queue import BgeQueue
from .bge_contract import SPACE as BGE_SPACE,validate_vector
from .e5_contract import SPACE as E5_SPACE
from .contracts import SearchOutput,SearchResult
from .rank_fusion import MODES,fuse
from .local_e5 import encoding_timings
logger=logging.getLogger(__name__)

async def wait_result(queue,actor,job_id,seconds):
    deadline=time.monotonic()+seconds
    while True:
        row=await asyncio.to_thread(queue.result,actor,job_id)
        if row['state']=='done':return row
        if time.monotonic()>=deadline:return None
        await asyncio.sleep(.05)

async def main_search(backend,query,principal,*,match_count=8,main_job_id=None,aliases=None):
    started=time.monotonic();queue=BgeQueue(os.environ['RKB_BGE_QUEUE_PATH'])
    mode=os.environ.get('RKB_BGE_WARM_MODE','bge_lexical')
    if mode not in ('bge','bge_lexical','e5_bge','e5_bge_lexical'):raise RuntimeError('invalid BGE warm path')
    query_hash=hashlib.sha256(query.encode()).hexdigest()
    status=await asyncio.to_thread(queue.status)
    try:
        if main_job_id:
            job=await asyncio.to_thread(queue.result,principal.subject,main_job_id)
            if job['identity'].get('query_sha256')!=query_hash:raise ValueError('main job query mismatch')
        else:
            main_job_id=await asyncio.to_thread(queue.enqueue,principal.subject,'query:'+str(uuid.uuid4()),[query],identity={'query_sha256':query_hash})
            job=None
    except RuntimeError:
        fast=await backend.search(query,principal,match_count=match_count,_fast_only=True)
        logger.info(json.dumps({'event':'main_retrieval_degraded','state':'unavailable','retrieval_mode':fast.retrieval_mode}))
        return fast.model_copy(update={'main_state':'unavailable'})
    e5_task=None;e5_times={}
    async def encode_e5():
        vector=await backend.embedder.embed(query)
        return vector,dict(encoding_timings.get())
    if status['state']=='ready' and 'e5' in MODES[mode]:e5_task=asyncio.create_task(encode_e5())
    try:
        if not job or job['state']!='done':
            job=await wait_result(queue,principal.subject,main_job_id,10 if status['state']=='ready' else 0)
        if job is None:
            fast=await backend.search(query,principal,match_count=match_count,_fast_only=True)
            state='starting' if status['state'] in ('stopped','starting') else 'unavailable' if status['state']=='failed' else 'pending'
            logger.info(json.dumps({'event':'main_retrieval_pending','state':state,'job_id':main_job_id,'retrieval_mode':fast.retrieval_mode,'initial_seconds':time.monotonic()-started}))
            return fast.model_copy(update={'main_state':state,'main_job_id':main_job_id})
        bge=job['result']
        if bge['space']!=BGE_SPACE:raise ValueError('BGE query result space mismatch')
        bge_vector=validate_vector(bge['vectors'][0]);e5_vector=None
        if e5_task:
            try:e5_vector,e5_times=await e5_task
            except Exception:e5_vector=None
        elif 'e5' in MODES[mode]:
            try:e5_vector,e5_times=await encode_e5()
            except Exception:e5_vector=None
        database_start=time.monotonic()
        literal=lambda vector:'['+','.join(format(value,'.9g') for value in vector)+']' if vector is not None else None
        response=await backend.client.post(f'{backend.config.url.rstrip("/")}/rest/v1/rpc/rkb_multilingual_rankings',headers=backend._headers(principal),json={'query_text':query,'bge_vector':literal(bge_vector),'bge_space':BGE_SPACE,'e5_vector':literal(e5_vector),'e5_space':E5_SPACE if e5_vector else None,'aliases':aliases or [],'depth':100})
        response.raise_for_status();rows=response.json();branches=list(MODES[mode])
        if hasattr(backend,'corpus') and not any(r['branch'] in ('e5','bge') for r in rows):
            mode='lexical_only';branches=['lexical']
            if aliases:branches+=['exact_current_alias','exact_historical_alias','exact_alias']
        if aliases:branches+=['exact_current_alias','exact_historical_alias','exact_alias']
        fusion_start=time.monotonic();ids,diagnostics=fuse(rows,branches,limit=match_count)
        results=[]
        # Every row is already RLS-filtered; normal fetch later rechecks ACL and
        # revisions. Metadata lookup also uses the ordinary actor bridge.
        if hasattr(backend,'corpus'):
            titles=[{'id':ident,'title':backend.corpus.one('rkb_chunks',ident)['title']} for ident in ids if backend.corpus.one('rkb_chunks',ident)]
        else:
            async with backend.data_client._connection(backend._headers(principal)) as connection:
                titles=await(await connection.execute('select id,title from public.rkb_chunks where id=any(%s::uuid[])',(ids,))).fetchall()
        by_id={str(row['id']):row['title'] for row in titles}
        for chunk in ids:
            if chunk in by_id:results.append(SearchResult(id=chunk,title=by_id[chunk],url=backend._evidence_url(chunk),ranking_signals=diagnostics[chunk]))
        logger.info(json.dumps({'event':'main_retrieval_served','job_id':main_job_id,'space':BGE_SPACE,'retrieval_mode':mode,'results':len(results),'seconds':time.monotonic()-started}))
        return SearchOutput(results=results,mode='lexical_degraded' if mode=='lexical_only' else 'hybrid',retrieval_mode=mode,main_state='ready',main_job_id=main_job_id,timings={**e5_times,'bge_queue_seconds':bge['queue_seconds'],'bge_encoder_seconds':bge.get('encoder_seconds',0),'database_seconds':fusion_start-database_start,'fusion_metadata_seconds':time.monotonic()-fusion_start,'search_seconds':time.monotonic()-started})
    finally:
        if e5_task and not e5_task.done():
            e5_task.cancel()
            await asyncio.gather(e5_task,return_exceptions=True)
