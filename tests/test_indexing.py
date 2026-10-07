"""Real Postgres/RLS + durable SQLite queue indexing; no model downloads."""
import asyncio,hashlib,json,time,os
from uuid import uuid4,UUID
import httpx,psycopg,pytest
from test_graph_postgres import graph_db
from regional_knowledge.postgres_backend import PostgresBackend
from regional_knowledge.contracts import Principal
from regional_knowledge.local_e5 import LocalE5Embedder
from regional_knowledge.e5_contract import SPACE as E5_SPACE
from regional_knowledge.bge_contract import SPACE as BGE_SPACE
from regional_knowledge.bge_queue import BgeQueue
from regional_knowledge.indexing import IndexReconciler,run_loop,bge_key,bge_identity
from regional_knowledge.index_readiness import counts,status
from regional_knowledge.contracts import FetchOutput

V384=[1.0]+[0.0]*383;V1024=[1.0]+[0.0]*1023

@pytest.mark.asyncio
async def test_unavailable_mirror_grant_does_not_block_automatic_vectors(graph_db,tmp_path,monkeypatch):
    monkeypatch.setenv('RKB_REQUIRED_VECTOR_SPACES','e5,bge')
    monkeypatch.setenv('RKB_VIBEPUBLISH_GRANT_FILE',str(tmp_path/'unavailable-grant.json'))
    b,q,actor,doc,rows,calls=source_fixture(graph_db,tmp_path,n=1)
    try:
        result=await IndexReconciler(b,q).tick()
        assert result['e5_written']==result['bge_submitted']==1 and result['errors']==[]
    finally:
        async with b.data_client._connection({'x-rkb-service':'1'}) as db:
            await db.execute('update rkb_documents set active_revision=0 where id=%s',(doc,))
        await b.aclose()

def source_fixture(dsn,tmp_path,n=10,active=True):
    actor=Principal(subject=str(uuid4()),client_id='test',issuer='test',access_token='not-token');doc,obj,page=uuid4(),uuid4(),uuid4();raw=bytearray();chunks=[]
    with psycopg.connect(dsn,autocommit=True) as db:
        db.execute('insert into rkb_users(id) values(%s)',(UUID(actor.subject),))
        db.execute("insert into rkb_documents(id,owner_user_id,title,source_sha256,active_revision,page_count) values(%s,%s,'Synthetic index control',%s,%s,1)",(doc,UUID(actor.subject),'a'*64,1 if active else 0))
        db.execute("insert into rkb_objects(id,document_id,kind,object_key,sha256,mime_type,size_bytes) values(%s,%s,'text_projection',%s,%s,'text/plain',1)",(obj,doc,str(obj),'b'*64))
        db.execute('insert into rkb_pages(id,document_id,physical_page_index,width,height,revision) values(%s,%s,0,1000,1000,1)',(page,doc))
        for i in range(n):
            text=f'Synthetic transport indexprobe chunk {i}. Not historical evidence.';chunk,region=uuid4(),uuid4();start=len(raw);raw.extend(text.encode());sha=hashlib.sha256(text.encode()).hexdigest()
            db.execute("insert into rkb_regions(id,page_id,kind,bbox,reading_order,text_sha256) values(%s,%s,'body','{\"left\":0,\"top\":0,\"right\":1000,\"bottom\":1000}',%s,%s)",(region,page,i,sha))
            db.execute("insert into rkb_chunks(id,document_id,text_object_id,title,text_start,text_end,text_sha256,fts,page_ids,region_ids,revision) values(%s,%s,%s,'Synthetic',%s,%s,%s,to_tsvector('simple',%s),%s,%s,1)",(chunk,doc,obj,start,len(raw),sha,text,[page],[region]));raw.extend(b'\n\n');chunks.append({'id':chunk,'document_id':doc,'revision':1,'text_sha256':sha})
    class Store:
        async def get_range(self,key,start,end):return bytes(raw[start:end])
    calls=[]
    async def handle(request):
        if request.url.path=='/health':return httpx.Response(200,json={'ready':True,'space':E5_SPACE})
        data=json.loads(request.content);calls.append(data);return httpx.Response(200,json={'space':E5_SPACE,'vectors':[V384 for _ in data['texts']],'queue_wait_seconds':0,'encoder_seconds':0})
    embed=LocalE5Embedder();embed.client=httpx.AsyncClient(base_url='http://127.0.0.1:8767',transport=httpx.MockTransport(handle))
    backend=PostgresBackend(dsn,embedder=embed,object_store=Store(),pool_max_size=2);queue=BgeQueue(tmp_path/'bge.sqlite3')
    return backend,queue,actor,doc,chunks,calls

def complete_documents(queue,actor):
    with queue.connect() as db:run=db.execute('select current_run from control').fetchone()[0]
    token=(queue.path.parent/'bge-worker-credentials'/run).read_text();queue.heartbeat(run,token,ready=True)
    done=[]
    while job:=queue.claim(run,token):
        queue.complete(run,token,job['id'],job['claim'],BGE_SPACE,[V1024],{});done.append(job)
    return done

@pytest.mark.asyncio
async def test_missing_vectors_batching_replay_recovery_and_safe_degradation(graph_db,tmp_path,monkeypatch):
    monkeypatch.setenv('RKB_REQUIRED_VECTOR_SPACES','e5,bge')
    monkeypatch.setenv('RKB_AUTO_INDEX_ENABLED','1');monkeypatch.setenv('RKB_BGE_ENABLED','1');monkeypatch.setenv('RKB_INDEXING_HEALTH_PATH',str(tmp_path/'health.json'))
    b,q,actor,doc,rows,calls=source_fixture(graph_db,tmp_path);monkeypatch.setenv('RKB_BGE_QUEUE_PATH',str(q.path))
    try:
        first=await IndexReconciler(b,q).tick();assert first['e5_written']==8 and first['bge_submitted']==10
        assert [len(c['texts']) for c in calls]==[4,4]
        c=await counts(b,actor);assert c=={'active_chunks':10,'e5_ready':8,'bge_ready':0}
        async with b.data_client._connection(b._headers(actor)) as db:
            ranks=await(await db.execute("select * from rkb_multilingual_rankings('indexprobe',null,null,%s,%s,'[]'::jsonb,20)",(json.dumps(V384),E5_SPACE))).fetchall()
            assert not any(r['branch']=='e5' for r in ranks) # SQL snapshot guard, even with a supplied vector.
        partial=await b.search('indexprobe',actor);assert partial.retrieval_mode=='lexical_only' and partial.main_state=='pending' and partial.indexing.e5_missing==2
        assert len(calls)==2 # No wasted query encoding under incomplete coverage.
        # A new worker recovers from PostgreSQL missing state + existing SQLite
        # jobs without another queue or re-encoding unchanged batches.
        resumed=IndexReconciler(b,q);again=await resumed.tick();assert again['e5_written']==2 and again['bge_submitted']==0
        assert [len(c['texts']) for c in calls]==[4,4,2]
        fast=await b.search('indexprobe',actor);assert fast.retrieval_mode=='fast_e5' and fast.main_state=='pending'
        # Query priority is retained ahead of all older import jobs.
        interactive=q.enqueue(actor.subject,'interactive',['query']);done=complete_documents(q,actor);assert done[0]['id']==interactive and all(j['kind']=='document' for j in done[1:])
        final=await IndexReconciler(b,q).tick();assert final['e5_written']==0 and final['bge_written']==10 and final['bge_submitted']==0
        ready=await status(b,actor);assert ready.e5_missing==ready.bge_missing==0 and ready.effective_retrieval_mode=='bge'
        monkeypatch.setenv('RKB_BGE_WARM_MODE','e5_bge_lexical')
        fused_ready=await status(b,actor)
        assert fused_ready.e5_missing==fused_ready.bge_missing==0
        assert fused_ready.effective_retrieval_mode=='e5_bge_lexical'
        monkeypatch.setenv('RKB_BGE_WARM_MODE','bge_lexical')
        query=q.enqueue(actor.subject,'ready-query',['indexprobe'],identity={'query_sha256':hashlib.sha256(b'indexprobe').hexdigest()});complete_documents(q,actor)
        main=await b.search('indexprobe',actor,main_job_id=query);assert main.retrieval_mode=='bge_lexical' and main.main_state=='ready'
        replay=await IndexReconciler(b,q).tick();assert replay['e5_written']==replay['bge_written']==replay['bge_submitted']==0
        # Completed production document jobs are recovery state only; once
        # durably installed they are retired instead of becoming a second vector store.
        with q.connect() as db:assert db.execute("select count(*) from jobs where kind='document'").fetchone()[0]==0
        async with b.data_client._connection({'x-rkb-service':'1'}) as db:
            await db.execute('delete from rkb_chunk_embeddings_e5 where chunk_id=%s',(rows[0]['id'],))
        repair=await IndexReconciler(b,q).tick();assert repair['e5_written']==1 and repair['bge_submitted']==0
        assert len(calls[-1]['texts'])==4 # Preserve the original batch, write only the missing row.
        # Ready source rev replaced/archived while inference was in flight.
        async with b.data_client._connection(b._headers(actor)) as db:await db.execute('update rkb_documents set active_revision=0 where id=%s',(doc,))
        assert (await counts(b,actor))=={'active_chunks':0,'e5_ready':0,'bge_ready':0}
        assert await IndexReconciler(b,q).install_bge(actor,rows[0],{'space':BGE_SPACE,'vectors':[V1024]})==0
        async with b.data_client._connection(b._headers(actor)) as db:assert (await(await db.execute('select count(*) n from rkb_chunk_embeddings_bge where chunk_id=any(%s::uuid[])',([r['id'] for r in rows],))).fetchone())['n']==10
    finally:await b.aclose()

@pytest.mark.asyncio
async def test_activation_wakes_running_loop_and_owner_restart(graph_db,tmp_path,monkeypatch):
    monkeypatch.setenv('RKB_REQUIRED_VECTOR_SPACES','e5,bge')
    monkeypatch.setenv('RKB_INDEXING_HEALTH_PATH',str(tmp_path/'health.json'));b,q,actor,doc,rows,calls=source_fixture(graph_db,tmp_path,n=2,active=False)
    task=asyncio.create_task(run_loop(b,q))
    try:
        for _ in range(100):
            if (tmp_path/'health.json').exists():break
            await asyncio.sleep(.01)
        assert not calls
        async with b.data_client._connection(b._headers(actor)) as db:await db.execute('update rkb_documents set active_revision=1 where id=%s',(doc,))
        for _ in range(200):
            if (await counts(b,actor))['e5_ready']==2:break
            await asyncio.sleep(.01)
        assert (await counts(b,actor))['e5_ready']==2 and len(calls)==1
        task.cancel();await asyncio.gather(task,return_exceptions=True)
        # Stop may occur between E5 installation and SQLite enqueue. Restart
        # must recover both windows, including a completely absent BGE job.
        task=asyncio.create_task(run_loop(b,q))
        for _ in range(200):
            if q.document_pending()==2:break
            await asyncio.sleep(.01)
        assert q.document_pending()==2
        complete_documents(q,actor)
        for _ in range(400):
            if (await counts(b,actor))['bge_ready']==2:break
            await asyncio.sleep(.02)
        assert (await counts(b,actor))['bge_ready']==2 and len(calls)==1
        # Exact activation replay produces no document job duplicate. Completed
        # document jobs have already been retired after durable installation.
        async with b.data_client._connection(b._headers(actor)) as db:await db.execute('update rkb_documents set active_revision=1 where id=%s',(doc,))
        with q.connect() as db:assert db.execute("select count(*) from jobs where kind='document'").fetchone()[0]==0
    finally:
        task.cancel();await asyncio.gather(task,return_exceptions=True)
        async with b.data_client._connection(b._headers(actor)) as db:await db.execute('update rkb_documents set active_revision=0 where id=%s',(doc,))
        await b.aclose()

@pytest.mark.asyncio
async def test_actor_status_stale_vectors_and_source_hash_fail_closed(graph_db,tmp_path,monkeypatch):
    monkeypatch.setenv('RKB_REQUIRED_VECTOR_SPACES','e5,bge')
    b,q,actor,doc,rows,calls=source_fixture(graph_db,tmp_path,n=1)
    try:
        await IndexReconciler(b,q).tick()
        other=Principal(subject=str(uuid4()),client_id='test',issuer='test',access_token='no-token')
        assert (await counts(b,other))=={'active_chunks':0,'e5_ready':0,'bge_ready':0}
        with pytest.raises(LookupError):await counts(b,other,doc)
        async with b.data_client._connection({'x-rkb-service':'1'}) as db:await db.execute("update rkb_chunk_embeddings_e5 set text_sha256=%s where chunk_id=%s",('c'*64,rows[0]['id']))
        assert (await counts(b,actor))['e5_ready']==0
        worker=IndexReconciler(b,q)
        async def wrong(*args):return FetchOutput(id=str(rows[0]['id']),title='Synthetic',text='wrong source',url='knowledge://synthetic')
        monkeypatch.setattr(b,'fetch',wrong)
        result=await worker.tick();assert result['errors'] and result['e5_written']==0
        assert (await counts(b,actor))['e5_ready']==0
    finally:
        async with b.data_client._connection(b._headers(actor)) as db:await db.execute('update rkb_documents set active_revision=0 where id=%s',(doc,))
        await b.aclose()

def test_no_external_fallback_and_long_document_payload(tmp_path,monkeypatch):
    from regional_knowledge.supabase_backend import _embedder_from_env,LexicalOnlyEmbedder
    monkeypatch.setenv('RKB_AUTO_INDEX_ENABLED','1');monkeypatch.setenv('RKB_FAST_E5_ENABLED','0')
    for key in ('ENDPOINT','API_KEY','MODEL','SPACE'):monkeypatch.setenv('RKB_EMBEDDING_'+key,'configured')
    with pytest.raises(RuntimeError,match='fallback forbidden'):_embedder_from_env()
    q=BgeQueue(tmp_path/'bound.sqlite3');q.enqueue('actor','long-document',['x'*40000],kind='document')
    with pytest.raises(ValueError):q.enqueue('actor','long-query',['x'*16001])
    with pytest.raises(ValueError):q.enqueue('actor','too-long',['x'*40001],kind='document')
