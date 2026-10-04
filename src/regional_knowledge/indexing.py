"""Small active-index reconciler. Missing vectors + existing BGE jobs are state.

No new durable scheduler/queue, source parser, native model or paid provider.
Source reads use each document owner and ordinary actor RLS. E5 preserves the
per-document ordered batch4 contract; BGE preserves single-document job priority.
"""
import asyncio,hashlib,json,logging,os,time
from pathlib import Path
from uuid import UUID
from .contracts import Principal
from .local_e5 import LocalE5Embedder
from .e5_contract import SPACE as E5_SPACE,validate_vector as validate_e5
from .bge_contract import SPACE as BGE_SPACE,REVISION,validate_vector as validate_bge
from .bge_queue import BgeQueue
log=logging.getLogger(__name__)
POLL_SECONDS=5

VALID_E5="e.chunk_id is not null and e.embedding_space=%s and e.revision=c.revision and e.text_sha256=c.text_sha256"
VALID_BGE="b.chunk_id is not null and b.embedding_space=%s and b.model_revision=%s and b.revision=c.revision and b.text_sha256=c.text_sha256"

def fingerprint(rows):
    return hashlib.sha256(json.dumps([(str(r['id']),r['text_sha256'],r['revision']) for r in rows],separators=(',',':')).encode()).hexdigest()

def bge_key(row):
    return 'bge:'+BGE_SPACE+':'+str(row['id'])+':'+str(row['revision'])+':'+row['text_sha256']

def bge_identity(row):
    return {'chunk_id':str(row['id']),'revision':row['revision'],'text_sha256':row['text_sha256']}

def literal(vector):return '['+','.join(format(v,'.9g') for v in vector)+']'

class IndexReconciler:
    def __init__(self,backend,queue):
        if not isinstance(backend.embedder,LocalE5Embedder):raise RuntimeError('local E5 required; no provider fallback')
        self.backend=backend;self.queue=queue;self.cursor=None

    def actor(self,owner):return Principal(subject=str(owner),client_id='index-maintenance',issuer='internal-actor-bridge',access_token='internal-actor-bridge')

    async def documents(self):
        # System metadata selection only. No source bytes/locators leave this
        # boundary until the owner account and chunk are authorized below.
        async with self.backend.data_client._connection({'x-rkb-service':'1'}) as db:
            rows=await(await db.execute(f'''select distinct d.id,d.owner_user_id from rkb_documents d
             join rkb_users u on u.id=d.owner_user_id and u.status='active'
             join rkb_chunks c on c.document_id=d.id and c.revision=d.active_revision
             left join rkb_chunk_embeddings_e5 e on e.chunk_id=c.id
             left join rkb_chunk_embeddings_bge b on b.chunk_id=c.id
             where (not ({VALID_E5}) or not ({VALID_BGE}))
             and (%s::uuid is null or d.id>%s) order by d.id limit 4''',(E5_SPACE,BGE_SPACE,REVISION,self.cursor,self.cursor))).fetchall()
        if not rows:
            wrapped=self.cursor is not None;self.cursor=None
            if wrapped:return await self.documents()
        else:self.cursor=rows[-1]['id']
        return rows

    async def e5_groups(self,actor,document):
        async with self.backend.data_client._connection(self.backend._headers(actor)) as db:
            rows=await(await db.execute(f'''with ranked as (
             select c.id,c.document_id,c.revision,c.text_sha256,c.text_start,
              ((row_number() over(order by c.text_start,c.id))-1)/4 batch,
              ({VALID_E5}) valid,e.batch_sha256 existing_batch
             from rkb_chunks c join rkb_documents d on d.id=c.document_id
             left join rkb_chunk_embeddings_e5 e on e.chunk_id=c.id
             where d.id=%s and c.revision=d.active_revision), missing as (
              select distinct batch from ranked where not valid order by batch limit 2)
             select * from ranked where batch in(select batch from missing) order by batch,text_start,id''',(E5_SPACE,document))).fetchall()
        groups=[]
        for row in rows:
            if not groups or groups[-1][0]['batch']!=row['batch']:groups.append([])
            groups[-1].append(row)
        return groups

    async def source(self,actor,row):
        result=await self.backend.fetch(str(row['id']),actor)
        if hashlib.sha256(result.text.encode()).hexdigest()!=row['text_sha256']:raise ValueError('source hash mismatch')
        return result.text

    async def install_e5(self,actor,group,vectors):
        batch=fingerprint(group);written=0
        async with self.backend.data_client._connection({'x-rkb-service':'1'}) as db:
            await db.execute("select set_config('rkb.actor_id',%s,true)",(actor.subject,))
            for row,vector in zip(group,vectors,strict=True):
                result=await db.execute('''insert into rkb_chunk_embeddings_e5(chunk_id,embedding_space,revision,text_sha256,batch_sha256,embedding)
                 select c.id,%s,c.revision,c.text_sha256,%s,%s::vector(384) from rkb_chunks c join rkb_documents d on d.id=c.document_id
                 join rkb_users u on u.id=d.owner_user_id and u.status='active'
                 where c.id=%s and c.revision=%s and c.text_sha256=%s and c.revision=d.active_revision and d.owner_user_id=rkb_current_actor_id() and rkb_can_read_document(c.document_id)
                 on conflict(chunk_id) do update set embedding_space=excluded.embedding_space,revision=excluded.revision,text_sha256=excluded.text_sha256,batch_sha256=excluded.batch_sha256,embedding=excluded.embedding,updated_at=now()
                 where rkb_chunk_embeddings_e5.embedding_space is distinct from excluded.embedding_space or rkb_chunk_embeddings_e5.revision is distinct from excluded.revision or rkb_chunk_embeddings_e5.text_sha256 is distinct from excluded.text_sha256 or rkb_chunk_embeddings_e5.batch_sha256 is distinct from excluded.batch_sha256''',(E5_SPACE,batch,literal(validate_e5(vector)),row['id'],row['revision'],row['text_sha256']))
                written+=result.rowcount
        return written

    async def e5(self,actor,document):
        written=0
        for group in await self.e5_groups(actor,document):
            texts=[await self.source(actor,row) for row in group]
            vectors=await self.backend.embedder.embed_passages(texts)
            written+=await self.install_e5(actor,group,vectors)
        return written

    async def install_bge(self,actor,row,result):
        if result['space']!=BGE_SPACE or len(result['vectors'])!=1:raise ValueError('BGE result contract')
        vector=validate_bge(result['vectors'][0])
        async with self.backend.data_client._connection({'x-rkb-service':'1'}) as db:
            await db.execute("select set_config('rkb.actor_id',%s,true)",(actor.subject,))
            cursor=await db.execute('''insert into rkb_chunk_embeddings_bge(chunk_id,embedding_space,model_revision,revision,text_sha256,embedding)
             select c.id,%s,%s,c.revision,c.text_sha256,%s::vector(1024) from rkb_chunks c join rkb_documents d on d.id=c.document_id
             join rkb_users u on u.id=d.owner_user_id and u.status='active'
             where c.id=%s and c.revision=%s and c.text_sha256=%s and c.revision=d.active_revision and d.owner_user_id=rkb_current_actor_id() and rkb_can_read_document(c.document_id)
             on conflict(chunk_id) do update set embedding_space=excluded.embedding_space,model_revision=excluded.model_revision,revision=excluded.revision,text_sha256=excluded.text_sha256,embedding=excluded.embedding,updated_at=now()
             where rkb_chunk_embeddings_bge.embedding_space is distinct from excluded.embedding_space or rkb_chunk_embeddings_bge.model_revision is distinct from excluded.model_revision or rkb_chunk_embeddings_bge.text_sha256 is distinct from excluded.text_sha256 or rkb_chunk_embeddings_bge.revision is distinct from excluded.revision''',(BGE_SPACE,REVISION,literal(vector),row['id'],row['revision'],row['text_sha256']))
            return cursor.rowcount

    async def bge(self,actor,document):
        async with self.backend.data_client._connection(self.backend._headers(actor)) as db:
            rows=await(await db.execute(f'''select c.id,c.document_id,c.revision,c.text_sha256 from rkb_chunks c join rkb_documents d on d.id=c.document_id
             left join rkb_chunk_embeddings_bge b on b.chunk_id=c.id where d.id=%s and c.revision=d.active_revision and not ({VALID_BGE})
             order by c.text_start,c.id limit 16''',(document,BGE_SPACE,REVISION))).fetchall()
        submitted=written=0
        for row in rows:
            key=bge_key(row);job=await asyncio.to_thread(self.queue.lookup,actor.subject,key)
            if job:
                if job['kind']!='document' or job['identity']!=bge_identity(row):raise ValueError('BGE source identity mismatch')
                if job['state']=='done':written+=await self.install_bge(actor,row,job['result'])
                continue
            # Reserve interactive capacity: at most 64 unfinished document jobs
            # globally. Claim still strictly prioritizes queries over documents.
            if await asyncio.to_thread(self.queue.document_pending)>=64:break
            text=await self.source(actor,row)
            async with self.backend.data_client._connection(self.backend._headers(actor)) as db:
                active=await(await db.execute('''select c.id from rkb_chunks c join rkb_documents d on d.id=c.document_id
                 where c.id=%s and c.revision=%s and c.text_sha256=%s and c.revision=d.active_revision and d.owner_user_id=rkb_current_actor_id() for share of d''',(row['id'],row['revision'],row['text_sha256']))).fetchone()
                if not active:continue
                await asyncio.to_thread(self.queue.enqueue,actor.subject,key,[text],kind='document',identity=bge_identity(row));submitted+=1
        return submitted,written

    async def tick(self):
        stats={'e5_written':0,'bge_submitted':0,'bge_written':0,'errors':[]}
        for document in await self.documents():
            actor=self.actor(document['owner_user_id'])
            # An E5 outage must not prevent durable BGE enqueue/install.
            for space in ('e5','bge'):
                try:
                    if space=='e5':stats['e5_written']+=await self.e5(actor,document['id'])
                    else:
                        submitted,written=await self.bge(actor,document['id']);stats['bge_submitted']+=submitted;stats['bge_written']+=written
                except Exception as error:
                    stats['errors'].append(type(error).__name__)
                    log.warning(json.dumps({'event':'indexing_retry','space':space,'document_id':str(document['id']),'error_type':type(error).__name__}))
        if any(stats[k] for k in ('e5_written','bge_submitted','bge_written')) or stats['errors']:
            log.info(json.dumps({'event':'indexing_progress',**stats,'external_embedding_api_calls':0}))
        return stats

def write_health(stats):
    path=Path(os.environ['RKB_INDEXING_HEALTH_PATH']);path.parent.mkdir(parents=True,exist_ok=True)
    value={'updated_at':time.time(),'pid':os.getpid(),'state':'degraded' if stats['errors'] else 'running' if any(stats[k] for k in ('e5_written','bge_submitted','bge_written')) else 'ready','error_type':stats['errors'][0] if stats['errors'] else None}
    temp=path.with_name(path.name+'.new');temp.write_text(json.dumps(value));temp.chmod(0o600);temp.replace(path)

async def run_loop(backend,queue):
    worker=IndexReconciler(backend,queue);await backend.data_client._ensure_open()
    # One independent connection owns LISTEN and a session advisory mutex.
    # It never holds source rows, model calls or private queue texts.
    async with backend.data_client.pool.connection() as listener:
        await listener.set_autocommit(True)
        lock=await(await listener.execute("select pg_try_advisory_lock(hashtext('rkb-index-maintenance')) locked")).fetchone()
        if not lock['locked']:raise RuntimeError('indexing owner already running')
        try:
            await listener.execute('listen rkb_index_activation')
            while True:
                try:stats=await worker.tick()
                except Exception as error:
                    stats={'e5_written':0,'bge_submitted':0,'bge_written':0,'errors':[type(error).__name__]};log.warning(json.dumps({'event':'indexing_pass_failed','error_type':type(error).__name__}))
                write_health(stats)
                async for _ in listener.notifies(timeout=POLL_SECONDS,stop_after=1):
                    log.info(json.dumps({'event':'indexing_activation_wakeup'}))
        finally:
            await listener.execute('unlisten rkb_index_activation');await listener.execute("select pg_advisory_unlock(hashtext('rkb-index-maintenance'))");await listener.set_autocommit(False)

async def main():
    from .supabase_backend import backend_from_env
    logging.basicConfig(level=logging.INFO);backend=backend_from_env()
    try:await run_loop(backend,BgeQueue(os.environ['RKB_BGE_QUEUE_PATH']))
    finally:await backend.aclose()
if __name__=='__main__':asyncio.run(main())
