"""Resumable owner-authorized local queue -> Kaggle CPU BGE backfill."""
import argparse,asyncio,fcntl,hashlib,json,os,time
from pathlib import Path
from operator_env import load_service_env
from accept_fast_e5 import principal
from regional_knowledge.bge_queue import BgeQueue
from regional_knowledge.bge_contract import SPACE,REVISION,validate_vector
from regional_knowledge.supabase_backend import backend_from_env

async def run(output):
    load_service_env();backend=backend_from_env();actor=principal();queue=BgeQueue(os.environ['RKB_BGE_QUEUE_PATH'])
    stats={'space':SPACE,'written':0,'skipped':0,'submitted':0,'external_embedding_api_calls':0};cache={};waiting={}
    try:
        async with backend.data_client._connection(backend._headers(actor)) as db:
            # BGE table and object metadata are both read under the ordinary
            # actor bridge/RLS. No cross-owner corpus enters Kaggle implicitly.
            rows=await(await db.execute('''select c.id,c.document_id,c.text_object_id,c.revision,c.text_sha256,c.text_start,c.text_end,e.text_sha256 existing_hash,e.revision existing_revision
             from rkb_chunks c join rkb_documents d on d.id=c.document_id
             left join rkb_chunk_embeddings_bge e on e.chunk_id=c.id where c.revision=d.active_revision order by c.document_id,c.text_start,c.id''')).fetchall()
        stats['authorized_active_chunks']=len(rows)
        # Object locators deliberately have no user SELECT policy. Follow the
        # existing fetch boundary: authorized chunk first, then a narrowly bound
        # service lookup for precisely those object+document identities.
        async with backend.data_client._connection({'x-rkb-service':'1'}) as db:
            objects=await(await db.execute('select id,document_id,object_key from rkb_objects where id=any(%s::uuid[])',([row['text_object_id'] for row in rows],))).fetchall()
        locators={(row['id'],row['document_id']):row['object_key'] for row in objects}
        async def install(row,result):
            if result['space']!=SPACE:raise ValueError('BGE backfill result space mismatch')
            vector=validate_vector(result['vectors'][0]);literal='['+','.join(format(value,'.9g') for value in vector)+']'
            async with backend.data_client._connection({'x-rkb-service':'1'}) as db:
                await db.execute("select set_config('rkb.actor_id',%s,true)",(actor.subject,))
                cursor=await db.execute('''insert into rkb_chunk_embeddings_bge(chunk_id,embedding_space,model_revision,revision,text_sha256,embedding)
                 select c.id,%s,%s,c.revision,c.text_sha256,%s::vector(1024) from rkb_chunks c join rkb_documents d on d.id=c.document_id
                 where c.id=%s and c.revision=%s and c.text_sha256=%s and c.revision=d.active_revision and rkb_can_read_document(c.document_id)
                 on conflict(chunk_id) do update set embedding=excluded.embedding,revision=excluded.revision,text_sha256=excluded.text_sha256,updated_at=now()
                 where rkb_chunk_embeddings_bge.text_sha256 is distinct from excluded.text_sha256 or rkb_chunk_embeddings_bge.revision is distinct from excluded.revision''',
                 (SPACE,REVISION,literal,row['id'],row['revision'],row['text_sha256']))
                stats['written']+=cursor.rowcount
        async def drain(*,all_=False):
            deadline=time.monotonic()+3600
            while waiting:
                progressed=False
                for job_id,row in list(waiting.items()):
                    status=await asyncio.to_thread(queue.result,actor.subject,job_id)
                    if status['state']=='done':
                        await install(row,status['result']);del waiting[job_id];progressed=True
                        if stats['written']%25==0:
                            output.write_text(json.dumps(stats,indent=2));print(json.dumps(stats),flush=True)
                if not all_ and len(waiting)<64:return
                if time.monotonic()>deadline:raise TimeoutError('BGE backfill wait exceeded; durable jobs remain resumable')
                if not progressed:await asyncio.sleep(.2)
        for row in rows:
            if row['existing_hash']==row['text_sha256'] and row['existing_revision']==row['revision']:
                stats['skipped']+=1;continue
            key=locators[(row['text_object_id'],row['document_id'])]
            if key not in cache:cache[key]=await backend.object_store.get_bytes(key)
            raw=cache[key][row['text_start']:row['text_end']]
            if hashlib.sha256(raw).hexdigest()!=row['text_sha256']:raise ValueError('BGE source hash mismatch')
            identity={'chunk_id':str(row['id']),'revision':row['revision'],'text_sha256':row['text_sha256']}
            idempotency='bge:'+SPACE+':'+str(row['id'])+':'+str(row['revision'])+':'+row['text_sha256']
            job=await asyncio.to_thread(queue.enqueue,actor.subject,idempotency,[raw.decode()],kind='document',identity=identity)
            waiting[job]=row;stats['submitted']+=1
            if len(waiting)>=64:await drain()
        await drain(all_=True)
        async with backend.data_client._connection(backend._headers(actor)) as db:
            counts=await(await db.execute('''select count(*) active_chunks,count(e.chunk_id) filter(where e.embedding_space=%s and e.revision=c.revision and e.text_sha256=c.text_sha256) valid_bge_vectors from rkb_chunks c join rkb_documents d on d.id=c.document_id left join rkb_chunk_embeddings_bge e on e.chunk_id=c.id where c.revision=d.active_revision''',(SPACE,))).fetchone()
        if counts['active_chunks']!=counts['valid_bge_vectors']:raise RuntimeError('BGE authorized active coverage incomplete')
        stats.update(counts);output.write_text(json.dumps(stats,indent=2));print(json.dumps(stats),flush=True)
    finally:await backend.aclose()

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('output',type=Path);args=parser.parse_args()
    lock=os.open('/home/dev/.local/state/regional-knowledge-base/bge-backfill.lock',os.O_RDWR|os.O_CREAT,0o600)
    try:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB);asyncio.run(run(args.output))
    finally:os.close(lock)
