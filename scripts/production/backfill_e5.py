"""Operator-only resumable E5 backfill: active source hashes, deterministic batch4."""
import argparse,asyncio,hashlib,json,fcntl,os
from pathlib import Path
import httpx
from operator_env import load_service_env
from regional_knowledge.supabase_backend import backend_from_env
from regional_knowledge.e5_contract import SPACE,validate_vector

async def backfill(output):
    load_service_env();b=backend_from_env();stats={'space':SPACE,'encoded_batches':0,'written_vectors':0,'skipped_vectors':0,'external_embedding_calls':0}
    try:
        async with b.data_client._connection({'x-rkb-service':'1'}) as c:
            await c.execute('select pg_advisory_xact_lock(hashtext(%s))',('rkb-fast-e5-backfill',))
            rows=await(await c.execute('''select c.id,c.document_id,c.revision,c.text_start,c.text_end,c.text_sha256,c.metadata,o.object_key,e.text_sha256 as existing_hash,e.batch_sha256 as existing_batch,e.revision as existing_revision
              from public.rkb_chunks c join public.rkb_documents d on d.id=c.document_id join public.rkb_objects o on o.id=c.text_object_id
              left join public.rkb_chunk_embeddings_e5 e on e.chunk_id=c.id
              where c.revision=d.active_revision order by c.document_id,c.text_start,c.id''')).fetchall()
        stats['expected_active_chunks']=len(rows)
        # Process one document at a time; reproduce the original text_start/id batch4 order.
        groups=[]
        for row in rows:
            if not groups or groups[-1][0]['document_id']!=row['document_id'] or len(groups[-1])==4:groups.append([])
            groups[-1].append(row)
        cache={}
        async with httpx.AsyncClient(base_url='http://127.0.0.1:8767',timeout=4,trust_env=False) as client:
            for group in groups:
                fingerprint=hashlib.sha256(json.dumps([(str(r['id']),r['text_sha256'],r['revision']) for r in group],separators=(',',':')).encode()).hexdigest()
                if all(r['existing_hash']==r['text_sha256'] and r['existing_batch']==fingerprint and r['existing_revision']==r['revision'] for r in group):
                    stats['skipped_vectors']+=len(group);continue
                texts=[]
                for row in group:
                    key=row['object_key']
                    if key not in cache:cache[key]=await b.object_store.get_bytes(key)
                    raw=cache[key][row['text_start']:row['text_end']]
                    assert hashlib.sha256(raw).hexdigest()==row['text_sha256'],'source hash mismatch'
                    texts.append(raw.decode())
                response=await client.post('/embed',json={'space':SPACE,'role':'passage','texts':texts});response.raise_for_status();data=response.json()
                assert data['space']==SPACE and len(data['vectors'])==len(group)
                vectors=[validate_vector(v) for v in data['vectors']]
                async with b.data_client._connection({'x-rkb-service':'1'}) as c:
                    # Recheck source/revision atomically; never install a vector for changed/retired input.
                    for row,vector in zip(group,vectors):
                        result=await c.execute('''insert into public.rkb_chunk_embeddings_e5(chunk_id,embedding_space,revision,text_sha256,batch_sha256,embedding)
                          select c.id,%s,c.revision,c.text_sha256,%s,%s::vector(384) from public.rkb_chunks c join public.rkb_documents d on d.id=c.document_id
                          where c.id=%s and c.text_sha256=%s and c.revision=%s and c.revision=d.active_revision
                          on conflict(chunk_id) do update set embedding=excluded.embedding,embedding_space=excluded.embedding_space,revision=excluded.revision,text_sha256=excluded.text_sha256,batch_sha256=excluded.batch_sha256,updated_at=now()
                          where rkb_chunk_embeddings_e5.text_sha256 is distinct from excluded.text_sha256 or rkb_chunk_embeddings_e5.batch_sha256 is distinct from excluded.batch_sha256 or rkb_chunk_embeddings_e5.revision is distinct from excluded.revision''',(SPACE,fingerprint,'['+','.join(format(v,'.9g') for v in vector)+']',row['id'],row['text_sha256'],row['revision']))
                        stats['written_vectors']+=result.rowcount
                stats['encoded_batches']+=1
                if stats['encoded_batches']%25==0:print('backfill',stats['written_vectors'],flush=True)
        async with b.data_client._connection({'x-rkb-service':'1'}) as c:
            counts=await(await c.execute('''select count(*) as active_chunks,count(e.chunk_id) filter(where e.text_sha256=c.text_sha256 and e.revision=c.revision and e.embedding_space=%s) as valid_vectors from public.rkb_chunks c join public.rkb_documents d on d.id=c.document_id left join public.rkb_chunk_embeddings_e5 e on e.chunk_id=c.id where c.revision=d.active_revision''',(SPACE,))).fetchone()
        stats.update(counts);assert counts['active_chunks']==counts['valid_vectors'],'incomplete/stale active backfill'
        output.write_text(json.dumps(stats,indent=2));print(json.dumps(stats))
    finally:await b.aclose()
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('output',type=Path);a=p.parse_args()
    lock=os.open('/home/dev/.local/state/regional-knowledge-base/fast-e5-backfill.lock',os.O_RDWR|os.O_CREAT,0o600)
    try:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        asyncio.run(backfill(a.output))
    finally:os.close(lock)
