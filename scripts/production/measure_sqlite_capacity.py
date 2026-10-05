"""Mixed-size capacity measurement; temporary remote vectors, never production corpus."""
import argparse,asyncio,hashlib,json,math,os,random
from pathlib import Path
from uuid import uuid5,NAMESPACE_URL
import psycopg
from psycopg.rows import dict_row
from operator_env import load_service_env
from regional_knowledge.sqlite_corpus import SQLiteCorpus
from regional_knowledge.sqlite_data import defaults

def vector(rng,dim):
 v=[rng.uniform(-1,1) for _ in range(dim)];norm=math.sqrt(sum(x*x for x in v));return '['+','.join(str(x/norm) for x in v)+']'
async def run(root):
 load_service_env();root.mkdir(mode=0o700,parents=True,exist_ok=True);corpus=SQLiteCorpus(root/'capacity.sqlite3');rng=random.Random(20261005);rows=[];pages=chunks=0;report=[]
 async with await psycopg.AsyncConnection.connect(os.environ['KB_SUPABASE_SESSION_CONNECTION'],row_factory=dict_row) as db:
  await db.execute('create temp table capacity_anchors(id uuid primary key,document_id uuid,revision bigint,text_sha256 text,search_material_sha256 text,source_sha256 text,owner_user_id uuid)')
  for name,dim in [('e5',384),('bge',1024)]:
   await db.execute(f'create temp table capacity_{name}(chunk_id uuid primary key,embedding vector({dim}))')
   await db.execute(f'create index on capacity_{name} using hnsw(embedding vector_cosine_ops)')
  for i in range(100):
   doc=str(uuid5(NAMESPACE_URL,'rkb-capacity-source-'+str(i)));owner=str(uuid5(NAMESPACE_URL,'rkb-capacity-owner'));n=(1,3,8,25,60)[i%5];pages+=n
   corpus.put('rkb_documents',[{**defaults('rkb_documents'),'id':doc,'owner_user_id':owner,'title':'Synthetic capacity '+str(i),'page_count':n,'active_revision':1,'source_sha256':hashlib.sha256(doc.encode()).hexdigest()}])
   for p in range(n):
    pid=str(uuid5(NAMESPACE_URL,doc+'page'+str(p)));corpus.put('rkb_pages',[{**defaults('rkb_pages'),'id':pid,'document_id':doc,'revision':1,'physical_page_index':p}])
    for j in range(1+(i%3)):
     cid=str(uuid5(NAMESPACE_URL,pid+'chunk'+str(j)));text=('Synthetic fixture '+str(i)+' page '+str(p)+' fragment '+str(j)+' '+''.join(rng.choice('abcdefghijklmnopqrstuvwxyz ') for _ in range(180+(i%7)*110)));sha=hashlib.sha256(text.encode()).hexdigest();chunks+=1
     corpus.put('rkb_chunks',[{**defaults('rkb_chunks'),'id':cid,'document_id':doc,'revision':1,'source_text':text,'search_material':text,'text_sha256':sha,'search_material_sha256':sha,'page_ids':[pid]}])
     await db.execute('insert into capacity_anchors values(%s,%s,1,%s,%s,%s,%s)',(cid,doc,sha,sha,hashlib.sha256(doc.encode()).hexdigest(),owner))
     for name,dim in [('e5',384),('bge',1024)]:await db.execute(f'insert into capacity_{name} values(%s,%s::vector)',(cid,vector(rng,dim)))
   if i+1 in (10,25,50,100):
    remote=await(await db.execute("select sum(pg_total_relation_size(c.oid))::bigint allocated,sum(pg_relation_size(c.oid))::bigint heap from pg_class c where relnamespace=pg_my_temp_schema() and relname in ('capacity_anchors','capacity_e5','capacity_bge')")).fetchone()
    logical=await(await db.execute('select (select sum(pg_column_size(a)) from capacity_anchors a)+(select sum(pg_column_size(e)) from capacity_e5 e)+(select sum(pg_column_size(b)) from capacity_bge b) bytes')).fetchone()
    with corpus.connect() as local:local.execute('pragma wal_checkpoint(truncate)')
    report.append({'materials':i+1,'pages':pages,'chunks':chunks,'remote_allocated_bytes':remote['allocated'],'remote_logical_bytes':logical['bytes'],'sqlite_bytes':corpus.path.stat().st_size,'remote_bytes_per_chunk':remote['allocated']/chunks})
    (root/'capacity.json').write_text(json.dumps({'fixture':'mixed 1/3/8/25/60 pages; variable distinct text; normalized random vectors, both original dimensions; real HNSW allocation; capacity only, not quality','measurements':report},indent=2))
  await db.rollback()
 print(json.dumps(report,indent=2))
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--evidence',type=Path,required=True);asyncio.run(run(p.parse_args().evidence))
