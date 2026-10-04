"""Destructive synthetic test only; requires explicit isolated test DB DSN."""
import argparse,json,uuid,hashlib
from pathlib import Path
import psycopg
from regional_knowledge.e5_contract import SPACE

def main():
 p=argparse.ArgumentParser();p.add_argument('dsn');p.add_argument('output',type=Path);a=p.parse_args()
 owner=str(uuid.uuid4());doc=str(uuid.uuid4());chunk=str(uuid.uuid4());vec='['+','.join(['1']+['0']*383)+']'
 with psycopg.connect(a.dsn,autocommit=True) as c:
  if not(c.info.host=='127.0.0.1' and c.info.port==54329 and c.info.dbname=='rkb_e5_test'):
   raise RuntimeError('isolated fixture DB required')
  c.execute('create extension if not exists vector')
  for role in ('anon','authenticated','rkb_app'):
   if not c.execute('select 1 from pg_roles where rolname=%s',(role,)).fetchone():c.execute(f'create role {role}')
  c.execute('create table public.rkb_documents(id uuid primary key,owner_user_id uuid,active_revision bigint)')
  c.execute('create table public.rkb_chunks(id uuid primary key,document_id uuid references rkb_documents,revision bigint,title text,page_ids uuid[],illustration_ids uuid[],fts tsvector,text_sha256 text,embedding halfvec(768))')
  c.execute("create function public.rkb_can_read_document(uuid) returns boolean language sql stable as $$ select exists(select 1 from public.rkb_documents d where d.id=$1 and d.owner_user_id::text=current_setting('rkb.actor_id',true)) $$")
  c.execute('grant usage on schema public to rkb_app');c.execute('grant select on rkb_documents,rkb_chunks to rkb_app')
  c.execute('alter table rkb_documents enable row level security');c.execute("create policy readable_docs on rkb_documents for select using(owner_user_id::text=current_setting('rkb.actor_id',true))")
  c.execute('alter table rkb_chunks enable row level security');c.execute('create policy readable_chunks on rkb_chunks for select using(rkb_can_read_document(document_id))')
  c.execute('insert into rkb_documents values(%s,%s,1)',(doc,owner));c.execute("insert into rkb_chunks values(%s,%s,1,'synthetic',array[]::uuid[],array[]::uuid[],to_tsvector('simple','synthetic'),%s,null)",(chunk,doc,'a'*64))
  migration=Path('sql/010_fast_e5.sql').read_text();c.execute(migration);c.execute(migration)
  c.execute('insert into rkb_chunk_embeddings_e5(chunk_id,embedding_space,revision,text_sha256,batch_sha256,embedding) values(%s,%s,1,%s,%s,%s::vector)',(chunk,SPACE,'a'*64,'b'*64,vec))
  for value,space in [('[1,0]',SPACE),(vec,'wrong-space')]:
   try:c.execute('select * from rkb_fast_e5_search(%s,%s,%s,10)',('query',value,space))
   except psycopg.Error:pass
   else:raise AssertionError('incompatible query accepted')
  c.execute('set role rkb_app');c.execute("select set_config('rkb.actor_id',%s,false)",(owner,));assert len(c.execute('select * from rkb_fast_e5_search(%s,%s,%s,10)',('query',vec,SPACE)).fetchall())==1
  c.execute("select set_config('rkb.actor_id',%s,false)",(str(uuid.uuid4()),));assert c.execute('select * from rkb_fast_e5_search(%s,%s,%s,10)',('query',vec,SPACE)).fetchall()==[]
  c.execute("select set_config('rkb.actor_id',%s,false)",(owner,))
  fused=c.execute('select score,retrieval_mode from rkb_fast_e5_search(%s,%s,%s,10)',('synthetic',vec,SPACE)).fetchone();assert abs(fused[0]-2/61)<1e-12 and fused[1]=='fast_e5'
  lexical=c.execute('select score,retrieval_mode from rkb_fast_e5_search(%s,null,null,10)',('synthetic',)).fetchone();assert abs(lexical[0]-1/61)<1e-12 and lexical[1]=='lexical_only'
  c.execute('reset role');c.execute('update rkb_chunk_embeddings_e5 set text_sha256=%s',('c'*64,));assert c.execute('select * from rkb_fast_e5_search(%s,%s,%s,10)',('query',vec,SPACE)).fetchall()==[]
  c.execute(Path('sql/010_fast_e5.rollback.sql').read_text());assert c.execute('select count(*) from rkb_chunks').fetchone()[0]==1;assert c.execute("select to_regclass('public.rkb_chunk_embeddings_e5')").fetchone()[0] is None
  result={'migration_sha256':hashlib.sha256(migration.encode()).hexdigest(),'rollback_sha256':hashlib.sha256(Path('sql/010_fast_e5.rollback.sql').read_bytes()).hexdigest(),'postgres':c.execute('select version()').fetchone()[0],'pgvector':c.execute("select extversion from pg_extension where extname='vector'").fetchone()[0],'migration_twice':True,'dimension_and_space_rejection':True,'owner_and_denied_actor':True,'stale_hash_excluded':True,'rrf60_fusion':True,'lexical_only_without_vector':True,'rollback_preserves_legacy_chunks':True}
 a.output.write_text(json.dumps(result,indent=2));print(json.dumps(result))
if __name__=='__main__':main()
