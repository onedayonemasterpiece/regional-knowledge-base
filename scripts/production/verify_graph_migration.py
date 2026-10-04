"""Synthetic isolated PostgreSQL proof; never runs against the production DSN."""
import argparse,json,hashlib
from pathlib import Path
from uuid import uuid4
import psycopg

def main():
 p=argparse.ArgumentParser();p.add_argument('output',type=Path);a=p.parse_args()
 with psycopg.connect('postgresql://postgres:graph_fixture_only@127.0.0.1:54329/rkb_graph_test',autocommit=True) as db:
  assert db.info.host=='127.0.0.1' and db.info.dbname=='rkb_graph_test'
  db.execute('drop schema public cascade;create schema public;grant usage on schema public to public')
  db.execute('create schema if not exists auth;create table if not exists auth.users(id uuid primary key);create or replace function auth.uid() returns uuid language sql as $$ select nullif(current_setting(\'rkb.actor_id\',true),\'\')::uuid $$')
  for role in ('anon','authenticated','service_role','rkb_app'):
   if not db.execute('select 1 from pg_roles where rolname=%s',(role,)).fetchone():db.execute('create role '+role)
  for path in sorted(Path('sql').glob('0*.sql')):
   if path.name.endswith('rollback.sql') or path.name.startswith('012'):continue
   db.execute(path.read_text())
  migration=Path('sql/012_entity_graph.sql').read_text();db.execute(migration);db.execute(migration)
  owner,other,doc,page,region,chunk,obj,nid=([uuid4() for _ in range(8)])
  db.execute('insert into rkb_users(id) values(%s),(%s)',(owner,other))
  db.execute("insert into rkb_documents(id,owner_user_id,title,source_sha256,active_revision,page_count) values(%s,%s,'Synthetic graph source',%s,1,1)",(doc,owner,'a'*64))
  db.execute("insert into rkb_objects(id,document_id,kind,object_key,sha256,mime_type,size_bytes) values(%s,%s,'text_projection','synthetic',%s,'text/plain',12)",(obj,doc,'b'*64))
  db.execute('insert into rkb_pages(id,document_id,physical_page_index,width,height,revision) values(%s,%s,0,1000,1000,1)',(page,doc))
  db.execute("insert into rkb_regions(id,page_id,kind,bbox,reading_order) values(%s,%s,'body','{\"left\":0,\"top\":0,\"right\":1000,\"bottom\":1000}',0)",(region,page))
  db.execute("insert into rkb_chunks(id,document_id,text_object_id,title,text_start,text_end,text_sha256,fts,page_ids,region_ids,revision) values(%s,%s,%s,'Synthetic',0,12,%s,to_tsvector('simple','Ada attended'),%s,%s,1)",(chunk,doc,obj,'c'*64,[page],[region]))
  e={'chunk_id':str(chunk),'page_id':str(page),'region_id':str(region),'exact_quote':'Ada attended'}
  db.execute('set role rkb_app');db.execute("select set_config('rkb.actor_id',%s,false)",(str(owner),))
  db.execute("insert into rkb_entities values(%s,%s,'person','Ada',null,%s,1,'candidate','{}')",(nid,owner,doc))
  args=(uuid4(),nid,doc,chunk,page,region,json.dumps(e))
  for _ in range(2):db.execute("insert into rkb_entity_mentions(id,entity_id,document_id,revision,chunk_id,page_id,region_id,exact_source_spelling,evidence,state) values(%s,%s,%s,1,%s,%s,%s,'Ada',%s::jsonb,'candidate') on conflict(id) do nothing",args)
  assert db.execute('select count(*) from rkb_entity_mentions').fetchone()[0]==1
  invalid={**e,'region_id':str(uuid4())}
  try:db.execute("insert into rkb_entity_aliases(id,entity_id,value,normalized_value,alias_type,document_id,revision,evidence) values(%s,%s,'Ada','ada','current',%s,1,%s::jsonb)",(uuid4(),nid,doc,json.dumps(invalid)))
  except psycopg.Error:pass
  else:raise AssertionError('forged source locator accepted')
  db.execute("select set_config('rkb.actor_id',%s,false)",(str(other),))
  for table in ('rkb_entities','rkb_entity_mentions','rkb_entity_aliases','rkb_entity_relations'):
   assert db.execute('select count(*) from '+table).fetchone()[0]==0
  try:db.execute("insert into rkb_entities values(%s,%s,'person','Forged',null,%s,1,'candidate','{}')",(uuid4(),other,doc))
  except psycopg.Error:pass
  else:raise AssertionError('unauthorized graph write accepted')
  db.execute('reset role');db.execute('update rkb_documents set active_revision=2 where id=%s',(doc,))
  assert db.execute('select count(*) from rkb_graph_discovery_jobs where document_id=%s',(doc,)).fetchone()[0]==1
  db.execute('update rkb_documents set active_revision=2 where id=%s',(doc,));assert db.execute('select count(*) from rkb_graph_discovery_jobs where document_id=%s',(doc,)).fetchone()[0]==1
  db.execute('set role rkb_app');db.execute("select set_config('rkb.actor_id',%s,false)",(str(owner),));assert db.execute('select count(*) from rkb_entity_mentions where rkb_graph_active(document_id,revision)').fetchone()[0]==0
 result={'migration_sha256':hashlib.sha256(migration.encode()).hexdigest(),'migration_twice':True,'owner_write_read':True,'acl_denied':True,'forged_evidence_rejected':True,'idempotent_mentions':True,'revision_enqueue_idempotent':True,'stale_mentions_hidden':True}
 a.output.write_text(json.dumps(result,indent=2));print(json.dumps(result))
if __name__=='__main__':main()
