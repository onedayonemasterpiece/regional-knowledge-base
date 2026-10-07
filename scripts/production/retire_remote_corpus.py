"""Backup/parity/dependency-gated retirement; vectors and all revisions survive.

Run only after SQLite runtime read/write acceptance and quiescing legacy writers.
A restore requires the retained exact SQLite snapshot and historical SQL schemas.
No CASCADE, VACUUM FULL, embedding deletion or embedding regeneration.
"""
import argparse,asyncio,json,os
from pathlib import Path
import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from operator_env import load_service_env
from regional_knowledge.sqlite_corpus import SQLiteCorpus,TABLES,COLUMNS,canonical,row_key
KEEP={'rkb_vector_items','rkb_chunk_embeddings_e5','rkb_chunk_embeddings_bge'}
FUNCTIONS={'rkb_current_actor_id','rkb_vector_scope','rkb_vector_revision_scope','rkb_vector_candidates_v2','rkb_vector_candidates_v3','rkb_default_material_identity'}
async def run(args):
 load_service_env();local=SQLiteCorpus(args.database);backup=SQLiteCorpus(args.backup);receipt=json.loads(args.migration_report.read_text())
 if backup.digest()!=receipt['parity'] or not receipt['backup_restored']:raise ValueError('restored migration snapshot digest required')
 args.evidence.mkdir(mode=0o700,parents=True,exist_ok=True);report={'backup_digest':backup.digest(),'parity':{},'deleted':False}
 async with await psycopg.AsyncConnection.connect(os.environ['KB_SUPABASE_SESSION_CONNECTION'],row_factory=dict_row) as db:
  await db.execute("set local lock_timeout='5s';set local statement_timeout='60s'")
  tables=await(await db.execute("select tablename from pg_tables where schemaname='public' and tablename like 'rkb_%'")).fetchall();retire={r['tablename'] for r in tables}-KEEP-{'rkb_schema_migrations'}
  if not retire:print('already retired');return
  if retire != TABLES-{'rkb_chunk_embeddings_e5','rkb_chunk_embeddings_bge'}:raise ValueError('unexpected corpus table inventory')
  # Lock all legacy details before the final exact comparison and deletion.
  await db.execute(sql.SQL('lock table {} in access exclusive mode').format(sql.SQL(',').join(sql.Identifier(t) for t in sorted(retire))))
  for table in sorted(retire):
   names=[c['column_name'] for c in COLUMNS[table] if c['column_name']!='catalog']
   rows=await(await db.execute(sql.SQL('select {} from {}').format(sql.SQL(',').join(map(sql.Identifier,names)),sql.Identifier(table)))).fetchall()
   for row in rows:
    row=json.loads(canonical(row));key=row_key(table,row);current=local.one(table,key);saved=backup.one(table,key)
    if not current or not saved or any(current.get(k)!=v or saved.get(k)!=v for k,v in row.items()):raise ValueError('legacy detail parity changed: '+table)
   report['parity'][table]=len(rows)
  fks=await(await db.execute("select conrelid::regclass::text source,confrelid::regclass::text target from pg_constraint where contype='f'")).fetchall()
  if any(f['target'] in retire and f['source'] not in retire for f in fks):raise ValueError('external foreign key still references corpus')
  functions=await(await db.execute("select p.oid::regprocedure::text signature,p.proname name,pg_get_functiondef(p.oid) definition from pg_proc p join pg_namespace n on n.oid=p.pronamespace where n.nspname='public' and p.proname like 'rkb_%'")).fetchall()
  report['function_restore']=functions
  report['before_bytes']=(await(await db.execute('select pg_database_size(current_database()) bytes')).fetchone())['bytes']
  report['vectors_before']=await(await db.execute('select (select count(*) from rkb_chunk_embeddings_e5) e5,(select count(*) from rkb_chunk_embeddings_bge) bge,(select count(*) from rkb_vector_items) anchors')).fetchone()
  (args.evidence/'retirement.json').write_text(json.dumps(report,indent=2))
  if not args.apply:await db.rollback();print('verified; --apply required to retire');return
  await db.execute(Path('sql/021_vector_only_plane.sql').read_text().strip().removeprefix('begin;').removesuffix('commit;'))
  await db.execute(Path('sql/022_vector_candidate_hotpath.sql').read_text().strip().removeprefix('begin;').removesuffix('commit;'))
  # RESTRICT is intentional: any unreviewed database dependency aborts the transaction.
  await db.execute(sql.SQL('drop table {} restrict').format(sql.SQL(',').join(sql.Identifier(t) for t in sorted(retire))))
  for f in functions:
   if f['name'] not in FUNCTIONS:await db.execute(sql.SQL('drop function {} restrict').format(sql.SQL(f['signature'])))
  report['vectors_after']=await(await db.execute('select (select count(*) from rkb_chunk_embeddings_e5) e5,(select count(*) from rkb_chunk_embeddings_bge) bge,(select count(*) from rkb_vector_items) anchors')).fetchone()
  if report['vectors_before']!=report['vectors_after']:raise ValueError('vector preservation failed')
  report['deleted']=True
 # Drop file allocation is measured only after commit, on a fresh read.
 async with await psycopg.AsyncConnection.connect(os.environ['KB_SUPABASE_SESSION_CONNECTION'],row_factory=dict_row) as db:
  report['after_bytes']=(await(await db.execute('select pg_database_size(current_database()) bytes')).fetchone())['bytes']
  report['tables']=await(await db.execute("select c.relname,pg_total_relation_size(c.oid) allocated from pg_class c join pg_namespace n on n.oid=c.relnamespace where n.nspname='public' and c.relkind='r' and c.relname like 'rkb_%' order by c.relname")).fetchall()
  report['logical_bytes']=(await(await db.execute('select (select sum(pg_column_size(a)) from rkb_vector_items a)+(select sum(pg_column_size(e)) from rkb_chunk_embeddings_e5 e)+(select sum(pg_column_size(b)) from rkb_chunk_embeddings_bge b) bytes')).fetchone())['bytes']
 (args.evidence/'retirement.json').write_text(json.dumps(report,indent=2));print(json.dumps({k:report[k] for k in ('deleted','before_bytes','after_bytes','logical_bytes','vectors_after')}))
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--database',type=Path,required=True);p.add_argument('--backup',type=Path,required=True);p.add_argument('--migration-report',type=Path,required=True);p.add_argument('--evidence',type=Path,required=True);p.add_argument('--apply',action='store_true');asyncio.run(run(p.parse_args()))