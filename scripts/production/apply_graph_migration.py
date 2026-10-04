"""Guarded additive graph migration; existing corpus/vector digest must survive."""
import asyncio,argparse,json,hashlib
from pathlib import Path
from operator_env import load_service_env
from regional_knowledge.supabase_backend import backend_from_env
async def run(proof,output):
 verified=json.loads(proof.read_text());sql=Path('sql/012_entity_graph.sql').read_text()
 guard=Path('sql/013_async_finalize_guard.sql').read_text()
 if not verified.get('finalize_guard_twice') or verified.get('finalize_guard_sha256')!=hashlib.sha256(guard.encode()).hexdigest():raise RuntimeError('exact isolated finalize guard proof required')
 if verified['migration_sha256']!=hashlib.sha256(sql.encode()).hexdigest() or not all(verified[k] for k in ('migration_twice','owner_write_read','acl_denied','forged_evidence_rejected','idempotent_mentions','revision_enqueue_idempotent','stale_mentions_hidden')):raise RuntimeError('exact isolated migration proof required')
 load_service_env();b=backend_from_env()
 query="""select (select count(*) from rkb_chunks) chunks,(select count(*) from rkb_chunk_embeddings_e5) e5,(select count(*) from rkb_chunk_embeddings_bge) bge,
 (select md5(string_agg(id::text||coalesce(embedding::text,''),',' order by id)) from rkb_chunks) legacy_digest,
 (select md5(string_agg(c.id::text||c.revision::text||c.text_sha256,',' order by c.id)) from rkb_chunks c join rkb_documents d on d.id=c.document_id where c.revision=d.active_revision) projection_digest"""
 try:
  async with b.data_client._connection({'x-rkb-service':'1'}) as db:
   await db.execute("select pg_advisory_xact_lock(hashtext('rkb-graph-migration'))")
   before=await(await db.execute(query)).fetchone();await db.execute(sql.strip().removeprefix('begin;').removesuffix('commit;'))
   await db.execute(guard[guard.index('begin;')+6:].strip().removesuffix('commit;'))
   after=await(await db.execute(query)).fetchone()
   if before!=after:raise RuntimeError('existing corpus changed')
  output.write_text(json.dumps({'migration_sha256':verified['migration_sha256'],'before':before,'after':after,'existing_storage_preserved':True},indent=2));print('Additive graph schema applied; corpus and vectors unchanged')
 finally:await b.aclose()
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('proof',type=Path);p.add_argument('output',type=Path);a=p.parse_args();asyncio.run(run(a.proof,a.output))
