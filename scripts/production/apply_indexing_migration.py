"""Guarded additive production activation/readiness migration; no data rebuild."""
import argparse,asyncio,json,hashlib
from pathlib import Path
from operator_env import load_service_env
from regional_knowledge.supabase_backend import backend_from_env
QUERY="""select (select count(*) from rkb_chunks) chunks,(select count(*) from rkb_chunk_embeddings_e5) e5,(select count(*) from rkb_chunk_embeddings_bge) bge,
 (select md5(string_agg(id::text||coalesce(embedding::text,''),',' order by id)) from rkb_chunks) legacy_digest,
 (select md5(string_agg(chunk_id::text||embedding::text||revision::text||text_sha256||batch_sha256,',' order by chunk_id)) from rkb_chunk_embeddings_e5) e5_digest,
 (select md5(string_agg(chunk_id::text||embedding::text||revision::text||text_sha256,',' order by chunk_id)) from rkb_chunk_embeddings_bge) bge_digest"""
async def main(proof,output):
    verified=json.loads(proof.read_text());sql=Path('sql/014_automatic_indexing.sql').read_text()
    assert verified['migration_sha256']==hashlib.sha256(sql.encode()).hexdigest() and all(verified[k] for k in ('migration_twice','snapshot_semantic_guards','one_activation_trigger'))
    load_service_env();b=backend_from_env()
    try:
        async with b.data_client._connection({'x-rkb-service':'1'}) as db:
            await db.execute("select pg_advisory_xact_lock(hashtext('rkb-auto-index-migration'))")
            before=await(await db.execute(QUERY)).fetchone();await db.execute(sql.strip().removeprefix('begin;').removesuffix('commit;'));after=await(await db.execute(QUERY)).fetchone();assert before==after
        output.write_text(json.dumps({'migration_sha256':verified['migration_sha256'],'before':before,'after':after,'corpus_vectors_preserved':True},indent=2));print('Automatic indexing migration applied; corpus/vector digests unchanged')
    finally:await b.aclose()
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('proof',type=Path);p.add_argument('output',type=Path);a=p.parse_args();asyncio.run(main(a.proof,a.output))
