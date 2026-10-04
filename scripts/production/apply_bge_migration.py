"""Operator additive BGE migration, guarded by exact isolated SQL proof."""
import argparse,asyncio,hashlib,json
from pathlib import Path
from operator_env import load_service_env
from regional_knowledge.supabase_backend import backend_from_env

async def run(proof,output):
    verified=json.loads(proof.read_text());sql=Path('sql/011_bge_rankings.sql').read_text()
    if verified['migration_sha256']!=hashlib.sha256(sql.encode()).hexdigest() or not all(verified[key] for key in ('migration_twice','dimension_space_rejection','acl_denied','independent_branches','stale_revision_excluded','rollback_preserves_e5_legacy')):
        raise RuntimeError('isolated BGE migration proof required')
    load_service_env();backend=backend_from_env()
    try:
        async with backend.data_client._connection({'x-rkb-service':'1'}) as db:
            await db.execute("select pg_advisory_xact_lock(hashtext('rkb-bge-migration'))")
            query="select count(*) as chunks,count(embedding) as legacy_vectors,md5(string_agg(id::text||coalesce(embedding::text,'')||coalesce(metadata->>'embedding_space',''),',' order by id)) as legacy_digest from rkb_chunks"
            before=await(await db.execute(query)).fetchone()
            e5_before=await(await db.execute('select count(*) as vectors from rkb_chunk_embeddings_e5')).fetchone()
            await db.execute(sql.strip().removeprefix('begin;').removesuffix('commit;'))
            after=await(await db.execute(query)).fetchone();e5_after=await(await db.execute('select count(*) as vectors from rkb_chunk_embeddings_e5')).fetchone()
            if before!=after or e5_before!=e5_after:raise RuntimeError('existing storage changed')
        output.write_text(json.dumps({'migration_sha256':verified['migration_sha256'],'legacy_before':before,'legacy_after':after,'e5_vectors':e5_after['vectors'],'existing_storage_preserved':True},indent=2));print('BGE storage added; E5 and legacy unchanged')
    finally:await backend.aclose()

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('proof',type=Path);parser.add_argument('output',type=Path);args=parser.parse_args();asyncio.run(run(args.proof,args.output))
