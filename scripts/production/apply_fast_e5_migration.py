"""Guarded additive migration after isolated verification; never touches legacy vectors."""
import argparse,asyncio,hashlib,json
from pathlib import Path
from operator_env import load_service_env
from regional_knowledge.supabase_backend import backend_from_env

async def main(proof,output):
    migration=Path('sql/010_fast_e5.sql').read_text();verified=json.loads(proof.read_text())
    assert verified['migration_sha256']==hashlib.sha256(migration.encode()).hexdigest()
    assert all(verified[k] for k in ('migration_twice','dimension_and_space_rejection','owner_and_denied_actor','stale_hash_excluded','rollback_preserves_legacy_chunks'))
    load_service_env();b=backend_from_env()
    try:
        async with b.data_client._connection({'x-rkb-service':'1'}) as c:
            await c.execute("select pg_advisory_xact_lock(hashtext('rkb-fast-e5-migration'))")
            before=await(await c.execute("select count(*) as rows,count(embedding) as vectors,md5(string_agg(id::text||coalesce(embedding::text,'')||coalesce(metadata->>'embedding_space',''),',' order by id)) as legacy_digest from public.rkb_chunks")).fetchone()
            await c.execute(migration.strip().removeprefix('begin;').removesuffix('commit;'))
            after=await(await c.execute("select count(*) as rows,count(embedding) as vectors,md5(string_agg(id::text||coalesce(embedding::text,'')||coalesce(metadata->>'embedding_space',''),',' order by id)) as legacy_digest from public.rkb_chunks")).fetchone()
            assert before==after,'legacy storage changed'
        output.write_text(json.dumps({'migration_sha256':verified['migration_sha256'],'before':before,'after':after,'legacy_unchanged':True,'status':'applied'},indent=2));print('Additive E5 migration applied; legacy storage unchanged',before['rows'],before['vectors'])
    finally:await b.aclose()
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('proof',type=Path);p.add_argument('output',type=Path);a=p.parse_args();asyncio.run(main(a.proof,a.output))
