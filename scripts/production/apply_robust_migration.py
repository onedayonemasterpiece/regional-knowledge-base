"""Additive migration guarded by isolated proof and production identity audit."""
import asyncio,hashlib,json,sys
from pathlib import Path
from operator_env import load_service_env
from regional_knowledge.supabase_backend import backend_from_env
from apply_indexing_migration import QUERY

async def main(proof,output):
    checked=json.loads(proof.read_text());assert checked['migration_twice'] and checked['material_snapshot_guards']
    sources={name:Path('sql',name).read_text() for name in checked['migration_hashes']}
    for name,source in sources.items():assert hashlib.sha256(source.encode()).hexdigest()==checked['migration_hashes'][name]
    load_service_env();b=backend_from_env()
    try:
        async with b.data_client._connection({'x-rkb-service':'1'}) as db:
            await db.execute("select pg_advisory_xact_lock(hashtext('rkb-robust-migration'))")
            audit=await(await db.execute('select count(*) n from (select owner_user_id,source_sha256 from rkb_documents group by owner_user_id,source_sha256 having count(*)>1) duplicates')).fetchone()
            before=await(await db.execute(QUERY)).fetchone()
            roots=await(await db.execute('select count(*) n from rkb_documents')).fetchone()
            for source in sources.values():await db.execute(source.strip().removeprefix('begin;').removesuffix('commit;'))
            after=await(await db.execute(QUERY)).fetchone();assert before==after
            assert roots==await(await db.execute('select count(*) n from rkb_documents')).fetchone()
        output.write_text(json.dumps({'migration_hashes':checked['migration_hashes'],'duplicate_groups_audited':audit['n'],'historical_roots_not_merged':True,'corpus_vectors_preserved':True,'before':before,'after':after},indent=2));print('Robust additive migrations applied; historical roots and vectors preserved')
    finally:await b.aclose()

if __name__=='__main__':asyncio.run(main(Path(sys.argv[1]),Path(sys.argv[2])))
