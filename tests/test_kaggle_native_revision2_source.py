"""Source-only revision2 contracts and real PostgreSQL17 SQL017 DDL smoke.

This does not import a user's PDF, run BGE, or publish a Dataset. The native
SQL fixture is isolated by the existing GitHub CI PG service.
"""
import json
from pathlib import Path
from uuid import uuid4

import pytest

from regional_knowledge.kaggle_native_revision2_upgrade import (
    ARTIFACTS,REVISION2_REVISIONS,NativeRevision2UpgradePins,
    revision2_upgrade_handler,ROUTE as UPGRADE_ROUTE,sha,
)
from regional_knowledge.kaggle_native_source_bindings import (
    revision2_native_handler,resolved_revision2_native_command,
    validate_revision2_native_command,
)
from regional_knowledge.source_stage import (
    REVISION2_STAGE_ROUTE,REVISION2_SEAL_ROUTE,REVISION2_BATCH_CODEC,REVISION2_SEAL_CODEC,
    ZERO_HASH,canonical,digest,validate_revision2_stage_payload,
)
from regional_knowledge.source_activation import (
    REVISION2_ACTIVATE_ROUTE,REVISION2_MATERIALIZE_ROUTE,
    REVISION2_ACTIVATION_CODEC,REVISION2_MATERIALIZE_CODEC,
)
from test_source_stage import SOURCE,INGESTION,batch,original
from test_kaggle_pg_driver import database
from test_source_activation_pg import image_pg
from test_kaggle_apply import PG
from regional_knowledge.kaggle_apply import initialize_database

ROOT=Path(__file__).resolve().parents[1]
PERSON='60000000-0000-0000-0000-000000000002'


class Revision2FixtureRollback(Exception):
    """Explicit test-only rollback sentinel; psycopg.Rollback is swallowed by transaction()."""



def _batch():
    base=batch(count=1)
    result={**base,'codec':REVISION2_BATCH_CODEC,'source_revision':2,'prior_revision':1,
        'ingestion_id':str(uuid4()),'original_ingestion_id':INGESTION,
        'predecessor_command_sha256':'a'*64}
    return result


def test_sql017_fixed_order_manifest_never_uses_sql015_as_new_migration():
    assert ARTIFACTS==('015_source_illustrations.sql','016_story_reconcile.sql','017_source_revision2.sql')
    hashes=[sha((ROOT/'sql/kaggle'/p).read_bytes()) for p in ARTIFACTS]
    pins=NativeRevision2UpgradePins(*hashes,'1'*64,'2'*64)
    assert pins.artifact_sha256==sha(canonical(pins.manifest))
    assert [r['name'] for r in pins.manifest['artifacts']]==list(ARTIFACTS)
    handler=revision2_upgrade_handler(pins)
    assert handler.route==UPGRADE_ROUTE
    assert handler.transition is not None and handler.transition.target==REVISION2_REVISIONS
    with pytest.raises(ValueError):
        NativeRevision2UpgradePins('BAD',hashes[1],hashes[2],'1'*64,'2'*64)


@pytest.mark.parametrize('route',[
    REVISION2_STAGE_ROUTE,REVISION2_SEAL_ROUTE,REVISION2_MATERIALIZE_ROUTE,REVISION2_ACTIVATE_ROUTE
])
def test_versioned_command_routes_are_unique_and_do_not_masquerade_as_v1(route):
    handler=revision2_native_handler(route,check_sha256='1'*64,apply_sha256='2'*64)
    assert handler.route==route and handler.revisions==REVISION2_REVISIONS
    assert handler.dependency.scope_class=='public_source'
    assert 'source_id' in handler.dependency.resource_keys


def test_revision2_stage_binds_original_page_and_distinct_ingestion():
    doc=_batch()
    assert validate_revision2_stage_payload(canonical(doc),source_id=SOURCE)==doc
    command=resolved_revision2_native_command(route=REVISION2_STAGE_ROUTE,source_id=SOURCE,
        payload=doc,sequence=1,operation_key='revision2-test-1',request_id=str(uuid4()),
        person_id=PERSON,logical_time='2026-10-22T12:00:00Z')
    assert validate_revision2_native_command(command)==doc
    assert command.expected_revisions['active_revision']=='1'
    assert command.expected_revisions['predecessor_command_sha256']=='a'*64
    changed={**doc,'original_ingestion_id':str(uuid4())}
    with pytest.raises(ValueError):
        validate_revision2_stage_payload(canonical(changed),source_id=SOURCE)
    same={**doc,'ingestion_id':INGESTION}
    with pytest.raises(ValueError):
        validate_revision2_stage_payload(canonical(same),source_id=SOURCE)


def test_activation_predecessor_and_exact_schema17_contract():
    batch_data=_batch()
    base=dict(source_revision=2,prior_revision=1,ingestion_id=batch_data['ingestion_id'],
        batch_count=1,last_batch_sha256=digest(canonical(batch_data)),
        materializer_sha256='b'*64,predecessor_command_sha256='a'*64)
    for route,codec,extra in (
        (REVISION2_ACTIVATE_ROUTE,REVISION2_ACTIVATION_CODEC,{}),
        (REVISION2_MATERIALIZE_ROUTE,REVISION2_MATERIALIZE_CODEC,{'ordinal':0}),
    ):
        command=resolved_revision2_native_command(route=route,source_id=SOURCE,
            payload=dict(base,codec=codec,**extra),sequence=2,operation_key='revision2-test-2',
            request_id=str(uuid4()),person_id=PERSON,logical_time='2026-10-22T12:00:00Z')
        assert validate_revision2_native_command(command)['source_revision']==2


@PG
@pytest.mark.asyncio
async def test_sql017_installs_after_real_sql015_016_without_rewriting_prior_ddl(database):
    connection=database
    async with connection.transaction():
        await connection.execute((ROOT/'sql/kaggle/002_recovery.sql').read_text(),prepare=False)
    await initialize_database(connection,database_generation='rev2-fixture',writer_generation='writer-fixture',
        revisions=REVISION2_REVISIONS)
    import psycopg
    with pytest.raises(Revision2FixtureRollback):
        async with connection.transaction():
            # SQL016's source-side grants require existing role names.
            # These NOLOGIN fixture identities are created only inside an
            # intentionally rolled-back CI transaction; no live grants.
            for role in ('rkb_writer','rkb_reader'):
                row=await(await connection.execute('SELECT 1 FROM pg_roles WHERE rolname=%s',(role,))).fetchone()
                assert row is None, 'do not mutate a pre-existing role'
                await connection.execute('CREATE ROLE '+role+' NOLOGIN')
            for seq in range(3,18):
                found=list((ROOT/'sql/kaggle').glob(f'{seq:03d}_*.sql'))
                assert len(found)==1, f'missing or duplicate pinned SQL{seq:03d}'
                await connection.execute(found[0].read_text(),prepare=False)
            relations=await(await connection.execute('''
                SELECT count(*) FROM pg_catalog.pg_tables
                 WHERE schemaname='rkb_v4' AND tablename LIKE 'source_revision2_%'
            ''')).fetchone()
            assert relations is not None and relations[0]>=9
            for name in ('check_source_stage_revision2_v1','apply_source_stage_revision2_v1',
                         'prepare_source_revision2_activation_v1','install_source_revision2_vector_v1',
                         'apply_source_activate_revision2_v1'):
                found=await(await connection.execute(
                    "SELECT count(*) FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='rkb_v4' AND p.proname=%s",
                    (name,))).fetchone()
                assert found==(1,)
            assert await(await connection.execute('SELECT count(*) FROM rkb_v4.source_revision2_origins')).fetchone()==(0,)
            raise Revision2FixtureRollback()



@PG
@pytest.mark.asyncio
async def test_sql017_real_accepted_revision1_original_allows_only_exact_same_id_revision2_stage(image_pg):
    """Use actual old accepted source SQL014; SQL017 is new and rolled back.

    Checks a real PG17 accepted v1 image/archive origin and the SQL017 stage
    precondition on immutable source data. This is not yet a full accepted
    revision2 activation; that separate integration acceptance belongs to
    Linza after source wiring.
    """
    import psycopg
    from psycopg.types.json import Jsonb
    from test_source_activation_pg import (
        stage_all,prepare,stage_command,accepted,install,next_command
    )
    from test_source_activation import synthetic_knowledge,activation_payload
    from test_source_stage import seal,INGESTION
    from regional_knowledge.source_stage import SEAL_ROUTE
    from regional_knowledge.source_stage import digest
    env=image_pg
    connection=env['connection']
    chain,head,doc=synthetic_knowledge(1)
    await stage_all(env,chain)
    h=await prepare(env,chain,head,doc)
    assert (await accepted(env,stage_command(seal(1,head),env['sequence']+1,SEAL_ROUTE))).status=='applied'
    assert await install(env,h,0)==digest(doc['passages'][0]['body'].encode())
    original_command=next_command(activation_payload(chain,head),env['sequence']+1,ordinal=0)
    assert (await accepted(env,original_command)).status=='applied'
    original_command=next_command(activation_payload(chain,head),env['sequence']+1)
    assert (await accepted(env,original_command)).status=='applied'
    source_id=SOURCE
    old_sha=digest(original_command.encode())
    data={**chain[0],'codec':REVISION2_BATCH_CODEC,
        'source_revision':2,'prior_revision':1,
        'ingestion_id':str(uuid4()),'original_ingestion_id':INGESTION,
        'predecessor_command_sha256':old_sha}
    command=resolved_revision2_native_command(route=REVISION2_STAGE_ROUTE,source_id=source_id,
        payload=data,sequence=env['sequence']+1,operation_key='revision2-stage-real-pg',
        request_id=str(uuid4()),person_id=PERSON,logical_time='2026-10-22T12:10:00Z')
    with pytest.raises(Revision2FixtureRollback):
        async with connection.transaction():
            for role in ('rkb_reader','rkb_writer'):
                assert await (await connection.execute('SELECT 1 FROM pg_roles WHERE rolname=%s',(role,))).fetchone() is None
                await connection.execute('CREATE ROLE '+role+' NOLOGIN')
            for file in ARTIFACTS:
                await connection.execute((ROOT/'sql/kaggle'/file).read_text(),prepare=False)
            row=await(await connection.execute('SELECT rkb_v4.check_source_stage_revision2_v1(%s,%s::jsonb,%s::jsonb,%s)',
                (command.payload,Jsonb(dict(command.ids)),Jsonb(dict(command.expected_revisions)),command.logical_time))).fetchone()
            assert row==(True,), 'v2 stage must validate real accepted v1 original at same source ID'
            tampered={**data,'source':{**data['source'],'edition_id':str(uuid4())}}
            counterfeit=resolved_revision2_native_command(route=REVISION2_STAGE_ROUTE,source_id=source_id,
                payload=tampered,sequence=command.sequence+1,operation_key='revision2-tamper-real-pg',
                request_id=str(uuid4()),person_id=PERSON,logical_time=command.logical_time)
            row=await(await connection.execute('SELECT rkb_v4.check_source_stage_revision2_v1(%s,%s::jsonb,%s::jsonb,%s)',
                (counterfeit.payload,Jsonb(dict(counterfeit.ids)),Jsonb(dict(counterfeit.expected_revisions)),counterfeit.logical_time))).fetchone()
            assert row==(False,), 'wrong edition must never inherit archived book identity'
            assert await(await connection.execute('SELECT revision FROM rkb_v4.active_revision WHERE source_id=%s',(source_id,))).fetchone()==(1,)
            assert await(await connection.execute('SELECT count(*) FROM rkb_v4.source_revision2_origins')).fetchone()==(0,)
            raise Revision2FixtureRollback()
