"""Existing disposable fixture: actual accepted11->12->14; no fresh14 init."""
import copy,json,base64
from dataclasses import replace
from pathlib import Path
import pytest
import pytest_asyncio
import psycopg
from test_kaggle_pg_driver import database
from test_kaggle_apply import PG,pinned
from test_recovery_journal import fixture as journal_fixture
from test_geo_migration import snapshot,uid
from regional_knowledge.kaggle_apply import initialize_database,HandlerRegistry,apply_accepted
from regional_knowledge.geo_manifest_commands import (
    GEO_REVISIONS,GEO_V2_REVISIONS,geo_manifest_handler,geo_dependency_transition_handler,
    geo_dependency_repair_commands,plan_geo_manifest)
from regional_knowledge.geo_whitelist import build_geo_whitelist
from regional_knowledge.kaggle_geo_upgrade import NativeGeoMigration
from regional_knowledge.kaggle_native_source_upgrade import (
    NativeSourceUpgradePins,NativeSourceMigration,native_source_upgrade_handler,
    resolved_native_source_upgrade,transition_sql,transition_definitions,ARTIFACTS,sha,canonical)
from regional_knowledge.source_activation import ACTIVATION_REVISIONS
ROOT=Path(__file__).resolve().parents[1]
DB='native-upgrade-fixture';WRITER='native-upgrade-writer'

async def apply(env,command):
    seal=await env['journal'].locate_accepted(command)
    if seal is None:seal=await env['journal'].seal([command],predecessor=await env['journal'].head(),writer_generation=WRITER)
    return await apply_accepted(env['connection'],journal=env['journal'],seal=seal,command=command,
        database_generation=DB,writer_generation=WRITER,registry=env['registry'])

@pytest_asyncio.fixture
async def schema12(database):
    c=database
    async with c.transaction():await c.execute((ROOT/'sql/kaggle/002_recovery.sql').read_text(),prepare=False)
    await initialize_database(c,database_generation=DB,writer_generation=WRITER,revisions=GEO_REVISIONS)
    async with c.transaction():
        for p in sorted((ROOT/'sql/kaggle').glob('*.sql')):
            if p.name[:3].isdigit() and 3<=int(p.name[:3])<=11:await c.execute(p.read_text(),prepare=False)
        await c.execute('SELECT rkb_v4.publish_read_frontier(%s,%s,0,0,true,true)',(DB,WRITER))
    old=geo_manifest_handler(revisions=GEO_REVISIONS,check_sha256=(await pinned(c,'check_geo_manifest_install_v1')).definition_sha256,
        apply_sha256=(await pinned(c,'apply_geo_manifest_install_v1')).definition_sha256)
    store,journal=journal_fixture();store.writer=WRITER
    env=dict(connection=c,journal=journal,registry=HandlerRegistry([old]))
    data=snapshot();template=data['mentions'][0]
    data['mentions'].extend(dict(template,id=uid(1000+n)) for n in range(81))
    data['expected_counts']=data['expected_counts'].model_copy(update={'mentions':83})
    commands=plan_geo_manifest(build_geo_whitelist(**data).require_ready()).commands(first_sequence=1,
        operation_key='upgrade-old-geo',request_id='upgrade-old-request',person_id=uid(1),logical_time='2026-10-21T00:00:00Z')
    outcomes={}
    for command in commands:outcomes[command.operation_key]=(await apply(env,command)).status
    artifact=(ROOT/'sql/kaggle/012_geo_dependency_bound.sql').read_bytes()
    async with c.transaction():
        await c.execute(artifact.decode(),prepare=False)
        transition=geo_dependency_transition_handler(artifact_sha256=sha(artifact),
            check_sha256=(await pinned(c,'check_geo_dependencies_v2_transition')).definition_sha256,
            apply_sha256=(await pinned(c,'apply_geo_dependencies_v2_transition')).definition_sha256)
        new=geo_manifest_handler(revisions=GEO_V2_REVISIONS,
            check_sha256=(await pinned(c,'check_geo_manifest_install_v2')).definition_sha256,
            apply_sha256=(await pinned(c,'apply_geo_manifest_install_v2')).definition_sha256)
        raise psycopg.Rollback()
    env['registry']=HandlerRegistry([old,transition,new])
    repair=geo_dependency_repair_commands(commands,settled_outcomes=outcomes,artifact_sha256=sha(artifact))
    await NativeGeoMigration(sql_root=ROOT/'sql/kaggle',registry=env['registry'],database_generation=DB).prepare(c,repair[0],transition)
    for command in repair:assert (await apply(env,command)).status=='applied'
    env.update(sequence=repair[-1].sequence,old_commands=commands)
    return env

async def preflight(c,*,root=ROOT/'sql/kaggle'):
    pins=NativeSourceUpgradePins(*(sha((root/n).read_bytes()) for n in ARTIFACTS),'a'*64,'b'*64)
    async with c.transaction():
        for n in ARTIFACTS:await c.execute((root/n).read_text(),prepare=False)
        await c.execute(transition_sql(pins),prepare=False)
        hashes=await transition_definitions(c)
        raise psycopg.Rollback()
    return replace(pins,transition_check_sha256=hashes[0],transition_apply_sha256=hashes[1])

async def rows(c,query,parameters=None):
    async with c.transaction():
        return await(await c.execute(query,parameters)).fetchall()

async def ledger(c):
    return await rows(c,'SELECT sequence,command_bytes,outcome_bytes,handler_sha256 FROM rkb_v4.applied_ops ORDER BY sequence')

@PG
@pytest.mark.asyncio
async def test_real12_to14_preserves_old_ledger_functions_and_duplicate_replay(schema12):
    env=schema12;c=env['connection'];pins=await preflight(c)
    handler=native_source_upgrade_handler(pins)
    env['registry']=HandlerRegistry([*env['registry'].handlers.values(),handler])
    command=resolved_native_source_upgrade(pins=pins,sequence=env['sequence']+1,operation_key='native-upgrade',
        request_id='native-request',person_id=uid(1),logical_time='2026-10-21T01:00:00Z')
    before=await ledger(c)
    defs=await rows(c,"SELECT p.oid::regprocedure::text,pg_get_functiondef(p.oid) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='rkb_v4' AND p.prokind='f' ORDER BY 1")
    migration=NativeSourceMigration(sql_root=ROOT/'sql/kaggle',registry=env['registry'],database_generation=DB,pins=pins)
    await migration.prepare(c,command,handler)
    await migration.prepare(c,command,handler) # exact lost install ACK
    assert (await rows(c,'SELECT schema_revision,function_revision,executor_revision FROM rkb_v4.recovery_state'))[0]==GEO_V2_REVISIONS.wire()
    receipt=await apply(env,command);assert receipt.status=='applied'
    assert json.loads(base64.b64decode(json.loads(receipt.outcome)['result_b64']))==pins.manifest
    assert (await ledger(c))[:-1]==before
    for name,body in defs:
        assert (await rows(c,'SELECT pg_get_functiondef(to_regprocedure(%s))',(name,)))[0][0]==body
    assert (await rows(c,'SELECT schema_revision,function_revision,executor_revision FROM rkb_v4.recovery_state'))[0]==ACTIVATION_REVISIONS.wire()
    assert (await rows(c,'SELECT projection_seq,text_ready,dense_ready FROM rkb_v4.read_frontier'))[0]==(command.sequence,True,True)
    duplicate=await apply(env,command);assert duplicate.duplicate and duplicate.outcome==receipt.outcome
    old=await apply(env,env['old_commands'][1]);assert old.duplicate
    await migration.prepare(c,command,handler)

@PG
@pytest.mark.asyncio
async def test_second_artifact_failure_rolls_back_first(schema12,tmp_path):
    env=schema12;c=env['connection'];pins=await preflight(c)
    (tmp_path/ARTIFACTS[0]).write_bytes((ROOT/'sql/kaggle'/ARTIFACTS[0]).read_bytes())
    invalid=b'SELECT definitely_missing_native_upgrade_function();\n'
    (tmp_path/ARTIFACTS[1]).write_bytes(invalid)
    pins=replace(pins,sql014_sha256=sha(invalid))
    handler=native_source_upgrade_handler(pins);registry=HandlerRegistry([handler])
    command=resolved_native_source_upgrade(pins=pins,sequence=env['sequence']+1,operation_key='native-failure',
        request_id='native-failure-request',person_id=uid(1),logical_time='2026-10-21T01:00:00Z')
    before=await ledger(c)
    with pytest.raises(psycopg.Error):
        await NativeSourceMigration(sql_root=tmp_path,registry=registry,database_generation=DB,pins=pins).prepare(c,command,handler)
    assert (await rows(c,"SELECT to_regclass('rkb_v4.source_stage_batches')"))[0]==(None,)
    assert await ledger(c)==before
