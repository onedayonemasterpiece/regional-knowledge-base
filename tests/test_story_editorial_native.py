"""Only the explicitly owned, bounded synthetic PG fixture may run these tests."""
from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path

import pytest
import pytest_asyncio
from regional_knowledge.kaggle_apply import HandlerRegistry,initialize_database,ApplyExecutionError
from regional_knowledge.story_editorial import *
from regional_knowledge.source_import_v4 import resolved_import_v4_command,source_import_v4_handler
from test_source_story_native import connect,apply
from test_kaggle_apply import pinned
from test_recovery_journal import fixture as journal_fixture
from test_source_import_v2 import payload,STORY,VARIANT,FINDING
from test_source_import import SOURCE
from test_story_editorial import operation

ROOT=Path(__file__).resolve().parents[1]
MANAGER='60000000-0000-0000-0000-000000000002'
EDITOR='60000000-0000-0000-0000-000000000003'
pytestmark=pytest.mark.skipif(not os.getenv('RKB_STORY_FIXTURE_SOCKET'),reason='explicit synthetic native9 fixture required')


async def initialize(conn,generation):
    async with conn.transaction():
        await conn.execute('CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public')
        assert await(await conn.execute('SELECT to_regnamespace(\'rkb_v4\')')).fetchone()==(None,)
        for name in ('001_search.sql','002_recovery.sql'):
            await conn.execute((ROOT/'sql/kaggle'/name).read_text(),prepare=False)
    await initialize_database(conn,database_generation=generation,writer_generation='writer-1',revisions=EDITORIAL_REVISIONS)
    async with conn.transaction():
        for name in ('003_search_scope.sql','004_read_fences.sql','005_source_withdraw.sql','006_source_import.sql',
                '007_source_stories.sql','008_source_facts.sql','009_story_editorial.sql'):
            await conn.execute((ROOT/'sql/kaggle'/name).read_text(),prepare=False)
        await conn.execute('SELECT rkb_v4.publish_read_frontier(%s,%s,0,0,true,true)',(generation,'writer-1'))


@pytest_asyncio.fixture
async def native():
    conn,fd=await connect()
    try:
        await initialize(conn,'editorial-fixture-one');handlers=[]
        for route,check,action in [(STORY_EDIT,'check_story_edit_v1','apply_story_edit_v1'),
                (STORY_ACCESS,'check_story_access_v1','apply_story_access_v1')]:
            c=await pinned(conn,check);a=await pinned(conn,action)
            handlers.append(editorial_handler(route=route,check_sha256=c.definition_sha256,apply_sha256=a.definition_sha256))
        c=await pinned(conn,'check_source_import_v4');a=await pinned(conn,'apply_source_import_v4')
        handlers.append(source_import_v4_handler(check_sha256=c.definition_sha256,apply_sha256=a.definition_sha256))
        _,journal=journal_fixture()
        yield dict(connection=conn,registry=HandlerRegistry(handlers),journal=journal,generation='editorial-fixture-one')
    finally:
        await conn.rollback()
        async with conn.transaction():await conn.execute('DROP SCHEMA IF EXISTS rkb_v4 CASCADE')
        await conn.close();os.close(fd)


def imported():
    return resolved_import_v4_command(sequence=1,operation_key='editorial-source-first',request_id='import-1',
        person_id=MANAGER,logical_time='2026-10-20T18:00:00Z',source_id=SOURCE,payload=payload())


async def eligibility(n):
    async with n['connection'].transaction():
        return (await(await n['connection'].execute('SELECT eligibility_revision FROM rkb_v4.source_visibility WHERE source_id=%s',(SOURCE,))).fetchone())[0]


async def grant(n,sequence,revision,*,actor=MANAGER,role='editor'):
    return resolved_story_access_command(sequence=sequence,operation_key='editorial-access-'+str(sequence),
        request_id='access-'+str(sequence),person_id=actor,logical_time='2026-10-20T18:01:00Z',source_id=SOURCE,story_id=STORY,
        expected_revision=revision,source_revision=1,eligibility_revision=await eligibility(n),grantee_user_id=EDITOR,capability=role)


async def edit(n,sequence,revision,*,actor=EDITOR,body='A revised actual narrative body remains saved.',variant_revision=1):
    return resolved_story_edit_command(sequence=sequence,operation_key='editorial-edit-'+str(sequence),
        request_id='edit-'+str(sequence),person_id=actor,logical_time='2026-10-20T18:02:00Z',source_id=SOURCE,story_id=STORY,
        expected_revision=revision,source_revision=1,eligibility_revision=await eligibility(n),
        operation=operation(body=body,expected_variant_revision=variant_revision),embedding=[1.]+[0.]*1023,input_tokens=10)


async def read(n,*,revision=None,view='get'):
    async with n['connection'].transaction():
        return (await(await n['connection'].execute('SELECT rkb_v4.read_editorial_story_v1(%s,%s,%s,0,10)',
            (STORY,revision,view))).fetchone())[0]


@pytest.mark.asyncio
async def test_native_editor_grant_actual_body_default_ref_history_retry_and_restart(native):
    assert (await apply(native,imported())).status=='applied'
    original=await read(native);assert original['body']==payload()['semantic']['stories'][0]['default_variant']['body']
    assert (await apply(native,await grant(native,2,1))).status=='applied'
    c=await edit(native,3,2);saved=await apply(native,c);assert saved.status=='applied'
    current=await read(native)
    assert current['revision']==3 and current['body']==operation().model_copy(update={'body':'A revised actual narrative body remains saved.'}).body
    assert current['body']!=current['summary'] and current['default_variant_ref']==dict(variant_id=VARIANT,revision=2)
    assert current['variant']['state']=='drafting' and current['publication_approval'] is None
    assert current['variant']['approved_revision'] is None and current['variant']['approval_fingerprint'] is None
    assert (await read(native,revision=1))['body']==original['body']
    assert (await apply(native,c)).duplicate and (await apply(native,c)).outcome==saved.outcome
    conn,fd=await connect()
    try:
        other=dict(native,connection=conn)
        assert (await read(other))['body']==current['body']
    finally:await conn.close();os.close(fd)
    history=(await read(native,view='history'))['entries']
    assert [x['action'] for x in history]==['source_import','access_change','upsert_variant']
    assert history[-1]['actor_id']==EDITOR and history[-1]['command_sha256']==saved.command_sha256
    assert (await read(native,view='variants'))['entries'][0]['body']==current['body']


@pytest.mark.asyncio
async def test_native_editor_scope_manager_only_grants_revision_conflict_and_revocation(native):
    await apply(native,imported())
    denied=await apply(native,await edit(native,2,1));assert denied.status=='rejected_precondition'
    assert (await apply(native,await grant(native,3,1))).status=='applied'
    denied=await apply(native,await grant(native,4,2,actor=EDITOR,role='manager'));assert denied.status=='rejected_precondition'
    assert (await apply(native,await grant(native,5,2,role='revoke'))).status=='applied'
    assert (await apply(native,await edit(native,6,3))).status=='rejected_precondition'
    assert (await read(native))['body']==payload()['semantic']['stories'][0]['default_variant']['body']
    # A valid editor grant does not bypass source eligibility/current revision.
    await apply(native,await grant(native,7,3))
    stale=await edit(native,8,4)
    async with native['connection'].transaction():
        await native['connection'].execute('UPDATE rkb_v4.sources SET withdrawn=true WHERE source_id=%s',(SOURCE,))
    assert (await apply(native,stale)).status=='rejected_precondition'
    assert await read(native) is None and await read(native,view='history') is None


@pytest.mark.asyncio
async def test_native_editorial_failure_rolls_back_and_original_order_replay_is_exact(native):
    commands=[imported()];receipts=[await apply(native,commands[0])]
    commands.append(await grant(native,2,1));receipts.append(await apply(native,commands[-1]))
    c=await edit(native,3,2)
    async with native['connection'].transaction():
        await native['connection'].execute("""CREATE FUNCTION rkb_v4.synthetic_editor_failure() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN RAISE EXCEPTION 'synthetic editor failure';END $$;
            CREATE TRIGGER synthetic_editor_failure BEFORE INSERT ON rkb_v4.editorial_story_search
            FOR EACH ROW EXECUTE FUNCTION rkb_v4.synthetic_editor_failure()""",prepare=False)
    with pytest.raises(ApplyExecutionError):await apply(native,c)
    assert (await read(native))['revision']==2
    async with native['connection'].transaction():
        assert await(await native['connection'].execute('SELECT count(*) FROM rkb_v4.editorial_variant_versions')).fetchone()==(1,)
        assert await(await native['connection'].execute('SELECT applied_seq FROM rkb_v4.recovery_state')).fetchone()==(2,)
        await native['connection'].execute('DROP TRIGGER synthetic_editor_failure ON rkb_v4.editorial_story_search')
    commands.append(c);receipts.append(await apply(native,c));actual=await read(native,view='history')
    async with native['connection'].transaction():await native['connection'].execute('DROP SCHEMA rkb_v4 CASCADE')
    await initialize(native['connection'],'editorial-fixture-two');native['generation']='editorial-fixture-two'
    for c,r in zip(commands,receipts):assert (await apply(native,c)).outcome==r.outcome
    assert await read(native,view='history')==actual
