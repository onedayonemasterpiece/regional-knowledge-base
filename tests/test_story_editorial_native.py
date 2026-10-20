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
from test_source_manifest import manifest_payload
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
        person_id=MANAGER,logical_time='2026-10-20T18:00:00Z',source_id=SOURCE,payload=manifest_payload())


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
    async with native['connection'].transaction():
        stored=(await(await native['connection'].execute('SELECT rkb_v4.read_source_manifest_v1(%s)',(SOURCE,))).fetchone())[0]
        assert stored==manifest_payload()['source_manifest']
        assert await(await native['connection'].execute('SELECT rkb_v4.verify_source_story_origins_v3(ARRAY[%s::uuid])',(SOURCE,))).fetchone()==(True,)
    assert (await apply(native,await grant(native,2,1))).status=='applied'
    c=await edit(native,3,2);saved=await apply(native,c);assert saved.status=='applied'
    current=await read(native)
    assert current['revision']==3 and current['body']==operation().model_copy(update={'body':'A revised actual narrative body remains saved.'}).body
    assert current['body']!=current['summary'] and current['default_variant_ref']==dict(variant_id=VARIANT,revision=2)
    assert current['variant']['state']=='drafting' and current['publication_approval'] is None
    assert current['variant']['approved_revision'] is None and current['variant']['approval_fingerprint'] is None
    from regional_knowledge.kaggle_editorial_contracts import EditorialReadResult
    from regional_knowledge.kaggle_story_read import story_item
    EditorialReadResult(request_id='native-editorial-proof',connection_generation=1,turn_generation=1,status='ok',
        record=current,evidence=(story_item(current),),observed_data_seq=3,observed_projection_seq=3,
        observed_visibility_epoch=1,observed_database_generation=native['generation'])
    async with native['connection'].transaction():
        editor=(await(await native['connection'].execute('SELECT rkb_v4.read_story_edit_context_v1(%s,%s)',(STORY,EDITOR))).fetchone())[0]
        assert editor['role']=='editor' and editor['revision']==3 and editor['view']=='edit_context'

    assert (await read(native,revision=1))['body']==original['body']
    assert (await apply(native,c)).duplicate and (await apply(native,c)).outcome==saved.outcome
    conn,fd=await connect()
    try:
        other=dict(native,connection=conn)
        assert (await read(other))['body']==current['body']
        async with conn.transaction():
            from regional_knowledge.source_import import _json
            import base64
            row=await(await conn.execute('SELECT manifest,payload_sha256,original_receipt FROM rkb_v4.source_manifests WHERE source_id=%s',(SOURCE,))).fetchone()
            assert row[0]==manifest_payload()['source_manifest'] and row[1]==hashlib.sha256(imported().payload).hexdigest()
            assert bytes(row[2])==base64.b64decode(row[0]['original_receipt_b64'])
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


@pytest.mark.asyncio
async def test_native_catalogue_and_page_provenance_survive_without_local_spool(native):
    assert (await apply(native,imported())).status=='applied'
    doc=manifest_payload();conn=native['connection']
    async with conn.transaction():
        rows=await(await conn.execute('SELECT * FROM rkb_v4.find_source_catalog_v1(%s,8)',('Exact title',))).fetchall()
        assert len(rows)==1 and rows[0][3]==doc['source_manifest']['metadata']
        proof=(await(await conn.execute('SELECT metadata FROM rkb_v4.source_passage_metadata_v1(%s::uuid[])',
            ([doc['passages'][0]['passage_id']],))).fetchone())[0]
        assert proof['original_sha256']==doc['original']['sha256']
        assert proof['pages'][0]['physical_page_index']==1 and proof['pages'][0]['printed_page_number']=='105'
        assert proof['regions'][0]['bbox']==doc['source_manifest']['regions'][0]['bbox']
        assert not {'original_receipt_b64','native_version','key','original_receipt'} & proof.keys()
        whitespace='\u3000\t\n\u00a0'
        assert (await(await conn.execute('SELECT rkb_v4.source_text_trim_v1(%s)',(whitespace+'А🙂'+whitespace,))).fetchone())[0]=='А🙂'
        await conn.execute('UPDATE rkb_v4.sources SET withdrawn=true WHERE source_id=%s',(SOURCE,))
        assert not await(await conn.execute('SELECT * FROM rkb_v4.find_source_catalog_v1(%s,8)',('Exact title',))).fetchall()
        assert not await(await conn.execute('SELECT * FROM rkb_v4.source_passage_metadata_v1(%s::uuid[])',
            ([doc['passages'][0]['passage_id']],))).fetchall()


@pytest.mark.asyncio
async def test_native_history_reconstructs_editorial_grants_and_body_after_edits(native):
    await apply(native,imported())
    await apply(native,await grant(native,2,1))
    await apply(native,await edit(native,3,2))
    async def valid():
        return (await(await native['connection'].execute('SELECT rkb_v4.verify_source_story_origins_v3(ARRAY[%s::uuid])',(SOURCE,))).fetchone())[0]
    async with native['connection'].transaction():assert await valid() is True
    # Explicit test-owned corruption: these transactions are always rolled back.
    for statement,arguments in [
        ('UPDATE rkb_v4.story_editor_grants SET capability=\'manager\' WHERE story_id=%s',(STORY,)),
        ('UPDATE rkb_v4.editorial_story_heads SET owner_id=%s WHERE story_id=%s',(EDITOR,STORY)),
        ('UPDATE rkb_v4.editorial_variant_heads SET revision=1 WHERE variant_id=%s',(VARIANT,)),
        ('UPDATE rkb_v4.source_catalog SET search_text=\'forged native projection\' WHERE source_id=%s',(SOURCE,)),
    ]:
        async with native['connection'].transaction(force_rollback=True):
            await native['connection'].execute('ALTER TABLE rkb_v4.source_catalog DISABLE TRIGGER immutable_source_catalog')
            await native['connection'].execute(statement,arguments)
            assert await valid() is False
    async with native['connection'].transaction():assert await valid() is True
