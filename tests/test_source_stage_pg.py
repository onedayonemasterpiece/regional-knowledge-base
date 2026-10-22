"""Opt-in checked disposable PG fixture only; never a live worker/database."""
from pathlib import Path

import pytest
import pytest_asyncio

from regional_knowledge.kaggle_apply import (
    Revisions, HandlerRegistry, ApplyExecutionError, apply_accepted, initialize_database,
)
from regional_knowledge.source_stage import (
    STAGE_ROUTE, SEAL_ROUTE, STAGE_REVISIONS, ZERO_HASH, canonical, digest,
    stage_handler, resolved_stage_command, validate_stage_command, load_stage_batch,
)
from test_source_stage import SOURCE, INGESTION, REQUEST, batch, seal
from test_kaggle_apply import PG, pinned
from test_kaggle_pg_driver import database
from test_recovery_journal import fixture as journal_fixture

ROOT=Path(__file__).resolve().parents[1]
VERSIONS=Revisions(*STAGE_REVISIONS)


def command(data, sequence, *, route=STAGE_ROUTE):
    return resolved_stage_command(route=route,revisions=VERSIONS,source_id=SOURCE,payload=data,
        sequence=sequence,operation_key=f'synthetic-source-stage:{sequence}',request_id=REQUEST,
        person_id='60000000-0000-0000-0000-000000000002',logical_time='2026-10-21T19:00:00Z')


def test_handler_and_command_bind_exact_reviewed_revision_not_old_import_alias():
    c=command(batch(),1)
    assert validate_stage_command(c)==batch()
    spec=stage_handler(route=STAGE_ROUTE,revisions=VERSIONS,check_sha256='1'*64,apply_sha256='2'*64)
    assert spec.precondition_keys==frozenset({'stage_head'})
    assert spec.dependency.scope_class=='public_source' and not spec.dependency.restriction
    with pytest.raises(ValueError):
        stage_handler(route=STAGE_ROUTE,revisions=Revisions('old','old','old'),check_sha256='1'*64,apply_sha256='2'*64)


@pytest_asyncio.fixture
async def native_stage(database):
    connection=database
    async with connection.transaction():
        await connection.execute((ROOT/'sql/kaggle/002_recovery.sql').read_text(),prepare=False)
    await initialize_database(connection,database_generation='source-stage-fixture',writer_generation='writer-1',revisions=VERSIONS)
    async with connection.transaction():
        # These are the minimal existing dependencies of the new inactive table
        # artifact, not a claim of a deployed schema13 public-reader catalog.
        for name in ('003_search_scope.sql','004_read_fences.sql','013_source_stage.sql'):
            await connection.execute((ROOT/'sql/kaggle'/name).read_text(),prepare=False)
        await connection.execute("SELECT rkb_v4.publish_read_frontier('source-stage-fixture','writer-1',0,0,true,true)")
    handlers=[]
    for route,suffix in [(STAGE_ROUTE,'source_stage_v1'),(SEAL_ROUTE,'source_stage_seal_v1')]:
        check=await pinned(connection,'check_'+suffix); apply=await pinned(connection,'apply_'+suffix)
        handlers.append(stage_handler(route=route,revisions=VERSIONS,
            check_sha256=check.definition_sha256,apply_sha256=apply.definition_sha256))
    _,journal=journal_fixture()
    return dict(connection=connection,journal=journal,registry=HandlerRegistry(handlers))


async def apply_native(env,c):
    journal=env['journal']; head=await journal.head(); accepted=await journal.locate_accepted(c)
    if accepted is None: accepted=await journal.seal([c],predecessor=head,writer_generation='writer-1')
    return await apply_accepted(env['connection'],journal=journal,seal=accepted,command=c,
        database_generation='source-stage-fixture',writer_generation='writer-1',registry=env['registry'])


@PG
@pytest.mark.asyncio
async def test_174_page_native_batches_exact_replay_and_seal_are_not_public_activation(native_stage):
    env=native_stage; previous=ZERO_HASH
    for index,start in enumerate(range(0,174,8)):
        data=batch(index,previous,start=start); c=command(data,index+1)
        first=await apply_native(env,c); repeated=await apply_native(env,c)
        assert first.status=='applied' and not first.duplicate
        assert repeated.duplicate and repeated.outcome==first.outcome
        async with env['connection'].transaction():
            assert await load_stage_batch(env['connection'],source_id=SOURCE,ingestion_id=INGESTION,batch_index=index)==canonical(data)
        previous=digest(canonical(data))
    c=command(seal(22,previous),23,route=SEAL_ROUTE)
    first=await apply_native(env,c); repeated=await apply_native(env,c)
    assert first.status=='applied' and repeated.duplicate and first.outcome==repeated.outcome
    async with env['connection'].transaction():
        for table,count in [('source_stage_batches',22),('source_stage_pages',174),('source_stage_candidates',1)]:
            assert await (await env['connection'].execute(f'SELECT count(*) FROM rkb_v4.{table} WHERE source_id=%s',(SOURCE,))).fetchone()==(count,)
        for table in ('sources','active_revision','passage_search'):
            assert await (await env['connection'].execute(f'SELECT count(*) FROM rkb_v4.{table} WHERE source_id=%s',(SOURCE,))).fetchone()==(0,)
        assert await (await env['connection'].execute('SELECT applied_seq,projection_seq,text_ready,dense_ready '
            'FROM rkb_v4.recovery_state CROSS JOIN rkb_v4.read_frontier')).fetchone()==(23,23,True,True)


@PG
@pytest.mark.asyncio
async def test_chain_gap_conflict_and_post_seal_append_are_rejected_without_data_loss(native_stage):
    env=native_stage; first=batch(count=1); head=digest(canonical(first))
    assert (await apply_native(env,command(first,1))).status=='applied'
    assert (await apply_native(env,command(batch(2,head,start=2,count=1),2))).status=='rejected_precondition'
    second=batch(1,head,start=1,count=1); head2=digest(canonical(second))
    assert (await apply_native(env,command(second,3))).status=='applied'
    changed=batch(count=1); changed['model_payload']['pages'][0]['regions'][0]['source_text']='Changed accepted text'
    assert (await apply_native(env,command(changed,4))).status=='rejected_precondition'
    assert (await apply_native(env,command(seal(2,head2),5,route=SEAL_ROUTE))).status=='applied'
    assert (await apply_native(env,command(batch(2,head2,start=2,count=1),6))).status=='rejected_precondition'
    async with env['connection'].transaction():
        assert await load_stage_batch(env['connection'],source_id=SOURCE,ingestion_id=INGESTION,batch_index=0)==canonical(first)
        assert await load_stage_batch(env['connection'],source_id=SOURCE,ingestion_id=INGESTION,batch_index=1)==canonical(second)
        assert await load_stage_batch(env['connection'],source_id=SOURCE,ingestion_id=INGESTION,batch_index=2) is None


@PG
@pytest.mark.asyncio
async def test_page_insert_failure_rolls_back_and_retries_same_remote_accepted_command(native_stage):
    env=native_stage; connection=env['connection']; c=command(batch(count=1),1)
    async with connection.transaction():
        await connection.execute('''CREATE FUNCTION rkb_v4.synthetic_stage_failure() RETURNS trigger
            LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic failure'; END $$;
            CREATE TRIGGER synthetic_stage_failure BEFORE INSERT ON rkb_v4.source_stage_pages
            FOR EACH ROW EXECUTE FUNCTION rkb_v4.synthetic_stage_failure()''',prepare=False)
    with pytest.raises(ApplyExecutionError): await apply_native(env,c)
    async with connection.transaction():
        for table in ('source_stage_sessions','source_stage_batches','source_stage_pages'):
            assert await (await connection.execute(f'SELECT count(*) FROM rkb_v4.{table}')).fetchone()==(0,)
        assert await (await connection.execute('SELECT applied_seq FROM rkb_v4.recovery_state')).fetchone()==(0,)
        await connection.execute('DROP TRIGGER synthetic_stage_failure ON rkb_v4.source_stage_pages')
    assert (await apply_native(env,c)).status=='applied'
    assert (await apply_native(env,c)).duplicate


@PG
@pytest.mark.asyncio
async def test_repeated_issuance_supports_late_story_and_explicit_correction_without_overwriting_history(native_stage):
    env=native_stage; first=batch(count=1); head=digest(canonical(first))
    assert (await apply_native(env,command(first,1))).status=='applied'
    enriched=batch(1,head,count=1)
    enriched['model_payload']['chunks']=[]
    enriched['model_payload']['story_candidates']=[{'candidate_key':'late-story',
        'proposition':'Synthetic later contextual reading', 'evidence_refs':[
            {'page_id':first['model_payload']['pages'][0]['page_id'],'region_key':'body'}]}]
    assert (await apply_native(env,command(enriched,2))).status=='applied'
    changed=batch(2,digest(canonical(enriched)),count=1)
    changed['model_payload']['pages'][0]['regions'][0]['source_text']='Changed accepted transcription'
    assert (await apply_native(env,command(changed,3))).status=='applied'
    async with env['connection'].transaction():
        assert await (await env['connection'].execute('SELECT count(*) FROM rkb_v4.source_stage_pages')).fetchone()==(1,)
        assert await (await env['connection'].execute('SELECT count(*) FROM rkb_v4.source_stage_batches')).fetchone()==(3,)
        assert await load_stage_batch(env['connection'],source_id=SOURCE,ingestion_id=INGESTION,batch_index=0)==canonical(first)
        assert await load_stage_batch(env['connection'],source_id=SOURCE,ingestion_id=INGESTION,batch_index=1)==canonical(enriched)
        assert await load_stage_batch(env['connection'],source_id=SOURCE,ingestion_id=INGESTION,batch_index=2)==canonical(changed)


@PG
@pytest.mark.asyncio
async def test_standalone_stage_cannot_commit_without_atomic_applied_ops_receipt(native_stage):
    import psycopg
    env=native_stage; c=command(batch(count=1),1)
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        async with env['connection'].transaction():
            await env['connection'].execute('SELECT rkb_v4.apply_source_stage_v1(%s,%s::jsonb,%s::jsonb,%s)',
                (c.payload,canonical(dict(c.ids)).decode(),canonical(dict(c.expected_revisions)).decode(),c.logical_time))
    async with env['connection'].transaction():
        assert await (await env['connection'].execute('SELECT count(*) FROM rkb_v4.source_stage_batches')).fetchone()==(0,)
