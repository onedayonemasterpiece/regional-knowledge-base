"""Opt-in real PG proof on the existing strict, root-owned disposable fixture.

Collecting without an explicit fixture skips these tests; no provider/model is
used. Run serially with other rkb_v4 suites because the schema is shared.
"""
import base64
from dataclasses import replace
import json
import os
from pathlib import Path
from types import SimpleNamespace

import psycopg
import pytest
import pytest_asyncio

from regional_knowledge.bge_queue import BgeQueue
from regional_knowledge.control_spool import ControlSpool,SOURCE_WITHDRAW,WITHDRAW_DEPENDENCY
from regional_knowledge.kaggle_apply import (
    ApplyExecutionError,ApplyUnknown,HandlerRegistry,HandlerSpec,Revisions,initialize_database,
)
from regional_knowledge.recovery_journal import RecoveryJournal
from test_kaggle_apply import pinned
from test_kaggle_pg_driver import database,connect
from test_participants import people
from test_control_spool import withdraw,SOURCE,VERSIONS
from test_recovery_journal import ConditionalMemoryStore

ROOT=Path(__file__).resolve().parents[1]
PG=pytest.mark.skipif(not (os.environ.get('RKB_PG_DRIVER_FIXTURE_SOCKET') or os.environ.get('RKB_KAGGLE_TEST_DSN')),
    reason='explicit checked disposable PG fixture required; root owns execution')
pytestmark=PG


@pytest_asyncio.fixture
async def env(database,people,tmp_path):
    async with database.transaction():
        await database.execute((ROOT/'sql/kaggle/002_recovery.sql').read_text(),prepare=False)
    await initialize_database(database,database_generation='db-source',writer_generation='writer-1',revisions=VERSIONS)
    async with database.transaction():
        for name in ('003_search_scope.sql','004_read_fences.sql','005_source_withdraw.sql'):
            await database.execute((ROOT/'sql/kaggle'/name).read_text(),prepare=False)
    registry=HandlerRegistry([HandlerSpec('domain_command',SOURCE_WITHDRAW,VERSIONS,
        await pinned(database,'check_source_withdraw_v1'),await pinned(database,'apply_source_withdraw_v1'),
        frozenset({'eligibility_revision'}),dependency=WITHDRAW_DEPENDENCY)])
    journal=RecoveryJournal(ConditionalMemoryStore(),stream='source-withdraw-real-pg-synthetic')
    queue=BgeQueue(tmp_path/'queue.sqlite')
    spool=ControlSpool(queue,people.registry,journal,registry)
    e=SimpleNamespace(db=database,p=people,registry=registry,journal=journal,spool=spool,
        cmd=withdraw(people.owner.subject))
    yield e
    spool.close()


async def publish(e,*,text=True,dense=True):
    async with e.db.transaction():
        await e.db.execute("SELECT rkb_v4.publish_read_frontier('db-source','writer-1',0,0,%s,%s)",(text,dense))


async def accepted(e,cmd=None):
    cmd=cmd or e.cmd
    await e.spool.stage_source_withdraw(e.p.admin,cmd,database_generation='db-source')
    return await e.spool.seal(e.p.admin,cmd.operation_key,predecessor=await e.journal.head(),writer_generation='writer-1')


async def facts(e):
    async with e.db.transaction():
        return await (await e.db.execute('''SELECT src.withdrawn,v.eligibility_revision,s.applied_seq,
            f.projection_seq,f.text_ready,f.dense_ready,f.visibility_epoch,
            (SELECT count(*) FROM rkb_v4.applied_ops)
            FROM rkb_v4.sources src JOIN rkb_v4.source_visibility v USING(source_id)
            CROSS JOIN rkb_v4.recovery_state s CROSS JOIN rkb_v4.read_frontier f WHERE src.source_id=%s''',(SOURCE,))).fetchone()


@pytest.mark.asyncio
async def test_real_withdraw_effect_eligibility_ledger_frontier_and_remote_receipt_are_exact(env):
    e=env
    await publish(e);await accepted(e)
    result=await e.spool.apply(e.cmd.operation_key,e.db,writer_generation='writer-1')
    assert result.status=='applied'
    assert await facts(e)==(True,2,1,1,True,True,1,1)
    body=json.loads(base64.b64decode(json.loads(result.outcome)['result_b64']))
    assert body['source_id']==SOURCE and body['eligibility_revision']==2 and body['content_revision']==1
    async with e.db.transaction():
        assert (await (await e.db.execute("SELECT count(*) FROM rkb_v4.lexical_candidates('semantic harbour',20)")).fetchone())[0]==0
    duplicate=await e.spool.apply(e.cmd.operation_key,e.db,writer_generation='writer-1')
    assert duplicate.outcome==result.outcome and await facts(e)==(True,2,1,1,True,True,1,1)
    remote=await e.spool.publish_outcome(e.cmd.operation_key,writer_generation='writer-1')
    assert remote.outcome_sha256==result.outcome_sha256
    assert (await e.spool.barriers_for((SOURCE,))).barriers[0].restriction_active


@pytest.mark.asyncio
@pytest.mark.parametrize('prior_ready',[None,(True,False),(False,False)])
async def test_withdraw_cannot_make_unready_projection_ready(env,prior_ready):
    e=env
    if prior_ready is not None:await publish(e,text=prior_ready[0],dense=prior_ready[1])
    await accepted(e)
    await e.spool.apply(e.cmd.operation_key,e.db,writer_generation='writer-1')
    after=await facts(e)
    assert after[:4]==(True,2,1,1)
    assert after[4:6]==(prior_ready or (False,False))


@pytest.mark.asyncio
async def test_wrong_eligibility_rejected_without_effect_but_local_intent_never_reopens(env):
    e=env
    await publish(e)
    cmd=replace(e.cmd,expected_revisions={'eligibility_revision':'9'})
    await accepted(e,cmd)
    result=await e.spool.apply(cmd.operation_key,e.db,writer_generation='writer-1')
    assert result.status=='rejected_precondition'
    assert await facts(e)==(False,1,1,1,True,True,0,1)
    await e.spool.publish_outcome(cmd.operation_key,writer_generation='writer-1')
    barrier=(await e.spool.barriers_for((SOURCE,))).barriers[0]
    assert barrier.restriction_active and barrier.intent_remote_accepted
    assert barrier.apply_state=='REJECTED_PRECONDITION'


@pytest.mark.asyncio
async def test_failure_after_domain_effect_rolls_back_eligibility_frontier_and_cursor(env):
    e=env
    await publish(e);await accepted(e)
    async with e.db.transaction():
        await e.db.execute('''CREATE FUNCTION rkb_v4.fail_withdraw_ledger() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN RAISE EXCEPTION 'synthetic ledger write rejection'; END $$;
            CREATE TRIGGER fail_withdraw_ledger BEFORE INSERT ON rkb_v4.applied_ops
            FOR EACH ROW EXECUTE FUNCTION rkb_v4.fail_withdraw_ledger();''',prepare=False)
    with pytest.raises(ApplyExecutionError):await e.spool.apply(e.cmd.operation_key,e.db,writer_generation='writer-1')
    assert await facts(e)==(False,1,0,0,True,True,0,0)
    assert (await e.spool.get(e.cmd.operation_key)).apply_state=='SENT_UNKNOWN'
    async with e.db.transaction():await e.db.execute('DROP TRIGGER fail_withdraw_ledger ON rkb_v4.applied_ops')
    await e.spool.apply(e.cmd.operation_key,e.db,writer_generation='writer-1')
    assert await facts(e)==(True,2,1,1,True,True,1,1)


@pytest.mark.asyncio
async def test_actual_commit_lost_ack_reconciles_original_generation_without_second_withdraw(env,monkeypatch):
    e=env
    await publish(e);await accepted(e)
    connection=await connect();original=connection._exec_command;dropped=False
    def lose_ack(statement,*args,**kwargs):
        nonlocal dropped
        yield from original(statement,*args,**kwargs)
        if statement==b'COMMIT' and not dropped:
            dropped=True
            raise psycopg.OperationalError('synthetic lost native commit ACK')
    monkeypatch.setattr(connection,'_exec_command',lose_ack)
    try:
        with pytest.raises(ApplyUnknown):await e.spool.apply(e.cmd.operation_key,connection,writer_generation='writer-1')
    finally:await connection.close()
    assert dropped and (await e.spool.get(e.cmd.operation_key)).apply_state=='SENT_UNKNOWN'
    fresh=await connect()
    try:
        reconciled=await e.spool.apply(e.cmd.operation_key,fresh,writer_generation='writer-1')
        assert reconciled.duplicate and await facts(e)==(True,2,1,1,True,True,1,1)
    finally:await fresh.close()
