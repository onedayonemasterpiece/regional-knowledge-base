"""A real rolled-back source command receives a real PG rejection; worker survives."""
import base64
from contextlib import asynccontextmanager
from dataclasses import replace
import json
from types import SimpleNamespace

import pytest

from regional_knowledge.kaggle_apply import HandlerRegistry
from regional_knowledge.kaggle_source_commands import WorkerSourceExecutor,decode_receipt,seal_wire,digest
from test_kaggle_apply import env,accepted,item,pinned,facts,PG
from test_kaggle_pg_driver import database


def frame(command,seal,handler):
    return dict(type='DISPATCH',protocol_version=2,connection_generation=1,id='job-'+str(command.sequence),
        claim='claim',kind='source_command',run_id='same-run',launch_nonce='same-nonce',runtime_incarnation='same-incarnation',
        database_generation='db-1',writer_generation='writer-1',command_sha256=digest(command.encode()),
        handler_sha256=handler.fingerprint,command_b64=base64.b64encode(command.encode()).decode(),
        accepted=seal_wire(seal),compute_claim='same-compute',dispatch_generation=1)


@PG
@pytest.mark.asyncio
async def test_real_pg_rollback_rejection_duplicate_next_command_and_read_on_same_worker(env):
    connection=env['connection']
    async with connection.transaction():
        await connection.execute('''CREATE FUNCTION rkb_v4.apply_item_maybe_fail(p bytea,ids jsonb,expected jsonb,logical_time text)
            RETURNS bytea LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
            DECLARE result bytea;
            BEGIN
             result:=rkb_v4.apply_item_v1(p,ids,expected,logical_time);
             IF ids->>'item_id'='bad' THEN RAISE EXCEPTION 'synthetic failure after actual domain effect';END IF;
             RETURN result;
            END $$''')
    handler=replace(env['registry'].resolve(item()),apply=await pinned(connection,'apply_item_maybe_fail'))
    registry=HandlerRegistry([handler])
    @asynccontextmanager
    async def acquire():yield connection
    worker=WorkerSourceExecutor(acquire=acquire,registry=registry,
        database=SimpleNamespace(database_generation='db-1',writer_generation='writer-1'),
        run_id='same-run',launch_nonce='same-nonce',runtime_incarnation='same-incarnation')
    bad=replace(item(),ids={'item_id':'bad'});seal=await accepted(env,bad);request=frame(bad,seal,handler)
    receipt,outcome=decode_receipt(await worker(request))
    assert receipt.status=='rejected_precondition' and not receipt.duplicate
    assert json.loads(base64.b64decode(outcome['result_b64']))=={'reason':'native_source_sql_rolled_back'}
    # This is read from the actual PG ledger, not the worker's Python response.
    assert await facts(connection)==((1,'s1','f1','e1'),1,0,[])
    duplicate,_=decode_receipt(await worker(request))
    assert duplicate.duplicate and duplicate.outcome==receipt.outcome
    good=item(2);seal=await accepted(env,good)
    following,_=decode_receipt(await worker(frame(good,seal,handler)))
    assert following.status=='applied'
    state,ledger,calls,rows=await facts(connection)
    assert state[0]==2 and ledger==2 and calls==1
    assert rows==[('chosen-exact-id',1,'accepted exact body 2',good.logical_time)]
    assert worker.identity==('same-run','same-nonce','same-incarnation')


@pytest.mark.asyncio
async def test_preparation_sql_rollback_uses_real_apply_boundary_but_uncertain_connection_does_not(monkeypatch):
    import psycopg
    from regional_knowledge.kaggle_apply import ApplyUnknown
    from test_native_source_worker_wiring import worker_fixture
    from regional_knowledge.source_stage import SEAL_ROUTE
    value=await worker_fixture(SEAL_ROUTE,monkeypatch,
        materializer_error=psycopg.errors.InvalidParameterValue('synthetic rolled-back preparation'),
        status='rejected_precondition')
    receipt,_=decode_receipt(await value.worker(value.frame))
    assert receipt.status=='rejected_precondition'
    assert value.calls[0]['reject_reason']=='native_source_sql_rolled_back'
    value=await worker_fixture(SEAL_ROUTE,monkeypatch,
        materializer_error=psycopg.OperationalError('synthetic lost connection'))
    with pytest.raises(ApplyUnknown):await value.worker(value.frame)
    assert value.calls==[]


@pytest.mark.asyncio
async def test_unknown_apply_never_requests_a_rejected_receipt(monkeypatch):
    from test_native_source_worker_wiring import worker_fixture
    from regional_knowledge.source_stage import STAGE_ROUTE
    from regional_knowledge.kaggle_apply import ApplyUnknown
    value=await worker_fixture(STAGE_ROUTE,monkeypatch)
    calls=[]
    async def unknown(connection,**kwargs):
        calls.append(kwargs)
        raise ApplyUnknown('synthetic unknown commit')
    monkeypatch.setattr('regional_knowledge.kaggle_source_commands.apply_accepted',unknown)
    with pytest.raises(ApplyUnknown):await value.worker(value.frame)
    assert len(calls)==1 and 'reject_reason' not in calls[0]
