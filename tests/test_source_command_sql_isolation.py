"""A real rolled-back source command receives a real PG rejection; worker survives."""
import base64
from contextlib import asynccontextmanager
import json
from types import SimpleNamespace

import pytest

from regional_knowledge.kaggle_source_commands import WorkerSourceExecutor,decode_receipt,seal_wire,digest
from test_kaggle_apply import PG
from test_source_activation_pg import image_pg_sql18,accepted,stage_command,DB,WRITER
from test_source_batch_pg import packet
from test_native_source_materializer import exact_batch
from regional_knowledge.source_stage import canonical
from regional_knowledge.kaggle_source_materializer import NativeSourceMaterializer
from test_kaggle_pg_driver import database


def frame(command,seal,handler):
    return dict(type='DISPATCH',protocol_version=2,connection_generation=1,id='job-'+str(command.sequence),
        claim='claim',kind='source_command',run_id='same-run',launch_nonce='same-nonce',runtime_incarnation='same-incarnation',
        database_generation=DB,writer_generation=WRITER,command_sha256=digest(command.encode()),
        handler_sha256=handler.fingerprint,command_b64=base64.b64encode(command.encode()).decode(),
        accepted=seal_wire(seal),compute_claim='same-compute',dispatch_generation=1)


@PG
@pytest.mark.asyncio
async def test_real_pg_rollback_rejection_duplicate_next_command_and_read_on_same_worker(image_pg_sql18):
    env=image_pg_sql18;connection=env['connection'];doc=exact_batch(count=1)
    assert (await accepted(env,stage_command(doc,1))).status=='applied'
    async with connection.transaction():
        await connection.execute('''ALTER FUNCTION rkb_v4.apply_source_batch_v1(bytea,jsonb,jsonb,text)
            RENAME TO fixture_original_source_batch;
            CREATE FUNCTION rkb_v4.apply_source_batch_v1(p bytea,ids jsonb,expected jsonb,logical_time text)
            RETURNS bytea LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
            DECLARE result bytea;
            BEGIN
             result:=rkb_v4.fixture_original_source_batch(p,ids,expected,logical_time);
             IF convert_from(p,'UTF8')::jsonb->>'batch_index'='0' THEN
              RAISE EXCEPTION 'synthetic failure after actual source projection';END IF;
             RETURN result;
            END $$''',prepare=False)
    @asynccontextmanager
    async def acquire():yield connection
    async def count(text):return len(text.split())+2
    async def encode(text):return [1.]+[0.]*1023
    worker=WorkerSourceExecutor(acquire=acquire,registry=env['registry'],
        database=SimpleNamespace(database_generation=DB,writer_generation=WRITER),
        run_id='same-run',launch_nonce='same-nonce',runtime_incarnation='same-incarnation',
        materializer=NativeSourceMaterializer(count=count,encode=encode,current=lambda:True))
    bad=packet(env,doc);handler=env['registry'].resolve(bad)
    seal=await env['journal'].seal([bad],predecessor=await env['journal'].head(),writer_generation=WRITER)
    request=frame(bad,seal,handler)
    receipt,outcome=decode_receipt(await worker(request))
    assert receipt.status=='rejected_precondition' and not receipt.duplicate
    assert json.loads(base64.b64decode(outcome['result_b64']))=={'reason':'native_source_sql_rolled_back'}
    async with connection.transaction():
        assert await (await connection.execute('SELECT count(*) FROM rkb_v4.passage_search')).fetchone()==(0,)
        assert await (await connection.execute('SELECT count(*) FROM rkb_v4.source_batch_receipts')).fetchone()==(0,)
        assert await (await connection.execute('SELECT applied_seq FROM rkb_v4.recovery_state')).fetchone()==(2,)
        assert await (await connection.execute('SELECT status FROM rkb_v4.applied_ops WHERE sequence=2')).fetchone()==('rejected_precondition',)
    duplicate,_=decode_receipt(await worker(request))
    assert duplicate.duplicate and duplicate.outcome==receipt.outcome
    env['sequence']=bad.sequence
    following=exact_batch(1,digest(canonical(doc)),start=1,count=1)
    assert (await accepted(env,stage_command(following,env['sequence']+1))).status=='applied'
    good=packet(env,following)
    seal=await env['journal'].seal([good],predecessor=await env['journal'].head(),writer_generation=WRITER)
    result,_=decode_receipt(await worker(frame(good,seal,handler)))
    assert result.status=='applied'
    async with connection.transaction():
        assert await (await connection.execute('SELECT count(*) FROM rkb_v4.lexical_candidates(%s,20)',('synthetic',))).fetchone()==(1,)
        assert await (await connection.execute('SELECT count(*) FROM rkb_v4.source_batch_receipts')).fetchone()==(1,)
        assert await (await connection.execute('SELECT applied_seq FROM rkb_v4.recovery_state')).fetchone()==(4,)
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
    assert len(calls)==2 and all('reject_reason' not in call for call in calls)


@PG
@pytest.mark.asyncio
@pytest.mark.parametrize('timeouts',[1,2])
async def test_real_prepare_57014_rollback_retries_locally_once_same_key(image_pg_sql18,timeouts):
    """True PG17 timed cancellation is contained before any apply/cursor write."""
    import psycopg
    from psycopg.pq import TransactionStatus
    from regional_knowledge.kaggle_apply import ApplyUnknown
    env=image_pg_sql18;connection=env['connection'];doc=exact_batch(count=1)
    assert (await accepted(env,stage_command(doc,1))).status=='applied'
    async with connection.transaction():
        await connection.execute("""
            CREATE TABLE rkb_v4.fixture_prepare_attempt_marker (attempt integer NOT NULL);
            ALTER FUNCTION rkb_v4.prepare_source_batch_v1(bytea,bytea,jsonb,text,text)
                RENAME TO fixture_prepare_source_batch;
            CREATE FUNCTION rkb_v4.prepare_source_batch_v1(
                command_bytes bytea,knowledge_bytes bytea,media jsonb,
                expected_database text,expected_writer text)
            RETURNS text LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
            BEGIN
                INSERT INTO rkb_v4.fixture_prepare_attempt_marker VALUES (1);
                PERFORM pg_sleep(6);
                RETURN rkb_v4.fixture_prepare_source_batch(
                    command_bytes,knowledge_bytes,media,expected_database,expected_writer);
            END $$;
        """,prepare=False)
    from test_kaggle_pg_driver import connect
    acquisitions=[]
    @asynccontextmanager
    async def acquire():
        owned=await connect()
        acquisitions.append(owned)
        try:yield owned
        finally:await owned.close()
    async def count(text):return len(text.split())+2
    async def encode(text):return [1.]+[0.]*1023
    native=NativeSourceMaterializer(count=count,encode=encode,current=lambda:True)
    calls=[]
    async def materializer(command,*,acquire,database):
        calls.append(command.encode())
        try:return await native(command,acquire=acquire,database=database)
        except psycopg.errors.QueryCanceled:
            # Match NativeSnapshotFactory: the owned acquisition closes after
            # transaction rollback, before WorkerSourceExecutor catches it.
            assert acquisitions[-1].closed
            # Test the *first* rollback before the executor catches it.
            assert connection.info.transaction_status==TransactionStatus.IDLE
            async with connection.transaction():
                assert await (await connection.execute(
                    'SELECT count(*) FROM rkb_v4.fixture_prepare_attempt_marker')).fetchone()==(0,)
                assert await (await connection.execute(
                    'SELECT count(*) FROM rkb_v4.applied_ops WHERE sequence=2')).fetchone()==(0,)
                assert await (await connection.execute(
                    'SELECT applied_seq FROM rkb_v4.recovery_state')).fetchone()==(1,)
            if timeouts==1:
                async with connection.transaction():
                    await connection.execute("""
                        DROP FUNCTION rkb_v4.prepare_source_batch_v1(bytea,bytea,jsonb,text,text);
                        ALTER FUNCTION rkb_v4.fixture_prepare_source_batch(bytea,bytea,jsonb,text,text)
                            RENAME TO prepare_source_batch_v1;
                    """,prepare=False)
            raise
    worker=WorkerSourceExecutor(acquire=acquire,registry=env['registry'],
        database=SimpleNamespace(database_generation=DB,writer_generation=WRITER),
        run_id='same-run',launch_nonce='same-nonce',runtime_incarnation='same-incarnation',
        materializer=materializer)
    cmd=packet(env,doc);handler=env['registry'].resolve(cmd)
    seal=await env['journal'].seal([cmd],predecessor=await env['journal'].head(),writer_generation=WRITER)
    request=frame(cmd,seal,handler)
    if timeouts==2:
        receipt,_=decode_receipt(await worker(request))
        assert receipt.status=='rejected_precondition' and not receipt.duplicate
        assert len(calls)==2 and calls==[cmd.encode()]*2
        async with connection.transaction():
            assert await (await connection.execute(
                "SELECT status FROM rkb_v4.applied_ops WHERE sequence=2")).fetchone()==('rejected_precondition',)
            assert await (await connection.execute(
                'SELECT count(*) FROM rkb_v4.source_batch_prepared WHERE command_sha256=%s',
                (digest(cmd.encode()),))).fetchone()==(0,)
            assert await (await connection.execute(
                'SELECT applied_seq FROM rkb_v4.recovery_state')).fetchone()==(2,)
        duplicate,_=decode_receipt(await worker(request))
        assert duplicate.duplicate and duplicate.outcome==receipt.outcome
    else:
        receipt,_=decode_receipt(await worker(request))
        assert receipt.status=='applied' and not receipt.duplicate
        assert len(calls)==2 and calls==[cmd.encode()]*2
        duplicate,_=decode_receipt(await worker(request))
        assert duplicate.duplicate and duplicate.outcome==receipt.outcome
        async with connection.transaction():
            assert await (await connection.execute(
                'SELECT count(*) FROM rkb_v4.applied_ops WHERE operation_key=%s',
                (cmd.operation_key,))).fetchone()==(1,)
            assert await (await connection.execute(
                'SELECT applied_seq FROM rkb_v4.recovery_state')).fetchone()==(2,)
            assert await (await connection.execute(
                'SELECT count(*) FROM rkb_v4.source_batch_receipts')).fetchone()==(1,)
    assert connection.info.transaction_status==TransactionStatus.IDLE
    assert acquisitions and all(c.closed for c in acquisitions)


@pytest.mark.asyncio
async def test_query_canceled_outside_observed_prepare_cannot_retry_or_reject(monkeypatch):
    import psycopg
    from regional_knowledge.kaggle_apply import ApplyUnknown
    from test_native_source_worker_wiring import worker_fixture
    from regional_knowledge.source_stage import SEAL_ROUTE
    value=await worker_fixture(SEAL_ROUTE,monkeypatch,
        materializer_error=psycopg.errors.QueryCanceled('synthetic outside prepare SQL'))
    with pytest.raises(ApplyUnknown):
        await value.worker(value.frame)
    assert value.events.count('prehook')==1 and value.calls==[]
