"""Real queue/control + native S3 conditional adapter; SDK effects synthetic."""
import asyncio
from dataclasses import replace
import json
import sqlite3
import threading
from types import SimpleNamespace

import pytest
import pytest_asyncio

from regional_knowledge.bge_queue import BgeQueue
from regional_knowledge.kaggle_readiness import SearchRuntimeExpectation
from regional_knowledge.kaggle_target_launch import persist_empty_intent
from regional_knowledge.queue_writer import QueueWriterAuthority
from regional_knowledge.recovery_journal import RecoveryJournal,StaleWriter,JournalUnknown,StoreUnavailable
from regional_knowledge.recovery_s3 import S3ConditionalRecoveryStore
from test_read_authority import auth
from test_participants import people
from test_kaggle_target_runtime import spec,hashes
from test_recovery_s3 import Client


@pytest_asyncio.fixture
async def owner(auth,tmp_path):
    q=BgeQueue(tmp_path/'control.sqlite',clock=lambda:1000.)
    q.enqueue(auth.p.owner.subject,'fixture-intake',['synthetic'],kind='document')
    run=q.maintenance()[0]['id'];nonce=q.prepare_channel(run)
    a=q.configure_shared_reads(auth.resolver)
    writer=QueueWriterAuthority(q,run_id=run,database_generation='synthetic-db',writer_generation='writer-1',stream='writer-fixture')
    client=Client()
    client.meta.endpoint_url='https://existing-s3.fixture.invalid'
    import hashlib
    prefix='recovery/v1/'+hashlib.sha256(writer.stream.encode()).hexdigest()+'/'
    store=S3ConditionalRecoveryStore(client=client,bucket='existing-private',prefix=prefix,writer_guard=writer)
    journal=RecoveryJournal(store,stream=writer.stream)
    await persist_empty_intent(q,run,spec(fresh=True),launch_nonce=nonce,recovery_journal=journal,sql_hashes=hashes())
    expected=SearchRuntimeExpectation({key: 'a' * 64 for key in ('code', 'model', 'launch_config', 'executed_source', 'dependency', 'schema')}, 'synthetic-db', 0)
    q.expect_search_runtime(run,expected)
    token=q.launch_claim(run);q.launch_record(run,'owner/stable-slug',provider_version=88)
    generation=q.attach_channel(run,token,nonce,'retained-incarnation')['connection_generation']
    value=dict(hashes=dict(expected.hashes), database_generation='synthetic-db', schema_version=expected.schema_version, applied_seq=0, projection_seq=0, visibility_epoch=0, text_search_ready=True, dense_search_ready=True, archive_read_ready=None, proof_ready=None)
    assert q.accept_search_runtime(run,token,generation,value).ok
    q.heartbeat(run,token,ready=True,connection_generation=generation)
    await a.resource_state(run,token,generation,runtime_incarnation='retained-incarnation',state='idle')
    await writer.ensure_current(journal)
    yield SimpleNamespace(q=q,a=a,w=writer,c=client,s=store,j=journal,run=run,token=token,generation=generation,
        key=prefix+'head',auth=auth)
    if client.release is not None:client.release.set()
    writer.close();a.close()


def marker(e):
    with e.q.connect(write=False) as db:return db.execute('SELECT publication_json FROM rkb_writer_control').fetchone()[0]


@pytest.mark.asyncio
async def test_exact_current_draining_writer_can_finish_checkpoint_but_not_expired_writer(owner):
    e=owner
    with e.q.connect() as db:
        db.execute("UPDATE runs SET status='draining' WHERE id=?",(e.run,))
    assert e.w.current(database_generation='synthetic-db',writer_generation='writer-1')
    assert await e.s.compare_and_swap(e.key,b'final-checkpoint',expected_token=None,writer_generation='writer-1')
    with e.q.connect() as db:
        db.execute("UPDATE runs SET status='expired' WHERE id=?",(e.run,))
    assert not e.w.current(database_generation='synthetic-db',writer_generation='writer-1')
    with pytest.raises(StaleWriter):
        await e.s.compare_and_swap(e.key,b'forbidden',expected_token=None,writer_generation='writer-1')


@pytest.mark.asyncio
async def test_real_guard_blocks_retirement_and_control_handover_through_native_io(owner):
    e=owner;e.c.release=threading.Event()
    writing=asyncio.create_task(e.s.compare_and_swap(e.key,b'exact',expected_token=None,writer_generation='writer-1'))
    assert await asyncio.to_thread(e.c.entered.wait,2)
    retiring=asyncio.create_task(e.w.retire('writer-1'))
    await asyncio.sleep(.05)
    assert not retiring.done() and json.loads(marker(e))['state']=='IN_FLIGHT'
    with e.q.connect() as db:
        with pytest.raises(sqlite3.IntegrityError,match='publication unresolved'):
            db.execute('UPDATE control SET current_run=NULL WHERE id=1')
    # Ready reads remain SQLite snapshots; native I/O holds no DB writer txn.
    assert e.w.current(database_generation='synthetic-db',writer_generation='writer-1')
    e.c.release.set();assert await writing
    await retiring
    assert marker(e) is None and not e.w.current(database_generation='synthetic-db',writer_generation='writer-1')
    with pytest.raises(StaleWriter):
        await e.s.compare_and_swap(e.key,b'forbidden',expected_token=None,writer_generation='writer-1')
    assert len([call for call in e.c.calls if call[0]=='put'])==1


@pytest.mark.asyncio
async def test_cancel_drains_sdk_and_requires_exact_native_reconciliation_before_retire(owner):
    e=owner;e.c.release=threading.Event()
    task=asyncio.create_task(e.s.compare_and_swap(e.key,b'exact',expected_token=None,writer_generation='writer-1'))
    assert await asyncio.to_thread(e.c.entered.wait,2)
    for _ in range(3):task.cancel();await asyncio.sleep(0)
    retiring=asyncio.create_task(e.w.retire('writer-1'));await asyncio.sleep(.05)
    assert not task.done() and not retiring.done()
    e.c.release.set()
    with pytest.raises(asyncio.CancelledError):await task
    with pytest.raises(JournalUnknown):await retiring
    assert json.loads(marker(e))['state']=='UNKNOWN'
    assert await e.w.reconcile(e.s)
    await e.w.retire('writer-1')


@pytest.mark.asyncio
async def test_lost_ack_restart_retains_unknown_and_exact_body_readback(owner):
    e=owner;e.c.lost_ack=True
    with pytest.raises(StoreUnavailable):
        await e.s.compare_and_swap(e.key,b'exact',expected_token=None,writer_generation='writer-1')
    reopened=QueueWriterAuthority(e.q,run_id=e.run,database_generation='synthetic-db',writer_generation='writer-1',stream=e.j.stream)
    recovered=S3ConditionalRecoveryStore(client=e.c,bucket=e.s.bucket,prefix=e.s.prefix,writer_guard=reopened)
    with pytest.raises(JournalUnknown):
        await recovered.compare_and_swap(e.key,b'new',expected_token=None,writer_generation='writer-1')
    assert await reopened.reconcile(recovered)
    assert marker(e) is None
    await reopened.ensure_current(RecoveryJournal(recovered,stream=e.j.stream))  # Exact restart, nonempty head allowed.
    assert reopened.current(database_generation='synthetic-db',writer_generation='writer-1')


@pytest.mark.asyncio
async def test_absence_cannot_clear_unknown_but_original_conditional_retry_can(owner):
    e=owner;e.c.put_failure=TimeoutError('private provider error')
    with pytest.raises(StoreUnavailable):
        await e.s.compare_and_swap(e.key,b'exact',expected_token=None,writer_generation='writer-1')
    with pytest.raises(JournalUnknown):await e.w.reconcile(e.s)
    with pytest.raises(ValueError):await e.w.reconcile(e.s,exact_data=b'wrong')
    assert marker(e) is not None
    e.c.put_failure=None
    assert await e.w.reconcile(e.s,exact_data=b'exact')
    assert e.c.objects[e.key][0]==b'exact' and marker(e) is None


@pytest.mark.asyncio
async def test_separate_owner_instances_serialize_and_old_generation_never_reactivates(owner):
    e=owner
    other=QueueWriterAuthority(e.q,run_id=e.run,database_generation='synthetic-db',writer_generation='writer-1',stream=e.j.stream)
    S3ConditionalRecoveryStore(client=e.c,bucket=e.s.bucket,prefix=e.s.prefix,writer_guard=other)
    e.c.release=threading.Event()
    task=asyncio.create_task(e.s.compare_and_swap(e.key,b'exact',expected_token=None,writer_generation='writer-1'))
    assert await asyncio.to_thread(e.c.entered.wait,2)
    waiting=asyncio.create_task(other.retire('writer-1'));await asyncio.sleep(.05)
    assert not waiting.done()
    e.c.release.set();await task;await waiting
    with pytest.raises(StaleWriter):await e.w.ensure_current(e.j)
    with pytest.raises(StaleWriter):
        await e.s.compare_and_swap(e.key,b'new',expected_token=None,writer_generation='writer-2')


@pytest.mark.asyncio
async def test_waiter_cancel_cannot_release_another_publication_and_control_transition_retires_known_writer(owner):
    e=owner;e.c.release=threading.Event()
    writing=asyncio.create_task(e.s.compare_and_swap(e.key,b'exact',expected_token=None,writer_generation='writer-1'))
    assert await asyncio.to_thread(e.c.entered.wait,2)
    waiter=asyncio.create_task(e.w.retire('writer-1'));await asyncio.sleep(.03);waiter.cancel()
    with pytest.raises(asyncio.CancelledError):await waiter
    assert json.loads(marker(e))['state']=='IN_FLIGHT'
    with e.q.connect() as db:
        assert e.w.current(database_generation='synthetic-db',writer_generation='writer-1')
    e.c.release.set();await writing
    with e.q.connect() as db:db.execute('UPDATE control SET current_run=NULL WHERE id=1')
    assert not e.w.current(database_generation='synthetic-db',writer_generation='writer-1')
    with pytest.raises(StaleWriter):await e.w.ensure_current(e.j)


@pytest.mark.asyncio
async def test_existing_storage_authority_cannot_be_swapped(owner):
    e=owner
    with pytest.raises(StaleWriter,match='storage authority'):
        S3ConditionalRecoveryStore(client=e.c,bucket='different',prefix=e.s.prefix,writer_guard=e.w)
