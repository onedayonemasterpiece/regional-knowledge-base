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


@pytest.mark.asyncio
async def test_retired_writer_keeps_old_verified_reads_until_ready_successor_handover(owner):
    """Synthetic controller a-d: source reads survive retirement; no write resurrection.

    The successor READY below is an authenticated in-memory/SQLite WSS
    readiness simulation, NOT a PG restore or an actual Kaggle installation.
    """
    from regional_knowledge.control_spool import ControlSpool, SourceCursor
    from regional_knowledge.kaggle_apply import HandlerRegistry
    from regional_knowledge.kaggle_service_startup import TargetServiceStartup, TargetServiceConfig
    from regional_knowledge.kaggle_public_backend import current_binding, TargetUnavailable
    from regional_knowledge.kaggle_protocol import QueryResult
    from test_kaggle_service_startup import HistoryAdapter
    from test_control_spool import handler
    from test_bge_admission import query
    import time

    e=owner
    spool=ControlSpool(e.q,e.auth.p.registry,e.j,HandlerRegistry([handler()]))
    startup=TargetServiceStartup(TargetServiceConfig(e.auth.provider,e.auth.p.registry,
        e.q,spool,HistoryAdapter(),'writer-1',e.w.current,writer_owner=e.w))
    startup.running=True
    binding=current_binding(e.q)
    startup.binding=binding
    startup.coverage=SourceCursor('synthetic-db',binding,None,0,0,(),(),
        startup.read_binding_current)
    try:
        assert startup.reads_current() and startup.imports_current()
        assert startup.binding_current(binding)
        # a) The verified Saved boundary is assumed by this queue fixture.
        # The actual retirement is guarded and rejects further publication.
        await e.w.retire('writer-1')
        assert not e.w.current(database_generation='synthetic-db',writer_generation='writer-1')
        assert e.w.retired_serving(binding)
        assert startup.read_binding_current(binding) and startup.reads_current()
        assert not startup.binding_current(binding) and not startup.imports_current()
        assert not startup.command_binding_current(run_id=binding.run_id,
            runtime_incarnation=binding.runtime_incarnation,
            connection_generation=binding.connection_generation,
            database_generation=binding.database_generation,
            writer_generation='writer-1')
        with pytest.raises(StaleWriter):
            await e.s.compare_and_swap(e.key,b'forbidden-after-retire',
                expected_token=None,writer_generation='writer-1')
        # Read-only maintenance retains the old cursor while the writer is gone.
        assert await startup.refresh() and startup.reads_current()
        # A real existing admission roundtrip must still be possible.
        context=await e.auth.resolver.resolve_oauth(
            e.auth.tokens.access_token,destination=e.auth.destination)
        q=query(context,'retired-writer-read',generation=e.generation)
        ticket=await e.a.begin_read(context,q,dialog_ref='retired-serving',
            deadline=time.monotonic()+1.9)
        work=await e.q.next_channel_work(e.run,e.token,e.generation)
        assert work is not None and work.kind=='read'
        e.a.mark_work_sent(work)
        answer=QueryResult(request_id=work.query.request_id,
            connection_generation=e.generation,turn_generation=work.query.turn_generation,
            status='ok',mode='hybrid',observed_data_seq=0,observed_projection_seq=0)
        assert await e.a.finish_read(e.run,e.token,e.generation,answer)
        assert (await ticket.result).status=='ok'
        resource=work.resource
        assert await e.a.resource_released(e.run,e.token,e.generation,
            runtime_incarnation=resource.runtime_incarnation,
            resource_kind='read',resource_id=resource.resource_id,
            compute_claim=resource.compute_claim,
            dispatch_generation=resource.dispatch_generation)
        # b) A separate successor pointer is prepared; serving current is old.
        with e.q.connect() as db:
            successor=e.q._new_run(db,e.q.clock())
            db.execute('UPDATE control SET successor=? WHERE id=1',(successor,))
        assert current_binding(e.q)==binding
        assert startup.reads_current() and not startup.imports_current()
        next_nonce=e.q.prepare_channel(successor)
        next_token=e.q.launch_claim(successor,provider_ref='owner/stable-slug')
        assert next_token is not None
        e.q.launch_record(successor,'owner/stable-slug',provider_version=89)
        next_gen=e.q.attach_channel(successor,next_token,next_nonce,'next-incarnation')[
            'connection_generation']
        assert startup.reads_current()
        # c) Synthetic READY of the successor executes existing atomic pointer
        # handover, without reactivating the retired old writer.
        e.q.heartbeat(successor,next_token,ready=True,connection_generation=next_gen)
        with e.q.connect(write=False) as db:
            assert db.execute('SELECT current_run FROM control WHERE id=1').fetchone()[0]==successor
            assert db.execute("SELECT status FROM runs WHERE id=?",(e.run,)).fetchone()[0]=='draining'
        assert not startup.reads_current() and not startup.imports_current()
        assert not e.w.retired_serving(binding)
        # d) Existing STOP only becomes eligible after pointer handover.
        assert e.q.stop_eligible(e.run,e.token,e.generation)
    finally:
        await startup.source_dispatch.aclose()
        spool.close()

def marker(e):
    with e.q.connect(write=False) as db:return db.execute('SELECT publication_json FROM rkb_writer_control').fetchone()[0]


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
