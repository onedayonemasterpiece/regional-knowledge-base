"""Lost migration receipt across the actual queue/admission recovery boundary."""
import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
import hashlib
import json
from types import SimpleNamespace as NS

import pytest

from regional_knowledge.kaggle_pg_snapshot_native import NativeSnapshotFactory
from regional_knowledge.kaggle_public_backend import current_binding,TargetUnavailable
from regional_knowledge.kaggle_recovery_dispatch import RecoveryPhaseDispatch
from regional_knowledge.kaggle_recovery_phases import WorkerRecoveryPhases
from regional_knowledge.kaggle_restore_startup import SourceRestoreCoordinator
from regional_knowledge.kaggle_schema_owner import SourceSchemaMigrations,request_migration
from regional_knowledge.kaggle_service_startup import TargetServiceStartup
from test_bge_runtime_readiness import run,expected,observation
from test_read_authority import auth
from test_participants import people
from test_kaggle_recovery_phases import database
from test_bge_shared_channel import peer


async def owners(queue,ident,nonce,monkeypatch):
    initial=current_binding(queue);db=database();version=db.schema_version+1
    spool=NS(_lock=asyncio.Lock(),_source_covered_sequence=5,_writing=0)
    with queue.connect() as conn:
        conn.execute('CREATE TABLE rkb_control_operations(seal_state TEXT,apply_state TEXT,outcome_state TEXT)')
        conn.execute('CREATE TABLE rkb_writer_control(run_id TEXT,writer_generation TEXT,publication_json TEXT)')
    startup=object.__new__(TargetServiceStartup)
    startup.config=NS(queue=queue,spool=spool,writer_generation=db.writer_generation,
        history_adapter=NS(database=db),writer_owner=NS(run_id=ident),writer_current=lambda **kwargs:True)
    startup.dataset_seed=None;startup.closed=False;startup.running=True
    startup.gate=NS(invalidate=lambda:None)
    proof=NS(current=lambda:True,accepted_sequence=5,applied_sequence=5)
    startup.coverage=proof
    startup.history=NS(_coverage=None,schema_version=db.schema_version,database=db)
    restorer=object.__new__(SourceRestoreCoordinator)
    restorer.startup=startup;restorer.queue=queue;restorer.database=db;restorer.complete=True;restorer.proof=None
    restorer.config=NS(launch_config_sha256=expected().hashes['launch_config'])
    restorer.writer=NS(run_id=ident,current=lambda **kwargs:True,recovery_current=lambda _:False)
    startup.restorer=restorer
    bridge=RecoveryPhaseDispatch(queue,database=db,identity=(ident,nonce,initial.runtime_incarnation),binding_current=restorer.current)
    restorer.dispatch=bridge
    owner=startup.schema_migrations=SourceSchemaMigrations(startup)
    checkpoints=[]
    async def checkpoint(binding,**kwargs):
        assert binding==current_binding(queue) and kwargs['schema_version']==version
        assert kwargs['expected_sequence']==5
        checkpoints.append(kwargs['operation_key'])
    startup.dataset_checkpoints=NS(checkpoint_schema=checkpoint)
    from regional_knowledge.kaggle_schema_live import LiveSchemaMigrations,bundle_hash
    files={f'sql/{n:03d}_fixture.sql':b'SELECT 1;' for n in range(1,version+1)}
    key=request_migration(queue,target_schema_version=version,bundle_sha256=bundle_hash(files),
        database_generation=db.database_generation,writer_generation=db.writer_generation,run_id=ident)
    native=object.__new__(NativeSnapshotFactory)
    native.database=db;native.identity=bridge.identity
    native._current=lambda binding:None
    worker=WorkerRecoveryPhases(factory=native,download=lambda *args:None,workspace=lambda *args:None)
    effects=[]
    migrated=observation(schema_version=version,hashes=dict(expected().hashes,
        schema=hashlib.sha256(json.dumps(version).encode()).hexdigest()))
    ledger=[db.schema_version]
    class Connection:
        @asynccontextmanager
        async def transaction(self):yield
        async def execute(self,query,*args,**kwargs):
            self.row=(db.database_generation,db.writer_generation,False) if 'w.retired' in query else (ledger[0],)
            return self
        async def fetchone(self):return self.row
    @asynccontextmanager
    async def acquire():yield Connection()
    async def migrate(connection,files,*,before_file):
        await before_file(connection)
        effects.append(version);ledger[0]=version
        return (version,)
    async def promote(actual):
        assert actual==version
        native.database=replace(db,schema_version=version)
        return migrated
    monkeypatch.setattr('regional_knowledge.kaggle_schema_live.migrate',migrate)
    worker.schema_control=LiveSchemaMigrations(files=files,acquire=acquire,current=lambda _:None,
        download=None,database=db,on_version=promote)
    bridge.rebind_current=owner.resume_attachment
    return NS(initial=initial,db=db,version=version,startup=startup,owner=owner,bridge=bridge,
        worker=worker,effects=effects,checkpoints=checkpoints,key=key,migrated=migrated)


@pytest.mark.asyncio
async def test_lost_receipt_after_worker_promotion_reconciles_before_ready_then_one_checkpoint(run,auth,monkeypatch):
    queue,ident,token,nonce=run
    admission=queue.configure_shared_reads(auth.resolver)
    generation=queue.attach_channel(ident,token,nonce,'incarnation')['connection_generation']
    assert queue.accept_search_runtime(ident,token,generation,observation()).ok
    queue.heartbeat(ident,token,ready=True,connection_generation=generation)
    await admission.resource_state(ident,token,generation,runtime_incarnation='incarnation',state='idle')
    p=await owners(queue,ident,nonce,monkeypatch)
    initial,db,version,startup,owner,bridge,worker,effects,checkpoints,key,migrated=(
        p.initial,p.db,p.version,p.startup,p.owner,p.bridge,p.worker,p.effects,p.checkpoints,p.key,p.migrated)
    producer=asyncio.create_task(owner.advance(initial))
    async def next_work():
        for _ in range(200):
            work=await admission.next_channel_work(ident,token,generation)
            if work is not None:return work
            await asyncio.sleep(.001)
        pytest.fail('original migration became ineligible')
    def frame(work):
        return dict(type='DISPATCH',protocol_version=2,connection_generation=generation,
            **dict(work.document_payload),compute_claim=work.compute_claim,dispatch_generation=work.dispatch_generation)
    try:
        first=await next_work();admission.mark_work_sent(first)
        first_frame=frame(first);receipt=await worker(first_frame)
        assert receipt['value']['schema_version']==version and len(effects)==1
        # The worker committed/promoted, but its result never reached receive().
        generation=queue.attach_channel(ident,token,nonce,'incarnation')['connection_generation']
        assert not queue.accept_search_runtime(ident,token,generation,migrated).ok
        with pytest.raises(TargetUnavailable):current_binding(queue)
        resource=first.resource
        await admission.resource_state(ident,token,generation,runtime_incarnation='incarnation',state='busy',
            resource_kind='document',resource_id=resource.resource_id,compute_claim=resource.compute_claim,
            dispatch_generation=resource.dispatch_generation)
        # Actual startup maintenance can rebind the original job without READY,
        # including while the old producer is still waiting for its lost result.
        assert not await startup.refresh()
        # The worker still owns the original compute claim and re-emits its
        # retained result; the new attachment does not allocate another job.
        new_frame=dict(first_frame,connection_generation=generation)
        repeated=await worker(new_frame)
        result=dict(job_id=new_frame['id'],claim=new_frame['claim'],receipt=repeated,
            run_id=ident,launch_nonce=nonce,runtime_incarnation='incarnation',
            compute_claim=new_frame['compute_claim'],dispatch_generation=new_frame['dispatch_generation'])
        await bridge.receive(result,run_id=ident,token=token,generation=generation)
        await admission.resource_released(ident,token,generation,runtime_incarnation='incarnation',
            resource_kind='document',resource_id=resource.resource_id,compute_claim=resource.compute_claim,
            dispatch_generation=resource.dispatch_generation)
        with pytest.raises(PermissionError,match='binding expired'):await producer
        assert queue.accept_search_runtime(ident,token,generation,migrated).ok
        queue.heartbeat(ident,token,ready=True,connection_generation=generation)
        # The old READY was invalidated during the lost receipt. Simulate the
        # existing startup binding re-establishing its public coverage cursor;
        # no history-verifier private cache is needed for this consumer.
        assert startup.coverage is None
        startup.coverage=proof
        assert await owner.advance(current_binding(queue))
        assert not await owner.advance(current_binding(queue))
        assert len(effects)==len(checkpoints)==1 and effects==[version]
        assert owner.pending() is None and startup.history.schema_version==version
        with queue.connect(write=False) as conn:
            assert conn.execute("SELECT count(*) FROM jobs WHERE kind='recovery_phase'").fetchone()[0]==1
            assert tuple(conn.execute("SELECT attempt,state FROM jobs WHERE kind='recovery_phase'").fetchone())==(1,'done')
    finally:
        if not producer.done():producer.cancel()
        await asyncio.gather(producer,return_exceptions=True)
        await bridge.aclose();admission.close()


def test_retained_result_beats_startup_refresh_through_actual_channel(peer,monkeypatch):
    from test_bge_shared_channel import connect,attach,send
    from regional_knowledge.bge_contract import REVISION,SPACE
    p=peer
    p.q.expect_search_runtime(p.run,expected())
    # The fixture's generic placeholder demand is not part of source migration.
    with p.q.connect() as conn:conn.execute("UPDATE jobs SET state='done' WHERE kind='document'")
    held={};ready_seen=[]
    def receive(socket,generation,kind):
        for _ in range(20):
            message=socket.receive_json()
            if message['type']=='RESOURCE_PROBE':
                send(socket,generation,'RESOURCE_STATE',state='busy' if held else 'idle',
                    probe_nonce=message['probe_nonce'],**held)
            elif message['type']=='READY_ACK':
                ready_seen.append(message)
                if kind=='READY_ACK':return message
            elif message['type']==kind:return message
            else:raise AssertionError(message['type'])
        pytest.fail('bounded channel observation exhausted')
    state=producer=None
    try:
        with connect(p) as socket:
            generation=attach(p,socket)
            send(socket,generation,'RESOURCE_STATE',state='idle')
            send(socket,generation,'READY',space=SPACE,model_revision=REVISION,runtime_readiness=observation())
            assert receive(socket,generation,'READY_ACK')['status']=='ready'
            state=p.client.portal.call(owners,p.q,p.run,p.nonce,monkeypatch)
            producer=p.client.portal.start_task_soon(state.owner.advance,state.initial)
            frame=receive(socket,generation,'DISPATCH')
            assert frame['kind']=='recovery_phase' and frame['phase']=='schema_migrate'
            held=dict(resource_kind='document',resource_id=frame['id'],compute_claim=frame['compute_claim'],
                dispatch_generation=frame['dispatch_generation'])
            send(socket,generation,'RESOURCE_STATE',state='busy',**held)
            receipt=p.client.portal.call(state.worker,frame)
            assert len(state.effects)==1
            # No result or release reaches the original connection.
        with connect(p) as socket:
            generation=attach(p,socket)
            send(socket,generation,'RESOURCE_STATE',state='busy',**held)
            send(socket,generation,'READY',space=SPACE,model_revision=REVISION,runtime_readiness=state.migrated)
            assert receive(socket,generation,'READY_ACK')['status']=='runtime_not_ready'
            # Deliver before any startup.refresh. A stale ACK would make the
            # real worker discard this retained result and ignore that claim.
            send(socket,generation,'RECOVERY_PHASE_RESULT',job_id=frame['id'],claim=frame['claim'],receipt=receipt,
                run_id=p.run,launch_nonce=p.nonce,runtime_incarnation='synthetic-incarnation',
                compute_claim=frame['compute_claim'],dispatch_generation=frame['dispatch_generation'])
            assert receive(socket,generation,'RESULT_ACK')['status']=='accepted'
            send(socket,generation,'RESOURCE_RELEASED',**held);held.clear()
            if not any(m['status']=='ready' and m['connection_generation']==generation for m in ready_seen):
                assert receive(socket,generation,'READY_ACK')['status']=='ready'
            with pytest.raises(PermissionError,match='binding expired'):producer.result(timeout=3)
            assert p.client.portal.call(state.owner.advance,current_binding(p.q))
            assert state.owner.pending() is None and len(state.effects)==len(state.checkpoints)==1
            with p.q.connect(write=False) as conn:
                jobs=conn.execute("SELECT id,claim,attempt,state FROM jobs WHERE kind='recovery_phase'").fetchall()
                assert [tuple(j) for j in jobs]==[(frame['id'],frame['claim'],1,'done')]
    finally:
        if producer is not None and not producer.done():producer.cancel()
        if state is not None:p.client.portal.call(state.bridge.aclose)
