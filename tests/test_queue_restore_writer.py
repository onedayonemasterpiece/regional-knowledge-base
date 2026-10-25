"""Recovery-only ownership over real queue SQLite/native conditional adapter."""
import asyncio
from contextlib import asynccontextmanager
import json
import sqlite3
import threading
from types import SimpleNamespace

import pytest
import pytest_asyncio

from regional_knowledge.kaggle_readiness import SearchRuntimeExpectation
from regional_knowledge.kaggle_restore_startup import SourceRestoreConfig,PreReadyRecoveryBinding
from regional_knowledge.queue_writer import QueueWriterAuthority
from regional_knowledge.recovery_journal import RecoveryJournal,ObjectRef,StaleWriter
from regional_knowledge.recovery_s3 import S3ConditionalRecoveryStore
from test_queue_writer import owner
from test_read_authority import auth
from test_participants import people


@pytest_asyncio.fixture
async def recovery_owner(owner):
    e=owner;q=e.q
    from regional_knowledge.kaggle_readiness import RunIdentity,CapabilityReceipt,TerminalObservation
    with q.connect(write=False) as db:old=dict(db.execute('SELECT * FROM runs WHERE id=?',(e.run,)).fetchone())
    identity=RunIdentity(old['provider_ref'],str(old['provider_version']),None,e.run,old['launch_nonce'],old['runtime_incarnation'])
    capabilities=CapabilityReceipt('synthetic-provider','1','test-native-status','synthetic-terminal-proof',
        exact_status=True,status_identifies_execution=True)
    q.clock=lambda:1002.
    observed=TerminalObservation(identity,'complete',1001.,execution_binding_verified=True)
    assert (await e.a.confirm_terminal(identity,capabilities,observed,max_age_seconds=10)).ok
    # The actual execution-qualified native receipt, unlike a Kaggle
    # version-only status, atomically releases old provider ownership.
    with q.connect(write=False) as db:
        terminal=db.execute('select status,launch_state from runs where id=?',(e.run,)).fetchone()
        assert tuple(terminal)==('expired','terminal')
    from regional_knowledge.bge_queue import StaleAttachment
    with pytest.raises(StaleAttachment,match='terminal'):
        q.launch_record(e.run,old['provider_ref'],provider_version=old['provider_version'])
    with q.connect() as db:
        run=q._new_run(db,q.clock());db.execute('UPDATE control SET current_run=? WHERE id=1',(run,))
    nonce=q.prepare_channel(run)
    expected=SearchRuntimeExpectation({key: 'a' * 64 for key in ('code', 'model', 'launch_config', 'executed_source', 'dependency', 'schema')}, 'synthetic-db', 0)
    q.expect_search_runtime(run,expected);token=q.launch_claim(run);q.launch_record(run,'owner/stable-slug',provider_version=89)
    generation=q.attach_channel(run,token,nonce,'restored-incarnation')['connection_generation']
    await e.a.resource_state(run,token,generation,runtime_incarnation='restored-incarnation',state='idle')
    writer=QueueWriterAuthority(q,run_id=run,database_generation='synthetic-db',writer_generation='writer-2',stream=e.j.stream)
    store=S3ConditionalRecoveryStore(client=e.c,bucket=e.s.bucket,prefix=e.s.prefix,writer_guard=writer)
    journal=RecoveryJournal(store,stream=e.j.stream)
    versions=dict(server_version_num=170011,pgvector_version='0.8.2',catalog_sha256='a'*64,model_sha256='b'*64,
        tokenizer_sha256='c'*64,schema_revision='s1',function_revision='f1',executor_revision='e1')
    plan=SourceRestoreConfig(ObjectRef(journal.prefix+'/snapshots/manifests/'+'d'*64,'d'*64,100,0),
        'same-retained-restore',versions,'a'*64,('https://existing-s3.fixture.invalid',),seconds=2)
    binding=PreReadyRecoveryBinding(run,'restored-incarnation',generation,'synthetic-db','writer-2')
    yield SimpleNamespace(old=e,q=q,a=e.a,w=writer,s=store,j=journal,run=run,token=token,generation=generation,
        nonce=nonce,expected=expected,plan=plan,b=binding)
    if e.c.release is not None:e.c.release.set()
    writer.close()




@pytest.mark.asyncio
async def test_execution_proven_old_terminal_allows_honest_pending_for_new_run(recovery_owner):
    """Only native exact terminal, not TTL/version-only, releases the fence."""
    e=recovery_owner  # Uses real SharedReadAdmission.confirm_terminal.
    with e.q.connect(write=False) as db:
        prior=db.execute("select status,launch_state from runs where id=?",(e.old.run,)).fetchone()
        assert tuple(prior)==('expired','terminal')
    # The new run was created after exact provider terminal proof. It is now
    # entitled to warm, but its PG/accepted-tail restore is NOT ready yet.
    demand=e.q.request_startup()
    assert demand['run_id']==e.run
    assert demand['status']=='pending'
    assert e.q.startup_status(demand['startup_ref'])['reason']=='target_warming'
    assert e.q.status()['state']=='starting'


@pytest.mark.asyncio
async def test_recovery_only_writer_fences_old_generation_and_acceptance_until_native_ready_proof(recovery_owner):
    e=recovery_owner
    assert SourceRestoreConfig.from_wire(e.plan.wire()).wire()==e.plan.wire()
    await e.w.begin_restore(e.j,e.b,plan=e.plan)
    assert e.w.recovery_current(e.b) and not e.w.current(database_generation='synthetic-db',writer_generation='writer-2')
    await e.w.begin_restore(e.j,e.b,plan=e.plan)
    for writer,store,generation in ((e.w,e.s,'writer-2'),(e.old.w,e.old.s,'writer-1')):
        with pytest.raises(StaleWriter):await store.compare_and_swap(e.j.head_key,b'forged-head',expected_token=None,writer_generation=generation)
    from regional_knowledge.pg_snapshot_recovery import ControllerRecoveryProof,PhysicalSnapshotRecovery
    from test_kaggle_target_runtime import spec
    from dataclasses import replace
    db=replace(spec().database,writer_generation='writer-2')
    identity=(e.run,e.nonce,e.b.runtime_incarnation)
    @asynccontextmanager
    async def guard(binding):
        assert e.w.recovery_current(binding);yield
    class Native:
        async def verify_initialized(self,binding,reference):
            assert reference=='actual-retained-native-reference'
            return dict(database=db.wire(),identity=list(identity),applied_sequence=0,schema_version=db.schema_version)
    physical=PhysicalSnapshotRecovery(e.j,None,Native(),registry=None,history_verifier=None,
        maintenance_guard=guard,current=lambda b:e.w.recovery_current(b),private_storage_current=lambda:e.w.recovery_current(e.b))
    proof=ControllerRecoveryProof(physical,'actual-retained-native-reference',db,identity,e.plan.manifest,None,0,
        __import__('json').dumps({'schema_version':db.schema_version,'journal_head':None}).encode(),e.b)
    physical._proofs.add(proof)
    with pytest.raises(StaleWriter,match='verified target runtime'):
        await e.w.activate_recovered(e.j,proof,e.b)
    value=dict(hashes=dict(e.expected.hashes), database_generation='synthetic-db', schema_version=e.expected.schema_version, applied_seq=0, projection_seq=0, visibility_epoch=0, text_search_ready=True, dense_search_ready=True, archive_read_ready=None, proof_ready=None)
    assert e.q.accept_search_runtime(e.run,e.token,e.generation,value).ok
    e.q.heartbeat(e.run,e.token,ready=True,connection_generation=e.generation)
    assert not e.w.current(database_generation='synthetic-db',writer_generation='writer-2')
    # A pointer newer than the recovered base is never replaced (no rollback).
    newer=ObjectRef(e.j.prefix+'/snapshots/manifests/'+'e'*64,'e'*64,10,5)
    async def pointer():return newer,None,{}
    physical.current_pointer=pointer
    with pytest.raises(StaleWriter,match='older than the current snapshot pointer'):
        await e.w.activate_recovered(e.j,proof,e.b)
    # A superseded pointer (e.g. Dataset1 after Dataset2 promotion) is replaced
    # by its token alone; its old archive is not re-validated by the new transport.
    async def superseded():return replace(newer,last_sequence=0),None,{}
    async def unreadable():raise AssertionError('superseded base must not be re-validated')
    physical.current_pointer=superseded;physical.current_snapshot=unreadable
    await e.w.activate_recovered(e.j,proof,e.b)
    assert e.w.current(database_generation='synthetic-db',writer_generation='writer-2')


@pytest.mark.asyncio
async def test_recovery_publication_unknown_holds_queue_handover_and_reconciles_exact_native_bytes(recovery_owner):
    from hashlib import sha256
    e=recovery_owner;await e.w.begin_restore(e.j,e.b,plan=e.plan)
    key=e.j.prefix+'/snapshots/recovery-intents/'+sha256(e.plan.operation_key.encode()).hexdigest()
    e.old.c.release=threading.Event()
    writing=asyncio.create_task(e.s.compare_and_swap(key,b'exact-recovery-intent',expected_token=None,writer_generation='writer-2'))
    assert await asyncio.to_thread(e.old.c.entered.wait,2)
    for _ in range(2):writing.cancel();await asyncio.sleep(.01);assert not writing.done()
    with pytest.raises(sqlite3.IntegrityError,match='publication unresolved'):
        with e.q.connect() as db:db.execute('UPDATE control SET current_run=NULL WHERE id=1')
    e.old.c.release.set()
    with pytest.raises(asyncio.CancelledError):await writing
    with e.q.connect(write=False) as db:
        intent=json.loads(db.execute('SELECT publication_json FROM rkb_writer_control').fetchone()[0])
    assert intent['state']=='UNKNOWN'
    assert await e.w.reconcile(e.s)
    with e.q.connect(write=False) as db:assert db.execute('SELECT publication_json FROM rkb_writer_control').fetchone()[0] is None
    with pytest.raises(StaleWriter):
        await e.s.compare_and_swap(e.j.prefix+'/snapshots/recovery-intents/unrelated',b'other',expected_token=None,writer_generation='writer-2')
    # Attachment renewal changes only the transport generation; an old binding
    # cannot issue a new scoped ticket or publication through its coordinator.
    stale=PreReadyRecoveryBinding(e.run,e.b.runtime_incarnation,e.generation+1,'synthetic-db','writer-2')
    assert not e.w.recovery_current(stale)
