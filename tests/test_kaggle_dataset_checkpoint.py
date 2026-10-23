import json
from dataclasses import replace
from types import SimpleNamespace as NS

import pytest

from regional_knowledge.kaggle_dataset_base import DatasetFile
from regional_knowledge.kaggle_dataset_checkpoint import SourceDatasetCheckpoints
from regional_knowledge.kaggle_pg_snapshot_native import encode,sha
from test_dataset_recovery_store import prepared
from test_control_spool import env,people,receipt


@pytest.mark.asyncio
@pytest.mark.parametrize('advance_sequence',[True,False])
async def test_work_cycle_waits_for_quiet_then_resumes_published_version_after_lost_native_readback(tmp_path,monkeypatch,advance_sequence):
    p=prepared(tmp_path);store=p.store;clock=[0.];calls=[]
    base_sequence=8 if advance_sequence else 7
    if not advance_sequence:
        assert await store.put_if_absent(p.prefix+'outcomes/late',b'late outcome bytes')
    control=encode(dict(database_generation='db',sequence=base_sequence))
    archive=replace(p.checkpoint.archive,name='pg-'+64*'a'+'.blob')
    manifest=encode(dict(database_generation='db',applied_sequence=base_sequence,
        blob=dict(size=archive.size,sha256=archive.sha256),
        source_control=dict(key=p.prefix+'source-base-control/'+sha(control),sha256=sha(control))))
    journal=store.export_control()
    def pin(name,data):return DatasetFile(name,len(data),sha(data))
    checkpoint=replace(p.checkpoint,dataset_version=2,base_sequence=base_sequence,archive=archive,
        manifest=pin('base-manifest.json',manifest),control=pin('source-control.json',control),
        journal=pin('journal-control.json',journal))
    class Publisher:
        ref=p.publisher.ref
        def read_control(self,file,*,version):
            assert version==2
            return {'base-manifest.json':manifest,'source-control.json':control,'journal-control.json':journal}[file.name]
    store.publisher=Publisher()
    with p.q.connect() as db:
        db.execute('CREATE TABLE rkb_control_spool_meta(id INTEGER PRIMARY KEY,sequence INTEGER)')
        db.execute('INSERT INTO rkb_control_spool_meta VALUES(1,?)',(base_sequence,))
        db.execute('CREATE TABLE rkb_control_operations(operation_key TEXT,sequence INTEGER,seal_state TEXT,apply_state TEXT,outcome_state TEXT)')
    proof=NS(applied_sequence=base_sequence,accepted_sequence=base_sequence)
    versions=dict(schema_version=16,server_version_num=170010,pgvector_version='0.8.2',catalog_sha256='a'*64,
        model_sha256='b'*64,tokenizer_sha256='c'*64,schema_revision='schema11',
        function_revision='geo6',executor_revision='executor1')
    class Batch:
        def __init__(self,startup,config,publisher):self.config=config;self.tickets=NS()
        async def capture_once(self,binding):
            with p.q.connect() as db:
                if db.execute('SELECT 1 FROM rkb_dataset_seed WHERE operation_key=?',(self.config.operation_key,)).fetchone():return 2
                calls.append('capture_and_publish')
                package=dict(database_generation='db',base_sequence=base_sequence,manifest_sha256=checkpoint.manifest.sha256,
                    previous_version=1,files=[dict(name=f.name,size=f.size,sha256=f.sha256)
                        for f in (checkpoint.manifest,archive,checkpoint.control,checkpoint.journal)])
                db.execute('INSERT INTO rkb_dataset_seed VALUES(?,?,?,?,?)',
                    (self.config.operation_key,Publisher.ref,'published',json.dumps(package),2))
            return 2
    monkeypatch.setattr('regional_knowledge.kaggle_dataset_checkpoint._WorkCycleCapture',Batch)
    async def request(binding,phase,args,**kwargs):
        calls.append('native_verify')
        assert phase=='dataset_verify' and args['checkpoint']==checkpoint.wire()
        if calls.count('native_verify')==1:raise TimeoutError('readback lost; publication already happened')
        return dict(dataset_ref=Publisher.ref,dataset_version=2,base_sequence=base_sequence,
            archive_sha256=archive.sha256,archive_size=archive.size)
    startup=NS(config=NS(queue=p.q,spool=NS(journal=NS(store=store,prefix=p.prefix.rstrip('/'))),
        source_restore=NS(versions=versions),history_adapter=NS(database=NS(**versions))),
        history=NS(schema_version=16,_coverage=proof,catalog_hash=versions['catalog_sha256']),
        restorer=NS(dispatch=NS(request=request)),binding_current=lambda binding:True)
    owner=SourceDatasetCheckpoints(startup,clock=lambda:clock[0],quiet_seconds=30)
    assert not await owner.refresh('binding')
    clock[0]=29
    assert not await owner.refresh('binding') and not calls
    clock[0]=31
    from regional_knowledge.kaggle_dataset_checkpoint import CheckpointPending
    with pytest.raises(CheckpointPending) as pending:await owner.refresh('binding')
    assert type(pending.value.__cause__) is TimeoutError and pending.value.args[0]=='TimeoutError'
    assert store.latest_checkpoint.dataset_version==1
    # A restarted owner resumes that exact published package; it cannot publish
    # version 3 just because version 2's native readback response was lost.
    again=SourceDatasetCheckpoints(startup,clock=lambda:clock[0])
    assert await again.refresh('binding')
    assert store.latest_checkpoint==checkpoint
    assert calls==['capture_and_publish','native_verify','native_verify']
    assert not await again.refresh('binding')


@pytest.mark.asyncio
async def test_capture_materializes_crash_gap_outcome_before_export_and_keeps_it_pending(tmp_path,env):
    from regional_knowledge.kaggle_dataset_checkpoint import _WorkCycleCapture
    from regional_knowledge.recovery_journal import RecoveryJournal
    p=prepared(tmp_path);journal=RecoveryJournal(p.store,stream='stream')
    command=env.cmd
    seal=await journal.seal([command],predecessor=None,writer_generation='writer')
    native=receipt(command,database='db')
    with p.q.connect() as db:
        db.execute('CREATE TABLE rkb_control_operations(operation_key TEXT,sequence INTEGER,seal_state TEXT,'
            'apply_state TEXT,outcome_state TEXT,outcome_sha256 TEXT)')
        db.execute('INSERT INTO rkb_control_operations VALUES(?,?,?,?,?,?)',
            (command.operation_key,1,'ACCEPTED','APPLIED','NOT_SENT',native.outcome_sha256))
    async def get(key):
        assert key==command.operation_key
        return NS(command=command,accepted=seal,apply_receipt=native,database_generation='db')
    proof=NS(current=lambda:True,accepted_sequence=1,applied_sequence=1,pending=('not remotely saved',))
    batch=_WorkCycleCapture.__new__(_WorkCycleCapture);batch.queue=p.q
    batch.startup=NS(config=NS(writer_generation='writer',spool=NS(journal=journal,get=get,_source_covered_sequence=1)),
        restorer=NS(physical=NS(limits=NS(max_receipts=100))))
    key=journal.prefix+'/outcomes/'+sha(command.operation_key.encode())
    assert await p.store.read(key,max_bytes=8192) is None
    assert not batch._prefix_ready(proof)
    await batch._prepare_prefix(proof)
    stored=await p.store.read(key,max_bytes=8192)
    assert stored is not None and batch._prefix_ready(proof)
    assert not p.store.remote_durable(key,stored.data)
    assert json.loads(stored.data)['outcome_sha256']==native.outcome_sha256
    assert key in {o['key'] for o in json.loads(p.store.export_control())['objects']}
    with p.q.connect(write=False) as db:
        assert db.execute('SELECT outcome_state FROM rkb_control_operations').fetchone()[0]=='NOT_SENT'


def control_cycle(tmp_path,monkeypatch,*,interval=900,quiet=30):
    """Real capture, tickets, store and owner; only native/provider I/O is fake."""
    import asyncio
    from regional_knowledge.kaggle_dataset_checkpoint import _WorkCycleCapture
    from regional_knowledge.kaggle_dataset_publisher import DatasetUpload
    from regional_knowledge.recovery_journal import RecoveryJournal
    p=prepared(tmp_path);p.clock=[0.];p.alive=True;p.calls=[];p.uploads={};p.saved={}
    p.fail_finalize=False;p.fail_verify=False;p.bad_verify=False;p.on_verify=None
    p.q.clock=lambda:0.
    p.binding=NS(run_id='run',runtime_incarnation='inc',writer_generation='writer')
    with p.q.connect() as db:
        db.execute("CREATE TABLE runs(id TEXT,started REAL,status TEXT NOT NULL DEFAULT 'ready')")
        db.execute("INSERT INTO runs(id,started,status) VALUES('run',0,'ready')")
        db.execute('CREATE TABLE rkb_control_spool_meta(id INTEGER PRIMARY KEY,sequence INTEGER)')
        db.execute('INSERT INTO rkb_control_spool_meta VALUES(1,7)')
        db.execute('CREATE TABLE rkb_control_operations(operation_key TEXT,sequence INTEGER,seal_state TEXT,'
            'apply_state TEXT,outcome_state TEXT,outcome_sha256 TEXT)')
        db.execute('CREATE TABLE rkb_control_source_heads(source_id TEXT,sequence INTEGER,command_sha256 TEXT,'
            'restriction INTEGER,pending INTEGER,unknown INTEGER)')
    versions=dict(schema_version=16,server_version_num=170010,pgvector_version='0.8.2',catalog_sha256='a'*64,
        model_sha256='b'*64,tokenizer_sha256='c'*64,schema_revision='schema11',
        function_revision='geo6',executor_revision='executor1')
    p.proof=NS(applied_sequence=7,accepted_sequence=7,current=lambda:True,head=None,
        pending=(),database_generation='db')
    original=p.publisher
    class Publisher:
        ref=original.ref
        def reserve(self,*,name,size,sha256,now):
            return DatasetUpload(name,size,sha256,'token-'+sha256,'https://storage.googleapis.com/fixture')
        def observe(self):return max(p.saved,default=1)
        def finalize(self,*,files,operation,previous_version):
            p.calls.append('finalize')
            assert previous_version==self.observe()
            version=previous_version+1
            p.saved[version]=(operation,files,{f.name:p.uploads.get(f.name) for f in files})
            if p.fail_finalize:
                p.fail_finalize=False;raise TimeoutError('provider committed; response lost')
            return version
        def reconcile(self,*,files,operation,expected_version):
            p.calls.append('reconcile')
            saved=p.saved.get(expected_version)
            return bool(saved and (operation,files)==saved[:2])
        def read_control(self,file,*,version):
            if version==1:return original.read_control(file,version=version)
            data=p.saved[version][2][file.name]
            assert (len(data),sha(data))==(file.size,file.sha256)
            return data
    p.store.publisher=Publisher()
    async def capture(batch,physical,bridge,binding,sequence):
        p.calls.append('capture')
        ticket=await batch.tickets.issue_upload(binding,batch.config.operation_key+':archive',
            size=20,sha256=sha(b'fixture archive'))
        return dict(database_generation='db',writer_generation='writer',applied_sequence=sequence,
            versions={k:v for k,v in versions.items() if k in ('schema_version','server_version_num','pgvector_version','model_sha256','tokenizer_sha256')},blob=dict(key=ticket.key,size=ticket.size,sha256=ticket.sha256,native_version='fixture'))
    monkeypatch.setattr(_WorkCycleCapture,'_capture',capture)
    monkeypatch.setattr('regional_knowledge.kaggle_dataset_seed._put_control',
        lambda grant,data:p.uploads.__setitem__(grant.name,data))
    async def request(binding,phase,args,**kwargs):
        assert phase=='dataset_verify';p.calls.append('verify')
        if p.fail_verify:
            p.fail_verify=False;raise TimeoutError('native readback response lost')
        if p.on_verify is not None:await p.on_verify()
        cp=args['checkpoint'];archive=cp['archive']
        return dict(dataset_ref=Publisher.ref,dataset_version=cp['dataset_version'],
            base_sequence=cp['base_sequence'],archive_sha256='0'*64 if p.bad_verify else archive['sha256'],
            archive_size=archive['size'])
    spool=NS(journal=RecoveryJournal(p.store,stream='stream'),_lock=asyncio.Lock(),_source_covered_sequence=7)
    p.startup=NS(config=NS(queue=p.q,spool=spool,writer_owner=p.guard,writer_generation='writer',
        source_restore=NS(versions=versions),history_adapter=NS(database=NS(**versions))),
        history=NS(schema_version=16,_coverage=p.proof,catalog_hash='a'*64,acl_hash='d'*64,registry_hash='e'*64,routes=()),
        restorer=NS(complete=True,physical=NS(journal=spool.journal,
            limits=NS(max_receipts=100,max_control_bytes=1024*1024)),dispatch=NS(request=request)),
        binding_current=lambda binding:p.alive and binding is p.binding)
    p.owner=SourceDatasetCheckpoints(p.startup,clock=lambda:p.clock[0],interval_seconds=interval,quiet_seconds=quiet)
    return p


async def put_control(p,name='one',data=b'pending'):
    key=p.prefix+'control/expert-requests/'+name+'/result'
    old=await p.store.read(key,max_bytes=1024)
    assert await p.store.compare_and_swap(key,data,expected_token=old.token if old else None,writer_generation='writer')
    return key


@pytest.mark.asyncio
async def test_control_bytes_at_unchanged_pg_cursor_are_grouped_not_per_write_or_tick(tmp_path,monkeypatch):
    p=control_cycle(tmp_path,monkeypatch)
    assert not await p.owner.refresh(p.binding,planned_stop=True) and not p.calls
    p.clock[0]=1000
    keys=[await put_control(p,str(i)) for i in range(3)]
    assert not await p.owner.refresh(p.binding) and not p.calls
    p.clock[0]=1029
    assert not await p.owner.refresh(p.binding) and not p.calls
    p.clock[0]=1031
    assert await p.owner.refresh(p.binding)
    assert p.store.latest_checkpoint.dataset_version==2
    assert p.store.latest_checkpoint.base_sequence==7
    assert all(p.store.remote_durable(key,b'pending') for key in keys)
    assert p.store.pending_control() is None and p.calls==['capture','finalize','verify']
    assert not await p.owner.refresh(p.binding,planned_stop=True)
    assert p.calls==['capture','finalize','verify']


@pytest.mark.asyncio
@pytest.mark.parametrize('lost',['finalize','verify'])
async def test_control_only_lost_ack_resumes_same_operation_version_after_restart(tmp_path,monkeypatch,lost):
    from regional_knowledge.kaggle_dataset_checkpoint import CheckpointPending
    p=control_cycle(tmp_path,monkeypatch);key=await put_control(p)
    setattr(p,'fail_'+lost,True)
    await p.owner.refresh(p.binding);p.clock[0]=31
    with pytest.raises(CheckpointPending):await p.owner.refresh(p.binding)
    assert not p.store.remote_durable(key,b'pending') and p.store.latest_checkpoint.dataset_version==1
    p.owner=SourceDatasetCheckpoints(p.startup,clock=lambda:p.clock[0])
    assert await p.owner.refresh(p.binding)
    assert p.store.remote_durable(key,b'pending') and p.store.latest_checkpoint.dataset_version==2
    assert p.calls.count('capture')==p.calls.count('finalize')==1
    assert not await p.owner.refresh(p.binding) and len(p.saved)==1


@pytest.mark.asyncio
@pytest.mark.parametrize('late_data',[b'new bytes',b'pending'])
async def test_late_control_cas_after_capture_remains_pending_for_next_group(tmp_path,monkeypatch,late_data):
    p=control_cycle(tmp_path,monkeypatch);key=await put_control(p)
    other=await put_control(p,'other')
    async def late():
        p.on_verify=None
        await put_control(p,data=late_data)
    p.on_verify=late
    await p.owner.refresh(p.binding);p.clock[0]=31
    assert await p.owner.refresh(p.binding)
    assert p.store.latest_checkpoint.dataset_version==2
    assert not p.store.remote_durable(key,late_data) and p.store.remote_durable(other,b'pending')
    assert p.store.pending_control() is not None
    p.clock[0]=32
    assert not await p.owner.refresh(p.binding) and p.calls.count('capture')==1
    p.clock[0]=63
    assert await p.owner.refresh(p.binding)
    assert p.store.latest_checkpoint.dataset_version==3 and p.store.remote_durable(key,late_data)
    assert p.calls.count('capture')==2


@pytest.mark.asyncio
async def test_new_control_write_restarts_quiet_window_but_periodic_bound_still_applies(tmp_path,monkeypatch):
    p=control_cycle(tmp_path,monkeypatch,interval=60);await put_control(p)
    await p.owner.refresh(p.binding)
    for tick in (20,40):
        p.clock[0]=tick;await put_control(p,data=str(tick).encode())
        assert not await p.owner.refresh(p.binding) and not p.calls
    p.clock[0]=61
    assert await p.owner.refresh(p.binding)
    assert p.store.latest_checkpoint.dataset_version==2 and p.calls.count('capture')==1


@pytest.mark.asyncio
async def test_control_native_failure_preserves_current_proof_and_retry_uses_same_package(tmp_path,monkeypatch):
    from regional_knowledge.kaggle_dataset_checkpoint import CheckpointPending
    p=control_cycle(tmp_path,monkeypatch);key=await put_control(p);p.bad_verify=True
    await p.owner.refresh(p.binding);p.clock[0]=31
    with pytest.raises(CheckpointPending):await p.owner.refresh(p.binding)
    assert p.startup.history._coverage is p.proof and p.proof.current()
    assert not p.store.remote_durable(key,b'pending') and p.store.latest_checkpoint.dataset_version==1
    p.bad_verify=False
    assert await p.owner.refresh(p.binding)
    assert p.calls.count('capture')==1 and p.store.remote_durable(key,b'pending')


@pytest.mark.asyncio
async def test_retired_writer_cannot_capture_or_promote_and_overlapping_tick_does_not_duplicate(tmp_path,monkeypatch):
    import asyncio
    from regional_knowledge.kaggle_dataset_checkpoint import CheckpointPending
    p=control_cycle(tmp_path,monkeypatch);key=await put_control(p)
    await p.owner.refresh(p.binding);p.clock[0]=31;p.alive=False
    with pytest.raises(CheckpointPending):await p.owner.refresh(p.binding)
    assert not p.calls
    p.alive=True;entered=asyncio.Event();release=asyncio.Event()
    async def retire():
        entered.set();await release.wait();p.alive=False
    p.on_verify=retire
    first=asyncio.create_task(p.owner.refresh(p.binding));await entered.wait()
    assert not await p.owner.refresh(p.binding,planned_stop=True)
    release.set()
    with pytest.raises(CheckpointPending):await first
    assert p.store.latest_checkpoint.dataset_version==1 and not p.store.remote_durable(key,b'pending')
    p.alive=True;p.on_verify=None
    assert await p.owner.refresh(p.binding)
    assert p.store.latest_checkpoint.dataset_version==2 and p.calls.count('capture')==1


@pytest.mark.asyncio
async def test_session_checkpoint_waits_15_minutes_of_quiet_not_15_seconds(tmp_path,monkeypatch):
    p=control_cycle(tmp_path,monkeypatch,interval=1800,quiet=900)
    await put_control(p)
    assert not await p.owner.refresh(p.binding)
    p.clock[0]=899
    assert not await p.owner.refresh(p.binding) and not p.calls
    p.clock[0]=900
    assert await p.owner.refresh(p.binding)
    assert p.calls==['capture','finalize','verify']
    assert p.owner.saved_seq==7


@pytest.mark.asyncio
async def test_continuous_writes_request_checkpoint_at_30_minutes_without_faked_rpo(tmp_path,monkeypatch):
    p=control_cycle(tmp_path,monkeypatch,interval=1800,quiet=900)
    await put_control(p)
    assert not await p.owner.refresh(p.binding)
    for second in (300,600,900,1200,1500,1799):
        p.clock[0]=second
        await put_control(p,data=('at-'+str(second)).encode())
        assert not await p.owner.refresh(p.binding) and not p.calls
    p.clock[0]=1800
    await put_control(p,data=b'continuous')
    assert await p.owner.refresh(p.binding)
    assert p.owner.saved_seq==7 and p.calls.count('capture')==1
    # Timer is an attempt cadence, not a guarantee of durable provider bytes.
    assert p.store.pending_control() is None


@pytest.mark.asyncio
async def test_cooperative_stop_forces_checkpoint_without_waiting_for_quiet(tmp_path,monkeypatch):
    p=control_cycle(tmp_path,monkeypatch,interval=1800,quiet=900)
    key=await put_control(p)
    assert await p.owner.before_stop(p.binding)
    assert p.owner.saved_seq==7
    assert p.store.remote_durable(key,b'pending')
    assert p.calls==['capture','finalize','verify']


@pytest.mark.asyncio
async def test_background_capture_never_holds_intake_lock_and_is_single_flight(tmp_path,monkeypatch):
    import asyncio
    from regional_knowledge.kaggle_dataset_checkpoint import _WorkCycleCapture

    p=control_cycle(tmp_path,monkeypatch)
    await put_control(p)
    assert not await p.owner.refresh(p.binding)
    p.clock[0]=31
    captured=asyncio.Event();resume=asyncio.Event()
    previous=_WorkCycleCapture._capture

    async def slow_capture(self,*args):
        captured.set()
        await resume.wait()
        return await previous(self,*args)

    monkeypatch.setattr(_WorkCycleCapture,'_capture',slow_capture)
    assert p.owner.kick(p.binding)
    await asyncio.wait_for(captured.wait(),timeout=2)
    # Native APPLIED source stages use this same lock. A Dataset publication
    # must not own it for an unbounded provider-readiness interval.
    assert not p.startup.config.spool._lock.locked()
    await asyncio.wait_for(p.startup.config.spool._lock.acquire(),timeout=.3)
    p.startup.config.spool._lock.release()
    assert not p.owner.kick(p.binding)
    resume.set()
    await asyncio.wait_for(p.owner._task,timeout=2)
    assert p.calls==['capture','finalize','verify']
    assert p.owner.saved_seq==7
    await p.owner.aclose()


@pytest.mark.asyncio
async def test_failed_dataset_verify_does_not_promote_saved_seq_or_discard_tail(tmp_path,monkeypatch):
    from regional_knowledge.kaggle_dataset_checkpoint import CheckpointPending
    p=control_cycle(tmp_path,monkeypatch,interval=1800,quiet=900)
    key=await put_control(p)
    p.fail_verify=True
    with pytest.raises(CheckpointPending):
        await p.owner.refresh(p.binding,planned_stop=True)
    assert p.owner.saved_seq==7 and p.store.latest_checkpoint.dataset_version==1
    assert not p.store.remote_durable(key,b'pending')
    # The ordinary controller restart path resumes the very same version.
    p.owner=SourceDatasetCheckpoints(p.startup,clock=lambda:p.clock[0])
    assert await p.owner.refresh(p.binding)
    assert p.store.remote_durable(key,b'pending')
    assert p.owner.saved_seq==7
    assert p.calls.count('capture')==1


@pytest.mark.asyncio
async def test_existing_planned_idle_drain_forces_checkpoint_before_quiet_timeout(tmp_path,monkeypatch):
    p=control_cycle(tmp_path,monkeypatch,interval=1800,quiet=900)
    key=await put_control(p)
    assert not await p.owner.refresh(p.binding)
    p.clock[0]=11
    with p.q.connect() as db:
        db.execute("UPDATE runs SET status='draining' WHERE id='run'")
    assert await p.owner.refresh(p.binding)
    assert p.owner.saved_seq==7
    assert p.store.remote_durable(key,b'pending')
    assert p.calls==['capture','finalize','verify']
