"""Actual durable admission/channel with synthetic native phases; no PG/model/S3."""
import asyncio
from dataclasses import replace
from types import SimpleNamespace

import pytest

from regional_knowledge.kaggle_recovery_dispatch import RecoveryPhaseDispatch,WssSnapshotPhases
from regional_knowledge.kaggle_recovery_phases import WorkerRecoveryPhases
from regional_knowledge.kaggle_pg_snapshot_native import NativeSnapshotFactory,SnapshotError,encode,sha
from regional_knowledge.kaggle_bootstrap import PreparedRuntime
from regional_knowledge.kaggle_snapshot_channel import SnapshotTicketChannel
from test_bge_admission import state,auth,people
from test_bge_shared_channel import peer
from test_bge_shared_worker import Peer,result
from test_bge_wss_worker import CONFIG,module
from test_kaggle_bootstrap import candidate
from test_kaggle_target_runtime import spec
from test_kaggle_native_bundle import fixture


def database():return replace(spec().database,mode='VERIFIED_INITIALIZED',ownership_evidence='synthetic-private-restore')


@pytest.mark.asyncio
@pytest.mark.parametrize('invalid',[None,'archive','control','missing_ticket'])
async def test_native_dataset_prepare_uses_pinned_download_and_same_pg_factory(invalid):
    from regional_knowledge.kaggle_dataset_base import DatasetCheckpoint,DatasetFile
    from regional_knowledge.native_snapshot_transfer import SnapshotTicket
    db=database();body=b'archive';digest=sha(body)
    blob=dict(key='dataset-blobs/pin/pg-fixture.blob',size=len(body),sha256=digest,native_version='dataset-1')
    manifest=encode(dict(blob=blob,applied_sequence=7,database_generation=db.database_generation))
    control=encode(dict(fixture='source-control'))
    part=lambda name,data:DatasetFile(name,len(data),sha(data))
    pin=DatasetCheckpoint('fixture-owner','rkb-checkpoints',1,db.database_generation,7,
        part('base-manifest.json',manifest),part('pg-fixture.blob',body),part('source-control.json',control),
        part('journal-control.json',b'journal'))
    if invalid in ('archive','control'):
        pin=replace(pin,**{invalid:replace(getattr(pin,invalid),sha256='f'*64)})
    native=object.__new__(NativeSnapshotFactory)
    native.database=db;native.identity=('r','nonce','inc');native.dataset_checkpoint=pin
    calls=[];candidate=object()
    async def prepare(binding,**kwargs):
        calls.append(('prepare',kwargs));return candidate
    async def download(ref,operation,**kwargs):
        assert ref.key==blob['key'] and kwargs['ticket']['sha256']==digest
        calls.append(('download',operation));return '/private/exact-version1-archive'
    native.prepare=prepare
    worker=WorkerRecoveryPhases(factory=native,download=download,workspace=lambda op,phase:'/private/'+phase)
    key='restore';ticket=SnapshotTicket('GET',sha(key.encode()),blob['key'],len(body),digest,10000,
        'https://storage.googleapis.com/version1',(('If-Match','v1'),))
    frame=dict(operation_key=key,download_ticket=ticket.wire())
    if invalid=='missing_ticket':frame.pop('download_ticket')
    args=dict(database=db.wire(),identity=list(native.identity),manifest_b64=__import__('base64').b64encode(manifest).decode(),
        control_b64=__import__('base64').b64encode(control).decode(),manifest_ref=dict(sha256=sha(manifest)),
        blob=blob,restore_operation='original-restore',expected_versions={})
    if invalid:
        with pytest.raises(SnapshotError,match='dataset_restore_'):await worker._execute('prepare',args,key,('r','inc',1),frame)
        assert not calls;return
    result=await worker._execute('prepare',args,key,('r','inc',1),frame)
    assert worker._sessions[result['handle']] is candidate
    assert [c[0] for c in calls]==['download','prepare']
    assert calls[1][1]['archive_path']=='/private/exact-version1-archive'
    assert calls[1][1]['manifest_bytes']==manifest and calls[1][1]['control_bytes']==control


@pytest.mark.asyncio
async def test_dataset_readback_uses_actual_deferred_workspace_resolver(tmp_path,monkeypatch):
    from regional_knowledge.kaggle_dataset_base import DatasetCheckpoint,DatasetFile
    from regional_knowledge.kaggle_recovery_composition import DeferredRecoveryRuntime
    from regional_knowledge.native_snapshot_transfer import NativeSnapshotTransfer,SnapshotTicket
    from test_kaggle_generated_restore import recovery
    db=database();key='checkpoint-2-readback';body=b'archive';digest=sha(body)
    part=lambda name:DatasetFile(name,7,'a'*64)
    base=DatasetCheckpoint('fixture-owner','rkb-checkpoints',1,db.database_generation,7,
        part('base-manifest.json'),DatasetFile('pg-fixture.blob',len(body),digest),
        part('source-control.json'),part('journal-control.json'))
    next_pin=replace(base,dataset_version=2,base_sequence=20)
    owner=DeferredRecoveryRuntime(dict(target_runtime=dict(recovery=recovery().wire(),
        database=dict(mode='VERIFIED_INITIALIZED'))),executed_source='/fixture')
    owner.workspace_root=tmp_path
    native=object.__new__(NativeSnapshotFactory)
    native.database=db;native.identity=('r','nonce','inc');native._current=lambda binding:None
    native.dataset_checkpoint=base
    native.dataset_transfer=NativeSnapshotTransfer(allowed_origins=frozenset({'https://storage.fixture.invalid'}),
        max_bytes=1024,clock=lambda:1)
    downloaded=[]
    async def download(transfer,ticket,path,*,operation_key):
        transfer._check(ticket,'GET',operation_key)
        assert path.parent==tmp_path and path.name==sha(key.encode())+'-download'
        path.mkdir();result=path/'archive.blob';result.write_bytes(body);downloaded.append(result)
        return result
    monkeypatch.setattr(NativeSnapshotTransfer,'download',download)
    worker=WorkerRecoveryPhases(factory=native,download=lambda *_:None,workspace=owner._workspace)
    blob=dict(key='datasets/2/pg-fixture.blob',size=len(body),sha256=digest,native_version='fixture')
    ticket=SnapshotTicket('GET',sha(key.encode()),blob['key'],len(body),digest,100,
        'https://storage.fixture.invalid/archive',(('If-Match','fixture'),))
    result=await worker._execute('dataset_verify',dict(checkpoint=next_pin.wire(),blob=blob),key,
        ('r','inc',1),dict(download_ticket=ticket.wire()))
    assert result['dataset_version']==2 and result['base_sequence']==20
    assert len(downloaded)==1 and not downloaded[0].exists()
    assert downloaded[0].parent in owner._paths


@pytest.mark.asyncio
async def test_failed_prepare_releases_initialization_waiter_without_readiness():
    native=object.__new__(NativeSnapshotFactory)
    native.database=database();native.identity=('r','nonce','inc')
    native._current=lambda binding:None
    worker=WorkerRecoveryPhases(factory=native,download=lambda *_:None,workspace=lambda *_:None)
    failure=SnapshotError('synthetic_dataset_mount_failure')
    async def execute(*args):raise failure
    worker._execute=execute
    phase='prepare';args={}
    frame=dict(type='DISPATCH',protocol_version=2,connection_generation=1,id='job',claim='claim',
        kind='recovery_phase',run_id='r',launch_nonce='nonce',runtime_incarnation='inc',
        database_generation=native.database.database_generation,writer_generation=native.database.writer_generation,
        phase=phase,operation_key='restore',arguments=args,request_sha256=sha(encode(dict(phase=phase,arguments=args))),
        compute_claim='slot',dispatch_generation=1)
    waiter=asyncio.create_task(worker.wait_initialized(native.database,*native.identity))
    with pytest.raises(SnapshotError) as phase_error:await worker(frame)
    with pytest.raises(SnapshotError) as load_error:await asyncio.wait_for(waiter,1)
    assert phase_error.value is failure and load_error.value is failure
    assert worker._initialized is None and not worker._results


@pytest.mark.asyncio
async def test_capture_refusal_preserves_native_owner_for_following_readback():
    native=object.__new__(NativeSnapshotFactory)
    native.database=database();native.identity=('r','nonce','inc')
    native._current=lambda binding:None
    closed=[]
    class Candidate:
        async def aclose(self):closed.append(True)
    candidate=Candidate();native._owned_candidates={candidate};native._capabilities=set()
    async def capture(*args,**kwargs):raise ConnectionError('provider upload unavailable')
    async def verify(*args):return {'resident_pg':'unchanged'}
    native.capture=capture;native.verify_reference=verify
    worker=WorkerRecoveryPhases(factory=native,download=lambda *_:None,workspace=lambda *_:None)
    def frame(phase,args,key):
        return dict(type='DISPATCH',protocol_version=2,connection_generation=1,id=key,claim='claim',
            kind='recovery_phase',run_id='r',launch_nonce='nonce',runtime_incarnation='inc',
            database_generation=native.database.database_generation,writer_generation=native.database.writer_generation,
            phase=phase,operation_key=key,arguments=args,request_sha256=sha(encode(dict(phase=phase,arguments=args))),
            compute_claim='slot',dispatch_generation=1)
    failed=frame('capture',dict(versions={},expected_sequence=7),'capture')
    result=await worker(failed)
    assert result['value']==dict(status='rejected',reason='snapshot_capture_rejected')
    assert await worker(failed)==result and not closed
    assert (await worker(frame('verify',{'reference':'retained'},'readback')))['value']=={'resident_pg':'unchanged'}
    assert native._owned_candidates=={candidate} and not closed


@pytest.mark.asyncio
async def test_exact_dataset_prepare_uses_versioned_download_without_mount_attestation(state):
    from regional_knowledge.kaggle_dataset_base import DatasetCheckpoint,DatasetFile
    from regional_knowledge.native_snapshot_transfer import SnapshotTicket
    s=state
    part=lambda name:DatasetFile(name,10,'a'*64)
    checkpoint=DatasetCheckpoint('fixture-owner','rkb-pg-checkpoint',3,database().database_generation,7,
        part('base-manifest.json'),part('pg.blob'),part('source-control.json'),part('journal-control.json'))
    blob=dict(key='dataset-blobs/pin/pg.blob',size=10,sha256='a'*64,native_version='dataset-3')
    class Tickets:
        async def issue_download(self,binding,key,ref):
            assert binding is s and ref.key==blob['key']
            return SnapshotTicket('GET',sha(key.encode()),ref.key,ref.size,ref.sha256,10000,
                'https://storage.googleapis.com/exact-version-3',(('If-Match','version3'),))
    bridge=RecoveryPhaseDispatch(s.q,database=database(),identity=(s.run,s.nonce,'incarnation-a'),
        binding_current=lambda b:b is s,dataset_checkpoint=checkpoint,snapshot_tickets=Tickets())
    task=None
    try:
        task,work,frame=await work_for(s,bridge,phase='prepare',args={'blob':blob})
        assert 'download_ticket' in frame
        assert 'dataset_base' not in frame and 'dataset_observation' not in frame
        assert frame['download_ticket']['sha256']==checkpoint.archive.sha256
        await bridge.receive(response(s,frame,{'handle':'native-dataset'}),run_id=s.run,token=s.token,generation=s.generation)
        assert await task=={'handle':'native-dataset'}
    finally:
        if task is not None and not task.done():task.cancel()
        await bridge.aclose()


@pytest.mark.asyncio
async def test_concrete_factory_binds_tools_to_retained_stage0_manifest(fixture,monkeypatch):
    from regional_knowledge.kaggle_native_bundle import prepare_native_bundle
    from regional_knowledge.kaggle_recovery_phases import build_recovery_worker
    import regional_knowledge.kaggle_owned_pg as owned
    import regional_knowledge.kaggle_pg_snapshot_native as native
    names=('pg_basebackup','pg_verifybackup','postgres')
    entries=[('usr/bin/'+n,'file',('synthetic-'+n).encode()) for n in names]+[('usr/lib/x86_64-linux-gnu/libfixture.so','file',b'synthetic-ELF')]
    def executable(manifest):
        for file in manifest['files']:file['executable']=True
    root,pin,inventory,_=fixture(entries=entries,mutate=executable)
    bundle=prepare_native_bundle(root,pin.manifest_name,pin.manifest_sha256,inventory=inventory)
    verified=[]
    monkeypatch.setattr(owned,'grant_native_code_access',lambda root,pins,**identity:verified.append((pins,identity)))
    async def verify(tools,run):verified.append(tools)
    monkeypatch.setattr(native.PhysicalPgTools,'verify',verify)
    tools=dict(binaries={n:dict(path='usr/bin/'+n,version_output=n+' (PostgreSQL) 17.6') for n in names},
        command_handlers=[dict(route='source.withdraw.v1',check_sha256='a'*64,apply_sha256='b'*64)])
    args=dict(native_bundle=bundle,database=database(),identity=('r','n','i'),tools=tools,native_uid=1001,native_gid=1002,
        current=lambda b:True,workspace_private_current=lambda p:True,upload=lambda *a,**k:None,
        download=lambda *a:None,workspace=lambda *a:None)
    worker=await build_recovery_worker(**args)
    assert type(worker) is WorkerRecoveryPhases and worker.factory.native_gid==1002
    assert worker.factory.tools.postgres.sha256==sha(b'synthetic-postgres')
    assert worker.factory.native_env['LD_LIBRARY_PATH']==str(bundle.prefix/'usr/lib/x86_64-linux-gnu')
    assert worker.factory.tools.postgres.path==str(bundle.prefix/'usr/bin/postgres')
    assert len(verified)==2
    changed=dict(tools,binaries=tools['binaries']|{'postgres':dict(path='outside/postgres',version_output='arbitrary')})
    with pytest.raises(SnapshotError,match='unverified'):await build_recovery_worker(**(args|{'tools':changed}))
    with pytest.raises(SnapshotError,match='stage0_owner'):await build_recovery_worker(**(args|{'native_bundle':{'pins':'json'}}))
    closed=[]
    class PartialCandidate:
        async def aclose(self):closed.append('partial-native-candidate')
    partial=PartialCandidate();worker.factory._owned_candidates.add(partial)
    await worker.aclose()
    assert closed==['partial-native-candidate'] and not worker.factory._owned_candidates


async def work_for(s,bridge,phase='verify',args=None,key='same-operation'):
    task=asyncio.create_task(bridge.request(s,phase,args or {'reference':'retained-ref'},operation_key=key))
    for _ in range(100):
        work=await s.a.next_channel_work(s.run,s.token,s.generation)
        if work is not None:break
        await asyncio.sleep(.001)
    else:raise AssertionError('maintenance work absent')
    s.a.mark_work_sent(work)
    frame=dict(type='DISPATCH',protocol_version=2,connection_generation=s.generation,
        **dict(work.document_payload),compute_claim=work.compute_claim,dispatch_generation=work.dispatch_generation)
    return task,work,frame


def response(s,frame,value):
    return dict(type='RECOVERY_PHASE_RESULT',protocol_version=2,connection_generation=s.generation,
        job_id=frame['id'],claim=frame['claim'],receipt=dict(phase=frame['phase'],request_sha256=frame['request_sha256'],value=value),
        **{k:frame[k] for k in ('run_id','launch_nonce','runtime_incarnation','compute_claim','dispatch_generation')})


@pytest.mark.asyncio
async def test_maintenance_before_ready_uses_actual_slot_and_durable_claim(state):
    s=state
    with s.q.connect() as db:db.execute("update runs set status='starting' where id=?",(s.run,))
    bridge=RecoveryPhaseDispatch(s.q,database=database(),identity=(s.run,s.nonce,'incarnation-a'),binding_current=lambda b:b is s)
    try:
        assert await s.a.next_channel_work(s.run,s.token,s.generation) is None  # ordinary doc stays pending
        task,work,frame=await work_for(s,bridge)
        assert frame['kind']=='recovery_phase' and work.resource.resource_kind=='document'
        assert 'texts' not in frame and frame['writer_generation']==database().writer_generation
        assert await s.a.next_channel_work(s.run,s.token,s.generation) is None
        wrong=response(s,frame,{'native':'readback'});wrong['compute_claim']='forged'
        with pytest.raises(PermissionError):await bridge.receive(wrong,run_id=s.run,token=s.token,generation=s.generation)
        receipt=response(s,frame,{'native':'readback'})
        settled=await bridge.receive(receipt,run_id=s.run,token=s.token,generation=s.generation)
        assert settled['status']=='accepted' and await task=={'native':'readback'}
        assert (await bridge.receive(receipt,run_id=s.run,token=s.token,generation=s.generation))['status']=='duplicate'
        assert await s.a.next_channel_work(s.run,s.token,s.generation) is None  # result != actual compute release
        r=work.resource
        await s.a.resource_released(s.run,s.token,s.generation,runtime_incarnation=r.runtime_incarnation,
            resource_kind='document',resource_id=r.resource_id,compute_claim=r.compute_claim,dispatch_generation=r.dispatch_generation)
        assert await s.a.next_channel_work(s.run,s.token,s.generation) is None
        assert s.q.result(s.auth.p.owner.subject,s.doc)['state']=='pending'
        with pytest.raises(ValueError):s.q.enqueue('owner','forged',[],kind='recovery_phase')
    finally:await bridge.aclose()


@pytest.mark.asyncio
async def test_worker_caches_exact_known_result_but_retains_unknown_before_effects():
    f=object.__new__(NativeSnapshotFactory);f.database=database();f.identity=('r','n','i');f._current=lambda b:None
    calls=[]
    async def verify(binding,ref):calls.append(ref);return {'observed':'native'}
    f.verify_reference=verify
    worker=WorkerRecoveryPhases(factory=f,download=lambda *_:None,workspace=lambda *_:None)
    args={'reference':'known'}
    frame=dict(type='DISPATCH',protocol_version=2,connection_generation=1,id='job',claim='claim',kind='recovery_phase',
        run_id='r',launch_nonce='n',runtime_incarnation='i',database_generation=f.database.database_generation,
        writer_generation=f.database.writer_generation,phase='verify',operation_key='op',arguments=args,
        request_sha256=sha(encode({'phase':'verify','arguments':args})),compute_claim='slot',dispatch_generation=1)
    first=await worker(frame)
    assert await worker(frame)==first and calls==['known']
    with pytest.raises(SnapshotError):await worker(frame|{'arguments':{'reference':'changed'}})
    with pytest.raises(SnapshotError):await worker(frame|{'sql':'select 1'})
    async def lost(binding,ref):calls.append('effect');raise ConnectionError('lost native response')
    f.verify_reference=lost;frame=frame|{'operation_key':'unknown'}
    with pytest.raises(ConnectionError):await worker(frame)
    with pytest.raises(SnapshotError,match='unknown_requires_reconciliation'):await worker(frame)
    assert calls==['known','effect']




@pytest.mark.asyncio
async def test_exact_run_read_handler_status_uses_retained_slot_without_pg_or_bge_effects(tmp_path):
    """Worker readback is a fresh WSS maintenance phase, never code-upload RPC."""
    from regional_knowledge.kaggle_warm_read import ReadHandlerSlot
    from test_kaggle_warm_read import old,context,staged_module,good_probe,good_stage
    native=object.__new__(NativeSnapshotFactory)
    native.database=database();native.identity=('r','nonce','inc')
    native._owned_candidates=set();native._capabilities=set()
    observed=[]
    native._current=lambda binding:observed.append(binding)
    worker=WorkerRecoveryPhases(factory=native,download=lambda *_:pytest.fail('download'),
        workspace=lambda *_:pytest.fail('workspace'))
    async def request(key,arguments=None):
        args=arguments if arguments is not None else {}
        frame=dict(type='DISPATCH',protocol_version=2,connection_generation=2,id='job-'+key,
            claim='claim',kind='recovery_phase',run_id='r',launch_nonce='nonce',
            runtime_incarnation='inc',database_generation=native.database.database_generation,
            writer_generation=native.database.writer_generation,phase='read_handler_status',
            operation_key=key,arguments=args,request_sha256=sha(encode({
                'phase':'read_handler_status','arguments':args})),
            compute_claim='retained-slot',dispatch_generation=2)
        return await worker(frame)
    with pytest.raises(SnapshotError,match='read_handler_status_unavailable'):
        await request('not-prepared')
    slot=ReadHandlerSlot(old,loaded_sha256='a'*64,compatibility=context())
    worker.read_handler_slot=slot
    inspected=[]
    async def live_probe(binding):
        inspected.append(binding)
        return dict(status='ready',pg_system_identifier='7420331119',
            resident_model_load_count=1,process_model_load_count=1)
    worker.read_handler_runtime_observer=live_probe
    result=await request('fresh-before')
    assert {k:v for k,v in result['value'].items() if k!='owned_runtime'}==slot.receipt()
    assert result['value']['loaded_sha256']=='a'*64
    assert result['value']['loaded_kind']=='verified_bundle'
    assert result['value']['generation']==0
    assert result['value']['owned_runtime']['pg_system_identifier']=='7420331119'
    assert inspected==[('r','inc',2)]
    with pytest.raises(SnapshotError,match='read_handler_status_arguments'):
        await request('forged-param',{'module_path':'/private/code.py'})
    root,path,digest=staged_module(tmp_path)
    await slot.stage(path=path,trusted_root=root,expected_sha256=digest,
        compatibility=context(),constructor={'marker':'new'},self_test=good_stage)
    await slot.activate(staged_sha256=digest,expected_loaded_sha256='a'*64,
        probe=good_probe)
    changed=await request('fresh-after')
    assert changed['value']['loaded_sha256']==digest
    assert changed['value']['loaded_kind']=='module'
    assert changed['value']['generation']==1
    assert changed['value']['owned_runtime']['resident_model_load_count']==1
    assert len(inspected)==2 and len(observed)==6  # before + after live PG check
    # Failed PG readback is an unavailable status, not fabricated "0 loads"
    # or a fatal worker exception closing the warmed encoder.
    async def denied(_):raise PermissionError('private SQL error')
    worker.read_handler_runtime_observer=denied
    missing=await request('fresh-pg-denied')
    assert missing['value']['owned_runtime']==dict(
        status='unavailable',reason='runtime_readback_unavailable')
    assert missing['value']['loaded_sha256']==digest
    # This source-only operation touched neither PG nor model/asset owner.
    await worker.aclose()




@pytest.mark.asyncio
async def test_reconnect_during_pg_readback_yields_stale_receipt_without_killing_wss_runtime():
    """Post-I/O generation change may not escape _encode/_failure as fatal."""
    from regional_knowledge.kaggle_warm_read import ReadHandlerSlot
    from test_kaggle_warm_read import old,context
    from test_bge_wss_worker import module,CONFIG
    native=object.__new__(NativeSnapshotFactory)
    native.database=database();native.identity=('r','n','i')
    active=[True]
    def current(binding):
        if not active[0]:raise SnapshotError('native_restore_binding_changed')
    native._current=current
    native._owned_candidates=set();native._capabilities=set()
    worker=WorkerRecoveryPhases(factory=native,download=lambda *_:None,
                                workspace=lambda *_:None)
    slot=ReadHandlerSlot(old,loaded_sha256='a'*64,compatibility=context())
    worker.read_handler_slot=slot
    async def changed_during_await(binding):
        assert active[0]
        await asyncio.sleep(0)
        active[0]=False  # Simulate old connection generation after PG I/O
        return {'status':'ready','pg_system_identifier':'previous-PG'}
    worker.read_handler_runtime_observer=changed_during_await
    phase='read_handler_status';arguments={}
    frame=dict(type='DISPATCH',protocol_version=2,connection_generation=1,
        id='job-stale',claim='claim',kind='recovery_phase',run_id='r',
        launch_nonce='n',runtime_incarnation='i',
        database_generation=native.database.database_generation,
        writer_generation=native.database.writer_generation,
        phase=phase,operation_key='readback-while-detached',arguments=arguments,
        request_sha256=sha(encode(dict(phase=phase,arguments=arguments))),
        compute_claim='slot',dispatch_generation=1)
    transport=module('bge_wss_transport').BgeWssTransport(
        dict(CONFIG,shared_reads=True),lambda:lambda text:[1.]+[0.]*1023,
        lambda:{},query_handler=old,recovery_handler=worker)
    task=asyncio.create_task(transport._encode(frame))
    await task
    transport._failure(task)  # Former code raised and triggered close_runtime.
    assert transport.pending['receipt']['value']==dict(
        status='unavailable',reason='runtime_readback_stale')
    assert 'previous-PG' not in repr(transport.pending)
    assert slot.receipt()['loaded_sha256']=='a'*64
    assert slot.receipt()['generation']==0
    await worker.aclose()


@pytest.mark.asyncio
async def test_readback_dispatch_uses_real_durable_owner_and_new_operation_per_poll(state):
    s=state
    bridge=RecoveryPhaseDispatch(s.q,database=database(),identity=(s.run,s.nonce,'incarnation-a'),
        binding_current=lambda binding:binding is s)
    phase=WssSnapshotPhases(bridge,operation_key='trusted-owner-readback')
    async def one_round(observed):
        task=asyncio.create_task(phase.read_handler_status(s))
        for _ in range(100):
            work=await s.a.next_channel_work(s.run,s.token,s.generation)
            if work:break
            await asyncio.sleep(.001)
        else:pytest.fail('WSS readback not scheduled')
        s.a.mark_work_sent(work)
        frame=dict(type='DISPATCH',protocol_version=2,connection_generation=s.generation,
            **dict(work.document_payload),compute_claim=work.compute_claim,
            dispatch_generation=work.dispatch_generation)
        assert frame['phase']=='read_handler_status' and frame['arguments']=={}
        assert frame['kind']=='recovery_phase'
        assert not any(k in frame for k in ('path','python','constructor','source'))
        settled=await bridge.receive(response(s,frame,observed),run_id=s.run,
            token=s.token,generation=s.generation)
        assert settled['status']=='accepted'
        out=await task
        assert out==observed
        res=work.resource
        await s.a.resource_released(s.run,s.token,s.generation,
            runtime_incarnation=res.runtime_incarnation,resource_kind='document',
            resource_id=res.resource_id,compute_claim=res.compute_claim,
            dispatch_generation=res.dispatch_generation)
        return frame['operation_key']
    try:
        key1=await one_round({'loaded_sha256':'a'*64,'generation':0})
        key2=await one_round({'loaded_sha256':'b'*64,'generation':1})
        assert key1!=key2  # stale cached first reply is not a live readback
    finally:await bridge.aclose()



@pytest.mark.asyncio
async def test_live_admission_wss_reviewed_blob_stage_and_reconcile_readback(state,tmp_path):
    """Same durable compute claim transports a pinned artifact, never Python bytes."""
    from regional_knowledge.kaggle_warm_maintenance import WarmHandlerMaintenance,WarmHandlerPatchApproval
    from regional_knowledge.kaggle_warm_read import ReadHandlerSlot
    from test_kaggle_warm_read import old,context,staged_module,good_probe,good_stage
    from types import SimpleNamespace
    from regional_knowledge.kaggle_pg_snapshot_native import BlobRef
    import hashlib
    s=state
    _,source,digest=staged_module(tmp_path)
    blob=BlobRef('rkb/snapshots/blobs/owner/'+digest,source.stat().st_size,digest,'"etag"')
    approval=WarmHandlerPatchApproval(blob,'a'*40)
    signed=[]
    class Tickets:
        async def issue_download(self,binding,key,item):
            signed.append((binding,key,item))
            return SimpleNamespace(wire=lambda:dict(scope='existing-signed-GET'))
    bridge=RecoveryPhaseDispatch(s.q,database=database(),identity=(s.run,s.nonce,'incarnation-a'),
        binding_current=lambda b:b is s,snapshot_tickets=Tickets())
    control=WssSnapshotPhases(bridge,operation_key='trusted-warm-review')
    root=tmp_path/'owned';root.mkdir(mode=0o700)
    def workpath(key,phase):
        return root/(hashlib.sha256(key.encode()).hexdigest()+'-'+phase)
    transfer=[]
    async def download(item,key,*,ticket,transfer_operation):
        assert item==blob and transfer_operation==key
        assert ticket==dict(scope='existing-signed-GET')
        target=workpath(key,'download')
        target.write_bytes(source.read_bytes());target.chmod(0o600)
        transfer.append(item)
        return target
    native=object.__new__(NativeSnapshotFactory)
    native.database=database();native.identity=(s.run,s.nonce,'incarnation-a')
    native._current=lambda binding:None
    native._owned_candidates=set();native._capabilities=set()
    worker=WorkerRecoveryPhases(factory=native,download=download,workspace=workpath)
    slot=ReadHandlerSlot(old,loaded_sha256='b'*64,compatibility=context())
    worker.read_handler_slot=slot
    worker.read_handler_control=WarmHandlerMaintenance(slot=slot,download=download,
        workspace=workpath,constructor={'marker':'new'},self_test=good_stage,
        probe=good_probe,current=lambda frame:None)

    async def deliver(task,*,rejected=False):
        for _ in range(100):
            work=await s.a.next_channel_work(s.run,s.token,s.generation)
            if work:break
            await asyncio.sleep(.001)
        else:pytest.fail('signed warm maintenance DISPATCH missing')
        s.a.mark_work_sent(work)
        # next_channel_work already runs the exact maintenance dispatch
        # and issues the scoped GET ticket once; a second dispatch_payload
        # call would unnecessarily request a second signing operation.
        frame=dict(type='DISPATCH',protocol_version=2,connection_generation=s.generation,
            **dict(work.document_payload),compute_claim=work.compute_claim,
            dispatch_generation=work.dispatch_generation)
        assert 'source' not in frame and 'module_path' not in frame
        actual=await worker(frame)
        # Lost WSS ACK/reconnect: the SAME persisted operation is served
        # from its exact retained result. It never downloads or activates a
        # second time; a changed claim or source needs independent status.
        assert await worker(frame)==actual
        settled=await bridge.receive(response(s,frame,actual['value']),
            run_id=s.run,token=s.token,generation=s.generation)
        assert settled['status']=='accepted'
        if rejected:
            assert actual['value']==dict(status='rejected',reason='read_handler_maintenance_rejected')
            with pytest.raises(SnapshotError,match='read_handler_maintenance_rejected'):
                await task
        else:
            result=await task
            assert result==actual['value']
        resource=work.resource
        await s.a.resource_released(s.run,s.token,s.generation,
            runtime_incarnation=resource.runtime_incarnation,resource_kind='document',
            resource_id=resource.resource_id,compute_claim=resource.compute_claim,
            dispatch_generation=resource.dispatch_generation)
        return frame

    try:
        staged=asyncio.create_task(control.stage_read_handler(s,approval,
            expected_loaded_sha256='b'*64,expected_generation=0))
        frame=await deliver(staged)
        assert frame['phase']=='read_handler_stage'
        assert frame['download_ticket']==dict(scope='existing-signed-GET')
        assert signed and signed[0][2]==blob and len(transfer)==1
        assert slot.receipt()['staged_sha256']==digest
        applied=asyncio.create_task(control.activate_read_handler(s,staged_sha256=digest,
            expected_loaded_sha256='b'*64,expected_generation=0))
        phase=await deliver(applied)
        assert phase['phase']=='read_handler_activate'
        assert 'download_ticket' not in phase
        status=asyncio.create_task(control.read_handler_status(s))
        await deliver(status)
        assert slot.receipt()['loaded_sha256']==digest and slot.receipt()['generation']==1
        assert len(signed)==1 and len(transfer)==1
        # A presigned GET may be renewed after reconnect. Transient ticket
        # bytes are deliberately excluded from the operation identity, so
        # an exact completed stage replays its pinned RESULT without a new
        # transfer or executing code again, even if the ticket has changed.
        cached=await worker(dict(frame,download_ticket={'renewed':'unused'}))
        assert cached['phase']=='read_handler_stage'
        assert cached['value']['staged_sha256']==digest
        assert len(signed)==1 and len(transfer)==1
        # A rejected competing activation is ACKed as a durable *operation*
        # failure. It must not escape _encode, kill the model-owning runtime,
        # replace an already active handler, or silently stage a second file.
        failure=asyncio.create_task(control.activate_read_handler(s,
            staged_sha256='c'*64,expected_loaded_sha256='b'*64,
            expected_generation=0))
        await deliver(failure,rejected=True)
        assert slot.receipt()['loaded_sha256']==digest
        assert slot.receipt()['generation']==1
        assert len(transfer)==1
    finally:
        await bridge.aclose()
        await worker.aclose()




@pytest.mark.asyncio
async def test_failed_warm_candidate_has_normal_rejected_receipt_and_existing_model_keeps_serving(tmp_path):
    from regional_knowledge.kaggle_warm_maintenance import WarmHandlerMaintenance,WarmHandlerPatchApproval
    from regional_knowledge.kaggle_warm_read import ReadHandlerSlot
    from test_kaggle_warm_read import old,context,staged_module,good_probe
    from regional_knowledge.kaggle_pg_snapshot_native import BlobRef
    from hashlib import sha256
    _,source,digest=staged_module(tmp_path)
    root=tmp_path/'owned';root.mkdir(mode=0o700)
    blob=BlobRef('rkb/snapshots/blobs/approved/'+digest,source.stat().st_size,digest,'"etag"')
    transfers=[]
    def workspace(key,phase):
        return root/(sha256(key.encode()).hexdigest()+'-'+phase)
    async def download(item,key,*,ticket,transfer_operation):
        transfers.append((item,key))
        output=workspace(key,'download')
        output.write_bytes(source.read_bytes());output.chmod(0o600)
        return output
    async def rejecting_selftest(candidate):
        assert (await candidate({'test':True},encode=None))['version']=='new'
        raise ValueError('private rejected candidate details')
    native=object.__new__(NativeSnapshotFactory)
    native.database=database();native.identity=('r','n','i')
    native._current=lambda b:None
    native._owned_candidates=set();native._capabilities=set()
    worker=WorkerRecoveryPhases(factory=native,download=download,workspace=workspace)
    slot=ReadHandlerSlot(old,loaded_sha256='a'*64,compatibility=context())
    worker.read_handler_slot=slot
    worker.read_handler_control=WarmHandlerMaintenance(slot=slot,download=download,
        workspace=workspace,constructor={'marker':'new'},
        self_test=rejecting_selftest,probe=good_probe,current=lambda _:None)
    def frame(phase,key,args,ticket=None):
        wire=dict(type='DISPATCH',protocol_version=2,connection_generation=1,id='job-'+key,
            claim='claim',kind='recovery_phase',run_id='r',launch_nonce='n',
            runtime_incarnation='i',database_generation=native.database.database_generation,
            writer_generation=native.database.writer_generation,phase=phase,operation_key=key,
            arguments=args,request_sha256=sha(encode({'phase':phase,'arguments':args})),
            compute_claim='resource',dispatch_generation=1)
        if ticket is not None:wire['download_ticket']=ticket
        return wire
    args=dict(artifact=WarmHandlerPatchApproval(blob,'a'*40).wire(),
        expected_loaded_sha256='a'*64,expected_generation=0)
    failed=frame('read_handler_stage','reject-me',args,{'scoped':'GET'})
    first=await worker(failed)
    assert first['phase']=='read_handler_stage'
    assert first['value']==dict(status='rejected',reason='read_handler_maintenance_rejected')
    assert await worker(failed)==first
    assert len(transfers)==1
    assert 'private' not in str(first)
    # Exercise the actual BgeWssTransport._encode path which previously
    # propagated the failed candidate into _failure()/close_runtime.
    from test_bge_wss_worker import module,CONFIG
    transport=module('bge_wss_transport')
    async def useful_query(frame,*,encode):return {'status':'ok'}
    executor=transport.BgeWssTransport(dict(CONFIG,shared_reads=True),
        lambda:lambda text:[1.]+[0.]*1023,lambda:{},
        query_handler=useful_query,recovery_handler=worker)
    rejected_again=dict(failed,id='job-transport',operation_key='transport-rejected')
    executor.encoding=asyncio.create_task(executor._encode(rejected_again))
    await executor.encoding
    executor._failure(executor.encoding)  # Must not raise and kill the worker.
    assert executor.pending['receipt']['value']==dict(
        status='rejected',reason='read_handler_maintenance_rejected')
    assert executor.pending['job_id']=='job-transport'
    # Ordinary worker work remains alive after the rejected patch.
    report=await worker(frame('read_handler_status','after',{}))
    assert report['value']['loaded_sha256']=='a'*64
    assert report['value']['generation']==0
    assert (await slot({'still':True},encode=None))['version']=='old'
    await worker.aclose()


@pytest.mark.asyncio
async def test_preparation_can_wait_for_recovery_while_same_channel_executes_phase(monkeypatch):
    transport=module('bge_wss_transport');p=Peer();stop=asyncio.Event();restored=asyncio.Event();calls=[]
    monkeypatch.setattr(transport.random,'uniform',lambda *_:.001)
    async def read(frame,*,encode):return result(frame)
    async def prepare(encoder,**identity):
        calls.append('prepare-wait');await restored.wait();calls.append('prepared')
        return PreparedRuntime(read,candidate())
    async def recovery(frame):
        calls.append('recovery');restored.set()
        return dict(phase=frame['phase'],request_sha256=frame['request_sha256'],value={'reference':'retained-cap'})
    def load():
        def forbidden(text):raise AssertionError('recovery invoked encoder')
        return forbidden
    worker=transport.BgeWssTransport(dict(CONFIG,shared_reads=True),load,lambda:{},prepare_runtime=prepare,
        recovery_handler=recovery,connector=p.connect,stop=stop,heartbeat_seconds=.01,maximum_seconds=3)
    task=asyncio.create_task(worker.run())
    try:
        await p.wait('RESOURCE_STATE')
        for _ in range(100):
            if calls:break
            await asyncio.sleep(.001)
        assert calls==['prepare-wait'] and not any(f['type']=='READY' for _,f in p.sent)
        frame=dict(type='DISPATCH',id='restore',claim='claim',kind='recovery_phase',run_id=CONFIG['run_id'],
            launch_nonce=CONFIG['launch_nonce'],runtime_incarnation=worker.incarnation,phase='finish',operation_key='op',
            request_sha256='a'*64,arguments={'retained':'native'},database_generation='db',writer_generation='writer',
            compute_claim='slot',dispatch_generation=1)
        await p.put(frame)
        receipt=await p.wait('RECOVERY_PHASE_RESULT')
        await p.wait('RESOURCE_RELEASED');await p.wait('READY')
        assert calls==['prepare-wait','recovery','prepared']
        await p.disconnect()
        replay=await p.wait('RECOVERY_PHASE_RESULT',generation=2)
        assert replay['receipt']==receipt['receipt'] and calls.count('recovery')==1
        await p.put(dict(type='RESULT_ACK',job_id='restore',claim='claim',status='accepted'),generation=2)
    finally:stop.set();await task


def test_actual_asgi_runs_maintenance_before_ready_and_persists_receipt(peer):
    from test_bge_shared_channel import connect,attach,send,receive_expected,release
    p=peer;retained={}
    with connect(p) as socket:
        generation=attach(p,socket)
        async def configure():
            bridge=RecoveryPhaseDispatch(p.q,database=database(),identity=(p.run,p.nonce,'synthetic-incarnation'),
                binding_current=lambda b:b is p)
            retained['bridge']=bridge
            retained['task']=asyncio.create_task(bridge.request(p,'verify',{'reference':'cap'},operation_key='restore-test'))
        p.client.portal.call(configure)
        try:
            send(socket,generation,'RESOURCE_STATE',state='idle')
            frame=receive_expected(socket,'DISPATCH')
            assert frame['kind']=='recovery_phase' and p.q.status()['state']=='starting'
            release(socket,generation,frame)
            send(socket,generation,'RECOVERY_PHASE_RESULT',job_id=frame['id'],claim=frame['claim'],
                receipt=dict(phase='verify',request_sha256=frame['request_sha256'],value={'verified':True}),
                **{k:frame[k] for k in ('run_id','launch_nonce','runtime_incarnation','compute_claim','dispatch_generation')})
            assert receive_expected(socket,'RESULT_ACK')['status']=='accepted'
            async def result():return await retained['task']
            assert p.client.portal.call(result)=={'verified':True}
            assert p.q.result(p.auth.p.owner.subject,p.document)['state']=='pending'
        finally:p.client.portal.call(retained['bridge'].aclose)




def test_actual_asgi_read_handler_loaded_sha_through_held_maintenance_claim(peer):
    """Real WSS frames carry loaded-version receipt, no patch source or paths."""
    from test_bge_shared_channel import connect,attach,send,receive_expected,release
    p=peer;retained={}
    with connect(p) as socket:
        generation=attach(p,socket)
        async def configure():
            bridge=RecoveryPhaseDispatch(p.q,database=database(),
                identity=(p.run,p.nonce,'synthetic-incarnation'),
                binding_current=lambda binding:binding is p)
            retained['bridge']=bridge
            maintenance=WssSnapshotPhases(bridge,operation_key='readback-owner')
            retained['task']=asyncio.create_task(maintenance.read_handler_status(p))
        p.client.portal.call(configure)
        try:
            send(socket,generation,'RESOURCE_STATE',state='idle')
            frame=receive_expected(socket,'DISPATCH')
            assert frame['kind']=='recovery_phase'
            assert frame['phase']=='read_handler_status'
            assert frame['arguments']=={}
            assert 'module_path' not in frame and 'constructor' not in frame
            receipt=dict(loaded_sha256='f'*64,loaded_kind='verified_bundle',generation=0)
            release(socket,generation,frame)
            send(socket,generation,'RECOVERY_PHASE_RESULT',
                job_id=frame['id'],claim=frame['claim'],
                receipt=dict(phase='read_handler_status',
                    request_sha256=frame['request_sha256'],value=receipt),
                **{key:frame[key] for key in (
                    'run_id','launch_nonce','runtime_incarnation',
                    'compute_claim','dispatch_generation')})
            assert receive_expected(socket,'RESULT_ACK')['status']=='accepted'
            async def output():return await retained['task']
            assert p.client.portal.call(output)==receipt
        finally:p.client.portal.call(retained['bridge'].aclose)


@pytest.mark.asyncio
async def test_upload_ticket_intent_survives_disconnect_without_second_capture(monkeypatch):
    transport=module('bge_wss_transport');p=Peer();stop=asyncio.Event();ready=asyncio.Event();calls=[]
    broker=SnapshotTicketChannel(timeout_seconds=2)
    monkeypatch.setattr(transport.random,'uniform',lambda *_:.001)
    async def read(frame,*,encode):return result(frame)
    async def prepare(encoder,**identity):await ready.wait();return PreparedRuntime(read,candidate())
    async def recovery(frame):
        calls.append('capture')
        ticket=await broker.upload_ticket(size=123,sha256='a'*64)
        assert ticket=={'synthetic':'exact-scoped-ticket'}
        ready.set()
        return dict(phase='capture',request_sha256=frame['request_sha256'],value={'uploaded':'verified-next'})
    worker=transport.BgeWssTransport(dict(CONFIG,shared_reads=True),lambda:lambda _:None,lambda:{},
        prepare_runtime=prepare,recovery_handler=recovery,snapshot_channel=broker,connector=p.connect,
        stop=stop,heartbeat_seconds=.01,maximum_seconds=3)
    task=asyncio.create_task(worker.run())
    try:
        await p.wait('RESOURCE_STATE')
        await p.put(dict(type='DISPATCH',id='capture',claim='claim',kind='recovery_phase',run_id=CONFIG['run_id'],
            launch_nonce=CONFIG['launch_nonce'],runtime_incarnation=worker.incarnation,phase='capture',operation_key='stable-op',
            request_sha256='b'*64,arguments={},database_generation='db',writer_generation='writer',
            compute_claim='actual-slot',dispatch_generation=1))
        first=await p.wait('SNAPSHOT_UPLOAD_INTENT')
        assert first['size']==123 and first['compute_claim']=='actual-slot'
        await p.disconnect()
        replay=await p.wait('SNAPSHOT_UPLOAD_INTENT',generation=2)
        assert {k:v for k,v in first.items() if k!='connection_generation'}=={k:v for k,v in replay.items() if k!='connection_generation'}
        assert broker.pending['request_id']==first['request_id'] and calls==['capture']
        await p.put(dict(type='SNAPSHOT_UPLOAD_TICKET',request_id=first['request_id'],ticket={'synthetic':'exact-scoped-ticket'}),generation=2)
        await p.wait('RECOVERY_PHASE_RESULT',generation=2);await p.wait('READY',generation=2)
        await p.put(dict(type='RESULT_ACK',job_id='capture',claim='claim',status='accepted'),generation=2)
        assert calls==['capture'] and broker.pending is None
    finally:ready.set();stop.set();await task


@pytest.mark.asyncio
async def test_ticket_issuance_requires_exact_capture_resource_and_urls_are_not_persisted(state):
    s=state;issued=[]
    class Ticket:
        def wire(self):return {'url':'https://private-fixture.invalid/synthetic-signed-token'}
    class Issuer:
        async def issue_upload(self,binding,key,**args):issued.append((binding,key,args));return Ticket()
        async def issue_download(self,binding,key,blob):issued.append(('get',blob));return Ticket()
    bridge=RecoveryPhaseDispatch(s.q,database=database(),identity=(s.run,s.nonce,'incarnation-a'),
        binding_current=lambda b:b is s,snapshot_tickets=Issuer())
    try:
        task,work,frame=await work_for(s,bridge,'capture',{'versions':{},'expected_sequence':0})
        intent=dict(job_id=frame['id'],claim=frame['claim'],operation_key=frame['operation_key'],size=123,sha256='a'*64,
            **{k:frame[k] for k in ('run_id','launch_nonce','runtime_incarnation','compute_claim','dispatch_generation')})
        with pytest.raises(PermissionError):
            await bridge.issue_upload(intent|{'compute_claim':'forged'},run_id=s.run,token=s.token,generation=s.generation)
        assert not issued
        ticket=await bridge.issue_upload(intent,run_id=s.run,token=s.token,generation=s.generation)
        assert ticket['url'].endswith('synthetic-signed-token') and len(issued)==1
        with s.q.connect(write=False) as db:
            row=db.execute('select identity,result from jobs where id=?',(frame['id'],)).fetchone()
            assert 'signed-token' not in str(tuple(row))
        await bridge.receive(response(s,frame,{'native':'capture'}),run_id=s.run,token=s.token,generation=s.generation)
        assert await task=={'native':'capture'}
    finally:await bridge.aclose()


def test_dataset_archive_ticket_uses_its_own_get_etag_not_the_upload_version():
    """Dataset1 shape: upload etag 'CL…=' differs from the version GET ETag '"md5"'."""
    from regional_knowledge.kaggle_dataset_base import DatasetCheckpoint,DatasetFile
    from regional_knowledge.kaggle_pg_snapshot_native import BlobRef
    from regional_knowledge.kaggle_recovery_phases import _download_ticket_matches
    from regional_knowledge.native_snapshot_transfer import SnapshotTicket
    archive=DatasetFile('pg-'+'a'*64+'.blob',56422400,'b'*64)
    other=lambda name:DatasetFile(name,10,'c'*64)
    pinned=DatasetCheckpoint('owner','checkpoints',1,'db',7,other('base-manifest.json'),archive,
        other('source-control.json'),other('journal-control.json')).wire()
    blob=BlobRef('dataset-blobs/'+'a'*64+'/'+archive.name,archive.size,archive.sha256,'CLvthNDhzpADEAE=')
    def ticket(etag,*,size=archive.size):
        return SnapshotTicket('GET','d'*64,blob.key,size,archive.sha256,2e9,'https://storage.googleapis.com/x',
            (('If-Match',etag),))
    get_etag='"'+'e'*32+'"'
    # Old behaviour: the S3-domain comparison rejects a correct Dataset ticket.
    assert not _download_ticket_matches(blob,ticket(get_etag))
    assert _download_ticket_matches(blob,ticket(get_etag),pinned)
    # Exact bytes and the pinned archive remain mandatory (GET tickets always carry If-Match).
    assert not _download_ticket_matches(blob,ticket(get_etag,size=1),pinned)
    unpinned=BlobRef(blob.key.replace('a'*64+'.blob','f'*64+'.blob'),archive.size,archive.sha256,'CLvthNDhzpADEAE=')
    assert not _download_ticket_matches(unpinned,SnapshotTicket('GET','d'*64,unpinned.key,archive.size,archive.sha256,
        2e9,'https://storage.googleapis.com/x',(('If-Match',get_etag),)),pinned)
    # Same-store recovery tickets keep the exact upload-version If-Match.
    assert _download_ticket_matches(blob,ticket('CLvthNDhzpADEAE='))
