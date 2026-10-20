"""Actual durable admission/channel with synthetic native phases; no PG/model/S3."""
import asyncio
from dataclasses import replace
from types import SimpleNamespace

import pytest

from regional_knowledge.kaggle_recovery_dispatch import RecoveryPhaseDispatch,WssSnapshotPhases
from regional_knowledge.kaggle_recovery_phases import WorkerRecoveryPhases
from regional_knowledge.kaggle_pg_snapshot_native import NativeSnapshotFactory,SnapshotError,encode,sha
from regional_knowledge.kaggle_bootstrap import PreparedRuntime
from test_bge_admission import state,auth,people
from test_bge_shared_channel import peer
from test_bge_shared_worker import Peer,result
from test_bge_wss_worker import CONFIG,module
from test_kaggle_bootstrap import candidate
from test_kaggle_target_runtime import spec


def database():return replace(spec().database,mode='VERIFIED_INITIALIZED',ownership_evidence='synthetic-private-restore')


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
