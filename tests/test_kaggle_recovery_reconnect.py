"""Actual queue/admission reconnect, without Kaggle/model/database effects."""
import asyncio
import json
from dataclasses import replace
from types import SimpleNamespace
import pytest
from regional_knowledge.kaggle_recovery_dispatch import RecoveryPhaseDispatch
from regional_knowledge.kaggle_restore_startup import PreReadyRecoveryBinding,SourceRestoreCoordinator
from test_bge_admission import state,auth,people
from test_kaggle_recovery_phases import database,response
from test_kaggle_public_backend import target


@pytest.mark.asyncio
@pytest.mark.parametrize('ready',[False,True])
async def test_attachment_refresh_uses_serving_binding_for_ready_original_transfer(target,ready):
    from regional_knowledge.kaggle_public_backend import current_binding,TargetReadBinding
    from regional_knowledge.original_intake_controller import OriginalIntakeController
    t=target;serving=current_binding(t.queue)
    pre=PreReadyRecoveryBinding(t.run,serving.runtime_incarnation,t.generation,serving.database_generation,'writer')
    seen=[];restorer=object.__new__(SourceRestoreCoordinator)
    restorer.queue=t.queue
    restorer.startup=SimpleNamespace(schema_migrations=SimpleNamespace(resume_attachment=lambda:False))
    restorer._observe=lambda:(pre,t.nonce)
    restorer.dispatch=SimpleNamespace(identity=(t.run,t.nonce,serving.runtime_incarnation),bind_attachment=seen.append)
    if not ready:
        with t.queue.connect() as db:db.execute("UPDATE runs SET status='starting' WHERE id=?",(t.run,))
    assert restorer.resume_attachment()
    if ready:
        assert type(seen[0]) is TargetReadBinding
        originals=object.__new__(OriginalIntakeController)
        originals.binding_current=lambda value:value==current_binding(t.queue)
        originals._private={}
        assert await originals.dispatch_transfer(dict(phase='original_archive',arguments={},operation_key='retained'),seen[0]) is None
    else:assert seen==[pre]


@pytest.mark.asyncio
@pytest.mark.parametrize('claimed',[False,True])
async def test_reconnect_dispatches_retained_job_without_another_client_request(state,claimed):
    s=state;db=database()
    old=PreReadyRecoveryBinding(s.run,'incarnation-a',s.generation,db.database_generation,db.writer_generation)
    current=[old]
    restorer=object.__new__(SourceRestoreCoordinator)
    restorer.queue=s.q
    restorer.startup=SimpleNamespace(schema_migrations=SimpleNamespace(resume_attachment=lambda:False))
    restorer._observe=lambda:(current[0],s.nonce)
    bridge=RecoveryPhaseDispatch(s.q,database=db,identity=(s.run,s.nonce,'incarnation-a'),
        binding_current=lambda b:b==current[0],
        rebind_current=restorer.resume_attachment)
    restorer.dispatch=bridge
    task=asyncio.create_task(bridge.request(old,'verify',{'reference':'retained'},operation_key='only-request'))
    try:
        for _ in range(100):
            with s.q.connect(write=False) as conn:
                row=conn.execute("select id from jobs where kind='recovery_phase'").fetchone()
            if row:break
            await asyncio.sleep(.001)
        else:pytest.fail('request not queued')
        original_id=row['id']
        if claimed:
            work=await s.a.next_channel_work(s.run,s.token,s.generation)
            assert work.document_payload['id']==original_id
        s.generation=s.q.attach_channel(s.run,s.token,s.nonce,'incarnation-a')['connection_generation']
        current[0]=replace(old,connection_generation=s.generation)
        s.q.heartbeat(s.run,s.token,ready=True,connection_generation=s.generation)
        await s.a.resource_state(s.run,s.token,s.generation,runtime_incarnation='incarnation-a',state='idle')
        work=await s.a.next_channel_work(s.run,s.token,s.generation)
        assert work is not None,'reconnect must not wait for another request or the 900-second timeout'
        assert work.document_payload['id']==original_id
        s.a.mark_work_sent(work)
        frame=dict(type='DISPATCH',protocol_version=2,connection_generation=s.generation,
            **dict(work.document_payload),compute_claim=work.compute_claim,dispatch_generation=work.dispatch_generation)
        await bridge.receive(response(s,frame,{'verified':True}),run_id=s.run,token=s.token,generation=s.generation)
        with pytest.raises(PermissionError,match='binding expired'):await task
        with s.q.connect(write=False) as conn:
            rows=conn.execute("select state,attempt from jobs where kind='recovery_phase'").fetchall()
            assert len(rows)==1 and rows[0]['state']=='done' and rows[0]['attempt']==1
    finally:
        if not task.done():task.cancel()
        await asyncio.gather(task,return_exceptions=True)
        await bridge.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize('restart_owner',[False,True])
async def test_claimed_unsent_maintenance_survives_generation_rebind(state,restart_owner):
    s=state;db=database()
    old=PreReadyRecoveryBinding(s.run,'incarnation-a',s.generation,db.database_generation,db.writer_generation)
    current=[old]
    bridge=RecoveryPhaseDispatch(s.q,database=db,identity=(s.run,s.nonce,'incarnation-a'),
        binding_current=lambda b:b==current[0])
    tasks=[]
    try:
        tasks.append(asyncio.create_task(bridge.request(old,'verify',{'reference':'retained'},operation_key='original')))
        for _ in range(100):
            original=await s.a.next_channel_work(s.run,s.token,s.generation)
            if original is not None:break
            await asyncio.sleep(.001)
        else:pytest.fail('original maintenance absent')
        original_id=original.document_payload['id'];original_claim=original.document_payload['claim']
        if restart_owner:
            await bridge.aclose()
            bridge=RecoveryPhaseDispatch(s.q,database=db,identity=(s.run,s.nonce,'incarnation-a'),
                binding_current=lambda b:b==current[0])
        # Disconnect after durable claim, before its frame is physically sent.
        s.generation=s.q.attach_channel(s.run,s.token,s.nonce,'incarnation-a')['connection_generation']
        current[0]=replace(old,connection_generation=s.generation)
        s.q.heartbeat(s.run,s.token,ready=True,connection_generation=s.generation)
        await s.a.resource_state(s.run,s.token,s.generation,runtime_incarnation='incarnation-a',state='idle')
        tasks.append(asyncio.create_task(bridge.request(current[0],'verify',{'reference':'next'},operation_key='following')))
        await asyncio.sleep(0)
        with s.q.connect(write=False) as conn:
            retained=dict(conn.execute('select * from jobs where id=?',(original_id,)).fetchone())
            run=dict(conn.execute('select * from runs where id=?',(s.run,)).fetchone())
        for field in ('run_id','launch_nonce','runtime_incarnation','database_generation','writer_generation','request_sha256'):
            identity=json.loads(retained['identity']);identity[field]='different'
            assert not bridge.eligible(dict(retained,identity=json.dumps(identity)),run,s.generation)
        for _ in range(100):
            resumed=await s.a.next_channel_work(s.run,s.token,s.generation)
            if resumed is not None:break
            await asyncio.sleep(.001)
        else:pytest.fail('claimed maintenance became permanently ineligible after reconnect')
        assert resumed.document_payload['id']==original_id and resumed.document_payload['claim']==original_claim
        assert resumed.dispatch_generation==s.generation
        s.a.mark_work_sent(resumed)
        frame=dict(type='DISPATCH',protocol_version=2,connection_generation=s.generation,
            **dict(resumed.document_payload),compute_claim=resumed.compute_claim,dispatch_generation=resumed.dispatch_generation)
        await bridge.receive(response(s,frame,{'verified':True}),run_id=s.run,token=s.token,generation=s.generation)
        with pytest.raises(PermissionError,match='binding expired'):await tasks[0]
        with s.q.connect(write=False) as conn:
            job=conn.execute('select state,attempt,claim from jobs where id=?',(original_id,)).fetchone()
            assert job['state']=='done' and job['attempt']==1 and job['claim']==original_claim
    finally:
        for task in tasks:
            if not task.done():task.cancel()
        await asyncio.gather(*tasks,return_exceptions=True)
        await bridge.aclose()
