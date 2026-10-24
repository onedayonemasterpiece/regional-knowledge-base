"""Existing queue/admission/journal with synthetic native apply; no PG/provider/model."""
import asyncio
import base64
from contextlib import asynccontextmanager
from dataclasses import replace
import json
from types import SimpleNamespace

import pytest
import pytest_asyncio

from regional_knowledge.control_spool import ControlSpool
from regional_knowledge.kaggle_apply import ApplyReceipt,HandlerRegistry
from regional_knowledge.kaggle_source_commands import (
    AcceptedSourceDispatch,ControllerAcceptedProof,WorkerSourceExecutor,canonical,digest,
    decode_receipt,receipt_wire,decode_seal,seal_wire,
)
from regional_knowledge.kaggle_bootstrap import PreparedRuntime
from regional_knowledge.bge_contract import SPACE
from test_bge_admission import state,auth,people
from test_bge_shared_worker import Peer,result
from test_bge_wss_worker import module,CONFIG
from test_kaggle_bootstrap import candidate
from test_kaggle_target_runtime import spec
from test_source_import import resolved,VERSIONS
from regional_knowledge.source_import import source_import_handler
from test_recovery_journal import fixture as journal_fixture


def native_receipt(command,handler,*,duplicate=False):
    result=canonical(dict(source_id=command.ids['source_id'],accepted=True))
    body=canonical(dict(codec='rkb.applied-outcome.v1',operation_key=command.operation_key,
        command_sha256=digest(command.encode()),cursor=command.sequence,status='applied',
        ids=dict(command.ids),request_id=command.request_id,person_id=command.person_id,route=command.route,
        logical_time=command.logical_time,before=list(VERSIONS.wire()),after=list(VERSIONS.wire()),
        handler_sha256=handler.fingerprint,result_b64=base64.b64encode(result).decode(),result_sha256=digest(result)))
    return ApplyReceipt('synthetic-db',command.operation_key,command.sequence,digest(command.encode()),
        'applied',body,digest(body),duplicate)


@pytest_asyncio.fixture
async def accepted(state):
    s=state
    with s.q.connect() as db:db.execute('DELETE FROM jobs WHERE id=?',(s.doc,))
    store,journal=journal_fixture()
    handler=source_import_handler(revisions=VERSIONS,check_sha256='a'*64,apply_sha256='b'*64)
    registry=HandlerRegistry([handler])
    spool=ControlSpool(s.q,s.auth.p.registry,journal,registry)
    command=resolved(person_id=s.auth.p.owner.subject)
    await spool.stage_source_import(s.auth.p.admin,command,database_generation='synthetic-db')
    seal=await spool.seal(s.auth.p.admin,command.operation_key,predecessor=None,writer_generation='writer-1')
    bindings=[]
    def current(**values):
        bindings.append(values)
        return (values['run_id'],values['runtime_incarnation'],values['database_generation'],values['writer_generation'])==(
            s.run,'incarnation-a','synthetic-db','writer-1')
    bridge=AcceptedSourceDispatch(spool,binding_current=current)
    e=SimpleNamespace(s=s,spool=spool,store=store,journal=journal,command=command,seal=seal,
        handler=handler,registry=registry,bridge=bridge,bindings=bindings)
    try:yield e
    finally:
        await bridge.aclose()
        spool.close()


async def dispatch(e):
    s=e.s
    job=await e.bridge.enqueue(s.auth.p.admin,e.command.operation_key,writer_generation='writer-1')
    work=await s.a.next_channel_work(s.run,s.token,s.generation)
    assert work.document_payload['id']==job and work.document_payload['kind']=='source_command'
    s.a.mark_work_sent(work)
    frame=dict(type='DISPATCH',protocol_version=2,connection_generation=s.generation,
        **dict(work.document_payload),compute_claim=work.compute_claim,dispatch_generation=work.dispatch_generation)
    return job,work,frame


def response(e,work,frame):
    return dict(type='SOURCE_COMMAND_RESULT',protocol_version=2,connection_generation=e.s.generation,
        job_id=frame['id'],claim=frame['claim'],receipt=receipt_wire(native_receipt(e.command,e.handler)),
        **{key:frame[key] for key in ('run_id','launch_nonce','runtime_incarnation','compute_claim','dispatch_generation')})


async def release(e,work):
    r=work.resource
    await e.s.a.resource_released(e.s.run,e.s.token,e.s.generation,runtime_incarnation=r.runtime_incarnation,
        resource_kind='document',resource_id=r.resource_id,compute_claim=r.compute_claim,dispatch_generation=r.dispatch_generation)


@pytest.mark.asyncio
async def test_only_exact_remote_accepted_source_enters_durable_queue(accepted):
    e=accepted;s=e.s
    with pytest.raises(ValueError):s.q.enqueue(s.auth.p.owner.subject,'forged',[],kind='source_command')
    with pytest.raises(PermissionError):s.q._enqueue_source_command(object(),s.auth.p.owner.subject,{})
    with pytest.raises(PermissionError):await e.bridge.enqueue(s.auth.p.personal,e.command.operation_key,writer_generation='writer-1')
    reads=len(e.store.reads)
    job,work,frame=await dispatch(e)
    assert len(e.store.reads)>reads
    assert base64.b64decode(frame['command_b64'])==e.command.encode()
    assert frame['accepted']==seal_wire(e.seal)
    assert frame['handler_sha256']==e.handler.fingerprint
    assert 'token' not in frame and 'vectors' not in frame and 'texts' not in frame
    assert e.bindings[-1]['connection_generation']==s.generation
    with pytest.raises(PermissionError):
        s.q.complete(s.run,s.token,job,frame['claim'],SPACE,[[1.]+[0.]*1023],{},connection_generation=s.generation)


@pytest.mark.asyncio
async def test_worker_uses_captured_proof_and_fixed_registry_without_encode(accepted,monkeypatch):
    e=accepted;_,work,frame=await dispatch(e);calls=[]
    database=spec().database
    @asynccontextmanager
    async def acquire():
        calls.append('acquire');yield object()
    async def apply(connection,**kwargs):
        assert await kwargs['journal'].verify_accepted(kwargs['seal'],kwargs['command'])==digest(e.command.encode())
        with pytest.raises(PermissionError):
            await kwargs['journal'].verify_accepted(kwargs['seal'],replace(e.command,logical_time='changed'))
        calls.append('apply')
        return native_receipt(e.command,e.handler)
    monkeypatch.setattr('regional_knowledge.kaggle_source_commands.apply_accepted',apply)
    executor=WorkerSourceExecutor(acquire=acquire,registry=e.registry,database=database,
        run_id=e.s.run,launch_nonce=e.s.nonce,runtime_incarnation='incarnation-a')
    receipt,_=decode_receipt(await executor(frame))
    assert receipt.command_sha256==digest(e.command.encode()) and calls==['acquire','apply']
    for changes in (dict(command_sha256='f'*64),dict(writer_generation='stale-writer'),
                    dict(handler_sha256='f'*64),dict(runtime_incarnation='forged'),dict(sql='DROP DATABASE rkb')):
        with pytest.raises((ValueError,PermissionError)):
            await executor(frame|changes)
    assert calls==['acquire','apply']


@pytest.mark.asyncio
async def test_local_apply_is_pending_until_remote_outcome_then_exact_ack(accepted):
    e=accepted;s=e.s;job,work,frame=await dispatch(e)
    await release(e,work)
    original=e.spool.publish_outcome;entered=asyncio.Event();finish=asyncio.Event()
    async def blocked(*args,**kwargs):
        entered.set();await finish.wait();return await original(*args,**kwargs)
    e.spool.publish_outcome=blocked
    assert await e.bridge.receive(response(e,work,frame),run_id=s.run,token=s.token,generation=s.generation) is None
    await asyncio.wait_for(entered.wait(),1)
    assert (await e.spool.get(e.command.operation_key)).apply_state=='APPLIED'
    assert (await e.spool.get(e.command.operation_key)).outcome_state!='DURABLE'
    assert s.q.result(s.auth.p.owner.subject,job)['state']=='claimed'
    assert await e.bridge.poll_settlement(run_id=s.run,token=s.token,generation=s.generation) is None
    finish.set()
    for _ in range(20):
        await asyncio.sleep(.01)
        settled=await e.bridge.poll_settlement(run_id=s.run,token=s.token,generation=s.generation)
        if settled is not None:break
    assert settled==dict(job_id=job,claim=frame['claim'],status='accepted')
    assert s.q.result(s.auth.p.owner.subject,job)['state']=='done'
    assert (await e.spool.get(e.command.operation_key)).outcome_state=='DURABLE'
    # Replayed native receipt reconstructs delivery from original durable state.
    await e.bridge.receive(response(e,work,frame),run_id=s.run,token=s.token,generation=s.generation)
    for _ in range(20):
        await asyncio.sleep(.01)
        duplicate=await e.bridge.poll_settlement(run_id=s.run,token=s.token,generation=s.generation,job_id=job)
        if duplicate:break
    assert duplicate['status']=='duplicate'


@pytest.mark.asyncio
async def test_checkpoint_pending_releases_assignment_without_saved_and_promotes_same_job_after_remote_receipt(accepted):
    from regional_knowledge.control_spool import SpoolConflict
    e=accepted;s=e.s;job,work,frame=await dispatch(e)
    await release(e,work)
    await e.spool.record_apply(e.command.operation_key,native_receipt(e.command,e.handler))
    status=s.q._complete_source_command(e.bridge,s.run,s.token,s.generation,job,frame['claim'],
        (await e.spool.get(e.command.operation_key)).command_sha256,remote_outcome_durable=False)
    assert status=='checkpoint_pending'
    pending=s.q.result(s.auth.p.owner.subject,job)
    assert pending['state']=='done' and pending['result']['remote_outcome_durable'] is False
    with pytest.raises(SpoolConflict,match='durable outcome'):
        e.spool._settle_recovered_jobs(e.command.operation_key)
    await e.spool.publish_outcome(e.command.operation_key,writer_generation='writer-1')
    assert e.spool._settle_recovered_jobs(e.command.operation_key)==[job]
    saved=s.q.result(s.auth.p.owner.subject,job)
    assert saved['result']['remote_outcome_durable'] is True
    assert saved['result']['command_sha256']==pending['result']['command_sha256']


@pytest.mark.asyncio
async def test_concurrent_channel_pollers_reserve_one_remote_publication(accepted):
    e=accepted;s=e.s;job,work,frame=await dispatch(e)
    await release(e,work)
    await e.spool.record_apply(e.command.operation_key,native_receipt(e.command,e.handler),
        delivery=(job,s.run,'incarnation-a',frame['claim']))
    original_entry=e.bridge._entry;original_reconcile=e.bridge.reconcile
    both_entered=asyncio.Event();finish_entry=asyncio.Event();finish_publication=asyncio.Event()
    entered=[];started=[]
    async def entry(job,*,verify=True):
        value=await original_entry(job,verify=verify)
        if not verify:
            entered.append(job['id'])
            if len(entered)==2:both_entered.set()
            await finish_entry.wait()
        return value
    async def reconcile(**kwargs):
        started.append(kwargs['job_id'])
        await finish_publication.wait()
        return await original_reconcile(**kwargs)
    e.bridge._entry=entry;e.bridge.reconcile=reconcile
    polls=[asyncio.create_task(e.bridge.poll_settlement(run_id=s.run,token=s.token,
        generation=s.generation)) for _ in range(2)]
    try:
        await asyncio.wait_for(both_entered.wait(),1)
        finish_entry.set()
        assert await asyncio.gather(*polls)==[None,None]
        await asyncio.sleep(0)
        assert started==[job], 'one job must retain exactly one publication task'
        assert len(e.bridge._publications)==1
        finish_publication.set()
        await asyncio.gather(*(task for _,task in e.bridge._publications.values()))
        settled=await e.bridge.poll_settlement(run_id=s.run,token=s.token,generation=s.generation)
        assert settled==dict(job_id=job,claim=frame['claim'],status='accepted')
        assert (await e.spool.get(e.command.operation_key)).outcome_state=='DURABLE'
        assert s.q.result(s.auth.p.owner.subject,job)['result']['remote_outcome_durable']
    finally:
        finish_entry.set();finish_publication.set()
        await asyncio.gather(*polls,return_exceptions=True)


@pytest.mark.asyncio
async def test_source_receipt_rejects_forged_resource_and_exact_outcome(accepted):
    e=accepted;s=e.s;_,work,frame=await dispatch(e)
    wrong=response(e,work,frame);wrong['compute_claim']='forged'
    with pytest.raises(PermissionError):await e.bridge.receive(wrong,run_id=s.run,token=s.token,generation=s.generation)
    wrong=response(e,work,frame);wrong['receipt']['database_generation']='other-db'
    with pytest.raises(PermissionError):await e.bridge.receive(wrong,run_id=s.run,token=s.token,generation=s.generation)
    assert (await e.spool.get(e.command.operation_key)).apply_receipt is None


@pytest.mark.asyncio
@pytest.mark.parametrize('final_status',['accepted','checkpoint_pending'])
async def test_worker_reconnect_retains_exact_native_receipt_and_never_reencodes(monkeypatch,final_status):
    transport=module('bge_wss_transport');peer=Peer();stop=asyncio.Event();calls=[]
    monkeypatch.setattr(transport.random,'uniform',lambda *_:.001)
    async def read(frame,*,encode):return result(frame)
    async def source(frame):
        calls.append(frame['command_sha256'])
        return dict(native='synthetic native receipt')
    async def prepare(encoder,**identity):return PreparedRuntime(read,candidate(),source)
    def load():
        def forbidden_encode(text):raise AssertionError('source command invoked BGE')
        return forbidden_encode
    worker=transport.BgeWssTransport(dict(CONFIG,shared_reads=True),load,lambda: {},
        prepare_runtime=prepare,connector=peer.connect,stop=stop,heartbeat_seconds=.01,maximum_seconds=3)
    task=asyncio.create_task(worker.run())
    try:
        await peer.wait('READY')
        frame=dict(type='DISPATCH',id='source-job',claim='source-claim',kind='source_command',
            run_id=CONFIG['run_id'],launch_nonce=CONFIG['launch_nonce'],runtime_incarnation=worker.incarnation,
            command_sha256='a'*64,command_b64='synthetic',accepted={},handler_sha256='b'*64,
            database_generation='db',writer_generation='writer',compute_claim='actual-slot',dispatch_generation=1)
        await peer.put(frame)
        first=await peer.wait('SOURCE_COMMAND_RESULT')
        await peer.put(dict(type='RESULT_ACK',job_id='source-job',claim='source-claim',status='apply_received'))
        assert worker.pending is not None
        await peer.disconnect()
        replay=await peer.wait('SOURCE_COMMAND_RESULT',generation=2)
        assert {k:v for k,v in replay.items() if k!='connection_generation'}=={k:v for k,v in first.items() if k!='connection_generation'}
        await peer.put(frame,generation=2)
        await peer.put(dict(type='RESULT_ACK',job_id='source-job',claim='source-claim',status=final_status),generation=2)
        await asyncio.sleep(.02)
        assert calls==['a'*64] and worker.pending is None
    finally:
        stop.set();await task


def test_actual_asgi_channel_delivers_source_command_and_remote_durable_ack(peer):
    from test_bge_shared_channel import connect,attach,ready,receive_expected,send,release as wire_release
    p=peer
    with p.q.connect() as db:db.execute('DELETE FROM jobs WHERE id=?',(p.document,))
    retained={}
    async def configure():
        store,journal=journal_fixture()
        handler=source_import_handler(revisions=VERSIONS,check_sha256='a'*64,apply_sha256='b'*64)
        spool=ControlSpool(p.q,p.auth.p.registry,journal,HandlerRegistry([handler]))
        command=resolved(person_id=p.auth.p.owner.subject)
        await spool.stage_source_import(p.auth.p.admin,command,database_generation='synthetic-db')
        await spool.seal(p.auth.p.admin,command.operation_key,predecessor=None,writer_generation='writer-1')
        bridge=AcceptedSourceDispatch(spool,binding_current=lambda **kwargs:kwargs['database_generation']=='synthetic-db')
        job=await bridge.enqueue(p.auth.p.admin,command.operation_key,writer_generation='writer-1')
        retained.update(spool=spool,bridge=bridge,job=job,command=command,handler=handler)
    p.client.portal.call(configure)
    try:
        with connect(p) as socket:
            generation=attach(p,socket);ready(socket,generation)
            frame=receive_expected(socket,'DISPATCH')
            assert frame['kind']=='source_command' and frame['id']==retained['job']
            assert base64.b64decode(frame['command_b64'])==retained['command'].encode()
            wire_release(socket,generation,frame)
            send(socket,generation,'SOURCE_COMMAND_RESULT',job_id=frame['id'],claim=frame['claim'],
                receipt=receipt_wire(native_receipt(retained['command'],retained['handler'])),
                **{key:frame[key] for key in ('run_id','launch_nonce','runtime_incarnation','compute_claim','dispatch_generation')})
            ack=receive_expected(socket,'RESULT_ACK')
            assert ack['status']=='apply_received'
            ack=receive_expected(socket,'RESULT_ACK')
            assert ack['status']=='accepted'
            assert p.q.result(p.auth.p.owner.subject,retained['job'])['result']['remote_outcome_durable']
            async def status():return await retained['spool'].get(retained['command'].operation_key)
            assert p.client.portal.call(status).outcome_state=='DURABLE'
    finally:
        p.client.portal.call(retained['bridge'].aclose)
        retained['spool'].close()


# Reuse the actual ASGI/identity fixture; it creates no external provider.
from test_bge_shared_channel import peer


@pytest.mark.asyncio
async def test_exact_read_uses_same_admitted_slot_and_requires_paired_result(state):
    from regional_knowledge.kaggle_protocol import PassageReadRequest,PassageReadResult,QueryResult
    from test_kaggle_exact_fetch import locator
    from test_bge_admission import query
    s=state
    request=PassageReadRequest(request_id='exact-slot',connection_generation=s.generation,turn_generation=3,
        locator=locator(),actor=query(s.context,'search').actor,min_data_seq=0,deadline_ms=1500)
    ticket=await s.a.begin_read(s.context,request,dialog_ref='exact-dialog',deadline=s.a.clock()+1.4)
    work=await s.a.next_channel_work(s.run,s.token,s.generation)
    assert work.query.type=='PASSAGE_READ' and work.resource.resource_kind=='read'
    s.a.mark_work_sent(work)
    common=dict(request_id='exact-slot',connection_generation=s.generation,turn_generation=3,
        status='unavailable',reason='evidence_withdrawn',observed_data_seq=1,observed_projection_seq=1)
    with pytest.raises(ValueError,match='operation type'):
        await s.a.finish_read(s.run,s.token,s.generation,QueryResult(**common,mode=None))
    exact=PassageReadResult(**common)
    assert await s.a.finish_read(s.run,s.token,s.generation,exact)
    assert await ticket.result is exact


@pytest.mark.asyncio
async def test_worker_exact_passage_wire_never_encodes():
    from test_bge_shared_worker import running
    from test_kaggle_exact_fetch import request
    from regional_knowledge.kaggle_protocol import PassageReadResult
    calls=[]
    async def handler(frame,*,encode):
        calls.append(frame['type'])
        return json.loads(canonical(PassageReadResult(request_id=frame['request_id'],
            connection_generation=frame['connection_generation'],turn_generation=frame['turn_generation'],
            status='unavailable',reason='evidence_withdrawn',observed_data_seq=12,observed_projection_seq=12).model_dump(mode='json')))
    def forbidden(text):raise AssertionError('exact passage read invoked BGE')
    async with running(handler,forbidden) as (peer,task,loads):
        frame=request(connection_generation=1).model_dump(mode='json')
        await peer.put(frame)
        answer=await peer.wait('PASSAGE_READ_RESULT')
        assert answer['reason']=='evidence_withdrawn' and 'mode' not in answer
        assert calls==['PASSAGE_READ']
        await peer.wait('RESOURCE_RELEASED')


@pytest.mark.asyncio
@pytest.mark.parametrize('unsettled',[False,True])
async def test_queued_source_waits_for_history_but_public_read_progresses(accepted,unsettled):
    from test_bge_admission import begin,answer,release as release_read
    e=accepted;s=e.s
    job=await e.bridge.enqueue(s.auth.p.admin,e.command.operation_key,writer_generation='writer-1')
    e.spool._native_history_unsettled=unsettled
    if not unsettled:await e.spool._lock.acquire()
    ticket=None
    try:
        assert e.bridge.maintenance_blocks_dispatch()
        assert await s.a.next_channel_work(s.run,s.token,s.generation) is None
        with s.q.connect(write=False) as db:
            assert db.execute('SELECT state FROM jobs WHERE id=?',(job,)).fetchone()[0]=='pending'
        ticket=await begin(s,'independent-public-read')
        work=await s.a.next_channel_work(s.run,s.token,s.generation)
        assert work.query is not None and work.query.request_id=='independent-public-read'
        s.a.mark_work_sent(work)
        assert await s.a.finish_read(s.run,s.token,s.generation,answer(work))
        assert (await ticket.result).status=='ok'
        await release_read(s,work)
        assert await s.a.next_channel_work(s.run,s.token,s.generation) is None
    finally:
        if not unsettled:e.spool._lock.release()
        e.spool._native_history_unsettled=False
        if ticket is not None and not ticket.result.done():s.a.cancel_read(ticket)
    work=await s.a.next_channel_work(s.run,s.token,s.generation)
    assert work.document_payload['id']==job
    s.a.discard_unsent(work)


@pytest.mark.asyncio
async def test_restored_dataset_cursor_dispatches_pending_command_without_resetting_history_fence(accepted):
    from regional_knowledge.kaggle_public_backend import TargetReadBinding
    e=accepted;s=e.s
    job=await e.bridge.enqueue(s.auth.p.admin,e.command.operation_key,writer_generation='writer-1')
    # NativeHistoryWSS.bind installed the legacy sentinel, but Dataset restore
    # never opened a native history session to clear it.
    e.spool._native_history_unsettled=True
    assert e.bridge.maintenance_blocks_dispatch()
    binding=TargetReadBinding(s.run,'incarnation-a',s.generation,'synthetic-db',0,'1')
    live=[True]
    def current(b):
        return (live[0] and b==binding and e.bridge.binding_current(
            run_id=b.run_id,runtime_incarnation=b.runtime_incarnation,
            connection_generation=b.connection_generation,
            database_generation=b.database_generation,writer_generation='writer-1'))
    cursor=await e.spool.dataset_cursor(binding,control={'sequence':0,'heads':[]},current=current)
    assert cursor.current() and cursor.accepted_sequence==1 and cursor.applied_sequence==0 and cursor.pending
    assert e.spool._native_history_unsettled is True  # no manual release of a native fence
    live[0]=False
    assert e.bridge.maintenance_blocks_dispatch()  # writer/binding must remain current
    live[0]=True
    assert not e.bridge.maintenance_blocks_dispatch()
    work=await s.a.next_channel_work(s.run,s.token,s.generation)
    assert work.document_payload['id']==job
    s.a.discard_unsent(work)
    with s.q.connect(write=False) as db:
        assert db.execute('SELECT state FROM jobs WHERE id=?',(job,)).fetchone()[0] in ('pending','claimed')

    # A lost cursor cannot grant a second dispatch just because the constructor
    # sentinel is still set; receipt/UNKNOWN checks remain on their old path.
    e.spool.invalidate_source_history()
    assert e.bridge.maintenance_blocks_dispatch()


@pytest.mark.asyncio
async def test_unknown_accepted_tail_does_not_revoke_restored_prefix_reads(accepted):
    from regional_knowledge.kaggle_service_startup import TargetServiceStartup
    e=accepted;s=e.s
    binding=SimpleNamespace(database_generation='synthetic-db',run_id=s.run,runtime_incarnation='incarnation-a')
    current=lambda candidate:candidate is binding
    control={'sequence':0,'heads':[]}
    base=await e.spool.dataset_cursor(binding,control=control,current=current)
    startup=SimpleNamespace(running=True,closed=False,snapshot=None,coverage=base,
        binding=binding,binding_current=current)
    assert TargetServiceStartup.reads_current(startup)
    assert base.applied_sequence==0 and base.accepted_sequence==1
    await e.spool.mark_apply_sent(e.command.operation_key)
    unknown=await e.spool.dataset_cursor(binding,control=control,current=current)
    startup.coverage=unknown
    scope=await e.spool.scope_state()
    assert scope['unknown'] and scope['pending']  # Canonical native intake remains fenced.
    assert unknown.applied_sequence==0 and unknown.pending
    assert TargetServiceStartup.reads_current(startup)  # Readiness is the Saved prefix, not tail.


@pytest.mark.asyncio
async def test_old_apply_receipt_replays_on_ordinary_queue_after_prefix_ready(accepted):
    """Preserve old receipts, resume without a live client and bind new writer."""
    e=accepted;s=e.s
    old,work,frame=await dispatch(e)
    await e.bridge.receive(response(e,work,frame),run_id=s.run,token=s.token,generation=s.generation)
    await e.bridge.reconcile(run_id=s.run,token=s.token,generation=s.generation)
    await release(e,work)
    original=await e.spool.get(e.command.operation_key)
    with s.q.connect() as db:
        # Only the synthetic fixture changes execution identity. The canonical
        # receipt/command and the old delivery itself remain retained.
        # A lost previous claim may remain permanently unresolved; it cannot
        # gate new PG work after the successor has completed the same command.
        db.execute("UPDATE jobs SET run_id='previous-owned-run',state='claimed' WHERE id=?",(old,))
        retained=dict(db.execute('SELECT * FROM jobs WHERE id=?',(old,)).fetchone())
        # Reuse the persisted recovery owner contract, rather than a synthetic
        # flag or an implicit assumption that all local receipts were restored.
        db.execute('''CREATE TABLE IF NOT EXISTS rkb_writer_restore_control(
            id INTEGER PRIMARY KEY CHECK(id=1),plan_json TEXT NOT NULL,
            identity_json TEXT NOT NULL,head_json TEXT NOT NULL,state TEXT NOT NULL)''')
        db.execute('INSERT INTO rkb_writer_restore_control VALUES(1,?,?,?,?)',
            ('{}',json.dumps([s.run,'synthetic-nonce','incarnation-a']),'{}','active'))
    binding=SimpleNamespace(database_generation='synthetic-db',run_id=s.run,runtime_incarnation='incarnation-a')
    current=lambda value:value is binding
    cursor=await e.spool.dataset_cursor(binding,control={'sequence':0,'heads':[]},current=current)
    assert cursor.applied_sequence==0 and cursor.accepted_sequence==1 and len(cursor.pending)==1
    assert (await e.spool.get(e.command.operation_key)).apply_receipt==original.apply_receipt
    e.bridge.binding_current=lambda **v:(v['run_id'],v['writer_generation'])==(s.run,'writer-2')
    s.a._commands.clear()  # API restart has no caller authority context in RAM.
    new=await e.bridge.resume_pending(cursor,writer_generation='writer-2')
    assert new!=old
    assert await e.bridge.resume_pending(cursor,writer_generation='writer-2')==new
    following=await s.a.next_channel_work(s.run,s.token,s.generation)
    assert following.document_payload['id']==new
    assert following.document_payload['writer_generation']=='writer-2'
    assert base64.b64decode(following.document_payload['command_b64'])==e.command.encode()
    s.a.mark_work_sent(following)
    # A retained old apply must never settle this new delivery before PG replies.
    assert await e.bridge.poll_settlement(run_id=s.run,token=s.token,generation=s.generation,job_id=new) is None
    assert await e.bridge.reconcile(run_id=s.run,token=s.token,generation=s.generation,job_id=new) is None
    assert s.q.result(s.auth.p.owner.subject,new)['state']=='claimed'
    request=dict(type='DISPATCH',protocol_version=2,connection_generation=s.generation,
        **dict(following.document_payload),compute_claim=following.compute_claim,
        dispatch_generation=following.dispatch_generation)
    await e.bridge.receive(response(e,following,request),run_id=s.run,token=s.token,generation=s.generation)
    await e.bridge.reconcile(run_id=s.run,token=s.token,generation=s.generation)
    await release(e,following)
    advanced=await e.spool.dataset_cursor(binding,control={'sequence':0,'heads':[]},current=current)
    assert advanced.applied_sequence==1 and advanced.pending==()
    assert not e.bridge.maintenance_blocks_dispatch()  # Old claimed job is not this writer.
    with s.q.connect(write=False) as db:
        assert dict(db.execute('SELECT * FROM jobs WHERE id=?',(old,)).fetchone())==retained
    assert (await e.spool.get(e.command.operation_key)).apply_receipt==original.apply_receipt


@pytest.mark.asyncio
async def test_recovered_native_outcome_waits_for_dataset_without_redispatch(accepted):
    e=accepted;s=e.s
    job=await e.bridge.enqueue(s.auth.p.admin,e.command.operation_key,writer_generation='writer-1')
    await e.spool.record_apply(e.command.operation_key,native_receipt(e.command,e.handler))
    assert e.bridge.maintenance_blocks_dispatch()
    assert await s.a.next_channel_work(s.run,s.token,s.generation) is None
    with s.q.connect(write=False) as db:
        assert db.execute('SELECT state FROM jobs WHERE id=?',(job,)).fetchone()[0]=='pending'
    entry=await e.spool.get(e.command.operation_key)
    remote=await e.journal.record_outcome(e.seal,command=e.command,database_generation='synthetic-db',
        outcome=entry.apply_receipt.outcome,writer_generation='writer-1')
    await e.spool._write(e.spool._durable,e.command.operation_key,remote)
    assert e.bridge.maintenance_blocks_dispatch()  # job settlement follows durable publication
    await e.spool._write(e.spool._settle_recovered_jobs,e.command.operation_key)
    assert not e.bridge.maintenance_blocks_dispatch()
    assert await s.a.next_channel_work(s.run,s.token,s.generation) is None
    with s.q.connect(write=False) as db:
        assert db.execute('SELECT state FROM jobs WHERE id=?',(job,)).fetchone()[0]=='done'


@pytest.mark.asyncio
async def test_history_fence_rechecked_after_source_authorization_await(accepted,monkeypatch):
    e=accepted;s=e.s
    job=await e.bridge.enqueue(s.auth.p.admin,e.command.operation_key,writer_generation='writer-1')
    original=e.bridge.authorize_job
    async def authorize(context,row):
        actor=await original(context,row)
        e.spool._native_history_unsettled=True
        return actor
    monkeypatch.setattr(e.bridge,'authorize_job',authorize)
    assert await s.a.next_channel_work(s.run,s.token,s.generation) is None
    assert s.a.status()['state']=='idle' and s.a.status()['resource'] is None
    with s.q.connect(write=False) as db:
        assert db.execute('SELECT state FROM jobs WHERE id=?',(job,)).fetchone()[0]=='pending'
    e.spool._native_history_unsettled=False
    monkeypatch.setattr(e.bridge,'authorize_job',original)
    work=await s.a.next_channel_work(s.run,s.token,s.generation)
    assert work.document_payload['id']==job
    s.a.discard_unsent(work)


@pytest.mark.asyncio
async def test_history_cleanup_phase_keeps_same_slot_while_source_is_fenced(accepted):
    from regional_knowledge.kaggle_recovery_dispatch import RecoveryPhaseDispatch
    from test_kaggle_recovery_phases import work_for,response as phase_response
    e=accepted;s=e.s
    job=await e.bridge.enqueue(s.auth.p.admin,e.command.operation_key,writer_generation='writer-1')
    e.spool._native_history_unsettled=True
    maintenance=RecoveryPhaseDispatch(s.q,database=SimpleNamespace(database_generation='synthetic-db',writer_generation='writer-1'),
        identity=(s.run,s.nonce,'incarnation-a'),binding_current=lambda b:b is s,timeout_seconds=2)
    await e.spool._lock.acquire()
    try:
        waiting,work,frame=await work_for(s,maintenance,phase='source_history',
            args={'action':'abort','session':'exact-retained-session'},key='history-abort')
        assert frame['kind']=='recovery_phase' and frame['phase']=='source_history'
        assert work.resource.resource_kind=='document'
        with s.q.connect(write=False) as db:
            assert db.execute('SELECT state FROM jobs WHERE id=?',(job,)).fetchone()[0]=='pending'
        result=phase_response(s,frame,{'aborted':True})
        ack=await maintenance.receive(result,run_id=s.run,token=s.token,generation=s.generation)
        assert ack['status']=='accepted' and await waiting=={'aborted':True}
        await release(e,work)
        assert await s.a.next_channel_work(s.run,s.token,s.generation) is None
    finally:
        e.spool._lock.release();e.spool._native_history_unsettled=False
        await maintenance.aclose()
    work=await s.a.next_channel_work(s.run,s.token,s.generation)
    assert work.document_payload['id']==job
    s.a.discard_unsent(work)


def test_native_apply_diagnostic_keeps_sqlstate_and_excludes_private_message(caplog):
    import logging,time
    import psycopg
    from regional_knowledge.kaggle_apply import _log_pg_failure
    from test_kaggle_apply import item
    secret='synthetic-private-query https://private.invalid/ticket?token=forbidden'
    error=psycopg.errors.LockNotAvailable(secret)
    command=replace(item(),payload=canonical({'body':secret}))
    with caplog.at_level(logging.WARNING,logger='regional_knowledge.kaggle_apply'):
        _log_pg_failure(error,command=command,stage='writer_state',started=time.monotonic())
    assert 'sqlstate=55P03' in caplog.text and 'stage=writer_state' in caplog.text
    assert secret not in caplog.text and 'private.invalid' not in caplog.text
    assert digest(command.encode()) in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize('phase',['applied','rejected_precondition','unknown','fresh','identity_mismatch'])
async def test_verified_recovered_terminal_source_job_checkpoint_pending_unblocks_next_accepted(
        accepted,phase,caplog):
    """Real auth/queue/spool/journal: old attempted job may complete without false Saved.

    No provider, PG or native effect is invoked. Subsequent remote publication
    uses the existing outcome/settlement contract independently.
    """
    from regional_knowledge.kaggle_service_startup import TargetServiceStartup
    from test_source_import import resolved

    e=accepted;s=e.s
    first,work,frame=await dispatch(e)
    await release(e,work)
    with s.q.connect() as db:
        # Exercise the real queue lease-expiry transition, never a manual live reset.
        db.execute('UPDATE jobs SET lease_until=? WHERE id=?',(s.q.clock()-1,first))
        s.q._expire(db,s.q.clock())
    assert s.q.result(s.auth.p.owner.subject,first)['state']=='pending'

    if phase!='unknown':
        native=native_receipt(e.command,e.handler)
        if phase=='rejected_precondition':
            body=json.loads(native.outcome)
            body['status']='rejected_precondition'
            body['result_b64']=''
            body['result_sha256']=digest(b'')
            packed=canonical(body)
            native=replace(native,status='rejected_precondition',
                outcome=packed,outcome_sha256=digest(packed))
        await e.spool.record_apply(e.command.operation_key,native)
    else:
        await e.spool.mark_apply_sent(e.command.operation_key)
    if phase=='fresh':
        with s.q.connect() as db:
            db.execute('UPDATE jobs SET attempt=0 WHERE id=?',(first,))
    if phase=='identity_mismatch':
        with s.q.connect() as db:
            db.execute("UPDATE jobs SET identity=json_set(identity,'$.command_sha256',?) WHERE id=?",
                ('f'*64,first))

    successor=resolved(person_id=e.command.person_id,
        source_id='00000000-0000-0000-0000-000000000005',
        sequence=e.command.sequence+1,operation_key='fixture-recovered-following-source',
        request_id='fixture-recovered-following-request')
    await e.spool.stage_source_import(s.auth.p.admin,successor,database_generation='synthetic-db')
    await e.spool.seal(s.auth.p.admin,successor.operation_key,
        predecessor=await e.journal.head(),writer_generation='writer-1')
    second=await e.bridge.enqueue(s.auth.p.admin,successor.operation_key,writer_generation='writer-1')
    assert s.q.result(s.auth.p.owner.subject,second)['state']=='pending'
    binding=SimpleNamespace(database_generation='synthetic-db',run_id=s.run)
    # A verified physical prefix, not an old local receipt, proves PG presence.
    cursor=await e.spool.dataset_cursor(binding,control={
        'sequence':e.command.sequence if phase!='unknown' else 0,'heads':[]},
        current=lambda value:value is binding)
    assert cursor.accepted_sequence==successor.sequence
    assert cursor.applied_sequence==(e.command.sequence if phase!='unknown' else 0)
    assert cursor.accepted_sequence>cursor.applied_sequence

    startup=SimpleNamespace(config=SimpleNamespace(queue=s.q,spool=e.spool))
    import logging
    with caplog.at_level(logging.INFO,logger='regional_knowledge.kaggle_service_startup'):
        settled=await TargetServiceStartup._settle_restored_source_jobs(startup,cursor)
    observed=[record.getMessage() for record in caplog.records
        if record.name=='regional_knowledge.kaggle_service_startup'
        and record.message.startswith('source_recovered_queue_settled')]
    assert len(observed)==int(phase in ('applied','rejected_precondition'))
    if observed:
        assert 'remote_outcome_durable=false' in observed[0] and 'count=1' in observed[0]
        assert 'operation_key' not in observed[0] and 'command_b64' not in observed[0]
    if phase in ('applied','rejected_precondition'):
        assert settled==(first,)
        old=s.q.result(s.auth.p.owner.subject,first)
        assert old['state']=='done'
        assert old['result']==dict(command_sha256=digest(e.command.encode()),
            remote_outcome_durable=False)
        assert (await e.spool.get(e.command.operation_key)).outcome_state=='NOT_SENT'
        assert not e.bridge.maintenance_blocks_dispatch()
        next_work=await s.a.next_channel_work(s.run,s.token,s.generation)
        assert next_work is not None and next_work.document_payload['id']==second
        assert next_work.document_payload['command_sha256']==digest(successor.encode())
        assert next_work.document_payload['claim']!=frame['claim']
        s.a.discard_unsent(next_work)
        # Only the real existing remote outcome path can promote NOT_SENT.
        await e.spool.publish_outcome(e.command.operation_key,writer_generation='writer-1')
        assert e.spool._settle_recovered_jobs(e.command.operation_key)==[first]
        durable=s.q.result(s.auth.p.owner.subject,first)
        assert durable['result']['remote_outcome_durable'] is True
        assert (await e.spool.get(e.command.operation_key)).outcome_state=='DURABLE'
    else:
        assert settled==()
        assert s.q.result(s.auth.p.owner.subject,first)['state']=='pending'
        assert (await e.spool.get(e.command.operation_key)).remote_outcome is None
        assert s.q.result(s.auth.p.owner.subject,second)['state']=='pending'
