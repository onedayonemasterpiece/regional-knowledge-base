"""Fair native vectors on existing admission; synthetic PG/model and real wire."""
import asyncio
import time
from types import SimpleNamespace

import pytest

from regional_knowledge.bge_queue import BgeQueue, StaleAttachment
from regional_knowledge.kaggle_bootstrap import PreparedRuntime
from test_bge_admission import state, auth, people, begin, answer, release
from test_bge_shared_worker import Peer, query, result
from test_bge_wss_worker import CONFIG, module
from test_kaggle_bootstrap import candidate
from test_native_source_worker_wiring import retained_encoder
from test_worker_job_boundary_20261023 import source_command


async def native_job(s):
    async def payload(job_id,**kwargs):
        with s.q.connect(write=False) as db:
            job=db.execute('select * from jobs where id=?',(job_id,)).fetchone()
        return dict(id=job_id,claim=job['claim'],kind='recovery_phase',phase='apply')
    s.q._recovery_phases=SimpleNamespace(eligible=lambda *args:True,dispatch_payload=payload)
    with s.q.connect() as db:
        db.execute("update jobs set kind='recovery_phase',actor=? where id=?",('maintenance:'+s.run,s.doc))
    work=await s.q.next_channel_work(s.run,s.token,s.generation)
    s.a.mark_work_sent(work)
    await release(s,work)
    return work


@pytest.mark.asyncio
async def test_pg_wait_and_vectors_leave_queries_fair_without_settling_native_job(state):
    s=state;original=await native_job(s);claim=original.document_payload['claim']
    ticket=await begin(s,'during-pg')
    read=await s.q.next_channel_work(s.run,s.token,1)
    assert read.query.request_id=='during-pg'
    s.a.mark_work_sent(read)
    assert await s.a.finish_read(s.run,s.token,1,answer(read))
    assert (await ticket.result).status=='ok'
    await release(s,read)
    await s.a.request_native_forward(s.run,s.token,1,job_id=s.doc,claim=claim,request_id='vector1')
    await begin(s,'before-vector')
    read=await s.q.next_channel_work(s.run,s.token,1)
    assert read.query.request_id=='before-vector'
    s.a.mark_work_sent(read);await s.a.finish_read(s.run,s.token,1,answer(read));await release(s,read)
    vector=await s.q.next_channel_work(s.run,s.token,1)
    assert vector.native_request==dict(job_id=s.doc,claim=claim,request_id='vector1')
    s.a.mark_work_sent(vector)
    assert await s.q.next_channel_work(s.run,s.token,1) is None
    await release(s,vector)
    assert not await s.a.request_native_forward(s.run,s.token,1,job_id=s.doc,claim=claim,request_id='vector1')
    # Later QUERY releases cannot erase the original durable receipt authority.
    await begin(s,'after-vector')
    read=await s.q.next_channel_work(s.run,s.token,1);s.a.mark_work_sent(read)
    await s.a.finish_read(s.run,s.token,1,answer(read));await release(s,read)
    r=original.resource
    s.a.require_durable_resource(s.run,r.runtime_incarnation,1,s.doc,r.compute_claim,r.dispatch_generation)
    with s.q.connect(write=False) as db:
        row=db.execute('select state,claim,attempt,result from jobs where id=?',(s.doc,)).fetchone()
    assert tuple(row)==('claimed',claim,1,None)
    with pytest.raises(StaleAttachment):
        await s.a.request_native_forward(s.run,s.token,1,job_id=s.doc,claim='wrong',request_id='vector2')
    with pytest.raises(ValueError,match='distinct'):
        await s.a.request_native_forward(s.run,s.token,1,job_id=s.doc,claim=claim,request_id=s.doc)


@pytest.mark.asyncio
async def test_query_expiry_does_not_release_a_running_native_vector(state):
    s=state;original=await native_job(s);claim=original.document_payload['claim']
    await s.a.request_native_forward(s.run,s.token,1,job_id=s.doc,claim=claim,request_id='held')
    work=await s.q.next_channel_work(s.run,s.token,1);s.a.mark_work_sent(work)
    from test_bge_admission import query as admitted_query
    ticket=await s.a.begin_read(s.context,admitted_query(s.context,'expires'),
        dialog_ref='expires',deadline=time.monotonic()+.02)
    await asyncio.sleep(.04)
    assert ticket.result.cancelled()
    assert await s.q.next_channel_work(s.run,s.token,1) is None
    assert s.a.status()['resource']==work.resource
    await release(s,work)
    assert s.a.status()['state']=='idle' and s.a.status()['document_result_pending']


@pytest.mark.asyncio
async def test_native_vectors_still_progress_under_continuous_queries(state):
    s=state;original=await native_job(s);claim=original.document_payload['claim']
    await s.a.request_native_forward(s.run,s.token,1,job_id=s.doc,claim=claim,request_id='bounded-progress')
    for n in range(s.a.max_read_streak):
        await begin(s,'read-'+str(n))
        read=await s.q.next_channel_work(s.run,s.token,1)
        assert read.query is not None
        s.a.mark_work_sent(read);await s.a.finish_read(s.run,s.token,1,answer(read));await release(s,read)
    await begin(s,'still-waiting')
    native=await s.q.next_channel_work(s.run,s.token,1)
    assert native.native_request['request_id']=='bounded-progress'


@pytest.mark.asyncio
async def test_unreceived_encode_grant_reuses_pending_request_after_exact_idle_probe(state):
    s=state;original=await native_job(s);claim=original.document_payload['claim']
    await s.a.request_native_forward(s.run,s.token,1,job_id=s.doc,claim=claim,request_id='lost-grant')
    old=await s.q.next_channel_work(s.run,s.token,1);s.a.mark_work_sent(old)
    nonce=await s.a.begin_resource_probe(s.run,s.token,1)
    await s.a.resource_state(s.run,s.token,1,runtime_incarnation='incarnation-a',state='idle',probe_nonce=nonce)
    retried=await s.q.next_channel_work(s.run,s.token,1)
    assert retried.native_request==old.native_request and retried.compute_claim!=old.compute_claim


@pytest.mark.asyncio
async def test_controller_restart_recovers_original_claim_and_actual_vector_debt(state):
    s=state;original=await native_job(s);claim=original.document_payload['claim']
    await s.a.request_native_forward(s.run,s.token,1,job_id=s.doc,claim=claim,request_id='in-flight')
    vector=await s.q.next_channel_work(s.run,s.token,1);s.a.mark_work_sent(vector)
    s.a.close()
    q=BgeQueue(s.q.path,clock=s.q.clock);q._recovery_phases=s.q._recovery_phases
    a=q.configure_shared_reads(s.auth.resolver)
    generation=q.attach_channel(s.run,s.token,s.nonce,'incarnation-a')['connection_generation']
    assert generation==2
    q.heartbeat(s.run,s.token,ready=True,connection_generation=generation)
    async def observed(resource,method):
        return await method(s.run,s.token,generation,runtime_incarnation=resource.runtime_incarnation,
            resource_kind=resource.resource_kind,resource_id=resource.resource_id,
            compute_claim=resource.compute_claim,dispatch_generation=resource.dispatch_generation)
    try:
        await observed(original.resource,a.resource_released)
        await observed(vector.resource,lambda *args,**kwargs:a.resource_state(*args,state='busy',**kwargs))
        assert a.status()['resource']==vector.resource
        assert await q.next_channel_work(s.run,s.token,generation) is None
        await observed(vector.resource,a.resource_released)
        assert not await a.request_native_forward(s.run,s.token,generation,
            job_id=s.doc,claim=claim,request_id='in-flight')
        r=original.resource
        a.require_durable_resource(s.run,r.runtime_incarnation,generation,s.doc,r.compute_claim,r.dispatch_generation)
        await a.request_native_forward(s.run,s.token,generation,
            job_id=s.doc,claim=claim,request_id='next-vector')
        next_work=await q.next_channel_work(s.run,s.token,generation)
        assert next_work.native_request['request_id']=='next-vector'
        with q.connect(write=False) as db:
            row=db.execute('select state,claim,attempt from jobs where id=?',(s.doc,)).fetchone()
        assert tuple(row)==('claimed',claim,1)
    finally:a.close()


async def worker_fixture(handler,peer,stop,encoder):
    async def read(frame,*,encode):
        await encode(frame['query'])
        return result(frame)
    async def prepare(actual,**identity):
        assert actual is encoder
        return PreparedRuntime(read,candidate(),source_handler=handler)
    transport=module('bge_wss_transport')
    return transport.BgeWssTransport(dict(CONFIG,shared_reads=True),lambda:encoder,lambda:{},
        prepare_runtime=prepare,connector=peer.connect,stop=stop,maximum_seconds=5,heartbeat_seconds=.02)


@pytest.mark.asyncio
async def test_bound_native_encoder_cannot_deadlock_the_control_event_loop():
    encoder,_,forwarded,_=retained_encoder()
    async def forward(text):raise AssertionError('not called on event loop')
    encoder.bind_source_admission(asyncio.get_running_loop(),forward)
    with pytest.raises(RuntimeError,match='worker thread'):encoder('must-not-block')
    assert not forwarded


async def grant(peer,request,generation=1):
    await peer.put(dict(type='ENCODE_GRANT',**{k:request[k]for k in ('job_id','claim','request_id')},
        compute_claim='vector-'+request['request_id'],dispatch_generation=generation),generation)


@pytest.mark.asyncio
async def test_real_transport_serves_queries_during_pg_and_between_native_vectors():
    peer=Peer();stop=asyncio.Event();pg=asyncio.Event();between=asyncio.Event();first=asyncio.Event()
    encoder,_,_,vector=retained_encoder();calls=[];active=[0];peak=[0];handlers=[]
    def encode(text):
        active[0]+=1;peak[0]=max(peak[0],active[0]);calls.append(text);active[0]-=1
        return vector
    object.__setattr__(encoder,'encode',encode)
    async def source(frame):
        handlers.append(frame['id'])
        await pg.wait()
        assert await asyncio.to_thread(encoder,'native1')==vector
        first.set();await between.wait()
        assert await asyncio.to_thread(encoder,'native2')==vector
        return {'native':'same-result'}
    worker=await worker_fixture(source,peer,stop,encoder);running=asyncio.create_task(worker.run())
    try:
        await peer.wait('READY');await peer.put(source_command(worker))
        await peer.wait('RESOURCE_RELEASED',predicate=lambda f:f['compute_claim']=='original-compute')
        await peer.put(query(request='pg',text='query-pg'))
        await peer.wait('QUERY_RESULT',predicate=lambda f:f['request_id']=='pg')
        await peer.wait('RESOURCE_RELEASED',predicate=lambda f:f['resource_id']=='pg')
        pg.set();request=await peer.wait('ENCODE_REQUEST')
        assert calls==['query-pg']  # A request alone never starts inference.
        await grant(peer,request);await asyncio.wait_for(first.wait(),1)
        await peer.wait('RESOURCE_RELEASED',predicate=lambda f:f['compute_claim']=='vector-'+request['request_id'])
        await peer.put(query(request='between',claim='between-claim',text='query-between'))
        await peer.wait('QUERY_RESULT',predicate=lambda f:f['request_id']=='between')
        await peer.wait('RESOURCE_RELEASED',predicate=lambda f:f['resource_id']=='between')
        between.set();request2=await peer.wait('ENCODE_REQUEST',predicate=lambda f:f['request_id']!=request['request_id'])
        await grant(peer,request2)
        receipt=await peer.wait('SOURCE_COMMAND_RESULT')
        assert calls==['query-pg','native1','query-between','native2'] and peak==[1]
        assert handlers==['unresolved-job'] and receipt['compute_claim']=='original-compute'
        assert receipt['receipt']=={'native':'same-result'}
        await peer.put(dict(type='RESULT_ACK',job_id=receipt['job_id'],claim=receipt['claim'],status='checkpoint_pending'))
    finally:
        pg.set();between.set();stop.set();await asyncio.wait_for(running,2)


@pytest.mark.asyncio
async def test_native_forward_request_survives_reconnect_without_reexecuting_handler(monkeypatch):
    peer=Peer();stop=asyncio.Event();encoder,_,forwarded,_=retained_encoder();handlers=[]
    async def source(frame):
        handlers.append(frame['id']);await asyncio.to_thread(encoder,'retained-native');return {'same':True}
    worker=await worker_fixture(source,peer,stop,encoder)
    monkeypatch.setattr(worker.run.__globals__['random'],'uniform',lambda *_:.001)
    running=asyncio.create_task(worker.run())
    try:
        await peer.wait('READY');await peer.put(source_command(worker))
        request=await peer.wait('ENCODE_REQUEST',generation=1)
        await peer.disconnect(1)
        replay=await peer.wait('ENCODE_REQUEST',generation=2)
        assert {k:v for k,v in replay.items()if k!='connection_generation'}=={
            k:v for k,v in request.items()if k!='connection_generation'}
        await grant(peer,replay,2)
        receipt=await peer.wait('SOURCE_COMMAND_RESULT',generation=2)
        assert handlers==['unresolved-job'] and forwarded==['retained-native']
        assert receipt['dispatch_generation']==1 and receipt['compute_claim']=='original-compute'
    finally:
        stop.set();await asyncio.wait_for(running,2)


@pytest.mark.asyncio
async def test_stop_while_native_waits_for_compute_drains_without_fabricated_receipt():
    peer=Peer();stop=asyncio.Event();encoder,_,forwarded,_=retained_encoder()
    async def source(frame):
        await asyncio.to_thread(encoder,'never-started');return {'not-reached':True}
    worker=await worker_fixture(source,peer,stop,encoder);running=asyncio.create_task(worker.run())
    try:
        await peer.wait('READY');await peer.put(source_command(worker));await peer.wait('ENCODE_REQUEST')
    finally:
        stop.set();await asyncio.wait_for(running,2)
    assert not forwarded and not peer.frames('SOURCE_COMMAND_RESULT')
