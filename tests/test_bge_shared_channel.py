"""Actual ASGI shared channel with synthetic OAuth/queue; no provider/model/PG."""
import json
import asyncio
import threading
import time
from types import SimpleNamespace
from uuid import UUID

import pytest
from starlette.applications import Starlette
from starlette.testclient import TestClient
from starlette.websockets import WebSocket, WebSocketDisconnect

from regional_knowledge.bge_channel import channel_route
from regional_knowledge.bge_contract import REVISION, SPACE
from regional_knowledge.bge_queue import BgeQueue
from regional_knowledge.read_telemetry import ReadTelemetry
from regional_knowledge.kaggle_protocol import Query, TrustedActorContext
from test_participants import people
from test_read_authority import auth, issue


@pytest.fixture
def peer(auth, tmp_path):
    queue = BgeQueue(tmp_path / 'channel.sqlite')
    document = queue.enqueue(auth.p.owner.subject, 'synthetic-document', ['synthetic passage'], kind='document')
    with queue.connect(write=False) as db:
        run = db.execute('SELECT id FROM runs').fetchone()[0]
    token = (queue.path.parent / 'bge-worker-credentials' / run).read_text()
    nonce = queue.prepare_channel(run)
    queue.launch_claim(run)
    queue.launch_record(run, 'fixture-owner/stable-slug', provider_version=7)
    admission = queue.configure_shared_reads(auth.resolver)
    records=[];telemetry=ReadTelemetry(sink=lambda value:records.append(json.loads(value)))
    with TestClient(Starlette(routes=[channel_route(queue,read_telemetry=telemetry)])) as client:
        async def resolve():
            return await auth.resolver.resolve_oauth(auth.tokens.access_token, destination=auth.destination)
        context = client.portal.call(resolve)
        state = SimpleNamespace(q=queue, a=admission, auth=auth, client=client,
            run=run, token=token, nonce=nonce, document=document, context=context,telemetry=telemetry,records=records)
        try:
            yield state
        finally:
            client.portal.call(admission.close)


def send(socket, generation, kind, **payload):
    socket.send_json(dict(type=kind, protocol_version=2, connection_generation=generation, **payload))


def attach(p, socket):
    socket.send_json(dict(type='ATTACH', protocol_version=2, run_id=p.run,
        launch_nonce=p.nonce, runtime_incarnation='synthetic-incarnation'))
    welcome = socket.receive_json()
    assert welcome['type'] == 'WELCOME'
    return welcome['connection_generation']


def ready(socket, generation, *, probe_nonce=None):
    state = dict(state='idle')
    if probe_nonce is not None:
        state['probe_nonce'] = probe_nonce
    send(socket, generation, 'RESOURCE_STATE', **state)
    send(socket, generation, 'READY', space=SPACE, model_revision=REVISION)
    ack = socket.receive_json()
    assert ack['type'] == 'READY_ACK' and ack['status'] == 'ready'


def connect(p):
    return p.client.websocket_connect('/bge-worker/channel', headers={'Authorization': 'Bearer ' + p.token})


def receive_expected(socket, kind):
    # Completion can retire the consumer before its compute release arrives;
    # its exact cancellation control may interleave with unrelated ACKs/work.
    for _ in range(8):
        frame = socket.receive_json()
        if frame['type'] == kind:
            return frame
        assert frame['type'] == 'CANCEL_QUERY'
        assert frame['reason'] == 'caller_cancelled'
        assert isinstance(frame['request_id'], str)
        assert type(frame['connection_generation']) is int
        assert type(frame['turn_generation']) is int
    raise AssertionError('unbounded cancellation controls')


def begin(p, generation, name, *, context=None):
    context = context or p.context

    async def start():
        query = Query(request_id=name, connection_generation=generation, turn_generation=17,
            query='synthetic regional query', deadline_ms=2000, min_data_seq=7,
            actor=TrustedActorContext(person_id=context.person_id, audience=context.destination.key,
                auth_epoch=context.auth_revision, visibility_epoch=0, context_ref=context.context_ref))
        return await p.a.begin_read(context, query, dialog_ref='dialog-' + name, deadline=time.monotonic() + 1.9)
    return p.client.portal.call(start)


def answer(frame, *, large=False):
    evidence = []
    if large:
        source = str(UUID(int=1))
        for n in range(3):
            evidence.append(dict(id=str(UUID(int=n + 20)), kind='source', text='\U0001f30d' * 8000,
                dependencies=dict(source_ids=[source], variant_refs=[], grant_refs=[],
                    source_revisions=[dict(source_id=source, content_revision=1, eligibility_revision=1)])))
    return dict(type='QUERY_RESULT', protocol_version=2,
        request_id=frame['request_id'], connection_generation=frame['connection_generation'],
        turn_generation=frame['turn_generation'], status='degraded', mode='hybrid', reason='bounded_retrieval',
        evidence=evidence, observed_data_seq=7, observed_projection_seq=7,
        observed_visibility_epoch=0, observed_database_generation='synthetic-db')


def release(socket, generation, frame):
    send(socket, generation, 'RESOURCE_RELEASED',
        resource_kind='read' if frame['type'] == 'QUERY' else 'document',
        resource_id=frame['request_id'] if frame['type'] == 'QUERY' else frame['id'],
        compute_claim=frame['compute_claim'],
        dispatch_generation=frame['connection_generation'] if frame['type'] == 'QUERY' else frame['dispatch_generation'])


def heartbeat(socket, generation):
    send(socket, generation, 'HEARTBEAT')
    ack = receive_expected(socket, 'HEARTBEAT_ACK')
    assert ack['type'] == 'HEARTBEAT_ACK'


def test_query_result_does_not_wait_for_slow_recovery_receipt(peer, monkeypatch):
    p = peer
    receiving = threading.Event()
    settled = threading.Event()
    finished = []
    finish_read = p.a.finish_read

    async def slow_receive(frame, **binding):
        receiving.set()
        await asyncio.sleep(3)
        settled.set()
        return dict(job_id=frame['job_id'], claim=frame['claim'], status='accepted')

    async def record_finish(*args, **kwargs):
        finished.append(time.monotonic())
        return await finish_read(*args, **kwargs)

    p.q._recovery_phases = SimpleNamespace(receive=slow_receive)
    monkeypatch.setattr(p.a, 'finish_read', record_finish)
    with connect(p) as socket:
        generation = attach(p, socket)
        ready(socket, generation)
        ticket = begin(p, generation, 'read-after-recovery-receipt')
        query = receive_expected(socket, 'QUERY')
        send(socket, generation, 'RECOVERY_PHASE_RESULT',
            job_id='retained-maintenance', claim='retained-claim', receipt={},
            run_id=p.run, launch_nonce=p.nonce, runtime_incarnation='synthetic-incarnation',
            compute_claim='retained-compute', dispatch_generation=generation)
        assert receiving.wait(.5)
        started = time.monotonic()
        socket.send_json(answer(query))

        async def delivered():
            return await asyncio.wait_for(asyncio.shield(ticket.result), .2)

        assert p.client.portal.call(delivered).mode == 'hybrid'
        assert finished and finished[0] - started < .2
        assert not settled.is_set()


def test_worker_resource_updates_do_not_wait_for_queue_writer(peer):
    """Actual worker busy/idle controls must not turn each QUERY into a write."""
    p = peer
    with connect(p) as socket:
        generation = attach(p, socket)
        ready(socket, generation)
        with p.q.connect() as writer:
            writer.execute('UPDATE runs SET useful=useful WHERE id=?', (p.run,))
            ticket = begin(p, generation, 'read-during-queue-write')
            frame = receive_expected(socket, 'QUERY')
            send(socket, generation, 'RESOURCE_STATE', state='busy', resource_kind='read',
                resource_id=frame['request_id'], compute_claim=frame['compute_claim'],
                dispatch_generation=generation)
            socket.send_json(answer(frame))

            async def delivered():
                return await asyncio.wait_for(asyncio.shield(ticket.result), .5)

            result = p.client.portal.call(delivered)
            assert result.mode == 'hybrid'
            release(socket, generation, frame)
            send(socket, generation, 'RESOURCE_STATE', state='idle')

            async def released():
                async with asyncio.timeout(.5):
                    while p.a.status()['state'] != 'idle':
                        await asyncio.sleep(.005)

            p.client.portal.call(released)
        heartbeat(socket, generation)


@pytest.mark.parametrize('large', [False, True])
def test_ready_typed_hybrid_delivery_does_not_release_compute(peer, large):
    p = peer
    with connect(p) as socket:
        generation = attach(p, socket)
        ready(socket, generation)
        first = begin(p, generation, 'first-read')
        frame = receive_expected(socket, 'QUERY')
        assert frame['type'] == 'QUERY' and frame['compute_claim']
        payload = json.dumps(answer(frame, large=large), ensure_ascii=False, separators=(',', ':'))
        if large:
            assert 65536 < len(payload.encode()) <= 256 * 1024
        socket.send_text(payload)

        async def delivered():
            return await first.result
        received = p.client.portal.call(delivered)
        assert received.mode == 'hybrid' and received.observed_data_seq == 7
        assert len(received.evidence) == (3 if large else 0)
        assert p.client.portal.call(p.a.status)['state'] == 'busy'
        second = begin(p, generation, 'second-read')
        following = receive_expected(socket, 'QUERY')
        assert not p.client.portal.call(second.result.done)
        assert len(p.client.portal.call(p.a.dispatched_reads))==2
        release(socket, generation, frame)
        assert following['type'] == 'QUERY' and following['request_id'] == 'second-read'
        p.client.portal.call(p.a.cancel_read, second)
        cancellation = socket.receive_json()
        assert cancellation['type'] == 'CANCEL_QUERY'
        assert (cancellation['request_id'], cancellation['connection_generation'], cancellation['turn_generation']) == (
            following['request_id'], generation, following['turn_generation'])
        release(socket, generation, following)
        heartbeat(socket, generation)
        assert p.client.portal.call(p.a.status)['state'] == 'idle'


def test_family_revocation_cancels_only_consumer_and_preserves_authenticated_channel(peer):
    p = peer

    async def owner_context():
        tokens = await issue(p.auth.provider)
        return await p.auth.resolver.resolve_oauth(tokens.access_token, destination=p.auth.destination)
    owner = p.client.portal.call(owner_context)
    with connect(p) as socket:
        generation = attach(p, socket)
        ready(socket, generation)
        first = begin(p, generation, 'revoked-consumer')
        frame = socket.receive_json()

        async def revoke():
            loaded = await p.auth.provider.load_access_token(p.auth.tokens.access_token)
            await p.auth.provider.revoke_token(loaded)
        p.client.portal.call(revoke)
        socket.send_json(answer(frame))
        cancellation = socket.receive_json()
        assert cancellation['type'] == 'CANCEL_QUERY'
        assert cancellation['request_id'] == 'revoked-consumer'
        assert p.client.portal.call(first.result.cancelled)
        assert p.client.portal.call(p.a.status)['state'] == 'busy'
        heartbeat(socket, generation)
        second = begin(p, generation, 'owner-next', context=owner)
        release(socket, generation, frame)
        following = receive_expected(socket, 'QUERY')
        assert following['type'] == 'QUERY' and following['actor']['person_id'] == owner.person_id
        socket.send_json(answer(following))

        async def delivered():
            return await second.result
        assert p.client.portal.call(delivered).mode == 'hybrid'
        release(socket, generation, following)
        heartbeat(socket, generation)


def test_reconnect_lost_before_receipt_requires_fresh_same_worker_probe(peer, monkeypatch):
    p = peer
    dropped, observed = [], threading.Event()
    original = WebSocket.send_text

    async def lose_first_query(socket, data):
        frame = json.loads(data)
        if frame['type'] == 'QUERY' and not dropped:
            dropped.append(frame)
            observed.set()
            raise WebSocketDisconnect()
        await original(socket, data)
    monkeypatch.setattr(WebSocket, 'send_text', lose_first_query)
    with connect(p) as first:
        old = attach(p, first)
        ready(first, old)
        ticket = begin(p, old, 'lost-before-worker')
        assert observed.wait(1)
    assert p.client.portal.call(p.a.status)['state'] == 'busy'
    with connect(p) as second:
        generation = attach(p, second)
        challenge = second.receive_json()
        assert challenge['type'] == 'RESOURCE_PROBE' and generation > old
        assert p.client.portal.call(ticket.result.cancelled)
        # A stale queued idle baseline cannot settle uncertain work.
        send(second, generation, 'RESOURCE_STATE', state='idle')
        heartbeat(second, generation)
        assert p.client.portal.call(p.a.status)['state'] == 'unknown'
        ready(second, generation, probe_nonce=challenge['probe_nonce'])
        assert p.client.portal.call(p.a.status)['state'] == 'idle'
        fresh = begin(p, generation, 'fresh-after-probe')
        frame = receive_expected(second, 'QUERY')
        assert frame['type'] == 'QUERY' and frame['request_id'] == fresh.request_id
        assert frame['compute_claim'] != dropped[0]['compute_claim']
        socket_result = answer(frame)
        second.send_json(socket_result)

        async def delivered():
            return await fresh.result
        assert p.client.portal.call(delivered).mode == 'hybrid'
        release(second, generation, frame)
        heartbeat(second, generation)


def test_source_settlement_poller_releases_result_debt_after_queueing_ack(peer):
    p=peer;settlements=[]
    async def poll(**_):return settlements.pop(0) if settlements else None
    p.q._source_commands=SimpleNamespace(poll_settlement=poll)
    p.client.portal.call(p.a.register_document_actor,p.auth.p.admin)
    with connect(p) as socket:
        generation=attach(p,socket);ready(socket,generation)
        document=receive_expected(socket,'DISPATCH')
        release(socket,generation,document)
        heartbeat(socket,generation)
        assert p.client.portal.call(p.a.status)['document_result_pending']
        outcome=p.q.complete(p.run,p.token,document['id'],document['claim'],SPACE,
            [[1.]+[0.]*1023],{},connection_generation=generation)
        settlements.append(dict(job_id=document['id'],claim=document['claim'],status=outcome))
        ack=receive_expected(socket,'RESULT_ACK')
        assert ack['job_id']==document['id']
        assert not p.client.portal.call(p.a.status)['document_result_pending']


def test_committed_receipt_cannot_dispatch_successor_before_result_ack(peer,monkeypatch):
    p=peer;committed=threading.Event();deliver=threading.Event();complete=p.q.complete
    def delayed_complete(*args,**kwargs):
        outcome=complete(*args,**kwargs)
        committed.set()
        assert deliver.wait(3)
        return outcome
    monkeypatch.setattr(p.q,'complete',delayed_complete)
    p.client.portal.call(p.a.register_document_actor,p.auth.p.admin)
    with connect(p) as socket:
        generation=attach(p,socket);ready(socket,generation)
        document=receive_expected(socket,'DISPATCH')
        release(socket,generation,document)
        second=p.q.enqueue(p.auth.p.owner.subject,'next-after-ack',['next passage'],kind='document')
        send(socket,generation,'RESULT',job_id=document['id'],claim=document['claim'],space=SPACE,
            vectors=[[1.]+[0.]*1023],timings={})
        try:
            assert committed.wait(1)
            assert p.client.portal.call(p.q.next_channel_work,p.run,p.token,generation) is None
        finally:deliver.set()
        ack=socket.receive_json()
        assert ack['type']=='RESULT_ACK' and ack['job_id']==document['id']
        successor=receive_expected(socket,'DISPATCH')
        assert successor['id']==second


def test_retained_document_release_precedes_probe_idle_and_preserves_result_debt(peer):
    p = peer
    p.client.portal.call(p.a.register_document_actor, p.auth.p.admin)
    with connect(p) as first:
        old = attach(p, first)
        ready(first, old)
        document = first.receive_json()
        assert document['type'] == 'DISPATCH' and document['id'] == p.document
        assert document['compute_claim'] and document['dispatch_generation'] == old
        # Native work finished, but its release never reached this attachment.
        # The worker will retain/replay that exact descriptor on reconnect.
        heartbeat(first, old)
        assert p.client.portal.call(p.a.status)['state'] == 'busy'
    with connect(p) as second:
        generation = attach(p, second)
        challenge = second.receive_json()
        assert challenge['type'] == 'RESOURCE_PROBE'
        # Same worker retained the release but has not received durable RESULT_ACK.
        release(second, generation, document)
        # The release changes the owned slot. A response to the superseded
        # challenge cannot establish idle; reconcile the new ordered challenge.
        send(second, generation, 'RESOURCE_STATE', state='idle', probe_nonce=challenge['probe_nonce'])
        fresh_challenge = second.receive_json()
        assert fresh_challenge['type'] == 'RESOURCE_PROBE'
        assert fresh_challenge['probe_nonce'] != challenge['probe_nonce']
        ready(second, generation, probe_nonce=fresh_challenge['probe_nonce'])
        assert p.client.portal.call(p.a.status)['document_result_pending']
        ticket = begin(p, generation, 'read-while-document-ack-pending')
        frame = receive_expected(second, 'QUERY')
        assert frame['type'] == 'QUERY', 'released document must not be encoded twice before result replay'
        second.send_json(answer(frame))

        async def delivered():
            return await ticket.result
        assert p.client.portal.call(delivered).mode == 'hybrid'
        release(second, generation, frame)
        payload = dict(job_id=document['id'], claim=document['claim'], space=SPACE,
            vectors=[[1.] + [0.] * 1023], timings={'encoder_seconds': .01})
        send(second, generation, 'RESULT', **payload)
        ack = receive_expected(second, 'RESULT_ACK')
        assert ack['type'] == 'RESULT_ACK' and ack['status'] == 'accepted'
        assert not p.client.portal.call(p.a.status)['document_result_pending']
        send(second, generation, 'RESULT', **payload)
        duplicate = receive_expected(second, 'RESULT_ACK')
        assert duplicate['type'] == 'RESULT_ACK' and duplicate['status'] == 'duplicate'
        with p.q.connect(write=False) as db:
            assert db.execute('SELECT attempt FROM jobs WHERE id=?', (p.document,)).fetchone()[0] == 1


@pytest.mark.parametrize('bad',[False,True])
def test_native_diagnostics_use_same_result_and_nonblocking_sink_without_payload(peer,bad):
    from hashlib import sha256
    p=peer
    with connect(p) as socket:
        generation=attach(p,socket);ready(socket,generation)
        ticket=begin(p,generation,'native-timing')
        frame=receive_expected(socket,'QUERY')
        event=dict(event='kaggle_read_adapter_terminal',plane='adapter',terminal=True,
            request_ref=sha256(frame['request_id'].encode()).hexdigest(),run_id=p.run,
            runtime_incarnation='synthetic-incarnation',connection_generation=generation,turn_generation=17,
            diagnostic_state='observed',executed_branches=['lexical','dense'],stages_ms={'encode_ms':5.},
            timing_semantics='overlapping_do_not_sum',status='degraded',reason='bounded_retrieval',loss_before={})
        if bad:event['private_query']='NEVER_LOG_PRIVATE_PAYLOAD'
        socket.send_json(dict(answer(frame),read_events=[event]))
        async def observed():return await ticket.result
        assert p.client.portal.call(observed).status=='degraded'
        p.telemetry._queue.join()
        assert p.telemetry.failed==int(bad)
        native_records=[r for r in p.records if r['event']=='kaggle_read_adapter_terminal']
        assert len(native_records)==(0 if bad else 1)
        assert 'NEVER_LOG' not in json.dumps(p.records)
        if not bad:assert native_records[0]['stages_ms']=={'encode_ms':5.}
        release(socket,generation,frame)

def test_completed_read_precedes_release_while_admission_lock_is_held(peer, monkeypatch):
    """Real channel plus synthetic native worker: a queued release cannot block its answer."""
    from contextlib import asynccontextmanager
    from test_bge_wss_worker import module

    p = peer
    transport = module('bge_wss_transport')
    finish_times = []
    original_finish = p.a.finish_read

    async def record_finish(*args, **kwargs):
        finish_times.append(time.monotonic())
        return await original_finish(*args, **kwargs)

    monkeypatch.setattr(p.a, 'finish_read', record_finish)

    async def exercise():
        block_heartbeat = asyncio.Event()
        heartbeat_waiting = asyncio.Event()
        resume_writer = asyncio.Event()
        handler_finished = asyncio.Event()
        ready_received = asyncio.Event()
        admission_held = threading.Event()
        stop = asyncio.Event()

        async def hold_admission():
            async with p.a._lock:
                admission_held.set()
                await asyncio.sleep(1.5)

        @asynccontextmanager
        async def connector():
            with connect(p) as websocket:
                class Socket:
                    async def send(self, raw):
                        frame = json.loads(raw)
                        if (frame['type'] == 'HEARTBEAT' and block_heartbeat.is_set()
                                and not heartbeat_waiting.is_set()):
                            heartbeat_waiting.set()
                            await resume_writer.wait()
                        await asyncio.to_thread(websocket.send_text, raw)

                    async def receive(self):
                        raw = await asyncio.to_thread(websocket.receive_text)
                        frame = json.loads(raw)
                        if frame['type'] == 'READY_ACK' and frame['status'] == 'ready':
                            ready_received.set()
                        return raw

                yield Socket()

        async def query_handler(frame, *, encode):
            await encode(frame['query'])
            block_heartbeat.set()
            await asyncio.wait_for(heartbeat_waiting.wait(), 1)
            p.client.portal.start_task_soon(hold_admission)
            assert await asyncio.to_thread(admission_held.wait, 1)
            handler_finished.set()
            return answer(frame)

        worker = transport.BgeWssTransport(
            dict(run_id=p.run, token=p.token, launch_nonce=p.nonce,
                 channel='wss://synthetic.invalid/bge-worker/channel', shared_reads=True),
            lambda: lambda _: [1.] + [0.] * 1023, lambda: {},
            query_handler=query_handler, connector=connector,
            heartbeat_seconds=.4, maximum_seconds=6, stop=stop)
        task = asyncio.create_task(worker.run())
        try:
            await asyncio.wait_for(ready_received.wait(), 2)
            ticket = await asyncio.to_thread(begin, p, worker._connection_generation, 'queued-read-first')
            await asyncio.wait_for(handler_finished.wait(), 1.5)
            # Let states() enqueue QUERY_RESULT, RESOURCE_RELEASED and RESOURCE_STATE
            # in one iteration while writer is held on an earlier heartbeat.
            async with asyncio.timeout(.4):
                while not any(r['task'].done() for r in worker.reads.values()) or not worker.released:
                    await asyncio.sleep(.005)
            await asyncio.sleep(.02)
            started = time.monotonic()
            resume_writer.set()

            async def delivered():
                return await asyncio.wait_for(asyncio.shield(ticket.result), .6)

            response = await asyncio.to_thread(p.client.portal.call, delivered)
            assert response.mode == 'hybrid'
            assert finish_times and finish_times[0] - started < .2
        finally:
            resume_writer.set()
            stop.set()
            await asyncio.wait_for(task, 3)

    asyncio.run(exercise())


@pytest.mark.parametrize('broken_observer',[False,True])
def test_channel_boundaries_separate_socket_and_delivery_without_gating_result(peer,monkeypatch,broken_observer):
    from hashlib import sha256
    p=peer
    original=p.a.finish_read
    async def slow_finish(*args,**kwargs):
        await asyncio.sleep(.03)
        return await original(*args,**kwargs)
    monkeypatch.setattr(p.a,'finish_read',slow_finish)
    if broken_observer:
        def broken(*args,**kwargs):raise OSError('PRIVATE_OBSERVATION_FAILURE')
        monkeypatch.setattr(p.telemetry,'channel_timing',broken)
    with connect(p) as socket:
        generation=attach(p,socket);ready(socket,generation)
        ticket=begin(p,generation,'channel-boundary')
        frame=receive_expected(socket,'QUERY')
        socket.send_json(answer(frame))
        async def delivered():return await asyncio.wait_for(ticket.result,.5)
        assert p.client.portal.call(delivered).status=='degraded'
        p.telemetry._queue.join()
        observed=[x for x in p.records if x['event']=='read_channel_boundary']
        assert 'PRIVATE_OBSERVATION_FAILURE' not in json.dumps(p.records)
        assert 'synthetic regional query' not in json.dumps(p.records)
        if broken_observer:
            assert observed==[]
        else:
            assert {x['phase'] for x in observed}=={'write','receive'}
            assert all(x['request_ref']==sha256(frame['request_id'].encode()).hexdigest() for x in observed)
            write=next(x for x in observed if x['phase']=='write')
            receive=next(x for x in observed if x['phase']=='receive')
            assert write['monotonic_start_ns']<=write['monotonic_end_ns']
            assert write['monotonic_start_ns']<=receive['monotonic_start_ns']
            assert receive['elapsed_ms']>=receive['settle_ms']>=30
            assert receive['status_ms']>=0
        release(socket,generation,frame)


def test_eight_actual_channel_claims_cancel_second_without_freeing_capacity(peer):
    p=peer
    with connect(p) as socket:
        generation=attach(p,socket);ready(socket,generation)
        tickets=[begin(p,generation,f'parallel-{n}') for n in range(9)]
        frames=[receive_expected(socket,'QUERY') for _ in range(8)]
        assert {f['request_id'] for f in frames}=={f'parallel-{n}' for n in range(8)}
        p.client.portal.call(p.a.cancel_read,tickets[1])
        cancel=receive_expected(socket,'CANCEL_QUERY')
        assert cancel['request_id']=='parallel-1'
        heartbeat(socket,generation)
        assert len(p.client.portal.call(p.a.dispatched_reads))==8
        assert all(not p.client.portal.call(t.result.done) for n,t in enumerate(tickets) if n!=1)
        release(socket,generation,frames[1])
        ninth=receive_expected(socket,'QUERY')
        assert ninth['request_id']=='parallel-8'
        assert len(p.client.portal.call(p.a.dispatched_reads))==8
        for frame in frames[:1]+frames[2:]+[ninth]:release(socket,generation,frame)
        heartbeat(socket,generation)
        assert p.client.portal.call(p.a.status)['state']=='idle'


def test_unknown_aggregate_debt_blocks_stop_until_fresh_idle(peer,monkeypatch):
    p=peer;drain={'enabled':False}
    monkeypatch.setattr(p.q,'stop_eligible',lambda *args:drain['enabled'])
    with connect(p) as first:
        old=attach(p,first);ready(first,old)
        begin(p,old,'old-aggregate');frame=receive_expected(first,'QUERY')
    with connect(p) as second:
        generation=attach(p,second)
        assert second.receive_json()['type']=='RESOURCE_PROBE'
        release(second,generation,frame)
        challenge=second.receive_json()
        assert challenge['type']=='RESOURCE_PROBE'
        state=p.client.portal.call(p.a.status)
        assert state['state']=='unknown' and state['resource'] is None
        drain['enabled']=True
        heartbeat(second,generation) # No STOP while other native work is unobserved.
        ready(second,generation,probe_nonce=challenge['probe_nonce'])
        stop=second.receive_json()
        assert stop['type']=='STOP' and stop['connection_generation']==generation


def test_native_scheduled_rotation_serial_handoff_keeps_reads_and_wakes_without_request(auth,tmp_path):
    """Real ASGI + shared admission; synthetic provider terminal, no PG or BGE."""
    from regional_knowledge.bge_queue import ROTATE_SECONDS, HEARTBEAT_SECONDS
    now=[1000.]
    q=BgeQueue(tmp_path/'serial-rotation.sqlite',clock=lambda:now[0])
    original=q.request_startup()['run_id']
    token=q.launch_claim(original,provider_ref='fixture-owner/stable-slug')
    nonce=q.prepare_channel(original)
    q.launch_record(original,'fixture-owner/stable-slug',provider_version=7)
    admission=q.configure_shared_reads(auth.resolver)
    checkpoints=[]
    async def checkpoint(run_id,generation):checkpoints.append((run_id,generation))
    with TestClient(Starlette(routes=[channel_route(q,before_stop=checkpoint)])) as client:
        async def resolve():
            return await auth.resolver.resolve_oauth(auth.tokens.access_token,destination=auth.destination)
        context=client.portal.call(resolve)
        p=SimpleNamespace(q=q,a=admission,client=client,run=original,
                          token=token,nonce=nonce,context=context)
        try:
            with connect(p) as socket:
                generation=attach(p,socket);ready(socket,generation)
                assert q.finish_startup(original)
                ticket=begin(p,generation,'current-still-reads')
                frame=receive_expected(socket,'QUERY')
                socket.send_json(answer(frame))
                async def delivered():return await ticket.result
                assert client.portal.call(delivered).status=='degraded'
                release(socket,generation,frame)
                async def idle():
                    async with asyncio.timeout(1):
                        while admission.status()['state']!='idle':
                            await asyncio.sleep(.01)
                client.portal.call(idle)
                with q.connect() as db:
                    db.execute('UPDATE runs SET started=? WHERE id=?',
                               (now[0]-ROTATE_SECONDS-1,original))
                q.maintenance()  # Existing controller tick, not a public request.
                with q.connect(write=False) as db:
                    control=db.execute('SELECT current_run,successor,startup_state FROM control').fetchone()
                    assert tuple(control)==(original,None,'pending')
                    assert db.execute('SELECT status FROM runs WHERE id=?',(original,)).fetchone()[0]=='draining'
                assert q.stop_eligible(original,token,generation)
                assert socket.receive_json()['type']=='STOP'
                assert checkpoints==[(original,generation)]
            # Synthetic native terminal observed after checkpoint/STOP. The
            # production controller requires sealed exact-session evidence.
            with q.connect() as db:
                db.execute("UPDATE runs SET launch_state='terminal',channel_active=0 WHERE id=?",(original,))
            now[0]+=HEARTBEAT_SECONDS+1
            q.maintenance()
            with q.connect(write=False) as db:
                next_run=db.execute('SELECT current_run FROM control').fetchone()[0]
                assert next_run not in (None,original)
                assert db.execute('SELECT successor FROM control').fetchone()[0] is None
            new_token=q.launch_claim(next_run,provider_ref='fixture-owner/stable-slug')
            new_nonce=q.prepare_channel(next_run)
            q.launch_record(next_run,'fixture-owner/stable-slug',provider_version=8)
            p.run,p.token,p.nonce=next_run,new_token,new_nonce
            with connect(p) as successor:
                new_generation=attach(p,successor)
                ready(successor,new_generation)
                assert q.finish_startup(next_run)
                ticket=begin(p,new_generation,'successor-reads')
                frame=receive_expected(successor,'QUERY')
                successor.send_json(answer(frame))
                assert client.portal.call(delivered).status=='degraded'
                release(successor,new_generation,frame)
        finally:
            client.portal.call(admission.close)
