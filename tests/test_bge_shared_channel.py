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
        # This ordered ACK proves no second QUERY was dispatched before release.
        heartbeat(socket, generation)
        assert not p.client.portal.call(second.result.done)
        release(socket, generation, frame)
        following = receive_expected(socket, 'QUERY')
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
        assert p.client.portal.call(p.a.status)['state'] == 'busy'
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
        assert len(p.records)==(0 if bad else 1)
        assert 'NEVER_LOG' not in json.dumps(p.records)
        if not bad:assert p.records[0]['stages_ms']=={'encode_ms':5.}
        release(socket,generation,frame)
