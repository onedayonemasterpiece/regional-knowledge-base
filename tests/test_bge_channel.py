"""Socket contract tests; real queue/attachment transaction tests live separately."""
import threading
import asyncio
from concurrent.futures import ThreadPoolExecutor

import pytest
from starlette.applications import Starlette
from starlette.testclient import TestClient
from starlette.websockets import WebSocket, WebSocketDisconnect

from regional_knowledge import bge_queue
from regional_knowledge.bge_channel import channel_route
from regional_knowledge.bge_contract import REVISION, SPACE


class StaleAttachment(PermissionError):
    pass


class Queue:
    def __init__(self, version='7'):
        self.version = version
        self.generation = 0
        self.incarnation = None
        self.conflict = False
        self.ready = False
        self.pending = None
        self.claimed = None
        self.completed = None
        self.completion = threading.Event()
        self.heartbeats = 0
        self.claim_calls = 0
        self.detached = []

    def attach_channel(self, run, token, nonce, incarnation):
        if run != 'run' or token != 'secret' or nonce != 'nonce':
            raise PermissionError()
        if self.incarnation is not None and incarnation != self.incarnation:
            self.conflict = True
        if self.conflict:
            raise StaleAttachment()
        self.incarnation = incarnation
        self.generation += 1
        return {'connection_generation': self.generation, 'provider_version': self.version}

    def channel_status(self, run, token, generation):
        if self.conflict or generation != self.generation:
            raise StaleAttachment()
        return {'state': 'ready' if self.ready else 'starting', 'ready': self.ready,
                'provider_version': self.version}

    def heartbeat(self, run, token, *, ready=False, diagnostics=None, connection_generation=None):
        self.channel_status(run, token, connection_generation)
        self.heartbeats += 1
        self.ready |= ready

    def claim(self, run, token, *, connection_generation=None):
        self.channel_status(run, token, connection_generation)
        self.claim_calls += 1
        if self.claimed or not self.pending:
            return None
        self.claimed, self.pending = self.pending, None
        return self.claimed

    def resume_channel_claim(self, run, token, generation):
        self.channel_status(run, token, generation)
        return self.claimed if self.completed is None else None

    def complete(self, run, token, job_id, claim, space, vectors, timings, *, connection_generation=None):
        self.channel_status(run, token, connection_generation)
        if self.completed:
            return 'duplicate'
        if claim != 'claim':
            raise bge_queue.StaleClaim()
        self.completed = (job_id, vectors)
        self.completion.set()
        return 'accepted'

    def detach_channel(self, run, token, generation):
        self.detached.append(generation)

    def enqueue(self):
        self.pending = {'id': 'job', 'claim': 'claim', 'kind': 'document',
                        'texts': ['authorized text'], 'space': SPACE}


@pytest.fixture
def queue(monkeypatch):
    monkeypatch.setattr(bge_queue, 'StaleAttachment', StaleAttachment, raising=False)
    return Queue()


def client(queue):
    return TestClient(Starlette(routes=[channel_route(queue)]))


def attach(socket, incarnation='instance'):
    socket.send_json({'type': 'ATTACH', 'protocol_version': 2, 'run_id': 'run',
                      'launch_nonce': 'nonce', 'runtime_incarnation': incarnation})
    return socket.receive_json()


def send(socket, generation, kind, **payload):
    socket.send_json({'type': kind, 'protocol_version': 2,
                      'connection_generation': generation, **payload})


def ready(socket, generation):
    send(socket, generation, 'READY', space=SPACE, model_revision=REVISION)
    return socket.receive_json()


def result(socket, generation, claim='claim'):
    send(socket, generation, 'RESULT', job_id='job', claim=claim, space=SPACE,
         vectors=[[1., *([0.] * 1023)]], timings={'encoder_seconds': .01})
    return socket.receive_json()


def test_socket_run_auth_and_no_public_query(queue):
    with client(queue) as app:
        with pytest.raises(WebSocketDisconnect) as denied:
            with app.websocket_connect('/bge-worker/channel'):
                pass
        assert denied.value.code == 4403
        with app.websocket_connect('/bge-worker/channel', headers={'Authorization': 'Bearer wrong'}) as socket:
            with pytest.raises(WebSocketDisconnect) as denied:
                attach(socket)
            assert denied.value.code == 4403
        with app.websocket_connect('/bge-worker/channel', headers={'Authorization': 'Bearer secret'}) as socket:
            generation = attach(socket)['connection_generation']
            send(socket, generation, 'QUERY', query='private read')
            with pytest.raises(WebSocketDisconnect) as invalid:
                socket.receive_json()
            assert invalid.value.code == 1012


def test_early_attach_and_ready_wait_for_native_receipt_without_reconnect(queue):
    queue.version = None
    queue.enqueue()
    with client(queue) as app, app.websocket_connect('/bge-worker/channel', headers={'Authorization': 'Bearer secret'}) as socket:
        welcome = attach(socket)
        assert welcome['provider_version'] is None
        generation = welcome['connection_generation']
        assert ready(socket, generation)['status'] == 'provider_pending'
        send(socket, generation, 'HEARTBEAT')
        assert socket.receive_json()['type'] == 'HEARTBEAT_ACK'
        assert queue.claim_calls == 0 and not queue.ready
        queue.version = '7'
        assert socket.receive_json()['status'] == 'ready'
        assert socket.receive_json()['type'] == 'DISPATCH'
        assert queue.generation == generation


def test_existing_job_dispatch_result_and_completion_notification(queue):
    with client(queue) as app, app.websocket_connect('/bge-worker/channel', headers={'Authorization': 'Bearer secret'}) as socket:
        generation = attach(socket)['connection_generation']
        assert ready(socket, generation)['status'] == 'ready'
        queue.enqueue()  # Appears after attachment; bounded reconciliation discovers it.
        job = socket.receive_json()
        assert job['type'] == 'DISPATCH' and job['texts'] == ['authorized text']
        ack = result(socket, generation)
        assert ack['status'] == 'accepted' and ack['claim'] == 'claim'
        assert queue.completion.wait(.1)
        assert queue.completed[0] == 'job'


def test_legal_document_utf8_bytes_are_dispatched_whole(queue):
    queue.enqueue()
    queue.pending['texts'] = ['\U0001f30d' * 40000]
    with client(queue) as app, app.websocket_connect('/bge-worker/channel', headers={'Authorization': 'Bearer secret'}) as socket:
        generation = attach(socket)['connection_generation']
        ready(socket, generation)
        dispatched = socket.receive_json()
        assert dispatched['texts'] == ['\U0001f30d' * 40000]


def test_reconnect_fences_old_socket_and_detach_does_not_touch_new(queue):
    with client(queue) as app:
        with app.websocket_connect('/bge-worker/channel', headers={'Authorization': 'Bearer secret'}) as first:
            old = attach(first)['connection_generation']
            with app.websocket_connect('/bge-worker/channel', headers={'Authorization': 'Bearer secret'}) as second:
                new = attach(second)['connection_generation']
                assert new > old
                with pytest.raises(WebSocketDisconnect) as stale:
                    first.receive_json()
                assert stale.value.code == 1012
                send(second, new, 'HEARTBEAT')
                assert second.receive_json()['type'] == 'HEARTBEAT_ACK'


def test_competing_incarnation_blocks_original_attachment(queue):
    with client(queue) as app:
        with app.websocket_connect('/bge-worker/channel', headers={'Authorization': 'Bearer secret'}) as first:
            attach(first)
            with app.websocket_connect('/bge-worker/channel', headers={'Authorization': 'Bearer secret'}) as second:
                with pytest.raises(WebSocketDisconnect) as conflict:
                    attach(second, 'competing')
                assert conflict.value.code == 1012
            with pytest.raises(WebSocketDisconnect) as blocked:
                first.receive_json()
            assert blocked.value.code == 1012


def test_committed_result_replay_after_reconnect_is_duplicate(queue):
    queue.enqueue()
    with client(queue) as app:
        with app.websocket_connect('/bge-worker/channel', headers={'Authorization': 'Bearer secret'}) as socket:
            generation = attach(socket)['connection_generation']
            ready(socket, generation)
            socket.receive_json()
            send(socket, generation, 'RESULT', job_id='job', claim='claim', space=SPACE,
                 vectors=[[1., *([0.] * 1023)]])
            assert queue.completion.wait(1)  # Deliberately do not consume ACK.
        with app.websocket_connect('/bge-worker/channel', headers={'Authorization': 'Bearer secret'}) as socket:
            generation = attach(socket)['connection_generation']
            ready(socket, generation)
            assert result(socket, generation)['status'] == 'duplicate'


def test_waiting_encoder_does_not_block_control_and_stale_claim_is_local(queue):
    queue.enqueue()
    with client(queue) as app, app.websocket_connect('/bge-worker/channel', headers={'Authorization': 'Bearer secret'}) as socket:
        generation = attach(socket)['connection_generation']
        ready(socket, generation)
        socket.receive_json()  # Worker now owns the forward, but has not returned it.
        send(socket, generation, 'HEARTBEAT', diagnostics={'cpu_seconds': 1})
        assert socket.receive_json()['type'] == 'HEARTBEAT_ACK'
        assert result(socket, generation, claim='expired')['status'] == 'stale'
        send(socket, generation, 'HEARTBEAT')
        assert socket.receive_json()['type'] == 'HEARTBEAT_ACK'
        assert queue.completed is None


@pytest.mark.parametrize('payload', [b'x' * 65537,
    b'{"type":"ATTACH","type":"ATTACH","protocol_version":2}',
    b'{"type":"ATTACH","protocol_version":2.0}'])
def test_malformed_or_oversize_frame_closes_without_claim(queue, payload):
    with client(queue) as app, app.websocket_connect('/bge-worker/channel', headers={'Authorization': 'Bearer secret'}) as socket:
        socket.send_bytes(payload)
        with pytest.raises(WebSocketDisconnect) as invalid:
            socket.receive_json()
        assert invalid.value.code == 1012
        assert queue.claim_calls == 0


def test_real_queue_receipt_roundtrip_waiter_and_lost_ack_replay(tmp_path):
    queue = bge_queue.BgeQueue(tmp_path / 'queue.sqlite')
    job_id = queue.enqueue('actor', 'one', ['fixture text'], kind='document')
    with queue.connect(write=False) as db:
        run_id = db.execute('select id from runs').fetchone()[0]
    token = (queue.path.parent / 'bge-worker-credentials' / run_id).read_text()
    nonce = queue.prepare_channel(run_id)
    queue.launch_claim(run_id)

    def connect(socket):
        socket.send_json({'type': 'ATTACH', 'protocol_version': 2, 'run_id': run_id,
                          'launch_nonce': nonce, 'runtime_incarnation': 'real-fixture'})
        return socket.receive_json()['connection_generation']

    with client(queue) as app, ThreadPoolExecutor(max_workers=1) as pool:
        with app.websocket_connect('/bge-worker/channel', headers={'Authorization': 'Bearer ' + token}) as socket:
            generation = connect(socket)
            assert ready(socket, generation)['status'] == 'provider_pending'
            assert queue.result('actor', job_id)['state'] == 'pending'
            queue.launch_record(run_id, 'fixture-owner/stable-slug', provider_version=7)
            assert socket.receive_json()['status'] == 'ready'
            job = socket.receive_json()
            assert job['id'] == job_id
            waiting = pool.submit(lambda: asyncio.run(queue.wait_result('actor', job_id, 2)))
            payload = {'job_id': job_id, 'claim': job['claim'], 'space': SPACE,
                       'vectors': [[1., *([0.] * 1023)]]}
            send(socket, generation, 'RESULT', **payload)
            assert waiting.result(timeout=3)['state'] == 'done'
            # The server may have queued ACK; deliberately leave it unread.
        with app.websocket_connect('/bge-worker/channel', headers={'Authorization': 'Bearer ' + token}) as socket:
            generation = connect(socket)
            assert ready(socket, generation)['status'] == 'ready'
            send(socket, generation, 'RESULT', **payload)
            ack = socket.receive_json()
            assert ack['status'] == 'duplicate' and ack['claim'] == job['claim']


def test_dispatch_lost_before_worker_observes_it_resumes_same_real_claim(tmp_path, monkeypatch):
    queue = bge_queue.BgeQueue(tmp_path / 'queue.sqlite')
    job_id = queue.enqueue('actor', 'one', ['fixture text'], kind='document')
    with queue.connect(write=False) as db:
        run_id = db.execute('select id from runs').fetchone()[0]
    token = (queue.path.parent / 'bge-worker-credentials' / run_id).read_text()
    nonce = queue.prepare_channel(run_id)
    queue.launch_claim(run_id)
    queue.launch_record(run_id, 'fixture-owner/stable-slug', provider_version=7)
    original_send = WebSocket.send_text
    dropped = []
    dropped_event = threading.Event()

    async def lose_first_dispatch(socket, data):
        import json
        frame = json.loads(data)
        if frame['type'] == 'DISPATCH' and not dropped:
            dropped.append(frame)
            dropped_event.set()
            # Simulate the connection dropping before ASGI writes this frame.
            raise WebSocketDisconnect()
        await original_send(socket, data)

    monkeypatch.setattr(WebSocket, 'send_text', lose_first_dispatch)

    def connect(socket):
        socket.send_json({'type': 'ATTACH', 'protocol_version': 2, 'run_id': run_id,
                          'launch_nonce': nonce, 'runtime_incarnation': 'real-fixture'})
        return socket.receive_json()['connection_generation']

    with client(queue) as app:
        with app.websocket_connect('/bge-worker/channel', headers={'Authorization': 'Bearer ' + token}) as first:
            generation = connect(first)
            assert ready(first, generation)['status'] == 'ready'
            assert dropped_event.wait(2)
        assert len(dropped) == 1
        with app.websocket_connect('/bge-worker/channel', headers={'Authorization': 'Bearer ' + token}) as second:
            new_generation = connect(second)
            assert ready(second, new_generation)['status'] == 'ready'
            resumed = second.receive_json()
            assert new_generation > generation
            assert resumed['type'] == 'DISPATCH' and resumed['id'] == job_id
            for key in ('id', 'claim', 'kind', 'texts', 'space'):
                assert resumed[key] == dropped[0][key]
            with queue.connect(write=False) as db:
                row = db.execute('select * from jobs where id=?', (job_id,)).fetchone()
                assert row['attempt'] == 1
                assert db.execute("select count(*) from jobs where state='claimed'").fetchone()[0] == 1


def test_buffered_dispatch_is_dropped_when_completion_overtakes_it(queue):
    queue.enqueue()
    queue.claimed, queue.pending = queue.pending, None
    entered = threading.Event()
    release = threading.Event()
    resume = queue.resume_channel_claim
    calls = 0

    def held_resume(run, token, generation):
        nonlocal calls
        calls += 1
        if calls == 2:  # Initial recovery queued it; sender is about to validate.
            entered.set()
            assert release.wait(2)
        return resume(run, token, generation)

    queue.resume_channel_claim = held_resume
    try:
        with client(queue) as app, app.websocket_connect('/bge-worker/channel', headers={'Authorization': 'Bearer secret'}) as socket:
            generation = attach(socket)['connection_generation']
            assert ready(socket, generation)['status'] == 'ready'
            assert entered.wait(1)
            send(socket, generation, 'RESULT', job_id='job', claim='claim', space=SPACE,
                 vectors=[[1., *([0.] * 1023)]])
            assert queue.completion.wait(1)
            release.set()
            # A stale buffered DISPATCH would arrive before this ACK if emitted.
            assert socket.receive_json()['type'] == 'RESULT_ACK'
            send(socket, generation, 'HEARTBEAT')
            assert socket.receive_json()['type'] == 'HEARTBEAT_ACK'
    finally:
        release.set()
