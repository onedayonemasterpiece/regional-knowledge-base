"""Run the real outbound transport with stub inference and a deterministic wire."""
import asyncio
from contextlib import asynccontextmanager
import importlib.util
import json
from pathlib import Path
import threading
import socket as sockets

import pytest

from regional_knowledge.bge_contract import SPACE, REVISION

ROOT = Path(__file__).resolve().parents[1]


def module(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / 'scripts/production' / (name + '.py'))
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    value.SPACE, value.REVISION = SPACE, REVISION
    return value


CONFIG = {'run_id': 'fixture-run', 'token': 'synthetic-worker-token',
          'channel': 'wss://fixture.example/bge-worker/channel', 'launch_nonce': 'fixture-nonce'}


class Wire:
    def __init__(self, stop, *, lose_ack=False, wrong_claim=False, replay_dispatch=False):
        self.stop, self.lose_ack, self.wrong_claim = stop, lose_ack, wrong_claim
        self.sessions = 0
        self.sent = []
        self.heartbeated = asyncio.Event()
        self.replay_dispatch = replay_dispatch

    @asynccontextmanager
    async def connect(self):
        self.sessions += 1
        generation = self.sessions
        incoming = asyncio.Queue()
        owner = self

        class Socket:
            async def send(self, raw):
                frame = json.loads(raw)
                owner.sent.append(frame)
                kind = frame['type']
                if kind == 'ATTACH':
                    reply = {'type': 'WELCOME', 'provider_version': '85'}
                elif kind == 'HEARTBEAT':
                    owner.heartbeated.set()
                    reply = {'type': 'HEARTBEAT_ACK', 'provider_version': '85'}
                elif kind == 'READY':
                    reply = {'type': 'READY_ACK', 'status': 'ready', 'provider_version': '85'}
                    if generation == 1 or owner.replay_dispatch:
                        await incoming.put(json.dumps({'type': 'DISPATCH', 'protocol_version': 2,
                            'connection_generation': generation, 'id': 'job', 'claim': 'claim',
                            'kind': 'document', 'texts': ['fixture text'], 'space': SPACE}))
                elif kind == 'RESULT':
                    if owner.lose_ack and generation == 1:
                        raise ConnectionError('lost acknowledgment after simulated commit')
                    reply = {'type': 'RESULT_ACK', 'job_id': 'job',
                             'claim': 'wrong' if owner.wrong_claim else 'claim', 'status': 'duplicate' if generation > 1 else 'accepted'}
                    if not owner.wrong_claim:
                        asyncio.get_running_loop().call_later(.01, owner.stop.set)
                else:
                    raise AssertionError(kind)
                await incoming.put(json.dumps({'protocol_version': 2,
                    'connection_generation': generation, **reply}))

            async def receive(self):
                return await incoming.get()

        yield Socket()


@pytest.mark.asyncio
async def test_early_control_before_load_and_single_forward():
    transport = module('bge_wss_transport')
    stop = asyncio.Event()
    wire = Wire(stop)
    allow_load = threading.Event()
    calls = []

    def load():
        assert wire.sent[0]['type'] == 'ATTACH'
        assert allow_load.wait(2)
        calls.append('load')

        def encode(text):
            calls.append(text)
            return [1.] + [0.] * 1023
        return encode

    async def release_after_control():
        await asyncio.wait_for(wire.heartbeated.wait(), 1)
        assert not any(frame['type'] == 'READY' for frame in wire.sent)
        allow_load.set()

    worker = transport.BgeWssTransport(CONFIG, load, lambda: {}, connector=wire.connect,
                                      heartbeat_seconds=.01, maximum_seconds=3, stop=stop)
    await asyncio.gather(worker.run(), release_after_control())
    assert calls == ['load', 'fixture text']
    assert wire.sessions == 1
    assert worker.pending is None


@pytest.mark.asyncio
@pytest.mark.parametrize('replay_dispatch', [False, True])
async def test_lost_ack_reconnect_retains_exact_result_and_warm_encoder(monkeypatch, replay_dispatch):
    transport = module('bge_wss_transport')
    monkeypatch.setattr(transport.random, 'uniform', lambda *args: .001)
    stop = asyncio.Event()
    wire = Wire(stop, lose_ack=True, replay_dispatch=replay_dispatch)
    calls = []

    def load():
        calls.append('load')
        def encode(text):
            calls.append(text)
            return [1.] + [0.] * 1023
        return encode

    worker = transport.BgeWssTransport(CONFIG, load, lambda: {}, connector=wire.connect,
                                      heartbeat_seconds=.01, maximum_seconds=3, stop=stop)
    await worker.run()
    assert wire.sessions == 2 and calls == ['load', 'fixture text']
    results = [f for f in wire.sent if f['type'] == 'RESULT']
    assert len(results) == 2
    assert results[0]['connection_generation'] == 1 and results[1]['connection_generation'] == 2
    assert {k: v for k, v in results[0].items() if k != 'connection_generation'} == {
        k: v for k, v in results[1].items() if k != 'connection_generation'}
    attaches = [f for f in wire.sent if f['type'] == 'ATTACH']
    assert attaches[0]['runtime_incarnation'] == attaches[1]['runtime_incarnation']


@pytest.mark.asyncio
async def test_wrong_ack_nonce_does_not_discard_exact_pending_result():
    transport = module('bge_wss_transport')
    stop = asyncio.Event()
    wire = Wire(stop, wrong_claim=True)
    worker = transport.BgeWssTransport(CONFIG, lambda: lambda _: [1.] + [0.] * 1023,
                                      lambda: {}, connector=wire.connect, maximum_seconds=3, stop=stop)
    with pytest.raises(ValueError, match='acknowledgment mismatch'):
        await worker.run()
    assert worker.pending['claim'] == 'claim'


@pytest.mark.asyncio
async def test_repeated_caller_cancellation_drains_actual_forward():
    transport = module('bge_wss_transport')
    entered, finish = threading.Event(), threading.Event()
    wire = Wire(asyncio.Event())

    def encode(text):
        entered.set()
        assert finish.wait(2)
        return [1.] + [0.] * 1023

    worker = transport.BgeWssTransport(CONFIG, lambda: encode, lambda: {},
                                      connector=wire.connect, maximum_seconds=3)
    running = asyncio.create_task(worker.run())
    try:
        assert await asyncio.to_thread(entered.wait, 1)
        running.cancel()
        await asyncio.sleep(.01)
        assert not running.done()
        running.cancel()
        await asyncio.sleep(.01)
        assert not running.done()
    finally:
        finish.set()
        with pytest.raises(asyncio.CancelledError):
            await running


def test_controller_embeds_one_helper_and_stable_prelaunch_nonce(tmp_path):
    from regional_knowledge.bge_queue import BgeQueue
    controller = module('bge_kaggle_controller')
    queue = BgeQueue(tmp_path / 'queue.sqlite')
    queue.enqueue('actor', 'document', ['fixture'], kind='document')
    run = queue.maintenance()[0]['id']
    template = (ROOT / 'scripts/production/bge_kaggle_worker.py').read_text()
    source = controller.worker_source(template, queue, run, 'synthetic-token',
                                     'https://fixture.example/bge-worker', transport='wss')
    assert source.count('class BgeWssTransport:') == 1
    assert "'channel': 'wss://fixture.example/bge-worker/channel'" in source
    assert source == controller.worker_source(template, queue, run, 'synthetic-token',
        'https://fixture.example/bge-worker', transport='wss')
    assert controller.source_run_id(source) == run
    compile(source, '<generated-private-worker-fixture>', 'exec')
    # Legacy bytes don't import the new optional network dependency or embed it.
    legacy = controller.worker_source(template, queue, run, 'synthetic-token',
                                      'https://fixture.example/bge-worker')
    assert 'class BgeWssTransport:' not in legacy


@pytest.mark.parametrize('channel', ['ws://fixture.example/channel', 'wss://fixture.example:444/channel',
                                   'wss://user:pass@fixture.example/channel', 'wss://fixture.example/channel?token=x'])
def test_worker_rejects_unsafe_remote_endpoint(channel):
    transport = module('bge_wss_transport')
    with pytest.raises(ValueError):
        transport.BgeWssTransport({**CONFIG, 'channel': channel}, lambda: None, lambda: {})


@pytest.mark.asyncio
async def test_actual_existing_asgi_outbound_worker_queue_and_waiter(tmp_path, monkeypatch):
    """Local native websocket, no TLS/egress/model/provider acceptance claim."""
    import aiohttp
    import uvicorn
    from regional_knowledge.bge_queue import BgeQueue
    from regional_knowledge.server import build_server

    transport = module('bge_wss_transport')
    queue = BgeQueue(tmp_path / 'queue.sqlite')
    job_id = queue.enqueue('actor', 'native-wire-fixture', ['fixture text'], kind='document')
    run_id = queue.maintenance()[0]['id']
    nonce = queue.prepare_channel(run_id)
    token = queue.launch_claim(run_id)
    queue.launch_record(run_id, 'fixture-owner/stable-slug', provider_version=7)
    monkeypatch.setenv('RKB_DEV_NOAUTH', '1')
    monkeypatch.setenv('RKB_BGE_QUEUE_PATH', str(queue.path))
    mcp = build_server()
    app = mcp.streamable_http_app()
    assert len([r for r in app.routes if getattr(r, 'path', None) == '/bge-worker/channel']) == 1
    listener = sockets.socket()
    listener.bind(('127.0.0.1', 0))
    listener.listen(5)
    port = listener.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(app, log_level='error', lifespan='off', ws='websockets-sansio'))
    serving = asyncio.create_task(server.serve(sockets=[listener]))
    stop = asyncio.Event()
    calls = []

    @asynccontextmanager
    async def connect():
        async with aiohttp.ClientSession() as client:
            async with client.ws_connect(f'http://127.0.0.1:{port}/bge-worker/channel',
                    headers={'Authorization': 'Bearer ' + token}) as wire:
                class Adapter:
                    async def send(self, raw):
                        await wire.send_str(raw)

                    async def receive(self):
                        message = await wire.receive()
                        if message.type not in (aiohttp.WSMsgType.TEXT, aiohttp.WSMsgType.BINARY):
                            raise ConnectionError('fixture wire closed')
                        return message.data
                yield Adapter()

    def load():
        calls.append('load')
        def encode(text):
            calls.append(text)
            return [1.] + [0.] * 1023
        return encode

    worker = transport.BgeWssTransport({**CONFIG, 'run_id': run_id, 'token': token,
                                      'launch_nonce': nonce}, load, lambda: {},
                                      connector=connect, heartbeat_seconds=.02,
                                      maximum_seconds=5, stop=stop)
    working = None
    try:
        for _ in range(100):
            if server.started:
                break
            await asyncio.sleep(.005)
        assert server.started
        working = asyncio.create_task(worker.run())
        row = await queue.wait_result('actor', job_id, 3)
        assert row['state'] == 'done' and row['result']['vectors'] == [[1.] + [0.] * 1023]
        for _ in range(100):
            if worker.acknowledged is not None:
                break
            await asyncio.sleep(.005)
        assert worker.acknowledged[0] == job_id
        assert calls == ['load', 'fixture text']
        with queue.connect(write=False) as db:
            assert db.execute('SELECT attempt FROM jobs WHERE id=?', (job_id,)).fetchone()[0] == 1
    finally:
        stop.set()
        try:
            if working is not None:
                await asyncio.wait_for(working, 3)
        finally:
            server.should_exit = True
            try:
                await asyncio.wait_for(serving, 3)
            finally:
                listener.close()
