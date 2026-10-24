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


@pytest.mark.asyncio
async def test_transport_failure_is_visible_before_native_drain_without_private_text(capsys):
    transport=module('bge_wss_transport')
    entered=asyncio.Event();release=asyncio.Event();closed=[]
    async def native_load():
        await release.wait()
    @asynccontextmanager
    async def connect():
        entered.set()
        raise ValueError('private URL and credential must never be logged')
        yield
    async def close():closed.append(True)
    worker=transport.BgeWssTransport(CONFIG,lambda:None,lambda:{},
        connector=connect,close_runtime=close)
    worker.loading=asyncio.create_task(native_load())
    running=asyncio.create_task(worker.run())
    try:
        await asyncio.wait_for(entered.wait(),1)
        output=capsys.readouterr().out
        assert CONFIG['token'] not in output and CONFIG['channel'] not in output
        event=next(e for e in map(json.loads,output.splitlines())
                   if e['event']=='worker_transport_failed')
        assert event==dict(event='worker_transport_failed',run_id='fixture-run',error_type='ValueError')
        assert not running.done() and not worker.loading.done() and not closed
    finally:
        release.set()
        with pytest.raises(ValueError,match='private URL'):
            await running
    assert closed==[True]


@pytest.mark.asyncio
async def test_stable_heartbeat_renews_outage_budget_without_reloading_encoder(monkeypatch):
    transport=module('bge_wss_transport')
    monkeypatch.setattr(transport.random,'uniform',lambda *args:.0001)
    stop=asyncio.Event();clock=[0.];attempts=[];sessions=[];loads=[]
    @asynccontextmanager
    async def connect():
        attempts.append(len(attempts)+1)
        if len(attempts) not in (10,19):
            raise ConnectionError('temporary API unavailability')
        generation=len(sessions)+1;sessions.append(generation)
        incoming=asyncio.Queue()
        class Socket:
            async def send(self,raw):
                frame=json.loads(raw);kind=frame['type']
                if kind=='ATTACH':reply=dict(type='WELCOME',provider_version='85')
                elif kind=='READY':reply=dict(type='READY_ACK',status='ready',provider_version='85')
                elif kind=='HEARTBEAT':
                    clock[0]+=31
                    reply=dict(type='HEARTBEAT_ACK',provider_version='85')
                else:raise AssertionError(kind)
                await incoming.put(json.dumps(dict(protocol_version=2,connection_generation=generation,**reply)))
                if kind=='HEARTBEAT':
                    if generation==1:await incoming.put(ConnectionError('later independent API restart'))
                    else:asyncio.get_running_loop().call_later(.01,stop.set)
            async def receive(self):
                value=await incoming.get()
                if isinstance(value,Exception):raise value
                return value
        yield Socket()
    def load():loads.append('encoder');return lambda text:[1.]+[0.]*1023
    worker=transport.BgeWssTransport(CONFIG,load,lambda:{},connector=connect,
        heartbeat_seconds=.01,maximum_seconds=3,stop=stop)
    worker._retry_clock=lambda:clock[0]
    await worker.run()
    assert len(attempts)==19 and sessions==[1,2] and loads==['encoder']


@pytest.mark.asyncio
async def test_unhealthy_connections_still_exhaust_bounded_outage_budget(monkeypatch):
    transport=module('bge_wss_transport')
    monkeypatch.setattr(transport.random,'uniform',lambda *args:.0001)
    attempts=[]
    @asynccontextmanager
    async def connect():
        attempts.append(1)
        raise ConnectionError('API stays unavailable')
        yield
    worker=transport.BgeWssTransport(CONFIG,lambda:None,lambda:{},connector=connect,maximum_seconds=3,
        reconnect_grace_seconds=9)
    clock=[0.]
    def tick():
        clock[0]+=1.;return clock[0]
    worker._retry_clock=tick
    with pytest.raises(transport.BgeChannelStopped,match='reconnect exhausted'):
        await worker.run()
    assert len(attempts)==10


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
async def test_wrong_ack_nonce_exhausts_reconnect_without_discarding_result(monkeypatch):
    transport = module('bge_wss_transport')
    monkeypatch.setattr(transport.random,'uniform',lambda *args:.001)
    stop = asyncio.Event()
    wire = Wire(stop, wrong_claim=True)
    worker = transport.BgeWssTransport(CONFIG, lambda: lambda _: [1.] + [0.] * 1023,
                                      lambda: {}, connector=wire.connect, maximum_seconds=3, stop=stop,
                                      reconnect_grace_seconds=9)
    clock=[0.]
    def tick():clock[0]+=1.;return clock[0]
    worker._retry_clock=tick
    with pytest.raises(transport.BgeChannelStopped,match='reconnect exhausted'):
        await worker.run()
    assert worker.pending['claim'] == 'claim' and wire.sessions>1


@pytest.mark.asyncio
@pytest.mark.parametrize('close_code',[4400,4403,4409,1012])
async def test_native_socket_close_reconnects_with_exact_pending_result(monkeypatch,close_code):
    import aiohttp
    from types import SimpleNamespace
    transport=module('bge_wss_transport')
    monkeypatch.setattr(transport.random,'uniform',lambda *args:.001)
    stop=asyncio.Event();wire=Wire(stop,lose_ack=True);observed=[];calls=[]
    class Session:
        def __init__(self,**kwargs):pass
        async def __aenter__(self):return self
        async def __aexit__(self,*args):pass
        @asynccontextmanager
        async def ws_connect(self,*args,**kwargs):
            async with wire.connect() as inner:
                closed=asyncio.Event()
                class Socket:
                    async def send_str(self,raw):
                        try:await inner.send(raw)
                        except ConnectionError:closed.set()
                    async def receive(self):
                        read=asyncio.create_task(inner.receive())
                        closing=asyncio.create_task(closed.wait())
                        try:
                            done,_=await asyncio.wait((read,closing),return_when=asyncio.FIRST_COMPLETED)
                            if closing in done:
                                observed.append(close_code)
                                return SimpleNamespace(type=aiohttp.WSMsgType.CLOSE,data=close_code)
                            return SimpleNamespace(type=aiohttp.WSMsgType.TEXT,data=read.result())
                        finally:
                            read.cancel();closing.cancel()
                            await asyncio.gather(read,closing,return_exceptions=True)
                yield Socket()
    monkeypatch.setattr(aiohttp,'ClientSession',Session)
    def load():
        calls.append('load')
        def encode(text):
            calls.append(text);return [1.]+[0.]*1023
        return encode
    worker=transport.BgeWssTransport(CONFIG,load,lambda:{},heartbeat_seconds=.01,maximum_seconds=3,stop=stop)
    await worker.run()
    assert observed==[close_code] and wire.sessions==2
    assert calls==['load','fixture text'] and worker.pending is None
    results=[f for f in wire.sent if f['type']=='RESULT']
    assert len(results)==2
    assert {k:v for k,v in results[0].items() if k!='connection_generation'}=={
        k:v for k,v in results[1].items() if k!='connection_generation'}


@pytest.mark.asyncio
async def test_stale_generation_ack_reconnects_without_discarding_result(monkeypatch):
    transport=module('bge_wss_transport')
    monkeypatch.setattr(transport.random,'uniform',lambda *args:.001)
    stop=asyncio.Event();wire=Wire(stop,wrong_claim=True);calls=[]
    @asynccontextmanager
    async def connect():
        async with wire.connect() as inner:
            class Socket:
                async def send(self,raw):await inner.send(raw)
                async def receive(self):
                    frame=json.loads(await inner.receive())
                    if wire.sessions==1 and frame['type']=='RESULT_ACK':
                        frame['connection_generation']=0
                        frame['claim']='claim';wire.wrong_claim=False
                    return json.dumps(frame)
            yield Socket()
    def load():
        calls.append('load')
        def encode(text):calls.append(text);return [1.]+[0.]*1023
        return encode
    worker=transport.BgeWssTransport(CONFIG,load,lambda:{},connector=connect,
        heartbeat_seconds=.01,maximum_seconds=3,stop=stop)
    await worker.run()
    assert wire.sessions==2 and calls==['load','fixture text'] and worker.pending is None
    assert len([f for f in wire.sent if f['type']=='RESULT'])==2


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


@pytest.mark.asyncio
async def test_native_handshake_timeout_and_failure_stages_do_not_log_secrets(monkeypatch,capsys):
    import aiohttp
    from types import SimpleNamespace
    transport=module('bge_wss_transport');stop=asyncio.Event();settings=[]
    class Session:
        def __init__(self,**kwargs):settings.append(kwargs)
        async def __aenter__(self):return self
        async def __aexit__(self,*args):pass
        @asynccontextmanager
        async def ws_connect(self,*args,**kwargs):
            trace=settings[-1]['trace_configs'][0]
            for callback in trace.on_dns_resolvehost_start:await callback(None,None,None)
            for callback in trace.on_dns_resolvehost_end:await callback(None,None,None)
            stop.set()
            raise aiohttp.WSServerHandshakeError(SimpleNamespace(real_url=CONFIG['channel']),(),
                status=503,message='private provider response '+CONFIG['token'])
            yield
    monkeypatch.setattr(aiohttp,'ClientSession',Session)
    worker=transport.BgeWssTransport(CONFIG,lambda:None,lambda:{},stop=stop)
    await worker.run()
    timeout=settings[0]['timeout']
    assert (timeout.total,timeout.connect,timeout.sock_connect)==(15,10,10)
    raw=capsys.readouterr().out
    assert CONFIG['token'] not in raw and CONFIG['channel'] not in raw
    assert 'private provider response' not in raw
    events=[json.loads(line)for line in raw.splitlines()]
    assert [e['stage']for e in events]==['aiohttp_import_start','aiohttp_import_complete',
        'wss_connect_start','dns_start','dns_complete','wss_connect_failed']
    assert events[-1]['error_type']=='WSServerHandshakeError'and events[-1]['http_status']==503
    assert all(e['timestamp']>0 and e['run_id']==CONFIG['run_id']for e in events)
