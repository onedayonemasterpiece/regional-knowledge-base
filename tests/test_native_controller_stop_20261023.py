"""Existing authenticated STOP, preserving handover jobs and provider uncertainty.

Synthetic WSS and local SQLite only; no Kaggle SDK, PG or provider calls.
"""
import asyncio
import threading

import pytest
from starlette.websockets import WebSocketDisconnect

from test_bge_channel import Queue, client, attach, ready, send
from test_bge_shared_worker import Peer
from test_bge_wss_worker import CONFIG, module
from regional_knowledge.bge_contract import SPACE


class PlannedQueue(Queue):
    """The two exact owner predicates; real SQLite predicates tested below."""
    current = True
    age_seconds = 0
    pending_job = False

    def stop_eligible(self, run, token, generation):
        self.channel_status(run, token, generation)
        return (not self.current or self.age_seconds >= 9 * 3600) and not self.pending_job


def expected_stop(generation):
    return dict(type='STOP', protocol_version=2, connection_generation=generation,
        run_id='run', launch_nonce='nonce', runtime_incarnation='instance')


def test_current_under_nine_hours_and_merely_draining_do_not_stop():
    queue = PlannedQueue()
    with client(queue) as app, app.websocket_connect(
            '/bge-worker/channel', headers={'Authorization':'Bearer secret'}) as socket:
        generation = attach(socket)['connection_generation']
        assert ready(socket, generation)['status'] == 'ready'
        send(socket, generation, 'HEARTBEAT')
        assert socket.receive_json()['type'] == 'HEARTBEAT_ACK'
        # A manual drain of the serving owner cannot stop it before handover.
        queue.current = True
        send(socket, generation, 'HEARTBEAT')
        assert socket.receive_json()['type'] == 'HEARTBEAT_ACK'


def test_handover_stops_only_noncurrent_idle_run_before_next_heartbeat():
    queue = PlannedQueue()
    with client(queue) as app, app.websocket_connect(
            '/bge-worker/channel', headers={'Authorization':'Bearer secret'}) as socket:
        generation = attach(socket)['connection_generation']
        assert ready(socket, generation)['status'] == 'ready'
        queue.current = False
        # Dispatch's existing .25s reconciliation, not worker heartbeat20s.
        assert socket.receive_json() == expected_stop(generation)


def test_current_nine_hours_stops_even_without_successor():
    queue = PlannedQueue()
    with client(queue) as app, app.websocket_connect(
            '/bge-worker/channel', headers={'Authorization':'Bearer secret'}) as socket:
        generation = attach(socket)['connection_generation']
        assert ready(socket, generation)['status'] == 'ready'
        queue.age_seconds = 9 * 3600
        assert socket.receive_json() == expected_stop(generation)


def test_claimed_or_pending_work_never_receives_stop():
    queue = PlannedQueue()
    with client(queue) as app, app.websocket_connect(
            '/bge-worker/channel', headers={'Authorization':'Bearer secret'}) as socket:
        generation = attach(socket)['connection_generation']
        assert ready(socket, generation)['status'] == 'ready'
        queue.current = False
        queue.pending_job = True
        send(socket, generation, 'HEARTBEAT')
        assert socket.receive_json()['type'] == 'HEARTBEAT_ACK'
        queue.pending_job = False
        assert socket.receive_json() == expected_stop(generation)


@pytest.mark.asyncio
async def test_worker_accepts_only_exact_idle_stop_without_pre_ready_cancellation():
    transport = module('bge_wss_transport')
    peer = Peer()
    async def read(frame, *, encode):
        return {}
    worker = transport.BgeWssTransport(dict(CONFIG, shared_reads=True),
        lambda: lambda text:[1.] + [0.] * 1023, lambda:{}, connector=peer.connect,
        query_handler=read, heartbeat_seconds=.01, maximum_seconds=3)
    run = asyncio.create_task(worker.run())
    try:
        await peer.wait('ATTACH')
        await peer.put(dict(type='STOP', run_id=CONFIG['run_id'],
            launch_nonce=CONFIG['launch_nonce'], runtime_incarnation=worker.incarnation))
        await asyncio.wait_for(run, 2)
        assert worker.stop.is_set() and peer.sessions == 1
    finally:
        if not run.done():
            worker.stop.set()
            run.cancel()
            await asyncio.gather(run, return_exceptions=True)


@pytest.mark.asyncio
async def test_forged_stop_does_not_stop_original_worker_or_revoke_identity():
    transport = module('bge_wss_transport')
    peer = Peer()
    async def read(frame, *, encode):
        return {}
    worker = transport.BgeWssTransport(dict(CONFIG, shared_reads=True),
        lambda: lambda text:[1.] + [0.] * 1023, lambda:{}, connector=peer.connect,
        query_handler=read, heartbeat_seconds=.01, maximum_seconds=4)
    run = asyncio.create_task(worker.run())
    try:
        await peer.wait('ATTACH', generation=1)
        await peer.put(dict(type='STOP', run_id='foreign-run',
            launch_nonce=CONFIG['launch_nonce'], runtime_incarnation=worker.incarnation), 1)
        await peer.wait('ATTACH', generation=2)
        assert not worker.stop.is_set()
        await peer.put(dict(type='STOP', run_id=CONFIG['run_id'],
            launch_nonce=CONFIG['launch_nonce'], runtime_incarnation=worker.incarnation), 2)
        await asyncio.wait_for(run, 2)
        assert peer.sessions == 2
    finally:
        if not run.done():
            worker.stop.set()
            run.cancel()
            await asyncio.gather(run, return_exceptions=True)


@pytest.mark.asyncio
async def test_external_cancel_remains_external_cancel_not_successful_stop():
    transport = module('bge_wss_transport')
    peer = Peer()
    async def read(frame, *, encode):
        return {}
    worker = transport.BgeWssTransport(dict(CONFIG, shared_reads=True),
        lambda:lambda text:[1.] + [0.]*1023, lambda:{}, connector=peer.connect,
        query_handler=read, heartbeat_seconds=.01, maximum_seconds=4)
    run = asyncio.create_task(worker.run())
    await peer.wait('ATTACH')
    run.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(run, 2)
    assert not worker.stop.is_set()


def test_actual_old_and_successor_channels_stop_only_predecessor(tmp_path):
    """Two real authenticated ASGI sockets, real BgeQueue pointers and claims.

    Synthetic launch metadata only; no provider/notebook, new run stays usable.
    """
    from regional_knowledge.bge_queue import BgeQueue
    from regional_knowledge.bge_contract import REVISION
    q = BgeQueue(tmp_path/'two-sockets.sqlite', clock=lambda:1000.)
    with q.connect() as db:
        old = q._new_run(db, 1000.)
        db.execute('UPDATE control SET current_run=? WHERE id=1',(old,))
    token = (q.path.parent/'bge-worker-credentials'/old).read_text()
    nonce = q.prepare_channel(old)
    q.launch_record(old, 'owner/fixture-old', provider_version=1)
    with client(q) as app:
        with app.websocket_connect('/bge-worker/channel',
                headers={'Authorization':'Bearer '+token}) as old_socket:
            old_socket.send_json(dict(type='ATTACH',protocol_version=2,
                run_id=old,launch_nonce=nonce,runtime_incarnation='old-instance'))
            old_gen = old_socket.receive_json()['connection_generation']
            send(old_socket,old_gen,'READY',space=SPACE,model_revision=REVISION)
            assert old_socket.receive_json()['status']=='ready'
            with q.connect() as db:
                successor = q._new_run(db,1000.)
                db.execute('UPDATE control SET successor=? WHERE id=1',(successor,))
            new_token = (q.path.parent/'bge-worker-credentials'/successor).read_text()
            new_nonce = q.prepare_channel(successor)
            q.launch_record(successor,'owner/fixture-successor',provider_version=2)
            with app.websocket_connect('/bge-worker/channel',
                    headers={'Authorization':'Bearer '+new_token}) as next_socket:
                next_socket.send_json(dict(type='ATTACH',protocol_version=2,
                    run_id=successor,launch_nonce=new_nonce,
                    runtime_incarnation='next-instance'))
                next_gen = next_socket.receive_json()['connection_generation']
                send(next_socket,next_gen,'READY',space=SPACE,model_revision=REVISION)
                assert next_socket.receive_json()['status']=='ready'
                expected = dict(type='STOP',protocol_version=2,
                    connection_generation=old_gen,run_id=old,launch_nonce=nonce,
                    runtime_incarnation='old-instance')
                assert old_socket.receive_json() == expected
                with q.connect(write=False) as db:
                    assert db.execute('SELECT current_run FROM control WHERE id=1').fetchone()[0]==successor
                    assert db.execute("SELECT launch_state FROM runs WHERE id=?",(old,)).fetchone()[0]=='dispatched'
                send(next_socket,next_gen,'HEARTBEAT')
                assert next_socket.receive_json()['type']=='HEARTBEAT_ACK'


def test_existing_authenticated_stop_waits_for_checkpoint_outcome():
    """One WSS STOP, with no new frame or manager; awaited before send."""
    from starlette.applications import Starlette
    from starlette.testclient import TestClient
    from regional_knowledge.bge_channel import channel_route
    calls=[]
    async def pre_stop(run_id,generation):
        calls.append(('before',run_id,generation))
        await asyncio.sleep(.01)
        calls.append(('settled',run_id,generation))
    queue=PlannedQueue()
    with TestClient(Starlette(routes=[channel_route(queue,before_stop=pre_stop)])) as app:
        with app.websocket_connect('/bge-worker/channel',headers={'Authorization':'Bearer secret'}) as socket:
            generation=attach(socket)['connection_generation']
            assert ready(socket,generation)['status']=='ready'
            queue.current=False
            assert socket.receive_json()==expected_stop(generation)
    assert calls==[('before','run',generation),('settled','run',generation)]


def test_existing_authenticated_stop_waits_for_checkpoint_error_then_stops():
    """Failed cloud durability is reported, not re-sent or faked as Saved."""
    from starlette.applications import Starlette
    from starlette.testclient import TestClient
    from regional_knowledge.bge_channel import channel_route
    calls=[]
    async def pre_stop(run_id,generation):
        calls.append((run_id,generation))
        raise TimeoutError('synthetic Dataset publication pending')
    queue=PlannedQueue()
    with TestClient(Starlette(routes=[channel_route(queue,before_stop=pre_stop)])) as app:
        with app.websocket_connect('/bge-worker/channel',headers={'Authorization':'Bearer secret'}) as socket:
            generation=attach(socket)['connection_generation']
            assert ready(socket,generation)['status']=='ready'
            queue.current=False
            assert socket.receive_json()==expected_stop(generation)
    assert calls==[('run',generation)]
