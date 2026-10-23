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
    worker = transport.BgeWssTransport(dict(CONFIG, shared_reads=True),
        lambda: lambda text:[1.] + [0.] * 1023, lambda:{}, connector=peer.connect,
        query_handler=lambda frame, *, encode: asyncio.sleep(0, result={}),
        heartbeat_seconds=.01, maximum_seconds=3)
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
