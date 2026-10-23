"""Explicit authenticated controller STOP, no provider, PG or notebook calls."""
import asyncio

import pytest

from test_bge_channel import Queue, client, attach, ready, send
from test_bge_shared_worker import Peer, result
from test_bge_wss_worker import CONFIG, module
from regional_knowledge.bge_contract import REVISION, SPACE


def test_planned_idle_controller_sends_stop_on_existing_heartbeat():
    class PlannedQueue(Queue):
        def __init__(self):
            super().__init__()
            self.planned=False

        def channel_status(self, run, token, generation):
            status=super().channel_status(run,token,generation)
            if self.planned:
                status['state']='draining'
            return status

        def heartbeat(self, *args, **kwargs):
            super().heartbeat(*args,**kwargs)
            return {'state':'draining' if self.planned else 'ready'}

    queue=PlannedQueue()
    with client(queue) as app, app.websocket_connect(
            '/bge-worker/channel', headers={'Authorization':'Bearer secret'}) as socket:
        generation=attach(socket)['connection_generation']
        assert ready(socket,generation)['status']=='ready'
        send(socket,generation,'HEARTBEAT')
        assert socket.receive_json()['type']=='HEARTBEAT_ACK'
        queue.planned=True  # Existing owner-approved planned drain, not a new state.
        send(socket,generation,'HEARTBEAT')
        stopped=socket.receive_json()
        assert stopped==dict(type='STOP',protocol_version=2,
            connection_generation=generation,run_id='run',launch_nonce='nonce',
            runtime_incarnation='instance')


@pytest.mark.asyncio
async def test_explicit_wss_stop_exits_unready_worker_without_900_seconds():
    transport=module('bge_wss_transport')
    peer=Peer();entered=asyncio.Event();closed=[];prepare_cancelled=[]
    never=asyncio.Event()
    async def read(frame,*,encode):return result(frame)
    async def prepare(encoder,**identity):
        entered.set()
        try:
            await never.wait()  # Existing unresolved restore initialization.
        except asyncio.CancelledError:
            prepare_cancelled.append(True)
            raise
        return None
    async def close():closed.append(True)
    worker=transport.BgeWssTransport(dict(CONFIG,shared_reads=True),
        lambda:lambda text:[1.]+[0.]*1023,lambda:{},connector=peer.connect,
        prepare_runtime=prepare,close_runtime=close,heartbeat_seconds=.01,
        maximum_seconds=3)
    task=asyncio.create_task(worker.run())
    try:
        await peer.wait('ATTACH')
        await asyncio.wait_for(entered.wait(),1)
        await peer.put(dict(type='STOP',run_id=CONFIG['run_id'],
            launch_nonce=CONFIG['launch_nonce'],runtime_incarnation=worker.incarnation))
        await asyncio.wait_for(task,1)
        assert peer.sessions==1 and worker.stop.is_set()
        assert prepare_cancelled==[True] and closed==[True]
    finally:
        if not task.done():
            worker.stop.set()
            task.cancel()
            never.set()
            await asyncio.gather(task,return_exceptions=True)
