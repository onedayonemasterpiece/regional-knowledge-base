"""Adversarial result ordering on the real server socket, with synthetic owner."""
import asyncio
import json
import threading
from types import SimpleNamespace
import pytest
from starlette.websockets import WebSocket,WebSocketDisconnect
from test_bge_channel import client,attach,ready,send
from test_telegram_binding_wire import RuntimeQueue

class Owner:
    def __init__(self):
        self.confirmed=[]
        self.lease=SimpleNamespace(current=lambda *args:True)
    def current(self,*args):return True
    async def grant(self,*args,**kwargs):return dict(source_name='TELEGRAM_RKB1')
    async def confirm_archive_auth(self,*args):self.confirmed.append(args);return True

def connect(app):return app.websocket_connect('/bge-worker/channel',headers={'Authorization':'Bearer secret'})

@pytest.mark.parametrize('answer',['wrong','missing'])
def test_result_wrong_or_missing_challenge_never_confirms(answer):
    q=RuntimeQueue();q._telegram_runtime_source=owner=Owner()
    with client(q) as app,connect(app) as socket:
        generation=attach(socket)['connection_generation'];ready(socket,generation)
        send(socket,generation,'RKB1_BIND_REQUEST',retained_client=False)
        grant=socket.receive_json();assert grant['type']=='RKB1_BIND_GRANT'
        fields={} if answer=='missing' else dict(challenge='x'*32)
        send(socket,generation,'RKB1_BIND_RESULT',status='authorized',**fields)
        with pytest.raises(WebSocketDisconnect):socket.receive_json()
    assert owner.confirmed==[]

def test_old_challenge_cannot_cross_reconnect():
    q=RuntimeQueue();q._telegram_runtime_source=owner=Owner()
    with client(q) as app:
        with connect(app) as socket:
            generation=attach(socket)['connection_generation'];ready(socket,generation)
            send(socket,generation,'RKB1_BIND_REQUEST',retained_client=False)
            old=socket.receive_json()['challenge']
        with connect(app) as socket:
            generation=attach(socket)['connection_generation'];ready(socket,generation)
            send(socket,generation,'RKB1_BIND_REQUEST',retained_client=True)
            current=socket.receive_json()['challenge'];assert old!=current
            send(socket,generation,'RKB1_BIND_RESULT',status='authorized',challenge=old)
            with pytest.raises(WebSocketDisconnect):socket.receive_json()
    assert owner.confirmed==[]

def test_early_result_while_sender_has_not_issued_grant_is_rejected():
    q=RuntimeQueue();q._telegram_runtime_source=owner=Owner()
    entered=threading.Event();release=threading.Event()
    def blocked_lease(*args):
        entered.set();assert release.wait(3);return True
    owner.lease.current=blocked_lease
    with client(q) as app,connect(app) as socket:
        generation=attach(socket)['connection_generation'];ready(socket,generation)
        send(socket,generation,'RKB1_BIND_REQUEST',retained_client=False)
        assert entered.wait(2)
        try:
            send(socket,generation,'RKB1_BIND_RESULT',status='authorized',challenge='x'*32)
            with pytest.raises(WebSocketDisconnect):socket.receive_json()
            assert owner.confirmed==[]
        finally:release.set()

def test_fast_valid_result_during_send_await_is_accepted(monkeypatch):
    q=RuntimeQueue();q._telegram_runtime_source=owner=Owner()
    confirmed=threading.Event();observed=threading.Event()
    async def confirm(*args):
        owner.confirmed.append(args);confirmed.set();return True
    owner.confirm_archive_auth=confirm
    original=WebSocket.send_text
    async def send_wait(self,raw):
        await original(self,raw)
        if json.loads(raw).get('type')=='RKB1_BIND_GRANT':
            observed.set()
            assert await asyncio.to_thread(confirmed.wait,2)
    monkeypatch.setattr(WebSocket,'send_text',send_wait)
    with client(q) as app,connect(app) as socket:
        generation=attach(socket)['connection_generation'];ready(socket,generation)
        send(socket,generation,'RKB1_BIND_REQUEST',retained_client=False)
        grant=socket.receive_json()
        assert observed.wait(1)
        send(socket,generation,'RKB1_BIND_RESULT',status='authorized',challenge=grant['challenge'])
        assert confirmed.wait(2)
        send(socket,generation,'HEARTBEAT')
        assert socket.receive_json()['type']=='HEARTBEAT_ACK'
    assert owner.confirmed==[('run','instance',generation,True)]
