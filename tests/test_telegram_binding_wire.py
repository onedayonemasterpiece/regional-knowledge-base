"""Real channel/worker paths with synthetic wire and SDK only; no network or PG."""
import asyncio
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
import pytest
from test_bge_channel import Queue,client,attach,ready,send
from test_bge_wss_worker import module,CONFIG

class RuntimeQueue(Queue):
    def channel_status(self,*args):
        return dict(super().channel_status(*args),active=True,runtime_verified=True)

def test_sender_race_returns_pending_then_result_is_separate_from_ready(caplog):
    caplog.set_level('INFO',logger='regional_knowledge.bge_channel')
    q=RuntimeQueue();eligible=[True];race=[True];confirmed=[]
    class Owner:
        def current(self,*args):return eligible[0]
        def lease_current(self,*args):
            if race[0]:eligible[0]=False
            return True
        async def grant(self,*args,**kwargs):
            return dict(run_id='run',runtime_incarnation='instance',source_name='TELEGRAM_RKB1',
                        locator={},bundle_b64='synthetic-not-a-real-session',api_id=1,api_hash='a'*32)
        async def confirm_archive_auth(self,*args):
            confirmed.append(args);return self.current()
    owner=Owner();owner.lease=SimpleNamespace(current=owner.lease_current)
    q._telegram_runtime_source=owner
    with client(q) as app,app.websocket_connect('/bge-worker/channel',headers={'Authorization':'Bearer secret'}) as socket:
        generation=attach(socket)['connection_generation']
        assert ready(socket,generation)['status']=='ready'
        assert confirmed==[]
        send(socket,generation,'RKB1_BIND_REQUEST',retained_client=False)
        pending=socket.receive_json()
        assert pending==dict(type='RKB1_BIND_PENDING',protocol_version=2,connection_generation=generation)
        eligible[0]=True;race[0]=False
        send(socket,generation,'RKB1_BIND_REQUEST',retained_client=False)
        granted=socket.receive_json()
        assert granted['type']=='RKB1_BIND_GRANT' and len(granted['challenge'])==32
        assert confirmed==[]
        send(socket,generation,'RKB1_BIND_RESULT',status='authorized',challenge=granted['challenge'])
        send(socket,generation,'HEARTBEAT')
        assert socket.receive_json()['type']=='HEARTBEAT_ACK'
        assert confirmed==[('run','instance',generation,True)]
    events=[json.loads(r.message) for r in caplog.records if r.name=='regional_knowledge.bge_channel']
    observed=[e for e in events if e.get('event')=='rkb1_archive_authorization']
    assert observed==[dict(event='rkb1_archive_authorization',run_id='run',
        connection_generation=generation,status='authorized',confirmed=True)]
    assert 'synthetic-not-a-real-session' not in caplog.text
    assert 'api_hash' not in caplog.text and granted['challenge'] not in caplog.text

@pytest.mark.asyncio
@pytest.mark.parametrize('fails',[False,True])
async def test_real_worker_pending_retries_and_emits_only_nonsecret_binding_result(fails):
    transport=module('bge_wss_transport')
    stop=asyncio.Event();sent=[];requests=[];received=[]
    class Owner:
        retained_client=False;failed_generation=None;authorized_generation=None
        def current(self):return self.authorized_generation==1
        async def receive(self,frame):
            received.append(frame)
            if fails:
                self.failed_generation=1
                raise ValueError('PRIVATE SYNTHETIC CREDENTIAL ERROR')
            self.authorized_generation=1
    owner=Owner()
    @asynccontextmanager
    async def connect():
        incoming=asyncio.Queue()
        class Socket:
            async def send(self,raw):
                frame=json.loads(raw);sent.append(frame);kind=frame['type']
                if kind=='ATTACH':reply=dict(type='WELCOME',provider_version='85')
                elif kind=='READY':reply=dict(type='READY_ACK',status='ready',provider_version='85')
                elif kind=='HEARTBEAT':reply=dict(type='HEARTBEAT_ACK',provider_version='85')
                elif kind=='RKB1_BIND_REQUEST':
                    requests.append(frame)
                    reply=dict(type='RKB1_BIND_PENDING' if len(requests)==1 else 'RKB1_BIND_GRANT')
                    if len(requests)>1:reply.update(bundle_b64='synthetic-only',api_id=1,api_hash='a'*32,challenge='c'*32)
                elif kind=='RKB1_BIND_RESULT':
                    stop.set();return
                else:raise AssertionError(kind)
                await incoming.put(json.dumps(dict(protocol_version=2,connection_generation=1,**reply)))
            async def receive(self):return await incoming.get()
        yield Socket()
    worker=transport.BgeWssTransport(CONFIG,lambda:lambda text:[1.]+[0.]*1023,lambda:{},
        connector=connect,heartbeat_seconds=.01,maximum_seconds=2,stop=stop)
    worker.original_intake_handler=SimpleNamespace(telegram_channel=owner,
        attached=lambda generation:None,detached=lambda generation:None)
    await worker.run()
    assert len(requests)==2 and len(received)==1
    assert not {'bundle_b64','api_id','api_hash'} & received[0].keys()
    result=[frame for frame in sent if frame['type']=='RKB1_BIND_RESULT']
    assert result==[dict(type='RKB1_BIND_RESULT',status='unavailable' if fails else 'authorized',
                        protocol_version=2,connection_generation=1,challenge='c'*32)]
    assert 'PRIVATE' not in json.dumps(sent)
