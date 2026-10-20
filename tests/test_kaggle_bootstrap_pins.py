"""Real control state; synthetic bounded native catalog and dependency bytes."""
import asyncio
import base64
from dataclasses import replace
import json
from types import SimpleNamespace
from contextlib import asynccontextmanager

import pytest

from regional_knowledge.bge_queue import BgeQueue
from regional_knowledge.kaggle_bootstrap_pins import (BootstrapRuntimeExpectation,canonical,digest,verify_observation)
from regional_knowledge.kaggle_readiness import SearchRuntimeExpectation
from regional_knowledge.kaggle_bootstrap import PreparedRuntime,CandidateReadiness
from test_kaggle_database_probe import catalog
from test_kaggle_target_runtime import bundled_spec,stage0,queue_run,hashes,generate
from regional_knowledge.kaggle_target_launch import persist_empty_intent
from test_recovery_journal import fixture as journal_fixture
from test_bge_wss_worker import CONFIG,Wire,module


def observation(plan):
    dependencies=dict(python='3.13 synthetic observed build',machine='x86_64',libc=['glibc','2.39'],packages=dict(plan.packages))
    records=[[section,name,value] for section,rows in catalog().items() for name,value in rows.items()]
    raw=canonical(dict(format='rkb-native-catalog-v1',records=records))
    readiness=dict(hashes=dict(plan.hashes,dependency=digest(canonical(dependencies)),schema=digest(raw)),
        database_generation=plan.database_generation,schema_revision=plan.schema_revision,
        function_revision=plan.function_revision,executor_revision=plan.executor_revision,
        applied_seq=0,projection_seq=0,visibility_epoch=0,text_search_ready=True,dense_search_ready=True,
        archive_read_ready=None,proof_ready=None)
    return dict(runtime_readiness=readiness,dependencies=dependencies,catalog_b64=base64.b64encode(raw).decode())


def setup(tmp_path,now_value=1000.):
    now=[now_value]
    queue=BgeQueue(tmp_path/'bootstrap.sqlite',clock=lambda:now[0])
    queue.enqueue('owner','synthetic-document',['not-dispatched'],kind='document')
    run=queue.maintenance()[0]['id'];nonce=queue.prepare_channel(run)
    plan=BootstrapRuntimeExpectation({key:'a'*64 for key in ('code','model','launch_config','executed_source')},
        'bootstrap-db','s1','f1','e1',{'torch':'2.11.0+cpu'},'b'*64,{'001_search.sql':'c'*64},seconds=60)
    queue.expect_bootstrap_runtime(run,plan)
    token=queue.launch_claim(run);queue.launch_record(run,'owner/stable-bootstrap',provider_version=7)
    generation=queue.attach_channel(run,token,nonce,'retained-incarnation')['connection_generation']
    value=observation(plan)
    expected=SearchRuntimeExpectation(value['runtime_readiness']['hashes'],'bootstrap-db','s1','f1','e1')
    return SimpleNamespace(q=queue,run=run,token=token,nonce=nonce,generation=generation,plan=plan,
        value=value,expected=expected,now=now)


def test_bootstrap_is_not_ready_and_trusted_finalization_cannot_replace_anchors(tmp_path):
    s=setup(tmp_path)
    receipt=s.q.record_bootstrap_observation(s.run,s.token,s.generation,s.value)
    assert not s.q.accept_search_runtime(s.run,s.token,s.generation,s.value['runtime_readiness']).ok
    assert not s.q.channel_status(s.run,s.token,s.generation)['ready']
    bad=replace(s.expected,hashes=dict(s.expected.hashes,code='f'*64))
    with pytest.raises(ValueError,match='reviewed final pins'):
        s.q.finalize_bootstrap_runtime(s.run,bad,observation_sha256=receipt,review_evidence='retained-source-review')
    with pytest.raises(ValueError,match='immutable'):
        s.q.expect_bootstrap_runtime(s.run,replace(s.plan,seconds=120))
    s.q.finalize_bootstrap_runtime(s.run,s.expected,observation_sha256=receipt,review_evidence='retained-source-review')
    assert s.q.accept_search_runtime(s.run,s.token,s.generation,s.value['runtime_readiness']).ok
    s.q.heartbeat(s.run,s.token,ready=True,connection_generation=s.generation)
    assert s.q.channel_status(s.run,s.token,s.generation)['ready']
    with pytest.raises(ValueError,match='unfinalized'):
        s.q.finalize_bootstrap_runtime(s.run,bad,observation_sha256=receipt,review_evidence='cannot-update-active')
    # Same source/runtime reconnect re-emits evidence without changing pins.
    s.q.detach_channel(s.run,s.token,s.generation)
    newer=s.q.attach_channel(s.run,s.token,s.nonce,'retained-incarnation')['connection_generation']
    assert s.q.record_bootstrap_observation(s.run,s.token,newer,s.value)==receipt


@pytest.mark.parametrize('change',['digest','deadline','attachment','packages','catalog'])
def test_unknown_changed_or_stale_bootstrap_evidence_never_finalizes(tmp_path,change):
    s=setup(tmp_path)
    if change=='packages':
        s.value['dependencies']['packages']['torch']='unapproved'
        with pytest.raises(ValueError,match='dependency'):
            s.q.record_bootstrap_observation(s.run,s.token,s.generation,s.value)
        return
    if change=='catalog':
        s.value['catalog_b64']=base64.b64encode(b'{}').decode()
        with pytest.raises(ValueError,match='catalog'):
            s.q.record_bootstrap_observation(s.run,s.token,s.generation,s.value)
        return
    receipt=s.q.record_bootstrap_observation(s.run,s.token,s.generation,s.value)
    if change=='digest':receipt='0'*64
    if change=='deadline':s.now[0]+=61
    if change=='attachment':
        s.q.detach_channel(s.run,s.token,s.generation)
        s.q.attach_channel(s.run,s.token,s.nonce,'retained-incarnation')
    with pytest.raises(ValueError,match='exact current'):
        s.q.finalize_bootstrap_runtime(s.run,s.expected,observation_sha256=receipt,review_evidence='review')


@pytest.mark.asyncio
async def test_generated_bootstrap_freezes_partial_pins_before_attach_with_none_not_zero(tmp_path):
    target=bundled_spec();target=replace(target,stage0=stage0(target),bootstrap_only=True,
        dependency_sha256=None,schema_sha256=None)
    queue,run=queue_run(tmp_path);_,journal=journal_fixture()
    await persist_empty_intent(queue,run,target,launch_nonce=queue.prepare_channel(run),
        recovery_journal=journal,sql_hashes=hashes())
    source=generate(queue,run,target)
    assert source==generate(queue,run,target)
    with queue.connect(write=False) as db:
        expected=json.loads(queue._runtime_row(db,run)['expected_json'])
    assert expected['codec']=='rkb.bootstrap-expectation.v1'
    assert set(expected['hashes'])=={'code','model','launch_config','executed_source'}
    assert expected['hashes']['executed_source']==digest(source.encode())
    assert expected['native_manifest_sha256']==target.native_bundle.manifest_sha256


@pytest.mark.asyncio
@pytest.mark.parametrize('catalog_padding',(0,230000))
async def test_worker_emits_bootstrap_observation_without_ready_or_reload(catalog_padding):
    transport=module('bge_wss_transport');stop=asyncio.Event();sent=[]
    plan=BootstrapRuntimeExpectation({key:'a'*64 for key in ('code','model','launch_config','executed_source')},
        'bootstrap-db','s1','f1','e1',{'torch':'2.11.0+cpu'},'b'*64,{'001_search.sql':'c'*64})
    value=observation(plan);loads=[]
    if catalog_padding:
        native=json.loads(base64.b64decode(value['catalog_b64']))
        definition=next(row[2] for row in native['records'] if row[0]=='functions')
        definition['definition']+='\n-- '+('x'*catalog_padding)
        raw=canonical(native)
        value['catalog_b64']=base64.b64encode(raw).decode()
        value['runtime_readiness']['hashes']['schema']=digest(raw)
        assert 262144<len(canonical(value))<1536*1024
    verify_observation(plan,value)
    async def prepare(encoder,**kwargs):
        async def query(frame,*,encode):raise AssertionError('never ready')
        return PreparedRuntime(query,CandidateReadiness(**value['runtime_readiness']),bootstrap_observation=value)
    def load():loads.append(True);return lambda text:[1.]+[0.]*1023
    @asynccontextmanager
    async def connect():
        incoming=asyncio.Queue()
        class Socket:
            async def send(self,raw):
                frame=json.loads(raw);sent.append(frame)
                if frame['type']=='ATTACH':
                    await incoming.put(json.dumps(dict(type='WELCOME',protocol_version=2,
                        connection_generation=1,provider_version='7')))
                if frame['type']=='BOOTSTRAP_OBSERVATION':stop.set()
            async def receive(self):return await incoming.get()
        yield Socket()
    worker=transport.BgeWssTransport(dict(CONFIG,shared_reads=True,target_runtime=dict(bootstrap_only=True,
        bootstrap_seconds=30)),load,lambda:{},prepare_runtime=prepare,connector=connect,
        stop=stop,heartbeat_seconds=.01,maximum_seconds=3)
    await worker.run()
    assert len(loads)==1 and any(frame['type']=='BOOTSTRAP_OBSERVATION' for frame in sent)
    assert not any(frame['type']=='READY' for frame in sent)


def test_authenticated_wss_observation_stays_closed_without_shared_admission(tmp_path):
    from starlette.applications import Starlette
    from starlette.testclient import TestClient
    from regional_knowledge.bge_channel import channel_route
    s=setup(tmp_path)
    s.q.detach_channel(s.run,s.token,s.generation)
    app=Starlette(routes=[channel_route(s.q)])
    with TestClient(app) as client:
        with client.websocket_connect('/bge-worker/channel',headers={'Authorization':'Bearer '+s.token}) as socket:
            socket.send_json(dict(type='ATTACH',protocol_version=2,run_id=s.run,launch_nonce=s.nonce,
                runtime_incarnation='retained-incarnation'))
            welcome=socket.receive_json();generation=welcome['connection_generation']
            socket.send_json(dict(type='BOOTSTRAP_OBSERVATION',protocol_version=2,
                connection_generation=generation,observation=s.value))
            acknowledgment=socket.receive_json()
            assert acknowledgment['type']=='BOOTSTRAP_ACK' and acknowledgment['status']=='observed_not_ready'
            assert not s.q.channel_status(s.run,s.token,generation)['ready']
            s.q.finalize_bootstrap_runtime(s.run,s.expected,
                observation_sha256=acknowledgment['observation_sha256'],review_evidence='independent native/catalog review')
            frame=socket.receive_json()
            assert frame['type']=='READY_ACK' and frame['status']=='runtime_not_ready'
            assert frame['reasons']==['shared_admission_required']
            assert not s.q.channel_status(s.run,s.token,generation)['ready']


@pytest.mark.asyncio
@pytest.mark.parametrize('finalize',(False,True))
async def test_generated_bootstrap_transport_idle_reaches_real_default_asgi_queue(tmp_path,monkeypatch,finalize):
    """Generated transport bytes, real local WebSocket, stub encoder/native proof."""
    import ast
    import time
    import socket as sockets
    import aiohttp
    import uvicorn
    from regional_knowledge.server import build_server
    from regional_knowledge.bge_contract import SPACE,REVISION
    from test_bge_wss_worker import ROOT
    s=setup(tmp_path,now_value=time.time())
    s.q.detach_channel(s.run,s.token,s.generation)
    controller=module('bge_kaggle_controller')
    generated_queue,generated_run=queue_run(tmp_path/'generated')
    source=controller.worker_source((ROOT/'scripts/production/bge_kaggle_worker.py').read_text(),
        generated_queue,generated_run,'synthetic-token','https://fixture.example/bge-worker',transport='wss')
    # Execute precisely the outbound helper embedded in generated launch bytes;
    # leave model/runtime entrypoint functions inert and inject bounded fixtures.
    tree=ast.parse(source)
    names={'BgeChannelStopped','_channel_json','BgeWssTransport'}
    body=[node for node in tree.body if isinstance(node,(ast.Import,ast.ImportFrom))
        or getattr(node,'name',None) in names]
    namespace={'SPACE':SPACE,'REVISION':REVISION}
    exec(compile(ast.Module(body=body,type_ignores=[]),'generated-bootstrap-transport','exec'),namespace)
    monkeypatch.setenv('RKB_DEV_NOAUTH','1')
    monkeypatch.setenv('RKB_BGE_QUEUE_PATH',str(s.q.path))
    app=build_server().streamable_http_app()
    listener=sockets.socket();listener.bind(('127.0.0.1',0));listener.listen(5)
    port=listener.getsockname()[1]
    server=uvicorn.Server(uvicorn.Config(app,log_level='error',lifespan='off',ws='websockets-sansio'))
    serving=asyncio.create_task(server.serve(sockets=[listener]))
    stop=asyncio.Event();sent=[];received=[];loads=[]
    @asynccontextmanager
    async def connect():
        async with aiohttp.ClientSession() as client:
            async with client.ws_connect(f'http://127.0.0.1:{port}/bge-worker/channel',
                    headers={'Authorization':'Bearer '+s.token}) as wire:
                class Socket:
                    async def send(self,raw):
                        sent.append(json.loads(raw));await wire.send_str(raw)
                    async def receive(self):
                        message=await wire.receive()
                        if message.type!=aiohttp.WSMsgType.TEXT:raise ConnectionError('fixture wire closed')
                        frame=json.loads(message.data);received.append(frame)
                        if frame['type']=='BOOTSTRAP_ACK':
                            if not finalize:stop.set()
                            elif frame['status']=='observed_not_ready':
                                s.q.finalize_bootstrap_runtime(s.run,s.expected,observation_sha256=frame['observation_sha256'],review_evidence='synthetic native fixture pin review')
                            elif frame['status']=='pins_finalized':
                                assert worker.bootstrap_finalized is False
                                asyncio.get_running_loop().call_later(1.1,stop.set)

                        return message.data
                yield Socket()
    def load():
        loads.append(True)
        return lambda text:(_ for _ in ()).throw(AssertionError('bootstrap cannot encode documents'))
    async def prepare(encoder,**kwargs):
        async def query(frame,*,encode):raise AssertionError('bootstrap cannot query')
        return PreparedRuntime(query,CandidateReadiness(**s.value['runtime_readiness']),bootstrap_observation=s.value)
    worker=namespace['BgeWssTransport'](dict(CONFIG,run_id=s.run,token=s.token,launch_nonce=s.nonce,
        shared_reads=True,target_runtime=dict(bootstrap_only=True,bootstrap_seconds=1 if finalize else 30)),load,lambda:{},
        prepare_runtime=prepare,connector=connect,heartbeat_seconds=.02,maximum_seconds=5,stop=stop)
    worker.incarnation='retained-incarnation'
    try:
        for _ in range(100):
            if server.started:break
            await asyncio.sleep(.005)
        assert server.started
        await asyncio.wait_for(worker.run(),5)
        assert len(loads)==1
        assert worker.bootstrap_pins_finalized==finalize and not worker.bootstrap_finalized
        if finalize:assert any(f.get('status')=='pins_finalized' for f in received)
        idle=[frame for frame in sent if frame['type']=='RESOURCE_STATE']
        assert idle and all(frame['state']=='idle' for frame in idle)
        assert any(frame['type']=='BOOTSTRAP_ACK' for frame in received)
        assert not any(frame['type'] in ('DISPATCH','QUERY','PASSAGE_READ') for frame in received)
        assert not any(frame['type']=='READY_ACK' and frame['status']=='ready' for frame in received)
        with s.q.connect(write=False) as db:
            row=db.execute('SELECT status FROM runs WHERE id=?',(s.run,)).fetchone()
            assert row['status']=='starting'
            assert db.execute('SELECT attempt FROM jobs').fetchone()[0]==0
    finally:
        stop.set();server.should_exit=True
        await asyncio.wait_for(serving,3);listener.close()


@pytest.mark.parametrize('change',['busy','resource','no_plan','expired','finalized'])
def test_default_channel_rejects_resource_state_outside_finite_idle_bootstrap(tmp_path,change):
    from starlette.applications import Starlette
    from starlette.testclient import TestClient
    from starlette.websockets import WebSocketDisconnect
    from regional_knowledge.bge_channel import channel_route
    s=setup(tmp_path);s.q.detach_channel(s.run,s.token,s.generation)
    if change=='no_plan':
        with s.q.connect() as db:db.execute('DELETE FROM bge_runtime_expectations WHERE run_id=?',(s.run,))
    if change=='expired':s.now[0]+=61
    if change=='finalized':
        with s.q.connect() as db:db.execute('UPDATE bge_runtime_bootstrap SET finalized=1 WHERE run_id=?',(s.run,))
    with TestClient(Starlette(routes=[channel_route(s.q)])) as client:
        with client.websocket_connect('/bge-worker/channel',headers={'Authorization':'Bearer '+s.token}) as socket:
            socket.send_json(dict(type='ATTACH',protocol_version=2,run_id=s.run,launch_nonce=s.nonce,
                runtime_incarnation='retained-incarnation'))
            welcome=socket.receive_json()
            frame=dict(type='RESOURCE_STATE',protocol_version=2,
                connection_generation=welcome['connection_generation'],state='busy' if change=='busy' else 'idle')
            if change=='resource':frame['resource_id']='not-allowed'
            socket.send_json(frame)
            with pytest.raises(WebSocketDisconnect):socket.receive_json()


def test_prepared_pins_extend_only_post_verification_heartbeat_lifetime_without_ready(tmp_path):
    from regional_knowledge.bge_queue import MAX_LIFETIME,HEARTBEAT_SECONDS
    s=setup(tmp_path)
    assert s.q.bootstrap_pins_current(s.run,s.token,s.generation) is None
    receipt=s.q.record_bootstrap_observation(s.run,s.token,s.generation,s.value)
    assert s.q.bootstrap_pins_current(s.run,s.token,s.generation) is None
    s.q.finalize_bootstrap_runtime(s.run,s.expected,observation_sha256=receipt,review_evidence='independent retained fixture review')
    proof=s.q.bootstrap_pins_current(s.run,s.token,s.generation)
    assert proof['hashes']==dict(s.expected.hashes) and proof['observation_sha256']==receipt
    for _ in range(16):
        s.now[0]+=60;s.q.heartbeat(s.run,s.token,connection_generation=s.generation)
    status=s.q.channel_status(s.run,s.token,s.generation)
    assert status['state']=='starting' and not status['ready']
    with s.q.connect(write=False) as db:
        row=db.execute('SELECT * FROM runs WHERE id=?',(s.run,)).fetchone()
        expected=s.q._runtime_row(db,s.run)
        assert row['bootstrap_finalized_at']==1000 and expected['accepted_generation'] is None
        assert not s.q._run_expired(row,s.now[0])
        assert s.q._run_expired(row,s.now[0]+HEARTBEAT_SECONDS+1)
        alive=dict(row,heartbeat=row['started']+MAX_LIFETIME,useful=row['started']+MAX_LIFETIME)
        assert s.q._run_expired(alive,row['started']+MAX_LIFETIME)
    from regional_knowledge.bge_queue import StaleAttachment
    with pytest.raises(StaleAttachment,match='runtime readiness required'):
        s.q.claim(s.run,s.token,connection_generation=s.generation)
    s.q.detach_channel(s.run,s.token,s.generation)
    newer=s.q.attach_channel(s.run,s.token,s.nonce,'retained-incarnation')['connection_generation']
    assert s.q.bootstrap_pins_current(s.run,s.token,newer)==proof


def test_unfinalized_heartbeats_never_extend_the_original_cold_budget(tmp_path):
    s=setup(tmp_path)
    for _ in range(14):
        s.now[0]+=60;s.q.heartbeat(s.run,s.token,connection_generation=s.generation)
    with s.q.connect(write=False) as db:row=db.execute('SELECT * FROM runs WHERE id=?',(s.run,)).fetchone()
    assert row['bootstrap_finalized_at'] is None and s.q._run_expired(row,1901)


def test_prepared_ack_requires_exact_retained_observation_and_never_marks_serving_ready(tmp_path):
    s=setup(tmp_path);transport=module('bge_wss_transport');calls=[]
    async def prepare(*args,**kwargs):raise AssertionError('no model/native effects')
    worker=transport.BgeWssTransport(dict(CONFIG,run_id=s.run,shared_reads=True,target_runtime=dict(bootstrap_only=True)),
        lambda:None,lambda:{},prepare_runtime=prepare)
    worker.incarnation='retained-incarnation'
    async def query(*args,**kwargs):raise AssertionError('no queries')
    worker.prepared_runtime=PreparedRuntime(query,CandidateReadiness(**s.value['runtime_readiness']),bootstrap_observation=s.value)
    worker._overall_deadline=40000;worker._lifetime_timeout=SimpleNamespace(reschedule=calls.append)
    frame=dict(type='BOOTSTRAP_ACK',protocol_version=2,connection_generation=s.generation,status='pins_finalized',run_id=s.run,
        runtime_incarnation=worker.incarnation,provider_version='7',observation_sha256=digest(canonical(s.value)),hashes=dict(s.expected.hashes))
    for key,value in [('run_id','other-run'),('runtime_incarnation','other-incarnation'),('connection_generation',99),
            ('provider_version','8'),('hashes',dict(s.expected.hashes,model='f'*64)),('observation_sha256','f'*64)]:
        with pytest.raises(ValueError):worker._accept_bootstrap_pins(dict(frame,**{key:value}),s.generation,'7')
        assert not worker.bootstrap_pins_finalized and calls==[]
    worker._accept_bootstrap_pins(frame,s.generation,'7')
    assert worker.bootstrap_pins_finalized and not worker.bootstrap_finalized and calls==[40000]
