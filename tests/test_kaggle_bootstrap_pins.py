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


def setup(tmp_path):
    now=[1000.]
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
async def test_worker_emits_bootstrap_observation_without_ready_or_reload():
    transport=module('bge_wss_transport');stop=asyncio.Event();sent=[]
    plan=BootstrapRuntimeExpectation({key:'a'*64 for key in ('code','model','launch_config','executed_source')},
        'bootstrap-db','s1','f1','e1',{'torch':'2.11.0+cpu'},'b'*64,{'001_search.sql':'c'*64})
    value=observation(plan);loads=[]
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


def test_authenticated_wss_observation_stays_closed_until_exact_local_finalization(tmp_path):
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
            for _ in range(5):
                frame=socket.receive_json()
                if frame['type']=='READY_ACK' and frame['status']=='ready':break
            else:pytest.fail('trusted finalization did not promote retained candidate')
            assert s.q.channel_status(s.run,s.token,generation)['ready']
