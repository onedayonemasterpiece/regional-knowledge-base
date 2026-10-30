"""Synthetic acceptance harness; never loads a model or opens a native DB."""
import asyncio
from dataclasses import replace
import hashlib
import json
import time
from types import SimpleNamespace
from uuid import UUID

import pytest

from regional_knowledge.acceptance_observer import AcceptanceObserver,FrozenAcceptanceFixture
from regional_knowledge.kaggle_acceptance_contracts import AcceptanceReadRequest,AcceptanceReadResult,BranchRanks
from regional_knowledge.kaggle_protocol import Query,QueryFilters,QueryResult,SourceDependency,decode_envelope,encode_envelope
from regional_knowledge.kaggle_public_backend import TargetReadBinding
from regional_knowledge.kaggle_search import fused_search
from regional_knowledge.kaggle_worker_query import KaggleWorkerQuery
from regional_knowledge.source_read_gate import SourceReadGate
from test_bge_admission import state,release
from test_control_spool import env,stage,SOURCE,OTHER
from test_kaggle_worker_query import Lease,pinned
from test_kaggle_pg_search import FixtureConnection
from test_read_authority import auth,issue
from test_participants import people

VECTOR=[1.]+[0.]*1023


def pid(index):return str(UUID(int=100+index))


def binding(**kw):
    return TargetReadBinding(**(dict(run_id='run',runtime_incarnation='inc',connection_generation=1,
        database_generation='db-source',min_data_seq=0,provider_version='7')|kw))


def fixture(owner,b=None):return FrozenAcceptanceFixture(owner,(SOURCE,OTHER),'b'*64,b or binding(),0,
    'd'*64,(hashlib.sha256(b'synthetic harbour').hexdigest(),),tuple(SourceDependency(
        source_id=s,content_revision=1,eligibility_revision=1) for s in (SOURCE,OTHER)))


def request(f,authority=None,**kw):
    actor=dict(person_id=authority.person_id if authority else f.owner_id,
        audience=authority.destination.key if authority else 'mcp:personal:acceptance',
        auth_epoch=authority.auth_revision if authority else 0,visibility_epoch=0,
        context_ref=authority.context_ref if authority else None)
    return AcceptanceReadRequest(**(dict(request_id='acceptance-1',connection_generation=f.binding.connection_generation,
        turn_generation=0,query='synthetic harbour',min_data_seq=0,deadline_ms=2000,
        fixture_sha256=f.sha256,fixture_sources=f.source_revisions,actor=actor,
        filters=QueryFilters(source_ids=f.source_ids,kinds=('source',)))|kw))


class BranchConnection(FixtureConnection):
    def __init__(self,missing=False):
        super().__init__();self.missing=missing
    async def fetchone(self):return (0,0,0,True,True,'db-source')
    async def fetchall(self):
        # Shared ranks11..15 beat unique top10 under RRF. Therefore hydrating
        # only both branch top10 misses actual fused top10 evidence.
        if 'lexical_candidates_scoped' in self.statement:
            return [(UUID(pid(i)),.9) for i in (*range(11,21),*range(21,26))]
        if 'dense_candidates_scoped' in self.statement:
            return [(UUID(pid(i)),.5) for i in (*range(1,11),*range(21,26))]
        if 'hydrate_scoped' in self.statement:
            ids=self.statements[-1][1][0]
            return [(v,UUID(SOURCE if int(v)%2 else OTHER),1,'page 1','synthetic body','citation',None,False)
                for v in (ids[:-1] if self.missing else ids)]
        if 'SELECT a.source_id,a.revision,v.eligibility_revision' in self.statement:
            return [(UUID(SOURCE),1,1),(UUID(OTHER),1,1)]
        return []


@pytest.mark.asyncio
async def test_actual_worker_shared_branches_once_same_snapshot_hydrates_full_top10_union():
    f=fixture(str(UUID(int=90)));lease=Lease(BranchConnection());encoded=[];counted=[]
    async def count(text):counted.append(text);return 12
    async def encode(text):encoded.append(text);return VECTOR
    handler=KaggleWorkerQuery(acquire=lease.acquire,tokenizer=pinned(count))
    wire=await handler(json.loads(request(f,compute_claim='real-slot').model_dump_json()),encode=encode)
    result=decode_envelope(json.dumps(wire).encode())
    assert type(result) is AcceptanceReadResult and result.mode=='hybrid'
    assert result.ranks.bge==tuple(pid(i) for i in range(1,11))
    assert result.ranks.lexical==tuple(pid(i) for i in range(11,21))
    assert set(pid(i) for i in range(21,26))<=set(result.ranks.fusion)
    assert len(result.ranks.fusion)==10 and len(result.evidence)==25
    assert set(i.id for i in result.evidence)==set(result.ranks.bge+result.ranks.lexical+result.ranks.fusion)
    assert encoded==counted==['synthetic harbour'] and lease.entered==lease.exited==1
    statements=[s for s,_ in lease.connection.statements]
    assert statements.count('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')==1
    assert sum('lexical_candidates_scoped' in s for s in statements)==1
    assert sum('dense_candidates_scoped' in s for s in statements)==1
    assert sum('hydrate_scoped' in s for s in statements)==1
    assert type(decode_envelope(encode_envelope(result))) is AcceptanceReadResult


@pytest.mark.asyncio
async def test_incomplete_hydration_or_failed_dense_cannot_report_branch_ranks():
    f=fixture(str(UUID(int=90)))
    async def encode(_):return VECTOR
    for missing,dense in ((True,encode),(False,None)):
        async def broken(_):raise RuntimeError('synthetic unavailable')
        handler=KaggleWorkerQuery(acquire=Lease(BranchConnection(missing)).acquire,tokenizer=pinned())
        wire=await handler(json.loads(request(f,compute_claim='slot').model_dump_json()),encode=dense or broken)
        assert wire['status']=='unavailable' and wire['mode'] is None
        assert wire['evidence']==[] and not any(wire['ranks'].values())


@pytest.mark.asyncio
async def test_missing_or_changed_fixture_source_fails_before_encode_even_with_empty_ranks():
    f=fixture(str(UUID(int=90)))
    class Changed(BranchConnection):
        async def fetchall(self):
            if 'SELECT a.source_id,a.revision,v.eligibility_revision' in self.statement:
                return [(UUID(SOURCE),2,1)]
            raise AssertionError('changed fixture reached candidate retrieval')
    async def forbidden(_):raise AssertionError('changed fixture reached inference')
    wire=await KaggleWorkerQuery(acquire=Lease(Changed()).acquire,tokenizer=pinned())(
        json.loads(request(f,compute_claim='slot').model_dump_json()),encode=forbidden)
    assert wire['status']=='unavailable' and wire['reason']=='evidence_withdrawn'
    assert wire['evidence']==[] and not any(wire['ranks'].values())


@pytest.mark.asyncio
async def test_default_query_still_hydrates_only_three_identical_fused_ids():
    f=fixture(str(UUID(int=90)));normal_lease=Lease(BranchConnection());acceptance_lease=Lease(BranchConnection())
    q=request(f,compute_claim='slot')
    ordinary=Query.model_validate(q.model_dump(exclude={'type','operation','operation_version','fixture_sha256','fixture_sources'}))
    async def encode(_):return VECTOR
    normal=await KaggleWorkerQuery(acquire=normal_lease.acquire,tokenizer=pinned())(
        json.loads(ordinary.model_dump_json()),encode=encode)
    observed=await KaggleWorkerQuery(acquire=acceptance_lease.acquire,tokenizer=pinned())(
        json.loads(q.model_dump_json()),encode=encode)
    assert normal['type']=='QUERY_RESULT' and normal['mode']=='hybrid' and len(normal['evidence'])==3
    assert {v['id'] for v in normal['evidence']}==set(observed['ranks']['fusion'][:3])
    full=await fused_search(BranchConnection(),'synthetic harbour',encode=encode,top_k=10,
        filters=QueryFilters(source_ids=(SOURCE,OTHER),kinds=('source',)))
    assert len(full.evidence)==10


@pytest.mark.asyncio
async def test_k10_worker_delivers_ten_bounded_passages_in_synthetic_subsecond_fixture():
    f=fixture(str(UUID(int=90)));lease=Lease(BranchConnection())
    q=request(f,compute_claim='slot').model_dump(exclude={'type','operation','operation_version','fixture_sha256','fixture_sources'})
    ordinary=Query.model_validate(q|{'evidence_limit':10})
    async def encode(_):return VECTOR
    began=time.perf_counter()
    wire=await KaggleWorkerQuery(acquire=lease.acquire,tokenizer=pinned())(
        json.loads(ordinary.model_dump_json()),encode=encode)
    fixture_elapsed_ms=(time.perf_counter()-began)*1000
    assert wire['status']=='ok' and len(wire['evidence'])==10
    assert len(decode_envelope(json.dumps(wire).encode()).evidence)==10
    assert fixture_elapsed_ms<1000  # Synthetic worker timing, not a live BGE/PG SLA.


def test_acceptance_contract_rejects_modified_fixture_and_duplicate_ranks():
    f=fixture(str(UUID(int=90)))
    for change in ({'filters':QueryFilters(source_ids=(SOURCE,),kinds=('source',))},
                   {'filters':QueryFilters(source_ids=(SOURCE,OTHER),kinds=('story',))},
                   {'fixture_sha256':'not-a-hash'},{'evidence_limit':10},{'evidence_limit':3.0}):
        with pytest.raises(ValueError):request(f,**change)
    with pytest.raises(ValueError):BranchRanks(bge=(pid(1),pid(1)))
    with pytest.raises(ValueError):AcceptanceReadResult(request_id='r',connection_generation=1,turn_generation=0,
        status='degraded',reason='bounded_retrieval',mode='hybrid',observed_data_seq=0,
        observed_projection_seq=0,ranks=BranchRanks(bge=(pid(1),)))


def test_transport_unavailable_preserves_distinct_operation_and_no_ranks_or_fake_success():
    from test_bge_wss_worker import module
    q=request(fixture(str(UUID(int=90))),compute_claim='slot')
    wire=module('bge_wss_transport').BgeWssTransport._unavailable(
        json.loads(q.model_dump_json()),'deadline_exceeded')
    result=decode_envelope(json.dumps(wire).encode())
    assert type(result) is AcceptanceReadResult and result.mode is None
    assert result.status=='unavailable' and not result.evidence and result.ranks==BranchRanks()


async def owner_context(auth):
    tokens=await issue(auth.provider)
    return await auth.resolver.resolve_oauth(tokens.access_token,destination=auth.destination)


@pytest.mark.asyncio
async def test_fixture_inactive_nonowner_copied_authority_and_runtime_or_gold_change_denied(env,auth):
    b=binding();gate=SourceReadGate(auth.resolver,env.spool,binding_current=lambda v:v==b)
    owner=await owner_context(auth);f=fixture(owner.person_id,b);q=request(f,owner)
    with pytest.raises(PermissionError):await gate.dispatch(owner,q,b)
    gate._acceptance_fixture=f
    other=await auth.resolver.resolve_oauth(auth.tokens.access_token,destination=auth.destination)
    with pytest.raises(PermissionError):await gate.dispatch(other,request(f,other),b)
    with pytest.raises(PermissionError):await gate.dispatch(replace(owner),q,b)
    with pytest.raises(PermissionError):await gate.dispatch(owner,q.model_copy(update={'fixture_sha256':'c'*64}),b)
    with pytest.raises(PermissionError):await gate.dispatch(owner,q.model_copy(update={'query':'unfrozen probe'}),b)
    with pytest.raises(PermissionError):await gate.dispatch(owner,q,replace(b,runtime_incarnation='changed'))




@pytest.mark.asyncio
async def test_deactivated_fixture_and_revoked_canonical_owner_withhold_output(env,auth):
    b=binding();owner=await owner_context(auth);f=fixture(owner.person_id,b)
    gate=SourceReadGate(auth.resolver,env.spool,binding_current=lambda v:v==b)
    gate._acceptance_fixture=f
    env.spool._source_anchor=SimpleNamespace(database_generation='db-source',current=lambda:True)
    q=request(f,owner)
    empty=AcceptanceReadResult(request_id=q.request_id,connection_generation=q.connection_generation,
        turn_generation=0,status='degraded',mode='hybrid',reason='bounded_retrieval',
        observed_data_seq=0,observed_projection_seq=0,observed_visibility_epoch=0,observed_database_generation='db-source')
    gated=await gate.finish(await gate.dispatch(owner,q,b),empty)
    gate._acceptance_fixture=None
    assert not gate.issuance_current(gated.ticket)
    gate._acceptance_fixture=f
    auth.p.corpus.put('rkb_users',[{'id':owner.person_id,'status':'disabled'}])
    with pytest.raises(PermissionError):await gate.refresh(gated)


@pytest.mark.asyncio
async def test_real_shared_admission_no_fixture_no_dispatch_then_exact_pair_only(state):
    s=state;owner=await owner_context(s.auth)
    b=binding(run_id=s.run,runtime_incarnation='incarnation-a',connection_generation=s.generation)
    f=fixture(owner.person_id,b);q=request(f,owner)
    with pytest.raises(PermissionError):await s.a.begin_read(owner,q,dialog_ref='bench',deadline=time.monotonic()+1.9)
    s.a._acceptance_fixture=f
    ticket=await s.a.begin_read(owner,q,dialog_ref='bench',deadline=time.monotonic()+1.9)
    work=await s.q.next_channel_work(s.run,s.token,s.generation)
    assert type(work.query) is AcceptanceReadRequest and work.query.compute_claim
    s.a.mark_work_sent(work)
    values=dict(request_id=q.request_id,connection_generation=q.connection_generation,turn_generation=0,
        status='degraded',mode='hybrid',reason='bounded_retrieval',observed_data_seq=0,observed_projection_seq=0)
    with pytest.raises(ValueError):await s.a.finish_read(s.run,s.token,s.generation,QueryResult(**values))
    assert await s.a.finish_read(s.run,s.token,s.generation,AcceptanceReadResult(**values))
    assert type(await ticket.result) is AcceptanceReadResult
    await release(s,work)


@pytest.mark.asyncio
async def test_real_generated_transport_asgi_admission_gate_and_final_private_issuance(state,env,monkeypatch):
    import aiohttp
    import socket
    import uvicorn
    from contextlib import asynccontextmanager
    from starlette.applications import Starlette
    from regional_knowledge.bge_channel import channel_route
    from test_bge_wss_worker import module,CONFIG
    import regional_knowledge.acceptance_observer as observer_module

    s=state;owner=await owner_context(s.auth)
    server=uvicorn.Server(uvicorn.Config(Starlette(routes=[channel_route(s.q)]),
        log_level='error',lifespan='off',ws='websockets-sansio'))
    listener=socket.socket();listener.bind(('127.0.0.1',0));listener.listen(5)
    port=listener.getsockname()[1]
    serving=asyncio.create_task(server.serve(sockets=[listener]))
    ready=asyncio.Event();stop=asyncio.Event();frames=[];encoded=[];worker_task=None
    @asynccontextmanager
    async def connect():
        async with aiohttp.ClientSession() as client:
            async with client.ws_connect(f'http://127.0.0.1:{port}/bge-worker/channel',
                    headers={'Authorization':'Bearer '+s.token}) as websocket:
                class Adapter:
                    async def send(self,raw):
                        frames.append(('worker',json.loads(raw)));await websocket.send_str(raw)
                    async def receive(self):
                        message=await websocket.receive()
                        if message.type not in (aiohttp.WSMsgType.TEXT,aiohttp.WSMsgType.BINARY):
                            raise ConnectionError('fixture channel closed')
                        frame=json.loads(message.data);frames.append(('controller',frame))
                        if frame['type']=='READY_ACK' and frame['status']=='ready':ready.set()
                        return message.data
                yield Adapter()
    def load():
        def encode(text):encoded.append(text);return VECTOR
        return encode
    async def until(predicate):
        async with asyncio.timeout(3):
            while not predicate():await asyncio.sleep(.005)
    try:
        await until(lambda:server.started)
        transport=module('bge_wss_transport')
        worker=transport.BgeWssTransport(dict(CONFIG,run_id=s.run,token=s.token,
            launch_nonce=s.nonce,shared_reads=True),load,lambda:{},connector=connect,
            query_handler=KaggleWorkerQuery(acquire=Lease(BranchConnection()).acquire,tokenizer=pinned()),
            heartbeat_seconds=.02,maximum_seconds=8,stop=stop)
        # state already attached this exact synthetic incarnation; this is a
        # reconnect of its owned process, never a competing runtime attach.
        worker.incarnation='incarnation-a'
        worker_task=asyncio.create_task(worker.run());await asyncio.wait_for(ready.wait(),3)
        generation=next(frame['connection_generation'] for direction,frame in frames
            if direction=='controller' and frame['type']=='WELCOME')
        b=binding(run_id=s.run,runtime_incarnation=worker.incarnation,connection_generation=generation)
        monkeypatch.setattr(observer_module,'current_binding',lambda queue:b)
        gate=SourceReadGate(s.auth.resolver,env.spool,binding_current=lambda value:value==b)
        env.spool._source_anchor=SimpleNamespace(database_generation='db-source',current=lambda:True)
        observer=AcceptanceObserver(queue=s.q,gate=gate,fixture=fixture(owner.person_id,b))
        consumed=[]
        await observer.observe('synthetic harbour',authority=owner,
            request_id='actual-acceptance-wire',consume=consumed.append)
        assert len(consumed)==1
        issued=json.loads(consumed[0]);assert len(issued['items'])==25
        assert len(issued['ranks']['bge'])==len(issued['ranks']['lexical'])==len(issued['ranks']['fusion'])==10
        assert issued['mode']=='hybrid' and issued['reason']=='bounded_retrieval'
        assert b'synthetic body' not in consumed[0] and b'query' not in consumed[0]
        assert encoded==['synthetic harbour']
        assert len([f for d,f in frames if d=='controller' and f['type']=='ACCEPTANCE_READ'])==1
        response=next(f for d,f in frames if d=='worker' and f['type']=='ACCEPTANCE_READ_RESULT')
        assert len(response['evidence'])==25 and 'vectors' not in response
        await until(lambda:s.a.status()['state']=='idle')
        with s.q.connect(write=False) as db:
            assert db.execute('SELECT count(*) FROM jobs').fetchone()[0]==1
        assert not gate._permits and not gate._tickets
        with pytest.raises(PermissionError):await s.auth.resolver.require(owner,'public_search')
    finally:
        stop.set()
        try:
            if worker_task is not None:await asyncio.wait_for(worker_task,3)
        finally:
            server.should_exit=True
            try:await asyncio.wait_for(serving,3)
            finally:listener.close()
