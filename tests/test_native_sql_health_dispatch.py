"""Real health codec/admission/WSS/worker boundaries, synthetic native I/O."""
import asyncio
from dataclasses import replace
import json
import time

import pytest
from starlette.applications import Starlette
from starlette.testclient import TestClient

from regional_knowledge.bge_channel import channel_route
from regional_knowledge.bge_contract import REVISION,SPACE
from regional_knowledge.kaggle_protocol import (
    SqlHealthRequest,SqlHealthResult,SqlHealthObservation,TrustedActorContext,
    decode_envelope,encode_envelope,
)
from regional_knowledge.kaggle_worker_query import KaggleWorkerQuery
from regional_knowledge.read_authority import TrustedReadDestination
from test_kaggle_worker_query import pinned,forbidden_encode
from test_kaggle_health import Native
from test_kaggle_public_backend import target,work,release
from test_bge_runtime_readiness import run,shared_channel_run,observation
from test_read_authority import auth
from test_participants import people
from test_bge_wss_worker import CONFIG,module


def request(**updates):
    fields=dict(request_id='fixed-health-request',connection_generation=1,turn_generation=0,
        database_generation='synthetic-db',min_data_seq=5,deadline_ms=2000,compute_claim='existing-claim',
        actor=TrustedActorContext(person_id='fixture-person',audience='personal',auth_epoch=1,visibility_epoch=0))
    fields.update(updates)
    return SqlHealthRequest(**fields)


def frame(**updates):return json.loads(request(**updates).model_dump_json())


def test_health_codec_has_no_corpus_query_or_sql_input_and_requires_complete_success():
    value=request()
    assert decode_envelope(encode_envelope(value))==value
    with pytest.raises(ValueError):SqlHealthRequest.model_validate(dict(value.model_dump(),sql='SELECT private'))
    with pytest.raises(ValueError):SqlHealthResult(**value.model_dump(include={'request_id','connection_generation','turn_generation'}),status='ok')
    with pytest.raises(ValueError):SqlHealthObservation(sql_response=True,elapsed_ms=float('nan'))


@pytest.mark.asyncio
async def test_actual_native_handler_reads_fixed_metadata_without_count_or_inference():
    native=Native()
    async def no_count(_):raise AssertionError('SQL health must not tokenize')
    handler=KaggleWorkerQuery(acquire=native.acquire,tokenizer=pinned(no_count))
    result=decode_envelope(json.dumps(await handler(frame(),encode=forbidden_encode,
        lease_current=lambda:True)).encode())
    assert type(result) is SqlHealthResult and result.status=='ok' and result.health.ready
    assert not result.evidence and result.health.applied_seq==5
    assert result.observed_database_generation=='synthetic-db'
    assert native.acquired==native.released==1
    assert native.sql[0][0]=='SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY'


@pytest.mark.asyncio
@pytest.mark.parametrize('case',['no_lease','stale_before','stale_after','generation','pending','projection'])
async def test_worker_health_cannot_turn_stale_or_pending_metadata_into_success(case):
    native=Native();current=[case!='stale_before']
    if case=='stale_after':native.after_query=lambda:current.__setitem__(0,False)
    if case=='generation':native.rows=[('other-db','s1',5,5,False)]
    if case=='pending':native.rows=[('synthetic-db','s1',5,5,True)]
    if case=='projection':native.rows=[('synthetic-db','s1',6,5,False)]
    handler=KaggleWorkerQuery(acquire=native.acquire,tokenizer=pinned())
    result=decode_envelope(json.dumps(await handler(frame(),encode=forbidden_encode,
        lease_current=None if case=='no_lease' else lambda:current[0])).encode())
    assert result.status=='unavailable' and (result.health is None or not result.health.ready)
    assert native.acquired==native.released
    if case in ('no_lease','stale_before'):assert native.acquired==0


@pytest.mark.asyncio
async def test_actual_transport_supplies_current_read_resource_and_connection_lease():
    transport=module('bge_wss_transport');native=Native()
    handler=KaggleWorkerQuery(acquire=native.acquire,tokenizer=pinned())
    worker=transport.BgeWssTransport(dict(CONFIG,shared_reads=True),lambda:None,lambda:{},query_handler=handler)
    worker.encoder=lambda _:(_ for _ in ()).throw(AssertionError('health inference'))
    worker._connection_generation=1
    value=frame();worker._validate_query(value);worker._start_read(value)
    await asyncio.wait_for(worker.read_task,1)
    result=decode_envelope(json.dumps(worker.read_pending).encode())
    assert result.status=='ok' and result.health.ready
    # A later connection cannot reuse the previous resource's health callback.
    worker._maybe_release();worker._connection_generation=2
    worker._start_read(frame(request_id='stale-generation'))
    await asyncio.wait_for(worker.read_task,1)
    result=decode_envelope(json.dumps(worker.read_pending).encode())
    assert result.status=='unavailable' and result.health.reason=='pg_binding_changed'
    assert native.acquired==native.released==1


def test_actual_asgi_channel_routes_health_result_through_verified_shared_admission(shared_channel_run,auth):
    queue,ident,token,nonce=shared_channel_run
    with TestClient(Starlette(routes=[channel_route(queue)])) as app:
        with app.websocket_connect('/bge-worker/channel',headers={'Authorization':'Bearer '+token}) as socket:
            socket.send_json(dict(type='ATTACH',protocol_version=2,run_id=ident,
                launch_nonce=nonce,runtime_incarnation='incarnation-a'))
            welcome=socket.receive_json();generation=welcome['connection_generation']
            def send(kind,**payload):socket.send_json(dict(type=kind,protocol_version=2,
                connection_generation=generation,**payload))
            send('RESOURCE_STATE',state='idle')
            send('READY',model_revision=REVISION,space=SPACE,runtime_readiness=observation())
            assert socket.receive_json()['status']=='ready'
            async def begin():
                authority=await auth.resolver.resolve_oauth(auth.tokens.access_token,
                    destination=TrustedReadDestination('mcp','personal','sql-health'))
                query=request(connection_generation=generation,compute_claim=None,
                    actor=TrustedActorContext(person_id=authority.person_id,audience=authority.destination.key,
                        auth_epoch=authority.auth_revision,visibility_epoch=0,context_ref=authority.context_ref))
                return await queue.shared_reads.begin_read(authority,query,
                    dialog_ref='sql-health',deadline=time.monotonic()+1.9)
            ticket=app.portal.call(begin)
            dispatched=socket.receive_json();assert dispatched['type']=='SQL_HEALTH'
            async def execute():
                native=Native()
                return await KaggleWorkerQuery(acquire=native.acquire,tokenizer=pinned())(
                    dispatched,encode=forbidden_encode,lease_current=lambda:True)
            socket.send_json(app.portal.call(execute))
            async def delivered():return await ticket.result
            result=app.portal.call(delivered)
            assert result.health.ready and result.request_id==ticket.request_id
            send('RESOURCE_RELEASED',resource_kind='read',resource_id=dispatched['request_id'],
                compute_claim=dispatched['compute_claim'],dispatch_generation=generation)


@pytest.mark.asyncio
async def test_existing_public_status_uses_native_admitted_health_and_sanitizes_identity(target,monkeypatch):
    s=target;startup_ref=s.queue.request_startup()['startup_ref']
    def no_startup(*args,**kwargs):raise AssertionError('health must not allocate startup')
    monkeypatch.setattr(s.queue,'request_startup',no_startup)
    pending=asyncio.create_task(s.backend.startup_status(startup_ref,s.principal))
    dispatched=await work(s)
    assert type(dispatched.query) is SqlHealthRequest
    native=Native()
    wire=await KaggleWorkerQuery(acquire=native.acquire,tokenizer=pinned())(
        json.loads(dispatched.query.model_dump_json()),encode=forbidden_encode,lease_current=lambda:True)
    assert await s.admission.finish_read(s.run,s.token,s.generation,decode_envelope(json.dumps(wire).encode()))
    await release(s,dispatched)
    result=await pending
    assert result.health['read_ready'] and result.health['postgres']['sql_response']
    encoded=json.dumps(result.health)
    assert s.run not in encoded and 'incarnation-a' not in encoded and 'compute_claim' not in encoded
