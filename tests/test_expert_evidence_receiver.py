"""Actual ASGI final-issuance tests for trusted expert→Kaggle search."""
import asyncio
from types import SimpleNamespace
import time

import httpx
import pytest
from starlette.applications import Starlette
from starlette.routing import Route

from regional_knowledge.expert_evidence_receiver import ExpertEvidenceReceiver,PATH,CODEC
from regional_knowledge.read_issuance import ReadIssuanceMiddleware
from regional_knowledge.participant_contracts import AccountUnlink,ParticipantGrant
from regional_knowledge.server import build_server
from test_expert_request_control import _prepare, _observation
from test_participants import change
from test_kaggle_public_backend import target, work, settle, release
from test_read_authority import auth
from test_participants import people
from test_kaggle_service_startup import service


async def _receiver(s,*,group=False):
    # Same Participants, actual target queue and existing spool/journal.
    env=SimpleNamespace(p=s.target.auth.p,spool=s.config.spool)
    ctl,creds,link=_prepare(env)
    obs=_observation(group=group)
    req=ctl.accept(obs)
    command=ctl.exact_remote_intent(req['request_id'],1)
    seal=await s.config.spool.journal.seal([command],predecessor=None,
                                           writer_generation='writer-1')
    await ctl.verify_remote_intent(req['request_id'],command,seal)
    claimed=ctl.claim(creds,request_id=req['request_id'])
    assert claimed['state']=='leased'
    receiver=ExpertEvidenceReceiver(control=ctl,backend=s.target.backend)
    app=ReadIssuanceMiddleware(Starlette(routes=[Route(PATH,receiver.__call__,methods=['POST'])]))
    payload=dict(codec=CODEC,request_id=req['request_id'],generation=claimed['generation'],
                 execution_token=claimed['execution_token'],min_data_seq=5,
                 deadline_monotonic=time.monotonic()+2)
    headers={'Authorization':'Bearer '+creds.token,'X-RKB-Expert-Service':creds.service_id}
    return receiver,app,payload,headers,link,req


@pytest.mark.asyncio
async def test_real_asgi_expert_identity_is_verified_and_uses_single_kaggle_read(service):
    s=service;receiver,app,payload,headers,link,req=await _receiver(s)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app,client=('127.0.0.1',9001)),
                                 base_url='http://127.0.0.1') as client:
        task=asyncio.create_task(client.post(PATH,json=payload,headers=headers))
        issued=await work(s.target)
        assert issued.query.actor.person_id==s.target.auth.p.other.subject
        assert issued.query.actor.person_id!=s.target.auth.p.owner.subject
        assert issued.query.request_id==req['request_id']
        assert issued.query.turn_generation==payload['generation']
        assert issued.query.min_data_seq==5
        assert issued.query.actor.audience.startswith('slack:personal:')
        await settle(s.target,issued)
        response=await task
        data=response.json()
        assert response.status_code==200
        assert data['codec']==CODEC and data['request_id']==req['request_id']
        assert data['mode']=='hybrid' and not data['context_gap']
        assert len(data['evidence'])==1 and data['evidence'][0]['text']=='Evidence'
        assert not s.target.auth.resolver._contexts
        await release(s.target,issued)


@pytest.mark.asyncio
async def test_lost_identity_before_final_http_issuance_suppresses_evidence(service):
    s=service;reader,app,payload,headers,link,req=await _receiver(s)
    original=s.target.gate.refresh
    async def revoke(gated):
        change(s.target.auth.p,AccountUnlink(
            link_id=link.link_id,expected_link_revision=2,
            reason='synthetic wrong participant',incorrect_binding=True))
        return await original(gated)
    s.target.gate.refresh=revoke
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app,client=('127.0.0.1',9001)),
                                 base_url='http://127.0.0.1') as client:
        task=asyncio.create_task(client.post(PATH,json=payload,headers=headers))
        issued=await work(s.target)
        await settle(s.target,issued)
        data=(await task).json()
        assert data['codec']==CODEC and data['context_gap'] is True
        assert data['evidence']==[] and data['mode'] is None
        assert not s.target.auth.resolver._contexts
        await release(s.target,issued)


@pytest.mark.asyncio
@pytest.mark.parametrize('revocation',['incorrect_link','public_search_grant'])
async def test_revocation_after_async_identity_snapshot_before_final_sync_fence(service,revocation):
    s=service;receiver,app,payload,headers,link,req=await _receiver(s)
    refreshed=[False]
    original=s.target.gate.refresh
    async def commit_revocation_after_snapshot(gated):
        # SourceReadGate.refresh has just completed its asynchronous identity
        # snapshot. Revoke BEFORE the final synchronous issuance_current; the
        # source ticket is still valid and therefore cannot cause a false PASS.
        fresh=await original(gated)
        if not refreshed[0]:
            refreshed[0]=True
            if revocation=='incorrect_link':
                change(s.target.auth.p,AccountUnlink(
                    link_id=link.link_id,expected_link_revision=2,
                    reason='Synthetic invalid account binding',incorrect_binding=True))
            else:
                change(s.target.auth.p,ParticipantGrant(
                    capability='public_search',granted=False,
                    reason='Synthetic grant revoked after identity read'))
        return fresh
    s.target.gate.refresh=commit_revocation_after_snapshot
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app,client=('127.0.0.1',9001)),
                                 base_url='http://127.0.0.1') as client:
        task=asyncio.create_task(client.post(PATH,json=payload,headers=headers))
        issued=await work(s.target)
        await settle(s.target,issued)
        response=await task
        data=response.json()
        assert refreshed[0]
        assert data['codec']==CODEC and data['request_id']==req['request_id']
        assert data['context_gap'] and data['mode'] is None and data['evidence']==[]
        assert not s.target.auth.resolver._contexts
        await release(s.target,issued)


@pytest.mark.asyncio
async def test_expired_execution_generation_before_final_http_issuance_suppresses_evidence(service):
    s=service;reader,app,payload,headers,link,req=await _receiver(s)
    original=s.target.gate.refresh
    changed=[False]
    async def bump(gated):
        if not changed[0]:
            changed[0]=True
            with s.config.participants.corpus.connect() as db:
                db.execute('UPDATE rkb_expert_requests SET generation=generation+1 WHERE id=?',
                           (req['request_id'],))
        return await original(gated)
    s.target.gate.refresh=bump
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app,client=('127.0.0.1',9001)),
                                 base_url='http://127.0.0.1') as client:
        task=asyncio.create_task(client.post(PATH,json=payload,headers=headers))
        issued=await work(s.target)
        await settle(s.target,issued)
        data=(await task).json()
        assert data['codec']==CODEC and data['context_gap']
        assert data['evidence']==[] and data['mode'] is None
        await release(s.target,issued)


@pytest.mark.asyncio
async def test_handler_finishes_but_final_http_refresh_crosses_absolute_deadline(service):
    s=service;receiver,app,payload,headers,link,req=await _receiver(s)
    # The handler completes within its own timeout, but the ASGI final
    # issuance can be delayed. The original absolute deadline must STILL
    # fence the buffered HTTP body, independently of the source gate TTL.
    payload['deadline_monotonic']=time.monotonic()+0.6
    original=s.target.gate.refresh
    async def late(gated):
        await asyncio.sleep(max(0,payload['deadline_monotonic']-time.monotonic())+.08)
        return await original(gated)
    s.target.gate.refresh=late
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app,client=('127.0.0.1',9001)),
                                 base_url='http://127.0.0.1') as client:
        task=asyncio.create_task(client.post(PATH,json=payload,headers=headers))
        dispatched=await work(s.target)
        await settle(s.target,dispatched)
        response=await task
        value=response.json()
        assert value['codec']==CODEC and value['request_id']==req['request_id']
        assert value['context_gap'] and value['evidence']==[]
        assert value['mode'] is None and not s.target.auth.resolver._contexts
        await release(s.target,dispatched)


@pytest.mark.asyncio
async def test_denied_unsigned_or_forged_expert_caller_never_dispatches(service):
    s=service;reader,app,payload,headers,link,req=await _receiver(s)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app,client=('192.0.2.1',9040)),
                                 base_url='http://127.0.0.1') as client:
        r=await client.post(PATH,json=payload,headers=headers)
        assert r.status_code==403 and not r.json().get('evidence')
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app,client=('127.0.0.1',9001)),
                                 base_url='http://127.0.0.1') as client:
        r=await client.post(PATH,json={**payload,'query':'FORGED OWNER INPUT'},headers=headers)
        assert r.status_code==422 and not r.json().get('evidence')
        r=await client.post(PATH,json=payload,headers={**headers,'Authorization':'Bearer fake-token'})
        assert r.status_code in (200,403) and not r.json().get('evidence')
        assert not s.target.gate.permits


@pytest.mark.asyncio
async def test_real_server_route_is_inert_without_explicit_same_target_configuration(service):
    startup=service.startup
    ctl,creds,_=_prepare(SimpleNamespace(p=service.target.auth.p,spool=service.config.spool))
    receiver=ExpertEvidenceReceiver(control=ctl,backend=startup.backend)
    absent=build_server(target_startup=startup).streamable_http_app(stateless_http=True,json_response=True)
    present=build_server(target_startup=startup,expert_evidence_receiver=receiver).streamable_http_app(
        stateless_http=True,json_response=True)
    assert not any(getattr(x,'path',None)==PATH for x in absent.routes)
    assert any(getattr(x,'path',None)==PATH for x in present.routes)
