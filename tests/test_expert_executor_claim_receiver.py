"""Actual signed ExpertSlackIngress → canonical service claim ASGI boundary.

The ingress, Participants, ControlSpool, RecoveryJournal, fair request order and
TargetExpertRequestControl are REAL classes. Only the remote journal store is
the original isolated test fixture. No provider, token issuance or live claim.
"""
import asyncio
import json

import httpx
import pytest
from starlette.applications import Starlette
from starlette.routing import Route

from regional_knowledge.expert_executor_claim_receiver import (
    ExpertExecutorClaimReceiver, PATH, CODEC, MAX_BODY,
)
from regional_knowledge.expert_slack_ingress import ExpertSlackIngress,PATH as INGRESS
from regional_knowledge.expert_request_control import ExecutorCredentials
from regional_knowledge.participant_contracts import AccountUnlink
from test_control_spool import env
from test_expert_request_control import _prepare,_event
from test_participants import people, change


def _app(control):
    signed=ExpertSlackIngress(control,signing_secret='test-signing-secret',
        clock=lambda:1792000001)
    claimant=ExpertExecutorClaimReceiver(control)
    return Starlette(routes=[
        Route(INGRESS,signed.__call__,methods=['POST']),
        Route(PATH,claimant.__call__,methods=['POST']),
    ])


def _claim_payload(request_id=None,lease_seconds=120):
    return dict(codec=CODEC,request_id=request_id,lease_seconds=lease_seconds)


def _auth(credentials):
    return {'Authorization':'Bearer '+credentials.token,
            'X-RKB-Expert-Service':credentials.service_id}


@pytest.mark.asyncio
async def test_signed_ingress_and_actual_remote_seal_required_for_single_executor_claim(env):
    control,credentials,_link=_prepare(env)
    app=_app(control)
    raw,signed=_event()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app,client=('127.0.0.1',9400)),
        base_url='http://127.0.0.1'
    ) as client:
        first=await client.post(INGRESS,content=raw,headers=signed)
        assert first.status_code==503
        empty=await client.post(PATH,json=_claim_payload(),headers=_auth(credentials))
        assert empty.status_code==200
        assert empty.json()==dict(codec=CODEC,state='empty')
        with env.p.corpus.connect() as db:
            rows=db.execute('SELECT id,state,remote_command_hash FROM rkb_expert_requests').fetchall()
            assert len(rows)==1
            assert rows[0]['state']=='received' and rows[0]['remote_command_hash'] is None
            request_id=rows[0]['id']
        command=control.exact_remote_intent(request_id,1)
        seal=await env.journal.seal(
            [command],predecessor=None,writer_generation='writer-1')
        assert (await control.verify_remote_intent(
            request_id,command,seal))['remote_accepted']
        accepted=await client.post(INGRESS,content=raw,headers=signed)
        assert accepted.status_code==200 and accepted.json()=={'ok':True}
        # Client replayed the SAME signed byte identity, not a fresh request.
        duplicate=await client.post(INGRESS,content=raw,headers=signed)
        assert duplicate.status_code==200
        claimed=await client.post(
            PATH,json=_claim_payload(request_id),headers=_auth(credentials))
        assert claimed.status_code==200
        lease=claimed.json()
        assert lease['codec']==CODEC and lease['state']=='leased'
        assert lease['request_id']==request_id and lease['generation']==1
        assert len(lease['execution_token'])>=32
        assert set(lease)=={'codec','state','request_id','generation','execution_token'}
        context=control.claim_context(credentials,request_id,
            lease['generation'],lease['execution_token'])
        assert context['person_id']==env.p.other.subject
        assert context['origin']['user_id']=='U111'
        assert context['input_text']
        second=await client.post(PATH,json=_claim_payload(request_id),
            headers=_auth(credentials))
        assert second.status_code==200
        assert second.json() in ({'codec':CODEC,'state':'empty'},
                                 {'codec':CODEC,'state':'deferred'})
        with env.p.corpus.connect() as db:
            assert db.execute('SELECT count(*) FROM rkb_expert_requests').fetchone()[0]==1
            assert db.execute('SELECT generation FROM rkb_expert_requests').fetchone()[0]==1


@pytest.mark.asyncio
async def test_concurrent_claims_issue_one_actual_lease_only(env):
    control,credentials,_=_prepare(env)
    observation_raw,signed=_event()
    app=_app(control)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app,client=('127.0.0.1',9400)),
        base_url='http://127.0.0.1'
    ) as client:
        assert (await client.post(INGRESS,content=observation_raw,headers=signed)).status_code==503
        with env.p.corpus.connect() as db:
            request_id=db.execute('SELECT id FROM rkb_expert_requests').fetchone()['id']
        cmd=control.exact_remote_intent(request_id,1)
        seal=await env.journal.seal([cmd],predecessor=None,writer_generation='writer-1')
        await control.verify_remote_intent(request_id,cmd,seal)
        requests=await asyncio.gather(*(
            client.post(PATH,json=_claim_payload(request_id),headers=_auth(credentials))
            for _ in range(2)
        ))
        assert all(r.status_code==200 for r in requests)
        states=[r.json()['state'] for r in requests]
        assert states.count('leased')==1
        assert all(s in ('leased','empty','deferred') for s in states)
        with env.p.corpus.connect() as db:
            assert db.execute('SELECT generation FROM rkb_expert_requests').fetchone()[0]==1


@pytest.mark.asyncio
async def test_forged_nonlocal_bad_service_and_invalid_claim_bound_fail_closed(env):
    control,credentials,_=_prepare(env)
    app=_app(control)
    fake=ExecutorCredentials(credentials.service_id,'not-the-true-secret')
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app,client=('192.0.2.55',9400)),
        base_url='http://127.0.0.1'
    ) as internet:
        response=await internet.post(PATH,json=_claim_payload(),headers=_auth(credentials))
        assert response.status_code==403
        assert response.json()=={'error':'executor_authorization_required'}
        # Spoofing a forwarded client address does not replace ASGI peer.
        forged=await internet.post(PATH,json=_claim_payload(),headers={
            **_auth(credentials),'X-Forwarded-For':'127.0.0.1'})
        assert forged.status_code==403
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app,client=('127.0.0.1',9400)),
        base_url='http://127.0.0.1'
    ) as local:
        response=await local.post(PATH,json=_claim_payload(),headers=_auth(fake))
        assert response.status_code==403
        assert response.json()=={'error':'executor_authorization_required'}
        for payload in (
            {'codec':CODEC,'request_id':None,'lease_seconds':True},
            {'codec':CODEC,'request_id':None,'lease_seconds':301},
            {'codec':CODEC,'request_id':'not-a-uuid','lease_seconds':120},
            {'codec':CODEC,'request_id':None,'lease_seconds':120,'user_id':'forged'},
            {'codec':'unknown-codec','request_id':None,'lease_seconds':120},
        ):
            bad=await local.post(PATH,json=payload,headers=_auth(credentials))
            assert bad.status_code==400 and bad.json()=={'error':'invalid_claim_request'}
        duplicate=b'{"codec":"'+CODEC.encode()+b'","request_id":null,"request_id":null,"lease_seconds":120}'
        assert (await local.post(PATH,content=duplicate,headers=_auth(credentials))).status_code==400
        oversized=await local.post(PATH,content=b'x'*(MAX_BODY+1),headers=_auth(credentials))
        assert oversized.status_code==413
    with env.p.corpus.connect() as db:
        assert db.execute('SELECT count(*) FROM rkb_expert_requests').fetchone()[0]==0


@pytest.mark.asyncio
async def test_revoked_verified_person_does_not_receive_executor_claim(env):
    control,credentials,link=_prepare(env)
    raw,signed=_event()
    app=_app(control)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app,client=('127.0.0.1',9400)),
        base_url='http://127.0.0.1'
    ) as client:
        assert (await client.post(INGRESS,content=raw,headers=signed)).status_code==503
        with env.p.corpus.connect() as db:
            request_id=db.execute('SELECT id FROM rkb_expert_requests').fetchone()['id']
        cmd=control.exact_remote_intent(request_id,1)
        seal=await env.journal.seal([cmd],predecessor=None,writer_generation='writer-1')
        await control.verify_remote_intent(request_id,cmd,seal)
        change(env.p,AccountUnlink(link_id=link.link_id,
            expected_link_revision=2,reason='synthetic mistaken identity',
            incorrect_binding=True))
        denied=await client.post(PATH,json=_claim_payload(request_id),headers=_auth(credentials))
        assert denied.status_code==200
        assert denied.json()=={'codec':CODEC,'state':'blocked'}
        with env.p.corpus.connect() as db:
            row=db.execute('SELECT state FROM rkb_expert_requests').fetchone()
            assert row['state']=='blocked'
