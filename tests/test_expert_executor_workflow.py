"""Exact M8 main/ASGI + original Dataset result/send-intent product boundaries.

Only native capture, Dataset provider and transport are existing synthetic
fixtures. TargetServiceStartup, Participants, signed Slack, request/delivery
controls, QueueWriterAuthority, Dataset CAS and SourceReadGate are real classes.
No installed service, provider send, credential mint or local-only Saved.
"""
import asyncio
from types import SimpleNamespace

import httpx
import pytest
from starlette.applications import Starlette
from starlette.routing import Route

import regional_knowledge.server as server
from regional_knowledge.expert_slack_ingress import ExpertSlackIngress, PATH as INGRESS
from regional_knowledge.expert_executor_claim_receiver import (
    PATH as CLAIM, CODEC as CLAIM_CODEC,
)
from regional_knowledge.expert_executor_workflow import (
    ExpertExecutorContext, BoundExpertClaimReceiver,
    ExpertExecutorResultReceiver, CODEC as RESULT_CODEC, PATH as RESULT,
)
from regional_knowledge.expert_delivery_control import TargetExpertDeliveryControl
from regional_knowledge.expert_request_control import ExecutorCredentials, RequestControlError, TargetExpertRequestControl
from regional_knowledge.participant_contracts import AccountUnlink
from test_expert_request_control import _prepare, _event
from test_expert_slack_composition import _owned_ingress
from test_expert_slack_main_composition import _capture_main
from test_expert_dataset_checkpoint_acceptance import composition, local_rows
from test_kaggle_public_backend import target
from test_kaggle_service_startup import service
from test_queue_writer import owner
from test_control_spool import env
from test_read_authority import auth
from test_participants import people, change


def _context(control,credentials):
    # Controlled local synthetic source gate only. Production requires the
    # already scoped, owner-provided exact TargetExpertDeliveryControl.
    return ExpertExecutorContext(credentials,
        TargetExpertDeliveryControl(control,source_gate=lambda row,origin:True))


def _auth(credentials):
    return {'Authorization':'Bearer '+credentials.token,
            'X-RKB-Expert-Service':credentials.service_id}


def _claim(rid):
    return {'codec':CLAIM_CODEC,'request_id':rid,'lease_seconds':120}


def _result(rid,lease,text='An exact source-grounded synthetic answer.'):
    return dict(codec=RESULT_CODEC,request_id=rid,
        generation=lease['generation'],execution_token=lease['execution_token'],
        text=text)


@pytest.mark.asyncio
async def test_main_wires_claim_and_result_only_with_one_provisioned_context(service,monkeypatch,env):
    s=service
    control,credentials,_link,ingress=_owned_ingress(s)
    captured=_capture_main(monkeypatch)
    server.main(target_startup=s.startup,expert_slack_ingress=ingress)
    assert len(captured)==1
    absent={r.path for r in captured[0][0].streamable_http_app(
        stateless_http=True,json_response=True).routes}
    assert {INGRESS}<=absent and CLAIM not in absent and RESULT not in absent

    context=_context(control,credentials)
    server.main(target_startup=s.startup,expert_slack_ingress=ingress,
        expert_executor_context=context)
    assert len(captured)==2
    mcp=captured[-1][0]
    app=mcp.streamable_http_app(stateless_http=True,json_response=True)
    assert {CLAIM,RESULT,INGRESS}<={r.path for r in app.routes}
    raw,headers=_event()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app,client=('127.0.0.1',9400)),
        base_url='http://knowledge.example.test'
    ) as client:
        assert (await client.post(INGRESS,content=raw,headers=headers)).status_code==503
        waiting=await client.post(CLAIM,json=_claim(None),headers=_auth(credentials))
        assert waiting.status_code==200 and waiting.json()['state']=='empty'
        with s.config.participants.corpus.connect() as db:
            rid=db.execute('SELECT id FROM rkb_expert_requests').fetchone()['id']
        cmd=control.exact_remote_intent(rid,1)
        seal=await s.config.spool.journal.seal(
            [cmd],predecessor=None,writer_generation='writer-1')
        await control.verify_remote_intent(rid,cmd,seal)
        assert (await client.post(INGRESS,content=raw,headers=headers)).status_code==200
        claim=(await client.post(CLAIM,json=_claim(rid),headers=_auth(credentials)))
        assert claim.status_code==200 and claim.json()['state']=='leased'
        attempt=await client.post(RESULT,
            json=_result(rid,claim.json()),headers=_auth(credentials))
        # The S3/conditional-store startup has no Dataset effect coverage.
        # No local result is even prepared, no fake send or Saved.
        assert attempt.status_code==503
        assert attempt.json()=={'error':'result_persistence_pending'}
        with s.config.participants.corpus.connect() as db:
            row=db.execute('SELECT state,result_json,delivery_attempt FROM rkb_expert_requests').fetchone()
            assert tuple(row)==('leased',None,0)

        bad=await client.post(CLAIM,json=_claim(None),
            headers={'Authorization':'Bearer invalid-credential',
                     'X-RKB-Expert-Service':credentials.service_id})
        assert bad.status_code==403
        external=await httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app,client=('192.0.2.5',444)),
            base_url='http://knowledge.example.test').__aenter__()
        try:
            rejected=await external.post(CLAIM,json=_claim(None),headers={
                **_auth(credentials),'X-Forwarded-For':'127.0.0.1'})
            assert rejected.status_code==403
        finally:
            await external.aclose()

    # Both async fixtures can share the same Participants authority. Construct
    # a second CONTROL over the other spool WITHOUT re-linking the same Slack
    # subject or provisioning another service.
    other=TargetExpertRequestControl(env.p.registry,env.spool,
        app_id='A111',team_id='T111',bot_user_id='UBOT',
        allowed_channels=('C111',),clock=lambda:1792000001.)
    foreign=_context(other,credentials)
    with pytest.raises(ValueError,match='authority does not match'):
        server.main(target_startup=s.startup,expert_slack_ingress=ingress,
            expert_executor_context=foreign)
    with pytest.raises(RequestControlError,match='service_denied'):
        server.main(target_startup=s.startup,expert_slack_ingress=ingress,
            expert_executor_context=ExpertExecutorContext(
                ExecutorCredentials(credentials.service_id,'wrong-existing-token'),
                context.delivery))
    with pytest.raises(RuntimeError,match='requires signed ingress'):
        server.main(target_startup=s.startup,expert_executor_context=context)
    assert len(captured)==2


@pytest.mark.asyncio
async def test_actual_dataset_signed_claim_result_send_intent_and_unknown_no_resend(
    env,owner,tmp_path,monkeypatch
):
    p,local,ctl,credentials,_=composition(owner,env,tmp_path,monkeypatch)
    try:
        ingress=ExpertSlackIngress(ctl,signing_secret='test-signing-secret',
                                   clock=lambda:1792000001.0)
        ctx=_context(ctl,credentials)
        claim=BoundExpertClaimReceiver(ctl,credentials)
        result=ExpertExecutorResultReceiver(ctx)
        assert claim.control is result.control is ingress.control
        app=Starlette(routes=[
            Route(INGRESS,ingress.__call__,methods=['POST']),
            Route(CLAIM,claim.__call__,methods=['POST']),
            Route(RESULT,result.__call__,methods=['POST']),
        ])
        raw,signed=_event()
        async with httpx.AsyncClient(transport=httpx.ASGITransport(
            app=app,client=('127.0.0.1',9444)),base_url='http://test') as client:
            assert (await client.post(INGRESS,content=raw,headers=signed)).status_code==503
            rid=local_rows(env.p)[0]['id']
            assert (await client.post(CLAIM,json=_claim(rid),
                headers=_auth(credentials))).json()['state']=='empty'
            # The original quiet native capture/checkpoint; not a fabricated
            # local accepted flag or inline per-event Dataset publish.
            # Actual checkpoint owner first observes the pending bytes at
            # t=0; 31s is the next quiet-cycle boundary, not an invented clock.
            assert not await p.owner.refresh(p.binding)
            p.clock[0]=31
            assert await p.owner.refresh(p.binding)
            assert p.store.latest_checkpoint.dataset_version==2
            accepted=await client.post(INGRESS,content=raw,headers=signed)
            assert accepted.status_code==200
            lease_response=await client.post(CLAIM,json=_claim(rid),headers=_auth(credentials))
            assert lease_response.status_code==200
            lease=lease_response.json()
            assert lease['state']=='leased' and lease['generation']==1
            assert ctl.claim_context(credentials,rid,lease['generation'],
                lease['execution_token'])['person_id']==env.p.other.subject
            body=_result(rid,lease)
            first=await client.post(RESULT,json=body,headers=_auth(credentials))
            assert first.status_code==202
            assert first.json()==dict(codec=RESULT_CODEC,state='result_pending',
                remote_accepted=False,saved=False,provider_sent=False)
            with env.p.corpus.connect() as db:
                row=db.execute('SELECT state,delivery_attempt FROM rkb_expert_requests').fetchone()
                assert tuple(row)==('result_unsealed',0)
            with pytest.raises(RequestControlError):
                ctx.delivery.claim_send_once(credentials,rid,lease['generation'],
                                              lease['execution_token'])
            p.clock[0]=32
            assert not await p.owner.refresh(p.binding)
            p.clock[0]=63
            assert await p.owner.refresh(p.binding)
            assert p.store.latest_checkpoint.dataset_version==3
            second=await client.post(RESULT,json=body,headers=_auth(credentials))
            assert second.status_code==202
            assert second.json()['state']=='send_intent_pending'
            assert second.json()['saved'] is False
            with pytest.raises(RequestControlError):
                ctx.delivery.claim_send_once(credentials,rid,lease['generation'],
                                              lease['execution_token'])
            p.clock[0]=64
            assert not await p.owner.refresh(p.binding)
            p.clock[0]=95
            assert await p.owner.refresh(p.binding)
            assert p.store.latest_checkpoint.dataset_version==4
            third=await client.post(RESULT,json=body,headers=_auth(credentials))
            assert third.status_code==200
            assert third.json()==dict(codec=RESULT_CODEC,state='send_intent_ready',
                remote_accepted=True,saved=False,provider_sent=False)
            duplicate=await client.post(RESULT,json=body,headers=_auth(credentials))
            assert duplicate.status_code==200
            changed=await client.post(RESULT,
                json={**body,'text':'Different authored output.'},
                headers=_auth(credentials))
            assert changed.status_code==409 and changed.json()=={'error':'result_conflict'}
            original=ctl._row(rid)
            assert original['state']=='send_intent_ready'
            assert original['result_remote_hash'] and original['send_intent_hash']
            assert original['delivery_attempt']==0
            # The owner-held original provider permit is not exposed by HTTP.
            # This is a *synthetic* post-send uncertainty, never a Slack API call.
            outgoing=ctx.delivery.claim_send_once(credentials,rid,lease['generation'],
                                                  lease['execution_token'])
            assert outgoing['delivery_id']
            assert ctx.delivery.uncertain_delivery(credentials,rid,
                outgoing['delivery_id'])['retry_send_allowed'] is False
            not_again=await client.post(RESULT,json=body,headers=_auth(credentials))
            assert not_again.status_code==503
            assert ctl.claim(credentials,request_id=rid)=={'state':'empty'}
            row=ctl._row(rid)
            assert row['state']=='unknown' and row['delivery_attempt']==1
            assert row['delivery_receipt'] is None
        # No created provider result or new writer authority.
        assert p.calls.count('capture')==3
    finally:
        local.spool.close()
        p.guard.close()
