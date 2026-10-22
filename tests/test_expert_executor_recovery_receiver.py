"""Real original Dataset+expert classes: safe recovery after local control loss.

No provider request, new IAM/credentials, second writer or runtime monkeypatch.
Native/remote Dataset artifacts are original synthetic I/O fixture only.
"""
import json

import httpx
import pytest
from starlette.applications import Starlette
from starlette.routing import Route

import regional_knowledge.server as server
from regional_knowledge.expert_executor_recovery_receiver import (
    ExpertExecutorRecoveryReceiver, PATH, CODEC, MAX_BODY,
)
from regional_knowledge.expert_executor_workflow import ExpertExecutorContext
from regional_knowledge.expert_delivery_control import TargetExpertDeliveryControl
from test_expert_delivery_recovery import (
    accepted,prepare,result_staged,promote,receipt_for,lose_local_and_reopen,
)
from test_expert_request_recovery import attached
from test_expert_slack_composition import _owned_ingress
from test_expert_slack_main_composition import _capture_main
from test_kaggle_public_backend import target
from test_kaggle_service_startup import service
from test_control_spool import env
from test_read_authority import auth
from test_participants import people


def _request(rid):
    return dict(codec=CODEC,request_id=rid)


def _headers(credentials):
    return {'Authorization':'Bearer '+credentials.token,
            'X-RKB-Expert-Service':credentials.service_id}


def _app(receiver):
    return Starlette(routes=[Route(PATH,receiver.__call__,methods=['POST'])])


async def _unknown_after_covered_send(a):
    entry,args=await accepted(a)
    delivery=prepare(a)
    await result_staged(a,delivery,args)
    promote(a,3)
    assert delivery.confirm_dataset_result(*args)['state']=='result_ready'
    await delivery.stage_dataset_send(*args)
    promote(a,4)
    assert delivery.confirm_dataset_send(*args)['state']=='send_intent_ready'
    outbound=delivery.claim_send_once(*args)
    assert delivery.uncertain_delivery(a.credentials,entry['request_id'],
        outbound['delivery_id'])['retry_send_allowed'] is False
    assert a.ctl.claim(a.credentials)=={'state':'empty'}
    return entry,delivery,outbound,args


@pytest.mark.asyncio
async def test_restored_covered_send_without_native_receipt_is_unknown_no_resend(env,tmp_path):
    a=attached(env,tmp_path)
    entry,delivery,outbound,args=await _unknown_after_covered_send(a)
    ctl,reopened=lose_local_and_reopen(a)
    receiver=ExpertExecutorRecoveryReceiver(ExpertExecutorContext(
        a.credentials,reopened))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_app(receiver),client=('127.0.0.1',9001)),
        base_url='http://127.0.0.1'
    ) as client:
        result=await client.post(PATH,json=_request(entry['request_id']),
            headers=_headers(a.credentials))
        assert result.status_code==202
        assert result.json()==dict(codec=CODEC,state='unknown',
            retry_send_allowed=False,native_receipt_covered=False,
            reason='provider_native_disposition_or_covered_receipt')
        # Repeating this readback cannot revive a provider permit or
        # overwrite a recalled original result.
        again=await client.post(PATH,json=_request(entry['request_id']),
            headers=_headers(a.credentials))
        assert again.status_code==202 and again.json()==result.json()
        assert ctl.claim(a.credentials)=={'state':'empty'}
        with env.p.corpus.connect() as db:
            row=db.execute('SELECT state,delivery_attempt,delivery_receipt FROM rkb_expert_requests').fetchone()
            assert tuple(row)==('unknown',1,None)


@pytest.mark.asyncio
async def test_covered_original_native_receipt_recovers_delivered_exact_once(env,tmp_path):
    a=attached(env,tmp_path)
    entry,delivery,outbound,args=await _unknown_after_covered_send(a)
    receipt=receipt_for(a,outbound)
    assert await delivery.stage_dataset_native_receipt(a.credentials,entry['request_id'],receipt) is False
    promote(a,5)
    ctl,reopened=lose_local_and_reopen(a)
    receiver=ExpertExecutorRecoveryReceiver(ExpertExecutorContext(
        a.credentials,reopened))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_app(receiver),client=('127.0.0.1',9001)),
        base_url='http://127.0.0.1'
    ) as client:
        first=await client.post(PATH,json=_request(entry['request_id']),
            headers=_headers(a.credentials))
        assert first.status_code==200
        assert first.json()==dict(codec=CODEC,state='delivered',
            retry_send_allowed=False,native_receipt_covered=True)
        saved=ctl._row(entry['request_id'])
        assert saved['delivery_receipt'] and saved['delivery_attempt']==1
        repeated=await client.post(PATH,json=_request(entry['request_id']),
            headers=_headers(a.credentials))
        assert repeated.status_code==200 and repeated.json()==first.json()
        restored=ctl._row(entry['request_id'])
        assert restored['delivery_attempt']==1
        assert restored['delivery_receipt']==saved['delivery_receipt']
        assert ctl.claim(a.credentials)=={'state':'empty'}


@pytest.mark.asyncio
async def test_recovery_requires_current_registered_service_and_strict_input(env,tmp_path):
    a=attached(env,tmp_path)
    entry,_=await accepted(a)
    receiver=ExpertExecutorRecoveryReceiver(ExpertExecutorContext(
        a.credentials,prepare(a)))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_app(receiver),client=('192.0.2.55',9001)),
        base_url='http://127.0.0.1'
    ) as foreign:
        forged=await foreign.post(PATH,json=_request(entry['request_id']),
            headers={**_headers(a.credentials),'X-Forwarded-For':'127.0.0.1'})
        assert forged.status_code==403
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_app(receiver),client=('127.0.0.1',9001)),
        base_url='http://127.0.0.1'
    ) as client:
        missing=await client.post(PATH,json=_request(entry['request_id']),headers={
            'X-RKB-Expert-Service':a.credentials.service_id})
        assert missing.status_code==403
        for body in (
            {'codec':CODEC,'request_id':'arbitrary-user-text'},
            {'codec':CODEC,'request_id':None},
            {'codec':'unknown','request_id':entry['request_id']},
            {'codec':CODEC,'request_id':entry['request_id'],'retry_send':True},
        ):
            invalid=await client.post(PATH,json=body,headers=_headers(a.credentials))
            assert invalid.status_code==400
        duplicate=(b'{"codec":"'+CODEC.encode()+
            b'","request_id":"'+entry['request_id'].encode()+
            b'","request_id":"'+entry['request_id'].encode()+b'"}')
        assert (await client.post(PATH,content=duplicate,
            headers=_headers(a.credentials))).status_code==400
        assert (await client.post(PATH,content=b'x'*(MAX_BODY+1),
            headers=_headers(a.credentials))).status_code==413
    # accepted(a) has legitimately claimed the first original generation.
    # Malformed/forged status checks must leave that same lease untouched.
    row=a.ctl._row(entry['request_id'])
    assert row['state']=='leased' and row['generation']==1
    assert row['delivery_attempt']==0 and row['result_json'] is None


def test_main_exposes_recovery_only_with_existing_bound_executor_context(
    service,monkeypatch
):
    s=service
    control,credentials,_link,ingress=_owned_ingress(s)
    captured=_capture_main(monkeypatch)
    server.main(target_startup=s.startup,expert_slack_ingress=ingress)
    absent=captured[-1][0].streamable_http_app(
        stateless_http=True,json_response=True)
    assert PATH not in {r.path for r in absent.routes}
    delivery=TargetExpertDeliveryControl(control,source_gate=lambda row,origin:True)
    ctx=ExpertExecutorContext(credentials,delivery)
    server.main(target_startup=s.startup,expert_slack_ingress=ingress,
        expert_executor_context=ctx)
    installed=captured[-1][0].streamable_http_app(
        stateless_http=True,json_response=True)
    assert PATH in {r.path for r in installed.routes}


@pytest.mark.asyncio
async def test_corrupt_covered_native_receipt_is_persistence_pending_not_client_error(env,tmp_path):
    """Local corrupted original bytes must never become delivered or retryable."""
    a=attached(env,tmp_path)
    entry,delivery,outbound,args=await _unknown_after_covered_send(a)
    receipt=receipt_for(a,outbound)
    await delivery.stage_dataset_native_receipt(a.credentials,entry['request_id'],receipt)
    promote(a,5)
    ctl,reopened=lose_local_and_reopen(a)
    # Synthetic disk corruption of a covered immutable receipt, not a guard
    # bypass or new provider outcome. The store must reject altered bytes.
    key=ctl.dataset_records.effect_key(entry['request_id'],'receipt')
    with env.q.connect() as db:
        db.execute(
            'UPDATE rkb_dataset_recovery_objects SET data=? '
            'WHERE storage=? AND key=?',
            (b'{}',a.saved.store.storage_identity,key))
    receiver=ExpertExecutorRecoveryReceiver(ExpertExecutorContext(
        a.credentials,reopened))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_app(receiver),client=('127.0.0.1',9001)),
        base_url='http://127.0.0.1'
    ) as client:
        response=await client.post(PATH,json=_request(entry['request_id']),
            headers=_headers(a.credentials))
        assert response.status_code==503
        assert response.json()=={'error':'recovery_pending'}
        assert ctl.claim(a.credentials)=={'state':'empty'}
        row=ctl._row(entry['request_id'])
        # Corruption is detected BEFORE state promotion to UNKNOWN: retain
        # the stricter recovery_review_required gate, with no receipt/permit.
        assert row['state']=='recovery_review_required'
        assert row['delivery_receipt'] is None
        assert row['delivery_attempt']==0
