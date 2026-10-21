"""CAS-F10: no double Slack effect, no local-only Saved, no owner impersonation."""
import pytest

from regional_knowledge.expert_delivery_control import (
    TargetExpertDeliveryControl, verified_native_slack_receipt,
)
from regional_knowledge.expert_request_control import RequestControlError
from test_control_spool import env
from test_participants import people
from test_expert_request_control import _prepare, _observation
from regional_knowledge.expert_request_control import TargetExpertRequestControl


async def _accepted_request(env, ctl, number, sequence, *, thread="1792000000.100"):
    entry=ctl.accept(_observation(n=number,thread=thread))
    command=ctl.exact_remote_intent(entry['request_id'],sequence)
    seal=await env.journal.seal([command],predecessor=await env.journal.head(),
                                writer_generation='writer-1')
    await ctl.verify_remote_intent(entry['request_id'],command,seal)
    return entry


async def _ready_delivery(env, ctl, delivery, creds, entry, sequence):
    claim=ctl.claim(creds,request_id=entry['request_id'])
    assert claim['state']=='leased'
    args=(creds,entry['request_id'],claim['generation'],claim['execution_token'])
    delivery.prepare_result(*args,text='Source-grounded synthetic bounded result.')
    result_command=delivery.exact_result_intent(entry['request_id'],sequence)
    seal=await env.journal.seal([result_command],predecessor=await env.journal.head(),
                                writer_generation='writer-1')
    await delivery.verify_remote_result(*args,result_command,seal)
    send_command=delivery.exact_send_intent(entry['request_id'],sequence+1)
    seal=await env.journal.seal([send_command],predecessor=await env.journal.head(),
                                writer_generation='writer-1')
    await delivery.verify_remote_send(*args,send_command,seal)
    return args


@pytest.mark.asyncio
async def test_crash_after_provider_post_never_grants_a_second_send(env):
    e=env;ctl,creds,_=_prepare(e)
    gate={'allow':True}
    delivery=TargetExpertDeliveryControl(ctl,source_gate=lambda row,origin:gate['allow'])
    first=await _accepted_request(e,ctl,1,1)
    args=await _ready_delivery(e,ctl,delivery,creds,first,2)
    outbound=delivery.claim_send_once(*args)
    assert outbound['text']=='Source-grounded synthetic bounded result.'
    assert outbound['channel_id']=='D111'
    assert outbound['delivery_id']
    assert outbound['thread_ts']=='1792000000.100'
    with pytest.raises(RequestControlError,match='request_fence_denied'):
        delivery.claim_send_once(*args)
    # The native send effect may have committed, while its response was lost.
    unknown=delivery.uncertain_delivery(creds,first['request_id'],outbound['delivery_id'])
    assert unknown['retry_send_allowed'] is False
    # Restart the request/delivery controllers against the same authority DB.
    # A new process must not treat missing in-memory send state as permission.
    ctl=TargetExpertRequestControl(e.p.registry,e.spool,app_id='A111',team_id='T111',
        bot_user_id='UBOT',allowed_channels=('C111',),clock=lambda:1792000001.0)
    delivery=TargetExpertDeliveryControl(ctl,source_gate=lambda row,origin:gate['allow'])
    with pytest.raises(RequestControlError,match='request_fence_denied'):
        delivery.claim_send_once(*args)
    # The next request in this dialog must not overtake an UNKNOWN first send.
    second=await _accepted_request(e,ctl,2,4)
    assert ctl.claim(creds,request_id=second['request_id'])=={'state':'empty'}
    # Different dialog remains able to proceed after a global UNKNOWN.
    other=await _accepted_request(e,ctl,3,5,thread='1792000000.200')
    assert ctl.claim(creds,request_id=other['request_id'])['state']=='leased'
    native=verified_native_slack_receipt(dict(app_id='A111',user='UBOT',channel='D111',
        thread_ts=outbound['thread_ts'],ts='1792000020.400',
        client_msg_id=outbound['delivery_id'],text=outbound['text']),
        app_id='A111',team_id='T111',bot_user_id='UBOT',channel_id='D111',
        thread_ts=outbound['thread_ts'])
    assert delivery.receipt(creds,first['request_id'],native)['state']=='delivered'
    assert delivery.receipt(creds,first['request_id'],native)['state']=='delivered'
    # Fair work may select the other dialog; the first dialog is no longer held.
    with e.p.corpus.connect() as db:
        pending=db.execute('SELECT head_seq FROM rkb_request_dialogs WHERE id=(SELECT dialog_id FROM rkb_expert_requests WHERE id=?)',
                           (first['request_id'],)).fetchone()
        assert pending['head_seq']==2
        assert db.execute('SELECT count(*) FROM rkb_expert_requests WHERE delivery_attempt>0').fetchone()[0]==1


@pytest.mark.asyncio
async def test_revoked_source_gate_before_send_prevents_provider_effect(env):
    e=env;ctl,creds,_=_prepare(e)
    gate={'allow':True}
    delivery=TargetExpertDeliveryControl(ctl,source_gate=lambda row,origin:gate['allow'])
    entry=await _accepted_request(e,ctl,1,1)
    args=await _ready_delivery(e,ctl,delivery,creds,entry,2)
    gate['allow']=False
    with pytest.raises(RequestControlError,match='source_access_unverified'):
        delivery.claim_send_once(*args)
    with e.p.corpus.connect() as db:
        row=db.execute('SELECT state,delivery_attempt FROM rkb_expert_requests WHERE id=?',
                       (entry['request_id'],)).fetchone()
        assert row['state']=='send_intent_ready' and row['delivery_attempt']==0


@pytest.mark.asyncio
async def test_external_account_unlink_between_result_and_send_fails_closed(env):
    from regional_knowledge.participant_contracts import AccountUnlink
    from test_participants import change
    e=env;ctl,creds,expert=_prepare(e)
    delivery=TargetExpertDeliveryControl(ctl,source_gate=lambda row,origin:True)
    entry=await _accepted_request(e,ctl,1,1)
    args=await _ready_delivery(e,ctl,delivery,creds,entry,2)
    change(e.p,AccountUnlink(link_id=expert.link_id,expected_link_revision=2,
        reason='Wrong account bound to synthetic person',incorrect_binding=True))
    with pytest.raises(Exception):
        delivery.claim_send_once(*args)
    with e.p.corpus.connect() as db:
        row=db.execute('SELECT state,delivery_attempt FROM rkb_expert_requests WHERE id=?',
                       (entry['request_id'],)).fetchone()
        assert row['state']=='send_intent_ready' and row['delivery_attempt']==0


@pytest.mark.asyncio
async def test_remote_result_send_intent_required_before_attempt_or_ack(env):
    e=env;ctl,creds,_=_prepare(e)
    delivery=TargetExpertDeliveryControl(ctl,source_gate=lambda row,origin:True)
    first=await _accepted_request(e,ctl,1,1)
    claim=ctl.claim(creds,request_id=first['request_id'])
    args=(creds,first['request_id'],claim['generation'],claim['execution_token'])
    delivery.prepare_result(*args,text='A bounded result.')
    with pytest.raises(RequestControlError,match='send_intent_unavailable'):
        delivery.exact_send_intent(first['request_id'],3)
    with pytest.raises(RequestControlError,match='request_fence_denied'):
        delivery.claim_send_once(*args)
    cmd=delivery.exact_result_intent(first['request_id'],2)
    with pytest.raises(Exception):
        await delivery.verify_remote_result(*args,cmd,None)
    with e.p.corpus.connect() as db:
        assert db.execute('SELECT state FROM rkb_expert_requests WHERE id=?',(first['request_id'],)).fetchone()[0]=='result_unsealed'


@pytest.mark.asyncio
async def test_native_receipt_rejects_wrong_channel_text_marker_and_unverified_inputs(env):
    e=env;ctl,creds,_=_prepare(e)
    delivery=TargetExpertDeliveryControl(ctl,source_gate=lambda row,origin:True)
    entry=await _accepted_request(e,ctl,1,1)
    args=await _ready_delivery(e,ctl,delivery,creds,entry,2)
    send=delivery.claim_send_once(*args)
    with pytest.raises(RequestControlError,match='native_receipt_required'):
        delivery.receipt(creds,entry['request_id'],dict(fake='yes'))
    for bad in (dict(client_msg_id='forged'),dict(text='wrong'),
                dict(channel='COTHER')):
        native=dict(app_id='A111',user='UBOT',channel='D111',
                    thread_ts=send['thread_ts'],ts='1792000020.400',
                    client_msg_id=send['delivery_id'],text=send['text'])
        native.update(bad)
        if bad.get('channel')=='COTHER':
            with pytest.raises(RequestControlError,match='native_receipt_mismatch'):
                verified_native_slack_receipt(native,app_id='A111',team_id='T111',
                    bot_user_id='UBOT',channel_id='D111',thread_ts=send['thread_ts'])
        else:
            proof=verified_native_slack_receipt(native,app_id='A111',team_id='T111',
                bot_user_id='UBOT',channel_id='D111',thread_ts=send['thread_ts'])
            with pytest.raises(RequestControlError,match='native_receipt_mismatch'):
                delivery.receipt(creds,entry['request_id'],proof)
