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


async def _accepted_request(env, ctl, number, sequence, *, thread="1792000000.100", group=False):
    entry=ctl.accept(_observation(n=number,thread=thread,group=group))
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
async def test_expired_pre_send_lease_recovers_original_result_and_send_intents(env):
    e=env;ctl,creds,_=_prepare(e)
    now=[1792000001.0]
    ctl.clock=lambda:now[0]
    delivery=TargetExpertDeliveryControl(ctl,source_gate=lambda row,origin:True)
    entry=await _accepted_request(e,ctl,1,1)
    first=ctl.claim(creds,request_id=entry['request_id'])
    one=(creds,entry['request_id'],first['generation'],first['execution_token'])
    assert delivery.prepare_result(*one,text='Exact preserved answer.')['state']=='result_unsealed'
    original_result=delivery.exact_result_intent(entry['request_id'],2)
    assert original_result.expected_revisions['generation']=='1'
    now[0]+=125
    second=ctl.claim(creds,request_id=entry['request_id'])
    assert second['generation']==2 and second['resume_state']=='result_unsealed'
    two=(creds,entry['request_id'],second['generation'],second['execution_token'])
    with pytest.raises(RequestControlError,match='request_fence_denied'):
        delivery.prepare_result(*one,text='Exact preserved answer.')
    assert delivery.prepare_result(*two,text='Exact preserved answer.')['state']=='result_unsealed'
    with pytest.raises(RequestControlError,match='result_conflict'):
        delivery.prepare_result(*two,text='Changed after remote intent?')
    assert delivery.exact_result_intent(entry['request_id'],2).encode()==original_result.encode()
    seal=await e.journal.seal([original_result],predecessor=await e.journal.head(),writer_generation='writer-1')
    await delivery.verify_remote_result(*two,original_result,seal)
    original_send=delivery.exact_send_intent(entry['request_id'],3)
    assert original_send.expected_revisions['generation']=='1'
    now[0]+=125
    third=ctl.claim(creds,request_id=entry['request_id'])
    assert third['generation']==3 and third['resume_state']=='result_ready'
    three=(creds,entry['request_id'],third['generation'],third['execution_token'])
    assert delivery.exact_send_intent(entry['request_id'],3).encode()==original_send.encode()
    seal=await e.journal.seal([original_send],predecessor=await e.journal.head(),writer_generation='writer-1')
    await delivery.verify_remote_send(*three,original_send,seal)
    now[0]+=125
    fourth=ctl.claim(creds,request_id=entry['request_id'])
    assert fourth['generation']==4 and fourth['resume_state']=='send_intent_ready'
    four=(creds,entry['request_id'],fourth['generation'],fourth['execution_token'])
    assert delivery.exact_send_intent(entry['request_id'],3).encode()==original_send.encode()
    with pytest.raises(RequestControlError,match='request_fence_denied'):
        delivery.claim_send_once(*three)
    outgoing=delivery.claim_send_once(*four)
    assert outgoing['delivery_id']==original_send.ids['client_msg_id']
    assert outgoing['text']=='Exact preserved answer.'
    now[0]+=125
    assert ctl.claim(creds,request_id=entry['request_id'])=={'state':'empty'}
    delivery.uncertain_delivery(creds,entry['request_id'],outgoing['delivery_id'])
    assert ctl.claim(creds,request_id=entry['request_id'])=={'state':'empty'}
    with e.p.corpus.connect() as db:
        row=db.execute('SELECT generation,result_generation,delivery_attempt FROM rkb_expert_requests WHERE id=?',
                       (entry['request_id'],)).fetchone()
        assert tuple(row)==(4,1,1)


@pytest.mark.asyncio
async def test_revoked_group_allowlist_blocks_context_and_final_send(env):
    e=env;ctl,creds,_=_prepare(e)
    delivery=TargetExpertDeliveryControl(ctl,source_gate=lambda row,origin:True)
    entry=await _accepted_request(e,ctl,1,1,group=True)
    args=await _ready_delivery(e,ctl,delivery,creds,entry,2)
    assert ctl.claim_context(*args)['origin']['channel_id']=='C111'
    # After a valid intake and remote send intent, the deployed policy changes.
    # Reopening the same control database must recheck allowlist before pixels.
    updated=TargetExpertRequestControl(e.p.registry,e.spool,app_id='A111',team_id='T111',
        bot_user_id='UBOT',allowed_channels=(),clock=lambda:1792000001.0)
    later=TargetExpertDeliveryControl(updated,source_gate=lambda row,origin:True)
    with pytest.raises(RequestControlError,match='slack_destination_revoked'):
        updated.claim_context(*args)
    with pytest.raises(RequestControlError,match='slack_destination_revoked'):
        later.claim_send_once(*args)
    with e.p.corpus.connect() as db:
        row=db.execute('SELECT state,delivery_attempt FROM rkb_expert_requests WHERE id=?',(entry['request_id'],)).fetchone()
        assert tuple(row)==('send_intent_ready',0)
    # If policy was already withdrawn before claim, there is no execution.
    separate=await _accepted_request(e,ctl,2,4,thread='1792000000.222',group=True)
    updated.allowed_channels=frozenset()
    assert updated.claim(creds,request_id=separate['request_id'])=={
        'state':'blocked','request_id':separate['request_id']}
    with e.p.corpus.connect() as db:
        assert db.execute('SELECT state FROM rkb_expert_requests WHERE id=?',
                          (separate['request_id'],)).fetchone()[0]=='blocked'


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
