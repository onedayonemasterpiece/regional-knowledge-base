"""Original native provider observation → exact Dataset receipt callback.

Real existing BgeQueue, TargetExpertDeliveryControl, DatasetRecoveryStore,
request/case identity, immutable child CAS. Only outside provider/native
capture is existing synthetic fixture. NO network/provider send or new IAM.
"""
import pytest

from regional_knowledge.expert_executor_workflow import ExpertExecutorContext
from regional_knowledge.expert_executor_native_receipt import BoundExpertNativeReceipt
from regional_knowledge.expert_delivery_control import verified_native_slack_receipt
from regional_knowledge.expert_request_control import RequestControlError
from test_control_spool import env
from test_participants import people
from test_expert_request_recovery import attached
from test_expert_delivery_recovery import (
    accepted,prepare,promote,receipt_for,lose_local_and_reopen,
)
from test_expert_executor_recovery_receiver import _unknown_after_covered_send


@pytest.mark.asyncio
async def test_original_native_receipt_pending_then_remote_covered_delivered_once(env,tmp_path):
    a=attached(env,tmp_path)
    entry,delivery,outbound,args=await _unknown_after_covered_send(a)
    original=a.ctl._row(entry['request_id'])
    assert original['state']=='unknown' and original['delivery_attempt']==1
    # This proof represents already observed native-provider correlation in
    # the existing test harness; this component never calls sendMessage.
    native=receipt_for(a,outbound)
    context=ExpertExecutorContext(a.credentials,delivery)
    bound=context.native_receipt_consumer()
    assert type(bound) is BoundExpertNativeReceipt
    assert bound.delivery is delivery and bound.context is context
    first=await bound.observe(entry['request_id'],native)
    assert first==dict(state='receipt_pending',remote_covered=False,
                       retry_send_allowed=False)
    row=a.ctl._row(entry['request_id'])
    assert row['state']=='unknown' and row['delivery_receipt'] is None
    assert row['delivery_attempt']==1
    assert a.ctl.claim(a.credentials)=={'state':'empty'}
    # The original Dataset publisher/capture promoted exact receipt bytes.
    promote(a,5)
    assert a.saved.store.latest_checkpoint.dataset_version==5
    final=await bound.observe(entry['request_id'],native)
    assert final==dict(state='delivered',remote_covered=True,
                       retry_send_allowed=False)
    repeated=await bound.observe(entry['request_id'],native)
    assert repeated==final
    row=a.ctl._row(entry['request_id'])
    assert row['state']=='delivered' and row['delivery_receipt']
    assert row['delivery_attempt']==1
    assert a.ctl.claim(a.credentials)=={'state':'empty'}
    # Lost local ACK cannot mint another permit; recovered records preserve
    # the exact original native observation and settled dialog.
    ctl,recovered=lose_local_and_reopen(a)
    settled=recovered.reconcile_restored_dataset_delivery(
        a.credentials,entry['request_id'])
    assert settled['state']=='delivered' and settled['retry_send_allowed'] is False
    assert ctl._row(entry['request_id'])['delivery_attempt']==1


@pytest.mark.asyncio
async def test_only_trusted_exact_native_provider_proof_can_stage_receipt(env,tmp_path):
    a=attached(env,tmp_path)
    entry,delivery,outbound,args=await _unknown_after_covered_send(a)
    context=ExpertExecutorContext(a.credentials,delivery)
    bound=context.native_receipt_consumer()
    native=receipt_for(a,outbound)
    for forged in (None,True,{'verified':True},native.__dict__):
        with pytest.raises(RequestControlError,match='native_receipt_required'):
            await bound.observe(entry['request_id'],forged)
    with pytest.raises(RequestControlError,match='request_unavailable'):
        await bound.observe('untrusted-request-id',native)
    # Native observation has actual identity but mismatches the original
    # immutable send marker/text; original delivery gate rejects it.
    for kwargs in (dict(change_id='wrong-message-id'),
                   dict(change_text='A forged replacement answer')):
        foreign=receipt_for(a,outbound,**kwargs)
        with pytest.raises(Exception,match='native_receipt_mismatch'):
            await bound.observe(entry['request_id'],foreign)
    with a.env.p.corpus.connect() as db:
        row=db.execute('SELECT state,delivery_attempt,delivery_receipt '
                       'FROM rkb_expert_requests WHERE id=?',
                       (entry['request_id'],)).fetchone()
        assert tuple(row)==('unknown',1,None)
    assert a.ctl.claim(a.credentials)=={'state':'empty'}


@pytest.mark.asyncio
async def test_covered_intake_alone_never_licenses_native_receipt_without_send(env,tmp_path):
    a=attached(env,tmp_path)
    entry,args=await accepted(a)
    d=prepare(a)
    native=verified_native_slack_receipt(
        dict(app_id='A111',user='UBOT',channel='D111',
             thread_ts='1792000000.100',ts='1792000123.100',
             client_msg_id='forged-previous-send',text='Synthetic text'),
        app_id='A111',team_id='T111',bot_user_id='UBOT',
        channel_id='D111',thread_ts='1792000000.100')
    bridge=ExpertExecutorContext(a.credentials,d).native_receipt_consumer()
    with pytest.raises(RequestControlError,match='delivery_unavailable'):
        await bridge.observe(entry['request_id'],native)
    with a.env.p.corpus.connect() as db:
        row=db.execute('SELECT state,delivery_attempt,delivery_receipt FROM '
                       'rkb_expert_requests WHERE id=?',
                       (entry['request_id'],)).fetchone()
        assert tuple(row)==('leased',0,None)
    with pytest.raises(TypeError,match='existing provisioned executor'):
        BoundExpertNativeReceipt(None)
