"""Dataset-original result/send/provider receipt recovery, synthetic only.

Real existing BgeQueue + DatasetRecoveryStore + TargetExpertDeliveryControl.
No provider call, credential, PostgreSQL, second journal, or new IAM/queue.
"""
import hashlib
import json
from types import SimpleNamespace

import pytest

from regional_knowledge.expert_delivery_control import (
    TargetExpertDeliveryControl, verified_native_slack_receipt,
)
from regional_knowledge.expert_delivery_recovery import (
    ExpertDeliveryRecoveryError, result_bytes, parse_result, send_bytes,
)
from regional_knowledge.expert_request_control import (
    TargetExpertRequestControl, RequestControlError,
)
from regional_knowledge.participant_contracts import AccountUnlink
from test_control_spool import env
from test_expert_request_control import _observation
from test_expert_request_recovery import attached,next_checkpoint
from test_participants import people,change


def promote(a, version):
    store=a.saved.store
    capture=store.export_control()
    checkpoint,reference,publisher,readback=next_checkpoint(
        a.saved,capture,version=version)
    store.publisher=publisher
    store.promote(checkpoint,readback,manifest_ref=reference)
    assert store.latest_checkpoint.dataset_version==version
    return capture


def prepare(a):
    return TargetExpertDeliveryControl(a.ctl,source_gate=lambda row,origin:True)


def intake_ready(a):
    entry=a.ctl.accept(_observation())
    return entry


async def accepted(a):
    entry=intake_ready(a)
    assert await a.ctl.stage_received_dataset(entry['request_id']) is False
    promote(a,2)
    assert a.ctl.reconcile_dataset_coverage()==0
    assert a.ctl.reconcile_dataset_coverage()==1
    assert a.ctl.confirm_dataset_coverage(entry['request_id'])
    lease=a.ctl.claim(a.credentials,request_id=entry['request_id'])
    assert lease['state']=='leased'
    args=(a.credentials,entry['request_id'],lease['generation'],
          lease['execution_token'])
    return entry,args


async def result_staged(a,delivery,args):
    entry_id=args[1]
    assert delivery.prepare_result(*args,text='A sourced original answer.')['state']=='result_unsealed'
    assert await delivery.stage_dataset_result(*args) is False
    assert delivery.confirm_dataset_result(*args)['remote_accepted'] is False
    return a.ctl._row(entry_id)


def receipt_for(a, outbound, *, change_id=None,change_text=None):
    return verified_native_slack_receipt(
        dict(app_id='A111',user='UBOT',channel='D111',
             thread_ts=outbound['thread_ts'],ts='1792000099.300',
             client_msg_id=change_id or outbound['delivery_id'],
             text=change_text or outbound['text']),
        app_id='A111',team_id='T111',bot_user_id='UBOT',
        channel_id='D111',thread_ts=outbound['thread_ts'])


def lose_local_and_reopen(a):
    with a.env.p.corpus.connect() as db:
        db.execute('DELETE FROM rkb_expert_requests')
        db.execute('DELETE FROM rkb_request_dialogs')
        db.execute('DELETE FROM rkb_request_people')
    ctl=TargetExpertRequestControl(a.env.p.registry,a.env.spool,
        app_id='A111',team_id='T111',bot_user_id='UBOT',
        allowed_channels=('C111',),clock=lambda:1792000001.)
    delivery=TargetExpertDeliveryControl(ctl,source_gate=lambda row,origin:True)
    return ctl,delivery


@pytest.mark.asyncio
async def test_covered_original_result_without_send_recovers_no_new_send_permission(env,tmp_path):
    a=attached(env,tmp_path)
    entry,args=await accepted(a)
    d=prepare(a)
    await result_staged(a,d,args)
    promote(a,3)
    assert d.confirm_dataset_result(*args)['state']=='result_ready'
    ctl,recovered=lose_local_and_reopen(a)
    assert ctl._row(entry['request_id'])['state']=='recovery_review_required'
    state=recovered.reconcile_restored_dataset_delivery(a.credentials,entry['request_id'])
    assert state['state']=='recovered_result_ready'
    assert state['retry_send_allowed'] is False
    assert state['claim_allowed'] is False
    assert state['missing_contract']=='original_send_disposition'
    assert ctl.claim(a.credentials)=={'state':'empty'}
    with pytest.raises(RequestControlError):
        recovered.claim_send_once(*args)
    # Exact covered intent is recoverable but never a new provider permit.
    assert recovered.reconcile_restored_dataset_delivery(
        a.credentials,entry['request_id'])['state']=='recovered_result_ready'


@pytest.mark.asyncio
async def test_pending_send_and_covered_send_without_receipt_remain_nonresendable_unknown(env,tmp_path):
    a=attached(env,tmp_path)
    entry,args=await accepted(a)
    d=prepare(a)
    await result_staged(a,d,args)
    promote(a,3)
    d.confirm_dataset_result(*args)
    assert await d.stage_dataset_send(*args) is False
    assert d.confirm_dataset_send(*args)['remote_accepted'] is False
    # Local send intent is not covered by the v3 checkpoint; restore may not
    # infer provider absence or original send permission from it.
    ctl,recovered=lose_local_and_reopen(a)
    result=recovered.reconcile_restored_dataset_delivery(a.credentials,entry['request_id'])
    assert result['state']=='recovered_result_ready'
    assert ctl.claim(a.credentials)=={'state':'empty'}
    # Persist EXACT original result+send bytes on the next verified checkpoint.
    promote(a,4)
    result=recovered.reconcile_restored_dataset_delivery(a.credentials,entry['request_id'])
    assert result['state']=='unknown'
    assert result['retry_send_allowed'] is False
    row=ctl._row(entry['request_id'])
    assert row['delivery_id'] and row['send_intent_hash'] and row['result_remote_hash']
    assert row['delivery_attempt']>=1
    assert ctl.claim(a.credentials)=={'state':'empty'}
    with pytest.raises(RequestControlError,match='request_fence_denied'):
        recovered.claim_send_once(*args)
    assert recovered.reconcile_restored_dataset_delivery(
        a.credentials,entry['request_id'])['state']=='unknown'


@pytest.mark.asyncio
async def test_covered_native_receipt_delivered_exact_once_even_after_local_loss(env,tmp_path):
    a=attached(env,tmp_path)
    entry,args=await accepted(a)
    initial_head=await a.env.journal.head()
    with a.env.q.connect(write=False) as db:
        initial_seq=db.execute(
            'SELECT sequence FROM rkb_control_spool_meta WHERE id=1').fetchone()[0]
    d=prepare(a)
    await result_staged(a,d,args)
    promote(a,3)
    d.confirm_dataset_result(*args)
    await d.stage_dataset_send(*args)
    promote(a,4)
    assert d.confirm_dataset_send(*args)['state']=='send_intent_ready'
    original_send_hash=a.ctl._row(entry['request_id'])['send_intent_hash']
    with a.env.p.corpus.connect() as db:
        db.execute('UPDATE rkb_expert_requests SET send_intent_hash=? WHERE id=?',
                   ('0'*64,entry['request_id']))
    with pytest.raises(RequestControlError,match='dataset_original_intent_not_covered'):
        d.claim_send_once(*args)
    with a.env.p.corpus.connect() as db:
        db.execute('UPDATE rkb_expert_requests SET send_intent_hash=? WHERE id=?',
                   (original_send_hash,entry['request_id']))
    outbound=d.claim_send_once(*args)
    assert outbound['delivery_id']
    d.uncertain_delivery(a.credentials,entry['request_id'],outbound['delivery_id'])
    receipt=receipt_for(a,outbound)
    assert await d.stage_dataset_native_receipt(a.credentials,entry['request_id'],receipt) is False
    with pytest.raises(RequestControlError,match='dataset_delivery_receipt_not_covered'):
        d.confirm_dataset_native_receipt(a.credentials,entry['request_id'],receipt)
    # A lost provider ACK is UNKNOWN, not permission for a second send.
    assert a.ctl.claim(a.credentials)=={'state':'empty'}
    promote(a,5)
    # Local loss discards historical result/send/receipt fields.
    ctl,recovered=lose_local_and_reopen(a)
    r=recovered.reconcile_restored_dataset_delivery(a.credentials,entry['request_id'])
    assert r['state']=='delivered'
    row=ctl._row(entry['request_id'])
    assert row['state']=='delivered'
    assert row['delivery_attempt']==1
    assert row['delivery_receipt']
    assert ctl.claim(a.credentials)=={'state':'empty'}
    previous_receipt=row['delivery_receipt']
    assert recovered.reconcile_restored_dataset_delivery(
        a.credentials,entry['request_id'])['state']=='delivered'
    assert ctl._row(entry['request_id'])['state']=='delivered'
    assert ctl._row(entry['request_id'])['delivery_receipt']==previous_receipt
    with a.env.p.corpus.connect() as db:
        row=db.execute('SELECT head_seq FROM rkb_request_dialogs').fetchone()
        assert row[0]==2
    covered,_=ctl.dataset_records.covered_page()
    assert len(covered)==1  # child result/send/receipt must not poison intake pages
    with a.env.q.connect(write=False) as db:
        current_seq=db.execute(
            'SELECT sequence FROM rkb_control_spool_meta WHERE id=1').fetchone()[0]
    assert initial_seq==current_seq
    assert await a.env.journal.head()==initial_head


@pytest.mark.asyncio
async def test_receipt_fails_when_message_id_or_text_mismatch_and_no_caller_success_bool(env,tmp_path):
    a=attached(env,tmp_path)
    entry,args=await accepted(a)
    d=prepare(a)
    await result_staged(a,d,args)
    promote(a,3)
    d.confirm_dataset_result(*args)
    original=a.ctl._row(entry['request_id'])['result_hash']
    with a.env.p.corpus.connect() as db:
        db.execute('UPDATE rkb_expert_requests SET result_hash=? WHERE id=?',
                   ('0'*64,entry['request_id']))
    with pytest.raises(ExpertDeliveryRecoveryError,match='original_result_identity_changed'):
        await d.stage_dataset_send(*args)
    with a.env.p.corpus.connect() as db:
        db.execute('UPDATE rkb_expert_requests SET result_hash=? WHERE id=?',
                   (original,entry['request_id']))
    await d.stage_dataset_send(*args)
    promote(a,4)
    d.confirm_dataset_send(*args)
    outbound=d.claim_send_once(*args)
    d.uncertain_delivery(a.credentials,entry['request_id'],outbound['delivery_id'])
    for proof in (receipt_for(a,outbound,change_id='wrong-native-message-id'),
                  receipt_for(a,outbound,change_text='Different source text')):
        with pytest.raises(ExpertDeliveryRecoveryError,match='native_receipt_mismatch'):
            await d.stage_dataset_native_receipt(a.credentials,entry['request_id'],proof)
    with pytest.raises(ExpertDeliveryRecoveryError,match='native_receipt_required'):
        await d.stage_dataset_native_receipt(a.credentials,entry['request_id'],True)
    row=a.ctl._row(entry['request_id'])
    assert row['state']=='unknown' and row['delivery_receipt'] is None
    assert a.ctl.claim(a.credentials)=={'state':'empty'}


@pytest.mark.asyncio
async def test_source_revocation_does_not_restore_send_permit(env,tmp_path):
    a=attached(env,tmp_path)
    entry,args=await accepted(a)
    d=prepare(a)
    await result_staged(a,d,args)
    promote(a,3)
    d.confirm_dataset_result(*args)
    await d.stage_dataset_send(*args)
    promote(a,4)
    d.confirm_dataset_send(*args)
    change(a.env.p,AccountUnlink(link_id=a.link.link_id,expected_link_revision=2,
                                 reason='wrong binding',incorrect_binding=True))
    with pytest.raises(Exception):
        d.claim_send_once(*args)
    ctl,recovered=lose_local_and_reopen(a)
    with a.env.p.corpus.connect() as db:
        assert db.execute('SELECT COUNT(*) FROM rkb_expert_requests').fetchone()[0]==0
    assert ctl.claim(a.credentials)=={'state':'empty'}
