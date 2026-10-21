"""CAS-F10: verified Slack intake and exact remote intent admission, synthetic only."""
import hashlib
import hmac
import json
from dataclasses import replace

import pytest

from regional_knowledge.expert_request_control import (
    TargetExpertRequestControl, ExecutorCredentials, RequestControlError,
    verified_slack_event,
)
from regional_knowledge.participant_contracts import ParticipantGrant, AccountUnlink
from test_control_spool import env
from test_participants import people, bind, change


def _event(n=1, *, text="Which event is supported by this source?", thread="1792000000.100", group=False):
    payload = {"api_app_id": "A111", "team_id": "T111", "event_id": f"Ev-{n}",
               "type": "event_callback", "event": {
                   "user": "U111", "channel": "C111" if group else "D111", "channel_type": "channel" if group else "im",
                   "type": "app_mention" if group else "message", "ts": f"1792000000.{n:03d}",
                   "thread_ts": thread, "text": text}}
    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
    stamp = "1792000000"
    signature = "v0=" + hmac.new(b"test-signing-secret",
        b"v0:" + stamp.encode() + b":" + raw, hashlib.sha256).hexdigest()
    headers = {"X-Slack-Request-Timestamp": stamp, "X-Slack-Signature": signature}
    return raw, headers


def _observation(*args, **kwargs):
    raw,headers=_event(*args, **kwargs)
    return verified_slack_event(raw,headers,"test-signing-secret",
        app_id="A111",team_id="T111",bot_user_id="UBOT",now=1792000001)


def _prepare(e):
    change(e.p,ParticipantGrant(capability="public_search",granted=True,
        reason="Synthetic verified expert test"))
    expert=bind(e.p)
    ctl=TargetExpertRequestControl(e.p.registry,e.spool,app_id="A111",
        team_id="T111",bot_user_id="UBOT",allowed_channels=("C111",),
        clock=lambda:1792000001.0)
    creds=ExecutorCredentials("rkb-request-test-executor","synthetic-executor-token")
    ctl.provision_executor(e.p.admin,creds.service_id,creds.token)
    return ctl,creds,expert


@pytest.mark.asyncio
async def test_verified_slack_retry_is_same_received_request_not_a_saved_result(env):
    e=env;ctl,creds,_=_prepare(e)
    obs=_observation()
    first=ctl.accept(obs)
    assert first["state"]=="received" and first["durable"] is False
    assert ctl.accept(obs)==first
    assert ctl.claim(creds)=={"state":"empty"}
    assert not e.store.objects  # No local-only acceptance in the remote journal.
    with e.p.corpus.connect() as db:
        assert db.execute("SELECT count(*) FROM rkb_expert_requests").fetchone()[0]==1
        assert db.execute("SELECT count(*) FROM rkb_request_dialogs").fetchone()[0]==1
    # The event's identity is not a mutable label on a resubmission.
    with pytest.raises(RequestControlError,match="event_id_conflict"):
        ctl.accept(replace(_observation(),text="Tampered second message"))
    raw,head=_event()
    head["X-Slack-Signature"]="v0"+"0"*64
    with pytest.raises(RequestControlError,match="slack_signature_invalid"):
        verified_slack_event(raw,head,"test-signing-secret",
            app_id="A111",team_id="T111",bot_user_id="UBOT",now=1792000001)
    with pytest.raises(RequestControlError,match="verified_slack_transport_required"):
        ctl.accept(replace(obs,_trusted=None))


@pytest.mark.asyncio
async def test_same_recovery_journal_proves_remote_intent_before_claim_and_identity_is_current(env):
    e=env;ctl,creds,expert=_prepare(e)
    first=ctl.accept(_observation())
    command=ctl.exact_remote_intent(first["request_id"],sequence=1)
    # A locator alone does not grant admission; use the existing exact remote
    # journal's original writer-generation acceptance/readback.
    assert ctl.claim(creds)=={"state":"empty"}
    seal=await e.journal.seal([command],predecessor=None,writer_generation="writer-1")
    ready=await ctl.verify_remote_intent(first["request_id"],command,seal)
    assert ready=={"request_id":first["request_id"],"state":"pending",
                   "remote_accepted":True,"saved":False}
    assert (await ctl.verify_remote_intent(first["request_id"],command,seal))["state"]=="pending"
    lease=ctl.claim(creds,request_id=first["request_id"])
    assert lease["state"]=="leased" and lease["generation"]==1
    ctx=ctl.claim_context(creds,lease["request_id"],lease["generation"],
                          lease["execution_token"])
    assert ctx["person_id"]==e.p.other.subject and ctx["origin"]["user_id"]=="U111"
    with pytest.raises(RequestControlError,match="request_fence_denied"):
        ctl.claim_context(creds,lease["request_id"],lease["generation"],
                          "forged-execution-token")
    # A later incorrect/unverified link cannot continue existing claimed work.
    change(e.p,AccountUnlink(link_id=expert.link_id,expected_link_revision=2,
        reason="wrong synthetic author",incorrect_binding=True))
    with pytest.raises(Exception):
        ctl.claim_context(creds,lease["request_id"],lease["generation"],
                          lease["execution_token"])


@pytest.mark.asyncio
async def test_remote_intent_mismatch_cannot_claim_or_mutate_another_event(env):
    e=env;ctl,creds,_=_prepare(e)
    one=ctl.accept(_observation())
    two=ctl.accept(_observation(n=2))
    cmd=ctl.exact_remote_intent(one["request_id"],1)
    seal=await e.journal.seal([cmd],predecessor=None,writer_generation="writer-1")
    with pytest.raises(RequestControlError,match="remote_intent_conflict"):
        await ctl.verify_remote_intent(two["request_id"],cmd,seal)
    # Incorrect command/sequence cannot mark local metadata durable.
    bad=replace(cmd,payload=b"{}")
    with pytest.raises(RequestControlError,match="remote_intent_conflict"):
        await ctl.verify_remote_intent(one["request_id"],bad,seal)
    assert ctl.claim(creds)=={"state":"empty"}
    assert ctl.accept(_observation(n=2))["state"]=="received"


@pytest.mark.asyncio
@pytest.mark.parametrize('field,value',[
    ('operation_key','expert-request:forged-operation'),
    ('logical_time','9999999999.99'),
    ('schema_revision','forged-schema'),
    ('function_revision','forged-function'),
    ('executor_revision','forged-executor'),
    ('kind','provider_intent'),
])
async def test_remote_proof_of_altered_exact_envelope_is_not_original_intent(env,field,value):
    e=env;ctl,creds,_=_prepare(e)
    req=ctl.accept(_observation())
    original=ctl.exact_remote_intent(req['request_id'],1)
    forged=replace(original,**{field:value})
    # Even genuine membership of an altered command in the REMOTE journal
    # must not reclassify this *different* original request as accepted.
    seal=await e.journal.seal([forged],predecessor=None,writer_generation='writer-1')
    with pytest.raises(RequestControlError,match='remote_intent_conflict'):
        await ctl.verify_remote_intent(req['request_id'],forged,seal)
    assert ctl._row(req['request_id'])['state']=='received'
    assert ctl.claim(creds)=={'state':'empty'}


@pytest.mark.asyncio
async def test_ordered_claim_cannot_jump_same_dialog_or_old_lease(env):
    e=env;ctl,creds,_=_prepare(e)
    first=ctl.accept(_observation())
    second=ctl.accept(_observation(n=2))
    for seq,entry in enumerate((first,second),1):
        command=ctl.exact_remote_intent(entry["request_id"],seq)
        prior=await e.journal.head()
        seal=await e.journal.seal([command],predecessor=prior,writer_generation="writer-1")
        await ctl.verify_remote_intent(entry["request_id"],command,seal)
    assert ctl.claim(creds,request_id=second["request_id"])=={"state":"deferred"}
    claim=ctl.claim(creds,request_id=first["request_id"])
    assert claim["state"]=="leased"
    assert ctl.claim(creds,request_id=second["request_id"])=={"state":"empty"}
    # These are only request-intent and lease controls; no Slack send may occur
    # before a separately verified result + outbox receipt.
    with e.p.corpus.connect() as db:
        assert db.execute("SELECT count(*) FROM rkb_expert_requests WHERE delivery_id IS NOT NULL").fetchone()[0]==0
