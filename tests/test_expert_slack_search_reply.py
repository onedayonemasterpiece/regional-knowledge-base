"""Read-only Slack bot composition: signed event, original actor, one provider post.

All events/Slack responses are synthetic; no provider, PG, deployment or grants.
"""
import json
from types import SimpleNamespace

import httpx
import pytest
from starlette.applications import Starlette
from starlette.routing import Route

from regional_knowledge.expert_slack_ingress import ExpertSlackIngress, PATH
from regional_knowledge.expert_slack_composition import compose_expert_evidence_receiver
from regional_knowledge.expert_slack_search_reply import (
    ExpertSlackSearchReply, SlackChatSender, answer_from_evidence,
)
from regional_knowledge.participant_contracts import AccountUnlink
from test_expert_request_control import _event, _prepare, _observation
from test_expert_slack_ingress import signed
from test_expert_slack_composition import _owned_ingress
from test_participants import people, change
from test_control_spool import env
from test_kaggle_service_startup import service
from test_read_authority import auth
from test_kaggle_public_backend import target


def _evidence(items, *, status="ok", gap=False):
    return dict(status=status, mode="hybrid", context_gap=gap,
                prior_write_pending=False, evidence=items)


def _sender(**options):
    return SlackChatSender(token="xoxb-synthetic-token",app_id="A111",team_id="T111",
                           bot_user_id="UBOT",**options)


def test_sourced_reply_uses_exact_citation_and_honest_missing_page():
    sourced=answer_from_evidence(_evidence([{
        "title":"Гаузе", "text":"Возникла крепость. <@U111>",
        "metadata":{"citation":"Кёнигсберг, том I","locator":"стр. 31"}}]))
    assert "Гаузе" in sourced and "стр. 31" in sourced and "Кёнигсберг, том I" in sourced
    assert "Возникла крепость." in sourced and "<@U111>" not in sourced
    assert "&lt;@U111&gt;" in sourced
    no_page=answer_from_evidence(_evidence([{
        "title":"Wiechert","text":"Ein Satz","metadata":{"locator":"opaque-id"}}]))
    assert "Страница:* не указана" in no_page
    assert "opaque-id" not in no_page
    assert "не найдено" in answer_from_evidence(_evidence([]))
    for denied in (_evidence([],status="denied"),_evidence([],gap=True),
                   dict(_evidence([]),prior_write_pending=True)):
        with pytest.raises(PermissionError):
            answer_from_evidence(denied)


@pytest.mark.asyncio
async def test_bot_token_team_and_original_thread_verified_before_native_receipt():
    event_id="EvRKB002"
    calls=[]
    def reply(request):
        calls.append(request.url.path)
        if request.url.path.endswith("auth.test"):
            return httpx.Response(200,json={"ok":True,"team_id":"T111","user_id":"UBOT"})
        assert request.url.path.endswith("chat.postMessage")
        data=json.loads(request.content)
        assert data["client_msg_id"]==event_id and data["thread_ts"]=="1792000000.100"
        assert data["channel"]=="C111"
        return httpx.Response(200,json={"ok":True,"channel":"C111","ts":"1792000008.200",
            "message":{"user":"UBOT","text":"Source","client_msg_id":event_id,
                       "thread_ts":"1792000000.100"}})
    receipt=await _sender(transport=httpx.MockTransport(reply)).post_once(
        event_id=event_id,channel="C111",thread="1792000000.100",
        text="Source",current=lambda:True)
    assert receipt["event_id"]==event_id and receipt["bot_user_id"]=="UBOT"
    assert calls==["/api/auth.test","/api/chat.postMessage"]


@pytest.mark.asyncio
async def test_slack_documented_reply_without_optional_echo_fields_is_accepted():
    def documented(request):
        if request.url.path.endswith("auth.test"):
            return httpx.Response(200,json={"ok":True,"team_id":"T111","user_id":"UBOT"})
        return httpx.Response(200,json={"ok":True,"channel":"C111","ts":"1792000010.201",
            "message":{"text":"Source","bot_id":"B111","ts":"1792000010.201"}})
    proof=await _sender(transport=httpx.MockTransport(documented)).post_once(
        event_id="EvReply3",channel="C111",thread="1792000000.100",
        text="Source",current=lambda:True)
    assert proof["event_id"]=="EvReply3" and proof["thread_ts"]=="1792000000.100"


@pytest.mark.asyncio
async def test_wrong_bot_or_ambiguous_echo_does_not_generate_another_post():
    calls=[]
    def wrong(request):
        calls.append(request.url.path)
        return httpx.Response(200,json={"ok":True,"team_id":"T111","user_id":"OTHER"})
    with pytest.raises(PermissionError):
        await _sender(transport=httpx.MockTransport(wrong)).post_once(
            event_id="Ev2",channel="C111",thread="1792000000.100",
            text="Source",current=lambda:True)
    assert calls==["/api/auth.test"]
    calls.clear()
    def no_marker(request):
        calls.append(request.url.path)
        if request.url.path.endswith("auth.test"):
            return httpx.Response(200,json={"ok":True,"team_id":"T111","user_id":"UBOT"})
        return httpx.Response(200,json={"ok":True,"channel":"C111","ts":"1792000008.200",
            "message":{"user":"UBOT","text":"Source","thread_ts":"1792000000.100",
                       "client_msg_id":"wrong-id"}})
    with pytest.raises(RuntimeError,match="echo"):
        await _sender(transport=httpx.MockTransport(no_marker)).post_once(
            event_id="Ev2",channel="C111",thread="1792000000.100",
            text="Source",current=lambda:True)
    assert calls==["/api/auth.test","/api/chat.postMessage"]


@pytest.mark.asyncio
async def test_signed_read_only_event_is_acked_once_without_dataset_or_bot_loop(env):
    control,_,_= _prepare(env)
    ingress=ExpertSlackIngress(control,signing_secret="test-signing-secret",
                               clock=lambda:1792000001)
    received=[]
    reply=SimpleNamespace(control=control,ingress=ingress,notify=received.append)
    ingress.bind_search_reply(reply)
    app=Starlette(routes=[Route(PATH,ingress.__call__,methods=["POST"])])
    raw,head=_event()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                 base_url="http://test") as http:
        for _ in range(2):
            outcome=await http.post(PATH,content=raw,headers=head)
            assert outcome.status_code==200 and outcome.json()=={"ok":True}
        assert len(received)==2 and len(set(received))==1
        with env.p.corpus.connect() as db:
            records=db.execute("SELECT event_id,state FROM rkb_expert_requests").fetchall()
            assert len(records)==1 and records[0]["state"]=="received"
        tampered=dict(head,**{"X-Slack-Signature":"v0="+"0"*64})
        assert (await http.post(PATH,content=raw,headers=tampered)).status_code==403
        bot=json.loads(raw);bot["event"]["user"]="UBOT"
        b,h=signed(bot)
        assert (await http.post(PATH,content=b,headers=h)).status_code==200
        assert len(received)==2


def test_original_nonowner_search_dedup_and_unknown_are_same_existing_event_row(service):
    s=service
    control,credentials,link,ingress=_owned_ingress(s)
    receiver=compose_expert_evidence_receiver(target_startup=s.startup,
        expert_slack_ingress=ingress)
    reply=ExpertSlackSearchReply(ingress=ingress,evidence=receiver,sender=_sender())
    entry=control.accept(_observation())
    rid=entry["request_id"]
    first=reply._row(rid,("received","searching"),begin=True)
    assert first["person_id"]==s.target.auth.p.other.subject
    assert first["person_id"]!=s.target.auth.p.owner.subject
    assert reply._row(rid,("searching",)) is not None
    assert reply._claim_post(rid)["event_id"]==first["event_id"]
    assert reply._claim_post(rid) is None  # event_id uniqueness gives one permit
    assert reply._finish(rid,"sending","unknown")
    assert reply._row(rid,("received","searching")) is None
    # A duplicate signed event cannot recreate a post permit after UNKNOWN.
    assert control.accept(_observation())["request_id"]==rid
    assert reply._claim_post(rid) is None


def test_revoked_slack_nonowner_grant_cannot_obtain_send_permit(service):
    s=service
    control,credentials,link,ingress=_owned_ingress(s)
    receiver=compose_expert_evidence_receiver(target_startup=s.startup,
        expert_slack_ingress=ingress)
    reply=ExpertSlackSearchReply(ingress=ingress,evidence=receiver,sender=_sender())
    rid=control.accept(_observation())["request_id"]
    assert reply._row(rid,("received",),begin=True) is not None
    change(s.target.auth.p,AccountUnlink(link_id=link.link_id,expected_link_revision=2,
        reason="Synthetic revocation",incorrect_binding=True))
    with pytest.raises(Exception):
        reply._claim_post(rid)
    with s.target.auth.p.corpus.connect() as db:
        row=db.execute("SELECT state,delivery_attempt FROM rkb_expert_requests WHERE id=?",(rid,)).fetchone()
        assert row["state"]=="searching" and row["delivery_attempt"]==0
