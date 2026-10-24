"""Source-only Slack bot reply tests; no provider, credentials, PG or live I/O."""
import asyncio
import json
from types import SimpleNamespace

import httpx
import pytest
from starlette.applications import Starlette
from starlette.routing import Route

from regional_knowledge.expert_slack_search_reply import (
    SlackChatSender, _BoundSearchGate, answer_from_evidence,
    compose_expert_slack_search_reply,
)
from regional_knowledge.expert_slack_ingress import ExpertSlackIngress, PATH
from regional_knowledge.expert_slack_composition import compose_expert_evidence_receiver
from test_expert_request_control import _event, _prepare
from test_expert_slack_composition import _owned_ingress
from test_control_spool import env
from test_kaggle_service_startup import service
from test_read_authority import auth
from test_kaggle_public_backend import target


def evidence(items, *, status="ok", context_gap=False):
    return dict(status=status, context_gap=context_gap, mode="hybrid",
                evidence=items, prior_write_pending=False)


def test_answer_is_only_source_citation_page_and_exact_text():
    body=answer_from_evidence(evidence([{
        "title": "История города",
        "text": "В 1255 году заложена крепость. <@U111> ",
        "metadata":{"citation":"Гаузе. Кёнигсберг, том I",
                    "locator":"стр. 31"},
        "url":"urn:rkb:passage:abcd",
    }]))
    assert "История города" in body and "стр. 31" in body
    assert "Гаузе. Кёнигсберг" in body
    assert "В 1255 году заложена крепость." in body
    assert "&lt;@U111&gt;" in body and "<@U111>" not in body
    assert "https://" not in body and "urn:rkb" not in body
    assert "Страница: стр. 31" in body
    assert "подтвержден" not in body


def test_missing_page_and_empty_search_are_honest():
    result=answer_from_evidence(evidence([{
        "title":"Wiechert", "text":"Ein deutscher Satz.",
        "metadata":{"locator":"passage:abc","citation":None},
    }]))
    assert "Страница: не указана" in result
    assert "Локатор: passage:abc" in result
    empty=answer_from_evidence(evidence([]))
    assert "не найдено" in empty and "не установлены" in empty
    for unsafe in (
        evidence([],status="denied"),
        evidence([],context_gap=True),
        evidence([],status="unavailable"),
        dict(evidence([]),prior_write_pending=True),
        evidence([{"title":"x","text":"","metadata":{}}]),
    ):
        with pytest.raises(ValueError):
            answer_from_evidence(unsafe)


def _permit():
    return {"request_id":"fixture-request", "delivery_id":"fixture-deterministic-marker",
            "channel_id":"C111", "thread_ts":"1792000000.100",
            "text":"Source citation and exact excerpt."}


@pytest.mark.asyncio
async def test_slack_sender_auth_test_and_one_native_chat_post_with_original_thread():
    calls=[]
    permit=_permit()
    def respond(request):
        path=request.url.path
        calls.append((path,request.headers.get("Authorization"),
                      json.loads(request.content) if path.endswith("chat.postMessage") else None))
        if path.endswith("auth.test"):
            return httpx.Response(200,json={"ok":True,"team_id":"T111","user_id":"UBOT"})
        if path.endswith("chat.postMessage"):
            return httpx.Response(200,json={"ok":True, "channel":"C111",
                "ts":"1792000002.200","message":{"user":"UBOT",
                  "thread_ts":permit["thread_ts"],"text":permit["text"],
                  "client_msg_id":permit["delivery_id"]}})
        pytest.fail("unexpected Slack API path")
    sender=SlackChatSender(token="xoxb-synthetic-token",app_id="A111",team_id="T111",
        bot_user_id="UBOT",transport=httpx.MockTransport(respond))
    native=await sender.post_once(permit)
    assert native.channel_id==permit["channel_id"]
    assert native.client_message_id==permit["delivery_id"]
    assert native.thread_ts==permit["thread_ts"]
    assert len(calls)==2
    assert calls[-1][2]=={"channel":"C111","thread_ts":"1792000000.100",
                           "text":permit["text"],"client_msg_id":permit["delivery_id"],
                           "unfurl_links":False,"unfurl_media":False}
    assert all(h=="Bearer xoxb-synthetic-token" for _,h,_ in calls)


@pytest.mark.asyncio
async def test_mismatched_bot_or_unverified_echo_cannot_create_receipt():
    permit=_permit()
    paths=[]
    def wrong_actor(request):
        paths.append(request.url.path)
        return httpx.Response(200,json={"ok":True,"team_id":"OTHER","user_id":"UBOT"})
    sender=SlackChatSender(token="xoxb-synthetic-token",app_id="A111",team_id="T111",
        bot_user_id="UBOT",transport=httpx.MockTransport(wrong_actor))
    with pytest.raises(PermissionError):
        await sender.post_once(permit)
    assert paths==["/api/auth.test"] # No owner impersonation or Slack post.

    paths.clear()
    def missing_echo(request):
        paths.append(request.url.path)
        if request.url.path.endswith("auth.test"):
            return httpx.Response(200,json={"ok":True,"team_id":"T111","user_id":"UBOT"})
        return httpx.Response(200,json={"ok":True,"channel":"C111",
            "ts":"1792000002.200","message":{"user":"UBOT",
                  "thread_ts":permit["thread_ts"],"text":permit["text"]}})
    sender=SlackChatSender(token="xoxb-synthetic-token",app_id="A111",team_id="T111",
        bot_user_id="UBOT",transport=httpx.MockTransport(missing_echo))
    with pytest.raises(RuntimeError,match="echo"):
        await sender.post_once(permit)
    assert paths.count("/api/chat.postMessage")==1 # UNKNOWN, no blind second post.


@pytest.mark.asyncio
async def test_only_remotely_accepted_signed_human_event_wakes_response(env):
    control, _credential, _link = _prepare(env)
    ingress=ExpertSlackIngress(control,signing_secret="test-signing-secret",
                               clock=lambda:1792000001)
    ids=[]
    responder=SimpleNamespace(control=control,ingress=ingress,notify=ids.append)
    ingress.bind_search_reply(responder)
    with pytest.raises(ValueError):
        ingress.bind_search_reply(responder)
    app=Starlette(routes=[Route(PATH,ingress.__call__,methods=["POST"])])
    raw,headers=_event()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                 base_url="http://test") as http:
        first=await http.post(PATH,content=raw,headers=headers)
        assert first.status_code==503 and ids==[]
        row=env.p.corpus
        with row.connect() as db:
            request_id=db.execute("SELECT id FROM rkb_expert_requests").fetchone()[0]
        cmd=control.exact_remote_intent(request_id,1)
        seal=await env.journal.seal([cmd],predecessor=None,writer_generation="writer-1")
        await control.verify_remote_intent(request_id,cmd,seal)
        response=await http.post(PATH,content=raw,headers=headers)
        assert response.status_code==200 and ids==[request_id]
        # Replayed exact event is the same immutable request, not a new one.
        again=await http.post(PATH,content=raw,headers=headers)
        assert again.status_code==200 and ids==[request_id,request_id]
        with row.connect() as db:
            assert db.execute("SELECT count(*) FROM rkb_expert_requests").fetchone()[0]==1
        altered=dict(headers,**{"X-Slack-Signature":"v0="+"0"*64})
        assert (await http.post(PATH,content=raw,headers=altered)).status_code==403
        assert len(ids)==2


def test_same_owner_composition_is_inert_until_explicit_reply_bind(service):
    s=service
    ctl, credentials, _link, ingress=_owned_ingress(s)
    evidence_reader=compose_expert_evidence_receiver(
        target_startup=s.startup, expert_slack_ingress=ingress)
    sender=SlackChatSender(token="xoxb-synthetic-token",app_id=ctl.app_id,
        team_id=ctl.team_id,bot_user_id=ctl.bot_user_id)
    reply=compose_expert_slack_search_reply(ingress=ingress,
        evidence=evidence_reader,credentials=credentials,sender=sender)
    assert reply.delivery.control is ctl
    assert reply.evidence.backend is s.startup.backend
    assert isinstance(reply.source_gate,_BoundSearchGate)
    assert reply.source_gate({"id":"fixture","person_id":"fixture"}, {}) is False
    assert ingress._reply is None
    ingress.bind_search_reply(reply)
    assert ingress._reply is reply
    # No new expert_executor_* route or service is automatically installed.
    assert reply.control.spool is s.config.spool


@pytest.mark.asyncio
async def test_reply_pipeline_never_posts_without_remote_result_and_send_receipts(service,monkeypatch):
    """Orchestration-only fixture. Dataset proof is tested by existing delivery suites."""
    from contextlib import asynccontextmanager
    s=service
    ctl, credentials, _link, ingress=_owned_ingress(s)
    receiver=compose_expert_evidence_receiver(
        target_startup=s.startup, expert_slack_ingress=ingress)
    sender=SlackChatSender(token="xoxb-synthetic-token",app_id=ctl.app_id,
        team_id=ctl.team_id,bot_user_id=ctl.bot_user_id)
    reply=compose_expert_slack_search_reply(ingress=ingress,
        evidence=receiver,credentials=credentials,sender=sender)
    claim=dict(request_id="synthetic",generation=1,execution_token="synthetic-token")
    calls=[]

    @asynccontextmanager
    async def synthetic_read(_claim):
        assert _claim is claim
        yield evidence([])
    monkeypatch.setattr(reply,"_read_claim",synthetic_read)
    monkeypatch.setattr(ctl,"_row",lambda _:{"state":"leased"})
    def prepared(*args):
        calls.append("prepare")
    async def staged(*args):
        calls.append("stage_result")
    def pending(*args):
        calls.append("confirm_result")
        return {"remote_accepted":False}
    async def forbidden(*args):
        pytest.fail("uncovered result must not enter send or provider")
    monkeypatch.setattr(reply.delivery,"prepare_result",prepared)
    monkeypatch.setattr(reply.delivery,"stage_dataset_result",staged)
    monkeypatch.setattr(reply.delivery,"confirm_dataset_result",pending)
    monkeypatch.setattr(reply.delivery,"stage_dataset_send",forbidden)
    monkeypatch.setattr(sender,"post_once",forbidden)
    await reply._one(claim)
    assert calls==["prepare","stage_result","confirm_result"]


@pytest.mark.asyncio
@pytest.mark.parametrize("send_fails",[False,True])
async def test_reply_pipeline_send_is_original_thread_and_unknown_never_replayed(
        service,monkeypatch,send_fails):
    from contextlib import asynccontextmanager
    from regional_knowledge.expert_delivery_control import verified_native_slack_receipt
    s=service
    ctl,credentials,_link,ingress=_owned_ingress(s)
    reader=compose_expert_evidence_receiver(target_startup=s.startup,
        expert_slack_ingress=ingress)
    sender=SlackChatSender(token="xoxb-synthetic-token",app_id=ctl.app_id,
        team_id=ctl.team_id,bot_user_id=ctl.bot_user_id)
    reply=compose_expert_slack_search_reply(ingress=ingress,
        evidence=reader,credentials=credentials,sender=sender)
    claim=dict(request_id="fixture-event-request",generation=1,
               execution_token="fixture-lease")
    permit={"request_id":claim["request_id"],"delivery_id":"fixture-one-shot",
            "channel_id":"C111","thread_ts":"1792000000.100",
            "text":answer_from_evidence(evidence([]))}
    stages=[]
    @asynccontextmanager
    async def fixture_read(_claim):
        yield evidence([])
    monkeypatch.setattr(reply,"_read_claim",fixture_read)
    monkeypatch.setattr(ctl,"_row",lambda _:{"state":"leased"})
    monkeypatch.setattr(reply.delivery,"prepare_result",
                        lambda *a:stages.append("prepared"))
    async def stage_result(*a):stages.append("result_staged")
    async def stage_send(*a):stages.append("send_staged")
    async def stage_receipt(*a):stages.append("receipt_staged")
    monkeypatch.setattr(reply.delivery,"stage_dataset_result",stage_result)
    monkeypatch.setattr(reply.delivery,"confirm_dataset_result",
                        lambda *a:stages.append("result_covered") or {"remote_accepted":True})
    monkeypatch.setattr(reply.delivery,"stage_dataset_send",stage_send)
    monkeypatch.setattr(reply.delivery,"confirm_dataset_send",
                        lambda *a:stages.append("send_covered") or {"remote_accepted":True})
    monkeypatch.setattr(reply.delivery,"claim_send_once",
                        lambda *a:stages.append("one_permit") or permit)
    async def post_once(p):
        stages.append("post")
        assert p is permit
        if send_fails:raise RuntimeError("ambiguous provider outcome")
        return verified_native_slack_receipt({
           "app_id":"A111","user":"UBOT","channel":"C111",
           "thread_ts":p["thread_ts"],"ts":"1792000005.200",
           "client_msg_id":p["delivery_id"],"text":p["text"]},
           app_id="A111",team_id="T111",bot_user_id="UBOT",
           channel_id="C111",thread_ts=p["thread_ts"])
    monkeypatch.setattr(sender,"post_once",post_once)
    monkeypatch.setattr(reply.delivery,"stage_dataset_native_receipt",stage_receipt)
    monkeypatch.setattr(reply.delivery,"confirm_dataset_native_receipt",
                        lambda *a:stages.append("receipt_covered"))
    monkeypatch.setattr(reply.delivery,"uncertain_delivery",
                        lambda *a:stages.append("unknown"))
    if send_fails:
        with pytest.raises(RuntimeError,match="ambiguous"):
            await reply._one(claim)
        assert stages[-2:]==["post","unknown"]
        assert "receipt_staged" not in stages
    else:
        await reply._one(claim)
        assert stages[-3:]==["post","receipt_staged","receipt_covered"]
    assert stages[:6]==["prepared","result_staged","result_covered",
                       "send_staged","send_covered","one_permit"]
    assert stages.count("post")==1
