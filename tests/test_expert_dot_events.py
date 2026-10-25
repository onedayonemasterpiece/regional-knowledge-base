"""Synthetic contract checks for the existing RKB Events / expert answer bridge.

No ChatGPT callback, Slack send, Kaggle source, credentials or production state.
"""
import asyncio
import base64
import hashlib
import hmac
import json
import sqlite3
from types import SimpleNamespace

import httpx
import pytest

from regional_knowledge.expert_dot_events import (
    EVENT, EventParams, ExpertDotEvents, _event_definition, _secret, _signed_headers,
)


def _whsec():
    return "whsec_"+base64.b64encode(b"rkb-dot-test-signing-secret-1234").decode()


def test_draft_event_has_exact_correlation_and_standard_webhook_signature():
    definition=_event_definition()
    assert definition["name"]==EVENT
    assert definition["delivery"]==["webhook"]
    assert definition["payloadSchema"]["required"]==["request_id","event_id"]
    assert definition["inputSchema"]["properties"]=={}
    body=b'{"eventId":"Ev-test","data":{"request_id":"one"}}'
    headers=_signed_headers(_whsec(),"sub_fixture","Ev-test",body)
    data=b"Ev-test."+headers["webhook-timestamp"].encode()+b"."+body
    expected="v1,"+base64.b64encode(
        hmac.new(_secret(_whsec()),data,hashlib.sha256).digest()).decode()
    assert headers["webhook-signature"]==expected
    assert headers["X-MCP-Subscription-Id"]=="sub_fixture"
    with pytest.raises(ValueError):_secret("not-a-webhook-secret")


class _Store:
    def __init__(self):
        self.data={"dot_event_subscriptions":{}}
    def read(self,fn):
        return fn(self.data)
    def mutate(self,fn):
        return fn(self.data)


def _instance(transport=None):
    """Bypass constructor only for pure callback/unit boundaries."""
    owner=object.__new__(ExpertDotEvents)
    owner.oauth=SimpleNamespace(store=_Store())
    owner.callback_host="callbacks.chatgpt.example"
    owner.transport=transport
    owner._scope="restricted-dot|bound-client"
    owner._reads={}
    owner._wakes={}
    return owner


@pytest.mark.asyncio
async def test_subscribe_keeps_chatgpt_secret_encrypted_store_and_fixed_destination(monkeypatch):
    observed=[]
    def callback(req):
        observed.append((str(req.url),req.headers,req.content))
        assert req.headers["X-MCP-Subscription-Id"].startswith("sub_")
        body=json.loads(req.content)
        assert body["type"]=="verification"
        return httpx.Response(200,json={"challenge":body["challenge"]})
    events=_instance(httpx.MockTransport(callback))
    monkeypatch.setattr(events,"_actor",lambda:events._scope)
    async def no_scan():
        pass
    monkeypatch.setattr(events,"resume_pending",no_scan)
    params=EventParams(name=EVENT,arguments={},delivery={
        "mode":"webhook","url":"https://callbacks.chatgpt.example/events",
        "secret":_whsec()})
    response=await events.subscribe(params)
    assert response["id"].startswith("sub_")
    row=events.oauth.store.data["dot_event_subscriptions"][events._scope]
    assert row["secret"]==_whsec()
    assert len(observed)==1
    assert observed[0][0]=="https://callbacks.chatgpt.example/events"
    with pytest.raises(ValueError):
        events._url("https://127.0.0.1/internal/expert")
    with pytest.raises(ValueError):
        events._url("https://callbacks.chatgpt.example:8443/internal")
    await events.subscribe(params)
    assert len(observed)==2
    events.unsubscribe(params)
    assert events._subscription() is None


@pytest.mark.asyncio
async def test_failed_callback_does_not_create_subscription(monkeypatch):
    events=_instance(httpx.MockTransport(lambda req:httpx.Response(401)))
    monkeypatch.setattr(events,"_actor",lambda:events._scope)
    params=EventParams(name=EVENT,arguments={},delivery={
        "mode":"webhook","url":"https://callbacks.chatgpt.example/events",
        "secret":_whsec()})
    with pytest.raises(ValueError,match="verification"):
        await events.subscribe(params)
    assert events.oauth.store.data["dot_event_subscriptions"]=={}


class _Corpus:
    def __init__(self,path):self.path=path
    def connect(self):
        conn=sqlite3.connect(self.path)
        conn.row_factory=sqlite3.Row
        return conn


@pytest.mark.asyncio
async def test_event_intent_precedes_callback_and_same_id_replay(tmp_path,monkeypatch):
    db=_Corpus(tmp_path/"control.sqlite")
    with db.connect() as conn:
        conn.execute("""CREATE TABLE rkb_expert_requests(
          id TEXT PRIMARY KEY,event_id TEXT,state TEXT,trigger_event_id TEXT,
          reconcile_cursor TEXT,created_at REAL)""")
        conn.execute("""INSERT INTO rkb_expert_requests
            VALUES('request-one','Ev-once','pending',NULL,NULL,1)""")
    class Control:
        corpus=db
        def confirm_dataset_coverage(self,request_id):return True
        async def stage_received_dataset(self,request_id):return True
        def _row(self,request_id):
            with db.connect() as conn:
                return dict(conn.execute("SELECT * FROM rkb_expert_requests WHERE id=?",
                                         (request_id,)).fetchone())
    calls=[]
    def response(request):
        with db.connect() as conn:
            row=conn.execute("SELECT trigger_event_id FROM rkb_expert_requests").fetchone()
            assert row["trigger_event_id"]=="Ev-once" # dot may race callback HTTP 200
        body=json.loads(request.content)
        calls.append(body)
        return httpx.Response(200,json={"ok":True})
    events=_instance(httpx.MockTransport(response))
    events.control=Control()
    monkeypatch.setattr(events,"_subscription",lambda:{
        "id":"sub_test","url":"https://callbacks.chatgpt.example/events","secret":_whsec()})
    await events._wake("request-one")
    await events._wake("request-one")
    # Webhook 2xx is not a completed answer. With no claim, restart/renewal
    # may wake the SAME original event ID; it must not create a new request.
    assert len(calls)==2
    assert all(item["eventId"]=="Ev-once" for item in calls)
    assert all(item["data"]=={"request_id":"request-one","event_id":"Ev-once"}
               for item in calls)
    with db.connect() as conn:
        row=conn.execute("SELECT id,trigger_event_id,state FROM rkb_expert_requests").fetchone()
        assert (row["id"],row["trigger_event_id"],row["state"])==(
            "request-one","Ev-once","pending")


@pytest.mark.asyncio
async def test_wake_and_search_start_in_parallel_without_extra_queue(monkeypatch):
    events=_instance()
    started=[]
    done=asyncio.Event()
    async def search(request_id):
        started.append("search")
        await done.wait()
        return {"status":"ok","evidence":[]}
    async def wake(request_id):
        started.append("wake")
        await done.wait()
    monkeypatch.setattr(events,"_subscription",lambda:{"state":"active"})
    monkeypatch.setattr(events,"_search",search)
    monkeypatch.setattr(events,"_wake",wake)
    for i in range(17):
        events.notify(f"request-{i}")
    events.notify("request-16") # duplicate notification is not a second wake
    await asyncio.sleep(0)
    assert len(started)==34
    assert started.count("search")==17 and started.count("wake")==17
    assert len(events._reads)==17 and len(events._wakes)==17
    done.set()
    await asyncio.gather(*events._reads.values(),*events._wakes.values())


@pytest.mark.asyncio
async def test_foreign_event_and_already_sent_result_never_claim_or_send(monkeypatch):
    events=_instance()
    monkeypatch.setattr(events,"_actor",lambda:events._scope)
    class Control:
        def _row(self,request_id):
            return {"event_id":"Ev-correct","trigger_event_id":"Ev-correct",
                    "state":"delivered"}
    events.control=Control()
    with pytest.raises(PermissionError):
        await events.get_request("request-one","Ev-other")
    with pytest.raises(PermissionError):
        await events.record_result("request-one","Ev-other",1,"x"*40,"answer")
    done=await events.record_result("request-one","Ev-correct",1,"x"*40,"answer")
    assert done=={"state":"delivered","request_id":"request-one",
                 "retry_send_allowed":False}


@pytest.mark.asyncio
async def test_record_result_uses_original_provider_sender_and_single_claim(monkeypatch):
    events=_instance()
    monkeypatch.setattr(events,"_actor",lambda:events._scope)
    request="request-one"
    marker="6a7ac5a1-0e00-4b72-a41d-849908f38435"
    origin={"event_id":"Ev-one","trigger_event_id":"Ev-one","state":"leased",
            "origin":json.dumps({"channel_id":"C0RKBTEST"})}
    posts=[]
    source_checks=[]
    class Control:
        app_id="A0RKBTEST"
        team_id="T0RKBTEST"
        bot_user_id="U0RKBTEST"
        def _row(self,rid):
            assert rid==request
            return dict(origin)
    class Delivery:
        def _source_current(self,row,destination):
            assert row["state"]=="sending"
            assert destination["channel_id"]=="C0RKBTEST"
            source_checks.append(True)
        async def stage_dataset_native_receipt(self,credentials,rid,proof):
            assert proof.channel_id=="C0RKBTEST"
            assert proof.thread_ts=="1792920000.123456"
            assert proof.client_message_id==marker
            posts.append("receipt_staged")
        def claim_send_once(self,credentials,rid,generation,token):
            assert (rid,generation,token)==(request,1,"x"*40)
            posts.append("claimed")
            origin["state"]="sending"
            return {"delivery_id":marker,"channel_id":"C0RKBTEST",
                    "thread_ts":"1792920000.123456",
                    "text":"Содержательный ответ.\nИсточники: подтверждённый текст."}
        def confirm_dataset_native_receipt(self,credentials,rid,proof):
            assert posts==["result_saved","claimed","posted","receipt_staged"]
            origin["state"]="delivered"
            return {"state":"delivered"}
    class Receiver:
        async def _advance(self,value):
            assert value["request_id"]==request
            posts.append("result_saved")
            return SimpleNamespace(body=b'{"state":"send_intent_ready"}')
    class Sender:
        async def post_once(self,**kw):
            # SlackChatSender checks current immediately before/after auth.test.
            assert kw["current"]()
            await asyncio.sleep(0)
            assert kw["current"]()
            assert kw["event_id"]==marker
            assert kw["channel"]=="C0RKBTEST"
            assert kw["thread"]=="1792920000.123456"
            posts.append("posted")
            return {"ts":"1792923000.789012"}
    events.control=Control()
    events.context=SimpleNamespace(credentials="owned-executor",delivery=Delivery())
    events.result_receiver=Receiver()
    events.reply=SimpleNamespace(sender=Sender(),
        _row=lambda rid,states: origin if origin["state"] in states else None)
    result=await events.record_result(request,"Ev-one",1,"x"*40,
        "Содержательный ответ.\nИсточники: подтверждённый текст.")
    assert result=={"state":"delivered","request_id":request,"retry_send_allowed":False}
    assert posts==["result_saved","claimed","posted","receipt_staged"]
    assert source_checks==[True,True]
    repeated=await events.record_result(request,"Ev-one",1,"x"*40,"same text")
    assert repeated["state"]=="delivered"
    assert posts==["result_saved","claimed","posted","receipt_staged"]


@pytest.mark.asyncio
async def test_missing_or_expired_subscription_uses_existing_source_reply(monkeypatch):
    from regional_knowledge.expert_slack_search_reply import ExpertSlackSearchReply
    reply=object.__new__(ExpertSlackSearchReply)
    fallback=[]
    async def original(request_id):
        fallback.append(request_id)
    reply._tasks={}
    reply._answer=original
    dot=SimpleNamespace(_subscription=lambda:None,
                        notify=lambda rid:pytest.fail("no dot without subscription"))
    reply._dot_events=dot
    reply.notify("first")
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert fallback==["first"]
    assert reply._tasks=={}


@pytest.mark.asyncio
async def test_webhook_ack_without_claim_resumes_original_pending_request(tmp_path,monkeypatch):
    db=_Corpus(tmp_path/"control.sqlite")
    with db.connect() as conn:
        conn.execute("""CREATE TABLE rkb_expert_requests(
            id TEXT,event_id TEXT,state TEXT,trigger_event_id TEXT,
            reconcile_cursor TEXT,created_at REAL)""")
        conn.execute("""INSERT INTO rkb_expert_requests VALUES(
            'still-pending','Ev-original','pending','Ev-original','event_accepted',1)""")
        conn.execute("""INSERT INTO rkb_expert_requests VALUES(
            'already-done','Ev-done','delivered','Ev-done','event_accepted',2)""")
    events=_instance()
    events.control=SimpleNamespace(corpus=db)
    wakes=[]
    monkeypatch.setattr(events,"_subscription",lambda:{"state":"active"})
    monkeypatch.setattr(events,"notify",lambda request_id:wakes.append(request_id))
    await events.resume_pending()
    assert wakes==["still-pending"]


def test_dot_answer_contract_requires_bibliography_and_honest_no_support():
    from regional_knowledge.expert_dot_events import ExpertDotEvents
    import inspect
    # Contract is returned with each scoped request, not merely project docs.
    body=inspect.getsource(ExpertDotEvents.get_request)
    for piece in ("1–4", "Источники:", "metadata.citation", "locator",
                  "метро", "cosmodrome", "В доступной базе подтверждений не найдено"):
        assert piece in body or (piece=="метро" and "metro" in body)



def test_callback_dns_requires_public_address_and_rejects_mixed_resolution(monkeypatch):
    import socket
    from regional_knowledge.expert_dot_events import _public_dns
    monkeypatch.setattr(socket,"getaddrinfo",
        lambda host,port,**kw:[(2,1,6,"",("203.0.113.5",port)),
                               (2,1,6,"",("127.0.0.1",port))])
    with pytest.raises(ValueError,match="public"):
        _public_dns("callbacks.chatgpt.example")
    monkeypatch.setattr(socket,"getaddrinfo",
        lambda host,port,**kw:[(2,1,6,"",("8.8.8.8",port))])
    _public_dns("callbacks.chatgpt.example")



@pytest.mark.asyncio
async def test_both_existing_http_and_socket_lifespan_resume_pending_request_once():
    from contextlib import asynccontextmanager
    from regional_knowledge.server import _resume_original_dot_requests
    for transport in ("http","socket"):
        observed=[]
        @asynccontextmanager
        async def target(server):
            observed.append("target_started")
            yield server
            observed.append("target_stopped")
        @asynccontextmanager
        async def socket(server):
            async with target(server) as runtime:
                observed.append("socket_connected")
                yield runtime
                observed.append("socket_closed")
        class Events:
            async def resume_pending(self):
                observed.append("original_request_rows_rescanned")
        existing=target if transport=="http" else socket
        async with _resume_original_dot_requests(existing,Events())("rkb") as runtime:
            assert runtime=="rkb"
            assert observed[-1]=="original_request_rows_rescanned"
        assert observed.count("original_request_rows_rescanned")==1
        assert observed[-1]=="target_stopped"



@pytest.mark.asyncio
async def test_actual_mcp_registers_discover_events_and_scoped_tools(monkeypatch):
    from mcp import types
    from mcp.server.mcpserver import MCPServer
    from regional_knowledge.expert_executor_workflow import ExpertExecutorResultReceiver
    dot=object.__new__(ExpertDotEvents)
    dot.result_receiver=object.__new__(ExpertExecutorResultReceiver)
    dot._actor=lambda:"synthetic restricted executor"
    mcp=MCPServer("Synthetic RKB Events contract")
    dot.register(mcp)
    registered=mcp._lowlevel_server._request_handlers
    assert {"server/discover","events/list","events/subscribe",
            "events/unsubscribe"}<=set(registered)
    context=SimpleNamespace(protocol_version="2026-07-28")
    discover=await registered["server/discover"].handler(context,types.RequestParams())
    assert discover["capabilities"]["events"]=={}
    listing=await registered["events/list"].handler(context,types.PaginatedRequestParams())
    assert listing["events"][0]["name"]==EVENT
    assert set(listing["events"][0]["inputSchema"]["properties"])==set()
    tools=await mcp.list_tools()
    exposed={tool.name for tool in tools}
    assert {"expert_request_get","expert_result_record"}<=exposed
