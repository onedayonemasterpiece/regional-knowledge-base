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
    headers=_signed_headers(_whsec(),"Ev-test",body)
    data=b"Ev-test."+headers["webhook-timestamp"].encode()+b"."+body
    expected="v1,"+base64.b64encode(
        hmac.new(_secret(_whsec()),data,hashlib.sha256).digest()).decode()
    assert headers["webhook-signature"]==expected
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
        "url":"https://callbacks.chatgpt.example/events","secret":_whsec()})
    await events._wake("request-one")
    await events._wake("request-one")
    assert len(calls)==1
    assert calls[0]["eventId"]=="Ev-once"
    assert calls[0]["data"]=={"request_id":"request-one","event_id":"Ev-once"}
    with db.connect() as conn:
        row=conn.execute("SELECT reconcile_cursor FROM rkb_expert_requests").fetchone()
        assert row["reconcile_cursor"]=="event_accepted"


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
    events.notify("same-request")
    events.notify("same-request")
    await asyncio.sleep(0)
    assert sorted(started)==["search","wake"]
    assert len(events._reads)==1 and len(events._wakes)==1
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
    origin={"event_id":"Ev-one","trigger_event_id":"Ev-one","state":"leased"}
    posts=[]
    class Control:
        app_id="A0RKBTEST"
        team_id="T0RKBTEST"
        bot_user_id="U0RKBTEST"
        def _row(self,rid):
            assert rid==request
            return dict(origin)
    class Delivery:
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
    repeated=await events.record_result(request,"Ev-one",1,"x"*40,"same text")
    assert repeated["state"]=="delivered"
    assert posts==["result_saved","claimed","posted","receipt_staged"]
