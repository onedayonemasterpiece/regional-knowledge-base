"""Opt-in MCP Events -> existing fenced expert claim/result -> original Slack bot.

The RKB control request row is the durable wake source. There is no second job
queue, answer model, source corpus, or service. A webhook ACK is not an answer.
"""
from __future__ import annotations

import asyncio
import base64
from datetime import datetime, timezone
import hashlib
import ipaddress
import hmac
import json
import logging
import re
import secrets
import socket
import time
from urllib.parse import urlsplit

import httpx
from mcp import types
from mcp.server.auth.middleware.auth_context import get_access_token
from mcp.server.mcpserver.exceptions import ToolError
from pydantic import ConfigDict, Field

from .expert_delivery_control import verified_native_slack_receipt
from .expert_request_control import RequestControlError
from .expert_executor_workflow import ExpertExecutorContext
from .expert_slack_search_reply import ExpertSlackSearchReply


_LOG = logging.getLogger(__name__)
EVENT = "rkb.expert.question.created"
_RECORD_STATES = frozenset(("sending", "unknown", "delivered"))
_EVENT_STATES = ("received", "pending", "leased", "result_unsealed",
                 "result_ready", "send_intent_ready")


class EventParams(types.RequestParams):
    model_config = ConfigDict(extra="forbid", strict=True, populate_by_name=True)
    name: str
    arguments: dict
    delivery: dict
    cursor: str | None = None
    ttl_ms: int | None = Field(default=86400000, alias="ttlMs", gt=0)
    max_age_ms: int | None = Field(default=None, alias="maxAgeMs", ge=0)


def _secret(value):
    """ChatGPT supplies this signing key at subscription; RKB never mints it."""
    if type(value) is not str or not value.startswith("whsec_"):
        raise ValueError("webhook signing secret required")
    try:
        raw=base64.b64decode(value[6:],validate=True)
    except (ValueError,TypeError):
        raise ValueError("webhook signing secret invalid") from None
    if not 24<=len(raw)<=64:
        raise ValueError("webhook signing secret invalid")
    return raw


def _signed_headers(secret, subscription_id, event_id, body):
    timestamp=str(int(time.time()))
    signed=event_id.encode()+b"."+timestamp.encode()+b"."+body
    mac=base64.b64encode(hmac.new(_secret(secret),signed,hashlib.sha256).digest()).decode()
    return {"content-type":"application/json","webhook-id":event_id,
            "webhook-timestamp":timestamp,"webhook-signature":"v1,"+mac,
            "X-MCP-Subscription-Id":subscription_id}


def _public_dns(host):
    """Reject private/mixed DNS before each HTTPS callback effect."""
    try:
        found={info[4][0] for info in socket.getaddrinfo(host,443,type=socket.SOCK_STREAM)}
        if not found or any(not ipaddress.ip_address(addr).is_global for addr in found):
            raise ValueError("callback DNS is not public")
    except (OSError,ValueError):
        raise ValueError("callback DNS must resolve only to public addresses") from None


def _event_definition():
    string={"type":"string","minLength":1}
    return {"name":EVENT,
        "description":"A verified RKB Slack question is ready for a scoped executor. "
            "Read by exact request/event ID; return a source-grounded answer with the same ID.",
        "delivery":["webhook"],
        "inputSchema":{"type":"object","properties":{},"additionalProperties":False},
        "payloadSchema":{"type":"object","properties":{"request_id":string,"event_id":string},
            "required":["request_id","event_id"],"additionalProperties":False}}


class ExpertDotEvents:
    """One owner-provisioned service, one signed webhook subscription, existing row."""

    def __init__(self, *, context: ExpertExecutorContext, reply: ExpertSlackSearchReply,
                 result_receiver, oauth_provider, callback_host: str, transport=None):
        if (type(context) is not ExpertExecutorContext
                or type(reply) is not ExpertSlackSearchReply
                or context.delivery.control is not reply.control
                or not context.mcp_subject or not context.mcp_client_id
                or context.mcp_subject == reply.control.people.owner
                or type(callback_host) is not str
                or re.fullmatch(r"[a-zA-Z0-9.-]{4,200}",callback_host) is None):
            raise ValueError("distinct, restricted existing RKB executor connection required")
        self.context,self.reply,self.control=context,reply,reply.control
        self.result_receiver=result_receiver
        self.oauth=oauth_provider
        self.callback_host=callback_host.lower()
        self.transport=transport
        self._wakes={}
        self._reads={}
        self._scope=context.mcp_subject+"|"+context.mcp_client_id

    def _actor(self):
        access=get_access_token()
        if (access is None or access.subject!=self.context.mcp_subject
                or access.client_id!=self.context.mcp_client_id
                or access.subject==self.control.people.owner):
            raise PermissionError("RKB executor connection required")
        with self.control.corpus.connect() as db:
            self.control._service(db,self.context.credentials)
        return self._scope

    def _url(self, url):
        if type(url) is not str or len(url)>2048:
            raise ValueError("bounded webhook URL required")
        try:
            parts=urlsplit(url)
            if (parts.scheme!="https" or parts.hostname!=self.callback_host
                    or parts.port not in (None,443) or parts.username is not None
                    or parts.password is not None or parts.fragment
                    or any(ord(c)<33 or ord(c)>126 for c in url)):
                raise ValueError("callback must be the operator-approved HTTPS host")
        except ValueError:
            raise ValueError("callback must be the operator-approved HTTPS host") from None
        return url

    def _subscription(self):
        record=self.oauth.store.read(
            lambda s:dict(s.get("dot_event_subscriptions",{}).get(self._scope) or {}))
        if (record.get("state")!="active" or record.get("expires",0)<=time.time()):
            return None
        return record

    async def _post(self,record,event_id,payload):
        body=json.dumps(payload,ensure_ascii=False,separators=(",",":")).encode()
        headers=_signed_headers(record["secret"],record["id"],event_id,body)
        if self.transport is None:
            # The host is already fixed by owner configuration and TLS still
            # verifies it; reject private/mixed callback resolution.
            await asyncio.to_thread(_public_dns,urlsplit(record["url"]).hostname)
        async with httpx.AsyncClient(transport=self.transport,timeout=8,
                                     follow_redirects=False,trust_env=False) as client:
            response=await client.post(record["url"],content=body,headers=headers)
        return response

    async def subscribe(self,params):
        self._actor()
        if (params.name!=EVENT or params.arguments!={}
                or params.cursor is not None or not isinstance(params.delivery,dict)
                or set(params.delivery)-{"mode","url","secret"}
                or not {"url","secret"}<=set(params.delivery)
                or params.delivery.get("mode","webhook")!="webhook"):
            raise ValueError("exact RKB event subscription required")
        url=self._url(params.delivery["url"])
        secret=params.delivery["secret"]
        _secret(secret)
        lifetime=min(params.ttl_ms or 86400000,86400000)/1000
        identity=hashlib.sha256((self._scope+"|"+EVENT+"|"+url).encode()).hexdigest()
        sub_id="sub_"+identity
        # The callback must prove possession before its secret is persisted.
        nonce=secrets.token_urlsafe(24)
        response=await self._post({"id":sub_id,"url":url,"secret":secret},
            "verify_"+identity[:24],{"type":"verification","challenge":nonce})
        try:
            if response.status_code!=200 or response.json().get("challenge")!=nonce:
                raise ValueError("callback verification unavailable")
        except (ValueError,TypeError,AttributeError):
            raise ValueError("callback verification unavailable") from None
        record={"id":sub_id,"state":"active","url":url,"secret":secret,
                "expires":time.time()+lifetime}
        def save(state):
            bucket=state["dot_event_subscriptions"]
            old=bucket.get(self._scope)
            if old is not None and old["id"]!=sub_id and old["state"]=="active" \
                    and old["expires"]>time.time():
                raise ValueError("existing RKB subscription must be unsubscribed first")
            bucket[self._scope]=record
        self.oauth.store.mutate(save)
        # The request journal, not an Events outbox, is the recovery source.
        asyncio.create_task(self.resume_pending())
        return {"id":sub_id,"refreshBefore":datetime.fromtimestamp(
            record["expires"],timezone.utc).isoformat().replace("+00:00","Z"),
            "cursor":None,"truncated":False}

    def unsubscribe(self,params):
        self._actor()
        if (params.name!=EVENT or params.arguments!={}
                or type(params.delivery) is not dict
                or params.delivery.get("mode")!="webhook"
                or type(params.delivery.get("url")) is not str):
            raise ValueError("exact RKB event subscription required")
        self._url(params.delivery["url"])
        def stop(state):
            row=state["dot_event_subscriptions"].get(self._scope)
            if row and row["url"]==params.delivery["url"]:
                row["state"]="stopped"
        self.oauth.store.mutate(stop)
        return {}

    def notify(self, request_id):
        """Prefetch and wake independently; hold only the original read ticket."""
        if self._subscription() is None:
            return
        if request_id not in self._reads:
            ready=asyncio.Event()
            task=asyncio.create_task(self._search(request_id,ready))
            entry=(task,ready)
            self._reads[request_id]=entry
            def read_finished(done):
                if self._reads.get(request_id) is entry:
                    self._reads.pop(request_id,None)
                try:done.result()
                except asyncio.CancelledError:pass
                except Exception as exc:
                    _LOG.warning("rkb_dot_prefetch status=deferred error_type=%s",
                                 type(exc).__name__)
            task.add_done_callback(read_finished)
        if request_id not in self._wakes:
            task=asyncio.create_task(self._wake(request_id))
            self._wakes[request_id]=task
            def wake_finished(done):
                if self._wakes.get(request_id) is done:
                    self._wakes.pop(request_id,None)
                try:done.result()
                except asyncio.CancelledError:pass
                except Exception as exc:
                    _LOG.warning("rkb_dot_wake status=deferred error_type=%s",
                                 type(exc).__name__)
            task.add_done_callback(wake_finished)

    async def resume_pending(self):
        if self._subscription() is None:
            return
        with self.control.corpus.connect() as db:
            rows=db.execute("""SELECT id FROM rkb_expert_requests
                WHERE state IN ('received','pending')
                ORDER BY created_at,id""").fetchall()
        for row in rows:
            self.notify(row["id"])

    async def _wake(self, request_id):
        # Continue this SAME wake while the original request is pending.
        # Coverage may take minutes: eight fixed attempts would lose an ACKed
        # question until restart despite a later accepted Dataset record.
        delay=2
        while self._subscription() is not None:
            try:
                if await asyncio.to_thread(self.control.confirm_dataset_coverage,request_id):
                    break
                with self.control.corpus.connect() as db:
                    state=db.execute("SELECT state FROM rkb_expert_requests WHERE id=?",
                                     (request_id,)).fetchone()
                if state is None or state["state"] not in ("received","pending"):
                    return
                await self.control.stage_received_dataset(request_id)
            except (RequestControlError, OSError) as exc:
                _LOG.warning("rkb_dot_coverage status=deferred error_type=%s",
                             type(exc).__name__)
            await asyncio.sleep(delay)
            delay=min(60,delay*2)
        else:
            return
        record=self._subscription()
        if record is None:return
        # Persist the SAME original event ID before any external callback:
        # a fast subscriber may read the correlated request before HTTP 200.
        # On unknown callback outcome replay only this ID, never mint a job.
        with self.control.corpus.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row=db.execute("SELECT * FROM rkb_expert_requests WHERE id=?",
                           (request_id,)).fetchone()
            if row is None or row["state"] not in _EVENT_STATES:
                return
            if row["trigger_event_id"] not in (None,row["event_id"]):
                raise PermissionError("event correlation changed")
            db.execute("""UPDATE rkb_expert_requests SET trigger_event_id=?
                WHERE id=? AND trigger_event_id IS NULL""",
                (row["event_id"],request_id))
        payload={"eventId":row["event_id"],"name":EVENT,
            "timestamp":datetime.fromtimestamp(row["created_at"],timezone.utc
                        ).isoformat().replace("+00:00","Z"),
            "cursor":None,"data":{"request_id":row["id"],"event_id":row["event_id"]}}
        # One continuing task for the SAME original event, not a new queue.
        # A fifth transient failure cannot strand an ACKed question forever.
        delay=2
        while self._subscription() is not None:
            try:response=await self._post(record,row["event_id"],payload)
            except (httpx.HTTPError,ValueError):
                response=None
            if response is not None and 200<=response.status_code<300:
                # 2xx acknowledges the callback, not the answer. If dot dies
                # before claim, existing startup/subscription scan replays it.
                return
            if response is not None and response.status_code<500 \
                    and response.status_code not in (408,425,429):
                return
            await asyncio.sleep(delay)
            delay=min(60,delay*2)

    async def _search(self, request_id, ready=None):
        # Existing source issuance remains current until dot reads it, with a
        # short bound when dot never claims; no second search/cache authority.
        try:
            return await self.reply._answer(
                request_id,model_only=True,ready_for_dot=ready)
        except Exception:
            return {"status":"unavailable"}

    async def get_request(self, request_id, event_id):
        self._actor()
        row=await asyncio.to_thread(self.control._row,request_id)
        if row["event_id"]!=event_id or row["trigger_event_id"]!=event_id:
            raise PermissionError("original event not correlated")
        # A cold/unavailable search has not produced a source-grounded answer.
        # Preserve the question unclaimed so the SAME event may be read again.
        # A completed native prefetch can wait for dot *inside* the same
        # issuance gate. Its ticket is refreshed when read; if no dot arrives
        # before the bound, the ticket is released and this reads fresh.
        entry=self._reads.pop(request_id,None)
        if entry is None:
            evidence=await self._search(request_id)
        else:
            task,ready=entry
            ready.set()
            evidence=await task
        if not isinstance(evidence,dict):
            evidence={"status":"unavailable"}
        if evidence.get("status") not in ("ok","degraded") or evidence.get("context_gap") is True:
            return {"state":"pending","request_id":request_id,"evidence":evidence}
        try:
            lease=await asyncio.to_thread(self.control.claim,self.context.credentials,
                request_id=request_id,lease_seconds=300)
        except RequestControlError:
            return {"state":"pending","request_id":request_id}
        if lease["state"]!="leased":
            return {"state":lease["state"],"request_id":request_id}
        claim=await asyncio.to_thread(self.control.claim_context,self.context.credentials,
            request_id,lease["generation"],lease["execution_token"])
        # The exact Slack source question is returned only to the bound
        # restricted executor; no credentials or other actor context leak.
        query=re.sub(r"<@"+re.escape(self.control.bot_user_id)+r"(?:\|[^>]+)?>",
                     "",claim["input_text"]).strip()
        return {"state":"leased","request_id":request_id,"event_id":event_id,
                "generation":lease["generation"],
                "execution_token":lease["execution_token"],
                "question":query,"evidence":evidence,
                "instructions":"Answer the question in your own words first; never substitute raw "
                    "quotes or source cards for an answer. Return ONE coherent answer "
                    "in the question language; for Russian, "
                    "use 1–4 meaningful Russian sentences followed by 'Источники:'. "
                    "Cite only passages that actually support the named fact, using verified "
                    "metadata.citation and locator (printed or scanned page when known); "
                    "omit unknown author/year/ISBN and never display UUID/urn as a public URL. "
                    "Do not treat unrelated top-K hits as evidence: for unsupported questions "
                    "such as metro, cosmodrome or an unrecorded exact temperature, state "
                    "'В доступной базе подтверждений не найдено' without invented sources. "
                    "Distinguish retrieval pending/unavailable ('Не удалось проверить базу сейчас') "
                    "from successful search with no supporting evidence. "
                    "Preserve author attribution and don't invent page links."}

    async def record_result(self, request_id, event_id, generation, execution_token, text):
        self._actor()
        if (type(generation) is not int or generation<1
                or type(execution_token) is not str or not 32<=len(execution_token)<=256
                or type(text) is not str or not 1<=len(text)<=12000
                or len(text.encode())>24000):
            raise ValueError("bounded executor result required")
        row=await asyncio.to_thread(self.control._row,request_id)
        if row["event_id"]!=event_id or row["trigger_event_id"]!=event_id:
            raise PermissionError("original event not correlated")
        if row["state"] in _RECORD_STATES:
            return {"state":row["state"],"request_id":request_id,
                    "retry_send_allowed":False}
        receiver=self.result_receiver
        if receiver is None:
            raise RuntimeError("exact existing result receiver required")
        from .expert_executor_workflow import CODEC
        response=await receiver._advance({"codec":CODEC,"request_id":request_id,
            "generation":generation,"execution_token":execution_token,"text":text})
        result=json.loads(response.body)
        if result["state"]!="send_intent_ready":
            return {"state":result["state"],"request_id":request_id}
        delivery=self.context.delivery
        credentials=self.context.credentials
        permit=await asyncio.to_thread(delivery.claim_send_once,credentials,
            request_id,generation,execution_token)
        def source_current():
            # Native sender checks this both before and AFTER Slack auth.test.
            # Reuse the delivery controller's real source gate at the last
            # moment; a source may be revoked after claim_send_once.
            try:
                current=self.reply._row(request_id,("sending",))
                if current is None:return False
                self.context.delivery._source_current(
                    current,json.loads(current["origin"]))
                return True
            except Exception:
                return False
        try:
            sent=await self.reply.sender.post_once(
                event_id=permit["delivery_id"],channel=permit["channel_id"],
                thread=permit["thread_ts"],text=permit["text"],
                current=source_current)
        except BaseException:
            await asyncio.to_thread(delivery.uncertain_delivery,credentials,
                                    request_id,permit["delivery_id"])
            raise
        proof=verified_native_slack_receipt({
                "app_id":self.control.app_id,"user":self.control.bot_user_id,
                "channel":permit["channel_id"],"thread_ts":permit["thread_ts"],
                "ts":sent["ts"],"client_msg_id":permit["delivery_id"],
                "text":permit["text"]},
            app_id=self.control.app_id,team_id=self.control.team_id,
            bot_user_id=self.control.bot_user_id,
            channel_id=permit["channel_id"],thread_ts=permit["thread_ts"])
        await delivery.stage_dataset_native_receipt(credentials,request_id,proof)
        try:
            receipt=await asyncio.to_thread(delivery.confirm_dataset_native_receipt,
                                            credentials,request_id,proof)
        except RequestControlError:
            return {"state":"receipt_pending","request_id":request_id,
                    "retry_send_allowed":False}
        return {"state":receipt["state"],"request_id":request_id,
                "retry_send_allowed":False}

    def register(self,mcp):
        """Add tools and draft Events methods to the SAME authenticated MCP."""
        from .expert_executor_workflow import ExpertExecutorResultReceiver
        if type(self.result_receiver) is not ExpertExecutorResultReceiver:
            raise ValueError("existing bound result receiver required")

        @mcp.tool(name="expert_request_get",
                  description="Claim the exact saved Slack expert request after rkb.expert.question.created; "
                    "read current scoped evidence and retain the lease token for record_result.")
        async def expert_request_get(request_id:str,event_id:str)->dict:
            try:return await self.get_request(request_id,event_id)
            except (PermissionError,RequestControlError):
                raise ToolError("executor_scope_denied") from None

        @mcp.tool(name="expert_result_record",
                  description="Write one source-grounded answer under the original RKB lease. "
                    "Existing Dataset result/send-intent and Slack receipt decide completion; "
                    "on pending, retry this SAME request/lease, never send Slack yourself.")
        async def expert_result_record(request_id:str,event_id:str,
                generation:int,execution_token:str,text:str)->dict:
            try:return await self.record_result(request_id,event_id,generation,execution_token,text)
            except (PermissionError,RequestControlError):
                raise ToolError("executor_scope_denied") from None

        async def list_events(ctx,params):
            self._actor()
            return {"events":[_event_definition()]}

        async def subscribe(ctx,params):
            return await self.subscribe(params)

        async def unsubscribe(ctx,params):
            return self.unsubscribe(params)

        async def discover(ctx,params):
            # Discovery is shared with ordinary RKB users; event tools
            # enforce the restricted executor principal independently.
            server=mcp._lowlevel_server
            original=await server._handle_discover(ctx,params)
            result=original.model_dump(mode="json",by_alias=True,exclude_none=True)
            if ctx.protocol_version=="2026-07-28":
                result["capabilities"]["events"]={}
            return result

        mcp._lowlevel_server.add_request_handler(
            "server/discover",types.RequestParams,discover)
        mcp._lowlevel_server.add_request_handler(
            "events/list",types.PaginatedRequestParams,list_events)
        mcp._lowlevel_server.add_request_handler(
            "events/subscribe",EventParams,subscribe)
        mcp._lowlevel_server.add_request_handler(
            "events/unsubscribe",EventParams,unsubscribe)
        mcp._target_dot_events=True
