"""Slack provider boundary for the in-authority expert request queue.

No model runs here. A native response/echo verifies an outgoing result; timeouts
remain unknown. Research execution uses separate request-bound service context.
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import random
import re
import time
from urllib.parse import urlsplit

import aiohttp

from .expert_requests import ExpertRequestError, SlackDeliveryReceipt, SlackObservation
from .participants import ParticipantError

log=logging.getLogger(__name__)


class SlackBoundaryError(Exception):
    def __init__(self,code):
        self.code=code
        super().__init__(code)


class SlackRateLimited(SlackBoundaryError):
    def __init__(self,retry_after):
        self.retry_after=retry_after
        super().__init__("slack_rate_limited")


def verify_http_body(raw,headers,signing_secret,*,timestamp=None):
    """Verify Slack's v0 HMAC over exact bytes, including a bounded replay window."""
    timestamp=time.time() if timestamp is None else timestamp
    if not isinstance(raw,bytes) or len(raw)>262144 or not signing_secret:
        raise SlackBoundaryError("invalid_signed_body")
    fields={k.lower():v for k,v in headers.items()}
    sent_at=fields.get("x-slack-request-timestamp","")
    signature=fields.get("x-slack-signature","")
    if not sent_at.isdigit() or abs(timestamp-int(sent_at))>300:
        raise SlackBoundaryError("slack_replay_window")
    expected="v0="+hmac.new(signing_secret.encode(),b"v0:"+sent_at.encode()+b":"+raw,hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected,signature):
        raise SlackBoundaryError("slack_signature_invalid")
    try:
        value=json.loads(raw)
        if not isinstance(value,dict): raise ValueError()
        return value
    except (ValueError,UnicodeDecodeError):
        raise SlackBoundaryError("invalid_signed_body") from None


def observation_from_event(payload,*,app_id,team_id,bot_user_id):
    """Only call on verified HTTP bytes or the authenticated Socket Mode client."""
    if not isinstance(payload,dict) or payload.get("api_app_id")!=app_id or payload.get("team_id")!=team_id:
        raise SlackBoundaryError("slack_event_scope_mismatch")
    if payload.get("type")!="event_callback": return None
    event=payload.get("event")
    if not isinstance(event,dict): raise SlackBoundaryError("slack_event_invalid")
    if event.get("bot_id") or event.get("user")==bot_user_id or event.get("subtype"):
        return None
    kind=event.get("type")
    if kind=="app_mention":
        # Every group, including a private channel, gets the public projection.
        channel_type="group"
    elif kind=="message" and event.get("channel_type")=="im":
        channel_type="im"
    else:
        return None
    required=("user","channel","ts","text")
    if any(not isinstance(event.get(k),str) or not event[k] for k in required) or not isinstance(payload.get("event_id"),str):
        raise SlackBoundaryError("slack_event_invalid")
    thread=event.get("thread_ts",event["ts"])
    if not isinstance(thread,str): raise SlackBoundaryError("slack_event_invalid")
    return SlackObservation(app_id=app_id,team_id=team_id,user_id=event["user"],channel_id=event["channel"],
        channel_type=channel_type,thread_ts=thread,message_ts=event["ts"],event_id=payload["event_id"],text=event["text"])


def result_blocks(text):
    # Preserve every character; no links, user mentions or instructions in model
    # output become Slack control syntax. Four blocks cover the result limit.
    return [{"type":"section","text":{"type":"plain_text","text":text[i:i+3000],"emoji":False}}
            for i in range(0,len(text),3000)]


def original_block_text(message):
    blocks=message.get("blocks")
    if not isinstance(blocks,list) or not blocks:
        raise SlackBoundaryError("slack_receipt_blocks_missing")
    pieces=[]
    for block in blocks:
        if (not isinstance(block,dict) or block.get("type")!="section" or not isinstance(block.get("text"),dict)
            or block["text"].get("type")!="plain_text" or not isinstance(block["text"].get("text"),str)):
            raise SlackBoundaryError("slack_receipt_blocks_mismatch")
        # Slack encodes these three control characters in returned text even
        # for plain_text blocks. Decode exactly once, not general HTML and not
        # recursively: a literal '&lt;' remains a literal '&lt;'.
        pieces.append(re.sub(r"&(amp|lt|gt);",lambda m:{"amp":"&","lt":"<","gt":">"}[m.group(1)],block["text"]["text"]))
    return "".join(pieces)


def native_receipt(message,*,app_id,team_id,bot_user_id,channel_id):
    if not isinstance(message,dict) or message.get("user")!=bot_user_id or message.get("app_id")!=app_id:
        raise SlackBoundaryError("slack_receipt_identity_mismatch")
    client_id=message.get("client_msg_id")
    if not isinstance(client_id,str): raise SlackBoundaryError("slack_receipt_correlation_missing")
    if not isinstance(message.get("ts"),str) or not isinstance(message.get("thread_ts"),str):
        raise SlackBoundaryError("slack_receipt_route_missing")
    return SlackDeliveryReceipt(app_id=app_id,team_id=team_id,bot_user_id=bot_user_id,
        channel_id=channel_id,thread_ts=message["thread_ts"],message_ts=message["ts"],
        client_message_id=client_id,text=original_block_text(message))


class SlackExpertAdapter:
    def __init__(self,requests,delivery_credentials,*,bot_token,app_token,session=None):
        self.requests=requests
        self.delivery_credentials=delivery_credentials
        self.bot_token=bot_token
        self.app_token=app_token
        self.session=session
        self._verified_token_digest=None

    async def _api(self,method,payload,*,app_token=False):
        if self.session is None: raise SlackBoundaryError("slack_session_missing")
        token=self.app_token if app_token else self.bot_token
        send=self.session.get if method=="conversations.replies" else self.session.post
        arguments={"params":payload} if method=="conversations.replies" else {"json":payload}
        async with send("https://slack.com/api/"+method,headers={"Authorization":"Bearer "+token},**arguments,
                timeout=aiohttp.ClientTimeout(total=15)) as response:
            if response.status==429:
                try: retry_after=max(1,int(response.headers.get("Retry-After","30")))
                except ValueError: retry_after=30
                raise SlackRateLimited(retry_after)
            if response.status!=200: raise SlackBoundaryError("slack_http_outcome_unknown")
            body=await response.json()
            if not isinstance(body,dict) or not body.get("ok"):
                # Most provider errors are safe rejections, but unknown internal
                # outcomes are never blindly retried. Rate limiting has an
                # explicit no-effect response and Retry-After contract.
                if isinstance(body,dict) and body.get("error") in {"rate_limited","ratelimited"}:
                    raise SlackRateLimited(30)
                raise SlackBoundaryError("slack_api_rejected_or_unknown")
            return body

    async def authenticate(self):
        used_token=self.bot_token
        data=await self._api("auth.test",{})
        if (used_token!=self.bot_token or data.get("team_id")!=self.requests.team_id
            or data.get("user_id")!=self.requests.bot_user_id):
            raise SlackBoundaryError("slack_bot_identity_mismatch")
        self._verified_token_digest=hashlib.sha256(used_token.encode()).hexdigest()

    async def send_one(self,request_id=None):
        if self._verified_token_digest!=hashlib.sha256(self.bot_token.encode()).hexdigest():
            await self.authenticate()
        request_id=request_id or await asyncio.to_thread(self.requests.next_delivery,self.delivery_credentials)
        if not request_id: return False
        try:
            claimed=await asyncio.to_thread(self.requests.claim_delivery,self.delivery_credentials,request_id)
            if not claimed["send_allowed"]: return False
            # Repeat current ACL immediately before the external boundary.
            checked=await asyncio.to_thread(self.requests.check_delivery,self.delivery_credentials,request_id,claimed["delivery_id"])
        except (ExpertRequestError,ParticipantError) as error:
            if error.code=="service_denied": raise
            await asyncio.to_thread(self.requests.block_delivery,self.delivery_credentials,request_id,error.code)
            log.info(json.dumps({"event":"expert_delivery_access_blocked","request_id":request_id,"reason_code":error.code}))
            return False
        origin=checked["origin"]
        try:
            if self._verified_token_digest!=hashlib.sha256(self.bot_token.encode()).hexdigest():
                await self.authenticate()
            body=await self._api("chat.postMessage",{"channel":origin["channel_id"],"thread_ts":origin["thread_ts"],
                "text":checked["text"],"blocks":result_blocks(checked["text"]),"mrkdwn":False,"parse":"none",
                "client_msg_id":checked["delivery_id"],"unfurl_links":False,"unfurl_media":False})
            receipt=native_receipt(body.get("message"),app_id=self.requests.app_id,team_id=self.requests.team_id,
                bot_user_id=self.requests.bot_user_id,channel_id=body.get("channel"))
            await asyncio.to_thread(self.requests.receipt,self.delivery_credentials,request_id,receipt)
        except SlackRateLimited as error:
            await asyncio.to_thread(self.requests.rate_limited_delivery,self.delivery_credentials,request_id,checked["delivery_id"],error.retry_after)
            log.info(json.dumps({"event":"expert_delivery_rate_limited","request_id":request_id,"retry_after":error.retry_after}))
            return False
        except asyncio.CancelledError:
            # The persisted sending state is already a fence. Cancellation
            # cannot turn it back into an automatically sendable result.
            raise
        except Exception:
            state=await asyncio.to_thread(self.requests.unknown_delivery,self.delivery_credentials,request_id,checked["delivery_id"])
            if state["state"]=="delivered":
                log.info(json.dumps({"event":"expert_delivery_verified_via_echo","request_id":request_id}))
                return True
            log.warning(json.dumps({"event":"expert_delivery_unknown","request_id":request_id}))
            return False
        log.info(json.dumps({"event":"expert_delivery_verified","request_id":request_id,"message_ts":receipt.message_ts}))
        return True

    async def receive(self,envelope,acknowledge):
        started=time.monotonic()
        eid=envelope.get("envelope_id")
        if not isinstance(eid,str): return
        if envelope.get("type")!="events_api":
            # No interactive mutation is implemented yet; callbacks cannot be
            # interpreted as grants or research commands.
            await acknowledge({"envelope_id":eid})
            return
        payload=envelope.get("payload")
        if (isinstance(payload,dict) and payload.get("api_app_id")==self.requests.app_id
            and payload.get("team_id")==self.requests.team_id and isinstance(payload.get("event"),dict)
            and payload["event"].get("user")==self.requests.bot_user_id):
            event=payload["event"]
            try:
                receipt=native_receipt(event,app_id=self.requests.app_id,team_id=self.requests.team_id,
                    bot_user_id=self.requests.bot_user_id,channel_id=event.get("channel"))
                recorded=await asyncio.to_thread(self.requests.receipt_from_echo,self.delivery_credentials,receipt)
                if recorded: log.info(json.dumps({"event":"expert_native_echo_verified","message_ts":receipt.message_ts}))
            except (SlackBoundaryError,ExpertRequestError):
                # Other app messages and mismatched echoes never create work.
                pass
            await acknowledge({"envelope_id":eid})
            return
        observation=observation_from_event(payload,app_id=self.requests.app_id,team_id=self.requests.team_id,bot_user_id=self.requests.bot_user_id)
        if observation is None:
            await acknowledge({"envelope_id":eid})
            return
        try:
            result=await asyncio.to_thread(self.requests.accept,observation)
        except (ExpertRequestError,ParticipantError) as error:
            log.info(json.dumps({"event":"expert_request_rejected","reason_code":error.code,"provider_event_id":observation.event_id}))
            await acknowledge({"envelope_id":eid})
            return
        # SQLite busy/crash exceptions deliberately propagate without ACK.
        # Slack retries; an accepted event is deduplicated after restart.
        await acknowledge({"envelope_id":eid})
        log.info(json.dumps({"event":"expert_request_accepted","request_id":result["request_id"],"state":result["state"],"duplicate":result["duplicate"],"ack_latency_ms":round((time.monotonic()-started)*1000)}))

    async def reconcile_one(self):
        target=await asyncio.to_thread(self.requests.next_reconciliation,self.delivery_credentials)
        if not target: return False
        request_id=target["request_id"]
        params={"channel":target["origin"]["channel_id"],"ts":target["origin"]["thread_ts"],"limit":50}
        if target["cursor"]: params["cursor"]=target["cursor"]
        try:
            body=await self._api("conversations.replies",params)
            messages=body.get("messages")
            if not isinstance(messages,list): raise SlackBoundaryError("slack_thread_readback_invalid")
            for message in messages:
                if isinstance(message,dict) and message.get("client_msg_id")==target["delivery_id"]:
                    receipt=native_receipt(message,app_id=self.requests.app_id,team_id=self.requests.team_id,
                        bot_user_id=self.requests.bot_user_id,channel_id=target["origin"]["channel_id"])
                    await asyncio.to_thread(self.requests.receipt,self.delivery_credentials,request_id,receipt)
                    log.info(json.dumps({"event":"expert_delivery_reconciled","request_id":request_id,"message_ts":receipt.message_ts}))
                    return True
            cursor=body.get("response_metadata",{}).get("next_cursor") if isinstance(body.get("response_metadata"),dict) else None
            if body.get("has_more") and not cursor:
                raise SlackBoundaryError("slack_thread_readback_incomplete")
            await asyncio.to_thread(self.requests.reconciliation_progress,self.delivery_credentials,request_id,
                cursor=cursor if body.get("has_more") else None,retry_after=60 if body.get("has_more") else 120)
        except SlackRateLimited as error:
            await asyncio.to_thread(self.requests.reconciliation_progress,self.delivery_credentials,request_id,cursor=target["cursor"],retry_after=error.retry_after)
        except Exception as error:
            # Absence/error is never permission to resend an uncertain effect.
            log.info(json.dumps({"event":"expert_reconcile_pending","request_id":request_id,"error_type":type(error).__name__}))
        return False

    async def socket_once(self):
        await self.authenticate()
        opened=await self._api("apps.connections.open",{},app_token=True)
        url=opened.get("url","")
        parts=urlsplit(url)
        if parts.scheme!="wss" or not parts.hostname or not parts.hostname.endswith(".slack.com") or parts.username or parts.password:
            raise SlackBoundaryError("slack_socket_endpoint_invalid")
        async with self.session.ws_connect(url,heartbeat=20,max_msg_size=262144) as ws:
            log.info(json.dumps({"event":"expert_socket_connected","component":"kaliningrad_base"}))
            async for message in ws:
                if message.type==aiohttp.WSMsgType.TEXT:
                    await self.receive(json.loads(message.data),ws.send_json)
                elif message.type in {aiohttp.WSMsgType.ERROR,aiohttp.WSMsgType.CLOSED}: break

    async def run(self):
        """Independent reconnect and outbox loops; no model or downstream gate."""
        if self.session is None:
            async with aiohttp.ClientSession(trust_env=False,timeout=aiohttp.ClientTimeout(total=15)) as session:
                self.session=session
                try: await self.run()
                finally:self.session=None
            return

        # Verify the actual native sender before either loop can send a result.
        # A readback rejection after chat.postMessage would be too late.
        await self.authenticate()

        async def receive_loop():
            delay=1
            while True:
                try:
                    await self.socket_once()
                    delay=1
                except asyncio.CancelledError: raise
                except Exception as error:
                    log.warning(json.dumps({"event":"expert_socket_reconnect","error_type":type(error).__name__,"retry_after":delay}))
                await asyncio.sleep(delay+random.uniform(0,min(5,delay/5)))
                delay=min(60,delay*2)

        async def deliver_loop():
            delay=2
            while True:
                try:
                    delivered=await self.send_one()
                    await self.reconcile_one()
                    delay=2
                except asyncio.CancelledError: raise
                except Exception as error:
                    delivered=False
                    delay=min(60,delay*2)
                    log.warning(json.dumps({"event":"expert_outbox_wait","error_type":type(error).__name__,"retry_after":delay}))
                await asyncio.sleep(1 if delivered else delay)

        async with asyncio.TaskGroup() as group:
            group.create_task(receive_loop(),name="rkb-slack-intake")
            group.create_task(deliver_loop(),name="rkb-slack-outbox")
