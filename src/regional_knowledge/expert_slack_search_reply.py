"""Opt-in read-only Kaliningrad Base reply; one signed event, at most one Slack post."""
from __future__ import annotations

import asyncio
import html
import json
import logging
import re
import time

import httpx
from mcp.server.mcpserver.exceptions import ToolError

from .participants import ParticipantError
from .contracts import EvidenceSearchOutput
from .expert_evidence_receiver import ExpertEvidenceReceiver, _RequestFence
from .expert_slack_ingress import ExpertSlackIngress
from .projects_hub_reader import PrivateInput, ReadContext, render as render_evidence
from .read_issuance import capture_read_output
from . import request_order

_LOG = logging.getLogger(__name__)
_PAGE = re.compile(r"(?i)(page|seite|стр(?:аница|\.)|лист|\bp\.\s*\d|\bs\.\s*\d)")


def _error_reply(reason):
    code=reason if type(reason) is str and re.fullmatch(r"[a-z_]{1,80}",reason) else None
    return "Ошибка поиска. Код: unavailable"+("/"+code if code is not None else "")


def answer_from_evidence(result):
    if (result.get("context_gap") is not False or result.get("status") not in ("ok","degraded")
            or result.get("mode") != "hybrid" or result.get("prior_write_pending")):
        raise PermissionError("verified source evidence required")
    if not result.get("evidence"):
        return "В доступных источниках подтверждений не найдено. Источник и страница не установлены."
    clean=lambda s,n:html.escape(" ".join(s.split())[:n],quote=False) if type(s) is str else ""
    lines=[]
    for item in result["evidence"][:3]:
        meta=item.get("metadata") or {}
        if type(meta) is not dict or not item.get("text"):
            raise PermissionError("exact source text required")
        locator=clean(meta.get("locator"),160)
        lines.append("*Источник:* "+(clean(item.get("title"),250) or "не указан")
            + "\n*Страница:* "+(locator if _PAGE.search(locator) else "не указана")
            + ("\n*Библиография:* "+clean(meta.get("citation"),350) if meta.get("citation") else "")
            + "\n*Цитата (фрагмент):* «"+clean(item["text"],480)+"»")
    text="\n\n".join(lines)
    if len(text.encode())>12000:raise ValueError("bounded reply required")
    return text


class SlackChatSender:
    """Original event_id is the native client_msg_id; ambiguous post is never retried."""
    def __init__(self, *, token, app_id, team_id, bot_user_id, current=lambda: True,
                 transport=None):
        if (type(token) is not str or not token.startswith("xoxb-")
                or any(type(x) is not str or not x for x in (app_id,team_id,bot_user_id))
                or not callable(current)):
            raise ValueError("explicit Slack bot credential required")
        self.token,self.app_id,self.team_id,self.bot_user_id=token,app_id,team_id,bot_user_id
        self.current,self.transport=current,transport

    async def post_once(self, *, event_id=None, channel=None, thread=None, text=None,
                        current=None, claim=None):
        headers={"Authorization":"Bearer "+self.token}
        async with httpx.AsyncClient(transport=self.transport,timeout=8) as http:
            await self._authenticate(http,current)
            if claim is not None:
                args=await claim()
                event_id,channel,thread,text=(args["event_id"],args["channel"],
                                               args["thread"],args["text"])
            response=await http.post("https://slack.com/api/chat.postMessage",headers=headers,
                json={"channel":channel,"thread_ts":thread,"text":text,
                      "client_msg_id":event_id,"metadata":{"event_type":"rkb_slack_reply","event_payload":{"event_id":event_id}},"unfurl_links":False,"unfurl_media":False})
            data=response.json()
        message=data.get("message")
        # Slack's documented chat.postMessage response need not echo optional
        # user/thread/client_msg_id fields. The bot principal was verified by
        # auth.test; reject conflicting values if those fields are present.
        if (response.status_code!=200 or data.get("ok") is not True or data.get("channel")!=channel
                or type(message) is not dict or message.get("text")!=text
                or message.get("user",self.bot_user_id)!=self.bot_user_id
                or message.get("thread_ts",thread)!=thread
                or message.get("client_msg_id",event_id)!=event_id
                or not re.fullmatch(r"[0-9]+\.[0-9]+",str(data.get("ts","")))):
            raise RuntimeError("Slack native provider echo unverified")
        return {"event_id":event_id,"channel":channel,"thread_ts":thread,
                "ts":data["ts"],"bot_user_id":self.bot_user_id}

    async def _authenticate(self,http,current=None):
        auth=(await http.post("https://slack.com/api/auth.test",headers={"Authorization":"Bearer "+self.token})).json()
        if (auth.get("ok") is not True or auth.get("team_id")!=self.team_id
                or auth.get("user_id")!=self.bot_user_id or not self.current()
                or current is not None and not current()):
            raise PermissionError("Slack bot identity or evidence changed")

    async def find_post(self,origin,marker):
        async with asyncio.timeout(30), httpx.AsyncClient(transport=self.transport,timeout=8) as http:
            await self._authenticate(http)
            cursor=""
            for _ in range(16):
                response=await http.get("https://slack.com/api/conversations.replies",headers={"Authorization":"Bearer "+self.token},params={"channel":origin["channel_id"],"ts":origin["thread_ts"],"limit":100,"cursor":cursor,"include_all_metadata":True})
                response.raise_for_status();data=response.json()
                if data.get("ok") is not True or type(data.get("messages")) is not list:raise PermissionError("Slack thread reconciliation unavailable")
                found=next((m for m in data.get("messages",[]) if (m.get("client_msg_id")==marker or m.get("metadata")=={"event_type":"rkb_slack_reply","event_payload":{"event_id":marker}}) and m.get("user")==self.bot_user_id and m.get("thread_ts")==origin["thread_ts"]),None)
                if found:return {"channel":origin["channel_id"],"ts":found["ts"],"event_id":marker}
                cursor=data.get("response_metadata",{}).get("next_cursor","")
                if not cursor and not data.get("has_more"):return None
                if not cursor:break
            raise RuntimeError("Slack thread reconciliation incomplete")


class ExpertSlackSearchReply:
    def __init__(self, *, ingress, evidence, sender):
        if (type(ingress) is not ExpertSlackIngress or type(evidence) is not ExpertEvidenceReceiver
                or type(sender) is not SlackChatSender or evidence.control is not ingress.control
                or (sender.app_id,sender.team_id,sender.bot_user_id) !=
                    (ingress.control.app_id,ingress.control.team_id,ingress.control.bot_user_id)):
            raise ValueError("one signed ingress, human read and Slack bot authority required")
        self.ingress,self.evidence,self.sender=ingress,evidence,sender
        self.control=ingress.control
        self._tasks={}

    def notify(self, request_id):
        if request_id in self._tasks:return
        if len(self._tasks)>=16:raise RuntimeError("bounded Slack event wake capacity")
        task=asyncio.create_task(self._answer_until_sent(request_id))
        self._tasks[request_id]=task
        def finish(t):
            self._tasks.pop(request_id,None)
            try:t.result()
            except asyncio.CancelledError:pass
            except Exception as exc:_LOG.warning("rkb_slack_search_pending error_type=%s",
                                                type(exc).__name__)
            else:self.resume_pending()
        task.add_done_callback(finish)

    def resume_pending(self):
        with self.control.corpus.connect() as db:
            rows=db.execute("SELECT r.id FROM rkb_expert_requests r JOIN rkb_request_dialogs d ON d.id=r.dialog_id AND d.head_seq=r.dialog_seq WHERE r.state IN ('received','searching','unknown') ORDER BY r.created_at LIMIT 16").fetchall()
        for row in rows:self.notify(row["id"])

    async def _answer_until_sent(self, request_id):
        # Retry the same acknowledged search while warming; the notice has a distinct marker.
        # A possible final provider effect remains sending/unknown and is not resent.
        delay=2
        while True:
            try:
                row=await asyncio.to_thread(self._row,request_id,("received","searching","unknown"))
                if row and row["state"]=="unknown":
                    receipt=await self.sender.find_post(json.loads(row["origin"]),row["delivery_id"] or row["event_id"])
                    if await asyncio.to_thread(self._finish,request_id,"unknown","delivered" if receipt else "abandoned_no_resend",receipt or {"marker_not_found":True}):
                        _LOG.info("rkb_slack_reconciled request_id=%s native_found=%s",request_id,bool(receipt))
                else:await self._answer(request_id)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                _LOG.warning("rkb_slack_reply_deferred request_id=%s error_type=%s",
                             request_id,type(exc).__name__)
            try:
                row=await asyncio.to_thread(self._row,request_id,("received","searching","unknown"))
                if row is None:
                    return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                _LOG.warning("rkb_slack_state_deferred error_type=%s",
                             type(exc).__name__)
            await asyncio.sleep(delay)
            delay=min(60,delay*2)

    def _verified(self, db, row):
        origin=json.loads(row["origin"])
        self.control._destination_allowed(origin)
        from .contracts import Principal
        ctx=self.control.people.resolve_external(provider="slack",tenant=origin["team_id"],
            provider_subject=origin["user_id"],connection=db,
            principal_factory=lambda who:Principal(subject=who,issuer="slack",
                client_id="rkb-trusted-slack",access_token=""))
        if (ctx.principal.subject!=row["person_id"] or ctx.link_id!=row["link_id"]
                or ctx.link_revision!=row["link_revision"]):
            raise PermissionError("original Slack actor changed")
        self.control.people.require(ctx,"public_search",connection=db)
        return origin

    def _row(self, request_id, states, *, begin=False):
        with self.control.corpus.connect() as db:
            db.execute("BEGIN IMMEDIATE" if begin else "BEGIN")
            row=db.execute("SELECT * FROM rkb_expert_requests WHERE id=?",(request_id,)).fetchone()
            if row is None or row["state"] not in states or not request_order.is_head(db,row):return None
            origin=self._verified(db,row)
            if begin and row["state"]=="received":
                db.execute("UPDATE rkb_expert_requests SET state='searching',updated_at=? WHERE id=?",
                    (self.control.clock(),request_id))
            return dict(row,origin=json.dumps(origin))

    def _claim_warming_post(self, request_id):
        with self.control.corpus.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row=db.execute("SELECT * FROM rkb_expert_requests WHERE id=?",(request_id,)).fetchone()
            if (row is None or row["state"]!="searching" or row["delivery_id"] is not None
                    or not request_order.is_head(db,row)):
                return None
            origin=self._verified(db,row)
            marker=row["event_id"]+":warming"
            # One durable intent before Slack POST, including uncertain provider outcomes.
            db.execute("UPDATE rkb_expert_requests SET delivery_id=?,updated_at=? WHERE id=?",
                       (marker,self.control.clock(),request_id))
            return dict(event_id=marker,channel=origin["channel_id"],thread=origin["thread_ts"])

    def _claim_post(self, request_id):
        with self.control.corpus.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row=db.execute("SELECT * FROM rkb_expert_requests WHERE id=?",(request_id,)).fetchone()
            if (row is None or row["state"]!="searching"
                    or not request_order.is_head(db,row)):
                return None
            origin=self._verified(db,row)
            marker=row["event_id"]
            db.execute("UPDATE rkb_expert_requests SET state='sending',delivery_id=?,"
                       "delivery_attempt=delivery_attempt+1,updated_at=? WHERE id=?",
                       (marker,self.control.clock(),request_id))
            return dict(event_id=marker,channel=origin["channel_id"],thread=origin["thread_ts"])

    def _finish(self, request_id, expected, state, receipt=None):
        with self.control.corpus.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row=db.execute("SELECT * FROM rkb_expert_requests WHERE id=?",(request_id,)).fetchone()
            if row is None or row["state"]!=expected:return False
            db.execute("UPDATE rkb_expert_requests SET state=?,delivery_receipt=?,updated_at=? WHERE id=?",
                       (state,json.dumps(receipt) if receipt else None,self.control.clock(),request_id))
            if state in ("delivered","abandoned_no_resend"):request_order.advance(db,row["dialog_id"])
            return True

    async def _answer(self, request_id):
        row=await asyncio.to_thread(self._row,request_id,("received","searching"),begin=True)
        if row is None:return
        origin=json.loads(row["origin"])
        current=lambda:self._row(request_id,("searching","sending")) is not None
        destination=self.evidence._destination(origin,row["person_id"],request_id)
        authority=await self.evidence.authority.resolve_adapter(
            provider="slack",tenant=origin["team_id"],provider_subject=origin["user_id"],
            destination=destination)
        calls=[]
        try:
            if (authority.person_id!=row["person_id"] or authority.link_id!=row["link_id"]
                    or authority.link_revision!=row["link_revision"]):
                raise PermissionError("verified Slack binding changed")
            query=re.sub(r"<@"+re.escape(self.control.bot_user_id)+r"(?:\|[^>]+)?>",
                         "",row["input_text"]).strip()
            # The Slack connector's presentation footer is not the question.
            query=re.sub(r"(?:^|\s+)\*Sent using\* <@[UW][A-Z0-9]+(?:\|[^>]+)?>\s*\Z",
                         "",query).strip()
            ctx=ReadContext(request_id,0,0,time.monotonic()+10)
            issued=None
            try:
                with capture_read_output() as calls:
                    if not 1<=len(query)<=1000:
                        _LOG.info("rkb_slack_query_rejected request_id=%s reason=query_length query_chars=%s",request_id,len(query))
                        raise ToolError('{"status":"invalid_query","reason":"query_length"}')
                    outcome=await self.evidence.backend.search_adapter(query,authority=authority,
                        request_id=request_id,turn_generation=0,min_data_seq=0,
                        deadline_monotonic=ctx.deadline_monotonic,destination=destination,limit=3)
            except Exception as exc:
                # A read that has not issued evidence can return one honest
                # text-only failure, but never turn an access denial into a post.
                if calls or isinstance(exc,(PermissionError,ParticipantError)):
                    raise
                text=_error_reply(None)
                if isinstance(exc,ToolError):
                    try:details=json.loads(str(exc))
                    except (ValueError,TypeError):raise
                    if details=={"status":"invalid_query","reason":"query_length"}:
                        text="Запрос должен содержать от 1 до 1000 символов без упоминания бота. Сократите вопрос и отправьте его снова."
                    elif type(details) is not dict or details.get("status") not in ("unavailable","pending"):
                        raise
                    else:
                        if details["status"]=="pending":return
                        text=_error_reply(details.get("reason"))
            else:
                if (not calls and type(outcome) is EvidenceSearchOutput
                        and outcome.status=="pending" and outcome.reason=="target_warming"
                        and not outcome.evidence):
                    # A separate one-shot notice does not finish the original request.
                    if row["delivery_id"] is None:
                        eta=outcome.startup.eta_seconds if outcome.startup else None
                        text=(f"Запускаю базу, ответ будет примерно через {max(1,(eta+59)//60)} мин."
                              if eta is not None else "Запускаю базу, отвечу после запуска.")
                        async def claim_warming():
                            permit=await asyncio.to_thread(self._claim_warming_post,request_id)
                            if permit is None:raise PermissionError("Slack warming notice already claimed")
                            return dict(permit,text=text)
                        await self.sender.post_once(claim=claim_warming)
                    return
                else:
                    if len(calls)!=1:raise PermissionError("retained source gate required")
                    issued=calls[0]
                    issued.gate=_RequestFence(issued.gate,current)
                    issued.gated=await issued.gate.refresh(issued.gated)
                    if not issued.gate.issuance_current(issued.gated.ticket):
                        raise PermissionError("source access revoked")
                    evidence=render_evidence(issued.render(issued.gated.result),ctx)
                    text=(_error_reply(evidence.get("reason"))
                          if evidence.get("context_gap") is True and evidence.get("status")=="unavailable"
                          else answer_from_evidence(evidence))
            permit=None
            async def claim_for_post():
                nonlocal permit
                permit=await asyncio.to_thread(self._claim_post,request_id)
                if permit is None:
                    raise PermissionError("original Slack request not claimable")
                return dict(permit,text=text)
            try:
                receipt=await self.sender.post_once(
                    current=(None if issued is None
                             else lambda:issued.gate.issuance_current(issued.gated.ticket)),
                    claim=claim_for_post)
            except BaseException:
                if permit is not None:
                    await asyncio.to_thread(self._finish,request_id,"sending","unknown")
                raise
            await asyncio.to_thread(self._finish,request_id,"sending","delivered",receipt)
        finally:
            if calls:calls[0].release()
            else:self.evidence.authority.release(authority)


def configured_expert_slack_search_reply(ingress,evidence,path):
    """Operator supplies private files and the existing Slack installation."""
    config=PrivateInput(path,2048)
    value=json.loads(config.value)
    if (type(value) is not dict or set(value)!={"codec","bot_token_file"}
            or value["codec"]!="rkb.expert-slack-search-reply.v1"):
        raise ValueError("exact private Slack reply config required")
    token=PrivateInput(value["bot_token_file"],512)
    sender=SlackChatSender(token=token.value.decode("ascii").strip(),
        app_id=ingress.control.app_id,team_id=ingress.control.team_id,
        bot_user_id=ingress.control.bot_user_id,
        current=lambda:config.current() and token.current())
    return ExpertSlackSearchReply(ingress=ingress,evidence=evidence,sender=sender)
