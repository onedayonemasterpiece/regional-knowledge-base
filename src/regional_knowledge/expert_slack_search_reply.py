"""Opt-in read-only Kaliningrad Base reply; one signed event, at most one Slack post."""
from __future__ import annotations

import asyncio
import html
import json
import logging
import re
import time

import httpx

from .expert_evidence_receiver import ExpertEvidenceReceiver, _RequestFence
from .expert_slack_ingress import ExpertSlackIngress
from .projects_hub_reader import PrivateInput, ReadContext, render as render_evidence
from .read_issuance import capture_read_output
from . import request_order

_LOG = logging.getLogger(__name__)
_PAGE = re.compile(r"(?i)(page|seite|стр(?:аница|\.)|лист|\bp\.\s*\d|\bs\.\s*\d)")


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

    async def post_once(self, *, event_id, channel, thread, text, current):
        if not self.current() or not current():raise PermissionError("current Slack scope required")
        headers={"Authorization":"Bearer "+self.token}
        async with httpx.AsyncClient(transport=self.transport,timeout=8) as http:
            auth=(await http.post("https://slack.com/api/auth.test",headers=headers)).json()
            if (auth.get("ok") is not True or auth.get("team_id")!=self.team_id
                    or auth.get("user_id")!=self.bot_user_id or not self.current()
                    or not current()):
                raise PermissionError("Slack bot identity or evidence changed")
            response=await http.post("https://slack.com/api/chat.postMessage",headers=headers,
                json={"channel":channel,"thread_ts":thread,"text":text,
                      "client_msg_id":event_id,"unfurl_links":False,"unfurl_media":False})
            data=response.json()
        message=data.get("message") or {}
        if (response.status_code!=200 or data.get("ok") is not True or data.get("channel")!=channel
                or message.get("user")!=self.bot_user_id or message.get("text")!=text
                or message.get("thread_ts")!=thread or message.get("client_msg_id")!=event_id
                or not re.fullmatch(r"[0-9]+\.[0-9]+",str(data.get("ts","")))):
            raise RuntimeError("Slack native provider echo unverified")
        return {"event_id":event_id,"channel":channel,"thread_ts":thread,
                "ts":data["ts"],"bot_user_id":self.bot_user_id}


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
        task=asyncio.create_task(self._answer(request_id))
        self._tasks[request_id]=task
        def finish(t):
            self._tasks.pop(request_id,None)
            try:t.result()
            except asyncio.CancelledError:pass
            except Exception as exc:_LOG.warning("rkb_slack_search_pending error_type=%s",
                                                type(exc).__name__)
        task.add_done_callback(finish)

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
            if row is None or row["state"] not in states:return None
            origin=self._verified(db,row)
            if begin and row["state"]=="received":
                db.execute("UPDATE rkb_expert_requests SET state='searching',updated_at=? WHERE id=?",
                    (self.control.clock(),request_id))
            return dict(row,origin=json.dumps(origin))

    def _claim_post(self, request_id):
        with self.control.corpus.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row=db.execute("SELECT * FROM rkb_expert_requests WHERE id=?",(request_id,)).fetchone()
            if row is None or row["state"]!="searching":return None
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
            if state=="delivered":request_order.advance(db,row["dialog_id"])
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
            if not 1<=len(query)<=1000:raise ValueError("bounded query required")
            ctx=ReadContext(request_id,0,0,time.monotonic()+10)
            with capture_read_output() as calls:
                await self.evidence.backend.search_adapter(query,authority=authority,
                    request_id=request_id,turn_generation=0,min_data_seq=0,
                    deadline_monotonic=ctx.deadline_monotonic,destination=destination,limit=3)
            if len(calls)!=1:raise PermissionError("retained source gate required")
            issued=calls[0]
            issued.gate=_RequestFence(issued.gate,current)
            issued.gated=await issued.gate.refresh(issued.gated)
            if not issued.gate.issuance_current(issued.gated.ticket):
                raise PermissionError("source access revoked")
            text=answer_from_evidence(render_evidence(issued.render(issued.gated.result),ctx))
            permit=await asyncio.to_thread(self._claim_post,request_id)
            if permit is None:return
            try:
                receipt=await self.sender.post_once(**permit,text=text,
                    current=lambda:current() and issued.gate.issuance_current(issued.gated.ticket))
            except BaseException:
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
