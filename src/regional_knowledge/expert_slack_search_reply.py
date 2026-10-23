"""Opt-in same-process Slack search reply using the existing expert authorities.

No model, second service, impersonation, provider scheduler or direct PG access.
An HTTP Slack ACK is earned by the existing remotely covered ingress, not by
this opportunistic search wakeup. Sending uses one durable Dataset send permit;
a lost Slack acknowledgment is UNKNOWN and MUST NOT trigger another post.
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import html
import json
import logging
from pathlib import Path
import re
import time

import httpx

from .expert_delivery_control import (
    TargetExpertDeliveryControl, verified_native_slack_receipt,
)
from .expert_evidence_receiver import ExpertEvidenceReceiver, _RequestFence
from .expert_request_control import ExecutorCredentials, TargetExpertRequestControl
from .expert_slack_ingress import ExpertSlackIngress
from .projects_hub_reader import PrivateInput, ReadContext, render as render_evidence
from .read_issuance import capture_read_output


_LOG = logging.getLogger(__name__)
_SLACK_API = "https://slack.com/api/"
_PAGE = re.compile(r"(?i)(?:\bpage\b|\bp\.\s*\d|\bseite\b|\bs\.\s*\d|стр(?:аница|\.)|лист|physical_page)")


def _plain(value, maximum=600):
    if type(value) is not str:
        return ""
    return html.escape(" ".join(value.split())[:maximum], quote=False)


def answer_from_evidence(response):
    """No synthesis: only an authorized current evidence pack becomes Slack text."""
    if (type(response) is not dict or response.get("context_gap") is not False
            or response.get("status") not in ("ok", "degraded")
            or response.get("mode") != "hybrid"
            or response.get("prior_write_pending")
            or type(response.get("evidence")) is not list):
        raise ValueError("search evidence was not issued")
    if not response["evidence"]:
        return "В доступных источниках подтверждений не найдено. Источник и страница не установлены."
    parts = []
    for item in response["evidence"][:3]:
        if type(item) is not dict or type(item.get("text")) is not str or not item["text"]:
            raise ValueError("exact evidence text required")
        metadata = item.get("metadata") or {}
        if type(metadata) is not dict:
            raise ValueError("exact evidence metadata required")
        title = _plain(item.get("title"), 250) or "Название источника не указано"
        citation = _plain(metadata.get("citation"), 350)
        locator = _plain(metadata.get("locator"), 160)
        page = locator if locator and _PAGE.search(locator) else "не указана"
        fragment = _plain(item["text"], 480)
        # A locator or internal URN is not a public hyperlink or proof scan.
        parts.append(
            f"*Источник:* {title}\n"
            f"*Страница:* {page}\n"
            + (f"*Библиография:* {citation}\n" if citation else "")
            + (f"*Локатор:* {locator}\n" if locator and page == "не указана" else "")
            + f"*Фрагмент текста:* «{fragment}»"
        )
    text = "\n\n".join(parts)
    if len(text.encode("utf-8")) > 12000:
        raise ValueError("bounded reply required")
    return text


class SlackChatSender:
    """Bot-token-bound native chat.postMessage; never retry an ambiguous send."""
    def __init__(self, *, token, app_id, team_id, bot_user_id, current=lambda: True,
                 transport=None):
        if (type(token) is not str or not token.startswith("xoxb-")
                or any(type(v) is not str or not v for v in (app_id, team_id, bot_user_id))
                or not callable(current)):
            raise ValueError("configured Slack bot identity required")
        self._token = token
        self.app_id, self.team_id, self.bot_user_id = app_id, team_id, bot_user_id
        self.current = current
        self.transport = transport

    async def post_once(self, permit):
        if (not self.current() or type(permit) is not dict
                or set(permit) != {"request_id", "delivery_id", "channel_id", "thread_ts", "text"}
                or not all(type(permit[k]) is str and permit[k]
                           for k in ("delivery_id", "channel_id", "thread_ts", "text"))):
            raise PermissionError("Slack send config or permit invalid")
        headers = {"Authorization": "Bearer " + self._token}
        async with httpx.AsyncClient(transport=self.transport, timeout=8.0) as client:
            # Verify that the installed bot token, not the owner's account, is
            # the transport principal for this exact Slack workspace.
            identity = await client.post(_SLACK_API + "auth.test", headers=headers)
            data = identity.json()
            if (identity.status_code != 200 or data.get("ok") is not True
                    or data.get("team_id") != self.team_id
                    or data.get("user_id") != self.bot_user_id):
                raise PermissionError("Slack bot token identity differs")
            if not self.current():
                raise PermissionError("Slack send configuration changed")
            posted = await client.post(_SLACK_API + "chat.postMessage",
                headers=headers, json={
                    "channel": permit["channel_id"],
                    "thread_ts": permit["thread_ts"],
                    "text": permit["text"],
                    "client_msg_id": permit["delivery_id"],
                    "unfurl_links": False, "unfurl_media": False,
                })
            body = posted.json()
            if posted.status_code != 200 or body.get("ok") is not True:
                raise RuntimeError("Slack send outcome unverified")
        message = body.get("message")
        if (type(message) is not dict
                or message.get("user") != self.bot_user_id
                or message.get("text") != permit["text"]
                or message.get("thread_ts") != permit["thread_ts"]
                or message.get("client_msg_id") != permit["delivery_id"]
                or body.get("channel") != permit["channel_id"]
                or type(body.get("ts")) is not str):
            # A post may have succeeded: the existing control records UNKNOWN.
            raise RuntimeError("Slack native echo unverified")
        return verified_native_slack_receipt({
            "app_id": self.app_id, "user": message["user"],
            "channel": body["channel"], "thread_ts": message["thread_ts"],
            "ts": body["ts"], "client_msg_id": message["client_msg_id"],
            "text": message["text"],
        }, app_id=self.app_id, team_id=self.team_id,
            bot_user_id=self.bot_user_id,
            channel_id=permit["channel_id"], thread_ts=permit["thread_ts"])


class _BoundSearchGate:
    """Exactly one in-flight retained native read, not a global allow-all."""
    def __init__(self):
        self.active = None

    def __call__(self, row, origin):
        if self.active is None:
            return False
        request_id, person_id, expected_origin, issued, current = self.active
        try:
            return (row["id"] == request_id and row["person_id"] == person_id
                    and origin == expected_origin and current()
                    and issued.gate.issuance_current(issued.gated.ticket))
        except Exception:
            return False


class ExpertSlackSearchReply:
    """In-process continuation of remotely accepted Slack events, no LLM."""
    def __init__(self, *, ingress, evidence, delivery, credentials, sender, search_gate):
        if (type(ingress) is not ExpertSlackIngress
                or type(evidence) is not ExpertEvidenceReceiver
                or type(delivery) is not TargetExpertDeliveryControl
                or type(credentials) is not ExecutorCredentials
                or type(sender) is not SlackChatSender
                or type(search_gate) is not _BoundSearchGate
                or evidence.control is not ingress.control
                or delivery.control is not ingress.control
                or delivery.source_gate is not search_gate
                or (sender.app_id, sender.team_id, sender.bot_user_id) !=
                   (ingress.control.app_id, ingress.control.team_id, ingress.control.bot_user_id)):
            raise ValueError("one existing ingress, human actor, source and delivery control required")
        self.ingress, self.evidence, self.delivery = ingress, evidence, delivery
        self.control = ingress.control
        self.credentials, self.sender, self.source_gate = credentials, sender, search_gate
        self._pending = set()
        self._worker = None

    def notify(self, request_id):
        """Only the signed ingress calls this after durable coverage/ACK."""
        if type(request_id) is not str or len(request_id) > 128:
            raise ValueError("exact remotely accepted request required")
        self._pending.add(request_id)
        if self._worker is None or self._worker.done():
            self._worker = asyncio.create_task(self._drain())
        # Work deliberately does not delay the Slack Events HTTP response.

    def _claim_current(self, claim, origin, person_id):
        try:
            context = self.control.claim_context(self.credentials, claim["request_id"],
                claim["generation"], claim["execution_token"])
            return context["origin"] == origin and context["person_id"] == person_id
        except Exception:
            return False

    @asynccontextmanager
    async def _read_claim(self, claim):
        context = await asyncio.to_thread(self.control.claim_context, self.credentials,
            claim["request_id"], claim["generation"], claim["execution_token"])
        origin = context["origin"]
        verified = self.evidence._destination(origin, context["person_id"], claim["request_id"])
        authority = await self.evidence.authority.resolve_adapter(
            provider="slack", tenant=origin["team_id"],
            provider_subject=origin["user_id"], destination=verified)
        retained = False
        calls = []
        try:
            row = await asyncio.to_thread(self.control._row, claim["request_id"])
            if (authority.person_id != context["person_id"]
                    or authority.link_id != row["link_id"]
                    or authority.link_revision != row["link_revision"]):
                raise PermissionError("verified Slack actor changed")
            query = re.sub(r"<@" + re.escape(self.control.bot_user_id) + r"(?:\|[^>]+)?>",
                "", context["input_text"]).strip()
            if not 1 <= len(query) <= 1000:
                raise ValueError("bounded human query required")
            deadline = time.monotonic() + 10.0
            ctx = ReadContext(claim["request_id"], claim["generation"], 0, deadline)
            with capture_read_output() as calls:
                await self.evidence.backend.search_adapter(
                    query, authority=authority, request_id=ctx.request_id,
                    turn_generation=ctx.turn_generation, min_data_seq=ctx.min_data_seq,
                    deadline_monotonic=deadline, destination=verified, limit=3)
            if len(calls) != 1:
                raise PermissionError("retained native output required")
            retained = True
            issued = calls[0]
            current = lambda: self._claim_current(claim, origin, context["person_id"])
            issued.gate = _RequestFence(issued.gate, current)
            issued.gated = await issued.gate.refresh(issued.gated)
            if not issued.gate.issuance_current(issued.gated.ticket):
                raise PermissionError("source or actor was revoked")
            rendered = render_evidence(issued.render(issued.gated.result), ctx)
            if rendered.get("context_gap"):
                raise PermissionError("fresh accepted source read unavailable")
            self.source_gate.active = (ctx.request_id, context["person_id"], origin, issued, current)
            try:
                yield rendered
            finally:
                self.source_gate.active = None
        finally:
            if calls:
                calls[0].release()
            elif not retained:
                self.evidence.authority.release(authority)

    async def _one(self, claim):
        args = (self.credentials, claim["request_id"],
                claim["generation"], claim["execution_token"])
        async with self._read_claim(claim) as evidence:
            text = answer_from_evidence(evidence)
            row = await asyncio.to_thread(self.control._row, claim["request_id"])
            if row["state"] in ("leased", "result_unsealed"):
                await asyncio.to_thread(self.delivery.prepare_result, *args, text)
            elif row["state"] in ("result_ready", "send_intent_ready"):
                # A recoverable result keeps its ORIGINAL bytes, not a new answer.
                if json.loads(row["result_json"])["text"] != text:
                    raise PermissionError("original result differs after restart")
            else:
                raise PermissionError("request cannot be resumed safely")
            if row["state"] != "send_intent_ready":
                await self.delivery.stage_dataset_result(*args)
                confirmed = await asyncio.to_thread(self.delivery.confirm_dataset_result, *args)
                if confirmed.get("remote_accepted") is not True:
                    return
            await self.delivery.stage_dataset_send(*args)
            confirmed = await asyncio.to_thread(self.delivery.confirm_dataset_send, *args)
            if confirmed.get("remote_accepted") is not True:
                return
            permit = await asyncio.to_thread(self.delivery.claim_send_once, *args)
            # From here onward any exception is UNKNOWN; never post again.
            try:
                proof = await self.sender.post_once(permit)
                await self.delivery.stage_dataset_native_receipt(self.credentials,
                    claim["request_id"], proof)
                await asyncio.to_thread(self.delivery.confirm_dataset_native_receipt,
                    self.credentials, claim["request_id"], proof)
            except BaseException:
                await asyncio.to_thread(self.delivery.uncertain_delivery,
                    self.credentials, claim["request_id"], permit["delivery_id"])
                raise

    async def _drain(self):
        try:
            while self._pending:
                self._pending.clear()
                while True:
                    claim = await asyncio.to_thread(self.control.claim, self.credentials)
                    if claim.get("state") != "leased":
                        return
                    try:
                        await self._one(claim)
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        # No secret, question text, Slack token, actor ID or
                        # source passage in log output.
                        _LOG.warning("rkb_slack_search_reply_pending reason=%s",
                                     type(exc).__name__)
                        return
        finally:
            self._pending.clear()


def compose_expert_slack_search_reply(*, ingress, evidence, credentials, sender):
    """Reuse the existing signed ingress, evidence receiver and Dataset control."""
    gate = _BoundSearchGate()
    delivery = TargetExpertDeliveryControl(ingress.control, source_gate=gate)
    reply = ExpertSlackSearchReply(ingress=ingress, evidence=evidence,
        delivery=delivery, credentials=credentials, sender=sender, search_gate=gate)
    return reply


def configured_expert_slack_search_reply(ingress, evidence, path):
    """Opt-in private operator config. No service provision or grants are minted."""
    from .expert_request_control import ExecutorCredentials
    config = PrivateInput(path, 4096)
    value = json.loads(config.value)
    if (type(value) is not dict
            or set(value) != {"codec", "service_id", "service_token_file", "bot_token_file"}
            or value["codec"] != "rkb.expert-slack-search-reply.v1"
            or not re.fullmatch(r"[A-Za-z0-9._-]{8,160}", value["service_id"])):
        raise ValueError("exact private Slack search reply configuration required")
    service = PrivateInput(value["service_token_file"], 512)
    bot = PrivateInput(value["bot_token_file"], 512)
    credentials = ExecutorCredentials(value["service_id"], service.value.decode("ascii").strip())
    with ingress.control.corpus.connect() as db:
        ingress.control._service(db, credentials)
    sender = SlackChatSender(token=bot.value.decode("ascii").strip(),
        app_id=ingress.control.app_id, team_id=ingress.control.team_id,
        bot_user_id=ingress.control.bot_user_id,
        current=lambda:config.current() and service.current() and bot.current())
    return compose_expert_slack_search_reply(ingress=ingress, evidence=evidence,
        credentials=credentials, sender=sender)
