"""Read-only Slack bot composition: signed event, original actor, one provider post.

All events/Slack responses are synthetic; no provider, PG, deployment or grants.
"""
import asyncio
import json
from types import SimpleNamespace

import httpx
import pytest
from starlette.applications import Starlette
from starlette.routing import Route

from regional_knowledge.expert_slack_ingress import ExpertSlackIngress, PATH
from regional_knowledge.contracts import EvidenceSearchOutput, StartupStatus
from regional_knowledge.kaggle_public_backend import PENDING_STARTUP_MESSAGE_RU
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


def test_sourced_reply_fallback_is_one_excerpt_per_book_not_three_source_cards():
    evidence=_evidence([{
        "title":"Гаузе", "text":"Возникла крепость. Второе предложение <@U111>.",
        "metadata":{"citation":"Кёнигсберг, том I <@U111>", "locator":"стр. 31"}}])
    sourced=answer_from_evidence(evidence)
    assert sourced.startswith("Найдено в источниках:\n1. Гаузе")
    assert "стр. 31" in sourced and "Возникла крепость." in sourced
    assert "Второе предложение" not in sourced and "не удалось" not in sourced.lower()
    assert "<@U111>" not in sourced and "&lt;@U111&gt;" in sourced
    assert "Цитата (фрагмент)" not in sourced
    no_page=answer_from_evidence(_evidence([{
        "title":"Wiechert","text":"Ein Satz. Weiterer Satz","metadata":{"locator":"opaque-id"}}]))
    assert "Найдено в источниках:\n1. Wiechert — Ein Satz." in no_page
    assert "opaque-id" not in no_page
    german=answer_from_evidence(_evidence([]), "Wo lag die Königsberger Synagoge?")
    assert german.startswith("In den verfügbaren Quellen")
    assert "Русский перевод:" in german
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


def _temporary_reply_harness(s, *, lost_ack=False):
    """Owner-approved synthetic non-owner route; no real Slack calls or data."""
    control, _, _, ingress = _owned_ingress(s)
    receiver = compose_expert_evidence_receiver(
        target_startup=s.startup, expert_slack_ingress=ingress)
    posts = []

    def transport(request):
        if request.url.path.endswith("/auth.test"):
            return httpx.Response(200, json={
                "ok": True, "team_id": "T111", "user_id": "UBOT"})
        assert request.url.path.endswith("/chat.postMessage")
        body = json.loads(request.content)
        posts.append(body)
        if lost_ack:
            raise httpx.ReadTimeout("synthetic lost ACK", request=request)
        return httpx.Response(200, json={
            "ok": True, "channel": body["channel"], "ts": "1792000008.200",
            "message": {"text": body["text"], "user": "UBOT",
                        "thread_ts": body["thread_ts"],
                        "client_msg_id": body["client_msg_id"]}})

    reply = ExpertSlackSearchReply(
        ingress=ingress, evidence=receiver,
        sender=_sender(transport=httpx.MockTransport(transport)))
    request_id = control.accept(_observation())["request_id"]
    return control, receiver, reply, request_id, posts


def _delivery_row(control, request_id):
    with control.corpus.connect() as db:
        return dict(db.execute(
            "SELECT state,delivery_attempt,delivery_id,delivery_receipt "
            "FROM rkb_expert_requests WHERE id=?", (request_id,)).fetchone())


@pytest.mark.asyncio
async def test_three_guarded_passages_post_one_answer_with_sources_in_original_thread(
        service, monkeypatch):
    from regional_knowledge.read_issuance import retain_read_output

    control, receiver, reply, request_id, posts = _temporary_reply_harness(service)

    class CurrentGate:
        async def refresh(self, gated):
            return gated

        def issuance_current(self, ticket):
            return True

        def release(self, ticket):
            pass

    result = {
        "status": "ok", "mode": "hybrid", "reason": None,
        "prior_write_pending": False,
        "observed_data_seq": 1, "observed_projection_seq": 1,
        "evidence": [
            {"title": "Гаузе", "text": "Текст первого фрагмента.",
             "metadata": {"citation": "Кёнигсберг. Том I. 1996", "locator": "стр. 31"}},
            {"title": "Гаузе", "text": "Текст второго фрагмента.",
             "metadata": {"citation": "Кёнигсберг. Том I. 1996", "locator": "стр. 32"}},
            {"title": "11111111-1111-1111-1111-111111111111",
             "text": "Нерелевантный фрагмент.",
             "metadata": {"locator": "opaque-id"}},
        ],
    }

    async def search(query, **kwargs):
        assert retain_read_output(CurrentGate(),
            SimpleNamespace(ticket=object(), result=result), lambda value: value)
        return None

    monkeypatch.setattr(receiver.backend, "search_adapter", search)
    await reply._answer(request_id)
    await reply._answer(request_id)
    assert len(posts) == 1
    sent = posts[0]
    assert sent["thread_ts"] == "1792000000.100" and sent["client_msg_id"] == "Ev-1"
    assert sent["text"].count("Найдено в источниках:") == 1
    assert "Гаузе" in sent["text"] and "стр. 31" in sent["text"]
    assert "Цитата (фрагмент)" not in sent["text"]
    assert "11111111-1111-1111-1111-111111111111" not in sent["text"]
    assert "Текст первого фрагмента." in sent["text"]
    assert "Текст второго фрагмента" not in sent["text"]
    assert "не удалось" not in sent["text"].lower()
    assert _delivery_row(control, request_id)["state"] == "delivered"


@pytest.mark.asyncio
@pytest.mark.parametrize("query",["", "я"*1001, "*Sent using* <@U123>"])
async def test_invalid_query_completes_once_without_search_and_unblocks_dialog(service,monkeypatch,query):
    control,receiver,reply,request_id,posts=_temporary_reply_harness(service)
    with control.corpus.connect() as db:
        db.execute("UPDATE rkb_expert_requests SET input_text=? WHERE id=?",("<@UBOT> "+query,request_id))
    async def forbidden(*args,**kwargs):
        pytest.fail("Invalid question must not execute a search")
    monkeypatch.setattr(receiver.backend,"search_adapter",forbidden)
    await reply._answer(request_id)
    saved=_delivery_row(control,request_id)
    assert saved["state"]=="delivered" and saved["delivery_attempt"]==1
    assert len(posts)==1 and "от 1 до 1000 символов" in posts[0]["text"]
    await reply._answer(request_id)
    assert len(posts)==1 and _delivery_row(control,request_id)==saved
    control.clock=lambda:1792000002.0
    second=control.accept(_observation(n=2))["request_id"]
    assert reply._row(second,("received",),begin=True) is not None


@pytest.mark.asyncio
@pytest.mark.parametrize(("message", "expected"), [
    ("Gentzen *Sent using* <@U123>", "Gentzen"),
    ("Gentzen\n\n*Sent using* <@U123|ChatGPT>", "Gentzen"),
    ("Что означает Sent using?", "Что означает Sent using?"),
    ("*Sent using* <@U123> — часть цитаты", "*Sent using* <@U123> — часть цитаты"),
])
async def test_connector_attribution_is_not_searched_or_removed_from_accepted_input(
        service, monkeypatch, message, expected):
    control, receiver, reply, request_id, posts = _temporary_reply_harness(service)
    original = "<@UBOT> " + message
    with control.corpus.connect() as db:
        db.execute("UPDATE rkb_expert_requests SET input_text=? WHERE id=?", (original, request_id))
    searched = []

    async def search(query, **kwargs):
        searched.append(query)
        from mcp.server.mcpserver.exceptions import ToolError
        raise ToolError('{"status":"pending","reason":"target_warming"}')

    monkeypatch.setattr(receiver.backend, "search_adapter", search)
    await reply._answer(request_id)
    assert searched == [expected]
    assert posts == [] and _delivery_row(control, request_id)["state"] == "searching"
    with control.corpus.connect() as db:
        assert db.execute("SELECT input_text FROM rkb_expert_requests WHERE id=?",
                          (request_id,)).fetchone()[0] == original


@pytest.mark.asyncio
@pytest.mark.parametrize("eta_seconds", [240, None])
async def test_target_warming_original_request_delivers_sourced_answer_after_ready(
        service, monkeypatch, eta_seconds):
    from regional_knowledge.read_issuance import retain_read_output

    control, receiver, reply, request_id, posts = _temporary_reply_harness(service)
    queries = []

    async def target_warming(query, **kwargs):
        queries.append(query)
        assert kwargs["authority"].person_id == service.target.auth.p.other.subject
        return EvidenceSearchOutput(evidence=[], status="pending", reason="target_warming",
                                    mode="lexical_degraded", main_state="starting",
                                    startup=StartupStatus(startup_ref="r"*51, status="pending",
                                        reason="target_warming", eta_seconds=eta_seconds,
                                        message_ru=PENDING_STARTUP_MESSAGE_RU))

    monkeypatch.setattr(receiver.backend, "search_adapter", target_warming)
    await reply._answer(request_id)
    assert _delivery_row(control, request_id)["state"] == "searching"
    assert _delivery_row(control, request_id)["delivery_attempt"] == 0
    assert _delivery_row(control, request_id)["delivery_id"] == "Ev-1:warming"
    assert len(posts) == 1
    assert posts[0]["text"] == (
        "Запускаю базу, ответ будет примерно через 4 мин." if eta_seconds
        else "Запускаю базу, отвечу после запуска.")
    assert posts[0]["client_msg_id"] == "Ev-1:warming"
    assert posts[0]["thread_ts"] == "1792000000.100"

    # A second warming poll (including after task restart) does not post a duplicate.
    await reply._answer(request_id)
    assert len(posts) == 1 and _delivery_row(control, request_id)["delivery_attempt"] == 0

    class SourceGate:
        async def refresh(self, gated):
            return gated

        def issuance_current(self, ticket):
            return True

        def release(self, ticket):
            pass

    async def ready(query, **kwargs):
        queries.append(query)
        evidence = {"status": "ok", "mode": "hybrid", "context_gap": False,
                    "observed_data_seq": 1, "observed_projection_seq": 1,
                    "prior_write_pending": False, "evidence": [{
                        "title": "Gause", "text": "Confirmed synthetic source.",
                        "metadata": {"citation": "Gause 1994", "locator": "стр. 59"}}]}
        assert retain_read_output(SourceGate(), SimpleNamespace(ticket=object(), result=evidence),
                                  lambda value: value)
        return None

    monkeypatch.setattr(receiver.backend, "search_adapter", ready)
    reply.notify(request_id)
    await asyncio.wait_for(reply._tasks[request_id], 6)
    assert queries == ["Which event is supported by this source?"] * 3
    assert len(posts) == 2 and "Confirmed synthetic source." in posts[1]["text"]
    assert posts[1]["client_msg_id"] == "Ev-1"
    assert posts[1]["thread_ts"] == "1792000000.100"
    assert _delivery_row(control, request_id)["state"] == "delivered"
    assert _delivery_row(control, request_id)["delivery_attempt"] == 1
    assert _delivery_row(control, request_id)["delivery_id"] == "Ev-1"
    assert control.accept(_observation())["request_id"] == request_id
    await reply._answer(request_id)
    assert len(posts) == 2

@pytest.mark.asyncio
@pytest.mark.parametrize(("status", "reason"), [("ok", None), ("denied", "access_denied")])
async def test_guardless_nonstartup_read_never_posts(service, monkeypatch, status, reason):
    control, receiver, reply, request_id, posts = _temporary_reply_harness(service)

    async def no_issued_evidence(query, **kwargs):
        return EvidenceSearchOutput(evidence=[], status=status, reason=reason,
                                    mode="lexical_degraded")

    monkeypatch.setattr(receiver.backend, "search_adapter", no_issued_evidence)
    with pytest.raises(PermissionError, match="retained source gate required"):
        await reply._answer(request_id)
    assert posts == []
    assert _delivery_row(control, request_id)["delivery_attempt"] == 0


@pytest.mark.asyncio
async def test_transient_preclaim_failure_sends_error_code_via_existing_permit(service, monkeypatch):
    from mcp.server.mcpserver.exceptions import ToolError

    control, receiver, reply, request_id, posts = _temporary_reply_harness(service)

    async def transient(query, **kwargs):
        assert query == "Which event is supported by this source?"
        assert kwargs["authority"].person_id == service.target.auth.p.other.subject
        raise ToolError('{"status":"unavailable","reason":"deadline_exceeded"}')

    monkeypatch.setattr(receiver.backend, "search_adapter", transient)
    await reply._answer(request_id)
    saved = _delivery_row(control, request_id)
    assert saved["state"] == "delivered" and saved["delivery_attempt"] == 1
    assert saved["delivery_id"] == "Ev-1" and saved["delivery_receipt"] is not None
    assert len(posts) == 1
    assert posts[0]["text"] == "Не удалось проверить базу сейчас. Код: unavailable/deadline_exceeded"
    assert posts[0]["client_msg_id"] == "Ev-1"
    assert posts[0]["thread_ts"] == "1792000000.100"

    # A duplicate signed Slack event and any repeated notify are readback-only.
    assert control.accept(_observation())["request_id"] == request_id
    await reply._answer(request_id)
    assert len(posts) == 1 and _delivery_row(control, request_id) == saved


@pytest.mark.asyncio
async def test_error_code_never_reflects_untrusted_exception_detail(service, monkeypatch):
    from mcp.server.mcpserver.exceptions import ToolError

    control, receiver, reply, request_id, posts = _temporary_reply_harness(service)

    async def transient(query, **kwargs):
        raise ToolError(json.dumps({"status": "unavailable", "reason": "secret\\nunsafe",
                                    "private_detail": "not for Slack"}))

    monkeypatch.setattr(receiver.backend, "search_adapter", transient)
    await reply._answer(request_id)
    assert _delivery_row(control, request_id)["state"] == "delivered"
    assert len(posts) == 1
    assert posts[0]["text"] == "Не удалось проверить базу сейчас. Код: unavailable"
    assert "secret" not in posts[0]["text"]


@pytest.mark.asyncio
@pytest.mark.parametrize("veto", ("explicit_denied", "revoked_grant", "source_issuance"))
async def test_preclaim_auth_or_source_veto_never_posts_generic(service, monkeypatch, veto):
    from mcp.server.mcpserver.exceptions import ToolError
    from regional_knowledge.participant_contracts import ParticipantGrant
    from regional_knowledge.participants import ParticipantError
    from regional_knowledge.read_issuance import retain_read_output

    control, receiver, reply, request_id, posts = _temporary_reply_harness(service)

    class RefusingSourceGate:
        def __init__(self):
            self.releases = 0

        async def refresh(self, gated):
            return gated

        def issuance_current(self, ticket):
            return False

        def release(self, ticket):
            self.releases += 1

    gate = RefusingSourceGate()

    async def refused(query, **kwargs):
        if veto == "explicit_denied":
            raise ToolError('{"status":"denied","reason":"access_denied"}')
        if veto == "revoked_grant":
            change(service.target.auth.p, ParticipantGrant(
                capability="public_search", granted=False,
                reason="Synthetic non-owner grant revoked before post"))
            raise ToolError('{"status":"unavailable","reason":"deadline_exceeded"}')
        gated = SimpleNamespace(ticket=object(), result={"status": "ok"})
        assert retain_read_output(gate, gated, lambda result: result)
        return None

    monkeypatch.setattr(receiver.backend, "search_adapter", refused)
    with pytest.raises((ToolError, PermissionError, ParticipantError)):
        await reply._answer(request_id)
    row = _delivery_row(control, request_id)
    assert row["state"] == "searching" and row["delivery_attempt"] == 0
    assert row["delivery_id"] is None and row["delivery_receipt"] is None
    assert posts == []
    if veto == "source_issuance":
        assert gate.releases == 1


@pytest.mark.asyncio
async def test_guarded_context_gap_returns_error_code_without_exposing_source(service, monkeypatch):
    from regional_knowledge.read_issuance import retain_read_output

    control, receiver, reply, request_id, posts = _temporary_reply_harness(service)

    class CurrentGate:
        def __init__(self):
            self.refreshes = self.checked = self.releases = 0

        async def refresh(self, gated):
            self.refreshes += 1
            return gated

        def issuance_current(self, ticket):
            self.checked += 1
            return True

        def release(self, ticket):
            self.releases += 1

    gate = CurrentGate()
    result = {
        "status": "unavailable", "reason": "not_ready",
        "mode": "lexical_degraded", "prior_write_pending": False,
        "evidence": [{"text": "unverified source, never render"}],
        "observed_data_seq": 5, "observed_projection_seq": 5,
    }

    async def incomplete(query, **kwargs):
        assert retain_read_output(gate, SimpleNamespace(ticket=object(), result=result),
                                  lambda value: value)
        return None

    monkeypatch.setattr(receiver.backend, "search_adapter", incomplete)
    await reply._answer(request_id)
    assert _delivery_row(control, request_id)["state"] == "delivered"
    assert len(posts) == 1 and posts[0]["text"] == "Не удалось проверить базу сейчас. Код: unavailable/not_ready"
    assert "unverified source" not in posts[0]["text"]
    assert gate.refreshes == gate.releases == 1 and gate.checked >= 2


@pytest.mark.asyncio
async def test_generic_post_unknown_never_resends_after_provider_lost_ack(service, monkeypatch):
    from mcp.server.mcpserver.exceptions import ToolError

    control, receiver, reply, request_id, posts = _temporary_reply_harness(
        service, lost_ack=True)

    async def transient(query, **kwargs):
        raise ToolError('{"status":"unavailable","reason":"target_warming"}')

    monkeypatch.setattr(receiver.backend, "search_adapter", transient)
    with pytest.raises(httpx.ReadTimeout):
        await reply._answer(request_id)
    saved = _delivery_row(control, request_id)
    assert saved["state"] == "unknown" and saved["delivery_attempt"] == 1
    assert saved["delivery_id"] == "Ev-1" and saved["delivery_receipt"] is None
    assert len(posts) == 1
    assert posts[0]["text"] == "Не удалось проверить базу сейчас. Код: unavailable/target_warming"

    assert control.accept(_observation())["request_id"] == request_id
    await reply._answer(request_id)
    assert len(posts) == 1 and _delivery_row(control, request_id) == saved


@pytest.mark.asyncio
@pytest.mark.parametrize("denial",("wrong_bot","token_revoked","source_revoked"))
async def test_legacy_post_claim_follows_auth_and_current_issuance_without_false_unknown(
        service,monkeypatch,denial):
    from mcp.server.mcpserver.exceptions import ToolError
    from regional_knowledge.read_issuance import retain_read_output

    ctl,receiver,reply,rid,posts=_temporary_reply_harness(service)
    paths=[]
    allowed={"token":True,"source":True,"deny":True}
    first_auth=asyncio.Event()
    reply.sender.current=lambda:allowed["token"]

    class IssuanceGate:
        async def refresh(self,gated):
            return gated
        def issuance_current(self,ticket):
            return allowed["source"]
        def release(self,ticket):
            pass
    gate=IssuanceGate()

    async def source_search(query,**kwargs):
        # A real captured source ticket is checked again after auth.test.
        if denial!="source_revoked":
            raise ToolError('{"status":"unavailable","reason":"deadline_exceeded"}')
        from regional_knowledge.read_issuance import retain_read_output
        value={"status":"ok","mode":"hybrid","context_gap":False,
               "observed_data_seq":1,"observed_projection_seq":1,
               "prior_write_pending":False,
               "evidence":[{"text":"Confirmed synthetic source.","title":"Gause",
                            "metadata":{"citation":"Gause 1994","locator":"стр. 59"}}]}
        assert retain_read_output(gate,SimpleNamespace(ticket=object(),result=value),
                                  lambda result:result)
        return None
    monkeypatch.setattr(receiver.backend,"search_adapter",source_search)

    def provider(request):
        paths.append(request.url.path)
        if request.url.path.endswith("/auth.test"):
            if allowed["deny"]:
                first_auth.set()
                if denial=="token_revoked":
                    allowed["token"]=False
                elif denial=="source_revoked":
                    allowed["source"]=False
                user="OTHER" if denial=="wrong_bot" else "UBOT"
            else:
                user="UBOT"
            return httpx.Response(200,json={"ok":True,"team_id":"T111",
                                            "user_id":user})
        body=json.loads(request.content)
        posts.append(body)
        return httpx.Response(200,json={"ok":True,"channel":body["channel"],
            "ts":"1792000008.200","message":{"text":body["text"],"user":"UBOT",
                "thread_ts":body["thread_ts"],"client_msg_id":body["client_msg_id"]}})
    reply.sender.transport=httpx.MockTransport(provider)

    # A SINGLE original Slack event and 200 ACK must carry the request
    # through a transient sender refusal without a second event/caller.
    reply.ingress.bind_search_reply(reply)
    app=Starlette(routes=[Route(PATH,reply.ingress.__call__,methods=["POST"])])
    raw,head=_event()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                 base_url="http://test") as http:
        accepted=await http.post(PATH,content=raw,headers=head)
    assert accepted.status_code==200 and accepted.json()=={"ok":True}
    await asyncio.wait_for(first_auth.wait(),3)
    await asyncio.sleep(0.1)  # First failed sender already returned to task.
    saved=_delivery_row(ctl,rid)
    assert saved["state"]=="searching" and saved["delivery_attempt"]==0
    assert saved["delivery_id"] is None and saved["delivery_receipt"] is None
    assert posts==[] and paths==["/api/auth.test"]

    # Restore the source/token; the original task performs the next attempt.
    allowed.update(token=True,source=True,deny=False)
    await asyncio.wait_for(reply._tasks[rid],6)
    await asyncio.sleep(0)  # Existing task completion callback.
    delivered=_delivery_row(ctl,rid)
    assert delivered["state"]=="delivered" and delivered["delivery_attempt"]==1
    assert len(posts)==1 and posts[0]["client_msg_id"]=="Ev-1"
    assert posts[0]["thread_ts"]=="1792000000.100"
    assert paths==["/api/auth.test","/api/auth.test","/api/chat.postMessage"]
    assert reply._tasks=={}
    # No second ACK/request and no additional provider message.
    assert _delivery_row(ctl,rid)==delivered


@pytest.mark.asyncio
async def test_generic_post_unknown_never_resends_after_provider_lost_ack(service, monkeypatch):
    from mcp.server.mcpserver.exceptions import ToolError

    control, receiver, reply, request_id, posts = _temporary_reply_harness(
        service, lost_ack=True)

    async def transient(query, **kwargs):
        raise ToolError('{"status":"unavailable","reason":"target_warming"}')

    monkeypatch.setattr(receiver.backend, "search_adapter", transient)
    with pytest.raises(httpx.ReadTimeout):
        await reply._answer(request_id)
    saved = _delivery_row(control, request_id)
    assert saved["state"] == "unknown" and saved["delivery_attempt"] == 1
    assert saved["delivery_id"] == "Ev-1" and saved["delivery_receipt"] is None
    assert len(posts) == 1
    assert posts[0]["text"] == "Не удалось проверить базу сейчас. Код: unavailable/target_warming"

    assert control.accept(_observation())["request_id"] == request_id
    await reply._answer(request_id)
    assert len(posts) == 1 and _delivery_row(control, request_id) == saved


@pytest.mark.asyncio
@pytest.mark.parametrize("observation",["found","absent","partial","denied","failed_http"])
async def test_restart_recovers_head_without_repeating_search_or_unknown_post(
        service,monkeypatch,observation):
    from mcp.server.mcpserver.exceptions import ToolError
    ctl,receiver,reply,first,posts=_temporary_reply_harness(service,lost_ack=True)
    searches=[]
    async def transient(query,**kwargs):
        searches.append(query)
        raise ToolError('{"status":"unavailable","reason":"target_warming"}')
    monkeypatch.setattr(receiver.backend,"search_adapter",transient)
    # First provider POST may have succeeded but its ACK was lost.
    with pytest.raises(httpx.ReadTimeout):
        await reply._answer(first)
    first_saved=_delivery_row(ctl,first)
    assert first_saved["state"]=="unknown"
    assert first_saved["delivery_attempt"]==1
    assert [x["client_msg_id"] for x in posts]==["Ev-1"]

    ctl.clock=lambda:1792000002.0
    second=ctl.accept(_observation(n=2))["request_id"]
    # Both questions remain in the same original Slack thread.
    assert ctl._row(first)["dialog_id"]==ctl._row(second)["dialog_id"]
    searches.clear()
    await reply._answer(second)
    await reply._answer(second)
    assert searches==[]
    assert _delivery_row(ctl,second)["state"]=="received"
    assert _delivery_row(ctl,second)["delivery_attempt"]==0
    assert _delivery_row(ctl,first)==first_saved
    assert [x["client_msg_id"] for x in posts]==["Ev-1"]
    reads=[];lookup=asyncio.Event()
    def provider(request):
        if request.url.path.endswith("auth.test"):
            lookup.set()
            return httpx.Response(200,json={"ok":True,"team_id":"T111",
                "user_id":"OTHER" if observation=="denied" else "UBOT"})
        if request.url.path.endswith("conversations.replies"):
            cursor=request.url.params["cursor"];reads.append(cursor);lookup.set()
            assert request.url.params["include_all_metadata"]=="true"
            if observation=="failed_http":return httpx.Response(503,json={"ok":True,"messages":[],"has_more":False})
            if not cursor:
                return httpx.Response(200,json={"ok":True,"messages":[{
                    "user":"OTHER","client_msg_id":"Ev-1","thread_ts":"1792000000.100"}],
                    "has_more":True,"response_metadata":{} if observation=="partial"
                    else {"next_cursor":"page2"}})
            return httpx.Response(200,json={"ok":True,"messages":[{
                "user":"UBOT","metadata":{"event_type":"rkb_slack_reply","event_payload":{"event_id":"Ev-1"}},"thread_ts":"1792000000.100",
                "ts":"1792000001.100"}] if observation=="found" else [],"has_more":False})
        data=json.loads(request.content);posts.append(data)
        assert data["client_msg_id"]=="Ev-2"  # never resend unknown Ev-1
        assert data["metadata"]=={"event_type":"rkb_slack_reply","event_payload":{"event_id":"Ev-2"}}
        return httpx.Response(200,json={"ok":True,"channel":data["channel"],"ts":"1792000003.100",
            "message":{"user":"UBOT","thread_ts":data["thread_ts"],"client_msg_id":"Ev-2","text":data["text"]}})
    # New sender/worker, same committed control rows: no original in-memory task.
    restarted=ExpertSlackSearchReply(ingress=reply.ingress,evidence=receiver,
        sender=_sender(transport=httpx.MockTransport(provider)))
    restarted.resume_pending()
    try:
        await asyncio.wait_for(lookup.wait(),1)
        if observation in ("partial","denied","failed_http"):
            await asyncio.sleep(.03)
            assert _delivery_row(ctl,first)==first_saved
            assert _delivery_row(ctl,second)["state"]=="received"
            assert searches==[] and len(posts)==1
        else:
            async with asyncio.timeout(2):
                while _delivery_row(ctl,second)["state"]!="delivered":await asyncio.sleep(.01)
            assert reads==["","page2"]
            assert _delivery_row(ctl,first)["state"]==("delivered" if observation=="found" else "abandoned_no_resend")
            assert _delivery_row(ctl,first)["delivery_attempt"]==1
            assert len(searches)==1
            assert [post["client_msg_id"] for post in posts]==["Ev-1","Ev-2"]
    finally:
        tasks=list(restarted._tasks.values())
        for task in tasks:task.cancel()
        await asyncio.gather(*tasks,return_exceptions=True)
