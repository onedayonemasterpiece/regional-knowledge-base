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
@pytest.mark.parametrize("eta_seconds", [300, None])
async def test_target_warming_pending_posts_one_text_only_reply(service, monkeypatch, eta_seconds):
    control, receiver, reply, request_id, posts = _temporary_reply_harness(service)

    async def target_warming(query, **kwargs):
        assert query == "Which event is supported by this source?"
        assert kwargs["authority"].person_id == service.target.auth.p.other.subject
        # Real backend returns this before any retain_read_output call.
        return EvidenceSearchOutput(evidence=[], status="pending", reason="target_warming",
                                    mode="lexical_degraded", main_state="starting",
                                    startup=StartupStatus(startup_ref="r"*51, status="pending",
                                        reason="target_warming", eta_seconds=eta_seconds,
                                        message_ru=PENDING_STARTUP_MESSAGE_RU))

    monkeypatch.setattr(receiver.backend, "search_adapter", target_warming)
    await reply._answer(request_id)
    saved = _delivery_row(control, request_id)
    assert saved["state"] == "delivered" and saved["delivery_attempt"] == 1
    # The backend owns the exact wording, including a later unknown-ETA correction.
    expected = PENDING_STARTUP_MESSAGE_RU
    if eta_seconds is not None:
        expected += "\nОжидание: примерно 300 с."
    assert len(posts) == 1 and posts[0]["text"] == expected
    assert posts[0]["client_msg_id"] == "Ev-1"
    assert posts[0]["thread_ts"] == "1792000000.100"
    assert control.accept(_observation())["request_id"] == request_id
    await reply._answer(request_id)
    assert len(posts) == 1 and _delivery_row(control, request_id) == saved


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
    assert posts[0]["text"] == "Ошибка поиска. Код: unavailable/deadline_exceeded"
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
    assert posts[0]["text"] == "Ошибка поиска. Код: unavailable"
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
    assert len(posts) == 1 and posts[0]["text"] == "Ошибка поиска. Код: unavailable/not_ready"
    assert "unverified source" not in posts[0]["text"]
    assert gate.refreshes == gate.releases == 1 and gate.checked >= 2


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
async def test_legacy_rebind_resumes_saved_searching_without_new_slack_event(
        service,monkeypatch):
    from mcp.server.mcpserver.exceptions import ToolError
    ctl,receiver,reply,rid,posts=_temporary_reply_harness(service)
    # Preserve the single original ACKed request as it would survive restart.
    assert reply._row(rid,("received",),begin=True) is not None
    sender=_sender(transport=reply.sender.transport)
    replacement_ingress=ExpertSlackIngress(ctl,signing_secret="test-signing-secret",
                                           clock=lambda:1792000001)
    restarted=ExpertSlackSearchReply(ingress=replacement_ingress,evidence=receiver,
                                      sender=sender)
    replacement_ingress.bind_search_reply(restarted)
    async def unavailable(query,**kw):
        raise ToolError('{"status":"unavailable","reason":"deadline_exceeded"}')
    monkeypatch.setattr(receiver.backend,"search_adapter",unavailable)
    # No HTTP POST, no Socket event or manual _answer invocation after restart.
    await restarted.resume_pending()
    assert list(restarted._tasks)==[rid]
    await asyncio.wait_for(restarted._tasks[rid],3)
    await asyncio.sleep(0)
    row=_delivery_row(ctl,rid)
    assert row["state"]=="delivered" and row["delivery_attempt"]==1
    assert len(posts)==1
    assert posts[0]["client_msg_id"]=="Ev-1"
    assert posts[0]["thread_ts"]=="1792000000.100"
    await restarted.resume_pending()
    assert restarted._tasks=={}
    assert len(posts)==1


@pytest.mark.asyncio
async def test_legacy_searching_restores_with_active_dot_after_one_slack_ack(
        service,monkeypatch):
    ctl,receiver,reply,rid,posts=_temporary_reply_harness(service)
    denied=asyncio.Event()
    valid={"bot":False}
    paths=[]
    def provider(request):
        paths.append(request.url.path)
        if request.url.path.endswith("/auth.test"):
            if not valid["bot"]:
                denied.set()
            return httpx.Response(200,json={"ok":True,"team_id":"T111",
                "user_id":"UBOT" if valid["bot"] else "OTHER"})
        body=json.loads(request.content)
        posts.append(body)
        return httpx.Response(200,json={"ok":True,"channel":body["channel"],
            "ts":"1792000008.200","message":{"text":body["text"],
                 "user":"UBOT","thread_ts":body["thread_ts"],
                 "client_msg_id":body["client_msg_id"]}})
    reply.sender.transport=httpx.MockTransport(provider)
    reply.ingress.bind_search_reply(reply)
    app=Starlette(routes=[Route(PATH,reply.ingress.__call__,methods=["POST"])])
    raw,headers=_event()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                 base_url="http://test") as http:
        accepted=await http.post(PATH,content=raw,headers=headers)
    assert accepted.status_code==200 and accepted.json()=={"ok":True}
    await asyncio.wait_for(denied.wait(),4)
    await asyncio.sleep(0)
    first=_delivery_row(ctl,rid)
    assert first["state"]=="searching" and first["delivery_attempt"]==0
    assert paths==["/api/auth.test"] and posts==[]
    # Simulate process exit with original persisted row but without its task.
    task=reply._tasks[rid]
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    valid["bot"]=True
    ingress=ExpertSlackIngress(ctl,signing_secret="test-signing-secret",
                               clock=lambda:1792000001)
    restarted=ExpertSlackSearchReply(ingress=ingress,evidence=receiver,
                                      sender=reply.sender)
    ingress.bind_search_reply(restarted)
    dot_scans=[]
    class ActiveDot:
        def _subscription(self):return {"state":"active"}
        async def resume_pending(self):dot_scans.append("dot original rows")
        def notify(self,request_id):
            pytest.fail("original searching legacy row must not enter dot wake")
    restarted._dot_events=ActiveDot()
    await restarted.resume_pending()
    assert dot_scans==["dot original rows"] and rid in restarted._tasks
    await asyncio.wait_for(restarted._tasks[rid],8)
    await asyncio.sleep(0)
    sent=_delivery_row(ctl,rid)
    assert sent["state"]=="delivered" and sent["delivery_attempt"]==1
    assert len(posts)==1 and posts[0]["client_msg_id"]=="Ev-1"
    assert posts[0]["thread_ts"]=="1792000000.100"
    assert paths==["/api/auth.test","/api/auth.test","/api/chat.postMessage"]
    await restarted.resume_pending()
    assert dot_scans==["dot original rows","dot original rows"]
    assert restarted._tasks=={} and len(posts)==1


@pytest.mark.asyncio
async def test_transient_post_attempt_sqlite_state_read_keeps_original_ack_task(
        service,monkeypatch):
    import sqlite3
    ctl,receiver,reply,rid,posts=_temporary_reply_harness(service)
    reply.ingress.bind_search_reply(reply)
    checked=asyncio.Event()
    loop=asyncio.get_running_loop()
    allowed={"bot":False,"fail_read":True}
    paths=[]
    original_row=ctl._row
    def transient_row(request_id):
        if not allowed["bot"] and allowed["fail_read"]:
            allowed["fail_read"]=False
            loop.call_soon_threadsafe(checked.set)
            raise sqlite3.OperationalError("database is locked")
        return original_row(request_id)
    monkeypatch.setattr(ctl,"_row",transient_row)
    def provider(request):
        paths.append(request.url.path)
        if request.url.path.endswith("/auth.test"):
            return httpx.Response(200,json={"ok":True,"team_id":"T111",
                "user_id":"UBOT" if allowed["bot"] else "OTHER"})
        body=json.loads(request.content)
        posts.append(body)
        return httpx.Response(200,json={"ok":True,"channel":body["channel"],
            "ts":"1792000008.200","message":{"text":body["text"],
                 "user":"UBOT","thread_ts":body["thread_ts"],
                 "client_msg_id":body["client_msg_id"]}})
    reply.sender.transport=httpx.MockTransport(provider)
    app=Starlette(routes=[Route(PATH,reply.ingress.__call__,methods=["POST"])])
    raw,headers=_event()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                 base_url="http://test") as http:
        accepted=await http.post(PATH,content=raw,headers=headers)
    assert accepted.status_code==200 and accepted.json()=={"ok":True}
    await asyncio.wait_for(checked.wait(),4)
    assert _delivery_row(ctl,rid)["state"]=="searching" and posts==[]
    allowed["bot"]=True
    await asyncio.wait_for(reply._tasks[rid],8)
    await asyncio.sleep(0)
    sent=_delivery_row(ctl,rid)
    assert sent["state"]=="delivered" and sent["delivery_attempt"]==1
    assert allowed["fail_read"] is False
    assert paths==["/api/auth.test","/api/auth.test","/api/chat.postMessage"]
    assert len(posts)==1 and posts[0]["client_msg_id"]=="Ev-1"
    await reply.resume_pending()
    assert len(posts)==1 and reply._tasks=={}


@pytest.mark.asyncio
async def test_restart_65_acknowledged_legacy_questions_paces_native_first_reads(
        service,monkeypatch):
    from mcp.server.mcpserver.exceptions import ToolError
    ctl,receiver,reply,first,posts=_temporary_reply_harness(service)
    requests=[first]
    for n in range(2,66):
        entry=ctl.accept(_observation(n=n,thread=f"1792001000.{n:03d}"))
        requests.append(entry["request_id"])
    assert len(requests)==len(set(requests))==65
    active=0
    peak=0
    seen=[]
    async def native_search(query,**kwargs):
        nonlocal active,peak
        active+=1
        peak=max(peak,active)
        seen.append(kwargs["request_id"])
        try:
            # If startup launches all 65 at once, the existing two-second
            # native deadline would yield false terminal source-only errors.
            await asyncio.sleep(.003)
            if active>1:
                raise ToolError('{"status":"unavailable","reason":"deadline_exceeded"}')
            return EvidenceSearchOutput(evidence=[],status="pending",
                reason="target_warming",mode="lexical_degraded",
                main_state="starting",startup=StartupStatus(
                    startup_ref="r"*51,status="pending",reason="target_warming",
                    eta_seconds=300,message_ru=PENDING_STARTUP_MESSAGE_RU))
        finally:
            active-=1
    monkeypatch.setattr(receiver.backend,"search_adapter",native_search)
    await reply.resume_pending()
    assert len(reply._tasks)==65  # All accepted work retained without cap16.
    tasks=tuple(reply._tasks.values())
    await asyncio.wait_for(asyncio.gather(*tasks),45)
    await asyncio.sleep(0)
    assert len(seen)==65 and set(seen)==set(requests)
    assert peak==1 and active==0
    assert len(posts)==65
    assert {item["client_msg_id"] for item in posts}=={
        f"Ev-{n}" for n in range(1,66)}
    assert all(item["text"].startswith(PENDING_STARTUP_MESSAGE_RU)
               and "deadline_exceeded" not in item["text"] for item in posts)
    assert all(_delivery_row(ctl,rid)["state"]=="delivered" for rid in requests)
    assert reply._tasks=={}
    await reply.resume_pending()
    assert not reply._tasks and len(posts)==65


@pytest.mark.asyncio
async def test_one_signed_legacy_ack_with_uncertain_post_never_retries_itself(
        service,monkeypatch):
    from mcp.server.mcpserver.exceptions import ToolError
    ctl,receiver,reply,rid,posts=_temporary_reply_harness(service,lost_ack=True)
    reply.ingress.bind_search_reply(reply)
    async def temporary(query,**kw):
        raise ToolError('{"status":"unavailable","reason":"target_warming"}')
    monkeypatch.setattr(receiver.backend,"search_adapter",temporary)
    app=Starlette(routes=[Route(PATH,reply.ingress.__call__,methods=["POST"])])
    raw,head=_event()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                 base_url="http://test") as http:
        first=await http.post(PATH,content=raw,headers=head)
        assert first.status_code==200 and first.json()=={"ok":True}
    await asyncio.wait_for(reply._tasks[rid],3)
    await asyncio.sleep(0)
    saved=_delivery_row(ctl,rid)
    assert saved["state"]=="unknown" and saved["delivery_attempt"]==1
    assert len(posts)==1 and posts[0]["client_msg_id"]=="Ev-1"
    assert reply._tasks=={}
    await reply.resume_pending()
    assert reply._tasks=={} and len(posts)==1
    assert _delivery_row(ctl,rid)==saved


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
    assert posts[0]["text"] == "Ошибка поиска. Код: unavailable/target_warming"

    assert control.accept(_observation())["request_id"] == request_id
    await reply._answer(request_id)
    assert len(posts) == 1 and _delivery_row(control, request_id) == saved
