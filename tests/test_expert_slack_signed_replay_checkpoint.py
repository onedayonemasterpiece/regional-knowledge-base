"""Signed Slack HTTP replay -> existing request/delivery authorities, synthetic I/O only.

These tests deliberately prove no Dataset Saved claim. Journal seal and all
application classes are real; native/provider boundary is synthetic. No Slack
network request, test publication, or live actor impersonation takes place.
"""
import asyncio
import json

import httpx
import pytest
from starlette.applications import Starlette
from starlette.routing import Route

from regional_knowledge.expert_delivery_control import TargetExpertDeliveryControl
from regional_knowledge.expert_request_control import (
    RequestControlError, TargetExpertRequestControl,
)
from regional_knowledge.expert_slack_ingress import ExpertSlackIngress, PATH
from test_control_spool import env
from test_expert_request_control import _event, _prepare
from test_expert_slack_ingress import NOW, SECRET, signed


def _test_app(control):
    ingress = ExpertSlackIngress(control, signing_secret=SECRET, clock=lambda: NOW)
    return Starlette(routes=[Route(PATH, ingress.__call__, methods=["POST"])])


async def _seal(env, command):
    """Real recovery journal; only underlying object store is synthetic."""
    return await env.journal.seal(
        [command], predecessor=await env.journal.head(),
        writer_generation="writer-1",
    )


@pytest.mark.asyncio
async def test_parallel_signed_retries_one_request_one_remote_intent_one_send_unknown(env):
    control, credentials, _ = _prepare(env)
    raw, headers = _event()
    app = _test_app(control)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://signed-test"
    ) as client:
        # Slack can retry while the original received event is not remotely
        # accepted. A real HTTP 200 is forbidden at this point.
        pending = await asyncio.gather(*(
            client.post(PATH, content=raw, headers=headers) for _ in range(3)
        ))
        assert [reply.status_code for reply in pending] == [503, 503, 503]
        assert all(reply.json() == {"error": "remote_intent_pending"} for reply in pending)
        with env.p.corpus.connect() as db:
            rows = db.execute(
                "SELECT id,state,remote_command_hash FROM rkb_expert_requests"
            ).fetchall()
            assert len(rows) == 1
            assert rows[0]["state"] == "received"
            assert rows[0]["remote_command_hash"] is None
            request_id = rows[0]["id"]
        assert control.claim(credentials) == {"state": "empty"}

        intent = control.exact_remote_intent(request_id, 1)
        accepted = await _seal(env, intent)
        verified = await control.verify_remote_intent(request_id, intent, accepted)
        assert verified["remote_accepted"] is True
        assert verified["saved"] is False

        # Lost HTTP acknowledgment replays exactly the same verified event;
        # no second control row and no second journal acceptance.
        replies = await asyncio.gather(*(
            client.post(PATH, content=raw, headers=headers) for _ in range(3)
        ))
        assert [reply.status_code for reply in replies] == [200, 200, 200]
        assert all(reply.json() == {"ok": True} for reply in replies)
        with env.p.corpus.connect() as db:
            assert db.execute("SELECT COUNT(*) FROM rkb_expert_requests").fetchone()[0] == 1

    first = control.claim(credentials, request_id=request_id)
    assert first["state"] == "leased"
    assert control.claim(credentials, request_id=request_id) == {"state": "empty"}
    args = (credentials, request_id, first["generation"], first["execution_token"])

    # Synthetic current-source gate only: not proof of live PG authorization.
    delivery = TargetExpertDeliveryControl(control, source_gate=lambda row, origin: True)
    assert delivery.prepare_result(
        *args, text="Synthetic source-grounded answer. No real Slack send."
    )["saved"] is False
    result_intent = delivery.exact_result_intent(request_id, 2)
    await delivery.verify_remote_result(*args, result_intent, await _seal(env, result_intent))
    send_intent = delivery.exact_send_intent(request_id, 3)
    await delivery.verify_remote_send(*args, send_intent, await _seal(env, send_intent))
    outbound = delivery.claim_send_once(*args)
    assert outbound["delivery_id"] == send_intent.ids["client_msg_id"]
    with pytest.raises(RequestControlError, match="request_fence_denied"):
        delivery.claim_send_once(*args)
    unknown = delivery.uncertain_delivery(credentials, request_id, outbound["delivery_id"])
    assert unknown["state"] == "unknown" and unknown["retry_send_allowed"] is False

    # New instances must inherit the same persistent no-resend fence.
    resumed = TargetExpertRequestControl(
        env.p.registry, env.spool, app_id="A111", team_id="T111",
        bot_user_id="UBOT", allowed_channels=("C111",), clock=lambda: NOW,
    )
    recovered = TargetExpertDeliveryControl(resumed, source_gate=lambda row, origin: True)
    with pytest.raises(RequestControlError, match="request_fence_denied"):
        recovered.claim_send_once(*args)
    with env.p.corpus.connect() as db:
        state, count = db.execute(
            "SELECT state,delivery_attempt FROM rkb_expert_requests WHERE id=?",
            (request_id,),
        ).fetchone()
        assert (state, count) == ("unknown", 1)


@pytest.mark.asyncio
async def test_signed_payload_conflict_after_remote_acceptance_is_denied_without_new_claim(env):
    control, credentials, _ = _prepare(env)
    raw, headers = _event()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_test_app(control)),
        base_url="http://signed-test",
    ) as client:
        assert (await client.post(PATH, content=raw, headers=headers)).status_code == 503
        with env.p.corpus.connect() as db:
            request_id = db.execute("SELECT id FROM rkb_expert_requests").fetchone()["id"]
        command = control.exact_remote_intent(request_id, 1)
        await control.verify_remote_intent(request_id, command, await _seal(env, command))
        assert (await client.post(PATH, content=raw, headers=headers)).status_code == 200
        altered = json.loads(raw)
        altered["event"]["text"] = "Different signed content with the same event_id"
        other_raw, other_headers = signed(altered)
        conflict = await client.post(PATH, content=other_raw, headers=other_headers)
        assert conflict.status_code == 403
        assert "event_id" not in conflict.text
        assert (await client.post(PATH, content=raw, headers=headers)).status_code == 200
        with env.p.corpus.connect() as db:
            records = db.execute(
                "SELECT id,state,remote_command_hash FROM rkb_expert_requests"
            ).fetchall()
            assert len(records) == 1
            assert records[0]["id"] == request_id
            assert records[0]["state"] == "pending"
            assert records[0]["remote_command_hash"] is not None


@pytest.mark.asyncio
async def test_signed_child_event_waits_for_remote_acceptance_and_dialog_head(env):
    control, credentials, _ = _prepare(env)
    parent_raw, parent_headers = _event(1)
    child_raw, child_headers = _event(2, thread="1792000000.100")
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_test_app(control)),
        base_url="http://signed-test",
    ) as client:
        first = await client.post(PATH, content=parent_raw, headers=parent_headers)
        second = await client.post(PATH, content=child_raw, headers=child_headers)
        assert (first.status_code, second.status_code) == (503, 503)
        with env.p.corpus.connect() as db:
            ids = {
                row["event_id"]: row["id"]
                for row in db.execute("SELECT event_id,id FROM rkb_expert_requests")
            }
        assert set(ids) == {"Ev-1", "Ev-2"}
        for sequence, key in enumerate(("Ev-1", "Ev-2"), start=1):
            command = control.exact_remote_intent(ids[key], sequence)
            await control.verify_remote_intent(ids[key], command, await _seal(env, command))
        assert (await client.post(PATH, content=child_raw, headers=child_headers)).status_code == 200
        assert (await client.post(PATH, content=parent_raw, headers=parent_headers)).status_code == 200
        assert control.claim(credentials, request_id=ids["Ev-2"]) == {"state": "empty"}
        first_claim = control.claim(credentials, request_id=ids["Ev-1"])
        assert first_claim["state"] == "leased"
        assert control.claim(credentials, request_id=ids["Ev-2"]) == {"state": "empty"}


@pytest.mark.asyncio
async def test_signed_stale_timestamp_and_tampered_bytes_create_no_request(env):
    control, _, _ = _prepare(env)
    raw, headers = _event()
    stale = dict(headers)
    stale["X-Slack-Request-Timestamp"] = "1791000000"
    tampered = raw.replace(b"Which event", b"Another event")
    assert tampered != raw
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_test_app(control)),
        base_url="http://signed-test",
    ) as client:
        for body, header in ((raw, stale), (tampered, headers)):
            response = await client.post(PATH, content=body, headers=header)
            assert response.status_code == 403
            assert not response.json().get("ok")
    with env.p.corpus.connect() as db:
        assert db.execute("SELECT COUNT(*) FROM rkb_expert_requests").fetchone()[0] == 0
