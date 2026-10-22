"""Negative WSS/queue regressions for M7 dispatch fastpath stacked on PR239.

Only fake clock, existing TestClient, actual BgeQueue/SharedReadAdmission,
ReadAuthority and source-bound channel_route are used. No Kaggle/BGE/PG
or external provider operations, no product implementation monkeypatch.
"""
from __future__ import annotations

import threading
import time

import pytest
from starlette.applications import Starlette
from starlette.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from regional_knowledge.bge_contract import REVISION, SPACE
from regional_knowledge.bge_queue import MAX_LIFETIME, RevokedRun, StaleAttachment
from regional_knowledge.kaggle_protocol import Query, TrustedActorContext
from regional_knowledge.participant_contracts import ParticipantGrant
from regional_knowledge.read_authority import ReadAuthorityError, TrustedReadDestination
from test_bge_dispatch_status_fastpath_linza import candidate_route
from test_bge_runtime_readiness import run, shared_channel_run, observation
from test_read_authority import auth
from test_participants import people, change


def _attach(socket, run_id, token, nonce):
    socket.send_json(dict(type="ATTACH", protocol_version=2, run_id=run_id,
        launch_nonce=nonce, runtime_incarnation="incarnation-a"))
    hello = socket.receive_json()
    assert hello["type"] == "WELCOME"
    return hello["connection_generation"]


def _send(socket, generation, kind, **fields):
    socket.send_json(dict(type=kind, protocol_version=2,
        connection_generation=generation, **fields))


def _ready(socket, generation):
    _send(socket, generation, "READY", model_revision=REVISION, space=SPACE,
          runtime_readiness=observation())
    reply = socket.receive_json()
    assert reply["type"] == "READY_ACK" and reply["status"] == "ready", reply


def _begin(app, queue, generation, auth, *, name):
    async def start():
        authority = await auth.resolver.resolve_oauth(
            auth.tokens.access_token,
            destination=TrustedReadDestination("mcp", "personal", "fastpath-negatives"))
        query = Query(request_id=name, connection_generation=generation,
            turn_generation=1, query="synthetic evidence only", deadline_ms=2000,
            min_data_seq=5, actor=TrustedActorContext(
                person_id=authority.person_id, audience=authority.destination.key,
                auth_epoch=authority.auth_revision, visibility_epoch=0,
                context_ref=authority.context_ref))
        return await queue.shared_reads.begin_read(
            authority, query, dialog_ref="fastpath-negatives",
            deadline=time.monotonic()+1.9)
    return app.portal.call(start)


def test_expired_real_run_cannot_become_wss_query_after_fastpath(shared_channel_run):
    """Authenticated generation expires; worker socket closes, never emits read."""
    queue, run_id, token, nonce = shared_channel_run
    observations = []
    route = candidate_route(fastpath=True, observations=observations)
    with TestClient(Starlette(routes=[route(queue)])) as app:
        with app.websocket_connect("/bge-worker/channel",
                headers={"Authorization": "Bearer " + token}) as socket:
            generation = _attach(socket, run_id, token, nonce)
            _send(socket, generation, "RESOURCE_STATE", state="idle")
            _ready(socket, generation)
            assert queue.channel_status(run_id,token,generation)["runtime_verified"]
            with queue.connect(write=False) as db:
                started = db.execute(
                    "SELECT started FROM runs WHERE id=?", (run_id,)).fetchone()[0]
            # The original queue owns lease expiry; replace only its clock.
            queue.clock = lambda: started + MAX_LIFETIME + 1
            with pytest.raises(RevokedRun):
                queue.channel_status(run_id,token,generation)
            _send(socket, generation, "HEARTBEAT")
            with pytest.raises(WebSocketDisconnect) as closed:
                socket.receive_json()
            assert closed.value.code == 4403
    assert any(caller == "sender" or caller == "receiver"
               for caller,_,_ in observations)


def test_expired_run_between_reservation_and_sender_fresh_status_cannot_hit_wire(
    shared_channel_run, auth
):
    """Actual work is reserved; WSS sender must revalidate before QUERY send."""
    queue, run_id, token, nonce = shared_channel_run
    reached, release = threading.Event(), threading.Event()
    observations=[]
    route=candidate_route(fastpath=True, observations=observations,
                          sender_entered=reached, sender_release=release)
    try:
        with TestClient(Starlette(routes=[route(queue)])) as app:
            with app.websocket_connect("/bge-worker/channel",
                    headers={"Authorization":"Bearer "+token}) as socket:
                generation=_attach(socket,run_id,token,nonce)
                _send(socket,generation,"RESOURCE_STATE",state="idle")
                _ready(socket,generation)
                ticket=_begin(app,queue,generation,auth,name="lease-expires-before-send")
                assert reached.wait(3), "real WSS sender never reached fresh-status fence"
                assert any(caller=="sender" and promoted
                           for caller,promoted,_ in observations)
                with queue.connect(write=False) as db:
                    started=db.execute(
                        "SELECT started FROM runs WHERE id=?", (run_id,)).fetchone()[0]
                queue.clock=lambda:started+MAX_LIFETIME+1
                release.set()
                # Source code has already made a compute reservation. If the
                # sender freshness check vanished, QUERY could escape here.
                with pytest.raises(WebSocketDisconnect) as disconnected:
                    socket.receive_json()
                assert disconnected.value.code==4403
                assert not ticket.result.done() or ticket.result.cancelled()
    finally:
        release.set()


def test_revoked_actor_denied_by_real_admission_before_any_query(
    shared_channel_run, auth
):
    queue, run_id, token, nonce = shared_channel_run
    observations = []
    route = candidate_route(fastpath=True, observations=observations)
    with TestClient(Starlette(routes=[route(queue)])) as app:
        with app.websocket_connect("/bge-worker/channel",
                headers={"Authorization": "Bearer " + token}) as socket:
            generation = _attach(socket, run_id, token, nonce)
            _send(socket, generation, "RESOURCE_STATE", state="idle")
            _ready(socket, generation)
            change(auth.p, ParticipantGrant(
                capability="public_search", granted=False,
                reason="synthetic revocation before admission"))
            with pytest.raises(ReadAuthorityError):
                _begin(app,queue,generation,auth,name="revoked-disallowed")
            assert queue.shared_reads.status()["pending_reads"] == 0
            # A live heartbeat cannot create an unauthorized QUERY.
            _send(socket,generation,"HEARTBEAT")
            response = socket.receive_json()
            assert response["type"] == "HEARTBEAT_ACK", response
    assert not any(caller == "dispatch" and promoted and probe
                   for caller,promoted,probe in observations)


def test_reconnect_probe_blocks_work_until_fresh_owned_nonce(
    shared_channel_run, auth
):
    """A reattached shared consumer may not dispatch before exact fresh probe."""
    queue, run_id, token, nonce = shared_channel_run
    observations = []
    probe_observed = threading.Event()
    route = candidate_route(
        fastpath=True, observations=observations,
        probe_event=probe_observed)
    with TestClient(Starlette(routes=[route(queue)])) as app:
        with app.websocket_connect("/bge-worker/channel",
                headers={"Authorization": "Bearer " + token}) as first:
            old_gen = _attach(first,run_id,token,nonce)
            _send(first,old_gen,"RESOURCE_STATE",state="idle")
            _ready(first,old_gen)
        with app.websocket_connect("/bge-worker/channel",
                headers={"Authorization": "Bearer " + token}) as second:
            generation = _attach(second,run_id,token,nonce)
            assert generation > old_gen
            challenge = second.receive_json()
            assert challenge["type"] == "RESOURCE_PROBE", challenge
            assert type(challenge["probe_nonce"]) is str and challenge["probe_nonce"]
            with pytest.raises(StaleAttachment):
                queue.channel_status(run_id,token,old_gen)
            async def stale_work():
                return await queue.shared_reads.next_channel_work(run_id,token,old_gen)
            with pytest.raises(StaleAttachment):
                app.portal.call(stale_work)

            # Even READY with valid model/runtime proof must not authorize
            # dispatch while current resource reconciliation is unresolved.
            _ready(second,generation)
            ticket = _begin(app,queue,generation,auth,name="probe-fenced-read")
            assert probe_observed.wait(3), "no real promoted dispatch status while probe pending"
            assert any(caller == "dispatch" and promoted and probe
                       for caller,promoted,probe in observations)
            assert queue.shared_reads.status()["pending_reads"] == 1

            _send(second,generation,"RESOURCE_STATE",state="idle",
                  probe_nonce="wrong-synthetic-stale-challenge")
            _send(second,generation,"HEARTBEAT")
            ack = second.receive_json()
            assert ack["type"] == "HEARTBEAT_ACK", ack
            assert queue.shared_reads.status()["pending_reads"] == 1

            # Exact fresh nonce closes the proof boundary, not a heartbeat
            # or the prior generation's owner/lease.
            _send(second,generation,"RESOURCE_STATE",state="idle",
                  probe_nonce=challenge["probe_nonce"])
            assigned = second.receive_json()
            assert assigned["type"] == "QUERY" and assigned["request_id"] == ticket.request_id
            _send(second,generation,"QUERY_RESULT",request_id=assigned["request_id"],
                  turn_generation=assigned["turn_generation"],status="degraded",
                  mode="hybrid",reason="bounded_retrieval",evidence=[],
                  observed_data_seq=5,observed_projection_seq=5,
                  observed_visibility_epoch=0,
                  observed_database_generation="synthetic-db")
            async def delivered():
                return await ticket.result
            assert app.portal.call(delivered).mode == "hybrid"
            _send(second,generation,"RESOURCE_RELEASED",
                  resource_kind="read",resource_id=assigned["request_id"],
                  compute_claim=assigned["compute_claim"],
                  dispatch_generation=generation)
