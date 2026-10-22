"""Explicit diagnostic-only READY_ACK interleaving against the REAL WSS/queue.

NOT auto-discovered by default pytest (file name intentionally not test_).
Run explicitly with:
  pytest -q -s tests/diagnostics/ready_ack_interleaving_diagnostic.py

Expected on unchanged bge_channel: diagnostic is RED and shows real WSS ACK
order. No sleeps/retries/fake validation/production source hooks or provider calls.
"""
from __future__ import annotations

import inspect
from pathlib import Path
import sys
from threading import Event, Lock

import pytest
from starlette.applications import Starlette
from starlette.testclient import TestClient

from regional_knowledge.bge_channel import channel_route
from regional_knowledge.bge_contract import REVISION, SPACE
from test_bge_runtime_readiness import run, shared_channel_run, observation
from test_read_authority import auth
from test_participants import people

_BARRIER_SECONDS = 4
_SOURCE = Path(inspect.getsourcefile(channel_route)).resolve()
_LINES, _BASE = inspect.getsourcelines(channel_route)
# Observe line AFTER receiver has replaced the proof and reset blocked; no
# inserted production callback and no monkeypatch of shared runtime locks.
_AFTER_VALID_RECEIVE = [
    number+1 for index, (number,line) in enumerate(
        ((i,text) for i,text in enumerate(_LINES, start=_BASE)))
    if line.strip() == "runtime_blocked = None"
    and index > 0 and _LINES[index-1].strip() == "runtime_readiness = frame['runtime_readiness']"
]
if len(_AFTER_VALID_RECEIVE) != 1:
    raise RuntimeError("receiver READY reset line missing or ambiguous")
_AFTER_VALID_RECEIVE = _AFTER_VALID_RECEIVE[0]


def test_real_wss_stale_negative_ack_after_new_valid_ready(shared_channel_run):
    queue, run_id, token, nonce = shared_channel_run
    old_result_entered = Event()
    valid_receiver_replaced = Event()
    release_old_result = Event()
    old_result_calls = []
    calls_lock = Lock()
    original_accept = queue.accept_search_runtime

    def hold_real_wrong_result(run_id, token, generation, proof):
        # Always execute the REAL production validation and persisted checks.
        # Only delay delivery of its already computed result to dispatch.
        validated = original_accept(run_id, token, generation, proof)
        if proof.get("database_generation") == "wrong":
            with calls_lock:
                old_result_calls.append(tuple(validated.issues))
            old_result_entered.set()
            if not release_old_result.wait(_BARRIER_SECONDS):
                raise AssertionError("diagnostic old-result event barrier expired")
        return validated

    def receiver_line_probe(frame, event, arg):
        if (event == "line" and frame.f_code.co_name == "receiver"
                and Path(frame.f_code.co_filename).resolve() == _SOURCE
                and frame.f_lineno == _AFTER_VALID_RECEIVE):
            incoming = frame.f_locals.get("frame")
            if (type(incoming) is dict and incoming.get("type") == "READY"
                    and incoming.get("runtime_readiness", {}).get(
                        "database_generation") == "synthetic-db"):
                valid_receiver_replaced.set()
        return receiver_line_probe

    with TestClient(Starlette(routes=[channel_route(queue)])) as app:
        # Test-only read-only line observer on the TestClient portal thread.
        # Production channel_route, queue/READY paths and both callers remain
        # unchanged. Attach BEFORE receiver task creation.
        app.portal.call(sys.settrace, receiver_line_probe)
        try:
            with app.websocket_connect(
                    "/bge-worker/channel",
                    headers={"Authorization": "Bearer " + token}) as socket:
                socket.send_json(dict(
                    type="ATTACH", protocol_version=2, run_id=run_id,
                    launch_nonce=nonce, runtime_incarnation="incarnation"))
                welcome = socket.receive_json()
                assert welcome["type"] == "WELCOME", welcome
                generation = welcome["connection_generation"]

                def send(kind, **fields):
                    socket.send_json(dict(
                        type=kind, protocol_version=2,
                        connection_generation=generation, **fields))

                def ordered_heartbeat():
                    send("HEARTBEAT")
                    reply = socket.receive_json()
                    assert reply["type"] == "HEARTBEAT_ACK", reply
                    return reply

                send("RESOURCE_STATE", state="idle")
                ordered_heartbeat()
                assert not queue.channel_status(run_id, token, generation)["ready"]
                assert queue.shared_reads.status()["state"] == "idle"

                send("READY", model_revision=REVISION, space=SPACE)
                missing = socket.receive_json()
                assert missing["type"] == "READY_ACK", missing
                assert missing["status"] == "runtime_not_ready", missing
                assert "runtime_readiness_required" in missing["reasons"]
                ordered_heartbeat()

                send("READY", model_revision=REVISION, space=SPACE,
                     runtime_readiness=observation(database_generation="wrong"))
                mismatch = socket.receive_json()
                assert mismatch["type"] == "READY_ACK", mismatch
                assert mismatch["status"] == "runtime_not_ready", mismatch
                assert "database_generation_mismatch" in mismatch["reasons"]
                ordered_heartbeat()
                assert not queue.channel_status(run_id, token, generation)["ready"]

                # Receiver already completed wrong-proof READY and its
                # HEARTBEAT_ACK. Thus next same wrong proof validation comes
                # from the EXISTING independent dispatch caller.
                queue.accept_search_runtime = hold_real_wrong_result
                assert old_result_entered.wait(_BARRIER_SECONDS), (
                    "actual dispatch did not enter old-proof validation")
                assert old_result_calls == [("database_generation_mismatch",)]
                assert not queue.channel_status(run_id, token, generation)["ready"]

                send("READY", model_revision=REVISION, space=SPACE,
                     runtime_readiness=observation())
                # Actual interpreter executes the existing receiver's
                # runtime_readiness=valid; runtime_blocked=None before event.
                assert valid_receiver_replaced.wait(_BARRIER_SECONDS), (
                    "receiver did not replace proof while dispatch lock held")
                assert not queue.channel_status(run_id, token, generation)["ready"]

                release_old_result.set()
                first = socket.receive_json()
                assert first["type"] == "READY_ACK", first
                if first["status"] == "runtime_not_ready":
                    second = socket.receive_json()
                    assert second["type"] == "READY_ACK", (first, second)
                    assert second["status"] == "ready", (first, second)
                    assert queue.channel_status(run_id, token, generation)["runtime_verified"]
                    assert queue.channel_status(run_id, token, generation)["ready"]
                    assert queue.shared_reads.status()["document_contexts"] == 0
                    pytest.fail(
                        "PRODUCT INTERLEAVING REPRODUCED: stale negative READY_ACK "
                        "from real dispatch before valid receiver READY_ACK; "
                        f"statuses={[first['status'], second['status']]}, "
                        f"old_validation={old_result_calls}. No runtime correction.")
                # Exact future owner correction should produce one current
                # positive ACK without a stale negative for newer proof.
                assert first["status"] == "ready", first
                assert queue.channel_status(run_id, token, generation)["runtime_verified"]
                assert queue.shared_reads.status()["document_contexts"] == 0
        finally:
            release_old_result.set()
            queue.accept_search_runtime = original_accept
            app.portal.call(sys.settrace, None)
