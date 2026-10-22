"""Real WSS/queue behavior for the M7 source hunk stacked on PR239.

The checked-in production channel has both PR239 proof-epoch protection and
M7 promoted shared fastpath. This test constructs an in-memory baseline by
reversing ONLY M7; it does not replace WSS, admission or queue behavior.
No provider/PG/model installation or live latency claim.
"""
from __future__ import annotations

import inspect
from pathlib import Path
import sys
import time
import types

import pytest
from starlette.applications import Starlette
from starlette.testclient import TestClient

from regional_knowledge.bge_contract import REVISION, SPACE
from regional_knowledge.kaggle_protocol import Query, TrustedActorContext
from regional_knowledge.read_authority import TrustedReadDestination
from test_bge_runtime_readiness import run, shared_channel_run, observation
from test_read_authority import auth
from test_participants import people

import regional_knowledge.bge_channel as production

ORIGINAL = """            async def dispatch():
                nonlocal last_work, cancelled_work
                resume_pending = True
                while True:
                    observed = await status()
                    await promote(observed)
"""
CANDIDATE = """            async def dispatch():
                nonlocal last_work, cancelled_work
                resume_pending = True
                while True:
                    if not promoted or shared is None or probe_nonce is not None:
                        observed = await status()
                        await promote(observed)
"""
BEFORE_STATUS = """            async def status():
                return await queue_call(queue.channel_status, run_id, token, generation)
"""
INSTRUMENTED_STATUS = """            async def status():
                # Test-only observation. Calls the REAL queue method and
                # preserves its attachment validation.
                frame = sys._getframe(1)
                caller = frame.f_code.co_name
                _status_observations.append((caller, promoted, probe_nonce is not None))
                if (caller == "dispatch" and promoted and probe_nonce is not None
                        and _probe_dispatch_event is not None):
                    _probe_dispatch_event.set()
                return await queue_call(queue.channel_status, run_id, token, generation)
"""


def candidate_route(*, fastpath: bool, observations: list, probe_event=None):
    """Build a temporary module; no global module, source or runtime patch."""
    source = Path(inspect.getfile(production)).read_text()
    if (source.count(CANDIDATE) != 1 or source.count(BEFORE_STATUS) != 1
            or "if runtime_readiness is not evaluated:" not in source):
        raise AssertionError("M7/PR239 exact source drift; fail closed")
    if not fastpath:
        source = source.replace(CANDIDATE, ORIGINAL)
    source = source.replace(BEFORE_STATUS, INSTRUMENTED_STATUS)
    module = types.ModuleType("regional_knowledge._rkb_linza_observe_status")
    module.__package__ = "regional_knowledge"
    module.__file__ = inspect.getfile(production)
    module.__dict__["sys"] = sys
    module.__dict__["_status_observations"] = observations
    module.__dict__["_probe_dispatch_event"] = probe_event
    exec(compile(source, module.__file__, "exec"), module.__dict__)
    return module.channel_route


def roundtrip(queue, ident, token, nonce, auth, *, fastpath):
    observations = []
    route = candidate_route(fastpath=fastpath, observations=observations)
    with TestClient(Starlette(routes=[route(queue)])) as app:
        with app.websocket_connect("/bge-worker/channel",
                headers={"Authorization": "Bearer " + token}) as socket:
            socket.send_json(dict(type="ATTACH",protocol_version=2,run_id=ident,
                launch_nonce=nonce,runtime_incarnation="incarnation-a"))
            welcome = socket.receive_json()
            assert welcome["type"] == "WELCOME"
            generation = welcome["connection_generation"]

            def send(kind, **payload):
                socket.send_json(dict(type=kind,protocol_version=2,
                    connection_generation=generation,**payload))

            send("RESOURCE_STATE",state="idle")
            send("READY",model_revision=REVISION,space=SPACE,
                 runtime_readiness=observation())
            ready = socket.receive_json()
            assert ready["type"] == "READY_ACK" and ready["status"] == "ready", ready
            assert queue.channel_status(ident,token,generation)["runtime_verified"]

            async def start():
                authority = await auth.resolver.resolve_oauth(
                    auth.tokens.access_token,
                    destination=TrustedReadDestination("mcp","personal","status-fastpath"))
                query=Query(request_id="one-status-fastpath-read",
                    connection_generation=generation,turn_generation=1,
                    query="synthetic-only control question",
                    deadline_ms=2000,min_data_seq=5,
                    actor=TrustedActorContext(person_id=authority.person_id,
                        audience=authority.destination.key,auth_epoch=authority.auth_revision,
                        visibility_epoch=0,context_ref=authority.context_ref))
                return await queue.shared_reads.begin_read(
                    authority,query,dialog_ref="status-fastpath",
                    deadline=time.monotonic()+1.9)

            began=time.perf_counter_ns()
            ticket=app.portal.call(start)
            work = socket.receive_json()
            assert work["type"] == "QUERY"
            assert work["request_id"] == ticket.request_id
            elapsed_ms=(time.perf_counter_ns()-began)/1_000_000
            # Sender awaited current status and admission.mark_work_sent
            # BEFORE the QUERY reached its existing attached consumer.
            send("QUERY_RESULT",request_id=work["request_id"],
                 turn_generation=work["turn_generation"],status="degraded",
                 mode="hybrid",reason="bounded_retrieval",evidence=[],
                 observed_data_seq=5,observed_projection_seq=5,
                 observed_visibility_epoch=0,observed_database_generation="synthetic-db")

            async def delivered():
                return await ticket.result
            assert app.portal.call(delivered).mode == "hybrid"
            send("RESOURCE_RELEASED",resource_kind="read",
                 resource_id=work["request_id"],compute_claim=work["compute_claim"],
                 dispatch_generation=generation)
    return observations,elapsed_ms


@pytest.mark.parametrize("fastpath", [False, True])
def test_source_fastpath_real_wss_roundtrip_keeps_current_fence(
    shared_channel_run, auth, fastpath
):
    queue, ident, token, nonce=shared_channel_run
    observations,elapsed=roundtrip(queue,ident,token,nonce,auth,fastpath=fastpath)
    assert elapsed > 0
    # This observer never overrides queue authorization or WSS send.
    assert any(caller == "sender" and ready for caller,ready,_ in observations)
    assert any(caller == "receiver" for caller,_,_ in observations)
    if fastpath:
        assert not any(caller == "dispatch" and ready and not probe for caller,ready,probe in observations)
    else:
        assert any(caller == "dispatch" and ready for caller,ready,_ in observations)


def test_exact_inert_patch_preserves_send_side_fences_and_protocol():
    """Only a redundant promoted dispatch status is conditionalized."""
    source = Path(inspect.getfile(production)).read_text()
    assert source.count(CANDIDATE) == 1
    assert "if runtime_readiness is not evaluated:" in source
    baseline = source.replace(CANDIDATE, ORIGINAL)
    assert baseline.count(ORIGINAL) == 1
    assert source.count("await status()  # Revalidate the authenticated attachment.") == 1
    assert source.count("observed = await status()") == baseline.count("observed = await status()")
    assert source.count("await queue_call(queue.channel_status, run_id, token, generation)") == 1
    assert source.count("await shared.next_channel_work(run_id, token, generation)") == 1
    assert source.count("await promote(observed)") == baseline.count("await promote(observed)")
    assert "if not promoted or shared is None or probe_nonce is not None:" in source
    assert baseline.replace(ORIGINAL, CANDIDATE) == source
