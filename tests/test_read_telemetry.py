import asyncio
import json
import threading

import pytest

from regional_knowledge.contracts import SearchOutput
from regional_knowledge.read_telemetry import ReadTelemetry, current_trace_id


@pytest.mark.asyncio
async def test_stuck_sink_and_full_queue_do_not_hold_read_or_consumer():
    entered, release = threading.Event(), threading.Event()
    records = []

    def blocked(raw):
        records.append(json.loads(raw))
        entered.set()
        assert release.wait(3)

    telemetry = ReadTelemetry(capacity=1, sink=blocked)
    seen = []

    async def backend():
        seen.append(current_trace_id.get())
        return SearchOutput(results=[], timings={"database_seconds": .012,
            "must never be logged": 42, "encoder_seconds": float("nan")})

    try:
        first = await asyncio.wait_for(telemetry.measure("search", backend()), .5)
        assert await asyncio.to_thread(entered.wait, 1)
        second = await asyncio.wait_for(telemetry.measure("search", backend()), .5)
        third = await asyncio.wait_for(telemetry.measure("search", backend()), .5)
        assert telemetry.dropped == 1
        assert current_trace_id.get() is None
        assert len(set(seen)) == 3
        assert first.read_trace.trace_id == seen[0]
        assert second.read_trace.trace_id == seen[1]
        assert third.read_trace.trace_id == seen[2]
        assert first.model_dump()["read_trace"]["backend_elapsed_ms"] >= 0
        assert records[0]["trace_id"] == seen[0]
        assert records[0]["backend_elapsed_ms"] == first.read_trace.backend_elapsed_ms
        assert records[0]["stages_seconds"] == {"database_seconds": .012}
        assert "must never be logged" not in json.dumps(records)
    finally:
        release.set()


@pytest.mark.asyncio
async def test_cancellation_and_failure_restore_trace_and_original_exception():
    records = []
    observed = threading.Event()

    def sink(raw):
        records.append(json.loads(raw))
        if len(records) == 2:
            observed.set()

    telemetry = ReadTelemetry(sink=sink)
    original = RuntimeError("private error contents")

    async def failing():
        raise original

    with pytest.raises(RuntimeError) as caught:
        await telemetry.measure("search", failing())
    assert caught.value is original
    assert current_trace_id.get() is None

    async def cancelled():
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await telemetry.measure("knowledge_search", cancelled())
    assert current_trace_id.get() is None
    assert await asyncio.to_thread(observed.wait, 1)
    assert [item["outcome"] for item in records] == ["error", "cancelled"]
    assert "private error contents" not in json.dumps(records)


@pytest.mark.asyncio
async def test_sink_failure_is_counted_without_changing_read():
    finished = threading.Event()

    def failed(_):
        finished.set()
        raise OSError("sink unavailable")

    telemetry = ReadTelemetry(sink=failed)

    async def backend():
        return SearchOutput(results=[])

    result = await telemetry.measure("search", backend())
    assert result.results == [] and result.read_trace is not None
    assert await asyncio.to_thread(finished.wait, 1)
    # Queue completion also establishes the consumer's failure-counter update.
    await asyncio.wait_for(asyncio.to_thread(telemetry._queue.join), 1)
    assert telemetry.failed == 1


@pytest.mark.asyncio
async def test_thread_start_failure_does_not_replace_result(monkeypatch):
    telemetry = ReadTelemetry()
    monkeypatch.setattr(threading.Thread, "start", lambda _: (_ for _ in ()).throw(RuntimeError()))

    async def backend():
        return SearchOutput(results=[])

    assert (await telemetry.measure("search", backend())).read_trace is not None
    assert telemetry.failed == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("profile,operation", [("full", "search"), ("live", "knowledge_search")])
async def test_real_mcp_output_includes_trace_without_private_stages(monkeypatch, profile, operation):
    from regional_knowledge.contracts import EvidenceSearchOutput, Principal
    import regional_knowledge.server as server_module

    class Backend:
        async def search(self, query, principal, **kwargs):
            assert query == "fixture" and principal.client_id == "fixture-client"
            return SearchOutput(results=[], retrieval_mode="bge_lexical", main_state="ready",
                                timings={"database_seconds": .01})

        async def search_evidence(self, query, principal, **kwargs):
            assert kwargs["max_evidence"] == 3
            return EvidenceSearchOutput(evidence=[], retrieval_mode="bge_lexical", main_state="ready")

    monkeypatch.setenv("RKB_DEV_NOAUTH", "1")
    monkeypatch.delenv("RKB_BGE_QUEUE_PATH", raising=False)
    monkeypatch.setattr(server_module, "_principal", lambda: Principal(
        subject="00000000-0000-0000-0000-000000000001", client_id="fixture-client",
        issuer="fixture-issuer", access_token="synthetic-never-logged"))
    server = server_module.build_server(Backend(), profile=profile)
    reply = await server.call_tool(operation, {"query": " fixture "})
    assert not reply.is_error
    wire = reply.structured_content
    assert len(wire["read_trace"]["trace_id"]) == 32
    assert wire["read_trace"]["backend_elapsed_ms"] >= 0
    assert "timings" not in wire
    assert "synthetic-never-logged" not in reply.model_dump_json()


@pytest.mark.parametrize("mode,retrieval,state", [
    ("lexical_degraded", "lexical_only", "starting"),
    ("hybrid", "bge", "ready"),
    ("hybrid", "fast_e5", "ready"),
    ("hybrid", "bge_lexical", "pending"),
])
def test_public_search_rejects_partial_or_unavailable_hybrid(mode, retrieval, state):
    from mcp.server.mcpserver.exceptions import ToolError
    from regional_knowledge.server import _require_hybrid_read
    output = SearchOutput(results=[], mode=mode, retrieval_mode=retrieval, main_state=state,
                          main_job_id="fixture-pending-job")
    with pytest.raises(ToolError, match="hybrid_search_required") as caught:
        _require_hybrid_read(output)
    status = json.loads(str(caught.value))
    assert status["status"] == "unavailable" and status["main_job_id"] == "fixture-pending-job"


@pytest.mark.asyncio
async def test_controller_boundary_timings_are_bounded_numeric_whitelist_only():
    records=[];telemetry=ReadTelemetry(sink=lambda raw:records.append(json.loads(raw)))
    attempt=telemetry.begin_attempt()
    telemetry.finish_attempt(attempt,status='ok',controller_stages_ms={
        'auth_ms':1.25,'admission_ms':2.,'dispatch_to_result_ms':40.,
        'finish_gate_ms':float('nan'),'result_to_terminal_ms':-1,
        'query':'PRIVATE_PAYLOAD_MUST_NOT_APPEAR','person_id':'PRIVATE_ACTOR'})
    await asyncio.to_thread(telemetry._queue.join)
    terminal=next(r for r in records if r.get('terminal'))
    assert terminal['controller_stages_ms']=={'auth_ms':1.25,'admission_ms':2.,'dispatch_to_result_ms':40.}
    assert 'PRIVATE_' not in json.dumps(records)
