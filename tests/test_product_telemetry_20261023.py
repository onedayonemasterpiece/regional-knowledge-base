"""Source/fixture-only FAST-1: no provider, PG, Kaggle or real book."""
from __future__ import annotations

import asyncio
import errno
import hashlib
import json
import os
import threading
import time

import pytest

import regional_knowledge.product_telemetry as telemetry


def event(**changes):
    return {"event": "read.terminal", "phase": "total", "status": "returned",
            "trace_id": "a" * 32, "duration_ms": 0.5, **changes}


def test_strict_whitelist_modes_and_counter_visibility():
    seen = []
    sink = telemetry.ProductTelemetry(mode="standard", sink=seen.append, capacity=4)
    assert not sink.try_emit(event(), level="debug")
    assert sink.try_emit(event())
    assert not sink.try_emit(dict(event(), query="PRIVATE BOOK QUERY"))
    assert not sink.try_emit(dict(event(), token="BEARER-SECRET"))
    assert not sink.try_emit(dict(event(), signed_url="https://private.example/?token=secret"))
    assert not sink.try_emit(dict(event(), duration_ms=float("nan")))
    assert not sink.try_emit(dict(event(), request_ref="raw-person-id"))
    sink._queue.join()
    assert len(seen) == 1
    assert seen[0]["trace_id"] == "a" * 32
    assert seen[0]["timing_semantics"] == "overlapping_do_not_sum"
    assert all(k not in json.dumps(seen) for k in ("PRIVATE", "BEARER", "secret"))
    assert sink.failed == 5 and sink.dropped == 0
    debug = telemetry.ProductTelemetry(mode="debug", sink=seen.append)
    assert debug.try_emit({"event":"resource.sample", "phase":"resource",
                           "status":"ok", "duration_ms":0.8, "cpu_percent": 4.25,
                           "rss_bytes": 4096, "cgroup_memory_current_bytes": 8192},
                          level="debug")
    assert not debug.try_emit(dict(event(), rss_bytes=42))
    assert not debug.try_emit({"event":"resource.sample", "phase":"resource",
                               "status":"ok", "rss_bytes": -1})
    debug._queue.join()
    assert seen[-1]["level"] == "debug"
    assert seen[-1]["rss_bytes"] == 4096
    assert seen[-1]["diagnostic_state"] == "observed"


@pytest.mark.asyncio
async def test_blocked_sink_and_full_queue_never_hold_result():
    entered, release = threading.Event(), threading.Event()

    def blocked(_):
        entered.set()
        release.wait(3)

    telemetry_sink = telemetry.ProductTelemetry(capacity=1, sink=blocked)
    try:
        assert telemetry_sink.try_emit(event())
        assert await asyncio.to_thread(entered.wait, 1)
        assert telemetry_sink.try_emit(event(trace_id="b" * 32))
        before = time.monotonic()
        assert not telemetry_sink.try_emit(event(trace_id="c" * 32))
        assert time.monotonic() - before < .2
        assert telemetry_sink.dropped == 1
        # No await or I/O in the bounded producer.
        async def backend():
            return "SEARCH_RESULT"
        assert await asyncio.wait_for(backend(), .2) == "SEARCH_RESULT"
    finally:
        release.set()
    await asyncio.wait_for(asyncio.to_thread(telemetry_sink._queue.join), 1)


def test_enospc_and_sink_exceptions_stop_retrying_without_deleting_events():
    calls = []

    def enospc(record):
        calls.append(record)
        raise OSError(errno.ENOSPC, "full")

    sink = telemetry.ProductTelemetry(sink=enospc)
    assert sink.try_emit(event())
    sink._queue.join()
    assert sink.failed == 1 and sink.counters()["sink_disabled"] is True
    assert not sink.try_emit(event(trace_id="b"*32))
    assert sink.dropped == 1 and len(calls) == 1
    assert sink.counters()["emitted"] == 1


def test_segment_hash_budget_sealed_handoff_and_three_day_hot_policy(tmp_path):
    spool = telemetry.SegmentedLogSink(tmp_path / "logs", segment_bytes=1024,
                                       disk_limit_bytes=2048)
    clean = telemetry._clean(event(), "standard")
    sent = 0
    while True:
        try:
            spool(clean)
            sent += 1
        except OSError as error:
            assert error.errno == errno.ENOSPC
            break
    assert sent >= 4
    spool.seal()
    manifest = spool.sealed_manifest()
    assert manifest and sum(row["size_bytes"] for row in manifest) <= 2048
    assert all(row["archived"] is False and row["archive_due"] is False for row in manifest)
    for row in manifest:
        data = (tmp_path / "logs" / "sealed" / row["name"]).read_bytes()
        assert hashlib.sha256(data).hexdigest() == row["sha256"]
        assert row["size_bytes"] == len(data)
        assert b"query" not in data and b"token" not in data
    assert not list((tmp_path / "logs").glob("*.open"))
    old_at = time.time() - telemetry.HOT_SECONDS - 10
    for row in manifest:
        os.utime(tmp_path / "logs" / "sealed" / row["name"], (old_at, old_at))
    older = spool.sealed_manifest()
    assert all(row["archive_due"] and not row["archived"] for row in older)
    assert len(list((tmp_path / "logs" / "sealed").glob("*.jsonl"))) == len(manifest)
    # Old segments deliberately still consume disk budget, never auto-GC.
    with pytest.raises(OSError) as error:
        spool(clean)
    assert error.value.errno == errno.ENOSPC


@pytest.mark.asyncio
async def test_outer_fetch_success_failure_and_cancellation_keep_result_and_error(monkeypatch):
    records = []
    sink = telemetry.ProductTelemetry(sink=records.append)
    monkeypatch.setattr(telemetry, "_instance", sink)

    async def successful():
        return {"private_original": "must never log this"}

    result = await telemetry.observe_async("fetch", lambda: successful())
    assert result == {"private_original": "must never log this"}
    secret = RuntimeError("private source and token")

    async def broken():
        raise secret

    with pytest.raises(RuntimeError) as failure:
        await telemetry.observe_async("fetch", lambda: broken())
    assert failure.value is secret

    async def cancelled():
        raise asyncio.CancelledError("private cancellation reason")

    with pytest.raises(asyncio.CancelledError):
        await telemetry.observe_async("fetch", lambda: cancelled())

    await asyncio.wait_for(asyncio.to_thread(sink._queue.join), 1)
    assert [row["status"] for row in records] == ["returned", "error", "cancelled"]
    assert all(row["duration_ms"] >= 0 for row in records)
    assert not any(word in json.dumps(records) for word in
                   ("private", "token", "source", "cancellation reason"))


def test_invalid_config_and_failed_global_sink_do_not_throw(monkeypatch):
    with pytest.raises(ValueError):
        telemetry.ProductTelemetry(capacity=0)
    with pytest.raises(ValueError):
        telemetry.SegmentedLogSink("/tmp/never-used", segment_bytes=32)
    monkeypatch.setattr(telemetry, "get_product_telemetry",
                        lambda: (_ for _ in ()).throw(OSError(errno.ENOSPC, "no disk")))
    assert telemetry.emit_product_event(event()) is False
