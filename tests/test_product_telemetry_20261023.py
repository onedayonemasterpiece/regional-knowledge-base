"""Source/fixture-only FAST-1: no provider, PG, Kaggle or real book."""
from __future__ import annotations

import asyncio
import errno
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
    sink = telemetry.ProductTelemetry(mode="standard", sink=seen.append, capacity=32)
    assert not sink.try_emit(event(), level="debug")
    assert sink.try_emit(event())
    assert sink.try_emit(dict(event(), query="PRIVATE BOOK QUERY"))
    assert sink.try_emit(dict(event(), token="BEARER-SECRET"))
    assert sink.try_emit(dict(event(), signed_url="https://private.example/?token=secret"))
    assert not sink.try_emit(dict(event(), duration_ms=float("nan")))
    assert sink.try_emit(dict(event(), request_ref="raw-person-id"))
    sink._queue.join()
    assert len(seen) == 5
    assert seen[0]["trace_id"] == "a" * 32
    assert seen[0]["timing_semantics"] == "overlapping_do_not_sum"
    assert all(r.get("ignored_fields") == 1 for r in seen[1:])
    assert all(k not in json.dumps(seen) for k in ("PRIVATE", "BEARER", "secret", "raw-person-id"))
    assert all("request_ref" not in row for row in seen)
    assert sink.failed == 1 and sink.dropped == 0
    debug_records = []
    debug = telemetry.ProductTelemetry(mode="debug", sink=debug_records.append)
    assert debug.try_emit({"event":"resource.sample", "phase":"resource",
                           "status":"ok", "duration_ms":0.8, "cpu_utilization_cores": 4.25,
                           "rss_kib": 4096, "memory_current_bytes": 8192},
                          level="debug")
    assert debug.try_emit(dict(event(), rss_kib=42))  # Missing cross-plane field is dropped.
    assert not debug.try_emit({"event":"resource.sample", "phase":"resource",
                               "status":"ok", "rss_kib": -1})
    debug._queue.join()
    assert debug_records[0]["level"] == "debug"
    assert debug_records[0]["rss_kib"] == 4096
    assert debug_records[0]["diagnostic_state"] == "observed"
    assert debug_records[1]["ignored_fields"] == 1
    assert "rss_kib" not in debug_records[1]


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


def test_bounded_sealed_segments_preserve_older_records_without_archive(tmp_path):
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
    folder = tmp_path / "logs" / "sealed"
    sealed = sorted(folder.glob("segment-*.jsonl"))
    assert sealed and sum(path.stat().st_size for path in sealed) <= 2048
    assert all(path.read_bytes().endswith(b"\\n") for path in sealed)
    assert all(b"query" not in path.read_bytes() and b"token" not in path.read_bytes()
               for path in sealed)
    assert not list((tmp_path / "logs").glob("*.open"))
    # Three-day operator hot horizon is not automatic deletion or publication.
    old_at = time.time() - 3 * 24 * 3600 - 10
    for path in sealed:
        os.utime(path, (old_at, old_at))
    assert sorted(folder.glob("segment-*.jsonl")) == sealed
    # Old unarchived segments still consume cap; additional writes drop.
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


def test_actual_resource_schema_routes_numbers_into_single_sink():
    records = []
    sink = telemetry.ProductTelemetry(mode="standard", sink=records.append)
    event = {"event":"resource.sample", "phase":"resource", "status":"ok",
             "run_id":"run-fixture", "runtime_incarnation":"incarnation-fixture",
             "release_sha256":"e"*64, "connection_generation":3,
             "sample_seq":10, "sample_time_unix_ms":1700000000000,
             "scope":"own_cgroup", "rss_kib":128, "pss_kib":75,
             "cpu_seconds":3.5, "cgroup_version":2,
             "cpu_usage_usec":50000, "memory_current_bytes":8192,
             "cpu_utilization_cores":1.2, "memory_events_oom":1,
             "io_read_bytes":16384, "sampling_failures_total":0}
    assert sink.try_emit(event)
    sink._queue.join()
    assert records and all(records[0][key] == value for key,value in event.items())
    assert records[0]["schema"] == "rkb.product.telemetry.v1"
    assert records[0]["diagnostic_state"] == "observed"
    assert not sink.try_emit(dict(event, cgroup_version=7))
    assert sink.try_emit(dict(event, private_argv="contains credentials"))
    assert not sink.try_emit(dict(event, rss_kib=float('nan')))
    sink._queue.join()
    assert "contains credentials" not in json.dumps(records)
    assert records[-1]["ignored_fields"] == 1
    # A malformed provider/release label is discarded, without losing the
    # usable sample or writing the suspicious string to local logs.
    assert sink.try_emit(dict(event, release_sha256="private-token-not-a-hash"))
    sink._queue.join()
    assert records[-1]["ignored_fields"] == 1
    assert "release_sha256" not in records[-1]
    assert "private-token-not-a-hash" not in json.dumps(records)


def test_server_binds_product_sink_to_resource_channel_when_supported(monkeypatch):
    import regional_knowledge.server as server_module
    import regional_knowledge.bge_broker as broker
    import regional_knowledge.bge_channel as channel
    captured = {}
    sink = telemetry.ProductTelemetry(sink=lambda record: None)
    monkeypatch.setattr(telemetry, "_instance", sink)
    monkeypatch.setenv("RKB_DEV_NOAUTH", "1")
    monkeypatch.delenv("RKB_BGE_QUEUE_PATH", raising=False)
    monkeypatch.setattr(broker, "register_routes", lambda *args: None)

    def fake_channel(queue, *, read_telemetry=None, product_telemetry=None):
        captured.update(queue=queue, read_telemetry=read_telemetry,
                        product_telemetry=product_telemetry)
        return lambda scope, receive, send: None

    monkeypatch.setattr(channel, "channel_route", fake_channel)

    class Backend:
        worker_queue = object()

        async def search(self, query, principal, **kwargs):
            from regional_knowledge.contracts import SearchOutput
            return SearchOutput(results=[])

        async def search_evidence(self, query, principal, **kwargs):
            from regional_knowledge.contracts import EvidenceSearchOutput
            return EvidenceSearchOutput(evidence=[])

    server = server_module.build_server(Backend(), profile="live")
    assert captured["product_telemetry"] is sink
    assert captured["read_telemetry"] is not None
    assert getattr(server, "_bge_channel_route", None) is not None
