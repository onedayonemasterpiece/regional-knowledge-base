#!/usr/bin/env python3
"""Bounded public-query latency leaf built on existing RKB read telemetry.

No client, authentication, transport, PG, model, dispatcher, or provider exists
here. The sole operator injects an *already authorized public MCP query* callable,
the same immutable question fixture, and independently verified corpus readback.
This leaf issues only that callback and emits payload-free raw samples. Offline
analysis joins native and controller events from existing ReadTelemetry sinks.
"""
from __future__ import annotations

import argparse
import asyncio
from collections import Counter, defaultdict
from collections.abc import Awaitable, Callable, Mapping, Sequence
from hashlib import sha256
import json
import math
from pathlib import Path
import re
import resource
import statistics
import sys
import time

CASE_IDS = tuple(f"q{i:02}" for i in range(1, 9))
COHORTS = (("serial", 1), ("concurrent2", 2))
WARMUPS_PER_CASE = 2
REPETITIONS_PER_CASE = 6
TOTAL_CALLS = len(CASE_IDS) * len(COHORTS) * (WARMUPS_PER_CASE + REPETITIONS_PER_CASE)
assert TOTAL_CALLS == 128
CALL_TIMEOUT_S = 3.0  # Caller wait guard, NOT the public 2s query deadline or SLO.
PUBLIC_SERVER_QUERY_DEADLINE_S = 2.0
PUBLIC_MCP_P95_SLO_MS = 1500
RUN_TIMEOUT_S = 450.0
PROFILES = frozenset({"limited_first_book", "comparison_corpus"})
MAX_LINES = 2048
MAX_LINE_BYTES = 8192
HEX64 = re.compile(r"[0-9a-f]{64}\Z")
HEX32 = re.compile(r"[0-9a-f]{32}\Z")
SAFE_REF = re.compile(r"[A-Za-z0-9_.:-]{1,256}\Z")
CLIENT_FIELDS = frozenset({"event", "case_id", "phase", "warmup",
    "trace_id", "attempt_id", "request_ref", "status", "reason", "mode",
    "evidence_count", "wall_ms", "callback_wall_ms", "client_semaphore_wait_ms",
    "latency_observation", "censor_lower_bound_ms", "client_cpu_ms", "client_rss_kib"})
CONTROLLER_FIELDS = frozenset({"event", "plane", "terminal", "trace_id",
    "attempt_id", "request_ref", "status", "reason", "backend_elapsed_ms",
    "executed_branches", "stages_ms", "timing_semantics", "dispatch_state",
    "diagnostic_state", "loss_before", "run_id", "runtime_incarnation",
    "connection_generation", "turn_generation", "controller_stages_ms"})
NATIVE_FIELDS = frozenset({"event", "plane", "terminal", "request_ref",
    "run_id", "runtime_incarnation", "connection_generation", "turn_generation",
    "status", "reason", "executed_branches", "stages_ms", "timing_semantics",
    "diagnostic_state", "loss_before", "controller_loss_before"})
STAGES = frozenset({"tokenize_ms", "acquire_ms", "encode_ms", "lexical_ms",
    "dense_ms", "hydrate_ms", "search_ms", "serialize_ms", "total_ms",
    "adapter_ms", "admission_ms", "transport_ms", "output_gate_ms"})
CONTROLLER_STAGES = frozenset({"auth_ms", "binding_ms", "startup_demand_ms",
    "dispatch_gate_ms", "admission_ms", "dispatch_to_result_ms",
    "finish_gate_ms", "result_to_terminal_ms"})
STATUSES = frozenset({"ok", "degraded", "pending", "unavailable", "cancelled",
                      "invalid_query", "denied", "error"})
REASONS = frozenset({"dense_unavailable", "projection_lag", "deadline_exceeded",
    "not_ready", "disconnected", "caller_cancelled", "consumer_cancelled",
    "turn_replaced", "query_too_large", "invalid_filters", "invalid_input",
    "token_limit_exceeded", "access_denied", "prior_write_pending",
    "context_gap", "evidence_withdrawn", "internal_error", "bounded_retrieval",
    "capacity_wait", "read_capacity", "person_read_capacity",
    "dialog_read_capacity", "output_gate_denied", "hybrid_search_required",
    "target_warming"})
CORPUS_FIELDS = frozenset({"database_generation_ref", "schema_revision",
    "accepted_source_count", "visible_passage_count", "applied_seq",
    "projection_seq", "visibility_epoch", "corpus_manifest_sha256",
    "release_sha", "owner_handoff_ref", "profile"})
RESOURCE_FIELDS = frozenset({"event", "phase", "client_process_cpu_ms",
    "client_peak_rss_kib"})
PRIVATE_KEYS = frozenset({"query", "text", "message", "authorization",
    "token", "secret", "password", "credential", "document", "evidence",
    "api_hash", "session", "sql", "vector", "prompt"})


def _number(value: object) -> bool:
    return type(value) in (int, float) and math.isfinite(value) and 0 <= value <= 86_400_000


def _safe(text: object) -> bool:
    return type(text) is str and SAFE_REF.fullmatch(text) is not None


def _no_extra(row: object, fields: frozenset, event: str) -> dict:
    if type(row) is not dict or row.get("event") != event or set(row) - fields:
        raise ValueError("unrecognized or potentially sensitive telemetry field")
    if PRIVATE_KEYS.intersection(row):
        raise ValueError("private telemetry field forbidden")
    return row


def valid_corpus(value: object) -> dict:
    """Owner observation, not a source of authority or proof of a runtime slot."""
    if type(value) is not dict or set(value) != CORPUS_FIELDS:
        raise ValueError("exact bounded corpus/version receipt required")
    for key in ("database_generation_ref", "corpus_manifest_sha256"):
        if type(value[key]) is not str or HEX64.fullmatch(value[key]) is None:
            raise ValueError("immutable corpus binding required")
    if type(value["release_sha"]) is not str or re.fullmatch("[0-9a-f]{40}", value["release_sha"]) is None:
        raise ValueError("exact loaded source SHA required")
    if not _safe(value["schema_revision"]) or not _safe(value["owner_handoff_ref"]):
        raise ValueError("exact operator handoff and schema required")
    for key in ("accepted_source_count", "visible_passage_count", "applied_seq",
                "projection_seq", "visibility_epoch"):
        if type(value[key]) is not int or value[key] < 0:
            raise ValueError("exact numeric corpus frontier required")
    if value["profile"] not in PROFILES:
        raise ValueError("explicit accepted-corpus benchmark profile required")
    if value["applied_seq"] < 1 or value["projection_seq"] != value["applied_seq"]:
        raise ValueError("corpus_not_ready_for_latency")
    if value["profile"] == "limited_first_book":
        # A first accepted real book is a valid narrowly labelled baseline,
        # not proof of corpus representativeness or production-scale readiness.
        if value["accepted_source_count"] != 1 or value["visible_passage_count"] < 1:
            raise ValueError("limited_first_book_requires_one_real_source_with_passages")
    elif value["accepted_source_count"] < 2 or value["visible_passage_count"] < 32:
        raise ValueError("comparison_corpus_requires_two_sources_and_32_passages")
    return dict(value)


def pct(samples: Sequence[float], proportion: float) -> float | None:
    if not samples:
        return None
    ordered = sorted(samples)
    return round(ordered[min(len(ordered)-1, max(0, math.ceil(len(ordered)*proportion)-1))], 3)


def _rss_kib() -> int | None:
    try:
        for line in Path("/proc/self/status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1])
    except (OSError, ValueError, IndexError):
        pass
    return None


def _value(result: object, field: str):
    return result.get(field) if isinstance(result, Mapping) else getattr(result, field, None)


def _trace(result: object) -> dict | None:
    value = _value(result, "read_trace")
    if value is None:
        return None
    return value if isinstance(value, Mapping) else {
        k: getattr(value, k, None) for k in ("trace_id", "attempt_id", "request_ref")}


def _client_row(result: object | None, *, case_id: str, phase: str,
                warmup: bool, elapsed_ms: float, callback_ms: float,
                semaphore_wait_ms: float, cpu_ms: float | None,
                rss_kib: int | None, exception: str | None = None) -> dict:
    """wall_ms includes the harness-local semaphore wait; callback_wall_ms does not.

    A 3-second caller timeout is right-censored. The observed time-to-censor
    is a lower bound on hypothetical uncensored completion, NOT completion
    latency. A timely remote deadline response is an exact failed outcome.
    """
    ref = _trace(result) if result is not None else None
    identifiers = {key: ref.get(key) if ref else None
                   for key in ("trace_id", "attempt_id", "request_ref")}
    status, reason, mode = (
        ("unavailable", "deadline_exceeded", None)
            if exception in ("caller_timeout", "remote_timeout")
        else ("cancelled", "caller_cancelled", None) if exception == "cancelled"
        else ("error", "internal_error", None) if exception is not None
        else (_value(result, "status"), _value(result, "reason"), _value(result, "mode")))
    evidence = _value(result, "evidence")
    if evidence is None:
        evidence = _value(result, "results")
    censored = exception in ("caller_timeout", "cancelled")
    row = dict(event="public_client_sample", case_id=case_id, phase=phase,
        warmup=warmup, **identifiers, status=status, reason=reason, mode=mode,
        evidence_count=len(evidence) if isinstance(evidence, (tuple, list)) else 0,
        # The whole scheduled task includes *local* harness wait, not just MCP.
        wall_ms=round(elapsed_ms, 3),
        # Only this interval is comparable with the public MCP response SLO.
        callback_wall_ms=round(callback_ms, 3),
        client_semaphore_wait_ms=round(semaphore_wait_ms, 3),
        latency_observation="right_censored" if censored else "completed",
        censor_lower_bound_ms=round(callback_ms, 3) if censored else None,
        client_cpu_ms=round(cpu_ms, 3) if cpu_ms is not None else None,
        client_rss_kib=rss_kib)
    _validate_client(row)
    return row


def _validate_client(row: dict) -> None:
    _no_extra(row, CLIENT_FIELDS, "public_client_sample")
    if (row["case_id"] not in CASE_IDS or row["phase"] not in dict(COHORTS)
            or type(row["warmup"]) is not bool
            or any(not _number(row[k]) for k in ("wall_ms", "callback_wall_ms",
                                                 "client_semaphore_wait_ms"))
            or abs(row["wall_ms"] - row["callback_wall_ms"]
                   - row["client_semaphore_wait_ms"]) > .02
            or row["client_cpu_ms"] is not None and not _number(row["client_cpu_ms"])
            or row["client_rss_kib"] is not None and (
                type(row["client_rss_kib"]) is not int or row["client_rss_kib"] < 0)
            or type(row["evidence_count"]) is not int or not 0 <= row["evidence_count"] <= 5
            or row["status"] not in STATUSES
            or row["reason"] is not None and row["reason"] not in REASONS
            or row["mode"] not in (None, "hybrid", "lexical_degraded")
            or row["latency_observation"] not in ("completed", "right_censored")):
        raise ValueError("invalid or unbounded public client sample")
    if row["latency_observation"] == "right_censored":
        if (not _number(row["censor_lower_bound_ms"])
                or abs(row["censor_lower_bound_ms"]-row["callback_wall_ms"]) > .02
                or (row["status"],row["reason"]) not in (
                    ("unavailable", "deadline_exceeded"),
                    ("cancelled", "caller_cancelled"))):
            raise ValueError("censored observation has no valid lower bound")
    elif row["censor_lower_bound_ms"] is not None:
        raise ValueError("completed latency cannot carry censor bound")
    ids = [row[k] for k in ("trace_id", "attempt_id", "request_ref")]
    if any(item is not None for item in ids):
        if not (type(ids[0]) is str and HEX32.fullmatch(ids[0]) is not None
                and type(ids[1]) is str and HEX32.fullmatch(ids[1]) is not None
                and type(ids[2]) is str and HEX64.fullmatch(ids[2]) is not None):
            raise ValueError("partial/untrusted public attempt correlation")


def _read_events(path: Path, *, max_bytes=MAX_LINES*MAX_LINE_BYTES) -> list[dict]:
    if path.stat().st_size > max_bytes:
        raise ValueError("bounded nonsecret trace input required")
    rows = []
    with path.open("rb") as stream:
        for line in stream:
            if len(line) > MAX_LINE_BYTES or len(rows) >= MAX_LINES:
                raise ValueError("bounded telemetry event count/line required")
            try:
                row = json.loads(line)
            except (TypeError, ValueError):
                raise ValueError("malformed JSONL trace") from None
            if type(row) is not dict or type(row.get("event")) is not str:
                raise ValueError("invalid telemetry record")
            rows.append(row)
    return rows


def _clean_stages(stages, *, kind):
    allowed = STAGES if kind == "native" else CONTROLLER_STAGES
    if type(stages) is not dict or any(k not in allowed or not _number(v)
                                       for k, v in stages.items()):
        raise ValueError("nonfinite, private or out-of-contract stage")
    return dict(stages)


def _clean_loss(record) -> dict:
    if type(record) is not dict or set(record) - {"emitted", "dropped", "failed"}:
        raise ValueError("invalid native telemetry loss counters")
    if any(type(v) is not int or v < 0 for v in record.values()):
        raise ValueError("invalid telemetry loss value")
    return dict(record)


def _sanitize_existing(record: dict) -> dict | None:
    """Select established controller/native schema; never copy the input blob."""
    event = record.get("event")
    if event in ("read_attempt_started", "read_attempt_dispatched", "read_completed"):
        return None  # Existing lifecycle records are nonterminal/legacy; do not infer success.
    if event == "read_attempt_terminal":
        _no_extra(record, CONTROLLER_FIELDS, event)
        if (type(record.get("trace_id")) is not str or HEX32.fullmatch(record["trace_id"]) is None
                or type(record.get("attempt_id")) is not str or HEX32.fullmatch(record["attempt_id"]) is None
                or type(record.get("request_ref")) is not str or HEX64.fullmatch(record["request_ref"]) is None
                or record.get("status") not in STATUSES or record.get("reason") not in REASONS|{None}
                or record.get("diagnostic_state") not in ("observed", "inconclusive")
                or record.get("dispatch_state") not in ("dispatched", "not_dispatched")
                or record.get("timing_semantics") != "overlapping_do_not_sum"
                or not _number(record.get("backend_elapsed_ms"))):
            raise ValueError("invalid controller terminal")
        return dict(event=event, trace_id=record["trace_id"], attempt_id=record["attempt_id"],
            request_ref=record["request_ref"], status=record["status"], reason=record["reason"],
            backend_elapsed_ms=record["backend_elapsed_ms"],
            controller_stages_ms=_clean_stages(record.get("controller_stages_ms", {}),kind="controller"),
            dispatch_state=record["dispatch_state"],
            diagnostic_state=record["diagnostic_state"],
            loss_before=_clean_loss(record.get("loss_before", {})),
            runtime_ref=(sha256((record["run_id"] + ":" + record["runtime_incarnation"]).encode()).hexdigest()
                if type(record.get("run_id")) is str and type(record.get("runtime_incarnation")) is str else None),
            connection_generation=record.get("connection_generation"),
            turn_generation=record.get("turn_generation"))
    if event in ("kaggle_read_completed", "kaggle_read_adapter_terminal"):
        _no_extra(record, NATIVE_FIELDS, event)
        if (record.get("diagnostic_state") not in ("observed", "inconclusive")
                or record.get("timing_semantics") != "overlapping_do_not_sum"
                or type(record.get("request_ref")) is not str
                or HEX64.fullmatch(record["request_ref"]) is None
                or type(record.get("run_id")) is not str
                or type(record.get("runtime_incarnation")) is not str):
            raise ValueError("invalid native event")
        return dict(event=event, request_ref=record["request_ref"],
            runtime_ref=sha256((record["run_id"] + ":" + record["runtime_incarnation"]).encode()).hexdigest(),
            connection_generation=record.get("connection_generation"),
            turn_generation=record.get("turn_generation"),
            stages_ms=_clean_stages(record.get("stages_ms", {}),kind="native"),
            status=record.get("status") if event.endswith("terminal") else None,
            reason=record.get("reason") if event.endswith("terminal") else None,
            diagnostic_state=record["diagnostic_state"],
            loss_before=_clean_loss(record.get("loss_before", {})),
            controller_loss_before=_clean_loss(record.get("controller_loss_before", {})))
    if event == "public_phase_resources":
        _no_extra(record, RESOURCE_FIELDS, event)
        if (record.get("phase") not in dict(COHORTS) or not _number(record.get("client_process_cpu_ms"))
                or type(record.get("client_peak_rss_kib")) is not int or record["client_peak_rss_kib"] < 0):
            raise ValueError("invalid client process resources")
        return dict(record)
    raise ValueError("unrecognized telemetry event: fail closed")


async def run_public_workload(questions: Mapping[str, str],
                              query_public: Callable[[str], Awaitable[object]],
                              corpus_readback: Callable[[], Awaitable[dict]],
                              emit: Callable[[dict], None]) -> dict:
    """Existing authenticated public MCP callback; never installs a transport.

    A 3s caller timeout starts *after* the local semaphore. The raw sample
    separately exposes local queued wait, public callback time-to-terminal,
    and the whole queued→terminal span. A caller timeout is censored, never
    counted as an observed 3s query completion.
    """
    if (type(questions) is not dict or tuple(sorted(questions)) != CASE_IDS
            or any(type(v) is not str or not 1 <= len(v.encode()) <= 4096 for v in questions.values())
            or not callable(query_public) or not callable(corpus_readback) or not callable(emit)):
        raise ValueError("eight exact owner-held public-query fixtures required")
    fixture_hash = sha256(json.dumps([questions[k] for k in CASE_IDS],
        ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()
    before = valid_corpus(await corpus_readback())
    raw = []
    def record(value):
        raw.append(value)
        emit(dict(value))
    started = time.monotonic()
    async def perform(case_id: str, phase: str, concurrency: int, warmup: bool,
                      queued_ns: int):
        begin = time.perf_counter_ns()
        queue_ms = (begin - queued_ns) / 1_000_000
        cpu_begin = time.process_time_ns() if concurrency == 1 else None
        result = None
        error = None
        # asyncio.timeout exposes whether *this caller* enforced the 3s cap.
        # A remote TimeoutError before 3s is a completed error observation.
        window = asyncio.timeout(CALL_TIMEOUT_S)
        try:
            async with window:
                result = await query_public(questions[case_id])
        except asyncio.CancelledError:
            error = "cancelled"
            raise
        except TimeoutError:
            error = "caller_timeout" if window.expired() else "remote_timeout"
        except Exception:
            # Private exception message and type never enter diagnostics.
            error = "other"
        finally:
            end = time.perf_counter_ns()
            elapsed = (end - queued_ns) / 1_000_000
            callback_elapsed = (end - begin) / 1_000_000
            cpu = (time.process_time_ns() - cpu_begin) / 1_000_000 if cpu_begin is not None else None
            record(_client_row(result, case_id=case_id, phase=phase, warmup=warmup,
                elapsed_ms=elapsed, callback_ms=callback_elapsed,
                semaphore_wait_ms=queue_ms, cpu_ms=cpu,
                rss_kib=_rss_kib(), exception=error))
    async with asyncio.timeout(RUN_TIMEOUT_S):
        for phase, concurrency in COHORTS:
            cpu_before = time.process_time_ns()
            for _ in range(WARMUPS_PER_CASE):
                for case_id in CASE_IDS:
                    await perform(case_id, phase, 1, True, time.perf_counter_ns())
            semaphore = asyncio.Semaphore(concurrency)
            async def call(case_id, queued_ns):
                async with semaphore:
                    await perform(case_id, phase, concurrency, False, queued_ns)
            for _ in range(REPETITIONS_PER_CASE):
                # Stamps are captured BEFORE task scheduling/semaphore wait,
                # making local queue delay visible in the full-schedule metric.
                tasks = []
                for case_id in CASE_IDS:
                    tasks.append(asyncio.create_task(
                        call(case_id, time.perf_counter_ns())))
                await asyncio.gather(*tasks)
            record(dict(event="public_phase_resources", phase=phase,
                client_process_cpu_ms=round((time.process_time_ns()-cpu_before)/1_000_000,3),
                client_peak_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss))
    after = valid_corpus(await corpus_readback())
    if before != after:
        raise RuntimeError("corpus/profile/version changed during workload; samples are not comparable")
    return dict(codec="rkb.public-query-telemetry-run.v1",
        status="public_query_callbacks_invoked_not_authority_proof",
        question_fixture_sha256=fixture_hash, corpus=before,
        calls=len([x for x in raw if x["event"]=="public_client_sample"]),
        warmups=len([x for x in raw if x["event"]=="public_client_sample" and x["warmup"]]),
        elapsed_seconds=round(time.monotonic()-started,3))


def analyze(client: Sequence[dict], telemetry: Sequence[dict],
            *, profile: str) -> tuple[dict, list[dict]]:
    """Never infer a whole-cohort completion p95 from survivors of 3s censoring.

    True public callback response times exclude the *local test semaphore*.
    Whole-schedule wall_ms includes that semaphore and is reported separately.
    In both cases timeouts are lower-bound observations, never exact completion.
    """
    if profile not in PROFILES:
        raise ValueError("explicit same benchmark corpus profile required")
    clients = []
    resources = {}
    for row in client:
        if row.get("event") == "public_client_sample":
            _validate_client(row)
            clients.append(dict(row))
        elif row.get("event") == "public_phase_resources":
            value = _sanitize_existing(row)
            if value["phase"] in resources:
                raise ValueError("duplicate client process resource observation")
            resources[value["phase"]] = value
        else:
            raise ValueError("unknown client observation")
    if len(clients) != TOTAL_CALLS:
        raise ValueError("exact workload sample count required")
    for name, _ in COHORTS:
        for case_id in CASE_IDS:
            rows = [r for r in clients if r["phase"] == name and r["case_id"] == case_id]
            if (len(rows) != WARMUPS_PER_CASE + REPETITIONS_PER_CASE
                    or sum(bool(r["warmup"]) for r in rows) != WARMUPS_PER_CASE):
                raise ValueError("incomplete or nonuniform question repetition")
    by_controller, by_native = {}, defaultdict(list)
    for event in telemetry:
        row = _sanitize_existing(event)
        if row is None:
            continue
        if row["event"] == "read_attempt_terminal":
            key = (row["trace_id"], row["attempt_id"], row["request_ref"])
            if key in by_controller:
                raise ValueError("duplicate controller terminal")
            by_controller[key] = row
        elif row["event"].startswith("kaggle_read_"):
            by_native[row["request_ref"]].append(row)
        else:
            raise ValueError("resource observations belong to client stream")
    joint = []
    seen = set()
    for sample in clients:
        key = tuple(sample[k] for k in ("trace_id", "attempt_id", "request_ref"))
        if key[0] is not None:
            if key in seen:
                raise ValueError("duplicate public attempt identity")
            seen.add(key)
        controller = by_controller.get(key)
        valid = controller is not None and controller["diagnostic_state"] == "observed"
        if controller is not None:
            valid &= controller["status"] == sample["status"]
            valid &= controller["reason"] == sample["reason"]
            valid &= not (controller["loss_before"].get("dropped", 0)
                          or controller["loss_before"].get("failed", 0))
        native = []
        if controller is not None and controller["dispatch_state"] == "dispatched":
            native = [e for e in by_native.get(sample["request_ref"], [])
                      if (e["runtime_ref"] == controller["runtime_ref"]
                          and e["connection_generation"] == controller["connection_generation"]
                          and e["turn_generation"] == controller["turn_generation"])]
            if len(native) != 2 or {e["event"] for e in native} != {
                    "kaggle_read_completed", "kaggle_read_adapter_terminal"}:
                valid = False
            else:
                valid &= all(e["diagnostic_state"] == "observed" and
                    not (e["loss_before"].get("dropped", 0) or
                         e["loss_before"].get("failed", 0) or
                         e["controller_loss_before"].get("dropped", 0) or
                         e["controller_loss_before"].get("failed", 0)) for e in native)
                native_terminal = next(e for e in native if e["event"] == "kaggle_read_adapter_terminal")
                valid &= (native_terminal["status"], native_terminal["reason"]) == (
                    controller["status"], controller["reason"])
        if controller is not None and controller["dispatch_state"] == "not_dispatched" and by_native.get(sample["request_ref"]):
            valid = False
        stage = next((e["stages_ms"] for e in native if e["event"] == "kaggle_read_completed"), {})
        adapter = next((e["stages_ms"] for e in native if e["event"] == "kaggle_read_adapter_terminal"), {})
        state = "observed" if valid else "inconclusive"
        public_success = (sample["latency_observation"] == "completed"
            and sample["status"] in ("ok", "degraded")
            and sample["mode"] == "hybrid" and sample["evidence_count"] > 0)
        useful = (public_success and state == "observed" and controller is not None
                  and controller["dispatch_state"] == "dispatched")
        joint.append({**sample, "diagnostic_state": state,
            "public_success": bool(public_success), "useful": bool(useful),
            "backend_elapsed_ms": controller["backend_elapsed_ms"] if controller else None,
            "controller_stages_ms": controller["controller_stages_ms"] if controller else {},
            "native_stages_ms": dict(stage, **({"adapter_ms": adapter["adapter_ms"]}
                                            if "adapter_ms" in adapter else {})),
            "dispatch_state": controller["dispatch_state"] if controller else "unknown"})
    summary = {
        "codec":"rkb.public-query-latency-report.v1",
        "scope":"public_MCP_query_observations_only_no_runtimes_launched",
        "corpus_profile":profile,
        "profile_label":("LIMITED_FIRST_ACCEPTED_BOOK_NOT_REPRESENTATIVE"
            if profile == "limited_first_book" else
            "EXPANDED_COMPARISON_CORPUS_NOT_PRODUCTION_SCALE_ACCEPTANCE"),
        "production_scale_accepted":False,
        "public_query_deadline_s":PUBLIC_SERVER_QUERY_DEADLINE_S,
        "public_mcp_p95_slo_ms":PUBLIC_MCP_P95_SLO_MS,
        "caller_timeout_s":CALL_TIMEOUT_S,
        "total_calls":len(clients),
        "warmups":sum(row["warmup"] for row in clients),
        "warmup_right_censored":sum(row["warmup"] and row["latency_observation"] ==
                                    "right_censored" for row in clients),
        "all_attempts_preserved":True,
        "overlapping_stages_not_summed":True,
        "semaphore_wait_in_public_callback_latency":False,
        "semaphore_wait_in_client_scheduled_latency":True,
        "transport_standalone_ms":None,
        "kaggle_native_cpu_ms":None, "kaggle_native_rss_kib":None,
        "notes":"Public callback includes true MCP transport/controller admission; local harness semaphore excluded. Full scheduled latency includes local wait. 3s caller timeouts right-censored; stage durations overlap."
    }
    groups = {}
    for phase, concurrency in COHORTS:
        rows = [x for x in joint if x["phase"] == phase and not x["warmup"]]
        n = len(rows)
        successes = [x for x in rows if x["public_success"]]
        completed = [x for x in rows if x["latency_observation"] == "completed"]
        observed = [x for x in rows if x["diagnostic_state"] == "observed"]
        censored = [x for x in rows if x["latency_observation"] == "right_censored"]
        timed_out = [x for x in rows if x["reason"] == "deadline_exceeded"]
        caller_timed_out = [x for x in censored if x["reason"] == "deadline_exceeded"]
        server_timed_out = [x for x in completed if x["reason"] == "deadline_exceeded"]
        groups[phase] = {
            "concurrency":concurrency,
            "samples":n,
            "completed_public_responses":len(completed),
            "public_success_count":len(successes),
            "failure_count":n-len(successes),
            "failure_rate":round((n-len(successes))/n, 5),
            "timeout_count":len(timed_out),
            "timeout_rate":round(len(timed_out)/n, 5),
            "caller_timeout_censored_count":len(caller_timed_out),
            "server_deadline_completed_count":len(server_timed_out),
            "right_censored_count":len(censored),
            # Main requested latency quantity: true public MCP caller roundtrip,
            # from callback invocation AFTER harness-local semaphore.
            "success_only_p50_public_callback_ms":pct([x["callback_wall_ms"] for x in successes], .5),
            "success_only_p95_public_callback_ms":pct([x["callback_wall_ms"] for x in successes], .95),
            # Schedule-inclusive user-observed simulation: queue+callback.
            "success_only_p50_scheduled_ms":pct([x["wall_ms"] for x in successes], .5),
            "success_only_p95_scheduled_ms":pct([x["wall_ms"] for x in successes], .95),
            "local_semaphore_wait_p50_ms":pct([x["client_semaphore_wait_ms"] for x in rows], .5),
            "local_semaphore_wait_p95_ms":pct([x["client_semaphore_wait_ms"] for x in rows], .95),
            # Never treat a 3s time-to-censor as a known completion.
            "all_requests_completion_p50_public_ms":(
                pct([x["callback_wall_ms"] for x in rows], .5) if not censored else None),
            "all_requests_completion_p95_public_ms":(
                pct([x["callback_wall_ms"] for x in rows], .95) if not censored else None),
            "all_requests_completion_p95_scheduled_ms":(
                pct([x["wall_ms"] for x in rows], .95) if not censored else None),
            "all_requests_p95_public_lower_bound_ms":pct([x["callback_wall_ms"] for x in rows], .95),
            "all_requests_p95_scheduled_lower_bound_ms":pct([x["wall_ms"] for x in rows], .95),
            "right_censor_interpretation":("unknown completion, times are lower bounds if a request could complete"
                if censored else "all public terminal times observed"),
            "p50_backend_completed_telemetry_ms":pct(
                [x["backend_elapsed_ms"] for x in observed if x["backend_elapsed_ms"] is not None], .5),
            "p95_backend_completed_telemetry_ms":pct(
                [x["backend_elapsed_ms"] for x in observed if x["backend_elapsed_ms"] is not None], .95),
            "observed_complete":len(observed),
            "useful_hybrid":sum(x["useful"] for x in rows),
            "status_reason_counts":dict(sorted(Counter(
                f"{x['status']}:{x['reason'] or 'none'}" for x in rows).items())),
            "gate_closures":sum(x["dispatch_state"] != "dispatched" or not x["useful"] for x in rows),
            "controller_stages":{}, "native_stages":{},
            "client_cpu_ms_serial_only":pct(
                [x["client_cpu_ms"] for x in rows if x["client_cpu_ms"] is not None], .5),
            "client_rss_kib_p95":pct(
                [x["client_rss_kib"] for x in rows if x["client_rss_kib"] is not None], .95),
            "client_phase_cpu_total_ms":resources.get(phase,{}).get("client_process_cpu_ms"),
            "client_phase_peak_rss_kib":resources.get(phase,{}).get("client_peak_rss_kib")}
        for result_key, source in (("controller_stages", "controller_stages_ms"),
                                   ("native_stages", "native_stages_ms")):
            keys = CONTROLLER_STAGES if result_key == "controller_stages" else STAGES
            groups[phase][result_key] = {k:{
                "n":sum(k in x[source] for x in observed),
                "p50_ms":pct([x[source][k] for x in observed if k in x[source]], .5),
                "p95_ms":pct([x[source][k] for x in observed if k in x[source]], .95)}
                for k in sorted(keys) if any(k in x[source] for x in observed)}
        groups[phase]["valid_for_profile_baseline"] = (
            n == len(CASE_IDS)*REPETITIONS_PER_CASE
            and groups[phase]["useful_hybrid"] == n
            and groups[phase]["observed_complete"] == n
            and len(censored) == 0)
    # A silent reconnect or switch to another native worker invalidates A/B
    # interpretation even if all per-event records are otherwise valid.
    owner_scope = {
        (by_controller[(x["trace_id"],x["attempt_id"],x["request_ref"])]["runtime_ref"],
         by_controller[(x["trace_id"],x["attempt_id"],x["request_ref"])]["connection_generation"])
        for x in joint if not x["warmup"] and
        (x["trace_id"],x["attempt_id"],x["request_ref"]) in by_controller and
        by_controller[(x["trace_id"],x["attempt_id"],x["request_ref"])]["dispatch_state"] == "dispatched"}
    summary["native_runtime_scope_stable"] = len(owner_scope) == 1
    summary["cohorts"] = groups
    complete = (summary["native_runtime_scope_stable"] and
                summary["warmup_right_censored"] == 0 and
                all(g["valid_for_profile_baseline"] for g in groups.values()))
    if not complete:
        summary["conclusion"] = "INCONCLUSIVE_OR_CENSORED"
    elif profile == "limited_first_book":
        summary["conclusion"] = "LIMITED_FIRST_BOOK_BASELINE_ONLY_NOT_REPRESENTATIVE"
    else:
        summary["conclusion"] = "COMPARISON_CORPUS_BASELINE_ONLY_NOT_PRODUCTION_ACCEPTANCE"
    summary["evidence_quality_and_corpus_ready_owner_gate"] = "required_independently"
    return summary, joint


def validate_receipt(value):
    if (type(value) is not dict or set(value) !=
            {"codec","status","question_fixture_sha256","corpus","calls","warmups","elapsed_seconds"}
            or value["codec"] != "rkb.public-query-telemetry-run.v1"
            or value["status"] != "public_query_callbacks_invoked_not_authority_proof"
            or type(value["question_fixture_sha256"]) is not str
            or HEX64.fullmatch(value["question_fixture_sha256"]) is None
            or type(value["calls"]) is not int or value["calls"] != TOTAL_CALLS
            or type(value["warmups"]) is not int or value["warmups"] !=
                len(CASE_IDS)*len(COHORTS)*WARMUPS_PER_CASE
            or not _number(value["elapsed_seconds"])):
        raise ValueError("complete exact workload receipt required")
    return {**value, "corpus":valid_corpus(value["corpus"])}


def assert_comparable_receipts(before, after) -> dict:
    """Before/after MUST share the exact corpus profile and immutable fixture.

    A first-book baseline cannot be misreported as the expanded comparison
    cohort, even if its timings appear faster on fewer visible passages.
    """
    earlier, later = validate_receipt(before), validate_receipt(after)
    if earlier["corpus"]["profile"] != later["corpus"]["profile"]:
        raise ValueError("before_after_profile_changed")
    if earlier["question_fixture_sha256"] != later["question_fixture_sha256"]:
        raise ValueError("before_after_question_fixture_changed")
    if earlier["corpus"] != later["corpus"]:
        raise ValueError("before_after_corpus_or_version_changed")
    return {"profile":earlier["corpus"]["profile"], "same_corpus":True,
            "same_fixture":True, "not_production_scale_acceptance":True}


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--receipt",type=Path,required=True)
    parser.add_argument("--before-receipt",type=Path)  # Optional matching before/after cross-run gate.
    parser.add_argument("--client",type=Path,required=True)
    parser.add_argument("--telemetry",type=Path,required=True)
    parser.add_argument("--report",type=Path,required=True)
    parser.add_argument("--raw",type=Path,required=True)
    args=parser.parse_args(argv)
    if args.report==args.raw or args.report in (args.client,args.telemetry):
        parser.error("separate output path required")
    if args.receipt.stat().st_size > 8192:
        raise ValueError("bounded workload receipt required")
    receipt=validate_receipt(json.loads(args.receipt.read_text()))
    if args.before_receipt is not None:
        if args.before_receipt.stat().st_size > 8192:
            raise ValueError("bounded before receipt required")
        pair=assert_comparable_receipts(json.loads(args.before_receipt.read_text()),receipt)
    else:
        pair=None
    result,samples=analyze(_read_events(args.client),_read_events(args.telemetry),
                           profile=receipt["corpus"]["profile"])
    result["before_after_match"]=pair
    result["corpus"]=receipt["corpus"]
    result["question_fixture_sha256"]=receipt["question_fixture_sha256"]
    result["runtime_owner_handoff_status"]="separate_operator_approval_required"
    args.report.write_text(json.dumps(result,sort_keys=True,indent=2)+"\n")
    with args.raw.open("w") as output:
        for sample in samples:
            output.write(json.dumps(sample,sort_keys=True,separators=(",",":"))+"\n")
    print(json.dumps({"status":result["conclusion"], "profile":result["corpus_profile"],
        "serial_success_only_p95_ms":result["cohorts"]["serial"]["success_only_p95_public_callback_ms"],
        "concurrent2_success_only_p95_ms":result["cohorts"]["concurrent2"]["success_only_p95_public_callback_ms"],
        "serial_timeout_rate":result["cohorts"]["serial"]["timeout_rate"],
        "concurrent2_timeout_rate":result["cohorts"]["concurrent2"]["timeout_rate"]},
        sort_keys=True))


if __name__=="__main__":
    main()
