"""Best-effort product spans, not a domain journal or a provider receipt.

No call into this module may determine search/import results or Saved state.
Only fixed metadata fields are accepted: no query, original, bearer, URL or CoT.
"""
from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
import errno
import hashlib
import json
import math
import os
from pathlib import Path
import queue
import re
import threading
import time
from uuid import uuid4

HOT_SECONDS = 3 * 24 * 60 * 60
DEFAULT_DISK_BYTES = 32 * 1024 * 1024
DEFAULT_SEGMENT_BYTES = 256 * 1024
_EVENTS = frozenset({"read.stage", "read.terminal", "fetch.terminal",
                     "import.phase", "import.terminal", "resource.sample"})
_PHASES = frozenset({"total", "auth", "admission", "dispatch", "transport", "adapter",
                     "acquire", "verify", "pages", "stage", "seal", "materialize",
                     "activation", "saved", "archive", "encode", "lexical", "dense", "hydrate",
                     "fusion", "serialize", "search", "fetch", "resource"})
_STATUSES = frozenset({"started", "ok", "returned", "degraded", "pending", "staged",
                       "applied", "saved", "acknowledged", "rejected", "failed",
                       "unavailable", "cancelled", "error", "dropped"})
_IDENTIFIERS = frozenset({"trace_id", "attempt_id", "run_id", "request_ref", "operation_ref"})
_FIELDS = frozenset({"event", "phase", "status", "duration_ms", "level",
                     "timing_semantics", "connection_generation", "turn_generation"}) | _IDENTIFIERS
# Resource-specific bounded numeric ABI shared with the existing heartbeat
# sanitizer. Optional/missing proc/cgroup measurements must stay absent, not 0.
_RESOURCE_FIELDS = frozenset({
    "rss_kib", "hwm_kib", "pss_kib", "cpu_seconds", "cpu_threads", "pid",
    "pg_master_rss_kib", "pg_master_hwm_kib", "pg_master_pss_kib",
    "pg_master_cpu_seconds", "pg_master_pid",
    "cgroup_version", "cpu_quota_us", "cpu_period_us", "cpu_usage_usec",
    "cpu_nr_throttled", "cpu_throttled_usec", "cpu_usage_delta_seconds",
    "cpu_throttled_delta_seconds", "cpu_utilization_cores",
    "memory_current_bytes", "memory_limit_bytes", "memory_events_high",
    "memory_events_max", "memory_events_oom", "memory_events_oom_kill",
    "io_read_bytes", "io_write_bytes", "sample_seq", "sample_time_unix_ms",
    "sampling_failures_total", "received_unix_ms",
    # Narrow compatibility aliases only; never allow arbitrary metadata.
    "cpu_percent", "rss_bytes", "pss_bytes", "shared_bytes",
    "cgroup_memory_current_bytes", "cgroup_memory_limit_bytes",
    "cgroup_memory_max_bytes", "read_queue_depth", "process_count"
})
_RESOURCE_INTEGER_FIELDS = frozenset({
    "pid", "pg_master_pid", "cpu_threads", "cgroup_version", "cpu_quota_us",
    "cpu_period_us", "cpu_usage_usec", "cpu_nr_throttled", "cpu_throttled_usec",
    "memory_current_bytes", "memory_limit_bytes", "memory_events_high",
    "memory_events_max", "memory_events_oom", "memory_events_oom_kill",
    "io_read_bytes", "io_write_bytes", "sample_seq", "sample_time_unix_ms",
    "received_unix_ms", "sampling_failures_total", "rss_kib", "hwm_kib",
    "pss_kib", "pg_master_rss_kib", "pg_master_hwm_kib", "pg_master_pss_kib"
})
_OPAQUE = re.compile(r"^[A-Za-z0-9_.:-]{1,256}$")
_SHA = re.compile(r"^[0-9a-f]{64}$")


def _clean(record: Mapping[str, object], level: str):
    resource_extras = {"runtime_incarnation", "release_sha256", "scope"}
    if not isinstance(record, Mapping) or set(record) - (_FIELDS | _RESOURCE_FIELDS | resource_extras):
        raise ValueError("only fixed telemetry metadata is accepted")
    if set(record) & (_RESOURCE_FIELDS | resource_extras) and record.get("event") != "resource.sample":
        raise ValueError("resource samples have separate fixed fields")
    if (record.get("event") not in _EVENTS or record.get("phase") not in _PHASES
            or record.get("status") not in _STATUSES or level not in ("standard", "debug")):
        raise ValueError("fixed product event/phase/status required")
    result = {"schema": "rkb.product.telemetry.v1", "event": record["event"],
              "phase": record["phase"], "status": record["status"], "level": level,
              "timing_semantics": "overlapping_do_not_sum"}
    if record.get("timing_semantics", "overlapping_do_not_sum") != "overlapping_do_not_sum":
        raise ValueError("overlapping stages may not be summed")
    for key in _IDENTIFIERS:
        value = record.get(key)
        if value is None:
            continue
        if type(value) is not str or _OPAQUE.fullmatch(value) is None:
            raise ValueError("opaque bounded correlation required")
        if key in ("request_ref", "operation_ref") and _SHA.fullmatch(value) is None:
            raise ValueError("hashed request/operation identity required")
        result[key] = value
    for key in ("connection_generation", "turn_generation"):
        value = record.get(key)
        if value is not None:
            if type(value) is not int or not 0 <= value < 2**63:
                raise ValueError("bounded generation required")
            result[key] = value
    if "duration_ms" in record:
        value = record["duration_ms"]
        if type(value) not in (int, float) or not math.isfinite(value) or not 0 <= value <= 86_400_000:
            raise ValueError("finite monotonic duration required")
        result["duration_ms"] = round(value, 3)
    if "runtime_incarnation" in record:
        value = record["runtime_incarnation"]
        if type(value) is not str or not 1 <= len(value) <= 256 or re.fullmatch(r"[A-Za-z0-9_.:-]+", value) is None:
            raise ValueError("bounded internal incarnation required")
        result["runtime_incarnation"] = value
    if "release_sha256" in record:
        value = record["release_sha256"]
        if type(value) is not str or _SHA.fullmatch(value) is None:
            raise ValueError("verified release digest only")
        result["release_sha256"] = value
    if "scope" in record:
        if record["scope"] not in ("own_cgroup", "process_only"):
            raise ValueError("fixed resource scope only")
        result["scope"] = record["scope"]
    for key in _RESOURCE_FIELDS & set(record):
        value = record[key]
        if type(value) not in (int, float) or not math.isfinite(value) or not 0 <= value < 2**63:
            raise ValueError("bounded finite resource reading required")
        if key in _RESOURCE_INTEGER_FIELDS and type(value) is not int:
            raise ValueError("integer resource counter required")
        if key == "cgroup_version" and value not in (1, 2):
            raise ValueError("supported cgroup scope required")
        if key in ("sample_seq", "sample_time_unix_ms", "received_unix_ms") and value < 1:
            raise ValueError("positive sample identity required")
        result[key] = round(value, 3) if type(value) is float else value
    return result


class SegmentedLogSink:
    """Append-only local JSONL spool. Never removes older or unarchived files.

    Sealed files can be passed through an independently authorized Dataset
    archive transfer later. The spool itself performs no provider operations.
    A full disk budget stops writing; it never trades a domain result for logs.
    """

    def __init__(self, directory: str | Path, *, segment_bytes: int = DEFAULT_SEGMENT_BYTES,
                 disk_limit_bytes: int = DEFAULT_DISK_BYTES, clock: Callable[[], float] = time.time):
        if (type(segment_bytes) is not int or not 1024 <= segment_bytes <= 8 * 1024 * 1024
                or type(disk_limit_bytes) is not int or not segment_bytes <= disk_limit_bytes <= 512 * 1024 * 1024):
            raise ValueError("bounded segment and total disk budgets required")
        self.directory = Path(directory)
        self.segment_bytes, self.disk_limit_bytes = segment_bytes, disk_limit_bytes
        self.clock = clock
        self._handle = None
        self._path = None
        self._hash = hashlib.sha256()
        self._size = 0
        self._opened_at = 0.0
        self._used = None

    def _prepare(self):
        if self._used is not None:
            return
        self.directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        sealed = self.directory / "sealed"
        sealed.mkdir(mode=0o700, exist_ok=True)
        # Existing open files, including crash leftovers, consume the budget.
        # No open/sealed file is cleaned up without separately verified archive.
        used = 0
        for folder in (self.directory, sealed):
            for path in folder.iterdir():
                if path.is_file():
                    used += path.stat().st_size
        self._used = used

    def _open(self):
        if self._handle is not None:
            return
        self._path = self.directory / ("segment-%d-%s.open" % (time.time_ns(), uuid4().hex))
        self._handle = self._path.open("xb")
        os.chmod(self._path, 0o600)
        self._size = 0
        self._opened_at = self.clock()
        self._hash = hashlib.sha256()

    def __call__(self, record: dict[str, object]) -> None:
        self._prepare()
        line = (json.dumps({"observed_unix_ms": int(self.clock() * 1000), **record},
                           ensure_ascii=True, separators=(",", ":"), allow_nan=False) + "\n").encode()
        if len(line) > 2048:
            raise ValueError("bounded event only")
        if self._used + len(line) > self.disk_limit_bytes:
            raise OSError(errno.ENOSPC, "product telemetry segment budget exhausted")
        if (self._handle is not None and self._size
                and (self._size + len(line) > self.segment_bytes
                     or self.clock() - self._opened_at >= 3600)):
            self.seal()  # Hourly maximum segment age on an active stream.
        self._open()
        self._handle.write(line)
        self._handle.flush()  # No fsync or durable-accepted claim.
        self._size += len(line)
        self._used += len(line)
        self._hash.update(line)
        if self._size >= self.segment_bytes:
            self.seal()

    def seal(self):
        if self._handle is None:
            return None
        self._handle.close()
        target = self.directory / "sealed" / (
            "%s-%d-%s.jsonl" % (self._path.stem, self._size, self._hash.hexdigest()))
        os.replace(self._path, target)
        self._handle = None
        self._path = None
        return target

    def sealed_manifest(self, now: float | None = None):
        """Operator-side readback contract; no upload, deletion or read-path call."""
        now = self.clock() if now is None else now
        folder = self.directory / "sealed"
        if not folder.is_dir():
            return []
        result = []
        for file in sorted(folder.glob("segment-*.jsonl")):
            if not file.is_file() or file.is_symlink():
                continue
            digest = hashlib.sha256()
            with file.open("rb") as handle:
                while chunk := handle.read(65536):
                    digest.update(chunk)
            size = file.stat().st_size
            sha = digest.hexdigest()
            if not file.name.endswith("-%d-%s.jsonl" % (size, sha)):
                continue  # Do not claim intact archival source.
            result.append({"name": file.name, "size_bytes": size, "sha256": sha,
                           "sealed_unix_ms": int(file.stat().st_mtime * 1000),
                           "archive_due": now - file.stat().st_mtime >= HOT_SECONDS,
                           "archived": False})
        return result


class ProductTelemetry:
    """One bounded daemon consumer per host; producers never wait for I/O."""

    def __init__(self, *, mode: str = "standard", capacity: int = 128,
                 sink: Callable[[dict[str, object]], None] | None = None,
                 directory: str | Path | None = None,
                 segment_bytes: int = DEFAULT_SEGMENT_BYTES,
                 disk_limit_bytes: int = DEFAULT_DISK_BYTES):
        if mode not in ("standard", "debug") or type(capacity) is not int or not 1 <= capacity <= 1024:
            raise ValueError("fixed mode and finite queue capacity required")
        self.mode = mode
        self._queue: queue.Queue = queue.Queue(maxsize=capacity)
        self._sink = sink if sink is not None else SegmentedLogSink(
            directory or "/tmp/rkb-product-telemetry",
            segment_bytes=segment_bytes, disk_limit_bytes=disk_limit_bytes)
        self.dropped = 0
        self.failed = 0
        self.emitted = 0
        self._disabled = False
        self._thread = threading.Thread(target=self._consume, daemon=True, name="rkb-product-telemetry")
        self._thread.start()

    def _consume(self):
        while True:
            record = self._queue.get()
            try:
                if self._disabled:
                    self.dropped += 1  # Already accepted, but no longer writable.
                else:
                    self._sink(record)
            except Exception:
                self.failed += 1
                # Disk full/ENOSPC/denied sink: fail closed for logging only,
                # never repeatedly retry broken writes or block the consumer.
                self._disabled = True
            finally:
                self._queue.task_done()

    def try_emit(self, record: Mapping[str, object], *, level: str = "standard") -> bool:
        """Immediate put_nowait. Validation failures are counted, not leaked."""
        if self.mode == "standard" and level == "debug":
            return False
        try:
            if self._disabled:
                self.dropped += 1
                return False
            cleaned = _clean(record, level)
            cleaned["loss_before"] = {"dropped": self.dropped, "failed": self.failed}
            cleaned["diagnostic_state"] = (
                "inconclusive" if self.dropped or self.failed else "observed")
            self._queue.put_nowait(cleaned)
            self.emitted += 1
            return True
        except queue.Full:
            self.dropped += 1
        except Exception:
            self.failed += 1
        return False

    def counters(self):
        return {"emitted": self.emitted, "dropped": self.dropped,
                "failed": self.failed, "queued": self._queue.qsize(),
                "sink_disabled": self._disabled}

    def sealed_manifest(self):
        if isinstance(self._sink, SegmentedLogSink):
            return self._sink.sealed_manifest()
        return []


_instance: ProductTelemetry | None = None
_instance_lock = threading.Lock()


def get_product_telemetry() -> ProductTelemetry:
    global _instance
    if _instance is None:
        with _instance_lock:
            if _instance is None:
                mode = os.getenv("RKB_PRODUCT_LOG_MODE", "standard")
                if mode not in ("standard", "debug"):
                    mode = "standard"
                _instance = ProductTelemetry(mode=mode,
                    directory=os.getenv("RKB_PRODUCT_LOG_DIR") or "/tmp/rkb-product-telemetry")
    return _instance


def emit_product_event(record: dict[str, object], *, level: str = "standard") -> bool:
    try:
        return get_product_telemetry().try_emit(record, level=level)
    except Exception:
        return False


async def observe_async(operation: str, run: Callable[[], object]):
    """Outer monotonic fetch/read timer, including auth and backend await.

    A cancellation or error is re-raised unchanged. No result payload is read.
    """
    if operation not in ("fetch", "search"):
        raise ValueError("fixed observed operation required")
    started = time.monotonic()
    status = "error"
    trace = uuid4().hex
    try:
        result = await run()
        status = "returned"
        return result
    except asyncio.CancelledError:
        status = "cancelled"
        raise
    finally:
        emit_product_event({"event": "fetch.terminal" if operation == "fetch" else "read.terminal",
                            "phase": "total", "status": status,
                            "trace_id": trace,
                            "duration_ms": (time.monotonic() - started) * 1000})
