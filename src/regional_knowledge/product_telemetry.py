"""Best-effort product spans, not a domain journal or a provider receipt.

No call into this module may determine search/import results or Saved state.
Only fixed metadata fields are accepted: no query, original, bearer, URL or CoT.
"""
from __future__ import annotations

import asyncio
import atexit
from itertools import count
from collections.abc import Callable, Mapping
from contextvars import ContextVar
import errno
import hashlib
import json
import logging
import math
import os
from pathlib import Path
import queue
import re
import threading
import time
from uuid import uuid4

DEFAULT_DISK_BYTES = 32 * 1024 * 1024
DEFAULT_SEGMENT_BYTES = 256 * 1024
_EVENTS = frozenset({"read.stage", "read.terminal", "fetch.terminal", "command.phase",
                     "import.phase", "import.terminal", "import.call", "resource.sample"})
_PHASES = frozenset({"total", "auth", "admission", "dispatch", "transport", "adapter",
                     "acquire", "verify", "pages", "stage", "seal", "materialize",
                     "activation", "saved", "archive", "encode", "lexical", "dense", "hydrate",
                     "fusion", "serialize", "search", "fetch", "resource", "text", "semantic", "finish", "catalog"})
_STATUSES = frozenset({"started", "ok", "returned", "degraded", "pending", "staged",
                       "applied", "saved", "acknowledged", "rejected", "failed",
                       "unavailable", "cancelled", "error", "dropped"})
_IDENTIFIERS = frozenset({"trace_id", "attempt_id", "run_id", "request_ref", "operation_ref", "ingestion_ref", "source_ref"})
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
    "sampling_failures_total", "received_unix_ms"
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
_IMPORT_NUMBERS = frozenset({'physical_page_index','page_count','region_count','chunk_count',
    'story_candidate_count','unit_marker_count','packet_bytes','image_bytes','batch_index',
    'command_sequence','batch_count','published_batch_count','enriched_batch_count','attempt',
    'call_started_unix_ms','event_time_unix_ms','image_width','image_height','entity_candidate_count',
    'poi_candidate_count','physical_page_end'})
_IMPORT_ENUMS = {
    'tool': frozenset({'book_ingest','book_pages','fetch','search'}),
    'command': frozenset({'start','stage','validate','finalize','status','reprocess','rechunk'}),
    'next_action': frozenset({'continue_pages','continue_tail','validate','finalize','wait','resume_finalize','done','blocker'}),
    'result_state': frozenset({'staged','processing','needs_review','ready','finalized','failed'}),
    'component': frozenset({'stage','text','semantic','catalog','finish'}),
    'result_status': frozenset({'invalid_query','denied','unavailable','pending','unknown','failed','rejected','error'}),
    'error_class': frozenset({'validation','access','domain','transport','internal'}),
}
_import_call = ContextVar('rkb_import_call_telemetry',default=None)


def _digest_label(value):
    return type(value) is str and len(value) == 64 and all(
        char in "0123456789abcdef" for char in value)


def _resource_number(key, value):
    if type(value) not in (int, float) or not 0 <= value < 2**63 or not math.isfinite(value):
        raise ValueError("bounded finite resource reading required")
    if key in _RESOURCE_INTEGER_FIELDS and type(value) is not int:
        raise ValueError("integer resource counter required")
    if key == "cgroup_version" and value not in (1, 2):
        raise ValueError("supported cgroup scope required")
    if key in ("sample_seq", "sample_time_unix_ms", "received_unix_ms") and value < 1:
        raise ValueError("positive sample identity required")
    return value


def clean_resource_numbers(values):
    """Shared heartbeat/history numeric ABI; unavailable observations stay absent."""
    if type(values) is not dict or len(values) > 128:
        return {}
    clean = {}
    for key in _RESOURCE_FIELDS & values.keys():
        try:clean[key] = _resource_number(key, values[key])
        except ValueError:pass
    return clean


def emit_resource_sample(collector, diagnostics, *, run_id, incarnation, generation, release=None):
    if collector is None:return False
    values = clean_resource_numbers(diagnostics)
    if not values.get('sample_seq') or not values.get('sample_time_unix_ms'):return False
    values.pop('received_unix_ms',None)  # Receipt time belongs to this host.
    event = dict(event='resource.sample', phase='resource',
        status='ok' if 'cgroup_version' in values and 'memory_current_bytes' in values else 'degraded',
        run_id=run_id, runtime_incarnation=incarnation, connection_generation=generation,
        scope='own_cgroup' if 'cgroup_version' in values else 'process_only',
        received_unix_ms=int(time.time()*1000), **values)
    if release is not None:event['release_sha256']=release
    try:
        return collector.try_emit(event, level='standard')
    except Exception:
        return False  # Optional history cannot fail the WSS heartbeat.


def _clean(record: Mapping[str, object], level: str):
    resource_extras = {"runtime_incarnation", "release_sha256", "scope"}
    if not isinstance(record, Mapping) or len(record) > 64:
        raise ValueError("bounded telemetry record required")
    is_resource = record.get("event") == "resource.sample"
    is_import = record.get('event') in ('import.phase','import.terminal','import.call','command.phase','fetch.terminal')
    permitted = (_FIELDS | (_RESOURCE_FIELDS | resource_extras if is_resource else frozenset())
        | (_IMPORT_NUMBERS | _IMPORT_ENUMS.keys() | {'has_more','physical_page_indices'} if is_import else frozenset()))
    # Unknown fields are discarded, never serialized (e.g. query/token/url).
    # Do not throw away known numbers because a new optional resource key
    # appeared in another worker version.
    ignored = len(set(record) - permitted)
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
            ignored += 1
            continue
        if key in ("request_ref", "operation_ref", "ingestion_ref", "source_ref") and not _digest_label(value):
            ignored += 1  # Do not write a raw request/operation value.
            continue
        result[key] = value
    if is_import:
        for key in _IMPORT_NUMBERS & record.keys():
            value=record[key]
            if type(value) is not int or not 0 <= value < 2**63:
                ignored += 1
            else: result[key]=value
        for key, allowed in _IMPORT_ENUMS.items():
            if key in record:
                if type(record[key]) is str and record[key] in allowed: result[key]=record[key]
                else: ignored += 1
        if 'has_more' in record:
            if type(record['has_more']) is bool: result['has_more']=record['has_more']
            else: ignored += 1
        if 'physical_page_indices' in record:
            indices=record['physical_page_indices']
            if type(indices) is list and 1<=len(indices)<=4 and all(type(v) is int and 0<=v<2500 for v in indices):
                result['physical_page_indices']=indices
            else: ignored += 1
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
    if is_resource and "runtime_incarnation" in record:
        value = record["runtime_incarnation"]
        if type(value) is str and 1 <= len(value) <= 256 and _OPAQUE.fullmatch(value):
            result["runtime_incarnation"] = value
        else:
            ignored += 1
    if is_resource and "release_sha256" in record:
        value = record["release_sha256"]
        if _digest_label(value):
            result["release_sha256"] = value
        else:
            ignored += 1  # Do not write unverified release strings.
    if is_resource and "scope" in record:
        if record["scope"] not in ("own_cgroup", "process_only"):
            raise ValueError("fixed resource scope only")
        result["scope"] = record["scope"]
    for key in _RESOURCE_FIELDS & set(record) if is_resource else ():
        value = _resource_number(key, record[key])
        result[key] = round(value, 3) if type(value) is float else value
    if ignored:
        result["ignored_fields"] = ignored  # Never include the omitted keys/values.
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
        if self._size >= self.segment_bytes:
            self.seal()

    def seal(self):
        if self._handle is None:
            return None
        self._handle.close()
        target = self.directory / "sealed" / (self._path.stem + ".jsonl")
        os.replace(self._path, target)
        self._handle = None
        self._path = None
        return target

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
        self._seq, self._process_ref = count(1), uuid4().hex
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
                logging.getLogger(__name__).warning(
                    'product_telemetry_sink state=disabled emitted=%s failed=%s dropped=%s',
                    self.emitted,self.failed,self.dropped)
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
            cleaned["seq"], cleaned["process_ref"] = next(self._seq), self._process_ref
            if cleaned["seq"] % 64 == 0: cleaned["counter_snapshot"] = self.counters()
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
                atexit.register(lambda sink=_instance: logging.getLogger(__name__).warning(
                    'product_telemetry_exit emitted=%s dropped=%s failed=%s sink_disabled=%s',
                    sink.emitted,sink.dropped,sink.failed,sink._disabled))
    return _instance


def emit_product_event(record: dict[str, object], *, level: str = "standard") -> bool:
    if os.getenv("RKB_PRODUCT_LOG_MODE") == "off":
        return False
    try:
        return get_product_telemetry().try_emit(record, level=level)
    except Exception:
        return False


def import_metadata(*, ingestion_id=None, source_id=None, operation_key=None, **fields):
    """Request-local metadata only; never retain payloads or authority objects."""
    try:
        record = _import_call.get()
        if record is None: return
        if type(ingestion_id) is str:
            record['ingestion_ref']=hashlib.sha256(ingestion_id.encode()).hexdigest()
        if type(source_id) is str and len(source_id)==36:
            record['source_ref']=hashlib.sha256(source_id.encode()).hexdigest()
        if type(operation_key) is str:
            record['operation_ref']=hashlib.sha256(operation_key.encode()).hexdigest()
        record.update({k:v for k,v in fields.items()
            if k in _IMPORT_NUMBERS and type(v) is int and 0 <= v < 2**63
            or k in _IMPORT_ENUMS and type(v) is str and v in _IMPORT_ENUMS[k]
            or k=='has_more' and type(v) is bool
            or k=='physical_page_indices' and type(v) is list and 1<=len(v)<=4 and all(type(i) is int and 0<=i<2500 for i in v)})
    except Exception: pass


def emit_import_event(*, ingestion_id=None, source_id=None, operation_key=None, **fields):
    """Use the same hashed identities at intake, apply and remote durability."""
    try:
        record={k:v for k,v in (_import_call.get() or {}).items()
            if k in ('trace_id','ingestion_ref','source_ref','tool','command')}
        record.update(fields)
        record['event_time_unix_ms']=int(time.time()*1000)
        if type(ingestion_id) is str:
            record['ingestion_ref']=hashlib.sha256(ingestion_id.encode()).hexdigest()
        if type(source_id) is str and len(source_id)==36:
            record['source_ref']=hashlib.sha256(source_id.encode()).hexdigest()
        if type(operation_key) is str:
            record['operation_ref']=hashlib.sha256(operation_key.encode()).hexdigest()
        return emit_product_event(record)
    except Exception:
        return False


async def observe_async(operation: str, run: Callable[[], object], *, arguments=None):
    """SDK dispatch through output binding, not HTTP delivery or model time."""
    if operation not in ('fetch','search','book_ingest','book_pages'):
        raise ValueError('fixed observed operation required')
    if os.getenv('RKB_PRODUCT_LOG_MODE') == 'off': return await run()
    event=('fetch.terminal' if operation=='fetch' and not (isinstance(arguments,dict) and arguments.get('source_id'))
        else 'read.terminal' if operation=='search' else 'import.call')
    record={'event':event,'phase':'total','status':'error',
            'tool':operation,'trace_id':uuid4().hex,'call_started_unix_ms':int(time.time()*1000)}
    token=_import_call.set(record)
    started=time.monotonic()
    try:
        if isinstance(arguments,dict):
            import_metadata(ingestion_id=arguments.get('ingestion_id'),source_id=arguments.get('source_id'),
                command=arguments.get('command'),physical_page_index=arguments.get('page_start'),
                physical_page_end=arguments.get('page_end'))
        result=await run()
        record['status']='returned'
        if getattr(result,'is_error',False) or getattr(result,'isError',False): record['status']='error'
        return result
    except asyncio.CancelledError:
        record['status']='cancelled'
        raise
    except Exception as error:
        # Read only fixed classifications from the already-normalized envelope.
        # Exception text/input, validation details and arbitrary reasons stay out.
        try:
            from mcp.server.mcpserver.exceptions import ToolError
            if isinstance(error,ToolError):
                text=str(error)
                if len(text)<=1024 and '{' in text:
                    value=json.loads(text[text.index('{'):text.rindex('}')+1])
                    import_metadata(result_status=value.get('status'),error_class=value.get('error_class'))
        except Exception: pass
        raise
    finally:
        record['duration_ms']=(time.monotonic()-started)*1000
        record['event_time_unix_ms']=int(time.time()*1000)
        try: emit_product_event(record)
        except Exception: pass
        finally: _import_call.reset(token)
