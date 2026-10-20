"""Lossy, bounded read diagnostics; never part of admission or durability."""
from __future__ import annotations

from contextvars import ContextVar
import asyncio
import json
import logging
import math
import queue
import threading
import time
from uuid import uuid4

from .query_telemetry import ReadAttempt

current_trace_id: ContextVar[str | None] = ContextVar("rkb_read_trace", default=None)
_CONTROLLER_STAGES=frozenset({'auth_ms','binding_ms','dispatch_gate_ms','admission_ms',
    'dispatch_to_result_ms','finish_gate_ms','result_to_terminal_ms'})
_STAGES = frozenset({"database_seconds", "queue_seconds", "encoder_seconds",
                     "hydration_seconds", "search_seconds", "total_seconds",
                     "bge_queue_seconds", "bge_encoder_seconds", "fusion_metadata_seconds"})


class ReadTelemetry:
    """One daemon sink per existing server, with nonblocking bounded intake.

    A stuck sink occupies one daemon thread, never a search permit or the event
    loop. Records are disposable on restart. No private text, token, person or
    arbitrary backend metadata is accepted. This is not an operation journal.
    """

    def __init__(self, *, capacity=128, sink=None):
        if type(capacity) is not int or not 1 <= capacity <= 1024:
            raise ValueError("bounded telemetry capacity required")
        self._queue = queue.Queue(maxsize=capacity)
        self._sink = sink or logging.getLogger("regional_knowledge.reads").info
        self._thread = None
        self._start_lock = threading.Lock()
        self.dropped = 0
        self.failed = 0

    def _consume(self):
        while True:
            record = self._queue.get()
            try:
                self._sink(record)
            except Exception:
                self.failed += 1
            finally:
                self._queue.task_done()

    def _record(self, record):
        # Thread startup is once per server; no disk/network work runs here.
        if self._thread is None:
            with self._start_lock:
                if self._thread is None:
                    self._thread = threading.Thread(target=self._consume,
                        name="rkb-read-telemetry", daemon=True)
                    self._thread.start()
        try:
            self._queue.put_nowait(record)
        except queue.Full:
            self.dropped += 1

    def _attempt_record(self, record):
        try:
            record = dict(record, loss_before={'dropped': self.dropped, 'failed': self.failed})
            if self.dropped or self.failed:
                record['diagnostic_state'] = 'inconclusive'
            self._record(json.dumps(record, allow_nan=False, separators=(',', ':')))
        except Exception:
            self.failed += 1

    def begin_attempt(self, operation='knowledge_search', *, trace_id=None, request_id=None):
        """Call before admission/dispatch; the returned request ID is server owned."""
        if operation not in {'search', 'knowledge_search', 'fetch'}:
            raise ValueError('unknown read operation')
        attempt = ReadAttempt(trace_id or current_trace_id.get() or uuid4().hex)
        if request_id is not None:
            if type(request_id) is not str or not 1<=len(request_id)<=256:
                raise ValueError('bounded trusted adapter request identity required')
            object.__setattr__(attempt,'request_id',request_id)
        self._attempt_record({'event': 'read_attempt_started', 'plane': 'controller', 'terminal': False,
            **attempt.correlation(), 'operation': operation, 'diagnostic_state': 'observed'})
        return attempt

    def bind_dispatch(self, attempt, *, runtime, connection_generation, turn_generation):
        """Bind once. A retry or changed attachment needs a new attempt/request ID."""
        if type(attempt) is not ReadAttempt:
            raise TypeError('server-minted ReadAttempt required')
        if not attempt.bind(runtime, connection_generation, turn_generation):
            return False
        self._attempt_record({'event': 'read_attempt_dispatched', 'plane': 'controller', 'terminal': False,
            **attempt.correlation(), **runtime.fields(), 'connection_generation': connection_generation,
            'turn_generation': turn_generation, 'diagnostic_state': 'observed'})
        return True

    def finish_attempt(self, attempt, *, status, reason=None, executed_branches=(), stages_ms=None, controller_stages_ms=None):
        """Emit one terminal observation, including refusal/cancel before dispatch."""
        if type(attempt) is not ReadAttempt:
            raise TypeError('server-minted ReadAttempt required')
        record = attempt.terminal(status, reason, executed_branches, stages_ms or {})
        if record is None:
            return False
        if isinstance(controller_stages_ms,dict):
            record['controller_stages_ms']={key:value for key,value in controller_stages_ms.items()
                if key in _CONTROLLER_STAGES and type(value) in (int,float)
                and math.isfinite(value) and 0<=value<=86400000}
        self._attempt_record(record)
        return True

    async def measure(self, operation, awaitable):
        """Measure backend execution, excluding auth, MCP serialization and I/O."""
        if operation not in {"search", "knowledge_search"}:
            raise ValueError("unknown read operation")
        trace_id = uuid4().hex
        token = current_trace_id.set(trace_id)
        started = time.monotonic()
        result = None
        outcome = "error"
        elapsed_ms = None
        try:
            result = await awaitable
            outcome = "returned"
            elapsed_ms = round((time.monotonic() - started) * 1000, 3)
            # Both existing public DTOs declare this optional diagnostic field.
            from .contracts import ReadTrace
            previous = getattr(result, 'read_trace', None)
            return result.model_copy(update={"read_trace": ReadTrace(
                trace_id=trace_id, backend_elapsed_ms=elapsed_ms,
                attempt_id=previous.attempt_id if previous and previous.trace_id == trace_id else None,
                request_ref=previous.request_ref if previous and previous.trace_id == trace_id else None)})
        except asyncio.CancelledError:
            outcome = "cancelled"
            raise
        finally:
            if elapsed_ms is None:
                elapsed_ms = round((time.monotonic() - started) * 1000, 3)
            record = {"event": "read_completed", "trace_id": trace_id,
                      "operation": operation, "outcome": outcome,
                      "backend_elapsed_ms": elapsed_ms}
            current_trace_id.reset(token)
            try:
                if result is not None:
                    record["mode"] = result.mode
                    record["retrieval_mode"] = result.retrieval_mode
                    record["result_count"] = len(getattr(result, "results",
                                                         getattr(result, "evidence", ())))
                    stages = {}
                    for key in _STAGES:
                        value = result.timings.get(key)
                        if (type(value) in (int, float) and math.isfinite(value)
                                and 0 <= value <= 86400):
                            stages[key] = value
                    record["stages_seconds"] = stages
                self._record(json.dumps(record, allow_nan=False, separators=(",", ":")))
            except Exception:
                self.failed += 1  # Diagnostics cannot replace a result or exception.
