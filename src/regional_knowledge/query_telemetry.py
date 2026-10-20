"""Lossy per-attempt correlation on the existing finite read collectors.

No query/actor/grant/credential data or arbitrary exception text is accepted.
Timings overlap: total, search and their component stages must never be summed.
"""
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from hashlib import sha256
import asyncio
import re
import threading
import time
from uuid import uuid4

from .kaggle_protocol import ReadIdentity

_STAGES = frozenset({'tokenize_ms', 'acquire_ms', 'encode_ms', 'lexical_ms', 'dense_ms',
                     'hydrate_ms', 'search_ms', 'serialize_ms', 'total_ms', 'adapter_ms',
                     'admission_ms', 'transport_ms', 'output_gate_ms'})
_STATUSES = frozenset({'ok', 'degraded', 'unavailable', 'cancelled', 'invalid_query', 'denied', 'error'})
_REASONS = frozenset({'dense_unavailable', 'projection_lag', 'deadline_exceeded', 'not_ready',
    'disconnected', 'caller_cancelled', 'consumer_cancelled', 'turn_replaced', 'query_too_large',
    'invalid_filters', 'invalid_input', 'token_limit_exceeded', 'access_denied', 'prior_write_pending',
    'context_gap', 'evidence_withdrawn', 'internal_error', 'bounded_retrieval', 'capacity_wait',
    'read_capacity', 'person_read_capacity', 'dialog_read_capacity', 'output_gate_denied',
    'hybrid_search_required'})
_RUNTIME = ContextVar('rkb_read_runtime_diagnostics', default=None)
_NATIVE = ContextVar('rkb_native_read_diagnostics', default=None)


def _opaque(value):
    return isinstance(value, str) and re.fullmatch(r'[A-Za-z0-9_.:-]{1,256}', value) is not None


def _generation(value, minimum):
    return type(value) is int and minimum <= value < 2**63


def stages_ms(values):
    if not isinstance(values, dict):
        return {}
    import math
    return {key: value for key, value in values.items() if key in _STAGES
            and type(value) in (int, float) and math.isfinite(value) and 0 <= value <= 86400000}


def clean_native_read_events(values, *, request_id, run_id, runtime_incarnation,
                             connection_generation, turn_generation):
    """Fixed optional diagnostics, never a result/authority or payload channel."""
    import json
    if (type(values) is not list or len(values)>4
            or len(json.dumps(values,allow_nan=False,separators=(',',':')).encode())>8192):
        raise ValueError('bounded native read events required')
    identity=dict(request_ref=sha256(request_id.encode()).hexdigest(),run_id=run_id,
        runtime_incarnation=runtime_incarnation,connection_generation=connection_generation,turn_generation=turn_generation)
    clean=[]
    for value in values:
        kinds={'kaggle_read_completed':('executor_stage',False),
               'kaggle_read_adapter_terminal':('adapter',True)}
        if type(value) is not dict or value.get('event') not in kinds:
            raise ValueError('fixed native read event required')
        event=value['event'];plane,terminal=kinds[event]
        allowed={'event','plane','terminal',*identity,'diagnostic_state','executed_branches',
            'stages_ms','timing_semantics','loss_before'}|({'status','reason'} if terminal else set())
        if (set(value)-allowed or any(value.get(k)!=v for k,v in identity.items())
                or value.get('plane')!=plane or value.get('terminal') is not terminal
                or value.get('diagnostic_state') not in ('observed','inconclusive')
                or value.get('timing_semantics')!='overlapping_do_not_sum'
                or type(value.get('stages_ms')) is not dict
                or stages_ms(value['stages_ms'])!=value['stages_ms']
                or not isinstance(value.get('executed_branches'),(tuple,list))
                or tuple(value['executed_branches'])!=branches(value['executed_branches'])):
            raise ValueError('native read identity or timing contract differs')
        loss=value.get('loss_before',{})
        if (type(loss) is not dict or set(loss)-{'emitted','dropped','failed'}
                or any(type(v) is not int or not 0<=v<2**63 for v in loss.values())):
            raise ValueError('native read loss counters differ')
        record=dict(event=event,plane=plane,terminal=terminal,**identity,
            diagnostic_state=value['diagnostic_state'],executed_branches=list(value['executed_branches']),
            stages_ms=dict(value['stages_ms']),timing_semantics='overlapping_do_not_sum',loss_before=dict(loss))
        if terminal:
            if outcome(value.get('status'),value.get('reason'))!=(value.get('status'),value.get('reason')):
                raise ValueError('fixed native outcome required')
            record.update(status=value['status'],reason=value.get('reason'))
        clean.append(record)
    return clean


def drain_native_read_events(collector, frame):
    """Drain finite diagnostics only; old/cancelled requests cannot join a new one."""
    reference=sha256(str(frame.get('request_id','')).encode()).hexdigest();result=[]
    telemetry_counts(collector)
    for _ in range(64):
        try:value=collector.get_nowait()
        except asyncio.QueueEmpty:break
        collector.task_done()
        if (value.get('request_ref')==reference and all(value.get(k)==frame.get(k)
                for k in ('connection_generation','turn_generation')) and len(result)<4):result.append(value)
        else:collector._rkb_read_event_counts['dropped']+=1
    counts=telemetry_counts(collector)
    if counts['dropped'] or counts['failed']:
        result=[dict(value,diagnostic_state='inconclusive',loss_before=counts) for value in result]
    return result


def branches(values):
    present = set(values)
    return tuple(name for name in ('lexical', 'dense') if name in present)


def outcome(status, reason):
    # Arbitrary messages never become diagnostic labels.
    return (status if isinstance(status, str) and status in _STATUSES else 'error',
            reason if reason is None or isinstance(reason, str) and reason in _REASONS else 'internal_error')


@dataclass(frozen=True)
class ReadRuntime:
    """Existing controller run/incarnation labels; never pass tokens/claims here."""
    run_id: str
    runtime_incarnation: str

    def __post_init__(self):
        if not _opaque(self.run_id) or not _opaque(self.runtime_incarnation):
            raise ValueError('bounded internal runtime labels required')

    def fields(self):
        return {'run_id': self.run_id, 'runtime_incarnation': self.runtime_incarnation}


@contextmanager
def bind_read_runtime(runtime):
    if type(runtime) is not ReadRuntime:
        raise TypeError('internal ReadRuntime required')
    token = _RUNTIME.set(runtime)
    try:
        yield
    finally:
        _RUNTIME.reset(token)


@dataclass(frozen=True)
class ReadAttempt:
    """One server-minted attempt. Retry/reconnect needs a new instance/request ID."""
    trace_id: str
    attempt_id: str = field(default_factory=lambda: uuid4().hex, init=False)
    request_id: str = field(default_factory=lambda: uuid4().hex, init=False, repr=False)
    started: float = field(default_factory=time.monotonic, init=False, repr=False)
    _lock: object = field(default_factory=threading.Lock, init=False, repr=False, compare=False)
    _dispatch: object = field(default=None, init=False, repr=False, compare=False)
    _finished: bool = field(default=False, init=False, repr=False, compare=False)
    _ambiguous: bool = field(default=False, init=False, repr=False, compare=False)

    def __post_init__(self):
        if not isinstance(self.trace_id, str) or re.fullmatch('[0-9a-f]{32}', self.trace_id) is None:
            raise ValueError('server logical trace ID required')

    @property
    def request_ref(self):
        return sha256(self.request_id.encode()).hexdigest()

    def correlation(self):
        return {'trace_id': self.trace_id, 'attempt_id': self.attempt_id, 'request_ref': self.request_ref}

    def bind(self, runtime, connection_generation, turn_generation):
        if (type(runtime) is not ReadRuntime or not _generation(connection_generation, 1)
                or not _generation(turn_generation, 0)):
            raise ValueError('exact internal dispatch scope required')
        value = (runtime, connection_generation, turn_generation)
        with self._lock:
            if self._finished or self._dispatch is not None and self._dispatch != value:
                object.__setattr__(self, '_ambiguous', True)
                return False
            object.__setattr__(self, '_dispatch', value)
            return True

    def terminal(self, status, reason, executed_branches, timings):
        with self._lock:
            if self._finished:
                return None
            object.__setattr__(self, '_finished', True)
            dispatched, ambiguous = self._dispatch, self._ambiguous
        status, reason = outcome(status, reason)
        record = {'event': 'read_attempt_terminal', 'plane': 'controller', 'terminal': True,
            **self.correlation(), 'status': status, 'reason': reason,
            'backend_elapsed_ms': (time.monotonic() - self.started) * 1000,
            'executed_branches': branches(executed_branches), 'stages_ms': stages_ms(timings),
            'timing_semantics': 'overlapping_do_not_sum',
            'dispatch_state': 'dispatched' if dispatched else 'not_dispatched',
            'diagnostic_state': 'inconclusive' if ambiguous else 'observed'}
        if dispatched:
            runtime, connection, turn = dispatched
            record.update(runtime.fields(), connection_generation=connection, turn_generation=turn)
        return record


def telemetry_counts(collector):
    """Advisory loss counters, never a domain receipt or proof of completeness."""
    value = getattr(collector, '_rkb_read_event_counts', None)
    if value is None:
        value = {'emitted': 0, 'dropped': 0, 'failed': 0}
        setattr(collector, '_rkb_read_event_counts', value)
    return dict(value)


def _offer(collector, record):
    if collector is None:
        return
    try:
        if not isinstance(collector, asyncio.Queue) or collector.maxsize <= 0:
            return
        counts = telemetry_counts(collector)
        record = dict(record, loss_before=dict(counts))
        if counts['dropped'] or counts['failed']:
            record['diagnostic_state'] = 'inconclusive'
        collector.put_nowait(record)
        collector._rkb_read_event_counts['emitted'] += 1
    except asyncio.QueueFull:
        collector._rkb_read_event_counts['dropped'] += 1
    except Exception:
        try:
            collector._rkb_read_event_counts['failed'] += 1
        except Exception:
            pass


class NativeReadTelemetry:
    """A callback invocation, not authority, retry state or a new collector."""
    def __init__(self, collector, frame):
        self.collector, self.runtime = collector, _RUNTIME.get()
        self.started, self.stages = time.monotonic(), {}
        self.identity = None
        try:
            if type(frame) is dict:
                identity = ReadIdentity.model_validate({key: frame.get(key) for key in (
                    'request_id', 'connection_generation', 'turn_generation')})
                if len(identity.request_id.encode()) <= 256:
                    self.identity = identity
        except (TypeError, ValueError):
            pass
        self.token = None

    def __enter__(self):
        self.token = _NATIVE.set(self)
        return self

    def __exit__(self, *unused):
        _NATIVE.reset(self.token)

    def fields(self):
        value = {'request_ref': None, 'connection_generation': None, 'turn_generation': None,
                 'diagnostic_state': 'inconclusive' if self.runtime is None or self.identity is None else 'observed'}
        if self.identity is not None:
            value.update(request_ref=sha256(self.identity.request_id.encode()).hexdigest(),
                connection_generation=self.identity.connection_generation, turn_generation=self.identity.turn_generation)
        if self.runtime is not None:
            value.update(self.runtime.fields())
        return value

    def finish(self, result=None, *, status='error', reason='internal_error'):
        if isinstance(result, dict):
            status, reason = result.get('status'), result.get('reason')
        status, reason = outcome(status, reason)
        _offer(self.collector, {'event': 'kaggle_read_adapter_terminal', 'plane': 'adapter', 'terminal': True,
            **self.fields(), 'status': status, 'reason': reason,
            'executed_branches': branches(name for name in ('lexical', 'dense') if name + '_ms' in self.stages),
            'stages_ms': {**self.stages, 'adapter_ms': (time.monotonic() - self.started) * 1000},
            'timing_semantics': 'overlapping_do_not_sum'})


def emit_executor_stage(collector, query, values):
    """Stage observations are explicitly not the adapter's final outcome."""
    scope = _NATIVE.get()
    clean = stages_ms(values)
    if scope is not None:
        scope.stages = clean
        fields = scope.fields()
    else:
        identity = query.model_dump(include={'request_id', 'connection_generation', 'turn_generation'})
        with NativeReadTelemetry(collector, identity) as standalone:
            fields = standalone.fields()
    _offer(collector, {'event': 'kaggle_read_completed', 'plane': 'executor_stage', 'terminal': False,
        **fields, 'executed_branches': branches(name for name in ('lexical', 'dense') if name + '_ms' in clean),
        'stages_ms': clean, 'timing_semantics': 'overlapping_do_not_sum'})
