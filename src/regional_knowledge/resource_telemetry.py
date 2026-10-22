"""Bounded, lossy resource observations via the shared controller product sink.

This is NOT a domain receipt, writer checkpoint or new persistent log service.
All values are allowlisted numerical observations. Missing measurements stay
unknown rather than being converted to zero or carried from a prior sample.
"""
from __future__ import annotations

import json
import math
import re
import time

# Wire names are shared by the existing heartbeat's latest-diagnostics view
# and this controller-only history adapter. No worker package import needed.
RESOURCE_NUMBERS = frozenset({
    'rss_kib', 'hwm_kib', 'pss_kib', 'cpu_seconds', 'cpu_threads', 'pid',
    'pg_master_rss_kib', 'pg_master_hwm_kib', 'pg_master_pss_kib',
    'pg_master_cpu_seconds', 'pg_master_pid',
    'cgroup_version', 'cpu_quota_us', 'cpu_period_us', 'cpu_usage_usec',
    'cpu_nr_throttled', 'cpu_throttled_usec', 'cpu_usage_delta_seconds',
    'cpu_throttled_delta_seconds', 'cpu_utilization_cores',
    'memory_current_bytes', 'memory_limit_bytes', 'memory_events_high',
    'memory_events_max', 'memory_events_oom', 'memory_events_oom_kill',
    'io_read_bytes', 'io_write_bytes', 'sample_seq', 'sample_time_unix_ms',
    'sampling_failures_total',
})
RESOURCE_INTEGERS = frozenset({
    'pid', 'pg_master_pid', 'cpu_threads', 'cgroup_version', 'cpu_quota_us',
    'cpu_period_us', 'cpu_usage_usec', 'cpu_nr_throttled',
    'cpu_throttled_usec', 'memory_current_bytes', 'memory_limit_bytes',
    'memory_events_high', 'memory_events_max', 'memory_events_oom',
    'memory_events_oom_kill', 'io_read_bytes', 'io_write_bytes',
    'sample_seq', 'sample_time_unix_ms', 'sampling_failures_total',
})
RESOURCE_LATEST = RESOURCE_NUMBERS - {'sample_seq', 'sample_time_unix_ms',
                                      'sampling_failures_total'}
_MAX_BYTES = 4096


def clean_resource_numbers(value):
    """Discard unknown, nonfinite, negative, boolean and out-of-range values."""
    if type(value) is not dict or len(value) > 128:
        return {}
    clean = {}
    for key, number in value.items():
        if key not in RESOURCE_NUMBERS or type(number) not in (int, float):
            continue
        if not math.isfinite(number) or not 0 <= number < 2**63:
            continue
        if key in RESOURCE_INTEGERS and type(number) is not int:
            continue
        if key == 'cgroup_version' and number not in (1, 2):
            continue
        if key in ('sample_seq', 'sample_time_unix_ms') and number < 1:
            continue
        clean[key] = number
    return clean


def emit_resource_sample(collector, diagnostics, *, run_id, incarnation,
                         generation, release=None, received_unix_ms=None):
    """Hand one authenticated worker observation to the shared nonblocking sink.

    The caller has already authenticated and fenced the WSS attachment. This
    adapter cannot grant that authority, mutate domain state, or reject a
    HEARTBEAT. It uses ReadTelemetry's existing finite, dropping queue.
    """
    if collector is None or not callable(getattr(collector, 'try_emit', None)):
        return False
    try:
        if (type(run_id) is not str or not 1 <= len(run_id) <= 256
                or re.fullmatch(r'[A-Za-z0-9_.:-]+', run_id) is None
                or type(incarnation) is not str or not 1 <= len(incarnation) <= 256
                or re.fullmatch(r'[A-Za-z0-9_.:-]+', incarnation) is None
                or type(generation) is not int or not 1 <= generation < 2**63):
            return False
        clean = clean_resource_numbers(diagnostics)
        if not clean.get('sample_seq') or not clean.get('sample_time_unix_ms'):
            return False  # Old heartbeat formats remain supported, not invented.
        if received_unix_ms is None:
            received_unix_ms = int(time.time() * 1000)
        if type(received_unix_ms) is not int or not 1 <= received_unix_ms < 2**63:
            return False
        if release is not None and (type(release) is not str
                or re.fullmatch('[0-9a-f]{64}', release) is None):
            release = None
        dropped = getattr(collector, 'dropped', 0)
        failed = getattr(collector, 'failed', getattr(collector, 'error', 0))
        if type(dropped) is not int or dropped < 0:
            dropped = 0
        if type(failed) is not int or failed < 0:
            failed = 0
        missing = ('cgroup_version' not in clean or 'memory_current_bytes' not in clean)
        inconclusive = bool(missing or clean.get('sampling_failures_total', 0)
                            or dropped or failed)
        event = {
            'event': 'resource_sample', 'schema': 'rkb.resource.sample.v1',
            'plane': 'controller', 'run_id': run_id,
            'runtime_incarnation': incarnation,
            'connection_generation': generation,
            'release_sha256': release,
            'sample_seq': clean.pop('sample_seq'),
            'sample_time_unix_ms': clean.pop('sample_time_unix_ms'),
            'received_unix_ms': received_unix_ms,
            'diagnostic_state': 'inconclusive' if inconclusive else 'observed',
            'scope': 'own_cgroup' if 'cgroup_version' in clean else 'process_only',
            'loss_before': {
                'controller_dropped': dropped,
                'controller_failed': failed,
                'worker_sampling_failed': clean.pop('sampling_failures_total', 0),
            },
            'metrics': clean,
        }
        serialized = json.dumps(event, allow_nan=False, separators=(',', ':'))
        if len(serialized.encode('utf-8')) > _MAX_BYTES:
            return False
        return collector.try_emit(event) is True
    except Exception:
        # Telemetry failure never replaces a signed channel ACK or useful read.
        return False
