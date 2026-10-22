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
# Latest-diagnostics values never survive a new observation when missing.
RESOURCE_LATEST = RESOURCE_NUMBERS - {'sample_seq', 'sample_time_unix_ms',
                                      'sampling_failures_total'}

# The Trassa shared ProductTelemetry has one flat bounded numeric allowlist.
# Units are explicit; no host/notebook capacity is inferred. New fields must be
# admitted exclusively by its resource.sample schema, not by generic records.
_KIB_TO_BYTES = {
    'rss_kib': 'rss_bytes',
    'hwm_kib': 'hwm_bytes',
    'pss_kib': 'pss_bytes',
    'pg_master_rss_kib': 'pg_master_rss_bytes',
    'pg_master_hwm_kib': 'pg_master_hwm_bytes',
    'pg_master_pss_kib': 'pg_master_pss_bytes',
}
_DIRECT_METRICS = {
    'cpu_seconds': 'cpu_seconds',
    'pid': 'worker_pid',
    'pg_master_cpu_seconds': 'pg_master_cpu_seconds',
    'pg_master_pid': 'pg_master_pid',
    'cgroup_version': 'cgroup_version',
    'cpu_quota_us': 'cpu_quota_us',
    'cpu_period_us': 'cpu_period_us',
    'cpu_usage_usec': 'cpu_usage_usec',
    'cpu_nr_throttled': 'cpu_nr_throttled',
    'cpu_throttled_usec': 'cpu_throttled_usec',
    'cpu_usage_delta_seconds': 'cpu_usage_delta_seconds',
    'cpu_throttled_delta_seconds': 'cpu_throttled_delta_seconds',
    'cpu_utilization_cores': 'cpu_utilization_cores',
    'memory_current_bytes': 'cgroup_memory_current_bytes',
    'memory_limit_bytes': 'cgroup_memory_limit_bytes',
    'memory_events_high': 'cgroup_memory_events_high',
    'memory_events_max': 'cgroup_memory_events_max',
    'memory_events_oom': 'cgroup_memory_events_oom',
    'memory_events_oom_kill': 'cgroup_memory_events_oom_kill',
    'io_read_bytes': 'io_read_bytes',
    'io_write_bytes': 'io_write_bytes',
}
_MAX_BYTES = 1600  # Shared file sink permits 2048 bytes including its envelope.


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
    """Send fixed numeric fields via ONE shared ProductTelemetry.try_emit queue.

    Caller owns authenticated WSS attachment; this lossy observation cannot
    change ACK, admission, domain receipts or read results. A missing sink or
    standard-mode debug filter simply drops telemetry without a second writer.
    """
    if collector is None or not callable(getattr(collector, 'try_emit', None)):
        return False
    try:
        if (type(run_id) is not str or not 1 <= len(run_id) <= 128
                or re.fullmatch(r'[A-Za-z0-9_.:-]+', run_id) is None
                or type(incarnation) is not str or not 1 <= len(incarnation) <= 128
                or re.fullmatch(r'[A-Za-z0-9_.:-]+', incarnation) is None
                or type(generation) is not int or not 1 <= generation < 2**63):
            return False
        values = clean_resource_numbers(diagnostics)
        if not values.get('sample_seq') or not values.get('sample_time_unix_ms'):
            return False
        if received_unix_ms is None:
            received_unix_ms = int(time.time() * 1000)
        if type(received_unix_ms) is not int or not 1 <= received_unix_ms < 2**63:
            return False
        event = {
            'event': 'resource.sample', 'phase': 'resource',
            'status': ('degraded' if values.get('sampling_failures_total', 0)
                       or 'cgroup_version' not in values
                       or 'memory_current_bytes' not in values else 'ok'),
            'run_id': run_id, 'runtime_incarnation': incarnation,
            'connection_generation': generation,
            'sample_seq': values['sample_seq'],
            'sample_time_unix_ms': values['sample_time_unix_ms'],
            'received_unix_ms': received_unix_ms,
            'worker_sampling_failed_total': values.get('sampling_failures_total', 0),
        }
        if type(release) is str and re.fullmatch('[0-9a-f]{64}', release):
            event['release_sha256'] = release
        for key, destination in _KIB_TO_BYTES.items():
            number = values.get(key)
            if number is not None and number < 2**53:
                event[destination] = number * 1024
        for key, destination in _DIRECT_METRICS.items():
            if key in values:
                event[destination] = values[key]
        # A small flat product record fits the shared sink's hard JSONL bound.
        if len(json.dumps(event, allow_nan=False, separators=(',', ':')).encode()) > _MAX_BYTES:
            return False
        return collector.try_emit(event, level='debug') is True
    except Exception:
        return False  # Lossy telemetry never replaces an authorized response.
