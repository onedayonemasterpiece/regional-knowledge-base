"""Offline fake /proc and cgroup evidence; never launch Kaggle or native PG."""
import importlib.util
import json
from pathlib import Path
import threading

import pytest

from regional_knowledge.bge_queue import BgeQueue
from regional_knowledge.read_telemetry import ReadTelemetry
from regional_knowledge.resource_telemetry import clean_resource_numbers, emit_resource_sample

ROOT = Path(__file__).resolve().parents[1]


def worker_module():
    spec = importlib.util.spec_from_file_location(
        'bge_kaggle_worker_resources',
        ROOT / 'scripts' / 'production' / 'bge_kaggle_worker.py')
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def write(root, relative, value):
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value)
    return path


def stat(pid, starttime, *, user=100, system=50):
    fields = ['0'] * 25
    fields[0] = 'S'
    fields[11] = str(user)
    fields[12] = str(system)
    fields[19] = str(starttime)
    return f'{pid} (worker test) ' + ' '.join(fields) + '\n'


def process(proc, pid, *, starttime=100, rss=100, hwm=200, pss=60,
            user=100, system=50):
    write(proc, f'{pid}/stat', stat(pid, starttime, user=user, system=system))
    write(proc, f'{pid}/status', f'Name:\tfixture\nVmRSS:\t{rss} kB\nVmHWM:\t{hwm} kB\n')
    write(proc, f'{pid}/smaps_rollup', f'00400000-0041b000 r--p 0 00:00 0\nPss: {pss} kB\n')


def cgroup2(proc, root, *, usage=1000000, throttle=50000):
    write(proc, 'self/cgroup', '0::/session/own\n')
    write(proc, 'self/mountinfo',
          '29 23 0:25 / /sys/fs/cgroup rw,relatime - cgroup2 cgroup rw\n')
    cg = 'sys/fs/cgroup/session/own/'
    write(root, cg + 'cpu.max', '200000 100000\n')
    write(root, cg + 'cpu.stat',
          f'usage_usec {usage}\nuser_usec 700000\nsystem_usec 300000\n'
          f'nr_throttled 2\nthrottled_usec {throttle}\n')
    write(root, cg + 'memory.current', '10000\n')
    write(root, cg + 'memory.max', '20000\n')
    write(root, cg + 'memory.events', 'low 0\nhigh 0\nmax 3\noom 1\noom_kill 0\n')
    write(root, cg + 'io.stat',
          '8:0 rbytes=100 wbytes=200 rios=1 wios=2\n8:16 rbytes=50 wbytes=80\n')


def new_sampler(tmp_path, *, pid=1001, clock=None):
    proc, root = tmp_path / 'proc', tmp_path / 'fs'
    process(proc, pid)
    sampler = worker_module().ResourceSampler(
        proc_root=proc, path_root=root, pid=pid,
        clock=lambda: clock[0] if clock else 10.0,
        wall_clock=lambda: 1700000000.0, clock_ticks=100)
    return sampler, proc, root


def test_v2_own_cgroup_cpu_deltas_memory_io_and_exact_process_pss(tmp_path):
    clock = [10.0]
    sampler, proc, root = new_sampler(tmp_path, clock=clock)
    process(proc, 2002, starttime=300, rss=2000, pss=400)
    cgroup2(proc, root)
    first = sampler.sample(pg_pid=2002)
    assert first['cgroup_version'] == 2
    assert first['memory_current_bytes'] == 10000  # cgroup, not sum of RSS
    assert first['memory_limit_bytes'] == 20000
    assert first['memory_events_oom'] == 1
    assert first['cpu_quota_us'] == 200000 and first['cpu_period_us'] == 100000
    assert first['cpu_usage_usec'] == 1000000
    assert first['cpu_nr_throttled'] == 2 and first['cpu_throttled_usec'] == 50000
    assert first['io_read_bytes'] == 150 and first['io_write_bytes'] == 280
    assert first['rss_kib'] == 100 and first['pss_kib'] == 60
    assert first['pg_master_rss_kib'] == 2000 and first['pg_master_pss_kib'] == 400
    assert first['cpu_seconds'] == 1.5 and first['pg_master_cpu_seconds'] == 1.5
    assert 'cpu_usage_delta_seconds' not in first  # No fabricated first delta.
    assert 'total_rss_kib' not in first and 'total_pss_kib' not in first
    cgroup2(proc, root, usage=3000000, throttle=70000)
    clock[0] = 12.0
    second = sampler.sample(pg_pid=2002)
    assert second['sample_seq'] == 2
    assert second['cpu_usage_delta_seconds'] == 2.0
    assert second['cpu_throttled_delta_seconds'] == pytest.approx(.02)
    assert second['cpu_utilization_cores'] == pytest.approx(1.0)
    cgroup2(proc, root, usage=100, throttle=50)  # Runtime counter reset.
    clock[0] = 13.0
    reset = sampler.sample()
    assert 'cpu_usage_delta_seconds' not in reset
    assert 'cpu_throttled_delta_seconds' not in reset
    assert len(json.dumps(second).encode()) < 4096


def test_real_v1_controller_mount_cpuacct_and_memory_without_v2_assumption(tmp_path):
    sampler, proc, root = new_sampler(tmp_path)
    write(proc, 'self/cgroup', '5:cpu,cpuacct:/jobs/own\n6:memory:/jobs/own\n')
    write(proc, 'self/mountinfo',
          '29 23 0:22 / /sys/fs/cgroup/cpu rw - cgroup cgroup rw,cpu,cpuacct\n'
          '30 23 0:23 / /sys/fs/cgroup/memory rw - cgroup cgroup rw,memory\n')
    cpu = 'sys/fs/cgroup/cpu/jobs/own/'
    mem = 'sys/fs/cgroup/memory/jobs/own/'
    write(root, cpu + 'cpu.cfs_quota_us', '-1\n')
    write(root, cpu + 'cpu.cfs_period_us', '100000\n')
    write(root, cpu + 'cpu.stat', 'nr_throttled 2\nthrottled_time 5000000\n')
    write(root, cpu + 'cpuacct.usage', '1200000000\n')
    write(root, mem + 'memory.usage_in_bytes', '35000\n')
    write(root, mem + 'memory.limit_in_bytes', '9223372036854771712\n')
    write(root, mem + 'memory.failcnt', '3\n')
    result = sampler.sample()
    assert result['cgroup_version'] == 1
    assert result['cpu_usage_usec'] == 1200000
    assert result['cpu_throttled_usec'] == 5000
    assert result['cpu_period_us'] == 100000
    assert 'cpu_quota_us' not in result and 'memory_limit_bytes' not in result
    assert result['memory_current_bytes'] == 35000
    assert result['memory_events_max'] == 3
    assert 'io_read_bytes' not in result


def test_missing_or_unrecognized_mount_is_unknown_not_host_capacity(tmp_path):
    sampler, proc, root = new_sampler(tmp_path)
    missing = sampler.sample()
    assert missing['rss_kib'] == 100
    assert 'cgroup_version' not in missing
    assert 'memory_current_bytes' not in missing
    write(proc, 'self/cgroup', '0::/unrelated/pod\n')
    write(proc, 'self/mountinfo',
          '29 23 0:25 /not-this-cgroup /sys/fs/cgroup rw - cgroup2 cgroup rw\n')
    unknown = sampler.sample()
    assert 'cgroup_version' not in unknown
    assert 'cpu_usage_usec' not in unknown


def test_denied_smaps_and_missing_pid_remain_unknown_no_heartbeat_exception(tmp_path, monkeypatch):
    sampler, proc, root = new_sampler(tmp_path)
    cgroup2(proc, root)
    path_open = Path.open
    def denied(self, *args, **kwargs):
        if self.name == 'smaps_rollup':
            raise PermissionError('not allowed to read private pages')
        return path_open(self, *args, **kwargs)
    monkeypatch.setattr(Path, 'open', denied)
    current = sampler.sample(pg_pid=9999)
    assert current['rss_kib'] == 100
    assert 'pss_kib' not in current
    assert 'pg_master_rss_kib' not in current
    assert current['sampling_failures_total'] >= 1
    assert 'private pages' not in json.dumps(current)
    assert current['memory_current_bytes'] == 10000


def test_process_pid_reuse_and_mid_read_identity_change(tmp_path, monkeypatch):
    sampler, proc, root = new_sampler(tmp_path)
    process(proc, 2002, starttime=300, rss=200, pss=22)
    first = sampler.sample(pg_pid=2002)
    assert first['pg_master_pid'] == 2002
    process(proc, 2002, starttime=301, rss=2000, pss=222)
    reused = sampler.sample(pg_pid=2002)
    assert not any(k.startswith('pg_master_') for k in reused)
    accepted = sampler.sample(pg_pid=2002)
    assert accepted['pg_master_rss_kib'] == 2000
    # Change starttime between two stat reads, so even the first read isn't trusted.
    original = sampler._stat
    sequence = iter([(301, 1.5), (302, 1.5)])
    monkeypatch.setattr(sampler, '_stat',
                        lambda pid: next(sequence) if pid == 2002 else original(pid))
    changed = sampler.sample(pg_pid=2002)
    assert 'pg_master_pid' not in changed


def test_new_diagnostics_clear_stale_optional_metrics_and_strip_private_payload(tmp_path):
    queue = BgeQueue(tmp_path / 'queue.sqlite')
    queue.enqueue('actor', 'one', ['query'])
    with queue.connect() as db:
        run = dict(db.execute("select * from runs where status='starting'").fetchone())
    run_id = run['id']
    token = (queue.path.parent / 'bge-worker-credentials' / run_id).read_text()
    queue.heartbeat(run_id, token, diagnostics={
        'sample_seq': 1, 'sample_time_unix_ms': 1700000000000, 'rss_kib': 128,
        'pss_kib': 72, 'cpu_usage_usec': 200, 'memory_current_bytes': 10000,
        'cgroup_version': 2, 'private_text': 'should not persist', 'token': 'secret'})
    queue.heartbeat(run_id, token, diagnostics={
        'sample_seq': 2, 'sample_time_unix_ms': 1700000020000,
        'rss_kib': 130, 'pss_kib': None, 'memory_current_bytes': float('nan'),
        'cpu_usage_usec': -1, 'cpu_throttled_usec': True,
        'sampling_failures_total': 1})
    with queue.connect(write=False) as db:
        latest = json.loads(db.execute('select diagnostics from runs where id=?',
                                       (run_id,)).fetchone()[0])
    assert latest['sample_seq'] == 2 and latest['rss_kib'] == 130
    assert latest['sampling_failures_total'] == 1
    assert not any(k in latest for k in ('pss_kib', 'cpu_usage_usec',
                'memory_current_bytes', 'cgroup_version', 'cpu_throttled_usec',
                'private_text', 'token'))
    assert len(json.dumps(latest)) < 4096


def observation(seq=1):
    return {'sample_seq': seq, 'sample_time_unix_ms': 1700000000000 + seq * 20000,
            'cgroup_version': 2, 'memory_current_bytes': 10000, 'cpu_usage_usec': 50000,
            'rss_kib': 100, 'sampling_failures_total': 0,
            'private_text': 'do not log me' * 1000}


def emit(collector, value):
    return emit_resource_sample(collector, value, run_id='fixture-run',
        incarnation='fixture.incarnation', generation=3, release='a' * 64,
        received_unix_ms=1700000000011)


def test_sanitizer_sink_correlation_release_and_no_private_keys():
    records = []
    class Sink:
        dropped = 0
        failed = 0
        def _record(self, raw):
            records.append(json.loads(raw))
    assert emit(Sink(), observation())
    record = records[0]
    assert (record['run_id'], record['runtime_incarnation'],
            record['connection_generation'], record['release_sha256']) == (
            'fixture-run', 'fixture.incarnation', 3, 'a' * 64)
    assert record['sample_seq'] == 1 and record['scope'] == 'own_cgroup'
    assert record['loss_before'] == dict(controller_dropped=0, controller_failed=0,
                                        worker_sampling_failed=0)
    assert 'private_text' not in json.dumps(record)
    assert not emit(Sink(), {'sample_seq': 3})  # Incomplete observation is not real.
    assert clean_resource_numbers({'rss_kib': False, 'memory_current_bytes': -1,
        'sample_seq': float('nan'), 'pid': 17, 'cgroup_version': 7,
        'secret': 'private'}) == {'pid': 17}


def test_full_sink_drops_without_blocking_heartbeat():
    entered, release = threading.Event(), threading.Event()
    def blocked(_):
        entered.set()
        release.wait(2)
    telemetry = ReadTelemetry(capacity=1, sink=blocked)
    try:
        assert emit(telemetry, observation(1))
        assert entered.wait(1)
        assert emit(telemetry, observation(2))
        assert not emit(telemetry, observation(3))
        assert telemetry.dropped == 1
    finally:
        release.set()
        telemetry._queue.join()


def test_failed_sink_is_counted_and_next_sample_reports_loss():
    records = []
    def failed(raw):
        records.append(json.loads(raw))
        raise RuntimeError('sensitive sink exception not in records')
    telemetry = ReadTelemetry(capacity=2, sink=failed)
    assert emit(telemetry, observation(1))
    telemetry._queue.join()
    assert telemetry.failed == 1
    assert emit(telemetry, observation(2))
    telemetry._queue.join()
    assert records[1]['loss_before']['controller_failed'] == 1
    assert records[1]['diagnostic_state'] == 'inconclusive'
    assert 'sensitive' not in json.dumps(records)
