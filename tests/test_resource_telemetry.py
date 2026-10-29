"""Offline fake /proc and cgroup evidence; never launch Kaggle or native PG."""
import importlib.util
import json
from pathlib import Path

import pytest

from regional_knowledge.bge_queue import BgeQueue
from regional_knowledge.product_telemetry import ProductTelemetry, SegmentedLogSink, clean_resource_numbers, emit_resource_sample

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
    # A different mounted cgroup must never inherit the old usage baseline.
    write(proc, 'self/cgroup', '0::/another/own\n')
    write(root, 'sys/fs/cgroup/another/own/cpu.stat',
          'usage_usec 4000000\nnr_throttled 1\nthrottled_usec 2000\n')
    clock[0] = 14.0
    moved = sampler.sample()
    assert moved['cpu_usage_usec'] == 4000000
    assert 'cpu_usage_delta_seconds' not in moved


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


def test_sampler_queue_and_actual_product_history_missing_metrics_and_privacy(tmp_path):
    sampler, proc, root = new_sampler(tmp_path)
    cgroup2(proc, root)
    queue = BgeQueue(tmp_path / 'queue.sqlite')
    queue.enqueue('actor', 'one', ['synthetic query'])
    with queue.connect(write=False) as db:
        run_id = db.execute("select id from runs where status='starting'").fetchone()[0]
    token = (queue.path.parent / 'bge-worker-credentials' / run_id).read_text()
    records = []
    file_sink = SegmentedLogSink(tmp_path/'telemetry')
    def sink(record):
        file_sink(record)
        records.append(record)
    telemetry = ProductTelemetry(mode='standard', capacity=4, sink=sink)
    def deliver(sample):
        queue.heartbeat(run_id, token, diagnostics=sample)
        assert emit_resource_sample(telemetry,sample,run_id=run_id,
            incarnation='fixture-incarnation',generation=1,release='a'*64)
        telemetry._queue.join()
        with queue.connect(write=False) as db:
            return json.loads(db.execute('select diagnostics from runs where id=?',(run_id,)).fetchone()[0])
    with pytest.raises(PermissionError):queue.heartbeat(run_id,'wrong-token',diagnostics={})
    first = deliver(dict(sampler.sample(),private_text='private corpus',token='secret',received_unix_ms=1))
    assert first['memory_current_bytes']==10000 and first['memory_events_oom']==1
    assert records[0]['cpu_quota_us']==200000 and records[0]['cpu_period_us']==100000
    assert records[0]['cpu_nr_throttled']==2 and records[0]['cpu_throttled_usec']==50000
    assert records[0]['runtime_incarnation']=='fixture-incarnation' and records[0]['release_sha256']=='a'*64
    assert records[0]['scope']=='own_cgroup' and records[0]['received_unix_ms']!=1
    (root/'sys/fs/cgroup/session/own/memory.current').unlink()
    (root/'sys/fs/cgroup/session/own/cpu.stat').unlink()
    (proc/'1001/smaps_rollup').unlink()
    second = deliver(dict(sampler.sample(),memory_current_bytes=float('nan'),cpu_usage_usec=-1,cpu_throttled_usec=True))
    for field in ('memory_current_bytes','cpu_usage_usec','cpu_throttled_usec','pss_kib'):
        assert field not in second and field not in records[1]
    assert records[1]['status']=='degraded' and records[1]['sample_seq']==2
    assert records[0]['memory_current_bytes']==10000  # History remains a separate observation.
    assert not any(secret in json.dumps([first,second,records]) for secret in ('private corpus','secret','private_text'))
    file_sink.seal()
    history=[json.loads(line) for path in (tmp_path/'telemetry'/'sealed').iterdir() for line in path.read_text().splitlines()]
    assert len(history)==2 and history[0]['cpu_throttled_usec']==50000
    assert clean_resource_numbers({'pid':True,'rss_kib':2**10000,'cgroup_version':3})=={}


def test_owned_pg_pid_uses_actual_restored_candidate_not_unused_empty_owner():
    from types import SimpleNamespace as NS
    worker = worker_module()
    holder = NS(instance=NS(owner=NS(process=NS(pid=12)),restored_database=NS(_candidate=NS(process=NS(pid=34)))))
    class Prepare:
        __self__=holder
    assert worker._owned_pg_master_pid(Prepare())==34
    holder.instance.restored_database=None
    assert worker._owned_pg_master_pid(Prepare())==12
    holder.instance.restored_database=NS(_candidate=NS(process=None))
    assert worker._owned_pg_master_pid(Prepare()) is None
    class DirectRuntime:
        owner=NS(process=NS(pid=56))
        def prepare(self):pass
    assert worker._owned_pg_master_pid(DirectRuntime().prepare)==56


@pytest.mark.parametrize('collector_fails',[False,True])
def test_authenticated_wss_heartbeat_emits_to_existing_product_sink(tmp_path,monkeypatch,collector_fails):
    import threading
    from starlette.applications import Starlette
    from starlette.testclient import TestClient
    from regional_knowledge.bge_channel import channel_route
    from test_bge_channel import Queue, attach, send
    sampler, proc, root = new_sampler(tmp_path)
    cgroup2(proc, root)
    records, observed = [], threading.Event()
    def sink(record):
        records.append(record)
        observed.set()
    telemetry = ProductTelemetry(mode='standard',capacity=2,sink=sink)
    if collector_fails:
        def failed(*args,**kwargs):raise RuntimeError('private collector failure')
        monkeypatch.setattr(telemetry,'try_emit',failed)
    queue = Queue()
    with TestClient(Starlette(routes=[channel_route(queue,product_telemetry=telemetry)])) as app:
        with app.websocket_connect('/bge-worker/channel',headers={'Authorization':'Bearer secret'}) as socket:
            generation = attach(socket)['connection_generation']
            send(socket,generation,'HEARTBEAT',diagnostics=sampler.sample())
            assert socket.receive_json()['type']=='HEARTBEAT_ACK'
            send(socket,generation,'HEARTBEAT',diagnostics=sampler.sample())
            assert socket.receive_json()['type']=='HEARTBEAT_ACK'
            if not collector_fails:assert observed.wait(1)
    assert queue.heartbeats==2
    if collector_fails:
        assert not records
        return
    assert records[0]['cpu_throttled_usec']==50000 and records[0]['connection_generation']==generation
    assert records[0]['run_id']=='run' and records[0]['runtime_incarnation']=='instance'
