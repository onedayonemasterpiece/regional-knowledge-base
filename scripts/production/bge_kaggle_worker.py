"""Private Kaggle CPU worker template. Controller substitutes only RUN_CONFIG.

Legacy execution encodes bounded jobs only. Explicit prepared target factories
can attach the owned Kaggle PG pool after control; no arbitrary code is accepted.
"""
import os
os.environ['CUDA_VISIBLE_DEVICES']=''
os.environ['TOKENIZERS_PARALLELISM']='false'
os.environ['OMP_NUM_THREADS']='4'
os.environ['MKL_NUM_THREADS']='4'
import json
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

RUN_CONFIG = {}  # Replaced in the private generated notebook, never committed.
REVISION = '5617a9f61b028005a4858fdac845db406aefb181'
SPACE = 'bge-m3:5617a9f:t211-tr5161:cls-l2-512:q1-d1:v1'
_MODEL_LOAD_COUNT = 0

def load_encoder():
    prepared = RUN_CONFIG.get('prepared_model')
    artifacts = None
    if RUN_CONFIG.get('stage0') is not None:
        from regional_knowledge.kaggle_stage0 import prepare_stage0
        prepare_stage0(prepared,RUN_CONFIG['stage0'],run_id=RUN_CONFIG['run_id'])
    if prepared is not None:
        from regional_knowledge.kaggle_bootstrap import PreparedArtifact, verify_artifacts, ResidentEncoder
        from regional_knowledge.kaggle_readiness import ArtifactRequirement
        if not isinstance(prepared, dict) or set(prepared) != {'root', 'entries', 'max_total_bytes'}:
            raise ValueError('explicit prepared model inventory required')
        entries = tuple(PreparedArtifact(ArtifactRequirement(e['name'], e['size'], e['sha256']), e['role'])
                        for e in prepared['entries'])
        artifacts = verify_artifacts(prepared['root'], entries, max_total_bytes=prepared['max_total_bytes'])
    if artifacts is not None:
        from regional_knowledge.kaggle_environment import bootstrap_stage
        with bootstrap_stage(RUN_CONFIG['run_id'],'model_tokenizer_load'):
            return _load_model(artifacts)
    return _load_model(artifacts)


def _load_model(artifacts):
    global _MODEL_LOAD_COUNT
    import torch
    import importlib.metadata
    versions={name:importlib.metadata.version(name) for name in ('torch','transformers','tokenizers','huggingface-hub')}
    if versions!={'torch':'2.11.0+cpu','transformers':'5.16.1','tokenizers':'0.23.1','huggingface-hub':'1.29.0'}:
        raise RuntimeError('pinned BGE CPU runtime mismatch')
    from transformers import AutoModel,AutoTokenizer
    torch.set_num_threads(4);torch.set_num_interop_threads(1)
    if artifacts is None:
        tokenizer=AutoTokenizer.from_pretrained('BAAI/bge-m3',revision=REVISION)
        model=AutoModel.from_pretrained('BAAI/bge-m3',revision=REVISION,torch_dtype=torch.float32).to('cpu').eval()
    else:
        model_path = artifacts.model_directory
        tokenizer=AutoTokenizer.from_pretrained(str(model_path),use_fast=True,
            tokenizer_file=str(model_path/'tokenizer.json'),local_files_only=True,trust_remote_code=False)
        model=AutoModel.from_pretrained(str(model_path),torch_dtype=torch.float32,
            local_files_only=True,trust_remote_code=False).to('cpu').eval()
    _MODEL_LOAD_COUNT += 1
    if model.config.hidden_size!=1024:raise RuntimeError('BGE dimension mismatch')
    def encode(text):
        # Same no-prefix CLS/L2 FP32 batch1 contract for query and document.
        tokens=tokenizer([text],padding=True,truncation=False,return_tensors='pt')
        if tokens['input_ids'].shape[1]>512:raise ValueError('BGE exact input exceeds 512 tokens')
        with torch.inference_mode():
            hidden=model(**tokens).last_hidden_state[:,0,:]
            result=torch.nn.functional.normalize(hidden,p=2,dim=1).float()[0].tolist()
        return result
    encode('readiness')
    if artifacts is not None:
        from regional_knowledge.kaggle_bootstrap import ResidentEncoder
        from regional_knowledge.kaggle_environment import bootstrap_stage
        with bootstrap_stage(RUN_CONFIG['run_id'],'tokenizer_guard'):
            return ResidentEncoder(encode,tokenizer,artifacts,artifacts.model_directory,_MODEL_LOAD_COUNT)
    return encode


class ResourceSampler:
    """Read only this notebook's mounted /proc and own cgroup, without sysstat.

    Totals for the cgroup include shared pages once. PG/BGE RSS and PSS are
    *separate* process observations and must never be added as a usage total.
    A PID must have matching /proc/stat starttime before/after each read; an
    observed PID reuse omits one sample instead of attributing the old process.
    """

    def __init__(self, *, proc_root='/proc', path_root='/', pid=None,
                 clock=None, wall_clock=None, clock_ticks=None):
        self.proc_root = Path(proc_root)
        self.path_root = Path(path_root)
        self.pid = os.getpid() if pid is None else pid
        self.clock = clock or time.monotonic
        self.wall_clock = wall_clock or time.time
        self.clock_ticks = clock_ticks or os.sysconf('SC_CLK_TCK')
        self.seq = 0
        self.failures = 0
        self._process_start = {}
        self._cpu_prior = {}

    def _read(self, path, *, limit=16384):
        try:
            with Path(path).open('rb') as stream:
                data = stream.read(limit + 1)
            if len(data) > limit:
                self.failures += 1
                return None
            return data.decode('ascii', errors='strict')
        except FileNotFoundError:
            return None
        except (OSError, UnicodeError, ValueError):
            self.failures += 1
            return None

    @staticmethod
    def _number(text):
        if not text or not text.isascii() or not text.isdecimal():
            return None
        value = int(text)
        return value if 0 <= value < 2**63 else None

    def _fields(self, text):
        if text is None:
            return {}
        result = {}
        for line in text.splitlines()[:128]:
            parts = line.split()
            if len(parts) == 2:
                value = self._number(parts[1])
                if value is not None:
                    result[parts[0].rstrip(':')] = value
        return result

    def _stat(self, pid):
        text = self._read(self.proc_root / str(pid) / 'stat', limit=8192)
        if text is None:
            return None
        # comm is parenthesized and can contain spaces and ')' characters.
        end = text.rfind(')')
        if end < 0 or not text.startswith(str(pid) + ' ('):
            return None
        fields = text[end+1:].split()
        if len(fields) < 20:
            return None
        start = self._number(fields[19])  # Linux field 22
        utime = self._number(fields[11])  # Linux field 14
        stime = self._number(fields[12])  # Linux field 15
        if start is None or start == 0 or utime is None or stime is None:
            return None
        return start, (utime + stime) / self.clock_ticks

    def _process(self, pid, prefix=''):
        if type(pid) is not int or not 1 <= pid < 2**31:
            return {}
        before = self._stat(pid)
        if before is None:
            return {}
        status = self._read(self.proc_root / str(pid) / 'status')
        pss = self._read(self.proc_root / str(pid) / 'smaps_rollup')
        after = self._stat(pid)
        if after is None or before[0] != after[0]:
            return {}
        prior = self._process_start.get((prefix, pid))
        self._process_start[(prefix, pid)] = before[0]
        if prior is not None and prior != before[0]:
            return {}  # Reused PID: an entire observation stays unknown.
        result = {prefix + 'cpu_seconds': after[1]}
        mapping = {'VmRSS': prefix + 'rss_kib', 'VmHWM': prefix + 'hwm_kib'}
        if status is not None:
            for line in status.splitlines()[:200]:
                parts = line.split()
                if len(parts) == 3 and parts[0].rstrip(':') in mapping and parts[2] == 'kB':
                    value = self._number(parts[1])
                    if value is not None:
                        result[mapping[parts[0].rstrip(':')]] = value
        if pss is not None:
            for line in pss.splitlines()[:200]:
                parts = line.split()
                if len(parts) == 3 and parts[0] == 'Pss:' and parts[2] == 'kB':
                    value = self._number(parts[1])
                    if value is not None:
                        result[prefix + 'pss_kib'] = value
                    break
        result[prefix + 'pid'] = pid
        return result

    def _mounts(self):
        groups = {}
        cg = self._read(self.proc_root / 'self' / 'cgroup', limit=8192)
        info = self._read(self.proc_root / 'self' / 'mountinfo', limit=32768)
        if cg is None or info is None:
            return []
        for line in cg.splitlines()[:64]:
            parts = line.split(':', 2)
            if len(parts) != 3 or not parts[2].startswith('/'):
                continue
            for name in (parts[1].split(',') if parts[1] else ('v2',)):
                if name in ('v2', 'cpu', 'cpuacct', 'memory', 'blkio'):
                    groups[name] = parts[2]
        mounts = []
        for line in info.splitlines()[:256]:
            if ' - ' not in line:
                continue
            left, right = line.split(' - ', 1)
            head, tail = left.split(), right.split()
            if len(head) < 5 or len(tail) < 3 or tail[0] not in ('cgroup2', 'cgroup'):
                continue
            root, mountpoint = head[3], head[4]
            if not root.startswith('/') or not mountpoint.startswith('/'):
                continue
            names = ('v2',) if tail[0] == 'cgroup2' else tuple(
                name for name in tail[2].split(',') if name in ('cpu', 'cpuacct', 'memory', 'blkio'))
            for name in names:
                membership = groups.get(name)
                if membership is None:
                    continue
                # Match the recognized mount *root*, never guess host root.
                prefix = root.rstrip('/') + '/'
                if membership != root and not membership.startswith(prefix):
                    continue
                relative = membership[len(root):].lstrip('/') if root != '/' else membership.lstrip('/')
                if any(part in ('.', '..') for part in relative.split('/')):
                    continue
                if '\\' in mountpoint or '\\' in root:
                    continue  # Refuse unhandled mountinfo path escaping.
                mounts.append((name, self.path_root / mountpoint.lstrip('/') / relative))
        return mounts

    def _sample_cgroup(self, now):
        mounts = self._mounts()
        observed = {}
        v2 = next((path for name, path in mounts if name == 'v2'), None)
        if v2 is not None:
            cpu = self._fields(self._read(v2 / 'cpu.stat'))
            maximum = self._read(v2 / 'cpu.max', limit=128)
            memory = self._number((self._read(v2 / 'memory.current', limit=128) or '').strip())
            memmax = (self._read(v2 / 'memory.max', limit=128) or '').strip()
            events = self._fields(self._read(v2 / 'memory.events'))
            observed['cgroup_version'] = 2
            if maximum:
                parts = maximum.split()
                if len(parts) == 2:
                    quota = self._number(parts[0]) if parts[0] != 'max' else None
                    period = self._number(parts[1])
                    if quota is not None: observed['cpu_quota_us'] = quota
                    if period is not None and period > 0: observed['cpu_period_us'] = period
            for source, dest in (('usage_usec','cpu_usage_usec'),
                                 ('nr_throttled','cpu_nr_throttled'),
                                 ('throttled_usec','cpu_throttled_usec')):
                if source in cpu: observed[dest] = cpu[source]
            if memory is not None: observed['memory_current_bytes'] = memory
            limit = self._number(memmax) if memmax != 'max' else None
            if limit is not None: observed['memory_limit_bytes'] = limit
            for name in ('high', 'max', 'oom', 'oom_kill'):
                if name in events: observed['memory_events_' + name] = events[name]
            io = self._read(v2 / 'io.stat', limit=8192)
            if io is not None:
                read = written = 0
                found_r = found_w = False
                for line in io.splitlines()[:32]:
                    parts = line.split()
                    if not parts or ':' not in parts[0]:
                        continue
                    for entry in parts[1:]:
                        key, sep, value_text = entry.partition('=')
                        value = self._number(value_text) if sep else None
                        if value is None: continue
                        if key == 'rbytes': read += value; found_r = True
                        if key == 'wbytes': written += value; found_w = True
                if found_r and read < 2**63: observed['io_read_bytes'] = read
                if found_w and written < 2**63: observed['io_write_bytes'] = written
        else:
            # v1 ONLY when real /proc/self/mountinfo and controller membership
            # declare a known cpu/cpuacct or memory mount.
            lookup = {name: path for name, path in mounts if name != 'v2'}
            if not any(k in lookup for k in ('cpu', 'cpuacct', 'memory')):
                return observed
            observed['cgroup_version'] = 1
            cpu_dir = lookup.get('cpu')
            if cpu_dir is not None:
                for filename, name in (('cpu.cfs_quota_us','cpu_quota_us'),
                                       ('cpu.cfs_period_us','cpu_period_us')):
                    text = (self._read(cpu_dir / filename, limit=128) or '').strip()
                    value = self._number(text)  # -1 unlimited -> unknown
                    if value is not None: observed[name] = value
                stats = self._fields(self._read(cpu_dir / 'cpu.stat'))
                if 'nr_throttled' in stats: observed['cpu_nr_throttled'] = stats['nr_throttled']
                if 'throttled_time' in stats:
                    observed['cpu_throttled_usec'] = stats['throttled_time'] // 1000
            acct = lookup.get('cpuacct')
            if acct is not None:
                text = (self._read(acct / 'cpuacct.usage', limit=128) or '').strip()
                value = self._number(text)
                if value is not None: observed['cpu_usage_usec'] = value // 1000
            memory_dir = lookup.get('memory')
            if memory_dir is not None:
                for filename, name in (('memory.usage_in_bytes','memory_current_bytes'),
                                       ('memory.limit_in_bytes','memory_limit_bytes'),
                                       ('memory.failcnt','memory_events_max')):
                    value = self._number((self._read(memory_dir / filename, limit=128) or '').strip())
                    # v1 memory.limit may be a very large sentinel, not a real quota.
                    if value is not None and (name != 'memory_limit_bytes' or value < 1 << 60):
                        observed[name] = value
        # Only compute deltas across comparable observed counters and scope.
        version = observed.get('cgroup_version')
        for raw_key, delta_key in (('cpu_usage_usec','cpu_usage_delta_seconds'),
                                   ('cpu_throttled_usec','cpu_throttled_delta_seconds')):
            current = observed.get(raw_key)
            if current is None:
                continue
            prior = self._cpu_prior.get((version, raw_key))
            self._cpu_prior[(version, raw_key)] = (current, now)
            if prior is None or now <= prior[1] or current < prior[0]:
                continue
            delta = (current - prior[0]) / 1_000_000
            observed[delta_key] = delta
            if raw_key == 'cpu_usage_usec':
                observed['cpu_utilization_cores'] = delta / (now - prior[1])
        return observed

    def sample(self, *, pg_pid=None):
        self.seq += 1
        try:
            stamp = int(self.wall_clock() * 1000)
            stamp = stamp if 1 <= stamp < 2**63 else 1
            metrics = {'sample_seq': self.seq, 'sample_time_unix_ms': stamp}
            metrics.update(self._process(self.pid))
            if type(pg_pid) is int and pg_pid != self.pid:
                metrics.update(self._process(pg_pid, 'pg_master_'))
            metrics.update(self._sample_cgroup(self.clock()))
            metrics['sampling_failures_total'] = min(self.failures, 2**63 - 1)
            if len(json.dumps(metrics, allow_nan=False).encode()) > 4096:
                self.failures += 1
                return dict(sample_seq=self.seq, sample_time_unix_ms=stamp,
                            sampling_failures_total=min(self.failures, 2**63 - 1))
            return metrics
        except Exception:
            self.failures += 1
            return dict(sample_seq=self.seq, sample_time_unix_ms=max(1,int(time.time() * 1000)),
                        sampling_failures_total=min(self.failures, 2**63 - 1))


def _owned_pg_master_pid(prepare_runtime):
    """Only a process object from our already attached native runtime is valid."""
    holder = getattr(prepare_runtime, '__self__', None)
    instance = getattr(holder, 'instance', None)
    owner = getattr(instance, 'owner', None)
    process = getattr(owner, 'process', None)
    pid = getattr(process, 'pid', None)
    return pid if type(pid) is int and 1 <= pid < 2**31 else None


def main(*, prepare_runtime=None, close_runtime=None, recovery_handler=None, snapshot_channel=None):
    config=RUN_CONFIG;started=time.monotonic();stop=threading.Event()
    if prepare_runtime is not None and config.get('transport') != 'wss':
        raise ValueError('resident preparation requires the existing WSS channel')
    if prepare_runtime is not None and (config.get('shared_reads') is not True
            or not isinstance(config.get('prepared_model'), dict)
            or set(config['prepared_model']) != {'root', 'entries', 'max_total_bytes'}):
        raise ValueError('resident preparation requires explicit shared model inventory')
    sampler = ResourceSampler()
    def diagnostics():
        return sampler.sample(pg_pid=_owned_pg_master_pid(prepare_runtime))
    if config.get('transport')=='wss':
        try:
            run_wss_worker(config,load_encoder,diagnostics,prepare_runtime=prepare_runtime,close_runtime=close_runtime,
                **({'recovery_handler':recovery_handler} if recovery_handler is not None else {}),
                **({'snapshot_channel':snapshot_channel} if snapshot_channel is not None else {}))
        finally:
            Path('/kaggle/working/bge-run-summary.json').write_text(json.dumps({
                'run_id':config['run_id'],'space':SPACE,'transport':'wss',
                'worker_wall_seconds':time.monotonic()-started,
                'cooperative_worker_exit':True,'provider_termination_verified':False}))
        return
    def call(action,payload):
        data=json.dumps({'run_id':config['run_id'],**payload}).encode()
        request=urllib.request.Request(config['broker']+'/'+action,data=data,headers={'Authorization':'Bearer '+config['token'],'Content-Type':'application/json'},method='POST')
        with urllib.request.urlopen(request,timeout=30) as response:return json.load(response)
    def heartbeat():
        while not stop.wait(20):
            try:call('heartbeat',{'ready':False,'space':SPACE,'diagnostics':diagnostics()})
            except urllib.error.HTTPError as error:
                if error.code in (401,403):stop.set()
            except (TimeoutError,OSError):pass
    thread=threading.Thread(target=heartbeat,daemon=True);thread.start()
    encode=load_encoder()
    ready=time.monotonic()-started
    call('heartbeat',{'ready':True,'space':SPACE,'model_revision':REVISION,'diagnostics':{'startup_seconds':ready,**diagnostics()}})
    print(json.dumps({'event':'bge_ready','run_id':config['run_id'],'space':SPACE,'startup_seconds':ready,'cpu_threads':4}),flush=True)
    pending=None
    try:
        while not stop.is_set() and time.monotonic()-started<11*3600:
            try:
                if pending is None:
                    job=call('claim',{'space':SPACE}).get('job')
                    if not job:stop.wait(.1);continue
                    if job['space']!=SPACE or len(job['texts'])!=1:raise RuntimeError('job contract mismatch')
                    before=time.monotonic();vector=encode(job['texts'][0]);seconds=time.monotonic()-before
                    pending={'job_id':job['id'],'claim':job['claim'],'space':SPACE,'vectors':[vector],'timings':{'encoder_seconds':seconds}}
                call('complete',pending);pending=None
            except urllib.error.HTTPError as error:
                if error.code in (401,403):stop.set()
                elif error.code==409:pending=None
                else:stop.wait(1)
            except (TimeoutError,OSError):stop.wait(1)
    finally:
        stop.set()
        open('/kaggle/working/bge-run-summary.json','w').write(json.dumps({'run_id':config['run_id'],'space':SPACE,'startup_seconds':ready,'worker_wall_seconds':time.monotonic()-started,'cpu_only':True}))

# WSS_TRANSPORT_HELPER

if __name__=='__main__':main()
