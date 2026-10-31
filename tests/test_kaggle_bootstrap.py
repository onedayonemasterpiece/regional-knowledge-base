"""Synthetic artifacts, tokenizer and PG probe; no model/provider/DB runtime."""
import asyncio
from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path
import threading
from types import SimpleNamespace
from contextlib import asynccontextmanager

import pytest

from regional_knowledge import kaggle_bootstrap as boot
from regional_knowledge.kaggle_schema_migrate import latest_schema_version
from regional_knowledge.kaggle_readiness import ArtifactRequirement, RUNTIME_HASHES
from test_bge_wss_worker import CONFIG, module
from test_bge_shared_worker import Peer, query, result


class SyntheticTokenizer:
    def __init__(self, artifacts, calls=None):
        self.name_or_path = str(artifacts.model)
        self.is_fast = True
        self.backend_tokenizer = SimpleNamespace(truncation=None,
            to_str=lambda: (artifacts.model/'tokenizer.json').read_text())
        self.calls = calls if calls is not None else []

    def __call__(self, texts, **kwargs):
        return {'input_ids': [[1, 2, 3] for _ in texts]}

    def encode(self, text, **kwargs):
        self.calls.append((text, kwargs, threading.get_ident()))
        return list(range(int(text))) if text.isdigit() else [1, 2, 3]


def observation(**changes):
    values = dict(database_generation='synthetic-db', applied_seq=5, projection_seq=5, visibility_epoch=3, text_search_ready=True, dense_search_ready=True, archive_read_ready=None, proof_ready=None, schema_version=latest_schema_version())
    values.update(changes)
    return values


def candidate(**changes):
    return boot.CandidateReadiness(hashes={name: 'a'*64 for name in RUNTIME_HASHES}, **observation(**changes))


@pytest.fixture
def artifacts(tmp_path, monkeypatch):
    """Explicit synthetic pin override: never attests real BGE/tokenizer bytes."""
    root = tmp_path / 'prepared'; model = root / 'model'; model.mkdir(parents=True)
    files = {'model/tokenizer.json': ('tokenizer', b'{"synthetic":true,"truncation":null,"padding":null}'),
        'model/config.json': ('model_config', b'{}'),
        'model/model.safetensors': ('weight', b'synthetic-weight-bytes-not-a-model'),
        'handler.py': ('code', b'# synthetic loaded handler\n')}
    entries = []
    for name, (role, data) in files.items():
        (root/name).write_bytes(data)
        entries.append(boot.PreparedArtifact(ArtifactRequirement(name, len(data), hashlib.sha256(data).hexdigest()), role))
    synthetic_sha = hashlib.sha256(files['model/tokenizer.json'][1]).hexdigest()
    monkeypatch.setattr(boot, 'TOKENIZER_SHA256', synthetic_sha)
    monkeypatch.setattr('regional_knowledge.kaggle_query.TOKENIZER_SHA256', synthetic_sha)
    monkeypatch.setattr('regional_knowledge.kaggle_worker_query.TOKENIZER_SHA256', synthetic_sha)
    monkeypatch.setattr(boot, 'REQUIRED_DEPENDENCIES', frozenset({'pytest'}))
    monkeypatch.setattr(boot, '_loaded_code_paths', lambda: (root/'handler.py',))
    source = root/'executed.py'; source.write_bytes(b'RUN_CONFIG = {"private":"synthetic-secret"}\n')
    result = SimpleNamespace(root=root, model=model, entries=tuple(entries), source=source)
    monkeypatch.setattr(boot, '_load_tokenizer_reference', lambda *_: SyntheticTokenizer(result))
    return result


def verify(a):
    return boot.verify_artifacts(a.root, a.entries, max_total_bytes=100000)


def resident(a):
    calls = []
    encoder = boot.ResidentEncoder(lambda _: [1.]+[0.]*1023, SyntheticTokenizer(a, calls), verify(a), a.model, 1)
    calls.clear()
    return encoder, calls


def test_exact_declared_inventory_bytes_and_model_coverage(artifacts):
    verified = verify(artifacts)
    assert verified.model_directory == artifacts.model
    assert verified.role_hash({'code'}) != verified.role_hash({'weight', 'model_config', 'tokenizer'})
    with pytest.raises(boot.BootstrapUnavailable, match='artifact_inventory_invalid'):
        boot.verify_artifacts(artifacts.root, artifacts.entries, max_total_bytes=1)
    (artifacts.model/'unlisted.json').write_bytes(b'not declared')
    with pytest.raises(boot.BootstrapUnavailable, match='model_inventory_binding_invalid'):
        verify(artifacts)


def test_complete_fresh11_prepared_inventory_still_checks_every_file(artifacts):
    entries=list(artifacts.entries)
    for index in range(130-len(entries)):
        name='domain-'+str(index)+'.py';data=b'# exact synthetic source\n'
        (artifacts.root/name).write_bytes(data)
        entries.append(boot.PreparedArtifact(ArtifactRequirement(name,len(data),hashlib.sha256(data).hexdigest()),'code'))
    verified=boot.verify_artifacts(artifacts.root,entries,max_total_bytes=100000)
    assert len(verified.entries)==130
    (artifacts.root/entries[-1].requirement.name).write_bytes(b'corrupt')
    with pytest.raises(boot.BootstrapUnavailable):
        boot.verify_artifacts(artifacts.root,entries,max_total_bytes=100000)


@pytest.mark.parametrize('kind', ['changed', 'missing', 'file_symlink', 'directory_symlink', 'fifo', 'wrong_tokenizer_pin'])
def test_hash_path_and_filetype_fail_closed_without_secret_paths(artifacts, monkeypatch, kind):
    a = artifacts
    path = a.model/'tokenizer.json'
    if kind == 'changed':
        path.write_bytes(b'x'*len(path.read_bytes()))
    elif kind == 'missing':
        path.unlink()
    elif kind == 'file_symlink':
        target = a.root/'other.json'; target.write_bytes(path.read_bytes()); path.unlink(); path.symlink_to(target)
    elif kind == 'directory_symlink':
        target = a.root/'moved'; a.model.rename(target); a.model.symlink_to(target, target_is_directory=True)
    elif kind == 'fifo':
        path.unlink(); os.mkfifo(path)
    else:
        monkeypatch.setattr(boot, 'TOKENIZER_SHA256', '0'*64)
    with pytest.raises(boot.BootstrapUnavailable) as error:
        verify(a)
    assert str(a.root) not in str(error.value)


def test_inventory_rejects_traversal_duplicates_and_incomplete_roles(artifacts):
    with pytest.raises(ValueError):
        ArtifactRequirement('../secret', 1, 'a'*64)
    with pytest.raises(boot.BootstrapUnavailable):
        boot.verify_artifacts(artifacts.root, artifacts.entries*2, max_total_bytes=100000)
    with pytest.raises(boot.BootstrapUnavailable, match='incomplete'):
        boot.verify_artifacts(artifacts.root, artifacts.entries[:2], max_total_bytes=100000)


@pytest.mark.asyncio
async def test_actual_loaded_handle_complete_count_offloop_and_effective_settings(artifacts):
    encoder, calls = resident(artifacts)
    thread = threading.get_ident()
    assert await encoder.count('512') == 512 and await encoder.count('513') == 513
    assert all(kwargs == {'add_special_tokens': True, 'truncation': False, 'padding': False}
               and count_thread != thread for _, kwargs, count_thread in calls)
    assert encoder('synthetic') == [1.]+[0.]*1023 and encoder.model_load_count == 1
    encoder.tokenizer.backend_tokenizer.truncation = {'max_length': 512}
    with pytest.raises(boot.BootstrapUnavailable, match='loaded_tokenizer_binding_invalid'):
        replace(encoder)
    encoder.tokenizer.backend_tokenizer.truncation = None
    encoder.tokenizer.backend_tokenizer.to_str = lambda: '{"synthetic":"modified"}'
    with pytest.raises(boot.BootstrapUnavailable, match='settings_mismatch'):
        replace(encoder)


@pytest.mark.asyncio
async def test_query_count_separates_executor_mutex_and_tokenizer_time(artifacts, monkeypatch):
    from regional_knowledge.query_telemetry import (
        NativeReadTelemetry, ReadRuntime, bind_read_runtime, emit_executor_stage,
        clean_native_read_events, drain_native_read_events,
    )
    encoder, calls = resident(artifacts)
    # Only this module's clock is replaced; real asyncio/thread/lock/count run.
    ticks = iter((1., 3., 6., 10., 11.))
    monkeypatch.setattr(boot, 'time', SimpleNamespace(monotonic=lambda: next(ticks)), raising=False)
    request = dict(request_id='count-timing', connection_generation=1, turn_generation=1)
    collector = asyncio.Queue(maxsize=8)
    with bind_read_runtime(ReadRuntime('run-count', 'inc-count')):
        with NativeReadTelemetry(collector, request) as event:
            assert await encoder.count('512') == 512
            emit_executor_stage(collector, SimpleNamespace(**request), {'tokenize_ms': 10000.})
            event.finish(status='ok', reason=None)
    records = clean_native_read_events(drain_native_read_events(collector, request),
        **request, run_id='run-count', runtime_incarnation='inc-count')
    for record in records:
        assert record['stages_ms'] == {
            'tokenize_ms': 10000., 'tokenize_executor_wait_ms': 2000.,
            'tokenize_mutex_wait_ms': 3000., 'tokenize_work_ms': 4000.,
            'tokenize_resume_ms': 1000.,
            **({'adapter_ms': record['stages_ms']['adapter_ms']} if record['terminal'] else {}),
        }
    assert len(records) == 2 and len(calls) == 1
    assert calls[0][2] != threading.get_ident()


@pytest.mark.asyncio
async def test_cancelled_native_token_count_drains_and_serializes_resident_tokenizer(artifacts):
    encoder, _ = resident(artifacts)
    entered, finish, encoded = threading.Event(), threading.Event(), threading.Event()
    def count(*args, **kwargs):
        entered.set()
        assert finish.wait(2)
        return [1, 2, 3]
    encoder = replace(encoder, encode=lambda _: encoded.set())
    encoder.tokenizer.encode = count
    task = asyncio.create_task(encoder.count('synthetic'))
    forward = None
    try:
        assert await asyncio.to_thread(entered.wait, 1)
        task.cancel()
        await asyncio.sleep(.01)
        task.cancel()
        await asyncio.sleep(.01)
        assert not task.done(), 'native token count must finish before handler cleanup'
        forward = asyncio.create_task(asyncio.to_thread(encoder, 'next document'))
        await asyncio.sleep(.01)
        assert not encoded.is_set(), 'same tokenizer cannot count and encode concurrently'
    finally:
        finish.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        if forward is not None:
            await forward
    assert encoded.is_set()


@pytest.mark.asyncio
async def test_prepare_uses_existing_owned_lease_and_hashes_actual_observations(artifacts):
    encoder, _ = resident(artifacts)
    leased = []
    @asynccontextmanager
    async def acquire():
        leased.append('enter')
        try:
            yield SimpleNamespace(catalog=b'native catalog snapshot s1', fields=observation())
        finally:
            leased.append('cleanup')
    async def probe(*, acquire):
        async with acquire() as connection:
            return connection.catalog, connection.fields
    kwargs = dict(acquire=acquire, probe_database=probe, launch_config={'private': 'synthetic-secret'},
        executed_source=artifacts.source, source_binding_verified=True, dependency_packages=('pytest',))
    prepared = await boot.prepare_resident_runtime(encoder, **kwargs)
    wire = prepared.readiness.to_wire()
    assert set(wire['hashes']) == RUNTIME_HASHES and leased == ['enter', 'cleanup']
    assert wire['hashes']['executed_source'] == hashlib.sha256(artifacts.source.read_bytes()).hexdigest()
    assert wire['hashes']['schema'] == hashlib.sha256(json.dumps(wire['schema_version']).encode()).hexdigest()
    assert 'synthetic-secret' not in json.dumps(wire) and str(artifacts.root) not in json.dumps(wire)
    assert prepared.query_handler.acquire is acquire
    artifacts.source.write_bytes(b'# changed actual source\n')
    changed = await boot.prepare_resident_runtime(encoder, **kwargs)
    assert changed.readiness.hashes['executed_source'] != prepared.readiness.hashes['executed_source']
    assert changed.readiness.hashes['model'] == prepared.readiness.hashes['model']
    with pytest.raises(boot.BootstrapUnavailable, match='source_binding_unverified'):
        await boot.prepare_resident_runtime(encoder, **dict(kwargs, source_binding_verified=False))
    with pytest.raises(boot.BootstrapUnavailable, match='dependency_inventory_invalid'):
        await boot.prepare_resident_runtime(encoder, **dict(kwargs, dependency_packages=()))


@pytest.mark.asyncio
async def test_resident_observer_hashes_large_launch_config(artifacts):
    encoder,_=resident(artifacts)
    config={'padding':'x'*65537}
    encoded=json.dumps(config,sort_keys=True,separators=(',',':')).encode()
    probes=[]
    async def probe(*,acquire):
        probes.append(True)
        return b'actual fixture catalog',observation()
    kwargs=dict(acquire=lambda:None,probe_database=probe,launch_config=config,
        executed_source=artifacts.source,source_binding_verified=True,dependency_packages=('pytest',))
    prepared=await boot.prepare_resident_runtime(encoder,**kwargs)
    assert prepared.readiness.hashes['launch_config']==hashlib.sha256(encoded).hexdigest()
    assert probes==[True]

@pytest.mark.asyncio
async def test_missing_loaded_code_binding_and_unready_probe_never_make_ready(artifacts, monkeypatch):
    encoder, _ = resident(artifacts)
    async def probe(*, acquire):
        return b'actual catalog', observation(text_search_ready=False, dense_search_ready=False, projection_seq=None)
    kwargs = dict(acquire=lambda: None, probe_database=probe, launch_config={}, executed_source=artifacts.source,
        source_binding_verified=True, dependency_packages=('pytest',))
    with pytest.raises(boot.BootstrapUnavailable, match='database_not_ready'):
        await boot.prepare_resident_runtime(encoder, **kwargs)
    monkeypatch.setattr(boot, '_loaded_code_paths', lambda: (artifacts.root/'not-loaded.py',))
    with pytest.raises(boot.BootstrapUnavailable, match='loaded_code_binding_invalid'):
        await boot.prepare_resident_runtime(encoder, **kwargs)


@pytest.mark.parametrize('changes', [dict(applied_seq=True), dict(projection_seq=True), dict(visibility_epoch=True),
    dict(applied_seq=2**63), dict(projection_seq=6), dict(projection_seq=4), dict(archive_read_ready=1),
    dict(dense_search_ready=1), dict(database_generation='x'*257)])
def test_candidate_wire_strict_bounded_fields(changes):
    with pytest.raises(ValueError):
        candidate(**changes)


def test_candidate_has_exact_keys_independent_hashes_and_no_added_fields():
    proof = candidate()
    object.__setattr__(proof, 'extra', 'synthetic-secret')
    wire = proof.to_wire()
    assert set(wire) == {'hashes', *observation()}
    wire['hashes']['code'] = 'b'*64
    assert proof.hashes['code'] == 'a'*64 and 'extra' not in wire
    object.__setattr__(proof, 'applied_seq', True)
    with pytest.raises(ValueError):
        proof.to_wire()


@pytest.mark.asyncio
async def test_early_control_once_preparation_and_context_reuse_across_reconnect(monkeypatch):
    # This deadline measures control/model ordering, not cold transport imports.
    import aiohttp
    transport = module('bge_wss_transport')
    monkeypatch.setattr(transport.random, 'uniform', lambda *_: .001)
    peer = Peer(); stop = asyncio.Event(); allow = asyncio.Event(); entered = asyncio.Event()
    loads, prepared_calls = [], []
    def load():
        assert peer.frames('ATTACH') and peer.frames('RESOURCE_STATE')
        loads.append(1)
        return lambda _: [1.]+[0.]*1023
    async def handler(frame, *, encode):
        await encode(frame['query'])
        return result(frame)
    ready = boot.PreparedRuntime(handler, candidate())
    async def prepare(encoder, **identity):
        prepared_calls.append(identity)
        entered.set()
        await allow.wait()
        return ready
    worker = transport.BgeWssTransport(dict(CONFIG, shared_reads=True), load, lambda: {},
        prepare_runtime=prepare, connector=peer.connect, heartbeat_seconds=.01, maximum_seconds=3, stop=stop)
    task = asyncio.create_task(worker.run())
    try:
        await asyncio.wait_for(entered.wait(), 1)
        await peer.wait('HEARTBEAT')
        assert not peer.frames('READY')
        await peer.put(dict(type='RESOURCE_PROBE', probe_nonce='preparation-live'))
        await peer.wait('RESOURCE_STATE', predicate=lambda f: f.get('probe_nonce') == 'preparation-live')
        allow.set()
        first = await peer.wait('READY', generation=1)
        assert first['runtime_readiness'] == ready.readiness.to_wire()
        await peer.disconnect()
        second = await peer.wait('READY', generation=2)
        assert second['runtime_readiness'] == first['runtime_readiness']
        assert len(loads) == len(prepared_calls) == 1 and worker.prepared_runtime is ready
        assert prepared_calls[0] == dict(run_id=CONFIG['run_id'], launch_nonce=CONFIG['launch_nonce'],
                                        runtime_incarnation=worker.incarnation)
        await peer.put(query(2))
        await peer.wait('QUERY_RESULT', generation=2)
    finally:
        allow.set(); stop.set()
        await asyncio.wait_for(task, 2)


@pytest.mark.asyncio
async def test_shutdown_repeated_cancel_drains_preparation_and_preserves_cancellation():
    transport = module('bge_wss_transport')
    peer = Peer(); entered = asyncio.Event(); finish = asyncio.Event()
    async def handler(frame, *, encode):
        return result(frame)
    async def prepare(encoder, **identity):
        entered.set()
        await finish.wait()
        return boot.PreparedRuntime(handler, candidate())
    worker = transport.BgeWssTransport(dict(CONFIG, shared_reads=True), lambda: lambda _: [], lambda: {},
        prepare_runtime=prepare, connector=peer.connect, heartbeat_seconds=.01, maximum_seconds=3)
    task = asyncio.create_task(worker.run())
    try:
        await asyncio.wait_for(entered.wait(), 1)
        task.cancel(); await asyncio.sleep(.01)
        assert not task.done()
        task.cancel(); await asyncio.sleep(.01)
        assert not task.done() and not peer.frames('READY')
    finally:
        finish.set()
        with pytest.raises(asyncio.CancelledError):
            await task


def test_factory_hook_validation_and_template_local_only_target_path():
    transport = module('bge_wss_transport')
    with pytest.raises(ValueError, match='async prepare_runtime'):
        transport.BgeWssTransport(dict(CONFIG, shared_reads=True), lambda: None, lambda: {}, prepare_runtime=lambda _: None)
    source = (Path(__file__).resolve().parents[1]/'scripts/production/bge_kaggle_worker.py').read_text()
    assert 'tokenizer_file=str(model_path/' in source and source.count('local_files_only=True') == 2
    assert 'prepare_runtime=prepare_runtime' in source and 'pip install' not in source


@pytest.mark.parametrize('changes', [
    {}, {'shared_reads': True}, {'shared_reads': True, 'prepared_model': {}},
    {'shared_reads': False, 'prepared_model': {'root': 'unused', 'entries': [], 'max_total_bytes': 1}},
])
def test_prepared_template_rejects_missing_target_inventory_before_any_loader(monkeypatch, changes):
    worker = module('bge_kaggle_worker')
    monkeypatch.setattr(worker, 'RUN_CONFIG', dict(CONFIG, transport='wss') | changes)
    def forbidden(*args, **kwargs):
        pytest.fail('incomplete target must not enter legacy model loader or WSS runner')
    monkeypatch.setattr(worker, 'load_encoder', forbidden)
    monkeypatch.setattr(worker, 'run_wss_worker', forbidden, raising=False)
    async def prepare(*args, **kwargs):
        pytest.fail('incomplete target must not prepare')
    with pytest.raises(ValueError, match='explicit shared model inventory'):
        worker.main(prepare_runtime=prepare)


@pytest.mark.asyncio
async def test_source_document_and_query_share_resident_fifo_forward_and_drain_cancel(artifacts):
    from regional_knowledge.kaggle_source_materializer import resident_source_materializer
    from test_bge_shared_worker import running, query, result
    from regional_knowledge.bge_contract import SPACE
    encoder,_=resident(artifacts)
    query_entered=asyncio.Event()
    entered=threading.Event();finish=threading.Event();calls=[];active=0;peak=0
    def native(text):
        nonlocal active,peak
        active+=1;peak=max(peak,active);calls.append(text)
        try:
            if text=='document':entered.set();assert finish.wait(3)
            return [1.]+[0.]*1023
        finally:active-=1
    encoder=replace(encoder,encode=native)
    materializer=resident_source_materializer(encoder,current=lambda:True)
    async def handler(frame,*,encode):
        query_entered.set();await encode(frame['query']);return result(frame)
    source=None
    try:
        async with running(handler,encoder) as (peer,worker,loads):
            await peer.put(dict(type='DISPATCH',id='fifo-doc',claim='doc-claim',kind='document',
                texts=['document'],space=SPACE,compute_claim='fifo-doc-compute',dispatch_generation=1))
            assert await asyncio.to_thread(entered.wait,1)
            await peer.wait('RESOURCE_RELEASED',predicate=lambda f:f['resource_kind']=='document')
            await peer.put(query())
            await asyncio.wait_for(query_entered.wait(),1)
            source=asyncio.create_task(materializer.encode('source'))
            await asyncio.sleep(.01)
            source.cancel();await asyncio.sleep(.01)
            assert not source.done() and calls==['document']
            finish.set()
            with pytest.raises(asyncio.CancelledError):await source
            await peer.wait('QUERY_RESULT');await peer.wait('RESULT')
            assert calls==['document','fixture query','source'] and peak==1 and loads==['loaded']
            await peer.put(dict(type='RESULT_ACK',job_id='fifo-doc',claim='doc-claim',status='accepted'))
    finally:
        finish.set()
        if source is not None:await asyncio.gather(source,return_exceptions=True)
