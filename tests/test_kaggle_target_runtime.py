"""Synthetic source, journal and pool checks; no native PG/model/provider effects."""
import ast
import asyncio
from dataclasses import replace
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from regional_knowledge.bge_queue import BgeQueue
from regional_knowledge.kaggle_bootstrap import PreparedRuntime
from regional_knowledge.kaggle_owned_pg import OwnedDatabase, OwnedPostgres, SQL_ARTIFACTS, digest
from regional_knowledge.kaggle_target_launch import persist_empty_intent
from regional_knowledge.kaggle_target_runtime import TargetLaunchSpec, TargetRuntime
from regional_knowledge.recovery_journal import JournalUnknown
from test_bge_wss_worker import CONFIG, ROOT, Wire, module
from test_kaggle_bootstrap import candidate
from test_recovery_journal import fixture as journal_fixture, command


def spec(*,fresh=False):
    native = {name:'/kaggle/working/native/'+name for name in
        ('initdb','postgres','pg_config','vector_library','vector_control','vector_sql')}
    database = OwnedDatabase(mode='NEW_EMPTY_GENERATION' if fresh else 'VERIFIED_INITIALIZED',
        database_generation='synthetic-db',writer_generation='writer-1',schema_revision='s1',
        function_revision='f1',executor_revision='e1',socket='/kaggle/working/owned/socket',
        dbname='rkb',ownership_evidence='stage0-fixture',
        **(dict(cluster='/kaggle/working/owned',uid=1000,gid=1000,native_paths=native) if fresh else {}))
    receipt = None if fresh else dict(database=database.wire(),schema_sha256='a'*64,stage0_receipt='stage0-fixture')
    return TargetLaunchSpec(database,'/kaggle/working',(
        dict(name='model/tokenizer.json',size=3,sha256='b'*64,role='tokenizer'),
        dict(name='model/config.json',size=2,sha256='c'*64,role='model_config'),
        dict(name='model/model.safetensors',size=10,sha256='d'*64,role='weight'),
        *(dict(name='native/'+name,size=1,sha256='e'*64,role='code') for name in native)),
        ('torch','transformers','tokenizers','huggingface-hub','psycopg','psycopg-pool','aiohttp','pydantic'),
        'f'*64,'a'*64,'stage0-fixture',receipt)


def queue_run(tmp_path):
    queue = BgeQueue(tmp_path/'queue.sqlite')
    queue.enqueue('actor','document',['fixture'],kind='document')
    return queue,queue.maintenance()[0]['id']


def hashes():
    return {name:digest((ROOT/'sql/kaggle'/name).read_bytes()) for name in SQL_ARTIFACTS}


def generate(queue,run,target):
    controller=module('bge_kaggle_controller')
    return controller.worker_source((ROOT/'scripts/production/bge_kaggle_worker.py').read_text(),
        queue,run,'fixture-token','https://fixture.example/bge-worker',transport='wss',target=target)


def test_generated_target_persists_final_hash_outside_source_and_is_stable(tmp_path):
    queue,run=queue_run(tmp_path)
    source=generate(queue,run,spec())
    assert source==generate(queue,run,spec())
    assert 'main(prepare_runtime=runtime.prepare, close_runtime=runtime.aclose)' in source
    assert 'pip install' not in source
    compile(source,'fixture-generated','exec')
    with queue.connect() as db:
        expected=json.loads(db.execute('SELECT expected_json FROM bge_runtime_expectations WHERE run_id=?',(run,)).fetchone()[0])
    assert expected['hashes']['executed_source']==hashlib.sha256(source.encode()).hexdigest()
    assert expected['hashes']['executed_source'] not in source
    assert expected['hashes']['schema']=='a'*64 and expected['hashes']['dependency']=='f'*64
    assert module('bge_kaggle_controller').source_run_id(source)==run
    with pytest.raises(ValueError,match='immutable'):
        generate(queue,run,replace(spec(),dependency_sha256='0'*64))


def test_generated_entrypoint_executes_lifecycle_with_stubs(tmp_path,monkeypatch):
    queue,run=queue_run(tmp_path)
    source=generate(queue,run,spec())
    tree=ast.parse(source)
    entry=tree.body.pop()
    namespace={'__name__':'generated_fixture','__file__':str(tmp_path/'main.py')}
    exec(compile(ast.Module(body=tree.body,type_ignores=[]),'fixture','exec'),namespace)
    calls=[]
    async def prepare(*args,**kwargs): pass
    async def close(): pass
    class Runtime:
        def __init__(self,config,**kwargs):
            calls.append(('runtime',config['run_id']))
            self.prepare,self.aclose=prepare,close
    monkeypatch.setattr('regional_knowledge.kaggle_target_runtime.TargetRuntime',Runtime)
    def main(**kwargs):
        assert kwargs==dict(prepare_runtime=prepare,close_runtime=close)
        calls.append(('main',))
    namespace.update(main=main,__name__='__main__')
    # Redirect the pure source extraction for this local fixture only. The source
    # whose final hash is persisted above retains its actual Kaggle paths.
    class Redirect(ast.NodeTransformer):
        def visit_Constant(self,node):
            if isinstance(node.value,str) and node.value.startswith('/kaggle/working/rkb-target-'):
                return ast.copy_location(ast.Constant(str(tmp_path/Path(node.value).name)),node)
            return node
    exec(compile(ast.fix_missing_locations(Redirect().visit(ast.Module(body=[entry],type_ignores=[]))),
                 'fixture-entry','exec'),namespace)
    assert calls==[('runtime',run),('main',)]


@pytest.mark.asyncio
async def test_fresh_intent_requires_real_empty_head_and_is_immutable(tmp_path):
    queue,run=queue_run(tmp_path)
    target=spec(fresh=True)
    with pytest.raises(ValueError,match='intent first'):
        generate(queue,run,target)
    nonce=queue.prepare_channel(run)
    store,journal=journal_fixture()
    args=dict(launch_nonce=nonce,recovery_journal=journal,sql_hashes=hashes())
    intent=await persist_empty_intent(queue,run,target,**args)
    assert journal.head_key in store.reads and intent['journal_head'] is None
    source=generate(queue,run,target)
    assert 'rkb.empty-generation.v1' in source
    with pytest.raises(ValueError,match='immutable'):
        await persist_empty_intent(queue,run,replace(target,database=replace(target.database,writer_generation='w2')),**args)
    await journal.seal([command()],predecessor=None,writer_generation='writer-1')
    with pytest.raises(ValueError,match='restore'):
        await persist_empty_intent(queue,run,target,**args)


@pytest.mark.asyncio
async def test_empty_head_read_unknown_never_records_intent(tmp_path):
    queue,run=queue_run(tmp_path)
    store,journal=journal_fixture();store.read_failures.add('/head')
    with pytest.raises(JournalUnknown):
        await persist_empty_intent(queue,run,spec(fresh=True),launch_nonce=queue.prepare_channel(run),
            recovery_journal=journal,sql_hashes=hashes())
    with queue.connect() as db:
        assert not db.execute("SELECT 1 FROM sqlite_master WHERE name='bge_empty_generation_intents'").fetchone()


@pytest.mark.asyncio
async def test_runtime_opens_once_keeps_loop_and_closes_pool_before_owned_pg():
    events=[];loop=asyncio.get_running_loop()
    target=spec()
    config=dict(CONFIG,target_runtime=dict(target.wire(),sql_root='/unused'))
    class Owner:
        def __init__(self,spec): pass
        async def start_empty(self,**kwargs): events.append('owned')
        async def aclose(self):
            assert asyncio.get_running_loop() is loop
            events.append('pg-close')
    class Pool:
        def __init__(self,**kwargs):
            assert kwargs['open'] is False and kwargs['kwargs']['user']=='rkb_reader'
            events.append('pool')
        async def open(self,**kwargs): events.append('open')
        async def close(self,**kwargs): events.append('pool-close')
        def connection(self): pass
    async def handler(frame,*,encode):
        from regional_knowledge.query_telemetry import _RUNTIME
        assert _RUNTIME.get().run_id==CONFIG['run_id']
        return {'ok':True}
    async def prepare(encoder,**kwargs):
        assert kwargs['launch_config'] is config
        return PreparedRuntime(handler,candidate())
    runtime=TargetRuntime(config,executed_source='/fixture',pool_factory=Pool,owner_factory=Owner,prepare=prepare)
    args=dict(run_id=CONFIG['run_id'],launch_nonce=CONFIG['launch_nonce'],runtime_incarnation='incarnation')
    prepared=await runtime.prepare(SimpleNamespace(artifacts=None),**args)
    assert await runtime.prepare(None,**args) is prepared
    assert await prepared.query_handler({},encode=None)=={'ok':True}
    await runtime.aclose();await runtime.aclose()
    assert events==['owned','pool','open','pool-close','pg-close']


@pytest.mark.asyncio
async def test_repeated_cancel_drains_close_on_worker_loop():
    transport=module('bge_wss_transport')
    stop=asyncio.Event();wire=Wire(stop)
    closing=asyncio.Event();finish=asyncio.Event();closed=[]
    async def close():
        closing.set()
        await finish.wait()
        closed.append(asyncio.get_running_loop())
    worker=transport.BgeWssTransport(CONFIG,lambda: lambda _: [1.]+[0.]*1023,lambda: {},
        connector=wire.connect,stop=stop,heartbeat_seconds=.01,maximum_seconds=3,close_runtime=close)
    running=asyncio.create_task(worker.run())
    await asyncio.wait_for(closing.wait(),2)
    for _ in range(3):
        running.cancel();await asyncio.sleep(0)
        assert not running.done()
    finish.set()
    with pytest.raises(asyncio.CancelledError): await running
    assert closed==[asyncio.get_running_loop()]


@pytest.mark.asyncio
async def test_native_bootstrap_refuses_existing_cluster_before_initdb(tmp_path,monkeypatch):
    target=spec(fresh=True);database=target.database
    cluster=tmp_path/'existing';cluster.mkdir()
    # Path redirection fixture; construction remains validated against Kaggle.
    object.__setattr__(database,'cluster',str(cluster))
    object.__setattr__(database,'socket',str(cluster/'socket'))
    paths=database.native_paths
    artifacts=SimpleNamespace(root=Path('/'),entries=[SimpleNamespace(role='code',
        requirement=SimpleNamespace(name=p.lstrip('/'))) for p in paths.values()])
    async def command(*args,**kwargs):
        if args[-1]=='--pkglibdir': return '/kaggle/working/native'
        if args[-1]=='--sharedir': return '/kaggle/working/native'
        raise AssertionError('initdb must not run')
    fixed=dict(paths,vector_library='/kaggle/working/native/vector.so',
        vector_control='/kaggle/working/native/extension/vector.control',
        vector_sql='/kaggle/working/native/extension/vector--0.8.2.sql')
    object.__setattr__(database,'native_paths',fixed)
    artifacts.entries=[SimpleNamespace(role='code',requirement=SimpleNamespace(name=p.lstrip('/'))) for p in fixed.values()]
    for entry in artifacts.entries:
        entry.requirement.size,entry.requirement.sha256=1,'e'*64
    monkeypatch.setattr('regional_knowledge.kaggle_owned_pg._file_hash',lambda *args:'e'*64)
    owner=OwnedPostgres(database);monkeypatch.setattr(owner,'_command',command)
    intent=dict(codec='rkb.empty-generation.v1',database=database.wire(),journal_head=None,
        journal_stream='synthetic',run_id='run',launch_nonce='nonce',sql_hashes=hashes())
    with pytest.raises(Exception,match='existing_cluster_refused'):
        await owner.start_empty(intent=intent,artifacts=artifacts,sql_root=ROOT/'sql/kaggle')


@pytest.mark.asyncio
async def test_owned_probe_checks_actual_writer_generation():
    from contextlib import asynccontextmanager
    target=spec()
    runtime=TargetRuntime(dict(CONFIG,target_runtime=dict(target.wire(),sql_root='/unused')),
                          executed_source='/fixture')
    async def catalog(**kwargs): return b'native-catalog',{}
    runtime.probe=catalog
    class Cursor:
        async def fetchone(self): return ('synthetic-db','stale-writer')
    class Connection:
        @asynccontextmanager
        async def transaction(self): yield
        async def execute(self,*args): return Cursor()
    @asynccontextmanager
    async def acquire(): yield Connection()
    with pytest.raises(Exception,match='writer_binding_mismatch'):
        await runtime._owned_probe(acquire=acquire)


def test_controller_rejects_missing_target_authority_before_provider_call(monkeypatch):
    controller=module('bge_kaggle_controller')
    monkeypatch.setenv('RKB_BGE_TRANSPORT','wss')
    with pytest.raises(TypeError,match='RecoveryJournal'):
        controller.main(target_spec=spec(fresh=True))


@pytest.mark.asyncio
async def test_native_command_cancel_reaps_exact_owned_child(monkeypatch):
    import os
    owner=OwnedPostgres(spec(fresh=True).database)
    calls=[]
    class Process:
        returncode=None
        async def read(self,size):
            await asyncio.Event().wait()
        @property
        def stdout(self): return self
        def kill(self): calls.append('kill');self.returncode=-9
        async def wait(self): calls.append('wait');return self.returncode
    process=Process()
    async def spawn(*args,**kwargs):
        assert kwargs['user']==1000 and kwargs['group']==1000 and kwargs['extra_groups']==[]
        return process
    monkeypatch.setattr(owner,'spawn',spawn)
    monkeypatch.setattr(os,'geteuid',lambda:0)
    running=asyncio.create_task(owner._command('/synthetic/initdb'))
    await asyncio.sleep(0);running.cancel()
    with pytest.raises(asyncio.CancelledError): await running
    assert calls==['kill','wait']
