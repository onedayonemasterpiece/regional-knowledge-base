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
from regional_knowledge.kaggle_owned_pg import OwnedDatabase, OwnedPostgres, SQL_ARTIFACTS, digest, canonical
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
        function_revision='f1',executor_revision='e1',socket='/kaggle/temp/owned/socket',
        dbname='rkb',ownership_evidence='stage0-fixture',
        **(dict(cluster='/kaggle/temp/owned',uid=1000,gid=1000,native_paths=native) if fresh else {}))
    receipt = None if fresh else dict(database=database.wire(),schema_sha256='a'*64,stage0_receipt='stage0-fixture')
    return TargetLaunchSpec(database,'/kaggle/working',(
        dict(name='model/tokenizer.json',size=3,sha256='b'*64,role='tokenizer'),
        dict(name='model/config.json',size=2,sha256='c'*64,role='model_config'),
        dict(name='model/model.safetensors',size=10,sha256='d'*64,role='weight'),
        *(dict(name='native/'+name,size=1,sha256='e'*64,role='code') for name in native)),
        ('torch','transformers','tokenizers','huggingface-hub','psycopg','psycopg-pool','aiohttp','pydantic'),
        'f'*64,'a'*64,'stage0-fixture',receipt)


def test_corpus_cluster_and_socket_cannot_enter_saved_notebook_outputs():
    from dataclasses import replace
    with pytest.raises(ValueError,match='private native PG endpoint'):
        replace(spec().database,socket='/kaggle/working/corpus/socket')
    with pytest.raises(ValueError,match='private native PG endpoint'):
        replace(spec(fresh=True).database,cluster='/kaggle/working/corpus',socket='/kaggle/working/corpus/socket')


def queue_run(tmp_path):
    queue = BgeQueue(tmp_path/'queue.sqlite')
    queue.enqueue('actor','document',['fixture'],kind='document')
    return queue,queue.maintenance()[0]['id']


def hashes():
    return {path.name:digest(path.read_bytes()) for path in sorted((ROOT/'sql/kaggle').glob('[0-9][0-9][0-9]_*.sql'))}


def generate(queue,run,target):
    controller=module('bge_kaggle_controller')
    return controller.worker_source((ROOT/'scripts/production/bge_kaggle_worker.py').read_text(),
        queue,run,'fixture-token','https://fixture.example/bge-worker',transport='wss',target=target)


def test_generated_target_persists_final_hash_outside_source_and_is_stable(tmp_path):
    queue,run=queue_run(tmp_path)
    source=generate(queue,run,spec())
    # Shipping the patch boundary in the exact notebook source inventory is
    # essential: a local-only module otherwise fails after Kaggle ATTACH.
    from regional_knowledge.kaggle_target_launch import source_bundle
    import base64
    _,shipped,entries=source_bundle(ROOT)
    for relative in ('regional_knowledge/kaggle_warm_read.py',
                     'regional_knowledge/kaggle_warm_maintenance.py'):
        assert relative in shipped
        raw=base64.b64decode(shipped[relative],validate=True)
        assert raw==(ROOT/'src'/relative).read_bytes()
        assert any(e['name'].endswith('/'+relative) and e['size']==len(raw)
                   and e['sha256']==hashlib.sha256(raw).hexdigest() for e in entries)
    assert source==generate(queue,run,spec())
    assert 'main(prepare_runtime=runtime.prepare, close_runtime=runtime.aclose)' in source
    assert 'pip install' not in source
    compile(source,'fixture-generated','exec')
    assignments=[node for node in ast.parse(source).body if isinstance(node,ast.Assign)
        and any(isinstance(t,ast.Name) and t.id=='RUN_CONFIG' for t in node.targets)]
    config=ast.literal_eval(assignments[0].value)
    assert 'artifacts' not in config['target_runtime']
    assert len(config['prepared_model']['entries'])>len(spec().artifacts)
    assert {entry['name'] for entry in spec().artifacts}<={entry['name'] for entry in config['prepared_model']['entries']}
    with queue.connect() as db:
        expected=json.loads(db.execute('SELECT expected_json FROM bge_runtime_expectations WHERE run_id=?',(run,)).fetchone()[0])
    assert expected['hashes']['executed_source']==hashlib.sha256(source.encode()).hexdigest()
    assert expected['hashes']['executed_source'] not in source
    assert expected['hashes']['schema']==digest(canonical(spec().database.schema_version))
    assert expected['hashes']['dependency']=='f'*64
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
        assert set(kwargs)=={'prepare_runtime','close_runtime'}
        assert calls==[]  # No dependency-bearing runtime import/construction yet.
        calls.append(('main',))
        async def lifecycle():
            await kwargs['prepare_runtime'](None,run_id=run,launch_nonce='nonce',runtime_incarnation='inc')
            await kwargs['close_runtime']()
        asyncio.run(lifecycle())
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
    assert calls==[('main',),('runtime',run)]


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
async def test_controller_intent_includes_the_selected_native_story_migration(tmp_path):
    from test_bge_controller import controller
    from regional_knowledge.source_import_v2 import STORY_REVISIONS
    from regional_knowledge.kaggle_owned_pg import STORY_SQL_ARTIFACTS
    queue,run=queue_run(tmp_path)
    base=spec(fresh=True)
    target=replace(base,database=replace(base.database,schema_revision=STORY_REVISIONS.schema,
        function_revision=STORY_REVISIONS.function,executor_revision=STORY_REVISIONS.executor))
    _,journal=journal_fixture()
    selected=controller().target_sql_hashes(target,ROOT)
    assert selected==hashes()
    await persist_empty_intent(queue,run,target,launch_nonce=queue.prepare_channel(run),
        recovery_journal=journal,sql_hashes=selected)
    # Without migration 007 in the controller's intent the real composer
    # refuses to generate a launch. This does not push a notebook or open PG.
    generated=generate(queue,run,target)
    ast.parse(generated)
    assert '007_source_stories.sql' in generated


@pytest.mark.asyncio
async def test_runtime_opens_once_keeps_loop_and_closes_pool_before_owned_pg(tmp_path):
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
        assert isinstance(kwargs['telemetry'],asyncio.Queue) and kwargs['telemetry'].maxsize==64
        return PreparedRuntime(handler,candidate())
    runtime=TargetRuntime(config,executed_source='/fixture',pool_factory=Pool,owner_factory=Owner,prepare=prepare)
    args=dict(run_id=CONFIG['run_id'],launch_nonce=CONFIG['launch_nonce'],runtime_incarnation='incarnation')
    prepared=await runtime.prepare(SimpleNamespace(artifacts=None),**args)
    assert await runtime.prepare(None,**args) is prepared
    assert await prepared.query_handler({},encode=None)=={'ok':True}
    # A verified compatible handler is applied to this SAME bound invocation.
    # BGE/PG/pool preparation must not run a second time.
    from test_kaggle_warm_read import staged_module,good_probe,good_stage
    root,path,digest=staged_module(tmp_path)
    slot=runtime.read_handler_slot
    initial=slot.receipt()['loaded_sha256']
    assert initial==candidate().hashes['code']
    await slot.stage(path=path,trusted_root=root,expected_sha256=digest,
        compatibility=slot._compatibility,constructor={'marker':'new'},self_test=good_stage)
    loaded=await slot.activate(staged_sha256=digest,expected_loaded_sha256=initial,
        probe=good_probe)
    assert loaded['loaded_sha256']==digest and loaded['generation']==1
    assert await prepared.query_handler({},encode=None)=={'version':'new','frame':{}}
    assert events.count('owned')==1 and events.count('open')==1
    assert prepared.read_event_drain({'request_id':'none','connection_generation':1,'turn_generation':0})==[]
    await runtime.aclose();await runtime.aclose()
    assert events==['owned','pool','open','pool-close','pg-close']




@pytest.mark.asyncio
async def test_new_empty_prepare_before_configure_recovery_binds_same_warm_maintenance(monkeypatch,tmp_path):
    """Real NEW_EMPTY TargetRuntime order: prepare → native recovery hookup.

    No fake warm attachment from previous order is inherited; no second
    model/PG preparation is allowed when the existing Stage0 worker arrives.
    """
    from regional_knowledge.kaggle_recovery_phases import WorkerRecoveryPhases
    from regional_knowledge.kaggle_warm_maintenance import WarmHandlerMaintenance
    from regional_knowledge.kaggle_worker_query import KaggleWorkerQuery
    from test_kaggle_worker_query import pinned
    events=[]
    launch=spec(fresh=True)
    config=dict(CONFIG,target_runtime=dict(launch.wire(),sql_root='/unused'))
    class Owner:
        def __init__(self,_):pass
        async def start_empty(self,**options):events.append('initdb-once')
        async def aclose(self):events.append('owned-close')
    class Pool:
        def __init__(self,**kwargs):events.append('pool-created')
        async def open(self,**kwargs):events.append('pool-open')
        async def close(self,**kwargs):events.append('pool-close')
        def connection(self):pass
    async def prepare(encoder,**kw):
        events.append('prepared-once')
        return PreparedRuntime(KaggleWorkerQuery(acquire=kw['acquire'],
            tokenizer=pinned(),telemetry=kw['telemetry']),candidate())
    runtime=TargetRuntime(config,executed_source='/fixture',
        owner_factory=Owner,pool_factory=Pool,prepare=prepare)
    sql_root=tmp_path/'sql';sql_root.mkdir()
    body=b'SELECT 1;'
    (sql_root/'001_fixture.sql').write_bytes(body)
    runtime.target['sql_root']=str(sql_root)
    encoder=SimpleNamespace(artifacts=SimpleNamespace(root=tmp_path,
        entries=(SimpleNamespace(role='code',requirement=SimpleNamespace(
            name='sql/001_fixture.sql',size=len(body),sha256=__import__('hashlib').sha256(body).hexdigest())),),
        role_hash=lambda roles:'f'*64))
    identity=dict(run_id=CONFIG['run_id'],launch_nonce=CONFIG['launch_nonce'],
                  runtime_incarnation='incarnation')
    original=await runtime.prepare(encoder,**identity)
    slot=runtime.read_handler_slot
    assert slot is not None and slot._pinned is not None
    assert runtime.recovery_worker is None

    async def fake_build(**kwargs):
        assert kwargs['identity']==tuple(identity.values())
        events.append('recovery-worker-attached')
        worker=object.__new__(WorkerRecoveryPhases)
        worker.read_handler_slot=None;worker.read_handler_control=None
        worker.download=lambda *args,**kw:None
        worker.workspace=lambda *args,**kw:None
        worker.factory=SimpleNamespace(_current=lambda b:None)
        async def close():events.append('recovery-close')
        worker.aclose=close
        return worker
    monkeypatch.setattr('regional_knowledge.kaggle_recovery_phases.build_recovery_worker',fake_build)
    worker=await runtime.configure_recovery(encoder,**identity,
        native_bundle=SimpleNamespace(root=tmp_path),tools={},
        native_uid=1000,native_gid=1000,current=lambda _:True,
        workspace_private_current=lambda _:True,workspace=lambda *a:None)
    assert worker is runtime.recovery_worker
    assert worker.read_handler_slot is slot
    assert type(worker.read_handler_control) is WarmHandlerMaintenance
    assert worker.read_handler_control.slot is slot
    assert runtime.prepared is original
    assert (await runtime.prepare(encoder,**identity)) is original
    runtime._bind_read_handler_maintenance()
    assert worker.read_handler_control.slot is slot
    assert events==['initdb-once','pool-created','pool-open','prepared-once',
                    'recovery-worker-attached']
    await runtime.aclose()
    assert events[-3:]==['pool-close','recovery-close','owned-close']


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
    monkeypatch.setattr('regional_knowledge.kaggle_owned_pg.PRIVATE_RUNTIME_ROOT',tmp_path)
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


def test_verified_nonroot_native_identity_does_not_attempt_privileged_setgroups(monkeypatch):
    import os
    owner=OwnedPostgres(spec(fresh=True).database)
    monkeypatch.setattr(os,'geteuid',lambda:1000)
    monkeypatch.setattr(os,'getegid',lambda:1000)
    monkeypatch.setattr(os,'getgroups',lambda:[1000])
    assert owner._identity()=={}
    monkeypatch.setattr(os,'getgroups',lambda:[1000,77])
    with pytest.raises(Exception,match='identity_mismatch'): owner._identity()


def stage0(target):
    from regional_knowledge.kaggle_stage0 import Stage0Download,Stage0Spec
    versions={name:'0.0' for name in target.dependency_packages}
    versions.update(torch='2.11.0+cpu',transformers='5.16.1',tokenizers='0.23.1',**{'huggingface-hub':'1.29.0'})
    return Stage0Spec(tuple(Stage0Download(e['name'],'https://fixture.invalid/'+e['name'],e['size'],e['sha256'],
        executable=e['name'] in {'native/initdb','native/postgres','native/pg_config'}) for e in target.artifacts),
        (),versions,install_bytes=0,total_seconds=60)


def test_typed_stage0_complete_inventory_versions_and_source_bundle_binding(tmp_path):
    target=spec();plan=stage0(target);target=replace(target,stage0=plan)
    queue,run=queue_run(tmp_path);source=generate(queue,run,target)
    config=next(ast.literal_eval(node.value) for node in ast.parse(source).body
        if isinstance(node,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='RUN_CONFIG' for t in node.targets))
    assert config['stage0']==plan.wire()
    assert 'stage0' not in config['target_runtime']
    assert any(e['name'].endswith('/regional_knowledge/kaggle_stage0.py') for e in config['prepared_model']['entries'])
    assert all(e['name'] in {x['name'] for x in config['prepared_model']['entries']} for e in config['stage0']['downloads'])
    with pytest.raises(ValueError,match='every external'):
        replace(target,stage0=replace(plan,downloads=plan.downloads[:-1]))
    changed=replace(plan.downloads[0],sha256='f'*64)
    with pytest.raises(ValueError,match='artifact pin'):
        replace(target,stage0=replace(plan,downloads=(changed,)+plan.downloads[1:]))
    with pytest.raises(ValueError,match='protected BGE'):
        replace(target,stage0=replace(plan,packages=dict(plan.packages,torch='9.0')))
    with pytest.raises(ValueError,match='dependency hash inventory'):
        replace(target,stage0=replace(plan,packages=dict(plan.packages,extra='1.0')))


def test_stage0_fresh_native_binaries_need_exact_executable_downloads():
    target=spec(fresh=True);plan=stage0(target)
    target=replace(target,stage0=plan)
    bad=tuple(replace(item,executable=False) if item.name=='native/postgres' else item for item in plan.downloads)
    with pytest.raises(ValueError,match='must be executable'):
        replace(target,stage0=replace(plan,downloads=bad))


def test_controller_fresh_launch_requires_complete_stage0_before_provider(monkeypatch):
    from test_recovery_journal import fixture
    controller=module('bge_kaggle_controller')
    monkeypatch.setenv('RKB_BGE_TRANSPORT','wss')
    _,journal=fixture()
    with pytest.raises(ValueError,match='complete pinned Stage0'):
        controller.main(target_spec=spec(fresh=True),recovery_journal=journal)


@pytest.mark.asyncio
async def test_restored_runtime_requires_retained_capability_and_closes_owned_candidate():
    from regional_knowledge.kaggle_restored_database import VerifiedInitializedDatabase
    target=spec();config=dict(CONFIG,target_runtime=dict(target.wire(),sql_root='/unused'))
    with pytest.raises(Exception,match='restore_capability_required'):
        TargetRuntime(config,executed_source='/fixture',restored_database={'verified':True})
    events=[];identity=(CONFIG['run_id'],CONFIG['launch_nonce'],'restored-incarnation')
    class Candidate:
        async def aclose(self):events.append('candidate-close')
    candidate_owner=Candidate()
    class Factory:
        _capabilities=set()
        _owned_candidates={candidate_owner}
        async def verify_initialized(self,cap,database,observed):
            if cap not in self._capabilities or observed!=identity or database!=target.database:
                raise PermissionError('retained verified restore required')
            events.append('verify-capability')
            return database.connection()
    factory=Factory()
    cap=VerifiedInitializedDatabase(factory,candidate_owner,target.database,identity,None,None,0,b'',None)
    factory._capabilities.add(cap)
    class Owner:
        def __init__(self,spec):pass
        async def start_empty(self,**kwargs):raise AssertionError('restored database was initialized')
        async def aclose(self):events.append('owner-close')
    class Pool:
        def __init__(self,**kwargs):
            assert kwargs['kwargs']['user']=='rkb_reader';events.append('pool')
        async def open(self,**kwargs):pass
        async def close(self,**kwargs):events.append('pool-close')
        def connection(self):pass
    async def prepare(encoder,**kwargs):
        async def read(frame,*,encode):pass
        return PreparedRuntime(read,candidate())
    runtime=TargetRuntime(config,executed_source='/fixture',restored_database=cap,
        owner_factory=Owner,pool_factory=Pool,prepare=prepare)
    await runtime.prepare(None,run_id=identity[0],launch_nonce=identity[1],runtime_incarnation=identity[2])
    await runtime.aclose()
    assert events==['verify-capability','pool','pool-close','candidate-close','owner-close']
    assert cap not in factory._capabilities and not factory._owned_candidates


def test_controller_fresh_launch_requires_native_closure_before_provider(monkeypatch):
    controller=module('bge_kaggle_controller')
    monkeypatch.setenv('RKB_BGE_TRANSPORT','wss')
    _,journal=journal_fixture()
    target=spec(fresh=True);target=replace(target,stage0=stage0(target))
    with pytest.raises(ValueError,match='complete pinned native bundle'):
        controller.main(target_spec=target,recovery_journal=journal)


def bundled_spec():
    from regional_knowledge.kaggle_native_bundle import NativeBundleSpec
    bundle=NativeBundleSpec('native-manifest.json','9'*64)
    target=spec(fresh=True)
    database=replace(target.database,native_paths={key:str(bundle.prefix(Path(target.artifact_root))/key)
        for key in target.database.native_paths})
    inventory=tuple(entry for entry in target.artifacts if not entry['name'].startswith('native/'))+(
        dict(name=bundle.manifest_name,size=100,sha256=bundle.manifest_sha256,role='code'),
        dict(name='postgres.deb',size=500,sha256='8'*64,role='code'))
    return replace(target,database=database,artifacts=inventory,native_bundle=bundle)


@pytest.mark.asyncio
async def test_bundle_descriptor_is_bound_before_intent_and_generated_once(tmp_path):
    target=bundled_spec();target=replace(target,stage0=stage0(target))
    queue,run=queue_run(tmp_path);_,journal=journal_fixture()
    intent=await persist_empty_intent(queue,run,target,launch_nonce=queue.prepare_channel(run),
        recovery_journal=journal,sql_hashes=hashes())
    assert intent['native_bundle']==target.native_bundle.wire()
    source=generate(queue,run,target)
    config=next(ast.literal_eval(node.value) for node in ast.parse(source).body
        if isinstance(node,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='RUN_CONFIG' for t in node.targets))
    assert config['target_runtime']['native_bundle']==target.native_bundle.wire()
    assert 'native_bundle' not in {key for key in config if key!='target_runtime'}
    names={entry['name'] for entry in config['prepared_model']['entries']}
    assert any(name.endswith('/regional_knowledge/kaggle_native_bundle.py') for name in names)
    assert any(name.endswith('/regional_knowledge/kaggle_environment.py') for name in names)
    with pytest.raises(ValueError,match='exact top-level code inventory'):
        replace(target,native_bundle=replace(target.native_bundle,manifest_sha256='0'*64))
    with pytest.raises(ValueError,match='content-addressed bundle prefix'):
        replace(target,database=spec(fresh=True).database)


@pytest.mark.asyncio
async def test_runtime_extracts_bundle_after_encoder_and_forwards_retained_receipt(monkeypatch):
    target=bundled_spec();events=[];receipt=object()
    encoder=SimpleNamespace(artifacts=SimpleNamespace(root=Path(target.artifact_root),entries=tuple(
        SimpleNamespace(requirement=SimpleNamespace(**{key:entry[key] for key in ('name','size','sha256')}),
            role=entry['role']) for entry in target.artifacts)))
    def extract(root,name,sha,*,inventory):
        assert root==encoder.artifacts.root and name==target.native_bundle.manifest_name
        assert sha==target.native_bundle.manifest_sha256 and inventory==target.artifacts
        events.append('extract');return receipt
    monkeypatch.setattr('regional_knowledge.kaggle_native_bundle.prepare_native_bundle',extract)
    class Owner:
        def __init__(self,spec):pass
        async def start_empty(self,**kwargs):
            assert kwargs['native_bundle'] is receipt and kwargs['artifacts'] is encoder.artifacts
            events.append('owned')
        async def aclose(self):events.append('close')
    class Pool:
        def __init__(self,**kwargs):events.append('pool')
        async def open(self,**kwargs):pass
        async def close(self,**kwargs):pass
        def connection(self):pass
    async def prepare(encoder,**kwargs):
        async def read(frame,*,encode):pass
        return PreparedRuntime(read,candidate())
    runtime=TargetRuntime(dict(CONFIG,target_runtime=dict(target.wire(),sql_root='/unused')),
        executed_source='/fixture',owner_factory=Owner,pool_factory=Pool,prepare=prepare)
    assert events==[]
    args=dict(run_id=CONFIG['run_id'],launch_nonce=CONFIG['launch_nonce'],runtime_incarnation='bundle-inc')
    await runtime.prepare(encoder,**args);await runtime.prepare(None,**args)
    await runtime.aclose()
    assert events==['extract','owned','pool','close']


@pytest.mark.asyncio
async def test_live_version_promotion_retains_prepared_query_pg_pool_and_models():
    from types import SimpleNamespace as NS
    from test_kaggle_bootstrap import observation
    owner=object.__new__(TargetRuntime)
    database=spec().database
    owner.database=database
    pool=owner.pool=NS(connection=object())
    async def query_handler(*args,**kwargs):pass
    prepared=owner.prepared=PreparedRuntime(query_handler,candidate())
    worker=owner.recovery_worker=NS(factory=NS(_capabilities=set()),schema_control=NS(files={'verified':'bytes'}))
    owner.source_history_observer=NS(database=database)
    async def probe(*,acquire):
        assert acquire is pool.connection
        return b'{}',observation(schema_version=database.schema_version+1)
    owner.probe=probe
    result=await owner._promote_schema_version(database.schema_version+1)
    assert owner.pool is pool and owner.prepared is prepared and prepared.query_handler is query_handler
    assert result['schema_version']==database.schema_version+1
    assert worker.factory.database is owner.database is worker.schema_control.database
    assert owner.source_history_observer.database is owner.database
    assert worker.factory.schema_files is worker.schema_control.files
    assert all(result['hashes'][key]==candidate().hashes[key] for key in result['hashes'] if key!='schema')
