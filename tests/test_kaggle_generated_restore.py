from regional_knowledge.kaggle_current_registry import current_registry,registry_sha256
"""Generated notebook recovery composition; no native/model/provider effects."""
import ast
import asyncio
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from regional_knowledge.kaggle_recovery_composition import SnapshotRecoveryConfig,DeferredRecoveryRuntime
from regional_knowledge.kaggle_target_runtime import TargetRuntime
from regional_knowledge.kaggle_bootstrap import PreparedRuntime
from regional_knowledge.kaggle_restored_database import VerifiedInitializedDatabase
from regional_knowledge.kaggle_launch_config import target_launch_from_wire
from test_kaggle_target_runtime import spec,bundled_spec,stage0,queue_run,generate
from test_kaggle_bootstrap import candidate


def recovery():
    return SnapshotRecoveryConfig({name:dict(path='usr/bin/'+name,version_output=name+' (PostgreSQL) 17.11')
        for name in ('postgres','pg_basebackup','pg_verifybackup')},('https://storage.fixture.invalid',),1000,1000,
        max_archive_bytes=1024**2,max_unpacked_bytes=2*1024**2,seconds=2)


def target():
    fresh=bundled_spec();db=spec().database
    receipt=dict(database=db.wire(),schema_sha256='a'*64,stage0_receipt='stage0-fixture')
    return replace(fresh,database=db,initialized_receipt=receipt,stage0=stage0(fresh),recovery=recovery(),
        registry_sha256=registry_sha256())


def test_restore_pending_launch_requires_native_proof_not_fabricated_initialized_receipt():
    """A cold target cannot possess a new PG's native initialized receipt yet."""
    pending=replace(target(),initialized_receipt=None)
    assert pending.database.mode=='VERIFIED_INITIALIZED'
    assert pending.recovery is not None and not pending.recovery.drill_empty
    assert pending.bootstrap_only is False
    assert pending.initialized_receipt is None
    assert target_launch_from_wire(pending.wire())==pending
    with pytest.raises(ValueError,match='initialized'):
        replace(pending,recovery=None)  # no independent native recovery owner


def test_restore_config_roundtrip_and_explicit_mode_only():
    value=target();loaded=target_launch_from_wire(value.wire())
    assert loaded.recovery.wire()==value.recovery.wire()
    assert 'recovery' not in spec().wire()  # Existing fresh/adoption wire is stable.
    with pytest.raises(ValueError,match='initialized recovery'):
        replace(bundled_spec(),recovery=recovery())
    with pytest.raises(ValueError,match='bundle-relative'):
        replace(recovery(),binaries={**recovery().wire()['binaries'],'postgres':dict(path='../postgres',version_output='postgres (PostgreSQL) 17.11')})
    with pytest.raises(ValueError,match='exact native recovery configuration'):
        SnapshotRecoveryConfig.from_wire(dict(recovery().wire(),sql='SELECT 1'))


def _entry(source,tmp_path):
    tree=ast.parse(source);entry=tree.body.pop()
    namespace={'__name__':'generated_fixture','__file__':str(tmp_path/'main.py')}
    exec(compile(ast.Module(body=tree.body,type_ignores=[]),'fixture','exec'),namespace)
    class Redirect(ast.NodeTransformer):
        def visit_Constant(self,node):
            if isinstance(node.value,str) and node.value.startswith('/kaggle/working/rkb-tgt-'):
                return ast.copy_location(ast.Constant(str(tmp_path/Path(node.value).name)),node)
            return node
    return namespace,ast.fix_missing_locations(Redirect().visit(ast.Module(body=[entry],type_ignores=[])))


@pytest.mark.parametrize('wrong_identity',[False,True])
@pytest.mark.parametrize('restore_pending',[False,True])
def test_real_generated_restore_entrypoint_waits_native_capability_and_refuses_wrong_identity(tmp_path,monkeypatch,wrong_identity,restore_pending):
    launch=replace(target(),initialized_receipt=None) if restore_pending else target()
    q,run=queue_run(tmp_path);source=generate(q,run,launch);namespace,entry=_entry(source,tmp_path)
    config=namespace['RUN_CONFIG'];events=[];ready=asyncio.Event();released=[]
    encoder=SimpleNamespace(artifacts=SimpleNamespace(root=Path('/kaggle/working'),entries=(),role_hash=lambda roles:'d'*64))
    monkeypatch.setattr('regional_knowledge.kaggle_native_bundle.prepare_native_bundle',
        lambda *a,**k:SimpleNamespace(root=encoder.artifacts.root))
    monkeypatch.setattr('regional_knowledge.kaggle_owned_pg.ensure_private_runtime_root',lambda **k:tmp_path)
    monkeypatch.setattr('pwd.getpwuid',lambda uid:SimpleNamespace(pw_gid=1000))
    class Owner:
        def __init__(self,database):pass
        async def start_empty(self,**kw):pytest.fail('restore attempted initdb')
        async def aclose(self):events.append('owner-close')
    class Pool:
        def __init__(self,**kw):events.append('pool-created')
        async def open(self,**kw):pass
        async def close(self,**kw):events.append('pool-close')
        def connection(self):pass
    async def prepared(encoder,**kw):
        async def query(frame,**kw):return {'ok':True}
        return PreparedRuntime(query,candidate())
    class Runtime(TargetRuntime):
        def _native_source_materializer(self,encoder):return None  # Native effects are injected below.
        def __init__(self,config,**kw):super().__init__(config,**kw,owner_factory=Owner,pool_factory=Pool,prepare=prepared)
    monkeypatch.setattr('regional_knowledge.kaggle_target_runtime.TargetRuntime',Runtime)
    async def build(**kw):
        events.append('factory-configured')
        assert kw['ticket_channel'] is retained[0].ticket_channel
        assert kw['tools']['registry_sha256']==config['target_runtime']['registry_sha256']
        assert callable(kw['acquire']) and kw['limits'].max_archive_bytes==1024**2
        database=kw['database'];identity=kw['identity']
        class Candidate:
            async def aclose(self):events.append('candidate-close')
        native=Candidate()
        class Factory:
            _capabilities=set();_owned_candidates={native}
            async def verify_initialized(self,cap,db,observed):
                if cap not in self._capabilities or observed!=cap.identity or db!=database:
                    raise PermissionError('wrong native capability identity')
                events.append('verify-capability');return db.connection()
        factory=Factory()
        cap=VerifiedInitializedDatabase(factory,native,database,
            identity if not wrong_identity else (identity[0],identity[1],'wrong-incarnation'),None,None,0,b'',None)
        factory._capabilities.add(cap)
        class Worker:
            async def wait_initialized(self,*identity):
                await ready.wait();return cap
            async def __call__(self,frame):
                assert kw['current']((frame['run_id'],frame['runtime_incarnation'],frame['connection_generation']))
                events.append('native-finish');ready.set();return {'reference':'retained-native-capability'}
            async def aclose(self):events.append('recovery-close')
        return Worker()
    monkeypatch.setattr('regional_knowledge.kaggle_recovery_phases.build_recovery_worker',build)
    retained=[]
    def main(**kwargs):
        runtime=kwargs['recovery_handler'];retained.append(runtime)
        assert type(runtime) is DeferredRecoveryRuntime and kwargs['snapshot_channel'] is runtime.ticket_channel
        assert runtime.instance is None and not events
        async def execute():
            runtime.attached(1,runtime_incarnation='incarnation')
            preparing=asyncio.create_task(kwargs['prepare_runtime'](encoder,run_id=run,
                launch_nonce=config['launch_nonce'],runtime_incarnation='incarnation'))
            while 'factory-configured' not in events:await asyncio.sleep(.001)
            assert 'pool-created' not in events and not preparing.done()
            frame=dict(run_id=run,launch_nonce=config['launch_nonce'],runtime_incarnation='incarnation',connection_generation=1)
            with pytest.raises(ValueError,match='attachment'):
                await runtime(dict(frame,runtime_incarnation='forged'))
            runtime.detached(1)
            with pytest.raises(ValueError,match='attachment'):await runtime(frame)
            runtime.attached(2,runtime_incarnation='incarnation');runtime.detached(1)
            assert runtime.current((run,'incarnation',2))
            await runtime(dict(frame,connection_generation=2))
            if wrong_identity:
                with pytest.raises(PermissionError,match='capability identity'):await preparing
                assert 'pool-created' not in events
            else:
                result=await preparing
                assert result.query_handler is not None
                # This fixture injects a synthetic native Worker rather than
                # the exact WorkerRecoveryPhases class. Its purpose is native
                # proof-before-pool; the fresh generated composition and
                # TargetRuntime tests cover the real warm hookup order.
                assert events.index('native-finish')<events.index('verify-capability')<events.index('pool-created')
            await kwargs['close_runtime']()
            assert not runtime.current((run,'incarnation',2))
        asyncio.run(execute())
    namespace.update(main=main,__name__='__main__')
    exec(compile(entry,'generated-restore-entrypoint','exec'),namespace)
    assert events[-2:]==['recovery-close','owner-close']


@pytest.mark.asyncio
async def test_transport_notifies_restore_owner_before_load_and_revokes_attachment():
    from test_bge_wss_worker import module
    from contextlib import asynccontextmanager
    import json
    config={'run_id':'run','launch_nonce':'nonce','token':'token','channel':'wss://fixture.invalid/channel','shared_reads':True}
    stop=asyncio.Event();calls=[];sent=[]
    class Recovery:
        async def __call__(self,frame):pass
        def attached(self,generation,*,runtime_incarnation):calls.append(('attached',generation,runtime_incarnation))
        def detached(self,generation):calls.append(('detached',generation))
    @asynccontextmanager
    async def connector():
        incoming=asyncio.Queue()
        class Socket:
            async def send(self,raw):
                frame=json.loads(raw);sent.append(frame['type'])
                if frame['type']=='ATTACH':await incoming.put(json.dumps(dict(type='WELCOME',protocol_version=2,connection_generation=1)))
                elif frame['type']=='HEARTBEAT':
                    assert 'READY' not in sent or sent.index('HEARTBEAT')<sent.index('READY')
                    stop.set()
            async def receive(self):return await incoming.get()
        yield Socket()
    async def prepare(encoder,**identity):
        assert calls[0][0]=='attached'
        await stop.wait()
        async def read(frame,**kw):pass
        return PreparedRuntime(read,candidate())
    worker=module('bge_wss_transport').BgeWssTransport(config,lambda:object(),lambda:{},
        connector=connector,stop=stop,heartbeat_seconds=.01,maximum_seconds=1,prepare_runtime=prepare,recovery_handler=Recovery())
    await worker.run()
    assert calls[0]==('attached',1,worker.incarnation) and calls[-1]==('detached',1)
    assert 'READY' not in sent or sent.index('HEARTBEAT')<sent.index('READY')


def test_generated_fresh_snapshot_owner_configures_after_pg_without_model_reload(tmp_path,monkeypatch):
    q,run=queue_run(tmp_path)
    fresh=bundled_spec()
    value=replace(fresh,stage0=stage0(fresh),recovery=replace(recovery(),drill_empty=True),
        registry_sha256=registry_sha256())
    from regional_knowledge.kaggle_target_launch import persist_empty_intent
    from test_recovery_journal import fixture
    from test_kaggle_target_runtime import hashes
    _,journal=fixture();nonce=q.prepare_channel(run)
    asyncio.run(persist_empty_intent(q,run,value,launch_nonce=nonce,recovery_journal=journal,sql_hashes=hashes()))
    namespace,entry=_entry(generate(q,run,value),tmp_path);events=[]
    encoder=SimpleNamespace(artifacts=SimpleNamespace(root=Path('/kaggle/working'),entries=()))
    monkeypatch.setattr('pwd.getpwuid',lambda uid:SimpleNamespace(pw_gid=1000))
    monkeypatch.setattr('regional_knowledge.kaggle_owned_pg.ensure_private_runtime_root',lambda **kw:tmp_path)
    monkeypatch.setattr('regional_knowledge.kaggle_native_bundle.prepare_native_bundle',lambda *a,**kw:SimpleNamespace(root=encoder.artifacts.root))
    prepared=object()
    from regional_knowledge.kaggle_recovery_phases import WorkerRecoveryPhases
    from regional_knowledge.kaggle_warm_read import ReadHandlerSlot,ReadHandlerCompatibility
    from regional_knowledge.kaggle_warm_maintenance import WarmHandlerMaintenance
    from regional_knowledge.kaggle_worker_query import KaggleWorkerQuery
    from test_kaggle_worker_query import pinned
    retained_factory=SimpleNamespace(_current=lambda binding:None)
    class Runtime:
        def __init__(self,config,**kwargs):
            self.recovery_worker=None;self.read_handler_slot=ReadHandlerSlot(
                KaggleWorkerQuery(acquire=lambda:None,tokenizer=pinned(),
                    source_provenance=True),
                loaded_sha256='a'*64,
                compatibility=ReadHandlerCompatibility('db', 'b' * 64, 'c' * 64))
            self.prepared=None
        async def prepare(self,actual,**kwargs):
            assert actual is encoder;events.append('prepare-owned-PG-A')
            self.prepared=prepared
            TargetRuntime._bind_read_handler_maintenance(self)
            assert self.recovery_worker is None
            return prepared
        async def _owned_probe(self,*,acquire):
            return None
        async def configure_recovery(self,actual,**kwargs):
            assert actual is encoder and events==['prepare-owned-PG-A']
            events.append('same-model-capture-owner')
            worker=object.__new__(WorkerRecoveryPhases)
            worker.factory=retained_factory
            worker.read_handler_slot=None;worker.read_handler_control=None
            worker.download=lambda *a,**k:None
            worker.workspace=lambda *a,**k:None
            self.recovery_worker=worker
            TargetRuntime._bind_read_handler_maintenance(self)
            assert worker.read_handler_slot is self.read_handler_slot
            assert type(worker.read_handler_control) is WarmHandlerMaintenance
        async def aclose(self):events.append('close')
    def drill(serving,**kwargs):
        assert serving is retained_factory
        events.append('isolated-drill-owner-B');return object()
    monkeypatch.setattr('regional_knowledge.kaggle_target_runtime.TargetRuntime',Runtime)
    monkeypatch.setattr('regional_knowledge.kaggle_snapshot_drill.EmptySnapshotDrill',drill)
    def main(**kwargs):
        owner=kwargs['recovery_handler'];owner.attached(1,runtime_incarnation='inc')
        async def execute():
            assert await kwargs['prepare_runtime'](encoder,run_id=run,launch_nonce=nonce,runtime_incarnation='inc') is prepared
            assert owner.instance.recovery_worker.drill is not None
            assert type(owner.instance.recovery_worker.read_handler_control) is WarmHandlerMaintenance
            await kwargs['close_runtime']()
        asyncio.run(execute())
    namespace.update(main=main,__name__='__main__')
    exec(compile(entry,'generated-fresh-snapshot','exec'),namespace)
    assert events==['prepare-owned-PG-A','same-model-capture-owner','isolated-drill-owner-B','close']
