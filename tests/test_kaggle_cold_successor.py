"""Real terminal proof -> cold demand -> actual main -> recovery-only writer.

Only provider SDK and native runtime are synthetic; queue, immutable generated
notebook, runtime expectation and QueueWriterAuthority are real. Run in CI only.
"""
import json
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace

import pytest

from regional_knowledge.kaggle_cold_successor import (
    prepare_successor, finalize_successor, successor_source_config)
from regional_knowledge.kaggle_readiness import RunIdentity, CapabilityReceipt, TerminalObservation
from regional_knowledge.kaggle_restore_startup import SourceRestoreConfig, PreReadyRecoveryBinding
from regional_knowledge.queue_writer import QueueWriterAuthority
from regional_knowledge.recovery_journal import RecoveryJournal, StaleWriter, ObjectRef
from regional_knowledge.recovery_s3 import S3ConditionalRecoveryStore
from test_bge_controller import controller
from test_kaggle_generated_restore import target
from test_queue_writer import owner
from test_read_authority import auth
from test_participants import people


def source_config(e):
    versions = dict(server_version_num=170011, pgvector_version='0.8.2', catalog_sha256='a'*64,
        model_sha256='b'*64, tokenizer_sha256='c'*64,
        schema_revision='s1', function_revision='f1', executor_revision='e1')
    plan = SourceRestoreConfig(
        ObjectRef(e.j.prefix + '/snapshots/manifests/' + 'd'*64, 'd'*64, 100, 0),
        'original-restore', versions, 'a'*64, ('https://existing-s3.fixture.invalid',), seconds=2)
    return dict(codec='rkb.source-service.v1', run_id=e.run, stream=e.j.stream,
        database=target().database.wire(), source_restore=plan.wire(),
        source_import=dict(launch_config_sha256='a'*64))


async def terminal_then_demand(e):
    with e.q.connect(write=False) as db:
        old = dict(db.execute('SELECT * FROM runs WHERE id=?', (e.run,)).fetchone())
    identity = RunIdentity(old['provider_ref'], str(old['provider_version']), None,
        e.run, old['launch_nonce'], old['runtime_incarnation'])
    capabilities = CapabilityReceipt('synthetic-provider', '1', 'native-execution-status',
        'synthetic-exact-terminal', exact_status=True, status_identifies_execution=True)
    e.q.clock = lambda: 1002.
    assert (await e.a.confirm_terminal(identity, capabilities,
        TerminalObservation(identity, 'complete', 1001., execution_binding_verified=True),
        max_age_seconds=10)).ok
    demand = e.q.request_startup()
    assert demand['status'] == 'pending' and demand['run_id'] != e.run
    return demand['run_id']


@pytest.mark.asyncio
async def test_real_main_prepares_exact_successor_once_and_begin_restore_keeps_acceptance_closed(
        owner, monkeypatch, caplog):
    e = owner; source = source_config(e); run = await terminal_then_demand(e)
    module = controller(); pushed = []
    class Api:
        def authenticate(self): pass
        def kernels_push(self, folder, **kwargs):
            pushed.append((Path(folder)/'main.py').read_text())
            value = successor_source_config(e.q, source)
            assert value['run_id'] == run and value['database']['writer_generation'] != 'writer-1'
            return SimpleNamespace(error=False, ref='fixture-owner/rkb-bge-m3-cpu', version_number=89)
    fake = ModuleType('kaggle.api.kaggle_api_extended'); fake.KaggleApi = Api
    for name in ('kaggle', 'kaggle.api'): monkeypatch.setitem(sys.modules, name, ModuleType(name))
    monkeypatch.setitem(sys.modules, 'kaggle.api.kaggle_api_extended', fake)
    monkeypatch.setattr(module, 'BgeQueue', lambda path: e.q)
    actual_read = Path.read_text
    monkeypatch.setattr(Path, 'read_text', lambda path, *a, **kw:
        'KAGGLE_USERNAME=fixture-owner\nKAGGLE_KEY=fixture-secret\n' if str(path)=='/home/dev/.env'
        else actual_read(path, *a, **kw))
    monkeypatch.setenv('RKB_BGE_QUEUE_PATH', str(e.q.path))
    monkeypatch.setenv('RKB_BGE_BROKER_URL', 'https://fixture.example')
    monkeypatch.setenv('RKB_BGE_TRANSPORT', 'wss')
    monkeypatch.setenv('RKB_BGE_DOCKER_IMAGE', 'gcr.io/kaggle-images/python@sha256:'+'a'*64)
    monkeypatch.delenv('RKB_BGE_TARGET_CONFIG', raising=False)
    monkeypatch.delenv('RKB_BGE_SUCCESSOR_SOURCE_CONFIG', raising=False)
    class StopLoop(BaseException): pass
    def stop(seconds): raise StopLoop
    monkeypatch.setattr(module.time, 'sleep', stop)
    with pytest.raises(StopLoop):
        module.main(target_spec=target(), successor_source_config=source)
    assert len(pushed) == 1
    plan = prepare_successor(e.q, run, target(), source)  # restart reuses exact persisted plan
    bound = finalize_successor(e.q, plan)
    assert bound == successor_source_config(e.q, source)
    assert plan.target.initialized_receipt is None
    assert bound['source_restore']['launch_config_sha256'] == bound['source_import']['launch_config_sha256']
    assert bound['source_restore']['manifest'] == source['source_restore']['manifest']
    assert bound['stream'] == source['stream']
    with e.q.connect(write=False) as db:
        assert db.execute('SELECT count(*) FROM rkb_cold_successor_plans').fetchone()[0] == 1
        row = db.execute('SELECT * FROM rkb_writer_control').fetchone()
        assert row['run_id'] == e.run and not row['active']  # metadata never grants authority
    token = (e.q.path.parent/'bge-worker-credentials'/run).read_text()
    with e.q.connect(write=False) as db:
        nonce = db.execute('SELECT launch_nonce FROM runs WHERE id=?', (run,)).fetchone()[0]
    generation = e.q.attach_channel(run, token, nonce, 'cold-incarnation')['connection_generation']
    await e.a.resource_state(run, token, generation, runtime_incarnation='cold-incarnation', state='idle')
    writer = QueueWriterAuthority(e.q, run_id=run, database_generation=plan.target.database.database_generation,
        writer_generation=plan.target.database.writer_generation, stream=e.j.stream)
    store = S3ConditionalRecoveryStore(client=e.c, bucket=e.s.bucket, prefix=e.s.prefix, writer_guard=writer)
    journal = RecoveryJournal(store, stream=e.j.stream)
    binding = PreReadyRecoveryBinding(run, 'cold-incarnation', generation,
        plan.target.database.database_generation, plan.target.database.writer_generation)
    try:
        await writer.begin_restore(journal, binding, plan=SourceRestoreConfig.from_wire(bound['source_restore']))
        assert writer.recovery_current(binding)
        assert not writer.current(database_generation=binding.database_generation,
            writer_generation=binding.writer_generation)
        with pytest.raises(StaleWriter):
            await e.s.compare_and_swap(e.j.head_key, b'forged', expected_token=None, writer_generation='writer-1')
    finally: writer.close()
    assert 'fixture-secret' not in '\n'.join(r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_unresolved_old_execution_and_changed_plan_never_mint_replacement(owner):
    e=owner; source=source_config(e)
    with pytest.raises(ValueError): prepare_successor(e.q, e.run, target(), source)
    run=await terminal_then_demand(e)
    plan=prepare_successor(e.q, run, target(), source)
    assert prepare_successor(e.q, run, target(), source)==plan
    with pytest.raises(ValueError, match='immutable'):
        prepare_successor(e.q, run, target(), dict(source, source_import=dict(launch_config_sha256='b'*64)))
    with pytest.raises(ValueError, match='not finalized'): successor_source_config(e.q, source)
    with e.q.connect() as db:
        assert db.execute('SELECT count(*) FROM rkb_writer_generations').fetchone()[0]==1


@pytest.mark.asyncio
async def test_running_owner_adoption_preserves_public_routes_spool_auth_and_storage(owner):
    from regional_knowledge.kaggle_source_owners import compose_source_service
    from regional_knowledge.kaggle_source_commands import fixed_registry
    from regional_knowledge.kaggle_successor_adoption import adopt_prepared_successor
    from regional_knowledge.source_history_wss import WssNativeHistoryProofAdapter
    from test_kaggle_target_runtime import generate
    e=owner; source=source_config(e);source.pop('source_import')
    registry=fixed_registry(target().database,
        [dict(route='source.withdraw.v1',check_sha256='a'*64,apply_sha256='b'*64)])
    history=WssNativeHistoryProofAdapter(database=target().database,
        expected_catalog_sha256='a'*64,expected_acl_sha256='b'*64,expected_observer_sha256='c'*64)
    startup=compose_source_service(queue=e.q,oauth_provider=e.auth.provider,participants=e.auth.p.registry,
        object_store=SimpleNamespace(client=e.c,bucket=e.s.bucket),stream=e.j.stream,registry=registry,
        history_adapter=history,run_id=e.run,database_generation='synthetic-db',writer_generation='writer-1',
        source_restore=SourceRestoreConfig.from_wire(source['source_restore']),successor_source_json=json.dumps(source))
    kept=(startup.backend,startup.config.spool,startup.authority,startup.admission,
        dict(startup.routes),startup.source_dispatch)
    try:
        run=await terminal_then_demand(e)
        plan=prepare_successor(e.q,run,target(),source)
        token=e.q.launch_claim(run,provider_ref='owner/stable-slug')
        generated=generate(e.q,run,plan.target)
        assert run in generated
        bound=finalize_successor(e.q,plan)
        assert await adopt_prepared_successor(startup)
        assert not await adopt_prepared_successor(startup)  # same retained owner, no replacement loop
        assert (startup.backend,startup.config.spool,startup.authority,startup.admission,
            dict(startup.routes),startup.source_dispatch)==kept
        assert startup.history.routes==startup.routes
        assert startup.restorer.config.wire()==bound['source_restore']
        assert startup.config.spool.journal.store.client is e.c
        assert startup.config.spool.journal.store.bucket==e.s.bucket
        assert startup.config.writer_owner._store_identity==e.w._store_identity
        assert not startup.reads_current() and startup.coverage is None
        with e.q.connect(write=False) as db:
            nonce=db.execute('SELECT launch_nonce FROM runs WHERE id=?',(run,)).fetchone()[0]
        e.q.launch_record(run,'owner/stable-slug',provider_version=89)
        gen=e.q.attach_channel(run,token,nonce,'cold-adopted-incarnation')['connection_generation']
        await e.a.resource_state(run,token,gen,runtime_incarnation='cold-adopted-incarnation',state='idle')
        binding=PreReadyRecoveryBinding(run,'cold-adopted-incarnation',gen,
            'synthetic-db',plan.target.database.writer_generation)
        await startup.config.writer_owner.begin_restore(startup.config.spool.journal,binding,
            plan=startup.restorer.config)
        assert startup.config.writer_owner.recovery_current(binding) and not startup.reads_current()
    finally:
        await startup.restorer.aclose()
        await startup.source_dispatch.aclose()
        startup.config.spool.close();startup.config.writer_owner.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('guard',['publication_unknown','writer_active','provider_unknown'])
async def test_successor_does_not_discard_unsettled_original_authority(owner,guard):
    e=owner;source=source_config(e);run=await terminal_then_demand(e)
    with e.q.connect() as db:
        if guard=='publication_unknown':
            db.execute("UPDATE rkb_writer_control SET publication_json=?", ('{"state":"UNKNOWN"}',))
        elif guard=='writer_active':db.execute('UPDATE rkb_writer_control SET active=1')
        else:db.execute("UPDATE runs SET launch_state='dispatched' WHERE id=?",(e.run,))
    with pytest.raises(ValueError):prepare_successor(e.q,run,target(),source)
    with e.q.connect(write=False) as db:
        # The failed transaction also rolls back its first CREATE TABLE.
        assert db.execute("SELECT 1 FROM sqlite_master WHERE name='rkb_cold_successor_plans'").fetchone() is None
        assert db.execute('SELECT count(*) FROM rkb_writer_generations').fetchone()[0]==1


@pytest.mark.asyncio
async def test_configured_restart_selects_finalized_run_before_constructing_restore_store(owner,tmp_path,monkeypatch):
    from regional_knowledge.kaggle_source_owners import configured_source_service
    from regional_knowledge.object_store import S3ObjectStore
    from test_kaggle_target_runtime import generate
    e=owner;source=source_config(e);source.pop('source_import')
    source.update(command_handlers=[dict(route='source.withdraw.v1',check_sha256='a'*64,apply_sha256='b'*64)],
        catalog_sha256='a'*64,acl_sha256='b'*64,observer_sha256='c'*64)
    run=await terminal_then_demand(e)
    assert successor_source_config(e.q,source,allow_pending=True) is source
    plan=prepare_successor(e.q,run,target(),source)
    e.q.launch_claim(run,provider_ref='owner/stable-slug')
    generate(e.q,run,plan.target)
    bound=finalize_successor(e.q,plan)
    path=tmp_path/'source-owner.json';path.write_text(json.dumps(source));path.chmod(0o600)
    store=S3ObjectStore.__new__(S3ObjectStore);store.client=e.c;store.bucket=e.s.bucket
    monkeypatch.setenv('RKB_KAGGLE_SOURCE_CONFIG',str(path))
    monkeypatch.setenv('RKB_BGE_QUEUE_PATH',str(e.q.path))
    monkeypatch.setattr('regional_knowledge.personal_auth_cli._runtime',lambda:(e.auth.provider,None))
    monkeypatch.setattr('regional_knowledge.supabase_backend._object_store_from_env',lambda:store)
    e.a.close()  # Actual API-owner restart releases its existing admission flock.
    startup=configured_source_service()
    try:
        assert startup.config.writer_owner.run_id==run
        assert startup.config.writer_generation==plan.target.database.writer_generation
        assert startup.config.source_restore.wire()==bound['source_restore']
        assert json.loads(startup.config.successor_source_json)==source  # immutable original anchor retained
        assert startup.config.spool.journal.store.client is e.c
        assert not startup.reads_current()
    finally:
        await startup.restorer.aclose();await startup.source_dispatch.aclose()
        startup.config.spool.close();startup.config.writer_owner.close();startup.admission.close()
