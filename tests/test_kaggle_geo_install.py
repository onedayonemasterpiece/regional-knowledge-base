"""Fixed in-process migration; synthetic sources/journal/drill, no native effects."""
import asyncio
import hashlib
from dataclasses import replace
from contextlib import asynccontextmanager
import json
from pathlib import Path
from types import SimpleNamespace
import time

import pytest
import pytest_asyncio

from regional_knowledge.control_spool import ControlSpool,SpoolConflict
from regional_knowledge.geo_inventory import GeoInventoryObservation,geo_watermark
from regional_knowledge.geo_migration import canonical
from regional_knowledge.geo_manifest_commands import plan_geo_manifest,GEO_REVISIONS,geo_manifest_handler
from regional_knowledge.geo_read_gate import GeoReadGate
from regional_knowledge.geo_whitelist import build_geo_whitelist
from regional_knowledge.kaggle_apply import HandlerRegistry
from regional_knowledge.kaggle_geo_install import SourceGeoInstallConfig,SourceGeoInstaller,read_manifest
from regional_knowledge.kaggle_source_commands import AcceptedSourceDispatch
from regional_knowledge.kaggle_public_backend import current_binding
from regional_knowledge.kaggle_service_startup import TargetServiceStartup
from regional_knowledge.recovery_journal import RecoveryJournal
from regional_knowledge.source_history_verifier import _Coverage
from regional_knowledge.source_read_gate import binding_key
from test_geo_migration import snapshot,uid
from test_kaggle_public_backend import target
from test_read_authority import auth
from test_participants import people
from test_recovery_journal import ConditionalMemoryStore
from test_control_spool import receipt
from test_queue_writer import owner


def populate(corpus,*,extra_mentions=0):
    data=snapshot()
    data['mentions'].extend(dict(data['mentions'][0],id=uid(1000+n)) for n in range(extra_mentions))
    data['expected_counts']=data['expected_counts'].model_copy(update={'mentions':2+extra_mentions})
    for key,table in [('entities','rkb_entities'),('mentions','rkb_entity_mentions'),('aliases','rkb_entity_aliases'),
            ('relations','rkb_entity_relations'),('source_metadata','rkb_documents'),('page_metadata','rkb_pages'),
            ('region_metadata','rkb_regions'),('chunk_metadata','rkb_chunks')]:
        with corpus.connect() as db:
            for row in data[key]:
                db.execute('INSERT INTO corpus_rows VALUES(?,?,?,?,?)',
                    (table,row['id'],row.get('document_id'),row.get('revision'),canonical(row).decode()))
    values=[dict({k:v for k,v in row.items() if k not in ('lease_token','accepted_book_text')},
        normalized_spelling='alter hafen',policy_version='synthetic-v1',created_at=1.,updated_at=1.) for row in data['geo_intents']]
    fields=tuple(values[0])
    with corpus.connect() as db:
        for row in values:
            db.execute('INSERT INTO rkb_geo_intents('+','.join(fields)+') VALUES('+','.join('?' for _ in fields)+')',
                [json.dumps(row[k]) if isinstance(row[k],(dict,list)) else row[k] for k in fields])
    return data,build_geo_whitelist(**data).require_ready()


def coverage(startup,accepted=0,applied=0):
    verifier=SimpleNamespace(clock=time.monotonic,binding_current=lambda b:b==startup.binding,
        registry_hash='unit-native-proof',_registry_hash=lambda:'unit-native-proof',_routes_current=lambda:True)
    proof=_Coverage(startup.binding.database_generation,verifier,startup.binding,binding_key(startup.binding),
        time.monotonic()+30,'unit-native-proof',None,accepted,applied)
    verifier._coverage=proof;startup.coverage=proof;startup.config.spool._source_anchor=proof
    startup.config.spool._source_covered_sequence=accepted


@pytest_asyncio.fixture
async def installation(target,tmp_path,request):
    t=target;data,manifest=populate(t.auth.p.registry.corpus,extra_mentions=getattr(request,'param',0))
    directory=tmp_path/'private';directory.mkdir(mode=0o700)
    path=directory/'manifest.json';path.write_bytes(canonical(manifest.wire())+b'\n');path.chmod(0o600)
    config=SourceGeoInstallConfig(str(path),manifest.sha256,str(t.auth.p.registry.corpus.path))
    keys=tuple('geo-bootstrap11:'+manifest.sha256+':'+str(n) for n in range(len(plan_geo_manifest(manifest).payloads)))
    registry=HandlerRegistry([geo_manifest_handler(revisions=GEO_REVISIONS,check_sha256='a'*64,apply_sha256='b'*64)])
    spool=ControlSpool(t.queue,t.auth.p.registry,RecoveryJournal(ConditionalMemoryStore(),stream='geo-install-unit'),registry)
    startup=SimpleNamespace(config=SimpleNamespace(spool=spool,queue=t.queue,participants=t.auth.p.registry,
        oauth_provider=t.auth.provider,writer_owner=object(),writer_generation='writer-1',
        source_snapshot=object(),source_restore=None,geo_manifest_operation_keys=keys,history_adapter=SimpleNamespace(database=SimpleNamespace(schema_version=11))),
        binding=current_binding(t.queue),snapshot=SimpleNamespace(verified=False),closed=False,geo_installer=None)
    startup.binding_current=lambda b:b==startup.binding
    startup.reads_current=lambda:startup.snapshot.verified and startup.coverage.current()
    startup.gate=SimpleNamespace(graph_supported=lambda:True)
    startup.geo_gate=GeoReadGate(t.auth.resolver,spool,binding_current=startup.binding_current)
    startup.source_dispatch=AcceptedSourceDispatch(spool,binding_current=lambda **v:startup.snapshot.verified)
    class Writer:
        held=False
        @asynccontextmanager
        async def __call__(self,generation):
            assert generation=='writer-1'
            self.held=True
            try:yield
            finally:self.held=False
    startup.config.writer_owner=Writer()
    startup.config.writer_current=lambda **values:startup.snapshot.verified and values==dict(
        database_generation=startup.binding.database_generation,writer_generation='writer-1')
    coverage(startup)
    owner=SourceGeoInstaller(startup,config);startup.geo_installer=owner
    yield SimpleNamespace(t=t,data=data,manifest=manifest,owner=owner,startup=startup,spool=spool,config=config,keys=keys)
    await owner.aclose();await startup.source_dispatch.aclose();spool.close()


@pytest.mark.asyncio
async def test_unrelated_precondition_rejection_still_stops_original_plan(installation):
    s=installation;s.startup.snapshot.verified=True
    for index,key in enumerate(s.keys[:2]):
        assert not await s.owner.advance(s.startup.binding)
        entry=await s.spool.get(key)
        await s.spool.record_apply(key,receipt(entry.command,
            status='applied' if index==0 else 'rejected_precondition',database=s.startup.binding.database_generation))
        await s.spool.publish_outcome(key,writer_generation='writer-1')
    with pytest.raises(SpoolConflict,match='GEO native precondition rejected'):
        await s.owner.advance(s.startup.binding)
    assert (await s.spool.get(s.keys[-1])).accepted is None


@pytest.mark.asyncio
@pytest.mark.parametrize('installation',[81],indirect=True)
async def test_dependency_rejection_keeps_receipt_and_completes_original_plan_without_import_success(installation):
    from regional_knowledge.geo_manifest_commands import geo_dependency_limit_rejection
    s=installation;s.startup.snapshot.verified=True
    rejected={};seen_rejection=False
    for key in s.keys:
        assert not await s.owner.advance(s.startup.binding)
        entry=await s.spool.get(key)
        assert entry.accepted is not None
        reject=geo_dependency_limit_rejection(entry.command) or seen_rejection and json.loads(entry.command.payload)['action']=='activate'
        status='rejected_precondition' if reject else 'applied'
        await s.spool.record_apply(key,receipt(entry.command,status=status,database=s.startup.binding.database_generation))
        await s.spool.publish_outcome(key,writer_generation='writer-1')
        if reject:
            seen_rejection=True;rejected[key]=(await s.spool.get(key)).apply_receipt.outcome
    assert rejected
    assert not await s.owner.advance(s.startup.binding)
    assert not s.owner.complete and not TargetServiceStartup.imports_current(s.startup)
    for key,outcome in rejected.items():
        entry=await s.spool.get(key)
        assert entry.apply_state=='REJECTED_PRECONDITION' and entry.outcome_state=='DURABLE'
        assert entry.apply_receipt.outcome==outcome


@pytest.mark.asyncio
async def test_fixed_installer_waits_for_drill_then_pending_resume_and_durable_native_prefix(installation):
    s=installation;owner=s.owner;startup=s.startup
    assert not await owner.advance(startup.binding)
    assert (await s.spool.scope_state())['operation_count']==0
    with s.t.queue.connect(write=False) as db:assert db.execute("SELECT count(*) FROM jobs WHERE kind='source_command'").fetchone()[0]==0
    startup.snapshot.verified=True
    assert not TargetServiceStartup.imports_current(startup)
    assert not await owner.advance(startup.binding)
    first=await s.spool.get(s.keys[0]);exact=first.command_bytes
    assert first.accepted is not None
    assert (await s.spool.scope_state())['operation_count']==len(s.keys)
    assert s.t.queue._source_commands is startup.source_dispatch and startup.source_dispatch.spool is s.spool
    assert not TargetServiceStartup.imports_current(startup)
    # Repeated pending continuation uses exact stored command bytes and one job.
    assert not await owner.advance(startup.binding)
    assert (await s.spool.get(s.keys[0])).command_bytes==exact
    with s.t.queue.connect(write=False) as db:assert db.execute("SELECT count(*) FROM jobs WHERE kind='source_command'").fetchone()[0]==1
    for index,key in enumerate(s.keys):
        if index:await owner.advance(startup.binding)
        entry=await s.spool.get(key)
        assert entry.accepted is not None
        await s.spool.record_apply(key,receipt(entry.command,database=startup.binding.database_generation))
        await s.spool.publish_outcome(key,writer_generation='writer-1')
    # All local/remote receipts still need an independently current native prefix.
    assert not await owner.advance(startup.binding)
    coverage(startup,len(s.keys),len(s.keys))
    await startup.geo_gate.establish(s.keys,binding=startup.binding)
    assert await owner.advance(startup.binding) and TargetServiceStartup.imports_current(startup)
    # API-owner restart reconstructs the same plan, actor, sequence and time.
    await owner.aclose()
    other=SourceGeoInstaller(startup,s.config);startup.geo_installer=other
    try:
        assert await other.advance(startup.binding)
        assert (await s.spool.get(s.keys[0])).command_bytes==exact
    finally:await other.aclose()


@pytest.mark.asyncio
async def test_geo_drift_blocks_new_acceptance_but_preserves_accepted_pending_reconciliation(installation):
    s=installation;s.startup.snapshot.verified=True
    await s.owner.advance(s.startup.binding)
    before=await s.spool.get(s.keys[0]);exact=before.command_bytes
    row=s.data['entities'][0];s.t.auth.p.registry.corpus.put('rkb_entities',[dict(row,canonical_label='Changed geographic identity')])
    assert not await s.owner.advance(s.startup.binding)
    assert (await s.spool.get(s.keys[0])).command_bytes==exact
    assert (await s.spool.get(s.keys[1])).accepted is None
    assert not TargetServiceStartup.imports_current(s.startup)
    with s.t.queue.connect(write=False) as db:assert db.execute("SELECT count(*) FROM jobs WHERE kind='source_command'").fetchone()[0]==1


@pytest.mark.asyncio
async def test_lost_remote_acceptance_ack_resumes_same_key_and_original_bytes(installation):
    from regional_knowledge.recovery_journal import JournalUnknown
    s=installation;s.startup.snapshot.verified=True;store=s.spool.journal.store
    store.fail_head_read_after_cas=True
    with pytest.raises(JournalUnknown):await s.owner.advance(s.startup.binding)
    pending=await s.spool.get(s.keys[0]);exact=pending.command_bytes
    assert pending.seal_state=='SENT_UNKNOWN' and not s.owner.current()
    store.fail_head_read_after_cas=False;store.read_failures.clear()
    assert not await s.owner.advance(s.startup.binding)
    entry=await s.spool.get(s.keys[0])
    assert entry.accepted is not None and entry.command_bytes==exact
    assert len([key for key in store.objects if '/segments/' in key])==1
    with s.t.queue.connect(write=False) as db:assert db.execute("SELECT count(*) FROM jobs WHERE kind='source_command'").fetchone()[0]==1


@pytest.mark.asyncio
async def test_partial_reservation_cancellation_preserves_original_actor_sequence_and_time(installation,monkeypatch):
    s=installation;s.startup.snapshot.verified=True;original=s.spool._write;seen=0
    async def interrupted(call,*args):
        nonlocal seen
        value=await original(call,*args)
        if call==s.spool._stage:
            seen+=1
            if seen==1:raise asyncio.CancelledError
        return value
    monkeypatch.setattr(s.spool,'_write',interrupted)
    with pytest.raises(asyncio.CancelledError):await s.owner.advance(s.startup.binding)
    first=await s.spool.get(s.keys[0]);exact=first.command_bytes
    assert first.accepted is None and await s.spool.journal.head() is None
    monkeypatch.setattr(s.spool,'_write',original)
    await s.owner.advance(s.startup.binding)
    assert (await s.spool.get(s.keys[0])).command_bytes==exact
    assert (await s.spool.scope_state())['operation_count']==len(s.keys)


@pytest.mark.asyncio
async def test_changed_manifest_and_competing_first_sequence_refuse_before_remote_acceptance(installation):
    s=installation;s.startup.snapshot.verified=True
    bad=replace(s.config,manifest_sha256='0'*64)
    with pytest.raises(ValueError):read_manifest(bad)
    coverage(s.startup,1,0)
    with pytest.raises(SpoolConflict,match='cursor-zero'):await s.owner.advance(s.startup.binding)
    assert await s.spool.journal.head() is None


def test_private_manifest_config_rejects_unknown_fields_and_public_or_symlink_input(tmp_path):
    with pytest.raises(ValueError):SourceGeoInstallConfig.from_wire(dict(manifest_path='/private/x',manifest_sha256='a'*64,owner=True))
    directory=tmp_path/'private';directory.mkdir(mode=0o700)
    manifest=build_geo_whitelist(**snapshot()).require_ready();path=directory/'manifest.json'
    path.write_bytes(canonical(manifest.wire()));path.chmod(0o644)
    with pytest.raises(ValueError):read_manifest(SourceGeoInstallConfig(str(path),manifest.sha256,str(tmp_path/'legacy.sqlite')))
    path.chmod(0o600);link=directory/'linked.json';link.symlink_to(path)
    with pytest.raises(ValueError):read_manifest(SourceGeoInstallConfig(str(link),manifest.sha256,str(tmp_path/'legacy.sqlite')))


def test_ro_geo_watermark_rechecks_unrelated_commits_but_rejects_required_geo_drift(people):
    data,manifest=populate(people.registry.corpus);expected=geo_watermark(manifest)
    observation=GeoInventoryObservation(people.registry.corpus.path)
    try:
        assert observation.refresh(expected)==expected and observation.current(expected)
        people.registry.corpus.put('rkb_documents',[dict(id=uid(999),owner_user_id=uid(1),title='Unrelated book',source_sha256='9'*64)])
        assert not observation.current(expected)
        assert observation.refresh(expected)==expected
        people.registry.corpus.put('rkb_entities',[dict(data['entities'][0],canonical_label='Different GEO label')])
        with pytest.raises(ValueError,match='watermark_changed'):observation.refresh(expected)
        assert not observation.current(expected)
    finally:observation.close()


@pytest.mark.asyncio
async def test_real_source_owner_composition_retains_fixed_installer_and_waits_snapshot(owner,tmp_path):
    from regional_knowledge.kaggle_source_owners import compose_source_service
    from regional_knowledge.kaggle_snapshot_startup import SourceSnapshotConfig
    from regional_knowledge.kaggle_source_commands import fixed_registry
    from regional_knowledge.kaggle_domain_bindings import DOMAIN_REVISIONS
    from regional_knowledge.source_history_wss import WssNativeHistoryProofAdapter
    from test_kaggle_target_runtime import spec
    e=owner;_,manifest=populate(e.auth.p.registry.corpus)
    private=tmp_path/'owner-private';private.mkdir(mode=0o700)
    path=private/'geo.json';path.write_bytes(canonical(manifest.wire()));path.chmod(0o600)
    keys=tuple('geo-bootstrap11:'+manifest.sha256+':'+str(n) for n in range(len(plan_geo_manifest(manifest).payloads)))
    database=replace(spec(fresh=True).database,schema_revision=DOMAIN_REVISIONS.schema,
        function_revision=DOMAIN_REVISIONS.function,executor_revision=DOMAIN_REVISIONS.executor)
    registry=fixed_registry(database,[dict(route=route,check_sha256='a'*64,apply_sha256='b'*64)
        for route in ('source.import.v4','geo.manifest.install.v1')])
    history=WssNativeHistoryProofAdapter(database=database,expected_catalog_sha256='a'*64,
        expected_acl_sha256='b'*64,expected_observer_sha256='c'*64,geo_bootstrap_operation_keys=keys)
    versions=dict(schema_version=database.schema_version,server_version_num=170011,pgvector_version='0.8.2',catalog_sha256='a'*64,
        model_sha256='a'*64,tokenizer_sha256='b'*64,schema_revision=DOMAIN_REVISIONS.schema,
        function_revision=DOMAIN_REVISIONS.function,executor_revision=DOMAIN_REVISIONS.executor)
    snapshot_config=SourceSnapshotConfig('empty-before-geo',versions,('https://existing-s3.fixture.invalid',),seconds=2)
    startup=compose_source_service(queue=e.q,oauth_provider=e.auth.provider,participants=e.auth.p.registry,
        object_store=SimpleNamespace(client=e.c,bucket=e.s.bucket),stream=e.j.stream,registry=registry,
        history_adapter=history,run_id=e.run,database_generation='synthetic-db',writer_generation='writer-1',
        source_snapshot=snapshot_config,geo_manifest_operation_keys=keys,
        geo_bootstrap=SourceGeoInstallConfig(str(path),manifest.sha256,str(e.auth.p.registry.corpus.path)))
    try:
        assert startup.geo_installer.startup is startup
        assert startup.geo_installer.spool is startup.source_dispatch.spool is startup.config.spool
        assert startup.geo_installer.queue is e.q and startup.admission is e.a
        assert startup.geo_installer.inventory.path==e.auth.p.registry.corpus.path
        assert not startup.imports_current()
        assert not await startup.geo_installer.advance(current_binding(e.q))
        assert (await startup.config.spool.scope_state())['operation_count']==0
    finally:
        await startup.geo_installer.aclose();await startup.snapshot.aclose()
        await startup.source_dispatch.aclose();startup.config.spool.close();startup.config.writer_owner.close()


async def finish_migration(s):
    s.startup.snapshot.verified=True
    await s.owner.advance(s.startup.binding)
    for index,key in enumerate(s.keys):
        if index:await s.owner.advance(s.startup.binding)
        entry=await s.spool.get(key)
        await s.spool.record_apply(key,receipt(entry.command,database=s.startup.binding.database_generation))
        await s.spool.publish_outcome(key,writer_generation='writer-1')
    coverage(s.startup,len(s.keys),len(s.keys))
    await s.startup.geo_gate.establish(s.keys,binding=s.startup.binding)
    assert await s.owner.advance(s.startup.binding)


@pytest.mark.asyncio
async def test_owner_final_cutover_retires_explicit_legacy_watch_and_restart_without_old_file(installation,tmp_path):
    s=installation
    with pytest.raises(SpoolConflict,match='completed current'):await s.owner.finalize_legacy(s.owner._context())
    await finish_migration(s)
    result=await s.owner.finalize_legacy(s.owner._context())
    assert result['legacy_source_path']==s.config.legacy_source_path
    assert result['source_watermark']==geo_watermark(s.manifest)
    assert result['operation_keys']==list(s.keys)
    assert result['activation_sequence']==len(s.keys)
    assert result['activation_command_sha256']==(await s.spool.get(s.keys[-1])).command_sha256
    assert s.owner.inventory.db is None and s.owner.current()
    assert await s.owner.finalize_legacy(s.owner._context())==result
    await s.owner.aclose()
    # Auth corpus is a separate retained authority; only the explicitly selected
    # old source disappears. Constructor and resumed current proof never open it.
    old=Path(s.config.legacy_source_path);old.rename(old.with_suffix('.retired'))
    other=SourceGeoInstaller(s.startup,s.config);s.startup.geo_installer=other
    try:
        assert await other.advance(s.startup.binding)
        assert other.inventory.db is None and TargetServiceStartup.imports_current(s.startup)
        s.startup.geo_gate.invalidate()
        assert not other.current() and not await other.advance(s.startup.binding)
    finally:await other.aclose()


@pytest.mark.asyncio
async def test_final_cutover_rejects_busy_source_and_changed_geo_and_stale_writer(installation):
    import sqlite3
    s=installation;await finish_migration(s)
    lock=sqlite3.connect(s.config.legacy_source_path);lock.execute('BEGIN IMMEDIATE')
    try:
        with pytest.raises(sqlite3.OperationalError,match='locked'):
            await s.owner.finalize_legacy(s.owner._context())
        assert s.owner._load_retirement() is None
    finally:lock.rollback();lock.close()
    s.startup.config.writer_current=lambda **values:False
    with pytest.raises(SpoolConflict,match='current writer'):await s.owner.finalize_legacy(s.owner._context())
    assert s.owner._load_retirement() is None
    s.startup.config.writer_current=lambda **values:True
    s.t.auth.p.registry.corpus.put('rkb_entities',[dict(s.data['entities'][0],canonical_label='Changed before cutover')])
    with pytest.raises(SpoolConflict):await s.owner.finalize_legacy(s.owner._context())
    assert s.owner._load_retirement() is None


@pytest.mark.asyncio
async def test_private_legacy_path_is_independent_of_participants_auth_database(installation,tmp_path):
    s=installation
    from regional_knowledge.sqlite_corpus import SQLiteCorpus
    legacy=SQLiteCorpus(tmp_path/'old-source.sqlite');populate(legacy)
    explicit=replace(s.config,legacy_source_path=str(legacy.path))
    owner=SourceGeoInstaller(s.startup,explicit)
    try:
        assert owner.inventory.path==legacy.path!=s.startup.config.participants.corpus.path
        assert owner.inventory.db is None
        assert owner.inventory.refresh(owner.expected_watermark)==owner.expected_watermark
    finally:await owner.aclose()


@pytest.mark.asyncio
async def test_final_cutover_cancellation_drains_committed_receipt_before_owner_unlock(installation,monkeypatch):
    import threading
    s=installation;await finish_migration(s)
    with pytest.raises(Exception):await s.owner.finalize_legacy(replace(s.owner._context(),route='personal'))
    assert s.owner._load_retirement() is None
    committed=threading.Event();release=threading.Event();original=s.owner._retire_legacy
    def slow_commit(*args):
        result=original(*args);committed.set()
        assert release.wait(3)
        return result
    monkeypatch.setattr(s.owner,'_retire_legacy',slow_commit)
    task=asyncio.create_task(s.owner.finalize_legacy(s.owner._context()))
    try:
        for _ in range(200):
            if committed.is_set():break
            await asyncio.sleep(.01)
        assert committed.is_set()
        task.cancel();await asyncio.sleep(.02)
        assert not task.done() and s.spool._lock.locked() and s.startup.config.writer_owner.held
    finally:release.set()
    with pytest.raises(asyncio.CancelledError):await task
    assert not s.spool._lock.locked() and not s.startup.config.writer_owner.held
    assert await s.owner.advance(s.startup.binding) and s.owner.retirement is not None
    assert await s.owner.finalize_legacy(s.owner._context())==s.owner.retirement
