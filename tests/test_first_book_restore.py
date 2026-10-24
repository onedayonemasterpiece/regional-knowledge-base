"""First-book contract, derived from public test commit 8da6375a926d7fab8be1ae64c0d96ca0ec62b2cc.

The future hot-update host inventory test stays in its original immutable suite.
This suite validates native book/Telegram inventory and the same restore contract.

All corpus, Dataset metadata and native replies are synthetic. No archive body,
PostgreSQL, model or provider is used. Run against the pinned integrated source;
missing ordered-upgrade classes or hooks are failures, never a skipped gate.
"""
import base64
from contextlib import asynccontextmanager
from dataclasses import replace
import importlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from regional_knowledge.dataset_recovery_store import DatasetRecoveryStore, DatasetSnapshotTransport
from regional_knowledge.geo_manifest_commands import (
    GEO_REVISIONS, GEO_V2_REVISIONS, GeoManifestInstallPlan,
    geo_dependency_repair_commands, plan_geo_manifest,
)
from regional_knowledge.geo_whitelist import build_geo_whitelist
from regional_knowledge.kaggle_apply import ApplyReceipt, HandlerRegistry
from regional_knowledge.bge_queue import BgeQueue
from regional_knowledge.kaggle_dataset_base import DatasetCheckpoint, DatasetFile
from regional_knowledge.kaggle_domain_bindings import CURRENT_ROUTES,current_handler
from regional_knowledge.kaggle_owned_pg import OwnedDatabase
from regional_knowledge.kaggle_schema_migrate import latest_schema_version
from regional_knowledge.kaggle_pg_snapshot_native import BlobRef, SnapshotError, encode, sha
from regional_knowledge.kaggle_readiness import (
    RUNTIME_HASHES, RunIdentity, SearchRuntimeExpectation, validate_search_runtime,
)
from regional_knowledge.kaggle_restore_startup import SourceRestoreConfig, SourceRestoreCoordinator
from regional_knowledge.kaggle_source_commands import decode_command, receipt_wire
from regional_knowledge.kaggle_target_launch import source_bundle
from regional_knowledge.pg_snapshot_recovery import PhysicalSnapshotRecovery
from regional_knowledge.recovery_journal import ObjectRef, RecoveryJournal
from regional_knowledge.queue_writer import QueueWriterAuthority
from regional_knowledge.source_history_wss import WssSourceHistoryVerifier
from test_control_spool import env
from test_geo_migration import snapshot, uid
from test_participants import people
from test_read_authority import auth
from test_recovery_journal import ConditionalMemoryStore


def package_root():
    import regional_knowledge
    return Path(regional_knowledge.__file__).resolve().parents[2]


def original_commands(person):
    data = snapshot()
    template = data['mentions'][0]
    data['mentions'].extend(dict(template, id=uid(1000 + n)) for n in range(81))
    data['expected_counts'] = data['expected_counts'].model_copy(update={'mentions': 83})
    plan = plan_geo_manifest(build_geo_whitelist(**data).require_ready())
    items = [item for raw in plan.payloads[1:-1] for item in json.loads(raw)['items']]
    assert len(items) >= 18
    batches = tuple(encode(dict(codec='rkb.geo-manifest-command.v1', action='batch',
                                items=items[n::18])) for n in range(18))
    split = GeoManifestInstallPlan(plan.manifest_id, plan.index_sha256,
                                  (plan.payloads[0], *batches, plan.payloads[-1]))
    return split.commands(first_sequence=1, operation_key='fixture-original-geo',
        request_id='fixture-original-request', person_id=person,
        logical_time='2026-10-21T00:00:00Z')


def outcome(command, registry, *, rejected=False):
    handler = registry.resolve(command)
    before = [command.schema_revision, command.function_revision, command.executor_revision]
    after = before
    status = 'rejected_precondition' if rejected else 'applied'
    body = encode(dict(codec='rkb.applied-outcome.v1', operation_key=command.operation_key,
        command_sha256=sha(command.encode()), cursor=command.sequence, status=status,
        ids=command.ids, request_id=command.request_id, person_id=command.person_id,
        route=command.route, logical_time=command.logical_time, before=before, after=after,
        handler_sha256=handler.fingerprint, result_b64=base64.b64encode(b'{}').decode(),
        result_sha256=sha(b'{}')))
    return ApplyReceipt('db-source', command.operation_key, command.sequence,
                        sha(command.encode()), status, body, sha(body), False)


@asynccontextmanager
async def guard(binding):
    yield


async def scenario(e):
    # These are actual reviewed PR212 classes, not local substitute transitions.
    activation = importlib.import_module('regional_knowledge.source_activation')
    final = activation.ACTIVATION_REVISIONS
    root = package_root()
    database = OwnedDatabase('VERIFIED_INITIALIZED', 'db-source', 'writer-2', '/kaggle/temp/compat/socket', 'rkb', ownership_evidence='synthetic-first-context')
    old_database = replace(database, schema_version=11)
    original_registry = HandlerRegistry([current_handler(route,check_sha256='a'*64,apply_sha256='b'*64,
        revisions=GEO_REVISIONS) for route in sorted(CURRENT_ROUTES)])
    from regional_knowledge.kaggle_current_registry import current_registry
    registry=current_registry()
    commands = original_commands(e.p.owner.subject)
    assert len(commands) == 20
    rejected = {5, 6, 7, 20}
    original_receipts = {c.sequence: outcome(c, original_registry, rejected=c.sequence in rejected) for c in commands}
    assert sum(r.status == 'applied' for r in original_receipts.values()) == 16
    assert sum(r.status == 'rejected_precondition' for r in original_receipts.values()) == 4
    memory = ConditionalMemoryStore()
    original = RecoveryJournal(memory, stream=e.journal.stream)
    genesis=encode(dict(codec='rkb.source-genesis.v1',stream=original.stream,
        database_generation='db-source',sequence=0,catalog_sha256='a'*64,acl_sha256='b'*64,
        registry_sha256='c'*64,routes=['old.routes'],verifier_sha256='d'*64))
    await original._fenced_exact(original.prefix+'/source-genesis/'+sha(b'db-source'),genesis,
        expected_token=None,writer_generation='writer-1',limit=8192)
    accepted = {}
    e.spool.journal=original;e.spool.registry=original_registry
    for command in commands:
        await e.spool.stage_domain_command(e.p.admin,command,database_generation='db-source')
        seal=await e.spool.seal(e.p.admin,command.operation_key,predecessor=await original.head(),writer_generation='writer-1')
        accepted[command.sequence] = seal
        await e.spool.record_apply(command.operation_key,original_receipts[command.sequence])
        await e.spool.publish_outcome(command.operation_key,writer_generation='writer-1')
    boundary = await original.head()
    prefix_rows = tuple((c.sequence, c.operation_key, sha(c.encode()), original_registry.resolve(c).fingerprint,
        accepted[c.sequence].manifest.sha256, original_receipts[c.sequence].status,
        original_receipts[c.sequence].outcome_sha256, 'db-source', 'writer-1') for c in commands)
    class PrefixCursor:
        async def __aenter__(self):
            self.rows = iter(prefix_rows)
            return self
        async def __aexit__(self, *args):
            pass
        async def execute(self, sql, parameters):
            # Lower-bound physical snapshots hash ONLY their pre-backup
            # committed prefix; later PG rows are not part of saved_seq.
            assert 'FROM rkb_v4.applied_ops WHERE sequence<=%s ORDER BY sequence LIMIT %s' in sql
            assert parameters == (20, SnapshotLimits().max_receipts+1)
        async def fetchone(self):
            return next(self.rows, None)
    class PrefixConnection:
        def cursor(self, **kwargs):
            return PrefixCursor()
    from regional_knowledge.kaggle_pg_snapshot_native import applied_prefix, SnapshotLimits
    prefix_digest = await applied_prefix(PrefixConnection(), 20, SnapshotLimits())
    routes = {key: (lambda: None) for key in ('source.search', 'source.fetch', 'source.withdraw',
                                             'geo.identity_read', 'geo.members_read')}
    origin=dict(catalog_sha256=sha(b'old-catalog'),acl_sha256=sha(b'old-acl'),
        registry_sha256='f'*64,routes=['old.routes'],closure_transitions=[{'legacy':'ignored'}])
    versions = dict(server_version_num=170011, pgvector_version='0.8.2',
        catalog_sha256=origin['catalog_sha256'], model_sha256='1' * 64, tokenizer_sha256='2' * 64,
        schema_revision=GEO_REVISIONS.schema, function_revision=GEO_REVISIONS.function,
        executor_revision=GEO_REVISIONS.executor)
    control = encode(dict(codec='rkb.source-base-control.v1', stream=original.stream,
        database_generation='db-source', sequence=20, journal_head=boundary.reference.wire(),
        heads=[], **origin))
    ckey = original.prefix + '/source-base-control/' + sha(control)
    blob = BlobRef('dataset-blobs/fixture/pg-fixture.blob', len(b'archive-not-downloaded'),
                   sha(b'archive-not-downloaded'), 'fixture-version')
    manifest = encode(dict(codec='rkb.pg-physical-base.v1', stream=original.stream,
        database_generation='db-source', writer_generation='writer-1', applied_sequence=20,
        journal_head=boundary.reference.wire(), versions=versions, blob=vars(blob),
        source_control=dict(key=ckey, sha256=sha(control), length=len(control)),
        applied_prefix_sha256=prefix_digest, system_identifier='1234567890', file_count=3, unpacked_bytes=100,
        inventory_sha256='4' * 64, backup_manifest_sha256='5' * 64))
    journal_control = encode(dict(codec='rkb.dataset-journal.v1', objects=[
        dict(key=key, token=value.token, data_b64=base64.b64encode(value.data).decode())
        for key, value in sorted(memory.objects.items())]))
    mref = ObjectRef(original.prefix + '/snapshots/manifests/' + sha(manifest), sha(manifest), len(manifest), 20)
    def pin(name, raw):
        return DatasetFile(name, len(raw), sha(raw))
    checkpoint = DatasetCheckpoint('fixture-owner', 'fixture-checkpoints', 2, 'db-source', 20,
        pin('base-manifest.json', manifest), DatasetFile('pg-fixture.blob', blob.size, blob.sha256),
        pin('source-control.json', control), pin('journal-control.json', journal_control))
    reads = []
    class Publisher:
        ref = 'fixture-owner/fixture-checkpoints'
        def read_control(self, file, *, version):
            assert version == 2 and file != checkpoint.archive
            reads.append(file.name)
            raw = {'base-manifest.json': manifest, 'source-control.json': control,
                   'journal-control.json': journal_control}[file.name]
            assert (len(raw), sha(raw)) == (file.size, file.sha256)
            return raw
    class Guard:
        def bind_store(self, store):
            self.store = store
        @asynccontextmanager
        async def __call__(self, generation):
            assert generation == 'writer-2'
            yield
    with e.q.connect() as db:
        db.execute('CREATE TABLE rkb_writer_control(id INTEGER PRIMARY KEY,storage_sha256 TEXT)')
        db.execute("INSERT INTO rkb_writer_control VALUES(1,'synthetic-original-authority')")
    store = DatasetRecoveryStore(e.q, checkpoint=checkpoint, manifest_ref=mref, publisher=Publisher(),
        writer_guard=Guard(), stream=original.stream)
    journal = RecoveryJournal(store, stream=original.stream)
    # Rejected GEO data commands replay normally after the SQL runner; no
    # schema transition is accepted or appended to the data journal.
    from regional_knowledge.kaggle_native_source_bindings import new_native_command
    tail=tuple(new_native_command(replace(commands[n-1],sequence=21+i,operation_key='ordinary-repair-'+str(n)))
        for i,n in enumerate(sorted(rejected)))
    e.spool.journal,e.spool.registry=journal,registry
    for command in tail:
        await e.spool.stage_domain_command(e.p.admin,command,database_generation='db-source')
        await e.spool.seal(e.p.admin,command.operation_key,predecessor=await journal.head(),writer_generation='writer-2')
    binding = SimpleNamespace(run_id='fixture-run', runtime_incarnation='fixture-incarnation',
                              connection_generation=1, database_generation='db-source',min_data_seq=0,provider_version='1')
    history = WssSourceHistoryVerifier(e.spool, acquire=None, binding_current=lambda b: b is binding, routes=routes, expected_schema_version=database.schema_version, writer_generation='writer-2', database=database, observer_sha256='6' * 64, geo_bootstrap_operation_keys=tuple((c.operation_key for c in commands)))
    assert history.registry_hash != origin['registry_sha256']
    events = []
    class Native:
        schema_version = 11
        ledger = dict(original_receipts)
        finished = False
        async def prepare(self, b, **args):
            assert b is binding and args['expected_versions'] == versions
            assert args['database'] == database.wire()
            assert json.loads(args['manifest_bytes'])['applied_prefix_sha256'] == await applied_prefix(
                PrefixConnection(), 20, SnapshotLimits())
            assert tuple(args['expected_versions'][key] for key in
                         ('schema_revision', 'function_revision', 'executor_revision')) == GEO_REVISIONS.wire()
            events.append(('prepare',20,self.schema_version))
            self.schema_version=database.schema_version
            events.append(('auto-migrate',20,self.schema_version))
            return 'retained-native-session'
        async def apply(self, b, handle, **args):
            command = decode_command(args['command_bytes'])
            assert handle == 'retained-native-session' and command.sequence > 20
            assert self.schema_version==database.schema_version
            spec = registry.resolve(command)
            assert spec.fingerprint == args['handler_sha256']
            receipt = outcome(command, registry)
            self.ledger[command.sequence] = receipt
            events.append(('apply',command.sequence,self.schema_version))
            return receipt_wire(receipt)
        async def finish(self, b, handle, **args):
            assert self.schema_version==database.schema_version and len(self.ledger)==20
            assert all(self.ledger[n]==receipt for n,receipt in original_receipts.items())
            current=json.loads(args['control_bytes'])
            assert current['schema_version']==database.schema_version
            assert not {'catalog_sha256','acl_sha256','closure_transitions'} & current.keys()
            assert current['registry_sha256']==history.registry_hash
            events.append(('finish',20,self.schema_version))
            self.finished = True
            return 'retained-native-capability'
        async def verify_initialized(self, b, reference):
            assert self.finished and reference == 'retained-native-capability'
            return dict(database=database.wire(), identity=['fixture-run', 'fixture-nonce', 'fixture-incarnation'],
                        applied_sequence=20,schema_version=database.schema_version)
    native = Native()
    physical = PhysicalSnapshotRecovery(journal, DatasetSnapshotTransport(store), native,
        registry=registry, history_verifier=history, maintenance_guard=guard,
        current=lambda b: b is binding, private_storage_current=lambda: True)
    return SimpleNamespace(physical=physical, manifest=mref, database=database, versions=versions,
        binding=binding, history=history, journal=journal, store=store, events=events,
        native=native, originals=commands, original_receipts=original_receipts, checkpoint=checkpoint, reads=reads)


def test_first_book_context_ships_actual_import_and_telegram_modules():
    _, files, _ = source_bundle(package_root())
    required={'kaggle_source_materializer','source_activation','telegram_runtime_channel'}
    assert {f'regional_knowledge/{name}.py' for name in required} <= files.keys()


@pytest.mark.asyncio
async def test_dataset2_base20_ignores_old_closure_and_becomes_ready_before_tail(env):
    s = await scenario(env)
    proof = await s.physical.recover(s.manifest, s.binding, database=s.database,
        expected_versions=s.versions, expected_schema_version=s.database.schema_version,
        identity=('fixture-run', 'fixture-nonce', 'fixture-incarnation'), operation_key='first-context-restore')
    assert proof.applied_sequence == 20
    assert s.events[0] == ('prepare',20,11)
    assert s.events[-1][0] == 'finish'
    assert not [e for e in s.events if e[0] == 'apply']
    for command in s.originals:
        remote = await s.journal.remote_outcome(command.operation_key)
        assert remote is not None
        body = json.loads(remote.data)
        assert base64.b64decode(body['outcome_b64']) == s.original_receipts[command.sequence].outcome
    assert await s.journal.remote_outcome('fixture-native-upgrade') is None
    assert s.store.latest_checkpoint.dataset_version == 2  # Tail is not Dataset Saved.
    assert set(s.reads) == {'base-manifest.json', 'source-control.json', 'journal-control.json'}


@pytest.mark.asyncio
async def test_dataset2_keeps_model_abi_bound_before_native_prepare(env):
    s = await scenario(env)
    wrong=dict(s.versions,model_sha256='f'*64)
    with pytest.raises(SnapshotError, match='restore_versions_or_writer_conflict'):
        await s.physical.recover(s.manifest, s.binding, database=s.database, expected_versions=wrong,
            expected_schema_version=s.database.schema_version,
            identity=('fixture-run', 'fixture-nonce', 'fixture-incarnation'), operation_key='wrong-origin')
    assert not s.events and not s.native.finished


def test_launch_target14_is_expected_final_state_and_never_accepts_origin11_readiness():
    final = importlib.import_module('regional_knowledge.source_activation').ACTIVATION_REVISIONS
    hashes = {key: 'a' * 64 for key in RUNTIME_HASHES}
    target = SearchRuntimeExpectation(hashes, 'db-source', 26)
    identity = RunIdentity('fixture-owner/stable-slug', '1', None,
                           'fixture-run', 'fixture-nonce', 'fixture-incarnation')
    origin = dict(hashes=hashes, database_generation='db-source', schema_version=11, applied_seq=20, projection_seq=20, visibility_epoch=0, text_search_ready=True, dense_search_ready=True, archive_read_ready=None, proof_ready=None)
    assert not validate_search_runtime(target, origin, identity).ok
    observed = dict(origin, schema_version=target.schema_version, applied_seq=26, projection_seq=26)
    assert validate_search_runtime(target, observed, identity).ok
    # This is readiness-shape validation, not a fabricated recovery capability.


@pytest.mark.asyncio
@pytest.mark.parametrize('initial_revision', [11, 14])
async def test_source_restore_observation_binds_final_launch_target_while_base_versions_stay11(auth, tmp_path, initial_revision):
    final = importlib.import_module('regional_knowledge.source_activation').ACTIVATION_REVISIONS
    queue = BgeQueue(tmp_path / 'first-context-queue.sqlite', clock=lambda: 1000.)
    queue.enqueue(auth.p.owner.subject, 'fixture-demand', ['synthetic'], kind='document')
    with queue.connect(write=False) as db:
        run = db.execute('SELECT id FROM runs').fetchone()[0]
    nonce = queue.prepare_channel(run)
    admission = queue.configure_shared_reads(auth.resolver)
    revision = GEO_REVISIONS if initial_revision == 11 else final
    expected = SearchRuntimeExpectation({key: 'a' * 64 for key in RUNTIME_HASHES}, 'db-source', 26, schema_version=initial_revision if initial_revision == 11 else latest_schema_version())
    queue.expect_search_runtime(run, expected)
    token = queue.launch_claim(run)
    queue.launch_record(run, 'fixture-owner/stable-slug', provider_version=1)
    generation = queue.attach_channel(run, token, nonce, 'fixture-incarnation')['connection_generation']
    await admission.resource_state(run, token, generation, runtime_incarnation='fixture-incarnation', state='idle')
    journal = RecoveryJournal(ConditionalMemoryStore(), stream='first-context-origin')
    writer = QueueWriterAuthority(queue, run_id=run, database_generation='db-source',
                                  writer_generation='writer-2', stream=journal.stream)
    try:
        database = OwnedDatabase('VERIFIED_INITIALIZED', 'db-source', 'writer-2', '/kaggle/temp/compat/socket', 'rkb', ownership_evidence='fixture-native-target')
        versions = dict(server_version_num=170011, pgvector_version='0.8.2', catalog_sha256='b'*64,
            model_sha256='d' * 64, tokenizer_sha256='e' * 64, schema_revision=GEO_REVISIONS.schema,
            function_revision=GEO_REVISIONS.function, executor_revision=GEO_REVISIONS.executor)
        manifest = ObjectRef(journal.prefix + '/snapshots/manifests/' + 'f' * 64, 'f' * 64, 100, 20)
        checkpoint = DatasetCheckpoint('fixture-owner', 'fixture-checkpoints', 2, 'db-source', 20,
            DatasetFile('base-manifest.json', 100, 'f' * 64), DatasetFile('pg.blob', 32, '1' * 64),
            DatasetFile('source-control.json', 100, '2' * 64), DatasetFile('journal-control.json', 100, '3' * 64))
        restore = SourceRestoreConfig(manifest, 'fixture-first-context', versions, 'a' * 64,
                                      ('https://storage.googleapis.com',), dataset_checkpoint=checkpoint)
        startup = SimpleNamespace(config=SimpleNamespace(writer_owner=writer,
            history_adapter=SimpleNamespace(database=database), spool=SimpleNamespace(journal=journal),
            queue=queue),
            history=SimpleNamespace(catalog_hash='4' * 64))
        coordinator = SourceRestoreCoordinator(startup, restore)
        if initial_revision == 11:
            with pytest.raises(SnapshotError, match='restore_pinned_launch_differs'):
                coordinator._observe()
        else:
            binding, observed_nonce = coordinator._observe()
            assert observed_nonce == nonce and binding.run_id == run
        assert queue.status()['state'] == 'starting'
        assert not writer.current(database_generation='db-source', writer_generation='writer-2')
        assert dict(restore.versions)['schema_revision'] == GEO_REVISIONS.schema
        assert SourceRestoreConfig.from_wire(restore.wire()).dataset_checkpoint.dataset_version == 2
    finally:
        writer.close()
        admission.close()


@pytest.mark.asyncio
async def test_native_finish_refuses_capability_when_ledger_is_behind(monkeypatch, capsys):
    import regional_knowledge.kaggle_pg_snapshot_native as native
    final = importlib.import_module('regional_knowledge.source_activation').ACTIVATION_REVISIONS
    database = OwnedDatabase('VERIFIED_INITIALIZED', 'db-source', 'writer-2', '/kaggle/temp/compat/socket', 'rkb', ownership_evidence='synthetic-final-target')
    binding = object()
    class Connection:
        async def execute(self, sql, args=None):
            assert 'applied_seq,writer_generation' in sql
            return self
        async def fetchone(self):
            return 20, 'writer-1'
        async def commit(self):
            pass
    @asynccontextmanager
    async def acquire():
        yield Connection()
    async def handover(*args, **kwargs):
        pass  # Native SQL I/O boundary only, not a replacement restore factory.
    monkeypatch.setattr(native, 'handover_writer', handover)
    async def actual_origin_probe(**kwargs):
        return b'{}', dict(schema_version=11, database_generation='db-source', applied_seq=20, projection_seq=20, text_search_ready=True, dense_search_ready=True)
    binary = native.NativeBinary('/synthetic/not-executed', 1, 'a' * 64, 'PostgreSQL 17.11')
    factory = native.NativeSnapshotFactory(tools=native.PhysicalPgTools(binary, binary, binary),
        database=database, identity=('fixture-run', 'fixture-nonce', 'fixture-incarnation'),
        registry=HandlerRegistry([current_handler('geo.manifest.install.v1',check_sha256='a'*64,apply_sha256='b'*64,revisions=GEO_REVISIONS)]),
        executor_factory=lambda **kwargs: None, current=lambda value: value is binding,
        workspace_private_current=lambda path: True, native_uid=1, native_gid=1,
        probe=actual_origin_probe)
    candidate = SimpleNamespace(acquire=acquire)
    session = native._NativeSession(candidate, dict(applied_sequence=20, writer_generation='writer-1'),
        b'{}', binding, None, [])
    factory._sessions.add(session)
    control = encode(dict(database_generation='db-source', sequence=20,
                          journal_head=None,schema_version=16))
    with pytest.raises(SnapshotError, match='native_restore_final_readiness'):
        await factory.finish(session, expected_tail=(), final_head=None,
            control_bytes=control, expected_schema_version=16)
    assert not factory._capabilities and session in factory._sessions
    observed = [json.loads(line) for line in capsys.readouterr().out.splitlines()
                if 'native_restore_final_observation' in line]
    assert len(observed) == 1
    assert observed[0]['schema_version']==11 and observed[0]['expected_schema_version']==16
    assert b'synthetic-catalog11'.decode() not in json.dumps(observed[0])


@pytest.mark.asyncio
async def test_dataset2_history_verifies_original_receipts_with_only_current_routes(env):
    from regional_knowledge.kaggle_source_history_native import NativeSourceHistoryObserver
    from regional_knowledge.kaggle_current_registry import current_registry
    from test_source_history_wss import NativePg,NativeCursor
    s=await scenario(env)
    await s.physical.recover(s.manifest,s.binding,database=s.database,expected_versions=s.versions,
        identity=('fixture-run','fixture-nonce','fixture-incarnation'),operation_key='current-registry-history')
    for command,seal in await s.physical._tail(await s.journal.head(),
            (await s.physical._manifest(s.manifest))[2],20):
        await s.native.apply(s.binding,'retained-native-session',command_bytes=command.encode(),
            handler_sha256=s.physical.registry.resolve(command).fingerprint)
    accepted={};ref=(await s.journal.head()).reference
    while ref is not None:
        previous,raws=await s.journal.read_segment(ref)
        for raw in raws:accepted[decode_command(raw).sequence]=ref
        ref=previous
    class Cursor(NativeCursor):
        async def execute(self,sql,args=None):
            result=await super().execute(sql,args)
            if 's.applied_seq FROM rkb_v4.recovery_state' in sql:
                self.rows=[('db-source','writer-2',self.pg.applied_seq)]
            elif 'WHERE NOT (source_id=ANY' in sql:self.rows=[(False,)]
            return result
    class Pg(NativePg):
        def cursor(self,**kwargs):return Cursor(self)
    pg=Pg();pg.applied_seq=24;pg.nonempty=True
    for sequence,receipt in sorted(s.native.ledger.items()):
        pg.receipts.append((sequence,receipt.operation_key,receipt.command_sha256,
            json.loads(receipt.outcome)['handler_sha256'],accepted[sequence].sha256,
            receipt.status,receipt.outcome_sha256,receipt.outcome_sha256,len(receipt.outcome),
            'db-source',receipt.command_sha256))
    async def probe(**kwargs):return b'{}',dict(schema_version=s.database.schema_version,
        database_generation='db-source',applied_seq=24)
    registry=current_registry()
    observer=NativeSourceHistoryObserver(acquire=pg.acquire,database=s.database,
        identity=('fixture-run','fixture-nonce','fixture-incarnation'),registry=registry,
        current=lambda key:key==('fixture-run','fixture-incarnation',1),probe=probe)
    observer.attached(1);s.history.observer_hash=observer.code_hash
    async def phase(binding,args):
        frame=dict(type='DISPATCH',protocol_version=2,connection_generation=1,id='current-history',claim='claim',
            kind='recovery_phase',run_id='fixture-run',launch_nonce='fixture-nonce',runtime_incarnation='fixture-incarnation',
            database_generation='db-source',writer_generation='writer-2',phase='source_history',operation_key=sha(encode(args)),
            arguments=args,request_sha256=sha(encode({'phase':'source_history','arguments':args})),
            compute_claim='shared-slot',dispatch_generation=1)
        value=(await observer(frame))['value']
        assert 'error' not in value,value
        return value
    s.history._phase=phase
    try:
        coverage=await s.history.restore(s.binding)
        assert coverage.current() and coverage.applied_sequence==24
        assert len(coverage.pending)==4  # New outcomes await a real Dataset checkpoint.
        assert set(registry.handlers)=={handler.route for handler in registry.handlers.values()}
        from regional_knowledge.kaggle_native_source_bindings import NATIVE_ROUTES
        assert set(registry.handlers)==NATIVE_ROUTES
        assert all(s.native.ledger[n]==receipt for n,receipt in s.original_receipts.items())
        for command in s.originals:
            retained=await env.spool.get(command.operation_key)
            assert retained.command_bytes==command.encode()
            assert retained.apply_receipt.outcome==s.original_receipts[command.sequence].outcome
            assert retained.outcome_state=='DURABLE'
        # A current registry is not permission to relabel the old PG receipt.
        from regional_knowledge.control_spool import SpoolConflict
        row=list(pg.receipts[0]);row[3]='f'*64;pg.receipts[0]=tuple(row)
        with pytest.raises(SpoolConflict,match='historical handler receipt'):
            await s.history.restore(s.binding)
        assert not observer._sessions
    finally:await observer.aclose()
