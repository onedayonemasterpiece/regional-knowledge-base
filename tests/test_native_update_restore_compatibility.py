"""First-context contract using real restore/journal/registry/readiness classes.

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
from regional_knowledge.kaggle_domain_bindings import CURRENT_ROUTES
from regional_knowledge.kaggle_geo_upgrade import GeoUpgradePins
from regional_knowledge.kaggle_owned_pg import OwnedDatabase
from regional_knowledge.kaggle_pg_snapshot_native import BlobRef, SnapshotError, encode, sha
from regional_knowledge.kaggle_readiness import (
    RUNTIME_HASHES, RunIdentity, SearchRuntimeExpectation, validate_search_runtime,
)
from regional_knowledge.kaggle_restore_startup import SourceRestoreConfig, SourceRestoreCoordinator
from regional_knowledge.kaggle_source_commands import decode_command, fixed_registry, receipt_wire
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
    after = list(handler.transition.target.wire()) if handler.transition and not rejected else before
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
    upgrade = importlib.import_module('regional_knowledge.kaggle_native_source_upgrade')
    activation = importlib.import_module('regional_knowledge.source_activation')
    final = activation.ACTIVATION_REVISIONS
    root = package_root()
    database = OwnedDatabase('VERIFIED_INITIALIZED', 'db-source', 'writer-2', *final.wire(),
        '/kaggle/temp/compat/socket', 'rkb', ownership_evidence='synthetic-first-context')
    old_database = replace(database, schema_revision=GEO_REVISIONS.schema,
        function_revision=GEO_REVISIONS.function, executor_revision=GEO_REVISIONS.executor)
    original_registry = fixed_registry(old_database, [
        dict(route=route, check_sha256='a' * 64, apply_sha256='b' * 64)
        for route in sorted(CURRENT_ROUTES)])
    pins = GeoUpgradePins(sha((root / 'sql/kaggle/012_geo_dependency_bound.sql').read_bytes()),
        *('c' * 64 for _ in range(4)), sha(b'synthetic-catalog11'), sha(b'synthetic-acl11'))
    native_pins = upgrade.NativeSourceUpgradePins(
        *(sha((root / 'sql/kaggle' / name).read_bytes()) for name in upgrade.ARTIFACTS),
        'd' * 64, 'e' * 64)
    registry12 = pins.registry(original_registry)
    registry = HandlerRegistry([*registry12.handlers.values(), upgrade.native_source_upgrade_handler(native_pins)])
    commands = original_commands(e.p.owner.subject)
    assert len(commands) == 20
    rejected = {5, 6, 7, 20}
    original_receipts = {c.sequence: outcome(c, original_registry, rejected=c.sequence in rejected) for c in commands}
    assert sum(r.status == 'applied' for r in original_receipts.values()) == 16
    assert sum(r.status == 'rejected_precondition' for r in original_receipts.values()) == 4
    memory = ConditionalMemoryStore()
    original = RecoveryJournal(memory, stream=e.journal.stream)
    accepted = {}
    for command in commands:
        seal = await original.seal([command], predecessor=await original.head(), writer_generation='writer-1')
        accepted[command.sequence] = seal
        await original.record_outcome(seal, command=command, database_generation='db-source',
            outcome=original_receipts[command.sequence].outcome, writer_generation='writer-1')
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
            assert 'FROM rkb_v4.applied_ops ORDER BY sequence' in sql
        async def fetchone(self):
            return next(self.rows, None)
    class PrefixConnection:
        def cursor(self, **kwargs):
            return PrefixCursor()
    from regional_knowledge.kaggle_pg_snapshot_native import applied_prefix, SnapshotLimits
    prefix_digest = await applied_prefix(PrefixConnection(), 20, SnapshotLimits())
    routes = {key: (lambda: None) for key in ('source.search', 'source.fetch', 'source.withdraw',
                                             'geo.identity_read', 'geo.members_read')}
    origin = pins.origin_closure(registry, routes)
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
    repair = geo_dependency_repair_commands(commands,
        settled_outcomes={c.operation_key: original_receipts[c.sequence].status for c in commands},
        artifact_sha256=pins.artifact_sha256)
    book = upgrade.resolved_native_source_upgrade(pins=native_pins, sequence=repair[-1].sequence + 1,
        operation_key='fixture-native-upgrade', request_id='fixture-native-upgrade-request',
        person_id=e.p.owner.subject, logical_time='2026-10-21T01:00:00Z')
    tail = (*repair, book)
    assert [c.sequence for c in tail] == list(range(21, 27))
    for command in tail:
        await journal.seal([command], predecessor=await journal.head(), writer_generation='writer-2')
    e.spool.journal, e.spool.registry = journal, registry
    binding = SimpleNamespace(run_id='fixture-run', runtime_incarnation='fixture-incarnation',
                              connection_generation=1, database_generation='db-source')
    history = WssSourceHistoryVerifier(e.spool, acquire=None, binding_current=lambda b: b is binding,
        routes=routes, expected_catalog_sha256=sha(b'synthetic-catalog14'),
        expected_acl_sha256=sha(b'synthetic-acl14'), writer_generation='writer-2',
        database=database, observer_sha256='6' * 64,
        geo_bootstrap_operation_keys=tuple(c.operation_key for c in commands),
        geo_upgrade_pins=pins, native_source_upgrade_pins=native_pins)
    assert history.origin_closure == origin
    assert history.registry_hash != origin['registry_sha256']
    events = []
    class Native:
        revisions = GEO_REVISIONS
        ledger = dict(original_receipts)
        finished = False
        async def prepare(self, b, **args):
            assert b is binding and args['expected_versions'] == versions
            assert args['database'] == database.wire()
            assert json.loads(args['manifest_bytes'])['applied_prefix_sha256'] == await applied_prefix(
                PrefixConnection(), 20, SnapshotLimits())
            assert tuple(args['expected_versions'][key] for key in
                         ('schema_revision', 'function_revision', 'executor_revision')) == GEO_REVISIONS.wire()
            events.append(('prepare', 20, self.revisions.wire()))
            return 'retained-native-session'
        async def apply(self, b, handle, **args):
            command = decode_command(args['command_bytes'])
            assert handle == 'retained-native-session' and command.sequence > 20
            assert (command.schema_revision, command.function_revision, command.executor_revision) == self.revisions.wire()
            spec = registry.resolve(command)
            assert spec.fingerprint == args['handler_sha256']
            receipt = outcome(command, registry)
            self.ledger[command.sequence] = receipt
            if spec.transition:
                self.revisions = spec.transition.target
            events.append(('apply', command.sequence, self.revisions.wire()))
            return receipt_wire(receipt)
        async def finish(self, b, handle, **args):
            assert self.revisions == final and len(self.ledger) == 26
            assert all(self.ledger[n] == receipt for n, receipt in original_receipts.items())
            current = json.loads(args['control_bytes'])
            assert current['catalog_sha256'] == history.catalog_hash
            assert current['acl_sha256'] == history.acl_hash
            assert current['registry_sha256'] == history.registry_hash
            assert current['routes'] == sorted(history.routes)
            assert [v['sequence'] for v in current['closure_transitions']] == [21, 26]
            events.append(('finish', 26, final.wire()))
            self.finished = True
            return 'retained-native-capability'
        async def verify_initialized(self, b, reference):
            assert self.finished and reference == 'retained-native-capability'
            return dict(database=database.wire(), identity=['fixture-run', 'fixture-nonce', 'fixture-incarnation'],
                        applied_sequence=26, catalog_sha256=history.catalog_hash)
    native = Native()
    physical = PhysicalSnapshotRecovery(journal, DatasetSnapshotTransport(store), native,
        registry=registry, history_verifier=history, maintenance_guard=guard,
        current=lambda b: b is binding, private_storage_current=lambda: True)
    return SimpleNamespace(physical=physical, manifest=mref, database=database, versions=versions,
        binding=binding, history=history, journal=journal, store=store, events=events,
        native=native, originals=commands, original_receipts=original_receipts, checkpoint=checkpoint, reads=reads)


def test_new_context_ships_host_hook_in_actual_source_inventory():
    _, files, _ = source_bundle(package_root())
    assert 'regional_knowledge/kaggle_native_update.py' in files.keys(), 'new context must actually ship the host hook'


@pytest.mark.asyncio
async def test_dataset2_schema11_restores_old_outcomes_then_ordered_12_14_before_capability(env):
    s = await scenario(env)
    proof = await s.physical.recover(s.manifest, s.binding, database=s.database,
        expected_versions=s.versions, expected_final_catalog_sha256=s.history.catalog_hash,
        identity=('fixture-run', 'fixture-nonce', 'fixture-incarnation'), operation_key='first-context-restore')
    assert proof.applied_sequence == 26
    assert s.events[0] == ('prepare', 20, GEO_REVISIONS.wire())
    assert s.events[-1][0] == 'finish'
    assert [e[1] for e in s.events if e[0] == 'apply'] == list(range(21, 27))
    for command in s.originals:
        remote = await s.journal.remote_outcome(command.operation_key)
        assert remote is not None
        body = json.loads(remote.data)
        assert base64.b64decode(body['outcome_b64']) == s.original_receipts[command.sequence].outcome
    assert await s.journal.remote_outcome('fixture-native-upgrade') is None
    assert s.store.latest_checkpoint.dataset_version == 2  # Tail is not Dataset Saved.
    assert set(s.reads) == {'base-manifest.json', 'source-control.json', 'journal-control.json'}


@pytest.mark.asyncio
async def test_target14_cannot_relabel_the_schema11_dataset_manifest(env):
    s = await scenario(env)
    wrong = dict(s.versions, schema_revision=s.database.schema_revision,
                 function_revision=s.database.function_revision, executor_revision=s.database.executor_revision)
    with pytest.raises(SnapshotError, match='restore_versions_or_writer_conflict'):
        await s.physical.recover(s.manifest, s.binding, database=s.database, expected_versions=wrong,
            expected_final_catalog_sha256=s.history.catalog_hash,
            identity=('fixture-run', 'fixture-nonce', 'fixture-incarnation'), operation_key='wrong-origin')
    assert not s.events and not s.native.finished


def test_launch_target14_is_expected_final_state_and_never_accepts_origin11_readiness():
    final = importlib.import_module('regional_knowledge.source_activation').ACTIVATION_REVISIONS
    hashes = {key: 'a' * 64 for key in RUNTIME_HASHES}
    target = SearchRuntimeExpectation(hashes, 'db-source', *final.wire(), 26)
    identity = RunIdentity('fixture-owner/stable-slug', '1', None,
                           'fixture-run', 'fixture-nonce', 'fixture-incarnation')
    origin = dict(hashes=hashes, database_generation='db-source', schema_revision=GEO_REVISIONS.schema,
        function_revision=GEO_REVISIONS.function, executor_revision=GEO_REVISIONS.executor,
        applied_seq=20, projection_seq=20, visibility_epoch=0, text_search_ready=True,
        dense_search_ready=True, archive_read_ready=None, proof_ready=None)
    assert not validate_search_runtime(target, origin, identity).ok
    observed = dict(origin, schema_revision=final.schema, function_revision=final.function,
                    executor_revision=final.executor, applied_seq=26, projection_seq=26)
    assert validate_search_runtime(target, observed, identity).ok
    # This is readiness-shape validation, not a fabricated recovery capability.


@pytest.mark.asyncio
@pytest.mark.parametrize('initial_revision', [11, 14])
async def test_source_restore_observation_binds_final_launch_target_while_base_versions_stay11(auth, tmp_path, initial_revision):
    upgrade = importlib.import_module('regional_knowledge.kaggle_native_source_upgrade')
    final = importlib.import_module('regional_knowledge.source_activation').ACTIVATION_REVISIONS
    queue = BgeQueue(tmp_path / 'first-context-queue.sqlite', clock=lambda: 1000.)
    queue.enqueue(auth.p.owner.subject, 'fixture-demand', ['synthetic'], kind='document')
    with queue.connect(write=False) as db:
        run = db.execute('SELECT id FROM runs').fetchone()[0]
    nonce = queue.prepare_channel(run)
    admission = queue.configure_shared_reads(auth.resolver)
    revision = GEO_REVISIONS if initial_revision == 11 else final
    expected = SearchRuntimeExpectation({key: 'a' * 64 for key in RUNTIME_HASHES},
        'db-source', *revision.wire(), 26)
    queue.expect_search_runtime(run, expected)
    token = queue.launch_claim(run)
    queue.launch_record(run, 'fixture-owner/stable-slug', provider_version=1)
    generation = queue.attach_channel(run, token, nonce, 'fixture-incarnation')['connection_generation']
    await admission.resource_state(run, token, generation, runtime_incarnation='fixture-incarnation', state='idle')
    journal = RecoveryJournal(ConditionalMemoryStore(), stream='first-context-origin')
    writer = QueueWriterAuthority(queue, run_id=run, database_generation='db-source',
                                  writer_generation='writer-2', stream=journal.stream)
    try:
        database = OwnedDatabase('VERIFIED_INITIALIZED', 'db-source', 'writer-2', *final.wire(),
            '/kaggle/temp/compat/socket', 'rkb', ownership_evidence='fixture-native-target')
        pins = GeoUpgradePins(*('b' * 64 for _ in range(7)))
        native = upgrade.NativeSourceUpgradePins(*('c' * 64 for _ in range(4)))
        versions = dict(server_version_num=170011, pgvector_version='0.8.2', catalog_sha256=pins.origin_catalog_sha256,
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
            queue=queue, geo_dependency_upgrade=pins, native_source_upgrade=native),
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
async def test_native_finish_refuses_capability_when_target14_has_no_applied_schema_transition(monkeypatch):
    import regional_knowledge.kaggle_pg_snapshot_native as native
    final = importlib.import_module('regional_knowledge.source_activation').ACTIVATION_REVISIONS
    database = OwnedDatabase('VERIFIED_INITIALIZED', 'db-source', 'writer-2', *final.wire(),
        '/kaggle/temp/compat/socket', 'rkb', ownership_evidence='synthetic-final-target')
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
        return b'synthetic-catalog11', dict(database_generation='db-source', applied_seq=20,
            projection_seq=20, text_search_ready=True, dense_search_ready=True,
            schema_revision=GEO_REVISIONS.schema, function_revision=GEO_REVISIONS.function,
            executor_revision=GEO_REVISIONS.executor)
    binary = native.NativeBinary('/synthetic/not-executed', 1, 'a' * 64, 'PostgreSQL 17.11')
    factory = native.NativeSnapshotFactory(tools=native.PhysicalPgTools(binary, binary, binary),
        database=database, identity=('fixture-run', 'fixture-nonce', 'fixture-incarnation'),
        registry=fixed_registry(replace(database, schema_revision=GEO_REVISIONS.schema,
            function_revision=GEO_REVISIONS.function, executor_revision=GEO_REVISIONS.executor),
            [dict(route='geo.manifest.install.v1', check_sha256='a' * 64, apply_sha256='b' * 64)]),
        executor_factory=lambda **kwargs: None, current=lambda value: value is binding,
        workspace_private_current=lambda path: True, native_uid=1, native_gid=1,
        probe=actual_origin_probe)
    candidate = SimpleNamespace(acquire=acquire)
    session = native._NativeSession(candidate, dict(applied_sequence=20, writer_generation='writer-1'),
        b'{}', binding, None, [])
    factory._sessions.add(session)
    control = encode(dict(database_generation='db-source', sequence=20,
                          journal_head=None, catalog_sha256=sha(b'synthetic-catalog14')))
    with pytest.raises(SnapshotError, match='native_restore_final_readiness'):
        await factory.finish(session, expected_tail=(), final_head=None,
            control_bytes=control, expected_catalog_sha256=sha(b'synthetic-catalog14'))
    assert not factory._capabilities and session in factory._sessions
