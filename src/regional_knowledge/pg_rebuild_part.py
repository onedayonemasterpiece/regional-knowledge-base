"""Finite private notebook caller. No provider client, controller or publication.

Assets and the config are delivered before execution. The config binds the input
manifest and prepared native/model inventory, not credentials or executable code.
Only main() loads the real pinned model; helpers support synthetic PG fixtures.
"""
import asyncio
import base64
from contextlib import asynccontextmanager
from dataclasses import replace
import gzip
import hashlib
import json
import os
from pathlib import Path
import runpy
import shutil
import sqlite3
import tarfile
import time
import psycopg

from .kaggle_apply import HandlerRegistry
from .kaggle_current_registry import current_registry
from .geo_manifest_commands import GEO_REVISIONS, geo_manifest_handler
from .kaggle_owned_pg import (
    OwnedDatabase, OwnedPostgres, configure_native_roles, create_native_roles,
    ensure_private_runtime_root, pinned_pgvector_version,
    grant_native_code_access,
)
from .kaggle_pg_snapshot_native import (
    SnapshotLimits, applied_prefix, digest_file, pack_tree, unpack_tree,
)
from .kaggle_schema_migrate import migrate
from .pg_rebuild_batch import SnapshotInput, historical_materializer, rebuild


def document(path):
    return json.loads(Path(path).read_bytes())


def extract_source(archive, root):
    with tarfile.open(archive) as source:
        source.extractall(root, filter='data')


def import_objects(inputs, destination):
    """Streaming local projection; full queue snapshots never enter the notebook."""
    with gzip.open(inputs/'input-metadata.sqlite.gz', 'rb') as source, destination.open('xb') as target:
        shutil.copyfileobj(source, target)
    with sqlite3.connect(destination) as connection:
        connection.execute('CREATE TABLE rkb_dataset_recovery_objects '
                           '(storage TEXT,key TEXT,data BLOB,token TEXT,PRIMARY KEY(storage,key))')
        with gzip.open(inputs/'journal-objects.jsonl.gz', 'rt') as source:
            for line in source:
                row = json.loads(line)
                connection.execute('INSERT INTO rkb_dataset_recovery_objects VALUES(?,?,?,?)',
                    (row['storage'], row['key'], base64.b64decode(row['data_b64'], validate=True), row['token']))


class GenesisPostgres(OwnedPostgres):
    async def _initialize_schema(self, connection, files):
        # SQL11 has mandatory revision columns. Current initialize_database is
        # deliberately for newer schemas and cannot initialize this genesis.
        async with connection.transaction():
            from psycopg import sql
            await connection.execute(sql.SQL('CREATE EXTENSION vector WITH SCHEMA public VERSION {}').format(
                sql.Literal(pinned_pgvector_version(self.spec.native_paths))))
            await create_native_roles(connection)
        await migrate(connection, {'sql/'+name: body for name, body in files.items()})
        async with connection.transaction():
            await connection.execute('INSERT INTO rkb_v4.writer_generations(generation) VALUES (%s)',
                                     (self.spec.writer_generation,))
            await connection.execute('''INSERT INTO rkb_v4.recovery_state
                (singleton,database_generation,writer_generation,applied_seq,
                 schema_revision,function_revision,executor_revision) VALUES(true,%s,%s,0,%s,%s,%s)''',
                (self.spec.database_generation, self.spec.writer_generation, *GEO_REVISIONS.wire()))
            await configure_native_roles(connection, selected_sql=tuple(files),
                                         dbname=self.spec.dbname, create_roles=False)


class HistoricalReplay:
    def __init__(self, connection, source, inputs, workspace, code, database, encoder):
        self.connection, self.source, self.inputs = connection, source, inputs
        self.workspace, self.database, self.encoder = workspace, database, encoder
        self.ranges = document(inputs/'schema-sequences.json')['ranges']
        self.files = {'sql/'+p.name: p.read_bytes() for p in (code/'sql/kaggle').glob('*.sql')}
        self.version = None
        self.encoder_seconds, self.encoder_calls, self.input_hashes = 0., 0, set()
        pins = document(inputs/'legacy-geo-handler.json')
        legacy = geo_manifest_handler(revisions=GEO_REVISIONS,
            check_sha256=pins['check_sha256'], apply_sha256=pins['apply_sha256'])
        if legacy.fingerprint != pins['handler_sha256']:
            raise ValueError('historical genesis handler differs')
        self.legacy, self.native = HandlerRegistry([legacy]), current_registry()

    @asynccontextmanager
    async def acquire(self):
        yield self.connection

    async def resolve(self, command):
        version = next(r['schema_version'] for r in self.ranges
                       if r['first'] <= command.sequence <= r['last'])
        if version != self.version:
            await migrate(self.connection, {name: raw for name, raw in self.files.items()
                                            if int(Path(name).name[:3]) <= version})
            self.version = version
        return self.legacy if command.sequence <= 20 else self.native

    async def prepare(self, command):
        if command.route != 'source.batch.v1':
            return  # The two historical seals were rejected; rebuild restores their receipts.
        fingerprint = json.loads(command.payload)['materializer_sha256']
        root = self.workspace/fingerprint
        if not root.exists():
            await asyncio.to_thread(extract_source, self.inputs/(fingerprint+'.tar.gz'), root)
        module = historical_materializer(root, fingerprint)
        worker = module.resident_source_materializer(self.encoder, current=lambda: True)
        forward = worker.encode
        async def measured(text):
            started = time.monotonic()
            try:
                return await forward(text)
            finally:
                self.encoder_seconds += time.monotonic()-started
                self.encoder_calls += 1
                self.input_hashes.add(hashlib.sha256(text.encode()).digest())
        worker.encode = measured
        await worker(command, acquire=self.acquire, database=self.database)


async def coverage(connection, limits):
    async with connection.transaction():
        generation, writer, sequence = await (await connection.execute(
            'SELECT database_generation,writer_generation,applied_seq FROM rkb_v4.recovery_state')).fetchone()
        result = dict(database_generation=generation, writer_generation=writer, applied_sequence=sequence,
                      prefix_sha256=await applied_prefix(connection, sequence, limits), tables={}, by_source={})
        for table in ('sources', 'source_stage_batches', 'source_stage_pages', 'passage_search', 'stories',
                      'story_versions', 'story_variant_versions', 'source_entity_ids',
                      'source_passage_provenance', 'source_illustrations', 'applied_ops'):
            if (await (await connection.execute('SELECT to_regclass(%s)', ('rkb_v4.'+table,))).fetchone())[0]:
                result['tables'][table] = (await (await connection.execute(
                    'SELECT count(*) FROM rkb_v4.'+table)).fetchone())[0]
                has_source = await (await connection.execute('''SELECT 1 FROM information_schema.columns
                    WHERE table_schema='rkb_v4' AND table_name=%s AND column_name='source_id' ''', (table,))).fetchone()
                if has_source:
                    rows = await (await connection.execute('SELECT source_id,count(*) FROM rkb_v4.'
                        +table+' GROUP BY source_id ORDER BY source_id')).fetchall()
                    result['by_source'][table] = {str(source): count for source, count in rows}
        if 'story_variant_versions' in result['tables']:
            result['nonempty_variant_bodies'] = (await (await connection.execute(
                "SELECT count(*) FROM rkb_v4.story_variant_versions WHERE length(btrim(body))>0")).fetchone())[0]
        result['postgres'] = (await (await connection.execute('SHOW server_version')).fetchone())[0]
        result['pgvector'] = (await (await connection.execute(
            "SELECT extversion FROM pg_extension WHERE extname='vector'")).fetchone())[0]
        return result


async def restore_archive(owner, archive, manifest, cluster, limits):
    """Same owned PG tools and bounded startup, isolated from the active cluster."""
    cluster.mkdir(mode=0o700)
    os.chown(cluster, owner.spec.uid, owner.spec.gid)
    await asyncio.to_thread(unpack_tree, archive, cluster/'data', manifest, limits)
    for path in (cluster/'data', *(cluster/'data').rglob('*')):
        os.chown(path, owner.spec.uid, owner.spec.gid)
    socket = cluster/'socket'
    socket.mkdir(mode=0o700)
    os.chown(socket, owner.spec.uid, owner.spec.gid)
    restored = OwnedPostgres(replace(owner.spec, cluster=str(cluster), socket=str(socket)), connect=owner.connect)
    restored.native_env = owner.native_env
    binary = Path(owner.spec.native_paths['postgres'])
    await restored._command(str(binary.with_name('pg_verifybackup')), str(cluster/'data'),
                            timeout=limits.timeout_seconds)
    restored.process = await restored.spawn(str(binary), '-D', str(cluster/'data'), '-k', str(socket),
        '-h', '', '-p', str(owner.spec.port), '-c', 'archive_mode=off', '-c', 'archive_command=',
        '-c', 'restore_command=', '-c', 'shared_preload_libraries=', '-c', 'max_connections=8',
        stdin=asyncio.subprocess.DEVNULL, stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL, env=restored.native_env, **restored._identity())
    try:
        async with asyncio.timeout(20):
            while restored.process.returncode is None:
                try:
                    connection = await restored.connect(**restored.spec.connection(reader=False))
                    return restored, connection
                except psycopg.OperationalError:
                    await asyncio.sleep(.1)
            raise RuntimeError('restored PG terminated before readiness')
    except BaseException:
        await restored.aclose()
        raise


async def save_checkpoint(owner, connection, output, workspace, report, limits):
    started = time.monotonic()
    before = await coverage(connection, limits)
    binary = Path(owner.spec.native_paths['postgres'])
    capture = workspace/'capture'
    capture.mkdir(mode=0o700)
    os.chown(capture, owner.spec.uid, owner.spec.gid)
    backup = capture/'backup'
    await owner._command(str(binary.with_name('pg_basebackup')), '-D', str(backup),
        '--wal-method=stream', '--checkpoint=fast', '--manifest-checksums=SHA256',
        '--no-password', '--host', owner.spec.socket, '--port', str(owner.spec.port),
        '--username', 'rkb_bootstrap', timeout=limits.timeout_seconds)
    await owner._command(str(binary.with_name('pg_verifybackup')), str(backup), timeout=limits.timeout_seconds)
    tar = workspace/'pg.tar'
    records, size, inventory, archive_size, archive_hash = await asyncio.to_thread(pack_tree, backup, tar, limits)
    manifest = dict(file_count=len(records), unpacked_bytes=size, inventory_sha256=inventory,
        backup_manifest_sha256=digest_file(backup/'backup_manifest', limits.max_unpacked_bytes)[1],
        archive_size=archive_size, archive_sha256=archive_hash, coverage=before, replay=report)
    manifest['backup_seconds'] = time.monotonic()-started
    compress_started = time.monotonic()
    compressed = output/'pg.tar.gz'
    with tar.open('rb') as source, gzip.open(compressed, 'xb', compresslevel=1) as target:
        shutil.copyfileobj(source, target)
    manifest['compress_seconds'] = time.monotonic()-compress_started
    restore_started = time.monotonic()
    tar.unlink()  # Verify the actual compressed output, not a separate earlier copy.
    with gzip.open(compressed, 'rb') as source, tar.open('xb') as target:
        shutil.copyfileobj(source, target)
    if digest_file(tar, limits.max_archive_bytes) != (archive_size, archive_hash):
        raise ValueError('compressed checkpoint differs')
    restored, restored_connection = await restore_archive(owner, tar, manifest, workspace/'restore', limits)
    try:
        async with restored_connection:
            after = await coverage(restored_connection, limits)
        if after != before:
            raise ValueError('actual restored coverage differs')
    finally:
        await restored.aclose()
    manifest['restore_verified'] = True
    manifest['restore_seconds'] = time.monotonic()-restore_started
    manifest['output_size'], manifest['output_sha256'] = digest_file(compressed, limits.max_archive_bytes)
    (output/'checkpoint.json').write_text(json.dumps(manifest, indent=2)+'\n')
    return manifest


def part_connection(deadline, marks):
    """Historical SQL uses this finite part's budget instead of live RPC deadlines."""
    class MeasuredConnection(psycopg.AsyncConnection):
        async def execute(self, query, params=None, **kwargs):
            before = time.monotonic()
            if isinstance(query, str) and query.upper().startswith('SET LOCAL STATEMENT_TIMEOUT'):
                query = "SELECT set_config('statement_timeout',%s,true)"
                params = (str(max(1, int((deadline-before)*1000))),)
            try:
                return await super().execute(query, params, **kwargs)
            finally:
                marks['sql_execute_seconds'] += time.monotonic()-before
                marks['sql_execute_calls'] += 1
    return MeasuredConnection


async def execute_part(config, started):
    """Run one part, save a verified physical checkpoint, then close every PG."""
    from .kaggle_bootstrap import PreparedArtifact, verify_artifacts
    from .kaggle_native_bundle import NativeBundleSpec, prepare_native_bundle
    from .kaggle_native_env import verified_native_environment
    from .kaggle_readiness import ArtifactRequirement
    marks = {'sql_execute_seconds': 0., 'sql_execute_calls': 0}
    MeasuredConnection = part_connection(started+config['seconds']-config['save_seconds'], marks)
    prepared = config['prepared_model']
    entries = tuple(PreparedArtifact(ArtifactRequirement(e['name'], e['size'], e['sha256']), e['role'])
                    for e in prepared['entries'])
    artifacts = verify_artifacts(prepared['root'], entries, max_total_bytes=prepared['max_total_bytes'])
    native = prepare_native_bundle(artifacts.root, config['native_manifest']['name'],
        config['native_manifest']['sha256'], inventory=prepared['entries'])
    spec = OwnedDatabase(**config['database'])
    pins = [(str(artifacts.root/e.requirement.name), e.requirement.size, e.requirement.sha256, False)
            for e in artifacts.entries if e.role == 'code'] + list(native.pins)
    await asyncio.to_thread(grant_native_code_access, artifacts.root, pins, uid=spec.uid, gid=spec.gid)
    ensure_private_runtime_root(native_uid=spec.uid, native_gid=spec.gid)
    workspace = Path(spec.cluster).parent
    workspace.mkdir(mode=0o711)
    workspace.chmod(0o711)  # Native PG can traverse; private files still use umask 077.
    output = Path(config['output_root'])
    output.mkdir(mode=0o700, exist_ok=True)
    inputs, code = Path(config['input_root']), Path(__file__).resolve().parents[2]
    path = workspace/'input.sqlite'
    await asyncio.to_thread(import_objects, inputs, path)
    source = SnapshotInput(path, storage=config['storage'], stream=config['stream'])
    owner = GenesisPostgres(spec, connect=MeasuredConnection.connect)
    connection = None
    marks['input_seconds'] = time.monotonic()-started
    try:
        if config.get('checkpoint') is not None:
            previous = config['checkpoint']
            manifest = document(previous['manifest'])
            if not manifest.get('restore_verified'):
                raise ValueError('previous checkpoint lacks actual restore result')
            archive = workspace/'previous.tar'
            if digest_file(previous['archive'], SnapshotLimits().max_archive_bytes) != (
                    manifest['output_size'], manifest['output_sha256']):
                raise ValueError('previous checkpoint archive differs')
            with gzip.open(previous['archive'], 'rb') as original, archive.open('xb') as target:
                shutil.copyfileobj(original, target)
            owner.native_env = verified_native_environment(native)
            owner, connection = await restore_archive(owner, archive, manifest, Path(spec.cluster), SnapshotLimits())
            if await coverage(connection, SnapshotLimits()) != manifest['coverage']:
                raise ValueError('previous checkpoint restored to different coverage')
        else:
            initial = workspace/'sql11'
            initial.mkdir()
            files = {p.name: p.read_bytes() for p in (code/'sql/kaggle').glob('*.sql')
                     if int(p.name[:3]) <= 11}
            for name, raw in files.items():
                (initial/name).write_bytes(raw)
            intent = dict(codec='rkb.empty-generation.v1', database=spec.wire(), journal_head=None,
                journal_stream=config['stream'], run_id=config['run_id'], launch_nonce=config['run_id'],
                sql_hashes={name: hashlib.sha256(raw).hexdigest() for name, raw in files.items()},
                native_bundle=NativeBundleSpec(native.manifest_name, native.manifest_sha256).wire())
            await owner.start_empty(intent=intent, artifacts=artifacts, sql_root=initial, native_bundle=native)
            connection = await owner.connect(**owner.spec.connection(reader=False))
        marks['bootstrap_seconds'] = time.monotonic()-started-marks['input_seconds']
        model_started = time.monotonic()
        worker = runpy.run_path(str(code/'scripts/production/bge_kaggle_worker.py'), run_name='_rkb_cpu_loader')
        loader = worker['load_encoder']
        loader.__globals__['RUN_CONFIG'] = dict(prepared_model=prepared, run_id=config['run_id'])
        encoder = await asyncio.to_thread(loader)
        marks['model_seconds'] = time.monotonic()-model_started
        history = HistoricalReplay(connection, source, inputs, workspace, code, owner.spec, encoder)
        replay_started = time.monotonic()
        report = await rebuild(connection, journal=source.journal, records=source.records(),
            database_generation=spec.database_generation, writer_generation=spec.writer_generation,
            resolve=history.resolve, prepare=history.prepare,
            keep_going=lambda: time.monotonic() < started+config['seconds']-config['save_seconds'])
        marks.update(replay_seconds=time.monotonic()-replay_started,
            encoder_seconds=history.encoder_seconds, encoder_calls=history.encoder_calls,
            unique_encoder_texts=len(history.input_hashes), encoder_device='cpu')
        save_started = time.monotonic()
        checkpoint = await save_checkpoint(owner, connection, output, workspace, report, SnapshotLimits())
        marks['backup_restore_seconds'] = time.monotonic()-save_started
        checkpoint['timings'] = marks
        checkpoint['run_id'] = config['run_id']
        checkpoint['input_manifest_sha256'] = config['input_manifest_sha256']
        checkpoint['input_scan_complete'] = 'next_sequence' not in report
        checkpoint['compute_terminal'] = False  # Only the provider receipt can establish this.
        (output/'checkpoint.json').write_text(json.dumps(checkpoint, indent=2)+'\n')
        (output/'README.txt').write_text('Private PG checkpoint. Restore pg.tar.gz with PG17 and the '
            'recorded pgvector version. Check checkpoint.json coverage and actual restore result. '
            'A partial checkpoint is not the full recovered corpus. Provider terminal is checked separately.\n')
        return checkpoint
    finally:
        if connection is not None:
            await connection.close()
        await owner.aclose()
        source.close()
