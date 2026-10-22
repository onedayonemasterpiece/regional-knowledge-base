"""Controller-owned immutable restore plans in the existing private queue.

Preparing metadata grants no writer capability. The existing SourceRestoreCoordinator
must still acquire recovery ownership and verify the native restored database.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
import hashlib
import json
import os
import stat
import uuid

from .kaggle_launch_config import target_launch_from_wire
from .kaggle_restore_startup import SourceRestoreConfig
from .kaggle_target_runtime import TargetLaunchSpec


def _encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


def read_source_config(path):
    """Explicit private metadata path; no environment or credential discovery."""
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result: raise ValueError('duplicate source configuration field')
            result[key] = value
        return result
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode) or before.st_mode & 0o077 or not 1 <= before.st_size <= 65536:
            raise ValueError('private bounded source configuration required')
        data = os.read(fd, 65537)
        after = os.fstat(fd)
        if (len(data) != before.st_size or os.read(fd, 1)
                or (after.st_size, after.st_mtime_ns) != (before.st_size, before.st_mtime_ns)):
            raise ValueError('source configuration changed during read')
    finally: os.close(fd)
    value = json.loads(data, object_pairs_hook=unique,
        parse_constant=lambda _: (_ for _ in ()).throw(ValueError('finite source configuration required')))
    if type(value) is not dict or value.get('codec') != 'rkb.source-service.v1':
        raise ValueError('exact source service configuration required')
    return value


@dataclass(frozen=True)
class SuccessorPlan:
    run_id: str
    target: TargetLaunchSpec
    source_json: str
    parent_run_id: str
    storage_sha256: str

    def source_config(self, launch_config_sha256):
        if (type(launch_config_sha256) is not str or len(launch_config_sha256) != 64
                or set(launch_config_sha256) - set('0123456789abcdef')):
            raise ValueError('exact generated launch configuration hash required')
        value = json.loads(self.source_json)
        value['source_restore']['launch_config_sha256'] = launch_config_sha256
        if 'source_import' in value:
            value['source_import']['launch_config_sha256'] = launch_config_sha256
        return value


def prepare_successor(queue, run_id, target, source):
    """Reserve one plan before launch claim; a restart reuses identical bytes.

    Only an already retired, settled writer and a queue-owned fresh successor
    qualify. A still running/UNKNOWN predecessor remains a provider reservation.
    No remote read/write, writer retirement, provider push or new identity source.
    """
    if (type(target) is not TargetLaunchSpec or target.database.mode != 'VERIFIED_INITIALIZED'
            or target.recovery is None or target.recovery.drill_empty
            or type(source) is not dict or source.get('codec') != 'rkb.source-service.v1'
            or source.get('database') != target.database.wire()
            or type(source.get('stream')) is not str):
        raise ValueError('same verified restore target/source authority required')
    restore = SourceRestoreConfig.from_wire(source['source_restore'])
    if restore.dataset_checkpoint != target.dataset_checkpoint:
        raise ValueError('target and source restore checkpoint differ')
    anchor = hashlib.sha256(_encoded(dict(target=target.wire(), source=source)).encode()).hexdigest()
    with queue.connect() as db:
        db.execute('''CREATE TABLE IF NOT EXISTS rkb_cold_successor_plans(
            run_id TEXT PRIMARY KEY,anchor_sha256 TEXT NOT NULL,parent_run_id TEXT NOT NULL,
            storage_sha256 TEXT NOT NULL,target_json TEXT NOT NULL,source_json TEXT NOT NULL,
            launch_config_sha256 TEXT)''')
        row = db.execute('SELECT * FROM rkb_cold_successor_plans WHERE run_id=?', (run_id,)).fetchone()
        current = db.execute('SELECT current_run FROM control WHERE id=1').fetchone()
        run = db.execute('SELECT * FROM runs WHERE id=?', (run_id,)).fetchone()
        if current[0] != run_id or run is None:
            raise ValueError('current successor required')
        if row is not None:
            if row['anchor_sha256'] != anchor:
                raise ValueError('successor input configuration is immutable')
            return SuccessorPlan(run_id, target_launch_from_wire(json.loads(row['target_json'])),
                row['source_json'], row['parent_run_id'], row['storage_sha256'])
        if (run['status'] != 'starting' or run['launch_state'] != 'pending'
                or run['channel_active'] or run['runtime_incarnation']
                or queue._run_expired(run, queue.clock())):
            raise ValueError('fresh pending successor required')
        writer = db.execute('SELECT * FROM rkb_writer_control WHERE id=1').fetchone()
        if (writer is None or writer['active'] or writer['publication_json'] is not None
                or writer['run_id'] == run_id
                or writer['database_generation'] != target.database.database_generation
                or writer['stream'] != source['stream']):
            raise ValueError('previous writer must be retired and settled in same authority')
        parent = db.execute('SELECT launch_state FROM runs WHERE id=?', (writer['run_id'],)).fetchone()
        if parent is None or parent['launch_state'] != 'terminal':
            raise ValueError('previous provider execution is still unresolved')
        if (db.execute("SELECT 1 FROM sqlite_master WHERE name='bge_runtime_expectations' AND type='table'").fetchone()
                and db.execute('SELECT 1 FROM bge_runtime_expectations WHERE run_id=?', (run_id,)).fetchone()):
            raise ValueError('successor must precede runtime expectations')
        generation = str(uuid.uuid4())
        if db.execute('SELECT 1 FROM rkb_writer_generations WHERE writer_generation=?',
                (generation,)).fetchone():
            raise ValueError('fresh writer generation collision')
        value = json.loads(_encoded(source))
        database = replace(target.database, writer_generation=generation)
        options = dict(database=database, initialized_receipt=None)
        if target.dataset_checkpoint is not None:
            checkpoint = target.dataset_checkpoint
            store = db.execute('SELECT * FROM rkb_dataset_recovery_store WHERE storage=?',
                (writer['storage_sha256'],)).fetchone()
            if store is None:
                raise ValueError('same existing Dataset storage authority required')
            from .kaggle_dataset_base import DatasetCheckpoint
            latest = DatasetCheckpoint.from_wire(json.loads(store['latest_json'] or store['checkpoint_json']))
            if ((latest.owner_slug, latest.dataset_slug, latest.database_generation) !=
                    (checkpoint.owner_slug, checkpoint.dataset_slug, checkpoint.database_generation)
                    or latest.base_sequence < checkpoint.base_sequence
                    or latest.dataset_version < checkpoint.dataset_version):
                raise ValueError('promoted Dataset checkpoint cannot change authority or regress')
            options.update(dataset_checkpoint=latest, min_data_seq=latest.base_sequence)
            value['source_restore'].update(dataset_checkpoint=latest.wire(), manifest=dict(
                key=restore.manifest.key.rsplit('/', 1)[0] + '/' + latest.manifest.sha256,
                sha256=latest.manifest.sha256, length=latest.manifest.size,
                last_sequence=latest.base_sequence))
        prepared = replace(target, **options)
        value.update(run_id=run_id, database=database.wire())
        # Includes the complete run ID; no release-prefix or truncated collision.
        value['source_restore']['operation_key'] = 'rkb-cold-restore-' + run_id
        source_json = _encoded(value)
        db.execute('INSERT INTO rkb_cold_successor_plans VALUES(?,?,?,?,?,?,NULL)',
            (run_id, anchor, writer['run_id'], writer['storage_sha256'],
             _encoded(prepared.wire()), source_json))
        return SuccessorPlan(run_id, prepared, source_json, writer['run_id'], writer['storage_sha256'])


def finalize_successor(queue, plan):
    """Bind source recovery to the actual immutable generated expectation."""
    with queue.connect() as db:
        row = db.execute('SELECT * FROM rkb_cold_successor_plans WHERE run_id=?', (plan.run_id,)).fetchone()
        runtime = queue._runtime_row(db, plan.run_id)
        current = db.execute('SELECT current_run FROM control WHERE id=1').fetchone()[0]
        run = db.execute('SELECT launch_state FROM runs WHERE id=?', (plan.run_id,)).fetchone()
        if (row is None or runtime is None or row['target_json'] != _encoded(plan.target.wire())
                or row['source_json'] != plan.source_json or current != plan.run_id
                or run is None or run['launch_state'] not in ('dispatching', 'dispatched')):
            raise ValueError('persisted successor and generated runtime expectation required')
        expected = json.loads(runtime['expected_json'])
        digest = expected['hashes']['launch_config']
        source = plan.source_config(digest)
        if (expected['database_generation'] != plan.target.database.database_generation
                or any(expected[name] != getattr(plan.target.database, name)
                    for name in ('schema_revision', 'function_revision', 'executor_revision'))
                or row['launch_config_sha256'] not in (None, digest)):
            raise ValueError('immutable successor launch binding differs')
        db.execute('UPDATE rkb_cold_successor_plans SET launch_config_sha256=? WHERE run_id=?',
            (digest, plan.run_id))
        return source


def successor_source_config(queue, original):
    """Read finalized current plan; unchanged initial-run configuration is valid.

    This selector does not rebind an already running service; its maintenance
    owner must separately close old leases and adopt the exact returned config.
    """
    with queue.connect(write=False) as db:
        run_id = db.execute('SELECT current_run FROM control WHERE id=1').fetchone()[0]
        if run_id == original['run_id']:
            return original
        if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='rkb_cold_successor_plans'").fetchone():
            raise ValueError('current successor recovery configuration unavailable')
        row = db.execute('SELECT * FROM rkb_cold_successor_plans WHERE run_id=?', (run_id,)).fetchone()
        if row is None or row['launch_config_sha256'] is None:
            raise ValueError('current successor recovery configuration not finalized')
        plan = SuccessorPlan(run_id, target_launch_from_wire(json.loads(row['target_json'])),
            row['source_json'], row['parent_run_id'], row['storage_sha256'])
        value = plan.source_config(row['launch_config_sha256'])
        if (value['stream'] != original['stream'] or
                value['database']['database_generation'] != original['database']['database_generation']):
            raise ValueError('successor source authority differs')
        return value
