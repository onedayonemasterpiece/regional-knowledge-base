"""One durable schema/checkpoint barrier on the existing source controller.

Requests are trusted operator metadata, not domain journal commands. The same
queue retains their phase across API restart; effects use existing durable WSS
jobs and the existing Dataset checkpoint publisher. New intake pauses first,
accepted commands drain, then writes stay fenced until each DDL checkpoint has
an independently verified provider receipt.
"""
from __future__ import annotations

import asyncio
import json
import logging

from .kaggle_pg_snapshot_native import SnapshotError, encode, sha
from .kaggle_warm_maintenance import WarmHandlerPatchApproval

LOG = logging.getLogger(__name__)


def initialize(queue):
    with queue.connect() as db:
        db.execute("""CREATE TABLE IF NOT EXISTS rkb_schema_maintenance(
            operation_key TEXT PRIMARY KEY, plan TEXT NOT NULL,
            state TEXT NOT NULL, version INTEGER NOT NULL, latest INTEGER,
            bundle_sha256 TEXT NOT NULL, base_sequence INTEGER, attempt INTEGER NOT NULL DEFAULT 0)""")


def request_migration(queue, *, expected_schema_version, bundle_sha256,
                      database_generation, writer_generation, run_id, artifact=None):
    """Persist one reviewable request; no provider/SQL effect or new controller.

    artifact is optional for applying SQL already shipped with this worker. A
    new SQL prefix uses the existing reviewed-commit/immutable-blob approval.
    """
    if (type(expected_schema_version) is not int or expected_schema_version < 0
            or type(bundle_sha256) is not str or len(bundle_sha256) != 64
            or any(c not in '0123456789abcdef' for c in bundle_sha256)):
        raise ValueError('exact schema version and bundle hash required')
    if artifact is not None:
        artifact = WarmHandlerPatchApproval.from_wire(artifact).wire()
    if any(type(v) is not str or not 1 <= len(v) <= 256
           for v in (database_generation, writer_generation, run_id)):
        raise ValueError('exact migration target identity required')
    plan = encode(dict(expected_schema_version=expected_schema_version,
                       bundle_sha256=bundle_sha256, artifact=artifact,
                       database_generation=database_generation,
                       writer_generation=writer_generation, run_id=run_id)).decode()
    key = 'schema-migrate:' + sha(plan.encode())
    initialize(queue)
    with queue.connect() as db:
        prior = db.execute('SELECT plan FROM rkb_schema_maintenance WHERE operation_key=?', (key,)).fetchone()
        if prior is not None:
            if prior['plan'] != plan:
                raise SnapshotError('migration_operation_changed')
            return key
        if db.execute("SELECT 1 FROM rkb_schema_maintenance WHERE state<>'done'").fetchone():
            raise SnapshotError('migration_existing_request_pending')
        db.execute("INSERT INTO rkb_schema_maintenance(operation_key,plan,state,version,bundle_sha256) "
                   "VALUES(?,?,'requested',?,?)", (key, plan, expected_schema_version, bundle_sha256))
    return key


class SourceSchemaMigrations:
    def __init__(self, startup):
        self.startup, self.queue = startup, startup.config.queue
        self.lock = asyncio.Lock()
        self.task = None
        initialize(self.queue)

    def kick(self,binding):
        if self.task is None or self.task.done():
            self.task=asyncio.create_task(self._run(binding))

    async def _run(self,binding):
        try:
            await self.advance(binding)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            # Durable phase/operation identity remains unchanged for the next
            # maintenance tick. Never log provider errors or artifact tickets.
            LOG.warning('schema_migrate pending error_type=%s',type(error).__name__)

    async def aclose(self):
        if self.task is not None and not self.task.done():
            self.task.cancel()
            await asyncio.gather(self.task,return_exceptions=True)

    def pending(self):
        with self.queue.connect(write=False) as db:
            return db.execute("SELECT * FROM rkb_schema_maintenance WHERE state<>'done' LIMIT 1").fetchone()

    def intake_allowed(self):
        return self.pending() is None

    def writes_allowed(self):
        row = self.pending()
        return row is None or row['state'] == 'requested'

    def _save(self, key, *, state, version, latest, bundle_hash, sequence, attempt=0):
        with self.queue.connect() as db:
            db.execute("UPDATE rkb_schema_maintenance SET state=?,version=?,latest=?,bundle_sha256=?,"
                       "base_sequence=?,attempt=? WHERE operation_key=?",
                       (state, version, latest, bundle_hash, sequence, attempt, key))

    async def advance(self, binding):
        row = self.pending()
        if row is None or self.lock.locked():
            return False
        async with self.lock:
            return await self._advance(binding, dict(row))

    async def _advance(self, binding, row):
        startup = self.startup
        if (startup.config.writer_owner is None or not startup.binding_current(binding)
                or startup.restorer is None or not startup.restorer.complete
                or startup.dataset_checkpoints is None):
            raise SnapshotError('migration_current_writer_and_checkpoint_required')
        spool = startup.config.spool
        key, state = row['operation_key'], row['state']
        plan = json.loads(row['plan'])
        if (plan['database_generation'], plan['writer_generation'], plan['run_id']) != (
                startup.config.history_adapter.database.database_generation,
                startup.config.writer_generation, binding.run_id):
            raise SnapshotError('migration_original_target_changed')
        version, latest, bundle_hash = row['version'], row['latest'], row['bundle_sha256']
        sequence = row['base_sequence']
        if state == 'requested':
            async with spool._lock:
                proof = startup.history._coverage
                if (proof is None or not proof.current() or proof.accepted_sequence != proof.applied_sequence
                        or proof.applied_sequence != spool._source_covered_sequence or spool._writing):
                    return False
                with self.queue.connect(write=False) as db:
                    unsettled = db.execute("SELECT 1 FROM rkb_control_operations WHERE "
                        "seal_state<>'ACCEPTED' OR apply_state NOT IN ('APPLIED','REJECTED_PRECONDITION') "
                        "OR outcome_state='SENT_UNKNOWN' LIMIT 1").fetchone()
                    claimed = db.execute("SELECT 1 FROM jobs WHERE kind='source_command' AND state='claimed' LIMIT 1").fetchone()
                if unsettled or claimed:
                    return False
                sequence = proof.applied_sequence
                state = 'stage' if plan['artifact'] is not None else 'migrate'
                self._save(key, state=state, version=version, latest=latest,
                           bundle_hash=bundle_hash, sequence=sequence)
        dispatch = startup.restorer.dispatch
        if state == 'stage':
            result = await dispatch.request(binding, 'schema_bundle_stage',
                dict(artifact=plan['artifact'], expected_bundle_sha256=plan['bundle_sha256']),
                operation_key=key + ':bundle')
            if result.get('status') == 'rejected':
                raise SnapshotError(result['reason'])
            bundle_hash, latest = result['bundle_sha256'], result['latest_schema_version']
            state = 'migrate'
            self._save(key, state=state, version=version, latest=latest,
                       bundle_hash=bundle_hash, sequence=sequence)
        if state == 'migrate':
            result = await dispatch.request(binding, 'schema_migrate',
                dict(expected_schema_version=version, bundle_sha256=bundle_hash),
                operation_key=key + ':' + str(version) + ':' + str(row['attempt']))
            if result.get('status') == 'rejected':
                if result.get('retryable'):
                    self._save(key, state=state, version=version, latest=latest,
                        bundle_hash=bundle_hash, sequence=sequence, attempt=row['attempt'] + 1)
                    # Existing periodic refresh supplies bounded backoff;
                    # only the known rolled-back refusal gets a new job key.
                    return False
                raise SnapshotError(result['reason'])
            latest = result['latest_schema_version']
            new_version = result['schema_version']
            if new_version not in (version, version + 1):
                raise SnapshotError('migration_unexpected_version')
            if not result['checkpoint_required']:
                if new_version != latest:
                    raise SnapshotError('migration_incomplete_prefix')
                state = 'done'
            else:
                version, state = new_version, 'checkpoint'
            self._save(key, state=state, version=version, latest=latest,
                       bundle_hash=bundle_hash, sequence=sequence)
        if state == 'checkpoint':
            # Refresh readiness for this committed version without admitting
            # writes. PR-2 supplies version-based history/physical capture.
            await startup.dataset_checkpoints.checkpoint_schema(binding,
                schema_version=version, operation_key=key + ':checkpoint:' + str(version),
                expected_sequence=sequence)
            state = 'done' if version == latest else 'migrate'
            self._save(key, state=state, version=version, latest=latest,
                       bundle_hash=bundle_hash, sequence=sequence)
            LOG.info('schema_migrate checkpoint_verified schema_version=%s state=%s', version, state)
        return state == 'done'
