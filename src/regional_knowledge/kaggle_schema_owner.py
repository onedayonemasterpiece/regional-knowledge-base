"""One persisted intent/write barrier; applied versions live only in PG's ledger."""
from __future__ import annotations

import asyncio
import json
import logging

from .kaggle_pg_snapshot_native import SnapshotError, encode, sha
from .kaggle_warm_maintenance import WarmHandlerPatchApproval

LOG=logging.getLogger(__name__)


def initialize(queue):
    with queue.connect() as db:
        db.execute("""CREATE TABLE IF NOT EXISTS rkb_schema_barrier(
            operation_key TEXT PRIMARY KEY,plan TEXT NOT NULL,
            barrier INTEGER NOT NULL DEFAULT 0,done INTEGER NOT NULL DEFAULT 0)""")


def request_migration(queue,*,target_schema_version,bundle_sha256,database_generation,
                      writer_generation,run_id,artifact=None):
    if (type(target_schema_version) is not int or target_schema_version<1
            or type(bundle_sha256) is not str or len(bundle_sha256)!=64
            or set(bundle_sha256)-set('0123456789abcdef')):
        raise ValueError('exact target version and bundle hash required')
    if any(type(v) is not str or not 1<=len(v)<=256 for v in (database_generation,writer_generation,run_id)):
        raise ValueError('exact migration target identity required')
    if artifact is not None:artifact=WarmHandlerPatchApproval.from_wire(artifact).wire()
    plan=encode(dict(target_schema_version=target_schema_version,bundle_sha256=bundle_sha256,
        database_generation=database_generation,writer_generation=writer_generation,run_id=run_id,artifact=artifact)).decode()
    key='schema-migrate:'+sha(plan.encode())
    initialize(queue)
    with queue.connect() as db:
        if db.execute('SELECT 1 FROM rkb_schema_barrier WHERE operation_key=?',(key,)).fetchone():return key
        if db.execute('SELECT 1 FROM rkb_schema_barrier WHERE done=0').fetchone():
            raise SnapshotError('migration_existing_request_pending')
        db.execute('INSERT INTO rkb_schema_barrier(operation_key,plan) VALUES(?,?)',(key,plan))
    return key


class SourceSchemaMigrations:
    def __init__(self,startup):
        self.startup,self.queue=startup,startup.config.queue
        self.lock=asyncio.Lock();self.task=None
        initialize(self.queue)

    def pending(self):
        with self.queue.connect(write=False) as db:
            return db.execute('SELECT * FROM rkb_schema_barrier WHERE done=0 LIMIT 1').fetchone()

    def intake_allowed(self):return self.pending() is None

    def writes_allowed(self):
        row=self.pending()
        return row is None or not row['barrier']  # accepted work can still drain

    def resume_attachment(self):
        """Reconcile an issued migration before its new READY is accepted.

        Existing restore authority validates the current run/incarnation. This
        only rebinds retained jobs; it does not enqueue DDL or lift the barrier.
        """
        row=self.pending();restorer=self.startup.restorer
        if row is None or not row['barrier'] or restorer is None or not restorer.complete or restorer.dispatch is None:
            return False
        plan=json.loads(row['plan']);root=row['operation_key']+':apply'
        binding,nonce=restorer._observe()
        if (plan['run_id'],plan['database_generation'],plan['writer_generation'])!=(
                binding.run_id,binding.database_generation,binding.writer_generation):
            raise SnapshotError('migration_original_target_changed')
        dispatch=restorer.dispatch
        if (binding.run_id,nonce,binding.runtime_incarnation)!=dispatch.identity:
            raise SnapshotError('migration_original_runtime_changed')
        with self.queue.connect(write=False) as db:
            jobs=db.execute("SELECT identity FROM jobs WHERE run_id=? AND actor=? "
                "AND kind='recovery_phase' AND state<>'done'",
                (binding.run_id,'maintenance:'+binding.run_id)).fetchall()
        for job in jobs:
            identity=json.loads(job['identity']);operation=identity.get('operation_key','')
            if (identity.get('phase')=='schema_migrate'
                    and (operation==root or operation.startswith(root+':retry:'))
                    and identity.get('arguments',{}).get('target_schema_version')==plan['target_schema_version']
                    and tuple(identity.get(k) for k in ('run_id','launch_nonce','runtime_incarnation'))==dispatch.identity):
                dispatch.schema_context=self.startup.promote_schema_context
                dispatch.bind_attachment(binding)
                return True
        return False

    def kick(self,binding):
        if self.task is None or self.task.done():self.task=asyncio.create_task(self._run(binding))

    async def _run(self,binding):
        try:await self.advance(binding)
        except asyncio.CancelledError:raise
        except Exception as error:LOG.warning('schema_migrate pending error_type=%s',type(error).__name__)

    async def aclose(self):
        if self.task is not None and not self.task.done():
            self.task.cancel();await asyncio.gather(self.task,return_exceptions=True)

    async def advance(self,binding):
        row=self.pending()
        if row is None or self.lock.locked():return False
        async with self.lock:
            startup=self.startup;spool=startup.config.spool;plan=json.loads(row['plan']);key=row['operation_key']
            if (startup.config.writer_owner is None or not startup.binding_current(binding)
                    or startup.restorer is None or not startup.restorer.complete or startup.dataset_checkpoints is None):
                raise SnapshotError('migration_current_writer_and_checkpoint_required')
            if (plan['database_generation'],plan['writer_generation'],plan['run_id'])!=(
                    startup.config.history_adapter.database.database_generation,startup.config.writer_generation,binding.run_id):
                raise SnapshotError('migration_original_target_changed')
            async with spool._lock:
                proof=startup.history._coverage
                if (proof is None or not proof.current() or proof.accepted_sequence!=proof.applied_sequence
                        or proof.applied_sequence!=spool._source_covered_sequence or spool._writing):return False
                with self.queue.connect() as db:
                    if db.execute("SELECT 1 FROM rkb_control_operations WHERE seal_state<>'ACCEPTED' "
                        "OR apply_state NOT IN ('APPLIED','REJECTED_PRECONDITION') OR outcome_state='SENT_UNKNOWN' LIMIT 1").fetchone():return False
                    if db.execute("SELECT 1 FROM jobs WHERE kind='source_command' AND state='claimed' LIMIT 1").fetchone():return False
                    db.execute('UPDATE rkb_schema_barrier SET barrier=1 WHERE operation_key=?',(key,))
                sequence=proof.applied_sequence
            dispatch=startup.restorer.dispatch;bundle=plan['bundle_sha256']
            dispatch.schema_context=getattr(startup,'promote_schema_context',None)
            if plan['artifact'] is not None:
                result=await dispatch.request(binding,'schema_bundle_stage',dict(artifact=plan['artifact'],
                    expected_bundle_sha256=bundle),operation_key=key+':bundle')
                if result.get('status')=='rejected':raise SnapshotError(result['reason'])
                if result['latest_schema_version']!=plan['target_schema_version']:
                    raise SnapshotError('migration_target_bundle_changed')
                bundle=result['bundle_sha256']
            result=await dispatch.request(binding,'schema_migrate',dict(bundle_sha256=bundle,
                target_schema_version=plan['target_schema_version']),operation_key=key+':apply')
            if result.get('status')=='rejected':
                if result.get('retryable'):return False  # existing refresh backoff, existing job retains outcome
                raise SnapshotError(result['reason'])
            if result['schema_version']!=plan['target_schema_version']:
                raise SnapshotError('migration_target_version_changed')
            await startup.dataset_checkpoints.checkpoint_schema(binding,schema_version=result['schema_version'],
                operation_key=key+':checkpoint',expected_sequence=sequence)
            with self.queue.connect() as db:
                db.execute('UPDATE rkb_schema_barrier SET done=1,barrier=0 WHERE operation_key=?',(key,))
            LOG.info('schema_migrate checkpoint_verified schema_version=%s',result['schema_version'])
            return True
