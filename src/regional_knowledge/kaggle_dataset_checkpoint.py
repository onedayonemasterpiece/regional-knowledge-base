"""Work-cycle checkpoints through the existing native capture and Dataset owner."""
from __future__ import annotations

import asyncio
import json
import logging
import secrets
import time
from contextlib import asynccontextmanager

from .dataset_recovery_store import DatasetRecoveryStore
from .kaggle_dataset_base import DatasetCheckpoint, DatasetFile
from .kaggle_dataset_seed import SourceDatasetSeed, DatasetSeedConfig
from .kaggle_pg_snapshot_native import BlobRef, SnapshotError, encode, sha
from .recovery_journal import ObjectRef

_LOG=logging.getLogger(__name__)


class CheckpointPending(Exception):
    """Capture/verify/promotion stopped before the pointer moved.

    Nothing in the spool or the verified read prefix changed, so the current
    read coverage stays valid; the checkpoint retries on a later refresh.
    """


class _WorkCycleCapture(SourceDatasetSeed):
    async def _prepare_prefix(self,proof):
        if proof is None:return
        spool=self.startup.config.spool
        with self.queue.connect(write=False) as db:
            keys=[r[0] for r in db.execute("SELECT operation_key FROM rkb_control_operations WHERE sequence<=? "
                "AND seal_state='ACCEPTED' AND apply_state IN ('APPLIED','REJECTED_PRECONDITION') "
                "AND outcome_state='NOT_SENT' ORDER BY sequence LIMIT ?",
                (proof.applied_sequence,self.startup.restorer.physical.limits.max_receipts))]
        for key in keys:
            entry=await spool.get(key)
            if entry.accepted is None or entry.apply_receipt is None:
                raise SnapshotError('dataset_checkpoint_exact_native_outcome_required')
            from .recovery_journal import JournalUnknown
            try:
                await spool.journal.record_outcome(entry.accepted,command=entry.command,
                    database_generation=entry.database_generation,outcome=entry.apply_receipt.outcome,
                    writer_generation=self.startup.config.writer_generation)
            except JournalUnknown as exc:
                if exc.args!=('outcome_pending_dataset_checkpoint',):raise

    def _prefix_ready(self,proof):
        if (proof is None or not proof.current() or proof.accepted_sequence!=proof.applied_sequence
                or proof.accepted_sequence!=self.startup.config.spool._source_covered_sequence):return False
        with self.queue.connect(write=False) as db:
            # Every accepted operation must have its exact native receipt. A
            # locally committed outcome remains pending until publication.
            unknown=db.execute("SELECT 1 FROM rkb_control_operations WHERE sequence<=? AND "
                "(seal_state<>'ACCEPTED' OR apply_state NOT IN ('APPLIED','REJECTED_PRECONDITION') "
                "OR outcome_state='SENT_UNKNOWN') LIMIT 1",(proof.applied_sequence,)).fetchone()
            if unknown is not None:return False
            rows=db.execute("SELECT operation_key,sequence,outcome_sha256 FROM rkb_control_operations "
                "WHERE sequence<=? AND outcome_state='NOT_SENT'",(proof.applied_sequence,)).fetchall()
            store=self.startup.config.spool.journal.store
            for row in rows:
                key=self.startup.config.spool.journal.prefix+'/outcomes/'+sha(row['operation_key'].encode())
                obj=db.execute('SELECT data FROM rkb_dataset_recovery_objects WHERE storage=? AND key=?',
                    (store.storage_identity,key)).fetchone()
                if obj is None:return False
                value=json.loads(obj['data'])
                if (value.get('operation_key'),value.get('sequence'),value.get('outcome_sha256'))!=(
                        row['operation_key'],row['sequence'],row['outcome_sha256']):return False
        return True

    def _control(self,physical,proof):
        with self.queue.connect(write=False) as db:
            rows=db.execute('SELECT source_id,sequence,command_sha256,restriction,pending,unknown '
                'FROM rkb_control_source_heads ORDER BY source_id LIMIT ?',
                (physical.limits.max_receipts+1,)).fetchall()
        if len(rows)>physical.limits.max_receipts or any(row[5] for row in rows):
            raise SnapshotError('dataset_checkpoint_source_control_unsettled')
        history=self.startup.history
        # Only this immutable future checkpoint describes the captured prefix
        # as settled. Live control state is promoted after native readback.
        data=encode(dict(codec='rkb.source-base-control.v1',stream=physical.journal.stream,
            database_generation=proof.database_generation,sequence=proof.applied_sequence,
            journal_head=proof.head.reference.wire() if proof.head else None,
            catalog_sha256=history.catalog_hash,acl_sha256=history.acl_hash,
            registry_sha256=history.registry_hash,routes=sorted(history.routes),
            heads=[list(row[:4])+[0,0] for row in rows]))
        if len(data)>physical.limits.max_control_bytes:raise SnapshotError('dataset_checkpoint_control_budget')
        return data

    async def _capture(self,physical,bridge,binding,sequence):
        from .kaggle_recovery_dispatch import WssSnapshotPhases
        native=WssSnapshotPhases(bridge,operation_key=self.config.operation_key)
        return await native.capture(binding,versions=dict(self.config.versions),expected_sequence=sequence)

    async def _journal_control(self):
        return await asyncio.to_thread(self.startup.config.spool.journal.store.export_control)


class _SchemaCycleCapture(_WorkCycleCapture):
    @asynccontextmanager
    async def capture_guard(self,binding):
        # The durable schema barrier already excludes intake AND dispatch;
        # retain the small spool lock only to validate that exclusion. Holding
        # it during basebackup/provider upload would prevent history renewal
        # and expire read coverage while an otherwise healthy search serves.
        async with self.startup.config.spool._lock:
            self._current_barrier(binding)
        yield
        self._current_barrier(binding)

    def _current_barrier(self,binding):
        owner=self.startup.schema_migrations
        if (owner.pending() is None or owner.writes_allowed()
                or not self.startup.binding_current(binding)):
            raise SnapshotError('migration_checkpoint_write_barrier_changed')

    def _prefix_ready(self,proof):
        # Read maintenance can renew the capability for the SAME immutable
        # prefix during capture. Native capture still proves exact versions,
        # generation, cursor and writer before any publication is promoted.
        current=self.startup.history._coverage
        if (proof is None or current is None
                or (current.database_generation,current.applied_sequence,current.accepted_sequence)
                !=(proof.database_generation,proof.applied_sequence,proof.accepted_sequence)):
            return False
        return super()._prefix_ready(current)


class SourceDatasetCheckpoints:
    def __init__(self,startup,*,interval_seconds=900,clock=time.monotonic):
        self.startup=startup;self.store=startup.config.spool.journal.store
        if type(self.store) is not DatasetRecoveryStore or not 1<=interval_seconds<=3600:
            raise TypeError('existing Dataset journal and bounded checkpoint interval required')
        self.interval,self.clock=interval_seconds,clock
        self.dirty_since=None;self.last_sequence=None;self.last_change=self.clock()
        self.last_control=None;self._refresh_lock=asyncio.Lock()
        with startup.config.queue.connect() as db:
            db.execute('''CREATE TABLE IF NOT EXISTS rkb_dataset_seed(
                operation_key TEXT PRIMARY KEY,provider_ref TEXT NOT NULL,
                state TEXT NOT NULL,package_json TEXT,version INTEGER)''')

    def _versions(self):
        result=dict(self.startup.config.source_restore.versions)
        database=self.startup.config.history_adapter.database
        result.update(catalog_sha256=self.startup.history.catalog_hash,
            **{key:getattr(database,key) for key in ('schema_revision','function_revision','executor_revision')})
        return result

    async def _capture_verify(self,binding,operation,latest,*,schema=False):
        startup=self.startup;spool=startup.config.spool;queue=startup.config.queue
        capture=_SchemaCycleCapture if schema else _WorkCycleCapture
        batch=capture(startup,DatasetSeedConfig(self.store.publisher.ref,operation,self._versions()),
            self.store.publisher)
        version=await batch.capture_once(binding)
        with queue.connect(write=False) as db:
            row=db.execute('SELECT package_json FROM rkb_dataset_seed WHERE operation_key=?',(operation,)).fetchone()
        package=json.loads(row['package_json']);files={f['name']:DatasetFile(f['name'],f['size'],f['sha256'])
            for f in package['files']}
        archives=[f for f in files.values() if f.name.startswith('pg-') and f.name.endswith('.blob')]
        if len(archives)!=1:raise SnapshotError('dataset_checkpoint_archive_ambiguous')
        checkpoint=DatasetCheckpoint(latest.owner_slug,latest.dataset_slug,version,package['database_generation'],
            package['base_sequence'],files['base-manifest.json'],archives[0],files['source-control.json'],
            files['journal-control.json'])
        batch.tickets.download_checkpoint=checkpoint
        bridge=startup.restorer.dispatch;bridge.snapshot_tickets=batch.tickets
        archive=checkpoint.archive;operation_ref=archive.name.removeprefix('pg-').removesuffix('.blob')
        blob=BlobRef('dataset-blobs/'+operation_ref+'/'+archive.name,archive.size,archive.sha256,
            'kaggle-dataset:'+str(version))
        observed=await bridge.request(binding,'dataset_verify',dict(checkpoint=checkpoint.wire(),blob=blob.__dict__),
            operation_key=sha(encode([operation,'readback',secrets.token_hex(24)])))
        manifest_ref=ObjectRef(spool.journal.prefix+'/snapshots/manifests/'+checkpoint.manifest.sha256,
            checkpoint.manifest.sha256,checkpoint.manifest.size,checkpoint.base_sequence)
        return checkpoint,observed,manifest_ref,version

    async def refresh(self,binding,*,planned_stop=False):
        """Checkpoint pending work; True retains the did-publish contract."""
        # The existing refresh owner may be called by shutdown and maintenance
        # concurrently. A second caller never starts another capture.
        if self._refresh_lock.locked():return False
        async with self._refresh_lock:
            return await self._refresh(binding,planned_stop=planned_stop)

    async def checkpoint_schema(self,binding,*,schema_version,operation_key,expected_sequence):
        """Immediate forced checkpoint, including unchanged journal sequences.

        The schema owner keeps its durable write barrier across this entire
        call and only releases it after the existing native archive readback
        and Dataset pointer promotion. Ordinary timed checkpoints cannot
        interleave. The operation key survives lost publication responses.
        """
        async with self._refresh_lock:
            if self._versions().get('schema_version')!=schema_version:
                # Deliberately unavailable until PR-2's version-based native
                # capture is installed. Never label a legacy hash-pinned base
                # as a checkpoint for an unobserved integer schema version.
                raise SnapshotError('migration_checkpoint_schema_version_unavailable')
            proof=self.startup.history._coverage
            if (proof is None or not proof.current() or proof.applied_sequence!=expected_sequence
                    or proof.accepted_sequence!=expected_sequence
                    or not self.startup.binding_current(binding)):
                raise SnapshotError('migration_checkpoint_prefix_changed')
            checkpoint,observed,manifest_ref,version=await self._capture_verify(
                binding,operation_key,self.store.latest_checkpoint,schema=True)
            if checkpoint.base_sequence!=expected_sequence:
                raise SnapshotError('migration_checkpoint_sequence_changed')
            await asyncio.to_thread(self.store.promote,checkpoint,observed,manifest_ref=manifest_ref,
                current=lambda:self.startup.binding_current(binding))
            with self.startup.config.queue.connect() as db:
                db.execute("UPDATE rkb_dataset_seed SET state='verified' WHERE operation_key=? AND version=?",
                    (operation_key,version))
            _LOG.info('schema_checkpoint_verified schema_version=%s dataset_version=%s base_sequence=%s',
                schema_version,version,expected_sequence)
            return checkpoint

    async def _refresh(self,binding,*,planned_stop=False):
        startup=self.startup;spool=startup.config.spool;queue=startup.config.queue
        latest=self.store.latest_checkpoint;proof=startup.history._coverage
        with queue.connect(write=False) as db:
            # Resume publication/readback after an API restart without another
            # capture or provider create/version operation.
            retained=db.execute("SELECT operation_key FROM rkb_dataset_seed WHERE provider_ref=? "
                "AND operation_key LIKE 'dataset-checkpoint:%' AND package_json IS NOT NULL "
                "AND state IN ('publishing','published') ORDER BY rowid LIMIT 1",
                (self.store.publisher.ref,)).fetchone()
            meta=db.execute('SELECT sequence FROM rkb_control_spool_meta WHERE id=1').fetchone()
            from .bge_queue import MAX_LIFETIME
            run_id=getattr(binding,'run_id',None)
            if run_id is not None:
                run=db.execute('SELECT started FROM runs WHERE id=?',(run_id,)).fetchone()
                planned_stop=planned_stop or (run is not None and queue.clock()-run['started']>=MAX_LIFETIME-600)
        if retained is None:
            late_outcomes=self.store.pending_outcomes()
            try:control=self.store.pending_control()
            except Exception as exc:raise CheckpointPending(type(exc).__name__) from exc
            if (proof is None or proof.applied_sequence<latest.base_sequence
                    or proof.applied_sequence==latest.base_sequence and not (late_outcomes or control)):
                self.dirty_since=None;self.last_control=None;return False
            if self.dirty_since is None:
                self.dirty_since=self.clock();self.last_change=self.clock()
            if self.last_sequence!=proof.applied_sequence or self.last_control!=control:
                self.last_sequence=proof.applied_sequence;self.last_control=control;self.last_change=self.clock()
            batch_complete=(meta is not None and meta['sequence']==proof.applied_sequence
                and proof.accepted_sequence==proof.applied_sequence)
            # Save at a drained work-cycle boundary, periodically, or before a
            # planned stop. Individual SQL/page operations do not request Saves.
            if not (batch_complete and self.clock()-self.last_change>=30
                    or planned_stop or self.clock()-self.dirty_since>=self.interval):return False
            operation='dataset-checkpoint:'+sha(encode([self.store.publisher.ref,latest.dataset_version,
                latest.database_generation,proof.applied_sequence]))
        else:operation=retained['operation_key']
        try:
            if not startup.binding_current(binding):raise SnapshotError('dataset_checkpoint_writer_changed')
            checkpoint,observed,manifest_ref,version=await self._capture_verify(binding,operation,latest)
            await asyncio.to_thread(self.store.promote,checkpoint,observed,manifest_ref=manifest_ref,
                current=lambda:startup.binding_current(binding))
        except asyncio.CancelledError:raise
        except Exception as exc:
            raise CheckpointPending(type(exc).__name__,
                exc.args[0] if len(exc.args)==1 and type(exc.args[0]) is str else '') from exc
        with queue.connect(write=False) as db:
            keys=[r[0] for r in db.execute("SELECT operation_key FROM rkb_control_operations WHERE sequence<=? "
                "AND seal_state='ACCEPTED' AND apply_state IN ('APPLIED','REJECTED_PRECONDITION') "
                "AND outcome_state<>'DURABLE' ORDER BY sequence",(checkpoint.base_sequence,))]
        unsettled=0
        for key in keys:
            from .recovery_journal import JournalUnknown
            try:
                await spool.publish_outcome(key,writer_generation=startup.config.writer_generation)
            except JournalUnknown as exc:
                if exc.args!=('outcome_pending_dataset_checkpoint',):raise
                # A legacy checkpoint may predate outcome materialization. Its
                # publication is real, but these bytes need the NEXT version.
                unsettled+=1;continue
            jobs=await spool._write(spool._settle_recovered_jobs,key)
            for job in jobs:queue._notify_result(job)
        with queue.connect() as db:
            db.execute("UPDATE rkb_dataset_seed SET state='verified' WHERE operation_key=? AND version=?",
                (operation,version))
        self.dirty_since=None
        _LOG.info('dataset_checkpoint_verified provider_ref=%s version=%s base_sequence=%s settled=%s pending_next_checkpoint=%s',
            self.store.publisher.ref,version,checkpoint.base_sequence,len(keys)-unsettled,unsettled)
        return True
