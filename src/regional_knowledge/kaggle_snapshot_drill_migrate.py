"""One owned Dataset checkpoint -> disposable native PG11->14 catalog/ACL drill.

A drill is a schema-observation measurement, NOT an accepted migration,
Dataset publication, schema change on the serving PG, or restore adoption.
The existing signed transfer, native backup verifier, PG system identity,
applied-prefix proof and current worker lease remain mandatory.
"""
from __future__ import annotations

import base64
import json
from pathlib import Path
import secrets

from .kaggle_pg_snapshot_native import SnapshotError,BlobRef,applied_prefix,encode,sha
from .kaggle_dataset_base import DatasetCheckpoint
from .kaggle_native_source_upgrade import (
    NativeSourceUpgradePins,native_source_upgrade_handler,transition_sql,
    transition_definitions)
from .geo_manifest_commands import GEO_REVISIONS,GEO_V2_REVISIONS,GEO_DEPENDENCY_MIGRATION
from .source_activation import ACTIVATION_REVISIONS
from .kaggle_database_probe import native_probe_for_schema
from .kaggle_source_acl import inspect_acl
from .kaggle_apply import HandlerRegistry


class NativeDrillMigrate:
    """Only a preconfigured native owner can construct a migration drill."""

    def __init__(self, owner, *, sql_root, native_pins):
        if (type(native_pins) is not NativeSourceUpgradePins
                or not isinstance(sql_root,(str,Path))):
            raise TypeError('exact pinned native drill migration owner required')
        self.owner=owner
        self.root=Path(sql_root)
        self.pins=native_pins
        self.intents={}
        self.proofs={}
        self.candidates=set()

    def can_reconcile(self,args):
        return type(args) is dict and args.get('restore_operation') in self.intents

    def _artifacts(self):
        """Resolve only deployment-pinned, already verified source SQL files."""
        registry=self.owner.serving.registry
        if type(registry) is not HandlerRegistry:
            raise SnapshotError('drill_migrate_registry_unavailable')
        geo=[h for h in registry.handlers.values()
             if h.kind=='schema_transition' and h.route=='geo.dependencies.upgrade.v2']
        native=[h for h in registry.handlers.values()
                if h.kind=='schema_transition' and h.route=='source.native.upgrade.v1']
        if (len(geo)!=1 or len(native)!=1
                or geo[0].revisions!=GEO_REVISIONS
                or geo[0].transition is None
                or geo[0].transition.predecessor is not None
                or geo[0].transition.target!=GEO_V2_REVISIONS
                or geo[0].transition.migration_id!=GEO_DEPENDENCY_MIGRATION
                or native[0]!=native_source_upgrade_handler(self.pins)
                or native[0].transition.predecessor!=GEO_DEPENDENCY_MIGRATION
                or native[0].transition.target!=ACTIVATION_REVISIONS):
            raise SnapshotError('drill_migrate_exact_native_transition_missing')
        blobs=[]
        for filename,digest,maximum in (
            ('012_geo_dependency_bound.sql',geo[0].transition.artifact_sha256,64*1024),
            ('013_source_stage.sql',self.pins.sql013_sha256,256*1024),
            ('014_source_stage_activation.sql',self.pins.sql014_sha256,256*1024),
        ):
            path=self.root/filename
            if path.is_symlink() or not path.is_file() or not 1<=path.stat().st_size<=maximum:
                raise SnapshotError('drill_migrate_pinned_artifact_missing')
            value=path.read_bytes()
            if sha(value)!=digest:raise SnapshotError('drill_migrate_pinned_artifact_changed')
            blobs.append(value)
        return tuple(blobs),geo[0],native[0]

    async def measure_candidate(self,candidate,binding,*,base_sequence,base_writer):
        """Commit shape-only DDL to temporary PG; run real IDLE probe AFTER.

        No accepted command is invented and no applied_ops cursor increments.
        This deliberately measures the *final schema shape* in the isolated
        candidate. GH PG17 compares this digest to separately real-applied
        GEO/native migration transitions before this may be treated as a pin.
        """
        serving=self.owner.serving
        factory=self.owner.factory
        serving._current(binding)
        blobs,geo,native=self._artifacts()
        if (candidate.database!=factory.database
                or candidate.database.socket==serving.database.socket
                or candidate not in factory._owned_candidates
                or candidate.process.returncode is not None
                or type(base_sequence) is not int or base_sequence<0
                or not isinstance(base_writer,str) or not base_writer):
            raise SnapshotError('drill_migrate_isolated_candidate_required')
        original_prefix=None
        original_sources=None
        async with candidate.acquire() as connection:
            async with connection.transaction():
                await connection.execute("SET LOCAL statement_timeout='8000ms'")
                state=await(await connection.execute('''SELECT database_generation,writer_generation,
                    applied_seq,schema_revision,function_revision,executor_revision,
                    last_migration_id,last_migration_hash FROM rkb_v4.recovery_state
                    WHERE singleton FOR UPDATE''')).fetchone()
                if (state is None or state[:3]!=(
                        serving.database.database_generation,base_writer,base_sequence)
                        or tuple(state[3:6])!=GEO_REVISIONS.wire()
                        or state[6] is not None or state[7] is not None):
                    raise SnapshotError('drill_migrate_exact_base_changed')
                original_prefix=await applied_prefix(connection,base_sequence,factory.limits)
                original_sources=await(await connection.execute(
                    'SELECT count(*) FROM rkb_v4.sources')).fetchone()
                for data in blobs:
                    await connection.execute(data.decode('utf-8'),prepare=False)
                await connection.execute(transition_sql(self.pins),prepare=False)
                # Mirror only the accepted transitions' final *schema marker*
                # in this owned temporary cluster, never a command/history.
                # Database catlog hashes include migration markers; they do
                # NOT include the applied cursor. The running writer and
                # accepted ledger stay completely untouched.
                update=await connection.execute('''UPDATE rkb_v4.recovery_state SET
                    schema_revision=%s,function_revision=%s,executor_revision=%s,
                    last_migration_id=%s,last_migration_hash=%s
                    WHERE singleton AND database_generation=%s AND writer_generation=%s
                      AND applied_seq=%s AND schema_revision=%s
                      AND function_revision=%s AND executor_revision=%s
                      AND last_migration_id IS NULL AND last_migration_hash IS NULL''',
                    (*ACTIVATION_REVISIONS.wire(),
                     native.transition.migration_id,native.transition.artifact_sha256,
                     serving.database.database_generation,base_writer,base_sequence,
                     *GEO_REVISIONS.wire()))
                if update.rowcount!=1:
                    raise SnapshotError('drill_migrate_original_cursor_changed')
                for pinned in (geo.check,geo.apply,native.check,native.apply):
                    row=await(await connection.execute(
                        'SELECT pg_get_functiondef(to_regprocedure(%s))',
                        (pinned.signature,))).fetchone()
                    if row is None or row[0] is None or sha(row[0].encode())!=pinned.definition_sha256:
                        raise SnapshotError('drill_migrate_transition_function_changed')
                if await transition_definitions(connection)!=(
                        self.pins.transition_check_sha256,self.pins.transition_apply_sha256):
                    raise SnapshotError('drill_migrate_native_completion_changed')
                if (await applied_prefix(connection,base_sequence,factory.limits)!=original_prefix
                        or await(await connection.execute(
                            'SELECT count(*) FROM rkb_v4.sources')).fetchone()!=original_sources):
                    raise SnapshotError('drill_migrate_historical_source_changed')
        # An IDLE connection is a hard requirement of the real read-only native
        # probe. A transaction-scoped shortcut would not measure actual catalog.
        serving._current(binding)
        probe=native_probe_for_schema(ACTIVATION_REVISIONS.schema,timeout_seconds=5.0)
        catalog,fields=await probe(acquire=candidate.acquire)
        if (fields['database_generation']!=serving.database.database_generation
                or tuple(fields[k] for k in ('schema_revision','function_revision','executor_revision'))
                    !=ACTIVATION_REVISIONS.wire()
                or fields['applied_seq']!=base_sequence
                or fields['projection_seq']!=base_sequence
                or not fields['text_search_ready'] or not fields['dense_search_ready']):
            raise SnapshotError('drill_migrate_final_native_probe_changed')
        async with candidate.acquire() as connection:
            async with connection.transaction():
                await connection.execute("SET LOCAL statement_timeout='5000ms'")
                acl=await inspect_acl(connection)
                if await applied_prefix(connection,base_sequence,factory.limits)!=original_prefix:
                    raise SnapshotError('drill_migrate_history_after_probe_changed')
        serving._current(binding)
        return dict(catalog_sha256=sha(catalog),acl_sha256=sha(acl),
            original_applied_prefix_sha256=original_prefix,
            original_source_count=original_sources[0],schema_revision=ACTIVATION_REVISIONS.schema,
            base_sequence=base_sequence,migration_id=native.transition.migration_id,
            migration_sha256=native.transition.artifact_sha256)

    async def __call__(self,args,key,binding,frame):
        """Strict fixed Dataset version + existing signed transfer; close always."""
        self.owner.serving._current(binding)
        checkpoint=self.owner.factory.dataset_checkpoint
        expected={'manifest_b64','manifest_ref','blob','control_b64','expected_versions',
            'database','identity','restore_operation'}
        if (type(args) is not dict or set(args)!=expected
                or type(checkpoint) is not DatasetCheckpoint
                or args['database']!=self.owner.factory.database.wire()
                or tuple(args['identity'])!=self.owner.factory.identity
                or 'download_ticket' not in frame):
            raise SnapshotError('drill_migrate_exact_checkpoint_required')
        manifest_b64,control_b64=args['manifest_b64'],args['control_b64']
        if (type(manifest_b64) is not str or type(control_b64) is not str
                or len(manifest_b64)>4*((self.owner.factory.limits.max_control_bytes+2)//3)
                or len(control_b64)>4*((self.owner.factory.limits.max_control_bytes+2)//3)):
            raise SnapshotError('drill_migrate_metadata_bound')
        try:
            raw=base64.b64decode(manifest_b64,validate=True)
            control=base64.b64decode(control_b64,validate=True)
            manifest=json.loads(raw);current=json.loads(control)
            valid=(encode(manifest)==raw and encode(current)==control
                and len(raw)==checkpoint.manifest.size and sha(raw)==checkpoint.manifest.sha256
                and len(control)==checkpoint.control.size and sha(control)==checkpoint.control.sha256
                and args['manifest_ref']=={'sha256':sha(raw)}
                and manifest['versions']==args['expected_versions']
                and manifest['blob']==args['blob']
                and manifest['database_generation']==checkpoint.database_generation
                and manifest['applied_sequence']==checkpoint.base_sequence
                and current['sequence']==checkpoint.base_sequence
                and current['journal_head']==manifest['journal_head']
                and manifest['source_control']['sha256']==sha(control)
                and manifest['writer_generation']!=self.owner.factory.database.writer_generation
                and (manifest['blob']['size'],manifest['blob']['sha256'])
                    ==(checkpoint.archive.size,checkpoint.archive.sha256)
                and manifest['blob']['key'].rsplit('/',1)[-1]==checkpoint.archive.name)
        except (ValueError,TypeError,KeyError,UnicodeError):
            valid=False
        if not valid:raise SnapshotError('drill_migrate_checkpoint_pin_changed')
        operation=args['restore_operation']
        if type(operation) is not str or not 1<=len(operation.encode())<=256:
            raise SnapshotError('drill_migrate_retained_operation_required')
        request=sha(encode(args))
        existing=self.intents.get(operation)
        if existing is not None:
            if existing!=(key,request):
                raise SnapshotError('drill_migrate_operation_changed')
            for reference,proof in self.proofs.items():
                if proof['operation']==operation:
                    return dict(proof['wire'],reference=reference)
            raise SnapshotError('drill_migrate_unknown_requires_owner_cleanup')
        if self.intents or self.owner.prepared or self.owner.sessions or self.owner.factory._owned_candidates:
            raise SnapshotError('drill_migrate_competing_native_candidate')
        # Reject a missing/stale artifact BEFORE allocating a restore process.
        self._artifacts()
        self.intents[operation]=(key,request)
        candidate=None
        try:
            archive=await self.owner.download(BlobRef(**args['blob']),operation,
                ticket=frame['download_ticket'],transfer_operation=key)
            session=await self.owner.factory.prepare(binding,operation_key=operation,
                manifest_bytes=raw,expected_manifest_sha256=sha(raw),archive_path=archive,
                control_bytes=control,expected_versions=args['expected_versions'],
                workspace=self.owner.workspace(operation,'drill_restore'))
            candidate=session.candidate
            self.candidates.add(candidate)
            receipt=await self.measure_candidate(candidate,binding,
                base_sequence=checkpoint.base_sequence,base_writer=manifest['writer_generation'])
            await candidate.aclose()
            if candidate.process.returncode is None:
                raise SnapshotError('drill_migrate_owned_shutdown_unknown')
            socket=Path(self.owner.factory.database.socket)/(
                '.s.PGSQL.'+str(self.owner.factory.database.port))
            if socket.exists():raise SnapshotError('drill_migrate_socket_remains')
            self.owner.factory._owned_candidates.discard(candidate)
            self.candidates.discard(candidate)
            self.owner.serving._current(binding)
            wire=dict(codec='rkb.native-drill-migrate.v1',closed=True,
                observation_only=True,checkpoint_version=checkpoint.dataset_version,
                checkpoint_manifest_sha256=checkpoint.manifest.sha256,
                database_generation=checkpoint.database_generation,**receipt)
            reference=secrets.token_urlsafe(24)
            self.proofs[reference]=dict(owner=self,operation=operation,candidate=candidate,
                wire=wire)
            return dict(wire,reference=reference)
        except BaseException:
            if candidate is not None:
                try:await candidate.aclose()
                except BaseException:pass
                if candidate.process.returncode is not None:
                    self.owner.factory._owned_candidates.discard(candidate)
                    self.candidates.discard(candidate)
            # Never retry an UNKNOWN migration with a new candidate/operation.
            raise

    async def verify_closed(self,reference,binding):
        self.owner.serving._current(binding)
        proof=self.proofs.get(reference)
        if (proof is None or proof['owner'] is not self
                or proof['candidate'].process.returncode is None
                or proof['candidate'] in self.candidates):
            raise SnapshotError('drill_migrate_shutdown_proof_unavailable')
        return dict(proof['wire'],reference=reference)

    async def aclose(self):
        for candidate in tuple(self.candidates):
            await candidate.aclose()
            if candidate.process.returncode is not None:
                self.owner.factory._owned_candidates.discard(candidate)
                self.candidates.discard(candidate)
