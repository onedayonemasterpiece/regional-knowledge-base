"""Native-only fixed recovery phases on the existing document compute permit.

The configured owner retains native sessions and capabilities. JSON references
only select those retained objects; neither paths nor SQL are accepted on wire.
"""
from __future__ import annotations

import asyncio
import base64
import inspect
import secrets

from .kaggle_pg_snapshot_native import SnapshotError, encode, sha

MAX_PHASE_FRAME_BYTES = 4 * 1024 * 1024
MAX_PHASE_RESULT_BYTES = 120 * 1024  # Reserve result envelope/claim identity bytes.
from .original_intake import PHASES as ORIGINAL_PHASES
from .kaggle_snapshot_drill import DRILL_PHASES
PHASES = frozenset({'capture','prepare','apply','finish','verify','source_history',
    'dataset_verify','read_handler_status','read_handler_stage','read_handler_activate','read_handler_rollback'})|ORIGINAL_PHASES|DRILL_PHASES


def _download_ticket_matches(blob,ticket,dataset_checkpoint=None):
    """Exact bytes always; If-Match identity only within one storage domain.

    A recovery-store ticket reuses the upload's native version as If-Match.
    A Kaggle Dataset version GET carries its own ETag, which differs from the
    upload-time GCS etag of the same archive; for the launch-pinned checkpoint
    archive the exact name/size/SHA-256 pin replaces that comparison, and the
    transfer still enforces the ticket's If-Match and streamed SHA-256.
    """
    headers={k.lower():v for k,v in ticket.headers}
    if (ticket.key,ticket.size,ticket.sha256)!=(blob.key,blob.size,blob.sha256) or not headers.get('if-match'):
        return False
    if dataset_checkpoint is not None:
        from .kaggle_dataset_base import DatasetCheckpoint
        archive=DatasetCheckpoint.from_wire(dataset_checkpoint).archive
        if (blob.key.rsplit('/',1)[-1],blob.size,blob.sha256)==(archive.name,archive.size,archive.sha256):
            return True
    return headers['if-match']==blob.native_version


async def build_recovery_worker(*,native_bundle,database,identity,tools,native_uid,native_gid,
                                current,workspace_private_current,workspace,upload=None,download=None,
                                ticket_channel=None,transfer=None,acquire=None,limits=None,model_pins=None,sql_root=None,dataset_checkpoint=None,source_materializer=None):
    """Construct the real native owner from the retained Stage0 verification.

    tools declares each binary's bundle-relative path and exact --version output.
    Hash/size/executable pins come from the verified native manifest. Transfer,
    attachment and output-exclusion policies are retained deployment callbacks.
    """
    from pathlib import Path
    from .kaggle_native_bundle import VerifiedNativeBundle
    from .kaggle_owned_pg import grant_native_code_access
    from .kaggle_pg_snapshot_native import NativeSnapshotFactory,NativeBinary,PhysicalPgTools,SnapshotLimits
    from .kaggle_source_commands import fixed_registry,WorkerSourceExecutor
    if ticket_channel is not None or transfer is not None:
        from .kaggle_snapshot_channel import SnapshotTicketChannel
        from .native_snapshot_transfer import NativeSnapshotTransfer,SnapshotTicket
        from .kaggle_pg_snapshot_native import BlobRef
        if (type(ticket_channel) is not SnapshotTicketChannel or type(transfer) is not NativeSnapshotTransfer
                or upload is not None or download is not None):
            raise SnapshotError('recovery_retained_transfer_owner_required')
        async def upload(path,*,size,sha256):
            ticket=SnapshotTicket.from_wire(await ticket_channel.upload_ticket(size=size,sha256=sha256))
            key=ticket_channel._assignment['operation_key']
            if ticket.size!=size or ticket.sha256!=sha256:
                raise SnapshotError('recovery_upload_ticket_bytes_changed')
            etag=await transfer.upload(ticket,path,operation_key=key)
            return BlobRef(ticket.key,size,sha256,etag)
        async def download(blob,restore_operation,*,ticket,transfer_operation):
            ticket=SnapshotTicket.from_wire(ticket)
            if not _download_ticket_matches(blob,ticket,dataset_checkpoint):
                raise SnapshotError('recovery_download_ticket_blob_changed')
            return await transfer.download(ticket,workspace(restore_operation,'download'),operation_key=transfer_operation)
    if (type(native_bundle) is not VerifiedNativeBundle
            or not await asyncio.to_thread(native_bundle.current)
            or not isinstance(tools,dict) or set(tools)!={'binaries','command_handlers'}
            or not isinstance(tools['binaries'],dict)
            or set(tools['binaries'])!={'pg_basebackup','pg_verifybackup','postgres'}
            or any(not callable(c) for c in (current,workspace_private_current,upload,download,workspace))):
        raise SnapshotError('recovery_stage0_owner_required')
    pins={absolute:(size,digest,executable) for absolute,size,digest,executable in native_bundle.pins}
    binaries={}
    for name,value in tools['binaries'].items():
        if not isinstance(value,dict) or set(value)!={'path','version_output'}:
            raise SnapshotError('recovery_stage0_binary_declaration')
        path=Path(value['path'])
        if path.is_absolute() or '..' in path.parts or not path.parts:
            raise SnapshotError('recovery_stage0_binary_path')
        absolute=str(native_bundle.prefix/path)
        pin=pins.get(absolute)
        if pin is None or pin[2] is not True:
            raise SnapshotError('recovery_stage0_binary_unverified')
        binaries[name]=NativeBinary(absolute,pin[0],pin[1],value['version_output'])
    await asyncio.to_thread(grant_native_code_access,native_bundle.root,native_bundle.pins,
        uid=native_uid,gid=native_gid)
    registry=fixed_registry(database,tools['command_handlers'])
    prepare_transition=None
    if any(h.transition is not None for h in registry.handlers.values()):
        if sql_root is None:raise SnapshotError('recovery_migration_bundle_required')
        from .kaggle_native_source_upgrade import ordered_migration_prepare
        prepare_transition=ordered_migration_prepare(sql_root=sql_root,registry=registry,
            database_generation=database.database_generation,entries=tools['command_handlers'])
    factory=NativeSnapshotFactory(tools=PhysicalPgTools(**binaries),database=database,identity=identity,
        registry=registry,executor_factory=(WorkerSourceExecutor
            if source_materializer is None else __import__('functools').partial(WorkerSourceExecutor,materializer=source_materializer)),
        prepare_transition=prepare_transition,
        current=current,workspace_private_current=workspace_private_current,native_uid=native_uid,
        native_gid=native_gid,acquire=acquire,upload_ticket=upload,limits=limits or SnapshotLimits())
    if dataset_checkpoint is not None:
        from .kaggle_dataset_base import DatasetCheckpoint
        factory.dataset_checkpoint=DatasetCheckpoint.from_wire(dataset_checkpoint)
        if factory.dataset_checkpoint.database_generation!=database.database_generation:
            raise SnapshotError('dataset_restore_generation_changed')
    factory.dataset_transfer=transfer
    # Verify exact installed ELF revisions before any backup/restore operation.
    from .kaggle_native_env import verified_native_environment
    if model_pins is not None:
        from .kaggle_pg_snapshot_native import _hash
        if type(model_pins) is not dict or set(model_pins)!={'model_sha256','tokenizer_sha256'}:
            raise SnapshotError('recovery_model_inventory_required')
        for value in model_pins.values():_hash(value)
        factory.model_pins=dict(model_pins)
    factory.native_env=await asyncio.to_thread(verified_native_environment,native_bundle)
    await factory.tools.verify(factory._run)
    return WorkerRecoveryPhases(factory=factory,download=download,workspace=workspace)


def bounded(value, limit=MAX_PHASE_FRAME_BYTES):
    if len(encode(value)) > limit:
        raise SnapshotError('recovery_phase_byte_budget')
    return value


def b64(data):return base64.b64encode(data).decode('ascii')


def unb64(value, limit=1048576):
    if not isinstance(value,str) or len(value)>4*((limit+2)//3):
        raise SnapshotError('recovery_phase_metadata_budget')
    try:data=base64.b64decode(value,validate=True)
    except ValueError:raise SnapshotError('recovery_phase_invalid_base64') from None
    if len(data)>limit or b64(data)!=value:
        raise SnapshotError('recovery_phase_metadata_budget')
    return data


class WorkerRecoveryPhases:
    """One factory, one isolated candidate, fixed replay and direct blob transfer.

    download is a retained scoped-transfer adapter: (BlobRef, operation_key) ->
    exact local downloaded path. workspace is a retained resolver of fresh owned
    private paths. Neither is reconstructed from a request or user labels.
    """
    def __init__(self, *, factory, download, workspace):
        from .kaggle_pg_snapshot_native import NativeSnapshotFactory
        if (type(factory) is not NativeSnapshotFactory or not callable(download)
                or not callable(workspace)):
            raise TypeError('retained native factory and scoped transfer required')
        self.factory,self.download,self.workspace=factory,download,workspace
        self._sessions={};self._results={};self._requests={};self._lock=asyncio.Lock()
        self._ready=asyncio.Event();self._initialized=None;self._initialization_error=None;self.drill=None
        # Assigned only by the already prepared TargetRuntime. No uploaded
        # pathname, module bytes or resource reference enters this WSS phase.
        self.read_handler_slot=None
        self.read_handler_control=None

    async def wait_initialized(self,database,run_id,launch_nonce,runtime_incarnation):
        if database!=self.factory.database or (run_id,launch_nonce,runtime_incarnation)!=self.factory.identity:
            raise SnapshotError('recovery_runtime_identity_changed')
        async with asyncio.timeout(900):await self._ready.wait()
        if self._initialization_error is not None:raise self._initialization_error
        await self._initialized.verify_for(database,run_id,launch_nonce,runtime_incarnation)
        return self._initialized

    async def __call__(self,frame):
        fields={'type','protocol_version','connection_generation','id','claim','kind','run_id',
            'launch_nonce','runtime_incarnation','database_generation','writer_generation','phase',
            'operation_key','request_sha256','arguments','compute_claim','dispatch_generation'}
        optional={'download_ticket','dataset_base','dataset_observation'}
        if (not isinstance(frame,dict) or set(frame)-optional!=fields
                or ('dataset_base' in frame)!=('dataset_observation' in frame)
                or 'dataset_base' in frame and frame.get('phase')!='prepare'
                or 'download_ticket' in frame and frame.get('phase') not in ('prepare','drill_prepare','read_handler_stage','dataset_verify') or frame['type']!='DISPATCH'
                or frame['protocol_version']!=2 or frame['kind']!='recovery_phase'
                or frame['phase'] not in PHASES-{'source_history'}-ORIGINAL_PHASES or type(frame['connection_generation']) is not int
                or frame['connection_generation']<1 or type(frame['dispatch_generation']) is not int
                or not 1<=frame['dispatch_generation']<=frame['connection_generation']
                or any(not isinstance(frame[k],str) or not 1<=len(frame[k].encode())<=256
                       for k in ('id','claim','compute_claim','operation_key'))
                or tuple(frame[k] for k in ('run_id','launch_nonce','runtime_incarnation'))!=self.factory.identity
                or (frame['database_generation'],frame['writer_generation'])!=(
                    self.factory.database.database_generation,self.factory.database.writer_generation)):
            raise SnapshotError('recovery_phase_dispatch_binding')
        bounded(frame)
        request=sha(encode({'phase':frame['phase'],'arguments':frame['arguments']}))
        if frame['request_sha256']!=request:raise SnapshotError('recovery_phase_request_hash')
        # Native current() is configured from this authenticated worker control
        # incarnation; it must not require database read readiness.
        binding=(frame['run_id'],frame['runtime_incarnation'],frame['connection_generation'])
        self.factory._current(binding)
        key=frame['operation_key']
        async with self._lock:
            prior=self._requests.get(key)
            if prior is not None:
                if prior!=request:raise SnapshotError('recovery_phase_operation_changed')
                if key in self._results:return self._results[key]
                session=self._sessions.get(frame.get('arguments',{}).get('handle'))
                if not (frame['phase']=='finish' and session in self.factory._finish_intents
                        or self.drill is not None and self.drill.can_reconcile(frame['phase'],frame['arguments'])):
                    raise SnapshotError('recovery_phase_unknown_requires_reconciliation')
            if len(self._requests)>=4096:raise SnapshotError('recovery_phase_retention_capacity')
            self._requests[key]=request  # UNKNOWN is retained before effects.
            if frame['phase'] in ('read_handler_stage','read_handler_activate','read_handler_rollback','capture','dataset_verify'):
                # Expected candidate/transfer/probe refusal is the outcome of
                # THIS durable maintenance operation, not a fatal worker
                # transport failure. BgeWssTransport._failure would otherwise
                # close the live model/PG runtime even after a safe rollback.
                # Only a fixed, secret-free result crosses the WSS boundary;
                # CancelledError/BaseException still propagates normally.
                try:
                    value=await self._execute(frame['phase'],frame['arguments'],key,binding,frame)
                except Exception:
                    reason=('snapshot_capture_rejected' if frame['phase']=='capture' else
                        'dataset_checkpoint_verify_rejected' if frame['phase']=='dataset_verify' else 'read_handler_maintenance_rejected')
                    value={'status':'rejected','reason':reason}
            else:
                try:
                    value=await self._execute(frame['phase'],frame['arguments'],key,binding,frame)
                except Exception as error:
                    # A fatal restore failure cannot produce readiness. Wake
                    # the initialization waiter with that same failure so
                    # transport cleanup does not spend 900 seconds draining
                    # a waiter whose producer has already failed.
                    # finish can be reconciled after a socket handover; do
                    # not poison that retained session's initialization.
                    if frame['phase']=='prepare' and self._initialized is None:
                        self._initialization_error=error;self._ready.set()
                    raise
            bounded(value,MAX_PHASE_RESULT_BYTES)
            result={'phase':frame['phase'],'request_sha256':request,'value':value}
            self._results[key]=result
            return result

    async def aclose(self):
        if self.drill is not None:await self.drill.aclose()
        # Includes a partially restored candidate that never released readiness.
        for candidate in tuple(self.factory._owned_candidates):
            await candidate.aclose()
            self.factory._owned_candidates.discard(candidate)
        self.factory._capabilities.clear()
        self._initialized=None

    def _session(self,args,binding):
        session=self._sessions.get(args['handle'])
        if session is None:raise SnapshotError('recovery_phase_session_unknown')
        session.binding=binding
        return session

    async def _execute(self,phase,args,key,binding,frame):
        from .kaggle_pg_snapshot_native import BlobRef
        if not isinstance(args,dict):raise SnapshotError('recovery_phase_arguments')
        if phase=='dataset_verify':
            from .kaggle_dataset_base import DatasetCheckpoint
            from .native_snapshot_transfer import SnapshotTicket,NativeSnapshotTransfer
            if set(args)!={'checkpoint','blob'}:raise SnapshotError('dataset_verify_arguments')
            checkpoint=DatasetCheckpoint.from_wire(args['checkpoint'])
            base=getattr(self.factory,'dataset_checkpoint',None)
            transfer=getattr(self.factory,'dataset_transfer',None)
            if (type(base) is not DatasetCheckpoint or type(transfer) is not NativeSnapshotTransfer
                    or (checkpoint.owner_slug,checkpoint.dataset_slug,checkpoint.database_generation)!=(
                        base.owner_slug,base.dataset_slug,self.factory.database.database_generation)
                    or checkpoint.dataset_version<base.dataset_version):
                raise SnapshotError('dataset_verify_runtime_binding')
            blob=BlobRef(**args['blob']);ticket=SnapshotTicket.from_wire(frame.get('download_ticket'))
            if ((ticket.key,ticket.size,ticket.sha256)!=(blob.key,checkpoint.archive.size,checkpoint.archive.sha256)
                    or blob.key.rsplit('/',1)[-1]!=checkpoint.archive.name):
                raise SnapshotError('dataset_verify_archive_changed')
            path=await transfer.download(ticket,self.workspace(key,'download'),operation_key=key)
            try:
                self.factory._current(binding)
                return dict(dataset_ref=checkpoint.owner_slug+'/'+checkpoint.dataset_slug,
                    dataset_version=checkpoint.dataset_version,base_sequence=checkpoint.base_sequence,
                    archive_sha256=checkpoint.archive.sha256,archive_size=checkpoint.archive.size)
            finally:path.unlink(missing_ok=True)
        if phase in DRILL_PHASES:
            if self.drill is None:raise SnapshotError('drill_fixed_owner_unavailable')
            return await self.drill(phase,args,key,binding,frame)
        if phase=='read_handler_status':
            from .kaggle_warm_read import ReadHandlerSlot
            if args:
                raise SnapshotError('read_handler_status_arguments')
            if type(self.read_handler_slot) is not ReadHandlerSlot:
                raise SnapshotError('read_handler_status_unavailable')
            # This is always a fresh, no-mutation loaded-version receipt.
            return self.read_handler_slot.receipt()
        if phase in ('read_handler_stage','read_handler_activate','read_handler_rollback'):
            from .kaggle_warm_maintenance import WarmHandlerMaintenance
            if type(self.read_handler_control) is not WarmHandlerMaintenance:
                raise SnapshotError('read_handler_maintenance_not_configured')
            return await self.read_handler_control(phase,args,key,frame)
        expected={'capture':{'versions','expected_sequence'},
            'prepare':{'manifest_b64','manifest_ref','blob','control_b64','expected_versions','database','identity','restore_operation'},
            'apply':{'handle','command_b64','accepted','handler_sha256','original_pg_writer'},
            'finish':{'handle','expected_tail','final_head','control_b64','expected_catalog_sha256'},
            'verify':{'reference'}}[phase]
        if set(args)!=expected:raise SnapshotError('recovery_phase_arguments')
        if phase=='capture':
            return await self.factory.capture(binding,workspace=self.workspace(key,'capture'),**args)
        if phase=='prepare':
            if args['database']!=self.factory.database.wire() or tuple(args['identity'])!=self.factory.identity:
                raise SnapshotError('recovery_phase_prepare_identity')
            manifest=unb64(args['manifest_b64']);control=unb64(args['control_b64'])
            if sha(manifest)!=args['manifest_ref']['sha256']:
                raise SnapshotError('recovery_phase_manifest_reference')
            import json
            declared=json.loads(manifest)
            if declared.get('blob')!=args['blob']:
                raise SnapshotError('recovery_phase_blob_reference_changed')
            checkpoint=getattr(self.factory,'dataset_checkpoint',None)
            if checkpoint is not None:
                if (checkpoint.database_generation!=self.factory.database.database_generation
                        or declared.get('database_generation')!=checkpoint.database_generation
                        or (len(manifest),len(control))!=(checkpoint.manifest.size,checkpoint.control.size)
                        or sha(manifest)!=checkpoint.manifest.sha256 or sha(control)!=checkpoint.control.sha256
                        or declared['blob']['key'].rsplit('/',1)[-1]!=checkpoint.archive.name
                        or (declared['blob']['size'],declared['blob']['sha256'])!=(checkpoint.archive.size,checkpoint.archive.sha256)
                        or declared.get('applied_sequence')!=checkpoint.base_sequence):
                    raise SnapshotError('dataset_restore_pinned_input_changed')
            if checkpoint is not None and 'dataset_base' in frame:
                from .kaggle_dataset_base import DatasetBasePin
                pin=DatasetBasePin.from_wire(frame.get('dataset_base'))
                observation=frame.get('dataset_observation',{})
                if (pin!=checkpoint.for_execution(run_id=frame['run_id'],notebook_version=pin.notebook_version)
                        or sha(manifest)!=pin.manifest.sha256 or sha(control)!=pin.control.sha256
                        or 'download_ticket' in frame):
                    raise SnapshotError('dataset_restore_pinned_input_changed')
                session=await self.factory.prepare_dataset_base(binding,operation_key=args['restore_operation'],pin=pin,
                    observed_dataset_ref=observation.get('dataset_ref'),
                    observed_dataset_version=observation.get('dataset_version'),
                    observed_notebook_version=observation.get('notebook_version'),
                    observed_run_id=observation.get('run_id'),expected_versions=args['expected_versions'],
                    workspace=self.workspace(args['restore_operation'],'restore'))
            else:
                if 'dataset_base' in frame or 'dataset_observation' in frame:
                    raise SnapshotError('dataset_restore_not_selected_by_launch')
                if checkpoint is not None and 'download_ticket' not in frame:
                    raise SnapshotError('dataset_restore_versioned_ticket_required')
                transfer={}
                if 'download_ticket' in frame:
                    transfer=dict(ticket=frame['download_ticket'],transfer_operation=frame['operation_key'])
                archive=self.download(BlobRef(**args['blob']),args['restore_operation'],**transfer)
                if inspect.isawaitable(archive):archive=await archive
                session=await self.factory.prepare(binding,operation_key=args['restore_operation'],
                    manifest_bytes=manifest,expected_manifest_sha256=sha(manifest),archive_path=archive,
                    control_bytes=control,expected_versions=args['expected_versions'],
                    workspace=self.workspace(args['restore_operation'],'restore'))
            for handle,retained in self._sessions.items():
                if retained is session:return {'handle':handle}
            handle=secrets.token_urlsafe(24);self._sessions[handle]=session
            return {'handle':handle}
        if phase=='apply':
            session=self._session(args,binding)
            if args['original_pg_writer']!=session.manifest['writer_generation']:
                raise SnapshotError('recovery_phase_original_writer_changed')
            # Reuse the fixed accepted command executor. The outer assignment
            # belongs to the current writer; replay keeps the old PG writer.
            command=unb64(args['command_b64'])
            inner={k:frame[k] for k in ('type','protocol_version','connection_generation','id','claim',
                'run_id','launch_nonce','runtime_incarnation','database_generation','compute_claim','dispatch_generation')}
            inner.update(kind='source_command',writer_generation=args['original_pg_writer'],
                command_b64=b64(command),command_sha256=sha(command),accepted=args['accepted'],handler_sha256=args['handler_sha256'])
            return await self.factory.apply(session,inner)
        if phase=='finish':
            session=self._session(args,binding)
            cap=await self.factory.finish(session,expected_tail=args['expected_tail'],final_head=args['final_head'],
                control_bytes=unb64(args['control_b64']),expected_catalog_sha256=args['expected_catalog_sha256'])
            self._initialized=cap;self._ready.set()
            return {'reference':self.factory.reference(cap)}
        return await self.factory.verify_reference(binding,args['reference'])
