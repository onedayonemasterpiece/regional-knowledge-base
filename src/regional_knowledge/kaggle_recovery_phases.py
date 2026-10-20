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
PHASES = frozenset({'capture','prepare','apply','finish','verify'})


async def build_recovery_worker(*,native_bundle,database,identity,tools,native_uid,native_gid,
                                current,workspace_private_current,workspace,upload=None,download=None,
                                ticket_channel=None,transfer=None,acquire=None,limits=None):
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
            headers={k.lower():v for k,v in ticket.headers}
            if ((ticket.key,ticket.size,ticket.sha256)!=(blob.key,blob.size,blob.sha256)
                    or headers.get('if-match')!=blob.native_version):
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
    factory=NativeSnapshotFactory(tools=PhysicalPgTools(**binaries),database=database,identity=identity,
        registry=fixed_registry(database,tools['command_handlers']),executor_factory=WorkerSourceExecutor,
        current=current,workspace_private_current=workspace_private_current,native_uid=native_uid,
        native_gid=native_gid,acquire=acquire,upload_ticket=upload,limits=limits or SnapshotLimits())
    # Verify exact installed ELF revisions before any backup/restore operation.
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
        self._ready=asyncio.Event();self._initialized=None

    async def wait_initialized(self,database,run_id,launch_nonce,runtime_incarnation):
        if database!=self.factory.database or (run_id,launch_nonce,runtime_incarnation)!=self.factory.identity:
            raise SnapshotError('recovery_runtime_identity_changed')
        async with asyncio.timeout(900):await self._ready.wait()
        await self._initialized.verify_for(database,run_id,launch_nonce,runtime_incarnation)
        return self._initialized

    async def __call__(self,frame):
        fields={'type','protocol_version','connection_generation','id','claim','kind','run_id',
            'launch_nonce','runtime_incarnation','database_generation','writer_generation','phase',
            'operation_key','request_sha256','arguments','compute_claim','dispatch_generation'}
        if (not isinstance(frame,dict) or set(frame)-{'download_ticket'}!=fields
                or 'download_ticket' in frame and frame.get('phase')!='prepare' or frame['type']!='DISPATCH'
                or frame['protocol_version']!=2 or frame['kind']!='recovery_phase'
                or frame['phase'] not in PHASES or type(frame['connection_generation']) is not int
                or frame['connection_generation']<1 or frame['dispatch_generation']!=frame['connection_generation']
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
                if key not in self._results:
                    raise SnapshotError('recovery_phase_unknown_requires_reconciliation')
                return self._results[key]
            if len(self._requests)>=4096:raise SnapshotError('recovery_phase_retention_capacity')
            self._requests[key]=request  # UNKNOWN is retained before effects.
            value=await self._execute(frame['phase'],frame['arguments'],key,binding,frame)
            bounded(value,MAX_PHASE_RESULT_BYTES)
            result={'phase':frame['phase'],'request_sha256':request,'value':value}
            self._results[key]=result
            return result

    def _session(self,args,binding):
        session=self._sessions.get(args['handle'])
        if session is None:raise SnapshotError('recovery_phase_session_unknown')
        session.binding=binding
        return session

    async def _execute(self,phase,args,key,binding,frame):
        from .kaggle_pg_snapshot_native import BlobRef
        if not isinstance(args,dict):raise SnapshotError('recovery_phase_arguments')
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
