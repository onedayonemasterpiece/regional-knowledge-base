"""Trusted notebook restore composition over the existing WSS resource owner.

Only deployment pins live on wire. Snapshot bytes arrive by scoped conditional
transfer and only the existing native verifier can release database readiness.
Imports before ATTACH remain standard-library only.
"""
from dataclasses import dataclass
from collections.abc import Mapping
from pathlib import PurePosixPath
from types import MappingProxyType


@dataclass(frozen=True)
class SnapshotRecoveryConfig:
    binaries: object
    allowed_origins: tuple[str,...]
    native_uid: int
    native_gid: int
    max_archive_bytes: int = 8*1024**3
    max_unpacked_bytes: int = 16*1024**3
    seconds: int = 900
    drill_empty: bool = False

    def __post_init__(self):
        from .native_snapshot_transfer import origin
        if (not isinstance(self.binaries,Mapping) or set(self.binaries)!={'postgres','pg_basebackup','pg_verifybackup'}
                or type(self.allowed_origins) is not tuple or not 1<=len(self.allowed_origins)<=4
                or len(set(self.allowed_origins))!=len(self.allowed_origins)
                or any(origin(value)!=value for value in self.allowed_origins)
                or any(type(value) is not int or not 1<=value<2**31 for value in (self.native_uid,self.native_gid))
                or type(self.max_archive_bytes) is not int or not 1<=self.max_archive_bytes<=8*1024**3
                or type(self.max_unpacked_bytes) is not int or not 1<=self.max_unpacked_bytes<=16*1024**3
                or type(self.seconds) is not int or not 1<=self.seconds<=900 or type(self.drill_empty) is not bool):
            raise ValueError('exact bounded native recovery deployment required')
        frozen={}
        for name,value in self.binaries.items():
            if (not isinstance(value,Mapping) or set(value)!={'path','version_output'}
                    or type(value['path']) is not str or not 1<=len(value['path'])<=512
                    or PurePosixPath(value['path']).is_absolute() or '..' in PurePosixPath(value['path']).parts
                    or str(PurePosixPath(value['path']))!=value['path']
                    or type(value['version_output']) is not str or not 1<=len(value['version_output'])<=256
                    or '\n' in value['version_output'] or 'PostgreSQL) 17.' not in value['version_output']):
                raise ValueError('exact bundle-relative recovery binary/version required')
            frozen[name]=MappingProxyType(dict(value))
        object.__setattr__(self,'binaries',MappingProxyType(frozen))

    def wire(self):
        return dict(binaries={key:dict(value) for key,value in self.binaries.items()},
            allowed_origins=list(self.allowed_origins),native_uid=self.native_uid,native_gid=self.native_gid,
            max_archive_bytes=self.max_archive_bytes,max_unpacked_bytes=self.max_unpacked_bytes,seconds=self.seconds,drill_empty=self.drill_empty)

    @classmethod
    def from_wire(cls,value):
        required=set(cls.__dataclass_fields__)-{'drill_empty'}
        if type(value) is not dict or not required<=set(value) or set(value)-required-{'drill_empty'}:
            raise ValueError('exact native recovery configuration fields required')
        if type(value['allowed_origins']) is not list:raise ValueError('exact recovery origin list required')
        return cls(**dict(value,allowed_origins=tuple(value['allowed_origins'])))


class DeferredRecoveryRuntime:
    """One native restore owner, installed before prepare blocks on its capability."""
    def __init__(self,config,*,executed_source):
        from .kaggle_snapshot_channel import SnapshotTicketChannel
        self.config,self.executed_source=config,executed_source
        self.spec=SnapshotRecoveryConfig.from_wire(config['target_runtime']['recovery'])
        if config['target_runtime']['database']['mode'] not in ('VERIFIED_INITIALIZED','NEW_EMPTY_GENERATION'):
            raise ValueError('fixed snapshot/restore deployment mode required')
        self.ticket_channel=SnapshotTicketChannel(timeout_seconds=min(self.spec.seconds,600))
        self.instance=None;self.identity=None;self.generation=None;self.closed=False;self._preparing=False
        self.workspace_root=None;self._paths=set();self._restore_operation=None

    def attached(self,generation,*,runtime_incarnation):
        if (self.closed or type(generation) is not int or generation<1
                or type(runtime_incarnation) is not str or not 1<=len(runtime_incarnation)<=256):
            raise ValueError('recovery attachment required')
        identity=(self.config['run_id'],self.config['launch_nonce'],runtime_incarnation)
        if self.identity is not None and self.identity!=identity:raise ValueError('recovery incarnation changed')
        self.identity=identity;self.generation=generation
        worker=getattr(self.instance,'recovery_worker',None)
        io=getattr(worker,'snapshot_io',None)
        if io is not None:io.rebind((identity[0],identity[2],generation))

    def detached(self,generation):
        if self.generation==generation:self.generation=None

    def current(self,binding):
        return (not self.closed and self.identity is not None and type(binding) is tuple
            and binding==(self.identity[0],self.identity[2],self.generation) and self.generation is not None)

    def _workspace(self,operation,phase):
        import hashlib
        from pathlib import Path
        if (self.closed or self.workspace_root is None or type(operation) is not str
                or not 1<=len(operation.encode())<=256 or phase not in ('restore','download','capture','drill_restore','read_handler')):
            raise ValueError('owned recovery workspace required')
        if phase=='drill_restore':
            from .kaggle_snapshot_drill import drill_database
            from .kaggle_owned_pg import OwnedDatabase
            path=Path(drill_database(OwnedDatabase(**self.config['target_runtime']['database']),self.identity).socket).parent
        elif phase=='restore':
            if self._restore_operation not in (None,operation):raise ValueError('another restore operation owns endpoint')
            self._restore_operation=operation
            path=Path(self.config['target_runtime']['database']['socket']).parent
        else:
            path=self.workspace_root/(hashlib.sha256(operation.encode()).hexdigest()+'-'+phase)
        self._paths.add(path)
        return path

    def _private(self,path):
        import os,stat
        from pathlib import Path
        from .kaggle_owned_pg import PRIVATE_RUNTIME_ROOT
        path=Path(path)
        if self.closed or path not in self._paths or not path.is_relative_to(PRIVATE_RUNTIME_ROOT):return False
        try:
            for parent in (path.parent,*path.parent.parents):
                if not parent.is_relative_to(PRIVATE_RUNTIME_ROOT):break
                info=parent.lstat()
                if not stat.S_ISDIR(info.st_mode) or info.st_uid not in (0,os.geteuid()) or info.st_mode&0o022:return False
            return True
        except OSError:return False

    async def prepare(self,encoder,**identity):
        import asyncio
        import os
        import pwd
        import tempfile
        from pathlib import Path
        from .kaggle_target_runtime import TargetRuntime
        from .kaggle_native_bundle import NativeBundleSpec,prepare_native_bundle
        from .kaggle_owned_pg import ensure_private_runtime_root
        from .kaggle_pg_snapshot_native import SnapshotLimits
        from .native_snapshot_transfer import NativeSnapshotTransfer
        actual=(identity['run_id'],identity['launch_nonce'],identity['runtime_incarnation'])
        if (self.closed or actual[:2]!=(self.config['run_id'],self.config['launch_nonce'])
                or self.identity is not None and self.identity!=actual or self.generation is None):
            raise ValueError('recovery runtime identity/attachment changed')
        if self._preparing:raise ValueError('recovery preparation already started')
        self._preparing=True
        self.identity=actual
        account=pwd.getpwuid(self.spec.native_uid)
        if account.pw_gid!=self.spec.native_gid:raise ValueError('recovery native account identity differs')
        root=ensure_private_runtime_root(native_uid=self.spec.native_uid,native_gid=self.spec.native_gid)
        self.workspace_root=Path(tempfile.mkdtemp(prefix='rkb-recovery-',dir=root))
        # The supervisor owns this parent; only the exact native identity may
        # traverse it. Descendant factory workspaces remain exclusive0700.
        if os.geteuid()==0:
            os.chown(self.workspace_root,-1,self.spec.native_gid)
            self.workspace_root.chmod(0o710)
        bundle_spec=NativeBundleSpec.from_wire(self.config['target_runtime']['native_bundle'])
        inventory=tuple(dict(name=e.requirement.name,size=e.requirement.size,sha256=e.requirement.sha256,role=e.role)
            for e in encoder.artifacts.entries)
        bundle=await asyncio.to_thread(prepare_native_bundle,encoder.artifacts.root,bundle_spec.manifest_name,
            bundle_spec.manifest_sha256,inventory=inventory)
        runtime=TargetRuntime(self.config,executed_source=self.executed_source)
        self.instance=runtime
        fresh=self.config['target_runtime']['database']['mode']=='NEW_EMPTY_GENERATION'
        prepared=await runtime.prepare(encoder,**identity) if fresh else None
        await runtime.configure_recovery(encoder,**identity,native_bundle=bundle,
            tools=dict(binaries={k:dict(v) for k,v in self.spec.binaries.items()},
                registry_sha256=self.config['target_runtime']['registry_sha256']),
            native_uid=self.spec.native_uid,native_gid=self.spec.native_gid,current=self.current,
            workspace_private_current=self._private,workspace=self._workspace,
            ticket_channel=self.ticket_channel,transfer=NativeSnapshotTransfer(
                allowed_origins=frozenset(self.spec.allowed_origins),max_bytes=self.spec.max_archive_bytes,
                timeout_seconds=self.spec.seconds),limits=SnapshotLimits(max_archive_bytes=self.spec.max_archive_bytes,
                max_unpacked_bytes=self.spec.max_unpacked_bytes,timeout_seconds=self.spec.seconds))
        if self.spec.drill_empty:
            if not fresh:raise ValueError('first snapshot drill requires fresh serving owner')
            from .kaggle_snapshot_drill import EmptySnapshotDrill
            runtime.recovery_worker.drill=EmptySnapshotDrill(runtime.recovery_worker.factory,
                download=runtime.recovery_worker.download,workspace=self._workspace)
        return prepared if fresh else await runtime.prepare(encoder,**identity)

    async def __call__(self,frame):
        import asyncio
        binding=(frame.get('run_id'),frame.get('runtime_incarnation'),frame.get('connection_generation'))
        if (not self.current(binding) or frame.get('launch_nonce')!=self.config['launch_nonce']):
            raise ValueError('recovery dispatch attachment changed')
        # Model/Stage0 and owner verification may still be in progress. Keep
        # the same assignment; no ordinary read becomes available while waiting.
        async with asyncio.timeout(self.spec.seconds):
            while self.instance is None:
                if not self.current(binding):raise ValueError('recovery dispatch attachment changed')
                await asyncio.sleep(.02)
            from .kaggle_pg_snapshot_native import SnapshotError
            for _ in range(3):
                try:return await self.instance.recovery(frame)
                except SnapshotError as exc:
                    if frame.get('phase') not in ('finish','drill_finish','drill_close','drill_verify') or str(exc)!='native_restore_binding_changed':raise
                    previous=frame['connection_generation']
                    while self.generation is None or self.generation==previous:
                        if self.closed:raise
                        await asyncio.sleep(.02)
                    # Keep the original resource/dispatch claim; renew only
                    # the authenticated attachment's generation.
                    frame=dict(frame,connection_generation=self.generation)
            raise SnapshotError('native_restore_reconnect_budget')

    async def aclose(self):
        self.closed=True;self.generation=None
        if self.instance is not None:await self.instance.aclose()
