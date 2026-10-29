"""Native-only private physical PG backup helpers; no controller or provider IAM.

Explicit operations only. No effect occurs on import. Stage0 must prove native
binary/dependency revisions and private writable paths before any invocation.
"""
from __future__ import annotations
import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass, asdict, replace
import hashlib
import json
import math
import os
from pathlib import Path, PurePosixPath
import shutil
import signal
import secrets
import stat
import tarfile
import psycopg

from .kaggle_database_probe import native_probe_for_schema, PGVECTOR_VERSIONS
from .kaggle_apply import handover_writer
from .kaggle_restored_database import VerifiedInitializedDatabase
from .kaggle_owned_pg import PRIVATE_RUNTIME_ROOT, ensure_private_runtime_root


class SnapshotError(RuntimeError):
    pass


async def _blocking(call,*args,**kwargs):
    """Cancellation is not completion of archive I/O or ownership changes."""
    task=asyncio.create_task(asyncio.to_thread(call,*args,**kwargs));cancelled=False
    while not task.done():
        try:await asyncio.shield(task)
        except asyncio.CancelledError:cancelled=True
        except Exception:break
    if cancelled:
        if not task.cancelled():task.exception()
        raise asyncio.CancelledError
    return task.result()


def encode(value):
    return json.dumps(value,sort_keys=True,separators=(',', ':'),ensure_ascii=False,allow_nan=False).encode()


def sha(data):return hashlib.sha256(data).hexdigest()


def digest_file(path,limit):
    h=hashlib.sha256();size=0
    with open(path,'rb') as source:
        while chunk:=source.read(1024*1024):
            size+=len(chunk)
            if size>limit:raise SnapshotError('snapshot_byte_budget')
            h.update(chunk)
    return size,h.hexdigest()


def _hash(value):
    if not isinstance(value,str) or len(value)!=64 or set(value)-set('0123456789abcdef'):
        raise ValueError('explicit SHA-256 required')


def validate_versions(versions):
    required={'server_version_num','pgvector_version','model_sha256','tokenizer_sha256'}
    legacy={'catalog_sha256','acl_sha256','schema_revision','function_revision','executor_revision'}
    if not isinstance(versions,dict) or not required<=set(versions) or set(versions)-required-legacy-{'schema_version','postgis_version','protocol_version'}:
        raise SnapshotError('snapshot_exact_runtime_versions_required')
    if (type(versions['server_version_num']) is not int or versions['server_version_num']//10000!=17
            or versions['pgvector_version'] not in PGVECTOR_VERSIONS
            or 'schema_version' in versions and (type(versions['schema_version']) is not int or versions['schema_version']<1)):
        raise SnapshotError('snapshot_native_version_mismatch')
    for k in ('model_sha256','tokenizer_sha256'):_hash(versions[k])


def compatible_versions(base,expected):
    validate_versions(base);validate_versions(expected)
    # Physical compatibility is PG major plus native/model/protocol versions.
    # An older schema is upgraded from its verified files before tail replay.
    return (base['server_version_num']//10000==expected['server_version_num']//10000
        and (base['pgvector_version']==expected['pgvector_version']
             or (base['pgvector_version'],expected['pgvector_version'])==('0.8.2','0.8.4'))
        and all(base.get(key)==expected.get(key) for key in (
            'postgis_version','model_sha256','tokenizer_sha256','protocol_version'))
        and ('schema_version' not in base or 'schema_version' not in expected
             or base['schema_version']<=expected['schema_version']))


@dataclass(frozen=True)
class SnapshotLimits:
    max_archive_bytes: int = 8*1024**3
    max_unpacked_bytes: int = 16*1024**3
    max_files: int = 200000
    max_receipts: int = 2000000
    max_tail_commands: int = 2048
    max_tail_bytes: int = 16*1024**2
    max_control_bytes: int = 1024**2
    timeout_seconds: float = 900

    def __post_init__(self):
        if (any(type(v) is not int or v<1 for k,v in asdict(self).items() if k!='timeout_seconds')
                or type(self.timeout_seconds) not in (int,float) or not math.isfinite(self.timeout_seconds)
                or not 0<self.timeout_seconds<=3600):
            raise ValueError('finite snapshot budgets required')


@dataclass(frozen=True)
class NativeBinary:
    path: str
    size: int
    sha256: str
    version_output: str

    def __post_init__(self):
        _hash(self.sha256)
        if (not Path(self.path).is_absolute() or '..' in Path(self.path).parts
                or type(self.size) is not int or not 0<self.size<=100*1024**2
                or not isinstance(self.version_output,str) or not 1<=len(self.version_output)<=256):
            raise ValueError('Stage0-pinned native binary required')


@dataclass(frozen=True)
class PhysicalPgTools:
    pg_basebackup: NativeBinary
    pg_verifybackup: NativeBinary
    postgres: NativeBinary

    async def verify(self,run):
        for name in ('pg_basebackup','pg_verifybackup','postgres'):
            binary=getattr(self,name)
            if await _blocking(digest_file,binary.path,binary.size)!=(binary.size,binary.sha256):
                raise SnapshotError('native_snapshot_binary_hash')
            output=await run(binary.path,'--version',timeout=5)
            if output.strip()!=binary.version_output or 'PostgreSQL) 17.' not in output:
                raise SnapshotError('native_snapshot_binary_version')


@dataclass(frozen=True)
class BlobRef:
    key: str
    size: int
    sha256: str
    native_version: str

    def __post_init__(self):
        _hash(self.sha256)
        if (not isinstance(self.key,str) or not 1<=len(self.key)<=1024
                or type(self.size) is not int or self.size<1
                or not isinstance(self.native_version,str) or not 1<=len(self.native_version)<=512):
            raise ValueError('exact private artifact native version required')


def private_workspace(path,required_bytes,*,native_uid=None,native_gid=None):
    root=Path(path)
    if (not root.is_absolute() or not root.is_relative_to(PRIVATE_RUNTIME_ROOT)
            or root==PRIVATE_RUNTIME_ROOT or '..' in root.parts):
        raise SnapshotError('snapshot_private_workspace_required')
    try:
        ensure_private_runtime_root(native_uid=native_uid,native_gid=native_gid)
        if root.parent.resolve(strict=True)!=root.parent:raise OSError('private parent symlink')
        for parent in (root.parent,*root.parent.parents):
            if not parent.is_relative_to(PRIVATE_RUNTIME_ROOT):break
            info=parent.stat()
            if info.st_uid not in (0,os.geteuid()) or stat.S_IMODE(info.st_mode)&0o022:
                raise OSError('untrusted private parent')
    except (OSError,RuntimeError):
        raise SnapshotError('snapshot_private_workspace_required') from None
    if shutil.disk_usage(root.parent).free<required_bytes:raise SnapshotError('snapshot_disk_budget')
    root.mkdir(mode=0o700) # exclusive; existing/partial/live directories are refused
    return root


def inventory_tree(root,limits):
    records=[];used=0
    for directory,dirs,files in os.walk(root,followlinks=False):
        for name in sorted(dirs+files):
            path=Path(directory)/name;info=path.lstat();relative=path.relative_to(root).as_posix()
            if len(relative.encode())>1024:raise SnapshotError('snapshot_member_path_budget')
            if stat.S_ISLNK(info.st_mode) or not (stat.S_ISDIR(info.st_mode) or stat.S_ISREG(info.st_mode)):
                raise SnapshotError('snapshot_nonregular_member')
            if len(records)>=limits.max_files:raise SnapshotError('snapshot_file_budget')
            if stat.S_ISDIR(info.st_mode):records.append((relative,0,None));continue
            size,digest=digest_file(path,limits.max_unpacked_bytes-used);used+=size
            records.append((relative,size,digest))
    records.sort()
    names={r[0] for r in records}
    if not {'PG_VERSION','backup_manifest','global/pg_control'}<=names:
        raise SnapshotError('snapshot_required_native_members_missing')
    if names & {'postmaster.pid','standby.signal','recovery.signal'}:
        raise SnapshotError('snapshot_live_or_replica_state_refused')
    return records,used,sha(encode(records))


def pack_tree(root,path,limits):
    records,used,digest=inventory_tree(root,limits)
    class BoundedWriter:
        def __init__(self,destination):self.destination,self.used=destination,0
        def write(self,data):
            self.used+=len(data)
            if self.used>limits.max_archive_bytes:raise SnapshotError('snapshot_archive_byte_budget')
            return self.destination.write(data)
    with open(path,'xb') as destination:
        os.chmod(path,0o600)
        with tarfile.open(fileobj=BoundedWriter(destination),mode='w|') as archive:
            for relative,_,_ in records:archive.add(root/relative,arcname=relative,recursive=False)
        destination.flush();os.fsync(destination.fileno())
    size,archive_hash=digest_file(path,limits.max_archive_bytes)
    return records,used,digest,size,archive_hash


def unpack_tree(path,destination,manifest,limits):
    destination.mkdir(mode=0o700)
    names=set();used=0
    counts={'headers':0,'metadata':0}
    class BoundedTarInfo(tarfile.TarInfo):
        def _proc_member(self,archive):
            value=self;counts['headers']+=1
            if (counts['headers']>limits.max_files*3+1024 or value.size<0
                    or value.size>limits.max_unpacked_bytes):
                raise SnapshotError('snapshot_tar_header_budget')
            if value.type in (tarfile.XHDTYPE,tarfile.XGLTYPE,tarfile.GNUTYPE_LONGNAME,tarfile.GNUTYPE_LONGLINK,tarfile.SOLARIS_XHDTYPE):
                counts['metadata']+=value.size
                if value.size>65536 or counts['metadata']>8*1024*1024:
                    raise SnapshotError('snapshot_tar_metadata_budget')
            if value.type==tarfile.GNUTYPE_SPARSE:raise SnapshotError('snapshot_sparse_archive_forbidden')
            return super()._proc_member(archive)
        def _proc_gnusparse_00(self,*args):raise SnapshotError('snapshot_sparse_archive_forbidden')
        def _proc_gnusparse_01(self,*args):raise SnapshotError('snapshot_sparse_archive_forbidden')
        def _proc_gnusparse_10(self,*args):raise SnapshotError('snapshot_sparse_archive_forbidden')
    with tarfile.open(path,mode='r|',tarinfo=BoundedTarInfo) as archive:
        for member in archive:
            name=member.name;parts=PurePosixPath(name).parts
            if (not parts or PurePosixPath(name).is_absolute() or any(p in ('..','.') for p in parts)
                    or len(name.encode())>1024 or '\\' in name or name in names or len(names)>=limits.max_files
                    or not (member.isdir() or member.isfile()) or member.size<0):
                raise SnapshotError('snapshot_unsafe_archive_member')
            if len(names)>=manifest.get('file_count',limits.max_files):raise SnapshotError('snapshot_file_count_differs')
            names.add(name);target=destination.joinpath(*parts)
            target.parent.mkdir(mode=0o700,parents=True,exist_ok=True)
            if member.isdir():target.mkdir(mode=0o700,exist_ok=True);continue
            used+=member.size
            if used>min(limits.max_unpacked_bytes,manifest.get('unpacked_bytes',limits.max_unpacked_bytes)):
                raise SnapshotError('snapshot_unpacked_byte_budget')
            source=archive.extractfile(member)
            with open(target,'xb') as output:
                os.chmod(target,0o600);copied=0
                while chunk:=source.read(min(1024*1024,member.size-copied+1)):
                    copied+=len(chunk)
                    if copied>member.size:raise SnapshotError('snapshot_member_length')
                    output.write(chunk)
                if copied!=member.size:raise SnapshotError('snapshot_member_length')
    records,total,digest=inventory_tree(destination,limits)
    if (len(records)!=manifest['file_count'] or total!=manifest['unpacked_bytes']
            or digest!=manifest['inventory_sha256']
            or digest_file(destination/'backup_manifest',limits.max_unpacked_bytes)[1]!=manifest['backup_manifest_sha256']):
        raise SnapshotError('snapshot_native_inventory_differs')


async def applied_prefix(connection,sequence,limits):
    h=hashlib.sha256(b'rkb.applied-prefix.v1');count=0
    async with connection.cursor(name='rkb_snapshot_prefix') as cursor:
        await cursor.execute('SELECT sequence,operation_key,command_sha256,handler_sha256,'
            'accepted_manifest_sha256,status,outcome_sha256,database_generation,writer_generation '
            'FROM rkb_v4.applied_ops WHERE sequence<=%s ORDER BY sequence LIMIT %s',(sequence,limits.max_receipts+1))
        while (row:=await cursor.fetchone()) is not None:
            count+=1
            if count>limits.max_receipts or row[0]!=count or count>sequence:
                raise SnapshotError('snapshot_applied_prefix_gap')
            encoded=encode(row)
            if len(encoded)>8192:raise SnapshotError('snapshot_receipt_metadata_bound')
            h.update(len(encoded).to_bytes(4,'big'));h.update(encoded)
    if count!=sequence:raise SnapshotError('snapshot_applied_cursor_differs')
    return h.hexdigest()


class _CandidatePg:
    def __init__(self,process,database,connect):self.process,self.database,self.connect=process,database,connect
    @asynccontextmanager
    async def acquire(self):
        connection=await self.connect(**self.database.connection(reader=False))
        async with connection:yield connection
    async def aclose(self):
        if self.process.returncode is None:
            self.process.send_signal(signal.SIGINT)
            async with asyncio.timeout(30):await self.process.wait()



@dataclass(eq=False)
class _NativeSession:
    candidate: object
    manifest: dict
    control: bytes
    binding: object
    executor: object
    receipts: list


class NativeSnapshotFactory:
    """Private native phases; fixed accepted-command replay, no journal client."""
    def __init__(self,*,tools,database,identity,registry,executor_factory,current,
                 workspace_private_current,native_uid,native_gid,acquire=None,
                 upload_ticket=None,
                 limits=SnapshotLimits(),connect=psycopg.AsyncConnection.connect,
                 spawn=asyncio.create_subprocess_exec,probe=None):
        if (type(tools) is not PhysicalPgTools or not callable(executor_factory)
                or not callable(current) or not callable(workspace_private_current)
                or not isinstance(identity,tuple) or len(identity)!=3):
            raise TypeError('fixed native restore deployment required')
        self.tools,self.database,self.identity,self.registry=tools,database,identity,registry
        self.executor_factory,self.current=executor_factory,current
        self._custom_probe=probe is not None
        self.workspace_private_current=workspace_private_current
        self.native_uid,self.native_gid=native_uid,native_gid
        self.acquire,self.limits,self.connect,self.spawn=acquire,limits,connect,spawn
        self.native_env={'PATH':'/usr/bin:/bin','LANG':'C','LC_ALL':'C'}
        self.model_pins=None
        self.upload_ticket=upload_ticket
        self.probe=probe or native_probe_for_schema()
        self._sessions=set();self._capabilities=set();self._owned_candidates=set()
        self._operations={}
        self._completed={};self._references={};self._finish_intents={}

    def _current(self,binding):
        if not self.current(binding):raise SnapshotError('native_restore_binding_changed')

    def _versions(self,versions):
        validate_versions(versions)
        if self.model_pins is not None and any(versions.get(key)!=value for key,value in self.model_pins.items()):
            raise SnapshotError('snapshot_resident_model_tokenizer_changed')

    async def capture(self,binding,*,workspace,versions,expected_sequence,upload_callback=None,before_effect=None):
        """A scoped direct upload callback receives bytes, never bucket credentials."""
        async def current():
            if before_effect is None:self._current(binding)
            else:await before_effect()
        await current();self._versions(versions)
        if not callable(self.acquire) or not callable(self.upload_ticket):
            raise SnapshotError('native_backup_scoped_transport_unavailable')
        async with asyncio.timeout(self.limits.timeout_seconds):
            await self.tools.verify(self._run)
            catalog,fields=await self.probe(acquire=self.acquire)
            if (fields['schema_version']!=versions.get('schema_version') or fields['database_generation']!=self.database.database_generation
                    or fields['applied_seq']<expected_sequence
                    ):
                raise SnapshotError('native_backup_current_prefix')
            root=await _blocking(self._workspace,workspace,self.limits.max_unpacked_bytes*2)
            async with self.acquire() as connection:
                async with connection.transaction():
                    await connection.execute("SET LOCAL statement_timeout='2000ms'")
                    cursor=await connection.execute('SELECT system_identifier::text FROM pg_control_system()')
                    system_identifier=(await cursor.fetchone())[0]
                    cursor=await connection.execute('SELECT current_setting(\'server_version_num\')::integer,'
                        'writer_generation FROM rkb_v4.recovery_state WHERE singleton')
                    observed=await cursor.fetchone()
                    if observed is None or observed[0]//10000!=17 or observed[1]!=self.database.writer_generation:
                        raise SnapshotError('native_backup_version_or_writer')
                    cursor=await connection.execute("SELECT extversion FROM pg_extension WHERE extname='vector'")
                    if await cursor.fetchone()!=(versions['pgvector_version'],):
                        raise SnapshotError('native_backup_pgvector_version')
                    cursor=await connection.execute('SELECT datname FROM pg_database WHERE NOT datistemplate '
                        'ORDER BY datname COLLATE "C" LIMIT 4')
                    if {row[0] for row in await cursor.fetchall()}!={'postgres',self.database.dbname}:
                        raise SnapshotError('native_backup_cluster_not_dedicated')
                    prefix=await applied_prefix(connection,expected_sequence,self.limits)
            await self._run(self.tools.pg_basebackup.path,'--pgdata',str(root/'base'),
                '--wal-method=stream','--checkpoint=fast','--manifest-checksums=SHA256','--no-password',
                '--host',self.database.socket,'--port',str(self.database.port),'--username','rkb_bootstrap',
                timeout=self.limits.timeout_seconds)
            await self._run(self.tools.pg_verifybackup.path,str(root/'base'),timeout=self.limits.timeout_seconds)
            records,unpacked,inventory,size,digest=await _blocking(pack_tree,root/'base',root/'base.tar',self.limits)
            _,after=await self.probe(acquire=self.acquire)
            await current()
            # pg_basebackup + streamed WAL is a consistent PG image. Its actual
            # recovered cursor may exceed the pre-backup verified lower bound;
            # do not reject a concurrent committed APPLIED operation.
            if (after['applied_seq']<expected_sequence
                    or after['schema_version']!=versions.get('schema_version')
                    or after['database_generation']!=self.database.database_generation):
                raise SnapshotError('native_backup_source_changed')
            blob=await (upload_callback or self.upload_ticket)(root/'base.tar',size=size,sha256=digest)
            if type(blob) is not BlobRef or (blob.size,blob.sha256)!=(size,digest):
                raise SnapshotError('native_backup_upload_receipt')
            return {'database_generation':self.database.database_generation,'writer_generation':self.database.writer_generation,
                'applied_sequence':expected_sequence,'applied_prefix_sha256':prefix,'versions':dict(versions),'blob':asdict(blob),
                'system_identifier':system_identifier,'file_count':len(records),'unpacked_bytes':unpacked,
                'inventory_sha256':inventory,'backup_manifest_sha256':digest_file(root/'base/backup_manifest',self.limits.max_unpacked_bytes)[1],
                'native_tools':{name:asdict(getattr(self.tools,name)) for name in ('pg_basebackup','pg_verifybackup','postgres')}}

    async def prepare_dataset_base(self, binding, *, operation_key, pin,
                                   observed_dataset_ref, observed_dataset_version,
                                   observed_notebook_version, observed_run_id,
                                   expected_versions, workspace,
                                   mount_root=Path('/kaggle/input')):
        """Converge a version-attested Kaggle Dataset on the SAME native PG path.

        This is callable only by the trusted already-attached restore owner.
        The provider attestation must originate independently of the pin; a
        dataset mount slug alone is NOT sufficient. No version evidence means
        no filesystem read or PostgreSQL process side effects.
        """
        from .kaggle_dataset_base import DatasetBasePin, verify_mounted_dataset_base
        self._current(binding)
        self._versions(expected_versions)
        if (type(pin) is not DatasetBasePin or pin.run_id != self.identity[0]
                or pin.database_generation != self.database.database_generation
                or observed_run_id != self.identity[0]):
            raise SnapshotError('dataset_restore_runtime_binding')
        verified=await _blocking(verify_mounted_dataset_base,pin,
            observed_dataset_ref=observed_dataset_ref,
            observed_dataset_version=observed_dataset_version,
            observed_notebook_version=observed_notebook_version,
            observed_run_id=observed_run_id,mount_root=mount_root)
        checkpoint=getattr(self,'dataset_checkpoint',None)
        if checkpoint is not None:
            from .kaggle_dataset_base import verify_mounted_journal
            await _blocking(verify_mounted_journal,checkpoint,mount_root=mount_root)
        try:
            manifest=json.loads(verified.manifest_bytes)
            if not compatible_versions(manifest['versions'],expected_versions):
                raise SnapshotError('dataset_restore_versions_mismatch')
        except (ValueError,TypeError,KeyError):
            raise SnapshotError('dataset_restore_versions_mismatch') from None
        # Existing prepare() supplies completed-session reuse and native
        # pg_verifybackup/PG17+pgvector/system ID/accepted-prefix validation.
        # It independently rehashes the archive bytes from this mounted path.
        return await self.prepare(binding,operation_key=operation_key,
            manifest_bytes=verified.manifest_bytes,
            expected_manifest_sha256=pin.manifest.sha256,
            archive_path=verified.archive_path,
            control_bytes=verified.control_bytes,
            expected_versions=expected_versions,
            workspace=workspace)

    async def prepare(self,binding,*,operation_key,**arguments):
        """Reuse completed candidates; worker serialization owns in-flight work."""
        self._current(binding)
        if not isinstance(operation_key,str) or not 1<=len(operation_key.encode())<=256:
            raise SnapshotError('native_restore_operation_key_required')
        request=sha(encode({'manifest':arguments.get('expected_manifest_sha256'),
            'control':sha(arguments.get('control_bytes',b'')),
            'expected_versions':arguments.get('expected_versions'),'identity':self.identity}))
        previous=self._operations.get(operation_key)
        if previous is not None:
            digest,session=previous
            if digest!=request:raise SnapshotError('native_restore_operation_changed')
            session.binding=binding
            return session
        if len(self._operations)>=2:raise SnapshotError('native_restore_operation_capacity')
        async with asyncio.timeout(self.limits.timeout_seconds):
            session=await self._prepare(binding,**arguments)
        self._operations[operation_key]=(request,session)
        return session

    async def _prepare(self,binding,*,manifest_bytes,expected_manifest_sha256,archive_path,
                      control_bytes,expected_versions,workspace):
        """Artifact bytes arrive through an existing exact scoped download ticket.

        This API is internal to authenticated owned-worker control, never a
        caller-facing tool or source of bucket/user credentials.
        """
        self._current(binding);self._versions(expected_versions)
        if len(manifest_bytes)>self.limits.max_control_bytes or len(control_bytes)>self.limits.max_control_bytes:
            raise SnapshotError('native_restore_metadata_budget')
        if sha(manifest_bytes)!=expected_manifest_sha256:raise SnapshotError('native_restore_manifest_hash')
        m=json.loads(manifest_bytes)
        if (encode(m)!=manifest_bytes or m.get('codec')!='rkb.pg-physical-base.v1'
                or not compatible_versions(m.get('versions'),expected_versions) or m.get('database_generation')!=self.database.database_generation
                or m.get('writer_generation')==self.database.writer_generation
                or len(manifest_bytes)>self.limits.max_control_bytes
                or len(control_bytes)>self.limits.max_control_bytes
                or sha(control_bytes)!=m['source_control']['sha256']):
            raise SnapshotError('native_restore_manifest_binding')
        blob=BlobRef(**m['blob'])
        # Current executables are verified against the loaded bundle below.
        # Snapshot binary hashes are provenance; PG17 patch releases may differ.
        if (blob.size>self.limits.max_archive_bytes or m['unpacked_bytes']>self.limits.max_unpacked_bytes
                or m['file_count']>self.limits.max_files):raise SnapshotError('native_restore_artifact_budget')
        if len(self._owned_candidates)>=2:raise SnapshotError('native_restore_candidate_capacity')
        # One controller-selected target pin for this retained restore owner.
        selected=expected_versions['pgvector_version']
        if getattr(self,'_selected_pgvector_version',selected)!=selected:
            raise SnapshotError('native_restore_expected_pgvector_changed')
        self._selected_pgvector_version=selected
        if not self._custom_probe:self.probe=native_probe_for_schema(pgvector_version=selected)
        await self.tools.verify(self._run)
        root=await _blocking(self._workspace,workspace,m['unpacked_bytes'])
        if self.database.socket!=str(root/'socket'):raise SnapshotError('native_restore_socket_binding')
        if await _blocking(digest_file,archive_path,self.limits.max_archive_bytes)!=(blob.size,blob.sha256):
            raise SnapshotError('native_restore_archive_hash')
        await _blocking(unpack_tree,archive_path,root/'data',m,self.limits)
        await _blocking(self._own_workspace,root/'data',recursive=True)
        await self._run(self.tools.pg_verifybackup.path,str(root/'data'),timeout=self.limits.timeout_seconds)
        (root/'socket').mkdir(mode=0o700);self._own_workspace(root/'socket')
        process=await self.spawn(self.tools.postgres.path,'-D',str(root/'data'),'-k',self.database.socket,
            '-h','','-p',str(self.database.port),'-c','archive_mode=off','-c','archive_command=',
            '-c','restore_command=','-c','shared_preload_libraries=','-c','session_preload_libraries=',
            '-c','local_preload_libraries=','-c','max_connections=8','-c','shared_buffers=32MB',
            '-c','work_mem=4MB','-c','maintenance_work_mem=16MB',stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.DEVNULL,stderr=asyncio.subprocess.DEVNULL,
            env=self.native_env,**self._identity())
        candidate=_CandidatePg(process,self.database,self.connect);self._owned_candidates.add(candidate)
        startup_started=asyncio.get_running_loop().time()
        try:
            while True:
                if process.returncode is not None:raise SnapshotError('native_restore_start_failed')
                try:
                    async with candidate.acquire():pass
                    break
                except psycopg.OperationalError:await asyncio.sleep(.1)
            print(encode(dict(event='native_pg_restore_startup',run_id=self.identity[0],state='ready',
                elapsed_seconds=round(asyncio.get_running_loop().time()-startup_started,3))).decode(),flush=True)
            async with candidate.acquire() as connection:
                async with connection.transaction():
                    await connection.execute("SET LOCAL statement_timeout='2000ms'")
                    cursor=await connection.execute('SELECT system_identifier::text FROM pg_control_system()')
                    if await cursor.fetchone()!=(m['system_identifier'],):raise SnapshotError('native_restore_system_identity')
                    # The exact archive/inventory and pg_verifybackup already
                    # verify this ready Dataset. Its internal cursor is checked
                    # below; do not rescan lifetime applied_ops on every boot.
                    cursor=await connection.execute('SELECT writer_generation FROM rkb_v4.recovery_state WHERE singleton')
                    if await cursor.fetchone()!=(m['writer_generation'],):raise SnapshotError('native_restore_original_writer')
                    cursor=await connection.execute('SELECT database_generation,applied_seq '
                        'FROM rkb_v4.recovery_state WHERE singleton')
                    observed_generation,restored_cursor=await cursor.fetchone()
                    if (observed_generation!=self.database.database_generation
                            or type(restored_cursor) is not int
                            or restored_cursor<m['applied_sequence']):
                        raise SnapshotError('native_restore_internal_cursor')
            await self._upgrade_restored(candidate,m,expected_versions)
            original=replace(self.database,writer_generation=m['writer_generation'])
            executor=self.executor_factory(acquire=candidate.acquire,registry=self.registry,database=original,
                run_id=self.identity[0],launch_nonce=self.identity[1],runtime_incarnation=self.identity[2])
            session=_NativeSession(candidate,m,control_bytes,binding,executor,[])
            self._sessions.add(session);self._current(binding)
            return session
        except BaseException:
            await candidate.aclose();self._owned_candidates.discard(candidate);raise

    async def _upgrade_restored(self,candidate,manifest,expected_versions):
        """Upgrade only an isolated verified candidate, before replay/READY."""
        if not compatible_versions(manifest['versions'],expected_versions):
            raise SnapshotError('native_restore_version_mismatch')
        old=manifest['versions']['pgvector_version']
        target=expected_versions['pgvector_version']
        files=getattr(self,'schema_files',None)
        if files is None:raise SnapshotError('native_restore_migration_bundle_required')
        from .kaggle_schema_migrate import migrate
        from .kaggle_owned_pg import configure_migration_defaults
        async with candidate.acquire() as connection:
            async with connection.transaction():
                # New checkpoints bind their ledger version; old Dataset2 has
                # no integer and is established by the runner's fixed baseline.
                declared=manifest['versions'].get('schema_version')
                if declared is not None:
                    actual=await (await connection.execute(
                        'SELECT max(version) FROM rkb_v4.schema_migrations')).fetchone()
                    if actual!=(declared,):raise SnapshotError('native_restore_base_schema_version')
                observed=await (await connection.execute(
                    "SELECT extversion FROM pg_extension WHERE extname='vector'")).fetchone()
                if observed!=(old,):raise SnapshotError('native_restore_base_pgvector_version')
                if old!=target:
                    # The only proven forward path; the package has to supply
                    # its native library and extension update SQL from Stage0.
                    if (old,target)!=('0.8.2','0.8.4'):
                        raise SnapshotError('native_restore_pgvector_upgrade_unsupported')
                    await connection.execute("ALTER EXTENSION vector UPDATE TO '0.8.4'")
                    observed=await (await connection.execute(
                        "SELECT extversion FROM pg_extension WHERE extname='vector'")).fetchone()
                    if observed!=(target,):raise SnapshotError('native_restore_pgvector_upgrade_unverified')
                await configure_migration_defaults(connection)
            await migrate(connection,files)
        _,fields=await self.probe(acquire=candidate.acquire)
        if (fields['schema_version']!=self.database.schema_version
                or fields['database_generation']!=self.database.database_generation
                or fields['applied_seq']<manifest['applied_sequence']):
            raise SnapshotError('native_restore_migrated_version_or_cursor')

    async def apply(self,session,frame):
        if session not in self._sessions:raise SnapshotError('native_restore_session_unknown')
        self._current(session.binding)
        receipt=await session.executor(frame)
        item=(receipt['sequence'],receipt['command_sha256'],receipt['outcome_sha256'])
        expected=session.manifest['applied_sequence']+len(session.receipts)+1
        if item[0]<expected:
            if item not in session.receipts:raise SnapshotError('native_restore_duplicate_receipt_changed')
        elif item[0]==expected:session.receipts.append(item)
        else:raise SnapshotError('native_restore_receipt_order')
        return receipt

    async def finish(self,session,*,expected_tail,final_head,control_bytes,
                     expected_schema_version=None):
        """Controller calls only after exact remote outcomes and old tail drain.

        The current authenticated control binding is the authority; supplied
        fields are compared with native locally retained receipts, not accepted
        as standalone JSON permission.
        """
        expected_tail=tuple(tuple(item) for item in expected_tail)
        expected_schema_version=self.database.schema_version if expected_schema_version is None else expected_schema_version
        if len(control_bytes)>self.limits.max_control_bytes:raise SnapshotError('native_restore_metadata_budget')
        completed=self._completed.get(session)
        if completed is not None:
            if (completed.control!=control_bytes or completed.journal_head!=final_head
                    or expected_tail!=tuple(session.receipts)
                    or json.loads(completed.control).get('schema_version')!=expected_schema_version):
                raise SnapshotError('native_restore_finish_request_changed')
            self._current(session.binding)
            object.__setattr__(completed,'_binding',session.binding)
            await self.verify_initialized(completed,self.database,self.identity)
            return completed
        if session not in self._sessions:raise SnapshotError('native_restore_session_unknown')
        self._current(session.binding)
        if tuple(expected_tail)!=tuple(session.receipts):raise SnapshotError('native_restore_tail_receipts_differ')
        final_sequence=session.manifest['applied_sequence']+len(session.receipts)
        control=json.loads(control_bytes)
        if (encode(control)!=control_bytes or control.get('database_generation')!=self.database.database_generation
                or control.get('sequence')!=final_sequence or control.get('journal_head')!=final_head
                or control.get('schema_version')!=expected_schema_version):
            raise SnapshotError('native_restore_final_control_binding')
        intent=sha(encode(dict(expected_tail=expected_tail,final_head=final_head,
            control_sha256=sha(control_bytes),schema_version=expected_schema_version)))
        prior=self._finish_intents.get(session)
        if prior is not None and prior!=intent:raise SnapshotError('native_restore_finish_request_changed')
        self._finish_intents[session]=intent
        async with session.candidate.acquire() as connection:
            cursor=await connection.execute('SELECT applied_seq,writer_generation FROM rkb_v4.recovery_state WHERE singleton')
            row=await cursor.fetchone()
            if row is None or row[0]!=final_sequence:raise SnapshotError('native_restore_final_cursor')
            if row[1]==self.database.writer_generation:
                if prior is None:raise SnapshotError('native_restore_unowned_handover')
                cursor=await connection.execute('SELECT generation,retired FROM rkb_v4.writer_generations '
                    'WHERE generation=ANY(%s::text[])',([session.manifest['writer_generation'],self.database.writer_generation],))
                if dict(await cursor.fetchall())!={session.manifest['writer_generation']:True,self.database.writer_generation:False}:
                    raise SnapshotError('native_restore_handover_state_changed')
                await connection.commit()
            elif row[1]==session.manifest['writer_generation']:
                await connection.commit()
                await handover_writer(connection,database_generation=self.database.database_generation,
                    expected_writer=session.manifest['writer_generation'],new_writer=self.database.writer_generation)
            else:raise SnapshotError('native_restore_writer_changed')
        catalog,fields=await self.probe(acquire=session.candidate.acquire)
        if (fields['schema_version']!=expected_schema_version or fields['database_generation']!=self.database.database_generation
                or fields['applied_seq']!=final_sequence
                or fields['projection_seq']!=final_sequence or not fields['text_search_ready'] or not fields['dense_search_ready']):
            print(json.dumps(dict(event='native_restore_final_observation',
                schema_version=fields['schema_version'],expected_schema_version=expected_schema_version,
                applied_seq=fields['applied_seq'])),flush=True)
            raise SnapshotError('native_restore_final_readiness')
        async with session.candidate.acquire() as connection:
            async with connection.transaction():
                await connection.execute("SET LOCAL statement_timeout='2000ms'")
                await self._sql_readiness(connection)
        self._current(session.binding)
        cap=VerifiedInitializedDatabase(self,session.candidate,self.database,self.identity,
            {'sha256':sha(encode(session.manifest))},final_head,final_sequence,control_bytes,session.binding)
        self._capabilities.add(cap);self._sessions.discard(session)
        self._completed[session]=cap
        return cap

    def reference(self,cap):
        if cap not in self._capabilities:raise SnapshotError('native_restore_capability_unknown')
        for ref,value in self._references.items():
            if value is cap:return ref
        ref=secrets.token_urlsafe(24);self._references[ref]=cap
        return ref

    async def verify_reference(self,binding,reference):
        self._current(binding)
        cap=self._references.get(reference)
        if cap is None:raise SnapshotError('native_restore_reference_unknown')
        # The same owned process may reconnect with a new control attachment.
        # A JSON token only selects an existing retained object; fresh native
        # checks below prove its database/writer/readiness before adoption.
        object.__setattr__(cap,'_binding',binding)
        await self.verify_initialized(cap,self.database,self.identity)
        return {'database':self.database.wire(),'identity':list(self.identity),
            'applied_sequence':cap.applied_sequence,'schema_version':json.loads(cap.control)['schema_version']}

    async def verify_initialized(self,cap,database,identity):
        if (type(cap) is not VerifiedInitializedDatabase or cap not in self._capabilities
                or cap.database!=database or cap.identity!=identity or cap._candidate.process.returncode is not None):
            raise SnapshotError('native_restore_capability_unknown')
        self._current(cap._binding)
        catalog,fields=await self.probe(acquire=cap._candidate.acquire)
        if (fields['schema_version']!=json.loads(cap.control)['schema_version'] or fields['database_generation']!=database.database_generation
                or fields['applied_seq']!=cap.applied_sequence
                or fields['projection_seq']!=cap.applied_sequence or not fields['text_search_ready'] or not fields['dense_search_ready']):
            raise SnapshotError('native_restore_adoption_state_changed')
        async with cap._candidate.acquire() as connection:
            cursor=await connection.execute('SELECT writer_generation FROM rkb_v4.recovery_state WHERE singleton')
            if await cursor.fetchone()!=(database.writer_generation,):raise SnapshotError('native_restore_adoption_writer_changed')
        return database.connection(reader=True)
    def _identity(self):
        uid,gid=self.native_uid,self.native_gid
        if type(uid) is not int or type(gid) is not int or min(uid,gid)<1:
            raise SnapshotError('snapshot_stage0_native_identity_required')
        if os.geteuid()==0:return dict(user=uid,group=gid,extra_groups=[])
        if os.geteuid()!=uid or os.getegid()!=gid or not set(os.getgroups())<={gid}:
            raise SnapshotError('snapshot_native_identity_mismatch')
        return {}
    def _own_workspace(self,path,*,recursive=False):
        self._identity()
        if os.geteuid()==0:
            os.chown(path,self.native_uid,self.native_gid)
            if recursive:
                for directory,dirs,files in os.walk(path,followlinks=False):
                    for name in dirs+files:os.chown(Path(directory)/name,self.native_uid,self.native_gid,follow_symlinks=False)
    def _workspace(self,path,required):
        if not self.workspace_private_current(path):
            raise SnapshotError('snapshot_stage0_private_output_exclusion_required')
        root=private_workspace(path,required,native_uid=self.native_uid,native_gid=self.native_gid);self._own_workspace(root)
        return root
    async def _run(self,*args,timeout=30):
        process=await self.spawn(*args,stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,stderr=asyncio.subprocess.DEVNULL,
            env=self.native_env,**self._identity())
        try:
            async with asyncio.timeout(timeout):
                output=bytearray()
                while chunk:=await process.stdout.read(4096):
                    output.extend(chunk)
                    if len(output)>65536:raise SnapshotError('snapshot_native_output_bound')
                if await process.wait()!=0:raise SnapshotError('snapshot_native_command_failed')
                return output.decode('utf-8').strip()
        finally:
            if process.returncode is None:process.kill();await process.wait()
    @staticmethod
    async def _sql_readiness(connection):
        cursor=await connection.execute('''WITH p AS (
            SELECT p.embedding,p.source_id,(tsvector_to_array(p.lexemes))[1] token
            FROM rkb_v4.passage_search p JOIN rkb_v4.active_revision a USING(source_id,revision)
            JOIN rkb_v4.sources s USING(source_id) WHERE s.accepted AND NOT s.withdrawn
            ORDER BY p.passage_id LIMIT 1)
            SELECT EXISTS(SELECT 1 FROM rkb_v4.lexical_candidates_scoped(coalesce(p.token,'snapshot_probe'),200,ARRAY[p.source_id])),
                EXISTS(SELECT 1 FROM rkb_v4.dense_candidates_scoped(p.embedding,200,ARRAY[p.source_id])) FROM p''')
        row=await cursor.fetchone()
        if row is not None and row!=(True,True):raise SnapshotError('snapshot_sql_dense_probe_failed')
        # Empty eligible corpus is explicit; still execute both native branches.
        if row is None:
            await connection.execute("SELECT count(*) FROM rkb_v4.lexical_candidates('snapshot_probe',1)")
            await connection.execute('SELECT count(*) FROM rkb_v4.dense_candidates('
                'array_fill(1.0::real,ARRAY[1024])::public.vector,1)')
