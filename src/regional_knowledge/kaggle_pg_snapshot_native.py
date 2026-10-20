"""Native-only private physical PG backup helpers; no controller or provider IAM.

Explicit operations only. No effect occurs on import. Stage0 must prove native
binary/dependency revisions and private writable paths before any invocation.
"""
from __future__ import annotations
import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass, asdict
import hashlib
import json
import math
import os
from pathlib import Path, PurePosixPath
import shutil
import signal
import stat
import tarfile
import psycopg


class SnapshotError(RuntimeError):
    pass


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
    required={'server_version_num','pgvector_version','catalog_sha256','model_sha256','tokenizer_sha256',
        'schema_revision','function_revision','executor_revision'}
    if not isinstance(versions,dict) or set(versions)!=required:
        raise SnapshotError('snapshot_exact_runtime_versions_required')
    if (type(versions['server_version_num']) is not int or versions['server_version_num']//10000!=17
            or versions['pgvector_version']!='0.8.2'
            or any(not isinstance(versions[k],str) or not 1<=len(versions[k].encode())<=256
                for k in ('schema_revision','function_revision','executor_revision'))):
        raise SnapshotError('snapshot_native_version_mismatch')
    for k in ('catalog_sha256','model_sha256','tokenizer_sha256'):_hash(versions[k])


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
            if await asyncio.to_thread(digest_file,binary.path,binary.size)!=(binary.size,binary.sha256):
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


def private_workspace(path,required_bytes):
    root=Path(path)
    if (not root.is_absolute() or not root.is_relative_to('/kaggle/working')
            or root==Path('/kaggle/working') or '..' in root.parts
            or root.parent.resolve(strict=True)!=root.parent):
        raise SnapshotError('snapshot_private_workspace_required')
    if shutil.disk_usage(root.parent).free<required_bytes:raise SnapshotError('snapshot_disk_budget')
    root.mkdir(mode=0o700) # exclusive; existing/partial/live directories are refused
    return root


def inventory_tree(root,limits):
    records=[];used=0
    for directory,dirs,files in os.walk(root,followlinks=False):
        for name in sorted(dirs+files):
            path=Path(directory)/name;info=path.lstat();relative=path.relative_to(root).as_posix()
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
    with open(path,'xb') as destination:
        os.chmod(path,0o600)
        with tarfile.open(fileobj=destination,mode='w|') as archive:
            for relative,_,_ in records:archive.add(root/relative,arcname=relative,recursive=False)
        destination.flush();os.fsync(destination.fileno())
    size,archive_hash=digest_file(path,limits.max_archive_bytes)
    return records,used,digest,size,archive_hash


def unpack_tree(path,destination,manifest,limits):
    destination.mkdir(mode=0o700)
    names=set();used=0
    with tarfile.open(path,mode='r|') as archive:
        for member in archive:
            name=member.name;parts=PurePosixPath(name).parts
            if (not parts or PurePosixPath(name).is_absolute() or any(p in ('..','.') for p in parts)
                    or '\\' in name or name in names or len(names)>=limits.max_files
                    or not (member.isdir() or member.isfile()) or member.size<0):
                raise SnapshotError('snapshot_unsafe_archive_member')
            names.add(name);target=destination.joinpath(*parts)
            target.parent.mkdir(mode=0o700,parents=True,exist_ok=True)
            if member.isdir():target.mkdir(mode=0o700,exist_ok=True);continue
            used+=member.size
            if used>limits.max_unpacked_bytes:raise SnapshotError('snapshot_unpacked_byte_budget')
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
            'FROM rkb_v4.applied_ops ORDER BY sequence LIMIT %s',(limits.max_receipts+1,))
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

