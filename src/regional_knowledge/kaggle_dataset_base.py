"""Read-only, version-bound Kaggle Dataset input for the EXISTING PG restore.

No provider calls, mount mutation, PG startup or credential access occur here.
The trusted launch controller must attest the exact dataset VERSION associated
with this notebook execution. The mount slug alone cannot prove its version.
The already existing NativeSnapshotFactory.prepare rehashes the archive again
before extraction, then validates PostgreSQL, pgvector and native receipts.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import hashlib
import json
import os
import re
import stat


_SHA = re.compile(r"[0-9a-f]{64}\Z")
_PART = re.compile(r"[a-z0-9][a-z0-9-]{1,98}[a-z0-9]\Z")
_NAME = re.compile(r"[a-zA-Z0-9][a-zA-Z0-9._-]{0,199}\Z")
MAX_CONTROL_BYTES = 1024 * 1024
MAX_ARCHIVE_BYTES = 8 * 1024 ** 3


class DatasetBaseUnavailable(RuntimeError):
    pass


@dataclass(frozen=True)
class DatasetFile:
    name: str
    size: int
    sha256: str

    def __post_init__(self):
        if (type(self.name) is not str or _NAME.fullmatch(self.name) is None
                or self.name in ('.', '..') or type(self.size) is not int
                or not 1 <= self.size <= MAX_ARCHIVE_BYTES or type(self.sha256) is not str
                or _SHA.fullmatch(self.sha256) is None):
            raise DatasetBaseUnavailable('dataset_file_pin_invalid')

    def wire(self):
        return dict(name=self.name,size=self.size,sha256=self.sha256)


@dataclass(frozen=True)
class DatasetBasePin:
    """Pinned separately by authorized controller and real notebook metadata."""
    owner_slug: str
    dataset_slug: str
    dataset_version: int
    notebook_version: int
    run_id: str
    database_generation: str
    base_sequence: int
    manifest: DatasetFile
    archive: DatasetFile
    control: DatasetFile

    def __post_init__(self):
        if (type(self.owner_slug) is not str or _PART.fullmatch(self.owner_slug) is None
                or type(self.dataset_slug) is not str or _PART.fullmatch(self.dataset_slug) is None
                or type(self.dataset_version) is not int or self.dataset_version < 1
                or type(self.notebook_version) is not int or self.notebook_version < 1
                or type(self.run_id) is not str or not 1 <= len(self.run_id) <= 256
                or type(self.database_generation) is not str
                or not 1 <= len(self.database_generation) <= 256
                or type(self.base_sequence) is not int or not 0 <= self.base_sequence < 2**63
                or any(type(a) is not DatasetFile for a in (self.manifest,self.archive,self.control))
                or len({self.manifest.name,self.archive.name,self.control.name}) != 3
                or self.manifest.size > MAX_CONTROL_BYTES or self.control.size > MAX_CONTROL_BYTES):
            raise DatasetBaseUnavailable('dataset_base_pin_invalid')

    @property
    def ref(self):
        return self.owner_slug + '/' + self.dataset_slug

    def wire(self):
        return dict(owner_slug=self.owner_slug,dataset_slug=self.dataset_slug,
            dataset_version=self.dataset_version,notebook_version=self.notebook_version,
            run_id=self.run_id,database_generation=self.database_generation,
            base_sequence=self.base_sequence,
            manifest=self.manifest.wire(),archive=self.archive.wire(),control=self.control.wire())

    @classmethod
    def from_wire(cls,value):
        if type(value) is not dict or set(value)!=set(cls.__dataclass_fields__):
            raise DatasetBaseUnavailable('dataset_base_pin_fields')
        inner={}
        for key in ('manifest','archive','control'):
            item=value.get(key)
            if type(item) is not dict or set(item)!=set(DatasetFile.__dataclass_fields__):
                raise DatasetBaseUnavailable('dataset_base_pin_fields')
            inner[key]=DatasetFile(**item)
        return cls(**dict(value,**inner))


@dataclass(frozen=True)
class DatasetBaseReadback:
    """Validated bytes for a subsequent NATIVE prepare, not readiness/Accepted."""
    pin: DatasetBasePin
    manifest_bytes: bytes
    control_bytes: bytes
    archive_path: Path


def _unique(pairs):
    value={}
    for name,entry in pairs:
        if name in value:raise DatasetBaseUnavailable('dataset_duplicate_metadata')
        value[name]=entry
    return value


def _read_exact(fd,file:DatasetFile,*,capture=False):
    info=os.fstat(fd)
    if (not stat.S_ISREG(info.st_mode) or info.st_nlink!=1
            or info.st_mode & 0o022 or info.st_size!=file.size):
        raise DatasetBaseUnavailable('dataset_file_identity_differs')
    digest=hashlib.sha256();used=0;parts=[]
    while used < file.size:
        chunk=os.read(fd,min(1024*1024,file.size-used))
        if not chunk:raise DatasetBaseUnavailable('dataset_file_length_differs')
        used+=len(chunk);digest.update(chunk)
        if capture:parts.append(chunk)
    if os.read(fd,1):
        raise DatasetBaseUnavailable('dataset_file_length_differs')
    if digest.hexdigest()!=file.sha256:raise DatasetBaseUnavailable('dataset_file_hash_differs')
    return b''.join(parts) if capture else None


def _canonical_json(data):
    try:
        record=json.loads(data,object_pairs_hook=_unique,parse_constant=lambda _:(_ for _ in ()).throw(ValueError('nonfinite')))
        canonical=json.dumps(record,sort_keys=True,ensure_ascii=False,separators=(',',':'),allow_nan=False).encode()
        if type(record) is not dict or canonical!=data:
            raise ValueError
        return record
    except (ValueError,TypeError):
        raise DatasetBaseUnavailable('dataset_manifest_noncanonical') from None


def verify_mounted_dataset_base(pin:DatasetBasePin, *,
                                observed_dataset_ref:str | None,
                                observed_dataset_version:int | None,
                                observed_notebook_version:int | None,
                                observed_run_id:str | None,
                                mount_root:Path=Path('/kaggle/input')) -> DatasetBaseReadback:
    """Only verify an exact mounted version; never infer it from a slug path.

    Caller must supply version observation from its authenticated notebook
    launch/attach owner. If the Kaggle provider cannot attest version-to-run,
    this refuses to route that Dataset as a recovery source.
    """
    if type(pin) is not DatasetBasePin:
        raise DatasetBaseUnavailable('dataset_base_pin_required')
    if ((observed_dataset_ref,observed_dataset_version,observed_notebook_version,observed_run_id)
            != (pin.ref,pin.dataset_version,pin.notebook_version,pin.run_id)):
        raise DatasetBaseUnavailable('dataset_version_or_run_unverified')
    mount_root=Path(mount_root)
    root=mount_root/pin.dataset_slug
    # Trust only the fixed Kaggle input location selected by launch config;
    # caller-provided root in fixture never becomes an MCP/upload path.
    if (not mount_root.is_absolute() or mount_root.resolve(strict=True)!=mount_root
            or root.resolve(strict=True)!=root or root.parent!=mount_root):
        raise DatasetBaseUnavailable('dataset_mount_untrusted')
    try:root_fd=os.open(root,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
    except OSError:raise DatasetBaseUnavailable('dataset_mount_unreadable') from None
    try:
        metadata=os.fstat(root_fd)
        if not stat.S_ISDIR(metadata.st_mode) or metadata.st_mode & 0o022:
            raise DatasetBaseUnavailable('dataset_mount_writable')
        results={}
        for kind,file in (('manifest',pin.manifest),('control',pin.control),('archive',pin.archive)):
            try:fd=os.open(file.name,os.O_RDONLY|os.O_NOFOLLOW,dir_fd=root_fd)
            except OSError:raise DatasetBaseUnavailable('dataset_file_identity_differs') from None
            try:results[kind]=_read_exact(fd,file,capture=kind!='archive')
            finally:os.close(fd)
    finally:os.close(root_fd)
    manifest=_canonical_json(results['manifest'])
    control=_canonical_json(results['control'])
    try:
        if (manifest['codec']!='rkb.pg-physical-base.v1'
                or control['codec']!='rkb.source-base-control.v1'
                or manifest['database_generation']!=pin.database_generation
                or control['database_generation']!=pin.database_generation
                or manifest['applied_sequence']!=pin.base_sequence
                or control['sequence']!=pin.base_sequence
                or manifest['source_control']['sha256']!=pin.control.sha256
                or manifest['source_control']['length']!=pin.control.size
                or control['journal_head']!=manifest['journal_head']
                or control['catalog_sha256']!=manifest['versions']['catalog_sha256']
                or manifest['blob']['size']!=pin.archive.size
                or manifest['blob']['sha256']!=pin.archive.sha256):
            raise ValueError
    except (KeyError,TypeError,ValueError):
        raise DatasetBaseUnavailable('dataset_pg_manifest_binding_differs') from None
    # The native PG recovery factory independently checks version ABI, archive
    # SHA AGAIN on use, signed original control, sequence/replay, PG system id,
    # ordered accepted outcomes, writer and final query readiness.
    return DatasetBaseReadback(pin,results['manifest'],results['control'],root/pin.archive.name)
