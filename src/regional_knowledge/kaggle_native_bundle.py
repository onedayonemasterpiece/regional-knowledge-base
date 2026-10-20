"""Pinned .deb data extraction into an immutable, fully inventoried native prefix.

Stdlib only; no package installer, maintainer scripts, compiler, provider, PG or
model operation. The retained capability certifies bytes, never user authority.
"""
from __future__ import annotations

import bz2
from dataclasses import dataclass, field
import ctypes
import errno
import hashlib
import json
import lzma
import os
from pathlib import Path, PurePosixPath
import platform
import posixpath
import shutil
import stat
import tarfile
import tempfile
import time
import weakref
import zlib

from .kaggle_stage0 import CHUNK_BYTES, _hash, _integer, _name, _parent, _verify, _remaining

TRUSTED_ROOT=Path('/kaggle/working')
OS_RELEASE=Path('/etc/os-release')
MAX_MANIFEST_BYTES=4*1024*1024
MAX_FILES=8192
MAX_PACKAGES=64
MAX_ARCHIVE_BYTES=512*1024*1024
MAX_TOTAL_BYTES=4*1024**3
MAX_FILE_BYTES=256*1024*1024
PLATFORM_FIELDS={'os_id','os_version_id','machine','libc_name','libc_version'}
_SEAL=object()
_CAPABILITIES=weakref.WeakSet()


class NativeBundleError(RuntimeError):
    pass


@dataclass(frozen=True)
class NativeBundleSpec:
    manifest_name:str
    manifest_sha256:str

    def __post_init__(self):
        _name(self.manifest_name);_hash(self.manifest_sha256)

    def wire(self):
        return dict(manifest_name=self.manifest_name,manifest_sha256=self.manifest_sha256)

    @classmethod
    def from_wire(cls,value):
        if type(value) is not dict or set(value)!={'manifest_name','manifest_sha256'}:
            raise ValueError('native_bundle_invalid_spec')
        return cls(**value)

    def prefix(self,root):
        return Path(root)/('rkb-native-'+self.manifest_sha256)


def _json_object(pairs):
    result={}
    for key,value in pairs:
        if key in result:raise ValueError('native_bundle_duplicate_field')
        result[key]=value
    return result


def _load_manifest(root_fd,name,expected_sha):
    parent=_parent(root_fd,_name(name),create=False)
    try:fd=os.open(name.split('/')[-1],os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK,dir_fd=parent)
    finally:os.close(parent)
    try:
        metadata=os.fstat(fd)
        if not stat.S_ISREG(metadata.st_mode) or not 1<=metadata.st_size<=MAX_MANIFEST_BYTES:
            raise NativeBundleError('native_bundle_manifest_invalid')
        data=b''
        while len(data)<=MAX_MANIFEST_BYTES:
            value=os.read(fd,min(CHUNK_BYTES,MAX_MANIFEST_BYTES+1-len(data)))
            if not value:break
            data+=value
        if len(data)!=metadata.st_size or hashlib.sha256(data).hexdigest()!=_hash(expected_sha):
            raise NativeBundleError('native_bundle_manifest_mismatch')
        value=json.loads(data,object_pairs_hook=_json_object,
            parse_constant=lambda _:(_ for _ in ()).throw(ValueError('nonfinite')))
        return _validate_manifest(value)
    finally:os.close(fd)


def _platform_identity():
    if OS_RELEASE.stat().st_size>65536:
        raise NativeBundleError('native_bundle_unknown_abi')
    values={}
    for line in OS_RELEASE.read_text().splitlines():
        key,separator,value=line.partition('=')
        if separator and key in ('ID','VERSION_ID'):
            values[key]=value.strip().strip('"').strip("'")
    libc_name,libc_version=platform.libc_ver()
    return dict(os_id=values.get('ID'),os_version_id=values.get('VERSION_ID'),
        machine=platform.machine(),libc_name=libc_name,libc_version=libc_version)


def _validate_manifest(value):
    if (type(value) is not dict or set(value)!={'codec','platform','packages','files','total_seconds'}
            or value['codec']!='rkb.native_bundle.v1' or type(value['platform']) is not dict
            or set(value['platform'])!=PLATFORM_FIELDS):
        raise ValueError('native_bundle_manifest_invalid')
    if any(type(part) is not str or not 1<=len(part)<=128
           or any(ord(char)<33 for char in part) for part in value['platform'].values()):
        raise ValueError('native_bundle_platform_invalid')
    if (type(value['packages']) is not list or not 1<=len(value['packages'])<=MAX_PACKAGES
            or type(value['files']) is not list or not 1<=len(value['files'])<=MAX_FILES):
        raise ValueError('native_bundle_inventory_invalid')
    _integer(value['total_seconds'],1,900)
    packages={};archives=set()
    for package in value['packages']:
        if type(package) is not dict or set(package)!={'name','archive','size','sha256','unpacked_bytes'}:
            raise ValueError('native_bundle_package_invalid')
        name=_name(package['name']);archive=_name(package['archive'])
        _integer(package['size'],1,MAX_ARCHIVE_BYTES);_hash(package['sha256'])
        _integer(package['unpacked_bytes'],1024,MAX_TOTAL_BYTES)
        if name in packages or archive in archives:
            raise ValueError('native_bundle_duplicate_package')
        packages[name]=package;archives.add(archive)
    files={};source_pins={}
    for item in value['files']:
        if type(item) is not dict or set(item)!={'path','size','sha256','package','member','executable'}:
            raise ValueError('native_bundle_file_invalid')
        path=_name(item['path']);member=_name(item['member'])
        _integer(item['size'],0,MAX_FILE_BYTES);_hash(item['sha256'])
        if type(item['executable']) is not bool or item['package'] not in packages or path in files:
            raise ValueError('native_bundle_file_invalid')
        key=(item['package'],member)
        pin=(item['size'],item['sha256'])
        if key in source_pins and source_pins[key]!=pin:
            raise ValueError('native_bundle_source_pin_conflict')
        source_pins[key]=pin;files[path]=item
    if (sum(p['size'] for p in packages.values())>MAX_TOTAL_BYTES
            or sum(p['unpacked_bytes'] for p in packages.values())>MAX_TOTAL_BYTES
            or sum(p['size'] for p in files.values())>MAX_TOTAL_BYTES):
        raise ValueError('native_bundle_byte_budget')
    paths=set(files)
    if any(str(parent) in paths for path in paths for parent in PurePosixPath(path).parents
           if str(parent)!='.'):
        raise ValueError('native_bundle_path_conflict')
    return value


class _Range:
    def __init__(self,fd,offset,size,deadline):
        self.fd,self.offset,self.size,self.position,self.deadline=fd,offset,size,0,deadline

    def read(self,size):
        _remaining(self.deadline)
        size=min(size,self.size-self.position)
        if size<=0:return b''
        data=os.pread(self.fd,size,self.offset+self.position)
        if not data:raise NativeBundleError('native_bundle_archive_truncated')
        self.position+=len(data)
        return data


def _deb_data(fd,size,deadline):
    if os.pread(fd,8,0)!=b'!<arch>\n':
        raise NativeBundleError('native_bundle_deb_invalid')
    offset=8;members={}
    while offset<size:
        _remaining(deadline)
        header=os.pread(fd,60,offset)
        if len(header)!=60 or header[58:]!=b'`\n' or len(members)>=16:
            raise NativeBundleError('native_bundle_deb_invalid')
        name=header[:16].decode('ascii').strip().removesuffix('/')
        length=int(header[48:58].decode('ascii').strip())
        start=offset+60
        if length<0 or start+length>size or name in members or '/' in name or not name:
            raise NativeBundleError('native_bundle_deb_invalid')
        members[name]=(start,length)
        offset=start+length+(length%2)
    data=[(name,position) for name,position in members.items()
          if name in ('data.tar','data.tar.xz','data.tar.gz','data.tar.bz2')]
    if (offset!=size or members.get('debian-binary') is None or members['debian-binary'][1]!=4 or len(data)!=1
            or os.pread(fd,members['debian-binary'][1],members['debian-binary'][0])!=b'2.0\n'):
        raise NativeBundleError('native_bundle_deb_invalid')
    return data[0]


class _Decoded:
    """Bound decompression allocation, total bytes and time before tar parsing."""
    def __init__(self,raw,kind,expected):
        self.raw,self.kind,self.expected=raw,kind,expected
        self.total=0;self.buffer=b'';self.tail=b'';self.done=False
        if kind=='data.tar.xz':self.decoder=lzma.LZMADecompressor(memlimit=128*1024*1024)
        elif kind=='data.tar.gz':self.decoder=zlib.decompressobj(16+zlib.MAX_WBITS)
        elif kind=='data.tar.bz2':self.decoder=bz2.BZ2Decompressor()
        else:self.decoder=None

    def read(self,size):
        if not 0<=size<=CHUNK_BYTES:
            raise NativeBundleError('native_bundle_stream_bound')
        while len(self.buffer)<size and not self.done:
            _remaining(self.raw.deadline)
            if self.decoder is None:
                output=self.raw.read(min(65536,self.expected-self.total+1))
                self.done=not output
            else:
                if self.kind=='data.tar.gz':
                    data=self.tail or self.raw.read(65536)
                    output=self.decoder.decompress(data,min(CHUNK_BYTES,self.expected-self.total+1))
                    self.tail=self.decoder.unconsumed_tail
                else:
                    data=self.raw.read(65536) if self.decoder.needs_input else b''
                    output=self.decoder.decompress(data,max_length=min(CHUNK_BYTES,self.expected-self.total+1))
                if self.decoder.eof:
                    if self.decoder.unused_data or self.raw.position!=self.raw.size:
                        raise NativeBundleError('native_bundle_archive_trailing_data')
                    self.done=True
                elif not data and not output and (self.kind=='data.tar.gz' or self.decoder.needs_input):
                    raise NativeBundleError('native_bundle_archive_truncated')
            self.total+=len(output)
            if self.total>self.expected:
                raise NativeBundleError('native_bundle_unpacked_byte_bound')
            self.buffer+=output
        data,self.buffer=self.buffer[:size],self.buffer[size:]
        if self.done and self.total!=self.expected:
            raise NativeBundleError('native_bundle_unpacked_size_mismatch')
        return data

    def finish(self):
        while self.read(65536):pass


def _tar_info(counts):
    class BoundedTarInfo(tarfile.TarInfo):
        @classmethod
        def frombuf(cls,buf,encoding,errors):
            value=super().frombuf(buf,encoding,errors)
            counts['headers']+=1
            if counts['headers']>32768 or value.size>MAX_FILE_BYTES or value.size<0:
                raise NativeBundleError('native_bundle_tar_bound')
            if value.type in (tarfile.XHDTYPE,tarfile.XGLTYPE,tarfile.GNUTYPE_LONGNAME,tarfile.GNUTYPE_LONGLINK):
                counts['metadata']+=value.size
                if value.size>65536 or counts['metadata']>8*1024*1024:
                    raise NativeBundleError('native_bundle_tar_metadata_bound')
            if value.type==tarfile.GNUTYPE_SPARSE:
                raise NativeBundleError('native_bundle_sparse_forbidden')
            return value
        def _proc_gnusparse_00(self,*args):raise NativeBundleError('native_bundle_sparse_forbidden')
        def _proc_gnusparse_01(self,*args):raise NativeBundleError('native_bundle_sparse_forbidden')
        def _proc_gnusparse_10(self,*args):raise NativeBundleError('native_bundle_sparse_forbidden')
    return BoundedTarInfo


def _member_name(value):
    if value in ('.','./'):return None
    if value.startswith('./'):value=value[2:]
    return _name(value.removesuffix('/'))


def _write_member(stage_fd,item,source,deadline):
    parent=_parent(stage_fd,item['path'],create=True)
    try:
        fd=os.open(item['path'].split('/')[-1],os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,
                   0o600,dir_fd=parent)
        try:
            size=0;digest=hashlib.sha256()
            while True:
                _remaining(deadline)
                data=source.read(min(65536,item['size']-size+1))
                if not data:break
                size+=len(data)
                if size>item['size']:raise NativeBundleError('native_bundle_file_size_mismatch')
                digest.update(data)
                view=memoryview(data)
                while view:view=view[os.write(fd,view):]
            if size!=item['size'] or digest.hexdigest()!=item['sha256']:
                raise NativeBundleError('native_bundle_file_hash_mismatch')
            os.fchmod(fd,0o500 if item['executable'] else 0o400);os.fsync(fd)
        finally:os.close(fd)
    finally:os.close(parent)


def _copy_member(stage_fd,source_path,item,deadline):
    parent=_parent(stage_fd,source_path,create=False)
    try:fd=os.open(source_path.split('/')[-1],os.O_RDONLY|os.O_NOFOLLOW,dir_fd=parent)
    finally:os.close(parent)
    with os.fdopen(fd,'rb') as source:_write_member(stage_fd,item,source,deadline)


def _extract_package(root_fd,stage_fd,package,files,deadline,counts):
    selected={}
    for item in files:
        if item['package']==package['name']:selected.setdefault(item['member'],[]).append(item)
    parent=_parent(root_fd,package['archive'],create=False)
    try:
        if not _verify(parent,package['archive'].split('/')[-1],package['size'],package['sha256'],deadline):
            raise NativeBundleError('native_bundle_package_missing')
        fd=os.open(package['archive'].split('/')[-1],os.O_RDONLY|os.O_NOFOLLOW,dir_fd=parent)
    finally:os.close(parent)
    try:
        kind,(offset,size)=_deb_data(fd,package['size'],deadline)
        stream=_Decoded(_Range(fd,offset,size,deadline),kind,package['unpacked_bytes'])
        members={};materialized={};seen=set()
        with tarfile.open(fileobj=stream,mode='r|',tarinfo=_tar_info(counts),encoding='utf-8',errors='strict') as archive:
            for member in archive:
                _remaining(deadline)
                name=_member_name(member.name)
                if name is None:continue
                if name in seen or member.size>MAX_FILE_BYTES or member.size<0 or member.sparse is not None:
                    raise NativeBundleError('native_bundle_tar_member_invalid')
                if member.isdir():kind='directory'
                elif member.isfile():kind='file'
                elif member.issym() or member.islnk():kind='link'
                else:raise NativeBundleError('native_bundle_special_file_forbidden')
                seen.add(name)
                target=None
                if kind=='link' and name in selected:
                    if member.linkname.startswith('/') or '\\' in member.linkname or not member.linkname:
                        raise NativeBundleError('native_bundle_link_escape')
                    target=posixpath.normpath(posixpath.join(posixpath.dirname(name),member.linkname)
                        if member.issym() else member.linkname)
                    target=_name(target)
                if name not in selected:continue
                members[name]=(kind,target)
                if kind=='file':
                    first=selected[name][0]
                    if member.size!=first['size']:
                        raise NativeBundleError('native_bundle_file_size_mismatch')
                    with archive.extractfile(member) as source:_write_member(stage_fd,first,source,deadline)
                    materialized[name]=first['path']
                    for item in selected[name][1:]:_copy_member(stage_fd,first['path'],item,deadline)
                elif kind!='link':raise NativeBundleError('native_bundle_selected_file_invalid')
        stream.finish()
        return members,materialized
    finally:os.close(fd)


def _materialize_links(stage_fd,manifest,records,materialized,deadline):
    selected={}
    for item in manifest['files']:
        selected.setdefault((item['package'],item['member']),[]).append(item)
    for key,items in selected.items():
        if key in materialized:continue
        target=key;visited=set()
        while target in records and records[target][0]=='link':
            if target in visited or len(visited)>=16:
                raise NativeBundleError('native_bundle_link_cycle')
            visited.add(target)
            path=records[target][1]
            same=(target[0],path)
            candidates=[same] if same in records else [candidate for candidate in records if candidate[1]==path]
            if len(candidates)!=1:
                raise NativeBundleError('native_bundle_link_target_unpinned')
            target=candidates[0]
        if target not in materialized:
            raise NativeBundleError('native_bundle_link_target_unpinned')
        for item in items:_copy_member(stage_fd,materialized[target],item,deadline)
        materialized[key]=items[0]['path']


def _inventory_records(inventory):
    if type(inventory) not in (tuple,list) or not 1<=len(inventory)<=128:
        raise ValueError('native_bundle_top_inventory_required')
    records={}
    for item in inventory:
        if type(item) is not dict or set(item)!={'name','size','sha256','role'}:
            raise ValueError('native_bundle_top_inventory_invalid')
        name=_name(item['name']);_integer(item['size'],0,16*1024**3);_hash(item['sha256'])
        if name in records:raise ValueError('native_bundle_top_inventory_duplicate')
        records[name]=(name,item['size'],item['sha256'],item['role'])
    return tuple(sorted(records.values()))


def _bind_inventory(root_fd,manifest,spec,inventory,deadline):
    records={item[0]:item for item in inventory}
    expected=[(spec.manifest_name,None,spec.manifest_sha256)]
    expected.extend((item['archive'],item['size'],item['sha256']) for item in manifest['packages'])
    for name,size,sha in expected:
        item=records.get(name)
        if item is None or item[3]!='code' or item[2]!=sha or size is not None and item[1]!=size:
            raise NativeBundleError('native_bundle_top_inventory_mismatch')
        parent=_parent(root_fd,name,create=False)
        try:
            if not _verify(parent,name.split('/')[-1],item[1],item[2],deadline):
                raise NativeBundleError('native_bundle_package_missing')
        finally:os.close(parent)


def _verify_prefix(root_fd,prefix,manifest,deadline):
    directory=os.open(prefix,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=root_fd)
    try:
        expected={item['path'] for item in manifest['files']}
        directories={str(parent) for name in expected for parent in PurePosixPath(name).parents if str(parent)!='.'}
        observed=set();stack=[(os.dup(directory),'')];visited=0
        try:
            while stack:
                fd,relative=stack.pop()
                try:
                    for entry in os.scandir(fd):
                        _remaining(deadline);visited+=1
                        path=relative+'/'+entry.name if relative else entry.name
                        if visited>MAX_FILES*17:raise NativeBundleError('native_bundle_tree_bound')
                        if entry.is_dir(follow_symlinks=False) and path in directories:
                            stack.append((os.open(entry.name,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=fd),path))
                        elif entry.is_file(follow_symlinks=False) and path in expected:
                            observed.add(path)
                        else:raise NativeBundleError('native_bundle_unexpected_path')
                finally:os.close(fd)
        finally:
            for fd,_ in stack:os.close(fd)
        if observed!=expected:raise NativeBundleError('native_bundle_file_missing')
        for item in manifest['files']:
            parent=_parent(directory,item['path'],create=False)
            try:
                if not _verify(parent,item['path'].split('/')[-1],item['size'],item['sha256'],deadline,
                               executable=item['executable']):
                    raise NativeBundleError('native_bundle_file_missing')
            finally:os.close(parent)
    finally:os.close(directory)


def _publish(root_fd,temporary,prefix):
    # Linux atomic directory publication without replacing even an empty path.
    libc=ctypes.CDLL(None,use_errno=True)
    operation=getattr(libc,'renameat2',None)
    if operation is None:raise NativeBundleError('native_bundle_atomic_publish_unsupported')
    operation.argtypes=[ctypes.c_int,ctypes.c_char_p,ctypes.c_int,ctypes.c_char_p,ctypes.c_uint]
    operation.restype=ctypes.c_int
    if operation(root_fd,os.fsencode(temporary),root_fd,os.fsencode(prefix),1)!=0:
        number=ctypes.get_errno()
        if number==errno.EEXIST:return False
        raise NativeBundleError('native_bundle_atomic_publish_failed')
    os.fsync(root_fd)
    return True


@dataclass(frozen=True,eq=False)
class VerifiedNativeBundle:
    root:Path
    prefix:Path
    manifest_name:str
    manifest_sha256:str
    pins:tuple[tuple[str,int,str,bool],...]
    _inventory:tuple=field(repr=False)
    _seal:object=field(repr=False)

    def __post_init__(self):
        if self._seal is not _SEAL:raise TypeError('retained native bundle verification required')
        _CAPABILITIES.add(self)

    def current(self):
        if self not in _CAPABILITIES:return False
        try:
            deadline=time.monotonic()+120
            root_fd=_open_root(self.root)
            try:
                manifest=_load_manifest(root_fd,self.manifest_name,self.manifest_sha256)
                if self.prefix!=NativeBundleSpec(self.manifest_name,self.manifest_sha256).prefix(self.root):return False
                if self.pins!=_pins(self.prefix,manifest):return False
                _bind_inventory(root_fd,manifest,NativeBundleSpec(self.manifest_name,self.manifest_sha256),self._inventory,deadline)
                if manifest['platform']!=_platform_identity():return False
                _verify_prefix(root_fd,self.prefix.name,manifest,deadline)
                return True
            finally:os.close(root_fd)
        except Exception:return False


def _pins(prefix,manifest):
    return tuple((str(prefix/item['path']),item['size'],item['sha256'],item['executable'])
                 for item in sorted(manifest['files'],key=lambda item:item['path']))


def _open_root(root):
    root=Path(root)
    if root!=TRUSTED_ROOT or root.resolve(strict=True)!=root or not root.is_dir():
        raise NativeBundleError('native_bundle_root_invalid')
    return os.open(root,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)


def prepare_native_bundle(root,spec_manifest_path,expected_sha,identity=None,*,inventory)->VerifiedNativeBundle:
    """Extract or fully reverify one pinned prefix; unknown ABI has no fallback.

    identity, when supplied, is an expected exact OS/ABI dictionary, never a
    replacement for observing this process's actual platform independently.
    """
    temporary=None;root_fd=None
    try:
        spec=NativeBundleSpec(spec_manifest_path,expected_sha)
        root=Path(root);root_fd=_open_root(root)
        manifest=_load_manifest(root_fd,spec.manifest_name,spec.manifest_sha256)
        inventory=_inventory_records(inventory)
        observed=_platform_identity()
        if manifest['platform']!=observed or identity is not None and identity!=observed:
            raise NativeBundleError('native_bundle_abi_mismatch')
        deadline=time.monotonic()+manifest['total_seconds']
        prefix=spec.prefix(root)
        _bind_inventory(root_fd,manifest,spec,inventory,deadline)
        try:os.stat(prefix.name,dir_fd=root_fd,follow_symlinks=False)
        except FileNotFoundError:exists=False
        else:exists=True
        if not exists:
            required=sum(item['size'] for item in manifest['files'])+CHUNK_BYTES
            if shutil.disk_usage(root).free<required:
                raise NativeBundleError('native_bundle_insufficient_disk')
            temporary=Path(tempfile.mkdtemp(prefix='.rkb-native-part-',dir=root))
            stage_fd=os.open(temporary,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
            try:
                records={};materialized={};counts={'headers':0,'metadata':0}
                for package in manifest['packages']:
                    members,ready=_extract_package(root_fd,stage_fd,package,manifest['files'],deadline,counts)
                    records.update({(package['name'],name):value for name,value in members.items()})
                    materialized.update({(package['name'],name):value for name,value in ready.items()})
                _materialize_links(stage_fd,manifest,records,materialized,deadline)
            finally:os.close(stage_fd)
            _verify_prefix(root_fd,temporary.name,manifest,deadline)
            if _publish(root_fd,temporary.name,prefix.name):temporary=None
        _verify_prefix(root_fd,prefix.name,manifest,deadline)
        return VerifiedNativeBundle(root,prefix,spec.manifest_name,spec.manifest_sha256,_pins(prefix,manifest),inventory,_SEAL)
    except NativeBundleError:raise
    except Exception:
        raise NativeBundleError('native_bundle_preparation_failed') from None
    finally:
        if root_fd is not None:os.close(root_fd)
        if temporary is not None:shutil.rmtree(temporary)
