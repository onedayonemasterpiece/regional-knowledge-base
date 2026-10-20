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
import sys
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
MAX_PACKAGES=96
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
    if type(value) is not dict:raise ValueError('native_bundle_manifest_invalid')
    expected={'codec','platform','packages','files','total_seconds'}
    if value.get('codec')=='rkb.native_bundle.v2':expected.add('collection')
    if (set(value)!=expected or value['codec'] not in ('rkb.native_bundle.v1','rkb.native_bundle.v2')
            or type(value['platform']) is not dict
            or set(value['platform'])!=PLATFORM_FIELDS):
        raise ValueError('native_bundle_manifest_invalid')
    if any(type(part) is not str or not 1<=len(part)<=128
           or any(ord(char)<33 for char in part) for part in value['platform'].values()):
        raise ValueError('native_bundle_platform_invalid')
    if (type(value['packages']) is not list or not 1<=len(value['packages'])<=MAX_PACKAGES
            or type(value['files']) is not list or not 1<=len(value['files'])<=MAX_FILES):
        raise ValueError('native_bundle_inventory_invalid')
    _integer(value['total_seconds'],1,900)
    if 'collection' in value:
        collection=value['collection']
        if type(collection) is not dict or set(collection)!={'archive','size','sha256'}:
            raise ValueError('native_bundle_collection_invalid')
        _name(collection['archive']);_integer(collection['size'],1,MAX_ARCHIVE_BYTES);_hash(collection['sha256'])
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
          if name in ('data.tar','data.tar.xz','data.tar.gz','data.tar.bz2','data.tar.zst')]
    if (offset!=size or members.get('debian-binary') is None or members['debian-binary'][1]!=4 or len(data)!=1
            or os.pread(fd,members['debian-binary'][1],members['debian-binary'][0])!=b'2.0\n'):
        raise NativeBundleError('native_bundle_deb_invalid')
    return data[0]


def _zstd_reader(raw):
    # Dev/CI may derive manifests with Python 3.14's bounded standard decoder.
    # Kaggle Python 3.12 requires the explicitly pinned Stage0 wheel; no ambient
    # command/libzstd probing or package installation fallback exists here.
    if sys.version_info>=(3,14):
        from compression import zstd
        return zstd.ZstdFile(raw,'rb',options={zstd.DecompressionParameter.window_log_max:27})
    import importlib.metadata
    if importlib.metadata.version('zstandard')!='0.25.0':
        raise NativeBundleError('native_bundle_zstd_decoder_unverified')
    import zstandard
    # The 0.25.0 C extension passes this value directly to zstd in bytes
    # (its Python documentation incorrectly calls the unit KiB).
    return zstandard.ZstdDecompressor(max_window_size=128*1024*1024).stream_reader(
        raw,read_size=65536,read_across_frames=True,closefd=False)


class _Decoded:
    """Bound decompression allocation, total bytes and time before tar parsing."""
    def __init__(self,raw,kind,expected):
        self.raw,self.kind,self.expected=raw,kind,MAX_TOTAL_BYTES if expected is None else expected
        self.exact=expected is not None
        self.zstd_stream=_zstd_reader(raw) if kind=='data.tar.zst' else None
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
            if self.zstd_stream is not None:
                output=self.zstd_stream.read(min(65536,self.expected-self.total+1))
                self.done=not output
                if self.done and self.raw.position!=self.raw.size:
                    raise NativeBundleError('native_bundle_archive_trailing_data')
            elif self.decoder is None:
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
        if self.done and self.exact and self.total!=self.expected:
            raise NativeBundleError('native_bundle_unpacked_size_mismatch')
        return data

    def finish(self):
        try:
            while self.read(65536):pass
        finally:
            if self.zstd_stream is not None:self.zstd_stream.close()


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
                    if '\\' in member.linkname or not member.linkname:
                        raise NativeBundleError('native_bundle_link_escape')
                    target=posixpath.normpath(member.linkname.lstrip('/') if member.linkname.startswith('/') else
                        posixpath.join(posixpath.dirname(name),member.linkname) if member.issym() else member.linkname)
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
    sources=[manifest['collection']] if 'collection' in manifest else manifest['packages']
    expected.extend((item['archive'],item['size'],item['sha256']) for item in sources)
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


def _prepare_collection(root_fd,root,manifest,deadline):
    collection=manifest['collection']
    prefix='rkb-native-archives-'+collection['sha256']
    files=[dict(path=package['archive'],size=package['size'],sha256=package['sha256'],executable=False)
           for package in manifest['packages']]
    expected={item['path']:item for item in files}
    tree=dict(files=files)
    try:os.stat(prefix,dir_fd=root_fd,follow_symlinks=False)
    except FileNotFoundError:exists=False
    else:exists=True
    if exists:
        _verify_prefix(root_fd,prefix,tree,deadline)
        return os.open(prefix,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=root_fd)
    needed=sum(item['size'] for item in files)+sum(item['size'] for item in manifest['files'])+CHUNK_BYTES
    if shutil.disk_usage(root).free<needed:
        raise NativeBundleError('native_bundle_insufficient_disk')
    temporary=Path(tempfile.mkdtemp(prefix='.rkb-native-archives-part-',dir=root))
    stage_fd=os.open(temporary,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
    try:
        parent=_parent(root_fd,collection['archive'],create=False)
        try:fd=os.open(collection['archive'].split('/')[-1],os.O_RDONLY|os.O_NOFOLLOW,dir_fd=parent)
        finally:os.close(parent)
        seen=set();directories={str(p) for name in expected for p in PurePosixPath(name).parents if str(p)!='.'}
        with os.fdopen(fd,'rb') as source:
            with tarfile.open(fileobj=source,mode='r|',tarinfo=_tar_info({'headers':0,'metadata':0}),
                              encoding='utf-8',errors='strict') as archive:
                for member in archive:
                    _remaining(deadline)
                    name=_member_name(member.name)
                    if name is None:continue
                    if member.isdir() and name in directories:continue
                    if (name in seen or name not in expected or not member.isfile() or member.sparse is not None
                            or member.size!=expected[name]['size']):
                        raise NativeBundleError('native_bundle_collection_member_invalid')
                    seen.add(name)
                    with archive.extractfile(member) as content:_write_member(stage_fd,expected[name],content,deadline)
        if seen!=set(expected):raise NativeBundleError('native_bundle_collection_member_missing')
        _verify_prefix(root_fd,temporary.name,tree,deadline)
        if _publish(root_fd,temporary.name,prefix):temporary=None
        _verify_prefix(root_fd,prefix,tree,deadline)
        return os.open(prefix,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=root_fd)
    finally:
        os.close(stage_fd)
        if temporary is not None:shutil.rmtree(temporary)


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
    temporary=None;root_fd=None;package_fd=None
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
        package_fd=_prepare_collection(root_fd,root,manifest,deadline) if 'collection' in manifest else os.dup(root_fd)
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
                    members,ready=_extract_package(package_fd,stage_fd,package,manifest['files'],deadline,counts)
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
        if package_fd is not None:os.close(package_fd)
        if root_fd is not None:os.close(root_fd)
        if temporary is not None:shutil.rmtree(temporary)


def build_native_manifest(root,packages,*,identity,include_prefixes,total_seconds=120):
    """Inspect local pinned .debs into exact nested pins, without installing them.

    Intended for managed public-artifact preparation on Dev or CI. The caller
    supplies a complete same-release package closure and explicit path prefixes.
    This function neither resolves missing packages nor executes native binaries;
    its output is a byte manifest, not a claim of ELF/PG/ABI readiness.
    """
    root_fd=None
    try:
        root=Path(root)
        if not root.is_absolute() or root.resolve(strict=True)!=root or not root.is_dir():
            raise ValueError('native_bundle_builder_root_invalid')
        if (type(packages) not in (tuple,list) or not 1<=len(packages)<=MAX_PACKAGES
                or type(identity) is not dict or set(identity)!=PLATFORM_FIELDS
                or type(include_prefixes) is not tuple or not 1<=len(include_prefixes)<=64):
            raise ValueError('native_bundle_builder_plan_invalid')
        _integer(total_seconds,1,900)
        prefixes=tuple(_name(prefix) for prefix in include_prefixes)
        deadline=time.monotonic()+total_seconds
        root_fd=os.open(root,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
        names=set();archives=set();records={};regular={};selected={};result_packages=[]
        counts={'headers':0,'metadata':0};total_unpacked=0
        for package in packages:
            if type(package) is not dict or set(package)!={'name','archive','size','sha256'}:
                raise ValueError('native_bundle_builder_package_invalid')
            name=_name(package['name']);path=_name(package['archive'])
            _integer(package['size'],1,MAX_ARCHIVE_BYTES);_hash(package['sha256'])
            if name in names or path in archives:
                raise ValueError('native_bundle_builder_duplicate_package')
            names.add(name);archives.add(path)
            parent=_parent(root_fd,path,create=False)
            try:
                if not _verify(parent,path.split('/')[-1],package['size'],package['sha256'],deadline):
                    raise NativeBundleError('native_bundle_package_missing')
                fd=os.open(path.split('/')[-1],os.O_RDONLY|os.O_NOFOLLOW,dir_fd=parent)
            finally:os.close(parent)
            try:
                kind,(offset,size)=_deb_data(fd,package['size'],deadline)
                stream=_Decoded(_Range(fd,offset,size,deadline),kind,None)
                seen=set()
                with tarfile.open(fileobj=stream,mode='r|',tarinfo=_tar_info(counts),encoding='utf-8',errors='strict') as archive:
                    for member in archive:
                        _remaining(deadline)
                        member_name=_member_name(member.name)
                        if member_name is None:continue
                        if member_name in seen or member.size>MAX_FILE_BYTES or member.size<0 or member.sparse is not None:
                            raise NativeBundleError('native_bundle_tar_member_invalid')
                        seen.add(member_name)
                        if member.isdir():continue
                        if not (member.isfile() or member.issym() or member.islnk()):
                            raise NativeBundleError('native_bundle_special_file_forbidden')
                        if not any(member_name==prefix or member_name.startswith(prefix+'/') for prefix in prefixes):
                            continue
                        key=(name,member_name)
                        selected[key]=dict(path=member_name,package=name,member=member_name,
                                           executable=bool(member.mode&0o111))
                        if len(selected)>MAX_FILES:raise NativeBundleError('native_bundle_file_bound')
                        if member.isfile():
                            digest=hashlib.sha256();count=0
                            with archive.extractfile(member) as source:
                                while True:
                                    _remaining(deadline)
                                    data=source.read(65536)
                                    if not data:break
                                    count+=len(data);digest.update(data)
                                    if count>MAX_FILE_BYTES:raise NativeBundleError('native_bundle_file_bound')
                            if count!=member.size:raise NativeBundleError('native_bundle_file_size_mismatch')
                            regular[key]=(count,digest.hexdigest())
                            records[key]=('file',None)
                        else:
                            if '\\' in member.linkname or not member.linkname:
                                raise NativeBundleError('native_bundle_link_escape')
                            target=posixpath.normpath(member.linkname.lstrip('/') if member.linkname.startswith('/') else
                                posixpath.join(posixpath.dirname(member_name),member.linkname) if member.issym() else member.linkname)
                            records[key]=('link',_name(target))
                stream.finish()
                total_unpacked+=stream.total
                if total_unpacked>MAX_TOTAL_BYTES:raise NativeBundleError('native_bundle_byte_budget')
                result_packages.append(dict(package,unpacked_bytes=stream.total))
            finally:os.close(fd)
        output={};directory_aliases=[]
        for key,item in selected.items():
            target=key;visited=set()
            while target in records and records[target][0]=='link':
                if target in visited or len(visited)>=16:raise NativeBundleError('native_bundle_link_cycle')
                visited.add(target);path=records[target][1]
                same=(target[0],path)
                candidates=[same] if same in records else [candidate for candidate in records if candidate[1]==path]
                if not candidates:
                    directory_aliases.append((key,item,path))
                    target=None
                    break
                if len(candidates)!=1:
                    error=NativeBundleError('native_bundle_link_target_unpinned')
                    error.context=(key,path,tuple(candidates))
                    raise error
                target=candidates[0]
            if target is None:continue
            if target not in regular:
                error=NativeBundleError('native_bundle_link_target_unpinned')
                error.context=(key,target)
                raise error
            size,digest=regular[target]
            complete=dict(item,size=size,sha256=digest)
            existing=output.get(item['path'])
            if existing and (existing['size'],existing['sha256'],existing['executable'])!=(size,digest,item['executable']):
                raise NativeBundleError('native_bundle_package_path_conflict')
            output.setdefault(item['path'],complete)
        # A directory alias is expanded only into copies of already pinned
        # regular leaves. It never creates a filesystem symlink or reads a host
        # path; the resulting ordinary file records bind every copied byte.
        base=dict(output)
        aliases={item['path']:(key,item,path) for key,item,path in directory_aliases}
        for key,item,path in directory_aliases:
            target=path;visited={item['path']}
            while target in aliases:
                if target in visited or len(visited)>=16:
                    raise NativeBundleError('native_bundle_link_cycle')
                visited.add(target);target=aliases[target][2]
            children=[(name,value) for name,value in base.items() if name.startswith(target+'/')]
            if not children:
                error=NativeBundleError('native_bundle_link_target_unpinned');error.context=(key,target)
                raise error
            for name,value in children:
                destination=item['path']+name[len(target):]
                _name(destination)
                copy=dict(value,path=destination)
                previous=output.get(destination)
                if previous and (previous['size'],previous['sha256'])!=(copy['size'],copy['sha256']):
                    raise NativeBundleError('native_bundle_package_path_conflict')
                output.setdefault(destination,copy)
                if len(output)>MAX_FILES:raise NativeBundleError('native_bundle_file_bound')
        manifest=dict(codec='rkb.native_bundle.v1',platform=dict(identity),packages=result_packages,
                      files=[output[path] for path in sorted(output)],total_seconds=total_seconds)
        return _validate_manifest(manifest)
    except NativeBundleError:raise
    except Exception:
        raise NativeBundleError('native_bundle_builder_failed') from None
    finally:
        if root_fd is not None:os.close(root_fd)


def assemble_native_collection(root,packages,*,identity,include_prefixes,
                               archive_name='native-packages.tar',manifest_name='native-manifest.json',total_seconds=300):
    """Build deterministic public launch artifacts from already pinned local .debs.

    No package/network/native execution occurs. Outputs are two ordinary files:
    one exact outer tar and its nested manifest, suitable as top code artifacts.
    Existing mismatched outputs are preserved and rejected.
    """
    root=Path(root);root_fd=None;temporary=None
    try:
        _name(archive_name);_name(manifest_name)
        if not root.is_absolute() or root.resolve(strict=True)!=root:
            raise ValueError('native_bundle_builder_root_invalid')
        deadline=time.monotonic()+total_seconds
        manifest=build_native_manifest(root,packages,identity=identity,include_prefixes=include_prefixes,
                                       total_seconds=total_seconds)
        _remaining(deadline)
        # tar padding + headers and a bounded manifest are included in headroom.
        estimate=sum(item['size']+1024 for item in packages)+10240+MAX_MANIFEST_BYTES
        if shutil.disk_usage(root).free<estimate:
            raise NativeBundleError('native_bundle_insufficient_disk')
        root_fd=os.open(root,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
        temporary=Path(tempfile.mkdtemp(prefix='.rkb-native-build-',dir=root))
        output=temporary/'collection.tar'
        with output.open('xb') as stream:
            with tarfile.open(fileobj=stream,mode='w|',format=tarfile.USTAR_FORMAT) as archive:
                for package in sorted(packages,key=lambda item:item['archive']):
                    _remaining(deadline)
                    parent=_parent(root_fd,package['archive'],create=False)
                    try:
                        if not _verify(parent,package['archive'].split('/')[-1],package['size'],package['sha256'],deadline):
                            raise NativeBundleError('native_bundle_package_missing')
                        fd=os.open(package['archive'].split('/')[-1],os.O_RDONLY|os.O_NOFOLLOW,dir_fd=parent)
                    finally:os.close(parent)
                    info=tarfile.TarInfo(package['archive']);info.size=package['size'];info.mode=0o400
                    with os.fdopen(fd,'rb') as content:archive.addfile(info,content)
            stream.flush();os.fsync(stream.fileno())
        digest=hashlib.sha256()
        with output.open('rb') as stream:
            while data:=stream.read(CHUNK_BYTES):_remaining(deadline);digest.update(data)
        collection=dict(archive=archive_name,size=output.stat().st_size,sha256=digest.hexdigest())
        manifest.update(codec='rkb.native_bundle.v2',collection=collection)
        _validate_manifest(manifest)
        data=json.dumps(manifest,sort_keys=True,separators=(',',':'),ensure_ascii=True).encode()
        if len(data)>MAX_MANIFEST_BYTES:raise NativeBundleError('native_bundle_manifest_byte_bound')
        manifest_output=temporary/'manifest.json';manifest_output.write_bytes(data)
        for source,name,size,sha in [(output,archive_name,collection['size'],collection['sha256']),
                                    (manifest_output,manifest_name,len(data),hashlib.sha256(data).hexdigest())]:
            parent=_parent(root_fd,name,create=True)
            try:
                if not _verify(parent,name.split('/')[-1],size,sha,deadline):
                    source.chmod(0o400)
                    try:os.link(source,name.split('/')[-1],dst_dir_fd=parent,follow_symlinks=False)
                    except FileExistsError:
                        if not _verify(parent,name.split('/')[-1],size,sha,deadline):raise
                    os.fsync(parent)
            finally:os.close(parent)
        return dict(codec='rkb.native_collection_artifacts.v1',
            spec=NativeBundleSpec(manifest_name,hashlib.sha256(data).hexdigest()).wire(),
            artifacts=[dict(name=archive_name,size=collection['size'],sha256=collection['sha256'],role='code'),
                       dict(name=manifest_name,size=len(data),sha256=hashlib.sha256(data).hexdigest(),role='code')],
            package_count=len(packages),file_count=len(manifest['files']),
            extracted_bytes=sum(item['size'] for item in manifest['files']),platform=dict(identity),
            target_abi_observed=False,native_execution_verified=False)
    except NativeBundleError:raise
    except Exception:
        raise NativeBundleError('native_bundle_assembly_failed') from None
    finally:
        if root_fd is not None:os.close(root_fd)
        if temporary is not None:shutil.rmtree(temporary)
