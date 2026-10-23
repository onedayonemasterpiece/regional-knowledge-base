"""Controller-pinned preparation after HELLO, before optional runtime imports.

Stdlib only. This is neither a package resolver nor a general downloader: every
byte must be in the final prepared artifact inventory, under Kaggle working.
"""
from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Mapping
from contextlib import contextmanager
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path, PurePosixPath
import re
import secrets
import shutil
import stat
import subprocess
import sys
import time
from types import MappingProxyType
from urllib.parse import urlsplit, urljoin
import urllib.request

TRUSTED_ROOT = Path('/kaggle/working')
TRUSTED_INPUT_ROOT = Path('/kaggle/input')
MAX_FILES = 128
# Native import source plus PDF/Telegram dependencies requires165 entries.
# Download and wheel budgets remain128; only the complete prepared inventory
# includes the additional immutable generated source files.
MAX_PREPARED_FILES = 192
MAX_FILE_BYTES = 16 * 1024**3
MAX_TOTAL_BYTES = 64 * 1024**3
CHUNK_BYTES = 1024 * 1024


class Stage0Error(RuntimeError):
    """Errors are fixed codes; never expose a URL or installer/provider output."""


def _integer(value, minimum, maximum):
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError('stage0_invalid_bound')
    return value


def _name(value):
    if (type(value) is not str or not 1 <= len(value.encode()) <= 512
            or value.startswith('/') or '\\' in value or any(ord(c)<32 for c in value)
            or any(part in ('','.', '..') for part in value.split('/'))
            or len(PurePosixPath(value).parts) > 16):
        raise ValueError('stage0_invalid_name')
    return value


def _hash(value):
    if type(value) is not str or re.fullmatch('[0-9a-f]{64}',value) is None:
        raise ValueError('stage0_invalid_hash')
    return value


def _https(value):
    try:
        parsed=urlsplit(value)
        if (type(value) is not str or len(value)>8192 or parsed.scheme!='https'
                or not parsed.hostname or parsed.username is not None or parsed.password is not None
                or parsed.fragment or parsed.port is not None and not 1 <= parsed.port <= 65535
                or any(ord(c)<33 for c in value)):
            raise ValueError
    except (ValueError,TypeError,AttributeError):
        raise ValueError('stage0_https_required') from None
    return value


def _dataset_locator(value):
    """Internal locator, never sent to urllib or interpreted as an HTTP URL.

    A mounted slug cannot attest a provider version. The controller attaches
    the declared version; exact size/SHA pins still verify every consumed byte.
    """
    if type(value) is not str or not value.startswith('kaggle-dataset:'):
        return None
    try:parsed=urlsplit(value)
    except ValueError:raise ValueError('stage0_dataset_locator_invalid') from None
    parts=parsed.path.split('/')[1:]
    slug_pattern=r'[a-z0-9][a-z0-9-]{1,98}[a-z0-9]'
    if (parsed.scheme!='kaggle-dataset' or parsed.query or parsed.fragment
            or re.fullmatch(slug_pattern,parsed.netloc) is None
            or len(parts)<3 or re.fullmatch(slug_pattern,parts[0]) is None
            or re.fullmatch(r'[1-9][0-9]{0,9}',parts[1]) is None
            or any(ord(c)<33 for c in value)
            or len(parts)>18 or any(re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]{0,199}',p) is None
                for p in parts[2:]) or len(value.encode())>1024):
        raise ValueError('stage0_dataset_locator_invalid')
    return parsed.netloc,parts[0],int(parts[1]),'/'.join(parts[2:])


@dataclass(frozen=True)
class Stage0Download:
    name: str
    url: str
    size: int
    sha256: str
    executable: bool = False

    def __post_init__(self):
        _name(self.name)
        if _dataset_locator(self.url) is None:_https(self.url)
        _integer(self.size,1,MAX_FILE_BYTES);_hash(self.sha256)
        if type(self.executable) is not bool:
            raise ValueError('stage0_invalid_executable')

    def wire(self):
        return dict(name=self.name,url=self.url,size=self.size,sha256=self.sha256,executable=self.executable)

    @classmethod
    def from_wire(cls,value):
        if type(value) is not dict or set(value)!={'name','url','size','sha256','executable'}:
            raise ValueError('stage0_invalid_download')
        return cls(**value)


@dataclass(frozen=True)
class Stage0Spec:
    downloads: tuple[Stage0Download,...]
    wheels: tuple[str,...]
    packages: Mapping[str,str]
    install_bytes: int
    total_seconds: int = 900

    def __post_init__(self):
        if (type(self.downloads) is not tuple or not 0<=len(self.downloads)<=MAX_FILES
                or any(type(value) is not Stage0Download for value in self.downloads)
                or len({value.name for value in self.downloads})!=len(self.downloads)
                or sum(value.size for value in self.downloads)>MAX_TOTAL_BYTES
                or type(self.wheels) is not tuple or len(self.wheels)>MAX_FILES
                or len(set(self.wheels))!=len(self.wheels)):
            raise ValueError('stage0_invalid_inventory')
        for wheel in self.wheels:
            if not _name(wheel).endswith('.whl'):
                raise ValueError('stage0_wheel_required')
        mounts={}
        for item in self.downloads:
            locator=_dataset_locator(item.url)
            if locator is not None:
                owner,slug,version,_=locator
                if slug in mounts and mounts[slug]!=(owner,version):
                    raise ValueError('stage0_dataset_mount_ambiguous')
                mounts[slug]=(owner,version)
        if not isinstance(self.packages,(dict,MappingProxyType)) or not 1<=len(self.packages)<=128:
            raise ValueError('stage0_exact_packages_required')
        packages=dict(self.packages)
        normalized=set()
        for name,version in packages.items():
            if (type(name) is not str or re.fullmatch('[A-Za-z0-9][A-Za-z0-9._-]{0,127}',name) is None
                    or type(version) is not str or re.fullmatch('[A-Za-z0-9][A-Za-z0-9._+!-]{0,127}',version) is None):
                raise ValueError('stage0_exact_packages_required')
            canonical=re.sub('[-_.]+','-',name).lower()
            if canonical in normalized:
                raise ValueError('stage0_duplicate_package')
            normalized.add(canonical)
        _integer(self.install_bytes,0,MAX_TOTAL_BYTES)
        _integer(self.total_seconds,1,3600)
        object.__setattr__(self,'packages',MappingProxyType(dict(sorted(packages.items()))))

    @property
    def dataset_sources(self):
        """Version-qualified dependencies to attach BEFORE the notebook starts."""
        return tuple(sorted({'/'.join((owner,slug,str(version)))
            for item in self.downloads if (locator:=_dataset_locator(item.url)) is not None
            for owner,slug,version,_ in (locator,)}))

    def wire(self, *, compact_wheels=True):
        wheels=list(self.wheels)
        positions={item.name:index for index,item in enumerate(self.downloads)}
        if compact_wheels and wheels and all(name in positions for name in wheels):
            # Exact selected download references; never infer/install every
            # downloaded .whl. Cached-only wheels retain the legacy name form.
            wheels={'download_indices':[positions[name] for name in wheels]}
        return dict(downloads=[value.wire() for value in self.downloads],wheels=wheels,
                    packages=dict(self.packages),install_bytes=self.install_bytes,total_seconds=self.total_seconds)

    @classmethod
    def from_wire(cls,value):
        if (type(value) is not dict
                or set(value)!={'downloads','wheels','packages','install_bytes','total_seconds'}
                or type(value['downloads']) is not list or len(value['downloads'])>MAX_FILES):
            raise ValueError('stage0_invalid_plan')
        downloads=tuple(Stage0Download.from_wire(item) for item in value['downloads'])
        wheels=value['wheels']
        if type(wheels) is dict:
            indices=wheels.get('download_indices')
            if (set(wheels)!={'download_indices'} or type(indices) is not list
                    or len(indices)>MAX_FILES):
                raise ValueError('stage0_invalid_wheel_references')
            for index in indices:_integer(index,0,len(downloads)-1)
            if len(set(indices))!=len(indices):
                raise ValueError('stage0_duplicate_wheel_reference')
            wheels=[downloads[index].name for index in indices]
        if type(wheels) is not list:
            raise ValueError('stage0_invalid_plan')
        return cls(downloads,tuple(wheels),value['packages'],value['install_bytes'],value['total_seconds'])


def _remaining(deadline):
    remaining=deadline-time.monotonic()
    if remaining<=0:
        raise Stage0Error('stage0_deadline_exceeded')
    return remaining


class _HTTPSRedirect(urllib.request.HTTPRedirectHandler):
    def __init__(self,deadline=None):
        self.deadline=deadline

    def redirect_request(self,req,fp,code,msg,headers,newurl):
        if self.deadline is not None:_remaining(self.deadline)
        try:
            _https(newurl)
        except ValueError:
            raise Stage0Error('stage0_unsafe_redirect') from None
        return super().redirect_request(req,fp,code,msg,headers,newurl)

    def http_error_302(self,req,fp,code,msg,headers):
        location=headers.get('location') or headers.get('uri')
        if location is None:return None
        try:
            newurl=urljoin(req.full_url,location)
            redirected=self.redirect_request(req,fp,code,msg,headers,newurl)
            if redirected is None:
                raise Stage0Error('stage0_unsafe_redirect')
            count=getattr(req,'_stage0_redirects',0)+1
            if count>5:
                raise Stage0Error('stage0_redirect_limit')
            redirected._stage0_redirects=count
        finally:
            # urllib's default redirect handler drains fp.read() without a
            # byte bound. Close this response; no redirect body is needed.
            fp.close()
        timeout=min(30,_remaining(self.deadline)) if self.deadline is not None else 30
        return self.parent.open(redirected,timeout=timeout)

    http_error_301=http_error_303=http_error_307=http_error_308=http_error_302


def _inventory(prepared):
    if (type(prepared) is not dict or set(prepared)!={'root','entries','max_total_bytes'}
            or type(prepared['root']) is not str or prepared['root'] != str(TRUSTED_ROOT)
            or type(prepared['entries']) not in (list,tuple)
            or not 1<=len(prepared['entries'])<=MAX_PREPARED_FILES):
        raise ValueError('stage0_prepared_inventory_required')
    _integer(prepared['max_total_bytes'],1,MAX_TOTAL_BYTES)
    inventory={}
    for value in prepared['entries']:
        if type(value) is not dict or set(value)!={'name','size','sha256','role'}:
            raise ValueError('stage0_invalid_prepared_entry')
        name=_name(value['name']);_integer(value['size'],1,MAX_FILE_BYTES);_hash(value['sha256'])
        if name in inventory:
            raise ValueError('stage0_duplicate_prepared_entry')
        inventory[name]=value
    if sum(item['size'] for item in inventory.values())>prepared['max_total_bytes']:
        raise ValueError('stage0_prepared_budget_exceeded')
    return inventory


def _parent(root_fd,name,*,create):
    """Hold directory descriptors so symlink replacement cannot redirect writes."""
    parent=os.dup(root_fd)
    try:
        for part in name.split('/')[:-1]:
            if create:
                try:os.mkdir(part,mode=0o700,dir_fd=parent)
                except FileExistsError:pass
            child=os.open(part,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=parent)
            os.close(parent);parent=child
        return parent
    except BaseException:
        os.close(parent)
        raise


def _verify(parent,name,size,digest,deadline,*,executable=False):
    try:fd=os.open(name,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK,dir_fd=parent)
    except FileNotFoundError:return False
    try:
        observed=os.fstat(fd)
        if (not stat.S_ISREG(observed.st_mode) or observed.st_size!=size
                or executable and not observed.st_mode&stat.S_IXUSR):
            raise Stage0Error('stage0_existing_file_mismatch')
        actual=hashlib.sha256()
        count=0
        while True:
            _remaining(deadline)
            data=os.read(fd,min(CHUNK_BYTES,size-count+1))
            if not data:break
            count+=len(data);actual.update(data)
            if count>size:
                raise Stage0Error('stage0_existing_file_mismatch')
        if count!=size or actual.hexdigest()!=digest:
            raise Stage0Error('stage0_existing_file_mismatch')
        return True
    finally:os.close(fd)


@contextmanager
def _open_source(item,opener,deadline):
    locator=_dataset_locator(item.url)
    if locator is None:
        request=urllib.request.Request(item.url,headers={'Accept-Encoding':'identity'},method='GET')
        with opener.open(request,timeout=min(30,_remaining(deadline))) as response:
            try:_https(response.geturl())
            except ValueError:raise Stage0Error('stage0_unsafe_redirect') from None
            if response.status!=200:raise Stage0Error('stage0_download_failed')
            yield response
        return
    _,slug,_,name=locator
    root_fd=parent=fd=None
    try:
        root=TRUSTED_INPUT_ROOT
        if not root.is_absolute() or root.resolve(strict=True)!=root:
            raise Stage0Error('stage0_dataset_mount_invalid')
        root_fd=os.open(root,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
        parent=_parent(root_fd,slug+'/'+name,create=False)
        fd=os.open(name.split('/')[-1],os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK,dir_fd=parent)
        info=os.fstat(fd)
        if (not stat.S_ISREG(info.st_mode) or info.st_nlink!=1 or info.st_size!=item.size
                or info.st_mode&0o022 and not os.fstatvfs(fd).f_flag&os.ST_RDONLY):
            raise Stage0Error('stage0_dataset_file_invalid')
        stream=os.fdopen(fd,'rb');fd=None
        with stream:yield stream
    except OSError:
        raise Stage0Error('stage0_dataset_unavailable') from None
    finally:
        for handle in (fd,parent,root_fd):
            if handle is not None:os.close(handle)


def _download(root_fd,item,opener,deadline):
    parent=_parent(root_fd,item.name,create=True)
    temporary='.rkb-part-'+secrets.token_hex(16)
    fd=None
    try:
        leaf=item.name.split('/')[-1]
        if _verify(parent,leaf,item.size,item.sha256,deadline,executable=item.executable):
            return False
        fd=os.open(temporary,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600,dir_fd=parent)
        with _open_source(item,opener,deadline) as response:
            digest=hashlib.sha256();count=0
            while True:
                _remaining(deadline)
                socket=getattr(getattr(getattr(response,'fp',None),'raw',None),'_sock',None)
                if socket is not None:socket.settimeout(min(30,_remaining(deadline)))
                data=response.read1(min(CHUNK_BYTES,item.size-count+1))
                if not data:break
                count+=len(data)
                if count>item.size:
                    raise Stage0Error('stage0_download_size_mismatch')
                digest.update(data)
                view=memoryview(data)
                while view:
                    written=os.write(fd,view)
                    view=view[written:]
            if count!=item.size:
                raise Stage0Error('stage0_download_size_mismatch')
            if digest.hexdigest()!=item.sha256:
                raise Stage0Error('stage0_download_hash_mismatch')
        os.fchmod(fd,0o500 if item.executable else 0o400);os.fsync(fd)
        os.close(fd);fd=None
        # link is an atomic no-overwrite publication in the held same directory.
        try:os.link(temporary,leaf,src_dir_fd=parent,dst_dir_fd=parent,follow_symlinks=False)
        except FileExistsError:
            if not _verify(parent,leaf,item.size,item.sha256,deadline,executable=item.executable):raise
        os.fsync(parent)
        return True
    finally:
        if fd is not None:os.close(fd)
        try:os.unlink(temporary,dir_fd=parent)
        except FileNotFoundError:pass
        os.close(parent)


def _versions(packages):
    observed={}
    for name in packages:
        try:observed[name]=importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:observed[name]=None
    return observed


def _install(root_fd,root,plan,inventory,deadline):
    if _versions(plan.packages)==dict(plan.packages):
        return False
    if not plan.wheels:
        raise Stage0Error('stage0_offline_wheels_required')
    for name in plan.wheels:
        parent=_parent(root_fd,name,create=False)
        try:
            value=inventory[name]
            if not _verify(parent,name.split('/')[-1],value['size'],value['sha256'],deadline):
                raise Stage0Error('stage0_wheel_missing')
        finally:os.close(parent)
    task='.rkb-stage0-'+secrets.token_hex(16)
    os.mkdir(task,mode=0o700,dir_fd=root_fd)
    directory=os.open(task,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=root_fd)
    try:
        fd=os.open('requirements.txt',os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600,dir_fd=directory)
        try:
            requirements=''.join((root/name).as_uri()+' --hash=sha256:'+inventory[name]['sha256']+'\n'
                                 for name in plan.wheels).encode()
            with os.fdopen(fd,'wb',closefd=False) as stream:
                stream.write(requirements);stream.flush();os.fsync(stream.fileno())
        finally:os.close(fd)
        command=[sys.executable,'-I','-m','pip','--isolated','--disable-pip-version-check',
            'install','--break-system-packages','--no-index','--no-deps','--only-binary=:all:','--require-hashes',
            '--no-cache-dir','--no-compile','--no-input','-r',str(root/task/'requirements.txt')]
        result=subprocess.run(command,stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,timeout=_remaining(deadline),check=False)
        if result.returncode!=0:
            raise Stage0Error('stage0_offline_install_failed')
        importlib.invalidate_caches()
        if _versions(plan.packages)!=dict(plan.packages):
            raise Stage0Error('stage0_installed_versions_mismatch')
        return True
    finally:
        os.close(directory)
        # Only files created by this invocation are disposable.
        own=os.open(task,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=root_fd)
        try:os.unlink('requirements.txt',dir_fd=own)
        finally:os.close(own)
        os.rmdir(task,dir_fd=root_fd)


def _receipt(root_fd,manifest,deadline):
    data=json.dumps(manifest,sort_keys=True,separators=(',',':')).encode()
    digest=hashlib.sha256(data).hexdigest()
    leaf='.rkb-stage0-'+manifest['plan_sha256']+'.json'
    if _verify(root_fd,leaf,len(data),digest,deadline):return
    temporary='.rkb-stage0-receipt-'+secrets.token_hex(16)
    fd=os.open(temporary,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600,dir_fd=root_fd)
    try:
        with os.fdopen(fd,'wb',closefd=False) as stream:
            stream.write(data);stream.flush();os.fsync(stream.fileno())
        os.link(temporary,leaf,src_dir_fd=root_fd,dst_dir_fd=root_fd,follow_symlinks=False)
        os.fsync(root_fd)
    except FileExistsError:
        if not _verify(root_fd,leaf,len(data),digest,deadline):raise
    finally:
        os.close(fd);os.unlink(temporary,dir_fd=root_fd)


def prepare_stage0(prepared_model:dict,plan:dict,*,run_id=None)->dict:
    """Verify/cache pinned bytes, optionally install one offline wheel closure.

    The controller supplies both manifests. No provider inference, PG startup,
    pip network resolution, model discovery or unknown-file cleanup occurs here.
    """
    try:
        spec=Stage0Spec.from_wire(plan)
        inventory=_inventory(prepared_model)
        for item in spec.downloads:
            if (item.name not in inventory or any(inventory[item.name][key]!=getattr(item,key)
                    for key in ('size','sha256'))):
                raise ValueError('stage0_download_not_in_prepared_inventory')
        if any(name not in inventory or inventory[name]['role']!='code' for name in spec.wheels):
            raise ValueError('stage0_wheel_not_in_prepared_inventory')
        root=Path(prepared_model['root'])
        if root.resolve(strict=True)!=root or not root.is_dir():
            raise ValueError('stage0_root_invalid')
        deadline=time.monotonic()+spec.total_seconds
        root_fd=os.open(root,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
        try:
            needed=spec.install_bytes
            for item in spec.downloads:
                # No directory creation or network until all current files and
                # the complete missing-byte + install reservation are checked.
                try:parent=_parent(root_fd,item.name,create=False)
                except FileNotFoundError:needed+=item.size;continue
                try:
                    if not _verify(parent,item.name.split('/')[-1],item.size,item.sha256,deadline,
                            executable=item.executable):
                        needed+=item.size
                finally:os.close(parent)
            if shutil.disk_usage(root).free<needed:
                raise Stage0Error('stage0_insufficient_disk')
            opener=urllib.request.build_opener(_HTTPSRedirect(deadline),urllib.request.ProxyHandler({}))
            downloaded=0;downloaded_files=0;dataset_bytes=0;dataset_files=0
            from contextlib import nullcontext
            from .kaggle_environment import bootstrap_stage
            with bootstrap_stage(run_id,'stage0_download') if run_id is not None else nullcontext():
                for item in spec.downloads:
                    try:
                        copied=_download(root_fd,item,opener,deadline)
                    except Exception as error:
                        if run_id is not None:
                            status=getattr(error,'code',None)
                            print(json.dumps(dict(event='stage0_download_failed',run_id=run_id,
                                artifact_name=item.name,artifact_sha256=item.sha256,
                                source_host=urlsplit(item.url).hostname,
                                error_type=type(error).__name__,
                                http_status=status if type(status) is int and 100<=status<=599 else None)),flush=True)
                        raise
                    if copied:
                        if _dataset_locator(item.url) is None:
                            downloaded+=item.size;downloaded_files+=1
                        else:
                            dataset_bytes+=item.size;dataset_files+=1
            with bootstrap_stage(run_id,'stage0_install') if run_id is not None else nullcontext():
                installed=_install(root_fd,root,spec,inventory,deadline)
            _remaining(deadline)
            plan_hash=hashlib.sha256(json.dumps(spec.wire(),sort_keys=True,separators=(',',':')).encode()).hexdigest()
            manifest=dict(codec='rkb.stage0.v1',plan_sha256=plan_hash,packages=dict(spec.packages))
            _receipt(root_fd,manifest,deadline)
            return dict(manifest,downloaded_bytes=downloaded,
                dataset_copied_bytes=dataset_bytes,dataset_copied_files=dataset_files,
                reused_files=len(spec.downloads)-downloaded_files-dataset_files,
                installed=installed)
        finally:os.close(root_fd)
    except Stage0Error:raise
    except subprocess.TimeoutExpired:
        raise Stage0Error('stage0_deadline_exceeded') from None
    except (ValueError,TypeError,KeyError,AttributeError,OSError):
        raise Stage0Error('stage0_preparation_failed') from None
