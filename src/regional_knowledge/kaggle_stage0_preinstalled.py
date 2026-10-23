"""Immutable CPU site-packages archive for existing version-pinned Dataset Stage0.

No provider API, new manager, credential, runtime network or pip effect.
"""
from __future__ import annotations

import gzip
import hashlib
import importlib
import importlib.metadata
import io
import json
import os
from pathlib import Path, PurePosixPath
from types import MappingProxyType
import re
import shutil
import sys
import tarfile
import tempfile

CODEC='rkb.stage0-preinstalled.v1'
MAX_FILES=100000
MAX_BYTES=16*1024**3
MAX_MANIFEST=16*1024**2
CHUNK=1024*1024
ENV_FIELDS=frozenset({'python_version','cache_tag','machine','libc_name',
    'libc_version','os_id','os_version_id'})
PIN_FIELDS=frozenset({'archive_name','environment','packages_sha256',
    'manifest_sha256','file_count','unpacked_bytes','accelerator'})


def canonical(name):
    return re.sub(r'[-_.]+','-',name).lower()


def raw(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=True).encode()


def digest(value):
    return hashlib.sha256(value).hexdigest()


def safe_name(value):
    if (type(value) is not str or not 1<=len(value.encode())<=512
            or value.startswith('/') or '\\' in value
            or len(PurePosixPath(value).parts)>16
            or any(part in ('','.','..') for part in value.split('/'))
            or any(ord(char)<32 or ord(char)==127 for char in value)):
        raise ValueError('preinstalled_invalid_member')
    leaf=value.rsplit('/',1)[-1]
    if leaf.endswith(('.pth','.egg-link')) or leaf in ('sitecustomize.py','usercustomize.py'):
        raise ValueError('preinstalled_import_hook_forbidden')
    return value


def packages_sha256(packages):
    return digest(raw(sorted((canonical(key),value) for key,value in packages.items())))


def environment():
    from .kaggle_environment import observe_environment
    observed=observe_environment()
    return dict(python_version='.'.join(map(str,sys.version_info[:3])),
        **{field:observed[field] for field in ENV_FIELDS-{'python_version'}})


def validate_environment(value):
    if (type(value) is not dict or set(value)!=ENV_FIELDS
            or any(type(item) is not str or not 1<=len(item)<=128 or
                any(ord(c)<33 for c in item) for item in value.values())):
        raise ValueError('preinstalled_environment_invalid')


def validate_pin(pin,downloads,packages,install_bytes):
    if type(pin) not in (dict,MappingProxyType) or set(pin)!=PIN_FIELDS:
        raise ValueError('preinstalled_pin_fields')
    name=safe_name(pin['archive_name'])
    validate_environment(pin['environment'])
    if (not name.endswith('.tar.gz') or pin['accelerator']!='cpu'
            or not all(type(pin[k]) is str and re.fullmatch('[0-9a-f]{64}',pin[k])
                for k in ('manifest_sha256','packages_sha256'))
            or pin['packages_sha256']!=packages_sha256(packages)
            or type(pin['file_count']) is not int or not 1<=pin['file_count']<=MAX_FILES
            or type(pin['unpacked_bytes']) is not int
            or not 1<=pin['unpacked_bytes']<=min(install_bytes,MAX_BYTES)
            or any(canonical(k)=='torch' and not v.endswith('+cpu')
                for k,v in packages.items())):
        raise ValueError('preinstalled_package_pin_invalid')
    archives=[item for item in downloads if item.name==name]
    if (len(archives)!=1 or not archives[0].url.startswith('kaggle-dataset://')
            or archives[0].executable):
        raise ValueError('preinstalled_requires_versioned_dataset')
    return dict(pin,environment=dict(pin['environment']))


def installed_files(site,packages):
    installed={canonical(dist.metadata.get('Name','')):dist
        for dist in importlib.metadata.distributions(path=[str(site)])}
    members={}
    for package,version in sorted(packages.items()):
        dist=installed.get(canonical(package))
        if dist is None or dist.version!=version or dist.files is None:
            raise ValueError('preinstalled_missing_distribution')
        for entry in dist.files:
            rel=Path(str(entry))
            if '..' in rel.parts:
                # Console scripts in ../bin are not imported by the warm RKB
                # process. Never include a file outside the pinned site root.
                continue
            name=safe_name(rel.as_posix())
            file=site/rel
            if file.resolve(strict=True)!=file or not file.is_file():
                raise ValueError('preinstalled_unsafe_file')
            if name in members and members[name]!=file:
                raise ValueError('preinstalled_duplicate_member')
            members[name]=file
    if not 1<=len(members)<=MAX_FILES:
        raise ValueError('preinstalled_member_count')
    return members


def build_archive(site_root,packages,output,*,artifact_name,observed=None):
    """One-time build under matching Kaggle CPU Python. No upload/publish."""
    site=Path(site_root).absolute()
    output=Path(output).absolute()
    safe_name(artifact_name)
    if (not artifact_name.endswith('.tar.gz') or site.resolve(strict=True)!=site
            or any(canonical(k)=='torch' and not v.endswith('+cpu')
                for k,v in packages.items())):
        raise ValueError('preinstalled_cpu_or_path')
    observed=environment() if observed is None else observed
    validate_environment(observed)
    members=installed_files(site,packages)
    files=[];total=0
    for name,path in sorted(members.items()):
        size=path.stat().st_size
        total+=size
        if size<0 or total>MAX_BYTES:
            raise ValueError('preinstalled_byte_budget')
        with path.open('rb') as stream:
            checksum=hashlib.file_digest(stream,'sha256').hexdigest()
        files.append(dict(name='site-packages/'+name,size=size,sha256=checksum))
    manifest=dict(codec=CODEC,packages=dict(sorted(packages.items())),
        environment=observed,files=files)
    manifest_bytes=raw(manifest)
    if len(manifest_bytes)>MAX_MANIFEST:
        raise ValueError('preinstalled_manifest_budget')
    output.parent.mkdir(parents=True,exist_ok=True)
    temp=None
    try:
        with tempfile.NamedTemporaryFile(prefix='.rkb-env-',suffix='.tar.gz',
                dir=output.parent,delete=False) as target:
            temp=Path(target.name)
            with gzip.GzipFile(filename='',fileobj=target,mode='wb',
                    compresslevel=1,mtime=0) as zipped:
                with tarfile.open(fileobj=zipped,mode='w|',format=tarfile.PAX_FORMAT) as archive:
                    header=tarfile.TarInfo('rkb-preinstalled-manifest.json')
                    header.size=len(manifest_bytes);header.mode=0o400;header.mtime=0
                    archive.addfile(header,io.BytesIO(manifest_bytes))
                    for row in files:
                        name=row['name'].removeprefix('site-packages/')
                        header=tarfile.TarInfo(row['name'])
                        header.size=row['size'];header.mtime=0
                        header.mode=0o500 if name.endswith(('.so','.so.1','.so.2')) else 0o400
                        with members[name].open('rb') as source:
                            archive.addfile(header,source)
        with temp.open('rb') as source:
            checksum=hashlib.file_digest(source,'sha256').hexdigest()
        size=temp.stat().st_size
        os.link(temp,output,follow_symlinks=False)  # Never overwrite a prior artifact.
        return dict(name=artifact_name,size=size,sha256=checksum,
            preinstalled=dict(archive_name=artifact_name,environment=observed,
                packages_sha256=packages_sha256(packages),
                manifest_sha256=digest(manifest_bytes),
                file_count=len(files),unpacked_bytes=total,accelerator='cpu'))
    finally:
        if temp is not None:temp.unlink(missing_ok=True)


def activate(root,pin,packages,deadline):
    """Verify exact copied archive and activate overlay; no pip fallback."""
    from .kaggle_stage0 import Stage0Error, _remaining
    if environment()!=pin['environment']:
        raise Stage0Error('preinstalled_runtime_abi_mismatch')
    if packages_sha256(packages)!=pin['packages_sha256']:
        raise Stage0Error('preinstalled_package_closure_mismatch')
    temp=None;site=None;inserted=False
    try:
        temp=Path(tempfile.mkdtemp(prefix='.rkb-env-',dir=root))
        site=temp/'site-packages';site.mkdir(mode=0o700)
        with tarfile.open(root/pin['archive_name'],mode='r|gz') as archive:
            first=archive.next()
            if (first is None or first.name!='rkb-preinstalled-manifest.json'
                    or not first.isfile() or not 0<first.size<=MAX_MANIFEST):
                raise Stage0Error('preinstalled_manifest_invalid')
            with archive.extractfile(first) as source:
                payload=source.read(MAX_MANIFEST+1)
            if len(payload)!=first.size or digest(payload)!=pin['manifest_sha256']:
                raise Stage0Error('preinstalled_manifest_changed')
            manifest=json.loads(payload)
            if (type(manifest) is not dict
                    or set(manifest)!={'codec','packages','environment','files'}
                    or manifest['codec']!=CODEC or manifest['packages']!=dict(packages)
                    or manifest['environment']!=pin['environment']
                    or type(manifest['files']) is not list
                    or len(manifest['files'])!=pin['file_count']):
                raise Stage0Error('preinstalled_manifest_invalid')
            seen=set();total=0
            for row in manifest['files']:
                _remaining(deadline)
                item=archive.next()
                if (type(row) is not dict or set(row)!={'name','size','sha256'}
                        or item is None or not item.isfile() or item.name!=row['name']
                        or type(row['size']) is not int or row['size']<0
                        or item.size!=row['size'] or type(row['sha256']) is not str
                        or re.fullmatch('[0-9a-f]{64}',row['sha256']) is None
                        or not item.name.startswith('site-packages/')):
                    raise Stage0Error('preinstalled_member_invalid')
                name=safe_name(item.name.removeprefix('site-packages/'))
                if name in seen:raise Stage0Error('preinstalled_member_duplicate')
                seen.add(name);total+=row['size']
                if total>pin['unpacked_bytes']:
                    raise Stage0Error('preinstalled_byte_budget')
                path=site/name
                path.parent.mkdir(mode=0o700,parents=True,exist_ok=True)
                observed=hashlib.sha256()
                with archive.extractfile(item) as source:
                    descriptor=os.open(path,os.O_CREAT|os.O_EXCL|os.O_WRONLY|os.O_NOFOLLOW,0o600)
                    with os.fdopen(descriptor,'wb') as target:
                        remaining=row['size']
                        while remaining:
                            _remaining(deadline)
                            block=source.read(min(CHUNK,remaining))
                            if not block:raise Stage0Error('preinstalled_member_truncated')
                            target.write(block);observed.update(block);remaining-=len(block)
                if observed.hexdigest()!=row['sha256']:
                    raise Stage0Error('preinstalled_member_hash')
                path.chmod(0o500 if name.endswith(('.so','.so.1','.so.2')) else 0o400)
            if archive.next() is not None or total!=pin['unpacked_bytes']:
                raise Stage0Error('preinstalled_member_inventory')
        installed={}
        for dist in importlib.metadata.distributions(path=[str(site)]):
            key=canonical(dist.metadata.get('Name',''))
            if key in installed:raise Stage0Error('preinstalled_duplicate_distribution')
            installed[key]=dist.version
        if installed!={canonical(k):v for k,v in packages.items()}:
            raise Stage0Error('preinstalled_distribution_set')
        sys.path.insert(0,str(site));inserted=True
        importlib.invalidate_caches()
        return str(site)
    except BaseException as error:
        if inserted:sys.path.remove(str(site))
        if temp is not None:shutil.rmtree(temp,ignore_errors=True)
        if isinstance(error,Stage0Error):raise
        if isinstance(error,(ValueError,TypeError,OSError,tarfile.TarError,
                EOFError,KeyError,AttributeError)):
            raise Stage0Error('preinstalled_activation_failed') from None
        raise
