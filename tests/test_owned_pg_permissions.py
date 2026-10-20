"""Verified public-code service access, fake OS identity; no PG process."""
import hashlib
import os
from pathlib import Path
import stat
from types import SimpleNamespace

import pytest

from regional_knowledge.kaggle_owned_pg import grant_native_code_access,_can_access,BootstrapUnavailable


@pytest.fixture
def code(tmp_path):
    root=tmp_path/'working';root.mkdir(mode=0o711)
    files={'native/bin/postgres':b'synthetic ELF', 'native/lib/library.so':b'synthetic library',
        'native/share/postgres.bki':b'synthetic bootstrap', 'code/handler.py':b'# public handler',
        'wheels/public.whl':b'synthetic public wheel'}
    pins=[]
    for name,data in files.items():
        path=root/name;path.parent.mkdir(parents=True,mode=0o700,exist_ok=True)
        path.write_bytes(data);path.chmod(0o500 if name.endswith('/postgres') else 0o400)
        pins.append((str(path),len(data),hashlib.sha256(data).hexdigest(),name.endswith('/postgres')))
    private=root/'main.py';private.write_bytes(b'synthetic private config');private.chmod(0o600)
    model=root/'model';model.mkdir(mode=0o700);(model/'weight').write_bytes(b'synthetic model');(model/'weight').chmod(0o400)
    return root,files,pins


def fake_identity(monkeypatch,*,uid=9999,gid=8888):
    real_stat=os.fstat;real_chmod=os.fchmod;groups={};calls=[]
    def observed(fd):
        info=real_stat(fd)
        return SimpleNamespace(st_uid=0,st_gid=groups.get((info.st_dev,info.st_ino),0),
            st_mode=info.st_mode,st_size=info.st_size,st_mtime_ns=info.st_mtime_ns,st_ctime_ns=info.st_ctime_ns,
            st_dev=info.st_dev,st_ino=info.st_ino)
    def chgrp(fd,owner,group):
        assert owner==-1 and group==gid
        info=real_stat(fd);groups[(info.st_dev,info.st_ino)]=group;calls.append(('group',info.st_ino))
    def chmod(fd,mode):
        assert not mode&0o020
        calls.append(('mode',real_stat(fd).st_ino));real_chmod(fd,mode)
    monkeypatch.setattr(os,'geteuid',lambda:0)
    monkeypatch.setattr(os,'fstat',observed)
    monkeypatch.setattr(os,'fchown',chgrp)
    monkeypatch.setattr(os,'fchmod',chmod)
    return observed,calls


def test_verified_code_only_becomes_readable_by_exact_native_identity(code,monkeypatch):
    root,files,pins=code
    root_before=root.stat().st_mode
    observed,calls=fake_identity(monkeypatch)
    grant_native_code_access(root,pins,uid=9999,gid=8888)
    assert calls and root.stat().st_mode==root_before
    for absolute,size,digest,executable in pins:
        assert hashlib.sha256(Path(absolute).read_bytes()).hexdigest()==digest
        fd=os.open(absolute,os.O_RDONLY)
        try:info=observed(fd)
        finally:os.close(fd)
        assert info.st_uid==0 and info.st_gid==8888
        assert _can_access(info,9999,8888,5 if executable else 4)
        if not executable:assert not stat.S_IMODE(info.st_mode)&0o010
        assert not stat.S_IMODE(info.st_mode)&0o022
    assert stat.S_IMODE((root/'main.py').stat().st_mode)==0o600
    assert stat.S_IMODE((root/'model').stat().st_mode)==0o700
    assert stat.S_IMODE((root/'model/weight').stat().st_mode)==0o400
    assert stat.S_IMODE((root/'native/bin').stat().st_mode)&0o070==0o010


@pytest.mark.parametrize('case',['symlink_parent','changed_bytes','root_not_traversable'])
def test_permission_grants_refuse_unsafe_code_without_launching_native(code,monkeypatch,case):
    root,files,pins=code;fake_identity(monkeypatch)
    if case=='symlink_parent':
        alias=root/'alias';alias.symlink_to(root/'native/bin',target_is_directory=True)
        old=pins[0];pins=[(str(alias/'postgres'),*old[1:])]
    if case=='changed_bytes':
        path=Path(pins[0][0]);path.chmod(0o700);path.write_bytes(b'x'*pins[0][1]);path.chmod(0o500)
    if case=='root_not_traversable':root.chmod(0o700)
    with pytest.raises(BootstrapUnavailable):grant_native_code_access(root,pins,uid=9999,gid=8888)


def test_same_verified_nonroot_identity_needs_no_permission_mutation(code,monkeypatch):
    root,files,pins=code
    monkeypatch.setattr(os,'geteuid',lambda:1234)
    monkeypatch.setattr(os,'getegid',lambda:1234)
    monkeypatch.setattr(os,'getgroups',lambda:[1234])
    original=os.fstat
    def owned(fd):
        info=original(fd)
        return SimpleNamespace(st_uid=1234,st_gid=1234,st_mode=info.st_mode,st_size=info.st_size,
            st_mtime_ns=info.st_mtime_ns,st_ctime_ns=info.st_ctime_ns)
    monkeypatch.setattr(os,'fstat',owned)
    def forbidden(*args):raise AssertionError('verified nonroot identity attempted permission mutation')
    monkeypatch.setattr(os,'fchmod',forbidden);monkeypatch.setattr(os,'fchown',forbidden)
    grant_native_code_access(root,pins,uid=1234,gid=1234)
