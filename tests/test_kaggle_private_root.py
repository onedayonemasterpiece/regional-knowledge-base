"""Cold session filesystem regressions; no PG, provider, model or credentials."""
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from regional_knowledge import kaggle_owned_pg as pg
from regional_knowledge.kaggle_bootstrap import BootstrapUnavailable
from test_kaggle_target_runtime import spec, hashes, ROOT


@pytest.fixture
def cold_root(tmp_path,monkeypatch):
    provider=tmp_path/'provider';provider.mkdir(mode=0o755);provider.chmod(0o755)
    private=provider/'temp'
    monkeypatch.setattr(pg,'PRIVATE_RUNTIME_ROOT',private)
    return private


def test_cold_fixed_parent_survives_umask_and_native_identity_can_traverse(cold_root):
    assert not cold_root.exists()
    old=os.umask(0o077)
    try:assert pg.ensure_private_runtime_root(native_uid=65534,native_gid=65534)==cold_root
    finally:os.umask(old)
    info=cold_root.stat()
    assert info.st_mode&0o777==0o711 and info.st_uid==os.geteuid()
    assert pg._can_access(info,65534,65534,1)
    assert not pg._can_access(info,65534,65534,2)
    assert not list(cold_root.iterdir())  # Writable probe is removed.
    assert pg.ensure_private_runtime_root(native_uid=65534,native_gid=65534)==cold_root


@pytest.mark.parametrize('bad',('symlink_root','symlink_provider','writable_root','writable_provider','native_parent_denied'))
def test_untrusted_provider_parent_is_refused(cold_root,bad,tmp_path):
    if bad=='symlink_root':
        elsewhere=tmp_path/'elsewhere';elsewhere.mkdir();cold_root.symlink_to(elsewhere,target_is_directory=True)
    elif bad=='symlink_provider':
        elsewhere=tmp_path/'elsewhere';elsewhere.mkdir()
        cold_root.parent.rmdir();cold_root.parent.symlink_to(elsewhere,target_is_directory=True)
    elif bad=='writable_root':cold_root.mkdir();cold_root.chmod(0o777)
    elif bad=='writable_provider':cold_root.parent.chmod(0o777)
    elif bad=='native_parent_denied':cold_root.parent.chmod(0o700)
    with pytest.raises(BootstrapUnavailable,match='private_root_unavailable'):
        pg.ensure_private_runtime_root(native_uid=65534,native_gid=65534)
    if bad in ('writable_provider','native_parent_denied'):assert not cold_root.exists()


def test_missing_provider_directory_is_never_created(cold_root):
    cold_root.parent.rmdir()
    with pytest.raises(BootstrapUnavailable,match='private_root_unavailable'):
        pg.ensure_private_runtime_root()
    assert not cold_root.parent.exists()


def test_provider_parent_owned_by_another_identity_is_refused(cold_root,monkeypatch):
    monkeypatch.setattr(pg.os,'geteuid',lambda:65533)
    with pytest.raises(BootstrapUnavailable,match='private_root_unavailable'):
        pg.ensure_private_runtime_root()
    assert not cold_root.exists()


@pytest.mark.asyncio
@pytest.mark.parametrize('missing_caller_parent',(False,True))
async def test_start_empty_creates_fixed_cold_parent_before_exclusive_native_cluster(cold_root,monkeypatch,missing_caller_parent):
    # Validate real launch paths before redirecting filesystem effects to the
    # central synthetic fixture (whose long socket path is never opened).
    with monkeypatch.context() as construction:
        construction.setattr(pg,'PRIVATE_RUNTIME_ROOT',Path('/kaggle/temp'))
        database=spec(fresh=True).database
    cluster=cold_root/('caller-missing/owned' if missing_caller_parent else 'owned')
    for key,value in dict(cluster=str(cluster),socket=str(cluster/'socket'),uid=os.geteuid(),gid=os.getegid()).items():
        object.__setattr__(database,key,value)
    fixed=dict(database.native_paths,vector_library='/kaggle/working/native/vector.so',
        vector_control='/kaggle/working/native/extension/vector.control',
        vector_sql='/kaggle/working/native/extension/vector--0.8.2.sql')
    object.__setattr__(database,'native_paths',fixed)
    artifacts=SimpleNamespace(root=Path('/'),entries=[SimpleNamespace(role='code',
        requirement=SimpleNamespace(name=p.lstrip('/'),size=1,sha256='e'*64)) for p in fixed.values()])
    monkeypatch.setattr(pg,'_file_hash',lambda *args:'e'*64)
    monkeypatch.setattr(pg,'grant_native_code_access',lambda *args,**kwargs:None)
    owner=pg.OwnedPostgres(database);calls=[]
    class NativeBoundaryReached(Exception):pass
    async def command(*args,**kwargs):
        calls.append(args)
        if args[-1]=='--pkglibdir':return '/kaggle/working/native'
        if args[-1]=='--sharedir':return '/kaggle/working/native'
        assert args[0]==fixed['initdb']
        assert cluster.stat().st_mode&0o777==0o700
        assert (cluster/'socket').stat().st_mode&0o777==0o700
        assert (cluster/'initialization.json').stat().st_mode&0o777==0o400
        assert cluster.stat().st_uid==database.uid
        assert not (cluster/'data').exists()
        raise NativeBoundaryReached
    monkeypatch.setattr(owner,'_command',command)
    intent=dict(codec='rkb.empty-generation.v1',database=database.wire(),journal_head=None,
        journal_stream='synthetic',run_id='run',launch_nonce='nonce',sql_hashes=hashes())
    if missing_caller_parent:
        with pytest.raises(BootstrapUnavailable,match='parent_unverified'):
            await owner.start_empty(intent=intent,artifacts=artifacts,sql_root=ROOT/'sql/kaggle')
        assert cold_root.exists() and not cluster.parent.exists() and len(calls)==2
        return
    with pytest.raises(NativeBoundaryReached):
        await owner.start_empty(intent=intent,artifacts=artifacts,sql_root=ROOT/'sql/kaggle')
    assert len(calls)==3 and cold_root.exists() and owner.process is None
    before=(cluster/'initialization.json').read_bytes()
    with pytest.raises(BootstrapUnavailable,match='existing_cluster_refused'):
        await owner.start_empty(intent=intent,artifacts=artifacts,sql_root=ROOT/'sql/kaggle')
    assert len(calls)==3 and (cluster/'initialization.json').read_bytes()==before
