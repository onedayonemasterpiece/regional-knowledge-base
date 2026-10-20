"""Synthetic private backup bytes; no native PG/provider/model effects."""
import ast
from dataclasses import asdict,replace
import io
from pathlib import Path
import tarfile
from types import SimpleNamespace

import pytest

from regional_knowledge.kaggle_pg_snapshot_native import (
    SnapshotError,SnapshotLimits,NativeBinary,PhysicalPgTools,sha,digest_file,
    inventory_tree,pack_tree,unpack_tree,private_workspace,
)
from regional_knowledge.kaggle_restored_database import VerifiedInitializedDatabase


def backup(tmp_path):
    root=tmp_path/'base';root.mkdir();(root/'global').mkdir()
    for name,data in {'PG_VERSION':b'17\n','backup_manifest':b'{"synthetic":true}',
                      'global/pg_control':b'synthetic-control'}.items():(root/name).write_bytes(data)
    return root


def manifest(root,packed,limits):
    rows,used,digest,size,archive_hash=pack_tree(root,packed,limits)
    return {'file_count':len(rows),'unpacked_bytes':used,'inventory_sha256':digest,
        'backup_manifest_sha256':digest_file(root/'backup_manifest',limits.max_unpacked_bytes)[1]}


def test_physical_inventory_roundtrip_and_archive_hash(tmp_path):
    root=backup(tmp_path);limits=SnapshotLimits();packed=tmp_path/'base.tar'
    expected=manifest(root,packed,limits)
    unpack_tree(packed,tmp_path/'restored',expected,limits)
    assert inventory_tree(root,limits)==inventory_tree(tmp_path/'restored',limits)
    assert (tmp_path/'restored/global/pg_control').read_bytes()==b'synthetic-control'


@pytest.mark.parametrize('kind',['traversal','symlink','duplicate'])
def test_archive_extraction_refuses_unsafe_members(tmp_path,kind):
    packed=tmp_path/'bad.tar'
    with tarfile.open(packed,'w') as archive:
        info=tarfile.TarInfo('../outside' if kind=='traversal' else 'member')
        if kind=='symlink':info.type=tarfile.SYMTYPE;info.linkname='/outside'
        archive.addfile(info)
        if kind=='duplicate':archive.addfile(info)
    with pytest.raises(SnapshotError,match='unsafe_archive_member'):
        unpack_tree(packed,tmp_path/'candidate',{},SnapshotLimits())
    assert not (tmp_path/'outside').exists()


def test_inventory_refuses_live_state_links_and_budget(tmp_path):
    root=backup(tmp_path);limits=SnapshotLimits()
    (root/'postmaster.pid').write_text('synthetic')
    with pytest.raises(SnapshotError,match='live_or_replica'):inventory_tree(root,limits)
    (root/'postmaster.pid').unlink();(root/'link').symlink_to(root/'PG_VERSION')
    with pytest.raises(SnapshotError,match='nonregular'):inventory_tree(root,limits)
    (root/'link').unlink()
    with pytest.raises(SnapshotError,match='byte_budget'):inventory_tree(root,replace(limits,max_unpacked_bytes=1))


@pytest.mark.asyncio
async def test_native_binary_exact_hash_and_actual_version_required(tmp_path):
    path=tmp_path/'pinned-native';path.write_bytes(b'synthetic binary bytes')
    binary=NativeBinary(str(path),path.stat().st_size,sha(path.read_bytes()),'pg_tool (PostgreSQL) 17.6')
    tools=PhysicalPgTools(binary,binary,binary)
    async def run(*args,**kwargs):return 'pg_tool (PostgreSQL) 17.6'
    await tools.verify(run)
    path.write_bytes(b'changed native bytes')
    with pytest.raises(SnapshotError,match='binary_hash'):await tools.verify(run)


def test_native_modules_have_no_controller_authority_dependency():
    package=Path(__file__).parents[1]/'src/regional_knowledge'
    forbidden={'control_spool','source_history_verifier','participants','sqlite_corpus','story_registry','recovery_s3'}
    for filename in ('kaggle_pg_snapshot_native.py','kaggle_restored_database.py'):
        tree=ast.parse((package/filename).read_text())
        imports={n.module.split('.')[0] for n in ast.walk(tree) if isinstance(n,ast.ImportFrom) and n.module}
        assert not imports&forbidden


@pytest.mark.asyncio
async def test_capability_copied_fields_never_replace_owner_retention():
    class Owner:
        def __init__(self):self.retained=set()
        async def verify_initialized(self,cap,database,identity):
            if cap not in self.retained:raise PermissionError('unknown retained recovery')
            assert cap.database is database and cap.identity==identity
            return {'host':'synthetic-private-socket','user':'rkb_reader'}
    owner=Owner();database=object()
    cap=VerifiedInitializedDatabase(owner,None,database,('run','nonce','inc'),None,None,0,b'{}',object())
    owner.retained.add(cap)
    assert (await cap.verify_for(database,'run','nonce','inc'))['user']=='rkb_reader'
    with pytest.raises(PermissionError):await replace(cap).verify_for(database,'run','nonce','inc')
