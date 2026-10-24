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
import regional_knowledge.kaggle_pg_snapshot_native as native


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


def test_archive_metadata_budget_checked_before_extended_header_read(tmp_path):
    path=tmp_path/'metadata.tar';info=tarfile.TarInfo('extended')
    info.type=tarfile.XHDTYPE;info.size=1024**3
    path.write_bytes(info.tobuf())  # No giant metadata body is created/read.
    with pytest.raises(SnapshotError,match='tar_metadata_budget'):
        unpack_tree(path,tmp_path/'candidate',{},SnapshotLimits())


@pytest.mark.asyncio
async def test_archive_thread_retains_capacity_until_actual_io_completion():
    import asyncio
    import threading
    started=threading.Event();finish=threading.Event()
    def io():started.set();finish.wait(2)
    task=asyncio.create_task(native._blocking(io))
    await asyncio.to_thread(started.wait,1)
    task.cancel();await asyncio.sleep(.01)
    assert not task.done()
    finish.set()
    with pytest.raises(asyncio.CancelledError):await task


def test_inventory_refuses_live_state_links_and_budget(tmp_path):
    root=backup(tmp_path);limits=SnapshotLimits()
    (root/'postmaster.pid').write_text('synthetic')
    with pytest.raises(SnapshotError,match='live_or_replica'):inventory_tree(root,limits)
    (root/'postmaster.pid').unlink();(root/'link').symlink_to(root/'PG_VERSION')
    with pytest.raises(SnapshotError,match='nonregular'):inventory_tree(root,limits)
    (root/'link').unlink()
    with pytest.raises(SnapshotError,match='byte_budget'):inventory_tree(root,replace(limits,max_unpacked_bytes=1))


def test_private_native_workspace_excludes_notebook_outputs(tmp_path,monkeypatch):
    with pytest.raises(SnapshotError,match='private_workspace_required'):
        private_workspace('/kaggle/working/corpus',1)
    monkeypatch.setattr(native,'PRIVATE_RUNTIME_ROOT',tmp_path)
    monkeypatch.setattr('regional_knowledge.kaggle_owned_pg.PRIVATE_RUNTIME_ROOT',tmp_path)
    candidate=tmp_path/'fresh-owned'
    assert private_workspace(candidate,1)==candidate
    assert candidate.stat().st_mode&0o777==0o700
    with pytest.raises(FileExistsError):private_workspace(candidate,1)


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


@pytest.mark.asyncio
@pytest.mark.parametrize('archive_cursor',[0,1])
async def test_native_replay_exact_receipts_precede_rollover_and_adoption(monkeypatch,archive_cursor):
    from contextlib import asynccontextmanager
    events=[];binding=object();writer=['old'];sequence=[archive_cursor]
    class Connection:
        async def execute(self,sql,*args):
            self.row=(sequence[0],writer[0]) if 'applied_seq,writer_generation' in sql else (sequence[0],) if 'applied_seq' in sql else (writer[0],)
            return self
        async def fetchone(self):return self.row
        async def commit(self):pass
        @asynccontextmanager
        async def transaction(self):yield
    @asynccontextmanager
    async def acquire():yield Connection()
    async def executor(frame):
        sequence[0]+=1;events.append(('apply',sequence[0]))
        return {'sequence':sequence[0],'command_sha256':str(sequence[0])*64,'outcome_sha256':'a'*64}
    async def probe(**kwargs):return b'catalog',dict(database_generation='db', applied_seq=sequence[0], schema_version=16, projection_seq=sequence[0], text_search_ready=True, dense_search_ready=True)
    async def rollover(connection,**kwargs):
        events.append(('rollover',sequence[0]));assert sequence[0]==2;writer[0]='new'
    async def readiness(connection):events.append(('readiness',sequence[0]))
    monkeypatch.setattr(native,'handover_writer',rollover)
    binary=NativeBinary('/synthetic/native',1,'1'*64,'pg_tool (PostgreSQL) 17.6')
    database=SimpleNamespace(schema_version=16,database_generation='db',writer_generation='new',schema_revision='s',
        function_revision='f',executor_revision='e',connection=lambda **kwargs:{'host':'private','user':'rkb_reader'})
    factory=native.NativeSnapshotFactory(tools=PhysicalPgTools(binary,binary,binary),database=database,
        identity=('run','nonce','inc'),registry=object(),executor_factory=lambda **kwargs:executor,
        current=lambda b:b is binding,workspace_private_current=lambda path:True,native_uid=1,native_gid=1,probe=probe)
    monkeypatch.setattr(factory,'_sql_readiness',readiness)
    candidate=SimpleNamespace(acquire=acquire,process=SimpleNamespace(returncode=None))
    session=native._NativeSession(candidate,{'applied_sequence':0,'writer_generation':'old'},
        b'{}',binding,executor,[],archived_sequence=archive_cursor)
    factory._sessions.add(session)
    for _ in range(archive_cursor+1,3):await factory.apply(session,{})
    control=native.encode({'database_generation':'db','sequence':2,'journal_head':None,'schema_version':16})
    with pytest.raises(SnapshotError,match='tail_receipts_differ'):
        await factory.finish(session,expected_tail=(),final_head=None,control_bytes=control,expected_schema_version=16)
    assert not any(event[0]=='rollover' for event in events)
    cap=await factory.finish(session,expected_tail=tuple(session.receipts),final_head=None,
        control_bytes=control,expected_schema_version=16)
    assert events==[('apply',n) for n in range(archive_cursor+1,3)]+[('rollover',2),('readiness',2)]
    assert (await cap.verify_for(database,'run','nonce','inc'))['user']=='rkb_reader'
    with pytest.raises(SnapshotError):await replace(cap).verify_for(database,'run','nonce','inc')
    reference=factory.reference(cap);new_binding=object()
    factory.current=lambda value:value is new_binding
    database.wire=lambda:{'database_generation':'db','writer_generation':'new'}
    with pytest.raises(SnapshotError):await cap.verify_for(database,'run','nonce','inc')
    observed=await factory.verify_reference(new_binding,reference)
    assert observed['applied_sequence']==2 and cap._binding is new_binding
    assert (await cap.verify_for(database,'run','nonce','inc'))['user']=='rkb_reader'
