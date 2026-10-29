"""Actual restore prepare deadline, synthetic PG process/bytes only."""
import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from types import SimpleNamespace as NS

import psycopg
import pytest
from regional_knowledge import kaggle_pg_snapshot_native as native


@pytest.mark.asyncio
@pytest.mark.parametrize('becomes_ready',[True,False])
async def test_restore_waits_past_old_startup_cutoff_but_parent_still_bounds_it(tmp_path,monkeypatch,becomes_ready):
    # Scale the real timeout contexts only: 20s ->20ms, parent900s ->900ms.
    # The checkpoint occurs after the old cutoff and before the parent deadline.
    timeout=asyncio.timeout
    monkeypatch.setattr(native.asyncio,'timeout',lambda seconds:timeout(seconds/1000))
    passed_old_cutoff=asyncio.Event()
    checkpoint=asyncio.get_running_loop().call_later(.05,passed_old_cutoff.set)
    closed=[]
    class Process:
        returncode=None
        def send_signal(self,signal):self.returncode=0;closed.append(signal)
        async def wait(self):return self.returncode
    process=Process()
    class Connection:
        async def __aenter__(self):return self
        async def __aexit__(self,*args):pass
        @asynccontextmanager
        async def transaction(self):yield
        async def execute(self,sql):
            self.row=('system',) if 'system_identifier' in sql else ('old',) if 'SELECT writer_generation' in sql else ('db',0)
            return self
        async def fetchone(self):return self.row
    async def connect(**kwargs):
        if not becomes_ready or not passed_old_cutoff.is_set():
            raise psycopg.OperationalError('the database system is starting up')
        return Connection()
    @dataclass
    class Database:
        database_generation:str='db'
        writer_generation:str='new'
        socket:str=str(tmp_path/'socket')
        port:int=5432
        def connection(self,**kwargs):return {}
    async def noop(*args,**kwargs):pass
    async def spawn(*args,**kwargs):return process
    async def blocking(call,*args,**kwargs):return call(*args,**kwargs)
    monkeypatch.setattr(native,'_blocking',blocking)
    monkeypatch.setattr(native,'compatible_versions',lambda *args:True)
    monkeypatch.setattr(native,'digest_file',lambda *args:(1,'a'*64))
    monkeypatch.setattr(native,'unpack_tree',lambda *args:None)
    factory=object.__new__(native.NativeSnapshotFactory)
    factory.database=Database();factory.identity=('run','nonce','incarnation')
    factory.limits=native.SnapshotLimits();factory._operations={};factory._owned_candidates=set();factory._sessions=set()
    factory._current=lambda binding:None;factory._versions=lambda versions:None
    factory._custom_probe=True;factory._workspace=lambda *args:tmp_path
    factory._own_workspace=lambda *args,**kwargs:None;factory._identity=lambda:{}
    factory._run=noop;factory._upgrade_restored=noop
    factory.tools=NS(verify=noop,pg_verifybackup=NS(path='synthetic-verify'),postgres=NS(path='synthetic-postgres'))
    factory.spawn=spawn;factory.connect=connect;factory.native_env={}
    factory.registry=object();factory.executor_factory=lambda **kwargs:object()
    control=b'{}';versions={'pgvector_version':'0.8.2'}
    manifest=native.encode(dict(codec='rkb.pg-physical-base.v1',versions=versions,database_generation='db',
        writer_generation='old',source_control={'sha256':native.sha(control)},
        blob=dict(key='synthetic',size=1,sha256='a'*64,native_version='fixture'),
        unpacked_bytes=1,file_count=1,system_identifier='system',applied_sequence=0))
    arguments=dict(manifest_bytes=manifest,expected_manifest_sha256=native.sha(manifest),archive_path=tmp_path/'unused',
        control_bytes=control,expected_versions=versions,workspace=tmp_path)
    try:
        if becomes_ready:
            session=await factory.prepare(object(),operation_key='restore',**arguments)
            assert passed_old_cutoff.is_set() and session.candidate.process is process
            assert not closed and 'restore' in factory._operations
            await session.candidate.aclose()
        else:
            with pytest.raises(TimeoutError):
                await factory.prepare(object(),operation_key='restore',**arguments)
            assert passed_old_cutoff.is_set(), 'the obsolete20s cutoff aborted before the parent budget'
            assert closed and not factory._operations and not factory._owned_candidates
    finally:
        checkpoint.cancel()
