import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace as N
import pytest
from regional_knowledge.kaggle_pg_snapshot_native import NativeSnapshotFactory,SnapshotError
from regional_knowledge.kaggle_recovery_phases import WorkerRecoveryPhases,PHASES
from regional_knowledge.kaggle_snapshot_io import SnapshotIo
from regional_knowledge.kaggle_target_launch import MODULES

@pytest.mark.asyncio
@pytest.mark.parametrize('exit_code',[0,1])
async def test_fixed_probe_before_restore_finish_returns_stderr_without_provider_or_promotion(tmp_path,monkeypatch,exit_code):
    factory=object.__new__(NativeSnapshotFactory)
    factory._current=lambda binding:None
    factory.limits=N(timeout_seconds=2,max_unpacked_bytes=100)
    factory.database=N(socket='/private/pg/socket',port=5544,writer_generation='new-writer')
    factory.tools=N(pg_basebackup=N(path='/pinned/pg_basebackup'),pg_verifybackup=N(path='/pinned/pg_verifybackup'))
    factory.native_env={'LANG':'C'};factory._identity=lambda:{}
    async def execute(sql):
        assert sql.lstrip().startswith('SELECT') and 'UPDATE' not in sql
        async def fetchone():return (170010,8,8,25299,'old-restored-writer')
        return N(fetchone=fetchone)
    @asynccontextmanager
    async def acquire():yield N(execute=execute)
    factory.acquire=acquire
    async def probe(**kwargs):return {},{}
    factory.probe=probe
    async def prefix(*args):return 'fixture-prefix'
    monkeypatch.setattr('regional_knowledge.kaggle_snapshot_probe.applied_prefix',prefix)
    monkeypatch.setattr('regional_knowledge.kaggle_snapshot_probe.pack_tree',lambda *args:((),160,'inventory',100,'digest'))
    root=tmp_path/'private-probe'
    def workspace(path,required):assert path==root;root.mkdir();return root
    factory._workspace=workspace
    calls=[]
    async def spawn(program,*args,**kwargs):
        calls.append(program)
        assert 'shell' not in kwargs and kwargs['start_new_session'] is True
        stdout=asyncio.StreamReader();stdout.feed_eof()
        stderr=asyncio.StreamReader();stderr.feed_data(b'pg_basebackup: remaining connection slots are reserved' if exit_code else b'');stderr.feed_eof()
        async def wait():return exit_code
        return N(stdout=stdout,stderr=stderr,returncode=exit_code,wait=wait)
    monkeypatch.setattr('regional_knowledge.kaggle_snapshot_probe.asyncio.create_subprocess_exec',spawn)
    worker=WorkerRecoveryPhases(factory=factory,download=lambda *args:None,workspace=lambda key,phase:root)
    worker.snapshot_io=SnapshotIo(factory=factory,tickets=None,transfer=None)
    assert worker._initialized is None and not worker._ready.is_set()
    assert 'snapshot_probe' in PHASES and 'kaggle_snapshot_probe' in MODULES
    with pytest.raises(SnapshotError,match='snapshot_probe_arguments'):
        await worker._execute('snapshot_probe',{'script':'arbitrary'},'bad',('r','i',1),{})
    pending=await worker._execute('snapshot_probe',{},'once',('r','i',1),{})
    await worker.snapshot_io.operations[pending['handle']].task
    status=worker.snapshot_io.status(pending['handle'],('r','i',1));result=status['value']
    assert status['status']=='complete' and result['success']==(exit_code==0)
    assert result['database']['applied_sequence']==25299
    assert not result['saved_promoted'] and result['provider_calls']==0
    assert len(calls)==(2 if exit_code==0 else 1) and not root.exists()
    if exit_code:assert 'remaining connection slots' in result['commands'][0]['stderr']
    assert await worker._execute('snapshot_probe',{},'once',('r','i',1),{})==pending
    assert len(calls)==(2 if exit_code==0 else 1),'same operation must not execute commands again'
    assert not worker.snapshot_io.operations[pending['handle']].upload
    await worker.snapshot_io.aclose()
