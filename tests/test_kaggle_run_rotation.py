"""Run/Saved pinning without provider effects or process restarts."""
from dataclasses import replace
import json
import stat
import subprocess
import sys

import pytest

from regional_knowledge.bge_queue import BgeQueue
from regional_knowledge.kaggle_dataset_base import DatasetCheckpoint,DatasetFile
from regional_knowledge.kaggle_launch_config import successor_target
from regional_knowledge.kaggle_recovery_composition import SnapshotRecoveryConfig
from test_kaggle_target_runtime import bundled_spec,stage0


def checkpoint(version=1,sequence=7):
    def file(name):return DatasetFile(name,100,'a'*64)
    return DatasetCheckpoint('fixture-owner','rkb-base',version,'synthetic-db',sequence,
        file('base-manifest.json'),file('pg-fixture.blob'),file('source-control.json'),file('journal-control.json'))


def restore_target(base=None):
    target=bundled_spec()
    recovery=SnapshotRecoveryConfig({name:dict(path='bin/'+name,version_output='(PostgreSQL) 17.1')
        for name in ('postgres','pg_basebackup','pg_verifybackup')},('https://storage.googleapis.com',),1000,1000)
    from regional_knowledge.kaggle_current_registry import registry_sha256
    return replace(target,database=replace(target.database,mode='VERIFIED_INITIALIZED',
        cluster=None,uid=None,gid=None,native_paths=None),stage0=stage0(target),
        registry_sha256=registry_sha256(),recovery=recovery,dataset_checkpoint=base or checkpoint())


def test_successor_freezes_verified_saved_and_unique_writer_across_restarts(tmp_path):
    queue=BgeQueue(tmp_path/'q.sqlite',clock=lambda:1000.)
    run=queue.request_startup()['run_id'];base=checkpoint();latest=checkpoint(2,20)
    with queue.connect() as db:
        db.execute('CREATE TABLE rkb_dataset_recovery_store(checkpoint_json TEXT,latest_json TEXT)')
        db.execute('INSERT INTO rkb_dataset_recovery_store VALUES(?,?)',
            (json.dumps(base.wire()),json.dumps(latest.wire())))
    target=restore_target(base);wire=target.wire()
    actual=successor_target(queue,wire,run)
    assert actual.dataset_checkpoint==latest and actual.min_data_seq==20
    assert actual.database.writer_generation==run
    path=tmp_path/'bge-launches'/run/'target.json'
    assert stat.S_IMODE(path.stat().st_mode)==0o600
    assert target.wire()==wire  # The installed template remains unchanged.
    with queue.connect() as db:
        db.execute('UPDATE rkb_dataset_recovery_store SET latest_json=?',(json.dumps(checkpoint(3,25).wire()),))
    assert successor_target(queue,wire,run).wire()==actual.wire()
    with queue.connect() as db:
        successor=queue._new_run(db,queue.clock())
        db.execute('UPDATE control SET successor=? WHERE id=1',(successor,))
    next_target=successor_target(queue,wire,successor)
    assert next_target.database.writer_generation==successor!=run
    assert next_target.dataset_checkpoint==checkpoint(3,25)


@pytest.mark.parametrize('bad',[checkpoint(2,3),replace(checkpoint(2,20),database_generation='other'),
    replace(checkpoint(2,20),dataset_slug='other-base')])
def test_successor_rejects_rollback_or_foreign_saved_before_freezing(tmp_path,bad):
    queue=BgeQueue(tmp_path/'q.sqlite',clock=lambda:1000.)
    run=queue.request_startup()['run_id']
    with queue.connect() as db:
        db.execute('CREATE TABLE rkb_dataset_recovery_store(checkpoint_json TEXT,latest_json TEXT)')
        db.execute('INSERT INTO rkb_dataset_recovery_store VALUES(?,?)',
            (json.dumps(checkpoint().wire()),json.dumps(bad.wire())))
    with pytest.raises(ValueError,match='same restore base'):
        successor_target(queue,restore_target().wire(),run)
    assert not (tmp_path/'bge-launches'/run/'target.json').exists()


def test_uvicorn_rotation_shutdown_reaches_systemd_failure_restart_status():
    # Exercise the actual signal handler in a separate small ASGI process;
    # SIGTERM would be re-raised before main could choose exit status 75.
    code='''
import asyncio,os,signal,uvicorn
async def app(scope,receive,send):
    assert scope['type']=='lifespan'
    assert (await receive())['type']=='lifespan.startup'
    await send({'type':'lifespan.startup.complete'})
    asyncio.get_running_loop().call_later(.1,os.kill,os.getpid(),signal.SIGINT)
    assert (await receive())['type']=='lifespan.shutdown'
    print('graceful-shutdown',flush=True)
    await send({'type':'lifespan.shutdown.complete'})
uvicorn.run(app,host='127.0.0.1',port=0,log_level='error')
raise SystemExit(75)
'''
    result=subprocess.run([sys.executable,'-c',code],capture_output=True,text=True,timeout=10)
    assert result.returncode==75 and result.stdout.strip()=='graceful-shutdown',result.stderr
