import importlib.util
from pathlib import Path

import pytest


def module():
    path=Path(__file__).resolve().parents[1]/'scripts/production/mint_restore_successor.py'
    spec=importlib.util.spec_from_file_location('mint_restore_successor',path)
    value=importlib.util.module_from_spec(spec);spec.loader.exec_module(value);return value


def flat(value,prefix=''):
    if isinstance(value,dict):
        for key,item in value.items():yield from flat(item,prefix+'.'+key)
    else:yield prefix,value


def configs():
    database=dict(database_generation='db',writer_generation='old-writer',mode='VERIFIED_INITIALIZED')
    target=dict(database=dict(database),artifacts=[1,2])
    service=dict(codec='rkb.source-service.v1',run_id='old-run',database=dict(database),stream='s',
        source_import=dict(launch_config_sha256='a'*64,codec='x'),
        source_restore=dict(launch_config_sha256='a'*64,operation_key='old-op',dataset_checkpoint=dict(dataset_version=1)))
    return target,service


def test_successor_changes_only_run_bound_fields():
    m=module();target,service=configs()
    new_target,new_service=m.successor_configs(target,service,run_id='new-run-12345678',
        writer_generation='new-writer',launch_config_sha256='b'*64,release_sha='c'*40)
    assert {k for k,v in flat(new_target) if dict(flat(target))[k]!=v}=={'.database.writer_generation'}
    changed={k for k,v in flat(new_service) if dict(flat(service))[k]!=v}
    assert changed=={'.run_id','.database.writer_generation','.source_import.launch_config_sha256',
        '.source_restore.launch_config_sha256','.source_restore.operation_key'}
    assert new_service['source_restore']['dataset_checkpoint']==service['source_restore']['dataset_checkpoint']
    assert target['database']['writer_generation']=='old-writer'  # inputs are not mutated
    service['database']['database_generation']='other'
    with pytest.raises(ValueError):m.successor_configs(target,service,run_id='r',writer_generation='w',
        launch_config_sha256='b'*64,release_sha='c'*40)


def test_pending_successor_requires_fresh_run_and_settled_writer(tmp_path):
    from regional_knowledge.bge_queue import BgeQueue
    m=module();q=BgeQueue(tmp_path/'q.sqlite',clock=lambda:1000.)
    run=q.request_startup()['run_id']
    with q.connect() as db:
        db.execute('CREATE TABLE rkb_writer_generations(writer_generation TEXT,database_generation TEXT,run_id TEXT,stream TEXT,revision INTEGER,retired INTEGER)')
        db.execute('CREATE TABLE rkb_writer_control(id INTEGER PRIMARY KEY,run_id TEXT,active INTEGER,publication_json TEXT)')
        db.execute("INSERT INTO rkb_writer_control VALUES(1,'old-run',1,NULL)")
    with pytest.raises(ValueError,match='retired and settled'):m.pending_successor(q)
    with q.connect() as db:db.execute('UPDATE rkb_writer_control SET active=0')
    run_id,writer=m.pending_successor(q)
    assert run_id==run and len(writer)==36
    q.launch_claim(run,provider_ref='owner/slug')  # controller already claimed it
    with pytest.raises(ValueError,match='pending current successor'):m.pending_successor(q)
