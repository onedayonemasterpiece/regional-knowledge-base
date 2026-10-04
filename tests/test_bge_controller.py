import importlib.util
from pathlib import Path
from enum import Enum
import json
from uuid import uuid4

def test_kaggle_status_accepts_current_sdk_enum_and_legacy_dictionary():
    path=Path(__file__).resolve().parents[1]/'scripts/production/bge_kaggle_controller.py'
    spec=importlib.util.spec_from_file_location('bge_controller_test',path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    class KernelWorkerStatus(Enum):ERROR=1;RUNNING=2
    class Response:status=KernelWorkerStatus.ERROR
    assert module.provider_phase(Response())=='ERROR'
    assert module.provider_phase({'status':'failed'})=='FAILED'
    assert module.provider_phase({'status':KernelWorkerStatus.RUNNING})=='RUNNING'

def controller():
    spec=importlib.util.spec_from_file_location('bge_controller_test',Path(__file__).resolve().parents[1]/'scripts/production/bge_kaggle_controller.py')
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module

def test_stable_notebook_across_runs_and_frozen_legacy_unknown_launch(monkeypatch,tmp_path):
    module=controller();monkeypatch.setenv('KAGGLE_USERNAME','fixture-owner')
    a={'id':str(uuid4()),'launch_state':'pending','provider_ref':None};b={**a,'id':str(uuid4())}
    assert module.notebook_ref(a,tmp_path)==module.notebook_ref(b,tmp_path)=='fixture-owner/rkb-bge-m3-cpu'
    a['launch_state']='dispatching';folder=tmp_path/a['id'];folder.mkdir()
    (folder/'kernel-metadata.json').write_text(json.dumps({'id':'fixture-owner/legacy-run-slug'}))
    assert module.notebook_ref(a,tmp_path)=='fixture-owner/legacy-run-slug'
    a['provider_ref']='fixture-owner/frozen-slug';monkeypatch.setenv('RKB_BGE_KERNEL_SLUG','changed-slug')
    assert module.notebook_ref(a,tmp_path)=='fixture-owner/frozen-slug'
    monkeypatch.setenv('RKB_BGE_KERNEL_SLUG','../unsafe')
    import pytest
    with pytest.raises(ValueError):module.notebook_ref(b,tmp_path)

def test_unknown_save_binds_source_run_and_version_without_second_push(monkeypatch,tmp_path):
    from regional_knowledge.bge_queue import BgeQueue
    module=controller();queue=BgeQueue(tmp_path/'queue.sqlite');queue.enqueue('actor','query',['fixture'])
    run=next(r for r in queue.maintenance() if r['status']=='starting');ref='fixture-owner/rkb-bge-m3-cpu'
    assert queue.launch_claim(run['id'],provider_ref=ref)
    run=next(r for r in queue.maintenance() if r['id']==run['id'])
    monkeypatch.setattr(module,'notebook_snapshot',lambda api,ref:{'run_id':'previous-run','version':4})
    assert not module.reconcile_launch(None,queue,run,ref)
    assert next(r for r in queue.maintenance() if r['id']==run['id'])['launch_state']=='dispatching'
    monkeypatch.setattr(module,'notebook_snapshot',lambda api,ref:{'run_id':run['id'],'version':5})
    assert module.reconcile_launch(None,queue,run,ref)
    bound=next(r for r in queue.maintenance() if r['id']==run['id']);assert bound['provider_version']==5
    assert queue.launch_claim(run['id'],provider_ref=ref) is None
    monkeypatch.setattr(module,'notebook_snapshot',lambda api,ref:{'run_id':run['id'],'version':6})
    assert not module.reconcile_launch(None,queue,bound,ref)
    import pytest
    with pytest.raises(ValueError):queue.launch_record(run['id'],ref,provider_version=6)
    with pytest.raises(ValueError):queue.launch_record(run['id'],'fixture-owner/another-slug')

def test_run_identity_read_does_not_execute_notebook_source():
    module=controller()
    assert module.source_run_id("RUN_CONFIG = {'run_id':'exact','token':'private'}\nraise RuntimeError('not executed')")=='exact'
    assert module.source_run_id("RUN_CONFIG = __import__('os').environ") is None
