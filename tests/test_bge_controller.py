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



def test_ready_lost_heartbeat_version_terminal_never_releases_execution_without_proof(monkeypatch,tmp_path):
    """Even COMPLETE on the pinned notebook version is not session death proof."""
    from regional_knowledge.bge_queue import BgeQueue
    module=controller()
    now=[1000.]
    queue=BgeQueue(tmp_path/'ready-terminal.sqlite',clock=lambda:now[0])
    demand=queue.request_startup()
    run_id=demand['run_id']
    ref='fixture-owner/stable-slug'
    token=queue.launch_claim(run_id,provider_ref=ref)
    queue.launch_record(run_id,ref,provider_version=7)
    queue.heartbeat(run_id,token,ready=True)
    monkeypatch.setattr(module,'notebook_snapshot',
        lambda api,requested:{'run_id':run_id,'version':7})
    observed=[]
    phase=['RUNNING']
    def native_status(api,requested,version):
        observed.append((requested,version))
        return {'status':phase[0]}
    monkeypatch.setattr(module,'version_status',native_status)
    assert module.reconcile_dispatched_terminal(None,queue,
        next(x for x in queue.maintenance() if x['id']==run_id),ref) is None
    assert not observed, 'healthy READY must never poll remote each iteration'
    now[0]+=121
    assert queue.request_startup()['run_id']==run_id
    assert queue.status()['state']=='unknown'
    row=next(x for x in queue.maintenance() if x['id']==run_id)
    assert queue.needs_dispatched_terminal_probe(run_id)
    assert module.reconcile_dispatched_terminal(None,queue,row,ref)=='RUNNING'
    assert observed==[(ref,7)]
    assert queue.request_startup()['run_id']==run_id
    assert len(queue.maintenance())==1
    phase[0]='COMPLETE'
    assert module.reconcile_dispatched_terminal(None,queue,row,ref)=='terminal_unverified'
    with queue.connect(write=False) as db:
        assert db.execute('select status from runs where id=?',(run_id,)).fetchone()[0]=='ready'
        assert db.execute('select startup_state from control where id=1').fetchone()[0]=='pending'
    now[0]+=361
    assert queue.request_startup()['startup_ref']==demand['startup_ref']
    assert queue.request_startup()['status']=='unavailable'
    assert queue.startup_status(demand['startup_ref'])['reason']=='provider_execution_unknown'
    assert len(queue.maintenance())==1


def test_ready_lost_heartbeat_mismatched_version_keeps_unknown_capacity(monkeypatch,tmp_path):
    from regional_knowledge.bge_queue import BgeQueue
    module=controller()
    now=[1000.]
    queue=BgeQueue(tmp_path/'mismatch-terminal.sqlite',clock=lambda:now[0])
    run=queue.request_startup()
    rid=run['run_id']
    token=queue.launch_claim(rid,provider_ref='fixture-owner/stable-slug')
    queue.launch_record(rid,'fixture-owner/stable-slug',provider_version=7)
    queue.heartbeat(rid,token,ready=True)
    now[0]+=121
    monkeypatch.setattr(module,'notebook_snapshot',
        lambda api,ref:{'run_id':rid,'version':8})
    def forbidden(*args,**kwargs):raise AssertionError('wrong version must never count as terminal')
    monkeypatch.setattr(module,'version_status',forbidden)
    row=next(x for x in queue.maintenance() if x['id']==rid)
    assert module.reconcile_dispatched_terminal(None,queue,row,'fixture-owner/stable-slug')=='unknown'
    assert queue.status()['state']=='unknown'
    assert queue.request_startup()['run_id']==rid
    assert len(queue.maintenance())==1


def test_old_ready_run_terminal_does_not_resurrect_completed_idle_demand(tmp_path):
    from regional_knowledge.bge_queue import BgeQueue
    now=[1000.]
    queue=BgeQueue(tmp_path/'idle-terminal.sqlite',clock=lambda:now[0])
    demand=queue.request_startup()
    run_id=demand['run_id'];ref='fixture-owner/stable-slug'
    token=queue.launch_claim(run_id,provider_ref=ref)
    queue.launch_record(run_id,ref,provider_version=7)
    queue.heartbeat(run_id,token,ready=True)
    with queue.connect() as db:
        db.execute("update control set startup_state='ready' where id=1")
    # Simulate a separately execution-attested terminal outcome: this public
    # launch_record path is NOT called for version-only status observations.
    queue.launch_record(run_id,ref,failed=True,provider_version=7)
    with queue.connect(write=False) as db:
        assert db.execute('select startup_state from control where id=1').fetchone()[0]=='ready'
    now[0]+=61
    assert not any(x['status']=='starting' for x in queue.maintenance())
    assert len(queue.maintenance())==1


def test_run_identity_read_does_not_execute_notebook_source():
    module=controller()
    assert module.source_run_id("RUN_CONFIG = {'run_id':'exact','token':'private'}\nraise RuntimeError('not executed')")=='exact'
    assert module.source_run_id("RUN_CONFIG = __import__('os').environ") is None


def test_status_uses_native_frozen_version_not_old_cli_latest():
    from types import SimpleNamespace
    import pytest
    module=controller()
    calls=[]
    class Request:
        version_label=None
    class Client:
        def __enter__(self):
            self.kernels=SimpleNamespace(kernels_api_client=self)
            return self
        def __exit__(self,*args):pass
        def get_kernel_session_status(self,request):
            calls.append((request.user_name,request.kernel_slug,request.version_label))
            return {'status':'COMPLETE'}
    class Api:
        def build_kaggle_client(self):return Client()
        def kernels_status(self,*args):raise AssertionError('latest-by-slug is not exact')
    assert module.version_status(Api(),'owner/stable-slug',85,request_factory=Request)=={'status':'COMPLETE'}
    assert calls==[('owner','stable-slug','v85')]
    for bad in (None,True,0,'85'):
        with pytest.raises(ValueError):
            module.version_status(Api(),'owner/stable-slug',bad,request_factory=Request)
    with pytest.raises(RuntimeError,match='unsupported'):
        module.version_status(Api(),'owner/stable-slug',85,request_factory=lambda:object())


def test_unknown_launch_source_reconciliation_is_bounded_without_changing_original_state(monkeypatch,tmp_path):
    import pytest
    from regional_knowledge.bge_queue import BgeQueue
    module=controller();queue=BgeQueue(tmp_path/'queue.sqlite')
    with queue.connect() as db:run_id=queue._ensure(db,queue.clock(),useful=False)
    ref='fixture-owner/stable-slug';queue.launch_claim(run_id,provider_ref=ref)
    run=next(r for r in queue.maintenance() if r['id']==run_id)
    calls=[];probed={}
    def observation(api,requested):
        calls.append(requested);return {'run_id':'another-run','version':1}
    monkeypatch.setattr(module,'notebook_snapshot',observation)
    assert module.reconcile_launch_due(None,queue,run,ref,probed,now=100) is False
    for now in (100,105,110,129.9):
        assert module.reconcile_launch_due(None,queue,run,ref,probed,now=now) is None
    assert calls==[ref]
    def unavailable(api,requested):calls.append(requested);raise OSError('synthetic native source unavailable')
    monkeypatch.setattr(module,'notebook_snapshot',unavailable)
    with pytest.raises(OSError):module.reconcile_launch_due(None,queue,run,ref,probed,now=130)
    assert module.reconcile_launch_due(None,queue,run,ref,probed,now=135) is None
    assert calls==[ref,ref]
    unchanged=next(r for r in queue.maintenance() if r['id']==run_id)
    assert unchanged['launch_state']=='dispatching' and unchanged['provider_version'] is None
    assert queue.launch_claim(run_id,provider_ref=ref) is None
    monkeypatch.setattr(module,'notebook_snapshot',lambda api,ref:{'run_id':run_id,'version':92})
    assert module.reconcile_launch_due(None,queue,run,ref,probed,now=160) is True
    assert next(r for r in queue.maintenance() if r['id']==run_id)['provider_version']==92
