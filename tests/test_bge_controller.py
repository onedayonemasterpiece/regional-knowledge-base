import importlib.util
from pathlib import Path
from enum import Enum
import json
from uuid import uuid4



def test_old_unknown_provider_slug_fences_new_launch_even_after_control_reassignment(tmp_path):
    """A migrated/legacy orphan UNKNOWN remains owner of the stable notebook slug.

    A new run can be locally allocated after control-pointer loss or prior
    migration, but must not perform a second provider push while the prior
    exact execution outcome remains unknown.
    """
    from regional_knowledge.bge_queue import BgeQueue
    now=[1000.]
    q=BgeQueue(tmp_path/'unknown-orphan.sqlite',clock=lambda:now[0])
    before=q.request_startup()['run_id']
    ref='owner/rkb-bge-m3-cpu'
    assert q.launch_claim(before,provider_ref=ref) is not None
    with q.connect() as db:
        db.execute('update control set current_run=null,successor=null where id=1')
    second=next(row['id'] for row in q.maintenance()
                if row['id']!=before and row['status']=='starting')
    assert q.launch_claim(second,provider_ref=ref) is None
    with q.connect(write=False) as db:
        original=db.execute('select launch_state from runs where id=?',(before,)).fetchone()[0]
        replacement=db.execute('select launch_state from runs where id=?',(second,)).fetchone()[0]
    assert original=='dispatching' and replacement=='pending'
    assert q.launch_claim(second,provider_ref=ref) is None

    # Even with a version number and stale READY heartbeat this run still
    # has an unknown session outcome; no new save to the same stable slug.
    q.launch_record(before,ref,provider_version=9)
    token=(q.path.parent/'bge-worker-credentials'/before).read_text()
    q.heartbeat(before,token,ready=True)
    now[0]+=121
    assert q.launch_claim(second,provider_ref=ref) is None
    with q.connect(write=False) as db:
        assert db.execute('select launch_state from runs where id=?',(second,)).fetchone()[0]=='pending'



def test_unlinked_ready_run_cannot_be_silently_overwritten(tmp_path):
    """Only the nominated warming successor may share the stable slug."""
    from regional_knowledge.bge_queue import BgeQueue
    q=BgeQueue(tmp_path/'unlinked-ready.sqlite',clock=lambda:1000.)
    run=q.request_startup()['run_id'];ref='owner/rkb-bge-m3-cpu'
    token=q.launch_claim(run,provider_ref=ref)
    q.launch_record(run,ref,provider_version=7)
    q.heartbeat(run,token,ready=True)
    with q.connect() as db:
        db.execute('update control set current_run=null,successor=null where id=1')
    second=next(x['id'] for x in q.maintenance() if x['status']=='starting')
    assert q.launch_claim(second,provider_ref=ref) is None
    with q.connect(write=False) as db:
        assert db.execute('select launch_state from runs where id=?',(second,)).fetchone()[0]=='pending'


def test_healthy_ready_run_allows_one_intentional_warming_successor(tmp_path):
    """Expected bounded hot rotation remains possible for the same slug."""
    from regional_knowledge.bge_queue import BgeQueue
    q=BgeQueue(tmp_path/'hot-rotation.sqlite',clock=lambda:1000.)
    run=q.request_startup()['run_id']
    ref='owner/rkb-bge-m3-cpu'
    token=q.launch_claim(run,provider_ref=ref)
    q.launch_record(run,ref,provider_version=7)
    q.heartbeat(run,token,ready=True)
    with q.connect() as db:
        new=q._new_run(db,1000.)
        db.execute('update control set successor=? where id=1',(new,))
    assert q.launch_claim(new,provider_ref=ref) is not None
    with q.connect(write=False) as db:
        assert db.execute('select launch_state from runs where id=?',(new,)).fetchone()[0]=='dispatching'

def test_preparation_failure_receipt_identifies_stage_without_secrets():
    """Installed logs give root-cause boundary without echoing env/token/URL."""
    module=controller()
    private="SECRET_VALUE_1234 https://private.example/?token=leak"
    exc=ValueError(private)
    expected=('create_private_folder','verify_empty_journal_intent',
              'compose_notebook_source','write_private_notebook','write_private_metadata')
    for stage in expected:
        row=module.provider_boundary_report(exc,'synthetic-run','launch_preparation',stage)
        raw=json.dumps(row)
        assert row==dict(event='bge_provider_boundary_failed',
                         run_id='synthetic-run',boundary='launch_preparation',
                         submission_state='not_sent',error_type='ValueError',
                         preparation_step=stage)
        assert private not in raw and 'SECRET_VALUE_1234' not in raw
    provider=module.provider_boundary_report(RuntimeError(private),
        'synthetic-run','save_kernel','provider_submission')
    assert provider['submission_state']=='unknown'
    assert 'preparation_step' not in provider
    assert private not in json.dumps(provider)
    forged=module.provider_boundary_report(exc,'synthetic-run','launch_preparation',
        'UNVERIFIED_'+private)
    assert forged['preparation_step']=='not_started'


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
