"""Bounded preparation with fake HTTPS/pip; no downloads/install/models/PG."""
from dataclasses import replace
import hashlib
import io
import json
from pathlib import Path
from types import SimpleNamespace
import urllib.request
import urllib.error

import pytest

import regional_knowledge.kaggle_stage0 as stage0
from regional_knowledge.kaggle_stage0 import Stage0Download,Stage0Spec,Stage0Error,prepare_stage0


def download(name='native/postgres',data=b'pinned native fixture',**changes):
    values=dict(name=name,url='https://fixture.example/'+name,size=len(data),sha256=hashlib.sha256(data).hexdigest())
    values.update(changes)
    return Stage0Download(**values)


def spec(item=None,**changes):
    values=dict(downloads=(item or download(),),wheels=(),packages={'fixture-package':'1.2.3'},install_bytes=0)
    values.update(changes)
    return Stage0Spec(**values)


def prepared(root,plan):
    return dict(root=str(root),entries=[dict(name=d.name,size=d.size,sha256=d.sha256,role='code')
        for d in plan.downloads],max_total_bytes=sum(d.size for d in plan.downloads))


class Response(io.BytesIO):
    status=200
    def geturl(self):return 'https://fixture.example/final'


class Opener:
    def __init__(self,data):self.data,self.requests,self.read_sizes=data,[],[]
    def open(self,request,*,timeout):
        self.requests.append((request,timeout))
        response=Response(self.data)
        original=response.read1
        def bounded(size):
            self.read_sizes.append(size)
            return original(size)
        response.read1=bounded
        return response


@pytest.fixture
def sandbox(tmp_path,monkeypatch):
    root=tmp_path/'kaggle-working';root.mkdir()
    monkeypatch.setattr(stage0,'TRUSTED_ROOT',root)
    monkeypatch.setattr(stage0,'_versions',lambda packages:dict(packages))
    opener=Opener(b'pinned native fixture')
    monkeypatch.setattr(stage0.urllib.request,'build_opener',lambda *args:opener)
    def no_pip(*args,**kwargs):raise AssertionError('unexpected installer')
    monkeypatch.setattr(stage0.subprocess,'run',no_pip)
    return root,opener


def test_download_failure_identifies_artifact_without_private_url(sandbox,capsys):
    root,opener=sandbox
    item=download(url='https://fixture.example/native/postgres?signature=private-value')
    plan=spec(item)
    def unavailable(*args,**kwargs):
        raise urllib.error.HTTPError(item.url,403,'private-error-detail',
            {'Authorization':'private-header'},None)
    opener.open=unavailable
    with pytest.raises(Stage0Error,match='stage0_preparation_failed'):
        prepare_stage0(prepared(root,plan),plan.wire(),run_id='download-fixture')
    output=capsys.readouterr().out
    events=[json.loads(line) for line in output.splitlines()]
    failure=next(event for event in events if event['event']=='stage0_download_failed')
    assert failure==dict(event='stage0_download_failed',run_id='download-fixture',
        artifact_name=item.name,artifact_sha256=item.sha256,source_host='fixture.example',
        error_type='HTTPError',http_status=403)
    assert 'private-' not in output and 'signature=' not in output and 'https://' not in output
    assert not (root/item.name).exists()


def test_plan_roundtrip_detaches_versions_and_rejects_ambient_fields():
    packages={'fixture-package':'1.2.3'}
    plan=spec(packages=packages)
    packages['fixture-package']='changed'
    assert plan.packages['fixture-package']=='1.2.3'
    assert Stage0Spec.from_wire(plan.wire())==plan
    with pytest.raises(TypeError):plan.packages['fixture-package']='changed'
    with pytest.raises(ValueError):Stage0Spec.from_wire(dict(plan.wire(),extra='private'))


@pytest.mark.parametrize('change',[
    {'name':'../escape'}, {'name':'/absolute'}, {'name':'double//slash'}, {'name':'dot/./file'},
    {'name':'windows\\file'}, {'url':'http://unsafe.example/file'},
    {'url':'https://user:secret@fixture.example/file'}, {'url':'https://fixture.example/file#fragment'},
    {'size':True}, {'size':0}, {'size':stage0.MAX_FILE_BYTES+1}, {'sha256':'A'*64},
])
def test_download_contract_rejects_unsafe_or_unbounded_inputs(change):
    with pytest.raises(ValueError):download(**change)


def test_stream_verify_publish_cache_and_private_receipt(sandbox):
    root,opener=sandbox
    data=b'x'*(stage0.CHUNK_BYTES+37)
    opener.data=data
    item=download(data=data,executable=True);plan=spec(item)
    manifest=prepared(root,plan)
    first=prepare_stage0(manifest,plan.wire())
    assert (root/item.name).read_bytes()==data
    assert (root/item.name).stat().st_mode&0o777==0o500
    assert first['downloaded_bytes']==len(data) and first['reused_files']==0 and not first['installed']
    assert len(opener.requests)==1 and max(opener.read_sizes)<=stage0.CHUNK_BYTES
    assert opener.requests[0][1]<=30
    second=prepare_stage0(manifest,plan.wire())
    assert second['downloaded_bytes']==0 and second['reused_files']==1 and len(opener.requests)==1
    receipts=list(root.glob('.rkb-stage0-*.json'))
    assert len(receipts)==1 and receipts[0].stat().st_mode&0o777==0o600
    assert 'https://' not in receipts[0].read_text()
    assert not list(root.rglob('.rkb-part-*'))


@pytest.mark.parametrize('data,reason',[(b'short','size'),(b'x'*100,'size'),(b'x'*21,'hash')])
def test_bad_stream_never_publishes_and_cleans_only_own_partial(sandbox,data,reason):
    root,opener=sandbox;opener.data=data
    plan=spec();(root/'unrelated').write_text('preserve')
    with pytest.raises(Stage0Error,match='stage0_download_'+reason+'_mismatch'):
        prepare_stage0(prepared(root,plan),plan.wire())
    assert not (root/'native/postgres').exists()
    assert (root/'unrelated').read_text()=='preserve' and not list(root.rglob('.rkb-part-*'))


def test_wrong_existing_file_refused_before_network_or_mutation(sandbox):
    root,opener=sandbox;plan=spec();(root/'native').mkdir()
    file=root/'native/postgres';file.write_bytes(b'wrong')
    with pytest.raises(Stage0Error,match='existing_file_mismatch'):
        prepare_stage0(prepared(root,plan),plan.wire())
    assert file.read_bytes()==b'wrong' and not opener.requests


@pytest.mark.parametrize('kind',['root','parent','leaf'])
def test_symlinks_are_refused_without_touching_targets(sandbox,kind):
    root,opener=sandbox;plan=spec()
    external=root.parent/'external';external.mkdir();(external/'postgres').write_bytes(b'outside')
    if kind=='root':
        alternate=root.parent/'link';alternate.symlink_to(root,target_is_directory=True)
        manifest=prepared(alternate,plan)
    elif kind=='parent':
        (root/'native').symlink_to(external,target_is_directory=True);manifest=prepared(root,plan)
    else:
        (root/'native').mkdir();(root/'native/postgres').symlink_to(external/'postgres');manifest=prepared(root,plan)
    with pytest.raises(Stage0Error):prepare_stage0(manifest,plan.wire())
    assert (external/'postgres').read_bytes()==b'outside' and not opener.requests


def test_full_missing_and_install_reservation_checked_before_download(sandbox,monkeypatch):
    root,opener=sandbox;plan=spec(install_bytes=500)
    monkeypatch.setattr(stage0.shutil,'disk_usage',lambda path:SimpleNamespace(free=500))
    with pytest.raises(Stage0Error,match='insufficient_disk'):
        prepare_stage0(prepared(root,plan),plan.wire())
    assert not opener.requests and not (root/'native').exists()


def test_download_must_match_final_inventory_exactly(sandbox):
    root,opener=sandbox;plan=spec();manifest=prepared(root,plan)
    manifest['entries'][0]['sha256']='0'*64
    with pytest.raises(Stage0Error):prepare_stage0(manifest,plan.wire())
    assert not opener.requests


def test_redirect_handler_rejects_downgrade_before_request():
    handler=stage0._HTTPSRedirect()
    req=urllib.request.Request('https://fixture.example/start')
    with pytest.raises(Stage0Error,match='unsafe_redirect'):
        handler.redirect_request(req,None,302,'Found',{},'http://unsafe.example/private-token')
    redirected=handler.redirect_request(req,None,302,'Found',{},'https://fixture.example/final')
    assert redirected.full_url=='https://fixture.example/final'


def test_unexpected_nonhttps_final_response_is_rejected(sandbox):
    root,opener=sandbox;plan=spec()
    opener.open=lambda *args,**kwargs:SimpleNamespace(
        __enter__=lambda s:s,__exit__=lambda *a:None)
    class Redirected(Response):
        def geturl(self):return 'http://downgrade.example/private-token'
    opener.open=lambda *args,**kwargs:Redirected(b'pinned native fixture')
    with pytest.raises(Stage0Error,match='unsafe_redirect'):
        prepare_stage0(prepared(root,plan),plan.wire())
    assert not (root/'native/postgres').exists()


def test_one_offline_hash_checked_install_and_actual_version_recheck(sandbox,monkeypatch):
    root,opener=sandbox
    wheel=download(name='wheels/fixture_package-1.2.3-py3-none-any.whl')
    plan=spec(wheel,wheels=(wheel.name,),install_bytes=1024)
    installed=[False];calls=[]
    monkeypatch.setattr(stage0,'_versions',lambda packages:dict(packages) if installed[0] else {'fixture-package':'old'})
    def pip(command,**options):
        calls.append(command)
        assert command[:4]==[stage0.sys.executable,'-I','-m','pip']
        for flag in ('--isolated','--break-system-packages','--no-index','--no-deps','--only-binary=:all:','--require-hashes','--no-cache-dir','--no-compile'):
            assert flag in command
        requirements=Path(command[command.index('-r')+1])
        assert requirements.parent.parent==root and requirements.stat().st_mode&0o777==0o600
        text=requirements.read_text()
        assert '--hash=sha256:'+wheel.sha256 in text and text.startswith('file://') and 'https://' not in text
        assert options['timeout']<=plan.total_seconds
        assert options['stdout']==stage0.subprocess.DEVNULL and options['stderr']==stage0.subprocess.DEVNULL
        installed[0]=True
        return SimpleNamespace(returncode=0)
    monkeypatch.setattr(stage0.subprocess,'run',pip)
    receipt=prepare_stage0(prepared(root,plan),plan.wire())
    assert receipt['installed'] and len(calls)==1
    assert not list(root.glob('.rkb-stage0-*/requirements.txt'))
    assert not prepare_stage0(prepared(root,plan),plan.wire())['installed'] and len(calls)==1


@pytest.mark.parametrize('pip_status,correct_versions,reason',[(1,False,'offline_install_failed'),(0,False,'installed_versions_mismatch')])
def test_installer_failure_never_falls_back_or_retries(sandbox,monkeypatch,pip_status,correct_versions,reason):
    root,opener=sandbox
    wheel=download(name='wheels/fixture_package-1.2.3-py3-none-any.whl');plan=spec(wheel,wheels=(wheel.name,))
    monkeypatch.setattr(stage0,'_versions',lambda packages:{'fixture-package':'old'})
    calls=[]
    def pip(*args,**kwargs):calls.append(args);return SimpleNamespace(returncode=pip_status)
    monkeypatch.setattr(stage0.subprocess,'run',pip)
    with pytest.raises(Stage0Error,match=reason):prepare_stage0(prepared(root,plan),plan.wire())
    assert len(calls)==1 and not list(root.glob('.rkb-stage0-*.json'))


def test_missing_wheel_closure_is_explicit_failure(sandbox,monkeypatch):
    root,opener=sandbox;plan=spec()
    monkeypatch.setattr(stage0,'_versions',lambda packages:{'fixture-package':None})
    with pytest.raises(Stage0Error,match='offline_wheels_required'):
        prepare_stage0(prepared(root,plan),plan.wire())


def test_explicit_trusted_base_needs_no_download_or_installer(sandbox):
    root,opener=sandbox
    plan=spec(downloads=())
    item=download()
    manifest=prepared(root,spec(item))
    result=prepare_stage0(manifest,plan.wire())
    assert not opener.requests and result['downloaded_bytes']==0 and not result['installed']


def test_deadline_covers_stream_and_removes_own_partial(sandbox,monkeypatch):
    root,opener=sandbox;plan=spec(total_seconds=1)
    clock=[0.]
    monkeypatch.setattr(stage0.time,'monotonic',lambda:clock[0])
    class Slow(Response):
        def read1(self,size):
            clock[0]=2.
            return super().read1(size)
    opener.open=lambda *a,**k:Slow(b'pinned native fixture')
    with pytest.raises(Stage0Error,match='deadline_exceeded'):
        prepare_stage0(prepared(root,plan),plan.wire())
    assert not (root/'native/postgres').exists() and not list(root.rglob('.rkb-part-*'))


def test_redirect_closes_without_unbounded_body_read_and_keeps_total_deadline(monkeypatch):
    monkeypatch.setattr(stage0.time,'monotonic',lambda:9.)
    handler=stage0._HTTPSRedirect(deadline=10.)
    calls=[]
    handler.parent=SimpleNamespace(open=lambda request,timeout:calls.append((request,timeout)))
    class RedirectBody:
        closed=False
        def read(self,*a):raise AssertionError('redirect body was drained')
        def close(self):self.closed=True
    body=RedirectBody()
    handler.http_error_302(urllib.request.Request('https://fixture.example/start'),body,302,'Found',
        {'location':'/final'})
    assert body.closed and len(calls)==1 and calls[0][1]==1.
    assert calls[0][0].full_url=='https://fixture.example/final'


def test_redirect_count_is_bounded():
    handler=stage0._HTTPSRedirect()
    request=urllib.request.Request('https://fixture.example/start');request._stage0_redirects=5
    body=SimpleNamespace(close=lambda:None)
    with pytest.raises(Stage0Error,match='redirect_limit'):
        handler.http_error_302(request,body,302,'Found',{'location':'/final'})
