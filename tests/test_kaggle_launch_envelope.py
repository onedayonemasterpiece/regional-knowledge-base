"""Generated launch bytes and real queue boundary tests; no provider/model calls."""
import ast
import base64
import hashlib
import json
from pathlib import Path
import sys
from types import ModuleType,SimpleNamespace
import zlib

import pytest

from regional_knowledge.kaggle_owned_pg import canonical
from regional_knowledge.kaggle_target_launch import (
    source_bundle,MAX_BUNDLE_JSON_BYTES,MAX_NOTEBOOK_SOURCE_BYTES,
)
from test_bge_controller import controller
from test_kaggle_target_runtime import generate,queue_run,spec


def decoder(source,packed=None):
    # Execute the actual shipped decoder statements, stopping before all file
    # writes, runtime imports, control connection and model loading.
    entry=ast.parse(source).body[-1]
    statements=[]
    for node in entry.body:
        if isinstance(node,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='_root' for t in node.targets):break
        statements.append(node)
    tree=ast.Module(body=statements,type_ignores=[])
    if packed is not None:
        class Replace(ast.NodeTransformer):
            def visit_Call(self,node):
                if isinstance(node.func,ast.Attribute) and node.func.attr=='b64decode':
                    node.args[0]=ast.copy_location(ast.Constant(packed),node.args[0])
                return self.generic_visit(node)
        tree=Replace().visit(tree)
    namespace={'RUN_CONFIG':{'run_id':'fixture-run'}}
    exec(compile(ast.fix_missing_locations(tree),'shipped-envelope-decoder','exec'),namespace)
    return namespace


@pytest.fixture
def shipped(tmp_path):
    queue,run=queue_run(tmp_path)
    return generate(queue,run,spec()),queue,run


def test_actual_shipped_target_fits_provider_and_roundtrips_every_pinned_source_byte(shipped):
    source,queue,run=shipped
    assert len(source.encode())<MAX_NOTEBOOK_SOURCE_BYTES==1_000_000
    result=decoder(source)
    _,files,entries=source_bundle(Path(__file__).resolve().parents[1])
    assert result['_bundle']==files and result['_raw']==canonical(files)
    assert len(result['_raw'])<=MAX_BUNDLE_JSON_BYTES
    for entry in entries:
        raw=files[entry['name'].split('/',1)[1]].encode('utf-8')
        assert (len(raw),hashlib.sha256(raw).hexdigest())==(entry['size'],entry['sha256'])
    with queue.connect(write=False) as db:
        expected=json.loads(db.execute('SELECT expected_json FROM bge_runtime_expectations WHERE run_id=?',(run,)).fetchone()[0])
    assert expected['hashes']['executed_source']==hashlib.sha256(source.encode()).hexdigest()


@pytest.mark.parametrize('fault',['invalid_base64','corrupt_zlib','truncated','trailing','concatenated','overflow'])
def test_actual_shipped_decoder_rejects_corrupt_or_oversized_transport_before_any_extraction(shipped,fault):
    source,_,_=shipped
    raw=canonical({'regional_knowledge/synthetic.py':'pass'})
    compressed=zlib.compress(raw)
    packed={'invalid_base64':'!', 'corrupt_zlib':base64.b64encode(b'not-zlib').decode(),
        'truncated':base64.b64encode(compressed[:-1]).decode(),
        'trailing':base64.b64encode(compressed+b'extra').decode(),
        'concatenated':base64.b64encode(compressed+compressed).decode(),
        'overflow':base64.b64encode(zlib.compress(b' '* (MAX_BUNDLE_JSON_BYTES+1))).decode()}[fault]
    with pytest.raises((ValueError,RuntimeError,zlib.error)):
        decoder(source,packed)


def test_provider_source_budget_fails_before_recording_runtime_expectation(tmp_path):
    from regional_knowledge.kaggle_target_launch import compose_target
    queue,run=queue_run(tmp_path)
    config=dict(run_id=run,token='fixture-token',broker='https://fixture.example',transport='wss',launch_nonce=queue.prepare_channel(run))
    template='RUN_CONFIG = {}\n#'+ 'x'*MAX_NOTEBOOK_SOURCE_BYTES +"\nif __name__=='__main__':main()"
    with pytest.raises(ValueError,match='provider notebook source byte budget'):
        compose_target(template,config,spec(),queue=queue,repository=Path(__file__).resolve().parents[1])
    with queue.connect(write=False) as db:
        assert db.execute("SELECT 1 FROM sqlite_master WHERE name='bge_runtime_expectations'").fetchone() is None


@pytest.mark.parametrize('body,status,expected',[
    (b'{"code":400,"message":"Kernel source size exceeds bound https://secret/?token=private"}',400,
        dict(http_status=400,provider_code=400,reason='kernel_source_size_exceeded')),
    (b'{"code":true,"message":"private payload","token":"private"}',True,{}),
    (b'{"code":999,"message":"private"}',999,{}),
    (b'not-json',503,dict(http_status=503)),
    (b'\xff',403,dict(http_status=403)),
    (b'['*7000+b'0'+b']'*7000,400,dict(http_status=400)),
    (b'x'*16385,502,dict(http_status=502)),
    (b'["private"]',401,dict(http_status=401)),
])
def test_provider_error_diagnostics_allowlist_never_returns_provider_payload(body,status,expected):
    error=RuntimeError('private request with signed URL and bearer')
    error.response=SimpleNamespace(status_code=status,content=body,url='https://secret/',headers={'authorization':'private'})
    facts=controller().provider_error_facts(error)
    assert facts==dict(error_type='RuntimeError',**expected)
    assert not any(secret in json.dumps(facts) for secret in ('private','secret','bearer','https://'))


@pytest.mark.parametrize('failure_boundary',['preparation','save','invalid_receipt'])
def test_actual_controller_queue_classifies_not_sent_and_preserves_unknown_without_second_push(tmp_path,monkeypatch,caplog,failure_boundary):
    module=controller();queue,run=queue_run(tmp_path)
    ref='fixture-owner/rkb-bge-m3-cpu';calls=[]
    class FakeApi:
        def authenticate(self):pass
        def kernels_push(self,*args,**kwargs):
            calls.append('push')
            if failure_boundary=='save':
                error=RuntimeError('private provider request')
                error.response=SimpleNamespace(status_code=503,content=b'{"message":"private signed URL"}')
                raise error
            return SimpleNamespace(error=True,ref=ref,version_number=0)
    # Import the normal main path against a fake SDK, with real BgeQueue state.
    fake=ModuleType('kaggle.api.kaggle_api_extended');fake.KaggleApi=FakeApi
    for name in ('kaggle','kaggle.api'):
        monkeypatch.setitem(sys.modules,name,ModuleType(name))
    monkeypatch.setitem(sys.modules,'kaggle.api.kaggle_api_extended',fake)
    monkeypatch.setattr(module,'BgeQueue',lambda path:queue)
    actual_read=Path.read_text
    monkeypatch.setattr(Path,'read_text',lambda path,*args,**kwargs:
        'KAGGLE_USERNAME=fixture-owner\nKAGGLE_KEY=fixture-secret\n' if str(path)=='/home/dev/.env'
        else actual_read(path,*args,**kwargs))
    monkeypatch.setenv('RKB_BGE_QUEUE_PATH',str(queue.path));monkeypatch.setenv('RKB_BGE_BROKER_URL','https://fixture.example')
    monkeypatch.setenv('RKB_BGE_TRANSPORT','http');monkeypatch.delenv('RKB_BGE_TARGET_CONFIG',raising=False)
    def source(*args,**kwargs):
        if failure_boundary=='preparation':raise ValueError('private preparation config')
        return 'RUN_CONFIG = {}'
    monkeypatch.setattr(module,'worker_source',source)
    # Do not create another run after known not-sent; inspect this exact run for
    # two loop iterations. Ambiguous saves must reconcile without another push.
    def maintenance():
        with queue.connect(write=False) as db:return [dict(db.execute('SELECT * FROM runs WHERE id=?',(run,)).fetchone())]
    monkeypatch.setattr(queue,'maintenance',maintenance)
    monkeypatch.setattr(module,'notebook_snapshot',lambda *args:dict(run_id='previous-unrelated-run',version=4))
    class StopLoop(BaseException):pass
    ticks=[]
    def sleep(seconds):
        ticks.append(seconds)
        if len(ticks)==2:raise StopLoop
    monkeypatch.setattr(module.time,'sleep',sleep)
    with pytest.raises(StopLoop):module.main()
    with queue.connect(write=False) as db:state=dict(db.execute('SELECT * FROM runs WHERE id=?',(run,)).fetchone())
    records=[json.loads(r.message) for r in caplog.records if r.message.startswith('{')]
    failure=next(r for r in records if r['event']=='bge_provider_boundary_failed')
    assert state['provider_ref']==ref and state['provider_version'] is None
    if failure_boundary=='preparation':
        assert not calls and state['launch_state']=='failed' and state['status']=='failed'
        assert failure['boundary']=='launch_preparation' and failure['submission_state']=='not_sent'
    else:
        assert calls==['push'] and state['launch_state']=='dispatching' and state['status']=='starting'
        assert failure['boundary']=='save_kernel' and failure['submission_state']=='unknown'
    assert 'private' not in json.dumps(records) and 'fixture-secret' not in json.dumps(records)
