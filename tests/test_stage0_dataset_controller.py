"""Actual controller/queue ordering with existing Dataset publisher and fake SDK."""
from dataclasses import replace
import json
from pathlib import Path
import sys
from types import ModuleType,SimpleNamespace

import pytest

from regional_knowledge.kaggle_stage0 import Stage0Download,Stage0Spec
from test_bge_controller import controller
from test_kaggle_target_runtime import queue_run,spec
from test_stage0_dataset_readiness import Api


@pytest.mark.parametrize('scenario',[
    'warming_then_ready','warming_only','observation_error','writer_conflict','lost_push_ack','already_unknown',
])
def test_main_observes_dataset_before_claim_and_preserves_existing_launch_boundaries(
        tmp_path,monkeypatch,caplog,scenario):
    module=controller();queue,run=queue_run(tmp_path);target=spec();events=[]
    packages={name:'1.0' for name in target.dependency_packages}
    packages.update(torch='2.11.0+cpu',transformers='5.16.1',tokenizers='0.23.1',**{'huggingface-hub':'1.29.0'})
    downloads=tuple(Stage0Download(e['name'],
        'kaggle-dataset://fixture-owner/rkb-bootstrap/7/'+e['name'] if e['name']=='native/initdb'
        else 'https://fixture.example/'+e['name'],e['size'],e['sha256']) for e in target.artifacts)
    target=replace(target,stage0=Stage0Spec(downloads,(),packages,0))
    ref='fixture-owner/rkb-bge-m3-cpu'
    with queue.connect() as db:
        db.execute('CREATE TABLE rkb_writer_generations(writer_generation TEXT,run_id TEXT)')
        if scenario=='writer_conflict':
            db.execute('INSERT INTO rkb_writer_generations VALUES(?,?)',(target.database.writer_generation,'other-run'))
    actual_claim=queue.launch_claim
    def claim(*args,**kwargs):events.append('claim');return actual_claim(*args,**kwargs)
    monkeypatch.setattr(queue,'launch_claim',claim)
    if scenario=='already_unknown':actual_claim(run,provider_ref=ref)
    class FakeApi(Api):
        def __init__(self):
            super().__init__();self.versions=[7]*6
            self.statuses=['preparing','ready','ready'] if scenario=='warming_then_ready' else ['ready']*6
            if scenario=='warming_only':self.statuses=['preparing']*6
            self.pages=[dict(files=[dict(name='native/initdb',total_bytes=1)])]*3
        def authenticate(self):pass
        def get_dataset(self,request):
            events.append('dataset_observation');return super().get_dataset(request)
        def dataset_status(self,ref):
            if scenario=='observation_error':raise RuntimeError('private signed provider detail')
            return super().dataset_status(ref)
        def kernels_push(self,folder,**kwargs):
            events.append('push')
            metadata=json.loads((Path(folder)/'kernel-metadata.json').read_text())
            assert metadata['dataset_sources']==['fixture-owner/rkb-bootstrap/7']
            if scenario=='lost_push_ack':raise RuntimeError('private lost provider response')
            return SimpleNamespace(error=False,ref=ref,version_number=7)
    fake=ModuleType('kaggle.api.kaggle_api_extended');fake.KaggleApi=FakeApi
    for name in ('kaggle','kaggle.api'):monkeypatch.setitem(sys.modules,name,ModuleType(name))
    monkeypatch.setitem(sys.modules,'kaggle.api.kaggle_api_extended',fake)
    monkeypatch.setattr(module,'BgeQueue',lambda path:queue)
    actual_read=Path.read_text
    monkeypatch.setattr(Path,'read_text',lambda path,*args,**kwargs:
        'KAGGLE_USERNAME=fixture-owner\nKAGGLE_KEY=fixture-secret\n' if str(path)=='/home/dev/.env'
        else actual_read(path,*args,**kwargs))
    monkeypatch.setenv('RKB_BGE_QUEUE_PATH',str(queue.path));monkeypatch.setenv('RKB_BGE_BROKER_URL','https://fixture.example')
    monkeypatch.setenv('RKB_BGE_TRANSPORT','wss');monkeypatch.delenv('RKB_BGE_TARGET_CONFIG',raising=False)
    monkeypatch.setenv('RKB_BGE_DOCKER_IMAGE','gcr.io/kaggle-images/python@sha256:'+'a'*64)
    monkeypatch.setattr(module,'worker_source',lambda *args,**kwargs:'RUN_CONFIG = {}')
    monkeypatch.setattr(module,'notebook_snapshot',lambda *args:dict(run_id='previous-unrelated-run',version=4))
    def maintenance():
        with queue.connect(write=False) as db:return [dict(db.execute('SELECT * FROM runs WHERE id=?',(run,)).fetchone())]
    monkeypatch.setattr(queue,'maintenance',maintenance)
    class StopLoop(BaseException):pass
    ticks=[];now=[100.]
    monkeypatch.setattr(module.time,'monotonic',lambda:now[0])
    def tick(seconds):
        assert seconds==5  # Preserve the existing loop, no new blocking sleep.
        ticks.append(list(events));now[0]+=15
        if len(ticks)==3:raise StopLoop
    monkeypatch.setattr(module.time,'sleep',tick)
    with pytest.raises(StopLoop):module.main(target_spec=target)
    state=maintenance()[0]
    if scenario=='warming_then_ready':
        assert 'claim' not in ticks[0] and 'claim' not in ticks[1]
        assert events.index('dataset_observation')<events.index('claim')<events.index('push')
        assert events.count('claim')==events.count('push')==1
        assert state['launch_state']=='dispatched' and state['provider_version']==7
    elif scenario in ('warming_only','observation_error'):
        assert 'claim' not in events and 'push' not in events
        assert state['launch_state']=='pending' and state['provider_version'] is None
        assert events.count('dataset_observation')==2  # t=100/130; not every tick.
    elif scenario=='writer_conflict':
        assert events.count('claim')==1 and 'push' not in events
        assert state['launch_state']=='failed' and state['provider_version'] is None
    elif scenario=='lost_push_ack':
        assert events.count('claim')==events.count('push')==1
        assert state['launch_state']=='dispatching' and state['provider_version'] is None
    else:
        assert not events and state['launch_state']=='dispatching'
    records=[json.loads(r.message) for r in caplog.records if r.message.startswith('{')]
    assert 'fixture-secret' not in json.dumps(records) and 'private' not in json.dumps(records)
