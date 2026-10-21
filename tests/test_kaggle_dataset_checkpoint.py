import json
from dataclasses import replace
from types import SimpleNamespace as NS

import pytest

from regional_knowledge.kaggle_dataset_base import DatasetFile
from regional_knowledge.kaggle_dataset_checkpoint import SourceDatasetCheckpoints
from regional_knowledge.kaggle_pg_snapshot_native import encode,sha
from test_dataset_recovery_store import prepared
from test_control_spool import env,people,receipt


@pytest.mark.asyncio
async def test_work_cycle_waits_for_quiet_then_resumes_published_version_after_lost_native_readback(tmp_path,monkeypatch):
    p=prepared(tmp_path);store=p.store;clock=[0.];calls=[]
    control=encode(dict(database_generation='db',sequence=8))
    archive=replace(p.checkpoint.archive,name='pg-'+64*'a'+'.blob')
    manifest=encode(dict(database_generation='db',applied_sequence=8,
        blob=dict(size=archive.size,sha256=archive.sha256),
        source_control=dict(key=p.prefix+'source-base-control/'+sha(control),sha256=sha(control))))
    journal=store.export_control()
    def pin(name,data):return DatasetFile(name,len(data),sha(data))
    checkpoint=replace(p.checkpoint,dataset_version=2,base_sequence=8,archive=archive,
        manifest=pin('base-manifest.json',manifest),control=pin('source-control.json',control),
        journal=pin('journal-control.json',journal))
    class Publisher:
        ref=p.publisher.ref
        def read_control(self,file,*,version):
            assert version==2
            return {'base-manifest.json':manifest,'source-control.json':control,'journal-control.json':journal}[file.name]
    store.publisher=Publisher()
    with p.q.connect() as db:
        db.execute('CREATE TABLE rkb_control_spool_meta(id INTEGER PRIMARY KEY,sequence INTEGER)')
        db.execute('INSERT INTO rkb_control_spool_meta VALUES(1,8)')
        db.execute('CREATE TABLE rkb_control_operations(operation_key TEXT,sequence INTEGER,seal_state TEXT,apply_state TEXT,outcome_state TEXT)')
    proof=NS(applied_sequence=8,accepted_sequence=8)
    versions=dict(server_version_num=170010,pgvector_version='0.8.2',catalog_sha256='a'*64,
        model_sha256='b'*64,tokenizer_sha256='c'*64,schema_revision='schema11',
        function_revision='geo6',executor_revision='executor1')
    class Batch:
        def __init__(self,startup,config,publisher):self.config=config;self.tickets=NS()
        async def capture_once(self,binding):
            with p.q.connect() as db:
                if db.execute('SELECT 1 FROM rkb_dataset_seed WHERE operation_key=?',(self.config.operation_key,)).fetchone():return 2
                calls.append('capture_and_publish')
                package=dict(database_generation='db',base_sequence=8,manifest_sha256=checkpoint.manifest.sha256,
                    previous_version=1,files=[dict(name=f.name,size=f.size,sha256=f.sha256)
                        for f in (checkpoint.manifest,archive,checkpoint.control,checkpoint.journal)])
                db.execute('INSERT INTO rkb_dataset_seed VALUES(?,?,?,?,?)',
                    (self.config.operation_key,Publisher.ref,'published',json.dumps(package),2))
            return 2
    monkeypatch.setattr('regional_knowledge.kaggle_dataset_checkpoint._WorkCycleCapture',Batch)
    async def request(binding,phase,args,**kwargs):
        calls.append('native_verify')
        assert phase=='dataset_verify' and args['checkpoint']==checkpoint.wire()
        if calls.count('native_verify')==1:raise TimeoutError('readback lost; publication already happened')
        return dict(dataset_ref=Publisher.ref,dataset_version=2,base_sequence=8,
            archive_sha256=archive.sha256,archive_size=archive.size)
    startup=NS(config=NS(queue=p.q,spool=NS(journal=NS(store=store,prefix=p.prefix.rstrip('/'))),
        source_restore=NS(versions=versions),history_adapter=NS(database=NS(**versions))),
        history=NS(_coverage=proof,catalog_hash=versions['catalog_sha256']),
        restorer=NS(dispatch=NS(request=request)))
    owner=SourceDatasetCheckpoints(startup,clock=lambda:clock[0])
    assert not await owner.refresh('binding')
    clock[0]=29
    assert not await owner.refresh('binding') and not calls
    clock[0]=31
    with pytest.raises(TimeoutError):await owner.refresh('binding')
    assert store.latest_checkpoint.dataset_version==1
    # A restarted owner resumes that exact published package; it cannot publish
    # version 3 just because version 2's native readback response was lost.
    again=SourceDatasetCheckpoints(startup,clock=lambda:clock[0])
    assert await again.refresh('binding')
    assert store.latest_checkpoint==checkpoint
    assert calls==['capture_and_publish','native_verify','native_verify']
    assert not await again.refresh('binding')


@pytest.mark.asyncio
async def test_capture_materializes_crash_gap_outcome_before_export_and_keeps_it_pending(tmp_path,env):
    from regional_knowledge.kaggle_dataset_checkpoint import _WorkCycleCapture
    from regional_knowledge.recovery_journal import RecoveryJournal
    p=prepared(tmp_path);journal=RecoveryJournal(p.store,stream='stream')
    command=env.cmd
    seal=await journal.seal([command],predecessor=None,writer_generation='writer')
    native=receipt(command,database='db')
    with p.q.connect() as db:
        db.execute('CREATE TABLE rkb_control_operations(operation_key TEXT,sequence INTEGER,seal_state TEXT,'
            'apply_state TEXT,outcome_state TEXT,outcome_sha256 TEXT)')
        db.execute('INSERT INTO rkb_control_operations VALUES(?,?,?,?,?,?)',
            (command.operation_key,1,'ACCEPTED','APPLIED','NOT_SENT',native.outcome_sha256))
    async def get(key):
        assert key==command.operation_key
        return NS(command=command,accepted=seal,apply_receipt=native,database_generation='db')
    proof=NS(current=lambda:True,accepted_sequence=1,applied_sequence=1,pending=('not remotely saved',))
    batch=_WorkCycleCapture.__new__(_WorkCycleCapture);batch.queue=p.q
    batch.startup=NS(config=NS(writer_generation='writer',spool=NS(journal=journal,get=get,_source_covered_sequence=1)),
        restorer=NS(physical=NS(limits=NS(max_receipts=100))))
    key=journal.prefix+'/outcomes/'+sha(command.operation_key.encode())
    assert await p.store.read(key,max_bytes=8192) is None
    assert not batch._prefix_ready(proof)
    await batch._prepare_prefix(proof)
    stored=await p.store.read(key,max_bytes=8192)
    assert stored is not None and batch._prefix_ready(proof)
    assert not p.store.remote_durable(key,stored.data)
    assert json.loads(stored.data)['outcome_sha256']==native.outcome_sha256
    assert key in {o['key'] for o in json.loads(p.store.export_control())['objects']}
    with p.q.connect(write=False) as db:
        assert db.execute('SELECT outcome_state FROM rkb_control_operations').fetchone()[0]=='NOT_SENT'
