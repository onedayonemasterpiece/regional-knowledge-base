"""Actual queue/admission/WSS phases beside blocked native I/O; no model or PG."""
import asyncio
from types import SimpleNamespace as NS

import pytest

from regional_knowledge.kaggle_pg_snapshot_native import NativeSnapshotFactory,SnapshotError,sha
from regional_knowledge.kaggle_recovery_phases import WorkerRecoveryPhases
from regional_knowledge.kaggle_recovery_dispatch import RecoveryPhaseDispatch
from regional_knowledge.kaggle_restore_startup import PreReadyRecoveryBinding
from regional_knowledge.kaggle_snapshot_io import SnapshotIo
from regional_knowledge.native_snapshot_transfer import SnapshotTicket
from test_bge_admission import state,auth,people,begin,answer,release
from test_kaggle_recovery_phases import database,response
from test_kaggle_original_intake import native as original_native


@pytest.mark.asyncio
@pytest.mark.parametrize('kind',['capture','dataset_verify','original_pages'])
async def test_actual_query_admission_between_short_snapshot_jobs_while_io_is_blocked(state,tmp_path,kind,original_native):
    s=state;db=database();binding=PreReadyRecoveryBinding(s.run,'incarnation-a',s.generation,
        db.database_generation,db.writer_generation)
    active=(s.run,'incarnation-a',s.generation)
    native=object.__new__(NativeSnapshotFactory)
    native.database=db;native.identity=(s.run,s.nonce,'incarnation-a');native.limits=NS(timeout_seconds=10)
    native._owned_candidates=set();native._capabilities=set()
    native.current=lambda b:b==active
    started=asyncio.Event();finish_backup=asyncio.Event();upload_started=asyncio.Event();finish_upload=asyncio.Event()
    effects=[]
    class Tickets:
        async def issue_download(self,binding,key,blob):
            return SnapshotTicket('GET',sha(key.encode()),blob.key,blob.size,blob.sha256,10000,
                'https://fixture.invalid/archive',(('If-Match','version'),))
        async def upload_ticket(self,*,size,sha256):
            effects.append('grant')
            return SnapshotTicket('PUT',sha('snapshot:upload'.encode()),'blob',size,sha256,10000,
                'https://fixture.invalid/archive',(('Content-Length',str(size)),
                ('Content-Type','application/octet-stream'),('If-None-Match','*'))).wire()
    class Transfer:
        async def upload(self,ticket,path,*,operation_key):
            effects.append('upload');upload_started.set()
            assert ticket.operation_ref==sha(operation_key.encode())
            await finish_upload.wait()
            return 'immutable-version'
    tickets=Tickets()
    io=SnapshotIo(factory=native,tickets=tickets,transfer=Transfer())
    worker=WorkerRecoveryPhases(factory=native,download=lambda *a:None,workspace=lambda *a:tmp_path)
    worker.snapshot_io=io
    async def capture(binding,*,upload_callback,before_effect,**kwargs):
        effects.append('backup');started.set();await finish_backup.wait()
        await before_effect()
        blob=await upload_callback(tmp_path/'archive',size=1,sha256='a'*64)
        return dict(blob=blob.__dict__,applied_sequence=0)
    native.capture=capture
    async def verify(*args):
        effects.append('download');started.set();await finish_backup.wait()
        return dict(dataset_version=2,base_sequence=0)
    worker._dataset_verify=verify
    original=original_native.handler
    original.database=db;original.identity=native.identity;original.current=lambda b:b==active
    original.attached(s.generation);original.snapshot_io=io
    async def pages(frame):
        effects.append('page');started.set();await finish_backup.wait()
        return dict(physical_page_index=0)
    original._execute=pages
    bridge=RecoveryPhaseDispatch(s.q,database=db,identity=native.identity,binding_current=lambda b:b==binding,
        snapshot_tickets=tickets,timeout_seconds=10)
    async def dispatch_transfer(*args):return None
    bridge.original_intake=NS(dispatch_transfer=dispatch_transfer)
    args=dict(versions={},expected_sequence=0) if kind=='capture' else dict(blob=dict(
        key='blob',size=1,sha256='a'*64,native_version='immutable-version'))
    if kind=='original_pages':args=dict(handle='retained-original',original_sha256='a'*64,index=0)
    producer=asyncio.create_task(bridge.request(binding,kind,args,operation_key='snapshot'))

    async def pump_until(predicate):
        async with asyncio.timeout(8):
            while not predicate():
                if producer.done():
                    await producer
                    raise AssertionError('snapshot completed before the blocked I/O was released')
                work=await s.a.next_channel_work(s.run,s.token,s.generation)
                if work is None:await asyncio.sleep(.001);continue
                s.a.mark_work_sent(work)
                if work.kind=='read':
                    await s.a.finish_read(s.run,s.token,s.generation,answer(work))
                else:
                    frame=dict(type='DISPATCH',protocol_version=2,connection_generation=s.generation,
                        **dict(work.document_payload),compute_claim=work.compute_claim,
                        dispatch_generation=work.dispatch_generation)
                    receipt=await (original(frame) if frame['phase']=='original_pages' else worker(frame))
                    await bridge.receive(response(s,frame,receipt['value']),run_id=s.run,token=s.token,generation=s.generation)
                await release(s,work)
                await asyncio.sleep(0)
    try:
        await pump_until(started.is_set)
        read=await begin(s,'while-native-io-blocked')
        await pump_until(read.result.done)
        assert (await read.result).status=='ok' and not finish_backup.is_set() and not producer.done()
        finish_backup.set()
        if kind=='capture':
            await pump_until(upload_started.is_set)
            read=await begin(s,'while-upload-blocked')
            await pump_until(read.result.done)
            assert (await read.result).status=='ok' and not finish_upload.is_set() and not producer.done()
            finish_upload.set()
        await pump_until(producer.done)
        result=await producer
        assert result.get('applied_sequence',result.get('base_sequence',result.get('physical_page_index')))==0
        assert effects==({'capture':['backup','grant','upload'],'dataset_verify':['download'],'original_pages':['page']}[kind])
    finally:
        if not producer.done():producer.cancel()
        await asyncio.gather(producer,return_exceptions=True)
        await worker.aclose();await bridge.aclose()


@pytest.mark.asyncio
async def test_snapshot_io_rebind_keeps_one_operation_and_requires_current_attachment():
    active=[('run','inc',1)]
    native=NS(_current=lambda b:None if b==active[0] else (_ for _ in ()).throw(SnapshotError('detached')),
              limits=NS(timeout_seconds=2))
    io=SnapshotIo(factory=native,tickets=None,transfer=None)
    started=asyncio.Event();complete=asyncio.Event();calls=[]
    async def work(operation):calls.append(True);started.set();await complete.wait();return {'verified':True}
    first=io.start('same-operation',active[0],work)
    await started.wait()
    active[0]=('run','inc',2)
    with pytest.raises(SnapshotError):io.status(first['handle'],('run','inc',1))
    io.rebind(active[0]);complete.set()
    await io.operations[first['handle']].task
    assert io.status(first['handle'],active[0])==dict(status='complete',value={'verified':True})
    assert io.start('same-operation',active[0],work)==first and len(calls)==1
    await io.aclose()


@pytest.mark.asyncio
async def test_known_migration_busy_retries_one_new_job_and_then_reuses_success(state,monkeypatch):
    from regional_knowledge.kaggle_schema_live import LiveSchemaMigrations
    from regional_knowledge.kaggle_schema_migrate import MigrationDenied
    s=state;db=database();binding=PreReadyRecoveryBinding(s.run,'incarnation-a',s.generation,
        db.database_generation,db.writer_generation)
    native=object.__new__(NativeSnapshotFactory)
    native.database=db;native.identity=(s.run,s.nonce,'incarnation-a');native.current=lambda b:True
    native._owned_candidates=set();native._capabilities=set()
    worker=WorkerRecoveryPhases(factory=native,download=lambda *a:None,workspace=lambda *a:None)
    worker.schema_control=LiveSchemaMigrations(files={'sql/001_test.sql':b'SELECT 1;'},
        acquire=None,current=lambda _:None,download=None,database=db)
    calls=[]
    async def migrate(*args):
        calls.append(True)
        if len(calls)==1:raise MigrationDenied('migration_busy')
        return dict(schema_version=1)
    monkeypatch.setattr(LiveSchemaMigrations,'__call__',migrate)
    bridge=RecoveryPhaseDispatch(s.q,database=db,identity=native.identity,binding_current=lambda b:b==binding)
    async def run():
        task=asyncio.create_task(bridge.request(binding,'schema_migrate',{},operation_key='schema'))
        async with asyncio.timeout(2):
            while not task.done():
                work=await s.a.next_channel_work(s.run,s.token,s.generation)
                if work is None:await asyncio.sleep(.001);continue
                s.a.mark_work_sent(work)
                frame=dict(type='DISPATCH',protocol_version=2,connection_generation=s.generation,
                    **dict(work.document_payload),compute_claim=work.compute_claim,dispatch_generation=work.dispatch_generation)
                receipt=await worker(frame)
                await bridge.receive(response(s,frame,receipt['value']),run_id=s.run,token=s.token,generation=s.generation)
                await release(s,work)
                await asyncio.sleep(0)
        return await task
    try:
        assert (await run())==dict(status='rejected',reason='migration_busy',retryable=True)
        assert await run()==dict(schema_version=1)
        assert await run()==dict(schema_version=1) and len(calls)==2
        with s.q.connect(write=False) as conn:
            assert [r[0] for r in conn.execute("SELECT idempotency FROM jobs WHERE kind='recovery_phase' ORDER BY rowid")]==[
                'recovery:schema','recovery:schema:retry:1']
    finally:
        await worker.aclose();await bridge.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize(('cause','safe_code'),[
    ('snapshot_native_version_mismatch','versions'),
    ('snapshot_applied_prefix_gap','prefix'),
    ('snapshot_disk_budget','disk'),
    ('snapshot_archive_byte_budget','archive'),
    ('snapshot_native_command_failed','subprocess'),
    ('unknown_snapshot','unclassified'),
    ('extra_args','unclassified'),
    ('other_exception','unclassified'),
])
async def test_snapshot_io_failed_status_logs_only_exact_safe_code_once(caplog,cause,safe_code):
    secret='postgres://fixture:DO_NOT_LOG@private.invalid/path?token=fixture'
    if cause=='unknown_snapshot':
        original=SnapshotError('snapshot_disk_budget '+secret)
    elif cause=='extra_args':
        original=SnapshotError('snapshot_disk_budget',secret)
    elif cause=='other_exception':
        original=RuntimeError(secret)
    else:
        original=SnapshotError(cause)
    binding=('run','incarnation',1)
    native=NS(_current=lambda _:None,limits=NS(timeout_seconds=2))
    io=SnapshotIo(factory=native,tickets=None,transfer=None)
    captures=[]
    async def failed_capture(operation):
        captures.append(operation)
        raise original
    started=io.start('one-failed-capture',binding,failed_capture)
    task=io.operations[started['handle']].task
    try:
        await asyncio.gather(task,return_exceptions=True)
        with caplog.at_level('WARNING',logger='regional_knowledge.kaggle_snapshot_io'):
            for _ in range(3):
                with pytest.raises(SnapshotError,match='^snapshot_io_failed$'):
                    io.status(started['handle'],binding)
        records=[record.getMessage() for record in caplog.records
                 if record.name=='regional_knowledge.kaggle_snapshot_io']
        assert records==[f'snapshot_io failed code={safe_code}']
        assert secret not in caplog.text
        assert task.exception() is original
        assert io.start('one-failed-capture',binding,failed_capture)==started
        assert io.operations[started['handle']].task is task and len(captures)==1
    finally:
        await io.aclose()
