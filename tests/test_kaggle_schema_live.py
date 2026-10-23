import asyncio
from contextlib import asynccontextmanager
import hashlib
from types import SimpleNamespace as NS

import pytest

from regional_knowledge.kaggle_schema_live import (
    LiveSchemaMigrations, bundle_hash, encode_sql_bundle, decode_sql_bundle,
)
from regional_knowledge.kaggle_schema_migrate import MigrationDenied
from regional_knowledge.kaggle_schema_owner import SourceSchemaMigrations, request_migration
from regional_knowledge.kaggle_pg_snapshot_native import BlobRef, SnapshotError
from regional_knowledge.kaggle_warm_maintenance import WarmHandlerPatchApproval
from test_control_spool import env, people
from test_kaggle_apply import PG
from test_kaggle_pg_driver import database
from test_source_activation_pg import image_pg_sql18


def test_live_bundle_is_canonical_complete_sql_only():
    files={'sql/001_first.sql':b'SELECT 1;'}
    assert dict(decode_sql_bundle(encode_sql_bundle(files))) == files
    for raw in (b'{"sql/001_first.sql":"Uw==","sql/001_first.sql":"Uw=="}',
                b'{"module.py":"Uw=="}',
                b'{"sql/002_gap.sql":"Uw=="}'):
        with pytest.raises(MigrationDenied):
            decode_sql_bundle(raw)


@pytest.mark.asyncio
@pytest.mark.parametrize('delta',[
    {'module.py':'print(1)'}, {'../sql/002_escape.sql':'SELECT 2;'},
    {'sql/003_gap.sql':'SELECT 3;'}, {'sql/001_first.sql':'SELECT 9;'},
    {'sql/002_next.sql':''}, {'sql/002_next.sql':'x'*(2*1024*1024)},
])
async def test_inline_sql_invalid_delta_never_opens_pg(delta):
    old={'sql/001_first.sql':b'SELECT 1;'}
    def forbidden():raise AssertionError('invalid SQL delivery reached PG')
    owner=LiveSchemaMigrations(files=old,acquire=forbidden,current=lambda _:None,
                               download=None,database=None)
    with pytest.raises(MigrationDenied):
        await owner('schema_migrate',dict(target_schema_version=2,
            bundle_sha256='a'*64,sql_files=delta),'invalid',{})
    assert dict(owner.files)==old


@PG
@pytest.mark.asyncio
async def test_pg17_inline_nullable_column_keeps_session_frontier_and_ledger(image_pg_sql18):
    from test_kaggle_schema_migrate import files
    from test_source_activation_pg import DB,WRITER
    connection=image_pg_sql18['connection'];old=files();version=len(old)+1
    name=f'sql/{version:03d}_inline_probe.sql'
    body='ALTER TABLE rkb_v4.sources ADD COLUMN inline_probe text;'
    target=dict(old,**{name:body.encode()})
    @asynccontextmanager
    async def acquire():yield connection
    async def state():
        async with connection.transaction():
            pid=(await (await connection.execute('SELECT pg_backend_pid()')).fetchone())[0]
            frontier=await (await connection.execute('SELECT * FROM rkb_v4.read_frontier')).fetchall()
            return pid,frontier
    before=await state()
    owner=LiveSchemaMigrations(files=old,acquire=acquire,current=lambda _:None,
        download=None,database=NS(database_generation=DB,writer_generation=WRITER))
    args=dict(target_schema_version=version,bundle_sha256=bundle_hash(target),sql_files={name:body})
    first=await owner('schema_migrate',args,'inline-probe',{})
    assert first['schema_version']==version and first['checkpoint_required']
    assert await state()==before
    async with connection.transaction():
        assert (await (await connection.execute("SELECT is_nullable FROM information_schema.columns "
            "WHERE table_schema='rkb_v4' AND table_name='sources' AND column_name='inline_probe'")).fetchone())==('YES',)
        ledger=await (await connection.execute('SELECT * FROM rkb_v4.schema_migrations ORDER BY version')).fetchall()
    assert (await owner('schema_migrate',args,'inline-probe',{}))['schema_version']==version
    async with connection.transaction():
        assert await (await connection.execute('SELECT * FROM rkb_v4.schema_migrations ORDER BY version')).fetchall()==ledger
    with pytest.raises(MigrationDenied,match='file_changed'):
        await owner('schema_migrate',dict(args,sql_files={name:body+' -- changed'}),'changed',{})
    assert await state()==before


@pytest.mark.asyncio
async def test_live_sql_delivery_pins_bytes_retains_process_and_never_changes_old_sql(tmp_path):
    old={'sql/001_first.sql':b'SELECT 1;'}
    new=dict(old, **{'sql/002_next.sql':b'SELECT 2;'})
    raw=encode_sql_bundle(new)
    blob=BlobRef('approved/sql-prefix',len(raw),hashlib.sha256(raw).hexdigest(),'immutable-v1')
    downloaded=[]
    async def download(ref,op,**kwargs):
        assert ref==blob and kwargs['transfer_operation']==op
        path=tmp_path/'verified-blob';path.write_bytes(raw);downloaded.append(path)
        return path
    owner=LiveSchemaMigrations(files=old,acquire=None,current=lambda _:None,
                               download=download,database=None)
    args=dict(artifact=WarmHandlerPatchApproval(blob,'a'*40).wire(),
              expected_bundle_sha256=bundle_hash(old))
    result=await owner('schema_bundle_stage',args,'same-operation',{'download_ticket':{}})
    assert result['latest_schema_version']==2 and dict(owner.files)==new
    assert len(downloaded)==1 and not downloaded[0].exists()
    assert await owner('schema_bundle_stage',args,'same-operation',{}) == result
    assert len(downloaded)==1
    with pytest.raises(MigrationDenied,match='arguments'):
        await owner('schema_migrate',{'sql':'DROP SCHEMA'},'bad',{})


@pytest.mark.asyncio
async def test_schema_migration_requires_actual_pg_writer_and_reconciles_same_version(monkeypatch):
    old={'sql/001_first.sql':b'SELECT 1;'}
    files=dict(old, **{'sql/002_next.sql':b'SELECT 2;'})
    state=dict(version=1,retired=True);effects=[]
    class Cursor:
        def __init__(self,row):self.row=row
        async def fetchone(self):return self.row
    class Connection:
        @asynccontextmanager
        async def transaction(self):yield
        async def execute(self,query,*args,**kwargs):
            if 'w.retired' in query:return Cursor(('db','writer',state['retired']))
            if 'to_regclass' in query:return Cursor(('rkb_v4.schema_migrations',))
            return Cursor((state['version'],))
    @asynccontextmanager
    async def acquire():yield Connection()
    async def migrate(connection,files,*,before_file):
        await before_file(connection)
        if state['version']==2:return ()
        effects.append(2);state['version']=2
        return (2,)
    monkeypatch.setattr('regional_knowledge.kaggle_schema_live.migrate',migrate)
    owner=LiveSchemaMigrations(files=old,acquire=acquire,current=lambda _:None,
        download=None,database=NS(database_generation='db',writer_generation='writer'))
    args=dict(target_schema_version=2,bundle_sha256=bundle_hash(files),
              sql_files={'sql/002_next.sql':'SELECT 2;'})
    with pytest.raises(MigrationDenied,match='writer_changed'):
        await owner('schema_migrate',args,'same-step',{})
    assert not effects and dict(owner.files)==old
    state['retired']=False
    result=await owner('schema_migrate',args,'same-step',{})
    assert result['schema_version']==2 and result['checkpoint_required']
    again=await owner('schema_migrate',args,'same-step',{})
    assert again['schema_version']==2 and again['checkpoint_required']
    assert effects==[2]
    # Actual fixed recovery WSS envelope retains the same result on repeat.
    from regional_knowledge.kaggle_recovery_phases import WorkerRecoveryPhases
    from regional_knowledge.kaggle_pg_snapshot_native import NativeSnapshotFactory, encode, sha
    native=object.__new__(NativeSnapshotFactory)
    native.database=owner.database;native.identity=('run','nonce','inc')
    native._current=lambda binding:None
    worker=WorkerRecoveryPhases(factory=native,download=lambda *a:None,workspace=lambda *a:None)
    worker.schema_control=owner
    frame=dict(type='DISPATCH',protocol_version=2,connection_generation=1,id='job',claim='claim',
        kind='recovery_phase',run_id='run',launch_nonce='nonce',runtime_incarnation='inc',
        database_generation='db',writer_generation='writer',phase='schema_migrate',operation_key='same-step',
        arguments=args,request_sha256=sha(encode(dict(phase='schema_migrate',arguments=args))),
        compute_claim='compute',dispatch_generation=1)
    receipt=await worker(frame)
    assert receipt['value']['checkpoint_required']
    assert await worker(frame)==receipt and effects==[2]
    with pytest.raises(SnapshotError,match='dispatch_binding'):
        await worker(dict(frame,writer_generation='stale'))


@pytest.mark.asyncio
async def test_controller_holds_barrier_across_lost_checkpoint_and_restart_before_next_ddl(env):
    effects=[];checkpoints=[];lose=[True];ledger=[11]
    binding=NS(run_id='run')
    async def request(binding,phase,args,*,operation_key):
        assert phase=='schema_migrate'
        assert args['target_schema_version']==13
        assert args['sql_files']=={'sql/012_next.sql':'SELECT 12;','sql/013_next.sql':'SELECT 13;'}
        effects.extend(range(ledger[0]+1,14));ledger[0]=13
        return dict(schema_version=13,latest_schema_version=13,checkpoint_required=True)
    async def checkpoint(binding,*,schema_version,operation_key,expected_sequence):
        checkpoints.append((schema_version,operation_key))
        assert expected_sequence==0
        if lose[0]:lose[0]=False;raise TimeoutError('lost provider readback')
    proof=NS(current=lambda:True,accepted_sequence=0,applied_sequence=0)
    startup=NS(config=NS(queue=env.q,spool=env.spool,writer_owner=object(),
                        writer_generation='writer',history_adapter=NS(database=NS(database_generation='db'))),
        history=NS(_coverage=proof),binding_current=lambda _:True,
        restorer=NS(complete=True,dispatch=NS(request=request)),
        dataset_checkpoints=NS(checkpoint_schema=checkpoint))
    options=dict(target_schema_version=13,bundle_sha256='a'*64,
                 database_generation='db',writer_generation='writer',run_id='run',
                 sql_files={'sql/012_next.sql':'SELECT 12;','sql/013_next.sql':'SELECT 13;'})
    key=request_migration(env.q,**options)
    assert request_migration(env.q,**options)==key
    owner=SourceSchemaMigrations(startup)
    assert not owner.intake_allowed() and owner.writes_allowed()
    startup.config.writer_owner=None
    with pytest.raises(SnapshotError,match='current_writer'):
        await owner.advance(binding)
    assert not effects
    startup.config.writer_owner=object()
    with pytest.raises(TimeoutError):await owner.advance(binding)
    assert not owner.writes_allowed() and effects==[12,13]
    restarted=SourceSchemaMigrations(startup)
    assert not restarted.writes_allowed()
    assert await restarted.advance(binding)
    assert effects==[12,13] and checkpoints[0]==checkpoints[1]
    assert restarted.intake_allowed() and restarted.writes_allowed()


@pytest.mark.asyncio
async def test_schema_capture_releases_spool_lock_for_same_prefix_read_renewal(env):
    from regional_knowledge.kaggle_dataset_checkpoint import _SchemaCycleCapture
    active=[True]
    owner=NS(pending=lambda:object() if active[0] else None,writes_allowed=lambda:not active[0])
    renewed=NS(database_generation='db',current=lambda:True,accepted_sequence=0,applied_sequence=0)
    startup=NS(config=NS(spool=env.spool,queue=env.q),schema_migrations=owner,
        history=NS(_coverage=renewed),binding_current=lambda _:True,
        restorer=NS(physical=NS(limits=NS(max_receipts=100))))
    capture=object.__new__(_SchemaCycleCapture)
    capture.startup=startup;capture.queue=env.q
    old=NS(database_generation='db',current=lambda:False,accepted_sequence=0,applied_sequence=0)
    async with capture.capture_guard('same-binding'):
        assert not env.spool._lock.locked()
        async with asyncio.timeout(1):
            async with env.spool._lock:
                assert capture._prefix_ready(old)
    active[0]=False
    with pytest.raises(SnapshotError,match='write_barrier_changed'):
        async with capture.capture_guard('same-binding'):
            raise AssertionError('must not capture without retained barrier')
