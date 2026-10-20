"""Fixed empty-base native proof and owned shutdown; no PG/model/provider I/O."""
import base64
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest

from regional_knowledge.kaggle_pg_snapshot_native import NativeSnapshotFactory,NativeBinary,PhysicalPgTools,_NativeSession,encode,sha,SnapshotError
from regional_knowledge.kaggle_snapshot_drill import EmptySnapshotDrill,drill_database
from test_kaggle_target_runtime import spec


@pytest.mark.asyncio
@pytest.mark.parametrize('shutdown_unknown',[False,True])
async def test_empty_drill_proves_isolated_restore_and_owned_shutdown_without_serving_handover(monkeypatch,shutdown_unknown):
    db=replace(spec(fresh=True).database,writer_generation='serving-writer')
    identity=('run','nonce','inc');binding=('run','inc',1);active=[binding]
    serving_writer=[db.writer_generation];restore_writer=[db.writer_generation];events=[];shutdown=[not shutdown_unknown]
    class Connection:
        def __init__(self,serving):self.serving=serving
        async def execute(self,sql,*args):
            writer=serving_writer if self.serving else restore_writer
            if 'applied_seq,writer_generation' in sql:self.rows=[(0,writer[0])]
            elif 'system_identifier' in sql:self.rows=[('actual-synthetic-system',)]
            elif 'writer_generation' in sql:self.rows=[(writer[0],)]
            elif 'generation,retired' in sql:self.rows=[('serving-writer',True),(restore_writer[0],False)]
            else:self.rows=[]
            return self
        async def fetchone(self):return self.rows[0]
        async def fetchall(self):return self.rows
        async def commit(self):pass
        @asynccontextmanager
        async def transaction(self):yield
    @asynccontextmanager
    async def serving_acquire():yield Connection(True)
    class Candidate:
        process=SimpleNamespace(returncode=None)
        @asynccontextmanager
        async def acquire(self):yield Connection(False)
        async def aclose(self):
            if self.process.returncode is None:
                events.append('close-drill-B')
                if shutdown[0]:self.process.returncode=0
    async def probe(**kwargs):return b'catalog',dict(database_generation=db.database_generation,applied_seq=0,projection_seq=0,
        text_search_ready=True,dense_search_ready=True,schema_revision=db.schema_revision,function_revision=db.function_revision,executor_revision=db.executor_revision)
    async def sql_ready(connection):events.append('serving-A-readiness' if connection.serving else 'drill-B-readiness')
    binary=NativeBinary('/fixture/native',1,'a'*64,'postgres (PostgreSQL) 17.11')
    serving=NativeSnapshotFactory(tools=PhysicalPgTools(binary,binary,binary),database=db,identity=identity,registry=None,
        executor_factory=lambda **kw:None,current=lambda b:b==active[0],workspace_private_current=lambda p:False,
        native_uid=1000,native_gid=1000,acquire=serving_acquire,probe=probe)
    serving._sql_readiness=sql_ready
    async def download(blob,operation,**kwargs):
        events.append('scoped-GET');assert kwargs['ticket']=={'exact':'version'};return '/fixture/archive'
    owner=EmptySnapshotDrill(serving,download=download,workspace=lambda *args:'/fixture/private-B')
    assert owner.factory.database==drill_database(db,identity)
    assert owner.factory.database.socket!=db.socket and owner.factory.database.writer_generation!=db.writer_generation
    monkeypatch.setattr('regional_knowledge.kaggle_snapshot_drill.shutil.disk_usage',lambda path:SimpleNamespace(free=2**30))
    candidate=Candidate()
    async def prepare(binding,**kwargs):
        events.append('verified-restore-B')
        assert kwargs['expected_versions']['catalog_sha256']==sha(b'catalog')
        assert kwargs['workspace']=='/fixture/private-B'
        manifest=__import__('json').loads(kwargs['manifest_bytes'])
        session=_NativeSession(candidate,manifest,kwargs['control_bytes'],binding,None,[])
        owner.factory._sessions.add(session);owner.factory._owned_candidates.add(candidate)
        return session
    owner.factory.prepare=prepare;owner.factory._sql_readiness=sql_ready
    async def handover(connection,**kwargs):
        assert not connection.serving
        events.append('handover-B');restore_writer[0]=kwargs['new_writer']
    monkeypatch.setattr('regional_knowledge.kaggle_pg_snapshot_native.handover_writer',handover)
    versions=dict(server_version_num=170011,pgvector_version='0.8.2',catalog_sha256=sha(b'catalog'),model_sha256='a'*64,
        tokenizer_sha256='b'*64,schema_revision=db.schema_revision,function_revision=db.function_revision,executor_revision=db.executor_revision)
    manifest=dict(blob=dict(key='private/blob',size=100,sha256='c'*64,native_version='"v1"'),applied_sequence=0,journal_head=None,
        writer_generation=db.writer_generation,system_identifier='actual-synthetic-system',unpacked_bytes=200,versions=versions)
    control=encode(dict(database_generation=db.database_generation,sequence=0,journal_head=None,heads=[],catalog_sha256=sha(b'catalog')))
    args=dict(manifest_b64=base64.b64encode(encode(manifest)).decode(),manifest_ref={'sha256':sha(encode(manifest))},blob=manifest['blob'],
        control_b64=base64.b64encode(control).decode(),expected_versions=versions,database=owner.factory.database.wire(),identity=list(identity),restore_operation='stable-drill')
    with pytest.raises(SnapshotError,match='fixed_database_identity'):
        await owner('drill_prepare',dict(args,database=db.wire()),'op',binding,{'download_ticket':{'exact':'version'}})
    prepared=await owner('drill_prepare',args,'op',binding,{'download_ticket':{'exact':'version'}})
    finished=await owner('drill_finish',dict(handle=prepared['handle'],control_b64=args['control_b64'],catalog_sha256=sha(b'catalog')),'finish',binding,{})
    assert serving_writer[0]=='serving-writer' and events.count('handover-B')==1
    if shutdown_unknown:
        with pytest.raises(SnapshotError,match='shutdown_unknown'):
            await owner('drill_close',{'reference':finished['reference']},'close',binding,{})
        assert not owner.proofs and owner.can_reconcile('drill_close',{'reference':finished['reference']})
        shutdown[0]=True
    proof=await owner('drill_close',{'reference':finished['reference']},'close',binding,{})
    assert candidate.process.returncode==0 and not owner.factory._capabilities
    assert events.index('drill-B-readiness')<events.index('close-drill-B')
    assert proof['owned_peak_bytes_bound']==600
    assert await owner('drill_verify',{'reference':proof['reference']},'fresh-verify',binding,{})==proof
    with pytest.raises(SnapshotError,match='shutdown_proof_unknown'):
        await owner('drill_verify',{'reference':'caller-constructed'},'bad',binding,{})
    serving_writer[0]='another-writer'
    with pytest.raises(SnapshotError,match='serving_writer_changed'):
        await owner('drill_verify',{'reference':proof['reference']},'changed',binding,{})
    assert events.count('handover-B')==1
