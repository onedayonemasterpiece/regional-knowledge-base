"""Committed handover reconciliation; no native PostgreSQL/provider effects."""
import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from regional_knowledge.kaggle_pg_snapshot_native import NativeSnapshotFactory,NativeBinary,PhysicalPgTools,SnapshotError,encode,sha,_NativeSession
from regional_knowledge.kaggle_recovery_phases import WorkerRecoveryPhases


@pytest.mark.asyncio
async def test_finish_detach_after_handover_reconciles_same_session_without_second_effect(monkeypatch):
    writer=['old'];current=[('run','inc',1)];handover=[];probes=[0]
    class Connection:
        async def execute(self,sql,*args):
            self.rows=[(0,writer[0])] if 'applied_seq,writer_generation' in sql else [('old',True),('new',False)]
            return self
        async def fetchone(self):return self.rows[0]
        async def fetchall(self):return self.rows
        async def commit(self):pass
        @asynccontextmanager
        async def transaction(self):yield
    class Candidate:
        @asynccontextmanager
        async def acquire(self):yield Connection()
    async def rollover(connection,**kwargs):
        assert writer[0]=='old';writer[0]='new';handover.append('committed-once')
    async def probe(**kwargs):
        probes[0]+=1
        if probes[0]==1:current[0]=None  # Actual detach after SQL handover committed.
        return b'catalog',dict(schema_version=16, database_generation='db', applied_seq=0, projection_seq=0, text_search_ready=True, dense_search_ready=True)
    async def readiness(connection):pass
    monkeypatch.setattr('regional_knowledge.kaggle_pg_snapshot_native.handover_writer',rollover)
    binary=NativeBinary('/fixture/postgres',1,'a'*64,'postgres (PostgreSQL) 17.11')
    database=SimpleNamespace(schema_version=16,database_generation='db',writer_generation='new',schema_revision='s',function_revision='f',executor_revision='e')
    factory=NativeSnapshotFactory(tools=PhysicalPgTools(binary,binary,binary),database=database,identity=('run','nonce','inc'),
        registry=None,executor_factory=lambda **kw:None,current=lambda b:b==current[0],workspace_private_current=lambda p:False,
        native_uid=1000,native_gid=1000,probe=probe)
    monkeypatch.setattr(factory,'_sql_readiness',readiness)
    session=_NativeSession(Candidate(),{'applied_sequence':0,'writer_generation':'old'},b'',('run','inc',1),None,[])
    factory._sessions.add(session)
    worker=WorkerRecoveryPhases(factory=factory,download=lambda *a:None,workspace=lambda *a:None)
    worker._sessions['retained-session']=session
    args=dict(handle='retained-session',expected_tail=[],final_head=None,
        control_b64=__import__('base64').b64encode(encode(dict(database_generation='db',sequence=0,journal_head=None,schema_version=16))).decode(),
        expected_schema_version=16)
    frame=dict(type='DISPATCH',protocol_version=2,connection_generation=1,dispatch_generation=1,id='job',claim='claim',kind='recovery_phase',
        run_id='run',launch_nonce='nonce',runtime_incarnation='inc',database_generation='db',writer_generation='new',phase='finish',
        operation_key='same-finish',arguments=args,request_sha256=sha(encode(dict(phase='finish',arguments=args))),compute_claim='resource')
    with pytest.raises(SnapshotError,match='binding_changed'):await worker(frame)
    assert handover==['committed-once'] and not worker._ready.is_set() and not factory._capabilities
    current[0]=('run','inc',2)
    with pytest.raises(SnapshotError):await worker(dict(frame,connection_generation=2,arguments=dict(args,expected_schema_version=15)))
    result=await worker(dict(frame,connection_generation=2))
    assert result['value']['reference'] and worker._ready.is_set() and handover==['committed-once']
    assert await worker(dict(frame,connection_generation=2))==result
    assert probes[0]==2


@pytest.mark.asyncio
async def test_deferred_owner_waits_for_real_reconnect_preserving_original_resource_claim():
    from regional_knowledge.kaggle_recovery_composition import DeferredRecoveryRuntime
    from test_kaggle_generated_restore import recovery
    config=dict(run_id='run',launch_nonce='nonce',target_runtime=dict(database=dict(mode='VERIFIED_INITIALIZED'),recovery=recovery().wire()))
    owner=DeferredRecoveryRuntime(config,executed_source='/fixture')
    owner.attached(1,runtime_incarnation='inc')
    calls=[]
    class Runtime:
        async def recovery(self,frame):
            calls.append(dict(frame))
            if len(calls)==1:
                owner.detached(1)
                raise SnapshotError('native_restore_binding_changed')
            return {'retained':'capability'}
    owner.instance=Runtime()
    original=dict(run_id='run',launch_nonce='nonce',runtime_incarnation='inc',connection_generation=1,dispatch_generation=1,
        compute_claim='same-resource',phase='finish')
    task=asyncio.create_task(owner(original))
    await asyncio.sleep(.03)
    assert not task.done()
    owner.attached(2,runtime_incarnation='inc')
    assert await task=={'retained':'capability'}
    assert calls[1]==dict(original,connection_generation=2)
