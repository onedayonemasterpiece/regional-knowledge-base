"""Actual spool/journal + fixed native observer over synthetic PG; no effects."""
import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest

from regional_knowledge.control_spool import SpoolConflict
from regional_knowledge.kaggle_source_history_native import NativeSourceHistoryObserver,canonical,digest
from regional_knowledge.kaggle_source_acl import NativeHistoryError
from regional_knowledge.source_history_wss import WssSourceHistoryVerifier
from test_control_spool import env,accept,people,receipt
from test_source_history_verifier import Pg,Cursor
from test_source_read_gate import binding


class NativeCursor(Cursor):
    async def execute(self,sql,args=None):
        self.pg.sql.append(sql)
        if 'SELECT kind,name' in sql:self.rows=list(self.pg.acl)
        elif 's.applied_seq FROM rkb_v4.recovery_state' in sql:self.rows=[('db-source','writer-1',self.pg.applied_seq)]
        elif sql.startswith('SELECT EXISTS'):self.rows=[(self.pg.nonempty,)]
        elif sql.startswith('SELECT sequence,'):
            start,earlier,enabled,_=args
            self.rows=[r for r in self.pg.receipts if enabled and (r[0]>start or r[0] in earlier)]
        elif sql.startswith('SELECT rkb_v4.verify_source_story_origins_'):self.rows=[(getattr(self.pg,'origin_valid',True),)]
        elif sql.startswith('SELECT %s'):self.rows=[args]
        else:self.rows=[]
        return self
    async def fetchmany(self,n):result,self.rows=self.rows[:n],self.rows[n:];return result
    async def close(self):pass


class NativePg(Pg):
    def cursor(self,**kwargs):return NativeCursor(self)
    async def execute(self,sql,args=None):return await self.cursor().execute(sql,args)


def pair(e,*,lease_seconds=10,schema_revision=None):
    b=binding();pg=NativePg();database=SimpleNamespace(database_generation='db-source',writer_generation='writer-1')
    if schema_revision is not None:database.schema_revision=schema_revision
    routes=('source.search','source.fetch','source.withdraw')
    if schema_revision in ('rkb-v4-sql001-008-v1','rkb-v4-sql001-009-v1'):
        routes=routes+('source.import','story.get','story.history','story.search','story.variants',
            'source.fact_read','source.calendar','source.observations')
    if schema_revision=='rkb-v4-sql001-009-v1':routes=routes+('story.edit','story.access','source.catalog')
    async def probe(**kwargs):return b'catalog',dict(database_generation='db-source',applied_seq=pg.applied_seq)
    native=NativeSourceHistoryObserver(acquire=pg.acquire,database=database,identity=('run','nonce','inc'),registry=e.registry,
        current=lambda key:key==('run','inc',1),probe=probe,lease_seconds=lease_seconds)
    native.attached(1)
    verifier=WssSourceHistoryVerifier(e.spool,acquire=None,database=database,observer_sha256=native.code_hash,
        binding_current=lambda current:current is b,routes={k:lambda:None for k in routes},
        expected_catalog_sha256=digest(b'catalog'),expected_acl_sha256=digest(canonical(pg.acl)),writer_generation='writer-1')
    calls=[]
    async def phase(binding,args):
        calls.append(args)
        frame=dict(type='DISPATCH',protocol_version=2,connection_generation=1,id='native-history',claim='claim',
            kind='recovery_phase',run_id='run',launch_nonce='nonce',runtime_incarnation='inc',
            database_generation='db-source',writer_generation='writer-1',phase='source_history',operation_key=digest(canonical(args)),
            arguments=args,request_sha256=digest(canonical({'phase':'source_history','arguments':args})),
            compute_claim='actual-shared-slot',dispatch_generation=1)
        value=(await native(frame))['value']
        if 'error' in value:raise SpoolConflict(value['error'])
        return value
    verifier._phase=phase
    return SimpleNamespace(v=verifier,n=native,pg=pg,b=b,calls=calls)


@pytest.mark.asyncio
async def test_native_empty_genesis_then_exact_delta_and_cheap_renewal(env):
    p=pair(env)
    try:
        proof=await p.v.establish_genesis(p.b)
        assert proof.current() and proof.applied_sequence==0 and not p.n._sessions
        seal=await accept(env)
        # Pending accepted withdrawal is covered as a restriction before apply.
        proof=await p.v.extend(p.b)
        assert proof.current() and len(proof.pending)==1 and proof.applied_sequence==0
        native=receipt(env.cmd);p.pg.applied_seq=1
        p.pg.receipts=[(1,env.cmd.operation_key,digest(env.cmd.encode()),env.registry.resolve(env.cmd).fingerprint,
            seal.manifest.sha256,native.status,native.outcome_sha256,digest(native.outcome),len(native.outcome),
            'db-source',digest(env.cmd.encode()))]
        await env.journal.record_outcome(seal,command=env.cmd,database_generation='db-source',outcome=native.outcome,writer_generation='writer-1')
        proof=await p.v.extend(p.b)
        assert proof.current() and proof.applied_sequence==1 and not proof.pending
        before=len(env.store.reads)
        proof=await p.v.renew(p.b)
        assert proof.current() and not any('/manifests/' in x for x in env.store.reads[before:])
        assert p.calls[-3]['mode']=='renew'
        assert not any('pg_authid' in sql for sql in p.pg.sql)
        assert not p.n._sessions
    finally:await p.n.aclose()


@pytest.mark.asyncio
async def test_uncovered_native_data_and_forged_native_receipt_fail_closed(env):
    p=pair(env)
    try:
        p.pg.nonempty=True
        with pytest.raises(SpoolConflict):await p.v.establish_genesis(p.b)
        assert p.v._coverage is None
    finally:await p.n.aclose()
    p=pair(env)
    try:
        await p.v.establish_genesis(p.b);seal=await accept(env);native=receipt(env.cmd);p.pg.applied_seq=1
        p.pg.receipts=[(1,env.cmd.operation_key,digest(env.cmd.encode()),env.registry.resolve(env.cmd).fingerprint,
            seal.manifest.sha256,'applied',native.outcome_sha256,'f'*64,len(native.outcome),'db-source',digest(env.cmd.encode()))]
        with pytest.raises(SpoolConflict,match='metadata differs'):await p.v.extend(p.b)
        assert p.v._coverage is None
    finally:await p.n.aclose()


@pytest.mark.asyncio
async def test_native_lock_lease_expires_and_json_cannot_create_prefix(env):
    p=pair(env,lease_seconds=.02)
    try:
        args=dict(action='open',mode='full',nonce='a'*48,catalog_sha256=p.v.catalog_hash,acl_sha256=p.v.acl_hash,
            registry_sha256=p.v.registry_hash,observer_sha256=p.n.code_hash,routes=sorted(p.v.routes),prefix=None,pending_sequences=[])
        opened=await p.v._phase(p.b,args)
        assert opened['session'] in p.n._sessions
        await asyncio.sleep(.04)
        assert not p.n._sessions
        with pytest.raises(SpoolConflict,match='expired'):
            await p.v._phase(p.b,dict(action='page',session=opened['session'],after=0))
        with pytest.raises(SpoolConflict,match='retained_prefix'):
            await p.v._phase(p.b,args|dict(mode='delta',prefix='serialized-complete-is-not-permission'))
    finally:await p.n.aclose()


@pytest.mark.asyncio
async def test_actual_transport_invokes_prepared_history_handler_on_same_resource():
    from regional_knowledge.kaggle_bootstrap import PreparedRuntime
    from test_bge_shared_worker import Peer,result
    from test_bge_wss_worker import module,CONFIG
    from test_kaggle_bootstrap import candidate
    transport=module('bge_wss_transport');peer=Peer();stop=asyncio.Event();calls=[]
    class Observer:
        def attached(self,generation):calls.append(('attached',generation))
        def detached(self,generation):calls.append(('detached',generation))
        async def __call__(self,frame):
            calls.append(('observe',frame['compute_claim']))
            return dict(phase='source_history',request_sha256=frame['request_sha256'],value={'nonce':'native'})
    async def read(frame,*,encode):return result(frame)
    async def prepare(encoder,**identity):return PreparedRuntime(read,candidate(),source_history_handler=Observer())
    def load():
        def forbidden(text):raise AssertionError('native history invoked BGE')
        return forbidden
    worker=transport.BgeWssTransport(dict(CONFIG,shared_reads=True),load,lambda:{},prepare_runtime=prepare,
        connector=peer.connect,stop=stop,heartbeat_seconds=.01,maximum_seconds=3)
    task=asyncio.create_task(worker.run())
    try:
        await peer.wait('READY')
        await peer.put(dict(type='DISPATCH',id='history',claim='claim',kind='recovery_phase',phase='source_history',
            operation_key='history',request_sha256='a'*64,arguments={'action':'open'},run_id=CONFIG['run_id'],
            launch_nonce=CONFIG['launch_nonce'],runtime_incarnation=worker.incarnation,database_generation='db',
            writer_generation='writer',compute_claim='same-native-slot',dispatch_generation=1))
        result=await peer.wait('RECOVERY_PHASE_RESULT');await peer.wait('RESOURCE_RELEASED')
        assert result['receipt']['value']=={'nonce':'native'}
        assert ('attached',1) in calls and ('observe','same-native-slot') in calls
        await peer.put(dict(type='RESULT_ACK',job_id='history',claim='claim',status='accepted'))
    finally:stop.set();await task


@pytest.mark.asyncio
async def test_fresh8_history_uses_fixed_v2_origin_proof_and_fact_table_locks(env):
    p=pair(env,schema_revision='rkb-v4-sql001-008-v1')
    try:
        proof=await p.v.establish_genesis(p.b)
        assert proof.current()
        assert any('LOCK TABLE rkb_v4.source_event_dates,rkb_v4.source_observations' in sql for sql in p.pg.sql)
        assert 'SELECT rkb_v4.verify_source_story_origins_v2(%s::uuid[])' in p.pg.sql
        assert not any('verify_source_story_origins_v1(' in sql for sql in p.pg.sql)
        p.pg.origin_valid=False
        with pytest.raises(SpoolConflict,match='story_origin_uncovered'):await p.v.renew(p.b)
        assert p.v._coverage is None
    finally:await p.n.aclose()

@pytest.mark.asyncio
async def test_fresh9_history_requires_exact_manifest_editorial_origin_proof_and_locks(env):
    p=pair(env,schema_revision='rkb-v4-sql001-009-v1')
    try:
        assert (await p.v.establish_genesis(p.b)).current()
        assert 'SELECT rkb_v4.verify_source_story_origins_v3(%s::uuid[])' in p.pg.sql
        assert any('LOCK TABLE rkb_v4.source_manifests,rkb_v4.editorial_story_heads' in sql for sql in p.pg.sql)
        assert not any('verify_source_story_origins_v2(' in sql for sql in p.pg.sql)
        p.pg.origin_valid=False
        with pytest.raises(SpoolConflict,match='story_origin_uncovered'):await p.v.renew(p.b)
        assert p.v._coverage is None
    finally:await p.n.aclose()


def test_editorial_history_classification_uses_exact_installed_contract_and_route(env):
    from regional_knowledge.kaggle_apply import HandlerRegistry
    from regional_knowledge.story_editorial import editorial_handler,STORY_EDIT
    from test_story_editorial import command
    h=editorial_handler(route=STORY_EDIT,check_sha256='a'*64,apply_sha256='b'*64)
    env.spool.registry=HandlerRegistry([h]);env.registry=env.spool.registry
    p=pair(env,schema_revision='rkb-v4-sql001-009-v1')
    c=command()
    assert p.v._classify(c.encode(),None)[1]==h
    p.v.routes.pop('story.edit')
    with pytest.raises(SpoolConflict):p.v._classify(c.encode(),None)
