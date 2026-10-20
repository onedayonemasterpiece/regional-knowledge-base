"""Deterministic native-boundary faults; these are not physical PG acceptance."""
from contextlib import asynccontextmanager
import hashlib
import json

import pytest
from psycopg.pq import TransactionStatus
from psycopg.rows import tuple_row

import regional_knowledge.source_history_verifier as module
from regional_knowledge.source_history_verifier import NativeSourceHistoryVerifier, inspect_acl
from regional_knowledge.control_spool import SpoolConflict
from test_control_spool import env, SOURCE, stage, accept
from test_participants import people
from test_source_read_gate import binding


class Cursor:
    def __init__(self, pg):self.pg,self.rows=pg,[]
    async def __aenter__(self):return self
    async def __aexit__(self,*args):pass
    async def execute(self,sql,args=None):
        self.pg.sql.append(sql)
        if 'SELECT kind,name' in sql:self.rows=list(self.pg.acl)
        elif 'SELECT s.database_generation' in sql:self.rows=[('db-source','writer-1',self.pg.applied_seq)]
        elif sql.startswith('SELECT EXISTS'):self.rows=[(self.pg.nonempty,)]
        elif sql.startswith('SELECT sequence,'):self.rows=list(self.pg.receipts)
        elif sql.startswith('SELECT %s'):self.rows=[args]
        else:self.rows=[]
    async def fetchone(self):return self.rows.pop(0) if self.rows else None
    async def fetchall(self):rows,self.rows=self.rows,[];return rows


class Pg:
    closed=False
    row_factory=tuple_row
    info=type('Info',(),{'transaction_status':TransactionStatus.IDLE})()
    def __init__(self):
        self.row_factory=tuple_row
        self.sql=[];self.nonempty=False;self.applied_seq=0;self.receipts=[]
        self.acl=[('role',role,json.dumps([False,False,False,False,True,False,False,-1,None]))
                  for role in ('rkb_bootstrap','rkb_writer','rkb_reader')]
    def cursor(self,**kwargs):return Cursor(self)
    @asynccontextmanager
    async def transaction(self):yield
    @asynccontextmanager
    async def acquire(self):yield self


def verifier(e,pg,b,monkeypatch):
    class Probe:
        def __init__(self,**kwargs):pass
        async def __call__(self,**kwargs):return b'catalog',{'database_generation':'db-source','applied_seq':pg.applied_seq}
    monkeypatch.setattr(module,'NativeDatabaseProbe',Probe)
    # Keep the real safe-attribute validation and exact native ACL hash check.
    expected=module._json(pg.acl)
    return NativeSourceHistoryVerifier(e.spool,acquire=pg.acquire,binding_current=lambda current:current is b,
        routes={k:(lambda:None) for k in ('source.search','source.fetch','source.withdraw')},
        expected_catalog_sha256=hashlib.sha256(b'catalog').hexdigest(),
        expected_acl_sha256=hashlib.sha256(expected).hexdigest(),writer_generation='writer-1')


@pytest.mark.asyncio
async def test_genesis_requires_empty_native_and_remote_and_immutable_exact_checkpoint(env,monkeypatch):
    pg=Pg();b=binding();v=verifier(env,pg,b,monkeypatch)
    with pytest.raises(SpoolConflict):await v.restore(b)
    pg.nonempty=True
    with pytest.raises(SpoolConflict):await v.establish_genesis(b)
    assert env.spool._source_anchor is None
    pg.nonempty=False
    proof=await v.establish_genesis(b)
    assert proof.current() and proof.nonce
    assert env.spool.facts_current(await env.spool.read_facts(None))
    assert any('LOCK TABLE' in sql for sql in pg.sql)
    checkpoint=env.store.objects[v._checkpoint_key('db-source')]
    assert b'rkb.source-genesis.v1' in checkpoint.data
    restored=await v.restore(b)
    assert restored is not proof and not proof.current() and restored.current()


@pytest.mark.asyncio
async def test_restore_replays_full_classified_chain_and_keeps_unapplied_restriction(env,monkeypatch):
    pg=Pg();b=binding();v=verifier(env,pg,b,monkeypatch)
    await v.establish_genesis(b)
    await accept(env)
    env.spool.invalidate_source_history()
    await v.restore(b)
    facts=await env.spool.read_facts(None,(SOURCE,))
    assert facts.history_complete and facts.covered_sequence==1 and facts.pending
    assert facts.changes[0].restriction and facts.changes[0].sequence==1
    pg.applied_seq=1
    with pytest.raises(SpoolConflict):await v.restore(b)
    assert env.spool._source_anchor is None


@pytest.mark.asyncio
async def test_native_role_membership_privilege_and_acl_drift_invalidate(env,monkeypatch):
    pg=Pg();b=binding();v=verifier(env,pg,b,monkeypatch)
    await v.establish_genesis(b)
    pg.acl.append(('membership','rkb_bootstrap:rkb_reader','[false,false,true]'))
    with pytest.raises(SpoolConflict):await v.restore(b)
    pg.acl.pop()
    attrs=json.loads(pg.acl[1][2]);attrs[0]=True
    pg.acl[1]=('role','rkb_writer',json.dumps(attrs))
    with pytest.raises(SpoolConflict):await inspect_acl(pg)
    assert env.spool._source_anchor is None


@pytest.mark.asyncio
async def test_actual_route_identity_and_generation_changes_revoke_coverage(env,monkeypatch):
    pg=Pg();b=binding();v=verifier(env,pg,b,monkeypatch)
    proof=await v.establish_genesis(b)
    v.routes['source.withdraw']=lambda:None
    assert not proof.current()
    with pytest.raises(SpoolConflict):await v.restore(b)


@pytest.mark.asyncio
async def test_nonempty_unjournaled_source_after_genesis_cannot_be_restored(env,monkeypatch):
    pg=Pg();b=binding();v=verifier(env,pg,b,monkeypatch)
    await v.establish_genesis(b)
    pg.nonempty=True
    with pytest.raises(SpoolConflict,match='canonical applied import origin'):await v.restore(b)
    assert env.spool._source_anchor is None


@pytest.mark.asyncio
async def test_expired_exact_unchanged_prefix_renews_without_traversal(env,monkeypatch):
    pg=Pg();b=binding();v=verifier(env,pg,b,monkeypatch)
    now=[0.0];v.clock=lambda:now[0]
    proof=await v.establish_genesis(b)
    now[0]=31
    assert not proof.current()
    async def forbidden(*args):raise AssertionError('unchanged renewal traversed old journal history')
    monkeypatch.setattr(env.journal,'read_segment',forbidden)
    renewed=await v.renew(b)
    assert renewed.current() and renewed is not proof and renewed.nonce!=proof.nonce
    assert renewed.head==proof.head and renewed.applied_sequence==proof.applied_sequence


@pytest.mark.asyncio
async def test_renewal_reads_only_retained_head_pointer_for_nonempty_verified_chain(env,monkeypatch):
    pg=Pg();b=binding();v=verifier(env,pg,b,monkeypatch)
    await v.establish_genesis(b)
    await accept(env)
    proof=await v.restore(b)
    async def forbidden(*args):raise AssertionError('renewal traversed an already verified segment')
    monkeypatch.setattr(env.journal,'read_segment',forbidden)
    env.store.reads.clear()
    renewed=await v.renew(b)
    assert renewed.current() and renewed.accepted_sequence==1
    assert env.store.reads==[env.journal.head_key,env.journal.head_key]


@pytest.mark.asyncio
async def test_changed_native_prefix_cannot_renew_verified_history(env,monkeypatch):
    pg=Pg();b=binding();v=verifier(env,pg,b,monkeypatch)
    await v.establish_genesis(b)
    pg.applied_seq=1
    with pytest.raises(SpoolConflict,match='prefix changed'):await v.renew(b)
    assert env.spool._source_anchor is None


@pytest.mark.asyncio
async def test_locally_appended_accepted_head_requires_native_prefix_extension(env,monkeypatch):
    pg=Pg();b=binding();v=verifier(env,pg,b,monkeypatch)
    await v.establish_genesis(b)
    await accept(env)
    with pytest.raises(SpoolConflict,match='retained source prefix unavailable'):await v.renew(b)
    assert env.spool._source_anchor is None
