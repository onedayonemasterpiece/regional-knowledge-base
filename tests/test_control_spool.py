"""Canonical SQLite intake + deterministic remote journal; synthetic data only."""
import asyncio
from contextlib import contextmanager
from dataclasses import replace
import hashlib
import json
import sqlite3
import threading
from types import SimpleNamespace
from unittest.mock import create_autospec

import pytest
import pytest_asyncio

import regional_knowledge.control_spool as module
from regional_knowledge.bge_queue import BgeQueue
from regional_knowledge.control_spool import (
    ControlSpool, SOURCE_WITHDRAW, WITHDRAW_DEPENDENCY, WITHDRAW_PAYLOAD,
    SpoolConflict,SpoolFull,SpoolDenied,SpoolError,
)
from regional_knowledge.kaggle_apply import (
    ApplyReceipt,ApplyUnknown,DependencySpec,HandlerRegistry,HandlerSpec,PgFunction,Revisions,
)
from regional_knowledge.recovery_journal import RecoveryJournal,JournalUnknown,StaleWriter
from test_participants import people
from test_recovery_journal import ConditionalMemoryStore,command

SOURCE='00000000-0000-0000-0000-000000000001'
OTHER='00000000-0000-0000-0000-000000000002'
VERSIONS=Revisions('source-s1','source-f1','source-e1')


def handler(dependency=WITHDRAW_DEPENDENCY):
    return HandlerSpec('domain_command',SOURCE_WITHDRAW,VERSIONS,
        PgFunction('check_source_withdraw_v1','1'*64),PgFunction('apply_source_withdraw_v1','2'*64),
        frozenset({'eligibility_revision'}),dependency=dependency)


def withdraw(person,sequence=1,**changes):
    return command(sequence,person_id=person,route=SOURCE_WITHDRAW,
        schema_revision=VERSIONS.schema,function_revision=VERSIONS.function,executor_revision=VERSIONS.executor,
        ids={'source_id':SOURCE},expected_revisions={'eligibility_revision':'1'},payload=WITHDRAW_PAYLOAD,**changes)


def receipt(cmd,*,status='applied',database='db-source',outcome=b'synthetic exact committed outcome'):
    return ApplyReceipt(database,cmd.operation_key,cmd.sequence,hashlib.sha256(cmd.encode()).hexdigest(),
        status,outcome,hashlib.sha256(outcome).hexdigest(),False)


@pytest_asyncio.fixture
async def env(people,tmp_path):
    queue=BgeQueue(tmp_path/'existing-queue.sqlite')
    store=ConditionalMemoryStore()
    journal=RecoveryJournal(store,stream='source-control-synthetic',max_outcome_bytes=8192)
    registry=HandlerRegistry([handler()])
    spool=ControlSpool(queue,people.registry,journal,registry)
    value=SimpleNamespace(p=people,q=queue,store=store,journal=journal,registry=registry,spool=spool,
        cmd=withdraw(people.owner.subject))
    yield value
    spool.close()


async def stage(e,cmd=None):
    return await e.spool.stage_source_withdraw(e.p.admin,cmd or e.cmd,database_generation='db-source')


async def accept(e,cmd=None,predecessor=None):
    cmd=cmd or e.cmd
    await stage(e,cmd)
    return await e.spool.seal(e.p.admin,cmd.operation_key,predecessor=predecessor,writer_generation='writer-1')


@pytest.mark.asyncio
async def test_legacy_derived_counters_migrate_without_losing_sequence(env):
    e=env;await stage(e);e.spool.close()
    with e.q.connect() as db:
        for name in ('operation_count','pending_count','unknown_count'):
            db.execute(f'ALTER TABLE rkb_control_spool_meta ADD COLUMN {name} INTEGER NOT NULL DEFAULT 99')
        db.execute('CREATE TABLE rkb_control_source_head_meta(id INTEGER PRIMARY KEY,sequence INTEGER,pending_count INTEGER,unknown_count INTEGER)')
        db.execute('INSERT INTO rkb_control_source_head_meta VALUES(1,99,99,99)')
    e.spool=ControlSpool(e.q,e.p.registry,e.journal,e.registry)
    with e.spool._db() as db:
        cols={r[1] for r in db.execute('PRAGMA table_info(rkb_control_spool_meta)')}
        assert not cols.intersection({'operation_count','pending_count','unknown_count'})
        assert db.execute("SELECT 1 FROM sqlite_master WHERE name='rkb_control_source_head_meta'").fetchone() is None
    scope=await e.spool.scope_state()
    assert (scope['sequence'],scope['operation_count'],scope['pending_count'],scope['unknown_count'])==(1,1,1,0)


def test_static_dependency_frozen_and_legacy_fingerprint_unchanged():
    legacy=handler(None)
    expected={'key':legacy.key,'check':(legacy.check.name,legacy.check.definition_sha256),
        'apply':(legacy.apply.name,legacy.apply.definition_sha256),'preconditions':['eligibility_revision'],'transition':None}
    data=json.dumps(expected,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()
    assert legacy.fingerprint==hashlib.sha256(data).hexdigest()
    assert handler().fingerprint!=legacy.fingerprint
    with pytest.raises(ValueError):DependencySpec('private.v1','private_draft',frozenset({'draft_id'}),'shared')
    with pytest.raises(ValueError):replace(handler(),dependency=replace(WITHDRAW_DEPENDENCY,operation='different.v1'))


@pytest.mark.asyncio
async def test_real_canonical_intake_is_immutable_pending_before_remote(env):
    e=env
    entry=await stage(e)
    assert entry.command_bytes==e.cmd.encode() and entry.seal_state=='NOT_SENT'
    assert not e.store.objects
    assert (await e.spool.get(e.cmd.operation_key)).command.encode()==entry.command_bytes
    before=await e.spool.scope_state()
    assert before['pending'] and before['pending_count']==1 and not before['unknown']
    assert not before['scope_complete'] and not before['history_complete']
    same=await stage(e)
    assert same.command_bytes==entry.command_bytes and (await e.spool.scope_state())['operation_count']==1
    e.cmd.ids['source_id']=OTHER
    assert (await e.spool.get(e.cmd.operation_key)).command.ids['source_id']==SOURCE
    with pytest.raises(SpoolConflict):await stage(e)
    with e.spool._db(write=True) as db:
        with pytest.raises(sqlite3.IntegrityError):db.execute('UPDATE rkb_control_operations SET source_id=?',(OTHER,))


@pytest.mark.asyncio
async def test_current_owner_route_and_person_are_authority_not_command_fields(env):
    e=env
    with pytest.raises(SpoolDenied):await e.spool.stage_source_withdraw(e.p.personal,e.cmd,database_generation='db-source')
    with pytest.raises(SpoolDenied):await stage(e,replace(e.cmd,person_id=e.p.other.subject))
    with pytest.raises(SpoolDenied):await e.spool.stage_source_withdraw(e.p.owner.subject,e.cmd,database_generation='db-source')
    assert (await e.spool.scope_state())['operation_count']==0 and not e.store.objects


@pytest.mark.asyncio
@pytest.mark.parametrize('changes',[
    {'operation_key':'x'*4097}, {'operation_key':'é'*2049},
    {'ids':{'source_id':'00000000000000000000000000000001'}},
    {'expected_revisions':{'eligibility_revision':'01'}},
    {'expected_revisions':{'eligibility_revision':str(2**63)}},
    {'ids':{'source_id':SOURCE,'person_id':'fake-grant'}},
    {'payload':b'{"withdrawn":false}'},
])
async def test_malformed_control_never_enters_promised_spool(env,changes):
    with pytest.raises((ValueError,SpoolDenied)):
        await stage(env,replace(env.cmd,**changes))
    assert not env.store.objects and (await env.spool.scope_state())['operation_count']==0


@pytest.mark.asyncio
async def test_sequence_and_exact_key_database_binding_conflicts(env):
    e=env
    await stage(e)
    with pytest.raises(SpoolConflict):await stage(e,replace(e.cmd,operation_key='other-global-key'))
    with pytest.raises(SpoolConflict):await stage(e,replace(e.cmd,logical_time='2026-10-20T12:00:00Z'))
    with pytest.raises(SpoolConflict):await e.spool.stage_source_withdraw(e.p.admin,e.cmd,database_generation='new-db')
    assert (await e.spool.scope_state())['operation_count']==1


@pytest.mark.asyncio
async def test_budget_reserves_outcome_before_remote_acceptance_and_never_ttl_deletes(env):
    e=env
    e.spool.max_bytes=1
    with pytest.raises(SpoolFull):await stage(e)
    assert not e.store.objects and (await e.spool.scope_state())['operation_count']==0
    e.spool.max_bytes=100000
    await stage(e)
    e.spool.clock=lambda:10**12
    assert (await e.spool.get(e.cmd.operation_key)).seal_state=='NOT_SENT'
    assert (await e.spool.barriers_for((SOURCE,))).barriers[0].restriction_active
    e.spool.max_bytes=1 # existing promised rows remain resumable, new intake backpressures.
    await e.spool.seal(e.p.admin,e.cmd.operation_key,predecessor=None,writer_generation='writer-1')
    with pytest.raises(SpoolFull):await stage(e,withdraw(e.p.owner.subject,2))


@pytest.mark.asyncio
async def test_known_outcome_releases_reserve_and_legacy_byte_counter_is_removed(env):
    e=env
    await accept(e)
    with e.spool._db() as db:
        reserved=e.spool._used_bytes(db)
        assert 'used_bytes' not in {r[1] for r in db.execute('PRAGMA table_info(rkb_control_spool_meta)')}
    capacity=2*reserved-100
    e.spool.max_bytes=capacity
    next_command=withdraw(e.p.owner.subject,2)
    with pytest.raises(SpoolFull):await stage(e,next_command)
    applied=receipt(e.cmd)
    await e.spool.record_apply(e.cmd.operation_key,applied)
    with e.spool._db() as db:
        actual=e.spool._used_bytes(db)
    assert actual==reserved-e.spool.max_outcome_bytes+len(applied.outcome)
    await e.spool.record_apply(e.cmd.operation_key,applied)
    with e.spool._db() as db:
        assert e.spool._used_bytes(db)==actual
    # A stale old ledger must neither survive nor block admission on upgrade.
    with e.spool._db(write=True) as db:
        db.execute('ALTER TABLE rkb_control_spool_meta ADD COLUMN used_bytes INTEGER NOT NULL DEFAULT 536200848')
    e.spool.close()
    e.spool=ControlSpool(e.q,e.p.registry,e.journal,e.registry,max_bytes=capacity)
    with e.spool._db() as db:
        assert e.spool._used_bytes(db)==actual
        assert 'used_bytes' not in {r[1] for r in db.execute('PRAGMA table_info(rkb_control_spool_meta)')}
    # The unchanged capacity now admits work because the first outcome is known.
    await stage(e,next_command)
    before=await e.spool.get(e.cmd.operation_key)
    assert before.apply_receipt.outcome==applied.outcome and before.command.encode()==e.cmd.encode()
    with e.spool._db() as db:
        total=e.spool._used_bytes(db)
    e.spool.close()
    e.spool=ControlSpool(e.q,e.p.registry,e.journal,e.registry)
    with e.spool._db() as db:
        assert e.spool._used_bytes(db)==total


@pytest.mark.asyncio
async def test_seal_unknown_preserves_exact_predecessor_provisional_barrier_and_retry(env):
    e=env
    await stage(e)
    e.store.faults.append(('cas-before','/head'))
    with pytest.raises(JournalUnknown):
        await e.spool.seal(e.p.admin,e.cmd.operation_key,predecessor=None,writer_generation='writer-1')
    entry=await e.spool.get(e.cmd.operation_key)
    assert entry.seal_state=='SENT_UNKNOWN' and entry.accepted is None
    scope=await e.spool.scope_state()
    assert scope['unknown_count']==1 and scope['accepted_sequence']==0
    assert (await e.spool.barriers_for((SOURCE,))).barriers[0].restriction_active
    accepted=await e.spool.seal(e.p.admin,e.cmd.operation_key,writer_generation='writer-1')
    assert await e.journal.verify_accepted(accepted,e.cmd)==entry.command_sha256
    scope=await e.spool.scope_state()
    assert not scope['unknown'] and scope['accepted_sequence']==1 and scope['pending']
    assert await e.spool.seal(e.p.admin,e.cmd.operation_key,writer_generation='writer-1')==accepted


@pytest.mark.asyncio
async def test_remote_accepted_lost_ack_recovery_and_restart(env):
    e=env
    await stage(e)
    e.store.fail_head_read_after_cas=True
    with pytest.raises(JournalUnknown):
        await e.spool.seal(e.p.admin,e.cmd.operation_key,predecessor=None,writer_generation='writer-1')
    original=(await e.spool.get(e.cmd.operation_key)).command_bytes
    e.spool.close()
    e.spool=ControlSpool(e.q,e.p.registry,e.journal,e.registry)
    e.store.fail_head_read_after_cas=False;e.store.read_failures.clear()
    accepted=await e.spool.seal(e.p.admin,e.cmd.operation_key,writer_generation='writer-1')
    assert accepted.manifest.last_sequence==1 and (await e.spool.get(e.cmd.operation_key)).command_bytes==original
    assert len([k for k in e.store.objects if '/segments/' in k])==1


@pytest.mark.asyncio
async def test_retired_writer_cannot_publish_but_pending_restriction_survives(env):
    e=env
    await stage(e);e.store.writer='writer-new'
    with pytest.raises(StaleWriter):
        await e.spool.seal(e.p.admin,e.cmd.operation_key,predecessor=None,writer_generation='writer-1')
    assert (await e.spool.scope_state())['unknown']
    accepted=await e.spool.seal(e.p.admin,e.cmd.operation_key,writer_generation='writer-new')
    assert accepted.manifest.last_sequence==1


@pytest.mark.asyncio
async def test_unaccepted_seal_rechecks_current_identity_after_waiting_for_operation_lock(env):
    e=env
    await stage(e)
    await e.spool._lock.acquire()
    task=asyncio.create_task(e.spool.seal(e.p.admin,e.cmd.operation_key,predecessor=None,writer_generation='writer-1'))
    await asyncio.sleep(0)
    e.p.corpus.put('rkb_users',[{'id':e.p.owner.subject,'status':'inactive'}])
    e.spool._lock.release()
    with pytest.raises(SpoolDenied):await task
    assert not e.store.objects and (await e.spool.get(e.cmd.operation_key)).seal_state=='NOT_SENT'


@pytest.mark.asyncio
async def test_already_accepted_replay_retains_original_author_after_current_identity_disabled(env):
    e=env
    original=await accept(e)
    e.p.corpus.put('rkb_users',[{'id':e.p.owner.subject,'status':'inactive'}])
    assert await e.spool.seal(e.p.admin,e.cmd.operation_key,writer_generation='writer-1')==original
    await e.spool.record_apply(e.cmd.operation_key,receipt(e.cmd))
    assert (await e.spool.publish_outcome(e.cmd.operation_key,writer_generation='writer-1')).command_sha256==hashlib.sha256(e.cmd.encode()).hexdigest()
    with pytest.raises(SpoolDenied):await stage(e,withdraw(e.p.owner.subject,2))


@pytest.mark.asyncio
async def test_unknown_but_remotely_accepted_recovers_after_current_identity_disabled(env):
    e=env
    await stage(e);e.store.fail_head_read_after_cas=True
    with pytest.raises(JournalUnknown):
        await e.spool.seal(e.p.admin,e.cmd.operation_key,predecessor=None,writer_generation='writer-1')
    e.p.corpus.put('rkb_users',[{'id':e.p.owner.subject,'status':'inactive'}])
    e.store.fail_head_read_after_cas=False;e.store.read_failures.clear()
    version=e.store.version
    located=await e.spool.seal(e.p.admin,e.cmd.operation_key,writer_generation='writer-1')
    assert located.manifest.last_sequence==1 and e.store.version==version
    assert (await e.spool.get(e.cmd.operation_key)).seal_state=='ACCEPTED'


@pytest.mark.asyncio
async def test_unknown_unpublished_orphan_does_not_newly_accept_after_disable(env):
    e=env
    await stage(e);e.store.faults.append(('cas-before','/head'))
    with pytest.raises(JournalUnknown):
        await e.spool.seal(e.p.admin,e.cmd.operation_key,predecessor=None,writer_generation='writer-1')
    e.p.corpus.put('rkb_users',[{'id':e.p.owner.subject,'status':'inactive'}])
    version=e.store.version
    with pytest.raises(SpoolDenied):await e.spool.seal(e.p.admin,e.cmd.operation_key,writer_generation='writer-1')
    assert e.store.version==version and await e.journal.head() is None
    assert (await e.spool.get(e.cmd.operation_key)).seal_state=='SENT_UNKNOWN'


@pytest.mark.asyncio
async def test_apply_unknown_then_exact_receipt_and_remote_outcome(env,monkeypatch):
    e=env
    await accept(e)
    native_apply=module.apply_accepted
    async def lost(*args,**kwargs):raise ApplyUnknown('synthetic lost SQL commit ACK')
    lost_call=create_autospec(native_apply,side_effect=lost)
    monkeypatch.setattr(module,'apply_accepted',lost_call)
    with pytest.raises(ApplyUnknown):await e.spool.apply(e.cmd.operation_key,object(),writer_generation='writer-1')
    assert lost_call.await_args.kwargs['journal'] is e.journal
    assert lost_call.await_args.kwargs['registry'] is e.registry
    assert lost_call.await_args.kwargs['command'].encode()==e.cmd.encode()
    assert (await e.spool.get(e.cmd.operation_key)).apply_state=='SENT_UNKNOWN'
    exact=receipt(e.cmd)
    async def reconciled(*args,**kwargs):return exact
    monkeypatch.setattr(module,'apply_accepted',create_autospec(native_apply,side_effect=reconciled))
    assert await e.spool.apply(e.cmd.operation_key,object(),writer_generation='writer-1')==exact
    assert (await e.spool.scope_state())['unknown_count']==0
    remote=await e.spool.publish_outcome(e.cmd.operation_key,writer_generation='writer-1')
    assert remote.outcome_sha256==exact.outcome_sha256
    assert not (await e.spool.scope_state())['pending']
    assert (await e.spool.barriers_for((SOURCE,))).barriers[0].restriction_active
    assert await e.spool.publish_outcome(e.cmd.operation_key,writer_generation='writer-1')==remote
    with e.spool._db() as db:assert db.execute('SELECT count(*) FROM rkb_control_receipts').fetchone()[0]==1


@pytest.mark.asyncio
async def test_rejected_domain_outcome_never_reopens_remotely_accepted_restriction(env):
    e=env
    await accept(e)
    await e.spool.mark_apply_sent(e.cmd.operation_key)
    await e.spool.record_apply(e.cmd.operation_key,receipt(e.cmd,status='rejected_precondition'))
    await e.spool.publish_outcome(e.cmd.operation_key,writer_generation='writer-1')
    facts=await e.spool.barriers_for((SOURCE,))
    assert facts.barriers[0].restriction_active and facts.barriers[0].intent_remote_accepted
    assert facts.barriers[0].apply_state=='REJECTED_PRECONDITION'
    assert facts.barriers[0].accepted_restriction.sequence==1


@pytest.mark.asyncio
async def test_changed_or_wrong_database_outcome_cannot_settle_original(env):
    e=env
    await accept(e)
    with pytest.raises(SpoolConflict):await e.spool.record_apply(e.cmd.operation_key,receipt(e.cmd,database='fork'))
    await e.spool.record_apply(e.cmd.operation_key,receipt(e.cmd))
    with pytest.raises(SpoolConflict):await e.spool.record_apply(e.cmd.operation_key,receipt(e.cmd,outcome=b'different'))
    assert (await e.spool.get(e.cmd.operation_key)).apply_receipt.outcome==receipt(e.cmd).outcome


@pytest.mark.asyncio
async def test_unknown_remote_outcome_keeps_original_pg_bytes_and_retry(env):
    e=env
    await accept(e);await e.spool.record_apply(e.cmd.operation_key,receipt(e.cmd))
    e.store.faults.append(('cas-before','/outcomes/'))
    with pytest.raises(JournalUnknown):await e.spool.publish_outcome(e.cmd.operation_key,writer_generation='writer-1')
    entry=await e.spool.get(e.cmd.operation_key)
    assert entry.apply_state=='APPLIED' and entry.outcome_state=='SENT_UNKNOWN'
    assert entry.apply_receipt.outcome==receipt(e.cmd).outcome and (await e.spool.scope_state())['unknown']
    await e.spool.publish_outcome(e.cmd.operation_key,writer_generation='writer-1')
    assert not (await e.spool.scope_state())['unknown']


@pytest.mark.asyncio
async def test_scope_and_topk_views_preserve_older_accepted_barrier_without_remote_or_writes(env,monkeypatch):
    e=env
    await accept(e)
    second=withdraw(e.p.owner.subject,2)
    await stage(e,second)
    e.store.reads.clear()
    original=e.spool._db
    with original(write=True) as writer:
        writer.execute('UPDATE rkb_control_spool_meta SET sequence=sequence')
        @contextmanager
        def readonly(*,write=False):
            assert not write,'ready barrier lookup took SQLite writer'
            with original() as db:yield db
        monkeypatch.setattr(e.spool,'_db',readonly)
        facts=await e.spool.barriers_for((SOURCE,OTHER))
        barrier=facts.barriers[0]
        assert barrier.sequence==2 and not barrier.intent_remote_accepted
        assert barrier.accepted_restriction.sequence==1 and barrier.accepted_restriction.operation_key==e.cmd.operation_key
        assert barrier.pending_sequence==2 and barrier.restriction_active
        whole=await e.spool.scope_state()
        assert whole['pending_count']==2 and whole['accepted_sequence']==1
        independent=await e.spool.scope_state((OTHER,))
        assert independent['operation_count']==0 and not independent['scope_complete']
        assert not facts.scope_complete and not facts.history_complete
    assert not e.store.reads


@pytest.mark.asyncio
async def test_bounded_views_pagination_and_process_owner(env):
    e=env
    with pytest.raises(BlockingIOError):ControlSpool(e.q,e.p.registry,e.journal,e.registry)
    await stage(e)
    for i in range(2,5):await stage(e,withdraw(e.p.owner.subject,i))
    page=await e.spool.pending_page(limit=2)
    assert len(page['items'])==2 and page['has_more'] and not page['scope_complete']
    tail=await e.spool.pending_page(after_key=page['next_after_key'],limit=2)
    assert len(tail['items'])==2 and not tail['has_more']
    for values,method in (([SOURCE]*161,e.spool.barriers_for),([SOURCE]*33,e.spool.scope_state)):
        with pytest.raises(ValueError):await method(values)


@pytest.mark.asyncio
async def test_cancelled_local_write_drains_before_owner_can_close(env,monkeypatch):
    e=env;entered=threading.Event();resume=threading.Event();original=e.spool._stage
    def blocked(*args):
        entered.set();assert resume.wait(2)
        return original(*args)
    monkeypatch.setattr(e.spool,'_stage',blocked)
    task=asyncio.create_task(stage(e))
    assert await asyncio.to_thread(entered.wait,2)
    task.cancel()
    with pytest.raises(SpoolError):e.spool.close()
    assert not task.done()
    resume.set()
    with pytest.raises(asyncio.CancelledError):await task
    assert (await e.spool.get(e.cmd.operation_key)).seal_state=='NOT_SENT'
    assert not e.store.objects


@pytest.mark.asyncio
async def test_recovered_job_settlement_requires_exact_remote_durability(env):
    e=env
    await accept(e)
    await e.spool.record_apply(e.cmd.operation_key,receipt(e.cmd))
    with pytest.raises(SpoolConflict,match='durable outcome'):
        e.spool._settle_recovered_jobs(e.cmd.operation_key)
    with e.q.connect(write=False) as db:
        assert db.execute('select outcome_state from rkb_control_operations').fetchone()[0]=='NOT_SENT'
