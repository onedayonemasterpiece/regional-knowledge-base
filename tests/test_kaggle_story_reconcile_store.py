"""Bounded synthetic accepted-intake/store tests. Native PG itself is NOT RUN."""
from __future__ import annotations

import base64
from dataclasses import replace
from types import SimpleNamespace

import pytest

from regional_knowledge.kaggle_story_reconcile import ReconcileError
from regional_knowledge.kaggle_story_reconcile_native import intent_bytes,sha,canonical,validate_reconcile_command,decode_payload
from regional_knowledge.kaggle_story_reconcile_store import (
    NativeAcceptedReconcileStore, ReconcilePending, TrustedWriteScope)
from test_kaggle_story_reconcile_native import intent, source_pinned_command_type, ACTOR, CASE


class SpoolError(Exception):
    pass


class FakeJournal:
    def __init__(self):
        self.last=0
        self.stream='synthetic'
        self.prefix='recovery/v1/'+sha(self.stream.encode())
        self.max_segment_bytes=1024*1024
        self.commands={}
        self.outcomes={}
        self.covered=True

    async def head(self):
        return SimpleNamespace(reference=SimpleNamespace(last_sequence=self.last)) if self.last else None

    async def remote_outcome(self,key):
        value=self.outcomes.get(key)
        return SimpleNamespace(data=value) if value is not None and self.covered else None

    async def _read(self,key,maximum):
        data=self.commands.get(key)
        return SimpleNamespace(data=data) if data is not None and len(data)<=maximum else None


class FakeLock:
    async def __aenter__(self):return None
    async def __aexit__(self,*args):return False


class FakeSpool:
    def __init__(self):
        self.entries={}
        self._lock=FakeLock()
        self._stage=object()
        self.journal=FakeJournal()
        self.seals=[]
        self.fail_unregistered=False
        self.facts=SimpleNamespace(pending=False,unknown=False,scope_sequence=0,covered_sequence=0)

    async def get(self,key):
        if key not in self.entries:
            raise SpoolError('control operation not found')
        return self.entries[key]

    async def scope_state(self):
        return dict(pending=False,unknown=False,sequence=self.journal.last,
                    accepted_sequence=self.journal.last)

    def _validate(self,command,database_generation):
        if self.fail_unregistered:
            raise ReconcileError('no exact semantic handler; route unregistered')
        assert database_generation=='gen-1'
        validate_reconcile_command(command)
        return command,b'fixture',object()

    async def _write(self,method,frozen,data,handler,database_generation):
        assert method is self._stage
        entry=SimpleNamespace(command=frozen,accepted=None,seal_state='NEW',
                              outcome_state='NONE',apply_receipt=None)
        self.entries[frozen.operation_key]=entry
        return entry

    async def seal(self,context,key,**kwargs):
        entry=self.entries[key]
        assert kwargs['writer_generation']=='writer-1'
        assert entry.command.sequence==self.journal.last+1
        self.seals.append(key)
        self.journal.last=entry.command.sequence
        self.journal.commands[self.journal.prefix+'/commands/'+sha(key.encode())]=entry.command.encode()
        entry.accepted=object()
        return object()

    async def read_facts(self,_):return self.facts
    def facts_current(self,facts):return facts is self.facts


class FakeAuthority:
    def __init__(self):
        self.revoked=False
        self.checks=0

    async def scope(self,actor_id,*,source_ids,mutate):
        assert actor_id==ACTOR
        assert source_ids in ((), (intent().anchor_source.source_id,intent().peer_source.source_id))
        async def current():
            self.checks+=1
            if self.revoked:
                raise PermissionError('current linked owner authority revoked')
        await current()
        return TrustedWriteScope(actor_id,SimpleNamespace(route='owner'),
            SimpleNamespace(database_generation='gen-1'),current)


class FakeNativePg:
    """A fake PG CAS result, not an execution of SQL016."""
    def __init__(self):
        self.rows={}
        self.effects=0
        self.last_effect=None
        self.permission=True

    async def read(self,actor,key):
        if not self.permission:
            raise PermissionError('native read denied')
        return self.rows.get((actor,key))

    @staticmethod
    def record(intent,*,version=1,step=0,attempted=False,case_id=None,done=False,
               rejected=False,rejected_step=None):
        raw=intent_bytes(intent)
        return dict(operation_key=intent.operation_key,actor_id=intent.actor_id,
                    intent_b64=base64.b64encode(raw).decode(),request_sha256=intent.request_sha256,
                    intent_sha256=sha(raw),version=version,step_index=step,
                    attempted=attempted,case_id=case_id,done=done,
                    rejected=rejected,rejected_step=rejected_step)

    def apply(self,command):
        value=decode_payload(command.payload)
        parent=value['parent_operation_key']
        key=(command.person_id,parent)
        current=self.rows.get(key)
        if value['action']=='stage':
            from regional_knowledge.kaggle_story_reconcile_native import parse_intent_bytes
            value_intent=parse_intent_bytes(base64.b64decode(value['intent_b64']))
            assert current is None
            self.rows[key]=self.record(value_intent)
        else:
            assert current is not None
            current=current.copy()
            current['version']+=1
            if value['action']=='begin':current['attempted']=True
            elif value['action']=='advance':
                current['attempted']=False
                current['step_index']+=1
                current['case_id']=value['case_id']
            elif value['action']=='reject':
                current['rejected']=True
                current['rejected_step']=current['step_index']
            elif value['action']=='finish':current['done']=True
            self.rows[key]=current
        self.effects+=1
        self.last_effect=command.operation_key


class FakeDispatch:
    def __init__(self,spool,native):
        self.spool=spool
        self.native=native
        self.delayed=False
        self.lost_ack_once=False
        self.enqueue_keys=[]

    async def enqueue(self,context,key,*,writer_generation):
        assert writer_generation=='writer-1'
        self.enqueue_keys.append(key)
        if self.delayed:return
        entry=self.spool.entries[key]
        self.native.apply(entry.command)
        parsed=decode_payload(entry.command.payload)
        native_record=self.native.rows[(entry.command.person_id,parsed['parent_operation_key'])]
        result_bytes=canonical(dict(parent_operation_key=native_record['operation_key'],
            actor_id=native_record['actor_id'],request_sha256=native_record['request_sha256'],
            progress_version=native_record['version'],step_index=native_record['step_index'],
            attempted=native_record['attempted'],case_id=native_record['case_id'],
            done=native_record['done'],rejected=native_record['rejected'],
            rejected_step=native_record['rejected_step']))
        cmd_hash=sha(entry.command.encode())
        applied=canonical(dict(codec='rkb.applied-outcome.v1',operation_key=key,
            command_sha256=cmd_hash,status='applied',person_id=entry.command.person_id,
            route=entry.command.route,result_b64=base64.b64encode(result_bytes).decode(),
            result_sha256=sha(result_bytes)))
        remote=canonical(dict(codec='rkb.outcome.v1',stream=self.spool.journal.stream,
            command_sha256=cmd_hash,sequence=entry.command.sequence,operation_key=key,
            database_generation='gen-1',outcome_b64=base64.b64encode(applied).decode(),
            outcome_sha256=sha(applied),outcome_length=len(applied)))
        self.spool.journal.outcomes[key]=remote
        entry.outcome_state='DURABLE'
        entry.apply_receipt=SimpleNamespace(status='applied')
        if self.lost_ack_once:
            self.lost_ack_once=False
            raise TimeoutError('synthetic lost native apply ACK')


@pytest.fixture
def env():
    pg=FakeNativePg()
    spool=FakeSpool()
    dispatch=FakeDispatch(spool,pg)
    authority=FakeAuthority()
    store=NativeAcceptedReconcileStore(spool=spool,dispatcher=dispatch,
        writer_generation='writer-1',scope=authority.scope,native_read=pg.read)
    return store,spool,dispatch,pg,authority


@pytest.mark.asyncio
async def test_real_store_calls_existing_spool_journal_and_native_readback(env):
    store,spool,dispatch,pg,auth=env
    current=intent()
    value=await store.stage(current)
    assert value.version==1 and value.step_index==0 and not value.done
    assert pg.effects==1 and spool.journal.last==1 and dispatch.enqueue_keys==[current.operation_key]
    assert (await store.stage(current))==value
    assert pg.effects==1
    begun=await store.begin(ACTOR,current.operation_key,expected_version=1)
    assert begun.version==2 and begun.attempted and begun.step_index==0
    advanced=await store.advance(ACTOR,current.operation_key,expected_version=2,step_index=1,case_id=CASE)
    assert advanced.version==3 and advanced.step_index==1 and not advanced.attempted
    assert advanced.case_id==CASE and pg.effects==3
    assert len(set(dispatch.enqueue_keys))==3
    assert auth.checks>=9


@pytest.mark.asyncio
async def test_lost_native_ack_reuses_the_same_command_and_pg_applied_result(env):
    store,spool,dispatch,pg,auth=env
    value=intent()
    dispatch.lost_ack_once=True
    with pytest.raises(TimeoutError,match='lost native apply ACK'):
        await store.stage(value)
    assert pg.effects==1 and spool.journal.last==1
    recovered=await store.stage(value)
    assert recovered.version==1 and recovered.intent==value
    assert pg.effects==1 and len(spool.entries)==1


@pytest.mark.asyncio
async def test_no_false_saved_when_accepted_but_native_or_remote_outcome_pending(env):
    store,spool,dispatch,pg,auth=env
    dispatch.delayed=True
    with pytest.raises(ReconcilePending,match='accepted_apply_or_remote_outcome_pending'):
        await store.stage(intent())
    assert pg.effects==0
    assert await store.get(ACTOR,intent().operation_key) is None
    dispatch.delayed=False
    got=await store.stage(intent())
    assert got.version==1 and pg.effects==1


@pytest.mark.asyncio
async def test_no_handler_registration_does_not_stage_or_fabricate_native_ready(env):
    store,spool,dispatch,pg,auth=env
    spool.fail_unregistered=True
    with pytest.raises(ReconcileError,match='no exact semantic handler'):
        await store.stage(intent())
    assert not spool.entries and pg.effects==0


@pytest.mark.asyncio
async def test_cas_version_and_conflicting_comparison_bytes_are_immutable(env):
    store,spool,dispatch,pg,auth=env
    value=intent()
    await store.stage(value)
    wrong=replace(value,request_sha256='f'*64)
    with pytest.raises(ReconcileError):await store.stage(wrong)
    with pytest.raises(ReconcileError,match='stale_reconciliation_progress'):
        await store.begin(ACTOR,value.operation_key,expected_version=17)
    assert pg.effects==1


@pytest.mark.asyncio
async def test_begin_advance_reject_finish_effects_remain_separate_pg_cas(env):
    store,spool,dispatch,pg,auth=env
    case=intent()
    await store.stage(case)
    await store.begin(ACTOR,case.operation_key,expected_version=1)
    rejected=await store.reject(ACTOR,case.operation_key,expected_version=2,rejected_step=0)
    assert rejected.rejected and rejected.rejected_step==0
    with pytest.raises(ReconcileError,match='reconciliation_already_terminal'):
        await store.finish(ACTOR,case.operation_key,expected_version=3)
    assert pg.effects==3
    # A zero-effect compatible comparison requires the SAME PG finish path.
    from regional_knowledge.kaggle_story_reconcile_native import canonical
    compatible=intent('none')
    comparison=compatible.comparison.model_copy(update={'comparison_key':'separate-non-conflict'})
    compatible=replace(compatible,comparison=comparison,
        operation_key='story-reconcile:'+sha(canonical([ACTOR,compatible.anchor_source.frontier_id,
                                                       comparison.comparison_key])),
        request_sha256=sha(canonical(comparison.model_dump(mode='json'))))
    await store.stage(compatible)
    finished=await store.finish(ACTOR,compatible.operation_key,expected_version=1)
    assert finished.done and finished.version==2 and finished.step_index==0
    assert pg.effects==5


@pytest.mark.asyncio
async def test_current_writer_auth_revoked_or_native_reader_denied_never_fabricates_state(env):
    store,spool,dispatch,pg,auth=env
    await store.stage(intent())
    auth.revoked=True
    with pytest.raises(PermissionError):
        await store.begin(ACTOR,intent().operation_key,expected_version=1)
    assert pg.effects==1
    auth.revoked=False
    pg.permission=False
    with pytest.raises(PermissionError):
        await store.get(ACTOR,intent().operation_key)
    assert pg.effects==1


@pytest.mark.asyncio
async def test_actor_binding_is_fixed_by_verified_scope_not_request_object(env):
    store,spool,dispatch,pg,auth=env
    with pytest.raises((AssertionError,ReconcileError)):
        await store.get('00000000-0000-0000-0000-000000000999',intent().operation_key)
    assert pg.effects==0


def test_native_readback_cannot_trust_unverified_case_pointer_or_hash(env):
    store,spool,dispatch,pg,auth=env
    parent=intent()
    row=FakeNativePg.record(parent)
    row['intent_sha256']='0'*64
    with pytest.raises(ReconcileError,match='untrusted_native'):
        store._record(row,ACTOR,parent.operation_key)
    row=FakeNativePg.record(parent)
    row['version']=True
    with pytest.raises(ReconcileError,match='untrusted_native'):
        store._record(row,ACTOR,parent.operation_key)


@pytest.mark.asyncio
async def test_durable_pg_row_without_remote_checkpoint_is_not_returned(env):
    store,spool,dispatch,pg,auth=env
    value=intent()
    await store.stage(value)
    assert pg.effects==1
    spool.journal.covered=False  # Dataset control/object bytes not promoted
    with pytest.raises(ReconcilePending,match='not_remotely_durable'):
        await store.get(ACTOR,value.operation_key)
    spool.journal.covered=True
    assert (await store.get(ACTOR,value.operation_key)).version==1
    assert pg.effects==1


@pytest.mark.asyncio
async def test_lost_progress_ack_readback_proves_original_exact_native_outcome(env):
    store,spool,dispatch,pg,auth=env
    value=intent()
    await store.stage(value)
    dispatch.lost_ack_once=True
    with pytest.raises(TimeoutError,match='lost native apply ACK'):
        await store.begin(ACTOR,value.operation_key,expected_version=1)
    assert pg.effects==2
    result=await store.begin(ACTOR,value.operation_key,expected_version=1)
    assert result.version==2 and result.attempted
    assert pg.effects==2


@pytest.mark.asyncio
async def test_missing_or_tampered_remote_receipt_does_not_pass_native_proof(env):
    store,spool,dispatch,pg,auth=env
    value=intent()
    await store.stage(value)
    saved=spool.journal.outcomes[value.operation_key]
    spool.journal.outcomes[value.operation_key]=saved[:-4]+b'XXXX'
    with pytest.raises(ReconcileError,match='reconciliation_remote_receipt_unproved'):
        await store.get(ACTOR,value.operation_key)
    spool.journal.outcomes[value.operation_key]=saved
    assert (await store.get(ACTOR,value.operation_key)).version==1
