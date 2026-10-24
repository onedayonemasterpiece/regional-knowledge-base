"""Synthetic existing SQLite identities and an actual-compute stub, no providers."""
import asyncio
from contextlib import contextmanager
from dataclasses import replace
from datetime import timedelta
import subprocess
import sys
import threading
import time
from types import SimpleNamespace

import pytest
import pytest_asyncio

from regional_knowledge.bge_admission import AdmissionLimit
from regional_knowledge.bge_queue import BgeQueue, StaleAttachment
from regional_knowledge.bge_contract import SPACE
from regional_knowledge.kaggle_protocol import Query, QueryResult, TrustedActorContext
from regional_knowledge.kaggle_readiness import CapabilityReceipt, RunIdentity, TerminalObservation
from regional_knowledge.read_authority import ReadAuthorityError
from regional_knowledge.participant_contracts import AccountBegin,AccountConfirm
from test_participants import people,bind,change
from test_read_authority import auth,issue


def query(context, request, generation=1, turn=0, min_seq=0):
    return Query(request_id=request,connection_generation=generation,turn_generation=turn,
        query='synthetic regional question',deadline_ms=2000,min_data_seq=min_seq,
        actor=TrustedActorContext(person_id=context.person_id,audience=context.destination.key,
            auth_epoch=context.auth_revision,visibility_epoch=0,context_ref=context.context_ref))


def answer(work, **updates):
    values=dict(request_id=work.query.request_id,connection_generation=work.dispatch_generation,
        turn_generation=work.query.turn_generation,status='ok',mode='hybrid',
        observed_data_seq=7,observed_projection_seq=7)
    return QueryResult(**(values|updates))


async def release(s,work,generation=None):
    r=work.resource
    return await s.a.resource_released(s.run,s.token,generation or s.generation,
        runtime_incarnation=r.runtime_incarnation,resource_kind=r.resource_kind,
        resource_id=r.resource_id,compute_claim=r.compute_claim,dispatch_generation=r.dispatch_generation)


@pytest_asyncio.fixture
async def state(auth,tmp_path):
    q=BgeQueue(tmp_path/'worker.sqlite',clock=lambda:1000.)
    doc=q.enqueue(auth.p.owner.subject,'document',['synthetic passage'],kind='document')
    with q.connect(write=False) as db:run=db.execute('select id from runs').fetchone()[0]
    token=(q.path.parent/'bge-worker-credentials'/run).read_text()
    nonce=q.prepare_channel(run)
    q.launch_claim(run);q.launch_record(run,'fixture-owner/stable-slug',provider_version=7)
    prior=BgeQueue(q.path,clock=q.clock)
    a=q.configure_shared_reads(auth.resolver,max_read_streak=3)
    generation=q.attach_channel(run,token,nonce,'incarnation-a')['connection_generation']
    q.heartbeat(run,token,ready=True,connection_generation=generation)
    await a.resource_state(run,token,generation,runtime_incarnation='incarnation-a',state='idle')
    context=await auth.resolver.resolve_oauth(auth.tokens.access_token,destination=auth.destination)
    s=SimpleNamespace(q=q,a=a,run=run,token=token,nonce=nonce,generation=generation,
                      context=context,doc=doc,auth=auth,prior=prior)
    yield s
    a.close()


async def begin(s,name,*,context=None,dialog='dialog-a',generation=None,min_seq=0):
    context=context or s.context
    return await s.a.begin_read(context,query(context,name,generation or s.generation,min_seq=min_seq),
        dialog_ref=dialog,deadline=time.monotonic()+1.9)


@pytest.mark.asyncio
async def test_ready_read_no_queue_writer_even_under_held_sqlite_writer(state,monkeypatch):
    s=state;original=s.q.connect
    with original() as writer:
        writer.execute("update runs set useful=useful where id=?",(s.run,))
        @contextmanager
        def readonly(*,write=True):
            assert not write,'ready read attempted SQLite writer'
            with original(write=False) as db:yield db
        monkeypatch.setattr(s.q,'connect',readonly)
        ticket=await begin(s,'read-only',min_seq=7)
        work=await s.q.next_channel_work(s.run,s.token,1)
        assert work.kind=='read' and work.query.min_data_seq==7
        s.a.mark_work_sent(work)
        assert await s.a.finish_read(s.run,s.token,1,answer(work))
        assert (await ticket.result).status=='ok'
        assert s.a.status()['state']=='busy' # RESULT is never a release.
        assert await release(s,work)


@pytest.mark.asyncio
async def test_cancel_deadline_and_result_leave_actual_slot_held(state):
    s=state
    first=await begin(s,'cancelled')
    work=await s.q.next_channel_work(s.run,s.token,1);s.a.mark_work_sent(work)
    s.a.cancel_read(first)
    assert first.result.cancelled()
    assert not s.a.read_consumer_active(work)
    second=await begin(s,'waiting')
    assert await s.q.next_channel_work(s.run,s.token,1) is None
    assert not await s.a.finish_read(s.run,s.token,1,answer(work))
    await release(s,work)
    next_work=await s.q.next_channel_work(s.run,s.token,1)
    assert next_work.query.request_id=='waiting'
    s.a.discard_unsent(next_work)
    s.a.cancel_read(second)
    deadline=await s.a.begin_read(s.context,query(s.context,'deadline'),dialog_ref='d',deadline=time.monotonic()+.02)
    timed=await s.q.next_channel_work(s.run,s.token,1);s.a.mark_work_sent(timed)
    await asyncio.sleep(.04)
    assert deadline.result.cancelled() and s.a.status()['state']=='busy'
    await release(s,timed)


@pytest.mark.asyncio
async def test_current_socket_can_settle_original_claim_stale_socket_cannot(state):
    s=state
    ticket=await begin(s,'old-socket')
    work=await s.q.next_channel_work(s.run,s.token,1);s.a.mark_work_sent(work)
    second=s.q.attach_channel(s.run,s.token,s.nonce,'incarnation-a')['connection_generation']
    await s.a.resource_state(s.run,s.token,second,runtime_incarnation='incarnation-a',state='busy',
        resource_kind='read',resource_id='old-socket',compute_claim=work.compute_claim,dispatch_generation=1)
    assert ticket.result.cancelled()
    with pytest.raises(StaleAttachment):await release(s,work,generation=1)
    with pytest.raises(StaleAttachment):
        await s.a.resource_state(s.run,s.token,second,runtime_incarnation='incarnation-a',state='idle')
    assert await release(s,work,generation=second)
    assert not await release(s,work,generation=second) # exact retained release replay
    assert not await s.a.finish_read(s.run,s.token,second,answer(work))


@pytest.mark.asyncio
async def test_retained_release_replay_after_reconnect_does_not_free_new_compute(state):
    s=state
    await begin(s,'completed-first')
    first=await s.q.next_channel_work(s.run,s.token,1);s.a.mark_work_sent(first)
    assert s.a.read_consumer_active(first)
    await release(s,first)
    generation=s.q.attach_channel(s.run,s.token,s.nonce,'incarnation-a')['connection_generation']
    assert not await release(s,first,generation=generation)
    await s.a.resource_state(s.run,s.token,generation,runtime_incarnation='incarnation-a',state='idle')
    await begin(s,'new-compute',generation=generation)
    second=await s.q.next_channel_work(s.run,s.token,generation);s.a.mark_work_sent(second)
    nonce=await s.a.begin_resource_probe(s.run,s.token,generation)
    assert not await release(s,first,generation=generation)
    await s.a.resource_state(s.run,s.token,generation,runtime_incarnation='incarnation-a',state='busy',
        resource_kind='read',resource_id=second.resource.resource_id,compute_claim=second.compute_claim,
        dispatch_generation=second.dispatch_generation,probe_nonce=nonce)
    assert s.a.status()['state']=='busy' and s.a.status()['resource']==second.resource


@pytest.mark.asyncio
@pytest.mark.parametrize('observation',['idle','released_busy'])
@pytest.mark.parametrize('sent',[False,True])
async def test_late_released_resource_state_does_not_reject_next_assignment(state,observation,sent):
    s=state
    ticket=await begin(s,'finished-read')
    first=await s.q.next_channel_work(s.run,s.token,1);s.a.mark_work_sent(first)
    assert await s.a.finish_read(s.run,s.token,1,answer(first))
    assert (await ticket.result).status=='ok'
    await release(s,first)
    # RESOURCE_RELEASED lets admission select the next durable job before the
    # old states() iteration's already queued RESOURCE_STATE reaches receiver.
    await s.a.register_document_actor(s.auth.p.admin)
    second=await s.q.next_channel_work(s.run,s.token,1)
    assert second.kind=='document'
    if sent:s.a.mark_work_sent(second)
    before=s.a.status()
    prior=first.resource
    fields={} if observation=='idle' else dict(resource_kind=prior.resource_kind,
        resource_id=prior.resource_id,compute_claim=prior.compute_claim,
        dispatch_generation=prior.dispatch_generation)
    await s.a.resource_state(s.run,s.token,1,runtime_incarnation='incarnation-a',
        state='idle' if observation=='idle' else 'busy',**fields)
    assert s.a.status()==before
    assert s.a.status()['resource']==second.resource
    with pytest.raises(StaleAttachment):
        await s.a.resource_state(s.run,s.token,1,runtime_incarnation='incarnation-a',
            state='busy',resource_kind='read',resource_id='unknown-work',
            compute_claim='unrecognized-claim',dispatch_generation=1)
    nonce=await s.a.begin_resource_probe(s.run,s.token,1)
    with pytest.raises(StaleAttachment):
        await s.a.resource_state(s.run,s.token,1,runtime_incarnation='incarnation-a',
            state='idle',probe_nonce='wrong-probe')
    # An actual fresh ordered observation keeps its original authority.
    current=second.resource
    await s.a.resource_state(s.run,s.token,1,runtime_incarnation='incarnation-a',
        state='busy',resource_kind=current.resource_kind,resource_id=current.resource_id,
        compute_claim=current.compute_claim,dispatch_generation=current.dispatch_generation,
        probe_nonce=nonce)
    assert s.a.status()['resource']==second.resource


@pytest.mark.asyncio
async def test_retained_release_after_controller_restart_still_requires_baseline(state):
    s=state
    await begin(s,'completed-offline')
    first=await s.q.next_channel_work(s.run,s.token,1);s.a.mark_work_sent(first)
    s.a.close()
    reopened=BgeQueue(s.q.path,clock=s.q.clock)
    a=reopened.configure_shared_reads(s.auth.resolver)
    try:
        generation=reopened.attach_channel(s.run,s.token,s.nonce,'incarnation-a')['connection_generation']
        r=first.resource
        stale_nonce=await a.begin_resource_probe(s.run,s.token,generation)
        assert await a.resource_released(s.run,s.token,generation,runtime_incarnation=r.runtime_incarnation,
            resource_kind=r.resource_kind,resource_id=r.resource_id,compute_claim=r.compute_claim,dispatch_generation=r.dispatch_generation)
        assert a.status()['state']=='unknown'
        with pytest.raises(StaleAttachment):
            await a.resource_state(s.run,s.token,generation,runtime_incarnation='incarnation-a',state='idle',probe_nonce=stale_nonce)
        nonce=await a.begin_resource_probe(s.run,s.token,generation)
        assert not await a.resource_released(s.run,s.token,generation,runtime_incarnation=r.runtime_incarnation,
            resource_kind=r.resource_kind,resource_id=r.resource_id,compute_claim=r.compute_claim,dispatch_generation=r.dispatch_generation)
        await a.resource_state(s.run,s.token,generation,runtime_incarnation='incarnation-a',state='idle',probe_nonce=nonce)
        assert a.status()['state']=='idle'
    finally:a.close()


@pytest.mark.asyncio
async def test_fresh_probe_recovers_dispatch_lost_before_worker_receive(state):
    s=state
    ticket=await begin(s,'lost-before-receive')
    work=await s.q.next_channel_work(s.run,s.token,1)
    s.a.mark_work_sent(work) # network never delivers it to the actual worker.
    generation=s.q.attach_channel(s.run,s.token,s.nonce,'incarnation-a')['connection_generation']
    with pytest.raises(StaleAttachment):
        await s.a.resource_state(s.run,s.token,generation,runtime_incarnation='incarnation-a',state='idle')
    nonce=await s.a.begin_resource_probe(s.run,s.token,generation)
    await s.a.resource_state(s.run,s.token,generation,runtime_incarnation='incarnation-a',
                             state='idle',probe_nonce=nonce)
    assert ticket.result.cancelled() and s.a.status()['state']=='idle'
    with pytest.raises(StaleAttachment):
        await s.a.resource_state(s.run,s.token,generation,runtime_incarnation='incarnation-a',state='idle',probe_nonce=nonce)


@pytest.mark.asyncio
async def test_probe_cannot_free_new_reservation_or_cross_attachment(state):
    s=state
    nonce=await s.a.begin_resource_probe(s.run,s.token,1)
    await begin(s,'new-reservation')
    work=await s.q.next_channel_work(s.run,s.token,1);s.a.mark_work_sent(work)
    with pytest.raises(StaleAttachment):
        await s.a.resource_state(s.run,s.token,1,runtime_incarnation='incarnation-a',state='idle',probe_nonce=nonce)
    nonce=await s.a.begin_resource_probe(s.run,s.token,1)
    generation=s.q.attach_channel(s.run,s.token,s.nonce,'incarnation-a')['connection_generation']
    with pytest.raises(StaleAttachment):
        await s.a.resource_state(s.run,s.token,generation,runtime_incarnation='incarnation-a',state='idle',probe_nonce=nonce)
    assert s.a.status()['state']=='busy'


@pytest.mark.asyncio
async def test_probe_rechecks_slot_after_reconciliation_snapshot_await(state,monkeypatch):
    s=state
    await begin(s,'probe-send-race')
    work=await s.q.next_channel_work(s.run,s.token,1)
    nonce=await s.a.begin_resource_probe(s.run,s.token,1)
    entered=threading.Event();resume=threading.Event();original=s.q._admission_snapshot
    def blocked(*args,**kwargs):
        snapshot=original(*args,**kwargs)
        entered.set()
        assert resume.wait(2)
        return snapshot
    monkeypatch.setattr(s.q,'_admission_snapshot',blocked)
    reconciling=asyncio.create_task(s.a.resource_state(s.run,s.token,1,runtime_incarnation='incarnation-a',state='idle',probe_nonce=nonce))
    assert await asyncio.to_thread(entered.wait,2)
    s.a.mark_work_sent(work) # new send after worker's earlier idle observation.
    resume.set()
    with pytest.raises(StaleAttachment):await reconciling
    assert s.a.status()['state']=='busy'


@pytest.mark.asyncio
async def test_delayed_old_result_snapshot_cannot_regress_current_attachment(state,monkeypatch):
    s=state
    await begin(s,'old-result-snapshot')
    work=await s.q.next_channel_work(s.run,s.token,1);s.a.mark_work_sent(work)
    entered=threading.Event();resume=threading.Event();original=s.q._admission_snapshot
    def delayed(run,token,generation,**kwargs):
        snapshot=original(run,token,generation,**kwargs)
        if generation==1:
            entered.set()
            assert resume.wait(2)
        return snapshot
    monkeypatch.setattr(s.q,'_admission_snapshot',delayed)
    delivering=asyncio.create_task(s.a.finish_read(s.run,s.token,1,answer(work)))
    assert await asyncio.to_thread(entered.wait,2)
    generation=s.q.attach_channel(s.run,s.token,s.nonce,'incarnation-a')['connection_generation']
    await s.a.resource_state(s.run,s.token,generation,runtime_incarnation='incarnation-a',state='busy',
        resource_kind='read',resource_id=work.resource.resource_id,compute_claim=work.compute_claim,dispatch_generation=1)
    resume.set()
    assert not await delivering
    ticket=await begin(s,'new-current-read',generation=generation)
    assert not ticket.result.done()


@pytest.mark.asyncio
async def test_restart_recovers_same_incarnation_then_owned_current_run_handover(state):
    s=state
    await begin(s,'lost-context')
    work=await s.q.next_channel_work(s.run,s.token,1);s.a.mark_work_sent(work)
    s.a.close()
    reopened=BgeQueue(s.q.path,clock=s.q.clock)
    a=reopened.configure_shared_reads(s.auth.resolver)
    try:
        assert a.status()['state']=='unknown'
        assert await reopened.next_channel_work(s.run,s.token,1) is None
        generation=reopened.attach_channel(s.run,s.token,s.nonce,'incarnation-a')['connection_generation']
        await a.resource_state(s.run,s.token,generation,runtime_incarnation='incarnation-a',state='busy',
            resource_kind='read',resource_id='lost-context',compute_claim=work.compute_claim,dispatch_generation=1)
        assert a.status()['pending_reads']==0 and a.status()['state']=='busy'
        await a.resource_released(s.run,s.token,generation,runtime_incarnation='incarnation-a',
            resource_kind='read',resource_id='lost-context',compute_claim=work.compute_claim,dispatch_generation=1)
        assert a.status()['state']=='idle'
        # Existing current-run fencing authorizes a new baseline after handover;
        # provider compute closure remains independent.
        with reopened.connect() as db:
            new_run=reopened._new_run(db,1000.)
            db.execute('update control set current_run=? where id=1',(new_run,))
        new_token=(reopened.path.parent/'bge-worker-credentials'/new_run).read_text()
        nonce=reopened.prepare_channel(new_run)
        g=reopened.attach_channel(new_run,new_token,nonce,'incarnation-new')['connection_generation']
        await a.resource_state(new_run,new_token,g,runtime_incarnation='incarnation-new',state='idle')
        assert a.status()['state']=='idle'
    finally:a.close()


@pytest.mark.asyncio
async def test_fair_person_share_does_not_multiply_with_many_dialogs_and_document_progress(state):
    s=state
    # Two independently verified token contexts project the same canonical UUID.
    other_context=await s.auth.resolver.resolve_oauth(s.auth.tokens.access_token,destination=s.auth.destination)
    assert other_context.context_ref!=s.context.context_ref and other_context.person_id==s.context.person_id
    await s.a.register_document_actor(s.auth.p.admin)
    for i in range(8):await begin(s,f'person-many-{i}',context=other_context if i%2 else s.context,dialog=f'dialog-{i}')
    kinds=[]
    for _ in range(4):
        work=await s.q.next_channel_work(s.run,s.token,1)
        kinds.append(work.kind);s.a.mark_work_sent(work)
        if work.kind=='read':
            assert await s.a.finish_read(s.run,s.token,1,answer(work))
        await release(s,work)
        if work.kind=='document':break
    assert 'document' in kinds and kinds.index('document')<=3
    with s.q.connect(write=False) as db:
        doc=db.execute('select state,attempt from jobs where id=?',(s.doc,)).fetchone()
    assert tuple(doc)==('claimed',1)


@pytest.mark.asyncio
async def test_distinct_providers_share_person_turn_and_do_not_outvote_second_person(state):
    s=state
    # Existing canonical external links, rather than invented provider UUIDs.
    bind(s.auth.p)
    link=change(s.auth.p,AccountBegin(provider='vk',tenant='community-1',provider_subject='123',reason='synthetic'))
    observation=s.auth.p.registry.observe_account(link_id=link['link_id'],link_revision=1,provider='vk',
        tenant='community-1',provider_subject='123',audience='rkb-account-binding',
        expires_at=s.auth.p.registry._time()+timedelta(minutes=1),receipt_id='synthetic-vk-proof')
    change(s.auth.p,AccountConfirm(link_id=link['link_id'],expected_link_revision=1,
        method='provider_observation',observation_id=observation,reason='synthetic'))
    contexts=[]
    for provider,tenant,subject in [('slack','T111','U111'),('vk','community-1','123')]:
        contexts.append(await s.auth.resolver.resolve_adapter(provider=provider,tenant=tenant,
            provider_subject=subject,destination=s.auth.destination))
    assert contexts[0].person_id==contexts[1].person_id
    owner_tokens=await issue(s.auth.provider)
    owner=await s.auth.resolver.resolve_oauth(owner_tokens.access_token,destination=s.auth.destination)
    for i in range(8):await begin(s,f'many-{i}',context=contexts[i%2],dialog=f'dialog-{i}')
    with pytest.raises(AdmissionLimit) as limited:
        await begin(s,'ninth-provider-request',context=contexts[1],dialog='another-dialog')
    assert limited.value.code=='person_read_capacity'
    await begin(s,'second-person',context=owner)
    served=[]
    for _ in range(2):
        w=await s.q.next_channel_work(s.run,s.token,1);s.a.mark_work_sent(w)
        served.append(w.query.actor.person_id)
        await s.a.finish_read(s.run,s.token,1,answer(w));await release(s,w)
    assert len(set(served))==2


@pytest.mark.asyncio
async def test_dialog_pending_cap_reclaimed_on_cancel_without_releasing_compute(state):
    s=state
    first=await begin(s,'dialog-first')
    await begin(s,'dialog-second')
    with pytest.raises(AdmissionLimit) as limited:await begin(s,'dialog-third')
    assert limited.value.code=='dialog_read_capacity'
    work=await s.q.next_channel_work(s.run,s.token,1);s.a.mark_work_sent(work)
    s.a.cancel_read(first)
    await begin(s,'dialog-replacement')
    assert s.a.status()['pending_reads']==2 and s.a.status()['state']=='busy'
    assert await s.q.next_channel_work(s.run,s.token,1) is None


@pytest.mark.asyncio
async def test_read_dialogs_round_robin_within_one_canonical_person(state):
    s=state
    for name,dialog in [('a1','a'),('a2','a'),('b1','b')]:await begin(s,name,dialog=dialog)
    names=[]
    for _ in range(2):
        w=await s.q.next_channel_work(s.run,s.token,1);s.a.mark_work_sent(w)
        names.append(w.query.request_id)
        await s.a.finish_read(s.run,s.token,1,answer(w));await release(s,w)
    assert names==['a1','b1']


@pytest.mark.asyncio
async def test_same_person_document_forced_after_bounded_read_streak(state):
    s=state
    owner_tokens=await issue(s.auth.provider)
    owner=await s.auth.resolver.resolve_oauth(owner_tokens.access_token,destination=s.auth.destination)
    await s.a.register_document_actor(s.auth.p.admin)
    for i in range(7):await begin(s,f'owner-read-{i}',context=owner,dialog=f'dialog-{i}')
    kinds=[]
    for _ in range(4):
        w=await s.q.next_channel_work(s.run,s.token,1);s.a.mark_work_sent(w)
        kinds.append(w.kind)
        if w.kind=='read':await s.a.finish_read(s.run,s.token,1,answer(w))
        await release(s,w)
    assert kinds==['read','read','read','document']


@pytest.mark.asyncio
async def test_document_result_is_durable_but_does_not_free_compute(state):
    s=state
    await s.a.register_document_actor(s.auth.p.admin)
    w=await s.q.next_channel_work(s.run,s.token,1);s.a.mark_work_sent(w)
    assert w.kind=='document'
    payload=w.document_payload
    for expected in ('accepted','duplicate'):
        assert s.q.complete(s.run,s.token,payload['id'],payload['claim'],SPACE,
            [[1]+[0]*1023],{},connection_generation=1)==expected
        assert s.a.status()['state']=='busy'
    await begin(s,'behind-done-document')
    assert await s.q.next_channel_work(s.run,s.token,1) is None
    await release(s,w)
    assert (await s.q.next_channel_work(s.run,s.token,1)).kind=='read'


@pytest.mark.asyncio
async def test_document_resumes_original_durable_assignment_after_controller_restart(state):
    s=state
    await s.a.register_document_actor(s.auth.p.admin)
    first=await s.q.next_channel_work(s.run,s.token,1)
    # Controller dies before trying network send: worker's current idle baseline
    # proves no compute, but the durable assignment must retain its exact claim.
    s.a.close()
    reopened=BgeQueue(s.q.path,clock=s.q.clock)
    a=reopened.configure_shared_reads(s.auth.resolver)
    try:
        generation=reopened.attach_channel(s.run,s.token,s.nonce,'incarnation-a')['connection_generation']
        await a.resource_state(s.run,s.token,generation,runtime_incarnation='incarnation-a',state='idle')
        assert await reopened.next_channel_work(s.run,s.token,generation) is None # missing accepted context
        await a.register_document_actor(s.auth.p.admin)
        resumed=await reopened.next_channel_work(s.run,s.token,generation)
        assert resumed.document_payload['claim']==first.document_payload['claim']
        assert resumed.compute_claim!=first.compute_claim
        with reopened.connect(write=False) as db:assert db.execute('select attempt from jobs where id=?',(s.doc,)).fetchone()[0]==1
    finally:a.close()


@pytest.mark.asyncio
async def test_uuid_only_document_stays_durable_pending_and_legacy_instances_fenced(state):
    s=state
    assert await s.q.next_channel_work(s.run,s.token,1) is None
    with s.q.connect(write=False) as db:assert db.execute('select state from jobs where id=?',(s.doc,)).fetchone()[0]=='pending'
    legacy=BgeQueue(s.q.path,clock=s.q.clock)
    for action in (
        lambda:s.prior.claim(s.run,s.token,connection_generation=1),
        lambda:legacy.claim(s.run,s.token,connection_generation=1),
        lambda:s.q.claim(s.run,s.token,connection_generation=1),
        lambda:legacy.attach_channel(s.run,s.token,s.nonce,'incarnation-a'),
        lambda:legacy.resume_channel_claim(s.run,s.token,1),
        lambda:legacy.enqueue('arbitrary','query',['synthetic']),
    ):
        with pytest.raises(StaleAttachment):action()
    with pytest.raises(BlockingIOError):legacy.configure_shared_reads(s.auth.resolver)


@pytest.mark.asyncio
async def test_other_process_cannot_own_ram_or_bypass_persisted_target_flag(state):
    s=state
    code='''
import sys
from regional_knowledge.bge_queue import BgeQueue,StaleAttachment
from regional_knowledge.read_authority import ReadAuthority
q=BgeQueue(sys.argv[1],clock=lambda:1000.)
try:q.configure_shared_reads(ReadAuthority.__new__(ReadAuthority))
except BlockingIOError:pass
else:raise AssertionError('second controller acquired process ownership')
try:q.claim(sys.argv[2],sys.argv[3],connection_generation=1)
except StaleAttachment:pass
else:raise AssertionError('legacy process bypassed target fence')
'''
    completed=await asyncio.to_thread(subprocess.run,[sys.executable,'-c',code,str(s.q.path),s.run,s.token],
        capture_output=True,text=True,timeout=5)
    assert completed.returncode==0,completed.stderr


@pytest.mark.asyncio
async def test_authority_projection_and_min_sequence_cannot_be_forged(state):
    s=state
    with pytest.raises(PermissionError):
        await s.a.begin_read(replace(s.context),query(s.context,'forged'),dialog_ref='d',deadline=time.monotonic()+1)
    forged=query(s.context,'forged-person').model_copy(update={'actor':query(s.context,'x').actor.model_copy(update={'person_id':s.auth.p.owner.subject})})
    with pytest.raises(PermissionError):await s.a.begin_read(s.context,forged,dialog_ref='d',deadline=time.monotonic()+1)
    for deadline in (float('nan'),time.monotonic()+3):
        with pytest.raises(ValueError):await s.a.begin_read(s.context,query(s.context,'budget'),dialog_ref='d',deadline=deadline)
    ticket=await begin(s,'fenced-result',min_seq=8)
    work=await s.q.next_channel_work(s.run,s.token,1)
    assert not await s.a.finish_read(s.run,s.token,1,answer(work)) # unsolicited before dispatch
    s.a.mark_work_sent(work)
    with pytest.raises(ValueError):
        await s.a.finish_read(s.run,s.token,1,answer(work,status='degraded',mode='lexical_degraded',
            reason='dense_unavailable',observed_data_seq=8,observed_projection_seq=8))
    with pytest.raises(ValueError):await s.a.finish_read(s.run,s.token,1,answer(work))
    s.auth.resolver.release(s.context)
    assert not await s.a.finish_read(s.run,s.token,1,answer(work,observed_data_seq=8,observed_projection_seq=8))
    assert ticket.result.cancelled() and s.a.status()['state']=='busy'


@pytest.mark.asyncio
async def test_document_release_before_result_holds_only_result_debt_and_serves_reads(state):
    s=state
    await s.a.register_document_actor(s.auth.p.admin)
    work=await s.q.next_channel_work(s.run,s.token,1);s.a.mark_work_sent(work)
    payload=work.document_payload
    await release(s,work)
    assert s.a.status()['state']=='idle' and s.a.status()['document_result_pending']
    assert await s.q.next_channel_work(s.run,s.token,1) is None # no redispatch/new compute claim
    await begin(s,'read-after-document-release')
    read=await s.q.next_channel_work(s.run,s.token,1)
    assert read.kind=='read' and s.a.read_consumer_active(read)
    s.a.mark_work_sent(read)
    assert not s.a.durable_result_settled(payload['id'],'wrong-claim','accepted')
    status=s.q.complete(s.run,s.token,payload['id'],payload['claim'],SPACE,
        [[1]+[0]*1023],{},connection_generation=1)
    assert s.a.durable_result_settled(payload['id'],payload['claim'],status)
    assert not s.a.status()['document_result_pending'] and s.a.status()['resource']==read.resource


@pytest.mark.asyncio
async def test_restart_retained_document_release_blocks_replay_until_result_settled(state):
    s=state
    await s.a.register_document_actor(s.auth.p.admin)
    work=await s.q.next_channel_work(s.run,s.token,1);s.a.mark_work_sent(work)
    payload=work.document_payload
    s.a.close()
    reopened=BgeQueue(s.q.path,clock=s.q.clock)
    a=reopened.configure_shared_reads(s.auth.resolver)
    try:
        generation=reopened.attach_channel(s.run,s.token,s.nonce,'incarnation-a')['connection_generation']
        r=work.resource
        await a.resource_released(s.run,s.token,generation,runtime_incarnation=r.runtime_incarnation,
            resource_kind=r.resource_kind,resource_id=r.resource_id,compute_claim=r.compute_claim,dispatch_generation=r.dispatch_generation)
        await a.resource_state(s.run,s.token,generation,runtime_incarnation='incarnation-a',state='idle')
        await a.register_document_actor(s.auth.p.admin)
        assert await reopened.next_channel_work(s.run,s.token,generation) is None
        status=reopened.complete(s.run,s.token,payload['id'],payload['claim'],SPACE,
            [[1]+[0]*1023],{},connection_generation=generation)
        assert a.durable_result_settled(payload['id'],payload['claim'],status)
        assert not a.status()['document_result_pending']
    finally:a.close()


@pytest.mark.asyncio
async def test_actor_head_seek_sees_later_person_behind_large_legacy_actor_tail(state):
    s=state
    # Synthetic legacy history may predate the current enqueue cap. Candidate
    # lookup must not confuse this with authority to claim the second actor.
    with s.q.connect() as db:
        for i in range(300):
            db.execute("insert into jobs(id,actor,idempotency,kind,texts,identity,state,created,updated) values(?,?,?,'document','[\"synthetic\"]','{}','pending',?,?)",
                (f'legacy-{i}',s.auth.p.owner.subject,f'legacy-{i}',i,i))
        db.execute("insert into jobs(id,actor,idempotency,kind,texts,identity,state,created,updated) values(?,?,?,'document','[\"synthetic\"]','{}','pending',999,999)",
            ('later-person',s.auth.p.other.subject,'later-person'))
    _,_,heads,_=s.q._admission_snapshot(s.run,s.token,1,
        document_actors=(s.auth.p.owner.subject,s.auth.p.other.subject))
    assert len(heads)==2
    assert {r['actor'] for r in heads}=={s.auth.p.owner.subject,s.auth.p.other.subject}
    assert next(r['id'] for r in heads if r['actor']==s.auth.p.other.subject)=='later-person'
    with s.q.connect(write=False) as db:
        plan=' '.join(str(tuple(r)) for r in db.execute("explain query plan select * from jobs where state='pending' and kind='document' and actor=? order by created,id limit 1",(s.auth.p.other.subject,)))
    assert 'bge_admission_actor' in plan and 'TEMP B-TREE' not in plan


@pytest.mark.asyncio
async def test_immediate_intake_wake_no_batch_and_no_lost_completion_wake(state):
    s=state
    await s.a.wait_channel_work(0) # consume startup hint
    await begin(s,'wake')
    started=time.monotonic()
    await s.q.wait_channel_work(.25)
    assert time.monotonic()-started<.1
    assert (await s.q.next_channel_work(s.run,s.token,1)).query.request_id=='wake'


@pytest.mark.asyncio
async def test_exact_native_terminal_required_to_change_owned_run(state):
    s=state
    await begin(s,'terminal')
    work=await s.q.next_channel_work(s.run,s.token,1);s.a.mark_work_sent(work)
    expected=RunIdentity('fixture-owner/stable-slug','7',None,s.run,s.nonce,'incarnation-a')
    caps=CapabilityReceipt('fixture','1','fixture','synthetic-evidence',exact_status=True,status_identifies_execution=True)
    # Heartbeat exactly1000: observation must be later, now1000 cannot assert it.
    unknown=await s.a.confirm_terminal(expected,caps,None,max_age_seconds=10)
    assert not unknown.ok and s.a.status()['state']=='busy'
    s.q.clock=lambda:1002.
    wrong=TerminalObservation(replace(expected,runtime_instance_id='other'),'complete',1001.,execution_binding_verified=True)
    assert not (await s.a.confirm_terminal(expected,caps,wrong,max_age_seconds=10)).ok
    terminal=TerminalObservation(expected,'complete',1001.,execution_binding_verified=True)
    assert (await s.a.confirm_terminal(expected,caps,terminal,max_age_seconds=10)).ok
    assert s.a.status()['state']=='unknown'
    with s.q.connect(write=False) as db:
        assert db.execute('select run_id from bge_shared_admission').fetchone()[0] is None
        assert db.execute('select status from runs where id=?',(s.run,)).fetchone()[0]=='expired'


@pytest.mark.asyncio
async def test_noncurrent_successor_cannot_clear_ram_before_owned_baseline(state):
    s=state
    ticket=await begin(s,'owned-inflight')
    work=await s.q.next_channel_work(s.run,s.token,1)
    s.a.mark_work_sent(work)
    attachment=s.a._attachment
    slot=s.a._slot_version
    with s.q.connect() as db:
        successor=s.q._new_run(db,1000.)
        db.execute('update control set successor=? where id=1',(successor,))
    token=(s.q.path.parent/'bge-worker-credentials'/successor).read_text()
    nonce=s.q.prepare_channel(successor)
    generation=s.q.attach_channel(successor,token,nonce,'successor')['connection_generation']
    with pytest.raises(StaleAttachment):
        await s.a.resource_state(successor,token,generation,runtime_incarnation='successor',state='idle')
    assert await s.q.next_channel_work(successor,token,generation) is None
    assert s.a._work is work and s.a._sent
    assert s.a._attachment==attachment and s.a._slot_version is slot
    assert not ticket.result.done()
    with s.q.connect() as db:
        db.execute('update control set current_run=?,successor=null where id=1',(successor,))
    assert await s.q.next_channel_work(successor,token,generation) is None
    assert s.a._work is work and not ticket.result.done()
    await s.a.resource_state(successor,token,generation,runtime_incarnation='successor',state='idle')
    assert ticket.result.cancelled()
    assert s.a._work is None and not s.a._sent
    with s.q.connect(write=False) as db:
        owner=db.execute('select run_id,incarnation from bge_shared_admission where id=1').fetchone()
    assert tuple(owner)==(successor,'successor')
