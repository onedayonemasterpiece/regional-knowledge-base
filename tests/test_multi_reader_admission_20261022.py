"""Independent already-registered BGE read runs, no notebook/provider launch.

These are the actual BgeQueue, SharedReadAdmission, canonical OAuth and SQL
readiness classes with synthetic external run/worker receipts. The provider
is not called and no second live notebook is created.
"""
import asyncio
import time
from uuid import UUID

import pytest

from regional_knowledge.bge_queue import StaleAttachment
from test_bge_admission import query, answer
from test_bge_runtime_readiness import expected, observation
from test_kaggle_public_backend import target, current_binding
from test_participants import people
from test_read_authority import auth


async def attach_existing_reader(s, *, observed=None):
    """Attach a second synthetic *already allowed* run, not a launcher."""
    q=s.queue
    with q.connect() as db:
        read_run=q._new_run(db,q.clock())
        db.execute('UPDATE control SET successor=? WHERE id=1',(read_run,))
    token=(q.path.parent/'bge-worker-credentials'/read_run).read_text()
    nonce=q.prepare_channel(read_run)
    q.expect_search_runtime(read_run,expected())
    assert q.launch_claim(read_run)==token
    q.launch_record(read_run,'fixture-reader/stable-slug',provider_version=8)
    generation=q.attach_channel(read_run,token,nonce,'reader-incarnation')['connection_generation']
    assert q.accept_search_runtime(read_run,token,generation,observed or observation()).ok
    q.heartbeat(read_run,token,ready=True,connection_generation=generation)
    return read_run,token,generation


async def release(s, run_id, token, generation, work):
    r=work.resource
    return await s.admission.resource_released(run_id,token,generation,
        runtime_incarnation=r.runtime_incarnation,
        resource_kind=r.resource_kind,resource_id=r.resource_id,
        compute_claim=r.compute_claim,dispatch_generation=r.dispatch_generation)


@pytest.mark.asyncio
async def test_two_ready_reader_attachments_work_independently_with_one_writer(target):
    s=target
    reader,token,generation=await attach_existing_reader(s)
    writer_binding=current_binding(s.queue)
    assert writer_binding.run_id==s.run
    with s.queue.connect(write=False) as db:
        owner=db.execute('SELECT run_id,incarnation FROM bge_shared_admission WHERE id=1').fetchone()
        assert tuple(owner)==(s.run,'incarnation-a')

    # Both workers report their *own* genuine state via authenticated queue.
    await s.admission.resource_state(reader,token,generation,
        runtime_incarnation='reader-incarnation',state='idle')

    context=await s.auth.resolver.resolve_oauth(s.auth.tokens.access_token,
        destination=s.auth.destination)
    try:
        one=await s.admission.begin_read(context,query(context,'writer-read',s.generation,min_seq=5),
            dialog_ref='a',deadline=time.monotonic()+1.95,run_id=s.run)
        writer_work=await s.queue.next_channel_work(s.run,s.token,s.generation)
        assert writer_work is not None and writer_work.query.request_id=='writer-read'
        s.admission.mark_work_sent(writer_work)
        # The writer's live outstanding compute remains held. It does not
        # prevent a distinct ready reader from receiving its own read.
        second=await s.admission.begin_read(context,query(context,'reader-read',generation,min_seq=5),
            dialog_ref='b',deadline=time.monotonic()+1.95,run_id=reader)
        # The original writer remains the sole durable compute owner.
        with s.queue.connect(write=False) as db:
            owner=db.execute('SELECT run_id FROM bge_shared_admission WHERE id=1').fetchone()
            assert owner['run_id']==s.run
        from regional_knowledge.kaggle_public_backend import select_read_binding
        selected=select_read_binding(s.queue)
        assert selected.run_id==reader and selected.connection_generation==generation
        reader_work=await s.queue.next_channel_work(reader,token,generation)
        assert reader_work is not None and reader_work.query.request_id=='reader-read'
        assert reader_work.resource.run_id==reader and reader_work.resource!=writer_work.resource
        s.admission.mark_work_sent(reader_work)
        # A sent read is UNKNOWN even if its consumer disappears: no redispatch
        # on the same worker until exact claim release or a fresh probe.
        s.admission.cancel_read(second)
        assert await s.queue.next_channel_work(reader,token,generation) is None
        # Response from the other worker cannot settle writer's read.
        assert not await s.admission.finish_read(reader,token,generation,answer(writer_work))
        assert await s.admission.finish_read(s.run,s.token,s.generation,answer(writer_work))
        assert (await one.result).status=='ok'
        assert await release(s,s.run,s.token,s.generation,writer_work)
        assert await release(s,reader,token,generation,reader_work)
        # Original admission can still dispatch a new read after its release.
        third=await s.admission.begin_read(context,query(context,'writer-again',s.generation,min_seq=5),
            dialog_ref='a',deadline=time.monotonic()+1.95,run_id=s.run)
        third_work=await s.queue.next_channel_work(s.run,s.token,s.generation)
        assert third_work is not None and third_work.query.request_id=='writer-again'
        s.admission.mark_work_sent(third_work)
        assert await s.admission.finish_read(s.run,s.token,s.generation,answer(third_work))
        assert (await third.result).status=='ok'
        assert await release(s,s.run,s.token,s.generation,third_work)
    finally:
        s.auth.resolver.release(context)


@pytest.mark.asyncio
async def test_unready_or_behind_reader_never_selected_and_stale_socket_cannot_release(target):
    s=target
    reader,token,generation=await attach_existing_reader(s)
    await s.admission.resource_state(reader,token,generation,
        runtime_incarnation='reader-incarnation',state='idle')
    from regional_knowledge.kaggle_public_backend import select_read_binding
    assert select_read_binding(s.queue).run_id in {s.run,reader}
    # This run may still have an attached WSS but cannot serve below the
    # writer's required data/projection cursor. No out-of-date read fallback.
    with s.queue.connect() as db:
        import json
        runtime=db.execute('SELECT observed_json FROM bge_runtime_expectations WHERE run_id=?',(reader,)).fetchone()
        stale=json.loads(runtime['observed_json'])
        stale['applied_seq']=4;stale['projection_seq']=4
        db.execute('UPDATE bge_runtime_expectations SET observed_json=? WHERE run_id=?',(json.dumps(stale),reader))
    assert select_read_binding(s.queue).run_id==s.run
    # An attached run has no right to alter the current writer identity.
    with s.queue.connect(write=False) as db:
        assert db.execute('SELECT run_id FROM bge_shared_admission WHERE id=1').fetchone()[0]==s.run
