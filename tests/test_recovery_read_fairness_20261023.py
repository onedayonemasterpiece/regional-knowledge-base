"""Ordinary reads beside retained maintenance, using the real queue/admission."""
import asyncio

import pytest

from regional_knowledge.kaggle_recovery_dispatch import RecoveryPhaseDispatch
from test_bge_admission import state, auth, people, begin, answer, release
from test_kaggle_recovery_phases import database, work_for


@pytest.mark.asyncio
@pytest.mark.parametrize('position',['pending','claimed_unsent','released_unknown'])
async def test_recovery_keeps_its_receipt_identity_without_excluding_reads(state,position):
    s=state
    bridge=RecoveryPhaseDispatch(s.q,database=database(),identity=(s.run,s.nonce,'incarnation-a'),
        binding_current=lambda b:b is s)
    task=None
    try:
        if position=='pending':
            task=asyncio.create_task(bridge.request(s,'verify',{'reference':'retained'},operation_key='maintenance'))
            for _ in range(100):
                if bridge._bindings:break
                await asyncio.sleep(.001)
            else:pytest.fail('maintenance did not enqueue')
            job_id=next(iter(bridge._bindings))
        else:
            task,work,frame=await work_for(s,bridge)
            job_id=frame['id']
            if position=='claimed_unsent':
                # work_for marks sent; instead simulate a fresh ordered worker
                # probe proving that the frame never reached its process.
                nonce=await s.a.begin_resource_probe(s.run,s.token,s.generation)
                await s.a.resource_state(s.run,s.token,s.generation,
                    runtime_incarnation='incarnation-a',state='idle',probe_nonce=nonce)
            else:
                # Compute is known finished, its outcome is still unknown.
                await release(s,work)
        with s.q.connect(write=False) as db:
            original=tuple(db.execute('select state,claim,attempt,result from jobs where id=?',(job_id,)).fetchone())
        ticket=await begin(s,'ordinary-read')
        read=await s.a.next_channel_work(s.run,s.token,s.generation)
        assert read is not None and read.kind=='read'
        s.a.mark_work_sent(read)
        assert await s.a.finish_read(s.run,s.token,s.generation,answer(read))
        assert (await ticket.result).status=='ok'
        await release(s,read)
        with s.q.connect(write=False) as db:
            assert tuple(db.execute('select state,claim,attempt,result from jobs where id=?',(job_id,)).fetchone())==original
        assert not task.done()  # Neither fake ACK nor maintenance reexecution.
    finally:
        if task is not None:
            task.cancel()
            await asyncio.gather(task,return_exceptions=True)
        await bridge.aclose()
