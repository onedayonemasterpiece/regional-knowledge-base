"""P0 read-path regressions using existing trusted OAuth/queue/native envelopes.

A synthetic worker result stands in for the already-validated PG response.
The PostgreSQL source/Case SQL coverage remains in its existing PG17 suite.
No Kaggle/provider calls or new read capabilities.
"""
import asyncio
from types import SimpleNamespace

import pytest

from regional_knowledge.kaggle_case_contracts import CaseReadResult
from regional_knowledge.kaggle_protocol import QueryResult
from regional_knowledge.read_issuance import capture_read_output
from test_bge_admission import state
from test_case_read_gate import header,request,result as case_result,setup as case_setup
from test_control_spool import env,stage,SOURCE,OTHER
from test_kaggle_public_backend import target,work,release
from test_read_authority import auth
from test_source_read_gate import item,query,result,setup as source_setup
from test_participants import people


@pytest.mark.asyncio
async def test_pending_next_source_stage_and_lost_history_keep_pg_published_items(env,auth):
    gate,actor,binding=await source_setup(env,auth)
    env.spool.invalidate_source_history()
    await stage(env)  # the next write has not modified the native PG snapshot
    permit=await gate.dispatch(actor,query(),binding)
    from_native=result(item(SOURCE,id='published-page'),item(OTHER,id='other-book'))
    gated=await gate.finish(permit,from_native)
    assert gated.result.status=='ok'
    assert [i.id for i in gated.result.evidence]==['published-page','other-book']
    assert not gated.result.prior_write_pending
    assert gate.issuance_current(gated.ticket)
    refreshed=await gate.refresh(gated)
    assert refreshed.result.status=='ok' and len(refreshed.result.evidence)==2
    binding.database_generation='forked-database'
    assert not gate.issuance_current(refreshed.ticket)


@pytest.mark.asyncio
async def test_actual_shared_queue_search_with_live_pg_binding_ignores_history_failure(target):
    s=target
    s.backend.startup=SimpleNamespace(reads_current=lambda:False)
    with capture_read_output() as outputs:
        task=asyncio.create_task(s.backend.search('existing-page',s.principal))
        dispatched=await work(s)
        native=QueryResult(request_id=dispatched.query.request_id,
            connection_generation=s.generation,turn_generation=dispatched.query.turn_generation,
            status='ok',mode='hybrid',observed_data_seq=5,observed_projection_seq=5,
            observed_visibility_epoch=0,observed_database_generation='synthetic-db',
            evidence=())
        assert await s.admission.finish_read(s.run,s.token,s.generation,native)
        returned=await task
        assert returned.status=='ok' and returned.results==[]
        assert outputs
        for pending in outputs:pending.release()
        await release(s,dispatched)


@pytest.mark.asyncio
async def test_case_native_pg_actor_role_serves_with_no_global_history_and_denies_unauthorized(env,auth):
    gate,actor,binding,_=await case_setup(env,auth)
    env.spool.invalidate_source_history()
    permit=await gate.dispatch(actor,request(),binding)
    granted=await gate.finish(permit,case_result(header()))
    assert granted.result.status=='ok' and granted.result.record['case_role']==4
    assert gate.issuance_current(granted.ticket)
    # The exact native read_disagreement_v1 function returns NULL for missing
    # actor grant; transport emits denial with no private bytes.
    permit=await gate.dispatch(actor,request(),binding)
    denied=CaseReadResult(request_id='case-read',connection_generation=1,turn_generation=1,
        status='denied',reason='access_denied',record_b64=None,
        observed_data_seq=7,observed_projection_seq=7,
        observed_visibility_epoch=0,observed_database_generation='db-source')
    rejected=await gate.finish(permit,denied)
    assert rejected.result.status=='denied' and rejected.result.record is None
