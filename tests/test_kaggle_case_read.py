"""Synthetic SQL leases and real typed worker/admission; no provider or PG."""
import base64
import json
import time

import pytest
from regional_knowledge.kaggle_case_contracts import CaseReadRequest,CaseReadResult,canonical
from regional_knowledge.kaggle_geo_contracts import GeoReadRequest,GeoReadResult
from regional_knowledge.kaggle_protocol import encode_envelope,decode_envelope
from regional_knowledge.kaggle_worker_query import KaggleWorkerQuery
from regional_knowledge.disagreement_native import CASE_REVISIONS
from test_kaggle_worker_query import Lease,pinned,forbidden_encode
from test_kaggle_pg_search import FixtureConnection
from test_bge_admission import state,auth,people,query,release
from test_disagreement_native import CASE,ACTOR


def request(**changes):
    return CaseReadRequest(request_id='case-read-1',connection_generation=4,turn_generation=9,
        case_id=CASE,min_data_seq=12,deadline_ms=2000,compute_claim='controller-claim',
        actor=dict(person_id=ACTOR,audience='personal',auth_epoch=2,visibility_epoch=100),**changes)


class Connection(FixtureConnection):
    def __init__(self,record):super().__init__();self.record=record
    async def fetchone(self):
        if 'FROM rkb_v4.read_frontier' in self.statement or 'CROSS JOIN rkb_v4.read_frontier' in self.statement:
            return (12,12,3,True,False,'synthetic-db')
        if 'SELECT schema_revision' in self.statement:return CASE_REVISIONS.wire()
        return (self.record,)


@pytest.mark.parametrize('changes',[dict(case_id='not-uuid'),dict(section='draft_get'),
    dict(section='case',draft_version=1),dict(section='operation'),dict(section='case',after_seq=1)])
def test_fixed_selectors(changes):
    values=json.loads(request().model_dump_json());values.update(changes)
    with pytest.raises(ValueError):CaseReadRequest.model_validate(values)


@pytest.mark.asyncio
async def test_real_worker_native_case_read_never_counts_or_encodes():
    record=dict(case_id=CASE,case_revision=1,acl_revision=1,event_seq=1,source_dependencies=[],section='case')
    lease=Lease(Connection(record))
    async def forbidden_count(_):raise AssertionError('Case read counted BGE input')
    handler=KaggleWorkerQuery(lease.acquire,pinned(forbidden_count))
    result=decode_envelope(json.dumps(await handler(json.loads(encode_envelope(request())),encode=forbidden_encode)).encode())
    assert type(result) is CaseReadResult and result.status=='ok' and result.record==record and not result.evidence
    assert lease.entered==lease.exited==1
    sql=[s for s,p in lease.connection.statements if 'read_disagreement_v1' in s]
    assert len(sql)==1
    params=next(p for s,p in lease.connection.statements if 'read_disagreement_v1' in s)
    assert params[:3]==(CASE,ACTOR,'case')
    altered=result.record;altered['case_revision']=9
    assert result.record['case_revision']==1 # immutable canonical byte payload


@pytest.mark.asyncio
async def test_case_denial_is_private_content_free():
    handler=KaggleWorkerQuery(Lease(Connection(None)).acquire,pinned())
    result=decode_envelope(json.dumps(await handler(json.loads(encode_envelope(request())),encode=forbidden_encode)).encode())
    assert result.status=='denied' and result.reason=='access_denied' and result.record is None


@pytest.mark.asyncio
async def test_case_admission_rejects_cross_operation_result_and_retains_actual_resource(state):
    s=state
    projection=query(s.context,'case-admitted',s.generation).actor
    q=CaseReadRequest(request_id='case-admitted',connection_generation=s.generation,turn_generation=0,
        case_id=CASE,actor=projection,min_data_seq=0,deadline_ms=2000)
    ticket=await s.a.begin_read(s.context,q,dialog_ref='case-dialog',deadline=time.monotonic()+1.9)
    work=await s.q.next_channel_work(s.run,s.token,s.generation);s.a.mark_work_sent(work)
    assert type(work.query) is CaseReadRequest
    wrong=GeoReadResult(request_id=q.request_id,connection_generation=s.generation,turn_generation=0,
        status='unavailable',reason='not_ready',observed_data_seq=None,observed_projection_seq=None)
    with pytest.raises(ValueError,match='operation type'):
        await s.a.finish_read(s.run,s.token,s.generation,wrong)
    result=CaseReadResult(**{k:getattr(q,k) for k in ('request_id','connection_generation','turn_generation')},
        status='ok',record_b64=base64.b64encode(canonical(dict(case_id=CASE))).decode(),
        observed_data_seq=7,observed_projection_seq=7,observed_visibility_epoch=1,observed_database_generation='db')
    assert await s.a.finish_read(s.run,s.token,s.generation,result)
    assert (await ticket.result).record==dict(case_id=CASE)
    assert s.a.status()['state']=='busy'
    assert await release(s,work)
