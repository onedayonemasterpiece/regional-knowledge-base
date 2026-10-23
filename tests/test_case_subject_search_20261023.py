"""P0 model-chosen Case subject discovery on existing authenticated CASE_READ.

Fixtures use synthetic PG leases and unchanged Case evidence/operation contracts.
No external provider, notebook, auth bypass or page reprocessing.
"""
import asyncio
import json
from types import SimpleNamespace

import pytest

from regional_knowledge.kaggle_case_contracts import (
    CaseReadRequest, CaseReadResult, validate_case_record,
)
from regional_knowledge.kaggle_protocol import encode_envelope, decode_envelope
from regional_knowledge.kaggle_worker_query import KaggleWorkerQuery
from test_kaggle_case_read import Connection
from test_kaggle_worker_query import Lease, pinned, forbidden_encode
from test_disagreement_native import CASE, ACTOR
from test_control_spool import env
from test_read_authority import auth
from test_participants import people

EMPTY_CASE = "00000000-0000-0000-0000-000000000000"
OTHER_CASE = "00000000-0000-0000-0000-000000000009"


def search_request(**changes):
    return CaseReadRequest(
        request_id="case-search-1", connection_generation=4, turn_generation=9,
        case_id=EMPTY_CASE, section="search", search_query="city foundation",
        min_data_seq=12, deadline_ms=2000, compute_claim="controller-claim",
        limit=5,
        actor=dict(person_id=ACTOR, audience="personal", auth_epoch=2,
                   visibility_epoch=100), **changes)


def candidate(case_id=CASE, **changes):
    return dict(case_id=case_id, question="Who founded the city?",
                context="Sources disagree on the attributed date.",
                state="open", case_revision=1, acl_revision=1, event_seq=0, **changes)


@pytest.mark.parametrize("changes", [
    dict(case_id=CASE), dict(section="case"), dict(search_query=" "),
    dict(search_query="x" * 121), dict(search_query="", place=None),
    dict(limit=9), dict(after_case_id="not-uuid"), dict(revision=1),
    dict(operation_key="lookup"), dict(draft_id=CASE),
])
def test_search_selector_is_bounded_and_cannot_be_used_as_case_id_bypass(changes):
    wire=search_request().model_dump(mode="json")
    wire.update(changes)
    with pytest.raises(ValueError):
        CaseReadRequest.model_validate(wire)


def test_search_response_disallows_private_fields_duplicates_wrong_cursors():
    q=search_request()
    record=dict(section="search",items=[candidate()],has_more=False,
                next_after_case=CASE)
    assert validate_case_record(q,record)==record
    for wrong in (
        dict(record, items=[dict(candidate(), private_draft="never")]),
        dict(record, items=[candidate(), candidate()]),
        dict(record, items=[candidate(OTHER_CASE), candidate(CASE)]),
        dict(record, next_after_case=OTHER_CASE),
        dict(record, items=[dict(candidate(), question="")]),
        dict(record, private_case_count=300),
    ):
        with pytest.raises(ValueError):
            validate_case_record(q,wrong)


class SearchConnection(Connection):
    def __init__(self, rows):
        super().__init__(None)
        self.rows=rows

    async def fetchall(self):
        assert "rkb_v4.disagreement_cases c" in self.statement
        assert "case_role_v1" in self.statement
        assert "case_sources_v1" in self.statement
        assert "disagreement_evidence e" in self.statement
        return self.rows


@pytest.mark.asyncio
async def test_actual_case_read_wire_and_pg_lease_use_actor_scoped_bounded_sql():
    # This is the actual worker native read path, no BGE or service restart.
    db=SearchConnection([
        (CASE,1,2,3,"open","Who founded the city?","Reported date dispute."),
    ])
    lease=Lease(db)

    async def no_encode(_):
        raise AssertionError("Case read must never count or encode BGE query")

    handler=KaggleWorkerQuery(lease.acquire,pinned(no_encode))
    query=search_request(place="historic harbour")
    result=decode_envelope(json.dumps(await handler(json.loads(encode_envelope(query)),
                                               encode=forbidden_encode)).encode())
    assert type(result) is CaseReadResult and result.status=="ok"
    record=result.record
    assert record["items"][0]["case_id"]==CASE
    assert record["next_after_case"]==CASE and not record["has_more"]
    assert validate_case_record(query,record)==record
    assert lease.entered==lease.exited==1
    stmt,params=next((s,p) for s,p in db.statements
                     if "rkb_v4.disagreement_cases c" in s)
    assert params[0]==ACTOR and params[1]==EMPTY_CASE
    assert params[2:6]==("city foundation","city foundation",
                         "historic harbour","historic harbour")
    assert params[-1]==query.limit+1


@pytest.mark.asyncio
async def test_pending_next_page_is_not_a_case_subject_read_gate(env,auth):
    from test_case_read_gate import setup
    from regional_knowledge.kaggle_case_contracts import CaseReadResult,canonical
    import base64
    gate,authority,b,anchor=await setup(env,auth)
    q=search_request()
    q=q.model_copy(update=dict(connection_generation=1,request_id="case-page-read",
        turn_generation=1,min_data_seq=0))
    permit=await gate.dispatch(authority,q,b)
    record=dict(section="search",items=[candidate()],has_more=False,next_after_case=CASE)
    result=CaseReadResult(request_id=q.request_id,
        connection_generation=q.connection_generation,turn_generation=q.turn_generation,
        status="ok",record_b64=base64.b64encode(canonical(record)).decode(),
        observed_data_seq=7,observed_projection_seq=7,
        observed_visibility_epoch=1,observed_database_generation="db-source")
    gated=await gate.finish(permit,result)
    assert gated.result.status=="ok"
    assert gate.issuance_current(gated.ticket)
    # The next page has not completed and global history proof turns stale.
    env.spool._source_anchor=None
    env.spool._source_version=object()
    assert gate.issuance_current(gated.ticket)
    # A case-specific pending grant/revoke never leaks this candidate.
    gate._case_restriction_after=lambda case_id,sequence:case_id==CASE
    assert not gate.issuance_current(gated.ticket)
    await gate.refresh(gated)
