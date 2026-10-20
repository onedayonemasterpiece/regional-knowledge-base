"""Exact source locators, worker callback and final authority/provenance fences."""
import json
from uuid import UUID

import pytest

from regional_knowledge.kaggle_protocol import (
    PassageContinuation, PassageReadRequest, PassageReadResult, QueryResult,
    SourceDependency, decode_envelope, encode_envelope,
)
from regional_knowledge.kaggle_public_backend import (
    KaggleSourceBackend, passage_fetch_id, parse_passage_fetch_id,
)
from regional_knowledge.kaggle_worker_query import KaggleWorkerQuery
from regional_knowledge.query_telemetry import ReadAttempt
from regional_knowledge.source_read_gate import SourceGateDenied
from test_kaggle_worker_query import Lease, ServingConnection, pinned, forbidden_encode, SOURCE, PASSAGE
from test_source_read_gate import setup, item as gate_item
from test_control_spool import env, stage, SOURCE as CONTROL_SOURCE, OTHER
from test_read_authority import auth
from test_participants import people


def locator(**changes):
    values = dict(database_generation='synthetic-db', visibility_epoch=3,
        source=SourceDependency(source_id=SOURCE, content_revision=1, eligibility_revision=7),
        passage_id=PASSAGE, text_offset=0, bibliography_offset=0)
    values.update(changes)
    return PassageContinuation(**values)


def request(loc=None, **changes):
    values = dict(request_id='exact-read', connection_generation=4, turn_generation=9,
        locator=loc or locator(), actor=dict(person_id='untrusted', audience='personal',
            auth_epoch=0, visibility_epoch=0), min_data_seq=12, deadline_ms=2000,
        compute_claim='same-compute-owner')
    values.update(changes)
    return PassageReadRequest(**values)


class ExactConnection(ServingConnection):
    async def fetchone(self):
        if 'SELECT p.passage_id' in self.statement:
            return (UUID(PASSAGE), UUID(SOURCE), 1, 'page 1', 'x' * 16001, 'b' * 2200, 7)
        # Dense readiness is deliberately false: exact read performs no inference.
        return (12, 12, 3, True, False, 'synthetic-db')


def test_versioned_locator_is_self_contained_strict_and_not_a_capability():
    value = locator()
    assert parse_passage_fetch_id(passage_fetch_id(value)) == value
    assert decode_envelope(encode_envelope(request())).locator == value
    for bad in ['source-uuid', passage_fetch_id(value) + '=', 'rkb:passage:v2:any', 'x'*4097]:
        with pytest.raises(ValueError):
            parse_passage_fetch_id(bad)
    payload = request().model_dump(mode='json')
    for fields in [{'query': SOURCE}, {'operation': 'knowledge_search'}, {'operation_version': True}]:
        with pytest.raises(ValueError):
            decode_envelope(json.dumps(dict(payload, **fields)).encode())


@pytest.mark.asyncio
async def test_worker_exact_read_pages_text_and_bibliography_without_tokenizer_or_dense():
    async def no_count(_):
        raise AssertionError('exact read reached tokenizer')
    lease = Lease(ExactConnection())
    handler = KaggleWorkerQuery(acquire=lease.acquire, tokenizer=pinned(no_count))
    loc = locator()
    bodies, bibliographies = [], []
    for number in range(3):
        result = decode_envelope(json.dumps(await handler(
            request(loc).model_dump(mode='json'), encode=forbidden_encode)).encode())
        assert type(result) is PassageReadResult and result.status == 'ok'
        assert 'mode' not in result.model_dump()
        row = result.evidence[0]
        bodies.append(row.text)
        bibliographies.append(row.metadata.citation)
        rendered = KaggleSourceBackend._render_fetch(result, locator=loc, attempt=ReadAttempt('a' * 32))
        assert rendered.metadata['text_offset'] == number * 8000
        if number < 2:
            assert not rendered.metadata['continuation_complete']
            loc = parse_passage_fetch_id(rendered.metadata['next_id'])
        else:
            assert rendered.metadata['continuation_complete'] and rendered.metadata['next_id'] is None
    assert ''.join(bodies) == 'x' * 16001 and ''.join(bibliographies) == 'b' * 2200
    assert lease.entered == lease.exited == 3
    statements = repr(lease.connection.statements)
    assert 'lexical_candidates' not in statements and 'dense_candidates' not in statements


@pytest.mark.asyncio
@pytest.mark.parametrize('change,reason', [
    ({'visibility_epoch': 2}, 'evidence_withdrawn'),
    ({'database_generation': 'old-db'}, 'evidence_withdrawn'),
    ({'source': SourceDependency(source_id=SOURCE,content_revision=1,eligibility_revision=8)}, 'evidence_withdrawn'),
    ({'text_offset': 16002}, 'invalid_input'),
])
async def test_worker_exact_read_rejects_stale_or_impossible_locator(change,reason):
    lease = Lease(ExactConnection())
    result = await KaggleWorkerQuery(acquire=lease.acquire, tokenizer=pinned())(
        request(locator(**change)).model_dump(mode='json'), encode=forbidden_encode)
    assert result['reason'] == reason and not result['evidence'] and 'mode' not in result
    assert lease.entered == lease.exited == 1


def gate_request(source=CONTROL_SOURCE):
    return request(PassageContinuation(database_generation='db-source', visibility_epoch=0,
        source=SourceDependency(source_id=source, content_revision=1, eligibility_revision=1),
        passage_id='passage', text_offset=0, bibliography_offset=0),
        request_id='read', connection_generation=1, turn_generation=1, min_data_seq=0)


def gate_result(row=None):
    return PassageReadResult(request_id='read', connection_generation=1, turn_generation=1,
        status='ok', evidence=(row or gate_item(CONTROL_SOURCE),), observed_data_seq=0,
        observed_projection_seq=0, observed_visibility_epoch=0, observed_database_generation='db-source')


@pytest.mark.asyncio
async def test_exact_gate_rechecks_pending_source_and_withholds_entire_continuation(env,auth):
    gate,authority,b = await setup(env,auth)
    permit = await gate.dispatch(authority, gate_request(), b)
    gated = await gate.finish(permit, gate_result())
    assert gated.result.status == 'ok' and gate.issuance_current(gated.ticket)
    await stage(env)
    assert not gate.issuance_current(gated.ticket)
    refreshed = await gate.refresh(gated)
    assert refreshed.result.status == 'unavailable' and not refreshed.result.evidence
    assert refreshed.result.reason == 'prior_write_pending'
    assert 'mode' not in refreshed.result.model_dump()


@pytest.mark.asyncio
async def test_exact_gate_rejects_wrong_result_type_source_or_passage(env,auth):
    gate,authority,b = await setup(env,auth)
    for bad in [gate_result(gate_item(OTHER)), gate_result(gate_item(CONTROL_SOURCE,id='other')),
        QueryResult(request_id='read', connection_generation=1, turn_generation=1,
            status='ok', mode='hybrid', observed_data_seq=0, observed_projection_seq=0)]:
        permit = await gate.dispatch(authority,gate_request(),b)
        with pytest.raises(SourceGateDenied):
            await gate.finish(permit,bad)
