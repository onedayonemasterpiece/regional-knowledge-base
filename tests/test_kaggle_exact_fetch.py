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


@pytest.mark.parametrize('metadata,title', [
    ({'source_provenance': {'catalog': {'title': 'История города'}}}, 'История города'),
    ({'title': 'Точное название', 'source_provenance': {'catalog': {'title': 'Книга'}}},
        'Книга'),
    ({'source_provenance': {'catalog': {'title': '   '}}}, PASSAGE),
    ({'source_provenance': {'catalog': 'invalid'}}, PASSAGE),
    ({}, PASSAGE),
])
def test_search_evidence_and_exact_fetch_share_catalog_title_without_changing_locator(metadata,title):
    from test_kaggle_protocol import item
    loc = locator()
    row = item(kind='source', id=PASSAGE, metadata=metadata, dependencies={
        'source_ids': (SOURCE,), 'source_revisions': (loc.source,),
        'variant_refs': (), 'grant_refs': ()})
    common = dict(request_id='title-read', connection_generation=4, turn_generation=9,
        status='ok', evidence=(row,), observed_data_seq=12, observed_projection_seq=12,
        observed_visibility_epoch=loc.visibility_epoch,
        observed_database_generation=loc.database_generation)
    search = QueryResult(mode='hybrid', **common)
    attempt = ReadAttempt('b' * 32)
    thin = KaggleSourceBackend._render(search, evidence=False, attempt=attempt).results[0]
    evidence = KaggleSourceBackend._render(search, evidence=True, attempt=attempt).evidence[0]
    fetched = KaggleSourceBackend._render_fetch(PassageReadResult(**common),
        locator=parse_passage_fetch_id(thin.id), attempt=attempt)
    assert thin.title == evidence.title == fetched.title == title
    assert thin.id == evidence.id == fetched.id == passage_fetch_id(loc)
    assert thin.url == evidence.url == fetched.url == 'urn:rkb:passage:' + PASSAGE
    assert fetched.text == evidence.text == row.text
    assert fetched.metadata['source_revisions'] == [loc.source.model_dump()]


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
    ({'database_generation': 'old-db'}, 'evidence_withdrawn'),
    ({'text_offset': 16002}, 'invalid_input'),
])
async def test_worker_exact_read_rejects_stale_or_impossible_locator(change,reason):
    lease = Lease(ExactConnection())
    result = await KaggleWorkerQuery(acquire=lease.acquire, tokenizer=pinned())(
        request(locator(**change)).model_dump(mode='json'), encode=forbidden_encode)
    assert result['reason'] == reason and not result['evidence'] and 'mode' not in result
    assert lease.entered == lease.exited == 1


@pytest.mark.asyncio
async def test_worker_exact_read_accepts_unchanged_source_after_global_epoch_advance():
    lease = Lease(ExactConnection())
    result = await KaggleWorkerQuery(acquire=lease.acquire, tokenizer=pinned())(
        request(locator(visibility_epoch=2)).model_dump(mode='json'), encode=forbidden_encode)
    assert result['status'] == 'ok' and result['reason'] is None
    assert result['observed_visibility_epoch'] == 3
    assert result['evidence'][0]['id'] == PASSAGE
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
async def test_exact_gate_accepts_new_global_epoch_when_source_revision_is_unchanged(env,auth):
    gate,authority,b = await setup(env,auth)
    permit = await gate.dispatch(authority,gate_request(),b)
    fresh = gate_result().model_copy(update={'observed_visibility_epoch': 42})
    gated = await gate.finish(permit,fresh)
    assert gated.result.status == 'ok' and gated.result.evidence == fresh.evidence
    assert gate.issuance_current(gated.ticket)


@pytest.mark.asyncio
async def test_exact_gate_rejects_wrong_result_type_source_or_passage(env,auth):
    gate,authority,b = await setup(env,auth)
    for bad in [gate_result(gate_item(OTHER)), gate_result(gate_item(CONTROL_SOURCE,id='other')),
        QueryResult(request_id='read', connection_generation=1, turn_generation=1,
            status='ok', mode='hybrid', observed_data_seq=0, observed_projection_seq=0)]:
        permit = await gate.dispatch(authority,gate_request(),b)
        with pytest.raises(SourceGateDenied):
            await gate.finish(permit,bad)


# Real OAuth/shared admission and worker callback; SQL here is a synthetic API
# fixture. The separate public_exact_fetch_roundtrip suite uses native PG.
from test_kaggle_public_backend import target, work, release
from regional_knowledge.read_issuance import capture_read_output


@pytest.mark.asyncio
async def test_public_search_id_fetch_uses_typed_shared_slot_without_reembedding(target):
    import asyncio
    s = target
    class CombinedConnection(ExactConnection):
        async def fetchone(self):
            if 'SELECT p.passage_id' in self.statement:
                return await super().fetchone()
            return (12,12,3,True,True,'synthetic-db')
    lease = Lease(CombinedConnection())
    seen = []
    async def encode(text):
        seen.append(text)
        return [1.] + [0.] * 1023
    handler = KaggleWorkerQuery(acquire=lease.acquire,tokenizer=pinned())
    async def consume(awaitable):
        with capture_read_output() as calls:
            output = await awaitable
        calls[0].complete(getattr(output,'status',None),getattr(output,'reason',None))
        calls[0].release()
        return output
    search = asyncio.create_task(consume(s.backend.search('harbour',s.principal)))
    first = await work(s)
    wire = await handler(first.query.model_dump(mode='json'),encode=encode)
    assert await s.admission.finish_read(s.run,s.token,s.generation,decode_envelope(json.dumps(wire).encode()))
    output = await search
    fetch_id = output.results[0].id
    assert parse_passage_fetch_id(fetch_id).text_offset == 0
    await release(s,first)
    exact = asyncio.create_task(consume(s.backend.fetch(fetch_id,s.principal)))
    second = await work(s)
    assert type(second.query) is PassageReadRequest and second.kind == 'read'
    wire = await handler(second.query.model_dump(mode='json'),encode=forbidden_encode)
    assert await s.admission.finish_read(s.run,s.token,s.generation,decode_envelope(json.dumps(wire).encode()))
    fetched = await exact
    assert fetched.status == 'ok' and fetched.text == 'x' * 8000
    assert fetched.metadata['next_id'] and not fetched.metadata['continuation_complete']
    assert seen == ['harbour'] and lease.entered == lease.exited == 2
    # Result delivery cannot free the exact read's actual slot.
    assert s.admission.status()['state'] == 'busy'
    await release(s,second)
