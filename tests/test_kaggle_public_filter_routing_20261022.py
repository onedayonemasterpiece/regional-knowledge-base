"""Public MCP filtering routed to fixed native reads; synthetic external I/O only."""
import asyncio
from copy import deepcopy
import hashlib
import json
from types import SimpleNamespace
from uuid import UUID, uuid5

import pytest
from mcp.server.mcpserver.exceptions import ToolError
from starlette.requests import Request

from regional_knowledge.bge_contract import REVISION, SPACE, TOKENIZER_SHA256
from regional_knowledge.kaggle_domain_bindings import DOMAIN_REVISIONS
from regional_knowledge.kaggle_protocol import QueryResult, QueryFilters, decode_envelope
from regional_knowledge.kaggle_query import PinnedTokenizer
from regional_knowledge.kaggle_search import fused_search
from regional_knowledge.kaggle_source_graph_read import (
    SourceGraphReadRequest, execute_source_graph_read,
)
from regional_knowledge.kaggle_worker_query import KaggleWorkerQuery
from regional_knowledge.kaggle_story_read import story_item
from regional_knowledge.read_issuance import _SCOPE_KEY
from regional_knowledge.server import build_server
from test_kaggle_fact_read import Native
from test_kaggle_pg_search import FixtureConnection
from test_kaggle_public_backend import target, work, release
from test_read_authority import auth
from test_participants import people
from test_kaggle_source_graph_backend import configured
from test_kaggle_source_graph_read import row, request as graph_request
from test_kaggle_story_read import record
from test_source_graph import payload
from test_source_import import SOURCE


def context():
    request = Request({'type': 'http', 'method': 'POST', 'path': '/mcp',
        'headers': [], 'app': SimpleNamespace(state=SimpleNamespace()),
        'state': {}})
    call = SimpleNamespace(request_context=SimpleNamespace(
        request=request, request_id='public-filter-routing'))
    return request, call


class SourcePages(Native):
    """Real graph request/function contract with a fake existing SQL cursor."""
    def __init__(self):
        nodes = payload()[0]['source_graph']['entities']
        rows = [row(index) for index in range(len(nodes))]
        super().__init__(sorted(rows, key=lambda item: (str(item[0]), item[3])),
            revision=(DOMAIN_REVISIONS.schema, DOMAIN_REVISIONS.function),
            frontier=(5, 5, 0, True, False, 'synthetic-db'))

    async def fetchall(self):
        if 'read_source_graph_v1' not in self.statement:
            return []
        params = next(params for sql, params in reversed(self.calls)
            if 'read_source_graph_v1' in sql)
        after_source, after_key, take = params[3:]
        rows = self.rows if after_source is None else [
            item for item in self.rows
            if (str(item[0]), item[3]) > (str(after_source), after_key)]
        return rows[:take]


async def forbidden(*_):
    raise AssertionError('exact source graph must not invoke a model/encoder')


@pytest.mark.asyncio
async def test_mcp_entity_list_name_kind_filters_survive_worker_and_paginated_source_scope(
        target, monkeypatch):
    t = target
    startup = await configured(t)
    native = SourcePages()
    worker = KaggleWorkerQuery(acquire=native.acquire,
        tokenizer=PinnedTokenizer(forbidden, REVISION, TOKENIZER_SHA256, SPACE))
    monkeypatch.setattr('regional_knowledge.server._principal', lambda: t.principal)
    server = build_server(target_startup=startup)

    async def read(cursor=None, *, kinds=None, query='SOURCE identity'):
        req, ctx = context()
        selector = ['person', 'poi_ref'] if kinds is None else kinds
        args = {'document_ids': [SOURCE], 'kinds': selector,
            'query': query, 'limit': 1}
        if cursor is not None:
            args['cursor'] = cursor
        pending = asyncio.create_task(server.call_tool('entity_list', args, ctx))
        dispatched = await work(t)
        assert type(dispatched.query) is SourceGraphReadRequest
        assert dispatched.query.source_id == SOURCE
        assert dispatched.query.kinds == (tuple(selector) or None)
        assert dispatched.query.query == query
        wire = await worker(dispatched.query.model_dump(mode='json'), encode=forbidden)
        result = decode_envelope(json.dumps(wire).encode())
        assert result.status == 'ok' and len(result.records) == 1
        assert await t.admission.finish_read(t.run, t.token, t.generation, result)
        output = await pending
        assert result.records[0]['data']['canonical_label'] in str(output)
        issued = req.scope[_SCOPE_KEY][1]
        assert issued.gate is startup.gate
        assert issued.gate.issuance_current(issued.gated.ticket)
        issued.release()
        await release(t, dispatched)
        return result, dispatched.query

    try:
        first, frame = await read()
        assert first.records[0]['data']['kind'] == 'person'
        assert first.next_cursor
        second, _ = await read(first.next_cursor)
        assert second.records[0]['data']['kind'] == 'poi_ref'
        empty_kinds, frame_unfiltered = await read(kinds=[], query='')
        assert empty_kinds.records and frame_unfiltered.kinds is None
        # Invalid nonempty public kinds must still be rejected by the typed
        # native request, not silently become an unfiltered read.
        with pytest.raises(ToolError, match='invalid_input'):
            await startup.backend.entity_list(t.principal,
                document_ids=[SOURCE], kinds=['unsupported_kind'], query='')
        # The cursor is tied to the exact kind/query/actor/frontier; changing
        # the selector fails BEFORE the native SQL graph function.
        previous = len([x for x, _ in native.calls if 'read_source_graph_v1' in x])
        changed = frame.model_copy(update={'cursor': first.next_cursor,
            'query': 'different identity'})
        rejected = await execute_source_graph_read(changed, acquire=native.acquire)
        assert rejected.reason == 'invalid_input' and not rejected.records
        assert previous == len([x for x, _ in native.calls if 'read_source_graph_v1' in x])
        assert len([x for x, _ in native.calls if 'read_source_graph_v1' in x]) > 2
        with pytest.raises(ToolError, match='target_graph_exact_source_scope_required'):
            await startup.backend.entity_list(t.principal,
                document_ids=None, kinds=['person'], query='person')
    finally:
        await startup.source_dispatch.aclose()
        startup.config.spool.close()


@pytest.mark.asyncio
async def test_mcp_story_search_source_filter_is_preserved_by_query_and_output_gate(
        target, monkeypatch):
    t = target
    startup = await configured(t)
    monkeypatch.setattr('regional_knowledge.server._principal', lambda: t.principal)
    server = build_server(target_startup=startup)
    try:
        req, ctx = context()
        task = asyncio.create_task(server.call_tool('story_search',
            {'query': 'source episode', 'filters': {'source_id': SOURCE}}, ctx))
        dispatched = await work(t)
        assert dispatched.query.filters == QueryFilters(
            kinds=('story',), source_ids=(SOURCE,))
        assert dispatched.query.actor.person_id == t.auth.p.other.subject
        item = story_item(record(source=SOURCE))
        result = QueryResult(request_id=dispatched.query.request_id,
            connection_generation=t.generation,
            turn_generation=dispatched.query.turn_generation,
            status='degraded', mode='hybrid', reason='bounded_retrieval',
            evidence=(item,), observed_data_seq=5, observed_projection_seq=5,
            observed_visibility_epoch=0,
            observed_database_generation='synthetic-db')
        assert await t.admission.finish_read(t.run, t.token, t.generation, result)
        output = await task
        assert item.text in str(output)
        issued = req.scope[_SCOPE_KEY][1]
        assert issued.gate.issuance_current(issued.gated.ticket)
        issued.release()
        await release(t, dispatched)
        for filters, reason in [
            ({'material_type': 'event_narrative'}, 'unsupported_target_story_filter'),
            ({'source_id': 'not-a-uuid'}, 'invalid_input'),
            ({'source_id': SOURCE, 'state': 'drafting'}, 'unsupported_target_story_filter'),
        ]:
            with pytest.raises(ToolError, match=reason):
                await startup.backend.story_search('source episode', t.principal,
                    filters=filters)
        with pytest.raises(ToolError, match='unsupported_target_story_search'):
            await startup.backend.story_search('source episode', t.principal,
                filters={'source_id': SOURCE}, cursor='unsupported')
    finally:
        await startup.source_dispatch.aclose()
        startup.config.spool.close()


@pytest.mark.asyncio
async def test_scoped_story_query_filters_before_every_native_rank_and_hydrate():
    class ExistingReadPool(FixtureConnection):
        async def fetchone(self):
            return (14,)

    connection = ExistingReadPool()

    async def vector(_):
        return [1.0] + [0.0] * 1023

    result = await fused_search(connection, 'source episode', encode=vector,
        filters=QueryFilters(kinds=('story',), source_ids=(SOURCE,)))
    assert result.evidence == ()
    statements = [(sql, args) for sql, args in connection.statements
        if any(name in sql for name in (
            'editorial_lexical_candidates_v1',
            'editorial_dense_candidates_v1',
            'editorial_hydrate_v1'))]
    assert len(statements) == 3
    assert all(args[-1] == [UUID(SOURCE)] for _, args in statements)
    assert connection.cleaned



@pytest.mark.asyncio
async def test_accepted_graph_original_spelling_distinct_from_canonical_and_alias_matches():
    # The real source graph resolver nests original spelling under mention;
    # canonical label and alias deliberately do NOT contain that spelling.
    accepted, _ = payload()
    node = next(item for item in accepted['source_graph']['entities']
        if item['kind'] == 'person')
    original = node['mention']['exact_source_spelling']
    assert original and original.casefold() not in node['canonical_label'].casefold()
    assert all(original.casefold() not in alias['value'].casefold()
        for alias in node['aliases'])
    native = SourcePages()
    request = graph_request(action='list', entity_id=None, source_id=SOURCE,
        kinds=('person',), query=original, min_data_seq=5, limit=1)
    result = await execute_source_graph_read(request, acquire=native.acquire)
    assert result.status == 'ok', result.reason
    assert len(result.records) == 1
    assert result.records[0]['data'] == node
    assert result.records[0]['data']['mention']['exact_source_spelling'] == original


@pytest.mark.asyncio
async def test_eight_empty_filtered_keyset_pages_return_cursor_then_late_original_match():
    native = SourcePages()
    template = next(item for item in payload()[0]['source_graph']['entities']
        if item['kind'] == 'person')
    rows = []
    unique_original = 'UniquePrintedOrthography'
    for ordinal in range(17):
        node = deepcopy(template)
        spelling = unique_original if ordinal == 16 else f'OtherPrintedName{ordinal:03d}'
        evidence = f'{spelling} appears in this synthetic source record.'
        node['key'] = f'fixture-{ordinal:03d}'
        node['entity_id'] = str(uuid5(UUID(SOURCE), f'fixture-entity-{ordinal:03d}'))
        node['kind'] = 'person'
        node['canonical_label'] = f'Unrelated canonical label {ordinal:03d}'
        node['aliases'] = []
        node['mention']['mention_id'] = str(uuid5(UUID(SOURCE), f'fixture-mention-{ordinal:03d}'))
        node['mention']['exact_source_spelling'] = spelling
        node['mention']['evidence']['exact_quote'] = evidence
        node['mention']['evidence']['start'] = 0
        node['mention']['evidence']['end'] = len(evidence)
        node['mention']['evidence']['source_region_sha256'] = hashlib.sha256(evidence.encode()).hexdigest()
        rows.append((UUID(SOURCE), 1, 7, node['key'], node, evidence, 'Synthetic bibliography'))
    native.rows = rows
    request = graph_request(action='list', entity_id=None, source_id=SOURCE,
        kinds=('person',), query=unique_original, min_data_seq=5, limit=1)
    first = await execute_source_graph_read(request, acquire=native.acquire)
    assert first.status == 'ok', first.reason
    assert first.records == () and first.evidence == ()
    assert first.next_cursor is not None  # empty is not a terminal no-match
    calls = [params for sql, params in native.calls if 'read_source_graph_v1' in sql]
    assert len(calls) == 8
    assert all(params[1:3] == (SOURCE, 'list') and params[-1] == 2
        for params in calls)
    second = await execute_source_graph_read(
        request.model_copy(update={'cursor': first.next_cursor}), acquire=native.acquire)
    assert second.status == 'ok', second.reason
    assert len(second.records) == 1
    assert second.records[0]['data']['mention']['exact_source_spelling'] == unique_original
    assert second.records[0]['data']['canonical_label'] != unique_original
    assert not second.next_cursor
    assert len([1 for sql, _ in native.calls if 'read_source_graph_v1' in sql]) == 9
