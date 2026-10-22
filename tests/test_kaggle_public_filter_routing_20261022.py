"""Public MCP filtering routed to fixed native reads; synthetic external I/O only."""
import asyncio
import json
from types import SimpleNamespace
from uuid import UUID

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
from test_kaggle_source_graph_read import row
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

    async def read(cursor=None):
        req, ctx = context()
        args = {'document_ids': [SOURCE], 'kinds': ['person', 'poi_ref'],
            'query': 'SOURCE identity', 'limit': 1}
        if cursor is not None:
            args['cursor'] = cursor
        pending = asyncio.create_task(server.call_tool('entity_list', args, ctx))
        dispatched = await work(t)
        assert type(dispatched.query) is SourceGraphReadRequest
        assert dispatched.query.source_id == SOURCE
        assert dispatched.query.kinds == ('person', 'poi_ref')
        assert dispatched.query.query == 'SOURCE identity'
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
            return ('rkb-v4-sql001-014-v1',)

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
