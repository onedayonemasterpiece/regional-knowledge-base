"""Final HTTP issuance checks with disposable in-memory gates and real MCP SDK."""
import asyncio
import json
from types import SimpleNamespace

import httpx
import pytest
from starlette.requests import Request

from regional_knowledge.read_issuance import (
    MAX_RESPONSE_BYTES, ReadIssuanceMiddleware, bind_request_output,
    capture_read_output, retain_read_output,
)


class Gate:
    def __init__(self, *, current=(True,), failure=None):
        self.current = iter(current)
        self.failure = failure
        self.refreshes = 0
        self.released = []

    async def refresh(self, gated):
        self.refreshes += 1
        if self.failure:
            raise self.failure
        # Model withdrawal by replacing an earlier allowed result at refresh.
        return SimpleNamespace(ticket=gated.ticket, result={'evidence': []})

    def issuance_current(self, ticket):
        return next(self.current)

    def release(self, ticket):
        self.released.append(ticket)


def bind(scope, gate, *, request_id='actual-http-request', render=lambda result: result):
    gated = SimpleNamespace(ticket=object(), result={'evidence': ['withdrawn-private-text']})
    context = SimpleNamespace(request_context=SimpleNamespace(
        request=Request(scope), request_id=request_id))
    with capture_read_output() as calls:
        assert retain_read_output(gate, gated, render)
        assert bind_request_output(context, calls)
    return gated


def response(request_id='actual-http-request', *, text='withdrawn-private-text'):
    return json.dumps({'jsonrpc': '2.0', 'id': request_id,
        'result': {'content': [{'type': 'text', 'text': text}],
                   'structuredContent': {'evidence': [text], 'read_trace': {'trace_id': 'a' * 32}},
                   'isError': False}}).encode()


async def run(gate, *, body=None, request_id='actual-http-request', wire_id=None,
              render=lambda result: result, sender=None, abort=None, guarded=True):
    scope = {'type': 'http', 'method': 'POST', 'path': '/mcp', 'headers': []}
    sent = []
    async def app(scope, receive, send):
        if guarded:
            bind(scope, gate, request_id=request_id, render=render)
        await send({'type': 'http.response.start', 'status': 200, 'headers': [
            (b'content-type', b'application/json'), (b'content-length', b'999')]})
        if abort:
            raise abort
        raw = body if body is not None else response(request_id if wire_id is None else wire_id)
        # Force assembly across real ASGI body fragments.
        await send({'type': 'http.response.body', 'body': raw[:17], 'more_body': True})
        await send({'type': 'http.response.body', 'body': raw[17:], 'more_body': False})
    async def send(message):
        sent.append(message)
        if sender:
            await sender(message)
    async def receive():
        return {'type': 'http.disconnect'}
    await ReadIssuanceMiddleware(app)(scope, receive, send)
    return sent


def issued(messages):
    return json.loads(b''.join(message.get('body', b'') for message in messages))


@pytest.mark.asyncio
async def test_withdrawal_refresh_removes_private_text_from_both_mcp_representations():
    gate = Gate()
    messages = await run(gate)
    result = issued(messages)
    assert result['id'] == 'actual-http-request'
    assert result['result']['structuredContent'] == {
        'evidence': [], 'read_trace': {'trace_id': 'a' * 32}}
    assert json.loads(result['result']['content'][0]['text'])['evidence'] == []
    assert 'withdrawn-private-text' not in repr(messages)
    assert gate.refreshes == 1 and len(gate.released) == 1
    assert (b'content-length', b'999') not in messages[0]['headers']


@pytest.mark.asyncio
@pytest.mark.parametrize('fences,denied', [((False, True), False), ((False, False), True)])
async def test_final_mutation_fence_refreshes_once_more_or_withholds_result(fences, denied):
    gate = Gate(current=fences)
    messages = await run(gate)
    assert gate.refreshes == 2 and len(gate.released) == 1
    assert issued(messages)['result']['isError'] is denied
    assert 'withdrawn-private-text' not in repr(messages)


@pytest.mark.asyncio
async def test_authority_exception_withholds_complete_result_and_releases_ticket():
    gate = Gate(failure=PermissionError('private authority detail'))
    messages = await run(gate)
    result = issued(messages)['result']
    assert result['isError'] is True and 'structuredContent' not in result
    assert json.loads(result['content'][0]['text'])['reason'] == 'context_gap'
    assert 'private authority detail' not in repr(messages)
    assert 'withdrawn-private-text' not in repr(messages)
    assert len(gate.released) == 1


@pytest.mark.asyncio
async def test_guarded_native_page_images_keep_normal_mcp_content_and_revoke_together():
    from mcp.types import CallToolResult, ImageContent, TextContent
    def page(_):
        return CallToolResult(content=[TextContent(type='text', text='actual-native-metadata'),
            ImageContent(type='image', data='cHJpdmF0ZS1wYWdl', mimeType='image/jpeg')])
    current = Gate()
    result = issued(await run(current, render=page))['result']
    assert result['content'][1] == {'type':'image', 'data':'cHJpdmF0ZS1wYWdl', 'mimeType':'image/jpeg'}
    revoked = Gate(current=(False, False))
    messages = await run(revoked, render=page)
    assert issued(messages)['result']['isError'] is True
    assert 'cHJpdmF0ZS1wYWdl' not in repr(messages) and 'actual-native-metadata' not in repr(messages)
    assert len(revoked.released) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('wire_id', ['different-request', 123, None])
async def test_wire_identity_must_match_actual_context_request_id(wire_id):
    gate = Gate()
    messages = await run(gate, body=response(wire_id))
    envelope = issued(messages)
    assert envelope['id'] == 'actual-http-request'
    assert envelope['result']['isError'] is True
    assert gate.refreshes == 0 and len(gate.released) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('wire_id', [True, 1.0])
async def test_json_rpc_id_type_cannot_alias_integer_request_id(wire_id):
    gate = Gate()
    messages = await run(gate, request_id=1, body=response(wire_id))
    envelope = issued(messages)
    assert type(envelope['id']) is int and envelope['id'] == 1
    assert envelope['result']['isError'] is True
    assert gate.refreshes == 0 and len(gate.released) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('abort', [asyncio.CancelledError(), ConnectionError('disconnected')])
async def test_cancellation_or_disconnect_before_body_releases_ticket(abort):
    gate = Gate()
    with pytest.raises(type(abort)):
        await run(gate, abort=abort)
    assert len(gate.released) == 1 and gate.refreshes == 0


@pytest.mark.asyncio
async def test_failed_network_send_never_attempts_a_second_body():
    gate = Gate()
    bodies = []
    async def broken(message):
        if message['type'] == 'http.response.body':
            bodies.append(message['body'])
            raise ConnectionError('peer may have received bytes')
    with pytest.raises(ConnectionError):
        await run(gate, sender=broken)
    assert len(bodies) == 1 and len(gate.released) == 1


@pytest.mark.asyncio
async def test_guarded_oversized_response_withheld_and_released():
    gate = Gate()
    messages = []
    async def record(message):
        messages.append(message)
    with pytest.raises(RuntimeError, match='bounded MCP read response'):
        await run(gate, body=response(text='x' * MAX_RESPONSE_BYTES), sender=record)
    assert not any(message['type'] == 'http.response.body' for message in messages)
    assert len(gate.released) == 1


@pytest.mark.asyncio
async def test_refreshed_oversized_result_becomes_small_safe_denial():
    gate = Gate()
    messages = await run(gate, render=lambda result: {'text': 'x' * MAX_RESPONSE_BYTES})
    assert issued(messages)['result']['isError'] is True
    assert len(json.dumps(issued(messages))) < 1024
    assert len(gate.released) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('status', ['unavailable', 'cancelled', 'invalid_query', 'denied'])
async def test_refreshed_typed_failure_retains_truthful_mcp_error_status(status):
    messages = await run(Gate(), render=lambda result: {'status': status, 'reason': 'context_gap'})
    envelope = issued(messages)
    assert envelope['result']['isError'] is True
    assert envelope['result']['structuredContent']['status'] == status


@pytest.mark.asyncio
async def test_unguarded_large_json_response_is_preserved():
    raw = response(text='x' * (MAX_RESPONSE_BYTES + 1))
    messages = await run(None, body=raw, guarded=False)
    assert b''.join(message.get('body', b'') for message in messages) == raw


@pytest.mark.asyncio
async def test_actual_installed_mcp_stateless_json_context_and_wire_identity():
    # In installed MCP 2.x, FastMCP is named MCPServer, as in RKB server.py.
    from mcp.server.mcpserver import Context, MCPServer
    from mcp.server.transport_security import TransportSecuritySettings
    server = MCPServer('issuance-test')
    gate = Gate()
    contexts = []

    @server.tool()
    async def guarded(context: Context) -> dict:
        contexts.append(context)
        gated = SimpleNamespace(ticket=object(), result={'evidence': ['withdrawn-private-text']})
        with capture_read_output() as calls:
            assert retain_read_output(gate, gated, lambda result: result)
            assert bind_request_output(context, calls)
        return gated.result

    app = server.streamable_http_app(json_response=True, stateless_http=True,
        transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False))
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=ReadIssuanceMiddleware(app)),
                                     base_url='http://localhost') as client:
            response = await client.post('/mcp', json={'jsonrpc': '2.0', 'id': 'json-rpc-client-id',
                'method': 'tools/call', 'params': {'name': 'guarded', 'arguments': {}}},
                headers={'accept': 'application/json, text/event-stream'})
    assert response.status_code == 200
    envelope = response.json()
    assert envelope['id'] == 'json-rpc-client-id'
    assert envelope['result']['isError'] is False
    assert 'withdrawn-private-text' not in response.text
    assert envelope['result']['structuredContent'] == {'evidence': []}
    assert contexts[0].request_context.request_id == 'json-rpc-client-id'
    assert gate.refreshes == 1 and len(gate.released) == 1


@pytest.mark.asyncio
async def test_real_knowledge_server_capture_and_middleware_integration():
    from regional_knowledge.server import KnowledgeMCPServer
    from mcp.server.transport_security import TransportSecuritySettings
    server = KnowledgeMCPServer('knowledge-issuance-test')
    server._current_read_output_required = True
    gate = Gate()

    @server.tool()
    async def guarded() -> dict:
        gated = SimpleNamespace(ticket=object(), result={'evidence': ['withdrawn-private-text']})
        # The real server override owns capture and binds the real HTTP request.
        assert retain_read_output(gate, gated, lambda result: result)
        return gated.result

    app = server.streamable_http_app(json_response=True, stateless_http=True,
        transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False))
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                     base_url='http://localhost') as client:
            response = await client.post('/mcp', json={'jsonrpc': '2.0', 'id': 27,
                'method': 'tools/call', 'params': {'name': 'guarded', 'arguments': {}}},
                headers={'accept': 'application/json, text/event-stream'})
    assert response.status_code == 200
    envelope = response.json()
    assert envelope['id'] == 27 and envelope['result']['isError'] is False
    assert envelope['result']['structuredContent'] == {'evidence': []}
    assert 'withdrawn-private-text' not in response.text
    assert gate.refreshes == 1 and len(gate.released) == 1


@pytest.mark.parametrize('options', [{}, {'json_response': True}, {'stateless_http': True}])
def test_guarded_server_rejects_non_json_or_stateful_transport(options):
    from regional_knowledge.server import KnowledgeMCPServer
    server = KnowledgeMCPServer('guarded-transport-test')
    server._current_read_output_required = True
    with pytest.raises(RuntimeError, match='stateless JSON HTTP'):
        server.streamable_http_app(**options)
