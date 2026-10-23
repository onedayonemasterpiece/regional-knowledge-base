"""Request-local current-evidence check at the existing MCP JSON boundary.

The native MCP transport retains the actual Starlette request in its context.
Its ASGI scope carries this one response's provenance; no token, global output
registry, durable read journal or second authorization authority is created.
"""
from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
import asyncio
import json

_SCOPE_KEY = 'rkb.current_read_output'
_JSON_KEY = 'rkb.trusted_json_read_output'
_capture = ContextVar('rkb_read_output_capture', default=None)
MAX_RESPONSE_BYTES = 512 * 1024


@dataclass
class GuardedOutput:
    gate: object
    gated: object
    render: object
    on_refresh: object = None
    on_release: object = None
    on_terminal: object = None
    released: bool = False
    terminal: bool = False

    def complete(self, status, reason):
        if self.terminal:
            return
        self.terminal = True
        if self.on_terminal is not None:
            try:
                self.on_terminal(status, reason)
            except Exception:
                pass  # Optional lossy telemetry cannot change issued evidence.

    def release(self):
        if self.released:
            return
        self.released = True
        try:
            self.complete('cancelled', 'consumer_cancelled')
            self.gate.release(self.gated.ticket)
        finally:
            if self.on_release is not None:
                self.on_release()


@contextmanager
def capture_read_output():
    calls = []
    token = _capture.set(calls)
    try:
        yield calls
    except BaseException:
        for call in calls:
            call.release()
        raise
    finally:
        _capture.reset(token)


def retain_read_output(gate, gated, render, *, on_release=None, on_terminal=None,
                       on_refresh=None):
    """Trusted backend seam; neither locators nor caller fields grant access.

    Media needs its EXISTING gate.issue + owner-bound bytes re-acquired after
    awaits; it must not pretend its IssuedPublicIllustration supports the
    generic text gate.refresh(result) API. The optional scoped on_refresh
    belongs to this one trusted backend call, never to request JSON.
    """
    if on_refresh is not None and not callable(on_refresh):
        raise TypeError("trusted output refresh callback required")
    calls = _capture.get()
    if calls is None:
        # A direct internal caller must check/consume its own GatedResult.
        return False
    if calls:
        raise RuntimeError('one guarded read result per MCP tool call required')
    calls.append(GuardedOutput(gate, gated, render,
                              on_refresh, on_release, on_terminal))
    return True


def bind_request_output(context, calls):
    if not calls:
        return False
    request = context.request_context.request
    if request is None or not isinstance(getattr(request, 'scope', None), dict):
        raise RuntimeError('guarded public read requires the actual HTTP request')
    scope = request.scope
    if _SCOPE_KEY in scope or len(calls) != 1:
        raise RuntimeError('ambiguous guarded response scope')
    scope[_SCOPE_KEY] = (context.request_context.request_id, calls[0])
    return True


def bind_json_output(request, calls, request_id, render, denied):
    """Trusted internal receiver only; caller JSON cannot select this renderer."""
    scope = request.scope
    if not isinstance(scope, dict) or len(calls) != 1 or _SCOPE_KEY in scope or _JSON_KEY in scope:
        raise RuntimeError('one actual HTTP read result required')
    previous = calls[0].render
    calls[0].render = lambda value: render(previous(value))
    scope[_SCOPE_KEY] = (request_id, calls[0])
    scope[_JSON_KEY] = denied


def _denied():
    return {'content': [{'type': 'text', 'text': '{"status":"unavailable","reason":"context_gap"}'}],
            'isError': True}


def _tool_result(value, previous):
    from mcp.types import CallToolResult
    if type(value) is CallToolResult:
        # Page previews use the ordinary MCP text/image content contract. The
        # retained gate owns this renderer and refreshes the whole result.
        return value.model_dump(mode='json', by_alias=True, exclude_none=True)
    payload = value.model_dump(mode='json') if hasattr(value, 'model_dump') else dict(value)
    # The outer existing ReadTelemetry attaches the public trace after the
    # backend creates its render closure. Keep only that allowlisted diagnostic.
    trace = previous.get('structuredContent', {}).get('read_trace')
    if trace is not None:
        payload['read_trace'] = trace
    return {'content': [{'type': 'text', 'text': json.dumps(payload, ensure_ascii=False,
                allow_nan=False, separators=(',', ':'))}],
            'structuredContent': payload,
            'isError': payload.get('status') in {'unavailable', 'cancelled', 'invalid_query', 'denied'}}


class ReadIssuanceMiddleware:
    """Final check for the target's existing stateless JSON MCP transport.

    Buffer only a bounded tool response until it can be re-rendered. Every
    refresh uses the existing gate, including current user authority; an error,
    missing provenance or ongoing invalidation withholds the complete result.
    """
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope['type'] != 'http':
            return await self.app(scope, receive, send)
        buffered = bytearray()
        is_json = False
        issued = None

        async def guarded_send(message):
            nonlocal is_json, issued
            if message['type'] == 'http.response.start':
                headers = message.get('headers', ())
                is_json = any(k.lower() == b'content-type' and v.lower().startswith(b'application/json')
                              for k, v in headers)
                if is_json:
                    # Rerendering may change bytes. The ASGI server determines
                    # framing; never retain an obsolete Content-Length.
                    message = {**message, 'headers': [(k, v) for k, v in headers
                        if k.lower() != b'content-length']}
                await send(message)
                return
            if message['type'] != 'http.response.body' or not is_json:
                if _SCOPE_KEY in scope:
                    raise RuntimeError('guarded MCP transport must use JSON responses')
                await send(message)
                return
            if _SCOPE_KEY not in scope and not buffered:
                await send(message)
                return
            buffered.extend(message.get('body', b''))
            if len(buffered) > MAX_RESPONSE_BYTES:
                raise RuntimeError('bounded MCP read response required')
            if message.get('more_body', False):
                return
            bound = scope.pop(_SCOPE_KEY, None)
            if bound is None:
                await send({**message, 'body': bytes(buffered)})
                return
            request_id, issued = bound
            typed_denied = scope.pop(_JSON_KEY, None)
            envelope = {'jsonrpc': '2.0', 'id': request_id}
            status, reason = 'denied', 'output_gate_denied'
            try:
                received = json.loads(buffered)
                if typed_denied is not None:
                    if not isinstance(received, dict) or received.get('request_id') != request_id:
                        raise RuntimeError('guarded typed response identity differs')
                elif (not isinstance(received, dict) or type(received.get('id')) is not type(request_id)
                        or received.get('id') != request_id
                        or not isinstance(received.get('result'), dict)):
                    raise RuntimeError('guarded MCP response identity differs')
                envelope = received
                previous = envelope.get('result', {})
                for _ in range(2):
                    if issued.on_refresh is None:
                        issued.gated = await issued.gate.refresh(issued.gated)
                        rendered = issued.render(issued.gated.result)
                    else:
                        # Entire media frame + its independently issued real
                        # image bytes are reobtained under the same root gate.
                        # No pre-refreshed pixels survive a source/lease change.
                        issued.gated = await issued.on_refresh(issued.gated)
                        rendered = issued.render(issued.gated)
                    if typed_denied is not None:
                        envelope = rendered
                    else:
                        rendered = _tool_result(rendered, previous)
                        envelope['result'] = rendered
                    data = json.dumps(envelope, ensure_ascii=False, allow_nan=False,
                                      separators=(',', ':')).encode('utf-8')
                    if len(data) > MAX_RESPONSE_BYTES:
                        raise ValueError('response bound')
                    if issued.gate.issuance_current(issued.gated.ticket):
                        payload = rendered if typed_denied is not None else rendered.get('structuredContent', {})
                        status, reason = payload.get('status', 'ok'), payload.get('reason')
                        break
                else:
                    raise PermissionError('evidence changed during issuance')
            except Exception:
                if typed_denied is not None:
                    envelope = typed_denied()
                else:
                    envelope['result'] = _denied()
                data = json.dumps(envelope, separators=(',', ':')).encode()
            # No await between the final synchronous mutation fence and this
            # issuance call. A failed network send is never retried as a second
            # body: the peer may already have received the first one.
            try:
                await send({**message, 'body': data})
            except asyncio.CancelledError:
                issued.complete('cancelled', 'consumer_cancelled')
                raise
            except Exception:
                issued.complete('error', 'internal_error')
                raise
            else:
                issued.complete(status, reason)

        try:
            await self.app(scope, receive, guarded_send)
        finally:
            bound = scope.pop(_SCOPE_KEY, None)
            scope.pop(_JSON_KEY, None)
            if issued is not None:
                issued.release()
            elif bound is not None:
                bound[1].release()
