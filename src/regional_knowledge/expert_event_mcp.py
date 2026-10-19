"""Draft MCP Events handlers on the same authenticated RKB resource."""
import asyncio
import json

from mcp import types
from mcp.shared.exceptions import MCPError
from pydantic import BaseModel, ConfigDict, Field

from .expert_events import EVENT
from .expert_requests import ExpertRequestError
from .expert_webhooks import WebhookError
from .mcp_authority import current_service


class DraftEventDiscovery:
    """Preserve the draft capability across the SDK's stable result sieve.

    Adapt only a bounded, successful modern discovery JSON response. The SDK
    continues to own auth, request parsing, negotiation and every tool response.
    """
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        headers = dict(scope.get("headers", ()))
        if scope["type"] != "http" or headers.get(b"mcp-method") != b"server/discover":
            return await self.app(scope, receive, send)
        start = None; chunks = []; size = 0; passthrough = False
        async def adapt(message):
            nonlocal start, size, passthrough
            if message["type"] == "http.response.start":
                start = message
                content_type = dict(message.get("headers", ())).get(b"content-type", b"")
                if message["status"] != 200 or not content_type.startswith(b"application/json"):
                    passthrough = True
                    await send(message)
            elif message["type"] == "http.response.body" and not passthrough:
                chunks.append(message.get("body", b"")); size += len(chunks[-1])
                if size > 65536:
                    passthrough = True
                    await send(start)
                    await send({**message, "body": b"".join(chunks)})
                    chunks.clear()
                    return
                if message.get("more_body"):
                    return
                body = b"".join(chunks)
                try:
                    payload = json.loads(body); result = payload.get("result", {})
                    if "supportedVersions" in result and isinstance(result.get("capabilities"), dict):
                        result["capabilities"]["events"] = {"delivery": {"webhook": {}}}
                        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
                except (ValueError, AttributeError, TypeError):
                    pass
                start = {**start, "headers": [(k,v) for k,v in start["headers"] if k.lower() not in {b"content-length", b"cache-control"}] +
                    [(b"content-length", str(len(body)).encode()), (b"cache-control", b"no-store")]}
                await send(start)
                await send({**message, "body": body})
            else:
                await send(message)
        await self.app(scope, receive, adapt)


class Delivery(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    mode: str
    url: str = Field(max_length=2048)
    secret: str | None = Field(default=None, max_length=128, repr=False)


class EventParams(types.RequestParams):
    model_config = ConfigDict(extra="forbid", strict=True, populate_by_name=True)
    name: str
    arguments: dict
    delivery: Delivery
    cursor: str | None = None
    ttl_ms: int | None = Field(default=86400000, alias="ttlMs", gt=0)
    max_age_ms: int | None = Field(default=None, alias="maxAgeMs", ge=0)


def register_event_methods(mcp, events):
    mcp.events_discovery_enabled = True
    async def grant():
        try:
            credentials = current_service()
            def check():
                with events.corpus.connect() as db:
                    events.requests._service(db, credentials, "executor")
            await asyncio.to_thread(check)
            return credentials
        except ExpertRequestError:
            raise MCPError(-32024, "Executor access unavailable") from None

    def parameters(params):
        if params.name != EVENT or params.arguments or params.cursor is not None or params.delivery.mode != "webhook":
            raise MCPError(-32602, "Unsupported event or delivery")

    async def discover(ctx, params):
        await grant()
        return types.DiscoverResult(supported_versions=["2026-07-28"],
            capabilities=types.ServerCapabilities(tools=types.ToolsCapability()),
            instructions="Claim only the saved request at this same MCP resource; keep its generation and execution token. Never use another account or send Slack directly.")

    async def listing(ctx, params):
        await grant()
        if params.cursor is not None:
            raise MCPError(-32602, "Unsupported cursor")
        return {"events": [{"name": EVENT, "description": "A saved expert request needs execution; no private text in the notification.", "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False}}]}

    async def subscribe(ctx, params):
        credentials = await grant(); parameters(params)
        if not params.delivery.secret:
            raise MCPError(-32602, "Webhook secret required")
        try:
            return await events.subscribe(credentials, params.delivery.url, params.delivery.secret,
                ttl_ms=params.ttl_ms, max_age_ms=params.max_age_ms)
        except WebhookError:
            raise MCPError(-32015, "Webhook verification failed") from None
        except ExpertRequestError as error:
            raise MCPError(-32024, error.code) from None

    async def unsubscribe(ctx, params):
        credentials = await grant(); parameters(params)
        try:
            return await asyncio.to_thread(events.unsubscribe, credentials, params.delivery.url)
        except (WebhookError, ExpertRequestError):
            raise MCPError(-32602, "Invalid or inaccessible subscription") from None

    for method, schema, handler in (
        ("server/discover", types.RequestParams, discover),
        ("events/list", types.PaginatedRequestParams, listing),
        ("events/subscribe", EventParams, subscribe),
        ("events/unsubscribe", EventParams, unsubscribe),
    ):
        mcp._lowlevel_server.add_request_handler(method, schema, handler)
