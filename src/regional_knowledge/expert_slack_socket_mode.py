"""Socket Mode transport for the existing RKB Slack ingress and search reply.

Slack's official async SocketModeClient authenticates the app-level WebSocket.
No HTTP event-body signature is invented for this trusted socket transport.
The request is admitted by the SAME TargetExpertRequestControl as HTTP.
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import logging

from .expert_request_control import _verified_slack_observation
from .expert_slack_ingress import ExpertSlackIngress

_LOG = logging.getLogger(__name__)


class ExpertSlackSocketMode:
    def __init__(self, *, ingress, app_token, bot_token, client_factory=None):
        if (type(ingress) is not ExpertSlackIngress or not ingress.socket_only
                or ingress._reply is None
                or type(app_token) is not str or not app_token.startswith("xapp-")
                or type(bot_token) is not str or not bot_token.startswith("xoxb-")
                or any(len(token) > 512 for token in (app_token, bot_token))):
            raise ValueError("exact existing socket ingress and app/bot tokens required")
        self.ingress, self.app_token, self.bot_token = ingress, app_token, bot_token
        self._client_factory = client_factory
        self._client = None

    async def handle(self, client, request):
        """ACK after the original event_id row is committed, before search."""
        from slack_sdk.socket_mode.request import SocketModeRequest
        from slack_sdk.socket_mode.response import SocketModeResponse

        if client is not self._client or not isinstance(request, SocketModeRequest):
            raise PermissionError("authenticated Socket Mode client required")
        if type(request.envelope_id) is not str or not request.envelope_id:
            raise ValueError("Socket Mode envelope ID required")
        payload = request.payload
        if (request.type != "events_api" or type(payload) is not dict
                or payload.get("type") != "event_callback"
                or type(payload.get("event")) is not dict
                or payload["event"].get("type") != "app_mention"):
            await client.send_socket_mode_response(SocketModeResponse(envelope_id=request.envelope_id))
            return
        # Only the official authenticated SocketModeClient calls this listener.
        # Reuse the existing parser's bot/subtype/app/team guards and original actor.
        observation = _verified_slack_observation(payload,
            app_id=self.ingress.control.app_id, team_id=self.ingress.control.team_id,
            bot_user_id=self.ingress.control.bot_user_id)
        if observation is None:
            await client.send_socket_mode_response(SocketModeResponse(envelope_id=request.envelope_id))
            return
        outcome = await asyncio.to_thread(self.ingress.control.accept, observation)
        # A failed SQLite/identity/capability check must not be acknowledged.
        await client.send_socket_mode_response(SocketModeResponse(envelope_id=request.envelope_id))
        try:
            self.ingress._reply.notify(outcome["request_id"])
        except Exception as exc:
            _LOG.warning("rkb_socket_read_wakeup_unavailable error_type=%s",
                         type(exc).__name__)

    def _make_client(self):
        if self._client_factory is not None:
            return self._client_factory(self.app_token, self.bot_token)
        from slack_sdk.socket_mode.websockets import SocketModeClient
        from slack_sdk.web.async_client import AsyncWebClient
        return SocketModeClient(app_token=self.app_token,
            web_client=AsyncWebClient(token=self.bot_token), trace_enabled=False)

    @asynccontextmanager
    async def lifespan(self):
        """Co-own the existing API process; the SDK handles reconnects."""
        if self._client is not None:
            raise RuntimeError("one Socket Mode client per existing RKB owner")
        client = self._make_client()
        self._client = client
        client.socket_mode_request_listeners.append(self.handle)
        try:
            await client.connect()
            # Reuse the existing Socket Mode lifecycle to resume original
            # request rows; no second background owner or scheduler.
            dot=getattr(self.ingress._reply,'_dot_events',None)
            if dot is not None:
                await dot.resume_pending()
            yield self
        finally:
            try:
                await client.close()
            finally:
                self._client = None

    @asynccontextmanager
    async def alongside(self, target_lifespan, server):
        async with target_lifespan(server) as target:
            async with self.lifespan():
                yield target
