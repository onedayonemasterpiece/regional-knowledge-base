"""Source-only Socket Mode → exact existing ingress/read-only reply regression.

Most tests use an in-process fake. The close regression uses the official SDK
against a loopback WebSocket. No external Slack connection, installed credential,
owner grants, provider post or Kaggle I/O.
"""
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest
from slack_sdk.socket_mode.request import SocketModeRequest

from regional_knowledge.expert_request_control import RequestControlError
from regional_knowledge.expert_slack_ingress import ExpertSlackIngress, PATH
from regional_knowledge.expert_slack_socket_mode import ExpertSlackSocketMode
from regional_knowledge.participant_contracts import AccountUnlink
from test_expert_request_control import _event, _prepare
from test_participants import people, change
from test_control_spool import env
from test_expert_slack_main_composition import _capture_main
from test_kaggle_service_startup import service
from test_kaggle_public_backend import target
from test_read_authority import auth


class FakeSocketClient:
    def __init__(self, *, control=None):
        self.control=control
        self.socket_mode_request_listeners=[]
        self.acks=[]
        self.calls=[]
    async def connect(self):
        self.calls.append("connect")
    async def close(self):
        self.calls.append("close")
    async def send_socket_mode_response(self, response):
        if self.control is not None:
            with self.control.corpus.connect() as db:
                count=db.execute("SELECT count(*) FROM rkb_expert_requests").fetchone()[0]
                assert count == 1 # ACK only after durable local received row exists.
        self.calls.append("ack")
        self.acks.append(response.envelope_id)


def _event_request(n=1, *, event_type="app_mention", **overrides):
    payload=json.loads(_event(n=n,group=True,text="<@UBOT> What does this source say?")[0])
    payload["event"]["type"]=event_type
    for key,value in overrides.items():
        if key.startswith("event_"):
            payload["event"][key[6:]]=value
        else:
            payload[key]=value
    return SocketModeRequest(type="events_api",envelope_id=f"envelope-{n}",payload=payload)


@pytest.mark.asyncio
async def test_socket_authenticated_original_nonowner_stored_before_ack_same_event_id(env):
    ctl,_,link=_prepare(env)
    ingress=ExpertSlackIngress(ctl,signing_secret=None,socket_only=True)
    wakes=[]
    ingress.bind_search_reply(SimpleNamespace(control=ctl,ingress=ingress,notify=wakes.append))
    fake=FakeSocketClient(control=ctl)
    adapter=ExpertSlackSocketMode(ingress=ingress,app_token="xapp-synthetic",
        bot_token="xoxb-synthetic",client_factory=lambda app,bot:fake)
    async with adapter.lifespan():
        request=_event_request()
        await adapter.handle(fake,request)
        assert fake.acks==["envelope-1"] and len(wakes)==1
        with ctl.corpus.connect() as db:
            rows=db.execute("SELECT * FROM rkb_expert_requests").fetchall()
            assert len(rows)==1 and rows[0]["state"]=="received"
            assert rows[0]["person_id"]==env.p.other.subject
            assert rows[0]["person_id"]!=env.p.owner.subject
            assert rows[0]["event_id"]=="Ev-1"
            assert json.loads(rows[0]["origin"])["thread_ts"]=="1792000000.100"
        # Slack redelivery has a different Socket envelope, the ORIGINAL event.
        await adapter.handle(fake,SocketModeRequest(type="events_api",
            envelope_id="envelope-retry",payload=request.payload))
        assert fake.acks==["envelope-1","envelope-retry"]
        assert wakes==[wakes[0],wakes[0]]
        with ctl.corpus.connect() as db:
            assert db.execute("SELECT count(*) FROM rkb_expert_requests").fetchone()[0]==1
        # The existing PR383 CAS, not the Socket envelope, fences a second post.
    assert fake.calls==["connect","ack","ack","close"]
    assert adapter._client is None


@pytest.mark.asyncio
async def test_socket_wrong_team_and_revoked_nonowner_have_no_ack_or_row(env):
    ctl,_,link=_prepare(env)
    ingress=ExpertSlackIngress(ctl,signing_secret=None,socket_only=True)
    ingress.bind_search_reply(SimpleNamespace(control=ctl,ingress=ingress,notify=lambda _:None))
    fake=FakeSocketClient()
    adapter=ExpertSlackSocketMode(ingress=ingress,app_token="xapp-synthetic",
        bot_token="xoxb-synthetic",client_factory=lambda *_:fake)
    async with adapter.lifespan():
        with pytest.raises(RequestControlError,match="slack_event_scope_mismatch"):
            await adapter.handle(fake,_event_request(team_id="OTHER"))
        assert fake.acks==[]
        change(env.p,AccountUnlink(link_id=link.link_id,expected_link_revision=2,
            reason="Synthetic external binding revoked",incorrect_binding=True))
        with pytest.raises(Exception):
            await adapter.handle(fake,_event_request(n=2))
        assert fake.acks==[]
    with ctl.corpus.connect() as db:
        assert db.execute("SELECT count(*) FROM rkb_expert_requests").fetchone()[0]==0


@pytest.mark.asyncio
async def test_socket_does_not_admit_bot_loop_unsubscribed_event_or_foreign_client(env):
    ctl,_,link=_prepare(env)
    ingress=ExpertSlackIngress(ctl,signing_secret=None,socket_only=True)
    seen=[]
    ingress.bind_search_reply(SimpleNamespace(control=ctl,ingress=ingress,notify=seen.append))
    fake=FakeSocketClient()
    adapter=ExpertSlackSocketMode(ingress=ingress,app_token="xapp-synthetic",
        bot_token="xoxb-synthetic",client_factory=lambda *_:fake)
    async with adapter.lifespan():
        with pytest.raises(PermissionError):
            await adapter.handle(object(),_event_request())
        await adapter.handle(fake,_event_request(n=2,event_user="UBOT"))
        await adapter.handle(fake,_event_request(n=3,event_type="message"))
        await adapter.handle(fake,SocketModeRequest(type="interactive",
            envelope_id="envelope-modal",payload={"type":"view_submission"}))
        assert fake.acks==["envelope-2","envelope-3","envelope-modal"]
        assert seen==[]
    with ctl.corpus.connect() as db:
        assert db.execute("SELECT count(*) FROM rkb_expert_requests").fetchone()[0]==0


@pytest.mark.asyncio
async def test_socket_existing_target_lifespan_only_once(env):
    ctl,_,_=_prepare(env)
    ingress=ExpertSlackIngress(ctl,signing_secret=None,socket_only=True)
    ingress.bind_search_reply(SimpleNamespace(control=ctl,ingress=ingress,notify=lambda _:None))
    fake=FakeSocketClient()
    adapter=ExpertSlackSocketMode(ingress=ingress,app_token="xapp-synthetic",
        bot_token="xoxb-synthetic",client_factory=lambda *_:fake)
    order=[]
    @asynccontextmanager
    async def existing(server):
        order.append("target_started")
        yield server
        order.append("target_stopped")
    async with adapter.alongside(existing,"server") as server:
        assert server=="server"
        assert order==["target_started"] and fake.calls==["connect"]
        with pytest.raises(RuntimeError,match="one Socket Mode client"):
            async with adapter.lifespan():pass
    assert order==["target_started","target_stopped"]
    assert fake.calls==["connect","close"]


def test_socket_server_main_is_inert_until_explicit_private_codec(service,tmp_path,monkeypatch):
    import regional_knowledge.server as server
    config=tmp_path/"socket-identity.json"
    config.write_text(json.dumps({"codec":"rkb.expert-slack-socket.v1",
        "app_id":"A111","team_id":"T111","bot_user_id":"UBOT",
        "allowed_channels":["C111"]}))
    config.chmod(0o600)
    capture=_capture_main(monkeypatch)
    monkeypatch.setenv("RKB_EXPERT_SLACK_INGRESS_CONFIG",str(config))
    monkeypatch.delenv("RKB_EXPERT_SLACK_REPLY_CONFIG",raising=False)
    monkeypatch.setenv("SLACK_APP_TOKEN","xapp-synthetic")
    monkeypatch.setenv("SLACK_BOT_TOKEN","xoxb-synthetic")
    server.main(target_startup=service.startup)
    assert len(capture)==1
    mcp,options=capture[0]
    assert mcp._target_startup is service.startup
    assert PATH not in {r.path for r in options["app"].routes}  # HTTP unchanged, socket-only
    assert options["host"]=="127.0.0.1"
    # Neither the SDK WebSocket nor the bot chat API is called until the real
    # API lifespan is started separately by Shkiper.


@pytest.mark.asyncio
async def test_official_sdk_slow_websocket_close_does_not_hold_api_lifespan(env):
    """Actual SDK + loopback WebSocket; peer never processes the close frame."""
    import asyncio
    from slack_sdk.socket_mode.websockets import SocketModeClient
    from slack_sdk.web.async_client import AsyncWebClient
    from websockets.asyncio.server import serve

    ctl,_,_=_prepare(env)
    ingress=ExpertSlackIngress(ctl,signing_secret=None,socket_only=True)
    ingress.bind_search_reply(SimpleNamespace(control=ctl,ingress=ingress,notify=lambda _:None))
    ready=asyncio.Event();release=asyncio.Event();peers=[]
    async def unresponsive_peer(ws):
        peers.append(ws)
        ws.transport.pause_reading()
        ready.set()
        try:await release.wait()
        finally:ws.transport.resume_reading()

    async with serve(unresponsive_peer,'127.0.0.1',0,ping_interval=None,close_timeout=.1)as server:
        port=server.sockets[0].getsockname()[1]
        client=SocketModeClient(app_token='xapp-synthetic',
            web_client=AsyncWebClient(token='xoxb-synthetic'))
        client.wss_uri=f'ws://127.0.0.1:{port}/'
        adapter=ExpertSlackSocketMode(ingress=ingress,app_token='xapp-synthetic',
            bot_token='xoxb-synthetic',client_factory=lambda *_:client)
        lifetime=adapter.lifespan()
        try:
            await asyncio.wait_for(lifetime.__aenter__(),2)
            await asyncio.wait_for(ready.wait(),2)
            assert client.current_session.close_timeout==10
            start=asyncio.get_running_loop().time()
            await asyncio.wait_for(lifetime.__aexit__(None,None,None),2.5)
            assert asyncio.get_running_loop().time()-start<2
            assert client.closed and not client.auto_reconnect_enabled
            assert adapter._client is None
        finally:
            release.set()
            for peer in peers:peer.transport.resume_reading()
            await asyncio.wait_for(client.close(),2)
            tasks=[getattr(client,name,None)for name in
                   ('message_processor','current_session_monitor','message_receiver')]
            await asyncio.gather(*(t for t in tasks if t is not None),return_exceptions=True)
