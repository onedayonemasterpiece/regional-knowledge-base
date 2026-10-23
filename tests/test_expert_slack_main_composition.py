"""Actual server.main expert ingress→evidence composition on one target.

No installed endpoint, external Slack transport, provider send, new credential,
native Dataset proof or second identity authority is created. Only Uvicorn's network launch is intercepted; the MCP server and ASGI app
are actually composed. No authorization, writer-fencing or read gates are
monkeypatched. A separate synthetic WSS test runs real Uvicorn locally.
"""
import asyncio
import json
import socket
import time
from types import SimpleNamespace

import httpx
import pytest
import uvicorn
import websockets
from starlette.routing import WebSocketRoute

import regional_knowledge.server as server
from regional_knowledge.expert_slack_ingress import ExpertSlackIngress, PATH as INGRESS
from regional_knowledge.expert_evidence_receiver import PATH as EVIDENCE, CODEC
from regional_knowledge.kaggle_protocol import QueryResult
from test_expert_slack_composition import _owned_ingress
from test_expert_request_control import _event, _prepare
from test_kaggle_public_backend import target, release
from test_kaggle_service_startup import service
from test_source_read_gate import item, SOURCE
from test_control_spool import env
from test_read_authority import auth
from test_participants import people


def _capture_main(monkeypatch):
    captured=[]
    composed={}
    monkeypatch.setenv('RKB_BACKEND_PROFILE','kaggle_source')
    monkeypatch.setenv('RKB_MCP_PROFILE','live')
    real_app=server.KnowledgeMCPServer.streamable_http_app

    def capture_app(instance,**options):
        app=real_app(instance,**options)
        composed[id(app)]=(instance,options)
        return app

    def capture_run(app,**options):
        instance,app_options=composed[id(app)]
        captured.append((instance,dict(app_options,**options,app=app)))

    # Compose the real MCP/ASGI stack, intercept ONLY its network launch.
    monkeypatch.setattr(server.KnowledgeMCPServer,'streamable_http_app',capture_app)
    monkeypatch.setattr(server.uvicorn,'run',capture_run)
    return captured


@pytest.mark.asyncio
async def test_main_explicit_signed_ingress_mounts_same_owner_receiver_and_asgi_read(
    service,monkeypatch
):
    s=service
    control,credentials,_link,ingress=_owned_ingress(s)
    captured=_capture_main(monkeypatch)
    server.main(target_startup=s.startup,expert_slack_ingress=ingress)
    assert len(captured)==1
    mcp,options=captured[0]
    assert mcp._target_startup is s.startup
    assert options['stateless_http'] and options['json_response']
    assert options['host']=='127.0.0.1' and options['port']==8000
    assert options['log_level']==mcp.settings.log_level.lower()
    assert options['timeout_graceful_shutdown']==10
    assert options['transport_security']==server._transport_security(
        s.startup.config.oauth_provider.resource)
    assert mcp._target_source_profile
    app=options['app']
    routes={r.path for r in app.routes}
    assert {INGRESS,EVIDENCE}<=routes

    s.adapter.available=True
    async with s.startup.lifespan(None):
        assert s.startup.reads_current()
        raw,signature=_event()
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app,client=('127.0.0.1',9042)),
            base_url='http://knowledge.example.test'
        ) as client:
            # Original Slack 503 is essential while only local received exists.
            first=await client.post(INGRESS,content=raw,headers=signature)
            assert first.status_code==503
            assert first.json()=={'error':'remote_intent_pending'}
            assert control.claim(credentials)=={'state':'empty'}
            with s.config.participants.corpus.connect() as db:
                row=db.execute('SELECT id FROM rkb_expert_requests').fetchone()
                assert row is not None
                request_id=row['id']
            command=control.exact_remote_intent(request_id,1)
            seal=await s.config.spool.journal.seal(
                [command],predecessor=None,writer_generation='writer-1')
            assert (await control.verify_remote_intent(
                request_id,command,seal))['remote_accepted']
            again=await client.post(INGRESS,content=raw,headers=signature)
            assert again.status_code==200
            assert again.json()=={'ok':True}

            claim=control.claim(credentials,request_id=request_id)
            assert claim['state']=='leased'
            req=dict(codec=CODEC,request_id=request_id,
                generation=claim['generation'],
                execution_token=claim['execution_token'],
                min_data_seq=5,deadline_monotonic=time.monotonic()+2)
            headers={'Authorization':'Bearer '+credentials.token,
                'X-RKB-Expert-Service':credentials.service_id}
            task=asyncio.create_task(client.post(EVIDENCE,json=req,headers=headers))
            issued=None
            for _ in range(1000):
                if task.done():
                    early=await task
                    pytest.fail('read rejected before dispatch: '+str(early.status_code)
                                +':'+str(early.json().get('reason','unknown')))
                issued=await s.target.queue.next_channel_work(
                    s.target.run,s.target.token,s.target.generation)
                if issued is not None:
                    assert issued.kind=='read'
                    s.target.admission.mark_work_sent(issued)
                    break
                await asyncio.sleep(.001)
            assert issued is not None
            assert issued.query.actor.person_id==s.target.auth.p.other.subject
            assert issued.query.actor.person_id!=s.target.auth.p.owner.subject
            assert issued.query.request_id==request_id
            source=QueryResult(
                request_id=issued.query.request_id,
                connection_generation=s.target.generation,
                turn_generation=issued.query.turn_generation,
                status='ok',mode='hybrid',
                observed_data_seq=5,observed_projection_seq=5,
                observed_database_generation='synthetic-db',
                observed_visibility_epoch=0,evidence=(item(SOURCE),))
            assert await s.startup.admission.finish_read(
                s.target.run,s.target.token,s.target.generation,source)
            response=await task
            assert response.status_code==200
            result=response.json()
            assert result['codec']==CODEC and result['request_id']==request_id
            assert not result['context_gap']
            assert len(result['evidence'])==1
            assert result['evidence'][0]['text']=='synthetic evidence'
            assert not s.target.auth.resolver._contexts
            await release(s.target,issued)


def test_main_denies_foreign_ingress_and_absent_target_context(service,env,monkeypatch):
    captured=_capture_main(monkeypatch)
    foreign_control,_,_=_prepare(env)
    foreign=ExpertSlackIngress(foreign_control,
        signing_secret='test-signing-secret',clock=lambda:1792000001)
    with pytest.raises(ValueError,match='authorities must be identical'):
        server.main(target_startup=service.startup,expert_slack_ingress=foreign)
    assert not captured
    monkeypatch.setenv('RKB_BACKEND_PROFILE','legacy')
    with pytest.raises(RuntimeError,match='expert read requires existing target startup'):
        server.main(expert_slack_ingress=foreign)
    assert not captured


def test_main_without_ingress_does_not_expose_expert_routes(service,monkeypatch):
    captured=_capture_main(monkeypatch)
    server.main(target_startup=service.startup)
    assert len(captured)==1
    routes={r.path for r in captured[0][1]['app'].routes}
    assert INGRESS not in routes and EVIDENCE not in routes


def test_main_installed_private_config_mounts_both_on_single_target(service,monkeypatch,tmp_path):
    captured=_capture_main(monkeypatch)
    signing=tmp_path/'synthetic-slack-secret'
    signing.write_text('test-signing-secret')
    signing.chmod(0o600)
    private=tmp_path/'synthetic-slack.json'
    private.write_text(json.dumps({
        'codec':'rkb.expert-slack-ingress.v1',
        'app_id':'A111','team_id':'T111','bot_user_id':'UBOT',
        'allowed_channels':['C111'],'signing_secret_file':str(signing)}))
    private.chmod(0o600)
    monkeypatch.setenv('RKB_EXPERT_SLACK_INGRESS_CONFIG',str(private))
    server.main(target_startup=service.startup)
    assert len(captured)==1
    routes={r.path for r in captured[0][1]['app'].routes}
    assert {INGRESS,EVIDENCE}<=routes
    # Two competing ingress owners must never consume the same Slack event.
    control,_,_,ingress=_owned_ingress(service)
    with pytest.raises(RuntimeError,match='more than one configured owner'):
        server.main(target_startup=service.startup,expert_slack_ingress=ingress)
    assert len(captured)==1


@pytest.mark.asyncio
async def test_main_graceful_timeout_reaches_real_lifespan_after_stalled_websocket(
    service,monkeypatch
):
    captured=_capture_main(monkeypatch)
    server.main(target_startup=service.startup)
    assert len(captured)==1
    mcp,options=captured[0]
    assert mcp._target_startup is service.startup
    assert options['timeout_graceful_shutdown']==10

    opened=asyncio.Event()
    cancelled=asyncio.Event()

    async def hung_websocket(websocket):
        await websocket.accept()
        opened.set()
        try:
            await asyncio.Event().wait()  # A genuine unfinished connection.
        finally:
            cancelled.set()

    # The actual ASGI app/lifespan from main, not a stub application. The
    # special route exists only in this synthetic network test.
    app=options['app']
    app.routes.append(WebSocketRoute('/synthetic-hanging-websocket',hung_websocket))
    sock=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    sock.bind((options['host'],0))
    sock.listen(16)
    sock.setblocking(False)
    # Substitute a short budget for CI; the production setting remains 10 s.
    uvicorn_server=uvicorn.Server(uvicorn.Config(
        app,host=options['host'],port=0,log_level='critical',
        timeout_graceful_shutdown=.15))
    task=asyncio.create_task(uvicorn_server.serve(sockets=[sock]))
    websocket=None
    try:
        for _ in range(300):
            if uvicorn_server.started:
                break
            if task.done():
                await task
            await asyncio.sleep(.01)
        assert uvicorn_server.started
        websocket=await websockets.connect(
            f"ws://127.0.0.1:{sock.getsockname()[1]}/synthetic-hanging-websocket",
            open_timeout=2,close_timeout=.2)
        await asyncio.wait_for(opened.wait(),2)
        stop_at=time.monotonic()
        uvicorn_server.should_exit=True
        await asyncio.wait_for(task,3)
        assert time.monotonic()-stop_at<3
        assert cancelled.is_set()
        assert service.startup.closed
        assert service.startup._maintenance is not None
        assert service.startup._maintenance.done()
        assert service.config.spool._fd is None
    finally:
        uvicorn_server.should_exit=True
        if not task.done():
            uvicorn_server.force_exit=True
            task.cancel()
            await asyncio.gather(task,return_exceptions=True)
        if websocket is not None:
            await websocket.close()
        sock.close()
