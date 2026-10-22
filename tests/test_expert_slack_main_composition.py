"""Actual server.main expert ingress→evidence composition on one target.

No installed endpoint, external Slack transport, provider send, new credential,
native Dataset proof or second identity authority is created. Only MCP.run is
intercepted to keep the real server/ASGI app in process for tests; no runtime
authorization, writer-fencing or read gates are monkeypatched.
"""
import asyncio
import json
import time
from types import SimpleNamespace

import httpx
import pytest

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
    monkeypatch.setenv('RKB_BACKEND_PROFILE','kaggle_source')
    monkeypatch.setenv('RKB_MCP_PROFILE','live')
    # Intercept only the network server start. All factory, route, control,
    # reader and startup classes remain their real implementations.
    monkeypatch.setattr(server.KnowledgeMCPServer,'run',
        lambda instance,**options:captured.append((instance,options)))
    return captured


def _app(instance):
    return instance.streamable_http_app(stateless_http=True,json_response=True)


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
    assert mcp._target_source_profile
    app=_app(mcp)
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
    routes={r.path for r in _app(captured[0][0]).routes}
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
    routes={r.path for r in _app(captured[0][0]).routes}
    assert {INGRESS,EVIDENCE}<=routes
    # Two competing ingress owners must never consume the same Slack event.
    control,_,_,ingress=_owned_ingress(service)
    with pytest.raises(RuntimeError,match='more than one configured owner'):
        server.main(target_startup=service.startup,expert_slack_ingress=ingress)
    assert len(captured)==1
