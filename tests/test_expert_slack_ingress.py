"""Native signed Slack Events API ingestion without false Saved acknowledgment."""
import hashlib
import hmac
import json
from types import SimpleNamespace

import httpx
import pytest
from starlette.applications import Starlette
from starlette.routing import Route

from regional_knowledge.expert_slack_ingress import ExpertSlackIngress, PATH, MAX_BODY
from regional_knowledge.server import build_server
from test_control_spool import env
from test_participants import people, change
from test_expert_request_control import _prepare, _event
from test_kaggle_public_backend import target
from test_read_authority import auth
from test_kaggle_service_startup import service
from regional_knowledge.participant_contracts import AccountUnlink


SECRET='test-signing-secret'
STAMP='1792000000'
NOW=1792000001


def signed(payload, *, stamp=STAMP, secret=SECRET):
    raw=(json.dumps(payload,separators=(',',':')).encode()
         if type(payload) is dict else payload)
    digest=hmac.new(secret.encode(),b'v0:'+stamp.encode()+b':'+raw,hashlib.sha256).hexdigest()
    return raw,{'X-Slack-Request-Timestamp':stamp,'X-Slack-Signature':'v0='+digest}


def app(control):
    ingress=ExpertSlackIngress(control,signing_secret=SECRET,clock=lambda:NOW)
    return Starlette(routes=[Route(PATH,ingress.__call__,methods=['POST'])])


@pytest.mark.asyncio
async def test_signed_request_is_received_but_slack_ack_only_after_exact_remote_seal(env):
    ctl,creds,link=_prepare(env)
    raw,headers=_event()
    service=app(ctl)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=service),base_url='http://test') as client:
        first=await client.post(PATH,content=raw,headers=headers)
        assert first.status_code==503 and first.json()=={'error':'remote_intent_pending'}
        assert first.headers['Retry-After']=='1'
        assert 'request_id' not in first.text and 'U111' not in first.text
        again=await client.post(PATH,content=raw,headers=headers)
        assert again.status_code==503
        with env.p.corpus.connect() as db:
            rows=db.execute('SELECT id,state,remote_command_hash FROM rkb_expert_requests').fetchall()
            assert len(rows)==1 and rows[0]['state']=='received' and rows[0]['remote_command_hash'] is None
            request_id=rows[0]['id']
        assert not env.store.objects # No remote accepted state yet; no 200.
        # Only the existing journal/writer's external proof upgrades the same
        # request; this synthetic direct seal is NOT executable production wiring.
        cmd=ctl.exact_remote_intent(request_id,1)
        seal=await env.journal.seal([cmd],predecessor=None,writer_generation='writer-1')
        assert (await ctl.verify_remote_intent(request_id,cmd,seal))['remote_accepted']
        accepted=await client.post(PATH,content=raw,headers=headers)
        assert accepted.status_code==200 and accepted.json()=={'ok':True}
        with env.p.corpus.connect() as db:
            assert db.execute('SELECT count(*) FROM rkb_expert_requests').fetchone()[0]==1
            assert db.execute('SELECT state FROM rkb_expert_requests').fetchone()[0]=='pending'


@pytest.mark.asyncio
async def test_signed_url_verification_returns_exact_challenge_without_actor_or_write(env):
    ctl,_,_=_prepare(env)
    source={'type':'url_verification','challenge':'signed-Challenge_0135','api_app_id':'A111'}
    raw,headers=signed(source)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app(ctl)),base_url='http://test') as client:
        response=await client.post(PATH,content=raw,headers=headers)
        assert response.status_code==200 and response.text==source['challenge']
        assert 'no-store' in response.headers.get('Cache-Control','')
        with env.p.corpus.connect() as db:
            assert db.execute('SELECT count(*) FROM rkb_expert_requests').fetchone()[0]==0
        invalid_raw,invalid_headers=signed({**source,'api_app_id':'OTHER'})
        bad=await client.post(PATH,content=invalid_raw,headers=invalid_headers)
        assert bad.status_code==403
        invalid_raw,invalid_headers=signed({**source,'challenge':'<script>'})
        assert (await client.post(PATH,content=invalid_raw,headers=invalid_headers)).status_code==403


@pytest.mark.asyncio
@pytest.mark.parametrize('scenario',['tampered','bad_signature','stale','wrong_team','wrong_app',
                                      'unverified_user','revoked_account','nonallowed_group','too_large'])
async def test_unverified_or_tampered_slack_event_cannot_create_request(env,scenario):
    ctl,creds,link=_prepare(env)
    raw,headers=_event()
    payload=json.loads(raw)
    if scenario=='tampered':
        raw=raw.replace(b'Which event',b'Which other')
    elif scenario=='bad_signature':
        headers['X-Slack-Signature']='v0='+'f'*64
    elif scenario=='stale':
        headers['X-Slack-Request-Timestamp']='1791000000'
    elif scenario=='wrong_team':
        payload['team_id']='OTHER';raw,headers=signed(payload)
    elif scenario=='wrong_app':
        payload['api_app_id']='OTHER';raw,headers=signed(payload)
    elif scenario=='unverified_user':
        payload['event']['user']='UUNKNOWN';raw,headers=signed(payload)
    elif scenario=='revoked_account':
        change(env.p,AccountUnlink(link_id=link.link_id,expected_link_revision=2,
            reason='Synthetic identity withdrawn',incorrect_binding=True))
    elif scenario=='nonallowed_group':
        payload['event']['type']='app_mention'
        payload['event']['channel_type']='channel'
        payload['event']['channel']='COTHER'
        raw,headers=signed(payload)
    elif scenario=='too_large':
        raw=b'X'*(MAX_BODY+1)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app(ctl)),base_url='http://test') as client:
        value=await client.post(PATH,content=raw,headers=headers)
        assert value.status_code in (400,403,413)
        assert not value.json().get('ok')
    with env.p.corpus.connect() as db:
        assert db.execute('SELECT count(*) FROM rkb_expert_requests').fetchone()[0]==0


@pytest.mark.asyncio
async def test_verified_unsupported_bot_event_is_noop_and_replay_identity_conflict_is_denied(env):
    ctl,_,_=_prepare(env)
    raw,headers=_event()
    body=json.loads(raw)
    body['event']['user']='UBOT'
    raw,headers=signed(body)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app(ctl)),base_url='http://test') as client:
        assert (await client.post(PATH,content=raw,headers=headers)).json()=={'ok':True}
        with env.p.corpus.connect() as db:
            assert db.execute('SELECT count(*) FROM rkb_expert_requests').fetchone()[0]==0
        valid,head=_event()
        assert (await client.post(PATH,content=valid,headers=head)).status_code==503
        tampered=json.loads(valid)
        tampered['event']['text']='Different text same Slack event ID'
        changed,head=signed(tampered)
        conflict=await client.post(PATH,content=changed,headers=head)
        assert conflict.status_code==403 and not conflict.json().get('ok')
        with env.p.corpus.connect() as db:
            assert db.execute('SELECT count(*) FROM rkb_expert_requests').fetchone()[0]==1


@pytest.mark.asyncio
async def test_server_route_inert_until_exact_same_target_ingress_optin(service):
    startup=service.startup
    ctl,_,_=_prepare(SimpleNamespace(p=service.target.auth.p,spool=service.config.spool))
    ingress=ExpertSlackIngress(ctl,signing_secret=SECRET,clock=lambda:NOW)
    absent=build_server(target_startup=startup).streamable_http_app(stateless_http=True,json_response=True)
    configured=build_server(target_startup=startup,expert_slack_ingress=ingress).streamable_http_app(
        stateless_http=True,json_response=True)
    assert not any(route.path==PATH for route in absent.routes)
    assert any(route.path==PATH for route in configured.routes)


@pytest.mark.asyncio
async def test_server_main_optional_slack_signed_ingress_stays_received_without_remote_intent(
        service, tmp_path, monkeypatch):
    """Real main -> build_server route; no installed Slack, PG, provider or seal."""
    import socket
    import time
    import regional_knowledge.server as server

    startup=service.startup
    _,_,verified_link=_prepare(SimpleNamespace(
        p=service.target.auth.p,spool=service.config.spool))
    monkeypatch.setenv('RKB_MCP_PROFILE','live')
    monkeypatch.delenv('RKB_PROJECTS_HUB_READER_CONFIG',raising=False)
    monkeypatch.delenv('RKB_EXPERT_SLACK_INGRESS_CONFIG',raising=False)
    servers=[]
    monkeypatch.setattr(server.KnowledgeMCPServer,'run',
                        lambda self,**kwargs:servers.append(self))

    server.main(target_startup=startup)
    absent=servers.pop().streamable_http_app(stateless_http=True,json_response=True)
    assert not any(route.path==PATH for route in absent.routes)

    secret=tmp_path/'fake-signing-secret'
    secret.write_text(SECRET);secret.chmod(0o600)
    config=tmp_path/'slack-ingress.json'
    config.write_text(json.dumps({
        'codec':'rkb.expert-slack-ingress.v1',
        'app_id':'A111','team_id':'T111','bot_user_id':'UBOT',
        'allowed_channels':['C111'],
        'signing_secret_file':str(secret)},sort_keys=True))
    config.chmod(0o600)
    monkeypatch.setenv('RKB_EXPERT_SLACK_INGRESS_CONFIG',str(config))
    # No remote journal writer is authorized by this composition slice.
    async def forbidden_seal(*args,**kwargs):
        pytest.fail('Slack ingress must not remotely accept a control command')
    monkeypatch.setattr(startup.config.spool.journal,'seal',forbidden_seal)
    server.main(target_startup=startup)
    configured=servers.pop().streamable_http_app(stateless_http=True,json_response=True)
    assert sum(route.path==PATH for route in configured.routes)==1

    def forbid_network(*args,**kwargs):
        pytest.fail('synthetic ASGI Slack ingress must not contact a network')
    monkeypatch.setattr(socket.socket,'connect',forbid_network)
    monkeypatch.setattr(socket,'create_connection',forbid_network)
    body,_=_event()
    raw,headers=signed(body,stamp=str(int(time.time())))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=configured),
                                 base_url='http://test') as client:
        for _ in range(2):
            response=await client.post(PATH,content=raw,headers=headers)
            assert response.status_code==503
            assert response.json()=={'error':'remote_intent_pending'}
            assert response.headers['Retry-After']=='1'
            assert 'request_id' not in response.text

        with service.target.auth.p.corpus.connect() as db:
            rows=db.execute('SELECT event_id,state,remote_command_hash,link_id,dialog_seq '
                            'FROM rkb_expert_requests').fetchall()
            assert len(rows)==1
            assert rows[0]['state']=='received' and rows[0]['remote_command_hash'] is None
            assert rows[0]['link_id']==verified_link.link_id
            assert rows[0]['dialog_seq']==1

        bot=json.loads(raw);bot['event']['user']='UBOT'
        bot_raw,bot_headers=signed(bot,stamp=str(int(time.time())))
        ignored=await client.post(PATH,content=bot_raw,headers=bot_headers)
        assert ignored.status_code==200 and ignored.json()=={'ok':True}

        unsigned=dict(headers,**{'X-Slack-Signature':'v0='+'0'*64})
        denied=await client.post(PATH,content=raw,headers=unsigned)
        assert denied.status_code==403

        unknown=json.loads(raw);unknown['event']['user']='UUNKNOWN'
        unknown_raw,unknown_headers=signed(unknown,stamp=str(int(time.time())))
        denied=await client.post(PATH,content=unknown_raw,headers=unknown_headers)
        assert denied.status_code==403

    with service.target.auth.p.corpus.connect() as db:
        assert db.execute('SELECT count(*) FROM rkb_expert_requests').fetchone()[0]==1
