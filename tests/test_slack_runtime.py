"""Private config, sender identity and existing-server lifecycle boundaries."""
import asyncio
import json
import threading

import pytest
from starlette.testclient import TestClient

from regional_knowledge.expert_requests import ExpertRequestError
from regional_knowledge.slack_expert import SlackBoundaryError, SlackExpertAdapter
from regional_knowledge.slack_runtime import bot_tokens, configured_adapter
from regional_knowledge.server import build_server, _transport_security
from test_expert_requests import queue, claim, observation, save_result
from test_expert_executor import sources
from test_personal_auth import personal, begin, login, approve_and_exchange, mcp


def private(path,text):
    path.write_text(text);path.chmod(0o600)
    return str(path)


def config(path,q):
    return {'app_id':'A111','team_id':'T111','bot_user_id':'UBOT','allowed_channels':['C111'],
        'credentials_file':private(path/'bot.env','SLACK_BOT_TOKEN=xoxb-synthetic\nSLACK_APP_TOKEN=xapp-synthetic\n'),
        'delivery_service_id':q.delivery.service_id,
        'delivery_service_token_file':private(path/'delivery.token',q.delivery.token)}


def test_private_configuration_uses_only_bot_app_and_scoped_delivery_grant(queue,tmp_path):
    q=queue;settings=config(tmp_path,q)
    adapter=configured_adapter(q.requests,settings)
    assert adapter.requests is q.requests and adapter.delivery_credentials==q.delivery
    assert adapter.bot_token.startswith('xoxb-') and adapter.app_token.startswith('xapp-')
    with pytest.raises(ExpertRequestError,match='service_denied'):
        configured_adapter(q.requests,{**settings,'delivery_service_id':q.executor.service_id,
            'delivery_service_token_file':private(tmp_path/'executor.token',q.executor.token)})
    with pytest.raises(ValueError):bot_tokens('relative.env')
    (tmp_path/'bot.env').chmod(0o644)
    with pytest.raises(ValueError):configured_adapter(q.requests,settings)
    (tmp_path/'bot.env').chmod(0o600)
    private(tmp_path/'bot.env','SLACK_BOT_TOKEN=xoxb-synthetic\nSLACK_APP_TOKEN=xapp-synthetic\nSLACK_USER_TOKEN=xoxp-forbidden\n')
    with pytest.raises(ValueError):configured_adapter(q.requests,settings)


@pytest.mark.asyncio
async def test_wrong_native_bot_identity_cannot_claim_or_send_any_result(queue,monkeypatch):
    q=queue;lease,_=claim(q);save_result(q,lease)
    adapter=SlackExpertAdapter(q.requests,q.delivery,bot_token='xoxb-synthetic',app_token='xapp-synthetic')
    methods=[]
    async def api(method,payload,**kwargs):
        methods.append(method)
        assert method=='auth.test'
        return {'team_id':'T111','user_id':'UWRONG'}
    monkeypatch.setattr(adapter,'_api',api)
    with pytest.raises(SlackBoundaryError,match='slack_bot_identity_mismatch'):
        await adapter.send_one(lease['request_id'])
    assert methods==['auth.test']
    with q.corpus.connect() as db:
        row=db.execute('SELECT * FROM rkb_expert_requests').fetchone()
        assert row['state']=='result_ready' and row['delivery_attempt']==0


@pytest.mark.asyncio
async def test_changed_bot_token_requires_new_native_identity_check(queue,monkeypatch):
    q=queue;adapter=SlackExpertAdapter(q.requests,q.delivery,bot_token='xoxb-original',app_token='xapp-synthetic')
    methods=[]
    async def api(method,payload,**kwargs):
        methods.append(method)
        return {'team_id':'T111','user_id':'UBOT' if adapter.bot_token=='xoxb-original' else 'UWRONG'}
    monkeypatch.setattr(adapter,'_api',api)
    await adapter.authenticate()
    adapter.bot_token='xoxb-changed'
    with pytest.raises(SlackBoundaryError,match='slack_bot_identity_mismatch'):await adapter.send_one()
    assert methods==['auth.test','auth.test']


@pytest.mark.asyncio
async def test_late_auth_response_cannot_verify_a_replaced_bot_token(queue,monkeypatch):
    q=queue;lease,_=claim(q);save_result(q,lease)
    adapter=SlackExpertAdapter(q.requests,q.delivery,bot_token='xoxb-original',app_token='xapp-synthetic')
    methods=[]
    async def api(method,payload,**kwargs):
        methods.append(method)
        assert method=='auth.test'
        await asyncio.sleep(0)
        adapter.bot_token='xoxb-replaced'
        return {'team_id':'T111','user_id':'UBOT'}
    monkeypatch.setattr(adapter,'_api',api)
    with pytest.raises(SlackBoundaryError,match='slack_bot_identity_mismatch'):
        await adapter.send_one(lease['request_id'])
    assert methods==['auth.test']
    with q.corpus.connect() as db:
        assert db.execute('SELECT state FROM rkb_expert_requests').fetchone()[0]=='result_ready'


@pytest.mark.asyncio
async def test_adapter_owned_session_and_both_loops_close_on_cancel(queue,monkeypatch):
    from regional_knowledge import slack_expert as module
    q=queue;accepted=q.requests.accept(observation());started=asyncio.Event();closed=[]
    class Response:
        status=200
        async def __aenter__(self):return self
        async def __aexit__(self,*args):pass
        async def json(self):return {'ok':True,'team_id':'T111','user_id':'UBOT'}
    class Session:
        async def __aenter__(self):return self
        async def __aexit__(self,*args):closed.append(True)
        def post(self,url,**kwargs):
            assert url.endswith('/auth.test')
            return Response()
    monkeypatch.setattr(module.aiohttp,'ClientSession',lambda **kwargs:Session())
    adapter=SlackExpertAdapter(q.requests,q.delivery,bot_token='xoxb-synthetic',app_token='xapp-synthetic')
    async def socket():
        started.set();await asyncio.Event().wait()
    monkeypatch.setattr(adapter,'socket_once',socket)
    task=asyncio.create_task(adapter.run())
    await asyncio.wait_for(started.wait(),1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):await task
    assert closed==[True] and adapter.session is None
    with q.corpus.connect() as db:
        assert db.execute('SELECT state FROM rkb_expert_requests WHERE id=?',(accepted['request_id'],)).fetchone()[0]=='pending'


@pytest.mark.parametrize('offline',[False,True])
def test_existing_backend_lifespan_keeps_mcp_up_and_cancels_slack_worker(personal,monkeypatch,offline):
    from regional_knowledge import slack_runtime as module
    p=personal;settings=config(p.path,p.q)
    path=p.path/'slack-config.json';private(path,json.dumps(settings))
    monkeypatch.setenv('RKB_EXPERT_REQUESTS_ENABLED','1')
    monkeypatch.setenv('RKB_SLACK_RUNTIME_ENABLED','1')
    monkeypatch.setenv('RKB_SLACK_CONFIG_PATH',str(path))
    started=threading.Event();closed=threading.Event()
    class Adapter:
        def __init__(self,*args,**kwargs):pass
        async def run(self):
            started.set()
            if offline:raise SlackBoundaryError('synthetic_provider_offline')
            try:await asyncio.Event().wait()
            finally:closed.set()
    monkeypatch.setattr(module,'SlackExpertAdapter',Adapter)
    server=build_server(backend=p.s.backend,issuer=p.provider.issuer,resource_url=p.provider.resource,oauth_provider=p.provider)
    app=server.streamable_http_app(json_response=True,stateless_http=True,transport_security=_transport_security(p.provider.resource))
    with TestClient(app,base_url=p.provider.origin) as client:
        assert started.wait(1)
        assert client.get('/health').status_code==200
        # Real OAuth + MCP source read remains available during a provider
        # outage; this is not merely a health check or a direct backend call.
        flow=begin(client,p.provider)
        assert login(client,p,flow,p.code).status_code==303
        client.get('/oauth/consent',params={'id':flow['request_id']})
        token=approve_and_exchange(client,p,flow)['access_token']
        assert 'PUBLIC_SENTINEL' in mcp(client,token,'fetch',{'id':p.s.chunks['public']})['text']
    if not offline:assert closed.is_set()
    asyncio.run(p.s.backend.aclose())


def test_service_admin_cli_recovers_same_file_epoch_and_never_prints_token(queue,tmp_path,monkeypatch,capsys):
    from regional_knowledge import expert_admin as module
    from regional_knowledge.expert_requests import ServiceCredentials
    q=queue;directory=tmp_path/'services';directory.mkdir(mode=0o700)
    path=directory/'delivery.token'
    monkeypatch.setattr(module,'runtime',lambda:(q.requests,q.admin))
    command=['--service-id','cli_delivery','--kind','delivery','--token-file',str(path),'--expected-epoch','0','--generate']
    module.main(command)
    token=path.read_text().strip();first=capsys.readouterr().out
    assert token not in first and 'token' not in json.loads(first)
    assert path.stat().st_mode&0o077==0
    module.main(command)
    assert capsys.readouterr().out==first and path.read_text().strip()==token
    with q.corpus.connect() as db:
        assert q.requests._service(db,ServiceCredentials('cli_delivery',token),'delivery')['epoch']==1
    module.main(command[:-2]+['1','--disable'])
    assert json.loads(capsys.readouterr().out)['enabled'] is False
    with q.corpus.connect() as db:
        with pytest.raises(ExpertRequestError,match='service_denied'):
            q.requests._service(db,ServiceCredentials('cli_delivery',token),'delivery')
    # Original bootstrap replay cannot undo a later explicit revocation.
    with pytest.raises(SystemExit):module.main(command)


def test_service_bootstrap_cannot_use_personal_or_delegated_context(queue,tmp_path):
    from regional_knowledge.expert_admin import service_to_file
    from regional_knowledge.participants import ParticipantError,RequestAuthority
    q=queue;directory=tmp_path/'services';directory.mkdir(mode=0o700)
    for route in ('personal','expert'):
        with pytest.raises(ParticipantError):
            service_to_file(q.requests,RequestAuthority(q.actors[0],route),'denied_delivery','delivery',
                directory/(route+'.token'),expected_epoch=0,generate=True)
        assert not (directory/(route+'.token')).exists()


def test_owner_can_revoke_a_service_after_its_credential_file_is_lost(queue,tmp_path):
    from regional_knowledge.expert_admin import service_to_file
    from regional_knowledge.expert_requests import ServiceCredentials
    q=queue;directory=tmp_path/'lost-service';directory.mkdir(mode=0o700)
    path=directory/'executor.token'
    service_to_file(q.requests,q.admin,'lost_executor','executor',path,expected_epoch=0,generate=True)
    old_token=path.read_text().strip();path.unlink()
    result=service_to_file(q.requests,q.admin,'lost_executor','executor',path,
        expected_epoch=1,enabled=False,generate=True)
    assert result['enabled'] is False and path.read_text().strip()!=old_token
    with q.corpus.connect() as db:
        with pytest.raises(ExpertRequestError,match='service_denied'):
            q.requests._service(db,ServiceCredentials('lost_executor',old_token),'executor')
