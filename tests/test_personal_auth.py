"""Canonical expert login through real RKB OAuth/HTTP, no external accounts."""
import asyncio
import base64
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import time
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit
from uuid import uuid4

import pytest
from starlette.testclient import TestClient

from regional_knowledge.oauth_provider import RegionalOAuthProvider, _EncryptedStateStore
from regional_knowledge.participant_contracts import ParticipantDisable, ParticipantUpsert
from regional_knowledge.participants import ParticipantError, RequestAuthority
from regional_knowledge.personal_auth import PersonalPolicy, new_login_code
from regional_knowledge.personal_auth_cli import issue_to_file
from regional_knowledge.server import build_server, _transport_security
from test_expert_requests import queue
from test_expert_executor import sources


def provider_for(path, q):
    provider=RegionalOAuthProvider(issuer='https://knowledge.example.test',resource='https://knowledge.example.test/mcp',
        client_id='personal-tests',client_secret='synthetic-client-secret-'+'x'*40,
        redirect_uris=['https://chatgpt.com/connector_platform_oauth_redirect'],
        owner_subject=q.owner.subject,owner_login_secret='synthetic-owner-login-code',
        state_file=path/'oauth.state',key_file=path/'oauth.key')
    provider.personal_policy=PersonalPolicy(q.people)
    return provider


@pytest.fixture
def personal(sources,tmp_path,monkeypatch):
    s=sources;q=s.q
    provider=provider_for(tmp_path,q)
    ident,code=new_login_code()
    issued=provider.issue_personal_credential(q.admin,q.actors[1].subject,login_code=code,
        expires_at=time.time()+86400,reason='Synthetic expert enrollment')
    monkeypatch.setenv('RKB_OWNER_SUBJECT',q.owner.subject)
    server=build_server(backend=s.backend,issuer=provider.issuer,resource_url=provider.resource,oauth_provider=provider)
    return SimpleNamespace(s=s,q=q,provider=provider,ident=ident,code=code,issued=issued,server=server,path=tmp_path)


def begin(client,provider,callback='https://chatgpt.com/connector_platform_oauth_redirect'):
    registered=client.post('/register',json={'redirect_uris':[callback],'client_name':'Synthetic expert client',
        'token_endpoint_auth_method':'none','grant_types':['authorization_code','refresh_token'],'response_types':['code']})
    assert registered.status_code==201,registered.text
    cid=registered.json()['client_id'];verifier='synthetic-pkce-verifier-'+'v'*48
    challenge=base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).decode().rstrip('=')
    started=client.get('/authorize',params={'client_id':cid,'redirect_uri':callback,'response_type':'code',
        'code_challenge':challenge,'code_challenge_method':'S256','state':'synthetic-state','resource':provider.resource},follow_redirects=False)
    assert started.status_code==302,started.text
    page=client.get(started.headers['location']);assert page.status_code==200
    return {'request_id':parse_qs(urlsplit(started.headers['location']).query)['id'][0],
        'client_id':cid,'verifier':verifier,'callback':callback}


def login(client,p,flow,code):
    return client.post('/oauth/login',data={'id':flow['request_id'],'csrf':client.cookies.get('__Host-rkb_csrf'),
        'secret':code,'subject':p.q.owner.subject,'owner':'true'},headers={'Origin':p.provider.origin},follow_redirects=False)


def approve_and_exchange(client,p,flow):
    accepted=client.post('/oauth/consent',data={'id':flow['request_id'],'csrf':client.cookies.get('__Host-rkb_csrf'),
        'decision':'allow','subject':p.q.owner.subject},headers={'Origin':p.provider.origin},follow_redirects=False)
    assert accepted.status_code==303,accepted.text
    callback=urlsplit(accepted.headers['location']);params=parse_qs(callback.query)
    assert params['state']==['synthetic-state'] and params['iss']==[p.provider.issuer]
    exchange={'client_id':flow['client_id'],'grant_type':'authorization_code','code':params['code'][0],
        'redirect_uri':flow['callback'],'code_verifier':flow['verifier'],'resource':p.provider.resource}
    wrong=client.post('/token',data={**exchange,'code_verifier':'bad'})
    assert wrong.status_code==400
    tokens=client.post('/token',data=exchange)
    assert tokens.status_code==200,tokens.text
    assert client.post('/token',data=exchange).status_code==400
    return tokens.json()


def mcp(client,token,name,arguments):
    response=client.post('/mcp',headers={'Authorization':'Bearer '+token,'Accept':'application/json, text/event-stream'},
        json={'jsonrpc':'2.0','id':1,'method':'tools/call','params':{'name':name,'arguments':arguments}})
    assert response.status_code==200,response.text
    result=response.json()['result']
    return result.get('structuredContent') or json.loads(result['content'][0]['text']) if not result.get('isError') else result


@pytest.mark.parametrize('callback',['https://chatgpt.com/connector_platform_oauth_redirect','https://grok.com/synthetic-expert-callback'])
def test_native_nonowner_oauth_pkce_mcp_acl_refresh_and_revoke(personal,callback):
    p=personal;q=p.q
    app=p.server.streamable_http_app(json_response=True,stateless_http=True,transport_security=_transport_security(p.provider.resource))
    with TestClient(app,base_url=p.provider.origin) as client:
        raw_code=client.post('/mcp',headers={'Authorization':'Bearer '+p.code,'Accept':'application/json, text/event-stream'},
            json={'jsonrpc':'2.0','id':1,'method':'tools/list'})
        assert raw_code.status_code==401
        flow=begin(client,p.provider,callback)
        assert login(client,p,flow,p.code).status_code==303
        consent=client.get('/oauth/consent',params={'id':flow['request_id']})
        assert 'Synthetic 2' in consent.text and 'Владелец РКБ' not in consent.text
        tokens=approve_and_exchange(client,p,flow)
        access=tokens['access_token']
        loaded=asyncio.run(p.provider.load_access_token(access))
        assert loaded.subject==q.actors[1].subject and loaded.subject!=q.owner.subject
        assert loaded.resource==p.provider.resource
        assert mcp(client,access,'profile',{})['id']==q.actors[1].subject
        public=mcp(client,access,'fetch',{'id':p.s.chunks['public']})
        assert 'PUBLIC_SENTINEL' in public['text']
        private=mcp(client,access,'fetch',{'id':p.s.chunks['private']})
        assert private.get('isError') and 'PRIVATE_SENTINEL' not in json.dumps(private)
        denied=mcp(client,access,'book_ingest',{'command':'start','owner':True})
        assert denied.get('isError')
        q.corpus.put('rkb_document_grants',[{'document_id':p.s.docs['private'],'grantee_user_id':q.actors[1].subject}])
        assert 'PRIVATE_SENTINEL' in mcp(client,access,'fetch',{'id':p.s.chunks['private']})['text']
        refreshed=client.post('/token',data={'client_id':flow['client_id'],'grant_type':'refresh_token',
            'refresh_token':tokens['refresh_token'],'resource':p.provider.resource})
        assert refreshed.status_code==200,refreshed.text
        rotated=refreshed.json()
        assert asyncio.run(p.provider.load_access_token(rotated['access_token'])).subject==q.actors[1].subject
        # Revoking the native credential closes every resulting session/grant,
        # not only the most recent refresh token family.
        p.provider.revoke_personal_credential(q.admin,p.ident,expected_epoch=1,reason='Synthetic revocation')
        assert asyncio.run(p.provider.load_access_token(access)) is None
        assert asyncio.run(p.provider.load_access_token(rotated['access_token'])) is None
        rejected=client.post('/token',data={'client_id':flow['client_id'],'grant_type':'refresh_token',
            'refresh_token':rotated['refresh_token'],'resource':p.provider.resource})
        assert rejected.status_code==400
        assert p.provider._session_subject(client.cookies.get('__Host-rkb_owner')) is None
    asyncio.run(p.s.backend.aclose())


def test_current_profile_disable_and_expiry_block_login_session_and_tokens(personal):
    p=personal;q=p.q
    session=p.provider._new_personal_session(p.code)
    assert p.provider._session_subject(session)==q.actors[1].subject
    q.change(q.actors[1].subject,ParticipantDisable(disabled=True,reason='Synthetic disable'))
    assert p.provider._session_subject(session) is None
    with pytest.raises(ValueError,match='login'):p.provider._new_personal_session(p.code)
    q.change(q.actors[1].subject,ParticipantDisable(disabled=False,reason='Synthetic enable'))
    assert p.provider._session_subject(session)==q.actors[1].subject
    from datetime import datetime,timezone,timedelta
    q.change(q.actors[1].subject,ParticipantUpsert(name='Synthetic expired',expires_at=q.clock[0]-timedelta(seconds=1),reason='Synthetic expiry'))
    assert p.provider._session_subject(session) is None


def test_credential_issue_denies_owner_unknown_person_and_nonowner_admin(personal):
    p=personal;q=p.q
    missing=str(uuid4())
    for subject in (q.owner.subject,missing):
        _,code=new_login_code()
        with pytest.raises(ValueError):p.provider.issue_personal_credential(q.admin,subject,login_code=code,expires_at=time.time()+100,reason='Synthetic denied')
    _,code=new_login_code()
    with pytest.raises(ParticipantError):
        p.provider.issue_personal_credential(RequestAuthority(q.actors[0],'personal'),q.actors[1].subject,
            login_code=code,expires_at=time.time()+100,reason='Synthetic denied')
    assert not q.corpus.one('rkb_users',missing)


def test_personal_credential_and_existing_owner_grants_survive_actual_restart(personal):
    p=personal
    session=p.provider._new_personal_session(p.code)
    owner_session=p.provider._new_owner_session()
    restored=provider_for(p.path,p.q)
    assert restored._session_subject(session)==p.q.actors[1].subject
    assert restored._session_subject(owner_session)==p.q.owner.subject
    raw=(p.path/'oauth.state').read_bytes()
    assert p.code.encode() not in raw and p.q.actors[1].subject.encode() not in raw
    assert p.provider.store.read(lambda state:state['personal_credentials'][p.ident]['secret_hash'])!=p.code


def test_protected_credential_file_recovery_and_no_duplicate_issuance(personal,tmp_path):
    p=personal;q=p.q
    directory=tmp_path/'credentials';directory.mkdir(mode=0o700)
    target=directory/'expert.json'
    first=issue_to_file(p.provider,q.admin,q.actors[0].subject,target,reason='Synthetic file issue')
    original=target.read_bytes()
    with pytest.raises(FileExistsError):issue_to_file(p.provider,q.admin,q.actors[0].subject,target,reason='Synthetic file issue')
    assert issue_to_file(p.provider,q.admin,q.actors[0].subject,target,reason='Synthetic file issue',resume=True)==first
    assert target.read_bytes()==original and target.stat().st_mode&0o077==0
    with pytest.raises(ValueError):issue_to_file(p.provider,q.admin,q.actors[1].subject,target,reason='Synthetic file issue',resume=True)
    target.chmod(0o644)
    with pytest.raises(ValueError):issue_to_file(p.provider,q.admin,q.actors[0].subject,target,reason='Synthetic file issue',resume=True)


def test_two_oauth_store_instances_preserve_concurrent_mutations(tmp_path):
    left=_EncryptedStateStore(tmp_path/'oauth.state',tmp_path/'oauth.key')
    right=_EncryptedStateStore(tmp_path/'oauth.state',tmp_path/'oauth.key')
    def update(index):
        store=left if index%2 else right
        def change(state):
            count=state['personal_audit'].get('synthetic-counter',0)
            time.sleep(.002)
            state['personal_audit']['synthetic-counter']=count+1
        store.mutate(change)
    with ThreadPoolExecutor(max_workers=8) as pool:list(pool.map(update,range(24)))
    assert left.read(lambda state:state['personal_audit']['synthetic-counter'])==24
    assert (tmp_path/'oauth.state.lock').stat().st_mode&0o077==0


def test_personal_browser_csrf_logout_switch_and_expired_session(personal):
    p=personal
    app=p.server.streamable_http_app(json_response=True,stateless_http=True,transport_security=_transport_security(p.provider.resource))
    with TestClient(app,base_url=p.provider.origin) as client:
        flow=begin(client,p.provider)
        bad=client.post('/oauth/login',data={'id':flow['request_id'],'csrf':'wrong','secret':p.code},
            headers={'Origin':p.provider.origin},follow_redirects=False)
        assert bad.status_code==401 and not client.cookies.get('__Host-rkb_owner')
        assert login(client,p,flow,p.code).status_code==303
        page=client.get('/oauth/consent',params={'id':flow['request_id']})
        assert 'Synthetic 2' in page.text
        assert client.post('/oauth/logout',data={'id':flow['request_id'],'csrf':'wrong'},headers={'Origin':p.provider.origin}).status_code==400
        logout=client.post('/oauth/logout',data={'id':flow['request_id'],'csrf':client.cookies.get('__Host-rkb_csrf')},
            headers={'Origin':p.provider.origin},follow_redirects=False)
        assert logout.status_code==303 and not client.cookies.get('__Host-rkb_owner')
        assert 'Synthetic 2' not in client.get(logout.headers['location']).text
        assert login(client,p,flow,'synthetic-owner-login-code').status_code==303
        page=client.get('/oauth/consent',params={'id':flow['request_id']})
        assert 'Владелец РКБ' in page.text and 'Synthetic 2' not in page.text
    session=p.provider._new_personal_session(p.code)
    previous=p.provider.clock
    p.provider.clock=lambda:previous()+901
    assert p.provider._session_subject(session) is None
    asyncio.run(p.s.backend.aclose())


def test_oauth_profile_revocation_and_html_name_escaping(personal):
    p=personal;q=p.q
    q.change(q.actors[1].subject,ParticipantUpsert(name='Synthetic <expert>&',reason='Synthetic display'))
    app=p.server.streamable_http_app(json_response=True,stateless_http=True,transport_security=_transport_security(p.provider.resource))
    with TestClient(app,base_url=p.provider.origin) as client:
        flow=begin(client,p.provider)
        assert login(client,p,flow,p.code).status_code==303
        page=client.get('/oauth/consent',params={'id':flow['request_id']})
        assert 'Synthetic &lt;expert&gt;&amp;' in page.text and 'Synthetic <expert>' not in page.text
        tokens=approve_and_exchange(client,p,flow)
        q.change(q.actors[1].subject,ParticipantDisable(disabled=True,reason='Synthetic revoke'))
        assert asyncio.run(p.provider.load_access_token(tokens['access_token'])) is None
        assert p.provider._session_subject(client.cookies.get('__Host-rkb_owner')) is None
        refresh=client.post('/token',data={'client_id':flow['client_id'],'grant_type':'refresh_token',
            'refresh_token':tokens['refresh_token'],'resource':p.provider.resource})
        assert refresh.status_code==400
    asyncio.run(p.s.backend.aclose())


def test_reconfigured_owner_cannot_load_previous_owner_tokens(tmp_path):
    from test_oauth_provider import _provider,_params
    provider=_provider(tmp_path)
    pending=asyncio.run(provider.authorize(provider.client,_params()))
    code=parse_qs(urlsplit(provider.approve(parse_qs(urlsplit(pending).query)['id'][0])).query)['code'][0]
    loaded=asyncio.run(provider.load_authorization_code(provider.client,code))
    tokens=asyncio.run(provider.exchange_authorization_code(provider.client,loaded))
    assert asyncio.run(_provider(tmp_path).load_access_token(tokens.access_token)) is not None
    changed=_provider(tmp_path,owner_subject=str(uuid4()))
    assert asyncio.run(changed.load_access_token(tokens.access_token)) is None


def test_actual_personal_cli_prints_only_receipts_and_replays_private_file(personal,tmp_path,monkeypatch,capsys):
    from regional_knowledge import personal_auth_cli as cli
    p=personal;q=p.q
    monkeypatch.setattr(cli,'_runtime',lambda:(p.provider,q.admin))
    directory=tmp_path/'cli-credentials';directory.mkdir(mode=0o700)
    target=directory/'expert.json'
    command=['issue','--person',q.actors[0].subject,'--output',str(target),'--reason','Synthetic CLI issue']
    cli.main(command)
    out=capsys.readouterr().out;receipt=json.loads(out)
    code=json.loads(target.read_text())['login_code']
    assert code not in out and 'login_code' not in out and 'secret_hash' not in out
    cli.main(command+['--resume'])
    assert json.loads(capsys.readouterr().out)==receipt
    cli.main(['revoke','--credential',receipt['credential_id'],'--expected-epoch','1','--reason','Synthetic CLI revoke'])
    assert json.loads(capsys.readouterr().out)['enabled'] is False
    with pytest.raises(ValueError,match='login'):p.provider._new_personal_session(code)


@pytest.mark.asyncio
async def test_oauth_file_lock_wait_does_not_block_other_http_tasks(personal):
    import threading
    from test_oauth_provider import _dynamic_client
    p=personal
    dynamic=_dynamic_client(redirect_uri='https://chatgpt.com/connector_platform_oauth_redirect')
    await p.provider.register_client(dynamic)
    started=threading.Event();released=threading.Event()
    second=_EncryptedStateStore(p.path/'oauth.state',p.path/'oauth.key')
    def writer():
        def hold(state):
            started.set();released.wait(2)
        second.mutate(hold)
    write=asyncio.create_task(asyncio.to_thread(writer))
    await asyncio.to_thread(started.wait)
    async def release():
        await asyncio.sleep(.03);released.set()
    async def read():
        client=await p.provider.get_client(dynamic.client_id)
        assert released.is_set(), 'OAuth lookup blocked the shared loop before its scheduled release'
        assert client.client_id==dynamic.client_id
    try:
        await asyncio.gather(read(),release(),write)
    finally:
        released.set();await write


def test_personal_live_read_bundle_preserves_one_tool_and_current_capability(personal):
    from regional_knowledge.participant_contracts import ParticipantGrant
    p=personal;q=p.q
    app=p.server.streamable_http_app(json_response=True,stateless_http=True,transport_security=_transport_security(p.provider.resource))
    with TestClient(app,base_url=p.provider.origin) as client:
        flow=begin(client,p.provider)
        assert login(client,p,flow,p.code).status_code==303
        client.get('/oauth/consent',params={'id':flow['request_id']})
        tokens=approve_and_exchange(client,p,flow)
    live=build_server(backend=p.s.backend,issuer=p.provider.issuer,resource_url=p.provider.resource,oauth_provider=p.provider,profile='live')
    app=live.streamable_http_app(json_response=True,stateless_http=True,transport_security=_transport_security(p.provider.resource))
    with TestClient(app,base_url=p.provider.origin) as client:
        headers={'Authorization':'Bearer '+tokens['access_token'],'Accept':'application/json, text/event-stream'}
        catalog=client.post('/mcp',headers=headers,json={'jsonrpc':'2.0','id':1,'method':'tools/list'})
        assert {t['name'] for t in catalog.json()['result']['tools']}=={'knowledge_search'}
        found=mcp(client,tokens['access_token'],'knowledge_search',{'query':'Synthetic atlas'})
        assert 'PUBLIC_SENTINEL' in json.dumps(found) and 'PRIVATE_SENTINEL' not in json.dumps(found)
        q.change(q.actors[1].subject,ParticipantGrant(capability='public_search',granted=False,reason='Synthetic Live revoke'))
        rejected=mcp(client,tokens['access_token'],'knowledge_search',{'query':'Synthetic atlas'})
        assert rejected['isError'] and 'PUBLIC_SENTINEL' not in json.dumps(rejected)
    asyncio.run(p.s.backend.aclose())
