"""Opt-in canonical participant OAuth uses existing HTTP/PKCE and encrypted store."""
import asyncio
import base64
import hashlib
from urllib.parse import parse_qs, urlsplit

import pytest
from starlette.testclient import TestClient

from regional_knowledge.backend import UnavailableBackend
from regional_knowledge.personal_auth_cli import issue_to_file
from regional_knowledge.server import build_server, _transport_security
from test_read_authority import auth, people


@pytest.mark.asyncio
async def test_real_nonowner_http_login_pkce_and_ignored_actor_fields(auth,monkeypatch):
    a=auth
    monkeypatch.setenv('RKB_OWNER_SUBJECT',a.p.owner.subject)
    server=build_server(backend=UnavailableBackend(),issuer=a.provider.issuer,
                       resource_url=a.provider.resource,oauth_provider=a.provider)
    verifier='synthetic-personal-pkce-'+'v'*40
    challenge=base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).decode().rstrip('=')
    def http_flow():
        with TestClient(server.streamable_http_app(stateless_http=True, json_response=True,
                        transport_security=_transport_security(a.provider.resource)), base_url=a.provider.origin) as client:
            callback=a.provider.redirect_uris[0]
            registered=client.post('/register',json={'redirect_uris':[callback],
                'token_endpoint_auth_method':'none','grant_types':['authorization_code','refresh_token'],
                'response_types':['code']})
            assert registered.status_code==201,registered.text
            cid=registered.json()['client_id']
            started=client.get('/authorize',params={'client_id':cid,'redirect_uri':callback,
                'response_type':'code','code_challenge':challenge,'code_challenge_method':'S256',
                'state':'synthetic','resource':a.provider.resource},follow_redirects=False)
            assert started.status_code==302
            page=client.get(started.headers['location'])
            assert page.status_code==200
            ident=parse_qs(urlsplit(started.headers['location']).query)['id'][0]
            login=client.post('/oauth/login',data={'id':ident,'csrf':client.cookies.get('__Host-rkb_csrf'),
                'secret':a.code,'subject':a.p.owner.subject,'owner':'true'},
                headers={'Origin':a.provider.origin},follow_redirects=False)
            assert login.status_code==303,login.text
            page=client.get(login.headers['location'])
            assert 'Synthetic expert' in page.text
            consent=client.post('/oauth/consent',data={'id':ident,'csrf':client.cookies.get('__Host-rkb_csrf'),
                'decision':'allow','subject':a.p.owner.subject},headers={'Origin':a.provider.origin},follow_redirects=False)
            assert consent.status_code==303,consent.text
            code=parse_qs(urlsplit(consent.headers['location']).query)['code'][0]
            fields={'client_id':cid,'grant_type':'authorization_code','code':code,
                'redirect_uri':callback,'resource':a.provider.resource,'code_verifier':verifier}
            bad=client.post('/token',data={**fields,'code_verifier':'wrong'})
            assert bad.status_code==400
            tokens=client.post('/token',data=fields)
            assert tokens.status_code==200,tokens.text
            assert client.post('/token',data=fields).status_code==400
            return tokens.json()
    tokens=await asyncio.to_thread(http_flow)
    loaded=await a.provider.load_access_token(tokens['access_token'])
    assert loaded.subject==a.p.other.subject and loaded.subject!=a.p.owner.subject
    context=await a.resolver.resolve_oauth(tokens['access_token'],destination=a.destination)
    assert context.person_id==a.p.other.subject


@pytest.mark.asyncio
async def test_local_credential_issue_resume_preserves_existing_authority_and_private_output(auth,tmp_path):
    a=auth
    private=tmp_path/'private'
    private.mkdir(mode=0o700)
    output=private/'synthetic-login.json'
    before_key=a.provider.store.key_file.read_bytes()
    metadata=await asyncio.to_thread(issue_to_file,a.provider,a.p.admin,a.p.other.subject,output,
                                     reason='synthetic owner enrollment')
    resumed=await asyncio.to_thread(issue_to_file,a.provider,a.p.admin,a.p.other.subject,output,
                                    reason='synthetic owner enrollment',resume=True)
    assert metadata==resumed and output.stat().st_mode & 0o077==0
    assert 'login_code' not in metadata
    assert a.provider.store.key_file.read_bytes()==before_key


@pytest.mark.asyncio
async def test_owner_session_requires_current_configured_owner_after_restart(auth):
    from uuid import uuid4
    from regional_knowledge.oauth_provider import RegionalOAuthProvider
    a=auth
    raw=await asyncio.to_thread(a.provider._new_owner_session)
    restored=RegionalOAuthProvider(issuer=a.provider.issuer,resource=a.provider.resource,
        client_id=a.provider.client.client_id,client_secret=str(a.provider.client.client_secret),
        redirect_uris=list(a.provider.redirect_uris),owner_subject=str(uuid4()),
        owner_login_secret='other-synthetic-owner-login',state_file=a.provider.store.state_file,
        key_file=a.provider.store.key_file)
    assert await asyncio.to_thread(restored._session_record,raw) is None
