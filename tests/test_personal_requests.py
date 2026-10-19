"""Two client kinds share request order, authority, results and fair starts."""
import asyncio
import json

import pytest
from starlette.testclient import TestClient

from regional_knowledge.expert_requests import ExpertResult
from regional_knowledge.expert_requests import ExpertRequests
from regional_knowledge.participants import RequestAuthority
from regional_knowledge.server import build_server,_transport_security
from test_expert_requests import queue,observation,expect
from test_expert_executor import sources
from test_personal_auth import personal,begin,login,approve_and_exchange,mcp


@pytest.fixture
def managed(personal,monkeypatch):
    p=personal
    config=p.path/'runtime.json'
    config.write_text(json.dumps({'app_id':'A111','team_id':'T111','bot_user_id':'UBOT','allowed_channels':['C111']}));config.chmod(0o600)
    monkeypatch.setenv('RKB_EXPERT_REQUESTS_ENABLED','1');monkeypatch.setenv('RKB_SLACK_CONFIG_PATH',str(config))
    p.server=build_server(backend=p.s.backend,issuer=p.provider.issuer,resource_url=p.provider.resource,oauth_provider=p.provider)
    p.q.requests.provision_service(p.q.admin,p.q.executor.service_id,'executor',p.q.executor.token,
        expected_epoch=1,personal_resource=p.provider.resource)
    return p


def native_client(p):
    return TestClient(p.server.streamable_http_app(json_response=True,stateless_http=True,
        transport_security=_transport_security(p.provider.resource)),base_url=p.provider.origin)


def authorize(client,p):
    flow=begin(client,p.provider)
    assert login(client,p,flow,p.code).status_code==303
    return flow,approve_and_exchange(client,p,flow)


def submit(client,token,key='question-0001',dialog='dialog-00001',text='Synthetic question'):
    return mcp(client,token,'research_request',{'action':'submit','dialog_key':dialog,'client_request_key':key,'text':text})


def test_http_personal_submit_restart_result_ack_and_no_slack_send(managed):
    p=managed;q=p.q
    with native_client(p) as client:
        flow,tokens=authorize(client,p);token=tokens['access_token']
        first=submit(client,token);second=submit(client,token,key='question-0002',text='Continue the first answer')
        assert first['dialog_seq']==1 and second['dialog_seq']==2
        assert submit(client,token)=={**first,'duplicate':True}
        assert submit(client,token,text='Different')['error']['code']=='event_identity_conflict'
        # Supplying a human ID is rejected by the MCP schema.
        for field in ('person_id','tenant_id','dialog_id'):
            spoof=mcp(client,token,'research_request',{'action':'submit','dialog_key':'dialog-00001',
                'client_request_key':'question-spoof','text':'Spoof',field:q.owner.subject})
            assert spoof['isError']
        lease=q.requests.claim(q.executor,request_id=first['request_id'])
        context=q.requests.context(q.executor,lease['request_id'],lease['generation'],lease['execution_token'])
        assert context.principal.subject==q.actors[1].subject and context.route=='delegated'
        expect('capability_denied',q.people.require,context,'import')
        q.requests.record_result(q.executor,lease['request_id'],lease['generation'],lease['execution_token'],ExpertResult(text='Synthetic final answer'))
        result=mcp(client,token,'research_request',{'action':'result','request_id':first['request_id']})
        assert result['result']['text']=='Synthetic final answer'
        assert q.requests.next_delivery(q.delivery) is None
        expect('request_unavailable',q.requests.claim_delivery,q.delivery,first['request_id'])
        assert q.requests.claim(q.executor)['state']=='empty'
        q.requests=ExpertRequests(q.people,app_id='A111',team_id='T111',bot_user_id='UBOT',allowed_channels=['C111'])
        wrong=mcp(client,token,'research_request',{'action':'ack','request_id':first['request_id'],'result_hash':'a'*64})
        assert wrong['error']['code']=='provider_receipt_mismatch'
        ack={'action':'ack','request_id':first['request_id'],'result_hash':result['result_hash']}
        assert mcp(client,token,'research_request',ack)['state']=='delivered'
        assert mcp(client,token,'research_request',ack)['state']=='delivered'
        next_lease=q.requests.claim(q.executor,request_id=second['request_id'])
        history=q.requests.history(q.executor,next_lease['request_id'],next_lease['generation'],next_lease['execution_token'])
        assert history['items'][0]['result']['text']=='Synthetic final answer'
        # A different OAuth client/person cannot guess an opaque result ID.
        denied=q.owner.model_copy(update={'client_id':'other-client'})
        loaded=asyncio.run(p.provider.load_access_token(token))
        expect('identity_changed',q.requests.personal_result,RequestAuthority(denied,'owner'),loaded.claims['rkb_request_grant'],p.provider.resource,first['request_id'])
    asyncio.run(p.s.backend.aclose())


def test_native_and_slack_same_human_get_one_fair_share_and_revocation_fences_active_work(managed):
    p=managed;q=p.q
    with native_client(p) as client:
        flow,tokens=authorize(client,p);token=tokens['access_token']
        native=submit(client,token)
        # Person U2 is also the OAuth person; U1 has a separate waiting turn.
        slack=q.requests.accept(observation('U2',100))
        other=q.requests.accept(observation('U1',101))
        first=q.requests.claim(q.executor)
        assert first['request_id']==native['request_id']
        assert q.requests.claim(q.executor,request_id=slack['request_id'])=={'state':'deferred'}
        assert q.requests.claim(q.executor)['request_id']==other['request_id']
        assert q.requests.claim(q.executor)['request_id']==slack['request_id']
        context=q.requests.context(q.executor,first['request_id'],first['generation'],first['execution_token'])
        # Family revocation is mirrored before token deletion and checked by
        # already-created contexts, not only at the next bearer authentication.
        loaded=asyncio.run(p.provider.load_access_token(token))
        asyncio.run(p.provider.revoke_token(loaded))
        expect('request_fence_denied',q.people.require,context,'public_search')
        expect('identity_changed',q.requests.record_result,q.executor,first['request_id'],first['generation'],first['execution_token'],ExpertResult(text='Too late'))
        assert asyncio.run(p.provider.load_access_token(token)) is None
    asyncio.run(p.s.backend.aclose())


def test_native_saved_work_survives_bearer_expiry_but_not_credential_revocation(managed):
    p=managed;q=p.q
    with native_client(p) as client:
        _,tokens=authorize(client,p)
        accepted=submit(client,tokens['access_token'])
        loaded=asyncio.run(p.provider.load_access_token(tokens['access_token']))
        # Client disconnect/token expiry must not cancel already accepted work.
        def expire(state):
            for row in state['access'].values():row['expires_at']=p.provider.clock()-1
        p.provider.store.mutate(expire)
        assert asyncio.run(p.provider.load_access_token(tokens['access_token'])) is None
        lease=q.requests.claim(q.executor,request_id=accepted['request_id'])
        context=q.requests.context(q.executor,lease['request_id'],lease['generation'],lease['execution_token'])
        assert q.people.require(context,'public_search')==q.actors[1].subject
        p.provider.revoke_personal_credential(q.admin,p.ident,expected_epoch=1,reason='Revoked during execution')
        expect('request_fence_denied',q.people.require,context,'public_search')
        assert loaded.claims['rkb_request_grant']
    asyncio.run(p.s.backend.aclose())


def test_two_oauth_clients_cannot_read_or_ack_each_others_result(managed):
    p=managed;q=p.q
    with native_client(p) as client:
        _,first_tokens=authorize(client,p)
        first=submit(client,first_tokens['access_token'])
        lease=q.requests.claim(q.executor,request_id=first['request_id'])
        q.requests.record_result(q.executor,lease['request_id'],lease['generation'],lease['execution_token'],ExpertResult(text='First client result'))
        own=mcp(client,first_tokens['access_token'],'research_request',{'action':'result','request_id':first['request_id']})
        _,other_tokens=authorize(client,p)
        denied=mcp(client,other_tokens['access_token'],'research_request',{'action':'result','request_id':first['request_id']})
        assert denied['error']['code']=='request_unavailable'
        denied_ack=mcp(client,other_tokens['access_token'],'research_request',{'action':'ack','request_id':first['request_id'],'result_hash':own['result_hash']})
        assert denied_ack['error']['code']=='request_unavailable'
        other=submit(client,other_tokens['access_token'])
        assert other['dialog_id']!=first['dialog_id'] and other['request_id']!=first['request_id']
        # Both clients still use one human fairness subject.
        with q.corpus.connect() as db:
            assert db.execute('SELECT count(*) FROM rkb_request_people WHERE person_id=?',(q.actors[1].subject,)).fetchone()[0]==1
    asyncio.run(p.s.backend.aclose())


def test_personal_requests_do_not_require_a_slack_install(personal,monkeypatch):
    p=personal
    monkeypatch.setenv('RKB_EXPERT_REQUESTS_ENABLED','1')
    monkeypatch.delenv('RKB_SLACK_CONFIG_PATH',raising=False);monkeypatch.delenv('RKB_REQUEST_CONFIG_PATH',raising=False)
    p.server=build_server(backend=p.s.backend,issuer=p.provider.issuer,resource_url=p.provider.resource,oauth_provider=p.provider)
    requests=p.server.expert_requests
    assert requests.bot_user_id=='' and not requests.allowed_channels
    with native_client(p) as client:
        _,tokens=authorize(client,p)
        accepted=submit(client,tokens['access_token'])
        assert accepted['state']=='pending'
    asyncio.run(p.s.backend.aclose())


def test_owner_managed_request_cannot_borrow_owner_administration(managed):
    p=managed;q=p.q
    with native_client(p) as client:
        flow=begin(client,p.provider)
        assert login(client,p,flow,'synthetic-owner-login-code').status_code==303
        tokens=approve_and_exchange(client,p,flow)
        accepted=submit(client,tokens['access_token'])
        lease=q.requests.claim(q.executor,request_id=accepted['request_id'])
        context=q.requests.context(q.executor,lease['request_id'],lease['generation'],lease['execution_token'])
        assert context.principal.subject==q.owner.subject and context.route=='delegated'
        for capability in ('import','participants_manage','maintain_shared_summary'):
            expect('capability_denied',q.people.require,context,capability)
    asyncio.run(p.s.backend.aclose())
