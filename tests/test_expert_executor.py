"""Real SQLite source reads and HTTP MCP auth for simultaneous expert routes."""
import asyncio
from contextlib import asynccontextmanager
import hashlib
import json
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
import pytest_asyncio

from regional_knowledge.expert_executor import ExpertExecutor
from regional_knowledge.expert_requests import ExpertRequestError,ExpertResult
from regional_knowledge.expert_source_scope import current_source_scope
from regional_knowledge.object_store import UnavailableObjectStore
from regional_knowledge.server import _transport_security
from regional_knowledge.sqlite_backend import SQLiteBackend
from regional_knowledge.sqlite_data import defaults
from regional_knowledge.supabase_backend import LexicalOnlyEmbedder
from test_expert_requests import claim,observation,queue


@pytest.fixture
def sources(queue,monkeypatch):
    q=queue
    monkeypatch.delenv('RKB_BGE_ENABLED',raising=False)
    monkeypatch.delenv('RKB_AUTO_INDEX_ENABLED',raising=False)
    backend=SQLiteBackend(corpus_path=q.corpus.path,embedder=LexicalOnlyEmbedder(),object_store=UnavailableObjectStore())
    docs={};chunks={}
    for kind in ('public','private'):
        doc,chunk,page,region=[str(uuid4()) for _ in range(4)]
        docs[kind]=doc;chunks[kind]=chunk
        text='Synthetic atlas '+('PUBLIC_SENTINEL' if kind=='public' else 'PRIVATE_SENTINEL')
        sha=hashlib.sha256(text.encode()).hexdigest()
        q.corpus.put('rkb_documents',[{**defaults('rkb_documents'),'id':doc,'owner_user_id':q.actors[0].subject,
            'title':kind+' book','active_revision':1,'source_sha256':'a'*64,'content_visibility':kind,
            'rights_status':'permission_granted' if kind=='public' else 'unknown',
            'rights_policy_version':'synthetic-v1','rights_evidence':{'public_distribution':kind=='public'}}])
        q.corpus.put('rkb_pages',[{**defaults('rkb_pages'),'id':page,'document_id':doc,'revision':1,'physical_page_index':0}])
        q.corpus.put('rkb_regions',[{**defaults('rkb_regions'),'id':region,'page_id':page,'source_text':text,'kind':'body','reading_order':0}])
        q.corpus.put('rkb_chunks',[{**defaults('rkb_chunks'),'id':chunk,'document_id':doc,'revision':1,'title':text,
            'page_ids':[page],'region_ids':[region],'source_text':text,'text_sha256':sha,'search_material':text,'search_material_sha256':sha,
            'metadata':{'private_editor_note':'CONTROL_INTERNAL_METADATA'}}])
        q.corpus.build_fragments(doc,1)
    return SimpleNamespace(q=q,backend=backend,executor=ExpertExecutor(q.requests,backend),docs=docs,chunks=chunks)


@pytest.mark.asyncio
async def test_simultaneous_same_actor_group_and_dm_use_distinct_source_scope(sources):
    s=sources;q=s.q
    personal,_=claim(q,observation('U1',1))
    public,_=claim(q,observation('U1',2,group=True))
    def args(lease):return (q.executor,lease['request_id'],lease['generation'],lease['execution_token'])
    private_result,public_result=await asyncio.gather(
        s.executor.search(*args(personal),'Synthetic atlas'),
        s.executor.search(*args(public),'Synthetic atlas'))
    assert {r['id'] for r in private_result['results']}==set(s.chunks.values())
    assert {r['id'] for r in public_result['results']}=={s.chunks['public']}
    assert 'PRIVATE_SENTINEL' not in json.dumps(public_result)
    assert current_source_scope.get() is None
    body=await s.executor.fetch(*args(public),s.chunks['public'])
    assert body['text'].endswith('PUBLIC_SENTINEL') and 'private_editor_note' not in body['metadata']
    with pytest.raises(ExpertRequestError,match='public_projection_required'):
        await s.executor.fetch(*args(public),s.chunks['private'])
    # An ordinary owner-source read after both calls retains its ordinary scope.
    ordinary=await s.backend.search('Synthetic atlas',q.actors[0])
    assert {r.id for r in ordinary.results}==set(s.chunks.values())
    await s.backend.aclose()


@pytest.mark.asyncio
async def test_source_read_dependencies_reject_omitted_references_after_revocation(sources):
    s=sources;q=s.q
    # U2 is a real independent synthetic account with only an explicit grant.
    q.corpus.put('rkb_document_grants',[{'document_id':s.docs['private'],'grantee_user_id':q.actors[1].subject}])
    lease,_=claim(q,observation('U2',1));args=(q.executor,lease['request_id'],lease['generation'],lease['execution_token'])
    body=await s.executor.fetch(*args,s.chunks['private'])
    assert 'PRIVATE_SENTINEL' in body['text']
    q.requests.record_result(*args,ExpertResult(text='Derived answer with deliberately omitted document IDs.'))
    with q.corpus.connect() as db:db.execute("DELETE FROM corpus_rows WHERE table_name='rkb_document_grants'")
    with pytest.raises(ExpertRequestError,match='resource_access_revoked'):
        q.requests.claim_delivery(q.delivery,lease['request_id'])
    await s.backend.aclose()


@pytest.mark.asyncio
async def test_public_text_policy_and_unaccepted_revision_fail_before_return(sources):
    s=sources;q=s.q;lease,_=claim(q,observation(group=True))
    args=(q.executor,lease['request_id'],lease['generation'],lease['execution_token'])
    row=q.corpus.one('rkb_chunks',s.chunks['public']);staged=str(uuid4())
    q.corpus.put('rkb_chunks',[{**row,'id':staged,'revision':2}])
    with pytest.raises(ExpertRequestError,match='source_revision_not_current'):await s.executor.fetch(*args,staged)
    document=q.corpus.one('rkb_documents',s.docs['public'])
    q.corpus.put('rkb_documents',[{**document,'rights_evidence':{'public_distribution':'true'}}])
    with pytest.raises(ExpertRequestError,match='public_projection_required'):
        await s.executor.fetch(*args,s.chunks['public'])
    assert current_source_scope.get() is None
    await s.backend.aclose()


@pytest_asyncio.fixture
async def surface(sources,tmp_path,monkeypatch):
    from mcp.server.auth.provider import AccessToken
    from regional_knowledge import server as module
    q=sources.q
    class Verifier:
        def __init__(self,**kwargs):pass
        async def verify_token(self,token):
            people={'synthetic-owner-token':q.owner, 'synthetic-personal-token':q.actors[0], 'synthetic-other-token':q.actors[1]}
            actor=people.get(token)
            if actor:
                return AccessToken(token=token,client_id='personal-client',subject=actor.subject,scopes=[],resource='http://127.0.0.1/mcp')
            return None
    monkeypatch.setattr(module,'JwtResourceVerifier',Verifier)
    monkeypatch.setenv('RKB_AUTH_MODE','external')
    monkeypatch.setenv('RKB_AUTH_ISSUER','https://auth.example')
    monkeypatch.setenv('RKB_RESOURCE_URL','http://127.0.0.1/mcp')
    monkeypatch.setenv('RKB_AUTH_JWKS_URL','https://auth.example/keys')
    monkeypatch.setenv('RKB_OWNER_SUBJECT',q.owner.subject)
    monkeypatch.setenv('RKB_EXPERT_REQUESTS_ENABLED','1')
    cfg=tmp_path/'slack-config.json'
    cfg.write_text(json.dumps({'app_id':'A111','team_id':'T111','bot_user_id':'UBOT','allowed_channels':['C111']}));cfg.chmod(0o600)
    monkeypatch.setenv('RKB_SLACK_CONFIG_PATH',str(cfg))
    mcp=module.build_server(backend=sources.backend)
    mcp.research_participants.clock=q.people.clock
    from cryptography.fernet import Fernet
    from regional_knowledge.expert_event_mcp import register_event_methods
    from regional_knowledge.expert_events import ExpertEvents
    verified=[]
    class Sender:
        async def verify(self,*args):verified.append(args[:2])
    events=ExpertEvents(q.requests,Fernet.generate_key(),sender=Sender())
    register_event_methods(mcp,events)
    app=mcp.streamable_http_app(json_response=True,stateless_http=True,transport_security=_transport_security())
    started=asyncio.Event();stop=asyncio.Event()
    async def serve():
        async with app.router.lifespan_context(app):
            started.set()
            await stop.wait()
    task=asyncio.create_task(serve())
    await started.wait()
    try:
        yield SimpleNamespace(server=mcp,raw_app=app,app=app,events=events,verified=verified)
    finally:
        stop.set()
        await task


@asynccontextmanager
async def client_for(surface,credentials):
    headers={'Accept':'application/json, text/event-stream','MCP-Protocol-Version':'2025-06-18'}
    if credentials:
        raw=credentials if isinstance(credentials,str) else credentials.service_id+':'+credentials.token
        headers['Authorization']='Bearer '+raw
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=surface.app),base_url='http://127.0.0.1',headers=headers) as client:
        yield client


async def rpc(client,method,params):
    if client.headers.get('MCP-Protocol-Version')=='2026-07-28':
        params={**params,'_meta':{'io.modelcontextprotocol/protocolVersion':'2026-07-28','io.modelcontextprotocol/clientCapabilities':{}}}
    response=await client.post('/mcp',headers={'MCP-Method':method},json={'jsonrpc':'2.0','id':str(uuid4()),'method':method,'params':params})
    assert response.status_code in (200,400),response.text
    if response.status_code==400:assert 'error' in response.json()
    return response.json()


def tool_value(data):
    result=data['result']
    return result.get('structuredContent') or json.loads(result['content'][0]['text'])


@pytest.mark.asyncio
async def test_http_executor_same_resource_has_no_owner_or_import_rights_and_rechecks_service(sources,surface):
    s=sources;q=s.q
    accepted=q.requests.accept(observation())
    async with client_for(surface,q.executor) as client:
        tools=await rpc(client,'tools/list',{})
        assert {t['name'] for t in tools['result']['tools']}=={'expert_request_delivery','expert_research'}
        assert 'actor_id' not in tools['result']['tools'][0]['inputSchema']['properties']
        lease=tool_value(await rpc(client,'tools/call',{'name':'expert_request_delivery','arguments':{'action':'claim','request_id':accepted['request_id']}}))
        assert lease['state']=='leased'
        bound={'request_id':lease['request_id'],'generation':lease['generation'],'execution_token':lease['execution_token']}
        fetched=tool_value(await rpc(client,'tools/call',{'name':'expert_research','arguments':{**bound,'operation':'source_fetch','chunk_id':s.chunks['private']}}))
        assert 'PRIVATE_SENTINEL' in fetched['text']
        invalid=await rpc(client,'tools/call',{'name':'expert_research','arguments':{**bound,'operation':'book_ingest'}})
        assert invalid['result'].get('isError')
        q.requests.provision_service(q.admin,q.executor.service_id,'executor',q.executor.token,expected_epoch=1,enabled=False)
        rejected=await client.post('/mcp',json={'jsonrpc':'2.0','id':1,'method':'tools/list','params':{}})
        assert rejected.status_code==401
    await s.backend.aclose()


@pytest.mark.asyncio
async def test_http_auth_rejects_delivery_and_anonymous_on_same_resource(sources,surface):
    s=sources;q=s.q
    for credentials in (None,q.delivery):
        async with client_for(surface,credentials) as client:
            response=await client.post('/mcp',json={'jsonrpc':'2.0','id':1,'method':'tools/list','params':{}})
            assert response.status_code==401
    await s.backend.aclose()


@pytest.mark.asyncio
async def test_one_endpoint_personal_service_authority_and_direct_forbidden_calls(sources,surface):
    s=sources;q=s.q
    async with client_for(surface,'synthetic-personal-token') as client:
        names={t['name'] for t in (await rpc(client,'tools/list',{}))['result']['tools']}
        assert 'search' in names and 'disagreement_contribute' in names
        assert not names & {'book_ingest','expert_research','expert_request_delivery'}
        found=tool_value(await rpc(client,'tools/call',{'name':'fetch','arguments':{'id':s.chunks['private']}}))
        assert 'PRIVATE_SENTINEL' in found['text']
        for name in ('book_ingest','expert_request_delivery','story_seed','document_access'):
            denied=await rpc(client,'tools/call',{'name':name,'arguments':{}})
            assert denied['result']['isError']
        child=await client.post('/expert/mcp',json={'jsonrpc':'2.0','id':1,'method':'tools/list','params':{}})
        assert child.status_code==404
    async with client_for(surface,q.executor) as client:
        denied=await rpc(client,'tools/call',{'name':'fetch','arguments':{'id':s.chunks['private']}})
        assert denied['result']['isError']
        denied=await rpc(client,'tools/call',{'name':'book_ingest','arguments':{}})
        assert denied['result']['isError']
    async with client_for(surface,'synthetic-owner-token') as client:
        tools=await rpc(client,'tools/list',{})
        assert 'book_ingest' in {t['name'] for t in tools['result']['tools']}
        rejected=await rpc(client,'tools/call',{'name':'expert_request_delivery','arguments':{}})
        assert rejected['result']['isError']
    await s.backend.aclose()


@pytest.mark.asyncio
async def test_personal_and_delegated_case_same_author_revoke_both(sources,surface):
    from regional_knowledge.disagreement_contracts import CaseCreate,CaseGrant,ContributionAdd
    from regional_knowledge.participant_contracts import ParticipantGrant
    s=sources;q=s.q
    case=q.requests.journal.create(q.admin,CaseCreate(question='Which date?',context='Synthetic shared case',classification='candidate',rationale='Compare sources',positions=[{'id':'a','text':'A','speaker':'Author','modality':'asserted'}]), 'shared-case-create')
    cid=case['case_id']
    q.requests.journal.grant(q.admin,cid,1,CaseGrant(grantee=q.actors[0].subject,capability='contributor',reason='test'),'shared-case-grant')
    accepted=q.requests.accept(observation())
    async with client_for(surface,q.executor) as client:
        lease=tool_value(await rpc(client,'tools/call',{'name':'expert_request_delivery','arguments':{'action':'claim','request_id':accepted['request_id']}}))
        bound={'request_id':lease['request_id'],'generation':lease['generation'],'execution_token':lease['execution_token']}
        delegated=tool_value(await rpc(client,'tools/call',{'name':'expert_research','arguments':{**bound,'operation':'case_get','case_id':cid}}))
    async with client_for(surface,'synthetic-personal-token') as client:
        direct=tool_value(await rpc(client,'tools/call',{'name':'disagreement_get','arguments':{'case_id':cid}}))
        assert direct==delegated
        q.change(q.actors[0].subject,ParticipantGrant(capability='case_read',granted=False,reason='test revoke both routes'))
        denied=tool_value(await rpc(client,'tools/call',{'name':'disagreement_get','arguments':{'case_id':cid}}))
        assert denied['error']['code']=='capability_denied'
    async with client_for(surface,q.executor) as client:
        denied=tool_value(await rpc(client,'tools/call',{'name':'expert_research','arguments':{**bound,'operation':'case_get','case_id':cid}}))
        assert denied['error']['code']=='capability_denied'
    await s.backend.aclose()


@pytest.mark.asyncio
async def test_events_discovery_subscribe_and_revoke_share_same_http_resource(sources,surface):
    import base64
    from regional_knowledge.expert_events import EVENT
    s=sources;q=s.q;verified=surface.verified
    params={'name':EVENT,'arguments':{},'delivery':{'mode':'webhook','url':'https://callback.example/rkb',
        'secret':'whsec_'+base64.b64encode(b'synthetic-test-http-event-secret-1234').decode()}}
    async with client_for(surface,q.executor) as client:
        client.headers['MCP-Protocol-Version']='2026-07-28'
        discovery=await rpc(client,'server/discover',{})
        assert 'result' in discovery,discovery
        assert discovery['result']['capabilities']['events']['delivery']=={'webhook':{}}
        catalog=await rpc(client,'events/list',{})
        assert catalog['result']['events'][0]['name']==EVENT
        subscribed=await rpc(client,'events/subscribe',params)
        assert subscribed['result']['refreshBefore'] and len(verified)==1
        invalid=await rpc(client,'events/subscribe',{**params,'arguments':{'actor_id':q.owner.subject}})
        assert invalid['error']['code']==-32602
        removed=await rpc(client,'events/unsubscribe',{**params,'delivery':{k:v for k,v in params['delivery'].items() if k!='secret'}})
        assert removed['result']['unsubscribed']
    async with client_for(surface,'synthetic-personal-token') as client:
        denied=await rpc(client,'events/subscribe',params)
        assert denied['error']['code']==-32024
    await s.backend.aclose()


@pytest.mark.asyncio
async def test_unified_guard_preserves_first_owner_bootstrap_but_never_reactivates(sources,surface):
    s=sources;q=s.q
    with q.corpus.connect() as db:
        db.execute("DELETE FROM corpus_rows WHERE table_name='rkb_users' AND row_key=?",(q.owner.subject,))
    async with client_for(surface,'synthetic-owner-token') as client:
        value=tool_value(await rpc(client,'tools/call',{'name':'profile','arguments':{}}))
        assert value['id']==q.owner.subject
        assert q.corpus.one('rkb_users',q.owner.subject)['status']=='active'
        q.corpus.put('rkb_users',[{'id':q.owner.subject,'status':'disabled'}])
        rejected=await rpc(client,'tools/call',{'name':'profile','arguments':{}})
        assert rejected['result']['isError']
        assert q.corpus.one('rkb_users',q.owner.subject)['status']=='disabled'
    await s.backend.aclose()
