"""Actual loopback ASGI receiver, canonical links, admission and HTTP issuance."""
import asyncio
from datetime import timedelta
import time
from uuid import uuid4

import httpx
import pytest
from starlette.applications import Starlette
from starlette.routing import Route

from regional_knowledge.participant_contracts import AccountBegin, AccountConfirm, AccountUnlink
from regional_knowledge.projects_hub_reader import ProjectsHubReader, PATH
from regional_knowledge.read_issuance import ReadIssuanceMiddleware
from test_participants import people, change
from test_read_authority import auth
from test_kaggle_public_backend import target, work, settle, release
from test_kaggle_service_startup import service

from regional_knowledge.service_auth import new_service_token


def linked(s):
    p=s.auth.p
    link=change(p,AccountBegin(provider='projects_hub',tenant='ph.fixture',provider_subject='usr_test',reason='verified synthetic fixture'))
    observed=p.registry.observe_account(link_id=link['link_id'],link_revision=1,provider='projects_hub',
        tenant='ph.fixture',provider_subject='usr_test',audience='rkb-account-binding',
        expires_at=p.registry._time()+timedelta(minutes=5),receipt_id='synthetic-trusted-link-observation')
    change(p,AccountConfirm(link_id=link['link_id'],expected_link_revision=1,method='provider_observation',
        observation_id=observed,reason='verified exact fixture'))
    return dict(actor_sub='usr_test',workspace_id='ws_test',link_id=link['link_id'],link_revision=2)


def setup(s,*,row=None,current=lambda:True):
    row=row or linked(s)
    ident,token=new_service_token()
    s.auth.provider.issue_service_credential(s.auth.p.admin,service_id='projects-hub-reader',token=token,
        scope=dict(operation='knowledge_search',provider='projects_hub',provider_tenant='ph.fixture',workspace_ids=[row['workspace_id']]),
        expires_at=s.auth.now[0]+86400,reason='synthetic service scope')
    reader=ProjectsHubReader(backend=s.backend,read_authority=s.auth.resolver,service_id='projects-hub-reader',
        credential_id=ident,credential_epoch=1,provider_tenant='ph.fixture',bindings=[row],current=current)
    app=ReadIssuanceMiddleware(Starlette(routes=[Route(PATH,reader.__call__,methods=['POST'])]))
    body=dict(codec='rkb.knowledge-query.v2',query='actual query',**row,provider_tenant='ph.fixture',
        request_id='actual-ph-request',turn_generation=7,min_data_seq=5,deadline_monotonic=time.monotonic()+2,max_evidence=3)
    headers={'Authorization':'Bearer '+token,'X-RKB-Service':'projects-hub-reader',
        'X-RKB-Service-Credential':ident,'X-RKB-Service-Epoch':'1'}
    return reader,app,body,headers


@pytest.mark.asyncio
async def test_actual_asgi_verified_human_link_same_queue_and_typed_http_final_issuance(target):
    s=target;reader,app,body,headers=setup(s)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app,client=('127.0.0.1',9001)),base_url='http://127.0.0.1') as client:
        request=asyncio.create_task(client.post(PATH,json=body,headers=headers))
        dispatched=await work(s)
        assert dispatched.query.actor.person_id==s.auth.p.other.subject
        assert dispatched.query.actor.person_id != body['actor_sub'] and dispatched.query.turn_generation==7
        assert dispatched.query.request_id!=body['request_id'] and dispatched.query.min_data_seq==5
        await settle(s,dispatched)
        response=await request
        value=response.json()
        assert value['request_id']==body['request_id']
        assert response.status_code==200 and value['codec']=='rkb.knowledge-search.v2'
        assert value['status']=='degraded' and value['mode']=='hybrid' and value['evidence'][0]['text']=='Evidence'
        assert value['observed_data_seq']==value['observed_projection_seq']==5 and value['min_data_seq']==5
        assert value['context_gap'] is False and 'result' not in value
        assert not s.auth.resolver._contexts and len(s.gate.released)==1
        await release(s,dispatched)


@pytest.mark.asyncio
@pytest.mark.parametrize('bad',['missing_link','revision','actor','tenant','remote','credential','public_header'])
async def test_no_service_credential_or_caller_projection_can_grant_human_rights(target,bad):
    s=target;row=linked(s)
    if bad=='missing_link':
        change(s.auth.p,AccountUnlink(link_id=row['link_id'],expected_link_revision=2,reason='revoked'))
    reader,app,body,headers=setup(s,row=row)
    client_host='127.0.0.1'
    if bad=='revision':body['link_revision']=1
    if bad=='actor':body['actor_sub']='usr_unknown'
    if bad=='tenant':body['provider_tenant']='ph.other'
    if bad=='remote':client_host='192.0.2.8'
    if bad=='credential':headers['Authorization']='Bearer '+s.auth.tokens.access_token
    if bad=='public_header':client_host='192.0.2.8';headers['X-Forwarded-For']='127.0.0.1'
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app,client=(client_host,9001)),base_url='http://127.0.0.1') as client:
        result=await client.post(PATH,json=body,headers=headers)
        assert result.status_code in (200,403) and not result.json().get('evidence')
        assert not s.gate.permits and not s.auth.resolver._contexts


@pytest.mark.asyncio
async def test_final_service_revocation_withholds_actual_http_evidence(target):
    s=target;current=[True];reader,app,body,headers=setup(s,current=lambda:current[0])
    original=s.gate.refresh
    async def refresh(gated):
        current[0]=False
        return await original(gated)
    s.gate.refresh=refresh
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app,client=('127.0.0.1',9001)),base_url='http://127.0.0.1') as client:
        task=asyncio.create_task(client.post(PATH,json=body,headers=headers));dispatched=await work(s)
        await settle(s,dispatched);value=(await task).json()
        assert value['evidence']==[] and value['mode'] is None and value['context_gap']
        assert value['reason']=='context_gap' and not s.auth.resolver._contexts
        await release(s,dispatched)


@pytest.mark.asyncio
async def test_deadline_failure_is_typed_and_no_query_dispatch(target):
    s=target;reader,app,body,headers=setup(s);body['deadline_monotonic']=time.monotonic()-1
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app,client=('127.0.0.1',9001)),base_url='http://127.0.0.1') as client:
        value=(await client.post(PATH,json=body,headers=headers)).json()
        assert value['status']=='unavailable' and value['reason']=='deadline_exceeded'
        assert value['mode'] is None and value['evidence']==[] and not s.gate.permits


@pytest.mark.asyncio
async def test_current_human_link_revoked_at_http_refresh_withholds_evidence(target):
    s=target;reader,app,body,headers=setup(s)
    original=s.gate.refresh
    async def refresh(gated):
        change(s.auth.p,AccountUnlink(link_id=body['link_id'],expected_link_revision=2,reason='revoke before issuance'))
        return await original(gated)
    s.gate.refresh=refresh
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app,client=('127.0.0.1',9001)),base_url='http://127.0.0.1') as client:
        task=asyncio.create_task(client.post(PATH,json=body,headers=headers));dispatched=await work(s)
        await settle(s,dispatched);value=(await task).json()
        assert value['evidence']==[] and value['mode'] is None and value['context_gap']
        assert not s.auth.resolver._contexts
        await release(s,dispatched)


@pytest.mark.asyncio
async def test_actual_asgi_disconnect_cancels_pending_read_and_releases_human_authority(target):
    import json
    s=target;reader,app,body,headers=setup(s);disconnected=asyncio.Event();received=[False];sent=[]
    async def receive():
        if not received[0]:
            received[0]=True
            return dict(type='http.request',body=json.dumps(body).encode(),more_body=False)
        await disconnected.wait()
        return dict(type='http.disconnect')
    async def send(message):sent.append(message)
    scope=dict(type='http',asgi={'version':'3.0'},method='POST',scheme='http',path=PATH,raw_path=PATH.encode(),
        query_string=b'',root_path='',server=('127.0.0.1',8199),client=('127.0.0.1',9001),
        headers=[(key.lower().encode(),value.encode()) for key,value in headers.items()])
    task=asyncio.create_task(app(scope,receive,send));dispatched=await work(s)
    disconnected.set()
    with pytest.raises(asyncio.CancelledError):await task
    assert not s.auth.resolver._contexts and not any(message['type']=='http.response.body' for message in sent)
    await release(s,dispatched)


@pytest.mark.asyncio
async def test_real_server_registers_receiver_only_when_explicit_and_for_same_target(service):
    from regional_knowledge.server import build_server
    startup=service.startup
    row=linked(service.target)
    ident,token=new_service_token()
    receiver=ProjectsHubReader(backend=startup.backend,read_authority=startup.authority,service_id='projects-hub-reader',
        credential_id=ident,credential_epoch=1,provider_tenant='ph.fixture',bindings=[row],current=lambda:True)
    absent=build_server(target_startup=startup).streamable_http_app(stateless_http=True,json_response=True)
    enabled=build_server(target_startup=startup,projects_hub_receiver=receiver).streamable_http_app(stateless_http=True,json_response=True)
    assert not any(getattr(route,'path',None)==PATH for route in absent.routes)
    assert any(getattr(route,'path',None)==PATH for route in enabled.routes)


@pytest.mark.asyncio
async def test_provisioned_service_epoch_revoke_is_checked_at_final_http_issuance(target):
    s=target;reader,app,body,headers=setup(s)
    original=s.gate.refresh
    async def refresh(gated):
        s.auth.provider.revoke_service_credential(s.auth.p.admin,reader.credential_id,expected_epoch=1,reason='revoke technical reader')
        return await original(gated)
    s.gate.refresh=refresh
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app,client=('127.0.0.1',9001)),base_url='http://127.0.0.1') as client:
        task=asyncio.create_task(client.post(PATH,json=body,headers=headers));dispatched=await work(s)
        await settle(s,dispatched);value=(await task).json()
        assert value['evidence']==[] and value['mode'] is None and value['context_gap']
        assert not s.auth.resolver._contexts
        await release(s,dispatched)


@pytest.mark.asyncio
async def test_receiver_private_descriptor_references_existing_credential_without_reading_secret(target,tmp_path):
    import json
    s=target;original,app,body,headers=setup(s)
    config=tmp_path/'reader.json'
    config.write_text(json.dumps(dict(codec='rkb.ph-reader.v2',service_id=original.service_id,
        service_credential_id=original.credential_id,service_credential_epoch=1,provider_tenant='ph.fixture',
        bindings=[{key:body[key] for key in ('actor_sub','workspace_id','link_id','link_revision')}])));config.chmod(0o600)
    reader=ProjectsHubReader.from_file(config,backend=s.backend)
    assert reader.credential_id==original.credential_id and reader.current()
    config.chmod(0o640)
    assert not reader.current()
    with pytest.raises(ValueError):ProjectsHubReader.from_file(config,backend=s.backend)


def test_p3_v2_codec_remains_exact_and_v3_selectors_fail_closed(target):
    import copy
    s=target
    reader,app,body,headers=setup(s)
    source='00000000-0000-0000-0000-000000000001'
    reader._query(body)  # old format still accepted with all original fields
    old_extra={**body,'document_ids':[source]}
    with pytest.raises(ValueError):reader._query(old_extra)
    v3={**body,'codec':'rkb.knowledge-query.v3','operation':'search'}
    reader._query(v3)
    scoped={**v3,'document_ids':[source]}
    reader._query(scoped)
    bad_values=[[],[source,source],[source]*6,['00000000-0000-0000-0000-00000000000A'],
        ['not-uuid'],[source,3],[[source]]]
    for ids in bad_values:
        with pytest.raises((ValueError,TypeError)):
            reader._query({**v3,'document_ids':ids})
    with pytest.raises(ValueError):
        reader._query({**scoped,'arbitrary':'ignore-me'})
    pages={key:value for key,value in body.items() if key!='query'}
    pages.update(codec='rkb.knowledge-query.v3',operation='fetch',
        source_id=source,page_start=3,page_end=6)
    reader._query(pages)
    for change in ({'page_end':7},{'page_start':-1},{'page_start':True},
                   {'source_id':'not-uuid'},{'id':'rkb:passage:v1:wrong'}):
        with pytest.raises((ValueError,TypeError)):
            reader._query({**pages,**change})
    with pytest.raises(ValueError):
        reader._query({**pages,'query':'unexpected'})


@pytest.mark.asyncio
async def test_p3_v3_document_scope_uses_existing_hybrid_source_filter(target):
    s=target;reader,app,body,headers=setup(s)
    source='00000000-0000-0000-0000-000000000001'
    body.update(codec='rkb.knowledge-query.v3',operation='search',document_ids=[source])
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app,client=('127.0.0.1',9001)),
                                  base_url='http://127.0.0.1') as client:
        read=asyncio.create_task(client.post(PATH,json=body,headers=headers))
        dispatched=await work(s)
        assert dispatched.query.filters.source_ids==(source,)
        await settle(s,dispatched)
        response=(await read).json()
        assert response['codec']=='rkb.knowledge-search.v2'
        assert response['status']=='degraded' and response['mode']=='hybrid'
        assert response['evidence'][0]['text']=='Evidence'
        assert not s.auth.resolver._contexts
        await release(s,dispatched)


@pytest.mark.asyncio
async def test_p3_v3_page_fetch_reads_same_accepted_chunks_and_current_grant(target):
    from regional_knowledge.kaggle_source_read import SourceReadRequest,SourceReadResult
    from regional_knowledge.kaggle_protocol import EvidenceItem,ItemDependencies,SourceDependency
    s=target;reader,app,body,headers=setup(s)
    source='00000000-0000-0000-0000-000000000001'
    chunk='60000000-0000-0000-0000-000000000001'
    page='40000000-0000-0000-0000-000000000001'
    region='50000000-0000-0000-0000-000000000001'
    body.pop('query')
    body.update(codec='rkb.knowledge-query.v3',operation='fetch',
        source_id=source,page_start=1,page_end=2,max_evidence=1)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app,client=('127.0.0.1',9001)),
                                  base_url='http://127.0.0.1') as client:
        read=asyncio.create_task(client.post(PATH,json=body,headers=headers))
        dispatched=await work(s)
        assert type(dispatched.query) is SourceReadRequest
        assert dispatched.query.operation=='accepted_chunks'
        assert (dispatched.query.source_id,dispatched.query.page_start,dispatched.query.page_end)==(source,1,2)
        dep=SourceDependency(source_id=source,content_revision=1,eligibility_revision=7)
        result=SourceReadResult(request_id=dispatched.query.request_id,
            connection_generation=s.generation,turn_generation=dispatched.query.turn_generation,
            operation='accepted_chunks',status='ok',records=(dict(source_id=source,chunk_id=chunk,
                ordinal=0,text='From the adjacent page',pages=[dict(page_id=page,physical_page_index=1)],
                regions=[dict(page_id=page,region_id=region,start=0,end=22,role='body')]),),
            evidence=(EvidenceItem(id=chunk,kind='source',text='From the adjacent page',
                dependencies=ItemDependencies(source_ids=(source,),variant_refs=(),grant_refs=(),
                    source_revisions=(dep,))),),
            observed_data_seq=5,observed_projection_seq=5,
            observed_visibility_epoch=4,observed_database_generation='db-source')
        assert await s.admission.finish_read(s.run,s.token,s.generation,result)
        value=(await read).json()
        assert value['status']=='ok' and value['mode']=='hybrid'
        assert value['evidence'][0]['text']=='From the adjacent page'
        assert value['evidence'][0]['metadata']['pages'][0]['physical_page_index']==1
        assert value['evidence'][0]['metadata']['source_id']==source
        assert not s.auth.resolver._contexts and len(s.gate.released)==1
        await release(s,dispatched)
