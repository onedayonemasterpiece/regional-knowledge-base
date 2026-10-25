"""Opt-in real PG, OAuth, shared admission, typed worker and actual MCP JSON.

Only BGE and object storage are synthetic. Root owns the disposable PG lifecycle.
"""
import asyncio
import json
from types import SimpleNamespace

import httpx
import pytest
from mcp.server.transport_security import TransportSecuritySettings

from regional_knowledge.kaggle_protocol import (
    PassageReadRequest, PassageReadResult, Query, QueryResult, decode_envelope,
)
from regional_knowledge.kaggle_public_backend import parse_passage_fetch_id
from regional_knowledge.contracts import FetchOutput, FetchChunksOutput
from regional_knowledge.server import KnowledgeMCPServer
from test_public_source_roundtrip import vertical, database, auth, people, A, B
from test_kaggle_query import PG, VECTOR

pytestmark = [PG,pytest.mark.asyncio]


async def call(s,operation,*,fetch_id=None,source_id=B,after_result=None):
    server=KnowledgeMCPServer('full-source-search-fetch')
    server._current_read_output_required=True
    principal=SimpleNamespace(access_token=s.auth.tokens.access_token)
    @server.tool()
    async def search() -> dict:
        return (await s.backend.search('castle',principal,document_ids=[source_id])).model_dump(mode='json')
    @server.tool()
    async def fetch(id:str) -> FetchOutput | FetchChunksOutput:
        return await s.backend.fetch(id,principal)
    app=server.streamable_http_app(stateless_http=True,json_response=True,
        transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False))
    async def boundary(scope,receive,send):
        async def hooked_send(message):
            if message['type']=='http.response.start' and after_result:
                await after_result(s)
            await send(message)
        await app(scope,receive,hooked_send)
    forwards=[]
    async def native_once():
        for _ in range(300):
            work=await s.queue.next_channel_work(s.run,s.token,s.generation)
            if work is not None:break
            await s.queue.wait_channel_work(.01)
        else:
            pytest.fail('public read never reached the shared slot')
        expected=PassageReadRequest if operation=='fetch' else Query
        assert type(work.query) is expected and work.kind=='read'
        s.admission.mark_work_sent(work)
        async def encode(text):
            assert operation=='search','exact fetch must not reach BGE'
            forwards.append(text)
            return VECTOR
        result=decode_envelope(json.dumps(await s.worker(work.query.model_dump(mode='json'),encode=encode)).encode())
        assert type(result) is (PassageReadResult if operation=='fetch' else QueryResult)
        assert result.status in ('ok','degraded')
        assert await s.admission.finish_read(s.run,s.token,s.generation,result)
        resource=work.resource
        await s.admission.resource_released(s.run,s.token,s.generation,
            runtime_incarnation=resource.runtime_incarnation,resource_kind=resource.resource_kind,
            resource_id=resource.resource_id,compute_claim=resource.compute_claim,
            dispatch_generation=resource.dispatch_generation)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=boundary),base_url='http://localhost') as client:
            worker=asyncio.create_task(native_once())
            try:
                response=await client.post('/mcp',json={'jsonrpc':'2.0','id':29,'method':'tools/call',
                    'params':{'name':operation,'arguments':{'id':fetch_id} if operation=='fetch' else {}}},
                    headers={'accept':'application/json, text/event-stream'})
                await asyncio.wait_for(worker,3)
            finally:
                if not worker.done():
                    worker.cancel()
                    await asyncio.gather(worker,return_exceptions=True)
    assert response.status_code==200 and response.json()['id']==29
    assert not s.gate._tickets and not s.gate._permits
    return response.json()['result'],forwards


async def test_full_profile_search_fetch_native_passage_and_bibliography(vertical):
    result,forwards=await call(vertical,'search')
    assert not result['isError'] and forwards==['castle']
    payload=result['structuredContent']
    assert payload['mode']=='hybrid'
    fetch_id=payload['results'][0]['id']
    locator=parse_passage_fetch_id(fetch_id)
    assert locator.source.source_id==B and locator.text_offset==locator.bibliography_offset==0
    result,forwards=await call(vertical,'fetch',fetch_id=fetch_id)
    assert not result['isError'] and forwards==[]
    assert list(result['structuredContent']) == ['result']
    payload=result['structuredContent']['result']
    assert payload['text']=='safe-source-b castle' and payload['status']=='ok'
    assert payload['metadata']['citation']=='safe bibliography B'
    assert payload['metadata']['continuation_complete'] and payload['metadata']['next_id'] is None
    assert 'mode' not in payload and 'retrieval_mode' not in payload


async def test_exact_fetch_final_json_rechecks_withdrawal_and_oauth(vertical):
    s=vertical
    initial,_=await call(s,'search',source_id=A)
    fetch_id=initial['structuredContent']['results'][0]['id']
    async def after_result(s):
        loaded=await s.auth.provider.load_access_token(s.auth.tokens.access_token)
        await s.auth.provider.revoke_token(loaded)
    result,forwards=await call(s,'fetch',fetch_id=fetch_id,after_result=after_result)
    assert forwards==[] and result['isError']
    assert 'withdrawn-source-a' not in json.dumps(result)
    assert 'private withdrawn bibliography A' not in json.dumps(result)
