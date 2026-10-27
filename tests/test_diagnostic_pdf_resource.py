"""Real in-process MCP client/server; synthetic OAuth/SQLite, no PDF download."""
from contextlib import asynccontextmanager
import json

import anyio
from mcp.client.session import ClientSession
from mcp.shared.memory import create_client_server_memory_streams
import pytest

from regional_knowledge.contracts import FetchOutput, Principal
from regional_knowledge.diagnostic_pdf_resource import FIXTURE_ID, FIXTURE_URL
from regional_knowledge.participant_contracts import ParticipantGrant
from regional_knowledge.server import build_server
from test_kaggle_native_source_importer import native_intake
from test_kaggle_public_backend import target
from test_read_authority import auth
from test_participants import people, change


@asynccontextmanager
async def client(server):
    lowlevel=server._lowlevel_server
    # The fixture already owns a running synthetic target. Exercise the real
    # MCP protocol/validation without starting its service lifespan a second time.
    @asynccontextmanager
    async def running_fixture(_):yield {}
    lowlevel.lifespan=running_fixture
    async with create_client_server_memory_streams() as (client_streams,server_streams):
        async with anyio.create_task_group() as tasks:
            tasks.start_soon(lowlevel.run,*server_streams,lowlevel.create_initialization_options())
            async with ClientSession(*client_streams) as session:
                await session.initialize()
                yield session
            tasks.cancel_scope.cancel()


def caller(s):
    # A legitimate non-owner has the same ordinary public_search access.
    return Principal(subject=s.t.auth.p.other.subject,
        client_id=s.t.auth.provider.client.client_id,issuer=s.t.auth.provider.issuer,
        access_token=s.t.auth.tokens.access_token)


@pytest.mark.asyncio
async def test_real_mcp_resource_link_survives_fetch_output_validation_and_serialization(native_intake,monkeypatch,caplog):
    s=native_intake
    monkeypatch.setenv('RKB_DIAGNOSTIC_PDF_RESOURCE_LINK','1')
    caplog.set_level('INFO',logger='regional_knowledge.diagnostic_pdf_resource')
    monkeypatch.setattr('regional_knowledge.server._principal',lambda:caller(s))
    async def unexpected(*args,**kwargs):pytest.fail('synthetic resource reached corpus fetch')
    monkeypatch.setattr(s.startup.backend,'fetch',unexpected)
    server=build_server(target_startup=s.startup)
    with s.t.queue.connect(write=False) as db:
        before_jobs=db.execute('SELECT count(*) FROM jobs').fetchone()[0]
    async with client(server) as session:
        tools={tool.name:tool for tool in (await session.list_tools()).tools}
        assert 'result' in tools['fetch'].output_schema['properties']
        result=await session.call_tool('fetch',{'id':FIXTURE_ID})
    wire=json.loads(result.model_dump_json(by_alias=True))
    assert not wire.get('isError',False)
    link=wire['content'][0]
    assert {k:link[k] for k in ('type','uri','name','mimeType','size')}==dict(
        type='resource_link',uri=FIXTURE_URL,name='synthetic-diagnostic.pdf',
        mimeType='application/pdf',size=13264)
    structured=FetchOutput.model_validate(wire['structuredContent']['result'])
    assert structured.id==FIXTURE_ID and structured.url==FIXTURE_URL
    assert structured.text=='Diagnostic synthetic PDF resource; not source evidence.'
    assert len(wire['content'])==1
    assert [r.message for r in caplog.records if r.name=='regional_knowledge.diagnostic_pdf_resource']==[
        'diagnostic_pdf_resource state=issued purpose=synthetic_transport']
    with s.t.queue.connect(write=False) as db:
        assert db.execute('SELECT count(*) FROM jobs').fetchone()[0]==before_jobs


@pytest.mark.asyncio
@pytest.mark.parametrize('flag,identifier',[(None,FIXTURE_ID),('true',FIXTURE_ID),('1',FIXTURE_ID+'-other')])
async def test_disabled_and_other_ids_keep_ordinary_fetch(native_intake,monkeypatch,flag,identifier):
    s=native_intake;seen=[]
    if flag is None:monkeypatch.delenv('RKB_DIAGNOSTIC_PDF_RESOURCE_LINK',raising=False)
    else:monkeypatch.setenv('RKB_DIAGNOSTIC_PDF_RESOURCE_LINK',flag)
    monkeypatch.setattr('regional_knowledge.server._principal',lambda:s.principal)
    async def ordinary(id,principal):
        seen.append((id,principal))
        return FetchOutput(id=id,title='Ordinary result',text='Ordinary backend response',url='https://knowledge.example.test/')
    monkeypatch.setattr(s.startup.backend,'fetch',ordinary)
    async with client(build_server(target_startup=s.startup)) as session:
        result=await session.call_tool('fetch',{'id':identifier})
    assert seen==[(identifier,s.principal)]
    assert all(item.type!='resource_link' for item in result.content)
    assert result.structured_content['result']['text']=='Ordinary backend response'


@pytest.mark.asyncio
@pytest.mark.parametrize('failure',['invalid_bearer','no_capability','revoked_before_issuance'])
async def test_fixture_retains_current_ordinary_fetch_authority(native_intake,monkeypatch,caplog,failure):
    s=native_intake;principal=caller(s)
    monkeypatch.setenv('RKB_DIAGNOSTIC_PDF_RESOURCE_LINK','1')
    caplog.set_level('INFO',logger='regional_knowledge.diagnostic_pdf_resource')
    if failure=='invalid_bearer':principal=principal.model_copy(update={'access_token':'not-a-real-bearer'})
    monkeypatch.setattr('regional_knowledge.server._principal',lambda:principal)
    def revoke():
        change(s.t.auth.p,ParticipantGrant(capability='public_search',granted=False,reason='Synthetic diagnostic test revoke'))
    if failure=='no_capability':revoke()
    elif failure=='revoked_before_issuance':
        original=s.startup.authority.require
        async def require(verified,capability):
            result=await original(verified,capability);revoke();return result
        monkeypatch.setattr(s.startup.authority,'require',require)
    async with client(build_server(target_startup=s.startup)) as session:
        result=await session.call_tool('fetch',{'id':FIXTURE_ID})
    assert result.is_error
    assert all(item.type!='resource_link' for item in result.content)
    assert [r.message for r in caplog.records if r.name=='regional_knowledge.diagnostic_pdf_resource']==[
        'diagnostic_pdf_resource state=denied purpose=synthetic_transport']
