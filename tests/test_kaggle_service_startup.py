"""Real OAuth/queue/server startup; only native history observations are synthetic."""
import asyncio
from dataclasses import replace
from types import SimpleNamespace

import pytest
import pytest_asyncio
from mcp.server.mcpserver.exceptions import ToolError
from starlette.requests import Request

from regional_knowledge.control_spool import ControlSpool
from regional_knowledge.kaggle_apply import HandlerRegistry
from regional_knowledge.kaggle_protocol import QueryResult
from regional_knowledge.kaggle_service_startup import TargetServiceConfig,TargetServiceStartup
from regional_knowledge.read_issuance import capture_read_output
from regional_knowledge.recovery_journal import RecoveryJournal
from regional_knowledge.server import build_server
from test_control_spool import handler
from test_kaggle_public_backend import target,work,release
from test_read_authority import auth
from test_participants import people
from test_recovery_journal import ConditionalMemoryStore
from test_source_read_gate import item, SOURCE


class HistoryAdapter:
    def __init__(self):self.available=False;self.calls=[];self.revoked=False
    def bind(self,*,spool,routes,binding_current,writer_generation):
        self.spool,self.routes,self.binding_current=spool,routes,binding_current
        self.coverage_seconds=30
        assert writer_generation=='writer-1'
        return self
    async def restore(self,binding):
        self.calls.append(('restore',binding))
        if not self.available:raise OSError('synthetic native history observation unavailable')
        self.binding=binding
        self.proof=SimpleNamespace(database_generation=binding.database_generation,
            current=lambda:not self.revoked and self.binding_current(binding))
        self.spool._source_anchor=self.proof
        return self.proof
    async def extend(self,binding):
        assert binding is self.binding
        self.calls.append(('extend',binding))
        if not self.available:raise OSError('synthetic native observation lost')
        return self.proof


@pytest_asyncio.fixture
async def service(target):
    s=target
    spool=ControlSpool(s.queue,s.auth.p.registry,
        RecoveryJournal(ConditionalMemoryStore(),stream='startup-synthetic'),HandlerRegistry([handler()]))
    adapter=HistoryAdapter();writer=[True]
    config=TargetServiceConfig(s.auth.provider,s.auth.p.registry,s.queue,spool,adapter,'writer-1',
        lambda **binding:writer[0] and binding['database_generation']=='synthetic-db')
    startup=TargetServiceStartup(config)
    yield SimpleNamespace(startup=startup,config=config,adapter=adapter,writer=writer,target=s)
    if not startup.closed:
        await startup.source_dispatch.aclose()
        s.admission.close();spool.close()


def test_env_target_requires_typed_configuration_before_legacy_selection(monkeypatch):
    monkeypatch.setenv('RKB_BACKEND_PROFILE','kaggle_source')
    monkeypatch.setenv('RKB_DEV_NOAUTH','1')
    def legacy():raise AssertionError('legacy backend must never be constructed')
    monkeypatch.setattr('regional_knowledge.server.backend_from_env',legacy)
    with pytest.raises(RuntimeError,match='explicit typed'):
        build_server()
    with pytest.raises(RuntimeError,match='explicit typed'):
        build_server(target_startup={'ready':True})
    monkeypatch.setenv('RKB_BACKEND_PROFILE','unknown')
    with pytest.raises(RuntimeError,match='RKB_BACKEND_PROFILE'):
        build_server()


@pytest.mark.asyncio
async def test_actual_startup_reuses_one_queue_and_closes_without_proof(service,monkeypatch):
    s=service;startup=s.startup
    monkeypatch.setattr('regional_knowledge.server.backend_from_env',lambda:pytest.fail('legacy fallback'))
    server=build_server(target_startup=startup)
    assert server._target_startup is startup
    assert startup.backend.worker_queue is s.target.queue is startup.config.spool.queue
    assert startup.admission is s.target.admission
    assert startup.authority is s.target.auth.resolver
    assert startup.routes['source.search'] is startup.history.routes['source.search']
    app=server.streamable_http_app(stateless_http=True,json_response=True)
    assert any(getattr(route,'path',None)=='/bge-worker/channel' for route in app.routes)
    async with startup.lifespan(server):
        assert not startup.reads_current()
        with pytest.raises(ToolError,match='source_history_unavailable'):
            await startup.backend.search('synthetic',s.target.principal)
        assert not startup.admission._reads
    assert startup.closed and not startup.reads_current()
    assert startup.admission._fd is None and startup.config.spool._fd is None


@pytest.mark.asyncio
async def test_proof_refresh_failure_and_writer_change_close_routing(service):
    s=service;startup=s.startup
    s.adapter.available=True
    async with startup.lifespan(None):
        assert startup.reads_current()
        original=startup.binding
        assert await startup.refresh()
        assert startup.binding is original and s.adapter.calls[-1]==('extend',original)
        s.adapter.available=False
        assert not await startup.refresh() and not startup.reads_current()
        s.adapter.available=True
        assert await startup.refresh() and startup.reads_current()
        s.writer[0]=False
        assert not startup.reads_current()
        assert not await startup.refresh()


@pytest.mark.asyncio
async def test_full_and_live_source_calls_share_actual_admission_and_no_domain_fallback(service,monkeypatch):
    s=service;startup=s.startup;s.adapter.available=True
    monkeypatch.setattr('regional_knowledge.server._principal',lambda:s.target.principal)
    full=build_server(target_startup=startup)
    live=build_server(target_startup=startup,profile='live')
    assert {tool.name for tool in live._tool_manager.list_tools()}=={'knowledge_search'}
    async with startup.lifespan(full):
        for server,name in ((full,'search'),(live,'knowledge_search')):
            request=Request({'type':'http','method':'POST','path':'/mcp','headers':[],
                'app':SimpleNamespace(state=SimpleNamespace()),'state':{}})
            context=SimpleNamespace(request_context=SimpleNamespace(request=request,request_id='startup-fixture'))
            task=asyncio.create_task(server.call_tool(name,{'query':'synthetic harbour'},context))
            dispatched=await work(s.target)
            assert dispatched.query.actor.person_id==s.target.auth.p.other.subject
            assert dispatched.query.filters.kinds==('source',)
            result=QueryResult(request_id=dispatched.query.request_id,connection_generation=s.target.generation,
                turn_generation=dispatched.query.turn_generation,status='ok',mode='hybrid',
                observed_data_seq=5,observed_projection_seq=5,observed_database_generation='synthetic-db',
                observed_visibility_epoch=0,evidence=(item(SOURCE),))
            assert await startup.admission.finish_read(s.target.run,s.target.token,s.target.generation,result)
            output=await task
            assert 'synthetic evidence' in str(output) if name=='knowledge_search' else 'passage' in str(output)
            from regional_knowledge.read_issuance import _SCOPE_KEY
            request.scope[_SCOPE_KEY][1].release()
            await release(s.target,dispatched)
        for tool in ('book_find','graph_fetch','illustration_fetch','catalog','document_access'):
            with pytest.raises(ToolError,match='target_domain_unavailable'):
                await full.call_tool(tool,{})


@pytest.mark.asyncio
async def test_explicit_main_selects_target_without_env_loader(service,monkeypatch):
    import regional_knowledge.server as module
    seen=[]
    monkeypatch.setenv('RKB_BACKEND_PROFILE','kaggle_source')
    monkeypatch.setenv('RKB_MCP_PROFILE','live')
    monkeypatch.setattr(module,'backend_from_env',lambda:pytest.fail('legacy fallback'))
    monkeypatch.setattr(module.KnowledgeMCPServer,'run',lambda self,**kwargs:seen.append((self,kwargs)))
    module.main(target_startup=service.startup)
    assert len(seen)==1 and seen[0][0]._target_startup is service.startup
    assert seen[0][1]['stateless_http'] and seen[0][1]['json_response']
    assert 'knowledge.example.test' in seen[0][1]['transport_security'].allowed_hosts
    with pytest.raises(RuntimeError,match='legacy backend profile'):
        build_server(target_startup=service.startup,backend_profile='legacy')
    with pytest.raises(RuntimeError,match='issuer/resource'):
        build_server(target_startup=service.startup,issuer='https://wrong.invalid')
    with pytest.raises(TypeError,match='canonical OAuth'):
        replace(service.config,participants=people)
