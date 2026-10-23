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


def test_ordinary_import_requires_current_source_history_not_geo_proof():
    binding=object();keys=('geo-install:0','geo-install:1','geo-install:2');current=[False]
    seen=[]
    def installation(actual,operations):
        seen.append((actual,operations));return current[0]
    startup=SimpleNamespace(reads_current=lambda:True,gate=SimpleNamespace(graph_supported=lambda:True),
        binding=binding,config=SimpleNamespace(geo_manifest_operation_keys=keys),geo_gate=None)
    assert TargetServiceStartup.imports_current(startup) and startup.reads_current()
    startup.geo_gate=SimpleNamespace(installation_current=installation)
    assert TargetServiceStartup.imports_current(startup) and not seen
    current[0]=True
    assert TargetServiceStartup.imports_current(startup) and not seen
    startup.reads_current=lambda:False
    assert not TargetServiceStartup.imports_current(startup)


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
        pending=await startup.backend.search('synthetic',s.target.principal)
        assert pending.status=='pending' and pending.startup.protocol=='rkb.startup.v1'
        assert pending.results==[] and pending.main_state=='starting'
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
async def test_schema_promotion_rebuilds_retired_prefix_without_failed_delta_tick(service,monkeypatch):
    s=service;startup=s.startup;s.adapter.available=True
    async with startup.lifespan(None):
        assert startup.reads_current()
        binding=startup.binding
        # promote_schema_context retires the old-schema native prefix while
        # the verified source sequence and current attachment remain the same.
        s.adapter._native_prefix=None
        async def invalid_delta(binding):
            raise RuntimeError('retained exact native source prefix unavailable')
        monkeypatch.setattr(s.adapter,'extend',invalid_delta)
        assert await startup.refresh()
        assert startup.binding is binding and startup.reads_current()
        assert s.adapter.calls[-1]==('restore',binding)


@pytest.mark.asyncio
async def test_full_and_live_source_calls_share_actual_admission_and_no_domain_fallback(service,monkeypatch):
    s=service;startup=s.startup;s.adapter.available=True
    monkeypatch.setattr('regional_knowledge.server._principal',lambda:s.target.principal)
    full=build_server(target_startup=startup)
    live=build_server(target_startup=startup,profile='live')
    assert {tool.name for tool in live._tool_manager.list_tools()}=={'knowledge_search','startup_status'}
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
async def test_mcp_cold_pending_status_and_new_live_read_after_ready(service,monkeypatch):
    """MCP client gets a normal typed pending response, then polls and retries."""
    import json
    s=service;startup=s.startup
    monkeypatch.setattr('regional_knowledge.server._principal',lambda:s.target.principal)
    live=build_server(target_startup=startup,profile='live')
    request=Request({'type':'http','method':'POST','path':'/mcp','headers':[],
        'app':SimpleNamespace(state=SimpleNamespace()),'state':{}})
    context=SimpleNamespace(request_context=SimpleNamespace(request=request,request_id='pending-mcp-fixture'))
    async with startup.lifespan(live):
        cold=await live.call_tool('knowledge_search',{'query':'harbour'},context)
        assert not cold.is_error
        assert cold.structured_content['status']=='pending'
        assert cold.structured_content['evidence']==[]
        operation=cold.structured_content['startup']
        assert operation['protocol']=='rkb.startup.v1'
        ref=operation['startup_ref']
        first=await live.call_tool('startup_status',{'startup_ref':ref},context)
        assert not first.is_error and first.structured_content['status']=='pending'
        s.adapter.available=True
        assert await startup.refresh() and startup.reads_current()
        second=await live.call_tool('startup_status',{'startup_ref':ref},context)
        assert not second.is_error and second.structured_content['status']=='ready'

        async def search_again():
            return await live.call_tool('knowledge_search',{'query':'harbour'},context)
        task=asyncio.create_task(search_again())
        dispatched=await work(s.target)
        result=QueryResult(request_id=dispatched.query.request_id,
            connection_generation=s.target.generation,
            turn_generation=dispatched.query.turn_generation,status='ok',mode='hybrid',
            observed_data_seq=5,observed_projection_seq=5,
            observed_database_generation='synthetic-db',
            observed_visibility_epoch=0,evidence=(item(SOURCE),))
        assert await startup.admission.finish_read(
            s.target.run,s.target.token,s.target.generation,result)
        fresh=await task
        assert not fresh.is_error and fresh.structured_content['status']=='ok'
        assert fresh.structured_content['evidence']
        from regional_knowledge.read_issuance import _SCOPE_KEY
        request.scope[_SCOPE_KEY][1].release()
        request.scope.pop(_SCOPE_KEY,None)
        await release(s.target,dispatched)


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

@pytest.mark.asyncio
async def test_native_story_tools_require_installed_v2_and_share_current_source_output_boundary(target,monkeypatch):
    from regional_knowledge.source_import_v2 import STORY_REVISIONS,source_import_v2_handler
    from regional_knowledge.kaggle_protocol import StoryReadRequest,StoryReadResult
    from test_kaggle_story_read import record,STORY
    from regional_knowledge.kaggle_story_read import story_item
    s=target
    registry=HandlerRegistry([handler(),source_import_v2_handler(revisions=STORY_REVISIONS,
        check_sha256='a'*64,apply_sha256='b'*64)])
    spool=ControlSpool(s.queue,s.auth.p.registry,RecoveryJournal(ConditionalMemoryStore(),stream='native-story-mcp'),registry)
    adapter=HistoryAdapter();adapter.available=True
    config=TargetServiceConfig(s.auth.provider,s.auth.p.registry,s.queue,spool,adapter,'writer-1',
        lambda **binding:binding['database_generation']=='synthetic-db')
    startup=TargetServiceStartup(config)
    monkeypatch.setattr('regional_knowledge.server._principal',lambda:s.principal)
    server=build_server(target_startup=startup)
    assert {'story_search','story_get','story_history','story_variants'}<={tool.name for tool in server._tool_manager.list_tools()}
    assert set(startup.routes)=={'source.search','source.fetch','source.import','source.withdraw',
        'story.search','story.get','story.history','story.variants'}
    async with startup.lifespan(server):
        for name,view in (('story_get','get'),('story_history','history'),('story_variants','variants')):
            request=Request({'type':'http','method':'POST','path':'/mcp','headers':[],
                'app':SimpleNamespace(state=SimpleNamespace()),'state':{}})
            context=SimpleNamespace(request_context=SimpleNamespace(request=request,request_id='native-story'))
            task=asyncio.create_task(server.call_tool(name,{'story_id':STORY},context))
            dispatched=await work(s);assert type(dispatched.query) is StoryReadRequest
            row=record(view=view)
            result=StoryReadResult(request_id=dispatched.query.request_id,connection_generation=s.generation,
                turn_generation=dispatched.query.turn_generation,status='ok',evidence=(story_item(row),),record=row,
                observed_data_seq=5,observed_projection_seq=5,observed_database_generation='synthetic-db',
                observed_visibility_epoch=0)
            assert await startup.admission.finish_read(s.run,s.token,s.generation,result)
            output=await task
            assert row['body'] in str(output) and row['body_sha256'] in str(output)
            from regional_knowledge.read_issuance import _SCOPE_KEY
            call=request.scope[_SCOPE_KEY][1]
            assert call.gate is startup.gate
            assert call.gated.result.record['default_variant_ref']==row['default_variant_ref']
            call.release();await release(s,dispatched)
        with pytest.raises(ToolError,match='target_domain_unavailable'):
            await server.call_tool('story_operation',{})


@pytest.mark.parametrize('keys',[('single',),['not-tuple'],('same','same'),('valid','x'*257)])
def test_geo_optin_rejects_unbounded_or_ambiguous_operation_key_configuration(service,keys):
    with pytest.raises(ValueError,match='GEO manifest operation keys'):
        replace(service.config,geo_manifest_operation_keys=keys)


def test_geo_optin_cannot_fall_back_to_non_geo_runtime(service):
    with pytest.raises(ValueError,match='fresh11 GEO handler'):
        TargetServiceStartup(replace(service.config,geo_manifest_operation_keys=('open','activate')))


@pytest.mark.asyncio
async def test_normal_maintenance_reconciles_publication_before_native_history(service):
    s=service;startup=s.startup;s.adapter.available=True
    events=[];unknown=[True];original=startup.history.restore
    async def ensure(journal):
        assert journal is s.config.spool.journal;events.append('ensure')
    async def reconcile(store):
        assert store is s.config.spool.journal.store;events.append('reconcile');unknown[0]=False
    async def history(binding):
        events.append('history')
        if unknown[0]:raise OSError('synthetic lost publication ACK remains unresolved')
        return await original(binding)
    # A spy on the existing owner seam; the fixture's queue/read authority and
    # native-history behavior remain the same. No runtime authority is minted.
    object.__setattr__(s.config,'writer_owner',SimpleNamespace(run_id='owned-fixture-run',ensure_current=ensure,reconcile=reconcile))
    with s.config.queue.connect() as db:
        db.execute('create table rkb_writer_control(run_id text,writer_generation text,publication_json text)')
        db.execute('insert into rkb_writer_control values(?,?,?)',('owned-fixture-run','writer-1','unknown-native-ack'))
    startup.history.restore=history;startup.running=True
    try:
        assert await startup.refresh()
        assert events==['reconcile','ensure','history'] and startup.reads_current()
    finally:startup.running=False


@pytest.mark.asyncio
async def test_pending_dataset_checkpoint_keeps_verified_reads_open(service):
    """A checkpoint stopped before promotion is retried; it does not close reads."""
    from regional_knowledge.kaggle_dataset_checkpoint import CheckpointPending
    s=service;startup=s.startup;s.adapter.available=True
    class Pending:
        calls=0
        async def refresh(self,binding):
            Pending.calls+=1
            raise CheckpointPending('JournalConflict','dataset_native_snapshot_readback_changed')
    async with startup.lifespan(None):
        assert startup.reads_current()
        startup.dataset_checkpoints=Pending()
        assert await startup.refresh() and startup.reads_current() and Pending.calls==1
        # Any other failure keeps the previous fail-closed behaviour.
        class Broken:
            async def refresh(self,binding):raise RuntimeError('post-promotion settle failed')
        startup.dataset_checkpoints=Broken()
        assert not await startup.refresh() and not startup.reads_current()


@pytest.mark.asyncio
async def test_idle_unchanged_prefix_skips_redundant_extend_until_margin(service):
    s=service;startup=s.startup;s.adapter.available=True
    now=[100.]
    async with startup.lifespan(None):
        assert startup.reads_current()
        spool=startup.config.spool;proof=s.adapter.proof
        proof.verifier=SimpleNamespace(clock=lambda:now[0],timeout_seconds=10)
        proof.deadline=now[0]+30;proof.pending=()
        proof.accepted_sequence=proof.applied_sequence=spool._source_covered_sequence
        calls=len(s.adapter.calls)
        assert await startup.refresh() and startup.reads_current()
        assert len(s.adapter.calls)==calls  # unchanged prefix, ample deadline
        now[0]+=15  # inside 2*refresh+timeout of the deadline
        assert await startup.refresh() and s.adapter.calls[-1][0]=='extend'
        now[0]=100.;calls=len(s.adapter.calls)
        proof.accepted_sequence+=1  # new accepted command not yet applied
        assert await startup.refresh() and len(s.adapter.calls)==calls+1


@pytest.mark.asyncio
async def test_unchanged_geo_denial_is_not_retried_every_tick(service,monkeypatch):
    import regional_knowledge.kaggle_service_startup as module
    s=service;startup=s.startup;s.adapter.available=True
    class Gate:
        _manifest=None;calls=0
        async def establish(self,keys,*,binding):
            Gate.calls+=1;raise RuntimeError('synthetic GEO outcome not durable')
        def invalidate(self):pass
    clock=[1000.];monkeypatch.setattr(module.time,'monotonic',lambda:clock[0])
    async with startup.lifespan(None):
        startup.geo_gate=Gate()
        assert await startup.refresh() and Gate.calls==1
        assert await startup.refresh() and Gate.calls==1  # same inputs: no spool-locked re-read
        clock[0]+=module.GEO_RETRY_SECONDS
        assert await startup.refresh() and Gate.calls==2  # bounded periodic retry
        startup.config.spool._source_covered_sequence+=1
        assert await startup.refresh() and Gate.calls==3  # new spool sequence retries at once
        assert startup.reads_current()
