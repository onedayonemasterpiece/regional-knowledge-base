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

    async def source_tail_refs(self,binding,source_id,ingestion_id):
        assert self.binding_current(binding)
        if not self.available or self.revoked:raise OSError('synthetic native observation lost')
        return dict(tail_refs=[],has_more=False)


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
async def test_native_book_ingest_registered_description_requires_same_pass_entities(service):
    # Launcher readers see tool descriptions, not the separate server instructions.
    server = build_server(target_startup=service.startup)
    registered = {tool.name: tool for tool in await server.list_tools()}
    description = registered["book_ingest"].description
    for required in (
        "During the SAME page review, send a bounded book_ingest(stage).entity_candidates GraphBundle",
        "person, organization, event, historical_thread, and poi_ref",
        "places, toponyms, historical addresses",
        "exact_source_spelling",
        "page_id + region_key + chunk_key (or an already accepted chunk_id)",
        "An empty entity_candidates bundle is valid only when the reviewed page names no source-grounded entities.",
    ):
        assert required in description, required


@pytest.mark.asyncio
async def test_actual_startup_reuses_one_queue_and_closes_without_proof(service,monkeypatch):
    s=service;startup=s.startup
    monkeypatch.setattr('regional_knowledge.server.backend_from_env',lambda:pytest.fail('legacy fallback'))
    server=build_server(target_startup=startup)
    registered = {tool.name: tool for tool in await server.list_tools()}
    story_field = registered["book_ingest"].input_schema["properties"]["story_candidates"]
    story_rules = next(option["description"] for option in story_field["anyOf"] if option.get("type") == "array")
    assert server.instructions.count("Story rules: see book_ingest.story_candidates.") == 1
    assert "Story body: write" not in server.instructions
    for required in ("max 12 per stage", "attribute only opinion, estimate, legend",
                     "State supported facts directly, including painful events",
                     "ONE body, not separate facts_only",
                     "A lone dated fact, list or uses without development stays facts/observations",
                     "with no story quota", "first/invention with adjacent development"):
        assert required in story_rules, required
    assert ('During the same page review, record source-mentioned places in '
        'book_ingest(stage).entity_candidates as poi_ref entities with exact_source_spelling '
        'and source passage evidence. A new name for the same model-identified place '
        'is an alias, not a new entity.') in server.instructions
    assert ('Preserve source-supported German and other historical names in aliases; '
        'set language, alias_type (such as historical, former or current) and time_scope '
        'only when supported by the book or cited research_sources, '
        'leaving unsupported values unset.') in server.instructions
    assert ('Add a modern Russian name only when the book or cited research_sources '
        '(URL and note) supports that alias; keep external research separate '
        'from book evidence.') in server.instructions
    assert ('Review all source-mentioned place classes: streets, squares, quarters, '
        'buildings, churches, castles, fortifications, gates, bridges, islands, rivers, '
        'villages and districts of the region.') in server.instructions
    assert ('For lost or demolished sites, record the supported state in place_context; '
        'do not guess dates, coordinates or modern addresses.') in server.instructions
    assert 'site_state' not in server.instructions
    assert server._target_startup is startup
    assert startup.backend.worker_queue is s.target.queue is startup.config.spool.queue
    assert startup.admission is s.target.admission
    assert startup.authority is s.target.auth.resolver
    assert startup.routes['source.search'] is startup.history.routes['source.search']
    app=server.streamable_http_app(stateless_http=True,json_response=True)
    assert any(getattr(route,'path',None)=='/bge-worker/channel' for route in app.routes)
    async with startup.lifespan(server):
        assert not startup.reads_current()  # writer history remains closed
        from regional_knowledge.kaggle_public_backend import current_binding
        assert startup.binding_current(current_binding(s.target.queue))
        assert not startup.admission._reads  # ordinary read admission is lazy
    assert startup.closed and not startup.reads_current()
    assert startup.admission._fd is None and startup.config.spool._fd is None


@pytest.mark.asyncio
async def test_drain_keeps_only_checkpoint_authority_and_closes_public_work(service):
    s=service;startup=s.startup;s.adapter.available=True
    async with startup.lifespan(None):
        assert startup.reads_current()
        proof,binding=startup.coverage,startup.binding
        with s.target.queue.connect() as db:
            db.execute("UPDATE runs SET status='draining' WHERE id=?",(binding.run_id,))
        assert startup.checkpoint_binding_current(binding)
        assert not startup.binding_current(binding)
        assert not startup.reads_current() and not startup.imports_current()
        from regional_knowledge.kaggle_cases_backend import KaggleCasesBackend
        from regional_knowledge.kaggle_story_editorial_backend import KaggleStoryEditorial
        from regional_knowledge.kaggle_importer import KaggleSourceImporter
        from regional_knowledge.original_intake_controller import OriginalIntakeController,OriginalIntakeError
        guarded=SimpleNamespace(startup=startup)
        assert not KaggleCasesBackend._binding_current(guarded,binding)
        assert not KaggleStoryEditorial._binding_current(guarded,binding)
        assert not KaggleSourceImporter._binding_current(guarded,binding)
        # This production ticket method must reject before consulting storage.
        original=SimpleNamespace(binding_current=startup.binding_current)
        with pytest.raises(OriginalIntakeError,match='original_current_binding_required'):
            await OriginalIntakeController._ticket(original,binding,'operation','PUT','key',1,'0'*64)
        assert not await startup.refresh()
        assert startup.coverage is proof and not proof.current()
        with s.target.queue.connect() as db:
            db.execute("UPDATE runs SET status='expired' WHERE id=?",(binding.run_id,))
        assert not startup.checkpoint_binding_current(binding) and not proof.current()


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
    assert {tool.name for tool in live._tool_manager.list_tools()}=={'knowledge_search','startup_status','source_unit_export'}
    assert 'source_unit_export' in {tool.name for tool in full._tool_manager.list_tools()}
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
            with pytest.raises(ToolError,match='tool_not_supported_in_profile'):
                await full.call_tool(tool,{})


@pytest.mark.asyncio
async def test_mcp_ready_pg_and_native_page_read_survive_unavailable_history(service,monkeypatch):
    # Real server.call_tool -> OAuth -> shared queue -> native envelope -> ASGI
    # output capture. No history adapter proof is installed.
    s=service;startup=s.startup
    monkeypatch.setattr('regional_knowledge.server._principal',lambda:s.target.principal)
    live=build_server(target_startup=startup,profile='live')
    request=Request({'type':'http','method':'POST','path':'/mcp','headers':[],
        'app':SimpleNamespace(state=SimpleNamespace()),'state':{}})
    context=SimpleNamespace(request_context=SimpleNamespace(request=request,request_id='pg-without-history'))
    async with startup.lifespan(live):
        assert not startup.reads_current()
        task=asyncio.create_task(live.call_tool('knowledge_search',{'query':'harbour'},context))
        dispatched=await work(s.target)
        response=QueryResult(request_id=dispatched.query.request_id,
            connection_generation=s.target.generation,
            turn_generation=dispatched.query.turn_generation,status='ok',mode='hybrid',
            observed_data_seq=5,observed_projection_seq=5,
            observed_database_generation='synthetic-db',
            observed_visibility_epoch=0,evidence=(item(SOURCE),))
        assert await startup.admission.finish_read(
            s.target.run,s.target.token,s.target.generation,response)
        found=await task
        assert not found.is_error and found.structured_content['status']=='ok'
        assert found.structured_content['evidence']
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
    real_app=module.KnowledgeMCPServer.streamable_http_app
    def capture_app(instance,**options):
        app=real_app(instance,**options)
        seen.append((instance,options,app))
        return app
    launched=[]
    monkeypatch.setattr(module.KnowledgeMCPServer,'streamable_http_app',capture_app)
    monkeypatch.setattr('regional_knowledge.http_shutdown.run_http_server',lambda app,**options:launched.append((app,options)))
    module.main(target_startup=service.startup)
    assert len(seen)==len(launched)==1 and seen[0][0]._target_startup is service.startup
    assert launched[0][0] is seen[0][2]
    assert seen[0][1]['stateless_http'] and seen[0][1]['json_response']
    assert 'knowledge.example.test' in seen[0][1]['transport_security'].allowed_hosts
    assert launched[0][1]['timeout_graceful_shutdown']==10
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
        with pytest.raises(ToolError,match='tool_not_supported_in_profile'):
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
    s=service;startup=s.startup;s.adapter.available=True
    class Pending:
        calls=0
        def kick(self,binding):
            Pending.calls+=1
            return True  # Dataset publication continues outside maintenance
        async def aclose(self):pass
    async with startup.lifespan(None):
        assert startup.reads_current()
        startup.dataset_checkpoints=Pending()
        assert await startup.refresh() and startup.reads_current() and Pending.calls==1
        # An unexpected failure to *schedule* the single-flight checkpoint
        # still fails closed, rather than reporting healthy stale coverage.
        class Broken:
            def kick(self,binding):raise RuntimeError('checkpoint scheduler failed')
            async def aclose(self):pass
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
async def test_ordinary_startup_refresh_never_reconstructs_geo_manifest(service):
    s=service;startup=s.startup;s.adapter.available=True
    class Gate:
        _manifest=None
        async def establish(self,*args,**kwargs):
            raise AssertionError('ordinary read maintenance must not reconstruct GEO')
        def invalidate(self):pass
    async with startup.lifespan(None):
        startup.geo_gate=Gate()
        for _ in range(3):
            assert await startup.refresh() and startup.reads_current()
            startup.config.spool._source_covered_sequence+=1


@pytest.mark.asyncio
async def test_existing_planned_stop_waits_for_checkpoint_outcome_before_nine_hours(tmp_path):
    from regional_knowledge.bge_queue import BgeQueue
    now=[100.]
    queue=BgeQueue(tmp_path/'stop-checkpoint.sqlite',clock=lambda:now[0])
    with queue.connect() as db:
        run_id=queue._new_run(db,now[0])
    binding=SimpleNamespace(run_id=run_id,connection_generation=1)
    events=[]
    class Checkpoint:
        async def before_stop(self,binding):
            events.append(('checkpoint',binding.run_id))
            await asyncio.sleep(0)
            return True
    fake=SimpleNamespace(dataset_checkpoints=Checkpoint(),binding=binding,
        config=SimpleNamespace(queue=queue))
    assert await TargetServiceStartup.before_planned_stop(fake,run_id,1)
    assert events==[('checkpoint',run_id)]
    # Invalid attachment must never borrow another owner's stop checkpoint.
    with pytest.raises(RuntimeError,match='stop_binding_changed'):
        await TargetServiceStartup.before_planned_stop(fake,run_id,2)
    now[0]+=9*3600
    with pytest.raises(TimeoutError,match='nine_hour_limit'):
        await TargetServiceStartup.before_planned_stop(fake,run_id,1)
    assert events==[('checkpoint',run_id)]


@pytest.mark.asyncio
async def test_successor_stop_waits_previous_run_existing_checkpoint_not_new_writer(tmp_path):
    from regional_knowledge.bge_queue import BgeQueue
    queue=BgeQueue(tmp_path/'successor-stop.sqlite',clock=lambda:100.)
    with queue.connect() as db:
        previous=queue._new_run(db,100.)
    old=SimpleNamespace(run_id=previous,connection_generation=4)
    newer=SimpleNamespace(run_id='next-writer',connection_generation=5)
    observed=[]
    class OldPending:
        _task_binding=old
        _task=object()
        async def before_stop(self,binding):
            observed.append(binding)
            return True
    fake=SimpleNamespace(dataset_checkpoints=OldPending(),binding=newer,
        config=SimpleNamespace(queue=queue))
    assert await TargetServiceStartup.before_planned_stop(fake,previous,4)
    assert observed==[old]
    with pytest.raises(RuntimeError,match='stop_binding_changed'):
        await TargetServiceStartup.before_planned_stop(fake,previous,999)
