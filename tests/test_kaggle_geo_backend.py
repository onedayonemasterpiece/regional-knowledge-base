"""GEO output uses actual shared admission, OAuth and the final MCP boundary."""
import asyncio
from dataclasses import replace
from types import SimpleNamespace

import pytest
from mcp.server.mcpserver.exceptions import ToolError

from regional_knowledge.control_spool import ControlSpool
from regional_knowledge.kaggle_apply import HandlerRegistry
from regional_knowledge.kaggle_geo_backend import KaggleGeoBackend
from regional_knowledge.kaggle_geo_contracts import GeoReadRequest,GeoReadResult
from regional_knowledge.kaggle_public_backend import current_binding
from regional_knowledge.kaggle_protocol import QueryResult,encode_envelope,decode_envelope
from regional_knowledge.participant_contracts import ParticipantDisable
from regional_knowledge.read_issuance import capture_read_output
from regional_knowledge.recovery_journal import RecoveryJournal
from test_geo_read_gate import setup,result
from test_kaggle_public_backend import target,work,release
from test_read_authority import auth
from test_participants import people,change
from test_recovery_journal import ConditionalMemoryStore
from test_control_spool import handler
from test_geo_migration import uid


async def configured(s,*,durable=True):
    store=ConditionalMemoryStore();journal=RecoveryJournal(store,stream='geo-output-fixture',max_outcome_bytes=8192)
    spool=ControlSpool(s.queue,s.auth.p.registry,journal,HandlerRegistry([handler()]))
    env=SimpleNamespace(p=s.auth.p,spool=spool,journal=journal,store=store)
    gate,authority,b,manifest,commands,_=await setup(env,s.auth,binding=current_binding(s.queue),durable=durable)
    s.auth.resolver.release(authority)
    backend=KaggleGeoBackend(worker_queue=s.queue,read_authority=s.auth.resolver,geo_gate=gate,
        destination_ref='geo-test',current=lambda:True) # fixture routing state, gate still verifies actual current proof
    return env,gate,backend,manifest,commands


@pytest.mark.asyncio
@pytest.mark.parametrize('state',['missing','pending','active'])
@pytest.mark.parametrize('cursor_kind',['legacy','dataset'])
async def test_registered_graph_fetch_routes_geo_without_bootstrap_only_after_activation(target,monkeypatch,state,cursor_kind):
    from mcp.server.mcpserver import MCPServer
    from regional_knowledge.kaggle_source_graph_tools import register_source_graph_tools
    active=state=='active'
    s=target;e,gate,geo,manifest,commands=await configured(s,durable=active)
    try:
        if cursor_kind=='dataset':
            await e.spool.dataset_cursor(current_binding(s.queue),control={'sequence':0,'heads':[]},current=gate.binding_current)
        gate.operation_keys=tuple(command.operation_key for command in commands)
        if state=='missing':gate.operation_keys=('missing-open','missing-activate')
        gate.invalidate()  # installed service without SourceGeoInstaller/bootstrap
        if not active:
            # A manifest loaded by an installer alone must not select GEO.
            gate._places=frozenset({uid(10)})
        seen=[]
        async def geo_fetch(*args,**kwargs):seen.append('geo');return {'branch':'geo'}
        async def source_fetch(*args,**kwargs):seen.append('source');return {'branch':'source'}
        monkeypatch.setattr(geo,'graph_fetch',geo_fetch)
        server=MCPServer('geo-routing-regression')
        register_source_graph_tools(server,SimpleNamespace(source_graph_fetch=source_fetch),geo,lambda:s.principal)
        response=await server.call_tool('graph_fetch',{'entity_id':uid(10)})
        assert not response.is_error
        assert seen==(['geo'] if active else ['source'])
        if active:assert gate.installation_current(current_binding(s.queue),gate.operation_keys)
    finally:e.spool.close()


def response(s,dispatched,manifest,commands):
    template=result(manifest,commands)
    return template.model_copy(update=dict(request_id=dispatched.query.request_id,
        connection_generation=s.generation,turn_generation=dispatched.query.turn_generation,
        observed_database_generation='synthetic-db',observed_data_seq=5,observed_projection_seq=5))


@pytest.mark.asyncio
@pytest.mark.parametrize('dataset_cursor',[False,True])
async def test_geo_roundtrip_exact_wire_shared_claim_and_final_current_output(target,dataset_cursor):
    s=target;e,gate,backend,manifest,commands=await configured(s)
    try:
        current=[True]
        if dataset_cursor:
            await e.spool.dataset_cursor(current_binding(s.queue),control={'sequence':0,'heads':[]},
                current=lambda binding:current[0] and gate.binding_current(binding))
        async def invoke():
            with capture_read_output() as calls:
                value=await backend.graph_fetch(uid(10),s.principal)
            return value,calls
        task=asyncio.create_task(invoke());dispatched=await work(s)
        assert type(dispatched.query) is GeoReadRequest and dispatched.query.actor.person_id==s.auth.p.other.subject
        frame=decode_envelope(encode_envelope(dispatched.query))
        assert type(frame) is GeoReadRequest
        exact=response(s,dispatched,manifest,commands)
        assert type(decode_envelope(encode_envelope(exact))) is GeoReadResult
        assert await s.admission.finish_read(s.run,s.token,s.generation,exact)
        output,calls=await task
        assert output['items'][0]['stable_id']==uid(10) and not output['accepted_target_source_evidence']
        assert output['historical_geometry']=='unresolved_with_owner' and len(calls)==1
        assert s.admission.status()['state']=='busy' # result is not compute release
        refreshed=await calls[0].gate.refresh(calls[0].gated)
        assert calls[0].gate.issuance_current(refreshed.ticket)
        if dataset_cursor:
            current[0]=False
            assert not calls[0].gate.issuance_current(refreshed.ticket)
            with pytest.raises(PermissionError):await gate.refresh(refreshed)
        gate.release(refreshed.ticket);calls[0].release();await release(s,dispatched)
    finally:e.spool.close()


@pytest.mark.asyncio
async def test_result_pairing_and_delayed_participant_revoke_refuse_geo_output(target):
    s=target;e,gate,backend,manifest,commands=await configured(s)
    try:
        async def invoke():
            with capture_read_output() as calls:value=await backend.graph_fetch(uid(10),s.principal)
            return value,calls
        task=asyncio.create_task(invoke());dispatched=await work(s)
        wrong=QueryResult(request_id=dispatched.query.request_id,connection_generation=s.generation,
            turn_generation=dispatched.query.turn_generation,status='unavailable',mode=None,reason='not_ready',
            observed_data_seq=None,observed_projection_seq=None)
        with pytest.raises(ValueError,match='operation type'):
            await s.admission.finish_read(s.run,s.token,s.generation,wrong)
        assert await s.admission.finish_read(s.run,s.token,s.generation,response(s,dispatched,manifest,commands))
        _,calls=await task
        change(s.auth.p,ParticipantDisable(disabled=True,reason='revoked delayed GEO read'))
        with pytest.raises(PermissionError):await calls[0].gate.refresh(calls[0].gated)
        calls[0].release();await release(s,dispatched)
    finally:e.spool.close()


@pytest.mark.asyncio
async def test_explicit_startup_registers_existing_mcp_routes_and_binds_real_output_guard(target,monkeypatch):
    from regional_knowledge.kaggle_service_startup import TargetServiceConfig,TargetServiceStartup
    from regional_knowledge.server import build_server
    from regional_knowledge.read_issuance import _SCOPE_KEY
    from starlette.requests import Request
    from test_kaggle_service_startup import HistoryAdapter
    s=target;e,gate,backend,manifest,commands=await configured(s)
    keys=tuple(c.operation_key for c in commands)
    startup=TargetServiceStartup(TargetServiceConfig(s.auth.provider,s.auth.p.registry,s.queue,e.spool,
        HistoryAdapter(),'writer-1',lambda **values:values['database_generation']=='synthetic-db',
        geo_manifest_operation_keys=keys))
    try:
        # Synthetic native coverage is independently exercised by the guarded
        # native suite. The actual startup gate establishes from real spool,
        # journal and already-verified immutable metadata here.
        startup.binding=current_binding(s.queue);startup.coverage=e.spool._source_anchor;startup.running=True
        await startup.geo_gate.establish(keys,binding=startup.binding)
        monkeypatch.setattr('regional_knowledge.server._principal',lambda:s.principal)
        monkeypatch.setattr('regional_knowledge.server.backend_from_env',lambda:pytest.fail('legacy fallback'))
        server=build_server(target_startup=startup)
        tools={tool.name:tool for tool in server._tool_manager.list_tools()}
        assert {'geo_lookup','graph_fetch','poi_context'}<=tools.keys()
        assert 'Old source text' in tools['graph_fetch'].description
        req=Request({'type':'http','method':'POST','path':'/mcp','headers':[],
            'app':SimpleNamespace(state=SimpleNamespace()),'state':{}})
        context=SimpleNamespace(request_context=SimpleNamespace(request=req,request_id='geo-mcp-fixture'))
        task=asyncio.create_task(server.call_tool('graph_fetch',{'entity_id':uid(10)},context))
        dispatched=await work(s)
        assert type(dispatched.query) is GeoReadRequest
        assert await s.admission.finish_read(s.run,s.token,s.generation,response(s,dispatched,manifest,commands))
        output=await task
        assert 'legacy_unanchored' in str(output) and uid(10) in str(output)
        issued=req.scope[_SCOPE_KEY][1]
        assert issued.gate is startup.geo_gate and issued.gate.issuance_current(issued.gated.ticket)
        issued.release();await release(s,dispatched)
    finally:
        await startup.source_dispatch.aclose();e.spool.close()
