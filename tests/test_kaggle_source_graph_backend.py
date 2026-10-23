"""Normal graph MCP calls share OAuth/admission and actual source final fences."""
import asyncio
from types import SimpleNamespace

import pytest
from mcp.server.mcpserver.exceptions import ToolError
from starlette.requests import Request

from regional_knowledge.control_spool import ControlSpool
from regional_knowledge.kaggle_apply import HandlerRegistry
from regional_knowledge.kaggle_domain_bindings import current_handler
from regional_knowledge.kaggle_protocol import TrustedActorContext
from regional_knowledge.kaggle_public_backend import current_binding
from regional_knowledge.kaggle_service_startup import TargetServiceConfig,TargetServiceStartup
from regional_knowledge.kaggle_source_graph_read import SourceGraphReadRequest,SourceGraphReadResult,_record
from regional_knowledge.read_issuance import _SCOPE_KEY
from regional_knowledge.recovery_journal import RecoveryJournal
from regional_knowledge.server import build_server
from test_kaggle_public_backend import target,work,release
from test_kaggle_service_startup import HistoryAdapter
from test_kaggle_source_graph_read import row
from test_source_import import SOURCE
from test_read_authority import auth
from test_participants import people
from test_recovery_journal import ConditionalMemoryStore


async def configured(t,*,with_geo=False):
    handlers=[current_handler('source.import.v4',check_sha256='1'*64,apply_sha256='2'*64)]
    if with_geo:handlers.append(current_handler('geo.manifest.install.v1',check_sha256='3'*64,apply_sha256='4'*64))
    spool=ControlSpool(t.queue,t.auth.p.registry,RecoveryJournal(ConditionalMemoryStore(),stream='source-graph-output'),HandlerRegistry(handlers))
    startup=TargetServiceStartup(TargetServiceConfig(t.auth.provider,t.auth.p.registry,t.queue,spool,HistoryAdapter(),
        'writer-1',lambda **values:values['database_generation']=='synthetic-db',
        geo_manifest_operation_keys=('not-yet-accepted-open','not-yet-accepted-activate') if with_geo else ()))
    startup.running=True;startup.binding=current_binding(t.queue)
    # Unit-native proof only; real native projection/origin/grants are tested in
    # the independent opt-in suite and retained root fixtures.
    anchor=SimpleNamespace(database_generation='synthetic-db',current=lambda:startup.binding_current(startup.binding))
    spool._source_anchor=anchor;startup.coverage=anchor
    return startup


def response(dispatched):
    record,item,_=_record(row(),dispatched.query)
    return SourceGraphReadResult(request_id=dispatched.query.request_id,connection_generation=dispatched.query.connection_generation,
        turn_generation=dispatched.query.turn_generation,status='ok',records=(record,),evidence=(item,),
        observed_data_seq=5,observed_projection_seq=5,observed_visibility_epoch=0,observed_database_generation='synthetic-db')


@pytest.mark.asyncio
@pytest.mark.parametrize('tool',['graph_fetch','entity_list'])
@pytest.mark.parametrize('with_geo',[False,True])
async def test_actual_mcp_graph_call_uses_one_typed_claim_and_current_source_output(target,monkeypatch,tool,with_geo):
    t=target;startup=await configured(t,with_geo=with_geo)
    try:
        monkeypatch.setattr('regional_knowledge.server._principal',lambda:t.principal)
        monkeypatch.setattr('regional_knowledge.server.backend_from_env',lambda:pytest.fail('legacy fallback'))
        server=build_server(target_startup=startup)
        tools=server._tool_manager.list_tools()
        assert sum(value.name=='graph_fetch' for value in tools)==1
        args={'entity_id':row()[4]['entity_id']} if tool=='graph_fetch' else {'document_ids':[SOURCE]}
        req=Request({'type':'http','method':'POST','path':'/mcp','headers':[],
            'app':SimpleNamespace(state=SimpleNamespace()),'state':{}})
        context=SimpleNamespace(request_context=SimpleNamespace(request=req,request_id='source-graph-mcp'))
        task=asyncio.create_task(server.call_tool(tool,args,context));dispatched=await work(t)
        assert type(dispatched.query) is SourceGraphReadRequest
        assert dispatched.query.actor.person_id==t.auth.p.other.subject
        assert await startup.admission.finish_read(t.run,t.token,t.generation,response(dispatched))
        output=await task;assert 'model_authored_source_claim_not_verified_identity_or_geometry' in str(output)
        issued=req.scope[_SCOPE_KEY][1];assert issued.gate is startup.gate
        assert issued.gate.issuance_current(issued.gated.ticket)
        startup.config.spool.invalidate_source_history()
        # Native PG graph records remain readable while an unrelated
        # source-history renewal fails. Current binding/actor still fence output.
        assert issued.gate.issuance_current(issued.gated.ticket)
        refreshed=await issued.gate.refresh(issued.gated)
        assert refreshed.result.records and refreshed.result.status=='ok'
        issued.gate.release(refreshed.ticket);issued.release();await release(t,dispatched)
    finally:await startup.source_dispatch.aclose();startup.config.spool.close()


@pytest.mark.asyncio
async def test_unknown_graph_scope_never_creates_another_reader_or_legacy_query(target):
    startup=await configured(target)
    try:
        with pytest.raises(ToolError,match='exact_source_scope_required'):
            await startup.backend.entity_list(target.principal,document_ids=None,query='unbounded graph')
        assert not startup.admission._reads
    finally:await startup.source_dispatch.aclose();startup.config.spool.close()
