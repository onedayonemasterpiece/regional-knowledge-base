"""Actual MCP declarations and authenticated calls, separate from live PMI."""
import json
from types import SimpleNamespace
from uuid import uuid4

from mcp.server.mcpserver import MCPServer
import pytest

from regional_knowledge.contracts import Principal
from regional_knowledge.disagreement_tools import register_research_tools
from regional_knowledge.participant_contracts import ParticipantUpsert
from regional_knowledge.participants import Participants, RequestAuthority
from regional_knowledge.sqlite_corpus import SQLiteCorpus


def unpack(value):
    return value if isinstance(value,dict) else json.loads(value[0].text)


@pytest.fixture
def surface(tmp_path):
    corpus=SQLiteCorpus(tmp_path/"corpus.sqlite3")
    owner=Principal(subject=str(uuid4()),issuer="test",client_id="test",access_token="synthetic")
    other=owner.model_copy(update={"subject":str(uuid4())})
    corpus.put("rkb_users",[{"id":owner.subject,"status":"active"}])
    people=Participants(corpus,owner.subject)
    current=[RequestAuthority(owner,"owner")]
    mcp=MCPServer("test")
    register_research_tools(mcp,people,lambda:current[0])
    return SimpleNamespace(corpus=corpus,owner=owner,other=other,people=people,current=current,mcp=mcp)


@pytest.mark.asyncio
async def test_mcp_typed_participant_create_receipt_and_own_read(surface):
    s=surface
    tools={tool.name:tool for tool in await s.mcp.list_tools()}
    assert set(tools)=={"disagreement_participants","disagreement_get","disagreement_edit","disagreement_contribute","disagreement_conclusion"}
    assert tools["disagreement_get"].annotations.read_only_hint
    assert "actor_id" not in tools["disagreement_participants"].input_schema["properties"]
    async def call(args): return unpack(await s.mcp._tool_manager.call_tool("disagreement_participants",args,None))
    args={"action":"change","person_id":s.other.subject,"expected_revision":0,"operation_key":"mcp-add-participant", "command":{"name":"Synthetic researcher","categories":["research_staff","guide"],"reason":"test"}}
    saved=await call(args)
    assert saved["revision"]==1
    assert await call(args)==saved
    assert await call({"action":"operation_status","operation_key":"mcp-add-participant"})==saved
    read=await call({"action":"get","person_id":s.other.subject})
    assert set(read["profile"]["categories"])=={"research_staff","guide"} and not read["grants"]
    s.current[0]=RequestAuthority(s.other,"personal")
    assert (await call({"action":"get","person_id":s.other.subject}))["person_id"]==s.other.subject
    assert (await call({"action":"list"}))["error"]["code"]=="capability_denied"
    assert (await call(args))["error"]["code"]=="capability_denied"


@pytest.mark.asyncio
async def test_actual_case_create_append_history_and_auth_context_per_call(surface):
    s=surface
    async def call(name,args): return unpack(await s.mcp._tool_manager.call_tool(name,args,None))
    args={"action":"create","operation_key":"mcp-new-case", "command":{"question":"Synthetic question","context":"Two sources","classification":"candidate","rationale":"Different events may explain dates","positions":[{"id":"a","text":"First date","speaker":"Author A","modality":"asserted"}]}}
    created=await call("disagreement_edit",args)
    assert await call("disagreement_edit",args)==created
    case=created["case_id"]
    assert (await call("disagreement_get",{"case_id":case}))["question"]=="Synthetic question"
    contribution=await call("disagreement_contribute",{"case_id":case,"operation_key":"mcp-case-opinion","command":{"kind":"opinion","original_text":"Read the footnote before deciding"}})
    history=await call("disagreement_get",{"case_id":case,"section":"history","limit":1})
    assert history["has_more"] and history["items"][0]["seq"]==1
    second=await call("disagreement_get",{"case_id":case,"section":"history","after_seq":history["next_after_seq"]})
    assert second["items"][0]["seq"]==contribution["event_seq"]
    s.people.change(s.current[0],s.other.subject,0,ParticipantUpsert(name="Other",reason="test"),"new-other-person")
    s.current[0]=RequestAuthority(s.other,"personal")
    assert (await call("disagreement_get",{"case_id":case}))["error"]["code"]=="capability_denied"
    assert (await call("disagreement_edit",{"action":"operation_status","operation_key":"mcp-new-case"}))["error"]["code"]=="capability_denied"
    # Changing only the actor between calls cannot inherit the last owner context.
    with s.corpus.connect() as db:
        assert db.execute("SELECT count(*) FROM disagreement_contributions").fetchone()[0]==1


@pytest.mark.asyncio
async def test_real_server_full_profile_registers_research_but_live_does_not(surface,monkeypatch):
    from regional_knowledge import server as module
    s=surface
    monkeypatch.setenv("RKB_DEV_NOAUTH","1")
    monkeypatch.setenv("RKB_OWNER_SUBJECT",s.owner.subject)
    monkeypatch.setattr(module,"_principal",lambda:s.owner)
    backend=SimpleNamespace(corpus=s.corpus)
    full=module.build_server(backend=backend)
    assert "disagreement_participants" in {t.name for t in await full.list_tools()}
    live=module.build_server(backend=backend,profile="live")
    assert {t.name for t in await live.list_tools()}=={"knowledge_search"}
    result=unpack(await full._tool_manager.call_tool("disagreement_participants",{"action":"list"},None))
    assert result=={"items":[],"has_more":False,"next_after_id":None}
