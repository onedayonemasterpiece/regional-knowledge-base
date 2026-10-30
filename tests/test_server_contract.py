import os
import json
import pytest

from regional_knowledge.server import _transport_security, build_server





def test_transport_security_keeps_dns_rebinding_protection_exact():
    settings = _transport_security("https://knowledge.example/mcp")

    assert settings.enable_dns_rebinding_protection is True
    assert "knowledge.example" in settings.allowed_hosts
    assert "https://knowledge.example" in settings.allowed_origins
    assert "127.0.0.1:*" in settings.allowed_hosts
    assert "http://127.0.0.1:*" in settings.allowed_origins
    assert "*.example" not in settings.allowed_hosts
    assert "*" not in settings.allowed_hosts


def test_model_surface_stays_small_and_goal_oriented(monkeypatch):
    monkeypatch.setenv("RKB_DEV_NOAUTH", "1")
    server = build_server()
    tools = {tool.name: tool for tool in server._tool_manager.list_tools()}
    assert set(tools) == {"catalog", "source_proof", "search", "fetch", "illustration_fetch", "book_find", "book_ingest", "book_pages", "document_access", "profile", "graph_stage", "graph_fetch", "graph_related", "indexing_status", "poi_registry", "poi_context"}
    assert tools['illustration_fetch'].annotations.read_only_hint is True
    assert tools["book_find"].annotations.read_only_hint is True
    assert "do not ask for document UUIDs" in tools["book_find"].description
    assert tools["book_ingest"].meta["openai/fileParams"] == ["file"]
    assert "verified archived source" in tools["book_ingest"].description
    assert "reprocess" in tools["book_ingest"].description
    assert tools["book_pages"].annotations.read_only_hint is True
    assert tools["profile"].meta["openai/profile"] is True
    assert tools["search"].annotations.read_only_hint is True
    assert tools["fetch"].annotations.read_only_hint is True
    assert tools["poi_context"].annotations.read_only_hint is True
    assert tools["poi_registry"].annotations.read_only_hint is False
    # Regression: defining the exposed tool named `profile` must not shadow
    # the server capability profile captured by poi_registry.
    import inspect
    assert inspect.getclosurevars(tools["poi_registry"].fn).nonlocals["profile"] == "full"
    assert "select" in str(tools["poi_registry"].fn_metadata.arg_model.model_json_schema())
    assert "create" in str(tools["poi_registry"].fn_metadata.arg_model.model_json_schema())

def test_live_profile_exposes_one_low_latency_knowledge_tool(monkeypatch):
    monkeypatch.setenv("RKB_DEV_NOAUTH", "1")
    server = build_server(profile="live")
    tools = {tool.name: tool for tool in server._tool_manager.list_tools()}
    assert set(tools) == {"knowledge_search"}
    assert tools["knowledge_search"].annotations.read_only_hint is True


@pytest.mark.asyncio
async def test_actual_mcp_declarations_teach_both_user_flows(monkeypatch):
    monkeypatch.setenv("RKB_DEV_NOAUTH", "1")
    server = build_server()
    tools = {tool.name: tool for tool in await server.list_tools()}
    find, ingest = tools["book_find"], tools["book_ingest"]
    assert find.input_schema["properties"]["limit"]["maximum"] == 8
    assert "reprocess" in ingest.input_schema["properties"]["command"]["enum"]
    assert "cursor" not in ingest.input_schema["properties"]
    assert "document_id" in ingest.input_schema["properties"]
    assert "document_id" not in ingest.input_schema["required"]
    actions = ingest.output_schema["properties"]["next_action"]["anyOf"][0]["enum"]
    assert set(actions) == {"continue_pages", "continue_tail", "validate", "finalize", "wait", "resume_finalize", "done", "blocker"}
    assert "book_find first" in ingest.description
    assert "new attached PDF/DjVu use start" in ingest.description
    assert "model-authored stage" in ingest.description
    assert "same logical document" in ingest.description
    assert "title, author and year" in find.description
    assert "never UUIDs" in server.instructions
    assert "book_pages" in server.instructions and "book_ingest(reprocess)" in server.instructions
    assert "model performs semantic reading" in server.instructions
    for phrase in ("ONE direct answer", "language of the user's question",
                   "names a specific author or book", "inline search or fetch evidence",
                   "Источники:", "В доступной базе подтверждений не найдено",
                   "Не удалось проверить базу сейчас", "printed-page numbers from scan numbers"):
        assert phrase in server.instructions
    live = {tool.name for tool in await build_server(profile="live").list_tools()}
    assert "book_find" not in live and "book_ingest" not in live



@pytest.mark.asyncio
async def test_adaptive_p1_policy_is_in_search_only_and_fetch_refers_to_it(monkeypatch):
    monkeypatch.setenv("RKB_DEV_NOAUTH", "1")
    tools = {tool.name: tool for tool in await build_server().list_tools()}
    search_description, fetch_description = tools["search"].description, tools["fetch"].description
    for phrase in (
        "Search/fetch checklist:", "user's language", "document_ids immediately",
        "inline search text", "incomplete and next_id", "After EACH read",
        "For ONE gap choose ONE action", "fetch(id=next_id)", "fetch using the hit's id",
        "source language/rephrase", "Never mix languages", "short OPEN question",
        "AND needs one entity", "do not prove NOT", "at most 6 sequential",
        "evidenced part and remaining question", "Answer in 1-4 sentences in the question's language",
        "for German questions, answer in German then immediately translate into Russian",
        "cite ALL supporting passages",
        "do not add details absent from read passages",
        "label them as outside the cited sources",
    ):
        assert phrase in search_description, phrase
    assert search_description.count("Search/fetch checklist:") == 1
    assert "only when needed" in search_description
    assert "Follow the search checklist." in fetch_description
    assert "when needed follow next_id" in fetch_description
    assert "Search/fetch checklist:" not in fetch_description
    assert "at most 6 sequential" not in fetch_description
    assert "document_ids" in tools["search"].input_schema["properties"]
    assert "source_id" in tools["fetch"].input_schema["properties"]


def test_auth_uses_application_issuer_not_supabase(monkeypatch):
    import regional_knowledge.server as server_module

    seen = {}

    class FakeVerifier:
        def __init__(self, **kwargs):
            seen.update(kwargs)

    monkeypatch.delenv("RKB_DEV_NOAUTH", raising=False)
    monkeypatch.setenv("KB_SUPABASE_URL", "https://kb-project.example")
    monkeypatch.setenv(
        "KB_SUPABASE_JWKS_URL",
        "https://kb-project.example/auth/v1/.well-known/jwks.json",
    )
    monkeypatch.setenv("RKB_AUTH_ISSUER", "https://auth.example")
    monkeypatch.setenv(
        "RKB_AUTH_JWKS_URL",
        "https://auth.example/.well-known/jwks.json",
    )
    monkeypatch.setenv("RKB_RESOURCE_URL", "https://knowledge.example/mcp")
    monkeypatch.setattr(server_module, "JwtResourceVerifier", FakeVerifier)

    server = build_server()
    assert server is not None
    assert seen == {
        "issuer": "https://auth.example",
        "jwks_url": "https://auth.example/.well-known/jwks.json",
        "resource": "https://knowledge.example/mcp",
    }


def test_supabase_configuration_never_enables_mcp_auth(monkeypatch):
    monkeypatch.delenv("RKB_DEV_NOAUTH", raising=False)
    monkeypatch.delenv("RKB_AUTH_ISSUER", raising=False)
    monkeypatch.delenv("RKB_AUTH_JWKS_URL", raising=False)
    monkeypatch.delenv("RKB_RESOURCE_URL", raising=False)
    monkeypatch.setenv("KB_SUPABASE_URL", "https://kb-project.example")
    monkeypatch.setenv(
        "KB_SUPABASE_JWKS_URL",
        "https://kb-project.example/auth/v1/.well-known/jwks.json",
    )

    import pytest

    with pytest.raises(RuntimeError, match="Supabase is only the data plane"):
        build_server()


@pytest.mark.asyncio
@pytest.mark.parametrize('tool',['indexing_status','catalog','unknown_tool'])
async def test_unsupported_profile_tool_does_not_report_a_kaggle_outage(tool):
    from regional_knowledge.server import KnowledgeMCPServer
    from mcp.server.mcpserver.exceptions import ToolError
    server=KnowledgeMCPServer('source-profile')
    server._target_source_profile=True
    with pytest.raises(ToolError) as error:
        await server.call_tool(tool,{})
    assert json.loads(str(error.value))=={
        'status':'unavailable','reason':'tool_not_supported_in_profile'}


@pytest.mark.asyncio
@pytest.mark.parametrize('name',['search','fetch','book_ingest','book_pages','profile','unknown_tool','participants_manage'])
async def test_rejected_request_still_records_idle_activity(name):
    from regional_knowledge.server import KnowledgeMCPServer
    from mcp.server.mcpserver.exceptions import ToolError
    server=KnowledgeMCPServer('request-activity-test')
    server._target_source_profile=True
    calls=[]
    server._bge_request_activity=lambda:calls.append(name)
    with pytest.raises((ToolError,ValueError)):
        await server.call_tool(name,{})
    assert calls==[name]


@pytest.mark.asyncio
async def test_search_k_ten_is_declared_in_full_and_live_mcp_without_default_change(monkeypatch):
    monkeypatch.setenv("RKB_DEV_NOAUTH", "1")
    for profile, name, field_name in (("full", "search", "top_k"),
                                      ("live", "knowledge_search", "max_evidence")):
        tools = {tool.name: tool for tool in await build_server(profile=profile).list_tools()}
        tool = tools[name]
        field = tool.input_schema["properties"][field_name]
        assert (field["minimum"], field["maximum"], field["default"]) == (1, 10, 3)
        if profile == "full":
            assert "For overview questions, top_k can be increased up to 10." not in tool.description
