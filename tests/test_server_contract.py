import os
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
    assert set(tools) == {"search", "fetch", "illustration_fetch", "book_find", "book_ingest", "book_pages", "document_access", "profile", "graph_stage", "graph_fetch", "graph_related", "indexing_status"}
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
    assert "document_id" in ingest.input_schema["properties"]
    assert "document_id" not in ingest.input_schema["required"]
    actions = ingest.output_schema["properties"]["next_action"]["anyOf"][0]["enum"]
    assert set(actions) == {"continue_pages", "validate", "finalize", "wait", "resume_finalize", "done", "blocker"}
    assert "book_find first" in ingest.description
    assert "new attached PDF/DjVu use start" in ingest.description
    assert "model-authored stage" in ingest.description
    assert "same logical document" in ingest.description
    assert "title, author and year" in find.description
    assert "never UUIDs" in server.instructions
    assert "book_pages" in server.instructions and "book_ingest(reprocess)" in server.instructions
    assert "model performs semantic reading" in server.instructions
    live = {tool.name for tool in await build_server(profile="live").list_tools()}
    assert "book_find" not in live and "book_ingest" not in live



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
