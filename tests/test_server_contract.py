import os

from regional_knowledge.server import build_server


def test_model_surface_stays_small_and_goal_oriented(monkeypatch):
    monkeypatch.setenv("RKB_DEV_NOAUTH", "1")
    server = build_server()
    tools = {tool.name: tool for tool in server._tool_manager.list_tools()}
    assert set(tools) == {"search", "fetch", "book_ingest", "book_pages", "document_access", "profile"}
    assert tools["book_ingest"].meta["openai/fileParams"] == ["file"]
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



def test_oauth_defaults_derive_from_kb_supabase_url(monkeypatch):
    import regional_knowledge.server as server_module

    seen = {}

    class FakeVerifier:
        def __init__(self, **kwargs):
            seen.update(kwargs)

    monkeypatch.delenv("RKB_DEV_NOAUTH", raising=False)
    monkeypatch.delenv("RKB_OAUTH_ISSUER", raising=False)
    monkeypatch.delenv("RKB_OAUTH_JWKS_URL", raising=False)
    monkeypatch.delenv("SUPABASE_URL", raising=False)
    monkeypatch.setenv("KB_SUPABASE_URL", "https://kb-project.example")
    monkeypatch.setenv("RKB_RESOURCE_URL", "https://knowledge.example/mcp")
    monkeypatch.setattr(server_module, "SupabaseJwtVerifier", FakeVerifier)

    server = build_server()
    assert server is not None
    assert seen["issuer"] == "https://kb-project.example/auth/v1"
    assert (
        seen["jwks_url"]
        == "https://kb-project.example/auth/v1/.well-known/jwks.json"
    )
    assert seen["resource"] == "https://knowledge.example/mcp"
