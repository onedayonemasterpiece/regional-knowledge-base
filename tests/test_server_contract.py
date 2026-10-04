import os

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
    assert set(tools) == {"search", "fetch", "book_ingest", "book_pages", "document_access", "profile", "graph_stage", "graph_fetch", "graph_related"}
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
