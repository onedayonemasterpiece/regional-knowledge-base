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