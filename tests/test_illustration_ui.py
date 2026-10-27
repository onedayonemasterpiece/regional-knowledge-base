"""MCP Apps viewer is a declared, self-contained resource; old tool results stay intact."""
import pytest

from regional_knowledge.illustration_ui import (
    ILLUSTRATION_UI_HTML,
    ILLUSTRATION_UI_META,
    ILLUSTRATION_UI_MIME,
    ILLUSTRATION_UI_URI,
)
from regional_knowledge.server import build_server


@pytest.mark.asyncio
async def test_illustration_mcp_app_tool_and_resource_contract(monkeypatch):
    monkeypatch.setenv("RKB_DEV_NOAUTH", "1")
    server = build_server()
    tool = next(t for t in await server.list_tools() if t.name == "illustration_fetch")
    assert tool.meta["ui"]["resourceUri"] == ILLUSTRATION_UI_URI
    assert tool.meta["openai/outputTemplate"] == ILLUSTRATION_UI_URI

    resources = await server.list_resources()
    resource = next(r for r in resources if str(r.uri) == ILLUSTRATION_UI_URI)
    assert resource.mime_type == ILLUSTRATION_UI_MIME
    assert resource.meta == ILLUSTRATION_UI_META
    content = list(await server.read_resource(ILLUSTRATION_UI_URI))
    assert len(content) == 1
    assert content[0].content == ILLUSTRATION_UI_HTML
    assert content[0].mime_type == ILLUSTRATION_UI_MIME
    assert content[0].meta == ILLUSTRATION_UI_META


def test_illustration_viewer_is_network_free_and_uses_tool_image_and_caption():
    html = ILLUSTRATION_UI_HTML
    assert '<img id="picture"' in html
    assert 'ui/notifications/tool-result' in html
    assert 'openai:set_globals' in html
    assert 'credit.text' in html
    assert 'caption.textContent' in html
    assert 'data:' in html and 'SAFE_MIME.test' in html and 'BASE64.test' in html
    for forbidden in ("http://", "https://", "fetch(", "XMLHttpRequest",
                      "<script src=", "<link ", "window.open(", "iframe", "image/svg+xml"):
        assert forbidden not in html
    assert ILLUSTRATION_UI_META["ui"]["csp"] == {
        "connectDomains": [], "resourceDomains": [], "frameDomains": [],
    }
