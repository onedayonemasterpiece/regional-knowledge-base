"""P0: actual registered MCP read/write failures carry safe existing envelopes.

No Kaggle, PG, original bytes, network or import mutation is executed here.
"""
import asyncio
import json
from types import SimpleNamespace

import pytest
from mcp.server.mcpserver.exceptions import ToolError

from regional_knowledge.contracts import BookIngestOutput, Principal
from regional_knowledge.kaggle_importer import failure
import regional_knowledge.server as server_module


_INGESTION = "00000000-0000-0000-0000-000000000001"
_SECRET = "Bearer PRIVATE_TOKEN SignedUrl=https://private.example/?token=secret BOOK_EXCERPT"


class FakeBooks:
    """Only data-plane seams; MCP registration, validation and envelope are real."""

    def __init__(self):
        self.error = None
        self.calls = []

    async def book_ingest(self, **kwargs):
        self.calls.append(("book_ingest", kwargs["command"]))
        if self.error is not None:
            raise self.error
        return BookIngestOutput(
            state="processing", message="Source page review continues",
            ingestion_id=_INGESTION, next_action="continue_pages",
            source_archive_status="verified",
        )

    async def book_pages(self, **kwargs):
        self.calls.append(("book_pages", kwargs["ingestion_id"]))
        if self.error is not None:
            raise self.error
        return SimpleNamespace(next_cursor="next", pages=[SimpleNamespace(
            page_id=_INGESTION, physical_page_index=0, printed_page_number="1",
            mime_type="image/jpeg", native_text=None, native_blocks=(),
            native_text_info=None, data=b"synthetic jpeg pixel data"
        )])


@pytest.fixture
def registered_books(monkeypatch):
    monkeypatch.setenv("RKB_DEV_NOAUTH", "1")
    monkeypatch.delenv("RKB_BGE_QUEUE_PATH", raising=False)
    monkeypatch.setattr(server_module, "_principal", lambda: Principal(
        subject=_INGESTION, client_id="fixture-client",
        issuer="fixture-issuer", access_token="PRIVATE_TOKEN_DO_NOT_LOG"
    ))
    backend = FakeBooks()
    server = server_module.build_server(backend, profile="full")
    return server, backend


def arguments(tool):
    return {"command": "status", "ingestion_id": _INGESTION} if tool == "book_ingest" else {
        "ingestion_id": _INGESTION, "batch_size": 2}


async def error_envelope(server, tool):
    """Exercise the registered callable and inspect MCP's actual error surface."""
    try:
        result = await server.call_tool(tool, arguments(tool))
    except ToolError as exc:
        raw = str(exc)
    else:
        assert result.is_error, "a thrown backend failure cannot become success"
        raw = "\n".join(item.text for item in result.content if item.type == "text")
    assert _SECRET not in raw
    assert "PRIVATE_TOKEN_DO_NOT_LOG" not in raw
    assert "Traceback" not in raw
    # MCP implementations may prepend 'Error executing tool ...'. The payload
    # must still be an actual short JSON reason, not that bare generic prefix.
    begin, end = raw.find("{"), raw.rfind("}")
    assert 0 <= begin < end
    envelope = json.loads(raw[begin:end + 1])
    assert set(envelope) == {"status", "reason", "error_class"}
    assert len(json.dumps(envelope)) <= 250
    return envelope


@pytest.mark.asyncio
@pytest.mark.parametrize("tool", ["book_ingest", "book_pages"])
@pytest.mark.parametrize("status,reason", [
    ("invalid_query", "invalid_original_page_request"),
    ("denied", "access_denied"),
    ("unavailable", "source_history_unavailable"),
    ("pending", "prior_write_pending"),
    ("unknown", "original_result_unknown"),
])
async def test_expected_domain_toolerror_preserves_actual_status_reason(
        registered_books, tool, status, reason):
    server, backend = registered_books
    backend.error = failure(reason, status=status)
    envelope = await error_envelope(server, tool)
    assert envelope == {"status": status, "reason": reason, "error_class": "domain"}
    assert len(backend.calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("tool", ["book_ingest", "book_pages"])
@pytest.mark.parametrize("error,status,reason,kind", [
    (PermissionError(_SECRET), "denied", "access_denied", "access"),
    (TimeoutError(_SECRET), "unknown", "transport_timeout", "transport"),
    (ConnectionError(_SECRET), "unknown", "transport_unavailable", "transport"),
    (OSError(_SECRET), "unknown", "transport_unavailable", "transport"),
    (RuntimeError(_SECRET), "unknown", "internal_error", "internal"),
    (ValueError(_SECRET), "unknown", "internal_error", "internal"),
    (ToolError(_SECRET), "unknown", "domain_error", "domain"),
])
async def test_untrusted_exception_never_leaks_or_becomes_final_rejection(
        registered_books, tool, error, status, reason, kind):
    server, backend = registered_books
    backend.error = error
    envelope = await error_envelope(server, tool)
    assert envelope == {"status": status, "reason": reason, "error_class": kind}
    assert len(backend.calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("tool", ["book_ingest", "book_pages"])
async def test_domain_json_extras_and_private_error_text_do_not_escape(registered_books, tool):
    server, backend = registered_books
    backend.error = ToolError(json.dumps({
        "status": "invalid_query", "reason": "invalid_small_source_input",
        "original_bytes": _SECRET, "traceback": _SECRET, "token": _SECRET,
    }))
    assert await error_envelope(server, tool) == {
        "status": "invalid_query", "reason": "invalid_small_source_input",
        "error_class": "domain",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("tool", ["book_ingest", "book_pages"])
async def test_invalid_json_reason_or_false_success_does_not_pass(registered_books, tool):
    server, backend = registered_books
    backend.error = ToolError(json.dumps({
        "status": "ok", "reason": _SECRET, "query": _SECRET,
    }))
    assert await error_envelope(server, tool) == {
        "status": "unknown", "reason": "domain_error", "error_class": "domain"}


@pytest.mark.asyncio
@pytest.mark.parametrize("tool", ["book_ingest", "book_pages"])
async def test_cancel_does_not_invent_rejected_or_retry(registered_books, tool):
    server, backend = registered_books
    backend.error = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await server.call_tool(tool, arguments(tool))
    assert len(backend.calls) == 1


@pytest.mark.asyncio
async def test_existing_plain_mcp_rejection_keeps_stable_local_reason(registered_books):
    server, backend = registered_books
    # This is raised by book_ingest's existing schema adapter before backend,
    # with explanatory private details deliberately omitted from the envelope.
    backend.error = ToolError("stage_file_invalid: " + _SECRET)
    assert await error_envelope(server, "book_ingest") == {
        "status": "invalid_query", "reason": "stage_file_invalid",
        "error_class": "domain"}


@pytest.mark.asyncio
async def test_successful_registered_ingest_and_page_response_are_unchanged(registered_books):
    server, backend = registered_books
    declarations = {tool.name: tool for tool in await server.list_tools()}
    assert "command" in declarations["book_ingest"].input_schema["properties"]
    assert "source_import" in declarations["book_ingest"].input_schema["properties"]
    assert declarations["book_ingest"].meta["openai/fileParams"] == ["file"]
    assert {"ingestion_id", "cursor", "batch_size"} <= set(
        declarations["book_pages"].input_schema["properties"])

    write = await server.call_tool("book_ingest", arguments("book_ingest"))
    assert not write.is_error
    assert write.structured_content["state"] == "processing"
    assert write.structured_content["ingestion_id"] == _INGESTION
    assert write.structured_content["next_action"] == "continue_pages"

    read = await server.call_tool("book_pages", arguments("book_pages"))
    assert not read.is_error
    manifest = json.loads(read.content[0].text)
    assert manifest["ingestion_id"] == _INGESTION
    assert manifest["next_cursor"] == "next"
    assert manifest["pages"][0]["physical_page_index"] == 0
    assert read.content[1].type == "image"
    assert backend.calls == [("book_ingest", "status"), ("book_pages", _INGESTION)]
