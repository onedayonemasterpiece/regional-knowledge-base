"""Finalized status and independently authorized original reads.

Controlled owner OAuth/SQLite/OriginalIntakeController fixtures from the actual
repository. Synthetic finalized state is explicitly not real Dataset Saved.
"""
from types import MethodType, SimpleNamespace

import pytest
from mcp.server.mcpserver.exceptions import ToolError

from regional_knowledge.kaggle_importer import canonical
from test_kaggle_source_importer import intake, start
from test_kaggle_public_backend import target
from test_read_authority import auth
from test_participants import people


class NativeStatus:
    def __init__(self, importer):
        self.importer = importer
        self.calls = 0

    async def status(self, access, row):
        self.calls += 1
        return self.importer._output(row, "finalized",
            "Native source activation outcome remotely durable; retrieval acceptance is separate",
            "done", source_archive_status="verified")


async def accepted_fixture(s, monkeypatch, *, research):
    original = await start(s)
    row = s.importer._row(s.principal.subject, original.ingestion_id)
    s.importer._save(row, phase="finalized", research_json=canonical(research).decode())
    peer = NativeStatus(s.importer)
    monkeypatch.setattr(s.importer, "native_intake", peer, raising=False)

    # This fallback is used by the canonical base before native SQL13/14. On
    # the compatible combined candidate the actual native .status route wins.
    async def saved(self, access, row):
        return await peer.status(access, row)
    async def existing(self, row):
        return SimpleNamespace(accepted=object())
    monkeypatch.setattr(s.importer, "_finalize", MethodType(saved, s.importer))
    monkeypatch.setattr(s.importer, "_entry", MethodType(existing, s.importer))
    return original, peer


@pytest.mark.asyncio
async def test_complete_native_research_status_keeps_original_done_without_reprocessing(intake, monkeypatch):
    s = intake
    first, peer = await accepted_fixture(s,monkeypatch,research={"state":"complete","continuation":{}})
    result = await s.importer.book_ingest(command="status",principal=s.principal,ingestion_id=first.ingestion_id)
    assert result.state=="finalized" and result.next_action=="done" and result.next_cursor is None
    assert result.knowledge_extraction is None
    assert "model_authored_research_pending" not in result.warnings
    assert len(s.originals.calls) == 1 and peer.calls == 1


@pytest.mark.asyncio
async def test_missing_original_does_not_block_accepted_status_but_prevents_page_read(intake, monkeypatch):
    s = intake
    first, peer = await accepted_fixture(s,monkeypatch,research=dict(
        state="pending",continuation=dict(tool="book_pages",arguments=dict(ingestion_id="SELF"))))
    row=s.importer._row(s.principal.subject, first.ingestion_id)
    s.importer._save(row,research_json=canonical(dict(state="pending",continuation=dict(
        tool="book_pages",arguments=dict(ingestion_id=first.ingestion_id)))).decode())
    s.originals.receipts.pop(first.ingestion_id)
    status=await s.importer.book_ingest(command="status",principal=s.principal,
        document_id=first.document_id)
    assert status.state=="finalized"
    assert status.next_cursor is None and peer.calls==1
    with pytest.raises(ToolError,match="verified_original_pending"):
        await s.importer.book_pages(principal=s.principal,ingestion_id=first.ingestion_id)
    assert len(s.originals.calls) == 1
