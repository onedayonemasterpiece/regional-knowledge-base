"""M5 post-acceptance source research is a reachable read, not a new import.

Controlled owner OAuth/SQLite/OriginalIntakeController fixtures from the actual
repository. Synthetic finalized state is explicitly not real Dataset Saved.
"""
from types import MethodType, SimpleNamespace
from uuid import uuid4

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
async def test_finalized_pending_status_reaches_exact_old_archive_page_cursor(intake, monkeypatch):
    s = intake
    first = await start(s)
    # Keep accepted fixture synthetic, and check the same original receipt
    # rather than starting another upload or changing the exact source ID.
    row = s.importer._row(s.principal.subject, first.ingestion_id)
    s.importer._save(row,phase="finalized",research_json=canonical(dict(
        state="pending",continuation=dict(tool="book_pages",arguments=dict(
            ingestion_id=first.ingestion_id,cursor="0",batch_size=1)))).decode())
    peer = NativeStatus(s.importer)
    monkeypatch.setattr(s.importer,"native_intake",peer,raising=False)
    async def native_final(self, access, row):return await peer.status(access,row)
    async def pending_entry(self,row):return object()
    monkeypatch.setattr(s.importer,"_finalize",MethodType(native_final,s.importer))
    monkeypatch.setattr(s.importer,"_entry",MethodType(pending_entry,s.importer))
    for _ in range(2):
        output = await s.importer.book_ingest(command="status",principal=s.principal,
            document_id=first.document_id)
        assert output.state == "finalized"
        assert output.ingestion_id == first.ingestion_id
        assert output.document_id == first.document_id
        assert output.next_action == "continue_pages" and output.next_cursor == "0"
        assert output.warnings.count("model_authored_research_pending") == 1
        assert "retrieval acceptance" in output.message
    assert len(s.originals.calls) == 1 and peer.calls == 2


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
@pytest.mark.parametrize("continuation", [
    {"tool":"search","arguments":{}},
    {"tool":"book_pages","arguments":{"ingestion_id":str(uuid4())}},
    {"tool":"book_pages","arguments":{"ingestion_id":"not-a-uuid"}},
    {"tool":"book_pages","arguments":{"ingestion_id":"SELF","file":"untrusted"}},
    {"tool":"book_pages","arguments":{"ingestion_id":"SELF","batch_size":2}},
    {"tool":"book_pages","arguments":{"ingestion_id":"SELF","batch_size":True}},
    {"tool":"book_pages","arguments":{"ingestion_id":"SELF","cursor":"\n"}},
    {"tool":"book_pages","arguments":{"ingestion_id":"SELF","cursor":"0"*257}},
    {"note":"please rescan later"},
])
async def test_finalized_pending_never_emits_forged_or_unusable_research_step(intake, monkeypatch, continuation):
    s = intake
    original = await start(s)
    args = continuation.get("arguments")
    if isinstance(args,dict) and args.get("ingestion_id")=="SELF":
        continuation={**continuation,"arguments":{**args,"ingestion_id":original.ingestion_id}}
    row = s.importer._row(s.principal.subject,original.ingestion_id)
    s.importer._save(row,phase="finalized",
        research_json=canonical({"state":"pending","continuation":continuation}).decode())
    peer = NativeStatus(s.importer)
    monkeypatch.setattr(s.importer, "native_intake",peer,raising=False)
    async def native_final(self, access, row): return await peer.status(access,row)
    async def pending_entry(self,row):return object()
    monkeypatch.setattr(s.importer,"_finalize",MethodType(native_final,s.importer))
    monkeypatch.setattr(s.importer,"_entry",MethodType(pending_entry,s.importer))
    with pytest.raises(ToolError, match="native_research_continuation_unreachable|native_research_accepted_frontier_invalid"):
        await s.importer.book_ingest(command="status",principal=s.principal,
            ingestion_id=original.ingestion_id)
    assert len(s.originals.calls) == 1


@pytest.mark.asyncio
async def test_verified_original_missing_blocks_frontier_without_reupload(intake, monkeypatch):
    s = intake
    first, peer = await accepted_fixture(s,monkeypatch,research=dict(
        state="pending",continuation=dict(tool="book_pages",arguments=dict(ingestion_id="SELF"))))
    row=s.importer._row(s.principal.subject, first.ingestion_id)
    s.importer._save(row,research_json=canonical(dict(state="pending",continuation=dict(
        tool="book_pages",arguments=dict(ingestion_id=first.ingestion_id)))).decode())
    s.originals.receipts.pop(first.ingestion_id)
    with pytest.raises(ToolError,match="native_research_archived_original_unavailable"):
        await s.importer.book_ingest(command="status",principal=s.principal,
            document_id=first.document_id)
    assert len(s.originals.calls) == 1


@pytest.mark.asyncio
async def test_finalized_complete_with_dangling_frontier_fails_closed(intake, monkeypatch):
    s = intake
    first, peer = await accepted_fixture(s,monkeypatch,research=dict(
        state="complete",continuation={"tool":"book_pages","arguments":{"ingestion_id":"fake"}}))
    with pytest.raises(ToolError,match="native_research_accepted_frontier_invalid"):
        await s.importer.book_ingest(command="status",principal=s.principal,
            document_id=first.document_id)
    assert len(s.originals.calls) == 1
