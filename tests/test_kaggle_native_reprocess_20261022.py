"""Native M2 reprocess boundary against the real owner OAuth/queue/importer.

These are controlled source tests: archive, Kaggle SQL and Dataset I/O are
synthetic. They must NOT be reported as a real accepted/reprocessed book.
The owned original and importer are the existing project fixtures.
"""
import base64
import json
import sys
from types import MethodType, ModuleType, SimpleNamespace
from uuid import uuid4

import pytest
from mcp.server.mcpserver.exceptions import ToolError

from regional_knowledge.dataset_recovery_store import DatasetRecoveryStore
from regional_knowledge.kaggle_importer import canonical, digest
from regional_knowledge.recovery_journal import RemoteOutcomeReceipt
from test_kaggle_source_importer import intake, start
from test_kaggle_public_backend import target  # transitive real queue/ASGI fixture
from test_read_authority import auth  # imported fixture name required by pytest
from test_participants import people  # imported fixture name required by pytest


class NativePeer:
    def __init__(self, importer, *, phase="staging", terminal=None):
        self.importer = importer
        self.phase = phase
        self.terminal = terminal
        self.status_calls = 0
        self.entry_calls = 0

    def _state(self, row):
        return dict(phase=self.phase, pending_operation_key=None, pending_kind=None)

    @staticmethod
    def _operation(row, kind):
        assert kind == "activate"
        return "source-activate:" + row["ingestion_id"]

    async def _entry(self, key):
        self.entry_calls += 1
        return self.terminal

    async def status(self, access, row):
        self.status_calls += 1
        return self.importer._output(row, "staged",
            "Exact native stage remains under its original operation",
            "validate", source_archive_status="verified")


def _installed_native(monkeypatch, importer, *, phase="staging", terminal=None):
    # The baseline PR intentionally precedes SQL013/014. In the combined
    # candidate Shkiper checks the actual native module and typed handlers.
    # Only the installed-revision symbol is supplied in this isolated test.
    module = ModuleType("regional_knowledge.source_activation")
    module.ACTIVATION_REVISIONS = importer.import_revisions
    monkeypatch.setitem(sys.modules, module.__name__, module)
    peer = NativePeer(importer, phase=phase, terminal=terminal)
    monkeypatch.setattr(importer, "native_intake", peer, raising=False)
    return peer


@pytest.mark.asyncio
async def test_reprocess_resumes_owned_inflight_archive_without_another_download(intake, monkeypatch):
    s = intake
    original = await start(s)
    peer = _installed_native(monkeypatch, s.importer)
    again = await s.importer.book_ingest(
        command="reprocess", principal=s.principal, document_id=original.document_id)
    assert again.ingestion_id == original.ingestion_id
    assert again.document_id == original.document_id
    assert again.next_action == "validate"
    assert again.state == "staged"
    assert "no second revision" in again.message
    assert peer.status_calls == 1 and peer.entry_calls == 0
    assert len(s.originals.calls) == 1
    # A second identical call resumes the same original operation, not a new
    # source, upload, vector job, lease, or volatile successor identity.
    another = await s.importer.book_ingest(
        command="reprocess", principal=s.principal, ingestion_id=original.ingestion_id)
    assert another.ingestion_id == again.ingestion_id
    assert peer.status_calls == 2 and len(s.originals.calls) == 1


@pytest.mark.asyncio
async def test_native_reuse_not_installed_is_explicit_and_never_mutates(intake):
    s = intake
    original = await start(s)
    with pytest.raises(ToolError, match="native_reuse_requires_installed_source_stage"):
        await s.importer.book_ingest(
            command="reprocess", principal=s.principal, document_id=original.document_id)
    assert len(s.originals.calls) == 1


@pytest.mark.asyncio
async def test_rechunk_requires_accepted_source_and_no_implicit_new_revision(intake, monkeypatch):
    s = intake
    original = await start(s)
    peer = _installed_native(monkeypatch, s.importer)
    with pytest.raises(ToolError, match="rechunk_requires_finalized_source"):
        await s.importer.book_ingest(
            command="rechunk", principal=s.principal, document_id=original.document_id)
    assert peer.status_calls == 0 and peer.entry_calls == 0
    assert len(s.originals.calls) == 1


@pytest.mark.asyncio
async def test_reprocess_never_treats_new_upload_or_unreviewed_payload_as_reuse(intake, monkeypatch):
    s = intake
    original = await start(s)
    peer = _installed_native(monkeypatch, s.importer)
    for extra in (
        dict(payload={"title":"forged revised work"}),
        dict(stage_file_sha256="a"*64),
        dict(file=object()),
    ):
        with pytest.raises(ToolError, match="native_reuse_cannot_replace_accepted_input|file_only_supported"):
            await s.importer.book_ingest(command="reprocess", principal=s.principal,
                document_id=original.document_id, **extra)
    assert peer.status_calls == 0
    assert len(s.originals.calls) == 1


@pytest.mark.asyncio
async def test_native_reprocess_cannot_bypass_second_active_owner_import(intake, monkeypatch):
    s = intake
    original = await start(s)
    peer = _installed_native(monkeypatch, s.importer)
    with s.t.queue.connect() as db:
        db.execute("""INSERT INTO rkb_source_intake_control
            (ingestion_id,source_id,person_id,file_id,request_id,metadata_json,rights_json,phase,created,updated)
            VALUES(?,?,?,?,?,?,?,?,?,?)""",
            (str(uuid4()),str(uuid4()),s.principal.subject,"other-file",str(uuid4()),"{}","{}",
                "staging",1,1))
    with pytest.raises(ToolError, match="another_native_ingestion_active"):
        await s.importer.book_ingest(command="reprocess", principal=s.principal,
            document_id=original.document_id)
    assert peer.status_calls == 0


@pytest.mark.asyncio
async def test_reuse_rejects_wrong_owned_document_and_identity(intake, monkeypatch):
    s = intake
    original = await start(s)
    _installed_native(monkeypatch, s.importer)
    with pytest.raises(ToolError, match="owned_ingestion_required"):
        await s.importer.book_ingest(command="reprocess",principal=s.principal,
            document_id=str(uuid4()))
    with pytest.raises(ToolError,match="ingestion_source_mismatch"):
        await s.importer.book_ingest(command="reprocess", principal=s.principal,
            ingestion_id=original.ingestion_id,document_id=str(uuid4()))
    assert len(s.originals.calls) == 1
