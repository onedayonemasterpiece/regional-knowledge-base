"""Ordinary owner-only book import must not require GEO or a legal questionnaire.

Synthetic verified original, real OAuth/owner admission and native source spool;
no Kaggle, remote PostgreSQL, provider upload, external model or public data.
"""
import json
from types import MethodType, SimpleNamespace

import pytest
from mcp.server.mcpserver.exceptions import ToolError

from regional_knowledge.contracts import ChatFile
from regional_knowledge.kaggle_import_contracts import TargetSourceImportInput
from regional_knowledge.kaggle_importer import OWNER_IMPORT_ATTESTATION
from regional_knowledge.kaggle_service_startup import TargetServiceStartup
from regional_knowledge.server import build_server
from regional_knowledge.source_stage import STAGE_ROUTE
from test_kaggle_native_source_importer import native_intake, batch, stage, pending
from test_kaggle_public_backend import target
from test_participants import people
from test_read_authority import auth


def _file(file_id):
    return ChatFile(file_id=file_id,
        download_url="https://files.example.test/synthetic-owner.djvu",
        mime_type="image/vnd.djvu")


def _metadata():
    return {"title": "Synthetic owner-only source", "authors": ["Source fixture"]}


@pytest.mark.asyncio
async def test_owner_start_and_native_stage_ignore_unready_geo(native_intake):
    s = native_intake
    s.geo_state["current"] = False
    s.startup.geo_installer = SimpleNamespace(current=lambda: False)
    s.startup.geo_gate = SimpleNamespace(installation_current=lambda *_: False)
    # The old fixture deliberately simulates a GEO gate. Replace only this
    # synthetic seam with the real production imports_current implementation.
    s.startup.imports_current = MethodType(TargetServiceStartup.imports_current, s.startup)
    assert s.startup.reads_current()
    assert s.startup.imports_current()

    result = await s.importer.book_ingest(
        command="start", principal=s.principal,
        file=_file("synthetic-owner-no-geo"), payload=_metadata(),
        source_import=None,
    )
    assert result.state == "staged"
    row = s.importer._row(s.principal.subject, result.ingestion_id)
    assert row["rights_json"] == OWNER_IMPORT_ATTESTATION
    assert json.loads(row["rights_json"]) == {
        "basis": "owner_attestation",
        "policy": "owner_legal_shared_source_import_v1",
    }
    assert len(s.originals.calls) == 1

    staged = await stage(s, result, batch(s, result, count=1))
    assert staged.state == "processing"
    entry = await pending(s, result)
    assert entry.command.route == STAGE_ROUTE
    assert json.loads(entry.command.payload)["source"]["public_rights"] == json.loads(OWNER_IMPORT_ATTESTATION)


@pytest.mark.asyncio
async def test_legacy_rights_exact_receipt_survives_missing_declaration_retry(native_intake):
    s = native_intake
    from test_kaggle_source_importer import options
    rights = options()
    result = await s.importer.book_ingest(
        command="start", principal=s.principal,
        file=_file("synthetic-existing-rights"), payload=_metadata(),
        source_import=rights,
    )
    row = s.importer._row(s.principal.subject, result.ingestion_id)
    frozen = row["rights_json"]
    assert frozen != OWNER_IMPORT_ATTESTATION

    resumed = await s.importer.book_ingest(
        command="start", principal=s.principal,
        file=_file("synthetic-existing-rights"), payload=_metadata(), source_import=None,
    )
    assert resumed.ingestion_id == result.ingestion_id
    assert resumed.document_id == result.document_id
    assert s.importer._row(s.principal.subject, result.ingestion_id)["rights_json"] == frozen

    different = TargetSourceImportInput.model_validate({
        "public_rights": {"rights_status": "unknown", "evidence": {},
                          "policy_version": "synthetic-explicit-owner-basis"},
    })
    calls = len(s.originals.calls)
    with pytest.raises(ToolError, match="existing_file_intake_differs"):
        await s.importer.book_ingest(
            command="start", principal=s.principal,
            file=_file("synthetic-existing-rights"), payload=_metadata(), source_import=different,
        )
    assert len(s.originals.calls) == calls
    assert s.importer._row(s.principal.subject, result.ingestion_id)["rights_json"] == frozen


@pytest.mark.asyncio
async def test_no_source_or_nonowner_still_rejected_before_original(native_intake):
    s = native_intake
    with pytest.raises(ToolError, match="attached_source_required"):
        await s.importer.book_ingest(
            command="start", principal=s.principal, file=None, payload=_metadata(), source_import=None,
        )
    assert s.originals.calls == []

    from regional_knowledge.contracts import Principal
    other = Principal(subject=s.t.auth.p.other.subject,
        client_id=s.principal.client_id, issuer=s.principal.issuer,
        access_token=s.t.auth.tokens.access_token)
    with pytest.raises(ToolError, match="access_denied"):
        await s.importer.book_ingest(
            command="start", principal=other,
            file=_file("synthetic-other-person"), payload=_metadata(), source_import=None,
        )
    assert s.originals.calls == []


@pytest.mark.asyncio
async def test_mcp_book_pages_advertises_default_one_max_four(native_intake):
    server = build_server(target_startup=native_intake.startup)
    tools = {tool.name: tool for tool in await server.list_tools()}
    parameter = tools["book_pages"].input_schema["properties"]["batch_size"]
    assert parameter["default"] == 1
    assert parameter["minimum"] == 1
    assert parameter["maximum"] == 4
    assert "at most four" in tools["book_pages"].description
    assert "Stage each reviewed page" in tools["book_pages"].description
    assert "no per-book public_rights declaration" in tools["book_ingest"].description
    assert "no per-book public_rights declaration" in server.instructions


def test_optional_import_options_do_not_become_another_approval():
    assert TargetSourceImportInput.model_validate({}).public_rights is None
    # The already authorized owner's optional typed claim is recorded, not
    # subjected to a second import-specific legal certification.
    input_ = TargetSourceImportInput.model_validate({
        "public_rights": {"rights_status": "unknown", "evidence": {},
                          "policy_version": "synthetic-owner-claim"},
    })
    assert input_.public_rights.rights_status.value == "unknown"
