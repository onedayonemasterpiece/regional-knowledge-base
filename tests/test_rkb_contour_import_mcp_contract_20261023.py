"""Owner-intake public MCP contract against real target API classes, no provider effects.

The current native activation is shared/public. Private-without-public-rights
must NOT be enabled by weakening the start gate until read eligibility exists.
"""
from dataclasses import replace
import json

import pytest
from mcp.server.mcpserver.exceptions import ToolError

from regional_knowledge.contracts import ChatFile, Principal
from regional_knowledge.original_intake import OriginalIntakeSpec
from regional_knowledge.server import build_server
from test_participants import people
from test_read_authority import auth
from test_kaggle_public_backend import target
from test_kaggle_native_source_importer import native_intake, start


@pytest.mark.asyncio
async def test_target_mcp_advertises_native_book_bounds_and_complete_story_input(native_intake):
    server = build_server(target_startup=native_intake.startup)
    tools = {tool.name: tool for tool in await server.list_tools()}
    ingest = tools["book_ingest"]
    schema = ingest.input_schema
    assert "source_import" in schema["properties"]
    assert "metadata" in schema["properties"]
    assert "basic_variant" in json.dumps(schema, ensure_ascii=False)
    assert "128 MiB" in server.instructions
    assert "2500 physical pages" in server.instructions
    assert "128 MiB" in ingest.description
    assert "2500 physical pages" in ingest.description
    assert "verified public-distribution basis" in ingest.description
    assert "basic_variant" in ingest.description
    assert "2 MiB and 32 pages" not in server.instructions


@pytest.mark.asyncio
async def test_owner_public_activation_still_rejects_missing_rights_before_original(native_intake):
    s = native_intake
    with pytest.raises(ToolError, match="attached_source_and_explicit_public_rights_required"):
        await s.importer.book_ingest(
            command="start",
            principal=s.principal,
            file=ChatFile(file_id="fixture-private-no-license",
                download_url="https://files.example.test/synthetic.pdf",
                mime_type="application/pdf"),
            payload={"title": "Synthetic rights-unknown private book"},
            source_import=None,
        )
    assert s.originals.calls == []


@pytest.mark.asyncio
async def test_nonowner_cannot_use_owner_private_start_even_without_rights(native_intake):
    s = native_intake
    other = Principal(
        subject=s.t.auth.p.other.subject,
        client_id=s.principal.client_id,
        issuer=s.principal.issuer,
        access_token=s.t.auth.tokens.access_token,
    )
    with pytest.raises(ToolError, match="access_denied"):
        await s.importer.book_ingest(
            command="start", principal=other,
            file=ChatFile(file_id="synthetic-other-person",
                download_url="https://files.example.test/other.pdf",
                mime_type="application/pdf"),
            payload={"title": "Synthetic source not owned by caller"},
            source_import=None,
        )
    assert s.originals.calls == []


@pytest.mark.asyncio
async def test_12752172_byte_174_page_original_fits_existing_target_bounds(native_intake):
    s = native_intake
    receipt_start = await start(s)
    original = s.originals.receipts[receipt_start.ingestion_id]
    specimen = replace(original, size=12_752_172)
    s.originals.receipts[receipt_start.ingestion_id] = specimen
    from regional_knowledge.kaggle_importer import MAX_BYTES, MAX_PAGES
    assert 12_752_172 <= MAX_BYTES and 174 <= MAX_PAGES
    spec = OriginalIntakeSpec(
        specimen.source_id, specimen.ingestion_id, specimen.request_id,
        "synthetic-original", expected_size=specimen.size,
        max_bytes=128 * 1024 * 1024, max_pages=2500,
    )
    assert spec.expected_size == 12_752_172
    assert specimen.page_count == 174
    async with s.importer._access(s.principal) as access:
        row = s.importer._row(s.principal.subject, receipt_start.ingestion_id)
        confirmed = await s.importer._receipt(access, row)
    assert confirmed.size == specimen.size and confirmed.page_count == 174
