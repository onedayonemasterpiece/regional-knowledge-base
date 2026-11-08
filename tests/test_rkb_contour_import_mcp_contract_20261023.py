"""Owner-intake MCP schema and authorization against synthetic originals.

Ordinary owner-only imports are shared-searchable with an owner attestation,
not a per-book legal questionnaire. Private Case and original grants remain
separate from the import capability.
"""
from dataclasses import replace
from copy import deepcopy
from pydantic import ValidationError

import pytest
from mcp.server.mcpserver.exceptions import ToolError

from regional_knowledge.contracts import ChatFile, Principal
from regional_knowledge.stage_file import MAX_STAGE_FILE_BYTES
from regional_knowledge.source_stage import MAX_BATCH_BYTES, MAX_BATCH_PAGES, validate_stage_payload
from regional_knowledge.story_contracts import StageStoryCandidateInput
from test_kaggle_source_importer import options
from regional_knowledge.original_intake import OriginalIntakeSpec
from regional_knowledge.server import build_server, _ACCEPTED_PAGE_CORRECTION_INSTRUCTIONS
from test_participants import people
from test_read_authority import auth
from test_kaggle_public_backend import target
from test_kaggle_native_source_importer import native_intake, start


@pytest.mark.asyncio
async def test_native_bibliography_guide_uses_existing_metadata_only_correction(native_intake, monkeypatch):
    import regional_knowledge.server as module
    from regional_knowledge.contracts import BookIngestOutput, CATALOG_STAGE_GUIDE
    s = native_intake
    book = await start(s)
    monkeypatch.setattr(module, '_principal', lambda: s.principal)
    captured = []

    async def ingest(**kwargs):
        captured.append(kwargs)
        return BookIngestOutput(state='processing', message='Synthetic accepted text',
            ingestion_id=book.ingestion_id, next_action='continue_pages')

    monkeypatch.setattr(s.startup.backend, 'book_ingest', ingest)
    server = build_server(target_startup=s.startup)
    tool = next(t for t in await server.list_tools() if t.name == 'book_ingest')
    page = dict(page_id=book.ingestion_id, physical_page_index=0, layout_kind='cover',
        regions=[], source_material='visual_reviewed', source_review_note='Synthetic cover reviewed')
    first = dict(command='stage', ingestion_id=book.ingestion_id, pages=[page], chunks=[],
        source_import={'research': {'state': 'pending', 'continuation': {
            'tool': 'book_pages', 'arguments': {'ingestion_id': book.ingestion_id,
                'cursor': '1', 'batch_size': 1}}}})
    # The existing adapter really rejects the old instructed combination.
    mixed = {**first, 'metadata': {'title': 'Observed synthetic title'}}
    with pytest.raises(ToolError, match='"reason":"domain_error"'):
        await server._tool_manager.call_tool('book_ingest', mixed, None)
    assert captured == []
    # Follow the advertised first-stage instruction, rather than merely testing
    # that one preferred phrase appears in a description.
    if 'include observed bibliography in that normal stage metadata' in tool.description:
        first = mixed
    await server._tool_manager.call_tool('book_ingest', first, None)
    assert captured[0]['source_import'].research.model_dump() == first['source_import']['research']
    assert set(captured[0]['payload']) == {'pages', 'chunks', 'poi_facts', 'poi_media_links', 'story_candidates'}
    for guide in (tool.description, book.message):
        assert guide.count(CATALOG_STAGE_GUIDE) == 1
        assert 'After the first TEXT is APPLIED' in guide
        assert 'metadata.catalog_update={expected_revision:1' in guide
        assert 'without pages/chunks/source_import' in guide
    # After accepted TEXT, observed bibliography uses the already-supported
    # metadata-only shape. No source, page or research payload is mixed into it.
    update = dict(expected_revision=1, title='Observed synthetic title',
        authors=['Synthetic author'], publication_year=1901, language='de',
        catalog={'publisher': 'Synthetic publisher', 'cover_page': 0, 'cover_kind': 'cover'})
    await server._tool_manager.call_tool('book_ingest', dict(command='stage',
        ingestion_id=book.ingestion_id, metadata={'catalog_update': update}), None)
    assert captured[1]['payload'] == {'catalog_update': update}
    assert 'source_import' not in captured[1]


@pytest.mark.asyncio
async def test_target_mcp_advertises_native_book_bounds_and_complete_story_input(native_intake):
    server = build_server(target_startup=native_intake.startup)
    tools = {tool.name: tool for tool in await server.list_tools()}
    ingest = tools["book_ingest"]
    schema = ingest.input_schema
    props = schema["properties"]
    assert {"source_import", "metadata", "story_candidates"} <= set(props)

    # Resolve actual JSON Schema references and Optional[T] unions: searching
    # serialized descriptions for a string can pass when the field is absent.
    top_import = _non_null(schema, props["source_import"])
    rights = _non_null(schema, top_import["properties"]["public_rights"])
    assert {"rights_status", "evidence", "policy_version"} <= set(rights["required"])
    assert rights["properties"]["evidence"]["type"] == "object"
    assert "research" in top_import["properties"]

    # Both real server-supported input locations must remain representable.
    metadata = _non_null(schema, props["metadata"])
    nested_import = _non_null(schema, metadata["properties"]["source_import"])
    assert nested_import["type"] == "object"
    assert nested_import["maxProperties"] == 2

    candidates = _non_null(schema, props["story_candidates"])
    assert candidates["type"] == "array"
    assert candidates["maxItems"] == 12
    story = _non_null(schema, candidates["items"])
    assert "basic_variant" in story["properties"]
    assert "basic_variant" not in story.get("required", [])
    assert set(story["properties"]["narrative_outcome"]["enum"]) == {
        "needs_context", "facts_only",
    }
    variant = _non_null(schema, story["properties"]["basic_variant"])
    assert {"body", "blocks"} <= set(variant["required"])
    assert variant["properties"]["body"]["type"] == "string"
    blocks = _non_null(schema, variant["properties"]["blocks"])
    assert blocks["type"] == "array" and blocks["minItems"] == 1
    block = _non_null(schema, blocks["items"])
    assert {"start", "end", "evidence_indexes"} <= set(block["required"])

    from regional_knowledge.kaggle_importer import MAX_BYTES, MAX_PAGES
    assert MAX_BYTES == 128 * 1024 * 1024
    assert MAX_PAGES == 2500
    assert "128 MiB" in ingest.description
    assert "2500 physical pages" in ingest.description
    assert "no per-book public_rights declaration" in ingest.description
    assert "narrative_outcome=needs_context or facts_only" in candidates["description"]
    assert "When evidence is insufficient, OMIT basic_variant" in candidates["description"]
    assert "2 MiB and 32 pages" not in server.instructions


@pytest.mark.asyncio
async def test_native_chunk_ceiling_in_mcp_guidance_matches_runtime(native_intake):
    from regional_knowledge.kaggle_source_materializer import MAX_TOKENS

    server = build_server(target_startup=native_intake.startup)
    ingest = next(tool for tool in await server.list_tools() if tool.name == "book_ingest")
    pages = next(tool for tool in await server.list_tools() if tool.name == "book_pages")
    for guidance in (ingest.description, pages.description):
        assert f"at most {MAX_TOKENS} pinned BGE-M3 tokenizer tokens" in guidance
        assert "complete native input (joined selected source region/span text, including selected headings and [Footnote] labels/text)" in guidance
        assert "chunk.title is metadata, not added separately" in guidance
        assert "Split longer thoughts at complete sentences" in guidance


@pytest.mark.asyncio
@pytest.mark.parametrize("profile,tool_name", [("full", "search"), ("live", "knowledge_search")])
async def test_native_search_tools_list_preserves_answer_and_pending_rules(native_intake, monkeypatch, profile, tool_name):
    server = build_server(profile=profile, target_startup=native_intake.startup)
    tools = {tool.name: tool for tool in await server.list_tools()}
    description = tools[tool_name].description
    for phrase in (
        "ONE direct answer in the language of the user's question",
        "Источники:", "В доступной базе подтверждений не найдено",
        "Не удалось проверить базу сейчас", "startup.message_ru", "measured ETA",
        "immediately", "this turn", "keep calling startup_status",
        "startup_ref", "until ready", "rerun this exact search", "with its original arguments",
        "The original read was not queued", "do not mint a second launch or retry UNKNOWN effects",
    ):
        assert phrase in description, (tool_name, phrase)
    if tool_name == "search":
        assert "at most 6 sequential search/fetch calls" in description
        assert "startup_status checks for the same cold start do not count" in description
    monkeypatch.setenv("RKB_DEV_NOAUTH", "1")
    legacy = build_server(profile=profile, backend_profile="legacy")
    legacy_tools = {tool.name: tool for tool in await legacy.list_tools()}
    assert "ONE direct answer" in legacy.instructions
    assert "ONE direct answer" not in legacy_tools[tool_name].description


@pytest.mark.asyncio
async def test_native_mcp_guidance_survives_540_char_client_cutoff(native_intake):
    server = build_server(target_startup=native_intake.startup)
    tools = {tool.name: tool for tool in await server.list_tools()}
    prefix = server.instructions[:540]
    assert len(server.instructions) <= 540
    assert prefix.startswith("Mandatory import checklist:\n1. ")
    assert prefix.count("\n") == 9  # title, seven numbered rules, final pointer
    assert prefix.endswith("Full rules in tool descriptions.\n")
    for phrase in ("One reader", "never blindly replay accepted pages",
                   "source_text verbatim", "[?]", "BGE-M3", "512 tokens",
                   "disagreement_search/get/edit", "book_pages(cursor)",
                   "next_cursor is a hint", "status", "catalog_update"):
        assert phrase in prefix, phrase
    assert prefix.index("never blindly replay accepted pages") < prefix.index("source_text verbatim") < prefix.index("BGE-M3")
    for name in ("book_ingest", "book_pages"):
        description = tools[name].description
        for phrase in ("Transcribe source_text verbatim",
                       "at most 512 pinned BGE-M3 tokenizer tokens",
                       "complete native input (joined selected source region/span text, including selected headings and [Footnote] labels/text)",
                       "disagreement_search", "disagreement_get", "disagreement_edit",
                       "do not search Cases on pages without such a contradiction"):
            assert phrase in description, (name, phrase)
    assert "Search/fetch checklist:" in tools["search"].description
    assert "Follow the search checklist." in tools["fetch"].description

@pytest.mark.asyncio
async def test_registered_native_book_ingest_instruction_is_truthful_and_legacy_unchanged(native_intake, monkeypatch):
    native = build_server(target_startup=native_intake.startup)
    native_tools = {tool.name: tool for tool in await native.list_tools()}
    imported = native_tools["book_ingest"]
    native_text = imported.description
    # Real tools/list carries the full correction contract exactly once.
    for phrase in (
        'Ordinary continuation: never blindly replay', 'ANY previously accepted physical page',
        'NEW region_key', 'NEW chunk_key', 'Never edit source_text',
        'source_review_note (1-500 characters)',
    ):
        assert phrase in native_text, phrase
    assert native_text.count(_ACCEPTED_PAGE_CORRECTION_INSTRUCTIONS) == 1
    assert native_text.count('ANY previously accepted physical page') == 1
    assert 'Revising an accepted book is unsupported in this profile' not in native_text
    # MCP initialize is frequently truncated; keep only the short point 2 there.
    assert '2. Ordinary continuation: never blindly replay accepted pages' in native.instructions
    assert 'ANY previously accepted physical page' not in native.instructions
    assert 'NEW region_key' not in native.instructions
    assert 'NEW chunk_key' not in native.instructions
    assert 'Revising an accepted book is unsupported in this profile' not in native.instructions
    assert imported.title == "Import and continue an attached scanned book"
    for phrase in ("book_ingest(command=start)", "book_pages (default 1, maximum 4",
                   "book_ingest(command=stage)", "after actual accepted text-packet apply",
                   "book_ingest(command=finalize) means only reading ended",
                   "correct and retry that page under the SAME ingestion",
                   "optional semantic, story and illustration enrichment does not undo accepted text",
                   "Return processing/wait/UNKNOWN/pending truthfully",
                   "On the first stage set source_import.research",
                   "pending with the exact owned book_pages continuation",
                   "or an already accepted chunk_id"):
        assert phrase in native_text, phrase
    assert native_text.startswith("If a reviewed page contradicts itself")
    assert "do not search Cases on pages without such a contradiction" in native_text[:500]
    assert "When disagreement_search is registered" not in native_text
    assert len(native.instructions) <= 540
    assert native.instructions.startswith("Mandatory import checklist:\n")
    assert native.instructions.endswith("Full rules in tool descriptions.\n")
    assert "On first stage supply source_import.research" not in native.instructions
    assert "exact page/region/quote evidence" not in native.instructions
    # Existing stage wire supports page-scoped regions/exact source spans.
    # Model chooses meaning boundaries; 256 tokens is not a new hard cap and a
    # 1-8-page packet is not a chunk or evidence of deployed tail handling.
    chunk_guidance = native_text
    assert chunk_guidance.count("Build semantically coherent source chunks") == 1
    for expected in (
        "model-chosen boundaries at changes of thought",
        "about 256 BGE tokens", "roughly 800-1000 characters",
        "as a rough estimate, not a ceiling", "within the exact token bound",
        "not a mandatory chunk boundary", "Preserve whole sentences",
        "split a long or visually unbroken paragraph at a complete sentence",
        "exact span_refs and source_text offsets",
        "combine short adjacent paragraphs about the same idea",
        "source-backed region_refs or span_refs",
        "Read continuing text with book_pages",
        "one book_ingest(stage) packet contains 1-8 issued, visually reviewed pages",
        "not one oversized chunk", "a completed thought spans pages",
        "ordered region_refs OR exact span_refs", "both real page_ids",
        "original page/region/span provenance", "never invent unseen continuation",
        "force a whole packet into one chunk",
    ):
        assert expected in chunk_guidance, expected
    assert chunk_guidance.count("If book_ingest(status) returns next_action=continue_tail") == 1
    for expected in (
        "tail_refs (page_id, physical_page_index, region_id, region_key, source_text_sha256, start, end",
        "exact Unicode codepoint offsets in the original source region",
        "contiguous uncovered body suffix", "book_pages at next_cursor",
        "small, sentence-complete semantic chunks",
        "ordered exact span_refs or region_refs",
        "retaining prior page/region/span provenance",
        "For span_refs supply page_id, region_key, start and end",
        "the server derives source_text_sha256 from the known region text",
        "the model must not calculate or supply a hash",
        "Stage only newly issued, visually reviewed page(s) in a 1-8-page packet",
        "do not re-stage the accepted prior page",
        "split sentences, invent missing text",
        "system-chosen split/merge boundaries",
        "finalize while status still indicates continue_tail",
    ):
        assert expected in chunk_guidance, expected
    assert "When evidence is insufficient, OMIT basic_variant" in _non_null(imported.input_schema, imported.input_schema["properties"]["story_candidates"])["description"]
    assert "Finalization alone activates" not in native_text
    # Search and cold-start guidance stays in the registered search/status tools.
    assert "keep calling startup_status" in native_tools["search"].description
    assert "repeat the exact original search" in native_tools["startup_status"].description
    for action in ("disagreement_search", "disagreement_get",
                   "disagreement_edit(add_evidence)", "disagreement_edit(create)"):
        assert action in native_text, action
    for state in ("UNKNOWN", "Saved"):
        assert state in native_text
    assert "In entity_candidates, evidence may use page_id, region_key and chunk_key from this stage packet." in native_text
    assert "General Case discovery/search is not presently registered" not in native_text
    # Runtime-registered rules must be available in tools/list, not only initialize.
    instruction = native_text
    story_rules = _non_null(imported.input_schema, imported.input_schema["properties"]["story_candidates"])["description"]
    assert instruction.count("Story rules: see book_ingest.story_candidates field description.") == 1
    assert "Story body: write" not in instruction
    assert "Story rules: see book_ingest.story_candidates field description." in native_text
    for required in (
        "max 12 per stage", "2-6 connected Russian sentences",
        "read, voiced and posted", "Map complete basic_variant.blocks",
        "including handbooks and lexicons", "Согласно справочнику",
        "По изложению книги", "По данным авторов",
        "По сообщению <автора>", "<Автор> сообщает/перечисляет/отмечает",
        "the author writes", "author, page and footnote go in evidence_refs",
        "attribute only opinion, estimate, legend, recollection or dispute AFTER",
        "exact source names, dates, numbers, historical places",
        "where/when when printed", "close with known outcome or honest uncertainty about the past itself",
        "never about the text or its author",
        "when a statement is a judgement, attribute it once inside that sentence",
        "its author's time, not 2026", "never invent causes, motives",
        "ONE body, not separate facts_only",
        "cite accepted earlier pages without restaging",
        "A lone dated fact, list or uses without development stays facts/observations",
        "first/invention with adjacent development",
        "Review whole page for episodes, with no story quota",
        "OMIT basic_variant and set narrative_outcome=needs_context or facts_only",
    ):
        assert required in story_rules, required
    for required in (
        "metadata.title", "metadata.authors", "metadata.publication_year",
        "metadata.language", "metadata.catalog.kind",
        "editors, translators, publisher, date, edition, identifiers and topics",
        "metadata.catalog.cover_page", "metadata.catalog.cover_kind",
        "zero-based physical page index", "leave unsupported fields unset",
    ):
        assert required in instruction, required
    assert "retain attributed people/events/places and a real body only with sufficient evidence" not in instruction
    # Old whole-book/unsupported command advice is not surfaced to the target.
    for obsolete in ("at most 8", "8 selected", "validate and finalize", "Finalization alone activates",
                     "reprocess", "rechunk", "story_reconcile"):
        assert obsolete not in native_text, obsolete
        assert obsolete not in native.instructions, obsolete

    # The native profile excludes unsupported repair commands. The legacy
    # profile retains its implemented commands and otherwise identical schema.
    monkeypatch.setenv("RKB_DEV_NOAUTH", "1")
    legacy = build_server(backend_profile="legacy")
    legacy_tools = {tool.name: tool for tool in await legacy.list_tools()}
    legacy_ingest = legacy_tools["book_ingest"]
    assert imported.name == legacy_ingest.name == "book_ingest"
    native_schema = deepcopy(imported.input_schema)
    legacy_schema = deepcopy(legacy_ingest.input_schema)
    native_commands = native_schema['properties']['command'].pop('enum')
    legacy_commands = legacy_schema['properties']['command'].pop('enum')
    assert native_commands == ['start', 'stage', 'validate', 'finalize', 'status']
    assert legacy_commands == ['start', 'reprocess', 'rechunk', 'stage', 'validate', 'finalize', 'status']
    native_story_description = _non_null(native_schema, native_schema["properties"]["story_candidates"]).pop("description")
    legacy_story_description = _non_null(legacy_schema, legacy_schema["properties"]["story_candidates"]).pop("description")
    assert native_story_description != legacy_story_description
    assert native_schema == legacy_schema  # Structural contract unchanged.
    for command in ('reprocess', 'rechunk'):
        with pytest.raises(ToolError, match='invalid_query'):
            await native.call_tool('book_ingest', {'command': command})
    assert imported.output_schema == legacy_ingest.output_schema
    assert imported.meta == legacy_ingest.meta == {"openai/fileParams": ["file"]}
    assert legacy_ingest.title == "Add, reprocess or continue a book"
    assert "Finalization alone activates" in legacy_ingest.description
    assert "book_find first" in legacy_ingest.description
    assert "book_ingest(reprocess)" in legacy.instructions
    assert "validate and finalize" in legacy.instructions
    assert legacy.instructions.startswith("Search and retrieve sourced regional knowledge.")
    assert "model review/stage, validate and finalize by following" in legacy.instructions
    assert "Stage at most 8" not in legacy.instructions
    assert "Finalized source activation" not in legacy.instructions
    # This fixture has only original intake, not a configured Case backend;
    # the PR330 Case-enabled MCP test separately checks search registration.
    assert "disagreement_search" not in native_tools
    pages = native_tools["book_pages"].input_schema["properties"]["batch_size"]
    assert pages["default"] == 1 and pages["minimum"] == 1 and pages["maximum"] == 4


@pytest.mark.asyncio
async def test_owner_start_without_rights_uses_existing_owner_attestation(native_intake):
    from regional_knowledge.kaggle_importer import OWNER_IMPORT_ATTESTATION
    s = native_intake
    result = await s.importer.book_ingest(
        command="start",
        principal=s.principal,
        file=ChatFile(file_id="fixture-owner-attested-original",
            download_url="https://files.example.test/synthetic.pdf",
            mime_type="application/pdf"),
        payload={"title": "Synthetic owner-attested original"},
        source_import=None,
    )
    assert result.state == "staged"
    assert s.importer._row(s.principal.subject, result.ingestion_id)["rights_json"] == OWNER_IMPORT_ATTESTATION
    assert len(s.originals.calls) == 1


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
    assert MAX_BYTES == 128 * 1024 * 1024 and MAX_PAGES == 2500
    assert 12_752_172 <= MAX_BYTES and 174 <= MAX_PAGES
    spec = OriginalIntakeSpec(
        specimen.source_id, specimen.ingestion_id, specimen.request_id,
        "synthetic-original", expected_size=specimen.size,
        max_bytes=MAX_BYTES, max_pages=MAX_PAGES,
    )
    assert spec.expected_size == 12_752_172
    assert specimen.page_count == 174
    async with s.importer._access(s.principal) as access:
        row = s.importer._row(s.principal.subject, receipt_start.ingestion_id)
        confirmed = await s.importer._receipt(access, row)
    assert confirmed.size == specimen.size and confirmed.page_count == 174


def _non_null(schema, field):
    """Follow MCP/Pydantic JSON Schema refs and its single non-null branch."""
    while True:
        if "$ref" in field:
            field = schema["$defs"][field["$ref"].rsplit("/", 1)[1]]
        elif "anyOf" in field:
            branches = [choice for choice in field["anyOf"]
                        if choice.get("type") != "null"]
            assert len(branches) == 1
            field = branches[0]
        else:
            return field


@pytest.mark.parametrize("outcome", ["needs_context", "facts_only"])
def test_story_without_sufficient_evidence_has_no_fabricated_body(outcome):
    data = dict(
        candidate_key="synthetic-harbour-episode",
        title="Synthetic harbour event",
        summary="Evidence only supports an attributed harbour observation.",
        proposition="An observation is attributed to a printed source.",
        evidence_refs=[dict(
            page_id="00000000-0000-0000-0000-000000000000",
            region_key="body",
            original_excerpt="Synthetic printed harbour observation.",
        )],
        narrative_outcome=outcome,
    )
    candidate = StageStoryCandidateInput.model_validate(data)
    assert candidate.basic_variant is None
    assert candidate.narrative_outcome == outcome

    body = "The printed synthetic source recounts a specific harbour episode."
    authored = {**data, "narrative_outcome": "needs_context",
                "basic_variant": dict(body=body, blocks=[dict(
                    start=0, end=len(body), evidence_indexes=[0])])}
    complete = StageStoryCandidateInput.model_validate(authored)
    assert complete.basic_variant.body == body
    assert complete.basic_variant.blocks[0].end == len(body)

    with pytest.raises(ValidationError):
        StageStoryCandidateInput.model_validate({
            **authored, "basic_variant": {"body": body},
        })
    with pytest.raises(ValidationError, match="Facts-only"):
        StageStoryCandidateInput.model_validate({
            **authored, "narrative_outcome": "facts_only",
        })


@pytest.mark.asyncio
@pytest.mark.parametrize("location", ["top_level", "metadata"])
async def test_mcp_accepts_each_existing_source_import_location(native_intake, monkeypatch, location):
    import regional_knowledge.server as source_server
    import regional_knowledge.read_issuance as read_issuance
    s = native_intake
    monkeypatch.setattr(source_server, "_principal", lambda: s.principal)
    # A direct unit call has no HTTP request context. Exercise actual MCP parsing,
    # owner authority and importer without binding a transport response.
    monkeypatch.setattr(read_issuance, "bind_request_output",
                        lambda context, calls: False)
    server = build_server(target_startup=s.startup)
    declared = options().model_dump(mode="json", exclude_none=True)
    arguments = dict(
        command="start",
        file=dict(
            file_id="synthetic-rights-attested",
            download_url="https://files.example.test/synthetic-rights-attested.djvu",
            mime_type="image/vnd.djvu",
        ),
        metadata=dict(title="Synthetic rights-attested 174-page original"),
    )
    if location == "top_level":
        arguments["source_import"] = declared
    else:
        arguments["metadata"]["source_import"] = declared

    await server.call_tool("book_ingest", arguments)
    assert len(s.originals.calls) == 1


@pytest.mark.asyncio
async def test_mcp_rejects_both_source_import_locations_before_original(native_intake, monkeypatch):
    import regional_knowledge.server as source_server
    s = native_intake
    monkeypatch.setattr(source_server, "_principal", lambda: s.principal)
    server = build_server(target_startup=s.startup)
    declared = options().model_dump(mode="json", exclude_none=True)
    with pytest.raises(ToolError, match="source_import_must_use_one_input_location"):
        await server.call_tool("book_ingest", dict(
            command="start",
            file=dict(
                file_id="synthetic-two-import-locations",
                download_url="https://files.example.test/synthetic-two-import-locations.djvu",
                mime_type="image/vnd.djvu",
            ),
            metadata=dict(title="Synthetic conflict", source_import=declared),
            source_import=declared,
        ))
    assert s.originals.calls == []


@pytest.mark.asyncio
async def test_stage_file_transport_and_native_canonical_envelope_have_distinct_caps(native_intake):
    server = build_server(target_startup=native_intake.startup)
    ingest = {tool.name: tool for tool in await server.list_tools()}["book_ingest"]
    assert MAX_STAGE_FILE_BYTES == 1024 * 1024
    assert MAX_BATCH_BYTES == 700 * 1024
    assert MAX_BATCH_PAGES == 8
    assert "<=1 MiB" in ingest.description
    assert "<=700 KiB" in ingest.description
    assert "INCLUDING issued image receipts" in ingest.description
    # Full canonical envelope, not just model text, counts against native cap.
    with pytest.raises(ValueError, match="bounded exact source stage bytes required"):
        validate_stage_payload(b"x" * (MAX_BATCH_BYTES + 1),
                               source_id="00000000-0000-0000-0000-000000000000")


@pytest.mark.asyncio
async def test_native_stage_shape_guide_survives_flattened_arrays_and_stages_without_hash(native_intake, monkeypatch):
    import json
    import regional_knowledge.server as source_server
    import regional_knowledge.read_issuance as read_issuance
    from test_kaggle_native_source_importer import batch, pending
    s = native_intake
    book = await start(s)
    monkeypatch.setattr(source_server, '_principal', lambda: s.principal)
    monkeypatch.setattr(read_issuance, 'bind_request_output', lambda context, calls: False)
    server = build_server(target_startup=s.startup)
    tool = next(t for t in await server.list_tools() if t.name == 'book_ingest')
    # Some connectors expose pages/chunks as Array<unknown>. Required nested
    # structure must remain discoverable in the tool's ordinary description.
    for name in tool.input_schema['$defs']['StageRegionInput']['required']:
        assert name in tool.description
    def without_nulls(value):
        if isinstance(value, dict):
            return {k: without_nulls(v) for k, v in value.items() if v is not None}
        if isinstance(value, list):
            return [without_nulls(v) for v in value]
        return value
    payload = without_nulls(batch(s, book, count=1))
    payload['pages'][0].pop('source_image_receipt_sha256')
    original_reads = list(s.originals.calls)
    await server.call_tool('book_ingest', dict(command='stage', ingestion_id=book.ingestion_id,
        pages=payload['pages'], chunks=payload['chunks'],
        source_import=options(research=True, ingestion_id=book.ingestion_id,
            state='pending').model_dump(mode='json')))
    accepted = json.loads((await pending(s, book)).command.payload)['model_payload']
    assert accepted['pages'][0]['regions'] == payload['pages'][0]['regions']
    assert accepted['chunks'] == payload['chunks']
    assert accepted['pages'][0]['source_image_receipt_sha256']
    assert s.originals.calls == original_reads


@pytest.mark.asyncio
async def test_native_status_exposes_stage_shape_without_tools_list_refresh(native_intake, monkeypatch):
    import regional_knowledge.server as source_server
    from regional_knowledge.read_issuance import _SCOPE_KEY
    from starlette.requests import Request
    from types import SimpleNamespace
    s = native_intake
    book = await start(s)
    monkeypatch.setattr(source_server, '_principal', lambda: s.principal)
    server = build_server(target_startup=s.startup)
    # Existing clients can retain an older tools/list. The ordinary response
    # itself carries the current structure, without any metadata refresh.
    request = Request({'type': 'http', 'method': 'POST', 'path': '/mcp', 'headers': [],
        'app': SimpleNamespace(state=SimpleNamespace()), 'state': {}})
    context = SimpleNamespace(request_context=SimpleNamespace(request=request, request_id='fictional-native-shape'))
    response = await server.call_tool('book_ingest', dict(command='status', ingestion_id=book.ingestion_id), context)
    issued = request.scope.pop(_SCOPE_KEY)[1]
    try:
        refreshed = await issued.gate.refresh(issued.gated)
        assert issued.render(refreshed.result).message == response.structured_content['message']
    finally:
        issued.release()
    message = response.structured_content['message']
    assert response.structured_content['next_action'] == 'continue_pages'
    assert message.startswith('Review issued pages and stage exact model input')
    from regional_knowledge.source_stage import MAX_BATCH_PAGES
    assert f'1-{MAX_BATCH_PAGES} issued, visually reviewed pages' in message
    assert 'separate from the book_pages request size' in message
    for field in ('region_key', 'kind', 'bbox', 'reading_order', 'chunk_key', 'region_refs'):
        assert field in message


@pytest.mark.asyncio
@pytest.mark.parametrize('state,action,warnings', [
    ('staged', 'continue_pages', ['native_source_stage_requires_review']),
    ('needs_review', 'continue_pages', []),
    ('processing', 'wait', []),
    ('staged', 'continue_tail', []),
    ('finalized', 'done', []),
])
async def test_shape_guide_keeps_precise_rejection_semantic_and_wait_messages(native_intake, state, action, warnings):
    from regional_knowledge.contracts import BookIngestOutput
    s = native_intake
    book = await start(s)
    original = BookIngestOutput(ingestion_id=book.ingestion_id, document_id=book.document_id,
        state=state, next_action=action, message='Existing precise response.', warnings=warnings)
    async with s.importer._access(s.principal) as access:
        result = await s.importer._reply(access, original)
    assert result is original
    assert result.message == 'Existing precise response.' and result.warnings == warnings
