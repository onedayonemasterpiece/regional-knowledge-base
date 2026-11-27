import json

import pytest
from jsonschema import Draft202012Validator

from regional_knowledge.server import build_server


@pytest.mark.asyncio
async def test_import_nested_fields_are_visible_without_reference_resolution(monkeypatch):
    monkeypatch.setenv("RKB_DEV_NOAUTH", "1")
    server = build_server()
    declaration = next(t for t in await server.list_tools() if t.name == "book_ingest")
    published = declaration.input_schema
    original = server._tool_manager.get_tool("book_ingest").fn_metadata.arg_model.model_json_schema()
    # Connected clients currently render referenced list elements as Array<unknown>.
    # Teach the exact existing DTO fields inline; keep execution validation intact.
    assert '"$ref"' not in json.dumps(published)
    assert "$defs" not in published
    page = published["properties"]["pages"]["anyOf"][0]["items"]
    region = page["properties"]["regions"]["items"]
    assert {"region_key", "kind", "bbox", "reading_order"} <= set(region["required"])
    story = published["properties"]["story_candidates"]["anyOf"][0]["items"]
    evidence = story["properties"]["evidence_refs"]["items"]
    assert evidence["properties"]["page_id"]["minLength"] == 36
    assert evidence["additionalProperties"] is False
    Draft202012Validator.check_schema(published)
    # Flattening changes representation, not accepted/rejected fictional arguments.
    fictional_stage = {
        "command": "stage",
        "pages": [{"page_id": "00000000-0000-4000-8000-000000000001", "physical_page_index": 0,
                   "regions": [{"region_key": "r1", "kind": "body", "bbox": {"left": 0, "top": 0, "right": 100, "bottom": 100}, "reading_order": 0}]}],
        "story_candidates": [{"candidate_key": "story1", "title": "Fiction", "summary": "Fictional episode", "proposition": "Fictional event",
                              "evidence_refs": [{"page_id": "00000000-0000-4000-8000-000000000001", "region_key": "r1", "chunk_key": "c1"}]}],
    }
    assert Draft202012Validator(published).is_valid(fictional_stage)
    server._tool_manager.get_tool("book_ingest").fn_metadata.arg_model.model_validate(fictional_stage)
    for arguments in (
        fictional_stage,
        {"command": "status", "ingestion_id": "fictional"},
        {"command": "stage", "pages": [{"regions": [{"kind": "invented"}]}]},
        {"command": "stage", "story_candidates": [{"evidence_refs": [{"page_id": "short"}]}]},
        {"command": "stage", "pages": None, "story_candidates": None},
    ):
        assert Draft202012Validator(original).is_valid(arguments) == Draft202012Validator(published).is_valid(arguments)
