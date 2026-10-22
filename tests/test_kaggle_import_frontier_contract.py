"""M2: same-pass needs_context must have a reachable, owner-scoped next read.

Only the schema is exercised here: the native stage must additionally compare
the supplied ingestion ID with the current owner record and prove source pages.
This is not a synthetic claim that a real PDF/DjVu was imported or Saved.
"""
from __future__ import annotations

from uuid import uuid4

import pytest
from pydantic import ValidationError

from regional_knowledge.kaggle_import_contracts import SourceResearchInput, TargetSourceImportInput


ID = "11111111-1111-4111-8111-111111111111"


def continuation(**arguments):
    return {"tool": "book_pages", "arguments": {"ingestion_id": ID, **arguments}}


def test_complete_has_no_continuation_and_pending_keeps_exact_reader():
    complete = SourceResearchInput.model_validate({"state": "complete"})
    assert complete.continuation == {}
    pending = SourceResearchInput.model_validate(
        {"state": "pending", "continuation": continuation(cursor="0", batch_size=1)}
    )
    assert pending.continuation["arguments"]["ingestion_id"] == ID
    assert pending.model_dump(mode="json") == SourceResearchInput.model_validate_json(
        pending.model_dump_json()
    ).model_dump(mode="json")
    # Importer still checks current actor, run/lease and OWNED ingestion_id at
    # stage admission; the schema may not mint an authority or a provider receipt.
    envelope = TargetSourceImportInput.model_validate({"research": pending.model_dump()})
    assert envelope.research == pending
    assert envelope.public_rights is None


@pytest.mark.parametrize(
    "value",
    [
        {},
        {"tool": "fetch", "arguments": {"id": "source"}},
        {"tool": "book_pages", "arguments": {}},
        {"tool": "book_pages", "arguments": {"ingestion_id": "not-a-uuid"}},
        {"tool": "book_pages", "arguments": {"ingestion_id": ID.upper()}},
        {"tool": "book_pages", "arguments": {"ingestion_id": ID, "file": "unsupported"}},
        {"tool": "book_pages", "arguments": {"ingestion_id": ID, "cursor": -1}},
        {"tool": "book_pages", "arguments": {"ingestion_id": ID, "cursor": ""}},
        {"tool": "book_pages", "arguments": {"ingestion_id": ID, "cursor": "x" * 257}},
        {"tool": "book_pages", "arguments": {"ingestion_id": ID, "cursor": "0\nnext"}},
        {"tool": "book_pages", "arguments": {"ingestion_id": ID, "batch_size": 2}},
        {"tool": "book_pages", "arguments": {"ingestion_id": ID, "batch_size": True}},
        {"tool": "book_pages", "arguments": [ID]},
        {"note": "later"},
        {"tool": "book_pages", "arguments": {"ingestion_id": ID}, "note": "x" * 9000},
    ],
)
def test_pending_rejects_unreachable_or_unbounded_continuation(value):
    with pytest.raises(ValidationError):
        SourceResearchInput.model_validate({"state": "pending", "continuation": value})


def test_complete_rejects_dangling_page_continuation():
    with pytest.raises(ValidationError, match="completed research"):
        SourceResearchInput.model_validate({"state": "complete", "continuation": continuation()})


def test_prevalidation_does_not_accept_wrong_field_types():
    with pytest.raises(ValidationError):
        SourceResearchInput.model_validate({"state": "pending", "continuation": []})
    with pytest.raises(ValidationError):
        SourceResearchInput.model_validate({"state": "pending", "continuation": continuation(cursor=None)})
    with pytest.raises(ValidationError):
        SourceResearchInput.model_validate({"state": "pending", "continuation": continuation(batch_size=1.0)})


def test_canonical_uuid_does_not_forge_ownership():
    other = str(uuid4())
    pending = SourceResearchInput(state="pending", continuation={
        "tool": "book_pages", "arguments": {"ingestion_id": other}
    })
    assert pending.continuation["arguments"]["ingestion_id"] == other
    # Valid UUID syntax is not proof that it belongs to the caller. Current
    # ownership and source/pixel receipt checks MUST remain in native stage.
