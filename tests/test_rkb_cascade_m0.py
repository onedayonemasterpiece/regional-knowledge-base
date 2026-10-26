"""M0: blind-policy adaptation must not contaminate the frozen pilot."""
import importlib.util
import json
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts/benchmarks/rkb_cascade_pilot_summary.py"
SPEC = importlib.util.spec_from_file_location("rkb_cascade_pilot_summary", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)
m1_record_view = MODULE.m1_record_view


def test_m1_scoped_search_and_neighbor_fetch_are_in_read_packet():
    record = {
        "family_id": "E01", "policy": "V4s",
        "calls": [
            {"operation": "search", "args": {"query": "SECRET QUERY"},
             "response": {"structuredContent": {
                 "status": "ok", "results": [{"id": "p1"}, {"id": "p2"}]}}},
            {"operation": "search", "args": {"query": "SECRET", "document_ids": ["SECRET SOURCE"]},
             "response": {"structuredContent": {
                 "status": "ok", "results": [{"id": "p3", "text": "SECRET BOOK TEXT"}]}}},
            {"operation": "fetch", "args": {"id": "p3"},
             "response": {"structuredContent": {"status": "ok", "text": "SECRET BOOK TEXT"}}},
            {"operation": "fetch",
             "args": {"source_id": "SECRET SOURCE", "page_start": 17, "page_end": 18},
             "response": {"structuredContent": {"status": "ok", "chunks": [
                 {"chunk_id": "p4", "text": "SECRET LEFT"},
                 {"chunk_id": "p5", "text": "SECRET RIGHT"}]}}},
        ],
        "answer": "SECRET ANSWER", "claims": ["claim 1", "claim 2"],
        "citations": [{"urn": "SECRET SOURCE"}],
        "judgment": {"search_complete": False, "read_complete": True,
                     "answer_supported": True, "citations_supported": False},
    }
    view = m1_record_view(record)
    assert view["search"] == {
        "calls": 2, "scoped_calls": 1, "returned_candidate_occurrences": 3,
        "text_exposed_units": 1, "complete_evidence_set": False}
    assert view["read"] == {
        "fetch_calls": 2, "neighbor_fetch_calls": 1, "fetch_text_units": 3,
        "neighbor_text_units": 2, "total_text_units": 4, "complete_evidence_set": True}
    assert view["answer"] == {
        "present": True, "claim_count": 2, "supported_complete": True}
    assert view["citations"] == {"count": 1, "support_complete": False}
    assert "SECRET" not in json.dumps(view)


def test_m1_unknown_not_false_and_blind_variant_untouched():
    view = m1_record_view({
        "family_id": "N04", "policy": "B",
        "calls": [{"operation": "search", "args": {"document_ids": []},
                   "response": {"structuredContent": {"status": "ok", "results": []}}}],
    })
    assert view["variant"] == "B"
    assert view["search"]["scoped_calls"] == 0
    assert view["search"]["returned_candidate_occurrences"] == 0
    assert view["search"]["complete_evidence_set"] is None
    assert view["answer"]["present"] is None
    assert view["citations"]["count"] is None
    assert view["citations"]["support_complete"] is None

    failed = m1_record_view({
        "family_id": "E02", "policy": "V0",
        "calls": [{"operation": "search", "args": {},
                   "response": {"structuredContent": {"status": "error"}}},
                  {"operation": "fetch", "args": {},
                   "response": {"structuredContent": {"status": "timeout"}}}],
    })
    assert failed["search"]["returned_candidate_occurrences"] is None
    assert failed["read"]["total_text_units"] is None
    assert failed["read"]["fetch_text_units"] is None
