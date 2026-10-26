"""Contract boundaries only: no model, runtime, network or authority creation."""
import json

import pytest
from pydantic import ValidationError

from regional_knowledge.kaggle_protocol import (
    CancelQuery, EvidenceItem, Hello, Query, QueryResult, TrustedActorContext,
    decode_envelope, encode_envelope,
)


def query_payload():
    return {
        "type": "QUERY", "request_id": "request-1", "connection_generation": 2,
        "turn_generation": 3, "query": "История города", "min_data_seq": 8,
        "deadline_ms": 250, "actor": {
            "person_id": "verified-person", "audience": "personal", "auth_epoch": 4,
            "visibility_epoch": 5,
        },
    }


def wire(payload):
    return json.dumps(payload, ensure_ascii=False).encode()


def item(**updates):
    payload = {
        "id": "item-1", "kind": "fact", "text": "Evidence",
        "dependencies": {"source_ids": ("source-a", "source-b"),
                         "variant_refs": (), "grant_refs": ("grant-1",),
                         "source_revisions": tuple({"source_id": source, "content_revision": 1,
                             "eligibility_revision": 1} for source in ("source-a", "source-b"))},
    }
    payload.update(updates)
    return EvidenceItem.model_validate(payload)


def result(**updates):
    payload = {
        "request_id": "request-1", "connection_generation": 2, "turn_generation": 3,
        "status": "ok", "mode": "hybrid", "observed_data_seq": 8,
        "observed_projection_seq": 7,
    }
    payload.update(updates)
    return QueryResult.model_validate(payload)


def test_query_roundtrip_default_limits_and_immutable_authority_projection():
    query = decode_envelope(wire(query_payload()))
    assert isinstance(query, Query)
    assert query.evidence_limit == 3
    assert query.filters.source_ids is None
    assert decode_envelope(encode_envelope(query)) == query
    with pytest.raises(ValidationError):
        query.actor.person_id = "other"


@pytest.mark.parametrize("update", [
    {"readonly": True}, {"operation": "mutation"}, {"protocol_version": 1},
    {"protocol_version": 2.0}, {"operation_version": 2.0},
    {"operation_version": True},
    {"query": "x" * 1001}, {"query": " "}, {"deadline_ms": 0},
    {"deadline_ms": True}, {"deadline_ms": 1.5}, {"deadline_ms": float("inf")},
    {"connection_generation": 0}, {"min_data_seq": -1}, {"evidence_limit": 11},
    {"evidence_limit": "3"}, {"filters": {"arbitrary_sql": "SELECT *"}},
    {"filters": {"source_ids": []}},
    {"filters": {"source_ids": [str(i) for i in range(33)]}},
    {"filters": {"source_ids": ["a", "a"]}},
    {"filters": {"kinds": ["vector"]}},
    {"actor": {"person_id": "owner", "audience": "all", "auth_epoch": 0,
               "visibility_epoch": 0, "grants": ["owner"]}},
])
def test_query_rejects_unbounded_unknown_or_coerced_input(update):
    payload = query_payload()
    payload.update(update)
    with pytest.raises(ValueError):
        decode_envelope(wire(payload))


def test_ten_evidence_allowed_but_default_three():
    payload = query_payload()
    payload['evidence_limit'] = 10
    assert decode_envelope(wire(payload)).evidence_limit == 10
    ten = tuple(item(id=f'item-{i}') for i in range(10))
    assert len(decode_envelope(encode_envelope(result(evidence=ten))).evidence) == 10
    with pytest.raises(ValueError):
        result(evidence=ten + (item(id='item-10'),))


def test_query_unicode_cap_and_exact_bytes_no_truncation():
    payload = query_payload()
    payload["query"] = "𐀀" * 1000
    assert decode_envelope(wire(payload)).query == payload["query"]
    payload["filters"] = {"source_ids": ["s" * 256 + str(i) for i in range(32)]}
    with pytest.raises(ValueError):
        decode_envelope(wire(payload))
    # The raw frame cap includes whitespace, not just canonical re-encoded JSON.
    with pytest.raises(ValueError, match="query_too_large"):
        decode_envelope(wire(query_payload()) + b" " * 16384)


def test_codec_rejects_duplicate_fields_unknown_types_and_nonobjects():
    for data in [b'{"type":"QUERY","type":"HELLO"}', b'[]',
                 b'{"type":"DISPATCH"}', b'\xff']:
        with pytest.raises(ValueError):
            decode_envelope(data)
    with pytest.raises(TypeError):
        decode_envelope("{}")


def test_success_with_no_evidence_is_explicit_completed_search():
    empty = result()
    assert empty.evidence == ()
    assert decode_envelope(encode_envelope(empty)) == empty


@pytest.mark.parametrize("update", [
    {"status": "unavailable"}, {"status": "degraded"}, {"mode": None},
    {"mode": "lexical_degraded"}, {"reason": "not_ready"},
    {"context_gap": True}, {"prior_write_pending": True},
    {"observed_data_seq": None}, {"observed_projection_seq": None},
    {"status": "denied", "mode": None, "reason": "access_denied", "evidence": (item(),)},
])
def test_result_cannot_claim_success_for_missing_or_partial_search(update):
    with pytest.raises(ValueError):
        result(**update)


def test_degradation_and_unknown_cursors_are_honest():
    degraded = result(status="degraded", mode="hybrid", reason="bounded_retrieval")
    assert decode_envelope(encode_envelope(degraded)) == degraded
    unavailable = result(status="unavailable", mode=None, reason="not_ready",
                         observed_data_seq=None, observed_projection_seq=None)
    assert unavailable.observed_data_seq is None
    with pytest.raises(ValueError):
        result(status="degraded", mode="hybrid", reason="dense_unavailable")


@pytest.mark.parametrize('evidence', [(), (item(),)])
def test_withdrawal_gate_can_report_empty_or_retained_subset(evidence):
    response = result(status='degraded', mode='hybrid', reason='evidence_withdrawn',
                      evidence=evidence)
    assert decode_envelope(encode_envelope(response)) == response


def test_exact_composite_dependencies_survive_roundtrip():
    evidence = item(kind="story", exact_variant_ref="variant-1", text_form="excerpt",
                    continuation="scoped-locator",
                    dependencies={"source_ids": ("a", "b"), "variant_refs": ("variant-1",),
                                  "grant_refs": ("grant-1", "grant-2"),
                                  "source_revisions": tuple({"source_id": source, "content_revision": 1,
                                      "eligibility_revision": 1} for source in ("a", "b"))})
    parsed = decode_envelope(encode_envelope(result(evidence=(evidence,))))
    assert parsed.evidence[0] == evidence


@pytest.mark.parametrize("updates", [
    {"text": "a" * 8001}, {"text_form": "excerpt"}, {"kind": "story"},
    {"exact_variant_ref": "absent"},
    {"dependencies": {"source_ids": (), "variant_refs": (), "grant_refs": ()}},
    {"dependencies": {"source_ids": ("a",)}},
    {"metadata": {"vector": [1.0]}},
    {"metadata": {"citation": "𐀀" * 4000}},
])
def test_evidence_rejects_incomplete_or_oversize_output(updates):
    with pytest.raises(ValueError):
        item(**updates)


def test_total_text_and_response_byte_caps_are_independent():
    with pytest.raises(ValueError, match="total evidence"):
        result(evidence=tuple(item(id=str(i), text="a" * 8000) for i in range(4)))
    huge_dependencies = tuple("s" * 250 + str(i) for i in range(1100))
    large = result(evidence=(item(dependencies={"source_ids": ("source-a",),
        "source_revisions": ({"source_id": "source-a", "content_revision": 1, "eligibility_revision": 1},),
        "variant_refs": (), "grant_refs": huge_dependencies}),))
    with pytest.raises(ValueError, match="response too large"):
        encode_envelope(large)
    with pytest.raises(ValueError):
        result(evidence=tuple(item(id=str(i)) for i in range(6)))


def test_cancel_has_identity_and_does_not_claim_compute_termination():
    cancel = CancelQuery(request_id="r", connection_generation=1, turn_generation=0,
                         reason="caller_cancelled")
    assert decode_envelope(encode_envelope(cancel)) == cancel
    with pytest.raises(ValidationError):
        CancelQuery.model_validate({**cancel.model_dump(), "compute_stopped": True})


def test_hello_preserves_independent_unknown_readiness():
    hello = Hello(run_id="run", provider_version="v1", launch_nonce="nonce",
                  runtime_incarnation="incarnation", connection_generation=1,
                  schema_version=None, model_version=None, observed_snapshot=None,
                  observed_data_seq=None, text_search_ready=True,
                  archive_read_ready=False, proof_ready=None)
    assert decode_envelope(encode_envelope(hello)) == hello
    assert hello.proof_ready is None
