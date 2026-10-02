import json
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID, uuid5

import httpx
import pytest

from regional_knowledge.contracts import Principal, StagePageInput, StagePoiFactInput
from regional_knowledge.stage_graph import StagedGraph, compile_model_stage, merge_stage
from regional_knowledge.stage_service import _build_poi_events


DOCUMENT_ID = "11111111-1111-1111-1111-111111111111"
AUTHOR_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
REVISION = 1


def principal() -> Principal:
    return Principal(
        subject="22222222-2222-2222-2222-222222222222",
        client_id="chatgpt-knowledge",
        issuer="https://issuer.example/auth/v1",
        access_token="user-jwt",
    )


def page_id(index: int = 0) -> str:
    return str(uuid5(UUID(DOCUMENT_ID), f"page:{REVISION}:{index}"))


def page() -> StagePageInput:
    return StagePageInput(
        page_id=page_id(),
        physical_page_index=0,
        regions=[
            {
                "region_key": "body-1",
                "kind": "body",
                "bbox": {"left": 50, "top": 50, "right": 950, "bottom": 850},
                "reading_order": 0,
                "source_text": "Ворота построены в 1843 году.",
            }
        ],
    )


def poi_fact(*, contributors=None) -> StagePoiFactInput:
    return StagePoiFactInput(
        candidate_key="royal-gate-built",
        poi_locator={
            "names": ["Королевские ворота", "Königstor"],
            "external_ids": {"wikidata": "Q-test"},
        },
        kind="construction",
        text="Ворота построены в 1843 году.",
        time_scope="1843",
        evidence_refs=[{"page_id": page_id(), "region_key": "body-1"}],
        contributor_names=contributors or [],
        publication_method="scholarly_monograph",
    )


def staged_graph(*, contributors=None):
    compiled = compile_model_stage(
        StagedGraph(revision=REVISION),
        document_id=DOCUMENT_ID,
        revision=REVISION,
        pages=[page()],
        chunks=[],
        poi_facts=[poi_fact(contributors=contributors)],
    )
    return merge_stage(StagedGraph(revision=REVISION), compiled)


def test_poi_fact_compiles_deterministic_candidate_and_exact_provenance():
    graph = staged_graph()
    fact = graph.poi_facts[0]
    expected = uuid5(
        UUID(DOCUMENT_ID),
        "poi-fact:1:royal-gate-built",
    )
    assert fact.candidate_id == expected
    assert [str(value) for value in fact.page_ids] == [page_id()]
    assert len(fact.region_ids) == 1
    assert fact.poi_locator.names == ["Королевские ворота", "Königstor"]


@pytest.mark.asyncio
async def test_book_evidence_score_uses_contextual_verified_authority():
    calls = []

    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/rest/v1/rpc/rkb_author_authority_for_names"
        assert request.headers["authorization"] == "Bearer user-jwt"
        body = json.loads(request.content)
        calls.append(body)
        return httpx.Response(
            200,
            json=[{
                "input_name": "Chapter Historian",
                "author_id": AUTHOR_ID,
                "score": 90,
                "policy_version": "author-authority-v1",
            }],
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    service = SimpleNamespace(
        client=client,
        config=SimpleNamespace(url="https://db.example"),
        _headers=lambda actor: {
            "Authorization": f"Bearer {actor.access_token}",
            "apikey": "anon",
        },
    )
    events = await _build_poi_events(
        service,
        principal=principal(),
        graph=staged_graph(contributors=["Chapter Historian"]),
        document={
            "authors": ["Generic Book Author"],
            "title": "Regional history",
            "publication_year": 1936,
            "content_visibility": "private",
            "workspace_id": None,
        },
        document_id=DOCUMENT_ID,
        revision=REVISION,
    )
    await client.aclose()

    assert calls == [{
        "p_names": ["Chapter Historian"],
        "p_subject": "construction",
        "p_geography": "kaliningrad_oblast",
    }]
    evidence = events[0]["evidence"]
    assert evidence["author_subject_authority"] == 90
    assert evidence["publication_method_score"] == 85
    assert evidence["provenance_precision_score"] == 100
    assert evidence["evidence_verification_score"] == 91
    assert evidence["author_profile_refs"] == [
        f"knowledge://authors/{AUTHOR_ID}"
    ]
    assert evidence["score_policy_version"] == (
        "book-evidence-v1+author-authority-v1"
    )


@pytest.mark.asyncio
async def test_unknown_author_remains_null_not_neutral_score():
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[])

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    service = SimpleNamespace(
        client=client,
        config=SimpleNamespace(url="https://db.example"),
        _headers=lambda actor: {
            "Authorization": f"Bearer {actor.access_token}",
            "apikey": "anon",
        },
    )
    events = await _build_poi_events(
        service,
        principal=principal(),
        graph=staged_graph(),
        document={
            "authors": ["Unknown Author"],
            "title": "Unknown regional history",
            "publication_year": 1928,
            "content_visibility": "private",
            "workspace_id": None,
        },
        document_id=DOCUMENT_ID,
        revision=REVISION,
    )
    await client.aclose()

    evidence = events[0]["evidence"]
    assert evidence["author_subject_authority"] is None
    assert evidence["evidence_verification_score"] is None
    assert evidence["author_profile_refs"] == []


def test_poi_outbox_sql_enforces_acl_provenance_and_conservative_lineage():
    sql = (
        Path(__file__).parents[1] / "sql" / "004_poi_outbox.sql"
    ).read_text()

    assert "rkb_author_profiles" in sql
    assert "rkb_author_authority_for_names" in sql
    assert "count(distinct matched.author_id) = 1" in sql
    assert "rkb_integration_outbox" in sql
    assert "owner_user_id = auth.uid()" in sql
    assert "POI event visibility mismatch" in sql
    assert "POI event owner mismatch" in sql
    assert "POI event references foreign page" in sql
    assert "POI event references foreign region" in sql
    assert "source_family_id like 'unresolved:%'" in sql
    assert "source_family_id := 'unknown'" in sql
    assert "when event_visibility = 'public'" in sql
    assert "'pending_delivery'" in sql
    assert "'pending_authorization'" in sql
    assert "POI event idempotency conflict" in sql
    assert "provenance_precision_score}' is null" in sql
    assert "POI verification score invalid" in sql
