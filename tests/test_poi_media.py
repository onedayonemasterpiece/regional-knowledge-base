from pathlib import Path
from uuid import UUID, uuid5

from regional_knowledge.contracts import (
    Principal,
    StagePageInput,
    StagePoiMediaLinkInput,
)
from regional_knowledge.stage_graph import (
    StagedGraph,
    compile_model_stage,
    merge_stage,
)
from regional_knowledge.stage_service import _build_poi_media_events


DOCUMENT_ID = "11111111-1111-1111-1111-111111111111"


def _page_id() -> str:
    return str(uuid5(UUID(DOCUMENT_ID), "page:1:0"))


def _page() -> StagePageInput:
    return StagePageInput(
        page_id=_page_id(),
        physical_page_index=0,
        regions=[
            {
                "region_key": "photo",
                "kind": "figure",
                "bbox": {
                    "left": 100,
                    "top": 200,
                    "right": 900,
                    "bottom": 700,
                },
                "reading_order": 0,
            },
            {
                "region_key": "caption",
                "kind": "caption",
                "bbox": {
                    "left": 100,
                    "top": 720,
                    "right": 900,
                    "bottom": 800,
                },
                "reading_order": 1,
                "source_text": "Königstor. Historische Aufnahme.",
            },
        ],
        relations=[
            {
                "kind": "caption_of",
                "source_region_key": "caption",
                "target_region_key": "photo",
            }
        ],
        illustrations=[
            {
                "illustration_key": "photo-1",
                "source_region_key": "photo",
                "kind": "photo",
                "caption_region_keys": ["caption"],
            }
        ],
    )


def _link() -> StagePoiMediaLinkInput:
    return StagePoiMediaLinkInput(
        link_key="royal-gate-photo",
        poi_locator={"names": ["Королевские ворота", "Königstor"]},
        illustration_ref={
            "page_id": _page_id(),
            "illustration_key": "photo-1",
        },
        relation="depicts",
        time_scope="early-20th-century",
    )


def test_poi_media_link_and_event_keep_exact_illustration_provenance():
    compiled = compile_model_stage(
        StagedGraph(revision=1),
        document_id=DOCUMENT_ID,
        revision=1,
        pages=[_page()],
        chunks=[],
        poi_media_links=[_link()],
    )
    graph = merge_stage(StagedGraph(revision=1), compiled)
    link = graph.poi_media_links[0]
    illustration = graph.illustrations[0]

    assert link.illustration_id == illustration.illustration_id

    events = _build_poi_media_events(
        principal=Principal(
            subject="22222222-2222-2222-2222-222222222222",
            client_id="chatgpt-knowledge",
            issuer="https://issuer.example/auth/v1",
            access_token="user-jwt",
        ),
        graph=graph,
        document={"title": "Königsberg", "publication_year": 1930},
        document_id=DOCUMENT_ID,
        revision=1,
        illustration_rows=[
            {
                "id": str(illustration.illustration_id),
                "source_crop_sha256": "a" * 64,
                "visibility": "private",
                "rights_status": "unknown",
            }
        ],
    )

    event = events[0]
    assert event["contract_version"] == "poi.media_evidence.v1"
    assert event["media"]["relation"] == "depicts"
    assert event["media"]["caption"] == "Königstor. Historische Aufnahme."
    assert event["media"]["illustration_ref"].startswith(
        "knowledge://illustrations/"
    )
    assert event["scope"]["visibility"] == "private"
    assert "object_key" not in str(event)


def test_poi_media_sql_validates_rights_acl_and_atomic_core_activation():
    sql = (
        Path(__file__).parents[1] / "sql" / "005_poi_media_outbox.sql"
    ).read_text()

    for expected in (
        "poi.media_evidence.v1",
        "to_regprocedure(",
        "rename to rkb_activate_revision_core",
        "POI media crop hash mismatch",
        "POI media rights mismatch",
        "POI media visibility mismatch",
        "POI media evidence escapes illustration page",
        "rkb_activate_revision_core",
        "pending_authorization",
        "POI media event idempotency conflict",
    ):
        assert expected in sql
