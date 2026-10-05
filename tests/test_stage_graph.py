from uuid import UUID, uuid5
import pytest

from regional_knowledge.contracts import (
    StageChunkInput,
    StagePageInput,
)
from regional_knowledge.stage_graph import (
    StagedGraph,
    compile_model_stage,
    merge_stage,
    validate_graph,
)


DOCUMENT_ID = "11111111-1111-1111-1111-111111111111"
REVISION = 1


def page_id(index: int) -> str:
    return str(uuid5(UUID(DOCUMENT_ID), f"page:{REVISION}:{index}"))


def page_input(index: int, *, needs_review: bool = False) -> StagePageInput:
    return StagePageInput(
        page_id=page_id(index),
        physical_page_index=index,
        source_material="visual_reviewed",
        source_review_note="Synthetic fixture visually checked against its source page",
        layout_kind="single_column",
        regions=[
            {
                "region_key": "body-1",
                "kind": "body",
                "bbox": {"left": 50, "top": 50, "right": 950, "bottom": 850},
                "reading_order": 0,
                "source_text": f"Page {index} factual text",
                "normalized_text": f"Page {index} factual text",
                "needs_review": needs_review,
            }
        ],
    )


def chunk_input(index: int) -> StageChunkInput:
    return StageChunkInput(
        chunk_key=f"page-{index}",
        title=f"Page {index}",
        region_refs=[
            {
                "page_id": page_id(index),
                "region_key": "body-1",
            }
        ],
    )


def test_model_stage_compiles_deterministic_ids_and_derives_chunk_text():
    compiled = compile_model_stage(
        StagedGraph(revision=REVISION),
        document_id=DOCUMENT_ID,
        revision=REVISION,
        pages=[page_input(0)],
        chunks=[chunk_input(0)],
    )

    page = compiled.pages[0]
    region = page.regions[0]
    chunk = compiled.chunks[0]

    assert str(page.page_id) == page_id(0)
    assert str(region.region_id) == str(
        uuid5(UUID(page_id(0)), "region:body-1")
    )
    assert str(chunk.chunk_id) == str(
        uuid5(UUID(DOCUMENT_ID), "chunk:1:page-0")
    )
    assert chunk.text == "Page 0 factual text"
    assert chunk.normalized_text == "Page 0 factual text"
    assert chunk.region_ids == [region.region_id]
    assert chunk.page_ids == [page.page_id]


def test_restage_page_drops_stale_chunk_until_model_resupplies_it():
    first_compiled = compile_model_stage(
        StagedGraph(revision=REVISION),
        document_id=DOCUMENT_ID,
        revision=REVISION,
        pages=[page_input(0)],
        chunks=[chunk_input(0)],
    )
    graph = merge_stage(StagedGraph(revision=REVISION), first_compiled)
    assert len(graph.chunks) == 1

    replacement = page_input(0).model_copy(
        update={
            "printed_page_number": "7",
            "regions": [
                page_input(0).regions[0].model_copy(
                    update={
                        "source_text": "Corrected factual text",
                        "normalized_text": "Corrected factual text",
                    }
                )
            ],
        }
    )
    replacement_compiled = compile_model_stage(
        graph,
        document_id=DOCUMENT_ID,
        revision=REVISION,
        pages=[replacement],
        chunks=[],
    )
    graph = merge_stage(graph, replacement_compiled)

    assert graph.pages[0].printed_page_number == "7"
    assert graph.pages[0].regions[0].source_text == "Corrected factual text"
    assert graph.chunks == []


def test_validation_requires_complete_page_and_text_region_coverage():
    compiled = compile_model_stage(
        StagedGraph(revision=REVISION),
        document_id=DOCUMENT_ID,
        revision=REVISION,
        pages=[page_input(0)],
        chunks=[],
    )
    graph = merge_stage(StagedGraph(revision=REVISION), compiled)
    result = validate_graph(graph, expected_page_count=2)

    assert any("missing pages" in item for item in result.errors)
    assert "no retrieval chunks staged" in result.errors
    assert any("uncovered textual regions" in item for item in result.errors)


def test_validation_marks_model_uncertainty_for_review():
    compiled = compile_model_stage(
        StagedGraph(revision=REVISION),
        document_id=DOCUMENT_ID,
        revision=REVISION,
        pages=[page_input(0, needs_review=True)],
        chunks=[chunk_input(0)],
    )
    graph = merge_stage(StagedGraph(revision=REVISION), compiled)
    result = validate_graph(graph, expected_page_count=1)

    assert result.errors == []
    assert result.ready is False
    assert any(item.startswith("review:region:") for item in result.warnings)


def test_validation_accepts_complete_small_graph():
    empty = StagedGraph(revision=REVISION)
    compiled = compile_model_stage(
        empty,
        document_id=DOCUMENT_ID,
        revision=REVISION,
        pages=[page_input(0), page_input(1)],
        chunks=[chunk_input(0), chunk_input(1)],
    )
    graph = merge_stage(empty, compiled)
    result = validate_graph(graph, expected_page_count=2)

    assert result.errors == []
    assert result.warnings == []
    assert result.ready is True


def test_multi_page_chunks_and_quality_diagnostics_are_supported():
    pages = [page_input(i) for i in range(6)]
    pages[5] = pages[5].model_copy(update={
        "regions": [pages[5].regions[0].model_copy(update={
            "source_text": "Long source passage " + ("material " * 310),
            "normalized_text": "Long source passage " + ("material " * 310),
        })]
    })
    chunks = [
        StageChunkInput(
            chunk_key="continuation-0-1",
            title="Cross-page continuation",
            region_refs=[
                {"page_id": page_id(0), "region_key": "body-1"},
                {"page_id": page_id(1), "region_key": "body-1"},
            ],
        ),
        *[chunk_input(i) for i in range(2, 6)],
    ]
    graph = merge_stage(StagedGraph(revision=REVISION), compile_model_stage(
        StagedGraph(revision=REVISION), document_id=DOCUMENT_ID, revision=REVISION,
        pages=pages, chunks=chunks,
    ))
    assert graph.chunks[0].page_ids == [UUID(page_id(0)), UUID(page_id(1))]
    result = validate_graph(graph, expected_page_count=6)
    assert result.errors == []
    assert "retrieval_quality:fragmented_body_chunks" in result.warnings
    assert any(item.startswith("retrieval_quality:encoder_budget_risk:") for item in result.warnings)


def test_encoder_budget_diagnostic_counts_visual_augmentation():
    page = StagePageInput(
        page_id=page_id(0), physical_page_index=0,
        source_material="visual_reviewed", source_review_note="checked",
        regions=[
            {"region_key":"body","kind":"body","bbox":{"left":0,"top":0,"right":1000,"bottom":400},"reading_order":0,"source_text":"Body "+("x"*1790)},
            {"region_key":"figure","kind":"figure","bbox":{"left":0,"top":400,"right":1000,"bottom":1000},"reading_order":1},
        ],
        illustrations=[{"illustration_key":"i","source_region_key":"figure","kind":"drawing","visual_description":"visual "+("y"*900)}],
    )
    chunk=StageChunkInput(
        chunk_key="augmented",title="Augmented",
        region_refs=[{"page_id":page_id(0),"region_key":"body"}],
        illustration_refs=[{"page_id":page_id(0),"illustration_key":"i"}],
    )
    graph=merge_stage(StagedGraph(revision=REVISION),compile_model_stage(
        StagedGraph(revision=REVISION),document_id=DOCUMENT_ID,revision=REVISION,pages=[page],chunks=[chunk]
    ))
    result=validate_graph(graph,expected_page_count=1)
    assert result.errors==[]
    assert any(value.startswith("retrieval_quality:encoder_budget_risk:") for value in result.warnings)


def test_illustration_uses_figure_bbox_and_chunk_reference():
    page = StagePageInput(
        page_id=page_id(0),
        physical_page_index=0,
        regions=[
            {
                "region_key": "body-1",
                "kind": "body",
                "bbox": {"left": 50, "top": 50, "right": 950, "bottom": 300},
                "reading_order": 0,
                "source_text": "Illustrated text",
            },
            {
                "region_key": "figure-1",
                "kind": "figure",
                "bbox": {"left": 100, "top": 350, "right": 900, "bottom": 800},
                "reading_order": 1,
            },
            {
                "region_key": "caption-1",
                "kind": "caption",
                "bbox": {"left": 100, "top": 810, "right": 900, "bottom": 880},
                "reading_order": 2,
                "source_text": "Historical photograph",
            },
        ],
        relations=[
            {
                "kind": "caption_of",
                "source_region_key": "caption-1",
                "target_region_key": "figure-1",
            }
        ],
        illustrations=[
            {
                "illustration_key": "image-1",
                "source_region_key": "figure-1",
                "kind": "photo",
                "display_rotation_degrees": 90,
                "caption_region_keys": ["caption-1"],
                "nearby_region_keys": ["body-1"],
            }
        ],
    )
    chunk = StageChunkInput(
        chunk_key="illustrated",
        title="Illustrated",
        region_refs=[
            {"page_id": page_id(0), "region_key": "body-1"},
            {"page_id": page_id(0), "region_key": "caption-1"},
        ],
        illustration_refs=[
            {"page_id": page_id(0), "illustration_key": "image-1"}
        ],
    )
    compiled = compile_model_stage(
        StagedGraph(revision=REVISION),
        document_id=DOCUMENT_ID,
        revision=REVISION,
        pages=[page],
        chunks=[chunk],
    )

    illustration = compiled.illustrations[0]
    assert illustration.bbox.left == 100
    assert illustration.bbox.bottom == 800
    assert illustration.display_rotation_degrees == 90
    assert compiled.chunks[0].illustration_ids == [illustration.illustration_id]


@pytest.mark.parametrize("missing_note", [None, "   "])
def test_page_ids_and_preview_do_not_prove_source_completeness(missing_note):
    for material in ("unreviewed", "preview", "full_native"):
        page = page_input(0).model_copy(update={"source_material": material})
        graph = merge_stage(StagedGraph(revision=1), compile_model_stage(
            StagedGraph(revision=1), document_id=DOCUMENT_ID, revision=1,
            pages=[page], chunks=[chunk_input(0)],
        ))
        assert any("source completeness unreviewed" in e for e in validate_graph(graph, expected_page_count=1).errors)
    page = page_input(0).model_copy(update={"source_review_note": missing_note})
    graph = merge_stage(StagedGraph(revision=1), compile_model_stage(
        StagedGraph(revision=1), document_id=DOCUMENT_ID, revision=1,
        pages=[page], chunks=[chunk_input(0)],
    ))
    assert not validate_graph(graph, expected_page_count=1).ready


def test_validation_reports_open_continuation_boundaries():
    pages=[page_input(i) for i in range(5)]
    texts=[
        "Кроме замка Унфридт построил сиротский приют, ворота, почтамт и",
        "Трагхаймскую церковь. Затем он занимался благоустройством.",
        "Это отдельное законченное предложение.",
        "Ещё одно самостоятельное предложение.",
        "Последний самостоятельный фрагмент заканчивается точкой.",
    ]
    pages=[
        page.model_copy(update={
            "regions":[page.regions[0].model_copy(update={
                "source_text":texts[i],
                "normalized_text":texts[i],
            })]
        })
        for i,page in enumerate(pages)
    ]
    base=StagedGraph(revision=REVISION)
    graph=merge_stage(base,compile_model_stage(
        base,
        document_id=DOCUMENT_ID,
        revision=REVISION,
        pages=pages,
        chunks=[chunk_input(i) for i in range(5)],
    ))
    result=validate_graph(graph,expected_page_count=5)
    assert result.errors==[]
    assert "retrieval_quality:open_continuation_boundaries:1" in result.warnings
