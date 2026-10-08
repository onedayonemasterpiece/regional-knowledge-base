"""Source-backed partial-region passage contract and source-integrity tests.

Synthetic reviewed pages only; no database access or document staging side effects.
"""
import hashlib
from uuid import UUID, uuid5

import pytest

from regional_knowledge.contracts import StageChunkInput, StagePageInput
from regional_knowledge.stage_graph import StagedGraph, compile_model_stage, merge_stage, validate_graph

DOC="22222222-2222-4222-8222-222222222222"
REVISION=7


def page_id():
    return str(uuid5(UUID(DOC),f"page:{REVISION}:0"))


def synthetic_page(text):
    return StagePageInput(
        page_id=page_id(),
        physical_page_index=0,
        source_material="visual_reviewed",
        source_review_note="Synthetic source image was reviewed",
        layout_kind="single_column",
        regions=[{
            "region_key":"body-1",
            "kind":"body",
            "bbox":{"left":50,"top":60,"right":950,"bottom":920},
            "reading_order":0,
            "source_text":text,
            "normalized_text":text,
        }]
    )


def span_ref(text,start,end):
    return {
        "page_id":page_id(),"region_key":"body-1",
        "start":start,"end":end,
        "source_text_sha256":hashlib.sha256(text.encode("utf-8")).hexdigest(),
    }


def chunk(text,index,start,end):
    return StageChunkInput(
        chunk_key=f"passage-{index}",
        title=f"Passage {index}",
        span_refs=[span_ref(text,start,end)],
    )


def fixture():
    text="Die Geschichte des Ordens und der Städte berichtet vom Handel. "*33
    a=text.index(". ",550)+1
    b=text.index(". ",a+500)+1
    return text,[chunk(text,1,0,a),chunk(text,2,a,b),chunk(text,3,b,len(text))]


def graph_with_chunks(text,chunks):
    initial=StagedGraph(revision=REVISION)
    result=compile_model_stage(
        initial,document_id=DOC,revision=REVISION,
        pages=[synthetic_page(text)],chunks=chunks
    )
    return merge_stage(initial,result)


def test_partial_source_spans_create_shorter_chucks_and_preserve_all_source_provenance():
    text,chunks=fixture()
    graph=graph_with_chunks(text,chunks)
    assert len(graph.chunks)==3
    assert validate_graph(graph,expected_page_count=1).errors==[]
    expected=[text[ref.span_refs[0].start:ref.span_refs[0].end].strip() for ref in chunks]
    assert [row.text for row in graph.chunks]==expected
    for row,original in zip(graph.chunks,chunks,strict=True):
        assert row.page_ids==[graph.pages[0].page_id]
        assert row.region_ids==[graph.pages[0].regions[0].region_id]
        assert len(row.source_spans)==1
        span=row.source_spans[0]
        assert span.start==original.span_refs[0].start
        assert span.end==original.span_refs[0].end
        assert hashlib.sha256(text.encode()).hexdigest()==span.source_text_sha256
    reconstructed="".join(text[span.start:span.end] for item in graph.chunks for span in item.source_spans)
    assert reconstructed==text


def test_source_span_missing_middle_or_tail_is_rejected_as_incomplete_book():
    text,chunks=fixture()
    first_and_last=graph_with_chunks(text,[chunks[0],chunks[2]])
    errors=validate_graph(first_and_last,expected_page_count=1).errors
    assert any("uncovered source span text" in value for value in errors)
    first_two=graph_with_chunks(text,chunks[:2])
    assert any("uncovered source span text" in value for value in validate_graph(first_two,expected_page_count=1).errors)


def test_source_span_hash_bound_to_unmodified_source_bytes_and_page():
    text,chunks=fixture()
    corrupted=chunks[0].model_copy(deep=True)
    corrupted.span_refs[0].source_text_sha256="0"*64
    with pytest.raises(ValueError,match="stale source region span hash"):
        graph_with_chunks(text,[corrupted])
    corrupted2=chunks[0].model_copy(deep=True)
    corrupted2.span_refs[0].end=len(text)+1
    with pytest.raises(ValueError,match="beyond reviewed region text"):
        graph_with_chunks(text,[corrupted2])


def test_spans_cannot_cut_through_words_or_select_figures():
    text,chunks=fixture()
    bad=chunks[0].model_copy(deep=True)
    bad.span_refs[0].end=6  # German word "Geschichte" begins before this offset
    with pytest.raises(ValueError,match="cuts through a word"):
        graph_with_chunks(text,[bad])
    bad_start=chunks[1].model_copy(deep=True)
    bad_start.span_refs[0].start=chunks[1].span_refs[0].start+4
    with pytest.raises(ValueError,match="cuts through a word"):
        graph_with_chunks(text,[bad_start])


def test_old_whole_region_and_new_span_refs_cannot_be_mixed_in_one_chunk():
    text,_=fixture()
    with pytest.raises(ValueError,match="either whole region_refs or source-backed span_refs"):
        StageChunkInput(
            chunk_key="mixed",
            title="Invalid mixed document",
            region_refs=[{"page_id":page_id(),"region_key":"body-1"}],
            span_refs=[span_ref(text,0,100)]
        )
    with pytest.raises(ValueError,match="either whole region_refs or source-backed span_refs"):
        StageChunkInput(chunk_key="empty",title="Empty",region_refs=[],span_refs=[])
