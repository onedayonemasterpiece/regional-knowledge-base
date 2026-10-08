"""Provenance and no-loss tests for staged within-region semantic passages."""
import hashlib
from uuid import UUID,uuid5

import pytest
from pydantic import ValidationError

from regional_knowledge.contracts import StageChunkInput,StagePageInput
from regional_knowledge.semantic_passages import draft_semantic_passages
from regional_knowledge.stage_graph import StagedGraph,compile_model_stage,merge_stage,validate_graph

DOCUMENT="11111111-1111-1111-1111-111111111111"
REVISION=1
PAGE=str(uuid5(UUID(DOCUMENT),"page:1:0"))


def fixture_page(text,*,heading="A historical account"):
    return StagePageInput(
        page_id=PAGE,physical_page_index=0,
        source_material="visual_reviewed",
        source_review_note="Synthetic source illustration and text cross-checked",
        regions=[
            {"region_key":"h1","reading_order":0,"kind":"heading",
             "bbox":{"left":10,"top":10,"right":990,"bottom":100},
             "source_text":heading},
            {"region_key":"body-1","reading_order":1,"kind":"body",
             "bbox":{"left":10,"top":110,"right":990,"bottom":990},
             "source_text":text},
        ],
    )


def counter(text):
    # Injectable exact-model counter in production; deterministic stand-in.
    return len(text.split())+2


def graph_for(chunks,page):
    graph=StagedGraph(revision=REVISION)
    stage=compile_model_stage(
        graph,document_id=DOCUMENT,revision=REVISION,
        pages=[page],chunks=chunks,
    )
    return merge_stage(graph,stage)


def test_draft_compiles_into_multiple_exact_source_spans_with_full_coverage():
    text=" ".join(
        f"Historic settlement provision {n} preserved inherited property."
        for n in range(75)
    )
    page=fixture_page(text)
    pieces=draft_semantic_passages(
        [page],counter,target_tokens=48,title_prefix="Brünneck test"
    )
    assert len(pieces)>2
    assert all(p.span_refs and not p.region_refs for p in pieces)
    assert all(counter(" ".join(text[s.start:s.end] for s in p.span_refs
                           if s.region_key=="body-1"))<=48 for p in pieces)
    graph=graph_for(pieces,page)
    assert validate_graph(graph,expected_page_count=1).errors==[]
    assert all(chunk.source_spans for chunk in graph.chunks)
    span_list=[
        span for c in pieces for span in c.span_refs
        if span.region_key=="body-1"
    ]
    digest=hashlib.sha256(text.encode()).hexdigest()
    assert all(span.source_text_sha256==digest for span in span_list)
    assert "".join(text[span.start:span.end] for span in span_list)==text


def test_missing_later_span_is_not_mistaken_for_full_region_coverage():
    text=" ".join(f"Historical sentence {i} remains source evidence." for i in range(60))
    page=fixture_page(text)
    chunks=draft_semantic_passages([page],counter,target_tokens=32)
    assert len(chunks)>3
    graph=graph_for(chunks[:-1],page)
    result=validate_graph(graph,expected_page_count=1)
    assert any("uncovered source span text" in error for error in result.errors)


def test_source_hash_and_midword_cut_fail_closed():
    text="Important historical negotiations remained securely documented."
    page=fixture_page(text)
    src=page.regions[1]
    digest=hashlib.sha256(text.encode()).hexdigest()
    bad=StageChunkInput(
        chunk_key="cut-word",title="Synthetic",
        span_refs=[{
            "page_id":PAGE,"region_key":"body-1","start":1,"end":len(text),
            "source_text_sha256":digest,
        }],
    )
    with pytest.raises(ValueError,match="word at start"):
        graph_for([bad],page)
    original=StageChunkInput(
        chunk_key="original",title="Synthetic",
        span_refs=[{
            "page_id":PAGE,"region_key":"body-1","start":0,"end":len(text),
            "source_text_sha256":digest,
        }],
    )
    modified=fixture_page(text+" Corrected after review.")
    with pytest.raises(ValueError,match="stale source region span hash"):
        graph_for([original],modified)


def test_whole_region_and_span_refs_are_mutually_exclusive():
    ref={"page_id":PAGE,"region_key":"body-1"}
    with pytest.raises(ValidationError,match="either whole region"):
        StageChunkInput(chunk_key="empty",title="Test")
    with pytest.raises(ValidationError,match="either whole region"):
        StageChunkInput(
            chunk_key="mixed",title="Test",region_refs=[ref],
            span_refs=[{**ref,"start":0,"end":4,
                "source_text_sha256":"a"*64}],
        )


def test_unreviewed_source_cannot_generate_pilot_passages():
    page=fixture_page("Historical facts are source material.")
    page.source_material="unreviewed"
    with pytest.raises(ValueError,match="unreviewed page"):
        draft_semantic_passages([page],counter)
    page=fixture_page("Historical facts are source material.")
    page.regions[1].needs_review=True
    with pytest.raises(ValueError,match="unreviewed region"):
        draft_semantic_passages([page],counter)


def test_oversized_unsplittable_source_fails_instead_of_truncating():
    text="X"*1000
    page=fixture_page(text)
    with pytest.raises(ValueError,match="unbreakable"):
        draft_semantic_passages([page],lambda s:len(s)//4+2,
                                target_tokens=48)


def test_draft_is_stable_across_retries_and_budget_checked():
    text=" ".join(f"Town historical source clause number {n}." for n in range(55))
    page=fixture_page(text)
    first=draft_semantic_passages([page],counter,target_tokens=48)
    second=draft_semantic_passages([page],counter,target_tokens=48)
    assert [p.model_dump() for p in first]==[p.model_dump() for p in second]
    graph=graph_for(first,page)
    assert all(counter(chunk.text)<=48 for chunk in graph.chunks)
