"""Lossless reviewed-page planner: exact text, provenance, token and review fences."""
from collections import defaultdict
from uuid import UUID,uuid5
import hashlib
import pytest

from regional_knowledge.contracts import StagePageInput
from regional_knowledge.source_span_planner import _split_text,plan_reviewed_page
from regional_knowledge.stage_graph import StagedGraph,compile_model_stage,merge_stage,validate_graph

DOC="33333333-3333-4333-8333-333333333333"
REVISION=3
PAGE=str(uuid5(UUID(DOC),f"page:{REVISION}:0"))


def source_page(*,include_footnote=False,include_figure=False):
    body=("Die Geschichte des Ortes wird durch die Stadtchronik überliefert. "
          "Die Menschen erzählten von den Bauwerken und dem Handel. ")*45
    regs=[
        {"region_key":"header","kind":"heading","reading_order":0,
         "bbox":{"left":60,"top":60,"right":940,"bottom":140},"source_text":"Einleitung zur Stadtgeschichte."},
        {"region_key":"body","kind":"body","reading_order":1,
         "bbox":{"left":60,"top":160,"right":940,"bottom":840},"source_text":body},
    ]
    if include_footnote:
        regs.append({"region_key":"fn","kind":"footnote","reading_order":2,
                     "bbox":{"left":60,"top":860,"right":940,"bottom":910},"source_text":"Quelle: Stadtarchiv."})
    excluded={}
    if include_figure:
        regs.append({"region_key":"figure","kind":"figure","reading_order":3,
                     "bbox":{"left":60,"top":925,"right":940,"bottom":990}})
        excluded["figure"]="Synthetic fixture has no source crop"
    page=StagePageInput(
        page_id=PAGE,physical_page_index=0,
        source_material="visual_reviewed",
        source_review_note="Synthetic reviewed fixture",
        regions=regs,excluded_figure_regions=excluded,
    )
    return page


def count_tokens(text):
    # Token-budget contract is injected. Test with a deterministic upper proxy;
    # production uses the exact pinned E5 and BGE tokenizers.
    return len(text.split())


def test_reviewed_span_planner_covers_all_characters_and_compiles_valid_graph():
    page=source_page(include_footnote=True,include_figure=True)
    planned=plan_reviewed_page(page,title="Stadtgeschichte",token_count=count_tokens,
                              target_tokens=60,max_chars=430,chunk_prefix="page-0")
    assert len(planned)>5
    assert all(bool(c.span_refs)!=bool(c.region_refs) for c in planned)
    assert len({x.chunk_key for x in planned})==len(planned)
    assert any(item.region_refs for item in planned)  # one exact footnote
    source={r.region_key:r.source_text for r in page.regions if r.source_text}
    intervals=defaultdict(list)
    for chunk in planned:
        for span in chunk.span_refs:
            intervals[span.region_key].append((span.start,span.end))
            assert source[span.region_key][span.start:span.end].strip()
            assert hashlib.sha256(source[span.region_key].encode()).hexdigest()==span.source_text_sha256
    for name in ("header","body"):
        segments=sorted(intervals[name])
        assert segments[0][0]==0
        assert segments[-1][1]==len(source[name])
        assert all(a[1]==b[0] for a,b in zip(segments,segments[1:]))
        assert "".join(source[name][start:end] for start,end in segments)==source[name]
    graph=merge_stage(
        StagedGraph(revision=REVISION),
        compile_model_stage(StagedGraph(revision=REVISION),
                            document_id=DOC,revision=REVISION,pages=[page],chunks=planned),
    )
    checked=validate_graph(graph,expected_page_count=1)
    assert not checked.errors
    assert all(count_tokens(x.text)<=60 for x in graph.chunks)
    assert all(len(x.text)<=430 for x in graph.chunks)
    assert all(x.page_ids==[graph.pages[0].page_id] for x in graph.chunks)


def test_splitter_fail_closes_when_sentence_has_no_safe_boundary():
    with pytest.raises(ValueError,match="safe textual split point"):
        _split_text("A"*1800,count_tokens=lambda text:len(text),target=256,max_chars=600)


def test_planner_never_invents_source_review():
    page=source_page()
    page.source_material="preview"
    planned=plan_reviewed_page(page,title="Unreviewed book",token_count=count_tokens,
                              target_tokens=90,max_chars=500)
    graph=merge_stage(
        StagedGraph(revision=REVISION),
        compile_model_stage(StagedGraph(revision=REVISION),
                            document_id=DOC,revision=REVISION,pages=[page],chunks=planned),
    )
    assert any("source completeness unreviewed" in err
               for err in validate_graph(graph,expected_page_count=1).errors)


def test_complete_reviewed_region_required_and_no_silent_head_or_tail_loss():
    page=source_page()
    chunks=plan_reviewed_page(page,title="Test",token_count=count_tokens,
                             target_tokens=80,max_chars=500)
    truncated=chunks[:-1]
    graph=merge_stage(
        StagedGraph(revision=REVISION),
        compile_model_stage(StagedGraph(revision=REVISION),
                            document_id=DOC,revision=REVISION,pages=[page],chunks=truncated),
    )
    assert any("uncovered source span text" in e
               for e in validate_graph(graph,expected_page_count=1).errors)


def test_reviewed_image_is_still_indexed_as_dedicated_visual_passage():
    from regional_knowledge.search_material import graph_material
    initial=source_page(include_figure=True)
    page=StagePageInput.model_validate(initial.model_dump()|{
        "excluded_figure_regions":{},
        "illustrations":[{
            "illustration_key":"figure-0",
            "source_region_key":"figure",
            "kind":"drawing",
            "visual_description":"A historically observed drawing of a street facade.",
            "visual_description_provenance":"model_observation",
            "visual_description_language":"en",
        }],
    })
    planned=plan_reviewed_page(page,title="Test illustrations",token_count=count_tokens,
                             target_tokens=80,max_chars=500)
    visual=[item for item in planned if item.illustration_refs]
    assert len(visual)==1
    assert visual[0].region_refs[0].region_key=="figure"
    graph=merge_stage(StagedGraph(revision=REVISION),
        compile_model_stage(StagedGraph(revision=REVISION),
            document_id=DOC,revision=REVISION,pages=[page],chunks=planned))
    checks=validate_graph(graph,expected_page_count=1)
    assert checks.errors==[]
    visual_chunk=next(item for item in graph.chunks if item.illustration_ids)
    assert visual_chunk.text==""
    material,sha=graph_material(graph,visual_chunk)
    assert "historically observed drawing" in material
    assert len(sha)==64


def test_unobserved_uncaptioned_image_never_vanishes_from_source_pilot():
    page=source_page(include_figure=True)
    page=StagePageInput.model_validate(page.model_dump()|{
        "excluded_figure_regions":{},
        "illustrations":[{
            "illustration_key":"undocumented",
            "source_region_key":"figure",
            "kind":"photo",
        }],
    })
    with pytest.raises(ValueError,match="caption or reviewed model observation"):
        plan_reviewed_page(page,title="Image without evidence",token_count=count_tokens,
                           target_tokens=80,max_chars=500)
