"""P0.A I01-I08: synthetic exact book-stage region boundaries, not BGE chunks."""
import copy
from uuid import UUID, uuid5

import pytest
from pydantic import ValidationError

from regional_knowledge.contracts import StageChunkInput, StagePageInput
from regional_knowledge.source_units import (
    SourceUnitBoundaryError, resolve_source_units, bounded_unit_records,
)
from regional_knowledge.kaggle_source_read import SourceReadRequest

SOURCE = "31111111-2222-4333-8444-555555555555"


def event(ident, action, region_key, offset, *, kind=None, title=None):
    result = dict(article_id=ident, action=action, region_key=region_key, offset=offset)
    if kind is not None:
        result["kind"] = kind
    if title is not None:
        result["title"] = title
    return result


def reviewed(index, regions, markers=(), *, version=1):
    """Validate through the actual model-bound book_ingest StagePageInput."""
    page_id = str(uuid5(UUID(SOURCE), f"page:1:{index}"))
    raw = dict(page_id=page_id,physical_page_index=index,
        source_material="visual_reviewed",source_review_note="Reviewed synthetic page",
        source_unit_version=version,
        regions=[dict(region_key=key,kind=kind,reading_order=i,source_text=text,
                bbox=dict(left=20,top=30+i*100,right=900,bottom=100+i*100))
                 for i,(key,kind,text) in enumerate(regions)],
        source_unit_boundaries=list(markers))
    return StagePageInput.model_validate(raw)


def projection(page):
    """Equivalent metadata-only read of the typed accepted model page."""
    wire = page.model_dump(mode="json",exclude_none=True)
    return dict(page_id=wire["page_id"],
        physical_page_index=wire["physical_page_index"],
        source_unit_boundaries=wire["source_unit_boundaries"],
        regions=[dict(region_key=r["region_key"],reading_order=r["reading_order"],
                      kind=r["kind"],source_length=len(r["source_text"]))
                 for r in wire["regions"]])


def spans_text(unit, original_pages):
    lookup={(p.physical_page_index,r.region_key):r.source_text
            for p in original_pages for r in p.regions}
    return "".join(lookup[(s["physical_page_index"],s["region_key"])][s["start"]:s["end"]]
                   for s in unit["source_spans"])


def test_i01_three_accepted_pages_remain_open_with_exact_start_and_search_independence():
    pages=[reviewed(0,[("heading","heading","Глава первая.\nНачало.")],
        [event("chapter-1","start","heading",0,kind="chapter",title="Глава первая")]),
        reviewed(1,[("body","body","Продолжение первой главы.")]),
        reviewed(2,[("body","body","Ещё один фрагмент.")])]
    before=[p.model_dump(mode="json") for p in pages]
    unit=resolve_source_units(list(map(projection,pages)),
        source_id=SOURCE,page_count=4,read_complete=False)[0]
    assert (unit["article_id"],unit["kind"],unit["title"]) == (
        "chapter-1","chapter","Глава первая")
    assert unit["start_anchor"]["physical_page_index"]==0
    assert unit["state"]=="unit_not_ready" and unit["end_anchor"] is None
    assert unit["source_spans"]==[]
    assert before==[p.model_dump(mode="json") for p in pages]
    # Ordinary accepted_chunks stays independently admitted.
    req=SourceReadRequest(request_id="test-unit-chunks",connection_generation=1,
        turn_generation=0,min_data_seq=0,deadline_ms=1000,operation="accepted_chunks",
        source_id=SOURCE,page_start=0,page_end=2,
        actor=dict(person_id="reader",audience="personal",auth_epoch=1,
                   visibility_epoch=0,context_ref="reader-context"))
    assert req.operation=="accepted_chunks"


def test_i02_next_confirmed_boundary_closes_previous_at_interior_region_offset():
    mixed="Продолжение А. ГЛАВА Б: начало второго текста."
    pivot=mixed.index("ГЛАВА Б")
    pages=[
        reviewed(0,[("r","heading","ГЛАВА А. Первый текст.")],
            [event("a","start","r",0,kind="chapter",title="Глава А")]),
        reviewed(1,[("r","body","Ещё текст А.")]),
        reviewed(2,[("r","body","Текст А продолжается.")]),
        reviewed(3,[("mixed","body",mixed)],[
            event("a","end","mixed",pivot),
            event("b","start","mixed",pivot,kind="chapter",title="Глава Б")]),
    ]
    a,b=resolve_source_units([projection(p) for p in pages],
        source_id=SOURCE,page_count=5,read_complete=False)
    assert a["state"]=="complete" and b["state"]=="unit_not_ready"
    assert a["end_anchor"]["offset"]==pivot and a["end_anchor"]["physical_page_index"]==3
    text=spans_text(a,pages)
    assert text.endswith(mixed[:pivot])
    assert "ГЛАВА Б" not in text
    assert [s["physical_page_index"] for s in a["source_spans"]]==[0,1,2,3]


def test_i03_two_articles_one_region_have_disjoint_half_open_spans_and_existing_ids():
    text="Повествование А. Статья Б: собственный текст."
    boundary=text.index("Статья Б")
    page=reviewed(0,[("both","body",text)],[
        event("article-a","start","both",0,kind="article",title="Статья А"),
        event("article-a","end","both",boundary),
        event("article-b","start","both",boundary,kind="article",title="Статья Б"),
        event("article-b","end","both",len(text)),
    ])
    a,b=resolve_source_units([projection(page)],source_id=SOURCE,page_count=1,
        read_complete=False)
    assert (a["state"],b["state"])==("complete","complete")
    assert (spans_text(a,[page]),spans_text(b,[page]))==(
        text[:boundary],text[boundary:])
    assert a["source_spans"][0]["end"]==b["source_spans"][0]["start"]==boundary
    # The scope identity is *exactly* the pre-existing StageChunkInput.article_id.
    passage=StageChunkInput(article_id="article-b",chunk_key="article-b-paragraph",
        title="Synthetic B",span_refs=[dict(page_id=page.page_id,region_key="both",
            start=boundary,end=len(text))])
    assert passage.article_id==b["article_id"]


def test_i04_eof_only_after_actual_read_complete_and_all_physical_pages_published():
    pages=[
        reviewed(0,[("a","heading","Текст главы.")],
            [event("chapter-1","start","a",0,kind="chapter",title="Глава")]),
        reviewed(1,[("foot","footnote","Последнее примечание.")]),
    ]
    for complete,accepted in ((False,pages),(True,pages[:1])):
        unit=resolve_source_units([projection(p) for p in accepted],
            source_id=SOURCE,page_count=2,read_complete=complete)[0]
        assert unit["state"]=="unit_not_ready" and unit["end_anchor"] is None
    unit=resolve_source_units([projection(p) for p in pages],
        source_id=SOURCE,page_count=2,read_complete=True)[0]
    assert unit["state"]=="complete" and unit["close_reason"]=="verified_eof"
    assert unit["end_anchor"]["physical_page_index"]==1
    assert unit["end_anchor"]["offset"]==len("Последнее примечание.")
    assert spans_text(unit,pages)=="Текст главы.Последнее примечание."


def test_i05_explicit_end_with_unaccepted_intermediate_page_is_not_ready():
    pages=[
        reviewed(0,[("opening","body","Открыта единица.")],
            [event("s","start","opening",0,kind="section",title="Раздел")]),
        reviewed(2,[("last","body","Последний фрагмент")],
            [event("s","end","last",len("Последний фрагмент"))]),
    ]
    one=resolve_source_units([projection(p) for p in pages],
        source_id=SOURCE,page_count=3,read_complete=True)[0]
    assert one["state"]=="unit_not_ready" and one["end_anchor"] is None
    assert one["source_spans"]==[]


def test_i06_legacy_page_without_unit_opt_in_is_not_reinterpreted_or_rewritten():
    old=reviewed(0,[("r","body","Legacy accepted search passage.")],version=None)
    old_json=old.model_dump(mode="json",exclude_none=True)
    assert "source_unit_version" not in old_json
    assert resolve_source_units([projection(old)],source_id=SOURCE,
        page_count=1,read_complete=True)==[]
    assert old_json==old.model_dump(mode="json",exclude_none=True)


def test_i07_versioned_stages_require_confirmed_region_coordinates():
    text="Оригинальный текст."
    marker=event("a","start","text",1,kind="article",title="А")
    page=reviewed(0,[("text","body",text)],[marker])
    assert page.source_unit_version==1
    assert page.source_unit_boundaries[0].offset==1
    base=page.model_dump(mode="json")
    for invalid in (
        dict(source_unit_version=None),
        dict(source_unit_boundaries=[dict(marker,region_key="other")]),
        dict(source_unit_boundaries=[dict(marker,offset=len(text))]),
    ):
        with pytest.raises(ValidationError):
            StagePageInput.model_validate(dict(base,**invalid))


def test_i08_heading_inside_region_then_later_footnote_keeps_source_order_without_next_article():
    first="Предисловие. §1 История. Начало раздела."
    start=first.index("§1")
    last="Сноска раздела. §2 Другая статья."
    close=last.index("§2")
    pages=[
        reviewed(0,[("text","body",first)],[
            event("sec-1","start","text",start,kind="section",title="§1")]),
        reviewed(1,[("footnote","footnote","Примечание к §1."),
                    ("end","body",last)],[
            event("sec-1","end","end",close),
            event("sec-2","start","end",close,kind="section",title="§2")]),
    ]
    sec,following=resolve_source_units([projection(p) for p in pages],
        source_id=SOURCE,page_count=3,read_complete=False)
    selected=spans_text(sec,pages)
    assert selected==first[start:]+"Примечание к §1."+last[:close]
    assert "Предисловие" not in selected and "Другая статья" not in selected
    assert [s["region_key"] for s in sec["source_spans"]]==["text","footnote","end"]
    assert following["state"]=="unit_not_ready"


def test_closed_unit_pagination_is_bounded_and_reuses_stable_source_ordinals():
    text="AAA BBB CCC"
    p=reviewed(0,[("line","body",text)],[
        event("a","start","line",0,kind="article",title="A"),
        event("a","end","line",4),
        event("b","start","line",4,kind="article",title="B"),
        event("b","end","line",8),
        event("c","start","line",8,kind="article",title="C"),
        event("c","end","line",len(text))])
    units=resolve_source_units([projection(p)],source_id=SOURCE,page_count=1,
        read_complete=False)
    first,cursor=bounded_unit_records(units,limit=2)
    second,last=bounded_unit_records(units,after_ordinal=cursor,limit=2)
    assert [u["article_id"] for u in first]==["a","b"]
    assert [u["article_id"] for u in second]==["c"] and last is None
    assert all(u["source_span_count"]==1 for u in first+second)


def test_conflicting_cross_page_duplicate_ids_and_reversed_closures_fail_closed():
    start=reviewed(0,[("a","body","Start.")],
        [event("x","start","a",0,kind="article",title="X")])
    duplicate=reviewed(1,[("a","body","Again.")],
        [event("x","start","a",0,kind="article",title="X")])
    orphan=reviewed(1,[("a","body","End.")],
        [event("unknown","end","a",2)])
    for other in (duplicate,orphan):
        with pytest.raises(SourceUnitBoundaryError):
            resolve_source_units([projection(start),projection(other)],
                source_id=SOURCE,page_count=2,read_complete=True)



@pytest.mark.asyncio
async def test_i07_native_materializer_opt_in_accepts_existing_article_id_but_legacy_replay_does_not():
    """Real native stage compilation; versionless historical article scope stays unsupported."""
    from regional_knowledge.kaggle_source_materializer import (
        NativeSourceCompiler, SourceMaterializationRejected,
    )
    from regional_knowledge.source_stage import canonical, digest
    from test_native_source_materializer import exact_batch, PERSON
    from test_source_stage import original, SOURCE as STAGED_SOURCE

    async def count(text):
        return len(text.split()) + 2

    compiler = NativeSourceCompiler(count=count,current=lambda: True)
    old = exact_batch(count=1,receipt=original(page_count=1))
    old['model_payload']['chunks'][0]['article_id'] = 'article-p0'
    encoded=canonical(old)
    with pytest.raises(SourceMaterializationRejected,match='native_source_component_activation_unavailable'):
        await compiler.compile([encoded],source_id=STAGED_SOURCE,person_id=PERSON,
                               last_batch_sha256=digest(encoded))
    new = copy.deepcopy(old)
    page = new['model_payload']['pages'][0]
    region = page['regions'][0]
    original_text=region['source_text']
    page['source_unit_version'] = 1
    page['source_unit_boundaries']=[
        event('article-p0','start',region['region_key'],0,kind='article',title='Published article'),
        event('article-p0','end',region['region_key'],len(original_text)),
    ]
    encoded=canonical(new)
    knowledge=await compiler.compile([encoded],source_id=STAGED_SOURCE,person_id=PERSON,
                                     last_batch_sha256=digest(encoded))
    assert len(knowledge['passages'])==1
    assert knowledge['passages'][0]['body']==original_text.strip()
    assert new['model_payload']['chunks'][0]['article_id']==page['source_unit_boundaries'][0]['article_id']
