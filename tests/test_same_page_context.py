"""Synthetic one-slot same-page retrieval without private books or titles."""
from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

import pytest

from regional_knowledge.continuation_context import same_page_topic_support
from regional_knowledge.contracts import SearchOutput, SearchResult
from regional_knowledge.supabase_backend import SupabaseRestBackend

QUESTION = (
    "Кто начал возводить башню на восточном краю острова "
    "и каким задумывалось здание?"
)
RANKED = (
    "В западной части острова поселились торговцы и возник "
    "новый базар. Отсюда начиналась дорога."
)
TARGET = (
    "На восточном краю острова архитектор начал возводить башню. "
    "Он задумывал здание как укреплённое сооружение."
)


def test_topic_context_requires_more_query_evidence_on_missing_side():
    assert same_page_topic_support(QUESTION, RANKED, TARGET)
    assert not same_page_topic_support(QUESTION, TARGET, RANKED)
    assert not same_page_topic_support("Чем известен город?", RANKED, TARGET)
    assert not same_page_topic_support(
        QUESTION, RANKED, "Совсем другой самостоятельный рассказ."
    )


@pytest.mark.asyncio
async def test_same_page_context_uses_one_slot_and_keeps_ranked_anchor():
    first, second, third, neighbor = (str(uuid4()) for _ in range(4))
    requested=[]

    def neighbors(actor, selected):
        requested.append((actor, [str(s) for s in selected]))
        return [{
            "id": first, "source_text": RANKED, "illustration_ids": [],
            "start_page": 10, "end_page": 10,
            "prev_id": neighbor, "prev_title": "Related source",
            "prev_text": TARGET, "prev_illustrations": [],
            "prev_page": 10,
        }]

    backend = SimpleNamespace(
        corpus=SimpleNamespace(neighbors=neighbors),
        client=SimpleNamespace(),
        _evidence_url=lambda ident: "knowledge://" + ident,
    )
    principal=SimpleNamespace(subject=str(uuid4()))
    output=SearchOutput(results=[
        SearchResult(id=first,title="Anchor",url="knowledge://first"),
        SearchResult(id=second,title="Other",url="knowledge://second"),
        SearchResult(id=third,title="Lower rank",url="knowledge://third"),
    ],retrieval_mode="bge",main_state="ready")
    result=await SupabaseRestBackend._expand_continuation_results(
        backend,output,principal,query=QUESTION,match_count=3
    )
    assert requested == [(principal.subject, [first, second])]
    assert [h.id for h in result.results] == [first,second,neighbor]
    assert result.results[2].ranking_signals[0]["branch"]=="same_page_context"
    assert result.results[2].ranking_signals[0]["direction"]=="previous"
    assert result.retrieval_mode=="bge"


@pytest.mark.asyncio
async def test_same_page_context_never_crosses_pages():
    first, other, neighbor=(str(uuid4()) for _ in range(3))

    def neighbors(_actor,_selected):
        return [{
            "id":first,"source_text":RANKED,"illustration_ids":[],
            "start_page":10,"end_page":10,
            "next_id":neighbor,"next_text":TARGET,"next_title":"Different page",
            "next_illustrations":[],"next_page":11,
        }]

    backend=SimpleNamespace(
        corpus=SimpleNamespace(neighbors=neighbors),client=SimpleNamespace(),
        _evidence_url=lambda ident:"knowledge://"+ident,
    )
    output=SearchOutput(results=[
        SearchResult(id=first,title="Anchor",url="knowledge://first"),
        SearchResult(id=other,title="Unrelated",url="knowledge://other"),
    ],retrieval_mode="bge",main_state="ready")
    actual=await SupabaseRestBackend._expand_continuation_results(
        backend,output,SimpleNamespace(subject=str(uuid4())),
        query=QUESTION,match_count=2,
    )
    assert [r.id for r in actual.results]==[first,other]


@pytest.mark.asyncio
async def test_second_neighbor_on_same_page_is_found_without_displacing_top_two():
    first, second, tail, middle, answer=(str(uuid4()) for _ in range(5))
    def neighbors(_actor,_selected):
        return [{
            "id":first,"source_text":RANKED,"illustration_ids":[],
            "start_page":10,"end_page":10,
            "next_id":middle,"next_text":"На набережной торговали фруктами.",
            "next_title":"Intermediate passage","next_illustrations":[],
            "next_page":10,
            "next2_id":answer,"next2_text":TARGET,
            "next2_title":"Second passage","next2_illustrations":[],
            "next2_page":10,
        }]
    backend=SimpleNamespace(
        corpus=SimpleNamespace(neighbors=neighbors),client=SimpleNamespace(),
        _evidence_url=lambda ident:"knowledge://"+ident,
    )
    result=await SupabaseRestBackend._expand_continuation_results(
        backend,SearchOutput(results=[
            SearchResult(id=first,title="Anchor",url="knowledge://first"),
            SearchResult(id=second,title="Another anchor",url="knowledge://second"),
            SearchResult(id=tail,title="Low-ranked evidence",url="knowledge://tail"),
        ],retrieval_mode="bge",main_state="ready"),
        SimpleNamespace(subject=str(uuid4())),query=QUESTION,match_count=3,
    )
    assert [h.id for h in result.results]==[first,second,answer]
    assert result.results[-1].ranking_signals[-1]["branch"]=="same_page_context"
    assert result.results[-1].ranking_signals[-1]["hops"]==2


@pytest.mark.asyncio
@pytest.mark.parametrize("intervening_page,intervening_images,target_page",[
    (11,False,10),     # A discontinuous page sequence is never context.
    (10,True,10),      # Do not skip an illustration-bearing intermediate chunk.
    (10,False,11),     # Neighbor lies outside the accepted original page.
])
async def test_second_neighbor_cannot_jump_a_boundary(
    intervening_page,intervening_images,target_page
):
    first, second, tail, middle, answer=(str(uuid4()) for _ in range(5))
    def neighbors(_actor,_selected):
        return [{
            "id":first,"source_text":RANKED,"illustration_ids":[],
            "start_page":10,"end_page":10,
            "next_id":middle,"next_text":"Рынок в торговом квартале.",
            "next_title":"Middle","next_illustrations":(["figure"] if intervening_images else []),
            "next_page":intervening_page,
            "next2_id":answer,"next2_text":TARGET,
            "next2_title":"Answer","next2_illustrations":[],
            "next2_page":target_page,
        }]
    backend=SimpleNamespace(
        corpus=SimpleNamespace(neighbors=neighbors),client=SimpleNamespace(),
        _evidence_url=lambda ident:"knowledge://"+ident,
    )
    response=await SupabaseRestBackend._expand_continuation_results(
        backend,SearchOutput(results=[
            SearchResult(id=first,title="Anchor",url="knowledge://first"),
            SearchResult(id=second,title="Anchor two",url="knowledge://second"),
            SearchResult(id=tail,title="Tail",url="knowledge://tail"),
        ],retrieval_mode="bge",main_state="ready"),
        SimpleNamespace(subject=str(uuid4())),query=QUESTION,match_count=3
    )
    assert [h.id for h in response.results]==[first,second,tail]


def test_strength_prefers_more_query_cues_over_merely_nearest_segment():
    from regional_knowledge.continuation_context import same_page_topic_score
    lower="На острове возник рынок рядом с торговыми рядами."
    assert same_page_topic_score(QUESTION,RANKED,TARGET) > same_page_topic_score(
        QUESTION,RANKED,lower)


def test_sqlite_source_positions_expose_two_nearby_chunks_under_actor_scope(tmp_path):
    from hashlib import sha256
    from regional_knowledge.sqlite_corpus import SQLiteCorpus
    from regional_knowledge.sqlite_data import defaults

    corpus=SQLiteCorpus(tmp_path/"synthetic-evidence.sqlite3")
    owner,document=(str(uuid4()) for _ in range(2))
    corpus.put("rkb_users",[{**defaults("rkb_users"),"id":owner,"status":"active"}])
    corpus.put("rkb_documents",[{
        **defaults("rkb_documents"),"id":document,
        "owner_user_id":owner,"active_revision":1,
        "source_sha256":"a"*64,"title":"Synthetic source",
    }])
    ids=[]
    for position,text in enumerate((RANKED,"Торговые лавки на острове.",TARGET)):
        chunk_id=str(uuid4())
        digest=sha256(text.encode()).hexdigest()
        corpus.put("rkb_chunks",[{
            **defaults("rkb_chunks"),"id":chunk_id,
            "document_id":document,"revision":1,
            "title":"Synthetic passage", "source_text":text,
            "search_material":text,
            "text_sha256":digest,"search_material_sha256":digest,
        }])
        with corpus.connect() as db:
            db.execute("""INSERT INTO chunk_positions(
                chunk_id,document_id,revision,start_position,end_position,article_id)
                VALUES(?,?,?,?,?,NULL)""",
                (chunk_id,document,1,position*1000,position*1000+len(text)))
            db.execute("""INSERT INTO chunk_fragments(
                chunk_id,ordinal,page_id,physical_page_index,region_id,
                text_start,text_end,source_sha256,revision)
                VALUES(?,?,?,?,?,?,?,?,?)""",
                (chunk_id,0,str(uuid4()),10,str(uuid4()),0,len(text),"a"*64,1))
        ids.append(chunk_id)

    results=corpus.neighbors(owner,[ids[0]])
    assert len(results)==1
    assert results[0]["next_id"]==ids[1]
    assert results[0]["next2_id"]==ids[2]
    assert results[0]["next_page"]==results[0]["next2_page"]==10

    outsider=str(uuid4())
    import pytest
    with pytest.raises((LookupError,PermissionError)):
        corpus.neighbors(outsider,[ids[0]])


def test_length_normalization_prefers_answer_dense_over_long_background():
    from regional_knowledge.continuation_context import same_page_topic_score
    # A broad descriptive page can mention more of a question's words than a
    # concise, answer-bearing passage; raw term-count bias is not answer quality.
    broad=("На восточном краю острова начали возводить башню. "
           "Здание задумывалось важным объектом. "
           + "Жители обсуждали рынки, улицы, мосты и товары. "*45)
    concise=TARGET
    source="Жители острова обсуждали новые сооружения."
    assert same_page_topic_score(QUESTION,source,broad)>0
    assert same_page_topic_score(QUESTION,source,concise)>0
    assert same_page_topic_score(QUESTION,source,concise)>same_page_topic_score(
        QUESTION,source,broad)
