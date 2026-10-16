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
    first, second, neighbor = (str(uuid4()) for _ in range(3))
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
    ],retrieval_mode="bge",main_state="ready")
    result=await SupabaseRestBackend._expand_continuation_results(
        backend,output,principal,query=QUESTION,match_count=2
    )
    assert requested == [(principal.subject, [first, second])]
    assert [h.id for h in result.results] == [first,neighbor]
    assert result.results[1].ranking_signals[0]["branch"]=="same_page_context"
    assert result.results[1].ranking_signals[0]["direction"]=="previous"
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
