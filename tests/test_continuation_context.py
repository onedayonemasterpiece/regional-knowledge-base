
import hashlib
import os
from uuid import UUID, uuid4

import psycopg
import pytest

from test_graph_postgres import graph_db
from regional_knowledge.continuation_context import (
    boundary_query_overlap,
    query_allows_continuation_expansion,
    strong_continuation_boundary,
)
from regional_knowledge.contracts import Principal, SearchOutput, SearchResult
from regional_knowledge.postgres_backend import PostgresBackend
from regional_knowledge.supabase_backend import LexicalOnlyEmbedder


def test_conservative_continuation_heuristic():
    assert strong_continuation_boundary(
        "Кроме замка Унфридт построил сиротский приют, почтамт и",
        "Трагхаймскую церковь. Затем он занялся улицами.",
    ) >= 4
    assert strong_continuation_boundary(
        "Радикальные силы пришли из социал-",
        "демократии. Дальше следовало новое предложение.",
    ) >= 4
    assert strong_continuation_boundary(
        "Это полностью законченное предложение.",
        "Следующее предложение начинается отдельно.",
    ) == 0
    assert strong_continuation_boundary(
        "Подпись без точки",
        "следующая подпись",
        left_has_illustrations=True,
    ) == 0
    assert boundary_query_overlap(
        "Какие здания, кроме замка, построил Унфридт?",
        "Кроме замка Унфридт построил сиротский приют, почтамт и",
        "Трагхаймскую церковь.",
    ) >= 2
    assert boundary_query_overlap(
        "совсем другой вопрос о торговле",
        "Кроме замка Унфридт построил сиротский приют, почтамт и",
        "Трагхаймскую церковь.",
    ) < 2
    assert query_allows_continuation_expansion(
        "Какие здания, кроме замка, построил Унфридт?",
        "Кроме замка Унфридт построил сиротский приют, почтамт и",
        "Трагхаймскую церковь.",
    )
    assert not query_allows_continuation_expansion(
        "Когда Унфридт построил сиротский приют?",
        "Кроме замка Унфридт построил сиротский приют, почтамт и",
        "Трагхаймскую церковь.",
    )
    assert query_allows_continuation_expansion(
        "Что известно про Унфридта и Трагхаймскую церковь?",
        "Кроме замка Унфридт построил сиротский приют, почтамт и",
        "Трагхаймскую церковь.",
    )
    assert not query_allows_continuation_expansion(
        "Когда Унфридт построил сиротский приют?",
        "Предыдущий исторический контекст и",
        "Унфридт построил сиротский приют в определённом году.",
        neighbor_side="left",
    )
    assert query_allows_continuation_expansion(
        "Что связывает предыдущий контекст и Унфридта?",
        "Предыдущий исторический контекст и",
        "Унфридт построил сиротский приют в определённом году.",
        neighbor_side="left",
    )


class _Store:
    async def get_range(self, key, start, end):
        raise AssertionError("continuation expansion must not read object bytes")


def _insert_doc(db, owner, title):
    doc, obj, page = uuid4(), uuid4(), uuid4()
    db.execute(
        "insert into rkb_documents(id,owner_user_id,title,source_sha256,active_revision,page_count) "
        "values(%s,%s,%s,%s,1,1)",
        (doc, owner, title, hashlib.sha256(title.encode()).hexdigest()),
    )
    db.execute(
        "insert into rkb_objects(id,document_id,kind,object_key,sha256,mime_type,size_bytes) "
        "values(%s,%s,'text_projection',%s,%s,'text/plain',4096)",
        (obj, doc, str(obj), "a" * 64),
    )
    db.execute(
        "insert into rkb_pages(id,document_id,physical_page_index,width,height,revision) "
        "values(%s,%s,0,1000,1000,1)",
        (page, doc),
    )
    return doc, obj, page


def _insert_chunk(db, doc, obj, page, order, text, title):
    chunk, region = uuid4(), uuid4()
    digest = hashlib.sha256(text.encode()).hexdigest()
    start = order * 10000
    db.execute(
        "insert into rkb_regions(id,page_id,kind,bbox,reading_order,text_sha256,source_text) "
        "values(%s,%s,'body','{\"left\":0,\"top\":0,\"right\":1000,\"bottom\":1000}',%s,%s,%s)",
        (region, page, order, digest, text),
    )
    db.execute(
        "insert into rkb_chunks("
        "id,document_id,text_object_id,title,text_start,text_end,text_sha256,source_text,"
        "fts,page_ids,region_ids,revision"
        ") values(%s,%s,%s,%s,%s,%s,%s,%s,to_tsvector('simple',%s),%s,%s,1)",
        (
            chunk, doc, obj, title, start, start + len(text.encode()), digest, text,
            text, [page], [region],
        ),
    )
    return chunk


@pytest.mark.asyncio
async def test_postgres_continuation_expansion_is_bounded_and_rls_safe(graph_db):
    owner, other = uuid4(), uuid4()
    with psycopg.connect(graph_db, autocommit=True) as db:
        db.execute("insert into rkb_users(id) values(%s),(%s)", (owner, other))
        doc, obj, page = _insert_doc(db, owner, "Continuation fixture")
        complete = _insert_chunk(
            db, doc, obj, page, 0,
            "Полностью законченное предложение.", "complete",
        )
        left = _insert_chunk(
            db, doc, obj, page, 1,
            "Кроме замка Унфридт построил сиротский приют, ворота, почтамт и",
            "left",
        )
        right = _insert_chunk(
            db, doc, obj, page, 2,
            "Трагхаймскую церковь. Затем он занимался благоустройством.",
            "right",
        )
        unrelated = _insert_chunk(
            db, doc, obj, page, 3,
            "Отдельный независимый сюжет заканчивается точкой.", "unrelated",
        )
        unrelated2 = _insert_chunk(
            db, doc, obj, page, 4,
            "Второй самостоятельный сюжет также завершён.", "unrelated-2",
        )
        unrelated3 = _insert_chunk(
            db, doc, obj, page, 5,
            "Третий самостоятельный сюжет также завершён.", "unrelated-3",
        )
        left2 = _insert_chunk(
            db, doc, obj, page, 6,
            "Унфридт построил и другие городские здания и",
            "left-2",
        )
        right2 = _insert_chunk(
            db, doc, obj, page, 7,
            "новую церковь для жителей другого квартала.",
            "right-2",
        )
        unrelated4 = _insert_chunk(
            db, doc, obj, page, 8,
            "Четвёртый самостоятельный сюжет также завершён.", "unrelated-4",
        )
        other_doc, other_obj, other_page = _insert_doc(db, other, "Other owner")
        hidden = _insert_chunk(
            db, other_doc, other_obj, other_page, 0,
            "скрытый текст и", "hidden",
        )
        _insert_chunk(
            db, other_doc, other_obj, other_page, 1,
            "продолжение скрытого текста.", "hidden-next",
        )

    backend = PostgresBackend(
        graph_db,
        embedder=LexicalOnlyEmbedder(),
        object_store=_Store(),
        pool_max_size=2,
    )
    principal = Principal(
        subject=str(owner), client_id="test", issuer="test", access_token="internal"
    )
    other_principal = Principal(
        subject=str(other), client_id="test", issuer="test", access_token="internal"
    )
    try:
        output = SearchOutput(
            results=[
                SearchResult(id=str(left), title="left", url="knowledge://left"),
                SearchResult(id=str(unrelated), title="unrelated", url="knowledge://unrelated"),
                SearchResult(id=str(complete), title="complete", url="knowledge://complete"),
            ],
            retrieval_mode="lexical_only",
        )
        expanded = await backend._expand_continuation_results(
            output, principal, query="Какие здания, кроме замка, построил Унфридт?", match_count=3
        )
        assert [item.id for item in expanded.results] == [
            str(left), str(unrelated), str(right)
        ]
        signal = expanded.results[2].ranking_signals[-1]
        assert signal["branch"] == "continuation_neighbor"
        assert signal["direction"] == "next"
        assert signal["source_chunk_id"] == str(left)
        assert len(expanded.results) == 3

        single_fact = await backend._expand_continuation_results(
            output,
            principal,
            query="Когда Унфридт построил сиротский приют?",
            match_count=3,
        )
        assert [item.id for item in single_fact.results] == [
            str(left), str(unrelated), str(complete)
        ]

        # Source adjacency is page/region order, not text-projection offset.
        # Deliberately reverse offsets and add an overlapping window: right must
        # still be the continuation neighbor, while overlap must be skipped.
        with psycopg.connect(graph_db, autocommit=True) as db:
            left_regions=db.execute(
                "select region_ids from rkb_chunks where id=%s",(left,)
            ).fetchone()[0]
            right_regions=db.execute(
                "select region_ids from rkb_chunks where id=%s",(right,)
            ).fetchone()[0]
            db.execute(
                "update rkb_chunks set text_start=50000,text_end=50100 where id=%s",
                (left,),
            )
            db.execute(
                "update rkb_chunks set text_start=10000,text_end=10100 where id=%s",
                (right,),
            )
            overlap=uuid4()
            overlap_text="overlapping retrieval window"
            overlap_sha=hashlib.sha256(overlap_text.encode()).hexdigest()
            db.execute(
                """insert into rkb_chunks(
                    id,document_id,text_object_id,title,text_start,text_end,text_sha256,
                    source_text,fts,page_ids,region_ids,revision
                ) values(%s,%s,%s,'overlap',20000,20020,%s,%s,
                    to_tsvector('simple',%s),%s,%s,1)""",
                (
                    overlap,doc,obj,overlap_sha,overlap_text,overlap_text,
                    [page],[left_regions[0],right_regions[0]],
                ),
            )
        source_order = SearchOutput(
            results=[
                SearchResult(id=str(left), title="left", url="knowledge://left"),
                SearchResult(id=str(unrelated), title="unrelated", url="knowledge://unrelated"),
                SearchResult(id=str(complete), title="complete", url="knowledge://complete"),
            ],
            retrieval_mode="e5_bge_lexical",
        )
        ordered = await backend._expand_continuation_results(
            source_order, principal,
            query="Какие здания, кроме замка, построил Унфридт?",
            match_count=3,
        )
        assert str(right) in [item.id for item in ordered.results]
        assert str(overlap) not in [item.id for item in ordered.results]

        # Pre-existing complete context for rank 2 must be protected before
        # rank 1 inserts its missing neighbor. Otherwise rank-8 right2 can be
        # mistaken for the lowest unrelated slot and evicted.
        protected_pair = SearchOutput(
            results=[
                SearchResult(id=str(left), title="left", url="knowledge://left"),
                SearchResult(id=str(left2), title="left-2", url="knowledge://left-2"),
                SearchResult(id=str(complete), title="complete", url="knowledge://complete"),
                SearchResult(id=str(unrelated), title="unrelated", url="knowledge://unrelated"),
                SearchResult(id=str(unrelated2), title="unrelated-2", url="knowledge://unrelated-2"),
                SearchResult(id=str(unrelated3), title="unrelated-3", url="knowledge://unrelated-3"),
                SearchResult(id=str(unrelated4), title="unrelated-4", url="knowledge://unrelated-4"),
                SearchResult(id=str(right2), title="right-2", url="knowledge://right-2"),
            ],
            retrieval_mode="e5_bge_lexical",
        )
        protected = await backend._expand_continuation_results(
            protected_pair,
            principal,
            query="Какие здания, кроме замка, построил Унфридт?",
            match_count=8,
        )
        protected_ids=[item.id for item in protected.results]
        assert str(right) in protected_ids
        assert str(left2) in protected_ids
        assert str(right2) in protected_ids
        assert len(protected_ids)==8

        # Low-ranked source hits are intentionally not expanded: preserving
        # independent tail evidence is more important than speculative context.
        low_ranked = SearchOutput(
            results=[
                SearchResult(id=str(unrelated), title="unrelated", url="knowledge://unrelated"),
                SearchResult(id=str(complete), title="complete", url="knowledge://complete"),
                SearchResult(id=str(unrelated2), title="unrelated-2", url="knowledge://unrelated-2"),
                SearchResult(id=str(unrelated3), title="unrelated-3", url="knowledge://unrelated-3"),
                SearchResult(id=str(left), title="left", url="knowledge://left"),
            ],
            retrieval_mode="bge_lexical",
        )
        low_expanded = await backend._expand_continuation_results(
            low_ranked, principal, query="Какие здания, кроме замка, построил Унфридт?", match_count=5
        )
        assert str(right) not in [item.id for item in low_expanded.results]

        already_present = SearchOutput(
            results=[
                SearchResult(id=str(right), title="right", url="knowledge://right"),
                SearchResult(id=str(left), title="left", url="knowledge://left"),
                SearchResult(id=str(unrelated), title="unrelated", url="knowledge://unrelated"),
            ],
            retrieval_mode="e5_bge_lexical",
        )
        present = await backend._expand_continuation_results(
            already_present, principal, query="Какие здания, кроме замка, построил Унфридт?", match_count=3
        )
        assert [item.id for item in present.results] == [
            str(right), str(left), str(unrelated)
        ]
        assert any(
            signal["branch"] == "continuation_neighbor"
            for signal in present.results[0].ranking_signals
        )

        complete_only = SearchOutput(
            results=[
                SearchResult(id=str(complete), title="complete", url="knowledge://complete"),
                SearchResult(id=str(unrelated), title="unrelated", url="knowledge://unrelated"),
            ],
            retrieval_mode="lexical_only",
        )
        unchanged = await backend._expand_continuation_results(
            complete_only, principal, query="Отдельный самостоятельный вопрос", match_count=2
        )
        assert [item.id for item in unchanged.results] == [str(complete), str(unrelated)]

        # A caller who cannot read the owner document never receives its adjacent
        # chunk even if a malicious synthetic SearchOutput names one owner chunk.
        denied_seed = SearchOutput(
            results=[
                SearchResult(id=str(left), title="left", url="knowledge://left"),
                SearchResult(id=str(hidden), title="hidden", url="knowledge://hidden"),
            ],
            retrieval_mode="lexical_only",
        )
        denied = await backend._expand_continuation_results(
            denied_seed, other_principal, query="Какие здания, кроме замка, построил Унфридт?", match_count=2
        )
        assert str(right) not in [item.id for item in denied.results]

        # Fast/lexical path invokes the same continuation post-pass after SQL
        # ranking. Routing is tested independently from the query-aware insertion
        # decision so a source-only query does not need to manufacture context.
        routed=[]
        original_expand=backend._expand_continuation_results
        async def route_spy(output, route_principal, *, query, match_count):
            routed.append((query,match_count,[item.id for item in output.results]))
            return output
        backend._expand_continuation_results=route_spy
        try:
            fast = await backend.search(
                "Унфридт", principal, match_count=3, _fast_only=True
            )
        finally:
            backend._expand_continuation_results=original_expand
        assert str(left) in [item.id for item in fast.results]
        assert routed and routed[0][0]=="Унфридт" and routed[0][1]==3

        # Main BGE path also applies the post-pass. Only the expensive rank
        # computation is replaced; neighbor hydration remains real Postgres/RLS.
        import regional_knowledge.multilingual_retrieval as multilingual
        original_main=multilingual.main_search
        async def fake_main(*args,**kwargs):
            return SearchOutput(
                results=[
                    SearchResult(id=str(left),title="left",url="knowledge://left"),
                    SearchResult(id=str(unrelated),title="unrelated",url="knowledge://unrelated"),
                    SearchResult(id=str(complete),title="complete",url="knowledge://complete"),
                ],
                retrieval_mode="bge_lexical",
                main_state="ready",
            )
        multilingual.main_search=fake_main
        old_bge=os.environ.get("RKB_BGE_ENABLED")
        old_auto=os.environ.get("RKB_AUTO_INDEX_ENABLED")
        os.environ["RKB_BGE_ENABLED"]="1"
        os.environ["RKB_AUTO_INDEX_ENABLED"]="0"
        try:
            main=await backend.search(
                "Какие здания, кроме замка, построил Унфридт?",
                principal,match_count=3
            )
        finally:
            multilingual.main_search=original_main
            if old_bge is None:os.environ.pop("RKB_BGE_ENABLED",None)
            else:os.environ["RKB_BGE_ENABLED"]=old_bge
            if old_auto is None:os.environ.pop("RKB_AUTO_INDEX_ENABLED",None)
            else:os.environ["RKB_AUTO_INDEX_ENABLED"]=old_auto
        main_ids=[item.id for item in main.results]
        assert main_ids==[str(left),str(unrelated),str(right)]
        assert main.results[-1].ranking_signals[-1]["branch"]=="continuation_neighbor"
    finally:
        await backend.aclose()
