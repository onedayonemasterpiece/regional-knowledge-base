"""Figure discovery preserves exact accepted passage links and private locators."""
from types import SimpleNamespace

import pytest

from regional_knowledge.kaggle_passage_illustrations import read_passage_illustrations


@pytest.mark.asyncio
async def test_linked_figure_descriptor_has_real_page_and_no_private_crop_binding():
    calls = []
    async def execute(sql, params):
        calls.append((sql, params))
    async def fetchall():
        return [('passage-id', 'source-id', 'figure-id', dict(page_id='page-id', source_region_id='region-id',
            caption_text='Printed caption', bbox=dict(left=10, top=20, right=30, bottom=40),
            crop_binding={'private': 'original receipt'}, archive_locator='private'), 7,'exact_shared_region')]
    result = await read_passage_illustrations(SimpleNamespace(fetchall=fetchall), execute,
        source_id='source-id', passage_id='passage-id')
    assert result == ((dict(id='rkb:illustration:v1:source-id:figure-id',
        page_id='page-id', association_basis='exact_shared_region',physical_page_index=7),),False)
    sql, params = calls[0]
    assert params == (['passage-id'], ['source-id'], 201)
    assert "p.data->'region_ids' @> jsonb_build_array(i.data->'source_region_id')" in sql
    assert 'NOT s.withdrawn' in sql
    assert 'availability' not in sql  # Discovery does not wait for mirroring.


@pytest.mark.asyncio
async def test_verbose_figures_do_not_expand_text_metadata_and_report_more():
    from regional_knowledge.kaggle_protocol import PassageReadResult,encode_envelope
    from regional_knowledge.kaggle_query import _item
    from regional_knowledge.kaggle_search import SearchEvidence
    from test_kaggle_query import SOURCE,PASSAGE,query
    from regional_knowledge.kaggle_protocol import SourceDependency
    async def execute(sql,params):
        assert 'figure_rank<=%s' in sql
    async def fetchall():
        return [(PASSAGE,SOURCE,str(i),dict(page_id='page',caption_text='c'*2000,
            visual_description='v'*2000),0,'exact_shared_region') for i in range(201)]
    descriptors,more=await read_passage_illustrations(SimpleNamespace(fetchall=fetchall),execute,
        source_id=SOURCE,passage_id=PASSAGE)
    hit=SearchEvidence(PASSAGE,SOURCE,1,'page:0','Accepted source text','Bibliography',
        source_provenance={'native_text':'p'*10000})
    item=_item(hit,SourceDependency(source_id=SOURCE,content_revision=1,eligibility_revision=1),
        0,database_generation='db')
    q=query()
    result=PassageReadResult(request_id=q.request_id,connection_generation=1,turn_generation=0,
        status='ok',evidence=(item,),illustrations=descriptors,illustrations_has_more=more,
        observed_data_seq=5,observed_projection_seq=5,observed_visibility_epoch=0,
        observed_database_generation='db')
    assert result.evidence[0].text=='Accepted source text'
    assert len(descriptors)==200 and more
    assert len(encode_envelope(result))<256*1024
    assert 'illustrations' not in item.metadata.source_provenance


@pytest.mark.asyncio
async def test_exact_figure_uses_current_corrected_catalog_without_leaking_crop():
    from regional_knowledge.kaggle_passage_illustrations import read_source_illustration
    from test_kaggle_public_illustration_gate import SOURCE, ILLUSTRATION
    calls = []
    async def execute(sql, params):
        calls.append((sql, params))
    async def fetchone():
        return (dict(illustration_key='figure',caption_text='Printed caption',
                     crop_binding={'secret': 'private'}), 6, 1, 'internal-ingestion',
                dict(title='Accepted book',authors=['Current author'],publication_year=2020))
    records,evidence,next_ordinal = await read_source_illustration(
        SimpleNamespace(source_id=SOURCE,query=ILLUSTRATION),
        SimpleNamespace(fetchone=fetchone), execute)
    assert next_ordinal is None
    assert len(records)==len(evidence)==1
    assert records[0]['_citation_catalog']==dict(
        title='Accepted book',authors=['Current author'],publication_year=2020)
    assert records[0]['physical_page_index']==6
    assert records[0]['caption_text']=='Printed caption'
    assert 'crop_binding' not in records[0]
    sql,params = calls[0]
    assert 'source_catalog_corrections' in sql
    assert 'coalesce(corrected.metadata,catalog.metadata)' in sql
    assert 's.accepted AND NOT s.withdrawn' in sql
    assert params==(SOURCE,ILLUSTRATION)


@pytest.mark.asyncio
async def test_caption_search_hit_exposes_same_accepted_illustration_id_as_fetch():
    from regional_knowledge.kaggle_passage_illustrations import read_passage_illustrations_batch
    from regional_knowledge.kaggle_search import SearchEvidence
    from regional_knowledge.kaggle_query import _item
    from regional_knowledge.kaggle_protocol import QueryResult, PassageReadResult, SourceDependency
    from regional_knowledge.kaggle_public_backend import KaggleSourceBackend
    from regional_knowledge.query_telemetry import ReadAttempt
    from test_kaggle_query import SOURCE, PASSAGE
    from test_kaggle_exact_fetch import locator

    calls = []
    async def execute(sql, params):
        calls.append((sql, params))
    async def fetchall():
        return [(PASSAGE, SOURCE, 'accepted-figure', {'page_id':'printed-page'}, 4,
                 'accepted_chunk_link')]
    cursor = SimpleNamespace(fetchall=fetchall)
    found = await read_passage_illustrations_batch(cursor, execute,
        passage_ids=(PASSAGE,), source_ids=(SOURCE,), limit=20)
    fetched, _ = await read_passage_illustrations(cursor, execute,
        source_id=SOURCE, passage_id=PASSAGE)

    dep = SourceDependency(source_id=SOURCE, content_revision=1, eligibility_revision=7)
    hit = SearchEvidence(PASSAGE, SOURCE, 1, 'printed page 4', 'Knight caption',
        'Author, page 4', illustrations=found[PASSAGE][0])
    item = _item(hit, dep, 3, database_generation='synthetic-db')
    common = dict(request_id='illustrated-hit', connection_generation=4, turn_generation=9,
        status='ok', evidence=(item,), observed_data_seq=5, observed_projection_seq=5,
        observed_visibility_epoch=3, observed_database_generation='synthetic-db')
    attempt = ReadAttempt('a' * 32)
    search = KaggleSourceBackend._render(QueryResult(mode='hybrid', **common),
        evidence=False, attempt=attempt).results[0]
    fetch = KaggleSourceBackend._render_fetch(PassageReadResult(
        illustrations=fetched, **common), locator=locator(), attempt=attempt)
    assert search.metadata['illustrations'][0]['id'] == fetch.metadata['illustrations'][0]['id']
    assert search.metadata['illustrations'][0]['association_basis'] == 'accepted_chunk_link'
    assert len(calls) == 2 and all('source_batch_publications' in sql for sql, _ in calls)


@pytest.mark.asyncio
async def test_actual_search_previews_five_links_while_fetch_retains_six():
    from uuid import UUID
    from regional_knowledge.kaggle_search import fused_search
    from test_kaggle_pg_search import FixtureConnection
    from test_kaggle_query import SOURCE, PASSAGE, encode

    class LinkedPassage(FixtureConnection):
        async def fetchall(self):
            if 'figure_rank<=' in self.statement:
                return [(PASSAGE, SOURCE, f'figure-{i}', {'page_id': 'page'}, 4,
                         'accepted_chunk_link') for i in range(min(6, self.statements[-1][1][-1]))]
            if 'lexical_candidates' in self.statement or 'dense_candidates' in self.statement:
                return [(UUID(PASSAGE), .1)]
            return [(UUID(PASSAGE), UUID(SOURCE), 1, 'page 4', 'caption', 'book', {}, True)]

    connection = LinkedPassage()
    result = await fused_search(connection, 'caption', encode=encode)
    hit = result.evidence[0]
    assert len(hit.illustrations) == 5 and hit.illustrations_has_more
    fetched, more = await read_passage_illustrations(connection, connection.execute,
        source_id=SOURCE, passage_id=PASSAGE)
    assert len(fetched) == 6 and not more
    assert hit.illustrations == fetched[:5]
