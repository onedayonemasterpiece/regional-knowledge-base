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
        return [('figure-id', dict(page_id='page-id', source_region_id='region-id',
            caption_text='Printed caption', bbox=dict(left=10, top=20, right=30, bottom=40),
            crop_binding={'private': 'original receipt'}, archive_locator='private'), 7,'exact_shared_region')]
    result = await read_passage_illustrations(SimpleNamespace(fetchall=fetchall), execute,
        source_id='source-id', passage_id='passage-id')
    assert result == ((dict(id='rkb:illustration:v1:source-id:figure-id',
        page_id='page-id', association_basis='exact_shared_region',physical_page_index=7),),False)
    sql, params = calls[0]
    assert params == ('source-id', 'passage-id')
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
        assert 'LIMIT 201' in sql
    async def fetchall():
        return [(str(i),dict(page_id='page',caption_text='c'*2000,
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
