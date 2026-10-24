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
    assert result == [dict(id='rkb:illustration:v1:source-id:figure-id',
        illustration_id='figure-id', source_id='source-id', page_id='page-id',
        association_basis='exact_shared_region',
        source_region_id='region-id', physical_page_index=7, caption_text='Printed caption',
        bbox=dict(left=10, top=20, right=30, bottom=40))]
    sql, params = calls[0]
    assert params == ('source-id', 'passage-id')
    assert "p.data->'region_ids' @> jsonb_build_array(i.data->'source_region_id')" in sql
    assert 'NOT s.withdrawn' in sql
    assert 'availability' not in sql  # Discovery does not wait for mirroring.
