"""Actual PG17 publication and computed tail closure, synthetic inputs only."""
import pytest

from regional_knowledge.source_packet_tails import pending_tail_refs
from test_kaggle_apply import PG
from test_source_activation_pg import image_pg_sql18,accepted,stage_command
from test_source_batch_pg import native,packet
from test_source_packet_tails import tail_packet,continuation_packet
from test_source_stage import SOURCE,INGESTION


@PG
@pytest.mark.asyncio
@pytest.mark.parametrize('partial',[False,True])
async def test_pending_tail_is_derived_from_native_regions_and_disappears_after_continuation(image_pg_sql18,partial):
    env=image_pg_sql18;first=tail_packet(partial)
    assert (await accepted(env,stage_command(first,env['sequence']+1))).status=='applied'
    prepare,_=native(env)
    c=packet(env,first);await prepare(c)
    assert (await accepted(env,c)).status=='applied'
    async with env['connection'].transaction():
        before=await pending_tail_refs(env['connection'],SOURCE,INGESTION)
        assert len(before['tail_refs'])==1 and not before['has_more']
        assert before['tail_refs'][0]['region_key']==('body' if partial else 'tail')
        assert before['tail_refs'][0]['physical_page_index']==1
        assert (before['tail_refs'][0]['start']>0) is partial
        with pytest.raises(ValueError,match='source_tail_scope_unavailable'):
            await pending_tail_refs(env['connection'],SOURCE,'00000000-0000-0000-0000-000000000099')
    second=continuation_packet(first)
    assert (await accepted(env,stage_command(second,env['sequence']+1))).status=='applied'
    c=packet(env,second);await prepare(c)
    assert (await accepted(env,c)).status=='applied'
    async with env['connection'].transaction():
        assert await pending_tail_refs(env['connection'],SOURCE,INGESTION)==dict(tail_refs=[],has_more=False)
        rows=await (await env['connection'].execute('SELECT body FROM rkb_v4.passage_search WHERE source_id=%s',(SOURCE,))).fetchall()
        assert ('An unfinished synthetic\nsentence finishes here.',) in rows
        assert len(rows)==3
