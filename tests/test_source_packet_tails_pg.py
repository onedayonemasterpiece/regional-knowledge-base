"""Actual PG17 publication and computed tail closure, synthetic inputs only."""
import pytest

from regional_knowledge.source_packet_tails import pending_tail_refs
from test_kaggle_apply import PG
from test_kaggle_pg_driver import database
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


@PG
@pytest.mark.asyncio
async def test_fully_covered_book_tail_query_keeps_two_second_budget(database):
    # Query-only projection fixture: 1000 pages × 16 regions, no vectors or corpus.
    # Materializer/continuation semantics are exercised above on current migrations.
    from uuid import UUID, uuid5
    import hashlib
    from psycopg.types.json import Jsonb
    c=database
    async with c.transaction():
        await c.execute("DROP SCHEMA rkb_v4 CASCADE; CREATE SCHEMA rkb_v4")
        await c.execute("""CREATE TABLE rkb_v4.sources(source_id uuid PRIMARY KEY,accepted bool,withdrawn bool);
            CREATE TABLE rkb_v4.source_stage_sessions(source_id uuid,ingestion_id uuid);
            CREATE TABLE rkb_v4.source_regions(region_id uuid PRIMARY KEY,source_id uuid,page_id uuid,source_text text,source_text_sha256 text);
            CREATE INDEX source_regions_page ON rkb_v4.source_regions(source_id,page_id);
            CREATE TABLE rkb_v4.source_stage_pages(source_id uuid,page_id uuid,physical_page_index int,batch_index int,model_page jsonb,UNIQUE(source_id,page_id));
            CREATE TABLE rkb_v4.source_stage_batches(source_id uuid,batch_index int,payload_bytes bytea,PRIMARY KEY(source_id,batch_index));
            CREATE TABLE rkb_v4.source_passage_provenance(passage_id uuid PRIMARY KEY,source_id uuid,source_revision bigint,data jsonb);
            CREATE INDEX source_passage_provenance_source ON rkb_v4.source_passage_provenance(source_id,source_revision);
            CREATE INDEX source_passage_provenance_pages ON rkb_v4.source_passage_provenance USING gin(data jsonb_path_ops);""")
        await c.execute('INSERT INTO rkb_v4.sources VALUES(%s,true,false)',(SOURCE,))
        await c.execute('INSERT INTO rkb_v4.source_stage_sessions VALUES(%s,%s)',(SOURCE,INGESTION))
        region_rows=[];provenance=[];pages=[];batches=[]
        text='Synthetic source words. '*12;sha=hashlib.sha256(text.encode()).hexdigest()
        for index in range(1000):
            page_id=str(uuid5(UUID(SOURCE),'page:1:'+str(index)));regions=[]
            for n in range(16):
                key='body'+str(n);ident=str(uuid5(UUID(page_id),'region:'+key))
                regions.append(dict(region_key=key,kind='body',source_text=text))
                region_rows.append((ident,SOURCE,page_id,text,sha))
                provenance.append((ident,SOURCE,1,Jsonb(dict(page_ids=[page_id],region_ids=[ident],
                    source_spans=[dict(region_id=ident,end=len(text))]))))
            page=dict(page_id=page_id,regions=regions)
            pages.append((SOURCE,page_id,index,index,Jsonb(page)))
            batches.append((SOURCE,index,Jsonb(dict(model_payload=dict(pages=[page])))))
        async with c.cursor() as cursor:
            await cursor.executemany('INSERT INTO rkb_v4.source_regions VALUES(%s,%s,%s,%s,%s)',region_rows)
            await cursor.executemany('INSERT INTO rkb_v4.source_passage_provenance VALUES(%s,%s,%s,%s)',provenance)
            await cursor.executemany('INSERT INTO rkb_v4.source_stage_pages VALUES(%s,%s,%s,%s,%s)',pages)
            await cursor.executemany("INSERT INTO rkb_v4.source_stage_batches VALUES(%s,%s,convert_to(%s::jsonb::text,'UTF8'))",batches)
        await c.execute('ANALYZE')
    async with c.transaction():
        await c.execute("SET LOCAL statement_timeout='2s'; SET LOCAL jit=off")
        assert await pending_tail_refs(c,SOURCE,INGESTION)==dict(tail_refs=[],has_more=False)
    # Same page/region scope, partial spans and full/footnote coverage keep their meaning.
    ident,_,page_id,_,_=region_rows[-1]
    async with c.transaction():
        await c.execute("DELETE FROM rkb_v4.source_passage_provenance WHERE passage_id=%s",(ident,))
        await c.execute("SET LOCAL statement_timeout='2s'; SET LOCAL jit=off")
        tail=(await pending_tail_refs(c,SOURCE,INGESTION))['tail_refs']
        assert len(tail)==1 and tail[0]['region_id']==ident and tail[0]['start']==0
        wrong=dict(page_ids=[pages[0][1]],region_ids=[ident],source_spans=[dict(region_id=ident,end=len(text))])
        await c.execute('INSERT INTO rkb_v4.source_passage_provenance VALUES(%s,%s,1,%s)',(ident,SOURCE,Jsonb(wrong)))
        assert len((await pending_tail_refs(c,SOURCE,INGESTION))['tail_refs'])==1
        partial=dict(page_ids=[page_id],region_ids=[ident],source_spans=[dict(region_id=ident,end=10)])
        await c.execute('UPDATE rkb_v4.source_passage_provenance SET data=%s WHERE passage_id=%s',(Jsonb(partial),ident))
        assert (await pending_tail_refs(c,SOURCE,INGESTION))['tail_refs'][0]['start']==10
        for coverage in [dict(page_ids=[page_id],region_ids=[ident],source_spans=[]),
                dict(page_ids=[page_id],region_ids=[],source_spans=[dict(region_id=ident,end=10)],footnote_region_ids=[ident])]:
            await c.execute('UPDATE rkb_v4.source_passage_provenance SET data=%s WHERE passage_id=%s',(Jsonb(coverage),ident))
            assert await pending_tail_refs(c,SOURCE,INGESTION)==dict(tail_refs=[],has_more=False)
        # Keep all candidates from the cutoff batch before ordering its authored pages.
        # A naive LIMIT 5 before the JSON page ordinal would lose the earliest page.
        chosen=pages[100:106]
        for _,page_id,index,_,_ in chosen:
            ident=str(uuid5(UUID(page_id),'region:body0'))
            await c.execute('DELETE FROM rkb_v4.source_passage_provenance WHERE passage_id=%s',(ident,))
            await c.execute('UPDATE rkb_v4.source_stage_pages SET batch_index=100 WHERE page_id=%s',(page_id,))
        authored=[page[-1].obj for page in reversed(chosen)]
        await c.execute("UPDATE rkb_v4.source_stage_batches SET payload_bytes=convert_to(%s::jsonb::text,'UTF8') WHERE batch_index=100",
            (Jsonb(dict(model_payload=dict(pages=authored))),))
        result=await pending_tail_refs(c,SOURCE,INGESTION)
        assert [ref['physical_page_index'] for ref in result['tail_refs']]==[105,104,103,102]
        assert result['has_more']
