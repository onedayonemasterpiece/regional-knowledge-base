"""P0.A #504: SQL044 reads published immutable model packets, not pending stage.

PG fixture is disposable; uses the existing journal, fake vectors and numbered
migration chain. No book files, provider, model, live service, or permissions.
"""
import copy

import pytest

from regional_knowledge.source_stage import canonical,digest
from regional_knowledge.source_units import resolve_source_units
from test_source_activation_pg import image_pg_sql18, accepted,stage_command
from test_source_batch_pg import native,packet
from test_native_source_materializer import exact_batch
from test_source_stage import SOURCE, original
from test_kaggle_apply import PG
from test_kaggle_pg_driver import database


async def unit_pages(db):
    rows=await (await db.execute('''SELECT physical_page_index,page,page_count,read_complete
        FROM rkb_v4.read_source_unit_pages_v1(%s) ORDER BY physical_page_index''',
        (SOURCE,))).fetchall()
    return rows


def mark_one_page(doc,*,title):
    page=doc['model_payload']['pages'][0]
    region=page['regions'][0]
    page['source_unit_version']=1
    page['source_unit_boundaries']=[
        dict(article_id='chapter-one',action='start',region_key=region['region_key'],
            offset=0,kind='chapter',title=title)]
    return doc


@PG
@pytest.mark.asyncio
async def test_sql044_stage_not_published_then_text_receipt_exposes_exact_anchor_and_duplicate_stays_same(image_pg_sql18):
    # Import only the existing disposable synthetic source; no live PG changes.
    env=image_pg_sql18
    first=mark_one_page(exact_batch(count=1,receipt=original(page_count=1)),title='Reviewed first heading')
    assert (await accepted(env,stage_command(first,1))).status=='applied'
    async with env['connection'].transaction():
        assert await unit_pages(env['connection'])==[]
        assert (await (await env['connection'].execute(
            "SELECT has_function_privilege('rkb_reader','rkb_v4.read_source_unit_pages_v1(uuid)','EXECUTE')")).fetchone())==(True,)
    prepare,calls=native(env)
    command=packet(env,first)
    await prepare(command)
    assert (await accepted(env,command)).status=='applied'
    async with env['connection'].transaction():
        rows=await unit_pages(env['connection'])
        assert len(rows)==1 and rows[0][2:]==(1,False)
        unit=resolve_source_units([rows[0][1]],source_id=SOURCE,page_count=rows[0][2],
            read_complete=rows[0][3])[0]
        assert unit['title']=='Reviewed first heading'
        assert unit['state']=='unit_not_ready' and unit['end_anchor'] is None
        assert rows[0][1]['regions'][0]['source_length']==len(first['model_payload']['pages'][0]['regions'][0]['source_text'])
    assert (await accepted(env,command)).duplicate
    assert len(calls)==1
    async with env['connection'].transaction():
        assert rows==await unit_pages(env['connection'])


@PG
@pytest.mark.asyncio
async def test_sql044_latest_only_when_corrected_packet_text_published(image_pg_sql18):
    env=image_pg_sql18
    first=mark_one_page(exact_batch(count=1,receipt=original(page_count=1)),title='Old observed heading')
    prepare,_=native(env)
    assert (await accepted(env,stage_command(first,1))).status=='applied'
    c=packet(env,first)
    await prepare(c)
    assert (await accepted(env,c)).status=='applied'
    second=copy.deepcopy(first)
    second.update(batch_index=1,previous_batch_sha256=digest(canonical(first)))
    second['model_payload']['pages'][0]['source_unit_boundaries'][0]['title']='Corrected reviewed heading'
    assert (await accepted(env,stage_command(second,env['sequence']+1))).status=='applied'
    async with env['connection'].transaction():
        rows=await unit_pages(env['connection'])
        assert rows[0][1]['source_unit_boundaries'][0]['title']=='Old observed heading'
    c=packet(env,second)
    await prepare(c)
    assert (await accepted(env,c)).status=='applied'
    async with env['connection'].transaction():
        rows=await unit_pages(env['connection'])
        assert rows[0][1]['source_unit_boundaries'][0]['title']=='Corrected reviewed heading'
        assert await (await env['connection'].execute(
            'SELECT count(*) FROM rkb_v4.passage_search WHERE source_id=%s',(SOURCE,))).fetchone()==(1,)


@PG
@pytest.mark.asyncio
async def test_sql044_historical_unmarked_stage_stays_unmarked(image_pg_sql18):
    env=image_pg_sql18
    old=exact_batch(count=1,receipt=original(page_count=1))
    frozen=canonical(old)
    prepare,_=native(env)
    assert (await accepted(env,stage_command(old,1))).status=='applied'
    c=packet(env,old);await prepare(c)
    assert (await accepted(env,c)).status=='applied'
    async with env['connection'].transaction():
        rows=await unit_pages(env['connection'])
        assert len(rows)==1 and rows[0][1]['source_unit_boundaries']==[]
        assert resolve_source_units([rows[0][1]],source_id=SOURCE,
            page_count=1,read_complete=False)==[]
    assert canonical(old)==frozen
