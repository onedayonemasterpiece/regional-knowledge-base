"""Actual disposable PG17 packet → Story → accepted passage refs; no live DB or provider."""
import copy
import time
from types import SimpleNamespace

import pytest

from regional_knowledge.kaggle_editorial_read import _accepted_story_passages
from regional_knowledge.kaggle_editorial_contracts import EditorialReadResult
from regional_knowledge.kaggle_story_read import story_item
from regional_knowledge.kaggle_public_backend import KaggleSourceBackend, parse_passage_fetch_id
from regional_knowledge.source_stage import canonical, digest
from test_source_batch_pg import native, packet
from test_source_activation_pg import image_pg_sql18, accepted, stage_command
from test_native_source_materializer import exact_batch
from test_kaggle_apply import PG
from test_kaggle_pg_driver import database  # fixture backing image_pg_sql18

@PG
@pytest.mark.asyncio
async def test_native_accepted_two_page_story_returns_fetchable_source_bound_page_refs(image_pg_sql18):
    env=image_pg_sql18
    first=exact_batch(count=1)
    second=exact_batch(1,digest(canonical(first)),start=1,count=1)
    pages=[first['model_payload']['pages'][0],second['model_payload']['pages'][0]]
    body='Один проверенный эпизод начинается на первой странице и завершается на второй.'
    second['model_payload']['story_candidates']=[dict(candidate_key='two-page-episode',
        title='Одна история, две страницы',summary='Проверенный синтетический эпизод.',
        proposition='Две страницы содержат один описанный эпизод.',
        evidence_refs=[dict(page_id=page['page_id'],region_key='body',
            original_excerpt=page['regions'][0]['source_text']) for page in pages],
        basic_variant=dict(body=body,language='ru',
            blocks=[dict(start=0,end=len(body),evidence_indexes=[0,1])]))]
    prepare,_=native(env)
    assert (await accepted(env,stage_command(first,1))).status=='applied'
    command=packet(env,first);await prepare(command)
    assert (await accepted(env,command)).status=='applied'
    assert (await accepted(env,stage_command(second,env['sequence']+1))).status=='applied'
    command=packet(env,second);await prepare(command)
    assert (await accepted(env,command)).status=='applied'
    command=packet(env,second,'semantic');await prepare(command)
    assert (await accepted(env,command)).status=='applied'
    async with env['connection'].transaction():
        async with env['connection'].cursor() as cursor:
            await cursor.execute('SELECT story_id FROM rkb_v4.stories LIMIT 1')
            story_id=(await cursor.fetchone())[0]
            await cursor.execute(
                "SELECT rkb_v4.read_editorial_story_v1(%s,NULL,'get',0,1,NULL)",(story_id,))
            record=(await cursor.fetchone())[0]
            assert record['body']==body
            async def execute(statement,args=()):
                await cursor.execute(statement,args)
            enriched=await _accepted_story_passages(record,execute,cursor)
            refs=enriched['passage_refs']
            assert len(refs)==2
            assert [v['physical_page_index'] for v in refs]==[0,1]
            assert {v['page_id'] for v in refs}=={p['page_id'] for p in pages}
            await cursor.execute('SELECT passage_id::text FROM rkb_v4.passage_search')
            assert {v['passage_id'] for v in refs}=={row[0] for row in await cursor.fetchall()}
            result=EditorialReadResult(request_id='story-passages',connection_generation=1,
                turn_generation=1,status='ok',record=enriched,evidence=(story_item(enriched),),
                observed_data_seq=env['sequence'],observed_projection_seq=env['sequence'],
                observed_visibility_epoch=1,observed_database_generation='fixture-generation')
            output=KaggleSourceBackend._render_story(result,
                attempt=SimpleNamespace(correlation=lambda:dict(trace_id='a'*32),started=time.monotonic()))
            for item in output['passage_refs']:
                locator=parse_passage_fetch_id(item['fetch_id'])
                assert locator.passage_id==item['passage_id']
                assert locator.source.source_id==enriched['source']['source_id']
                assert locator.source.eligibility_revision==enriched['source']['eligibility_revision']
                assert locator.database_generation=='fixture-generation'
            # A forged source selector cannot recycle another source's passage refs.
            foreign=copy.deepcopy(record)
            foreign['source']['source_id']='00000000-0000-0000-0000-000000000099'
            assert (await _accepted_story_passages(foreign,execute,cursor))['passage_refs']==[]
    # Ordered replay runs outside the explicit read transaction.
    assert (await accepted(env,command)).duplicate is True
