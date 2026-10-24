"""Accepted figure lookup under the actual native reader role, no media archive."""
from contextlib import asynccontextmanager
from copy import deepcopy
import json
from pathlib import Path

import pytest
import psycopg
from regional_knowledge.kaggle_owned_pg import configure_native_roles
from regional_knowledge.kaggle_passage_illustrations import read_passage_illustrations
from regional_knowledge.kaggle_source_read import SourceReadRequest,execute_source_read
from test_kaggle_apply import PG
from test_kaggle_pg_driver import database,connect
from test_native_image_projection_20261022 import illustrated_batch
from test_source_activation_pg import image_pg_sql18,accepted,stage_command,DB
from test_source_batch_pg import native,packet
from test_source_stage import SOURCE
from test_source_activation import PERSON

ROOT=Path(__file__).resolve().parents[1]

@PG
@pytest.mark.asyncio
async def test_accepted_figure_reader_and_same_page_negative(image_pg_sql18):
    env=image_pg_sql18;connection=env['connection'];doc=json.loads(illustrated_batch())
    page=doc['model_payload']['pages'][0]
    page['regions'].extend([
        dict(region_key='other-figure',kind='figure',reading_order=3,
             bbox=dict(left=600,top=600,right=800,bottom=800),source_text=''),
        dict(region_key='other-caption',kind='caption',reading_order=4,
             bbox=dict(left=600,top=820,right=800,bottom=860),source_text='An unrelated illustration.')])
    other=deepcopy(page['illustrations'][0])
    other.update(illustration_key='other',source_region_key='other-figure',
        caption_region_keys=['other-caption'])
    page['illustrations'].append(other)
    chunk=deepcopy(doc['model_payload']['chunks'][0])
    chunk.update(chunk_key='zz-unrelated',
        region_refs=[dict(page_id=page['page_id'],region_key='other-caption')],
        illustration_refs=[dict(page_id=page['page_id'],illustration_key='other')])
    doc['model_payload']['chunks'].append(chunk)
    prepare,_=native(env)
    assert (await accepted(env,stage_command(doc,1))).status=='applied'
    for part in ('text','semantic'):
        command=packet(env,doc,part);await prepare(command)
        assert (await accepted(env,command)).status=='applied'
    async with connection.transaction():
        await connection.execute('CREATE ROLE rkb_bootstrap NOLOGIN')
        await configure_native_roles(connection,selected_sql=sorted(
            path.name for path in (ROOT/'sql/kaggle').glob('[0-9][0-9][0-9]_*.sql')),
            dbname=connection.info.dbname,create_roles=False)
        figure=(await (await connection.execute("SELECT illustration_id FROM rkb_v4.source_illustrations WHERE data->>'illustration_key'='picture'")).fetchone())[0]
        passage=(await (await connection.execute('SELECT passage_id FROM rkb_v4.passage_search ORDER BY ordinal LIMIT 1')).fetchone())[0]
    @asynccontextmanager
    async def role(name):
        conn=await connect()
        try:
            async with conn.transaction():await conn.execute('SET ROLE '+name)
            yield conn
        finally:await conn.close()
    request=SourceReadRequest(request_id='figure',connection_generation=1,turn_generation=0,
        operation='source_illustration',source_id=SOURCE,query=str(figure),
        min_data_seq=env['sequence'],deadline_ms=2000,actor=dict(person_id=PERSON,
            audience='personal',auth_epoch=1,visibility_epoch=0,context_ref='fixture'))
    try:
        async with role('rkb_reader') as conn:
            async with conn.transaction():
                async with conn.cursor() as cursor:
                    descriptors,more=await read_passage_illustrations(cursor,cursor.execute,
                        source_id=SOURCE,passage_id=str(passage))
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                async with conn.transaction():
                    await conn.execute('SELECT original_receipt FROM rkb_v4.source_stage_sessions')
        assert len(descriptors)==1 and descriptors[0]['id'].endswith(':'+str(figure)) and not more
        result=await execute_source_read(request,acquire=lambda:role('rkb_reader'))
        assert result.status=='ok' and result.records[0]['physical_page_index']==0
        assert result.records[0]['illustration_id']==str(figure)
        assert result.records[0]['ingestion_id']==doc['ingestion_id']
        assert 'crop_binding' not in result.records[0]
        async with connection.transaction():
            await connection.execute('UPDATE rkb_v4.sources SET withdrawn=true WHERE source_id=%s',(SOURCE,))
        result=await execute_source_read(request,acquire=lambda:role('rkb_reader'))
        assert result.status=='unavailable' and not result.records and not result.evidence
    finally:
        async with connection.transaction():
            await connection.execute('DROP OWNED BY rkb_bootstrap')
            await connection.execute('DROP ROLE rkb_bootstrap')
