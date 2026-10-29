"""Current accepted catalog corrections feed PDF; original stage bytes stay immutable."""
from contextlib import asynccontextmanager

import pytest

from regional_knowledge.source_catalog_command import CODEC,resolved_catalog_command
from regional_knowledge.source_unit_export import render_accepted_source_pdf
from test_kaggle_apply import PG
from test_kaggle_pg_driver import database
from test_source_activation_pg import image_pg_sql18,accepted,stage_command,TIME
from test_source_batch_pg import native,packet
from test_native_source_materializer import exact_batch
from test_source_stage import SOURCE,INGESTION,REQUEST,original
from test_source_activation import PERSON


@PG
@pytest.mark.asyncio
async def test_pdf_edition_uses_latest_accepted_correction_and_original_fallback(image_pg_sql18):
    import pymupdf
    env=image_pg_sql18;db=env['connection']
    doc=exact_batch(count=1,receipt=original(page_count=1))
    doc['source']['metadata']['publication_year']=1985
    page=doc['model_payload']['pages'][0];region=page['regions'][0]
    page['source_unit_version']=1
    page['source_unit_boundaries']=[
        dict(article_id='fixture-unit',action='start',region_key=region['region_key'],offset=0,
            kind='article',title='Synthetic article'),
        dict(article_id='fixture-unit',action='end',region_key=region['region_key'],offset=len(region['source_text']))]
    assert (await accepted(env,stage_command(doc,1))).status=='applied'
    prepare,_=native(env);command=packet(env,doc)
    await prepare(command)
    assert (await accepted(env,command)).status=='applied'
    async with db.transaction():
        original_stage=(await (await db.execute(
            'SELECT payload_bytes FROM rkb_v4.source_stage_batches WHERE source_id=%s',(SOURCE,))).fetchone())[0]
    @asynccontextmanager
    async def acquire():yield db
    async def bibliography():
        pdf=await render_accepted_source_pdf(acquire=acquire,source_id=SOURCE,article_id='fixture-unit')
        with pymupdf.open(stream=pdf,filetype='pdf') as rendered:return rendered[0].get_text()
    assert '1985' in await bibliography()  # No correction: original metadata remains authoritative.
    for year in (1987,1988):
        correction=resolved_catalog_command(sequence=env['sequence']+1,operation_key=f'catalog:pdf-year:{year}',
            request_id=REQUEST,person_id=PERSON,source_id=SOURCE,logical_time=TIME,
            payload=dict(codec=CODEC,ingestion_id=INGESTION,owner_person_id=PERSON,
                catalog_update=dict(expected_revision=1,publication_year=year)))
        assert (await accepted(env,correction)).status=='applied'
    async with db.transaction():
        row=await (await db.execute('SELECT edition FROM rkb_v4.read_source_unit_export_pages_v1(%s,%s::integer[])',
            (SOURCE,[0]))).fetchone()
        assert row[0]['publication_year']==1988
        assert (await (await db.execute('SELECT payload_bytes FROM rkb_v4.source_stage_batches WHERE source_id=%s',
            (SOURCE,))).fetchone())[0]==original_stage
        assert (await (await db.execute("SELECT source_metadata->'metadata'->>'publication_year' "
            'FROM rkb_v4.source_stage_sessions WHERE source_id=%s',(SOURCE,))).fetchone())==('1985',)
    current=await bibliography()
    assert '1988' in current and '1985' not in current
