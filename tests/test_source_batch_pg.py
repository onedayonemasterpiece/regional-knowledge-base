"""Packet publication through current accepted commands, only on PG17 CI."""
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from regional_knowledge.kaggle_source_materializer import NativeSourceMaterializer,materializer_fingerprint,SourceMaterializationRejected
from regional_knowledge.source_batch import CODEC,resolved_command
from regional_knowledge.source_stage import canonical,digest
from test_native_source_materializer import exact_batch
from test_source_activation_pg import image_pg_sql18,accepted,stage_command,DB,WRITER,TIME
from test_source_stage import SOURCE,INGESTION,REQUEST
from test_source_activation import PERSON
from test_kaggle_pg_driver import database
from test_kaggle_apply import PG


def packet(env,doc):
    return resolved_command(source_id=SOURCE,sequence=env['sequence']+1,
        operation_key='packet:'+str(doc['batch_index']),request_id=REQUEST,person_id=PERSON,logical_time=TIME,
        payload=dict(codec=CODEC,ingestion_id=INGESTION,batch_index=doc['batch_index'],
            batch_count=doc['batch_index']+1,last_batch_sha256=digest(canonical(doc)),
            materializer_sha256=materializer_fingerprint()))


def native(env):
    calls=[]
    async def count(text):return len(text.split())+2
    async def encode(text):calls.append(text);return [1.]+[0.]*1023
    @asynccontextmanager
    async def acquire():yield env['connection']
    worker=NativeSourceMaterializer(count=count,encode=encode,current=lambda:True)
    async def prepare(c):
        if await env['journal'].locate_accepted(c) is None:
            await env['journal'].seal([c],predecessor=await env['journal'].head(),writer_generation=WRITER)
        await worker(c,acquire=acquire,database=SimpleNamespace(database_generation=DB,writer_generation=WRITER))
    return prepare,calls


@PG
@pytest.mark.asyncio
async def test_one_of_174_pages_is_searchable_without_seal_and_retry_reuses_vector(image_pg_sql18):
    env=image_pg_sql18;doc=exact_batch(count=1)
    assert (await accepted(env,stage_command(doc,1))).status=='applied'
    c=packet(env,doc);prepare,calls=native(env)
    await prepare(c);await prepare(c);assert len(calls)==1
    first=await accepted(env,c);assert first.status=='applied'
    await prepare(c);assert len(calls)==1
    assert (await accepted(env,c)).duplicate
    async with env['connection'].transaction():
        assert await (await env['connection'].execute('SELECT count(*) FROM rkb_v4.lexical_candidates(%s,20)',('synthetic',))).fetchone()==(1,)
        for table in ('source_stage_candidates','source_activation_candidates'):
            assert await (await env['connection'].execute('SELECT count(*) FROM rkb_v4.'+table)).fetchone()==(0,)
        assert await (await env['connection'].execute('SELECT count(*) FROM rkb_v4.source_batch_receipts')).fetchone()==(1,)
        manifest=await (await env['connection'].execute('SELECT rkb_v4.read_source_manifest_v1(%s)',(SOURCE,))).fetchone()
        assert len(manifest[0]['pages'])==1


@PG
@pytest.mark.asyncio
async def test_bad_packet_keeps_first_searchable_and_next_packet_can_publish(image_pg_sql18):
    env=image_pg_sql18;first=exact_batch(count=1);prepare,calls=native(env)
    await accepted(env,stage_command(first,1));c=packet(env,first);await prepare(c);await accepted(env,c)
    second=exact_batch(1,digest(canonical(first)),start=1,count=1)
    p=second['model_payload']['pages'][0]
    second['model_payload']['story_candidates']=[dict(candidate_key='bad-fact',title='Bad fact evidence',
        summary='Deliberately wrong source proof.',proposition='A synthetic event occurred.',narrative_outcome='facts_only',
        evidence_refs=[dict(page_id=p['page_id'],region_key='body',original_excerpt=p['regions'][0]['source_text'])],
        event_dates=[dict(original_date_text='2026',precision='year',calendar='unspecified',date_role='event',year=2026,
            month=None,day=None,rationale='Deliberately missing year in the source.',evidence_indexes=[0])])]
    await accepted(env,stage_command(second,env['sequence']+1));bad=packet(env,second)
    with pytest.raises(SourceMaterializationRejected):await prepare(bad)
    assert (await accepted(env,bad)).status=='rejected_precondition'
    third=exact_batch(2,digest(canonical(second)),start=2,count=1)
    await accepted(env,stage_command(third,env['sequence']+1));c=packet(env,third)
    await prepare(c);assert (await accepted(env,c)).status=='applied'
    async with env['connection'].transaction():
        assert await (await env['connection'].execute('SELECT count(*) FROM rkb_v4.passage_search WHERE source_id=%s',(SOURCE,))).fetchone()==(2,)
        assert await (await env['connection'].execute('SELECT count(*) FROM rkb_v4.source_batch_receipts')).fetchone()==(2,)
