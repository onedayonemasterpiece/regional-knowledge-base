"""Packet publication through current accepted commands, only on PG17 CI."""
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from regional_knowledge.kaggle_source_materializer import NativeSourceMaterializer,materializer_fingerprint,SourceMaterializationRejected
from regional_knowledge.source_batch import CODEC,resolved_command
from regional_knowledge.source_stage import canonical,digest
from test_native_source_materializer import exact_batch
from test_source_batch_compiler import passage_link_packet
from test_source_activation_pg import image_pg_sql18,accepted,stage_command,DB,WRITER,TIME
from test_source_stage import SOURCE,INGESTION,REQUEST
from test_source_activation import PERSON
from test_kaggle_pg_driver import database
from test_kaggle_apply import PG


@PG
@pytest.mark.asyncio
async def test_native_passage_links_persist_story_dates_graph_and_exact_replay(image_pg_sql18):
    """Current compiler → actual PG, with no duplicated evidence quotations."""
    import json
    from psycopg.types.json import Jsonb
    env=image_pg_sql18;doc=passage_link_packet()
    story=doc['model_payload']['story_candidates'][0]
    story['event_dates']=[dict(original_date_text='1930',precision='year',calendar='unspecified',
        date_role='event',year=1930,month=None,day=None,rationale='Synthetic date read from the linked source.',evidence_indexes=[0])]
    frozen=canonical(doc)
    assert (await accepted(env,stage_command(doc,1))).status=='applied'
    prepare,_=native(env)
    for component in ('text','semantic'):
        command=packet(env,doc,component);await prepare(command)
        assert (await accepted(env,command)).status=='applied'
        assert (await accepted(env,command)).duplicate
    assert canonical(doc)==frozen
    async with env['connection'].transaction():
        saved=await (await env['connection'].execute(
            "SELECT knowledge FROM rkb_v4.source_batch_prepared WHERE component='semantic'")).fetchone()
        knowledge=saved[0];encoded=json.dumps(knowledge)
        assert 'original_excerpt' not in encoded and 'exact_quote' not in encoded
        assert (await (await env['connection'].execute('SELECT body FROM rkb_v4.story_search')).fetchone())[0]==story['basic_variant']['body']
        assert (await (await env['connection'].execute('SELECT year FROM rkb_v4.source_event_dates')).fetchone())[0]==1930
        assert (await (await env['connection'].execute('SELECT count(*) FROM rkb_v4.source_entity_mentions')).fetchone())[0]==1
        evidence=knowledge['semantic']['findings'][0]['evidence'][0]
        assert (await (await env['connection'].execute('SELECT rkb_v4.check_source_passage_evidence_v1(%s,%s)',
            (Jsonb(knowledge),Jsonb(evidence)))).fetchone())==(True,)
        foreign={**evidence,'passage_id':'00000000-0000-0000-0000-000000000098'}
        assert (await (await env['connection'].execute('SELECT rkb_v4.check_source_passage_evidence_v1(%s,%s)',
            (Jsonb(knowledge),Jsonb(foreign)))).fetchone())==(False,)
    from regional_knowledge.kaggle_source_graph_read import SourceGraphReadRequest,execute_source_graph_read
    from regional_knowledge.kaggle_fact_contracts import FactReadRequest
    from regional_knowledge.kaggle_fact_read import execute_fact_read
    @asynccontextmanager
    async def acquire():yield env['connection']
    actor=dict(person_id='synthetic-reader',audience='personal',auth_epoch=1,visibility_epoch=0)
    identity=dict(request_id='source-link-read',connection_generation=1,turn_generation=1,
        actor=actor,min_data_seq=0,deadline_ms=2000)
    entity=knowledge['source_graph']['entities'][0]['entity_id']
    graph=await execute_source_graph_read(SourceGraphReadRequest(**identity,entity_id=entity,
        source_id=SOURCE),acquire=acquire)
    assert graph.status=='ok' and graph.evidence[0].text=='Berlin 1930. Berlin 1933.'
    assert 'exact_quote' not in json.dumps(graph.model_dump(mode='json'))
    facts=await execute_fact_read(FactReadRequest(**identity,action='assertions',source_id=SOURCE),acquire=acquire)
    assert facts.status=='ok' and facts.records[0]['proofs']==[evidence]
    assert 'original_excerpt' not in json.dumps(facts.model_dump(mode='json'))


def packet(env,doc,component='text'):
    return resolved_command(source_id=SOURCE,sequence=env['sequence']+1,
        operation_key='packet:'+str(doc['batch_index'])+':'+component,request_id=REQUEST,person_id=PERSON,logical_time=TIME,
        payload=dict(codec=CODEC,ingestion_id=INGESTION,batch_index=doc['batch_index'],
            batch_count=doc['batch_index']+1,last_batch_sha256=digest(canonical(doc)),
            materializer_sha256=materializer_fingerprint(),component=component))


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
@pytest.mark.parametrize(('kind','cover_kind'),[
    ('article','title_page'),('journal_issue','issue_cover')])
async def test_native_root_article_or_journal_text_accepted_and_catalogued_in_pg17(
        image_pg_sql18,kind,cover_kind):
    """Real disposable PG17, current accepted journal and source_catalog SQL."""
    from regional_knowledge.contracts import StartMetadataInput
    env=image_pg_sql18
    doc=exact_batch(count=1)
    metadata=StartMetadataInput(title='Synthetic scanned source',authors=['Synthetic author'],
        publication_year=1901,language='de',catalog=dict(kind=kind,publisher='Verified imprint',
            cover_page=0,cover_kind=cover_kind,volume='IV',issue='2'))
    doc['source']['metadata']=metadata.model_dump(mode='json')
    original=canonical(doc)
    staged=stage_command(doc,1)
    receipt=await accepted(env,staged)
    assert receipt.status=='applied'
    assert (await accepted(env,staged)).duplicate
    c=packet(env,doc)
    prepare,_=native(env)
    await prepare(c)
    first=await accepted(env,c)
    assert first.status=='applied'
    assert (await accepted(env,c)).duplicate
    assert canonical(doc)==original
    async with env['connection'].transaction():
        row=await (await env['connection'].execute(
            'SELECT metadata FROM rkb_v4.source_catalog WHERE source_id=%s',(SOURCE,))).fetchone()
        assert row[0]['catalog']['kind']==kind
        assert row[0]['catalog']['cover_page']==0
        assert row[0]['catalog']['cover_kind']==cover_kind
        visible=await (await env['connection'].execute(
            'SELECT metadata FROM rkb_v4.find_source_catalog_v1(%s,%s)',
            ('Synthetic scanned source',8))).fetchall()
        assert len(visible)==1 and visible[0][0]['catalog']['kind']==kind
        assert visible[0][0]['catalog']['publisher']=='Verified imprint'
        stored=await (await env['connection'].execute(
            'SELECT payload_bytes FROM rkb_v4.source_stage_batches WHERE source_id=%s AND batch_index=0',
            (SOURCE,))).fetchone()
        assert bytes(stored[0])==original


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
async def test_bad_optional_passage_link_does_not_block_text_or_other_pages(image_pg_sql18):
    env=image_pg_sql18;first=exact_batch(count=1);prepare,calls=native(env)
    await accepted(env,stage_command(first,1));c=packet(env,first);await prepare(c);await accepted(env,c)
    second=exact_batch(1,digest(canonical(first)),start=1,count=1)
    p=second['model_payload']['pages'][0]
    second['model_payload']['story_candidates']=[dict(candidate_key='bad-fact',title='Bad fact evidence',
        summary='Deliberately wrong source proof.',proposition='A synthetic event occurred.',narrative_outcome='facts_only',
        evidence_refs=[dict(page_id=p['page_id'],region_key='body',chunk_key='missing-source-passage')],
        event_dates=[dict(original_date_text='2026',precision='year',calendar='unspecified',date_role='event',year=2026,
            month=None,day=None,rationale='Model-authored date; passage link is deliberately absent.',evidence_indexes=[0])])]
    await accepted(env,stage_command(second,env['sequence']+1));text=packet(env,second)
    await prepare(text)
    async with env['connection'].transaction():
        # Preparing the next packet does not hide the first public page.
        assert await (await env['connection'].execute('SELECT count(*) FROM rkb_v4.lexical_candidates(%s,20)',('synthetic',))).fetchone()==(1,)
    assert (await accepted(env,text)).status=='applied'
    bad=packet(env,second,'semantic')
    with pytest.raises(SourceMaterializationRejected):await prepare(bad)
    assert (await accepted(env,bad)).status=='rejected_precondition'
    third=exact_batch(2,digest(canonical(second)),start=2,count=1)
    await accepted(env,stage_command(third,env['sequence']+1));c=packet(env,third)
    await prepare(c);assert (await accepted(env,c)).status=='applied'
    async with env['connection'].transaction():
        assert await (await env['connection'].execute('SELECT count(*) FROM rkb_v4.passage_search WHERE source_id=%s',(SOURCE,))).fetchone()==(3,)
        assert await (await env['connection'].execute('SELECT count(*) FROM rkb_v4.source_batch_receipts')).fetchone()==(3,)


@PG
@pytest.mark.asyncio
async def test_optional_story_body_and_illustration_apply_after_searchable_text(image_pg_sql18):
    import json
    from test_native_image_projection_20261022 import illustrated_batch
    env=image_pg_sql18;doc=json.loads(illustrated_batch())
    page=doc['model_payload']['pages'][0]
    body='The synthetic source records a specific event. Its printed account supplies the evidence.'
    doc['model_payload']['story_candidates']=[dict(candidate_key='episode',title='Synthetic source episode',
        summary='An authored episode with exact evidence.',proposition='The source records a synthetic event.',
        evidence_refs=[dict(page_id=page['page_id'],region_key='body',original_excerpt=page['regions'][0]['source_text'])],
        basic_variant=dict(body=body,language='en',blocks=[dict(start=0,end=len(body),evidence_indexes=[0])]))]
    prepare,_=native(env)
    assert (await accepted(env,stage_command(doc,1))).status=='applied'
    c=packet(env,doc);await prepare(c);assert (await accepted(env,c)).status=='applied'
    async with env['connection'].transaction():
        assert await (await env['connection'].execute('SELECT count(*) FROM rkb_v4.passage_search')).fetchone()==(1,)
        assert await (await env['connection'].execute('SELECT count(*) FROM rkb_v4.stories')).fetchone()==(0,)
    c=packet(env,doc,'semantic');await prepare(c)
    assert (await accepted(env,c)).status=='applied'
    async with env['connection'].transaction():
        assert await (await env['connection'].execute('SELECT body FROM rkb_v4.story_search')).fetchone()==(body,)
        assert await (await env['connection'].execute('SELECT count(*) FROM rkb_v4.source_illustrations')).fetchone()==(1,)
        assert await (await env['connection'].execute('SELECT count(*) FROM rkb_v4.read_source_illustrations_v1(%s,NULL,4)',(SOURCE,))).fetchone()==(1,)


@PG
@pytest.mark.asyncio
async def test_later_finding_uses_ordinary_prior_passage_without_history_scan(image_pg_sql18):
    env=image_pg_sql18;first=exact_batch(count=1);prepare,_=native(env)
    await accepted(env,stage_command(first,1));c=packet(env,first);await prepare(c);await accepted(env,c)
    second=exact_batch(1,digest(canonical(first)),start=8,count=1);page=first['model_payload']['pages'][0]
    second['model_payload']['story_candidates']=[dict(candidate_key='later-finding',title='Prior page context',
        summary='The model explicitly cites the previous page.',proposition='The prior source supplies this finding.',
        narrative_outcome='facts_only',evidence_refs=[dict(page_id=page['page_id'],region_key='body',original_excerpt=page['regions'][0]['source_text'])])]
    await accepted(env,stage_command(second,env['sequence']+1));c=packet(env,second)
    await prepare(c);await accepted(env,c);c=packet(env,second,'semantic');await prepare(c)
    assert (await accepted(env,c)).status=='applied'
    async with env['connection'].transaction():
        assert await (await env['connection'].execute('SELECT count(*) FROM rkb_v4.passage_search')).fetchone()==(2,)
        assert await (await env['connection'].execute('SELECT count(*) FROM rkb_v4.story_assertion_versions')).fetchone()==(1,)


@PG
@pytest.mark.asyncio
async def test_story_completed_on_next_page_persists_body_and_both_passage_refs(image_pg_sql18):
    """The finishing packet can cite an already published page without restaging it."""
    env=image_pg_sql18;first=exact_batch(count=1);prepare,_=native(env)
    assert (await accepted(env,stage_command(first,1))).status=='applied'
    c=packet(env,first);await prepare(c)
    assert (await accepted(env,c)).status=='applied'
    second=exact_batch(1,digest(canonical(first)),start=1,count=1)
    pages=[first['model_payload']['pages'][0],second['model_payload']['pages'][0]]
    body='Синтетический эпизод начинается на первой странице. Его окончание находится на следующей.'
    second['model_payload']['story_candidates']=[dict(candidate_key='finished-episode',
        title='Синтетический эпизод на двух страницах',summary='Один эпизод с окончанием.',
        proposition='Две страницы содержат один синтетический эпизод.',
        evidence_refs=[dict(page_id=p['page_id'],region_key='body',
            original_excerpt=p['regions'][0]['source_text']) for p in pages],
        basic_variant=dict(body=body,language='ru',
            blocks=[dict(start=0,end=len(body),evidence_indexes=[0,1])]))]
    assert (await accepted(env,stage_command(second,env['sequence']+1))).status=='applied'
    c=packet(env,second);await prepare(c)
    assert (await accepted(env,c)).status=='applied'
    c=packet(env,second,'semantic');await prepare(c)
    assert (await accepted(env,c)).status=='applied'
    assert (await accepted(env,c)).duplicate
    async with env['connection'].transaction():
        row=await (await env['connection'].execute('''SELECT v.body,v.data,a.assertion_id,a.data
            FROM rkb_v4.stories s
            JOIN rkb_v4.story_variant_versions v ON v.variant_id=s.default_variant_id
                AND v.revision=s.default_variant_revision
            JOIN rkb_v4.story_versions sv ON sv.story_id=s.story_id AND sv.revision=s.current_revision
            JOIN rkb_v4.story_assertion_versions a
                ON a.assertion_id=(sv.data->'finding_ids'->>0)::uuid''')).fetchone()
        assert row[0]==body
        assert row[1]['language']=='ru'
        assert row[1]['blocks']==[dict(start=0,end=len(body),assertion_ids=[str(row[2])])]
        evidence=row[3]['evidence']
        assert len(evidence)==2
        refs=await (await env['connection'].execute('''SELECT passage_id FROM rkb_v4.passage_search
            WHERE source_id=%s''',(SOURCE,))).fetchall()
        assert {e['passage_id'] for e in evidence}=={str(r[0]) for r in refs}
        assert len({e['region_id'] for e in evidence})==2
        assert await (await env['connection'].execute('SELECT body FROM rkb_v4.story_search')).fetchone()==(body,)
        assert await (await env['connection'].execute('SELECT count(*) FROM rkb_v4.source_story_pending')).fetchone()==(0,)

@PG
@pytest.mark.asyncio
@pytest.mark.parametrize(('metadata_language','stored_language','expected_config'), [
    ('ru','ru','pg_catalog.russian'),
    ('de','de','pg_catalog.german'),
    ('en','en','pg_catalog.simple'),
    ('en-US','en','pg_catalog.simple'),
    ('fr','fr','pg_catalog.simple'),
    ('und','und','pg_catalog.simple'),
    ('simple','simple','pg_catalog.simple'),
])
async def test_current_packet_preserves_language_with_simple_fallback(
        image_pg_sql18,metadata_language,stored_language,expected_config):
    """Actual SQL027 packet acceptance, search and replay on the disposable PG17 fixture."""
    env=image_pg_sql18;doc=exact_batch(count=1)
    doc['source']['metadata']['language']=metadata_language
    assert (await accepted(env,stage_command(doc,1))).status=='applied'
    prepare,_=native(env)
    command=packet(env,doc)
    await prepare(command)
    assert (await accepted(env,command)).status=='applied'
    assert (await accepted(env,command)).duplicate
    async with env['connection'].transaction():
        row=await (await env['connection'].execute(
            """SELECT language,config=%s::regconfig,passage_id
               FROM rkb_v4.passage_search WHERE source_id=%s""",
            (expected_config,SOURCE))).fetchone()
        assert row is not None and row[:2]==(stored_language,True)
        hits=await (await env['connection'].execute(
            'SELECT passage_id FROM rkb_v4.lexical_candidates(%s,20)',
            ('synthetic',))).fetchall()
        assert row[2] in {hit[0] for hit in hits}
