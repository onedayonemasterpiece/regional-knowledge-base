"""Native SQL014 integration on the existing strictly checked disposable PG only.

Vectors are synthetic unit vectors; this is SQL/replay proof, not live BGE proof.
"""
import base64
import copy
import json
from pathlib import Path

import psycopg
from psycopg.types.json import Jsonb
import pytest
import pytest_asyncio

from regional_knowledge.kaggle_apply import HandlerRegistry, apply_accepted, handover_writer
from pg_schema_metadata import initialize_fixture_database as initialize_database
from regional_knowledge.kaggle_native_source_bindings import native_source_handler
from regional_knowledge.recovery_journal import ExactCommand
from regional_knowledge.source_activation import (
    ACTIVATION_REVISIONS, MATERIALIZE_CODEC, knowledge_bytes,
    resolved_activation_command, resolved_materialize_command,
)
from regional_knowledge.source_stage import canonical, digest, STAGE_ROUTE, SEAL_ROUTE
from test_source_stage import SOURCE, INGESTION, REQUEST, batch, seal, original
from test_source_activation import PERSON, MATERIALIZER, synthetic_knowledge, activation_payload
from test_kaggle_apply import PG, pinned
from test_kaggle_pg_driver import database
from test_recovery_journal import fixture as journal_fixture

ROOT=Path(__file__).resolve().parents[1]
DB='native-image-fixture';WRITER='writer-1';TIME='2026-10-21T19:00:00Z'
SQL=('003_search_scope.sql','004_read_fences.sql','005_source_withdraw.sql','006_source_import.sql',
     '007_source_stories.sql','008_source_facts.sql','009_story_editorial.sql','010_disagreement_cases.sql',
     '011_geo_identity_manifest.sql','012_geo_dependency_bound.sql','013_source_stage.sql','014_source_stage_activation.sql')
VECTOR='['+','.join(['1']+['0']*1023)+']'


def stage_command(d,sequence,route=STAGE_ROUTE):
    return ExactCommand(sequence=sequence,operation_key=f'image-stage:{sequence}',request_id=REQUEST,person_id=PERSON,
        logical_time=TIME,route=route,schema_revision=ACTIVATION_REVISIONS.schema,
        function_revision=ACTIVATION_REVISIONS.function,executor_revision=ACTIVATION_REVISIONS.executor,
        ids={'source_id':SOURCE},expected_revisions={'stage_head':d['previous_batch_sha256'] if route==STAGE_ROUTE else d['last_batch_sha256']},
        payload=canonical(d))


def next_command(payload,sequence,*,ordinal=None):
    args=dict(sequence=sequence,operation_key=f'image-native:{sequence}',request_id=REQUEST,
        person_id=PERSON,logical_time=TIME,source_id=SOURCE)
    if ordinal is None:return resolved_activation_command(**args,payload=payload)
    return resolved_materialize_command(**args,payload={**payload,'codec':MATERIALIZE_CODEC,'ordinal':ordinal})


@pytest_asyncio.fixture
async def image_pg(database):
    connection=database
    async with connection.transaction():
        await connection.execute((ROOT/'sql/kaggle/002_recovery.sql').read_text(),prepare=False)
    await initialize_database(connection,database_generation=DB,writer_generation=WRITER,revisions=ACTIVATION_REVISIONS)
    async with connection.transaction():
        for name in SQL:await connection.execute((ROOT/'sql/kaggle'/name).read_text(),prepare=False)
        await connection.execute('SELECT rkb_v4.publish_read_frontier(%s,%s,0,0,true,true)',(DB,WRITER))
    handlers=[]
    for route,suffix in [(STAGE_ROUTE,'source_stage_v2'),(SEAL_ROUTE,'source_stage_seal_v2'),
                         ('source.materialize.v1','source_materialize_v1'),('source.activate.v1','source_activate_v1')]:
        check=await pinned(connection,'check_'+suffix);apply=await pinned(connection,'apply_'+suffix)
        handlers.append(native_source_handler(route,check_sha256=check.definition_sha256,apply_sha256=apply.definition_sha256))
    _,journal=journal_fixture()
    return dict(connection=connection,journal=journal,registry=HandlerRegistry(handlers),sequence=0)


async def accepted(env,command):
    journal=env['journal'];found=await journal.locate_accepted(command)
    if found is None:found=await journal.seal([command],predecessor=await journal.head(),writer_generation=WRITER)
    receipt=await apply_accepted(env['connection'],journal=journal,seal=found,command=command,
        database_generation=DB,writer_generation=WRITER,registry=env['registry'])
    env['sequence']=max(env['sequence'],command.sequence)
    return receipt


async def stage_all(env,chain):
    for data in chain:
        command=stage_command(data,env['sequence']+1)
        result=await accepted(env,command)
        assert result.status=='applied'
        duplicate=await accepted(env,command)
        assert duplicate.duplicate and duplicate.outcome==result.outcome


async def prepare(env,chain,head,doc,*,database_generation=DB,writer_generation=WRITER):
    async with env['connection'].transaction():
        row=await (await env['connection'].execute('SELECT rkb_v4.prepare_source_activation_v1(%s,%s,%s,%s,%s,%s,%s,%s)',
            (SOURCE,INGESTION,len(chain),head,MATERIALIZER,knowledge_bytes(doc),database_generation,writer_generation))).fetchone()
        return row[0]


async def install(env,h,ordinal,*,vector=VECTOR):
    async with env['connection'].transaction():
        return (await (await env['connection'].execute('SELECT rkb_v4.install_source_activation_vector_v1(%s,%s,%s,%s::public.vector,%s,%s)',
            (SOURCE,h,ordinal,vector,DB,WRITER))).fetchone())[0]


@PG
@pytest.mark.asyncio
async def test_174_page_candidate_is_searchable_only_after_all_local_inputs_and_atomic_activation(image_pg):
    env=image_pg;chain,head,doc=synthetic_knowledge(story=True);await stage_all(env,chain)
    h=await prepare(env,chain,head,doc)
    assert h==digest(knowledge_bytes(doc))
    assert await prepare(env,chain,head,doc)==h # lost preparation ACK is exact reuse
    command=stage_command(seal(len(chain),head),env['sequence']+1,SEAL_ROUTE)
    assert (await accepted(env,command)).status=='applied'
    p=activation_payload(chain,head)
    assert (await accepted(env,next_command(p,env['sequence']+1))).status=='rejected_precondition'
    async with env['connection'].transaction():
        assert await (await env['connection'].execute('SELECT count(*) FROM rkb_v4.sources WHERE source_id=%s',(SOURCE,))).fetchone()==(0,)
    for ordinal in range(175):
        expected=doc['passages'][ordinal]['body'] if ordinal<174 else doc['semantic']['stories'][0]['default_variant']['body']
        assert await install(env,h,ordinal)==digest(expected.encode())
        c=next_command(p,env['sequence']+1,ordinal=ordinal)
        receipt=await accepted(env,c);assert receipt.status=='applied'
        duplicate=await accepted(env,c);assert duplicate.duplicate and duplicate.outcome==receipt.outcome
        result=json.loads(base64.b64decode(json.loads(receipt.outcome)['result_b64']))
        assert result['ordinal']==ordinal and result['total_inputs']==175 and result['knowledge_sha256']==h
        assert 'embedding' not in result
    c=next_command(p,env['sequence']+1);first=await accepted(env,c)
    assert first.status=='applied'
    repeated=await accepted(env,c);assert repeated.duplicate and repeated.outcome==first.outcome
    async with env['connection'].transaction():
        assert await (await env['connection'].execute('SELECT count(*) FROM rkb_v4.passage_search WHERE source_id=%s',(SOURCE,))).fetchone()==(174,)
        actual=await (await env['connection'].execute('SELECT body FROM rkb_v4.story_variant_versions v JOIN rkb_v4.stories s USING(story_id) WHERE s.source_id=%s',(SOURCE,))).fetchone()
        assert actual==(doc['semantic']['stories'][0]['default_variant']['body'],)
        assert actual[0]!=doc['semantic']['stories'][0]['summary']
        assert await (await env['connection'].execute('SELECT rkb_v4.verify_source_story_origins_v4(ARRAY[%s]::uuid[]),rkb_v4.verify_source_graph_origins_v2(ARRAY[%s]::uuid[])',(SOURCE,SOURCE))).fetchone()==(True,True)
        assert await (await env['connection'].execute('SELECT count(*) FROM rkb_v4.lexical_candidates(%s,200) c JOIN rkb_v4.passage_search p USING(passage_id) WHERE p.source_id=%s',('synthetic',SOURCE))).fetchone()==(174,)
        assert await (await env['connection'].execute('SELECT metadata FROM rkb_v4.source_catalog WHERE source_id=%s',(SOURCE,))).fetchone()==(doc['image_manifest']['metadata'],)
        assert await (await env['connection'].execute('SELECT knowledge->%s FROM rkb_v4.source_activation_candidates WHERE source_id=%s',('public_rights',SOURCE))).fetchone()==(doc['public_rights'],)


@PG
@pytest.mark.asyncio
async def test_invalid_partial_model_does_not_seal_and_corrected_open_chain_remains_usable(image_pg):
    env=image_pg;receipt=original(page_count=2);one=batch(count=1,receipt=receipt);head=digest(canonical(one))
    await stage_all(env,[one])
    c=stage_command(seal(1,head),env['sequence']+1,SEAL_ROUTE)
    assert (await accepted(env,c)).status=='rejected_precondition'
    _,_,doc=synthetic_knowledge(2)
    with pytest.raises(psycopg.errors.InvalidParameterValue):await prepare(env,[one],head,doc)
    async with env['connection'].transaction():
        for table in ('source_stage_candidates','source_activation_candidates','sources'):
            assert await (await env['connection'].execute(f'SELECT count(*) FROM rkb_v4.{table} WHERE source_id=%s',(SOURCE,))).fetchone()==(0,)
    two=batch(1,head,start=1,count=1,receipt=receipt);await stage_all(env,[two]);head=digest(canonical(two))
    doc['research']=two['research'];h=await prepare(env,[one,two],head,doc)
    assert len(h)==64
    assert (await accepted(env,stage_command(seal(2,head),env['sequence']+1,SEAL_ROUTE))).status=='applied'


@PG
@pytest.mark.asyncio
async def test_valid_preparation_freezes_new_batches_not_exact_retry_or_existing_vector(image_pg):
    env=image_pg;chain,head,doc=synthetic_knowledge(1);await stage_all(env,chain);h=await prepare(env,chain,head,doc)
    # Retry of the exact accepted batch remains valid, while post-prepare append
    # cannot strand an immutable candidate under a newer unsealed head.
    assert (await accepted(env,stage_command(chain[0],env['sequence']+1))).status=='applied'
    later=batch(1,head,count=1,receipt=original(page_count=1))
    assert (await accepted(env,stage_command(later,env['sequence']+1))).status=='rejected_precondition'
    assert (await accepted(env,stage_command(seal(1,head),env['sequence']+1,SEAL_ROUTE))).status=='applied'
    await install(env,h,0)
    drift='['+','.join(['0.9999999','0.0001']+['0']*1022)+']'
    assert await install(env,h,0,vector=drift)==digest(doc['passages'][0]['body'].encode())
    async with env['connection'].transaction():
        assert await (await env['connection'].execute('SELECT embedding::text FROM rkb_v4.source_activation_vectors WHERE source_id=%s',(SOURCE,))).fetchone()==(VECTOR,)
    with pytest.raises(psycopg.Error):await prepare(env,chain,head,doc,writer_generation='stale-writer')
    assert await prepare(env,chain,head,doc)==h
    await handover_writer(env['connection'],database_generation=DB,expected_writer=WRITER,new_writer='writer-2')
    assert await prepare(env,chain,head,doc,writer_generation='writer-2')==h
    async with env['connection'].transaction():
        assert await (await env['connection'].execute('SELECT prepared_writer FROM rkb_v4.source_activation_candidates WHERE source_id=%s',(SOURCE,))).fetchone()==(WRITER,)
        assert (await (await env['connection'].execute('SELECT rkb_v4.install_source_activation_vector_v1(%s,%s,0,%s::public.vector,%s,%s)',
            (SOURCE,h,VECTOR,DB,'writer-2'))).fetchone())[0]==digest(doc['passages'][0]['body'].encode())


@PG
@pytest.mark.asyncio
async def test_activation_transaction_failure_leaves_no_half_public_source_and_same_command_retries(image_pg):
    env=image_pg;chain,head,doc=synthetic_knowledge(1);await stage_all(env,chain);h=await prepare(env,chain,head,doc)
    assert (await accepted(env,stage_command(seal(1,head),env['sequence']+1,SEAL_ROUTE))).status=='applied'
    await install(env,h,0);p=activation_payload(chain,head)
    assert (await accepted(env,next_command(p,env['sequence']+1,ordinal=0))).status=='applied'
    c=next_command(p,env['sequence']+1)
    class RollbackFixture(Exception):pass
    with pytest.raises(RollbackFixture):
        async with env['connection'].transaction():
            await env['connection'].execute('SELECT rkb_v4.apply_source_activate_v1(%s,%s,%s,%s)',
                (c.payload,Jsonb(dict(c.ids)),Jsonb(dict(c.expected_revisions)),c.logical_time))
            raise RollbackFixture()
    async with env['connection'].transaction():
        assert await (await env['connection'].execute('SELECT count(*) FROM rkb_v4.sources WHERE source_id=%s',(SOURCE,))).fetchone()==(0,)
    assert (await accepted(env,c)).status=='applied'


@PG
@pytest.mark.asyncio
async def test_latest_accepted_transcription_is_final_truth_not_first_page_snapshot(image_pg):
    env=image_pg;chain,head,doc=synthetic_knowledge(1);await stage_all(env,chain)
    changed=batch(1,head,count=1,receipt=original(page_count=1))
    text='The explicitly corrected model transcription from the same selected image.'
    changed['model_payload']['pages'][0]['regions'][0]['source_text']=text
    await stage_all(env,[changed]);head=digest(canonical(changed))
    # The issuance table deliberately retains first snapshot. Validation must
    # consume the last exact accepted page revision from immutable batch history.
    with pytest.raises(psycopg.errors.InvalidParameterValue):await prepare(env,chain+[changed],head,doc)
    doc['passages'][0]['body']=text;doc['semantic']['regions'][0]['source_text']=text
    doc['semantic']['regions'][0]['source_text_sha256']=digest(text.encode())
    doc['image_manifest']['regions'][0]['text_sha256']=digest(text.encode())
    doc['research']=changed['research']
    assert await prepare(env,chain+[changed],head,doc)==digest(knowledge_bytes(doc))
    async with env['connection'].transaction():
        old=await (await env['connection'].execute("SELECT model_page->'regions'->0->>'source_text' FROM rkb_v4.source_stage_pages WHERE source_id=%s",(SOURCE,))).fetchone()
        assert old[0]!=text


@PG
@pytest.mark.asyncio
async def test_real_native_compiler_preflight_and_one_input_preapply_match_sql14(image_pg):
    """Exercise the production preapply hook, not handcrafted resolved knowledge.

    Journal acceptance precedes each hook; BGE/tokenizer are deterministic local
    test doubles. No transport connection or model/provider call is implied.
    """
    from contextlib import asynccontextmanager
    from types import SimpleNamespace
    from regional_knowledge.kaggle_source_materializer import NativeSourceMaterializer, materializer_fingerprint

    env=image_pg;chain,head,_=synthetic_knowledge(2);await stage_all(env,chain)
    counts=[];forwards=[]
    async def count(text):counts.append(text);return len(text.split())+2
    async def encode(text):forwards.append(text);return [1.]+[0.]*1023
    @asynccontextmanager
    async def acquire():yield env['connection']
    native=NativeSourceMaterializer(count=count,encode=encode,current=lambda:True)
    owned=SimpleNamespace(database_generation=DB,writer_generation=WRITER)
    async def preapply(command):
        found=await env['journal'].locate_accepted(command)
        if found is None:
            await env['journal'].seal([command],predecessor=await env['journal'].head(),writer_generation=WRITER)
        await native(command,acquire=acquire,database=owned)

    sealing=stage_command(seal(len(chain),head),env['sequence']+1,SEAL_ROUTE)
    await preapply(sealing)
    assert len(counts)==2 and forwards==[] # pre-seal validation never runs BGE
    async with env['connection'].transaction():
        knowledge,total=await (await env['connection'].execute(
            'SELECT knowledge,total_inputs FROM rkb_v4.source_activation_candidates WHERE source_id=%s',(SOURCE,))).fetchone()
        assert total==2 and knowledge['image_manifest']['codec']=='rkb.source-image-manifest.v1'
        assert knowledge['passages'][0]['body']==counts[0]
        assert await (await env['connection'].execute('SELECT count(*) FROM rkb_v4.source_stage_candidates WHERE source_id=%s',(SOURCE,))).fetchone()==(0,)
        assert await (await env['connection'].execute('SELECT count(*) FROM rkb_v4.sources WHERE source_id=%s',(SOURCE,))).fetchone()==(0,)
    assert (await accepted(env,sealing)).status=='applied'
    p={**activation_payload(chain,head),'materializer_sha256':materializer_fingerprint()}
    for ordinal in range(total):
        c=next_command(p,env['sequence']+1,ordinal=ordinal);before=len(forwards)
        await preapply(c)
        assert len(forwards)==before+1
        # Private vector commit / lost ACK does not cause an extra forward.
        await preapply(c);assert len(forwards)==before+1
        assert (await accepted(env,c)).status=='applied'
        await preapply(c);assert len(forwards)==before+1 # accepted duplicate too
        async with env['connection'].transaction():
            assert await (await env['connection'].execute('SELECT count(*) FROM rkb_v4.sources WHERE source_id=%s',(SOURCE,))).fetchone()==(0,)
    assert len(counts)==2 and forwards==[p['body'] for p in knowledge['passages']]
    assert (await accepted(env,next_command(p,env['sequence']+1))).status=='applied'
    async with env['connection'].transaction():
        assert await (await env['connection'].execute('SELECT count(*) FROM rkb_v4.lexical_candidates(%s,200) c JOIN rkb_v4.passage_search p USING(passage_id) WHERE p.source_id=%s',('synthetic',SOURCE))).fetchone()==(2,)
        assert await (await env['connection'].execute('SELECT rkb_v4.verify_source_story_origins_v4(ARRAY[%s]::uuid[])',(SOURCE,))).fetchone()==(True,)


@PG
@pytest.mark.asyncio
async def test_native_repeated_entity_mentions_and_alias_proofs_survive_activation(image_pg):
    from regional_knowledge.kaggle_source_materializer import NativeSourceCompiler
    from regional_knowledge.source_stage import ZERO_HASH
    from test_native_source_materializer import exact_batch
    from test_native_graph_continuation import with_entity

    env=image_pg;chain=[];head=ZERO_HASH;receipt=original(page_count=2)
    for index in range(2):
        data=with_entity(exact_batch(index,head,start=index,count=1,receipt=receipt))
        entity=data['model_payload']['entity_candidates']['entities'][0]
        entity['aliases']=[dict(value='Synthetic',language='en',alias_type='spelling_variant',
            evidence=copy.deepcopy(entity['evidence']))]
        chain.append(data);head=digest(canonical(data))
    async def count(text):return len(text.split())+2
    doc=await NativeSourceCompiler(count=count,current=lambda:True).compile(
        [canonical(d) for d in chain],source_id=SOURCE,person_id=PERSON,last_batch_sha256=head)
    entity=doc['source_graph']['entities'][0]
    assert len(entity['mentions'])==2 and len(entity['aliases'])==1
    assert len(entity['aliases'][0]['evidences'])==2
    async with env['connection'].transaction():
        assert await (await env['connection'].execute(
            'SELECT rkb_v4.check_source_image_graph_v1(%s,%s,2)',(Jsonb(doc),SOURCE))).fetchone()==(True,)
        # Additional proof cannot be invalid even when legacy first proof is valid.
        changed=copy.deepcopy(doc)
        changed['source_graph']['entities'][0]['aliases'][0]['evidences'][1]['exact_quote']='fabricated outside staged region'
        assert await (await env['connection'].execute(
            'SELECT rkb_v4.check_source_image_graph_v1(%s,%s,2)',(Jsonb(changed),SOURCE))).fetchone()==(False,)
    await stage_all(env,chain)
    h=await prepare(env,chain,head,doc)
    assert (await accepted(env,stage_command(seal(len(chain),head),env['sequence']+1,SEAL_ROUTE))).status=='applied'
    p=activation_payload(chain,head)
    for ordinal in range(2):
        await install(env,h,ordinal)
        assert (await accepted(env,next_command(p,env['sequence']+1,ordinal=ordinal))).status=='applied'
    assert (await accepted(env,next_command(p,env['sequence']+1))).status=='applied'
    async with env['connection'].transaction():
        rows=await (await env['connection'].execute(
            'SELECT data FROM rkb_v4.source_entity_mentions WHERE source_id=%s',(SOURCE,))).fetchall()
        assert sorted([r[0] for r in rows],key=lambda m:m['mention_id'])==sorted(entity['mentions'],key=lambda m:m['mention_id'])
        assert await (await env['connection'].execute(
            'SELECT data FROM rkb_v4.source_entity_aliases WHERE source_id=%s',(SOURCE,))).fetchall()==[(entity['aliases'][0],)]
        assert await (await env['connection'].execute(
            'SELECT rkb_v4.verify_source_graph_origins_v2(ARRAY[%s]::uuid[])',(SOURCE,))).fetchone()==(True,)
