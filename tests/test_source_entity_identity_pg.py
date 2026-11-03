"""Source-only PG17: accepted town POI and later same-key person with full graph/story."""
import copy
from uuid import UUID,uuid5
import pytest
from regional_knowledge.source_stage import canonical,digest
from regional_knowledge.kaggle_source_materializer import SourceMaterializationRejected
from regional_knowledge.entity_graph import entity_id
from test_kaggle_apply import PG
from test_kaggle_pg_driver import database
from test_source_activation_pg import image_pg_sql18,accepted,stage_command
from test_source_batch_pg import packet,native
from test_source_batch_compiler import passage_link_packet
from test_native_source_materializer import exact_batch
from test_source_stage import SOURCE


def _town():
    d=passage_link_packet()
    p=d['model_payload']['pages'][0]
    p['regions'][0]['source_text']='Gusev 1930. Gusev 1933.'
    e=d['model_payload']['entity_candidates']['entities'][0]
    e.update(key='gusev',canonical_label='Gusev',exact_source_spelling='Gusev',
        poi_locator=dict(names=['Gusev']))
    s=d['model_payload']['story_candidates'][0]
    s.update(candidate_key='town-record',title='Synthetic town chronology',
        summary='A synthetic town event in two dated entries.',
        proposition='The synthetic town is recorded in 1930 and 1933.',
        entity_keys=['gusev'])
    return d


def _person(previous,*,key):
    d=exact_batch(1,digest(canonical(previous)),start=1,count=1)
    p=d['model_payload']['pages'][0]
    p['regions'][0]['source_text']='Gusev joined a synthetic meeting in the named town.'
    c=d['model_payload']['chunks'][0]
    ident=str(uuid5(UUID(SOURCE),'chunk:1:'+c['chunk_key']))
    page=p['page_id']
    reg=str(uuid5(UUID(page),'region:body'))
    proof=dict(chunk_id=ident,page_id=page,region_id=reg)
    body='Gusev attended a synthetic meeting at a particular place.'
    d['model_payload']['entity_candidates']=dict(entities=[
        dict(key=key,kind='person',canonical_label='Gusev',
            exact_source_spelling='Gusev',evidence=proof,
            aliases=[dict(value='Gusev',alias_type='spelling_variant',evidence=proof)]),
        dict(key='synthetic-meeting-p1',kind='event',
            canonical_label='Synthetic meeting',exact_source_spelling='meeting',
            evidence=proof)],
        entity_refs={'town-on-page-0':str(entity_id(UUID(SOURCE),'gusev'))},
        relations=[
            dict(source_key=key,target_key='synthetic-meeting-p1',kind='participated_in',
                evidence=[proof]),
            dict(source_key='synthetic-meeting-p1',target_key='town-on-page-0',
                kind='occurred_at',evidence=[proof])])
    d['model_payload']['story_candidates']=[dict(
        candidate_key='meeting-record',title='Gusev meeting',
        summary='Synthetic witnessed participation in a meeting.',
        proposition='Gusev attended the synthetic meeting.',
        entity_keys=[key,'synthetic-meeting-p1'],
        evidence_refs=[dict(page_id=page,region_key='body',chunk_key=c['chunk_key'])],
        basic_variant=dict(body=body,language='en',
            blocks=[dict(start=0,end=len(body),evidence_indexes=[0])]))]
    return d


@PG
@pytest.mark.asyncio
@pytest.mark.parametrize('new_key,valid',[('gusev',False),('gusev-person-on-page-1',True)])
async def test_source_global_entity_key_disambiguation_preserves_graph_story_and_text(
        image_pg_sql18,new_key,valid):
    env=image_pg_sql18;prepare,_=native(env)
    first=_town();frozen=canonical(first)
    assert (await accepted(env,stage_command(first,1))).status=='applied'
    for part in ('text','semantic'):
        command=packet(env,first,part)
        await prepare(command)
        assert (await accepted(env,command)).status=='applied'
    initial_semantic=command  # Preserve the original accepted synthetic command bytes.
    second=_person(first,key=new_key)
    assert (await accepted(env,stage_command(second,env['sequence']+1))).status=='applied'
    t=packet(env,second,'text')
    await prepare(t)
    assert (await accepted(env,t)).status=='applied'
    c=packet(env,second,'semantic')
    if not valid:
        with pytest.raises(SourceMaterializationRejected,match='native_source_packet_requires_review'):
            await prepare(c)
        assert (await accepted(env,c)).status=='rejected_precondition'
    else:
        await prepare(c)
        assert (await accepted(env,c)).status=='applied'
    assert canonical(first)==frozen
    assert (await accepted(env,initial_semantic)).duplicate
    async with env['connection'].transaction():
        conn=env['connection']
        ids=await (await conn.execute('''
            SELECT x.entity_key,x.entity_id,h.kind FROM rkb_v4.source_entity_representations x
            JOIN rkb_v4.source_entity_ids h USING(entity_id)
            WHERE x.source_id=%s ORDER BY x.entity_key''',(SOURCE,))).fetchall()
        passages=await (await conn.execute('SELECT count(*) FROM rkb_v4.passage_search')).fetchone()
        stories=await (await conn.execute('SELECT count(*) FROM rkb_v4.story_search')).fetchone()
        assert passages==(2,)
        if valid:
            assert set((k,kind) for k,ident,kind in ids)=={
                ('gusev','poi_ref'),('gusev-person-on-page-1','person'),('synthetic-meeting-p1','event')}
            assert len({ident for _,ident,_ in ids})==3
            assert stories==(2,)
            assert (await (await conn.execute('SELECT count(*) FROM rkb_v4.source_entity_aliases')).fetchone())==(1,)
            assert (await (await conn.execute('SELECT count(*) FROM rkb_v4.source_entity_relations')).fetchone())==(2,)
            link=await (await conn.execute("SELECT entity_id FROM rkb_v4.source_entity_finding_links WHERE entity_key=%s",
                (new_key,))).fetchone()
            assert link and link[0]==next(ident for k,ident,_ in ids if k==new_key)
        else:
            assert stories==(1,) and ids==[( 'gusev',entity_id(UUID(SOURCE),'gusev'),'poi_ref' )]
    if valid:
        # A future model-selected mention can explicitly reuse the SAME person UUID.
        person_id=next(ident for k,ident,kind in ids if k==new_key)
        later=exact_batch(2,digest(canonical(second)),start=2,count=1)
        page=later['model_payload']['pages'][0]
        page['regions'][0]['source_text']='Gusev returned to a second synthetic meeting.'
        chunk=later['model_payload']['chunks'][0]
        proof=dict(chunk_id=str(uuid5(UUID(SOURCE),'chunk:1:'+chunk['chunk_key'])),
            page_id=page['page_id'],
            region_id=str(uuid5(UUID(page['page_id']),'region:body')))
        later['model_payload']['entity_candidates']=dict(entities=[dict(
            key='gusev-person-second-mention',entity_id=str(person_id),
            kind='person',canonical_label='Gusev',exact_source_spelling='Gusev',
            evidence=proof)],relations=[])
        story_body='The same person returned to a separate synthetic meeting.'
        later['model_payload']['story_candidates']=[dict(
            candidate_key='person-return',title='Gusev returned',
            summary='A second fictional source mention.',
            proposition='The same person returned to the synthetic meeting.',
            entity_keys=['gusev-person-second-mention'],
            evidence_refs=[dict(page_id=page['page_id'],region_key='body',
                chunk_key=chunk['chunk_key'])],
            basic_variant=dict(body=story_body,language='en',
                blocks=[dict(start=0,end=len(story_body),evidence_indexes=[0])]))]
        assert (await accepted(env,stage_command(later,env['sequence']+1))).status=='applied'
        for part in ('text','semantic'):
            command=packet(env,later,part)
            await prepare(command)
            assert (await accepted(env,command)).status=='applied'
        async with env['connection'].transaction():
            conn=env['connection']
            row=await (await conn.execute(
                'SELECT entity_id FROM rkb_v4.source_entity_representations '
                "WHERE source_id=%s AND entity_key='gusev-person-second-mention'",
                (SOURCE,))).fetchone()
            assert row==(person_id,)
            assert (await (await conn.execute(
                'SELECT count(*) FROM rkb_v4.source_entity_ids')).fetchone())==(3,)
            assert (await (await conn.execute(
                'SELECT count(*) FROM rkb_v4.story_search')).fetchone())==(3,)



def test_entity_source_scope_is_exposed_to_normal_model_and_semantic_retry():
    from types import SimpleNamespace
    from regional_knowledge.kaggle_native_source_intake import NativeSourceIntake
    from regional_knowledge.contracts import BookIngestOutput
    def output(_,state,message,action,**kwargs):
        return BookIngestOutput(
            ingestion_id='00000000-0000-0000-0000-000000000001',
            document_id='00000000-0000-0000-0000-000000000002',
            state=state,message=message,next_action=action,**kwargs)
    native=object.__new__(NativeSourceIntake)
    native.importer=SimpleNamespace(_output=output)
    state=dict(phase='staging',pending_operation_key=None,
        enriched_batch_count=1,batch_count=1,read_completion_requested=0,
        rejected_batch_index=0,rejected_reason='native_source_packet_requires_review')
    result=native._output({},state,rejected_next_cursor='1')
    assert (result.state,result.next_action)==('staged','continue_pages')
    assert 'source-global' in result.message
    assert 'TEXT APPLIED' in result.message
    assert 'same ingestion' in result.message.lower()


@PG
@pytest.mark.asyncio
@pytest.mark.parametrize('changed_text',[False,True])
async def test_empty_semantic_followup_native_preserves_exact_text(image_pg_sql18,changed_text):
    env=image_pg_sql18;prepare,_=native(env)
    first=_town();model=first['model_payload'];entity=copy.deepcopy(model['entity_candidates']['entities'][0])
    story=copy.deepcopy(model['story_candidates'][0])
    model['entity_candidates']={'entities':[]};model['story_candidates']=[]
    assert (await accepted(env,stage_command(first,1))).status=='applied'
    for part in ('text','semantic'):
        command=packet(env,first,part);await prepare(command)
        assert (await accepted(env,command)).status=='applied'
    frozen=canonical(first)
    async with env['connection'].transaction():
        before=await (await env['connection'].execute('SELECT passage_id,body,language,locator FROM rkb_v4.passage_search WHERE source_id=%s ORDER BY passage_id',(SOURCE,))).fetchall()
    second=copy.deepcopy(first);second.update(batch_index=1,previous_batch_sha256=digest(frozen))
    entity.update(key='gusev-person',kind='person');entity.pop('poi_locator',None);entity.pop('place_context',None)
    second['model_payload']['entity_candidates']={'entities':[entity]}
    body='The synthetic person Gusev is recorded in 1930 and 1933.'
    story.update(candidate_key='person-record',entity_keys=['gusev-person'],
        title='Person record',summary=body,proposition=body,
        basic_variant=dict(body=body,language='en',blocks=[dict(start=0,end=len(body),evidence_indexes=[0])]))
    second['model_payload']['story_candidates']=[story]
    if changed_text:second['model_payload']['pages'][0]['regions'][0]['source_text']+=' Changed text.'
    assert (await accepted(env,stage_command(second,env['sequence']+1))).status=='applied'
    text=packet(env,second,'text')
    if changed_text:
        with pytest.raises(SourceMaterializationRejected):await prepare(text)
        assert (await accepted(env,text)).status=='rejected_precondition'
    else:
        await prepare(text);assert (await accepted(env,text)).status=='applied'
        semantic=packet(env,second,'semantic');await prepare(semantic)
        assert (await accepted(env,semantic)).status=='applied'
    async with env['connection'].transaction():
        after=await (await env['connection'].execute('SELECT passage_id,body,language,locator FROM rkb_v4.passage_search WHERE source_id=%s ORDER BY passage_id',(SOURCE,))).fetchall()
        assert after==before
        count=(await (await env['connection'].execute('SELECT count(*) FROM rkb_v4.source_entity_representations WHERE source_id=%s',(SOURCE,))).fetchone())[0]
        assert count==(0 if changed_text else 1)
        stories=(await (await env['connection'].execute('SELECT count(*) FROM rkb_v4.story_search WHERE source_id=%s',(SOURCE,))).fetchone())[0]
        assert stories==(0 if changed_text else 1)
    assert canonical(first)==frozen
