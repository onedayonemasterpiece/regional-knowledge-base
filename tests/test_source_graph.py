"""Deterministic explicit graph choices over exact reviewed native source bytes."""
import copy
from uuid import UUID,uuid5

import pytest

from regional_knowledge.entity_graph import GraphBundle,entity_id
from regional_knowledge.source_graph import resolve_source_graph,validate_graph_import_payload
from regional_knowledge.geo_migration import canonical
from test_source_manifest import inputs,manifest_payload
from test_source_import import SOURCE,PASSAGE
from test_source_import_v2 import REGION

PERSON='60000000-0000-0000-0000-000000000002'


def material():
    doc,metadata,receipt,proof,graph=inputs();text=doc['semantic']['regions'][0]['source_text'];page=str(graph.pages[0].page_id)
    evidence=dict(chunk_id=PASSAGE,page_id=page,region_id=REGION,exact_quote=text)
    names=[('person','person'),('organization','organization'),('event','event'),('thread','historical_thread'),('place','poi_ref')]
    bundle=GraphBundle.model_validate(dict(entities=[dict(key=key,kind=kind,canonical_label=key+' source identity',
        exact_source_spelling=text.split()[0],evidence=evidence,
        aliases=[dict(value='Recorded alias',evidence=evidence,research_sources=[dict(url='https://official.example.test/context',note='Explicit model research context')])],
        **(dict(poi_locator=dict(names=['Unresolved source place']),canonical_poi_ref='streetstory://poi/poi_ss_'+'a'*24,
            map_refs=['cartography://retained/source-reference']) if kind=='poi_ref' else {})) for key,kind in names],
        relations=[dict(source_key='organization',target_key='person',kind='founded_by',evidence=[evidence]),
                   dict(source_key='event',target_key='place',kind='occurred_at',evidence=[evidence])]))
    graph=graph.model_copy(update={'entity_candidates':[bundle]})
    doc['semantic']['findings'][0]['entity_keys']=['person','place']
    return doc,graph


def payload():
    doc,graph=material();full=manifest_payload();full['semantic']=doc['semantic']
    # Manifest passages/regions are unchanged; only explicit semantic links add.
    full['source_graph']=resolve_source_graph(graph,source_id=SOURCE,person_id=PERSON,semantic=full['semantic'])
    return full,graph


def test_all_independent_entity_kinds_exact_ids_source_spans_and_direction_survive():
    value,graph=payload();resolved=value['source_graph']
    assert {node['kind'] for node in resolved['entities']}=={'person','organization','event','historical_thread','poi_ref'}
    assert {node['entity_id'] for node in resolved['entities']}=={str(entity_id(UUID(SOURCE),node.key)) for node in graph.entity_candidates[0].entities}
    for node in resolved['entities']:
        proof=node['mention']['evidence'];assert proof['start']==0
        assert proof['end']==len(proof['exact_quote']) and proof['chunk_id']==PASSAGE
    founded=next(r for r in resolved['relations'] if r['kind']=='founded_by')
    assert founded['source_id']==str(entity_id(UUID(SOURCE),'organization'))
    assert founded['target_id']==str(entity_id(UUID(SOURCE),'person'))
    place=next(n for n in resolved['entities'] if n['kind']=='poi_ref')
    assert place['metadata']['owner_reference_verification']=='not_verified'
    assert place['metadata']['historical_geometry']=='not_verified'
    assert validate_graph_import_payload(canonical(value),source_id=SOURCE,person_id=PERSON)==value
    assert resolved==resolve_source_graph(graph,source_id=SOURCE,person_id=PERSON,semantic=value['semantic'])


@pytest.mark.parametrize('change',['owner','id','quote','offset','relation','link','geometry','alias'])
def test_resolved_graph_or_actor_or_provenance_cannot_be_forged(change):
    value,_=payload();graph=value['source_graph']
    if change=='owner':graph['owner_person_id']='60000000-0000-0000-0000-000000000099'
    if change=='id':graph['entities'][0]['entity_id']='60000000-0000-0000-0000-000000000099'
    if change=='quote':graph['entities'][0]['mention']['evidence']['exact_quote']='invented source quote'
    if change=='offset':graph['entities'][0]['mention']['evidence']['start']=1
    if change=='relation':graph['relations'][0]['source_id']=graph['relations'][0]['target_id']
    if change=='link':graph['finding_entities'][0]['entity_ids']=[]
    if change=='geometry':graph['entities'][0]['metadata']['historical_geometry']='accepted'
    if change=='alias':graph['entities'][0]['aliases'][0]['normalized_value']='other'
    with pytest.raises(ValueError):validate_graph_import_payload(canonical(value),source_id=SOURCE,person_id=PERSON)


def test_selected_entities_do_not_require_a_story_or_infer_name_merge():
    value,graph=payload();value['semantic']['stories']=[];value['semantic']['pending']=[]
    value['semantic']['findings'][0]['entity_keys']=['person','organization']
    value['source_graph']=resolve_source_graph(graph,source_id=SOURCE,person_id=PERSON,semantic=value['semantic'])
    assert validate_graph_import_payload(canonical(value),source_id=SOURCE,person_id=PERSON)
    assert len(value['source_graph']['finding_entities'][0]['entity_ids'])==2
    del value['source_graph']
    with pytest.raises(ValueError,match='require exact source graph'):
        validate_graph_import_payload(canonical(value),source_id=SOURCE,person_id=PERSON)


def test_ambiguous_quotes_and_invalid_directional_endpoint_types_refuse_before_acceptance():
    value,graph=payload();bundle=graph.entity_candidates[0]
    with pytest.raises(ValueError):
        GraphBundle.model_validate(dict(entities=[n.model_dump(mode='json') for n in bundle.entities],
            relations=[dict(source_key='person',target_key='organization',kind='founded_by',
                evidence=[bundle.entities[0].evidence.model_dump(mode='json')])]))
    region=graph.pages[0].regions[0]
    changed=region.model_copy(update={'source_text':region.source_text+' '+region.source_text})
    page=graph.pages[0].model_copy(update={'regions':[changed]})
    with pytest.raises(ValueError,match='unambiguous'):
        resolve_source_graph(graph.model_copy(update={'pages':[page]}),source_id=SOURCE,person_id=PERSON,semantic=value['semantic'])
