"""Synthetic GEO whitelist closure; no live corpus, owner DB or provider I/O."""
import copy
from uuid import UUID

import pytest

from regional_knowledge.geo_migration import GeoMigrationManifest,InventoryCounts,canonical,sha,owner_ref
from regional_knowledge.geo_whitelist import build_geo_whitelist,GeoWhitelistError


def uid(n):return str(UUID(int=n))


def snapshot():
    owner=uid(1);doc=uid(2);page=uid(3);region=uid(4);chunk=uid(5)
    original='SECRET OLD CHAPTER TEXT MUST NOT TRANSFER. The Old Harbour was here.'
    proof=dict(chunk_id=chunk,page_id=page,region_id=region,exact_quote=original)
    entities=[dict(id=uid(10),owner_user_id=owner,document_id=doc,revision=1,kind='poi_ref',
        canonical_label='Old Harbour',state='candidate',external_ref='streetstory://poi/poi_ss_'+'a'*24,
        metadata=dict(poi_locator=dict(names=['Old Harbour','Alter Hafen']),
            map_refs=['cartography://layers/retained-layer-1'],
            geo_queue=dict(contract_version='rkb.geo_claim.v1',attempt_id='geoa_'+'b'*32,
                resolution='owner_alias_candidate',identity_state='candidate',historical_geometry='not_verified'),
            private_book_body=original)),
        dict(id=uid(11),owner_user_id=owner,document_id=doc,revision=1,kind='poi_ref',
            canonical_label='Harbour Quarter',state='unresolved',external_ref=None,metadata={}),
        *[dict(id=uid(20+n),owner_user_id=owner,document_id=doc,revision=1,kind=kind,
            canonical_label='Excluded '+kind,private_text=original)
            for n,kind in enumerate(('person','organization','event','historical_thread'))]]
    mentions=[dict(id=uid(30+n),entity_id=uid(10+n),document_id=doc,revision=1,
        exact_source_spelling=name,evidence=proof,private_text=original)
        for n,name in enumerate(('Alter Hafen','Hafenviertel'))]
    aliases=[dict(id=uid(40),entity_id=uid(10),value='Historical Harbour',language='en',
        alias_type='historical',time_scope='1890–1910',document_id=doc,revision=1,evidence=proof)]
    relations=[dict(id=uid(50),source_id=uid(10),target_id=uid(11),kind='located_in',
        document_id=doc,revision=1,evidence=[proof],time_scope='1890–1910',state='reviewed'),
        dict(id=uid(51),source_id=uid(20),target_id=uid(10),kind='occurred_at',
             document_id=doc,revision=1,evidence=[proof],state='reviewed')]
    intent=dict(request_id='geor_'+'c'*32,actor_id=owner,document_id=doc,source_revision=1,
        mention_id=uid(30),entity_id=uid(10),source_sha256='a'*64,
        mention_sha256=sha(dict(doc=doc,revision=1,mention_id=uid(30),evidence=proof,name='Alter Hafen')),
        original_spelling='Alter Hafen',names_json=['Alter Hafen','Old Harbour'],
        scope_json=dict(owner_id=owner,visibility='private',source_visibility='private',
            content_visibility='private',distribution_authorized=False),
        period_json=dict(time_scope=None,precision='unknown',calendar='unspecified',valid_year_from=None,valid_year_to=None),
        locator_json=dict(names=['Alter Hafen','Old Harbour'],external_ids={'wikidata':'Q123'},latitude=54.7,longitude=20.4,
            coordinate_method='source_locator_unverified',historical_geometry='not_verified'),
        lease_token='SECRET_LEASE_NOT_TRANSFERRED',accepted_book_text=original)
    return dict(source_inventory_sha256='d'*64,
        expected_counts=InventoryCounts(entities=6,geo_places=2,mentions=2,aliases=1,relations=2,geo_intents=1,accepted_geo_metadata=1),
        entities=entities,mentions=mentions,aliases=aliases,
        relations=relations,source_metadata=[dict(id=doc,owner_user_id=owner,title='Legacy edition title',
            authors=['Recorded author'],publication_year=1900,source_sha256='a'*64,active_revision=1,
            catalog=dict(edition_statement='Recorded edition'),source_text=original)],
        page_metadata=[dict(id=page,document_id=doc,revision=1,physical_page_index=7,
            printed_page_number='VIII',private_page_text=original)],
        region_metadata=[dict(id=region,page_id=page)],
        chunk_metadata=[dict(id=chunk,document_id=doc,revision=1,page_ids=[page],region_ids=[region])],geo_intents=[intent])


def test_whitelist_preserves_geo_ids_private_scopes_and_minimal_unanchored_source_locators():
    data=snapshot();manifest=build_geo_whitelist(**data).require_ready()
    by_key={item.key:item for item in manifest.items}
    assert by_key['place:'+uid(10)].stable_id==uid(10)
    assert by_key['place:'+uid(10)].payload.canonical_poi_ref=='streetstory://poi/poi_ss_'+'a'*24
    assert by_key['place:'+uid(10)].payload.legacy_resolution.receipt_verification=='legacy_metadata_not_owner_receipt'
    assert all(item.access=='private_owner_only' and item.owner_person_id==uid(1) for item in manifest.items)
    provenance=by_key['provenance:'+uid(30)].payload
    assert provenance.edition.legacy_document_id==uid(2) and provenance.edition.source_sha256=='a'*64
    assert provenance.physical_page_index==7 and provenance.printed_page_number=='VIII'
    assert provenance.source_anchor=='legacy_unanchored' and provenance.target_source_id is None
    assert provenance.legacy_chunk_id==uid(5) and not provenance.source_text_transferred
    assert not manifest.accepted_target_sources_created and not manifest.canonical_owners_copied
    encoded=canonical(manifest.wire())
    for forbidden in ('SECRET OLD CHAPTER','SECRET_LEASE','private_book_body','accepted_book_text','embedding','exact_quote'):
        assert forbidden.encode() not in encoded
    owners=[item for item in manifest.items if item.payload.kind=='owner_reference']
    assert {item.payload.owner for item in owners}=={'street_story','cartography'}
    assert all(item.status=='left_with_owner' and not item.payload.target_copies_canonical_owner for item in owners)


def test_only_geographic_relations_and_entities_survive_and_omissions_are_explicit():
    manifest=build_geo_whitelist(**snapshot()).require_ready()
    excluded=[item for item in manifest.items if item.status=='excluded_non_geo']
    assert len(excluded)==5
    assert sum(item.payload.kind=='place' for item in manifest.items)==2
    relation=next(item.payload for item in manifest.items if item.payload.kind=='relation')
    assert (relation.source_id,relation.target_id,relation.relation)==(uid(10),uid(11),'located_in')
    assert relation.spatial_acceptance=='legacy_source_claim_not_owner_geometry'
    observation=next(item.payload for item in manifest.items if item.payload.kind=='source_geo_observation')
    assert observation.resolution=='owner_reference_candidate' and not observation.old_queue_work_transferred
    assert observation.locator_hint.latitude==54.7
    assert observation.locator_hint.coordinate_method=='source_locator_unverified'
    assert observation.locator_hint.historical_geometry=='not_verified'
    assert observation.period_precision=='unknown' and observation.valid_year_from is None


@pytest.mark.parametrize('missing',['source','sha','page','mention'])
def test_missing_dependency_never_becomes_successful_migration_or_silent_omission(missing):
    data=snapshot()
    if missing=='source':data['source_metadata']=[];data['geo_intents']=[]
    if missing=='sha':data['source_metadata'][0]['source_sha256']='';data['geo_intents']=[]
    if missing=='page':data['page_metadata']=[]
    if missing=='mention':data['mentions']=[]
    data['expected_counts']=data['expected_counts'].model_copy(update={'geo_intents':len(data['geo_intents']),'mentions':len(data['mentions'])})
    manifest=build_geo_whitelist(**data)
    assert manifest.missing_dependencies
    assert any(item.status=='blocked_missing_dependency' for item in manifest.items)
    with pytest.raises(ValueError,match='unresolved required dependencies'):manifest.require_ready()
    assert not any(item.status=='migrated' for item in manifest.items)


def test_manifest_is_deterministic_detached_hash_bound_and_roundtrips():
    first=snapshot();second=copy.deepcopy(first)
    for key in ('entities','mentions','aliases','relations','source_metadata','page_metadata','geo_intents'):second[key].reverse()
    a=build_geo_whitelist(**first);b=build_geo_whitelist(**second)
    assert a.sha256==b.sha256 and a.wire()==b.wire()
    first['entities'][0]['canonical_label']='changed caller memory'
    assert b'changed caller memory' not in canonical(a.wire())
    assert GeoMigrationManifest.from_wire(a.wire())==a
    changed=copy.deepcopy(a.wire());changed['items'][0]['payload']['reason']='non_geographic_relation'
    with pytest.raises(ValueError,match='hash mismatch'):GeoMigrationManifest.from_wire(changed)


def test_cross_owner_dependencies_and_invented_geometry_or_owner_ids_are_rejected():
    data=snapshot();data['entities'][1]['owner_user_id']=uid(99)
    with pytest.raises((ValueError,GeoWhitelistError)):build_geo_whitelist(**data)
    data=snapshot();data['geo_intents'][0]['locator_json']['historical_geometry']='verified'
    with pytest.raises(GeoWhitelistError,match='acceptance_unproved'):build_geo_whitelist(**data)
    for ref in ('streetstory://poi/POI_SS_'+'a'*24,'streetstory://poi/not-a-real-key'):
        with pytest.raises(ValueError):owner_ref(ref,'street_story')
    assert owner_ref('streetstory://poi/'+uid(9),'street_story').endswith(uid(9))
    with pytest.raises(ValueError):owner_ref('cartography://layers/../secret','cartography')


def test_non_geo_content_is_not_an_export_input_and_bound_excess_does_not_clip():
    with pytest.raises(TypeError):build_geo_whitelist(**snapshot(),old_chunks=[])
    data=snapshot();data['entities'][0]['metadata']['poi_locator']['names']=['name']*21
    with pytest.raises(ValueError):build_geo_whitelist(**data)


def test_input_coverage_cannot_silently_drop_places_or_geographic_intents():
    for key in ('entities','mentions','geo_intents'):
        data=snapshot();data[key]=data[key][1:]
        with pytest.raises(GeoWhitelistError,match='coverage_mismatch'):build_geo_whitelist(**data)


def test_manifest_rejects_source_text_fields_and_omitted_declared_dependencies_even_with_recomputed_hash():
    manifest=build_geo_whitelist(**snapshot())
    wire=manifest.wire();provenance=next(item for item in wire['items'] if item['payload']['kind']=='provenance')
    provenance['payload']['source_text']='Old chapter must not pass the whitelist'
    provenance['payload_sha256']=sha(provenance['payload'])
    with pytest.raises(ValueError):GeoMigrationManifest.from_wire(wire)
    wire=manifest.wire();place=next(item for item in wire['items'] if item['payload']['kind']=='place')
    place['dependencies']=[]
    with pytest.raises(ValueError,match='exact geographic dependency set'):GeoMigrationManifest.from_wire(wire)


def test_changed_legacy_geo_intent_proof_is_not_reused_as_an_accepted_locator():
    data=snapshot();data['mentions'][0]['evidence']=dict(data['mentions'][0]['evidence'],exact_quote='altered claim')
    with pytest.raises(GeoWhitelistError,match='source_or_owner_mismatch'):build_geo_whitelist(**data)


def test_measured_144_mention_place_preserves_every_provenance_and_dependency():
    # Actual GEO inventory has a 144-mention place. Preserve every locator.
    data=snapshot()
    first=data['mentions'][0]
    data['mentions'] += [dict(first,id=uid(300+n)) for n in range(143)]
    data['expected_counts']=data['expected_counts'].model_copy(update={'mentions':len(data['mentions'])})
    manifest=build_geo_whitelist(**data).require_ready()
    place=next(item for item in manifest.items if item.key=='place:'+uid(10))
    expected={'provenance:'+row['id'] for row in data['mentions'] if row['entity_id']==uid(10)}
    assert len(expected)==144
    assert set(place.payload.provenance_keys)==set(place.dependencies)==expected
    assert expected <= {item.key for item in manifest.items}
    assert GeoMigrationManifest.from_wire(manifest.wire())==manifest
    from regional_knowledge.geo_manifest_commands import plan_geo_manifest
    assert plan_geo_manifest(manifest).manifest_id==manifest.sha256
    assert b'SECRET OLD CHAPTER' not in canonical(manifest.wire())


def test_large_geographic_provenance_refuses_instead_of_silently_clipping():
    data=snapshot()
    data['mentions'] += [dict(data['mentions'][0],id=uid(300+n)) for n in range(256)]
    data['expected_counts']=data['expected_counts'].model_copy(update={'mentions':len(data['mentions'])})
    with pytest.raises(GeoWhitelistError,match='geographic_provenance_bound'):
        build_geo_whitelist(**data)
