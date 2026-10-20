"""Deterministic GEO-only whitelist. Importing/building performs no I/O.

Legacy source locators are references, never accepted target sources. No book
text, vectors, original bytes, owner credentials or executable queue work is in
this format. Provider effects and remote accepted-command membership are a
separate controller boundary.
"""
from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

Digest = Annotated[str, Field(pattern=r'^[0-9a-f]{64}$')]
Identity = Annotated[str, Field(min_length=1, max_length=512)]
Short = Annotated[str, Field(min_length=1, max_length=200)]


def canonical(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False,
                      separators=(',', ':')).encode()


def sha(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def exact_uuid(value):
    if type(value) is not str or str(UUID(value)) != value:
        raise ValueError('canonical retained UUID required')
    return value


def normalized(value):
    return ' '.join(unicodedata.normalize('NFKC',value).casefold().split())


def owner_ref(value, owner):
    if owner == 'street_story':
        if not value.startswith('streetstory://poi/'):
            raise ValueError('exact Street Story owner reference required')
        key = value.removeprefix('streetstory://poi/')
        if not re.fullmatch(r'poi_ss_[0-9a-f]{24}',key):
            exact_uuid(key)
    elif (not re.fullmatch(r'cartography://[A-Za-z0-9][A-Za-z0-9._:/-]{0,280}',value)
          or any(part in ('.','..') for part in value.removeprefix('cartography://').split('/'))):
        raise ValueError('bounded Cartography owner reference required')
    return value


class Contract(BaseModel):
    model_config=ConfigDict(extra='forbid',strict=True,frozen=True)


class Edition(Contract):
    """Recorded edition metadata, not a fabricated new canonical edition ID."""
    legacy_document_id: str
    source_sha256: Digest
    title: Annotated[str,Field(min_length=1,max_length=250)]
    authors: tuple[Annotated[str,Field(min_length=1,max_length=200)], ...] = Field(max_length=20)
    publication_year: Annotated[int,Field(ge=1,le=9999)] | None = None
    edition_statement: Annotated[str,Field(max_length=250)] | None = None
    binding: Literal['legacy_document_plus_recorded_source_sha'] = 'legacy_document_plus_recorded_source_sha'
    hash_verification: Literal['legacy_recorded_not_reverified'] = 'legacy_recorded_not_reverified'
    _id=field_validator('legacy_document_id')(exact_uuid)


class Provenance(Contract):
    kind: Literal['provenance'] = 'provenance'
    edition: Edition
    legacy_source_revision: Annotated[int,Field(ge=1)]
    source_record_kind: Literal['mention','alias','relation'] = 'mention'
    source_record_id: str
    locator_ordinal: Annotated[int,Field(ge=0,le=7)] = 0
    page_id: str
    physical_page_index: Annotated[int,Field(ge=0)]
    printed_page_number: Annotated[str,Field(max_length=40)] | None = None
    region_id: str | None = None
    # Exact original IDs remain locators; their bodies/bytes are excluded.
    legacy_chunk_id: str | None = None
    legacy_evidence_sha256: Digest
    source_anchor: Literal['legacy_unanchored'] = 'legacy_unanchored'
    target_source_id: None = None
    source_text_transferred: Literal[False] = False
    _page=field_validator('page_id')(exact_uuid)
    _record=field_validator('source_record_id')(exact_uuid)

    @field_validator('region_id','legacy_chunk_id')
    @classmethod
    def optional_id(cls,value):
        return None if value is None else exact_uuid(value)


class LegacyResolution(Contract):
    contract_version: Literal['rkb.geo_claim.v1']
    attempt_ref: Annotated[str,Field(pattern=r'^geoa_[0-9a-f]{32}$')]
    resolution: Literal['owner_alias_candidate']
    identity_state: Short
    recorded_metadata_sha256: Digest
    receipt_verification: Literal['legacy_metadata_not_owner_receipt'] = 'legacy_metadata_not_owner_receipt'
    historical_geometry: Literal['not_verified'] = 'not_verified'


class Place(Contract):
    kind: Literal['place'] = 'place'
    canonical_label: Short
    exact_source_spelling: Short
    state: Literal['candidate','reviewed','unresolved']
    provenance_keys: tuple[Identity, ...] = Field(min_length=1,max_length=64)
    metadata_names: tuple[Short, ...] = Field(default=(),max_length=20)
    historical_geometry: Literal['unresolved_with_owner'] = 'unresolved_with_owner'
    canonical_poi_ref: str | None = None
    legacy_resolution: LegacyResolution | None = None

    @field_validator('canonical_poi_ref')
    @classmethod
    def poi(cls,value):
        return None if value is None else owner_ref(value,'street_story')


class Alias(Contract):
    kind: Literal['alias'] = 'alias'
    entity_id: str
    value: Short
    language: Annotated[str,Field(max_length=20)] | None = None
    alias_type: Literal['current','historical','former','transliteration','spelling_variant']
    time_scope: Annotated[str,Field(max_length=120)] | None = None
    provenance_key: Identity
    _entity=field_validator('entity_id')(exact_uuid)


class Relation(Contract):
    kind: Literal['relation'] = 'relation'
    source_id: str
    target_id: str
    relation: Literal['located_in'] = 'located_in'
    time_scope: Annotated[str,Field(max_length=120)] | None = None
    provenance_keys: tuple[Identity, ...] = Field(min_length=1,max_length=8)
    claim_state: Literal['candidate','reviewed']
    spatial_acceptance: Literal['legacy_source_claim_not_owner_geometry'] = 'legacy_source_claim_not_owner_geometry'
    _endpoints=field_validator('source_id','target_id')(exact_uuid)

    @model_validator(mode='after')
    def different(self):
        if self.source_id==self.target_id:raise ValueError('distinct geographic relation endpoints required')
        return self


class GeoObservation(Contract):
    kind: Literal['source_geo_observation'] = 'source_geo_observation'
    entity_id: str
    mention_id: str
    original_spelling: Short
    names: tuple[Short, ...] = Field(min_length=1,max_length=20)
    provenance_key: Identity
    time_scope: Annotated[str,Field(max_length=120)] | None = None
    period_precision: Literal['unknown','year','range'] = 'unknown'
    valid_year_from: Annotated[int,Field(ge=1,le=9999)] | None = None
    valid_year_to: Annotated[int,Field(ge=1,le=9999)] | None = None
    calendar: Literal['gregorian','julian','unspecified'] = 'unspecified'
    locator_hint: 'LocatorHint'
    resolution: Literal['unresolved','owner_reference_candidate'] = 'unresolved'
    old_queue_work_transferred: Literal[False] = False
    _ids=field_validator('entity_id','mention_id')(exact_uuid)

    @model_validator(mode='after')
    def period(self):
        if ((self.valid_year_from is None)!=(self.valid_year_to is None) or
                self.valid_year_from is not None and self.valid_year_from>self.valid_year_to or
                self.period_precision=='unknown' and self.valid_year_from is not None or
                self.period_precision!='unknown' and self.valid_year_from is None):
            raise ValueError('explicit geographic validity without inferred years required')
        if self.period_precision=='year' and self.valid_year_from!=self.valid_year_to:
            raise ValueError('year scope requires exact same endpoint')
        return self


class ExternalId(Contract):
    namespace: Annotated[str,Field(min_length=1,max_length=60)]
    value: Annotated[str,Field(min_length=1,max_length=200)]


class LocatorHint(Contract):
    external_ids: tuple[ExternalId,...] = Field(default=(),max_length=20)
    latitude: Annotated[float,Field(ge=-90,le=90,allow_inf_nan=False)] | None = None
    longitude: Annotated[float,Field(ge=-180,le=180,allow_inf_nan=False)] | None = None
    coordinate_method: Literal['source_locator_unverified'] = 'source_locator_unverified'
    historical_geometry: Literal['not_verified'] = 'not_verified'

    @model_validator(mode='after')
    def paired_hint(self):
        if (self.latitude is None)!=(self.longitude is None):raise ValueError('paired source coordinate hint required')
        if len({item.namespace for item in self.external_ids})!=len(self.external_ids):
            raise ValueError('unique source external namespaces required')
        return self


class OwnerReference(Contract):
    kind: Literal['owner_reference'] = 'owner_reference'
    entity_id: str
    owner: Literal['street_story','cartography']
    ref: Identity
    recorded_binding: Literal['existing_reference_only'] = 'existing_reference_only'
    owner_acceptance: Literal['not_reverified'] = 'not_reverified'
    target_copies_canonical_owner: Literal[False] = False
    _entity=field_validator('entity_id')(exact_uuid)

    @model_validator(mode='after')
    def exact_owner(self):
        owner_ref(self.ref,self.owner)
        return self


class Exclusion(Contract):
    kind: Literal['excluded_non_geo'] = 'excluded_non_geo'
    source_table: Literal['rkb_entities','rkb_entity_aliases','rkb_entity_relations','rkb_geo_attempts',
        'rkb_geo_receipts','rkb_documents','rkb_chunks','rkb_pages','rkb_regions','rkb_objects',
        'rkb_chunk_embeddings_bge','rkb_chunk_embeddings_e5','stories','cases','drafts']
    reason: Literal['non_geo_entity','alias_of_non_geo_entity','non_geographic_relation',
        'old_queue_execution','old_non_geo_content']


class Blocked(Contract):
    kind: Literal['blocked_geo_identity'] = 'blocked_geo_identity'
    source_table: Literal['rkb_entities','rkb_entity_aliases','rkb_entity_relations','rkb_geo_intents']
    reason: Literal['missing_source_metadata','missing_source_sha','missing_page_locator',
        'missing_mention','missing_source_label','missing_geographic_entity','source_dependency_mismatch']


Payload=Annotated[Place|Alias|Relation|GeoObservation|OwnerReference|Provenance|Exclusion|Blocked,
                  Field(discriminator='kind')]


class InventoryCounts(Contract):
    entities: Annotated[int,Field(ge=0,le=4096)]
    geo_places: Annotated[int,Field(ge=0,le=4096)]
    mentions: Annotated[int,Field(ge=0,le=4096)]
    aliases: Annotated[int,Field(ge=0,le=4096)]
    relations: Annotated[int,Field(ge=0,le=4096)]
    geo_intents: Annotated[int,Field(ge=0,le=4096)]
    accepted_geo_metadata: Annotated[int,Field(ge=0,le=4096)]


class GeoManifestItem(Contract):
    stable_id: Identity
    source_owner: Literal['regional_knowledge'] = 'regional_knowledge'
    source_store: Literal['legacy_rkb_sqlite'] = 'legacy_rkb_sqlite'
    source_revision: Annotated[str,Field(min_length=1,max_length=128)]
    owner_person_id: str
    access: Literal['private_owner_only'] = 'private_owner_only'
    status: Literal['legacy_unanchored','left_with_owner','excluded_non_geo','blocked_missing_dependency']
    dependencies: tuple[Identity, ...] = Field(default=(),max_length=64)
    payload: Payload
    _owner=field_validator('owner_person_id')(exact_uuid)

    @model_validator(mode='after')
    def category_status(self):
        if len(set(self.dependencies))!=len(self.dependencies):raise ValueError('unique dependency keys required')
        expected=('blocked_missing_dependency' if self.payload.kind=='blocked_geo_identity' else
                  'left_with_owner' if self.payload.kind=='owner_reference' else
                  'excluded_non_geo' if self.payload.kind=='excluded_non_geo' else 'legacy_unanchored')
        if self.status not in (expected,'blocked_missing_dependency'):
            raise ValueError('manifest cannot claim an unobserved migration effect')
        if self.payload.kind in ('place','alias','relation'):
            exact_uuid(self.stable_id)
        if self.payload.kind=='provenance':
            expected_id=self.payload.source_record_id+(f':{self.payload.locator_ordinal}' if self.payload.locator_ordinal else '')
            if self.stable_id!=expected_id:raise ValueError('exact source record/locator identity required')
        payload=self.payload
        expected_dependencies=None
        if isinstance(payload,Place):expected_dependencies=set(payload.provenance_keys)
        elif isinstance(payload,Alias):expected_dependencies={'place:'+payload.entity_id,payload.provenance_key}
        elif isinstance(payload,Relation):expected_dependencies={'place:'+payload.source_id,'place:'+payload.target_id,*payload.provenance_keys}
        elif isinstance(payload,GeoObservation):
            if payload.provenance_key!='provenance:'+payload.mention_id:
                raise ValueError('exact geographic mention provenance required')
            expected_dependencies={'place:'+payload.entity_id,payload.provenance_key}
        elif isinstance(payload,OwnerReference):expected_dependencies={'place:'+payload.entity_id}
        elif isinstance(payload,(Provenance,Exclusion)):expected_dependencies=set()
        if expected_dependencies is not None and set(self.dependencies)!=expected_dependencies:
            raise ValueError('exact geographic dependency set required')
        if isinstance(payload,(Place,Relation)) and any(not key.startswith('provenance:') for key in payload.provenance_keys):
            raise ValueError('exact geographic provenance category required')
        if isinstance(payload,Alias) and not payload.provenance_key.startswith('provenance:'):
            raise ValueError('exact alias provenance category required')
        return self

    @property
    def key(self):return self.payload.kind+':'+self.stable_id

    def wire(self):
        value=self.model_dump(mode='json')
        return dict(value,payload_sha256=sha(value['payload']))

    @classmethod
    def from_wire(cls,value):
        value=json.loads(canonical(value))
        if type(value) is not dict or value.pop('payload_sha256',None)!=sha(value.get('payload')):
            raise ValueError('geographic payload hash mismatch')
        return cls.model_validate_json(canonical(value))


class GeoManifestHeader(Contract):
    codec: Literal['rkb.geo-migration-manifest.v1'] = 'rkb.geo-migration-manifest.v1'
    source_inventory_sha256: Digest
    inventory_counts: InventoryCounts
    missing_dependencies: tuple[Identity, ...] = Field(default=(),max_length=4096)
    canonical_owners_copied: Literal[False] = False
    accepted_target_sources_created: Literal[False] = False


class GeoMigrationManifest(GeoManifestHeader):
    items: tuple[GeoManifestItem, ...] = Field(max_length=4096)

    @model_validator(mode='after')
    def closure(self):
        keys=[item.key for item in self.items]
        if keys!=sorted(keys) or len(set(keys))!=len(keys):
            raise ValueError('unique deterministic geographic item ordering required')
        by_key={item.key:item for item in self.items}
        # The input inventory is independent of item ordering. It cannot be
        # replaced by an empty/partial output carrying the old count header.
        counts=self.inventory_counts
        actual=dict(entities=0,geo_places=0,aliases=0,relations=0,geo_intents=0)
        for item in self.items:
            kind=item.payload.kind
            table=getattr(item.payload,'source_table',None)
            if kind=='place' or kind=='blocked_geo_identity' and table=='rkb_entities':
                actual['entities']+=1;actual['geo_places']+=1
            elif kind=='excluded_non_geo' and table=='rkb_entities':actual['entities']+=1
            if kind=='alias' or kind in ('blocked_geo_identity','excluded_non_geo') and table=='rkb_entity_aliases':actual['aliases']+=1
            if kind=='relation' or kind in ('blocked_geo_identity','excluded_non_geo') and table=='rkb_entity_relations':actual['relations']+=1
            if kind=='source_geo_observation' or kind=='blocked_geo_identity' and table=='rkb_geo_intents':actual['geo_intents']+=1
        if any(value!=getattr(counts,key) for key,value in actual.items()):
            raise ValueError('geographic inventory record counts mismatch')
        missing=set()
        for item in self.items:
            for dependency in item.dependencies:
                other=by_key.get(dependency)
                if other is None or other.status in ('excluded_non_geo','blocked_missing_dependency'):
                    missing.add(dependency)
                elif other.owner_person_id!=item.owner_person_id:
                    raise ValueError('private geographic dependencies cannot cross owners')
        if tuple(sorted(missing))!=self.missing_dependencies:
            raise ValueError('exact missing geographic dependency inventory required')
        if len(canonical(self.wire()))>4*1024*1024:
            raise ValueError('geographic manifest byte bound exceeded')
        return self

    def wire(self):
        return dict(codec=self.codec,source_inventory_sha256=self.source_inventory_sha256,
            inventory_counts=self.inventory_counts.model_dump(mode='json'),
            items=[item.wire() for item in self.items],missing_dependencies=list(self.missing_dependencies),
            canonical_owners_copied=False,accepted_target_sources_created=False)

    @property
    def sha256(self):return sha(self.wire())

    def require_ready(self):
        if self.missing_dependencies or any(item.status=='blocked_missing_dependency' for item in self.items):
            raise ValueError('geographic manifest has unresolved required dependencies')
        if sum(item.payload.kind=='place' and item.payload.legacy_resolution is not None for item in self.items)!=self.inventory_counts.accepted_geo_metadata:
            raise ValueError('accepted geographic metadata coverage mismatch')
        return self

    @classmethod
    def from_wire(cls,value):
        value=json.loads(canonical(value))
        if type(value) is not dict or type(value.get('items')) is not list:
            raise ValueError('exact geographic manifest required')
        for item in value['items']:
            if item.pop('payload_sha256',None)!=sha(item.get('payload')):
                raise ValueError('geographic payload hash mismatch')
        return cls.model_validate_json(canonical(value))
