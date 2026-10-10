"""Bounded, model-authored graph candidates. No name-based identity merge."""
from __future__ import annotations
import hashlib,json,unicodedata
from typing import Literal
from uuid import UUID,uuid5
from pydantic import BaseModel,Field,ConfigDict,model_validator
from .contracts import PoiLocatorInput

Kind=Literal['person','organization','event','historical_thread','poi_ref']
AliasKind=Literal['current','historical','former','transliteration','spelling_variant']
RelationKind=Literal[
    'participated_in','occurred_at','member_of','affiliated_with',
    'operated_at','predecessor_of','founded_by','located_in',
]

# Edges are explicit historical source claims, not automatic identity inference.
# Keep these shapes identical to the SQLite write guard and the optional
# PostgreSQL schema extension 024_organization_graph.sql.
RELATION_ENDPOINTS = {
    'participated_in': {('person','event'),('organization','event')},
    'occurred_at': {('event','poi_ref')},
    'member_of': {(kind,'historical_thread') for kind in (
        'person','organization','event','poi_ref')},
    'affiliated_with': {('person','organization')},
    'operated_at': {('organization','poi_ref')},
    'predecessor_of': {('organization','organization')},
    'founded_by': {('organization','person')},
    'located_in': {('poi_ref','poi_ref')},
}

def valid_relation_shape(kind: str, source_kind: str, target_kind: str,
                         *, same_entity: bool = False) -> bool:
    return (not same_entity and
            (source_kind, target_kind) in RELATION_ENDPOINTS.get(kind, set()))

def normalize_alias(value:str)->str:
    return ' '.join(unicodedata.normalize('NFKC',value).casefold().split())

def digest(value)->str:
    return hashlib.sha256(json.dumps(value,sort_keys=True,ensure_ascii=False,separators=(',',':')).encode()).hexdigest()

class Strict(BaseModel):
    model_config=ConfigDict(extra='forbid')

class GraphEvidence(Strict):
    chunk_id:UUID
    page_id:UUID
    region_id:UUID
    exact_quote:str=Field(min_length=1,max_length=2000)

class ResearchSource(Strict):
    """Model-consulted context, not a quotation from the imported book."""
    url: str = Field(min_length=8, max_length=1000, pattern=r'^https?://')
    note: str = Field(min_length=1, max_length=500)

class GraphAlias(Strict):
    value:str=Field(min_length=1,max_length=200)
    language:str|None=Field(default=None,max_length=20)
    alias_type:AliasKind='spelling_variant'
    time_scope:str|None=Field(default=None,max_length=120)
    evidence:GraphEvidence
    research_sources:list[ResearchSource]=Field(default_factory=list,max_length=8)

class GraphEntity(Strict):
    key:str=Field(min_length=1,max_length=100)
    entity_id:UUID|None=None # Explicit reuse requires same owner and kind, never name matching.
    kind:Kind
    canonical_label:str=Field(min_length=1,max_length=200)
    exact_source_spelling:str=Field(min_length=1,max_length=200)
    state:Literal['candidate','reviewed']='candidate'
    review_note:str|None=Field(default=None,max_length=500)
    poi_locator:PoiLocatorInput|None=None
    evidence:GraphEvidence
    aliases:list[GraphAlias]=Field(default_factory=list,max_length=20)
    place_kind:str|None=Field(default=None,min_length=1,max_length=60)
    place_context:str|None=Field(default=None,max_length=500)
    research_sources:list[ResearchSource]=Field(default_factory=list,max_length=8)
    canonical_poi_ref:str|None=Field(default=None,max_length=120)
    map_refs:list[str]=Field(default_factory=list,max_length=20)

    @model_validator(mode='after')
    def reviewed(self):
        if not normalize_alias(self.canonical_label) or not normalize_alias(self.exact_source_spelling):raise ValueError('empty entity name')
        if self.state=='reviewed' and not (self.review_note or '').strip():raise ValueError('review note required')
        if self.kind=='poi_ref' and self.poi_locator is None:
            self.poi_locator=PoiLocatorInput(names=[self.canonical_label,self.exact_source_spelling])
        if self.kind!='poi_ref' and (self.poi_locator is not None or self.place_kind or self.place_context or self.canonical_poi_ref or self.map_refs):
            raise ValueError('only a place can carry location metadata')
        if self.canonical_poi_ref:
            from .poi_reference import canonical_poi_key
            canonical_poi_key(self.canonical_poi_ref)
        if any(not ref.startswith('cartography://') or len(ref)>300 for ref in self.map_refs):
            raise ValueError('map_refs require bounded cartography references')
        return self

class GraphRelation(Strict):
    source_key:str=Field(min_length=1,max_length=100)
    target_key:str=Field(min_length=1,max_length=100)
    kind:RelationKind
    evidence:list[GraphEvidence]=Field(min_length=1,max_length=8)
    time_scope:str|None=Field(default=None,max_length=120)
    state:Literal['candidate','reviewed']='candidate'
    review_note:str|None=Field(default=None,max_length=500)
    @model_validator(mode='after')
    def reviewed(self):
        if self.state=='reviewed' and not (self.review_note or '').strip():raise ValueError('review note required')
        return self

class GraphBundle(Strict):
    entities:list[GraphEntity]=Field(default_factory=list,max_length=32)
    entity_refs:dict[str,UUID]=Field(default_factory=dict,max_length=32)
    relations:list[GraphRelation]=Field(default_factory=list,max_length=64)
    @model_validator(mode='after')
    def shape(self):
        kinds={n.key:n.kind for n in self.entities}
        if len(kinds)!=len(self.entities) or set(kinds)&set(self.entity_refs):raise ValueError('duplicate entity key')
        if any(not k.strip() or len(k)>100 for k in self.entity_refs):raise ValueError('invalid reference key')
        keys=set(kinds)|set(self.entity_refs)
        for edge in self.relations:
            if edge.source_key not in keys or edge.target_key not in keys or edge.source_key==edge.target_key:
                raise ValueError('invalid relation endpoints')
            if edge.source_key in kinds and edge.target_key in kinds and not valid_relation_shape(
                    edge.kind,kinds[edge.source_key],kinds[edge.target_key]):
                raise ValueError('invalid relation endpoints')
        # Referenced identities are checked for owner, active source and actual
        # endpoint kind by GraphService; do not fabricate another source mention.
        return self

def entity_id(document_id:UUID,key:str)->UUID:
    return uuid5(document_id,'entity:'+key)

def validate_staged_bundle(bundle:GraphBundle,graph)->None:
    # Relation-only batches may refer to existing identities but still need
    # genuine evidence in this staged source.
    chunks={c.chunk_id:c for c in graph.chunks}
    evidence=[e for node in bundle.entities for e in [node.evidence,*[a.evidence for a in node.aliases]]]
    evidence += [e for r in bundle.relations for e in r.evidence]
    for e in evidence:
        c=chunks.get(e.chunk_id)
        region=next((r for p in graph.pages if p.page_id==e.page_id for r in p.regions if r.region_id==e.region_id),None)
        if graph.pages and (region is None or e.exact_quote not in region.source_text):raise ValueError('graph quote must match the exact source region')
        if c is None or e.page_id not in c.page_ids or e.region_id not in c.region_ids or e.exact_quote not in c.text:raise ValueError('graph evidence must reference exact staged chunk/page/region material')
    for node in bundle.entities:
        if node.exact_source_spelling not in node.evidence.exact_quote:raise ValueError('source spelling absent from evidence quote')
