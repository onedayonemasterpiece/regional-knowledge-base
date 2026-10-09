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
    'operated_at','predecessor_of','founded_by',
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

class GraphAlias(Strict):
    value:str=Field(min_length=1,max_length=200)
    language:str|None=Field(default=None,max_length=20)
    alias_type:AliasKind='spelling_variant'
    time_scope:str|None=Field(default=None,max_length=120)
    evidence:GraphEvidence

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

    @model_validator(mode='after')
    def reviewed(self):
        if not normalize_alias(self.canonical_label) or not normalize_alias(self.exact_source_spelling):raise ValueError('empty entity name')
        if self.state=='reviewed' and not (self.review_note or '').strip():raise ValueError('review note required')
        if (self.kind=='poi_ref') != (self.poi_locator is not None):raise ValueError('poi_ref requires a locator; other kinds cannot carry one')
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
    relations:list[GraphRelation]=Field(default_factory=list,max_length=64)
    @model_validator(mode='after')
    def shape(self):
        kinds={n.key:n.kind for n in self.entities}
        if len(kinds)!=len(self.entities):raise ValueError('duplicate entity key')
        for edge in self.relations:
            if not valid_relation_shape(
                    edge.kind,kinds.get(edge.source_key),kinds.get(edge.target_key),
                    same_entity=edge.source_key==edge.target_key):
                raise ValueError('invalid relation endpoints')
        return self

def entity_id(document_id:UUID,key:str)->UUID:
    return uuid5(document_id,'entity:'+key)

def validate_staged_bundle(bundle:GraphBundle,graph)->None:
    chunks={c.chunk_id:c for c in graph.chunks}
    for node in bundle.entities:
        evidence=[node.evidence,*[a.evidence for a in node.aliases]]
        evidence += [e for r in bundle.relations if node.key==r.source_key for e in r.evidence]
        for e in evidence:
            c=chunks.get(e.chunk_id)
            region=next((r for p in graph.pages if p.page_id==e.page_id for r in p.regions if r.region_id==e.region_id),None)
            if graph.pages and (region is None or e.exact_quote not in region.source_text):raise ValueError('graph quote must match the exact source region')
            if c is None or e.page_id not in c.page_ids or e.region_id not in c.region_ids or e.exact_quote not in c.text:raise ValueError('graph evidence must reference exact staged chunk/page/region material')
        if node.exact_source_spelling not in node.evidence.exact_quote:raise ValueError('source spelling absent from evidence quote')
