"""Fixed private GEO identity reads; legacy locators are not source evidence."""
from __future__ import annotations

import base64
import hashlib
import json
from typing import Annotated,Literal

from pydantic import Field,model_validator

from .geo_migration import GeoManifestItem,canonical,exact_uuid,normalized,owner_ref
from .kaggle_protocol import Contract,ReadIdentity,TrustedActorContext,Identifier,Nonnegative,EvidenceItem


class GeoReadRequest(ReadIdentity):
    type:Literal['GEO_READ']='GEO_READ'
    protocol_version:Literal[2]=2
    operation:Literal['geo_identity_read']='geo_identity_read'
    operation_version:Literal[1]=1
    action:Literal['identity','members']='identity'
    entity_id:str|None=None
    poi_ref:str|None=None
    name:Annotated[str,Field(min_length=1,max_length=200)]|None=None
    year:Annotated[int,Field(ge=1,le=9999)]|None=None
    category:Literal['owner_reference','provenance','alias','relation','source_geo_observation']|None=None
    cursor:Annotated[str,Field(max_length=1600)]|None=None
    limit:Annotated[int,Field(ge=1,le=3)]=3
    actor:TrustedActorContext
    min_data_seq:Nonnegative
    deadline_ms:Annotated[int,Field(gt=0)]
    compute_claim:Identifier|None=None

    @model_validator(mode='after')
    def fixed_scope(self):
        if self.entity_id is not None:exact_uuid(self.entity_id)
        if self.poi_ref is not None:owner_ref(self.poi_ref,'street_story')
        if self.action=='members':
            if self.entity_id is None or self.category is None or any(x is not None for x in (self.name,self.poi_ref,self.year)):
                raise ValueError('exact entity/member category required')
        elif sum(x is not None for x in (self.entity_id,self.poi_ref,self.name))!=1 or self.category is not None or (self.year is not None and self.name is None):
            raise ValueError('one fixed GEO identity selector required')
        if self.name is not None and not normalized(self.name):raise ValueError('nonempty GEO name required')
        return self


def decode_item(raw):
    if type(raw) is not str or not 1<=len(raw)<=87384:raise ValueError('bounded GEO item required')
    body=base64.b64decode(raw,validate=True)
    if not 1<=len(body)<=65536 or base64.b64encode(body).decode()!=raw:raise ValueError('exact GEO item bytes required')
    item=GeoManifestItem.from_wire(json.loads(body))
    if canonical(item.wire())!=body:raise ValueError('canonical GEO item bytes required')
    return item,hashlib.sha256(body).hexdigest()


class GeoReadRecord(Contract):
    item_key:Annotated[str,Field(min_length=1,max_length=512)]
    wire_b64:str
    matched_wire_b64:str|None=None
    manifest_id:Annotated[str,Field(pattern='^[0-9a-f]{64}$')]
    activation_sequence:Annotated[int,Field(ge=1)]
    activation_operation_key:Identifier
    activation_command_sha256:Annotated[str,Field(pattern='^[0-9a-f]{64}$')]

    @model_validator(mode='after')
    def exact_record(self):
        item,_=decode_item(self.wire_b64)
        if item.key!=self.item_key:raise ValueError('GEO item identity differs')
        if self.matched_wire_b64 is not None:decode_item(self.matched_wire_b64)
        return self


class GeoReadResult(ReadIdentity):
    type:Literal['GEO_READ_RESULT']='GEO_READ_RESULT'
    protocol_version:Literal[2]=2
    status:Literal['ok','unavailable','invalid_query','denied','cancelled']
    # This is an independent manifest domain. No SourceDependency is fabricated.
    evidence:tuple[EvidenceItem,...]=Field(default=(),max_length=0)
    records:tuple[GeoReadRecord,...]=Field(default=(),max_length=3)
    next_cursor:Annotated[str,Field(max_length=1600)]|None=None
    observed_data_seq:Nonnegative|None
    observed_projection_seq:Nonnegative|None
    observed_visibility_epoch:Nonnegative|None=None
    observed_database_generation:Identifier|None=None
    reason:Literal['projection_lag','deadline_exceeded','not_ready','disconnected','caller_cancelled','turn_replaced',
        'query_too_large','invalid_input','access_denied','prior_write_pending','evidence_withdrawn','internal_error']|None=None
    prior_write_pending:bool=False

    @model_validator(mode='after')
    def fixed_result(self):
        if self.status=='ok' and (self.reason is not None or self.prior_write_pending or any(x is None for x in
                (self.observed_data_seq,self.observed_projection_seq,self.observed_visibility_epoch,self.observed_database_generation))):
            raise ValueError('exact native GEO frontier required')
        if self.status!='ok' and (self.records or self.next_cursor):raise ValueError('failed GEO read cannot carry records')
        if len(canonical(self.model_dump(mode='json')))>256*1024:raise ValueError('GEO result byte bound')
        return self


def cursor_scope(request,observed):
    return hashlib.sha256(canonical(dict(action=request.action,entity=request.entity_id,poi=request.poi_ref,
        name=request.name,year=request.year,category=request.category,actor=request.actor.model_dump(mode='json'),
        observed=observed))).hexdigest()


def encode_cursor(scope,key):
    return base64.urlsafe_b64encode(canonical(dict(v=1,scope=scope,key=key))).decode().rstrip('=')


def decode_cursor(cursor,scope):
    try:
        value=json.loads(base64.b64decode(cursor+'='*(-len(cursor)%4),altchars=b'-_',validate=True))
        if (set(value)!={'v','scope','key'} or type(value['v']) is not int or value['v']!=1 or value['scope']!=scope
                or type(value['key']) is not str or not 1<=len(value['key'])<=512 or encode_cursor(scope,value['key'])!=cursor):raise ValueError
        return value['key']
    except (ValueError,TypeError,KeyError):raise ValueError('invalid or stale GEO cursor') from None
