"""Exact private Case read envelopes; wire data never grants Case access."""
from __future__ import annotations

import base64
import json
from typing import Annotated, Literal
from uuid import UUID

from pydantic import Field, model_validator
from .kaggle_protocol import ReadIdentity, TrustedActorContext, Identifier, Nonnegative, EvidenceItem


def canonical(value):
    return json.dumps(value,sort_keys=True,separators=(',', ':'),ensure_ascii=False,allow_nan=False).encode()


def exact_uuid(value):
    if type(value) is not str or str(UUID(value))!=value:
        raise ValueError('canonical Case UUID required')
    return value


class CaseReadRequest(ReadIdentity):
    type:Literal['CASE_READ']='CASE_READ'
    protocol_version:Literal[2]=2
    operation:Literal['disagreement_read']='disagreement_read'
    operation_version:Literal[1]=1
    case_id:str
    section:Literal['case','evidence','contributions','conclusions','history','draft_get','draft_list','operation']='case'
    after_seq:Nonnegative=0
    limit:Annotated[int,Field(ge=1,le=20)]=10
    revision:Annotated[int,Field(ge=1)]|None=None
    draft_id:str|None=None
    draft_version:Annotated[int,Field(ge=1)]|None=None
    after_draft:str|None=None
    operation_key:Identifier|None=None
    actor:TrustedActorContext
    min_data_seq:Nonnegative
    deadline_ms:Annotated[int,Field(gt=0)]
    compute_claim:Identifier|None=None

    @model_validator(mode='after')
    def fixed_scope(self):
        exact_uuid(self.case_id);exact_uuid(self.actor.person_id)
        for value in (self.draft_id,self.after_draft):
            if value is not None:exact_uuid(value)
        if ((self.section=='operation')!=(self.operation_key is not None)
                or (self.section=='draft_get')!=(self.draft_id is not None)
                or self.draft_version is not None and self.section!='draft_get'
                or self.after_draft is not None and self.section!='draft_list'
                or self.revision is not None and self.section!='case'
                or self.after_seq and self.section not in ('evidence','contributions','conclusions','history')):
            raise ValueError('fixed Case read selector required')
        return self


def decode_record(raw):
    if type(raw) is not str or not 1<=len(raw)<=131072:
        raise ValueError('bounded Case record required')
    body=base64.b64decode(raw,validate=True)
    if not 1<=len(body)<=96*1024 or base64.b64encode(body).decode()!=raw:
        raise ValueError('exact Case record bytes required')
    value=json.loads(body)
    if type(value) is not dict or canonical(value)!=body:
        raise ValueError('canonical Case record required')
    return value


class CaseReadResult(ReadIdentity):
    type:Literal['CASE_READ_RESULT']='CASE_READ_RESULT'
    protocol_version:Literal[2]=2
    status:Literal['ok','unavailable','invalid_query','denied','cancelled']
    evidence:tuple[EvidenceItem,...]=Field(default=(),max_length=0)
    record_b64:str|None=None
    observed_data_seq:Nonnegative|None
    observed_projection_seq:Nonnegative|None
    observed_visibility_epoch:Nonnegative|None=None
    observed_database_generation:Identifier|None=None
    reason:Literal['projection_lag','deadline_exceeded','not_ready','disconnected','caller_cancelled','turn_replaced',
        'query_too_large','invalid_input','access_denied','prior_write_pending','evidence_withdrawn','internal_error']|None=None
    prior_write_pending:bool=False

    @property
    def record(self):
        return decode_record(self.record_b64) if self.record_b64 is not None else None

    @model_validator(mode='after')
    def exact_record(self):
        if self.status=='ok':
            if self.reason is not None or self.prior_write_pending or self.record_b64 is None or any(v is None for v in (
                    self.observed_data_seq,self.observed_projection_seq,self.observed_visibility_epoch,self.observed_database_generation)):
                raise ValueError('exact Case record and native frontier required')
            decode_record(self.record_b64)
        elif self.record_b64 is not None:
            raise ValueError('failed Case read cannot carry private content')
        return self
