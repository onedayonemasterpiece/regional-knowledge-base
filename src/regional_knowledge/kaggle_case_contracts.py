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
    section:Literal['case','evidence','contributions','conclusions','history','draft_get','draft_list','operation','edit_context','search']='case'
    search_query:str|None=None
    place:str|None=None
    source_id:str|None=None
    after_case_id:str|None=None
    candidate_source_id:str|None=None
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
        for value in (self.draft_id,self.after_draft,self.candidate_source_id,self.source_id,self.after_case_id):
            if value is not None:exact_uuid(value)
        if self.section=='search':
            if (self.case_id!='00000000-0000-0000-0000-000000000000'
                    or not any((self.search_query,self.place,self.source_id))
                    or any(value is not None for value in (self.candidate_source_id,self.draft_id,
                        self.draft_version,self.after_draft,self.revision,self.operation_key))
                    or self.after_seq or not 1<=self.limit<=8):
                raise ValueError('bounded Case subject search selector required')
            for value in (self.search_query,self.place):
                if value is not None and (type(value) is not str or not value.strip()
                        or len(value)>120 or len(value.encode())>480):
                    raise ValueError('bounded Case subject term required')
            return self
        if any(value is not None for value in (self.search_query,self.place,self.source_id,self.after_case_id)):
            raise ValueError('Case search selector restricted to search')
        if ((self.section=='operation')!=(self.operation_key is not None)
                or (self.section=='draft_get')!=(self.draft_id is not None)
                or self.draft_version is not None and self.section!='draft_get'
                or self.after_draft is not None and self.section!='draft_list'
                or self.revision is not None and self.section!='case'
                or self.candidate_source_id is not None and self.section!='edit_context'
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


def validate_case_record(request,record):
    """Reject extra private payload paths before inspecting any dependencies."""
    common={'case_id','case_revision','acl_revision','event_seq','state','current_conclusion_id',
        'source_dependencies','case_role','section'}
    additions={
        'case':{'question','context','classification','rationale','positions','current_conclusion','new_material_since_conclusion','has_more'},
        'edit_context':{'candidate_dependency'},
        'draft_get':{'draft_id','version','current_version','visibility','payload','has_more','publication_ids'},
        'draft_list':{'items','has_more','next_after_id'},
        'evidence':{'items','has_more','next_after_seq'},'contributions':{'items','has_more','next_after_seq'},
        'conclusions':{'items','has_more','next_after_seq'},'history':{'items','has_more','next_after_seq'},
    }
    if request.section=='search':
        if (type(record) is not dict or set(record)!={'section','items','has_more','next_after_case'}
                or record['section']!='search' or type(record['has_more']) is not bool
                or type(record['items']) is not list or len(record['items'])>request.limit
                or (record['next_after_case'] is not None and
                    record['next_after_case']!=(record['items'][-1]['case_id'] if record['items'] else None))):
            raise ValueError('bounded Case subject search record required')
        previous=request.after_case_id or ''
        for item in record['items']:
            if (type(item) is not dict or set(item)!={'case_id','question','context','state','case_revision','acl_revision','event_seq'}
                    or type(item['case_id']) is not str or item['case_id']<=previous
                    or type(item['question']) is not str or not 1<=len(item['question'])<=2000
                    or type(item['context']) is not str or len(item['context'])>1000
                    or item['state'] not in ('open','paused','archived')
                    or any(type(item[k]) is not int or item[k]<1 for k in ('case_revision','acl_revision'))
                    or type(item['event_seq']) is not int or item['event_seq']<0):
                raise ValueError('invalid Case subject candidate')
            exact_uuid(item['case_id'])
            previous=item['case_id']
        if record['has_more'] and (not record['items'] or record['next_after_case']!=record['items'][-1]['case_id']):
            raise ValueError('Case search continuation required')
        return record
    if request.section=='operation':
        allowed={'case_id','source_dependencies','result','operation_key','sequence','command_sha256','request_id','state','outcome_durability'}
    else:allowed=common|additions[request.section]
    if type(record) is not dict or set(record)-allowed:raise ValueError('unexpected Case record fields')
    from .disagreement_contracts import EvidenceAdd,ContributionAdd,ConclusionAdd,PersonalDraft,CaseGrant
    def exact_model(model,value):
        parsed=model.model_validate_json(json.dumps(value,ensure_ascii=False,allow_nan=False),strict=True)
        if parsed.model_dump(mode='json')!=value:raise ValueError('exact Case payload required')
    if request.section=='case' and record.get('current_conclusion') is not None:
        conclusion=record['current_conclusion']
        extra={'human_support','conclusion_id','ordinal','actor_id','event_seq'}
        if (type(conclusion) is not dict or set(conclusion)!=set(ConclusionAdd.model_fields)|extra
                or conclusion['human_support']!=[]):raise ValueError('exact current conclusion required')
        exact_model(ConclusionAdd,{k:v for k,v in conclusion.items() if k not in extra})
    if request.section=='draft_get':
        if record.get('visibility')!='personal':raise ValueError('private draft visibility differs')
        exact_model(PersonalDraft,record['payload'])
    if request.section in ('evidence','contributions','conclusions','history','draft_list'):
        items=record.get('items')
        if type(items) is not list or len(items)>request.limit:raise ValueError('bounded whole Case records required')
        seen=set()
        shapes={
            'evidence':{'evidence_id','version','actor_id','event_seq','payload','logical_time'},
            'contributions':{'contribution_id','version','actor_id','recorded_by','event_seq','payload','logical_time'},
            'conclusions':{'conclusion_id','ordinal','actor_id','based_on_seq','event_seq','payload','logical_time'},
            'history':{'seq','actor_id','kind','object_id','version','payload','logical_time'},
            'draft_list':{'draft_id','version','visibility'},
        }
        for item in items:
            if type(item) is not dict or set(item)!=shapes[request.section]:raise ValueError('exact Case item fields required')
            identity=(item.get('draft_id') or item.get('evidence_id') or item.get('contribution_id') or item.get('conclusion_id') or item.get('seq'),item.get('version'))
            if identity in seen:raise ValueError('duplicate Case item')
            seen.add(identity)
            if request.section=='draft_list':
                if item['visibility']!='personal':raise ValueError('private draft visibility differs')
                continue
            payload=item['payload']
            if type(payload) is not dict:raise ValueError('exact Case item payload required')
            if request.section=='evidence':
                fields=set(EvidenceAdd.model_fields)
                if set(payload)!=fields|{'text_end','verification','visual_confirmation','physical_page_index','page_provenance','region_provenance'}:
                    raise ValueError('unexpected evidence payload fields')
                exact_model(EvidenceAdd,{k:payload[k] for k in fields})
                from .source_manifest import PAGE_KEYS,REGION_KEYS
                if (set(payload['page_provenance'])!=PAGE_KEYS or set(payload['region_provenance'])!=REGION_KEYS
                        or payload['page_provenance']['page_id']!=payload['page_id']
                        or payload['region_provenance']['region_id']!=payload['region_id']
                        or payload['region_provenance']['page_id']!=payload['page_id']
                        or payload['physical_page_index']!=payload['page_provenance']['physical_page_index']
                        or payload['text_end']!=payload['text_start']+len(payload['quote'])
                        or payload['verification']!='accepted_text_match' or payload['visual_confirmation'] is not None):
                    raise ValueError('exact native evidence provenance required')
            elif request.section=='contributions':
                fields=set(ContributionAdd.model_fields)
                if set(payload)-fields-{'withdrawn','reason','attribution','human_reading_confirmed','origin'}:
                    raise ValueError('unexpected contribution payload fields')
                exact_model(ContributionAdd,{k:payload[k] for k in fields})
                if (payload.get('human_reading_confirmed') is not False or type(payload.get('withdrawn')) is not bool
                        or type(payload.get('attribution')) is not dict
                        or set(payload['attribution'])!={'person_id','recorded_by','route','link_id','link_revision','human_reading_confirmed'}
                        or payload['attribution']['person_id']!=item['actor_id']
                        or payload['attribution']['recorded_by']!=item['recorded_by']
                        or payload['attribution']['human_reading_confirmed'] is not False):
                    raise ValueError('authenticated contribution provenance required')
            elif request.section=='conclusions':
                if payload.get('human_support')!=[]:raise ValueError('human support cannot be invented')
                exact_model(ConclusionAdd,{k:v for k,v in payload.items() if k!='human_support'})
            elif item['kind']=='grant':exact_model(CaseGrant,payload)
            elif (item['kind'] not in ('create','revise','state','add_evidence','contribute','correct_contribution','conclude','draft_publish')
                    or set(payload)!={'object_id','version'}):
                raise ValueError('shared history cannot contain private draft data')
    return record
