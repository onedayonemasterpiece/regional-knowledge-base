"""Bounded source-bound shared Story revisions; personal drafts are Case-only."""
import hashlib
import json
import re
from typing import Annotated,Literal
from uuid import UUID
from pydantic import Field,model_validator
from .kaggle_protocol import ReadIdentity,TrustedActorContext,Identifier,Nonnegative,PassageReadResult
from .story_contracts import UpsertVariant

MAX_EDITORIAL_RECORD_BYTES=196608
ROLES={'viewer','contributor','researcher','editor','publisher','manager'}


def _uuid(value):
    if type(value) is not str or str(UUID(value))!=value:raise ValueError('canonical editorial UUID required')


def _revision(value):
    if type(value) is not int or not 1<=value<2**63:raise ValueError('exact editorial revision required')


class EditorialReadRequest(ReadIdentity):
    type:Literal['EDITORIAL_READ']='EDITORIAL_READ'
    protocol_version:Literal[2]=2
    operation:Literal['story_editorial_read']='story_editorial_read'
    operation_version:Literal[1]=1
    story_id:Identifier
    revision:Annotated[int,Field(ge=1)]|None=None
    view:Literal['get','history','variants','edit_context']='get'
    after_revision:Nonnegative=0
    after_variant_id:Identifier|None=None
    limit:Annotated[int,Field(ge=1,le=10)]=10
    actor:TrustedActorContext
    min_data_seq:Nonnegative
    deadline_ms:Annotated[int,Field(gt=0)]
    compute_claim:Identifier|None=None

    @model_validator(mode='after')
    def exact_identifiers(self):
        _uuid(self.story_id);_uuid(self.actor.person_id)
        if self.after_variant_id is not None:_uuid(self.after_variant_id)
        if self.view!='variants' and self.after_variant_id is not None:raise ValueError('variant cursor belongs to variants')
        return self


def _variant(value,findings):
    if type(value) is not dict:raise ValueError('exact saved Variant required')
    extra={'body_sha256'} if 'body_sha256' in value else set()
    source={'source_blocks'} if 'source_blocks' in value else set()
    keys={'variant_id','revision','audience','format','language','body','blocks','attributions','media_refs',
          'channel','state','approved_revision','approval_fingerprint'}|extra|source
    if set(value)!=keys or value['state']!='drafting' or value['approved_revision'] is not None or value['approval_fingerprint'] is not None:
        raise ValueError('exact unapproved saved Variant required')
    _uuid(value['variant_id']);_revision(value['revision'])
    op={k:v for k,v in value.items() if k not in {'revision','state','approved_revision','approval_fingerprint','source_blocks','body_sha256'}}
    op.update(op='upsert_variant',expected_variant_revision=value['revision'])
    parsed=UpsertVariant.model_validate_json(json.dumps(op,ensure_ascii=False,allow_nan=False),strict=True)
    if parsed.model_dump(mode='json')!=op:raise ValueError('exact complete Variant metadata required')
    for block in parsed.blocks:
        if block.assertion_id is not None:
            _uuid(block.assertion_id)
            if block.assertion_id not in findings or block.assertion_revision!=1:raise ValueError('exact source assertion required')
        elif block.assertion_revision is not None:raise ValueError('stray assertion revision')
    if extra and value['body_sha256']!=hashlib.sha256(value['body'].encode()).hexdigest():raise ValueError('exact saved body hash required')
    if source:
        end=0
        if type(value['source_blocks']) is not list or not 1<=len(value['source_blocks'])<=32:raise ValueError('bounded source blocks required')
        for block in value['source_blocks']:
            if (type(block) is not dict or set(block)!={'start','end','assertion_ids'} or
                type(block['start']) is not int or type(block['end']) is not int or
                not end<=block['start']<block['end']<=len(value['body']) or value['body'][end:block['start']].strip() or
                type(block['assertion_ids']) is not list or not block['assertion_ids'] or
                not set(block['assertion_ids'])<=set(findings)):
                raise ValueError('exact original body-to-finding map required')
            end=block['end']
        if value['body'][end:].strip():raise ValueError('complete original body map required')


class EditorialReadResult(PassageReadResult):
    type:Literal['EDITORIAL_READ_RESULT']='EDITORIAL_READ_RESULT'
    record:dict|None=None

    @model_validator(mode='after')
    def exact_editorial(self):
        if self.status!='ok':
            if self.record is not None:raise ValueError('failed editorial read has no record')
            return self
        r=self.record
        keys={'story_id','revision','current_revision','title','summary','material_type','body','body_sha256','variant',
              'default_variant_ref','state','narrative_availability','visibility','publication_approval','bibliography',
              'source','finding_ids','view','entries','next_revision','next_variant_id'}
        if type(r) is not dict or r.get('view') not in ('get','history','variants','edit_context'):
            raise ValueError('exact shared Story record required')
        if r['view']=='edit_context':keys|={'role'}
        if set(r)!=keys or len(json.dumps(r,ensure_ascii=False,allow_nan=False).encode())>MAX_EDITORIAL_RECORD_BYTES:
            raise ValueError('bounded exact editorial record required')
        _uuid(r['story_id']);_revision(r['revision']);_revision(r['current_revision'])
        ref=r['default_variant_ref']
        if type(ref) is not dict or set(ref)!={'variant_id','revision'}:raise ValueError('exact default Variant ref required')
        _uuid(ref['variant_id']);_revision(ref['revision'])
        if (r['revision']>r['current_revision'] or r['state']!='drafting' or r['visibility']!='shared'
                or r['publication_approval'] is not None or r['narrative_availability']!='available'
                or type(r['body']) is not str or not 1<=len(r['body'])<=4000
                or r['body_sha256']!=hashlib.sha256(r['body'].encode()).hexdigest()
                or type(r['finding_ids']) is not list or not 1<=len(r['finding_ids'])<=12
                or len(set(r['finding_ids']))!=len(r['finding_ids'])
                or type(r['title']) is not str or not 1<=len(r['title'])<=250
                or type(r['summary']) is not str or len(r['summary'])>1000
                or type(r['bibliography']) is not str or len(r['bibliography'])>16000
                or type(r['entries']) is not list or len(r['entries'])>10):raise ValueError('complete exact editorial narrative required')
        for value in r['finding_ids']:_uuid(value)
        item=self.evidence[0]
        if (item.id!=r['story_id'] or item.kind!='story' or item.text!=r['body'] or item.text_form!='complete'
                or item.continuation is not None or item.exact_variant_ref!=f"variant:{ref['variant_id']}:r{ref['revision']}"
                or item.dependencies.variant_refs!=(item.exact_variant_ref,) or item.dependencies.grant_refs
                or len(item.dependencies.source_revisions)!=1 or item.dependencies.source_revisions[0].model_dump()!=r['source']):
            raise ValueError('exact source/default Variant dependencies required')
        if r['view']=='variants':
            if r['variant'] is not None:raise ValueError('variants page does not duplicate the default metadata')
            for entry in r['entries']:_variant(entry,r['finding_ids'])
            if [x['variant_id'] for x in r['entries']]!=sorted({x['variant_id'] for x in r['entries']}):raise ValueError('unique ordered Variant page required')
        else:
            _variant(r['variant'],r['finding_ids'])
            if (r['variant']['variant_id']!=ref['variant_id'] or r['variant']['revision']!=ref['revision'] or r['variant']['body']!=r['body']):raise ValueError('default Variant body differs')
            if r['view']!='history' and r['entries']:raise ValueError('get/context has no version page')
        if r['view']=='history':
            previous=0
            for entry in r['entries']:
                if (type(entry) is not dict or set(entry)!={'revision','action','logical_time','actor_id','request_id','operation_key','sequence','command_sha256'}
                        or type(entry['revision']) is not int or not previous<entry['revision']<=r['current_revision']
                        or type(entry['sequence']) is not int or entry['sequence']<1
                        or entry['action'] not in ('source_import','upsert_variant','access_change')
                        or any(type(entry[k]) is not str or not 1<=len(entry[k])<=256 for k in ('logical_time','actor_id','request_id','operation_key'))
                        or re.fullmatch('[0-9a-f]{64}',entry['command_sha256']) is None):raise ValueError('exact immutable editorial history required')
                _uuid(entry['actor_id']);previous=entry['revision']
        if r['next_revision'] is not None:_revision(r['next_revision'])
        if r['next_variant_id'] is not None:_uuid(r['next_variant_id'])
        if r['view']=='edit_context' and r['role'] is not None and r['role'] not in ROLES:raise ValueError('fixed current Story role required')
        return self
