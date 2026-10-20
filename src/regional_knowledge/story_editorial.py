"""Resolved existing Story editor/access operations for the ordered native9 gate.

This module neither authorizes a caller nor installs schema. Actor IDs are
controller projections bound to the exact accepted command. Personal case drafts
have a different contract and are deliberately absent here.
"""
from __future__ import annotations

import hashlib
import json
import struct
from uuid import UUID,uuid5

from .bge_contract import validate_vector
from .kaggle_apply import DependencySpec,HandlerSpec,PgFunction,Revisions
from .recovery_journal import ExactCommand
from .source_import import _json,_text,_uuid
from .story_contracts import UpsertVariant

EDITORIAL_REVISIONS=Revisions('rkb-v4-sql001-009-v1','rkb-v4-source-story-editorial-v4','rkb-python-apply-v1')
STORY_EDIT='story.edit.v1'
STORY_ACCESS='story.access.v1'
STORY_EDIT_DEPENDENCY=DependencySpec(STORY_EDIT,'public_source',frozenset({'source_id','story_id'}),'shared',False)
STORY_ACCESS_DEPENDENCY=DependencySpec(STORY_ACCESS,'public_source',frozenset({'source_id','story_id'}),'shared',False)
EDIT_PRECONDITIONS=frozenset({'story_revision','variant_revision','source_revision','eligibility_revision'})
ACCESS_PRECONDITIONS=frozenset({'story_revision','source_revision','eligibility_revision'})
ROLES=frozenset({'viewer','contributor','researcher','editor','publisher','manager','revoke'})
MAX_EDITORIAL_BYTES=96*1024


def _integer(value,*,minimum=1):
    if type(value) is not int or not minimum<=value<2**63:raise ValueError('exact bounded revision required')
    return value


def _expected(command,keys):
    if set(command.expected_revisions)!=keys:raise ValueError('complete editorial preconditions required')
    for key,value in command.expected_revisions.items():
        if (type(value) is not str or not value.isascii() or not value.isdecimal()
                or str(int(value))!=value or not (0 if key=='variant_revision' else 1)<=int(value)<2**63):
            raise ValueError('canonical editorial revisions required')


def _base(command,route,keys):
    if (type(command) is not ExactCommand or command.kind!='domain_command' or command.route!=route
            or (command.schema_revision,command.function_revision,command.executor_revision)!=EDITORIAL_REVISIONS.wire()
            or set(command.ids)!={'source_id','story_id','actor_id'} or command.ids['actor_id']!=command.person_id):
        raise ValueError('exact actor-bound native editorial command required')
    for value in command.ids.values():_uuid(value)
    _expected(command,keys)
    if type(command.payload) is not bytes or not 1<=len(command.payload)<=MAX_EDITORIAL_BYTES:
        raise ValueError('bounded exact editorial payload required')
    doc=json.loads(command.payload)
    if _json(doc)!=command.payload:raise ValueError('canonical exact editorial bytes required')
    return doc


def validate_story_edit_command(command):
    doc=_base(command,STORY_EDIT,EDIT_PRECONDITIONS)
    if type(doc) is not dict or set(doc)!={'codec','operation','resolved_variant_id','embedding','input_tokens','reason'} or doc['codec']!='rkb.story-edit.v1':
        raise ValueError('exact saved Variant input required')
    op=UpsertVariant.model_validate(doc['operation'])
    if op.model_dump(mode='json')!=doc['operation']:raise ValueError('detached typed Variant operation required')
    ident=_uuid(doc['resolved_variant_id'])
    expected=int(command.expected_revisions['variant_revision'])
    if op.variant_id is not None:
        if _uuid(op.variant_id)!=ident or op.expected_variant_revision!=expected:raise ValueError('exact existing Variant revision required')
    elif expected!=0:raise ValueError('new Variant revision starts at zero')
    _text(op.body,4000);_text(op.audience,500);_text(op.language,24)
    if len(op.language)<2:raise ValueError('exact Variant language required')
    for block in op.blocks:
        if block.assertion_id is None:
            if block.assertion_revision is not None:raise ValueError('assertion revision requires exact assertion ID')
        else:
            _uuid(block.assertion_id)
            if block.assertion_revision is None:raise ValueError('exact assertion revision required')
    vector=doc['embedding']
    if type(vector) is not list or any(type(v) not in (float,int) for v in vector):raise ValueError('exact FP32 vector required')
    validate_vector(vector)
    if any(float(v)!=struct.unpack('<f',struct.pack('<f',float(v)))[0] for v in vector):
        raise ValueError('resolved exact FP32 baseline required')
    _integer(doc['input_tokens'])
    if doc['input_tokens']>512:raise ValueError('bounded complete narrative encoding required')
    if doc['reason'] is not None:_text(doc['reason'],3000)
    return doc


def validate_story_access_command(command):
    doc=_base(command,STORY_ACCESS,ACCESS_PRECONDITIONS)
    if (type(doc) is not dict or set(doc)!={'codec','grantee_user_id','capability'}
            or doc['codec']!='rkb.story-access.v1' or doc['capability'] not in ROLES):
        raise ValueError('exact existing Story grant operation required')
    _uuid(doc['grantee_user_id'])
    return doc


def _command(*,route,sequence,operation_key,request_id,person_id,logical_time,source_id,story_id,expected,payload):
    command=ExactCommand(sequence=sequence,operation_key=operation_key,request_id=request_id,person_id=person_id,
        route=route,logical_time=logical_time,schema_revision=EDITORIAL_REVISIONS.schema,
        function_revision=EDITORIAL_REVISIONS.function,executor_revision=EDITORIAL_REVISIONS.executor,
        ids={'source_id':source_id,'story_id':story_id,'actor_id':person_id},expected_revisions=expected,payload=_json(payload))
    (validate_story_edit_command if route==STORY_EDIT else validate_story_access_command)(command)
    return command


def resolved_story_edit_command(*,sequence,operation_key,request_id,person_id,logical_time,source_id,story_id,
        expected_revision,source_revision,eligibility_revision,operation,embedding,input_tokens,reason=None):
    if type(operation) is not UpsertVariant:raise TypeError('existing typed UpsertVariant required')
    ident=operation.variant_id or str(uuid5(UUID(story_id),person_id+':variant:'+operation_key))
    return _command(route=STORY_EDIT,sequence=sequence,operation_key=operation_key,request_id=request_id,
        person_id=person_id,logical_time=logical_time,source_id=source_id,story_id=story_id,
        expected={k:str(_integer(v,minimum=0 if k=='variant_revision' else 1)) for k,v in dict(
            story_revision=expected_revision,variant_revision=operation.expected_variant_revision if operation.variant_id else 0,
            source_revision=source_revision,eligibility_revision=eligibility_revision).items()},
        payload=dict(codec='rkb.story-edit.v1',operation=operation.model_dump(mode='json'),resolved_variant_id=ident,
            embedding=embedding,input_tokens=input_tokens,reason=reason))


def resolved_story_access_command(*,sequence,operation_key,request_id,person_id,logical_time,source_id,story_id,
        expected_revision,source_revision,eligibility_revision,grantee_user_id,capability):
    return _command(route=STORY_ACCESS,sequence=sequence,operation_key=operation_key,request_id=request_id,
        person_id=person_id,logical_time=logical_time,source_id=source_id,story_id=story_id,
        expected={k:str(_integer(v)) for k,v in dict(story_revision=expected_revision,
            source_revision=source_revision,eligibility_revision=eligibility_revision).items()},
        payload=dict(codec='rkb.story-access.v1',grantee_user_id=grantee_user_id,capability=capability))


def editorial_handler(*,route,check_sha256,apply_sha256):
    if route not in (STORY_EDIT,STORY_ACCESS):raise ValueError('fixed editorial route required')
    edit=route==STORY_EDIT
    stem='story_edit_v1' if edit else 'story_access_v1'
    return HandlerSpec('domain_command',route,EDITORIAL_REVISIONS,PgFunction('check_'+stem,check_sha256),
        PgFunction('apply_'+stem,apply_sha256),EDIT_PRECONDITIONS if edit else ACCESS_PRECONDITIONS,
        dependency=STORY_EDIT_DEPENDENCY if edit else STORY_ACCESS_DEPENDENCY)
