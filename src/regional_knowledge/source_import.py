"""Bounded resolved source activation; no acquisition, inference or new authority.

Only already verified original receipts and exact accepted model/vector results
enter this command. Large multipart staging remains a separate import workflow.
"""
from __future__ import annotations

import json
import re
import struct
from uuid import UUID

from .bge_contract import DIMENSION, MAX_TOKENS, REVISION, SPACE, TOKENIZER_SHA256, validate_vector
from .kaggle_apply import DependencySpec, HandlerSpec, PgFunction, Revisions
from .recovery_journal import ExactCommand

SOURCE_IMPORT = 'source.import.v1'
IMPORT_PRECONDITIONS = frozenset({'absent_source'})
IMPORT_DEPENDENCY = DependencySpec(SOURCE_IMPORT,'public_source',frozenset({'source_id'}),'shared',False)
MAX_IMPORT_BYTES = 768 * 1024
MAX_PASSAGES = 100


def _json(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()


def _uuid(value):
    if not isinstance(value,str) or str(UUID(value)) != value:
        raise ValueError('canonical source import UUID required')
    return value


def _text(value,maximum,*,empty=False):
    if (not isinstance(value,str) or not empty and not value or '\x00' in value
            or len(value)>maximum or len(value.encode())>maximum*4):
        raise ValueError('bounded source import text required')
    return value


def _hash(value):
    if not isinstance(value,str) or re.fullmatch('[0-9a-f]{64}',value) is None:
        raise ValueError('exact source import hash required')
    return value


def validate_import_payload(payload):
    if not isinstance(payload,bytes) or not 1<=len(payload)<=MAX_IMPORT_BYTES:
        raise ValueError('bounded exact source import bytes required')
    try:
        value=json.loads(payload)
        if _json(value)!=payload:
            raise ValueError('canonical source import bytes required')
        if not isinstance(value,dict) or set(value)!={'codec','bibliography','revision','work_id','edition_id',
                'original','model','passages','research'}:
            raise ValueError('complete resolved source import keys required')
        if value['codec']!='rkb.source-import.v1' or type(value['revision']) is not int or value['revision']!=1:
            raise ValueError('new source revision one required')
        _text(value['bibliography'],8000);_uuid(value['work_id']);_uuid(value['edition_id'])
        original=value['original']
        if not isinstance(original,dict) or set(original)!={'sha256','size','ref','receipt_sha256'}:
            raise ValueError('verified durable original binding required')
        _hash(original['sha256']);_hash(original['receipt_sha256']);_text(original['ref'],2048)
        # The locator is a stable provider receipt reference, never a signed URL.
        if (re.fullmatch('[A-Za-z0-9][A-Za-z0-9._:/-]{0,2047}',original['ref']) is None
                or '://' in original['ref'] or type(original['size']) is not int
                or not 1<=original['size']<2**63):
            raise ValueError('stable verified original reference required')
        if _json(value['model']) != _json(dict(revision=REVISION,space=SPACE,tokenizer_sha256=TOKENIZER_SHA256,
                                  dimension=DIMENSION,max_tokens=MAX_TOKENS)):
            raise ValueError('pinned resolved BGE result required')
        passages=value['passages']
        if not isinstance(passages,list) or not 1<=len(passages)<=MAX_PASSAGES:
            raise ValueError('bounded resolved source passages required')
        ids=set()
        for ordinal,passage in enumerate(passages):
            if not isinstance(passage,dict) or set(passage)!={'passage_id','ordinal','locator','body','language','embedding','input_tokens'}:
                raise ValueError('complete exact passage result required')
            passage_id=_uuid(passage['passage_id'])
            if passage_id in ids or type(passage['ordinal']) is not int or passage['ordinal']!=ordinal:
                raise ValueError('unique source passage identity/order required')
            ids.add(passage_id);_text(passage['locator'],1000);_text(passage['body'],16000)
            if passage['language'] not in ('ru','de','simple'):
                raise ValueError('finite source language required')
            if type(passage['input_tokens']) is not int or not 1<=passage['input_tokens']<=MAX_TOKENS:
                raise ValueError('exact pinned-tokenizer input bound required')
            vector=passage['embedding']
            if (not isinstance(vector,list) or any(type(v) not in (int,float) for v in vector)):
                raise ValueError('resolved finite FP32 vector required')
            validate_vector(vector)
            if any(struct.unpack('f',struct.pack('f',v))[0] != v for v in vector):
                raise ValueError('exact resolved FP32 values required')
        research=value['research']
        if not isinstance(research,dict) or set(research)!={'frontier_id','state','continuation'}:
            raise ValueError('reachable resolved research frontier required')
        _uuid(research['frontier_id'])
        if research['state'] not in ('pending','complete') or not isinstance(research['continuation'],dict):
            raise ValueError('explicit research state and continuation required')
        if len(_json(research['continuation']))>65536:
            raise ValueError('bounded research continuation required')
        # The research producer resolves semantic work; intake never invents it.
        if research['state']=='pending' and not research['continuation']:
            raise ValueError('pending source research requires reachable work')
        return value
    except (KeyError,TypeError,OverflowError,struct.error,RecursionError,json.JSONDecodeError):
        raise ValueError('invalid bounded resolved source import') from None


def validate_import_command(command):
    if (not isinstance(command,ExactCommand) or command.kind!='domain_command' or command.route!=SOURCE_IMPORT
            or set(command.ids)!={'source_id'} or command.expected_revisions!={'absent_source':'true'}):
        raise ValueError('exact new-source import command required')
    _uuid(command.ids['source_id'])
    return validate_import_payload(command.payload)


def resolved_import_command(*,sequence,operation_key,request_id,person_id,logical_time,revisions,
                            source_id,payload):
    """Create immutable command bytes from exact resolved data, without authorization.

    Current owner authorization and verified original receipt checking belong to
    the canonical import workflow before staging; this builder grants no rights.
    """
    if type(revisions) is not Revisions:
        raise TypeError('exact installed source import revisions required')
    command=ExactCommand(sequence=sequence,operation_key=operation_key,request_id=request_id,
        person_id=person_id,route=SOURCE_IMPORT,logical_time=logical_time,
        schema_revision=revisions.schema,function_revision=revisions.function,executor_revision=revisions.executor,
        ids={'source_id':source_id},expected_revisions={'absent_source':'true'},payload=_json(payload))
    validate_import_command(command)
    return command


def source_import_handler(*,revisions,check_sha256,apply_sha256):
    """Register only the actually installed immutable native definitions."""
    return HandlerSpec('domain_command',SOURCE_IMPORT,revisions,
        PgFunction('check_source_import_v1',check_sha256),PgFunction('apply_source_import_v1',apply_sha256),
        IMPORT_PRECONDITIONS,dependency=IMPORT_DEPENDENCY)
