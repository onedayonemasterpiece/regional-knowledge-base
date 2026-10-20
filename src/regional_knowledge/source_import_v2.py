"""Exact resolved source+findings+narrative activation, version two.

No model call, authority, semantic judgement or SQL execution occurs here.
V1 bytes/handlers remain immutable and independently replayable.
"""
from __future__ import annotations

import hashlib
import re
import struct

from .source_import import (_json,_uuid,_text,_hash,validate_import_payload,
    MAX_IMPORT_BYTES,IMPORT_PRECONDITIONS)
from .bge_contract import validate_vector,MAX_TOKENS
from .kaggle_apply import DependencySpec,HandlerSpec,PgFunction,Revisions
from .recovery_journal import ExactCommand
from .story_contracts import StageEventDateInput,StageObservationInput,StoryMetadata

SOURCE_IMPORT_V2='source.import.v2'
IMPORT_V2_DEPENDENCY=DependencySpec(SOURCE_IMPORT_V2,'public_source',frozenset({'source_id'}),'shared',False)
STORY_REVISIONS=Revisions('rkb-v4-sql001-007-v1','rkb-v4-source-story-import-v2','rkb-python-apply-v1')


def _keys(value,keys):
    if type(value) is not dict or set(value)!=set(keys.split()):
        raise ValueError('exact source semantic object required')


def _array(value,maximum,*,minimum=0):
    if type(value) is not list or not minimum<=len(value)<=maximum:
        raise ValueError('bounded source semantic array required')
    return value


def _refs(value,known,maximum=12):
    _array(value,maximum,minimum=1)
    if any(type(ref) is not str or ref not in known for ref in value) or len(set(value))!=len(value):
        raise ValueError('exact unique source semantic references required')


def _optional_text(value,maximum):
    if value is not None:_text(value,maximum)


def validate_import_v2_payload(payload):
    if type(payload) is not bytes or not 1<=len(payload)<=MAX_IMPORT_BYTES:
        raise ValueError('bounded exact source import v2 required')
    import json
    try:
        doc=json.loads(payload)
        if _json(doc)!=payload or type(doc) is not dict or doc.get('codec')!='rkb.source-import.v2':
            raise ValueError('canonical source import v2 required')
        base={k:v for k,v in doc.items() if k!='semantic'}
        base['codec']='rkb.source-import.v1'
        validate_import_payload(_json(base))
        semantic=doc['semantic'];_keys(semantic,'regions findings stories pending')
        regions={};passages={x['passage_id']:x for x in doc['passages']}
        for region in _array(semantic['regions'],800,minimum=1):
            _keys(region,'region_id page_id source_text source_text_sha256')
            ident=_uuid(region['region_id']);_uuid(region['page_id']);_text(region['source_text'],8000,empty=True)
            if ident in regions or hashlib.sha256(region['source_text'].encode()).hexdigest()!=_hash(region['source_text_sha256']):
                raise ValueError('exact unique source region required')
            regions[ident]=region
        findings={};candidate_keys=set()
        for finding in _array(semantic['findings'],100):
            _keys(finding,'assertion_id candidate_key proposition account_kind attributed_to reported_by evidence event_dates observations entity_keys')
            ident=_uuid(finding['assertion_id']);key=finding['candidate_key']
            if type(key) is not str or re.fullmatch('[A-Za-z0-9][A-Za-z0-9._:-]{0,79}',key) is None:
                raise ValueError('exact semantic candidate key required')
            if ident in findings or key in candidate_keys:raise ValueError('duplicate source finding')
            _text(finding['proposition'],500)
            if finding['account_kind'] not in ('legend','tradition','rumor','recollection','testimony','other'):
                raise ValueError('explicit source account kind required')
            _optional_text(finding['attributed_to'],250);_optional_text(finding['reported_by'],250)
            for evidence in _array(finding['evidence'],4,minimum=1):
                _keys(evidence,'region_id passage_id start end original_excerpt')
                region=regions.get(evidence['region_id']);passage=passages.get(evidence['passage_id'])
                begin,end=evidence['start'],evidence['end'];quote=evidence['original_excerpt']
                _text(quote,500)
                if (region is None or passage is None or type(begin) is not int or type(end) is not int
                        or not 0<=begin<end<=len(region['source_text'])
                        or region['source_text'][begin:end]!=quote or quote not in passage['body']):
                    raise ValueError('exact source-bound finding evidence required')
            for date in _array(finding['event_dates'],8):
                parsed=StageEventDateInput.model_validate(date)
                if parsed.model_dump(mode='json')!=date or any(i>=len(finding['evidence']) for i in parsed.evidence_indexes):
                    raise ValueError('exact finding date evidence required')
            for observation in _array(finding['observations'],8):
                parsed=StageObservationInput.model_validate(observation)
                if parsed.model_dump(mode='json')!=observation or any(i>=len(finding['evidence']) for i in parsed.evidence_indexes):
                    raise ValueError('exact finding observation evidence required')
            _array(finding['entity_keys'],32)
            for entity in finding['entity_keys']:_text(entity,100)
            if len(set(finding['entity_keys']))!=len(finding['entity_keys']):raise ValueError('duplicate finding entity')
            findings[ident]=finding;candidate_keys.add(key)
        stories=set();variants=set();covered=set()
        for story in _array(semantic['stories'],12):
            _keys(story,'story_id finding_ids title summary material_type default_variant')
            ident=_uuid(story['story_id']);_refs(story['finding_ids'],findings)
            _text(story['title'],250);_text(story['summary'],1000)
            StoryMetadata(title=story['title'],summary=story['summary'],material_type=story['material_type'])
            variant=story['default_variant']
            _keys(variant,'variant_id revision audience format language body blocks embedding input_tokens')
            variant_id=_uuid(variant['variant_id'])
            if ident in stories or variant_id in variants or type(variant['revision']) is not int or variant['revision']!=1:
                raise ValueError('unique exact story/default variant revision required')
            _text(variant['audience'],500);_text(variant['language'],24);_text(variant['body'],4000)
            if variant['format']!='story' or len(variant['language'])<2:raise ValueError('basic source narrative required')
            end=0
            for block in _array(variant['blocks'],25,minimum=1):
                _keys(block,'start end assertion_ids');_refs(block['assertion_ids'],story['finding_ids'])
                if (type(block['start']) is not int or type(block['end']) is not int or block['start']!=end
                        or not end<block['end']<=len(variant['body'])):
                    raise ValueError('complete exact narrative evidence map required')
                end=block['end']
            if end!=len(variant['body']):raise ValueError('complete narrative body required')
            vector=variant['embedding'];validate_vector(vector)
            if type(vector) is not list or any(type(v) not in (int,float) or struct.unpack('f',struct.pack('f',v))[0]!=v for v in vector):
                raise ValueError('exact narrative FP32 vector required')
            if type(variant['input_tokens']) is not int or not 1<=variant['input_tokens']<=MAX_TOKENS:
                raise ValueError('complete narrative tokenizer input bound required')
            stories.add(ident);variants.add(variant_id);covered.update(story['finding_ids'])
        pending=set()
        for item in _array(semantic['pending'],12):
            _keys(item,'candidate_key finding_ids reason');_refs(item['finding_ids'],findings)
            if (item['candidate_key'] not in candidate_keys or item['candidate_key'] in pending
                    or item['reason']!='needs_context' or covered.intersection(item['finding_ids'])
                    or any(findings[ref]['candidate_key']!=item['candidate_key'] for ref in item['finding_ids'])
                    or doc['research']['state']!='pending' or not doc['research']['continuation']):
                raise ValueError('reachable pending narrative continuation required')
            pending.add(item['candidate_key'])
        return doc
    except (KeyError,TypeError,OverflowError,struct.error,RecursionError,json.JSONDecodeError):
        raise ValueError('invalid resolved source semantic payload') from None


def validate_import_v2_command(command):
    if (not isinstance(command,ExactCommand) or command.kind!='domain_command' or command.route!=SOURCE_IMPORT_V2
            or set(command.ids)!={'source_id'} or command.expected_revisions!={'absent_source':'true'}
            or (command.schema_revision,command.function_revision,command.executor_revision)!=STORY_REVISIONS.wire()):
        raise ValueError('exact new-source v2 command required')
    _uuid(command.ids['source_id'])
    return validate_import_v2_payload(command.payload)


def resolved_import_v2_command(*,sequence,operation_key,request_id,person_id,logical_time,revisions,source_id,payload):
    if type(revisions) is not Revisions or revisions!=STORY_REVISIONS:
        raise ValueError('ordered source-story revision barrier required')
    command=ExactCommand(sequence=sequence,operation_key=operation_key,request_id=request_id,person_id=person_id,
        route=SOURCE_IMPORT_V2,logical_time=logical_time,schema_revision=revisions.schema,
        function_revision=revisions.function,executor_revision=revisions.executor,
        ids={'source_id':source_id},expected_revisions={'absent_source':'true'},payload=_json(payload))
    validate_import_v2_command(command)
    return command


def source_import_v2_handler(*,revisions,check_sha256,apply_sha256):
    if revisions!=STORY_REVISIONS:raise ValueError('ordered source-story revision barrier required')
    return HandlerSpec('domain_command',SOURCE_IMPORT_V2,revisions,
        PgFunction('check_source_import_v2',check_sha256),PgFunction('apply_source_import_v2',apply_sha256),
        IMPORT_PRECONDITIONS,dependency=IMPORT_V2_DEPENDENCY)
