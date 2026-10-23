"""Explicit fresh9 import binding; v1/v2/v3 accepted semantics stay unchanged."""
from .kaggle_apply import HandlerSpec,PgFunction,DependencySpec
from .recovery_journal import ExactCommand
import json
from .source_import import _uuid,_json,IMPORT_PRECONDITIONS,MAX_IMPORT_BYTES
from .source_import_v3 import validate_import_v3_payload
from .story_editorial import EDITORIAL_REVISIONS
from .source_manifest import validate_source_manifest

SOURCE_IMPORT_V4='source.import.v4'
IMPORT_V4_DEPENDENCY=DependencySpec(SOURCE_IMPORT_V4,'public_source',frozenset({'source_id'}),'shared',False)


def validate_import_v4_command(command):
    if (type(command) is not ExactCommand or command.kind!='domain_command' or command.route!=SOURCE_IMPORT_V4
            or set(command.ids)!={'source_id'} or command.expected_revisions!={'absent_source':'true'}
            or (command.schema_revision,command.function_revision,command.executor_revision)!=EDITORIAL_REVISIONS.wire()):
        raise ValueError('exact fresh9 source import required')
    _uuid(command.ids['source_id'])
    return validate_import_v4_payload(command.payload,source_id=command.ids['source_id'])


def validate_import_v4_payload(payload,*,source_id):
    _uuid(source_id)
    if type(payload) is not bytes or not 1<=len(payload)<=MAX_IMPORT_BYTES:
        raise ValueError('bounded exact source import v4 required')
    doc=json.loads(payload)
    if _json(doc)!=payload or doc.get('codec')!='rkb.source-import.v4' or 'source_manifest' not in doc:
        raise ValueError('canonical source import v4 with manifest required')
    base={k:v for k,v in doc.items() if k!='source_manifest'}
    base['codec']='rkb.source-import.v2'
    validate_import_v3_payload(_json(base))
    validate_source_manifest(doc['source_manifest'],source_id=source_id,
        original=doc['original'],semantic=doc['semantic'],passages=doc['passages'])
    return doc


def resolved_import_v4_command(*,sequence,operation_key,request_id,person_id,logical_time,source_id,payload,
                              revisions=EDITORIAL_REVISIONS):
    if revisions!=EDITORIAL_REVISIONS:
        raise ValueError('ordered fresh9 import revisions required')
    c=ExactCommand(sequence=sequence,operation_key=operation_key,request_id=request_id,person_id=person_id,
        logical_time=logical_time,route=SOURCE_IMPORT_V4,schema_revision=EDITORIAL_REVISIONS.schema,
        function_revision=EDITORIAL_REVISIONS.function,executor_revision=EDITORIAL_REVISIONS.executor,
        ids={'source_id':source_id},expected_revisions={'absent_source':'true'},payload=_json(payload))
    validate_import_v4_command(c)
    return c


def source_import_v4_handler(*,check_sha256,apply_sha256):
    return HandlerSpec('domain_command',SOURCE_IMPORT_V4,EDITORIAL_REVISIONS,
        PgFunction('check_source_import_v4',check_sha256),PgFunction('apply_source_import_v4',apply_sha256),
        IMPORT_PRECONDITIONS,dependency=IMPORT_V4_DEPENDENCY)
