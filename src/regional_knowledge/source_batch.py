"""Internal bounded packet publication; the public API remains book_ingest."""
import json

from .kaggle_apply import DependencySpec, HandlerSpec, PgFunction
from .recovery_journal import ExactCommand
from .source_activation import ACTIVATION_REVISIONS
from .source_stage import canonical, _hash, _integer, _keys, _uuid

ROUTE = 'source.batch.v1'
CODEC = 'rkb.source-batch.v1'
FINISH_ROUTE = 'source.finish.v1'


def validate_command(command):
    if (type(command) is not ExactCommand or command.kind != 'domain_command'
            or command.route not in (ROUTE,FINISH_ROUTE) or set(command.ids) != {'source_id'}
            or (command.schema_revision, command.function_revision, command.executor_revision) != ACTIVATION_REVISIONS.wire()
            or not 1 <= len(command.payload) <= 2048):
        raise ValueError('exact internal source packet command required')
    _uuid(command.ids['source_id'])
    doc = json.loads(command.payload)
    if command.route == FINISH_ROUTE:
        _keys(doc,'codec ingestion_id')
        if (canonical(doc)!=command.payload or doc['codec']!='rkb.source-finish.v1'
                or command.expected_revisions!={'source_revision':'1'}):
            raise ValueError('exact source reading completion required')
        _uuid(doc['ingestion_id'])
        return doc
    _keys(doc, 'codec ingestion_id batch_index batch_count last_batch_sha256 materializer_sha256 component')
    if canonical(doc) != command.payload or doc['codec'] != CODEC:
        raise ValueError('canonical source packet payload required')
    _uuid(doc['ingestion_id']); _integer(doc['batch_count'], 1, 2500)
    if doc['component'] not in ('text','semantic'):
        raise ValueError('source packet component required')
    _integer(doc['batch_index'], 0, doc['batch_count']-1)
    # There is no backfill/repair mode: publication follows the current stage.
    if doc['batch_index'] != doc['batch_count']-1:
        raise ValueError('current source packet required')
    _hash(doc['last_batch_sha256']); _hash(doc['materializer_sha256'])
    if command.expected_revisions != {'batch_head': doc['last_batch_sha256']}:
        raise ValueError('exact source packet head required')
    return doc


def resolved_command(*, source_id, payload, revisions=ACTIVATION_REVISIONS, **identity):
    if revisions != ACTIVATION_REVISIONS:
        raise ValueError('installed source packet ABI required')
    finishing=payload.get('codec')=='rkb.source-finish.v1'
    if not finishing:
        payload=dict(payload);payload.setdefault('component','text')
    c = ExactCommand(**identity, route=FINISH_ROUTE if finishing else ROUTE, schema_revision=revisions.schema,
        function_revision=revisions.function, executor_revision=revisions.executor,
        ids={'source_id': source_id}, expected_revisions={'source_revision':'1'} if finishing else {'batch_head': payload['last_batch_sha256']},
        payload=canonical(payload))
    validate_command(c)
    return c


def handler(*, check_sha256, apply_sha256,route=ROUTE):
    finishing=route==FINISH_ROUTE
    suffix='source_finish_v1' if finishing else 'source_batch_v1'
    return HandlerSpec('domain_command', route, ACTIVATION_REVISIONS,
        PgFunction('check_'+suffix, check_sha256), PgFunction('apply_'+suffix, apply_sha256),
        frozenset({'source_revision' if finishing else 'batch_head'}),
        dependency=DependencySpec(route, 'public_source', frozenset({'source_id'}), 'shared', False))
