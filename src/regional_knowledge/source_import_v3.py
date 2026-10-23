"""Explicit fresh8 source facts semantics; exact v1/v2 replay stays unchanged."""
from .kaggle_apply import Revisions, HandlerSpec, PgFunction, DependencySpec
from .recovery_journal import ExactCommand
from .source_import import IMPORT_PRECONDITIONS, _uuid, _json
from .source_import_v2 import validate_import_v2_payload

SOURCE_IMPORT_V3 = 'source.import.v3'
FACT_REVISIONS = Revisions('rkb-v4-sql001-008-v1', 'rkb-v4-source-story-facts-v3', 'rkb-python-apply-v1')
IMPORT_V3_DEPENDENCY = DependencySpec(SOURCE_IMPORT_V3, 'public_source', frozenset({'source_id'}), 'shared', False)


def validate_import_v3_payload(payload):
    # Payload encoding is deliberately the existing complete source/story codec.
    # Only the resolved command and its exact pinned handler advance semantics.
    doc = validate_import_v2_payload(payload)
    for finding in doc['semantic']['findings']:
        for item in finding['event_dates'] + finding['observations']:
            indexes = item['evidence_indexes']
            if len(set(indexes)) != len(indexes):
                raise ValueError('unique exact fact evidence indexes required')
            proofs = [finding['evidence'][index]['original_excerpt'] for index in indexes]
            literal = item.get('original_date_text', item.get('original_value_text'))
            if not any(literal in proof for proof in proofs):
                raise ValueError('exact original fact wording required in selected source evidence')
            if 'period_text' in item and item['period_text'] != 'unknown' and not any(
                    item['period_text'] in proof for proof in proofs):
                raise ValueError('exact original observation period required in selected source evidence')
    return doc


def validate_import_v3_command(command):
    if (not isinstance(command, ExactCommand) or command.kind != 'domain_command' or
            command.route != SOURCE_IMPORT_V3 or set(command.ids) != {'source_id'} or
            command.expected_revisions != {'absent_source': 'true'} or
            (command.schema_revision, command.function_revision, command.executor_revision) != FACT_REVISIONS.wire()):
        raise ValueError('exact fresh8 source facts command required')
    _uuid(command.ids['source_id'])
    return validate_import_v3_payload(command.payload)


def resolved_import_v3_command(*, sequence, operation_key, request_id, person_id, logical_time,
                               revisions, source_id, payload):
    if type(revisions) is not Revisions or revisions != FACT_REVISIONS:
        raise ValueError('ordered source facts revision barrier required')
    command = ExactCommand(sequence=sequence, operation_key=operation_key, request_id=request_id,
        person_id=person_id, route=SOURCE_IMPORT_V3, logical_time=logical_time,
        schema_revision=revisions.schema, function_revision=revisions.function,
        executor_revision=revisions.executor, ids={'source_id': source_id},
        expected_revisions={'absent_source': 'true'}, payload=_json(payload))
    validate_import_v3_command(command)
    return command


def source_import_v3_handler(*, revisions, check_sha256, apply_sha256):
    if revisions != FACT_REVISIONS:
        raise ValueError('ordered source facts revision barrier required')
    return HandlerSpec('domain_command', SOURCE_IMPORT_V3, revisions,
        PgFunction('check_source_import_v3', check_sha256), PgFunction('apply_source_import_v3', apply_sha256),
        IMPORT_PRECONDITIONS, dependency=IMPORT_V3_DEPENDENCY)
