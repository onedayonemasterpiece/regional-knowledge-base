"""Canonical codec/shape, actor identity and accepted command for catalog corrections.

The Kaggle native route delegates validation here; SQL034 checks only source integrity.
Accepted stage bytes and the owner-authorized intake path remain unchanged.
"""
import json
from uuid import UUID

from .contracts import CatalogUpdateInput
from .kaggle_apply import DependencySpec, HandlerSpec, PgFunction
from .recovery_journal import ExactCommand
from .source_activation import ACTIVATION_REVISIONS
from .source_stage import canonical

ROUTE = 'source.catalog.v1'
CODEC = 'rkb.source-catalog.v1'
DEPENDENCY = DependencySpec(ROUTE, 'public_source', frozenset({'source_id'}), 'shared', False)


def _uuid(value):
    if type(value) is not str or str(UUID(value)) != value:
        raise ValueError('canonical source catalog identity required')
    return value


def validate_catalog_payload(payload):
    if type(payload) is not bytes or not 1 <= len(payload) <= 16384:
        raise ValueError('bounded catalog correction required')
    value = json.loads(payload)
    if canonical(value) != payload or type(value) is not dict or set(value) != {
        'codec', 'ingestion_id', 'owner_person_id', 'catalog_update'
    } or value['codec'] != CODEC:
        raise ValueError('exact catalog correction envelope required')
    _uuid(value['ingestion_id'])
    _uuid(value['owner_person_id'])
    if type(value['catalog_update']) is not dict:
        raise ValueError('catalog correction object required')
    patch = CatalogUpdateInput.model_validate(value['catalog_update'])
    if patch.expected_revision != 1 or patch.model_dump(mode='json', exclude_unset=True) != value['catalog_update']:
        raise ValueError('catalog_update_expected_revision_1_required')
    if patch.catalog is not None and 'kind' in patch.catalog.model_fields_set:
        raise ValueError('catalog_update_catalog_kind_requires_registration')
    return value


def validate_catalog_command(command):
    if (type(command) is not ExactCommand or command.kind != 'domain_command'
            or command.route != ROUTE or set(command.ids) != {'source_id'}
            or command.expected_revisions != {'source_revision': '1'}
            or (command.schema_revision, command.function_revision,
                command.executor_revision) != ACTIVATION_REVISIONS.wire()):
        raise ValueError('exact accepted source catalog command required')
    _uuid(command.ids['source_id'])
    value = validate_catalog_payload(command.payload)
    if value['owner_person_id'] != command.person_id:
        raise ValueError('catalog correction actor differs from accepted command')
    return value


def resolved_catalog_command(*, sequence, operation_key, request_id, person_id,
                             logical_time, source_id, payload, revisions=ACTIVATION_REVISIONS):
    if revisions != ACTIVATION_REVISIONS:
        raise ValueError('catalog requires current native source profile')
    command = ExactCommand(sequence=sequence, operation_key=operation_key,
        request_id=request_id, person_id=person_id, logical_time=logical_time,
        route=ROUTE, schema_revision=revisions.schema,
        function_revision=revisions.function, executor_revision=revisions.executor,
        ids={'source_id': source_id}, expected_revisions={'source_revision': '1'},
        payload=canonical(payload))
    validate_catalog_command(command)
    return command


def catalog_handler(*, check_sha256, apply_sha256):
    return HandlerSpec('domain_command', ROUTE, ACTIVATION_REVISIONS,
        PgFunction('check_source_catalog_v1', check_sha256),
        PgFunction('apply_source_catalog_v1', apply_sha256),
        frozenset({'source_revision'}), dependency=DEPENDENCY)
