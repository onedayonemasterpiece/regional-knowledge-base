"""Title-only native SetMetadata intent; historical Story codecs stay frozen."""
import json
from uuid import UUID

from .kaggle_apply import DependencySpec, HandlerSpec, PgFunction
from .recovery_journal import ExactCommand
from .source_activation import ACTIVATION_REVISIONS
from .source_stage import canonical
from .story_contracts import NativeStoryTitle

ROUTE = 'story.title.v1'
CODEC = 'rkb.story-title.v1'
EXPECTED = frozenset({'story_revision', 'source_revision', 'eligibility_revision'})
DEPENDENCY = DependencySpec(ROUTE, 'public_source', frozenset({'source_id', 'story_id'}), 'shared', False)


def validate_title_command(c):
    if (type(c) is not ExactCommand or c.kind != 'domain_command' or c.route != ROUTE
            or (c.schema_revision, c.function_revision, c.executor_revision) != ACTIVATION_REVISIONS.wire()
            or set(c.ids) != {'source_id', 'story_id', 'actor_id'} or c.ids['actor_id'] != c.person_id
            or set(c.expected_revisions) != EXPECTED or not 1 <= len(c.payload) <= 4096):
        raise ValueError('exact native Story title command required')
    for value in (c.person_id, *c.ids.values()):
        if type(value) is not str or str(UUID(value)) != value:
            raise ValueError('canonical Story identity required')
    for value in c.expected_revisions.values():
        if type(value) is not str or not value.isascii() or not value.isdecimal() or str(int(value)) != value or not 1 <= int(value) < 2**63:
            raise ValueError('canonical Story precondition required')
    doc = json.loads(c.payload)
    if (canonical(doc) != c.payload or type(doc) is not dict or set(doc) != {'codec', 'operation', 'reason'}
            or doc['codec'] != CODEC or type(doc['operation']) is not dict):
        raise ValueError('exact title payload required')
    op = NativeStoryTitle.model_validate(doc['operation'], strict=True)
    if op.model_dump(mode='json') != doc['operation']:
        raise ValueError('exact title-only metadata required')
    reason = doc['reason']
    if reason is not None and (type(reason) is not str or len(reason) > 3000):
        raise ValueError('bounded editorial reason required')
    return doc


def resolved_native_story_title_command(*, sequence, operation_key, request_id, person_id, logical_time,
                                        source_id, story_id, expected_revision, source_revision,
                                        eligibility_revision, operation, reason=None):
    if type(operation) is not NativeStoryTitle:
        raise TypeError('title-only SetMetadata required')
    command = ExactCommand(sequence=sequence, operation_key=operation_key, request_id=request_id,
        person_id=person_id, logical_time=logical_time, route=ROUTE,
        schema_revision=ACTIVATION_REVISIONS.schema, function_revision=ACTIVATION_REVISIONS.function,
        executor_revision=ACTIVATION_REVISIONS.executor,
        ids={'source_id': source_id, 'story_id': story_id, 'actor_id': person_id},
        expected_revisions={key: str(value) for key, value in {
            'story_revision': expected_revision, 'source_revision': source_revision,
            'eligibility_revision': eligibility_revision}.items()},
        payload=canonical({'codec': CODEC, 'operation': operation.model_dump(mode='json'), 'reason': reason}))
    validate_title_command(command)
    return command


def title_handler(*, check_sha256, apply_sha256):
    return HandlerSpec('domain_command', ROUTE, ACTIVATION_REVISIONS,
        PgFunction('check_story_title_v1', check_sha256),
        PgFunction('apply_story_title_v1', apply_sha256), EXPECTED, dependency=DEPENDENCY)
