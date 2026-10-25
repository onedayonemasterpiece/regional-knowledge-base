"""Minimal native GEO projection for accepted PG source mentions.

No legacy GEO queue, POI mutation, second ledger, model matching or runtime owner.
New intents use the installed ControlSpool + AcceptedSourceDispatch + applied_ops.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from uuid import UUID

from .geo_migration import exact_uuid, owner_ref
from .kaggle_apply import DependencySpec, HandlerSpec, PgFunction
from .recovery_journal import ExactCommand
from .source_activation import ACTIVATION_REVISIONS

ROUTE = 'geo.resolve.v1'
CODEC = 'rkb.geo-resolve.v1'
SOURCE_REVISION_KEY = 'source_revision'
MAX_SOURCE_REVISION = 9223372036854775807
FIELDS = frozenset({'codec', 'source_id', 'mention_id', 'status', 'poi_ref', 'url', 'checked_at', 'reason'})


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()


def _text(value, limit, *, required=False):
    if value is None and not required:
        return
    if type(value) is not str or (required and not value) or len(value.encode()) > limit or '\x00' in value:
        raise ValueError('exact bounded GEO value required')


def _source_revision(value):
    # The client supplies the current revision observed in the accepted queue.
    # PostgreSQL independently verifies that it still matches active_revision.
    if type(value) is not int or not 1 <= value <= MAX_SOURCE_REVISION:
        raise ValueError('canonical positive source revision required')
    return str(value)


def _expected_revision(expected):
    if set(expected) != {SOURCE_REVISION_KEY}:
        raise ValueError('exact GEO revision precondition required')
    value = expected[SOURCE_REVISION_KEY]
    if (type(value) is not str or not value.isascii() or not value.isdecimal()
            or not 1 <= len(value) <= 19):
        raise ValueError('exact GEO revision precondition required')
    revision = int(value)
    if _source_revision(revision) != value:
        raise ValueError('canonical GEO revision precondition required')
    return revision


def _payload(*, source_id, mention_id, status, poi_ref, url, checked_at, reason):
    exact_uuid(source_id)
    exact_uuid(mention_id)
    if status not in ('candidate', 'unresolved', 'ambiguous') or type(status) is not str:
        raise ValueError('fixed GEO status required')
    if status == 'candidate':
        _text(poi_ref, 256, required=True)
        owner_ref(poi_ref, 'street_story')
    elif poi_ref is not None:
        raise ValueError('unresolved/ambiguous cannot select a POI')
    _text(reason, 1024, required=True)
    if (url is None) != (checked_at is None):
        raise ValueError('web reference and check time must be paired')
    if url is not None:
        _text(url, 2048, required=True)
        _text(checked_at, 64, required=True)
        if not url.startswith('https://') or any(c in url for c in '\r\n\t '):
            raise ValueError('bounded checked HTTPS source required')
        if not checked_at.endswith('Z'):
            raise ValueError('explicit UTC check time required')
        parsed = datetime.fromisoformat(checked_at[:-1] + '+00:00')
        if parsed.tzinfo is None or parsed.utcoffset().total_seconds() != 0:
            raise ValueError('UTC timestamp required')
    return dict(codec=CODEC, source_id=source_id, mention_id=mention_id,
                status=status, poi_ref=poi_ref, url=url, checked_at=checked_at, reason=reason)


def validate_command(command):
    if (type(command) is not ExactCommand or command.kind != 'domain_command'
            or command.route != ROUTE or (command.schema_revision, command.function_revision,
            command.executor_revision) != ACTIVATION_REVISIONS.wire()
            or set(command.ids) != {'source_id', 'mention_id'}
            or not 1 <= len(command.payload) <= 8192):
        raise ValueError('exact installed GEO operation required')
    try:
        _expected_revision(dict(command.expected_revisions))
        payload = json.loads(command.payload)
        if type(payload) is not dict or set(payload) != FIELDS:
            raise ValueError
        checked = _payload(**{key: payload[key] for key in FIELDS if key != 'codec'})
        if (payload != checked or command.payload != canonical(checked)
                or payload['source_id'] != command.ids['source_id']
                or payload['mention_id'] != command.ids['mention_id']):
            raise ValueError
        return checked
    except (ValueError, TypeError, KeyError, UnicodeError):
        raise ValueError('invalid exact GEO accepted bytes') from None


def resolved_command(*, sequence, operation_key, person_id, logical_time,
                     source_id, mention_id, source_revision, status, poi_ref, url, checked_at, reason):
    _text(operation_key, 128, required=True)
    exact_uuid(person_id)
    command = ExactCommand(
        sequence=sequence, operation_key=operation_key,
        request_id='geo-resolve:' + hashlib.sha256(operation_key.encode()).hexdigest()[:40],
        person_id=person_id, route=ROUTE, kind='domain_command',
        logical_time=logical_time, schema_revision=ACTIVATION_REVISIONS.schema,
        function_revision=ACTIVATION_REVISIONS.function,
        executor_revision=ACTIVATION_REVISIONS.executor,
        ids={'source_id': source_id, 'mention_id': mention_id},
        expected_revisions={SOURCE_REVISION_KEY: _source_revision(source_revision)},
        payload=canonical(_payload(source_id=source_id, mention_id=mention_id, status=status,
            poi_ref=poi_ref, url=url, checked_at=checked_at, reason=reason)))
    validate_command(command)
    return command


def handler(*, check_sha256, apply_sha256):
    return HandlerSpec('domain_command', ROUTE, ACTIVATION_REVISIONS,
        PgFunction('check_geo_resolve_v1', check_sha256),
        PgFunction('apply_geo_resolve_v1', apply_sha256),
        frozenset({SOURCE_REVISION_KEY}),
        dependency=DependencySpec(ROUTE, 'public_source', frozenset({'source_id'}), 'shared', False))


def _error(status, reason):
    # MCP exists only on the controller, never as a Kaggle worker dependency.
    from mcp.server.mcpserver.exceptions import ToolError
    return ToolError(json.dumps({'status': status, 'reason': reason}, separators=(',', ':')))


async def geo_resolve(startup, principal, *, source_id, mention_id, source_revision, status,
                      poi_ref=None, url=None, checked_at=None, reason,
                      operation_key):
    """Owner-only exact accepted command, using the EXISTING source intake authority.

    Never infer accepted state from a staged row, SQL effect from a queue job,
    or Street Story POI owner receipt from this RKB projection.
    """
    from mcp.server.mcpserver.exceptions import ToolError
    try:
        _text(operation_key, 128, required=True)
        expected_revision = _source_revision(source_revision)
        payload = _payload(source_id=source_id, mention_id=mention_id, status=status,
            poi_ref=poi_ref, url=url, checked_at=checked_at, reason=reason)
    except (TypeError, ValueError):
        raise _error('invalid_query', 'invalid_geo_resolve_input') from None

    importer = getattr(startup, 'importer', None)
    if importer is None or getattr(startup, 'source_dispatch', None) is None:
        raise _error('unavailable', 'native_source_intake_unavailable')
    spool = importer.spool
    try:
        # Existing OAuth, canonical RequestAuthority, participant import
        # capability and current writer binding. No caller-supplied actor.
        async with importer._access(principal) as access:
            await importer._check(access.authority, access.context, access.binding)
            entry = None
            predecessor = None
            async with spool._lock:
                from .control_spool import SpoolError
                try:
                    entry = await spool.get(operation_key)
                except SpoolError as exc:
                    if str(exc) != 'control operation not found':
                        raise
                if entry is not None:
                    command = entry.command
                    if (command.route != ROUTE or command.person_id != access.authority.person_id
                            or command.payload != canonical(payload)
                            or dict(command.expected_revisions) != {SOURCE_REVISION_KEY: expected_revision}
                            or dict(command.ids) != {'source_id': source_id, 'mention_id': mention_id}):
                        raise _error('conflict', 'operation_key_has_different_accepted_bytes')
                    validate_command(command)
                else:
                    scope = await spool.scope_state()
                    if (scope['unknown']
                            or scope['sequence'] != scope['accepted_sequence']):
                        raise _error('unavailable', 'prior_write_pending')
                    predecessor = await spool.journal.head()
                    sequence = predecessor.reference.last_sequence if predecessor else 0
                    if sequence != scope['accepted_sequence']:
                        raise _error('unavailable', 'source_history_unavailable')
                    command = resolved_command(
                        sequence=sequence + 1, operation_key=operation_key,
                        person_id=access.authority.person_id,
                        logical_time=datetime.now(timezone.utc).isoformat(),
                        source_revision=source_revision,
                        **{key: payload[key] for key in FIELDS if key != 'codec'})
                    frozen, data, selected = spool._validate(command, access.binding.database_generation)
                    await importer._check(access.authority, access.context, access.binding)
                    facts = await spool.read_facts(None)
                    if (facts.database_generation != access.binding.database_generation
                            or facts.scope_sequence > facts.covered_sequence
                            or facts.pending or facts.unknown or not spool.facts_current(facts)):
                        raise _error('unavailable', 'source_history_unavailable')
                    entry = await spool._write(
                        spool._stage, frozen, data, selected, access.binding.database_generation)

            if entry.outcome_state == 'DURABLE':
                return dict(status=entry.apply_receipt.status, operation_key=operation_key,
                            source_id=source_id, mention_id=mention_id, duplicate=True)
            if entry.accepted is None:
                await importer._check(access.authority, access.context, access.binding)
                if predecessor is not None:
                    await spool.seal(access.context, operation_key,
                        writer_generation=startup.config.writer_generation, predecessor=predecessor)
                elif entry.seal_state == 'NOT_SENT':
                    # A staged intent after process interruption: current head
                    # must still be its exact immediately preceding sequence.
                    head = await spool.journal.head()
                    if (head.reference.last_sequence if head else 0) != entry.command.sequence - 1:
                        raise _error('unavailable', 'prior_write_pending')
                    await spool.seal(access.context, operation_key,
                        writer_generation=startup.config.writer_generation, predecessor=head)
                else:
                    await spool.seal(access.context, operation_key,
                        writer_generation=startup.config.writer_generation)
            await importer._check(access.authority, access.context, access.binding)
            await startup.source_dispatch.enqueue(access.context, operation_key,
                writer_generation=startup.config.writer_generation)
            return dict(status='processing', operation_key=operation_key,
                        source_id=source_id, mention_id=mention_id,
                        message='Remote accepted command pending exact PG outcome; not Saved')
    except ToolError:
        raise
    except PermissionError:
        raise _error('denied', 'access_denied') from None
    except Exception:
        # SQL/apply UNKNOWN is never converted to success and must be reconciled
        # by the existing source journal/dispatch under the SAME operation_key.
        raise _error('unavailable', 'geo_operation_unresolved') from None
