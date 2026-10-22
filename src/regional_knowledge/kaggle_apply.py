"""Atomic PG apply receipts and ordered semantic replay; no product Saved ACK."""
from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import re
import time
import weakref
from collections.abc import Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from types import MappingProxyType

import psycopg
from psycopg import sql
from psycopg.pq import TransactionStatus
from psycopg.rows import tuple_row
from psycopg.types.json import Jsonb

from .recovery_journal import AcceptedSeal, ExactCommand, RecoveryJournal, StaleWriter

_LOG = logging.getLogger(__name__)


def _log_pg_failure(error, *, command, stage, started):
    # PostgreSQL messages and SQL parameters can contain private source text.
    # Retain only the fixed SQLSTATE and execution boundary for diagnosis.
    code = getattr(error, 'sqlstate', None)
    if type(code) is not str or re.fullmatch('[A-Z0-9]{5}', code) is None:
        code = 'unknown'
    _LOG.warning('native_apply_failure sequence=%s command_sha256=%s stage=%s '
                 'elapsed_ms=%s error_type=%s sqlstate=%s', command.sequence,
                 _hash(command.encode()), stage, round((time.monotonic()-started)*1000, 1),
                 type(error).__name__, code)


class ApplyError(RuntimeError):
    pass


class ApplyConflict(ApplyError):
    pass


class ApplyUnknown(ApplyError):
    """Keep original DB generation/key/bytes and reconcile on a new connection."""


class ApplyLimit(ApplyError):
    pass


class ApplyExecutionError(ApplyError):
    pass


def _hash(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _json(value) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode()


def _text(value: str, limit: int = 256) -> None:
    if not isinstance(value, str) or not value or len(value) > limit or len(value.encode()) > limit:
        raise ValueError("bounded explicit identity/revision is required")


def _digest(value: str) -> None:
    if not isinstance(value, str) or re.fullmatch("[0-9a-f]{64}", value) is None:
        raise ValueError("exact SHA-256 is required")


@dataclass(frozen=True)
class Revisions:
    schema: str
    function: str
    executor: str

    def __post_init__(self):
        for value in self.wire():
            _text(value)

    def wire(self) -> tuple[str, str, str]:
        return self.schema, self.function, self.executor


@dataclass(frozen=True)
class PgFunction:
    """Trusted fixed four-argument function, pinned to pg_get_functiondef bytes."""
    name: str
    definition_sha256: str

    def __post_init__(self):
        if not isinstance(self.name, str) or re.fullmatch("[a-z][a-z0-9_]{0,62}", self.name) is None:
            raise ValueError("a static rkb_v4 function identifier is required")
        _digest(self.definition_sha256)

    @property
    def signature(self) -> str:
        return f"rkb_v4.{self.name}(bytea,jsonb,jsonb,text)"


@dataclass(frozen=True)
class DependencySpec:
    """Static operation write scope; read dependencies are a separate concern.

    This metadata confers no identity or permission. The trusted intake derives
    resources from these exact ID keys after existing current authorization.
    An absent/unknown specification cannot prove a read independent.
    """
    operation: str
    scope_class: str
    resource_keys: frozenset[str]
    scope_owner: str
    restriction: bool = False

    def __post_init__(self):
        _text(self.operation)
        if self.scope_class not in {'public_source','private_draft','case','geo_manifest','unknown'}:
            raise ValueError('unsupported static dependency class')
        if self.scope_owner not in {'actor','shared'} or type(self.restriction) is not bool:
            raise ValueError('explicit dependency scope ownership required')
        if (not isinstance(self.resource_keys,frozenset) or not 1<=len(self.resource_keys)<=32
                or any(not isinstance(k,str) for k in self.resource_keys)):
            raise ValueError('bounded immutable resource ID keys required')
        for key in self.resource_keys:_text(key)
        if (self.scope_class in {'private_draft','geo_manifest'} and self.scope_owner!='actor'
                or self.scope_class=='public_source' and self.scope_owner!='shared'):
            raise ValueError('dependency class ownership mismatch')

    def wire(self):
        return {'operation':self.operation,'scope_class':self.scope_class,
                'resource_keys':sorted(self.resource_keys),'scope_owner':self.scope_owner,
                'restriction':self.restriction}


@dataclass(frozen=True)
class HandlerSpec:
    kind: str
    route: str
    revisions: Revisions
    check: PgFunction
    apply: PgFunction
    precondition_keys: frozenset[str]
    dependency: DependencySpec | None = None

    def __post_init__(self):
        _text(self.kind)
        _text(self.route)
        if not isinstance(self.precondition_keys, frozenset) or len(self.precondition_keys) > 128:
            raise ValueError("precondition keys must be a bounded immutable set")
        for key in self.precondition_keys:
            _text(key)
        if self.kind not in {'domain_command','durable_intake','control','provider_intent'}:
            raise ValueError("SQL migrations belong to the verified migration runner")
        if self.dependency is not None and (not isinstance(self.dependency,DependencySpec)
                or self.dependency.operation!=self.route):
            raise ValueError('dependency metadata must bind the exact handler operation')

    @property
    def key(self):
        # Immutable v1 audit shape; executable lookup is by route below.
        return self.kind,self.route,*self.revisions.wire()

    @property
    def fingerprint(self) -> str:
        data={"key":(self.kind,self.route,*self.revisions.wire()),"check":(self.check.name,self.check.definition_sha256),
            "apply":(self.apply.name,self.apply.definition_sha256),
            "preconditions":sorted(self.precondition_keys),"transition":None}
        if self.dependency is not None:data['dependency']=self.dependency.wire()
        return _hash(_json(data))


@dataclass(frozen=True, init=False)
class HandlerRegistry:
    """Deployment-owned immutable function registry, never populated from requests."""
    handlers: Mapping

    def __init__(self, handlers: Sequence[HandlerSpec]):
        from .kaggle_native_source_bindings import NATIVE_ROUTES
        if not 1 <= len(handlers) <= len(NATIVE_ROUTES):
            raise ValueError("a bounded trusted registry is required")
        mapping = {handler.route: handler for handler in handlers}
        if len(mapping) != len(handlers):
            raise ValueError("duplicate current route")
        object.__setattr__(self, "handlers", MappingProxyType(mapping))

    def resolve(self, command: ExactCommand) -> HandlerSpec:
        handler=self.handlers.get(command.route)
        if handler is None or handler.kind!=command.kind:
            raise ApplyConflict("no exact current route handler")
        return handler



@dataclass(frozen=True)
class ApplyReceipt:
    database_generation: str
    operation_key: str
    sequence: int
    command_sha256: str
    status: str
    outcome: bytes
    outcome_sha256: str
    duplicate: bool


_IN_USE: weakref.WeakSet = weakref.WeakSet()


@asynccontextmanager
async def _exclusive(connection):
    if (connection in _IN_USE or connection.closed
            or connection.info.transaction_status != TransactionStatus.IDLE
            or connection.row_factory is not tuple_row):
        raise ValueError("exclusively checked-out open idle tuple_row connection required")
    _IN_USE.add(connection)
    try:
        yield
    finally:
        try:
            if not connection.closed and connection.info.transaction_status != TransactionStatus.IDLE:
                cleanup = asyncio.create_task(connection.rollback())
                while not cleanup.done():
                    try:
                        await asyncio.shield(cleanup)
                    except asyncio.CancelledError:
                        continue
                    except Exception:
                        break
                if cleanup.cancelled() or cleanup.exception() is not None:
                    await connection.close()
        finally:
            _IN_USE.discard(connection)


async def _settings(connection):
    await connection.execute("SELECT set_config('lock_timeout','1000ms',true), "
                             "set_config('statement_timeout','2000ms',true)")


async def _state(connection, database_generation: str, writer_generation: str | None = None):
    row = await (await connection.execute("""SELECT database_generation, writer_generation, applied_seq
        FROM rkb_v4.recovery_state WHERE singleton FOR UPDATE""")).fetchone()
    if row is None or row[0] != database_generation:
        raise ApplyConflict("original database generation is not the initialized target")
    if writer_generation is not None and row[1] != writer_generation:
        raise StaleWriter("executor does not own the current PG writer generation")
    return row


async def initialize_database(connection, *, database_generation: str, writer_generation: str,
                              applied_seq: int = 0) -> None:
    """Trusted one-time initialization, not an implicit migration or reset."""
    _text(database_generation)
    _text(writer_generation)
    if type(applied_seq) is not int or not 0 <= applied_seq < 2**63:
        raise ValueError("explicit bounded initial cursor required")
    async with _exclusive(connection):
        async with connection.transaction():
            await _settings(connection)
            await connection.execute("LOCK TABLE rkb_v4.recovery_state IN EXCLUSIVE MODE")
            if await (await connection.execute("SELECT 1 FROM rkb_v4.recovery_state")).fetchone():
                raise ApplyConflict("database is already initialized; explicit upgrade required")
            await connection.execute("INSERT INTO rkb_v4.writer_generations(generation) VALUES (%s)", (writer_generation,))
            await connection.execute("""INSERT INTO rkb_v4.recovery_state
                (singleton,database_generation,writer_generation,applied_seq)
                 VALUES (true,%s,%s,%s)""",
                (database_generation,writer_generation,applied_seq))


async def handover_writer(connection, *, database_generation: str, expected_writer: str,
                          new_writer: str) -> None:
    """Controller CAS under the apply row lock; retired generations never return."""
    for value in (database_generation, expected_writer, new_writer):
        _text(value)
    if expected_writer == new_writer:
        raise ValueError("handover requires a new generation")
    async with _exclusive(connection):
        try:
            async with connection.transaction():
                await _settings(connection)
                state = await _state(connection, database_generation)
                existing = await (await connection.execute(
                    "SELECT predecessor, retired FROM rkb_v4.writer_generations WHERE generation=%s", (new_writer,))).fetchone()
                if existing is not None and existing[0] == expected_writer:
                    return  # Historical success; never reactivate a retired writer.
                if state[1] != expected_writer or existing is not None:
                    raise ApplyConflict("handover CAS failed or generation was already used")
                await connection.execute("UPDATE rkb_v4.writer_generations SET retired=true WHERE generation=%s", (expected_writer,))
                await connection.execute("INSERT INTO rkb_v4.writer_generations(generation,predecessor) VALUES (%s,%s)",
                                         (new_writer, expected_writer))
                await connection.execute("UPDATE rkb_v4.recovery_state SET writer_generation=%s WHERE singleton", (new_writer,))
        except psycopg.OperationalError:
            raise ApplyUnknown("handover outcome unknown; reconcile original target and generations") from None


def _snapshot(command: ExactCommand, max_bytes: int) -> tuple[ExactCommand, bytes]:
    if type(max_bytes) is not int or not 1 <= max_bytes <= 1048576:
        raise ValueError("command cap must be 1..1048576 bytes")
    if type(command.sequence) is not int or not 1 <= command.sequence < 2**63:
        raise ValueError("sequence must fit positive PG bigint")
    if not isinstance(command.payload, bytes) or len(command.payload) > max_bytes:
        raise ApplyLimit("payload exceeds command budget")
    for value in (command.operation_key, command.request_id, command.person_id, command.route,
                  command.logical_time, command.schema_revision, command.function_revision,
                  command.executor_revision, command.kind):
        _text(value, 4096)
    for mapping in (command.ids, command.expected_revisions):
        if not isinstance(mapping, Mapping) or len(mapping) > 128:
            raise ApplyLimit("metadata count exceeds command budget")
        for key, value in mapping.items():
            _text(key, 4096)
            if not isinstance(value, str) or len(value) > 4096 or len(value.encode()) > 4096:
                raise ApplyLimit("metadata value exceeds command budget")
    frozen = replace(command, ids=MappingProxyType(dict(command.ids)),
                     expected_revisions=MappingProxyType(dict(command.expected_revisions)))
    data = frozen.encode()
    if len(data) > max_bytes:
        raise ApplyLimit("exact encoded command exceeds budget")
    return frozen, data


async def _function_contract(connection,function: PgFunction,*,precondition: bool):
    # Trusted SQL artifacts and the migration ledger own implementation bytes.
    # Keep the invoker/scalar/stable-call contract without catalog body pins.
    row=await (await connection.execute("""SELECT p.prorettype::regtype::text,
        p.provolatile,p.prosecdef,l.lanname,p.proretset,p.prokind
        FROM pg_proc p JOIN pg_language l ON l.oid=p.prolang
        WHERE p.oid=to_regprocedure(%s)""",(function.signature,))).fetchone()
    if (row is None or row[0]!=("boolean" if precondition else "bytea")
            or row[2] or row[4] or row[5]!="f" or row[3] not in ("sql","plpgsql")
            or precondition and row[1]!="s"):
        raise ApplyConflict("handler requires invoker SQL/PLpgSQL and a stable boolean precondition")


def _call(function: PgFunction):
    return sql.SQL("{}.{}(%s::bytea,%s::jsonb,%s::jsonb,%s::text)").format(
        sql.Identifier("rkb_v4"), sql.Identifier(function.name))


async def apply_accepted(connection, *, journal: RecoveryJournal, seal: AcceptedSeal,
                         command: ExactCommand, database_generation: str,
                         writer_generation: str, registry: HandlerRegistry,
                         max_command_bytes: int = 1048576,
                         max_outcome_bytes: int = 1048576) -> ApplyReceipt:
    """Verify remote acceptance before TX, then atomically apply a trusted PG handler.

    Functions and registry are trusted deployment artifacts. Both functions must
    be pure PG and use exact resolved inputs; all DDL writers share this state
    row lock. Command person/route fields confer no authorization by themselves.
    """
    _text(database_generation)
    _text(writer_generation)
    if type(max_outcome_bytes) is not int or not 1 <= max_outcome_bytes <= 1048576:
        raise ValueError("outcome cap must be 1..1048576 bytes")
    command, encoded = _snapshot(command, max_command_bytes)
    async with _exclusive(connection):
        # Provider I/O happens with an idle connection and no PostgreSQL lock.
        command_hash = await journal.verify_accepted(seal, command)
        if command_hash != _hash(encoded):
            raise ApplyConflict("journal did not verify the exact immutable command")
        started = time.monotonic()
        stage = 'transaction'
        try:
            async with connection.transaction():
                stage = 'settings'
                await _settings(connection)
                stage = 'writer_state'
                state = await _state(connection, database_generation, writer_generation)
                stage = 'existing_outcome'
                duplicates = await (await connection.execute("""SELECT operation_key,sequence,command_bytes,
                    command_sha256,status,outcome_bytes,outcome_sha256,database_generation
                    FROM rkb_v4.applied_ops WHERE operation_key=%s OR sequence=%s""",
                    (command.operation_key, command.sequence))).fetchall()
                if duplicates:
                    row = duplicates[0]
                    if (len(duplicates) != 1 or row[:2] != (command.operation_key, command.sequence)
                            or bytes(row[2]) != encoded or row[3] != command_hash or row[7] != database_generation):
                        raise ApplyConflict("operation key/sequence has different exact accepted bytes")
                    outcome = bytes(row[5])
                    if len(outcome) > max_outcome_bytes or _hash(outcome) != row[6]:
                        raise ApplyConflict("recorded outcome integrity/budget differs")
                    receipt = ApplyReceipt(database_generation, command.operation_key, command.sequence,
                                           command_hash, row[4], outcome, row[6], True)
                else:
                    if command.sequence != state[2] + 1:
                        raise ApplyConflict("applied cursor requires the exact next sequence")
                    # Immutable v1 audit metadata; never compared with the PG schema.
                    before=[command.schema_revision, command.function_revision, command.executor_revision]
                    handler = registry.resolve(command)
                    if set(command.expected_revisions) != handler.precondition_keys:
                        raise ApplyConflict("complete exact precondition key set is required")
                    stage = 'function_contract'
                    await _function_contract(connection, handler.check, precondition=True)
                    await _function_contract(connection, handler.apply, precondition=False)
                    args = (command.payload, Jsonb(dict(command.ids)), Jsonb(dict(command.expected_revisions)), command.logical_time)
                    stage = 'precondition'
                    checked = await (await connection.execute(sql.SQL("SELECT ") + _call(handler.check), args)).fetchone()
                    if checked is None or type(checked[0]) is not bool:
                        raise ApplyConflict("precondition function must return one boolean")
                    status = "applied" if checked[0] else "rejected_precondition"
                    result = b""
                    after = before
                    if checked[0]:
                        stage = 'effect'
                        row = await (await connection.execute(sql.SQL("WITH x AS MATERIALIZED (SELECT ")
                            + _call(handler.apply) + sql.SQL(" AS body) SELECT octet_length(body), "
                            "CASE WHEN octet_length(body)<=%s THEN body END FROM x"), (*args, max_outcome_bytes))).fetchone()
                        if row is None or row[1] is None:
                            raise ApplyLimit("handler outcome is absent or exceeds budget")
                        result = bytes(row[1])
                    outcome = _json({"codec": "rkb.applied-outcome.v1", "operation_key": command.operation_key,
                        "command_sha256": command_hash, "cursor": command.sequence, "status": status,
                        "ids": dict(command.ids), "request_id": command.request_id, "person_id": command.person_id,
                        "route": command.route, "logical_time": command.logical_time,
                        "before": before, "after": after, "handler_sha256": handler.fingerprint,
                        "result_b64": base64.b64encode(result).decode(), "result_sha256": _hash(result)})
                    if len(outcome) > max_outcome_bytes:
                        raise ApplyLimit("canonical outcome exceeds byte budget")
                    digest = _hash(outcome)
                    stage = 'receipt'
                    await connection.execute("""INSERT INTO rkb_v4.applied_ops VALUES
                        (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                        (command.operation_key, command.sequence, encoded, command_hash, seal.manifest.sha256,
                         database_generation, writer_generation, status, outcome, digest, handler.fingerprint))
                    await connection.execute("UPDATE rkb_v4.recovery_state SET applied_seq=%s WHERE singleton",
                        (command.sequence,))
                    receipt = ApplyReceipt(database_generation, command.operation_key, command.sequence,
                                           command_hash, status, outcome, digest, False)
                stage = 'commit'
            return receipt  # Only after the actual commit ACK.
        except psycopg.OperationalError as error:
            _log_pg_failure(error, command=command, stage=stage, started=started)
            raise ApplyUnknown("apply outcome unknown; reconcile same operation in original database generation") from None
        except psycopg.Error as error:
            _log_pg_failure(error, command=command, stage=stage, started=started)
            raise ApplyExecutionError("PostgreSQL apply failed; transaction must be reconciled/rolled back") from None
