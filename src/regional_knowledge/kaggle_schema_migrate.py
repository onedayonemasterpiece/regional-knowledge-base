"""Linear transactional migrations from an already verified worker bundle.

The caller owns the writer fence and checkpoint barrier. This module accepts
bundle bytes, never SQL supplied by a WSS command, and uses one idle, dedicated
connection (not a serving reader's transaction).
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import logging
import re
from collections.abc import Mapping

from psycopg.pq import TransactionStatus

LOG = logging.getLogger(__name__)
BASELINE_VERSIONS = {
    "rkb-v4-sql001-011-v1": 11,
    "rkb-v4-sql001-012-v1": 12,
    "rkb-v4-sql001-014-v1": 14,
}
LOCK_ID = 0x524B425343484D  # session lock, shared by every migration invocation
LEDGER_SQL = """CREATE TABLE rkb_v4.schema_migrations (
    version int PRIMARY KEY, name text NOT NULL, sha256 text NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT now())"""
INSERT_SQL = "INSERT INTO rkb_v4.schema_migrations(version,name,sha256) VALUES(%s,%s,%s)"


class MigrationDenied(RuntimeError):
    pass


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    body: bytes
    sha256: str


def bundle_migrations(files: Mapping[str, bytes]) -> tuple[Migration, ...]:
    """Select the complete numbered prefix from the verified bundle's sql/ root."""
    migrations = []
    for path, body in files.items():
        match = re.fullmatch(r"sql/([0-9]{3})_([a-z0-9_]+)\.sql", path)
        if match:
            if not isinstance(body, bytes) or not body:
                raise MigrationDenied("migration_artifact_invalid")
            body.decode("utf-8", errors="strict")
            migrations.append(Migration(int(match[1]), path[4:], body,
                                        hashlib.sha256(body).hexdigest()))
    migrations.sort(key=lambda m: m.version)
    if not migrations or [m.version for m in migrations] != list(range(1, len(migrations) + 1)):
        raise MigrationDenied("migration_prefix_invalid")
    return tuple(migrations)


async def _row(connection, query, params=None):
    return await (await connection.execute(query, params)).fetchone()


async def _baseline(connection, migrations, before_file=None):
    async with connection.transaction():
        if before_file is not None:
            await before_file(connection)
        exists = (await _row(connection, "SELECT to_regclass('rkb_v4.schema_migrations')"))[0]
        if exists is not None:
            return
        schema = (await _row(connection, "SELECT to_regnamespace('rkb_v4')"))[0]
        if schema is None:
            return  # SQL001 and its ledger row will commit together.
        state = (await _row(connection, "SELECT to_regclass('rkb_v4.recovery_state')"))[0]
        if state is None:
            raise MigrationDenied("migration_baseline_unknown")
        row = await _row(connection, "SELECT schema_revision FROM rkb_v4.recovery_state WHERE singleton")
        version = BASELINE_VERSIONS.get(row[0] if row else None)
        if version is None:
            raise MigrationDenied("migration_baseline_unknown")
        # SQL015/016 were additive candidates outside the old revision triples.
        # Require both their relation and callable marker; partial install is not
        # a baseline and must never be silently recorded as complete.
        for ordinal, relation, signature in (
            (15, "source_illustrations", "read_source_illustrations_v1(uuid,uuid,integer)"),
            (16, "story_reconciliation_cursor", "read_story_reconciliation_v1(text,uuid)"),
        ):
            markers = await _row(connection, "SELECT to_regclass(%s),to_regprocedure(%s)",
                                 ("rkb_v4." + relation, "rkb_v4." + signature))
            present = [marker is not None for marker in markers]
            if any(present):
                if not all(present) or version != ordinal - 1:
                    raise MigrationDenied("migration_baseline_partial")
                version = ordinal
        if version > len(migrations):
            raise MigrationDenied("migration_bundle_behind")
        await connection.execute(LEDGER_SQL)
        for migration in migrations[:version]:
            await connection.execute(INSERT_SQL, (migration.version, migration.name, migration.sha256))
        LOG.info("schema_migrate baseline schema_version=%s", version)


async def migrate(connection, files: Mapping[str, bytes], *, through_version: int | None = None,
                  before_file=None) -> tuple[int, ...]:
    """Apply pending files, each in its own transaction; return committed versions.

    On a lost commit ACK the caller reconciles the SAME database generation by
    calling again: committed ledger rows skip that file. No revision guessing or
    journal mutation occurs here.
    """
    migrations = bundle_migrations(files)
    if through_version is not None and (type(through_version) is not int
            or not 1 <= through_version <= len(migrations)):
        raise MigrationDenied("migration_target_invalid")
    if connection.info.transaction_status != TransactionStatus.IDLE:
        raise MigrationDenied("migration_connection_not_idle")
    locked = False
    try:
        async with connection.transaction():
            locked = (await _row(connection, "SELECT pg_try_advisory_lock(%s)", (LOCK_ID,)))[0]
        if not locked:
            raise MigrationDenied("migration_busy")
        await _baseline(connection, migrations, before_file)
        async with connection.transaction():
            exists = (await _row(connection, "SELECT to_regclass('rkb_v4.schema_migrations')"))[0]
            rows = [] if exists is None else await (await connection.execute(
                "SELECT version,name,sha256 FROM rkb_v4.schema_migrations ORDER BY version")).fetchall()
        if [r[0] for r in rows] != list(range(1, len(rows) + 1)):
            raise MigrationDenied("migration_ledger_not_prefix")
        if len(rows) > len(migrations):
            raise MigrationDenied("migration_bundle_behind")
        for row, migration in zip(rows, migrations):
            if row != (migration.version, migration.name, migration.sha256):
                raise MigrationDenied("migration_file_changed")
        applied = []
        for migration in migrations[len(rows):]:
            if through_version is not None and migration.version > through_version:
                break
            LOG.info("schema_migrate applying version=%s sha256=%s", migration.version, migration.sha256)
            async with connection.transaction():
                await connection.execute("SET LOCAL lock_timeout='5s'")
                await connection.execute("SET LOCAL statement_timeout='120s'")
                if before_file is not None:
                    await before_file(connection)
                await connection.execute(migration.body.decode("utf-8"), prepare=False)
                if migration.version == 1:
                    await connection.execute(LEDGER_SQL)
                await connection.execute(INSERT_SQL, (migration.version, migration.name, migration.sha256))
            applied.append(migration.version)
            LOG.info("schema_migrate committed schema_version=%s", migration.version)
        return tuple(applied)
    except Exception as error:
        LOG.warning("schema_migrate failed error_type=%s", type(error).__name__)
        raise
    finally:
        if locked and not connection.closed:
            async with connection.transaction():
                await connection.execute("SELECT pg_advisory_unlock(%s)", (LOCK_ID,))
