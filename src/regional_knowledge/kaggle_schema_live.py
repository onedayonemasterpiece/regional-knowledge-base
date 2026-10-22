"""Live SQL artifact delivery over the retained recovery transfer and WSS owner.

Only a complete immutable SQL prefix can be staged. Python, paths and SQL text
are not command arguments; the approved blob travels through the existing exact
size/SHA/ETag download adapter. The resident model and serving pool stay owned by
TargetRuntime.
"""
from __future__ import annotations

import base64
import hashlib
import json
from pathlib import Path
from types import MappingProxyType

from .kaggle_schema_migrate import MigrationDenied, bundle_migrations, migrate
from .kaggle_warm_maintenance import WarmHandlerPatchApproval

MAX_SQL_BUNDLE_BYTES = 2 * 1024 * 1024


def bundle_hash(files):
    return hashlib.sha256(json.dumps(
        {m.name: m.sha256 for m in bundle_migrations(files)},
        sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def encode_sql_bundle(files):
    bundle_migrations(files)
    return json.dumps({key: base64.b64encode(value).decode("ascii")
                       for key, value in sorted(files.items())},
                      sort_keys=True, separators=(",", ":")).encode()


def decode_sql_bundle(raw):
    if not 1 <= len(raw) <= MAX_SQL_BUNDLE_BYTES:
        raise MigrationDenied("migration_bundle_budget")
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise MigrationDenied("migration_bundle_duplicate")
            result[key] = value
        return result
    try:
        values = json.loads(raw, object_pairs_hook=unique)
        files = {key: base64.b64decode(value, validate=True) for key, value in values.items()}
        migrations = bundle_migrations(files)
        if len(migrations) != len(files):
            raise MigrationDenied("migration_bundle_sql_only")
        if encode_sql_bundle(files) != raw:
            raise MigrationDenied("migration_bundle_noncanonical")
        return MappingProxyType(files)
    except (ValueError, TypeError, AttributeError):
        raise MigrationDenied("migration_bundle_invalid") from None


class LiveSchemaMigrations:
    def __init__(self, *, files, acquire, current, download, database, on_version=None):
        bundle_migrations(files)
        self.files = MappingProxyType(dict(files))
        self.acquire, self.current, self.download, self.database = acquire, current, download, database
        self.on_version=on_version

    async def __call__(self, phase, args, operation_key, frame):
        self.current(frame)
        if phase == "schema_bundle_stage":
            if set(args) != {"artifact", "expected_bundle_sha256"}:
                raise MigrationDenied("migration_stage_arguments")
            approval = WarmHandlerPatchApproval.from_wire(args["artifact"])
            if not 1 <= approval.blob.size <= MAX_SQL_BUNDLE_BYTES:
                raise MigrationDenied("migration_bundle_budget")
            # Reconcile the same exact stage after a lost response, including
            # the narrow gap between the immutable assignment and its receipt.
            if approval.blob.sha256 == hashlib.sha256(encode_sql_bundle(self.files)).hexdigest():
                return self.receipt()
            if args["expected_bundle_sha256"] != bundle_hash(self.files):
                raise MigrationDenied("migration_bundle_stale")
            ticket = frame.get("download_ticket")
            if type(ticket) is not dict:
                raise MigrationDenied("migration_transfer_required")
            path = Path(await self.download(approval.blob, operation_key,
                ticket=ticket, transfer_operation=operation_key))
            try:
                raw = path.read_bytes()
                if (len(raw), hashlib.sha256(raw).hexdigest()) != (approval.blob.size, approval.blob.sha256):
                    raise MigrationDenied("migration_bundle_bytes_changed")
                candidate = decode_sql_bundle(raw)
                for key, value in self.files.items():
                    if candidate.get(key) != value:
                        raise MigrationDenied("migration_file_changed")
                self.current(frame)
                self.files = candidate
            finally:
                path.unlink(missing_ok=True)
            return self.receipt()
        if phase != "schema_migrate" or set(args) != {"target_schema_version", "bundle_sha256"}:
            raise MigrationDenied("migration_phase_arguments")
        latest = len(bundle_migrations(self.files))
        if (type(args['target_schema_version']) is not int or args['target_schema_version']!=latest
                or args["bundle_sha256"] != bundle_hash(self.files)):
            raise MigrationDenied("migration_version_changed")
        async with self.acquire() as connection:
            async def writer_fence(connection):
                self.current(frame)
                row = await (await connection.execute("""SELECT s.database_generation,s.writer_generation,w.retired
                    FROM rkb_v4.recovery_state s JOIN rkb_v4.writer_generations w
                    ON w.generation=s.writer_generation WHERE s.singleton FOR UPDATE OF s""")).fetchone()
                if row != (self.database.database_generation, self.database.writer_generation, False):
                    raise MigrationDenied("migration_writer_changed")
            async with connection.transaction():
                await writer_fence(connection)
                from .kaggle_owned_pg import configure_migration_defaults
                await configure_migration_defaults(connection)
            self.current(frame)
            applied = await migrate(connection, self.files, before_file=writer_fence)
            async with connection.transaction():
                version = (await (await connection.execute(
                    "SELECT max(version) FROM rkb_v4.schema_migrations")).fetchone())[0]
            self.current(frame)
        result=dict(self.receipt(), schema_version=version,checkpoint_required=True,applied_versions=list(applied))
        if self.on_version is not None:result['readiness']=await self.on_version(version)
        return result

    def receipt(self):
        return dict(bundle_sha256=bundle_hash(self.files), latest_schema_version=len(bundle_migrations(self.files)))
