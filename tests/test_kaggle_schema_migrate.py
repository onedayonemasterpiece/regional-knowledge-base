"""Runner contracts; real transactional DDL runs only on the existing PG17 CI service."""
import asyncio
import os
from pathlib import Path
import re

import psycopg
import pytest

from regional_knowledge.kaggle_schema_migrate import (
    BASELINE_VERSIONS, MigrationDenied, bundle_migrations, migrate,
)

ROOT = Path(__file__).resolve().parents[1]


def files():
    return {"sql/" + path.name: path.read_bytes()
            for path in (ROOT / "sql/kaggle").glob("[0-9][0-9][0-9]_*.sql")}


def test_verified_bundle_requires_unique_complete_numbered_prefix():
    assert [m.version for m in bundle_migrations(files())] == list(range(1,len(files())+1))
    for invalid in (
        {}, {"sql/002_gap.sql": b"SELECT 1"},
        {"sql/001_one.sql": b"SELECT 1", "sql/001_duplicate.sql": b"SELECT 2"},
        {"sql/001_empty.sql": b""}, {"../sql/001_escape.sql": b"SELECT 1"},
    ):
        with pytest.raises(MigrationDenied):
            bundle_migrations(invalid)


async def dump_schema():
    # Use the service's own PG17 binary; never install/run PostgreSQL on Dev.
    container = os.environ["RKB_PG17_CONTAINER"]
    process = await asyncio.create_subprocess_exec(
        "docker", "exec", container, "pg_dump", "-U", "postgres", "-d", "rkb_graph_test",
        "--schema-only", "--schema=rkb_v4", stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=30)
    assert process.returncode == 0, stderr.decode()
    # PG17 emits a random psql \restrict token. Everything else, including
    # owners, grants, triggers, defaults and function bodies, must match.
    return re.sub(r"^\\(?:un)?restrict .*\n", "", stdout.decode(), flags=re.MULTILINE)


@pytest.mark.asyncio
@pytest.mark.skipif(not os.getenv("RKB_KAGGLE_TEST_DSN"), reason="existing PG17 CI service only")
async def test_pg17_fresh_baselines_noop_integrity_rollback_and_schema_equivalence():
    from test_kaggle_public_illustration_native_read import _validated_ci_dsn

    connection = await psycopg.AsyncConnection.connect(_validated_ci_dsn(), autocommit=True)
    owned = False
    try:
        assert (await (await connection.execute("SHOW server_version_num")).fetchone())[0].startswith("17")
        assert (await (await connection.execute("SELECT to_regnamespace('rkb_v4')")).fetchone()) == (None,)
        assert await (await connection.execute(
            "SELECT rolname FROM pg_roles WHERE rolname IN ('rkb_reader','rkb_writer')")).fetchall() == []
        owned = True
        await connection.execute("CREATE ROLE rkb_reader NOLOGIN")
        await connection.execute("CREATE ROLE rkb_writer NOLOGIN")
        await connection.execute("CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public")
        artifacts = files()
        migrations = bundle_migrations(artifacts)
        assert await migrate(connection, artifacts) == tuple(range(1,len(artifacts)+1))
        reference = await dump_schema()
        original_rows = await (await connection.execute(
            "SELECT * FROM rkb_v4.schema_migrations ORDER BY version")).fetchall()
        assert await migrate(connection, artifacts) == ()
        assert await (await connection.execute(
            "SELECT * FROM rkb_v4.schema_migrations ORDER BY version")).fetchall() == original_rows

        changed = dict(artifacts)
        changed["sql/001_search.sql"] += b"\n-- changed applied file\n"
        with pytest.raises(MigrationDenied, match="migration_file_changed"):
            await migrate(connection, changed)

        failing = dict(artifacts, **{f"sql/{len(artifacts)+1:03d}_failure.sql": (
            b"CREATE TABLE rkb_v4.must_rollback(id int); SELECT 1/0;")})
        with pytest.raises(psycopg.errors.DivisionByZero):
            await migrate(connection, failing)
        assert (await (await connection.execute(
            "SELECT to_regclass('rkb_v4.must_rollback')")).fetchone()) == (None,)
        assert (await (await connection.execute(
            "SELECT max(version) FROM rkb_v4.schema_migrations")).fetchone()) == (len(artifacts),)
        # The failed runner released its session lock and left an idle connection.
        assert await migrate(connection, artifacts) == ()

        # The one baseline map covers all old revisions; candidates 015/016
        # are detected by their actual objects, even with the old 014 revision.
        for baseline in (11, 12, 14, 15, 16):
            await connection.execute("DROP SCHEMA rkb_v4 CASCADE")
            async with connection.transaction():
                for migration in migrations[:baseline]:
                    await connection.execute(migration.body.decode(), prepare=False)
                revision = next(k for k, v in BASELINE_VERSIONS.items() if v == min(baseline, 14))
                await connection.execute("INSERT INTO rkb_v4.writer_generations(generation) VALUES('fixture')")
                await connection.execute("""INSERT INTO rkb_v4.recovery_state
                    (singleton,database_generation,writer_generation,applied_seq,
                     schema_revision,function_revision,executor_revision)
                    VALUES(true,'fixture','fixture',0,%s,'fixture','fixture')""", (revision,))
            assert await migrate(connection, artifacts) == tuple(range(baseline+1,len(artifacts)+1))
            assert await migrate(connection, artifacts) == ()
            assert await dump_schema() == reference
            assert await (await connection.execute(
                "SELECT schema_version,applied_seq FROM rkb_v4.recovery_state WHERE singleton")).fetchone()==(len(artifacts),0)
            assert await (await connection.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_schema='rkb_v4' "
                "AND table_name='recovery_state' AND column_name IN "
                "('schema_revision','function_revision','executor_revision')")).fetchall()==[]

        # Additive DDL changes only the integer ledger. Existing v1 commands,
        # canonical outcomes and ordinary writes keep their exact audit fields.
        from regional_knowledge.kaggle_apply import HandlerRegistry,HandlerSpec,apply_accepted
        from regional_knowledge.recovery_journal import RecoveryJournal
        from test_recovery_journal import ConditionalMemoryStore
        from test_kaggle_apply import DOMAIN_SQL,V1,item,pinned
        await connection.execute(DOMAIN_SQL.split('CREATE FUNCTION rkb_v4.check_transition_v1')[0],prepare=False)
        registry=HandlerRegistry([HandlerSpec('domain_command','fixture.item',V1,
            await pinned(connection,'check_item_v1'),await pinned(connection,'apply_item_v1'),frozenset({'revision'}))])
        journal=RecoveryJournal(ConditionalMemoryStore(),stream='schema-integer-audit')
        first=item();original=first.encode()
        seal=await journal.seal([first],predecessor=None,writer_generation='fixture')
        async def apply(command,accepted):
            return await apply_accepted(connection,journal=journal,seal=accepted,command=command,
                database_generation='fixture',writer_generation='fixture',registry=registry)
        receipt=await apply(first,seal)
        before=await (await connection.execute('SELECT * FROM rkb_v4.read_frontier')).fetchall()
        extended=dict(artifacts,**{f'sql/{len(artifacts)+1:03d}_probe_column.sql':
            b'ALTER TABLE rkb_v4.sources ADD COLUMN schema_probe text;'})
        assert await migrate(connection,extended)==(len(artifacts)+1,)
        assert await (await connection.execute('SELECT * FROM rkb_v4.read_frontier')).fetchall()==before
        duplicate=await apply(first,seal)
        assert first.encode()==original and duplicate.outcome==receipt.outcome and duplicate.duplicate
        second=item(2,revision='1')
        second_seal=await journal.seal([second],predecessor=await journal.head(),writer_generation='fixture')
        assert (await apply(second,second_seal)).status=='applied'
        assert (first.schema_revision,first.function_revision,first.executor_revision)==(
            second.schema_revision,second.function_revision,second.executor_revision)
        assert await (await connection.execute(
            'SELECT schema_version,applied_seq FROM rkb_v4.recovery_state WHERE singleton')).fetchone()==(len(extended),2)
    finally:
        if owned:
            await connection.execute("DROP SCHEMA IF EXISTS rkb_v4 CASCADE")
            await connection.execute("DROP ROLE IF EXISTS rkb_reader")
            await connection.execute("DROP ROLE IF EXISTS rkb_writer")
        await connection.close()
