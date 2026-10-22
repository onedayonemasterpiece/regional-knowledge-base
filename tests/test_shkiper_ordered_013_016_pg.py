"""Real SQL001–016 ordered DDL gate on the EXISTING GitHub PG17 service only.

This is a source-compatibility/rollback test, NOT an accepted native migration
transition, media publication permit, Kaggle runtime change, or live database.
No Dev PostgreSQL, Docker, second PG pool, provider or credentials are used.
"""
from __future__ import annotations

import hashlib
import os
from pathlib import Path

import pytest

from test_kaggle_public_illustration_native_read import _validated_ci_dsn

ROOT = Path(__file__).resolve().parents[1]
SQL_ROOT = ROOT / "sql" / "kaggle"
EXPECTED_SHA256 = {
    13: "55f86fa1fdc2c2635c87f5edbc0f235cc629c6d59891ce199f30e264dce303b6",
    14: "5f8257e9fe58e2e2bdd00f67042ad17b8db007c7873dcc04f52d53be3523e855",
    15: "7faacbf7fbb2b99f0dbdbd4339dea5138f134347e31a6be09577dff35be8c773",
}
SQL16_GIT_BLOB = "702794fcac7463da1445647c8a1ad6006ad351b2"

if os.environ.get("GITHUB_ACTIONS") == "true":
    assert os.environ.get("RKB_KAGGLE_TEST_DSN"), "CI must supply its existing PG17 fixture; no silent skip"

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.skipif(
        not os.environ.get("RKB_KAGGLE_TEST_DSN"),
        reason="No local PG allowed; this test executes in the existing GitHub PG service",
    ),
]


class ExpectedSavepointRollback(Exception):
    pass


class ExpectedOuterRollback(Exception):
    pass


def _files():
    """No empty stand-ins, skipped ordinals, hidden SQL015 or changed predecessors."""
    discovered = {}
    for file in SQL_ROOT.glob("[0-9][0-9][0-9]_*.sql"):
        ordinal = int(file.name[:3])
        assert ordinal not in discovered, f"duplicate ordinal {ordinal}"
        discovered[ordinal] = file
    assert sorted(discovered) == list(range(1, 17)), "real complete SQL001–016 prefix required"
    for ordinal, name in (
        (13, "013_source_stage.sql"),
        (14, "014_source_stage_activation.sql"),
        (15, "015_source_illustrations.sql"),
        (16, "016_story_reconcile.sql"),
    ):
        assert discovered[ordinal].name == name, f"wrong exact predecessor SQL{ordinal}"
    for ordinal, expected in EXPECTED_SHA256.items():
        raw = discovered[ordinal].read_bytes()
        assert len(raw) >= 4096, f"SQL{ordinal} empty placeholder is forbidden"
        assert hashlib.sha256(raw).hexdigest() == expected, f"SQL{ordinal} source differs"
    raw16 = discovered[16].read_bytes()
    git_blob = hashlib.sha1(b"blob " + str(len(raw16)).encode() + b"\0" + raw16).hexdigest()
    assert git_blob == SQL16_GIT_BLOB, "SQL016 must be the reviewed PR214 source"
    return discovered


async def _definition_hashes(conn):
    rows = await (await conn.execute("""
        SELECT p.oid::regprocedure::text,
               encode(sha256(convert_to(pg_get_functiondef(p.oid),'UTF8')),'hex')
        FROM pg_proc p JOIN pg_namespace n ON p.pronamespace=n.oid
        WHERE n.nspname='rkb_v4'
          AND p.prokind IN ('f','p')
        ORDER BY 1
    """)).fetchall()
    result = dict(rows)
    assert len(result) == len(rows), "native function signature collision"
    return result


async def _must_exist(conn, *, relations=(), functions=()):
    for relation in relations:
        got = (await (await conn.execute(
            "SELECT to_regclass(%s)", ("rkb_v4." + relation,),
        )).fetchone())[0]
        assert got is not None, f"missing actual PostgreSQL relation {relation}"
    for signature in functions:
        got = (await (await conn.execute(
            "SELECT to_regprocedure(%s)", ("rkb_v4." + signature,),
        )).fetchone())[0]
        assert got is not None, f"missing actual PostgreSQL function {signature}"


async def test_pg17_exact_ordered_sql13_media15_case16_preserves_history_and_rolls_back():
    """Install actual 001..016 in order; prove old definitions, then rollback 13..16."""
    import psycopg

    files = _files()  # fail before connection if even one source artifact is absent
    connection = await psycopg.AsyncConnection.connect(
        _validated_ci_dsn(), connect_timeout=2, sslmode="disable",
        application_name="rkb_ordered_media_story_fixture",
    )
    try:
        try:
            async with connection.transaction():
                assert (await (await connection.execute(
                    "SELECT current_database(), current_user"
                )).fetchone()) == ("rkb_graph_test", "postgres")
                assert (await (await connection.execute("SHOW server_version_num")).fetchone())[0].startswith("17")
                assert (await (await connection.execute(
                    "SELECT to_regnamespace('rkb_v4')"
                )).fetchone())[0] is None, "disposable CI database must start without RKB schema"
                await connection.execute("CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public")
                assert (await (await connection.execute(
                    "SELECT extversion FROM pg_extension WHERE extname='vector'"
                )).fetchone()) == ("0.8.2",)

                for ordinal in range(1, 13):
                    await connection.execute(files[ordinal].read_text(), prepare=False)
                old12 = await _definition_hashes(connection)
                assert len(old12) >= 25
                await _must_exist(connection, functions=(
                    "check_disagreement_v1(bytea,jsonb,jsonb,text)",
                    "apply_disagreement_v1(bytea,jsonb,jsonb,text)",
                    "read_source_assertions_v1(uuid,uuid,integer)",
                ))

                # SQL016 pins rkb_reader/rkb_writer; create only temporary
                # NOLOGIN fixture roles, rolled back with the outer transaction.
                occupied = await (await connection.execute("""
                    SELECT rolname FROM pg_roles
                    WHERE rolname IN ('rkb_reader','rkb_writer')
                """)).fetchall()
                assert occupied == [], "must never operate on pre-existing roles"
                await connection.execute("CREATE ROLE rkb_reader NOLOGIN")
                await connection.execute("CREATE ROLE rkb_writer NOLOGIN")

                try:
                    async with connection.transaction():  # SAVEPOINT over real 013–016 DDL
                        await connection.execute(files[13].read_text(), prepare=False)
                        await _must_exist(connection, relations=(
                            "source_stage_sessions", "source_stage_batches", "source_stage_pages",
                        ), functions=("check_source_stage_v1(bytea,jsonb,jsonb,text)",))
                        after13 = await _definition_hashes(connection)
                        assert all(after13.get(k) == value for k, value in old12.items())

                        await connection.execute(files[14].read_text(), prepare=False)
                        await _must_exist(connection, relations=(
                            "source_activation_candidates", "source_image_origins",
                        ), functions=(
                            "prepare_source_activation_v1(uuid,uuid,integer,text,text,bytea,text,text)",
                        ))
                        after14 = await _definition_hashes(connection)
                        assert all(after14.get(k) == value for k, value in after13.items())

                        await connection.execute(files[15].read_text(), prepare=False)
                        await _must_exist(connection, relations=(
                            "source_illustration_candidates", "source_illustrations",
                            "source_illustration_origins", "source_illustration_availability",
                        ), functions=(
                            "check_source_illustrations_v1(jsonb,jsonb,uuid)",
                            "read_source_illustrations_v1(uuid,uuid,integer)",
                            "cas_source_illustration_availability_v1(uuid,uuid,text,bytea,bigint,integer,bigint,text,jsonb,jsonb,text,text)",
                        ))
                        after15 = await _definition_hashes(connection)
                        assert all(after15.get(k) == value for k, value in after14.items()), (
                            "SQL015 may only add media functions, not replace historical handlers"
                        )

                        await connection.execute(files[16].read_text(), prepare=False)
                        await _must_exist(connection, relations=(
                            "story_reconciliation_cursor", "story_reconciliation_intents",
                        ), functions=(
                            "check_story_reconcile_v1(bytea,jsonb,jsonb,text)",
                            "apply_story_reconcile_v1(bytea,jsonb,jsonb,text)",
                            "read_story_reconciliation_v1(text,uuid)",
                        ))
                        after16 = await _definition_hashes(connection)
                        assert all(after16.get(k) == value for k, value in after15.items()), (
                            "SQL016 must retain exact old Case and media handler definitions"
                        )

                        # Check the two domains coexist with real SQL catalog
                        # FKs, not a made-up stub or an unexecuted migration.
                        edges = await (await connection.execute("""
                            SELECT conrelid::regclass::text, confrelid::regclass::text
                            FROM pg_constraint
                            WHERE contype='f' AND connamespace='rkb_v4'::regnamespace
                        """)).fetchall()
                        edge_set = set(edges)
                        assert ("rkb_v4.source_illustration_candidates",
                                "rkb_v4.source_activation_candidates") in edge_set
                        assert ("rkb_v4.source_illustration_origins",
                                "rkb_v4.sources") in edge_set
                        assert ("rkb_v4.story_reconciliation_cursor",
                                "rkb_v4.sources") in edge_set
                        assert ("rkb_v4.story_reconciliation_intents",
                                "rkb_v4.disagreement_cases") in edge_set, (
                            "Case compatibility cannot be replaced by a synthetic receipt"
                        )

                        # Do not mistake source-only complete for cross-book
                        # complete: independent reconcile cursor starts open.
                        cursor_default = await (await connection.execute("""
                            SELECT column_default FROM information_schema.columns
                            WHERE table_schema='rkb_v4'
                              AND table_name='story_reconciliation_cursor'
                              AND column_name='reconciliation_state'
                        """)).fetchone()
                        assert cursor_default is not None and "'open'" in cursor_default[0]

                        # Real read function with empty scope, no source/corpus.
                        row = await (await connection.execute("""
                            SELECT count(*) FROM rkb_v4.read_source_illustrations_v1(
                                '00000000-0000-0000-0000-000000000001'::uuid,NULL,3)
                        """)).fetchone()
                        assert row == (0,)

                        print("RKB_SQL_013_014_015_016_REAL_PG_PASS", {
                            "ordered_sql": list(range(1, 17)),
                            "source_sha256": EXPECTED_SHA256,
                            "old_function_count": len(old12),
                            "post14_function_count": len(after14),
                            "post15_function_count": len(after15),
                            "post16_function_count": len(after16),
                            "history_preserved": True,
                        })
                        raise ExpectedSavepointRollback()
                except ExpectedSavepointRollback:
                    pass

                # Verify actual PG catalogs after rolling back SQL013–016.
                assert await _definition_hashes(connection) == old12
                for table in ("source_stage_sessions", "source_illustrations",
                              "source_illustration_availability",
                              "story_reconciliation_intents"):
                    row = await (await connection.execute(
                        "SELECT to_regclass(%s)", ("rkb_v4." + table,),
                    )).fetchone()
                    assert row == (None,), f"rolled-back SQL13–16 relation remains: {table}"
                assert (await (await connection.execute(
                    "SELECT to_regprocedure('rkb_v4.check_story_reconcile_v1(bytea,jsonb,jsonb,text)')"
                )).fetchone()) == (None,)
                raise ExpectedOuterRollback()
        except ExpectedOuterRollback:
            pass
        assert (await (await connection.execute(
            "SELECT to_regnamespace('rkb_v4')"
        )).fetchone()) == (None,), "entire disposable fixture must roll back"
    finally:
        await connection.close()

