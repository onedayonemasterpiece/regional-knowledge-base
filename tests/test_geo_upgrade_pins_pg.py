"""Compute SQL012 definition pins in the existing GitHub PG17 fixture.

This receipt proves source compatibility only; it never attests a live run.
"""
import hashlib
import json
import os
from pathlib import Path

import psycopg
import pytest

from regional_knowledge.geo_manifest_commands import GEO_REVISIONS
from regional_knowledge.kaggle_apply import initialize_database
from test_kaggle_pg_driver import database

ROOT = Path(__file__).resolve().parents[1]
EXPECTED = {
    "check_geo_manifest_install_v2": "659d4a7602d46fa30d463b38e572e61157001027400e58aebd4e5dcc17004777",
    "apply_geo_manifest_install_v2": "bdd2727c634d308b4e2115a684c5f5a9d2bfff5179a6cb9391a56a8ef8c7628a",
    "check_geo_dependencies_v2_transition": "240fbbbc5b01b3511dad3454514ae8b0c28e8a77d0f9b051135df5c079bedc86",
    "apply_geo_dependencies_v2_transition": "21b2ad4a290cba32dc606fbdc4be43a2316863bfe4bb32daa6249b081c1c3f26",
}


async def definitions(connection):
    rows = await (await connection.execute("""
        SELECT p.proname, pg_get_functiondef(p.oid)
        FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
        WHERE n.nspname='rkb_v4' AND p.prokind='f'
        ORDER BY p.proname, p.oid::regprocedure::text
    """)).fetchall()
    return {(name, body) for name, body in rows}


@pytest.mark.skipif(
    not os.getenv("RKB_KAGGLE_TEST_DSN"), reason="existing GitHub PG fixture only"
)
@pytest.mark.asyncio
async def test_sql012_definition_pins_and_rollback(database):
    connection = database
    async with connection.transaction():
        await connection.execute((ROOT / "sql/kaggle/002_recovery.sql").read_text(), prepare=False)
    await initialize_database(
        connection, database_generation="geo-pins-fixture",
        writer_generation="geo-pins-writer", revisions=GEO_REVISIONS,
    )
    async with connection.transaction():
        await connection.execute("SET LOCAL statement_timeout='5s'")
        for path in sorted((ROOT / "sql/kaggle").glob("[0-9][0-9][0-9]_*.sql")):
            if 3 <= int(path.name[:3]) <= 11:
                await connection.execute(path.read_text(), prepare=False)
        before = await definitions(connection)
        assert not any(name in EXPECTED for name, _ in before)
    artifact = (ROOT / "sql/kaggle/012_geo_dependency_bound.sql").read_bytes()
    async with connection.transaction():
        await connection.execute("SET LOCAL statement_timeout='5s'")
        await connection.execute(artifact.decode(), prepare=False)
        after = await definitions(connection)
        assert before <= after
        added = after - before
        computed = {name: hashlib.sha256(body.encode()).hexdigest() for name, body in added}
        assert len(added) == 4 and computed == EXPECTED
        raise psycopg.Rollback()
    async with connection.transaction():
        assert await definitions(connection) == before
        state = await (await connection.execute(
            "SELECT schema_revision,function_revision,executor_revision FROM rkb_v4.recovery_state"
        )).fetchone()
        assert state == GEO_REVISIONS.wire()
    receipt = {
        "evidence": "source_fixture_only", "source_sha": os.getenv("GITHUB_SHA"),
        "artifact_sha256": hashlib.sha256(artifact).hexdigest(),
        "function_sha256": computed, "old_definitions_preserved": True,
        "rollback_verified": True, "live_v105_crosscheck": "NOT_RUN",
    }
    # Only public source hashes; no DSN, private catalog or runtime identity.
    summary = os.getenv("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as stream:
            stream.write("\n### SQL012 computed PostgreSQL pins\n\n```json\n")
            stream.write(json.dumps(receipt, sort_keys=True, indent=2))
            stream.write("\n```\n")
