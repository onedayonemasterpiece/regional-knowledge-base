"""Generate public deployment function pins on the disposable GitHub PG17 only.

This measures definitions, not accepted migrations or live catalog/ACL parity.
No product configuration, identities or provider credentials are inputs.
"""
import asyncio
from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path
import sys

import psycopg

from regional_knowledge.kaggle_native_source_upgrade import (
    ARTIFACTS, NativeSourceUpgradePins, transition_definitions, transition_sql,
)

ROOT = Path(__file__).resolve().parents[2]
DSN = "postgresql://postgres:graph_fixture_only@127.0.0.1:54329/rkb_graph_test"


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


async def definitions(connection):
    rows = await (await connection.execute("""
        SELECT p.proname, pg_get_function_identity_arguments(p.oid),
               pg_get_functiondef(p.oid)
        FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
        WHERE n.nspname='rkb_v4' AND p.prokind='f'
        ORDER BY p.proname, pg_get_function_identity_arguments(p.oid)
    """)).fetchall()
    return {f"{name}({args})": sha(body.encode("utf-8")) for name, args, body in rows}


async def command_pin(connection, name):
    row = await (await connection.execute(
        "SELECT pg_get_functiondef(to_regprocedure(%s))",
        (f"rkb_v4.{name}(bytea,jsonb,jsonb,text)",),
    )).fetchone()
    if row is None or row[0] is None:
        raise RuntimeError(f"required function absent: {name}")
    return sha(row[0].encode("utf-8"))


async def generate():
    if (os.environ.get("GITHUB_ACTIONS") != "true"
            or os.environ.get("GITHUB_REPOSITORY") != "onedayonemasterpiece/regional-knowledge-base"
            or os.environ.get("RKB_KAGGLE_TEST_DSN") != DSN):
        raise RuntimeError("exact disposable GitHub database required")
    sql = ROOT / "sql/kaggle"
    artifacts = {}
    async with await psycopg.AsyncConnection.connect(DSN, autocommit=True, connect_timeout=3) as c:
        assert (await (await c.execute("SHOW server_version_num")).fetchone())[0].startswith("17")
        assert (await (await c.execute("SELECT current_database(),current_user")).fetchone()) == ("rkb_graph_test", "postgres")
        assert (await (await c.execute("SELECT to_regnamespace('rkb_v4')")).fetchone()) == (None,)
        await c.execute("SET statement_timeout='15s'")
        await c.execute("CREATE EXTENSION vector WITH SCHEMA public")
        assert (await (await c.execute("SELECT extversion FROM pg_extension WHERE extname='vector'")).fetchone()) == ("0.8.2",)
        async with c.transaction():
            for number in range(1, 15):
                paths = sorted(sql.glob(f"{number:03d}_*.sql"))
                if len(paths) != 1:
                    raise RuntimeError(f"exact SQL artifact required: {number}")
                path = paths[0]
                raw = path.read_bytes()
                artifacts[path.name] = sha(raw)
                await c.execute(raw.decode("utf-8"), prepare=False)
                if number == 11:
                    original = await definitions(c)
                if number == 12:
                    geo = {
                        "artifact_sha256": sha(raw),
                        "transition_check_sha256": await command_pin(c, "check_geo_dependencies_v2_transition"),
                        "transition_apply_sha256": await command_pin(c, "apply_geo_dependencies_v2_transition"),
                        "geo_check_sha256": await command_pin(c, "check_geo_manifest_install_v2"),
                        "geo_apply_sha256": await command_pin(c, "apply_geo_manifest_install_v2"),
                    }
            # transition_sql uses only the two artifact hashes via pins.manifest.
            # Constructor placeholders are never output as measured function pins.
            seed = NativeSourceUpgradePins(*(artifacts[n] for n in ARTIFACTS), "0" * 64, "0" * 64)
            await c.execute(transition_sql(seed), prepare=False)
            check, apply = await transition_definitions(c)
            native = replace(seed, transition_check_sha256=check, transition_apply_sha256=apply)
            final = await definitions(c)
            assert original and all(final.get(key) == value for key, value in original.items())
            version = (await (await c.execute("SELECT version()")).fetchone())[0]
        return {
            "format": "rkb-migration-function-pins-v1",
            "source_commit": os.environ["GITHUB_SHA"],
            "github_run_id": os.environ["GITHUB_RUN_ID"],
            "postgres": version, "pgvector": "0.8.2",
            "artifacts": artifacts,
            "schema11_function_definitions": original,
            "geo_upgrade_function_pins": geo,
            "native_source_upgrade": native.wire(),
            "schema14_function_definitions": final,
            "deployment_status": "requires_exact_live_v1_comparison_and_review",
            "origin_catalog_acl": "must_use_verified_live_origin_not_fixture_catalog",
        }


if __name__ == "__main__":
    result = asyncio.run(generate())
    Path(sys.argv[1]).write_text(json.dumps(result, sort_keys=True, indent=2) + "\n")
    print("Measured schema11/12/14 function definitions; live comparison and review still required.")
