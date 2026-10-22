"""Real existing GH PostgreSQL17 comparison: drill observation == accepted transition.

Never connects to Dev/Kaggle PG. Both runs use the SAME disposable CI service,
same SQL bytes, same role closure and native read-only catalog/ACL probes.
The existing native schema12 fixture really applies GEO accepted commands; its
source upgrade goes through NativeSourceMigration.prepare and apply_accepted.
"""
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest

from regional_knowledge.kaggle_apply import initialize_database,HandlerRegistry
from regional_knowledge.geo_manifest_commands import GEO_REVISIONS
from regional_knowledge.kaggle_native_source_upgrade import (
    NativeSourceUpgradePins,NativeSourceMigration,native_source_upgrade_handler,
    resolved_native_source_upgrade,sha)
from regional_knowledge.kaggle_pg_snapshot_native import SnapshotLimits
from regional_knowledge.kaggle_snapshot_drill_migrate import NativeDrillMigrate
from regional_knowledge.kaggle_database_probe import native_probe_for_schema
from regional_knowledge.kaggle_source_acl import inspect_acl
from test_kaggle_apply import PG
from test_native_source_upgrade_pg import schema12,preflight,apply,DB,WRITER
from test_kaggle_pg_driver import database  # transitive real PG17 pytest fixture
from test_geo_migration import uid

ROOT=Path(__file__).resolve().parents[1]
SQL=ROOT/"sql"/"kaggle"


@PG
@pytest.mark.asyncio
async def test_real_accepted_schema14_catalog_acl_equals_isolated_drill_shape(schema12):
    env=schema12
    connection=env["connection"]
    roles=[]
    try:
        async with connection.transaction():
            for role in ("rkb_bootstrap","rkb_writer","rkb_reader"):
                assert await(await connection.execute(
                    "SELECT 1 FROM pg_roles WHERE rolname=%s",(role,))).fetchone() is None
                await connection.execute("CREATE ROLE "+role+" NOLOGIN")
                roles.append(role)
        pins=await preflight(connection)
        native=native_source_upgrade_handler(pins)
        registry=HandlerRegistry([*env["registry"].handlers.values(),native])
        env["registry"]=registry
        command=resolved_native_source_upgrade(pins=pins,
            sequence=env["sequence"]+1,operation_key="actual-native-schema14",
            request_id="gh-drill-native-accepted-request",
            person_id=uid(1),logical_time="2026-10-22T15:00:00Z")
        migration=NativeSourceMigration(sql_root=SQL,registry=registry,
            database_generation=DB,pins=pins)
        await migration.prepare(connection,command,native)
        applied=await apply(env,command)
        assert applied.status=="applied"
        @asynccontextmanager
        async def acquire():
            yield connection
        probe=native_probe_for_schema("rkb-v4-sql001-014-v1",timeout_seconds=5)
        native_catalog,fields=await probe(acquire=acquire)
        assert fields["schema_revision"]=="rkb-v4-sql001-014-v1"
        async with connection.transaction():
            native_acl=await inspect_acl(connection)

        # Reset only the test fixture schema; accepted original was observed.
        # No Kaggle PG or production data is touched by this test cleanup.
        async with connection.transaction():
            await connection.execute("DROP SCHEMA rkb_v4 CASCADE")
            await connection.execute((SQL/"001_search.sql").read_text(),prepare=False)
            await connection.execute((SQL/"002_recovery.sql").read_text(),prepare=False)
        await initialize_database(connection,database_generation=DB,
            writer_generation=WRITER,revisions=GEO_REVISIONS)
        async with connection.transaction():
            for ordinal in range(3,12):
                files=list(SQL.glob(f"{ordinal:03d}_*.sql"))
                assert len(files)==1
                await connection.execute(files[0].read_text(),prepare=False)
            await connection.execute(
                "SELECT rkb_v4.publish_read_frontier(%s,%s,0,0,true,true)",(DB,WRITER))

        wire=SimpleNamespace(socket="/gh/disposable-drill/socket",port=5432)
        factory=SimpleNamespace(database=wire,limits=SnapshotLimits(),
            _owned_candidates=set())
        serving=SimpleNamespace(database=SimpleNamespace(database_generation=DB,
            socket="/gh/serving/socket"),registry=registry,_current=lambda binding:None)
        owner=SimpleNamespace(serving=serving,factory=factory)
        drill=NativeDrillMigrate(owner,sql_root=SQL,native_pins=pins)
        class IsolatedFixtureCandidate:
            process=SimpleNamespace(returncode=None)
            database=wire
            @asynccontextmanager
            async def acquire(self):
                yield connection
        candidate=IsolatedFixtureCandidate()
        factory._owned_candidates.add(candidate)
        measured=await drill.measure_candidate(candidate,("gh","fixture",1),
            base_sequence=0,base_writer=WRITER)
        assert measured["catalog_sha256"]==sha(native_catalog)
        assert measured["acl_sha256"]==sha(native_acl)
        assert measured["base_sequence"]==0 and measured["schema_revision"]=="rkb-v4-sql001-014-v1"
        assert measured["migration_sha256"]==pins.artifact_sha256
    finally:
        await connection.rollback()
        if roles:
            async with connection.transaction():
                await connection.execute("DROP OWNED BY "+",".join(roles))
                for role in reversed(roles):
                    await connection.execute("DROP ROLE "+role)
