"""Existing GitHub PG17 service: actual schema11→12→14 isolated-drill probe.

This is a disposable fixture, NOT Kaggle, real Dataset restore or real book.
It exercises the real NativeDatabaseProbe/inspect_acl and original ledger.
"""
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest

from regional_knowledge.kaggle_apply import initialize_database,HandlerRegistry
from regional_knowledge.geo_manifest_commands import (
    GEO_REVISIONS,geo_dependency_transition_handler)
from regional_knowledge.kaggle_native_source_upgrade import (
    NativeSourceUpgradePins,native_source_upgrade_handler,
    transition_sql,transition_definitions,sha)
from regional_knowledge.kaggle_pg_snapshot_native import SnapshotLimits,SnapshotError,applied_prefix
from regional_knowledge.kaggle_snapshot_drill_migrate import NativeDrillMigrate
from regional_knowledge.kaggle_database_probe import native_probe_for_schema
from regional_knowledge.kaggle_source_acl import inspect_acl
from test_kaggle_apply import PG,pinned
from test_kaggle_pg_driver import database

ROOT=Path(__file__).resolve().parents[1]
SQL=ROOT/"sql"/"kaggle"
GEN="drill-migrate-pg-fixture"
WRITER="drill-migrate-pg-writer"

class FixtureRollback(Exception):
    pass


@PG
@pytest.mark.asyncio
async def test_real_pg17_native_drill_catalog_acl_and_original_state(database):
    conn=database
    roles=[]
    try:
        async with conn.transaction():
            await conn.execute((SQL/"002_recovery.sql").read_text(),prepare=False)
        await initialize_database(conn,database_generation=GEN,writer_generation=WRITER,
            revisions=GEO_REVISIONS)
        async with conn.transaction():
            for ordinal in range(3,12):
                files=list(SQL.glob(f"{ordinal:03d}_*.sql"))
                assert len(files)==1
                await conn.execute(files[0].read_text(),prepare=False)
            await conn.execute(
                "SELECT rkb_v4.publish_read_frontier(%s,%s,0,0,true,true)",(GEN,WRITER))
            for role in ("rkb_bootstrap","rkb_writer","rkb_reader"):
                assert await(await conn.execute("SELECT 1 FROM pg_roles WHERE rolname=%s",
                    (role,))).fetchone() is None
                await conn.execute("CREATE ROLE "+role+" NOLOGIN")
                roles.append(role)

        sql12=(SQL/"012_geo_dependency_bound.sql").read_bytes()
        sql13=(SQL/"013_source_stage.sql").read_bytes()
        sql14=(SQL/"014_source_stage_activation.sql").read_bytes()
        seed=NativeSourceUpgradePins(sha(sql13),sha(sql14),"1"*64,"2"*64)
        with pytest.raises(FixtureRollback):
            async with conn.transaction():
                for raw in (sql12,sql13,sql14):
                    await conn.execute(raw.decode(),prepare=False)
                await conn.execute(transition_sql(seed),prepare=False)
                geo_check=await pinned(conn,"check_geo_dependencies_v2_transition")
                geo_apply=await pinned(conn,"apply_geo_dependencies_v2_transition")
                native_pins=NativeSourceUpgradePins(sha(sql13),sha(sql14),
                    *(await transition_definitions(conn)))
                raise FixtureRollback()
        registry=HandlerRegistry([
            geo_dependency_transition_handler(artifact_sha256=sha(sql12),
                check_sha256=geo_check.definition_sha256,
                apply_sha256=geo_apply.definition_sha256),
            native_source_upgrade_handler(native_pins),
        ])
        wire=SimpleNamespace(socket="/synthetic/drill/socket",port=5432)
        factory=SimpleNamespace(database=wire,limits=SnapshotLimits(),
            _owned_candidates=set())
        server=SimpleNamespace(database=SimpleNamespace(
            socket="/synthetic/serving/socket",database_generation=GEN),
            registry=registry,_current=lambda binding:None)
        owner=SimpleNamespace(serving=server,factory=factory)
        native=NativeDrillMigrate(owner,sql_root=SQL,native_pins=native_pins)
        class Candidate:
            database=wire
            process=SimpleNamespace(returncode=None)
            @asynccontextmanager
            async def acquire(self):
                yield conn
        candidate=Candidate()
        factory._owned_candidates.add(candidate)
        observation=await native.measure_candidate(candidate,("run","inc",1),
            base_sequence=0,base_writer=WRITER)
        assert observation["schema_revision"]=="rkb-v4-sql001-014-v1"
        assert observation["base_sequence"]==0 and observation["original_source_count"]==1
        assert len(observation["catalog_sha256"])==len(observation["acl_sha256"])==64
        assert observation["migration_id"]=="native-source-stage-activation-v1"
        probe=native_probe_for_schema("rkb-v4-sql001-014-v1",timeout_seconds=5)
        catalog,fields=await probe(acquire=candidate.acquire)
        assert sha(catalog)==observation["catalog_sha256"]
        assert fields["applied_seq"]==0 and fields["projection_seq"]==0
        async with conn.transaction():
            raw_acl=await inspect_acl(conn)
            assert sha(raw_acl)==observation["acl_sha256"]
            assert await applied_prefix(conn,0,factory.limits)==observation[
                "original_applied_prefix_sha256"]
            assert await(await conn.execute(
                "SELECT count(*) FROM rkb_v4.sources")).fetchone()==(1,)
    finally:
        await conn.rollback()
        if roles:
            async with conn.transaction():
                await conn.execute("DROP OWNED BY "+",".join(roles))
                for role in reversed(roles):
                    await conn.execute("DROP ROLE "+role)
