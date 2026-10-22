"""Version compatibility and the actual native restore migration boundary."""
from contextlib import asynccontextmanager
import os
from types import SimpleNamespace as NS

import psycopg
import pytest

from regional_knowledge.kaggle_pg_snapshot_native import NativeSnapshotFactory, compatible_versions
from regional_knowledge.kaggle_schema_migrate import bundle_migrations, migrate
from regional_knowledge.kaggle_database_probe import NativeDatabaseProbe
from test_kaggle_schema_migrate import files


def test_old_dataset_versions_ignore_catalog_acl_and_minor_pg_but_keep_runtime_abi():
    old=dict(server_version_num=170006,pgvector_version='0.8.2',model_sha256='a'*64,
        tokenizer_sha256='b'*64,catalog_sha256='old catalog',acl_sha256='old ACL',
        schema_revision='rkb-v4-sql001-011-v1',function_revision='old',executor_revision='old')
    current=dict(server_version_num=170011,pgvector_version='0.8.2',model_sha256='a'*64,
        tokenizer_sha256='b'*64,schema_version=16)
    assert compatible_versions(old,current)
    for update in ({'model_sha256':'c'*64},{'tokenizer_sha256':'c'*64},
                   {'protocol_version':2},{'postgis_version':'3.5'}):
        assert not compatible_versions(old,current | update)


@pytest.mark.asyncio
@pytest.mark.skipif(not os.getenv('RKB_KAGGLE_TEST_DSN'),reason='existing PG17 CI service only')
async def test_production_fresh_schema_sequence_creates_roles_before_sql_grants():
    from dataclasses import replace
    from regional_knowledge.kaggle_owned_pg import OwnedPostgres
    from test_kaggle_target_runtime import spec
    from test_kaggle_public_illustration_native_read import _validated_ci_dsn
    connection=await psycopg.AsyncConnection.connect(_validated_ci_dsn(),autocommit=True)
    owned=False
    try:
        assert await (await connection.execute("SELECT to_regnamespace('rkb_v4')")).fetchone()==(None,)
        assert await (await connection.execute("SELECT rolname FROM pg_roles WHERE rolname IN ('rkb_bootstrap','rkb_writer','rkb_reader')")).fetchall()==[]
        # Represents the sole identity made by initdb, not the application roles.
        await connection.execute('CREATE ROLE rkb_bootstrap SUPERUSER NOLOGIN')
        owned=True
        await connection.execute('SET ROLE rkb_bootstrap')
        owner=OwnedPostgres(replace(spec(fresh=True).database,dbname='rkb_graph_test'))
        artifacts={m.name:m.body for m in bundle_migrations(files())}
        await owner._initialize_schema(connection,artifacts)
        assert await (await connection.execute('SELECT max(version) FROM rkb_v4.schema_migrations')).fetchone()==(len(artifacts),)
        assert await (await connection.execute("SELECT rolname,rolsuper,rolcreaterole,rolcreatedb FROM pg_roles WHERE rolname IN ('rkb_writer','rkb_reader') ORDER BY rolname")).fetchall()==[
            ('rkb_reader',False,False,False),('rkb_writer',False,False,False)]
        assert await (await connection.execute("SELECT has_table_privilege('rkb_reader','rkb_v4.schema_migrations','SELECT'),has_table_privilege('rkb_writer','rkb_v4.schema_migrations','INSERT')")).fetchone()==(True,False)
    finally:
        await connection.execute('RESET ROLE')
        if owned:
            await connection.execute('DROP SCHEMA IF EXISTS rkb_v4 CASCADE')
            await connection.execute('DROP OWNED BY rkb_bootstrap,rkb_writer,rkb_reader')
            await connection.execute('DROP ROLE rkb_writer,rkb_reader,rkb_bootstrap')
        await connection.close()


@pytest.mark.asyncio
@pytest.mark.skipif(not os.getenv('RKB_KAGGLE_TEST_DSN'),reason='existing PG17 CI service only')
async def test_native_restore_n_minus_two_migrates_before_tail_and_readiness():
    """Runs real PG DDL/probe, not the physical archive/provider boundary."""
    from test_kaggle_public_illustration_native_read import _validated_ci_dsn
    connection=await psycopg.AsyncConnection.connect(_validated_ci_dsn(),autocommit=True)
    owned=False
    try:
        assert await (await connection.execute("SELECT to_regnamespace('rkb_v4')")).fetchone()==(None,)
        assert await (await connection.execute("SELECT rolname FROM pg_roles WHERE rolname='rkb_writer'")).fetchall()==[]
        owned=True
        await connection.execute('CREATE ROLE rkb_writer NOLOGIN')
        await connection.execute('CREATE ROLE rkb_reader NOLOGIN')
        await connection.execute('CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public')
        artifacts=files()
        older={name:body for name,body in artifacts.items() if int(name[4:7])<=len(artifacts)-2}
        assert await migrate(connection,older)==tuple(range(1,len(artifacts)-1))
        await connection.execute("INSERT INTO rkb_v4.writer_generations(generation) VALUES('old')")
        await connection.execute("""INSERT INTO rkb_v4.recovery_state
            (singleton,database_generation,writer_generation,applied_seq,schema_revision,function_revision,executor_revision)
            VALUES(true,'restore-fixture','old',20,'rkb-v4-sql001-014-v1','old','old')""")
        await connection.execute("SELECT rkb_v4.publish_read_frontier('restore-fixture','old',0,20,true,true)")
        @asynccontextmanager
        async def acquire():yield connection
        factory=object.__new__(NativeSnapshotFactory)
        factory.database=NS(schema_version=len(artifacts),database_generation='restore-fixture')
        factory.schema_files=artifacts
        factory.probe=NativeDatabaseProbe()
        manifest=dict(versions={'schema_version':len(artifacts)-2},applied_sequence=20)
        assert (await factory.probe(acquire=acquire))[1]['schema_version']==len(artifacts)-2
        # This is the same awaited boundary _prepare uses before constructing
        # the executor or allowing PhysicalSnapshotRecovery to replay its tail.
        await factory._upgrade_restored(NS(acquire=acquire),manifest)
        _,ready=await factory.probe(acquire=acquire)
        assert ready['schema_version']==len(artifacts) and ready['applied_seq']==20
        assert ready['text_search_ready'] and ready['dense_search_ready']
        assert await (await connection.execute(
            'SELECT version,name,sha256 FROM rkb_v4.schema_migrations WHERE version>%s ORDER BY version',(len(artifacts)-2,))).fetchall()==[
                (m.version,m.name,m.sha256) for m in bundle_migrations(artifacts)[-2:]]
        assert await migrate(connection,artifacts)==()
    finally:
        if owned:
            await connection.execute('DROP SCHEMA IF EXISTS rkb_v4 CASCADE')
            await connection.execute('DROP ROLE rkb_writer')
            await connection.execute('DROP ROLE rkb_reader')
        await connection.close()
