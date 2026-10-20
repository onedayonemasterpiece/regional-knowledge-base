"""Real psycopg async tests; explicit fixed disposable Unix socket or existing CI service only."""
import asyncio
import json
import os
from pathlib import Path
import subprocess

import psycopg
from psycopg.conninfo import conninfo_to_dict
from psycopg.pq import TransactionStatus
from psycopg.rows import tuple_row
import pytest
import pytest_asyncio

from regional_knowledge.kaggle_search import fused_search

ROOT = Path(__file__).resolve().parents[1]
CONTAINER = "rkb-v4-fixture-20261020"
SOCKET = Path("/home/dev/artifacts/regional-knowledge-base/"
              "20261020T110555Z-kaggle-implementation-20261020/pg/socket")
pytestmark = pytest.mark.skipif(
    os.environ.get("RKB_PG_DRIVER_FIXTURE_SOCKET") != str(SOCKET)
    and not os.environ.get("RKB_KAGGLE_TEST_DSN"),
    reason="explicit exact disposable socket opt-in required")
VECTOR = [1.] + [0.] * 1023
BODY = "semantic harbour " + "x" * 5000
BIBLIOGRAPHY = "fixture book " + "b" * 5000


_SOCKET_FD = None
_CI_PARAMS = None


async def connect():
    if _CI_PARAMS is not None:
        return await psycopg.AsyncConnection.connect(
            **_CI_PARAMS, connect_timeout=2, sslmode="disable", row_factory=tuple_row,
            application_name="rkb_disposable_driver_fixture")
    assert _SOCKET_FD is not None
    assert Path(os.readlink(f"/proc/self/fd/{_SOCKET_FD}")).resolve() == SOCKET
    return await psycopg.AsyncConnection.connect(
        host=f"/proc/self/fd/{_SOCKET_FD}", port=5432, dbname="rkb_fixture", user="postgres",
        password="", connect_timeout=2, sslmode="disable", row_factory=tuple_row,
        application_name="rkb_disposable_driver_fixture")


@pytest_asyncio.fixture
async def database():
    global _SOCKET_FD, _CI_PARAMS
    ci_dsn = os.environ.get("RKB_KAGGLE_TEST_DSN")
    if ci_dsn:
        assert os.environ.get("GITHUB_ACTIONS") == "true"
        assert os.environ.get("GITHUB_REPOSITORY") == "onedayonemasterpiece/regional-knowledge-base"
        expected = {"host": "127.0.0.1", "port": "54329", "dbname": "rkb_graph_test",
                    "user": "postgres", "password": "graph_fixture_only"}
        assert conninfo_to_dict(ci_dsn) == expected
        _CI_PARAMS = expected
        expected_database = "rkb_graph_test"
    else:
        assert os.environ.get("RKB_PG_DRIVER_FIXTURE_SOCKET") == str(SOCKET)
        inspected = subprocess.run(["docker", "inspect", CONTAINER], check=True,
                                   capture_output=True, text=True, timeout=3)
        info = json.loads(inspected.stdout)[0]
        assert info["Config"]["Labels"]["rkb.data_class"] == "test_fixture"
        assert info["HostConfig"]["NetworkMode"] == "none"
        assert info["State"]["Running"]
        assert any(m["Source"] == str(SOCKET) for m in info["Mounts"])
        _SOCKET_FD = os.open(SOCKET, os.O_RDONLY | os.O_DIRECTORY)
        expected_database = "rkb_fixture"
    try:
        admin = await connect()
    except BaseException:
        if _SOCKET_FD is not None:
            os.close(_SOCKET_FD)
            _SOCKET_FD = None
        _CI_PARAMS = None
        raise
    created = False
    try:
        async with admin.transaction():
            row = await (await admin.execute("SELECT current_database(), current_user")).fetchone()
            assert row == (expected_database, "postgres")
            assert (await (await admin.execute("SHOW server_version_num")).fetchone())[0].startswith("17")
            await admin.execute("SET LOCAL statement_timeout = '3s'")
            await admin.execute("SET LOCAL maintenance_work_mem = '8MB'")
            await admin.execute("CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public")
            version = await (await admin.execute(
                "SELECT extversion FROM pg_extension WHERE extname='vector'")).fetchone()
            assert version == ("0.8.2",)
            assert (await (await admin.execute(
                "SELECT to_regnamespace('rkb_v4')")).fetchone()) == (None,)
            await admin.execute((ROOT / "sql/kaggle/001_search.sql").read_text(), prepare=False)
            await admin.execute("INSERT INTO rkb_v4.sources VALUES (%s,%s,true,false)",
                                ("00000000-0000-0000-0000-000000000001", BIBLIOGRAPHY))
            await admin.execute("INSERT INTO rkb_v4.revisions VALUES (%s,1)",
                                ("00000000-0000-0000-0000-000000000001",))
            await admin.execute("INSERT INTO rkb_v4.active_revision VALUES (%s,1)",
                                ("00000000-0000-0000-0000-000000000001",))
            await admin.execute("""INSERT INTO rkb_v4.passage_search
                (passage_id,source_id,revision,ordinal,locator,body,language,config,embedding)
                VALUES (%s,%s,1,0,'page 1',%s,'simple','pg_catalog.simple',%s::vector)""",
                                ("10000000-0000-0000-0000-000000000001",
                                 "00000000-0000-0000-0000-000000000001", BODY,
                                 "[" + ",".join(map(str, VECTOR)) + "]"))
        created = True
        yield admin
    finally:
        await admin.rollback()
        if created:
            async with admin.transaction():
                await admin.execute("SET LOCAL lock_timeout='1s'")
                await admin.execute("DROP SCHEMA rkb_v4 CASCADE")
        await admin.close()
        if _SOCKET_FD is not None:
            os.close(_SOCKET_FD)
            _SOCKET_FD = None
        _CI_PARAMS = None


async def settings(connection):
    assert connection.info.transaction_status == TransactionStatus.IDLE
    async with connection.transaction():
        # Load pgvector in this backend before reading its custom GUC defaults.
        await connection.execute("SELECT vector_dims(array_fill(1.,ARRAY[1024])::vector)")
        result = await (await connection.execute("""SELECT current_setting('statement_timeout'),
            current_setting('hnsw.iterative_scan'), current_setting('hnsw.max_scan_tuples'),
            current_setting('hnsw.ef_search')""")).fetchone()
    assert connection.info.transaction_status == TransactionStatus.IDLE
    return result


async def encode_stub(query):
    return VECTOR


@pytest.mark.asyncio
async def test_actual_fused_search_full_fields_and_pool_settings(database):
    connection = await connect()
    try:
        baseline = await settings(connection)
        result = await fused_search(connection, "no lexical matches", encode=encode_stub)
        assert len(result.evidence) == 1
        assert result.evidence[0].body == BODY
        assert result.evidence[0].bibliography == BIBLIOGRAPHY
        assert result.incomplete
        assert await settings(connection) == baseline
        # A lexical matching request uses the same real connection after commit.
        result = await fused_search(connection, "harbour", encode=encode_stub)
        assert result.evidence[0].locator == "page 1"
        assert await settings(connection) == baseline
        print(json.dumps({"check": "actual_fused_driver", "psycopg": psycopg.__version__,
                          "body_chars": len(result.evidence[0].body),
                          "bibliography_chars": len(result.evidence[0].bibliography),
                          "transaction_idle": True, "settings_restored": True}))
    finally:
        await connection.close()


async def wait_for_lock_wait(observer, pid):
    for _ in range(100):
        async with observer.transaction():
            row = await (await observer.execute(
                "SELECT wait_event_type FROM pg_stat_activity WHERE pid=%s", (pid,))).fetchone()
        if row == ("Lock",):
            return
        await asyncio.sleep(.005)
    raise AssertionError("search never reached the actual PostgreSQL lock wait")


@pytest.mark.asyncio
@pytest.mark.parametrize("caller_cancel", [False, True])
async def test_actual_locked_query_cancel_cleanup_and_connection_reuse(database, caller_cancel):
    connection = await connect()
    blocker = await connect()
    task = None
    try:
        baseline = await settings(connection)
        await blocker.execute("LOCK TABLE rkb_v4.passage_search IN ACCESS EXCLUSIVE MODE")
        task = asyncio.create_task(fused_search(
            connection, "harbour", encode=encode_stub,
            timeout_seconds=1 if caller_cancel else .25))
        await wait_for_lock_wait(database, connection.info.backend_pid)
        if caller_cancel:
            task.cancel()
        with pytest.raises(asyncio.CancelledError if caller_cancel else TimeoutError):
            await task
        # Rollback has completed on return while the independent blocker remains.
        assert connection.info.transaction_status == TransactionStatus.IDLE
        assert await settings(connection) == baseline
        async with connection.transaction():
            assert (await (await connection.execute("SELECT 42")).fetchone()) == (42,)
        await blocker.rollback()
        result = await fused_search(connection, "harbour", encode=encode_stub)
        assert result.evidence[0].body == BODY
        assert await settings(connection) == baseline
        print(json.dumps({"check": "actual_pg_lock_cancellation",
                          "mode": "caller_cancel" if caller_cancel else "deadline",
                          "lock_wait_observed": True, "transaction_idle": True,
                          "settings_restored": True, "subsequent_search_succeeded": True}))
    finally:
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await blocker.rollback()
        await blocker.close()
        await connection.close()
