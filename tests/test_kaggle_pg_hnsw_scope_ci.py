"""Real PG17/pgvector0.8.2 HNSW SET LOCAL scope of the CURRENT fused_search.

CI uses the EXISTING mandatory ci.yml PG service + tests/test_kaggle_pg_driver.py
guarded database/connect/settings fixtures; no alternate runner, DSN, PG server,
model, provider or Dev smoke is provided. Not the old batched-GUC HNSW patch.
"""
from __future__ import annotations

import os

from psycopg.pq import TransactionStatus
import pytest

from regional_knowledge.kaggle_search import fused_search
from test_kaggle_pg_driver import BODY, database, connect, encode_stub, settings

# In ordinary Dev with no explicit existing safe PG opt-in, this module is
# skipped. In GitHub Actions, a missing/changed RKB_KAGGLE_TEST_DSN is a
# hard fixture error, NOT a fabricated PASS/skip.
pytestmark = pytest.mark.skipif(
    os.environ.get("GITHUB_ACTIONS") != "true" and not (
        os.environ.get("RKB_PG_DRIVER_FIXTURE_SOCKET")
        or os.environ.get("RKB_KAGGLE_TEST_DSN")),
    reason="existing explicitly opted-in disposable PG fixture required")


async def observed_transaction_gucs(cursor, execute):
    """Read real PG settings while current query holds the SAME transaction.

    The production execute callback refreshes statement_timeout before each
    SQL; this observer never sets a GUC or bypasses the query deadline.
    """
    await execute("""SELECT
        current_setting('hnsw.iterative_scan'),
        current_setting('hnsw.max_scan_tuples'),
        current_setting('hnsw.ef_search'),
        current_setting('transaction_read_only'),
        current_setting('transaction_isolation'),
        (SELECT setting::integer FROM pg_settings WHERE name='statement_timeout'),
        pg_backend_pid()""")
    row = await cursor.fetchone()
    assert len(row) == 7
    return row


@pytest.mark.asyncio
async def test_current_fused_search_real_pg_local_hnsw_timeout_and_same_connection(database):
    """Current 3x SET LOCAL visible in PG during query, gone before same PID reuse."""
    connection = await connect()
    try:
        baseline = await settings(connection)
        pid = connection.info.backend_pid
        phases = []

        async def snapshot_guard(cursor, execute):
            # Production calls guard after REPEATABLE READ / READ ONLY and
            # before the current three HNSW SET LOCAL statements.
            phases.append(("pre_hnsw", await observed_transaction_gucs(cursor, execute)))

        async def snapshot_finish(cursor, execute, result):
            assert len(result.evidence) == 1
            phases.append(("with_hnsw", await observed_transaction_gucs(cursor, execute)))
            # A bounded real SQL wait to prove the absolute remaining timeout
            # is refreshed by the *existing* per-statement execute wrapper.
            await execute("SELECT pg_sleep(0.035)")
            await cursor.fetchone()
            phases.append(("after_wait", await observed_transaction_gucs(cursor, execute)))

        result = await fused_search(connection, "harbour", encode=encode_stub,
                                    snapshot_guard=snapshot_guard,
                                    snapshot_finish=snapshot_finish,
                                    timeout_seconds=1.5)
        assert len(result.evidence) == 1 and result.evidence[0].body == BODY
        assert [name for name, _ in phases] == [
            "pre_hnsw", "with_hnsw", "after_wait"]
        before, during, later = (item for _, item in phases)
        assert before[:3] == baseline[1:]
        assert before[3:5] == ("on", "repeatable read")
        assert during[:3] == later[:3] == ("strict_order", "1000", "100")
        assert during[3:5] == later[3:5] == ("on", "repeatable read")
        # The statement_timeout is expressed in integer milliseconds by the
        # actual pg_settings table, not a textual SHOW parser or fake cursor.
        assert 0 < later[5] < during[5] <= before[5] <= 1500
        assert during[5] - later[5] >= 20
        assert before[6] == during[6] == later[6] == pid
        assert connection.info.transaction_status == TransactionStatus.IDLE
        assert await settings(connection) == baseline
        # Recheck the SAME physical backend/session; no reconnection, no
        # GUC drift, no schema mutation.
        assert connection.info.backend_pid == pid
        result2 = await fused_search(connection, "harbour", encode=encode_stub)
        assert result2 == result
        assert connection.info.backend_pid == pid
        assert await settings(connection) == baseline
    finally:
        await connection.close()


@pytest.mark.asyncio
async def test_current_fused_search_real_pg_post_hnsw_failure_rolls_back_and_reuses(database):
    """A failure after native HNSW setup cannot leak transaction-local knobs."""
    connection = await connect()
    try:
        baseline = await settings(connection)
        pid = connection.info.backend_pid
        seen = []

        async def reject_after_guc(cursor, execute, result):
            snapshot = await observed_transaction_gucs(cursor, execute)
            seen.append(snapshot)
            assert snapshot[:3] == ("strict_order", "1000", "100")
            assert snapshot[3:5] == ("on", "repeatable read")
            assert 0 < snapshot[5] <= 2000
            raise RuntimeError("synthetic source evidence gate rejected after real SQL")

        with pytest.raises(RuntimeError, match="synthetic source evidence gate rejected"):
            await fused_search(connection, "harbour", encode=encode_stub,
                               snapshot_finish=reject_after_guc)
        assert len(seen) == 1 and seen[0][6] == pid
        # psycopg's real transaction context has completed rollback before
        # this await returns; the same session is safely reusable.
        assert connection.info.transaction_status == TransactionStatus.IDLE
        assert await settings(connection) == baseline
        assert connection.info.backend_pid == pid
        read = await fused_search(connection, "harbour", encode=encode_stub)
        assert len(read.evidence) == 1 and read.evidence[0].body == BODY
        assert connection.info.transaction_status == TransactionStatus.IDLE
        assert connection.info.backend_pid == pid
        assert await settings(connection) == baseline
    finally:
        await connection.close()
