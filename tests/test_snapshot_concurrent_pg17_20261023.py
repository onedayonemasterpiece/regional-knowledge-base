"""Real PG17 physical WAL backup with concurrent source apply and exact replay.

Uses ONLY the pre-existing disposable GitHub Actions PG17 service. No Kaggle,
DevCoveer PG, provider, credentials, live import or additional deployment.
The restored PostgreSQL child lives inside that ephemeral CI service container
and is stopped after the test. All accepted commands use the existing exact
apply_accepted / UNIQUE applied_ops path.
"""
from __future__ import annotations

import asyncio
import os
import re
from uuid import uuid4

import psycopg
import pytest

from regional_knowledge.kaggle_pg_snapshot_native import (
    SnapshotLimits, applied_prefix,
)
from test_kaggle_apply import env, item, accepted, apply, facts

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.skipif(
        not os.environ.get("RKB_PG17_CONTAINER"),
        reason="Only the checked existing GitHub Actions PG17 CI service is permitted",
    ),
]


async def _docker(*args, timeout=35):
    proc = await asyncio.create_subprocess_exec(
        "docker", *args, stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    out, err = await asyncio.wait_for(proc.communicate(), timeout)
    assert proc.returncode == 0, (
        "disposable CI Docker command failed: "
        + err.decode(errors="replace")[-1500:]
    )
    return out.decode().strip()


def _existing_ci_container():
    from psycopg.conninfo import conninfo_to_dict
    assert os.environ.get("GITHUB_ACTIONS") == "true"
    assert os.environ.get("GITHUB_REPOSITORY") == "onedayonemasterpiece/regional-knowledge-base"
    assert conninfo_to_dict(os.environ["RKB_KAGGLE_TEST_DSN"]) == dict(
        host="127.0.0.1", port="54329", dbname="rkb_graph_test",
        user="postgres", password="graph_fixture_only",
    )
    cid = os.environ["RKB_PG17_CONTAINER"]
    assert re.fullmatch("[a-f0-9]{64}", cid), "explicit existing CI service container required"
    return cid


async def _wait_until_basebackup_started(connection, proc):
    # The source must already be in PostgreSQL's actual BASE_BACKUP phase
    # before the second exact accepted command is committed.
    for _ in range(180):
        assert proc.returncode is None, "backup exited before concurrent write"
        async with connection.transaction():
            n = (await (await connection.execute(
                "SELECT count(*) FROM pg_stat_progress_basebackup"
            )).fetchone())[0]
        if n:
            return
        await asyncio.sleep(.05)
    raise AssertionError("physical base backup did not enter PostgreSQL replication phase")


async def test_wal_backup_contains_concurrent_commit_and_replays_tail_once(env):
    cid = _existing_ci_container()
    source = env["connection"]
    first = item(1)
    first_seal = await accepted(env, first)
    assert not (await apply(env, first, first_seal)).duplicate
    assert (await facts(source))[0][0] == 1

    # This is the verified lower-bound cursor before the physical backup.
    saved_seq = 1
    async with source.transaction():
        prefix = await applied_prefix(source, saved_seq, SnapshotLimits())
    child = None
    archive_dir = "/tmp/rkb-concurrent-" + uuid4().hex
    restore_started = False
    try:
        child = await asyncio.create_subprocess_exec(
            "docker", "exec", "--user", "postgres",
            "--env", "PGPASSWORD=graph_fixture_only", cid,
            "pg_basebackup", "-h", "127.0.0.1", "-p", "5432",
            "-U", "postgres", "-D", archive_dir, "-X", "stream",
            "--checkpoint=fast", "--manifest-checksums=SHA256",
            "--max-rate=4096", "--no-password",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        await _wait_until_basebackup_started(source, child)
        second = item(2, revision="1")
        second_seal = await accepted(env, second)
        second_outcome = await apply(env, second, second_seal)
        assert not second_outcome.duplicate
        assert (await facts(source))[0][0] == 2

        out, err = await asyncio.wait_for(child.communicate(), 150)
        assert child.returncode == 0, (
            "physical base backup failed: " + err.decode(errors="replace")[-1500:]
        )
        await _docker(
            "exec", "--user", "postgres", cid,
            "pg_verifybackup", archive_dir,
        )
        # Post-backup history is accepted remotely but NOT in this archive.
        third = item(3, revision="2")
        third_seal = await accepted(env, third)

        # Restore the exact WAL-consistent snapshot as a separate child of the
        # SAME disposable CI service, never into the original live PGDATA.
        await _docker(
            "exec", "--user", "postgres", cid, "pg_ctl",
            "-D", archive_dir, "-l", archive_dir + "/restore.log",
            "-o", "-p 55441 -h 0.0.0.0 -k /tmp -c archive_mode=off",
            "-w", "start", timeout=40,
        )
        restore_started = True
        ips = await _docker(
            "inspect", "--format",
            "{{range .NetworkSettings.Networks}}{{.IPAddress}} {{end}}", cid,
        )
        ip = ips.split()[0]
        assert re.fullmatch(r"(?:[0-9]{1,3}\.){3}[0-9]{1,3}", ip)
        restored = await psycopg.AsyncConnection.connect(
            host=ip, port=55441, user="postgres",
            password="graph_fixture_only", dbname="rkb_graph_test",
            connect_timeout=4, sslmode="disable",
            application_name="rkb_snapshot_lower_bound_ci_fixture",
        )
        try:
            async with restored.transaction():
                restored_seq = (await (await restored.execute(
                    "SELECT applied_seq FROM rkb_v4.recovery_state WHERE singleton"
                )).fetchone())[0]
                restored_prefix = await applied_prefix(
                    restored, saved_seq, SnapshotLimits()
                )
            assert restored_seq == 2 and restored_seq > saved_seq
            assert restored_prefix == prefix

            # Ordinary authenticated accepted-command replay traverses the
            # journal tail (saved_seq, head] through the existing executor.
            # Seq2 is already in the physical snapshot: UNIQUE applied_ops
            # restores its exact original receipt with duplicate=True.
            repeated = await apply(env, second, second_seal, connection=restored)
            assert repeated.duplicate and repeated.outcome == second_outcome.outcome
            assert (await facts(restored))[:3] == (
                (2, "s1", "f1", "e1"), 2, 2
            )
            advanced = await apply(env, third, third_seal, connection=restored)
            again = await apply(env, third, third_seal, connection=restored)
            assert not advanced.duplicate and again.duplicate
            assert advanced.outcome == again.outcome
            assert (await facts(restored))[:3] == (
                (3, "s1", "f1", "e1"), 3, 3
            )
        finally:
            await restored.close()
    finally:
        if child is not None and child.returncode is None:
            child.kill()
            await child.wait()
        if restore_started:
            await _docker(
                "exec", "--user", "postgres", cid, "pg_ctl",
                "-D", archive_dir, "-m", "immediate", "-w", "stop",
                timeout=25,
            )
        # This private /tmp directory belongs solely to the disposable CI
        # service and disappears when GitHub tears down its fixture container.
