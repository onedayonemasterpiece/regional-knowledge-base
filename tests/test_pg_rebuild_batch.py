"""One offline replay regression, using a synthetic journal and real disposable PG."""
import json
import hashlib
import sqlite3
import os
import subprocess
import base64
import gzip
import shutil
import time

import psycopg
from dataclasses import replace

import pytest

from regional_knowledge.pg_rebuild_batch import ReplayRecord, SnapshotInput, rebuild
from regional_knowledge.pg_rebuild_part import import_objects, part_connection
from regional_knowledge.kaggle_apply import apply_accepted
from test_kaggle_apply import PG, env, accepted, item
from test_kaggle_pg_driver import CONTAINER, database, connect


@PG
@pytest.mark.asyncio
async def test_offline_sql_uses_part_deadline_and_preserves_constraints(database):
    # A live request deadline cancels work even when the finite part has time.
    async with database.transaction():
        await database.execute("SET LOCAL statement_timeout='1ms'")
        with pytest.raises(psycopg.errors.QueryCanceled):
            await database.execute('SELECT pg_sleep(0.03)')
    marks = dict(sql_execute_seconds=0., sql_execute_calls=0)
    bounded = part_connection(time.monotonic()+2, marks)
    async with await connect(bounded.connect) as connection:
        async with connection.transaction():
            await connection.execute("SET LOCAL statement_timeout='1ms'")
            await connection.execute('SELECT pg_sleep(0.03)')
        async with connection.transaction():
            await connection.execute('CREATE TEMP TABLE checked (n int CHECK(n>0))')
            with pytest.raises(psycopg.errors.CheckViolation):
                await connection.execute('INSERT INTO checked VALUES (%s)', (-1,))
    expired = part_connection(time.monotonic()-1, marks)
    async with await connect(expired.connect) as connection:
        async with connection.transaction():
            await connection.execute("SET LOCAL statement_timeout='5s'")
            with pytest.raises(psycopg.errors.QueryCanceled):
                await connection.execute('SELECT pg_sleep(0.03)')


@PG
@pytest.mark.asyncio
async def test_batch_preserves_correction_rejection_unknown_and_idempotency(env, monkeypatch, tmp_path):
    async def no_network(*args, **kwargs):
        raise AssertionError("offline replay attempted network access")

    monkeypatch.setattr("asyncio.open_connection", no_network)
    first = item(1)
    correction = replace(item(2, revision="1"), ids=first.ids,
                         payload=b'{"body":"corrected accepted body"}')
    rejected = replace(item(3, revision="999"), ids=first.ids)
    unknown = replace(item(4, revision="2"), ids=first.ids)
    not_sent = replace(item(5, revision="2"), ids=first.ids)
    records = []
    for cmd in (first, correction, rejected, unknown, not_sent):
        seal = await accepted(env, cmd)
        if cmd in (unknown, not_sent):
            records.append(ReplayRecord(cmd, seal,
                "SENT_UNKNOWN" if cmd is unknown else "NOT_SENT", None))
        else:
            receipt = await apply_accepted(env["connection"], journal=env["journal"],
                seal=seal, command=cmd, database_generation="db-1", writer_generation="writer-1",
                registry=env["registry"])
            state = "APPLIED" if receipt.status == "applied" else "REJECTED_PRECONDITION"
            records.append(ReplayRecord(cmd, seal, state, receipt.outcome))
    # Same external format as the private SQLite projection, including one retired raw command.
    metadata = tmp_path / "synthetic-metadata.sqlite"
    with sqlite3.connect(metadata) as snapshot:
        snapshot.execute("""CREATE TABLE rkb_control_operations(sequence, operation_key,
            command_bytes, command_sha256, seal_state, seal_bytes, apply_state,
            outcome_bytes, outcome_sha256, remote_receipt_bytes)""")
        for record in records:
            command = record.command
            snapshot.execute("INSERT INTO rkb_control_operations VALUES(?,?,?,?,?,?,?,?,?,?)",
                (command.sequence, command.operation_key,
                 None if command is first else command.encode(),
                 hashlib.sha256(command.encode()).hexdigest(), "ACCEPTED",
                 json.dumps(dict(manifest=record.seal.manifest.wire(),
                                 command_hashes=record.seal.command_hashes)),
                 record.state, record.outcome,
                 hashlib.sha256(record.outcome).hexdigest() if record.outcome else None, None))
    inputs = tmp_path / "inputs"
    inputs.mkdir()
    with metadata.open("rb") as source, gzip.open(inputs / "input-metadata.sqlite.gz", "wb") as target:
        shutil.copyfileobj(source, target)
    with gzip.open(inputs / "journal-objects.jsonl.gz", "wt") as output:
        for key, obj in env["journal"].store.objects.items():
            output.write(json.dumps(dict(storage="fixture", key=key, token=obj.token,
                data_b64=base64.b64encode(obj.data).decode())) + "\n")
    path = tmp_path / "synthetic-input.sqlite"
    import_objects(inputs, path)
    # Empty only the synthetic domain/receipt tables; preserve trusted function installation.
    async with env["connection"].transaction():
        await env["connection"].execute("TRUNCATE rkb_v4.applied_ops, rkb_v4.apply_fixture_items")
        await env["connection"].execute("UPDATE rkb_v4.recovery_state SET applied_seq=0")
        await env["connection"].execute("UPDATE rkb_v4.apply_fixture_calls SET n=0")

    prepared = []
    cancel_prepare = False
    deadline_expired = False

    async def prepare(cmd):
        nonlocal deadline_expired
        prepared.append(cmd.sequence)
        if cancel_prepare:
            try:
                async with env['connection'].transaction():
                    await env['connection'].execute("SET LOCAL statement_timeout='1ms'")
                    await env['connection'].execute('SELECT pg_sleep(0.03)')
            finally:
                deadline_expired = True

    async def resolve(cmd):
        return env["registry"]

    async def run(keep_going=lambda: True):
        source = SnapshotInput(path, storage="fixture", stream=env["journal"].stream)
        try:
            return await rebuild(env["connection"], journal=source.journal, records=source.records(),
                database_generation="db-1", writer_generation="writer-1",
                resolve=resolve, prepare=prepare, keep_going=keep_going)
        finally:
            source.close()

    partial = await run(lambda: len(prepared) < 1)
    assert partial == {"APPLIED": 1, "REJECTED_PRECONDITION": 0, "NOT_SENT": 0,
                       "SENT_UNKNOWN": 0, "restored_prefix": 1, "next_sequence": 2,
                       "accepted_last_sequence": 5}
    cancel_prepare = True
    with pytest.raises(psycopg.errors.QueryCanceled):
        await run()  # An SQL timeout while the part still has time remains a failure.
    deadline_expired = False
    assert await run(lambda: not deadline_expired) == partial
    cancel_prepare = False
    # A real PG dump/restore of this tiny synthetic checkpoint, not copied Python rows.
    container = os.environ.get("RKB_PG17_CONTAINER", CONTAINER)
    endpoint = [] if os.environ.get("RKB_PG17_CONTAINER") else ["-h", "/socket"]
    args = endpoint + ["-U", "postgres", "-d", env["connection"].info.dbname]
    checkpoint = tmp_path / "synthetic-prefix.dump"
    with checkpoint.open("wb") as output:
        subprocess.run(["docker", "exec", container, "pg_dump", *args,
                        "--format=custom", "--schema=rkb_v4"], stdout=output,
                       stderr=subprocess.PIPE, check=True, timeout=30)
    assert 0 < checkpoint.stat().st_size < 4 * 1024 * 1024
    async with env["connection"].transaction():
        await env["connection"].execute("DROP SCHEMA rkb_v4 CASCADE")
    with checkpoint.open("rb") as source:
        subprocess.run(["docker", "exec", "-i", container, "pg_restore", *args,
                        "--exit-on-error", "--no-owner"], stdin=source,
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True, timeout=30)
    async with env["connection"].transaction():
        assert await (await env["connection"].execute(
            "SELECT applied_seq FROM rkb_v4.recovery_state")).fetchone() == (1,)
    report = await run()
    assert report == {"APPLIED": 2, "REJECTED_PRECONDITION": 1, "NOT_SENT": 1,
                      "SENT_UNKNOWN": 1, "restored_prefix": 3}
    assert prepared == [1, 2, 2, 2]
    assert await run() == report
    assert prepared == [1, 2, 2, 2]
    async with env["connection"].transaction():
        assert await (await env["connection"].execute(
            "SELECT revision, body FROM rkb_v4.apply_fixture_items")).fetchone() == (
                2, "corrected accepted body")
        assert await (await env["connection"].execute(
            "SELECT n FROM rkb_v4.apply_fixture_calls")).fetchone() == (2,)
        receipts = await (await env["connection"].execute(
            "SELECT outcome_bytes FROM rkb_v4.applied_ops ORDER BY sequence")).fetchall()
    assert [bytes(row[0]) for row in receipts] == [r.outcome for r in records[:3]]
    assert json.loads(bytes(receipts[2][0]))["status"] == "rejected_precondition"
