"""One offline replay regression, using a synthetic journal and real disposable PG."""
import json
import hashlib
import sqlite3
from dataclasses import replace

import pytest

from regional_knowledge.pg_rebuild_batch import ReplayRecord, SnapshotInput, rebuild
from regional_knowledge.kaggle_apply import apply_accepted
from test_kaggle_apply import PG, env, accepted, item
from test_kaggle_pg_driver import database


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
    path = tmp_path / "synthetic-input.sqlite"
    with sqlite3.connect(path) as snapshot:
        snapshot.execute("CREATE TABLE rkb_dataset_recovery_objects(storage,key,data,token)")
        snapshot.executemany("INSERT INTO rkb_dataset_recovery_objects VALUES(?,?,?,?)",
            [("fixture", key, obj.data, obj.token)
             for key, obj in env["journal"].store.objects.items()])
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
    # Empty only the synthetic domain/receipt tables; preserve trusted function installation.
    async with env["connection"].transaction():
        await env["connection"].execute("TRUNCATE rkb_v4.applied_ops, rkb_v4.apply_fixture_items")
        await env["connection"].execute("UPDATE rkb_v4.recovery_state SET applied_seq=0")
        await env["connection"].execute("UPDATE rkb_v4.apply_fixture_calls SET n=0")

    prepared = []

    async def prepare(cmd):
        prepared.append(cmd.sequence)

    async def resolve(cmd):
        return env["registry"]

    async def run():
        source = SnapshotInput(path, storage="fixture", stream=env["journal"].stream)
        try:
            return await rebuild(env["connection"], journal=source.journal, records=source.records(),
                database_generation="db-1", writer_generation="writer-1",
                resolve=resolve, prepare=prepare)
        finally:
            source.close()

    report = await run()
    assert report == {"APPLIED": 2, "REJECTED_PRECONDITION": 1, "NOT_SENT": 1,
                      "SENT_UNKNOWN": 1, "restored_prefix": 3}
    assert prepared == [1, 2]
    assert await run() == report
    assert prepared == [1, 2]
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
