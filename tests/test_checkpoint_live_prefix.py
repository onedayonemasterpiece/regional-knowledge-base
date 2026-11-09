"""Existing checkpoint path with fictional provider/native I/O only."""
from types import SimpleNamespace as NS

import pytest

from test_kaggle_dataset_checkpoint import control_cycle, put_control
from test_control_spool import env, people


@pytest.mark.asyncio
async def test_capture_uses_current_proof_after_outcome_preparation(tmp_path, monkeypatch):
    from regional_knowledge.kaggle_dataset_checkpoint import _WorkCycleCapture
    p = control_cycle(tmp_path, monkeypatch)
    await put_control(p)
    p.clock[0] = 1801
    original = _WorkCycleCapture._prepare_prefix

    async def prepare(batch, proof):
        await original(batch, proof)
        # Ordinary status refresh reconstructs coverage during awaited I/O.
        p.startup.coverage = NS(**vars(proof))

    monkeypatch.setattr(_WorkCycleCapture, '_prepare_prefix', prepare)
    assert await p.owner.refresh(p.binding)
    assert p.calls == ['capture', 'finalize', 'verify']


@pytest.mark.asyncio
async def test_prepared_outcome_is_not_recreated_before_capture(tmp_path, env):
    from regional_knowledge.kaggle_dataset_checkpoint import _WorkCycleCapture
    from regional_knowledge.recovery_journal import RecoveryJournal
    from regional_knowledge.kaggle_pg_snapshot_native import sha
    from test_dataset_recovery_store import prepared
    from test_control_spool import receipt
    p = prepared(tmp_path)
    journal = RecoveryJournal(p.store, stream='stream')
    command = env.cmd
    seal = await journal.seal([command], predecessor=None, writer_generation='writer')
    native = receipt(command, database='db')
    with p.q.connect() as db:
        db.execute('CREATE TABLE rkb_control_operations(operation_key TEXT,sequence INTEGER,seal_state TEXT,'
                   'apply_state TEXT,outcome_state TEXT,outcome_sha256 TEXT)')
        db.execute('INSERT INTO rkb_control_operations VALUES(?,?,?,?,?,?)',
                   (command.operation_key, 1, 'ACCEPTED', 'APPLIED', 'NOT_SENT', native.outcome_sha256))
    calls = []
    async def get(key):
        calls.append(key)
        return NS(command=command, accepted=seal, apply_receipt=native, database_generation='db')
    proof = NS(current=lambda: True, accepted_sequence=1, applied_sequence=1)
    batch = _WorkCycleCapture.__new__(_WorkCycleCapture)
    batch.queue = p.q
    batch.startup = NS(config=NS(writer_generation='writer', spool=NS(journal=journal,
        get=get, _source_covered_sequence=1)), restorer=NS(physical=NS(limits=NS(max_receipts=100))))
    await batch._prepare_prefix(proof)
    assert batch._prefix_ready(proof)
    calls.clear()
    await batch._prepare_prefix(proof)
    assert calls == [], 'Already staged exact outcomes must not repeat journal verification'
    key = journal.prefix + '/outcomes/' + sha(command.operation_key.encode())
    with p.q.connect() as db:
        db.execute('UPDATE rkb_dataset_recovery_objects SET data=? WHERE key=?',
                   ('{"operation_key":"wrong","sequence":1,"outcome_sha256":"wrong"}', key))
    assert not batch._prefix_ready(proof), 'Existing final identity checks must still reject corruption'
