"""Frozen schema14 native intake: exact per-ordinal durable-outcome boundary.

Test-only leaf for the integrated source candidate; reuse real
KaggleSourceImporter -> NativeSourceIntake -> ControlSpool/RecoveryJournal
and its existing synthetic native/provider receipt fixture. No PG, BGE,
Telegram, remote Dataset calls, secret or new backend client.

This test MUST be selected only in a candidate where the frozen schema14
native intake and test_kaggle_native_source_importer fixture are present.
It is not CI-verified on unrelated main lacking those files.
"""
from __future__ import annotations

import json

import pytest

from regional_knowledge.source_activation import ACTIVATE_ROUTE, MATERIALIZE_ROUTE
from regional_knowledge.source_stage import SEAL_ROUTE, STAGE_ROUTE
from test_kaggle_native_source_importer import (
    native_intake, start, batch, stage, call, finish, pending, state, operation_count,
)


async def _pending_after_applied_but_no_remote_outcome(s, result, *,
                                                       expected_route, expected_ordinal,
                                                       expected_operations):
    """Real controller must NOT advance or publish Saved on PG-only APPLIED."""
    entry = await pending(s, result)
    key = entry.command.operation_key
    original = entry.command_bytes
    assert entry.command.route == expected_route
    if expected_ordinal is not None:
        assert json.loads(entry.command.payload)["ordinal"] == expected_ordinal
    assert operation_count(s) == expected_operations
    # Existing source fixture records a synthetic committed native PG receipt
    # through the real ControlSpool; publish=False withholds remote outcome.
    await finish(s, result, publish=False, total_inputs=expected_ordinal or 2)
    applied = await s.spool.get(key)
    assert applied.apply_receipt is not None
    assert applied.outcome_state != "DURABLE"
    assert state(s, result)["pending_operation_key"] == key

    receipt = await call(s, result, "status")
    assert receipt.state == "processing", receipt
    assert operation_count(s) == expected_operations
    assert state(s, result)["pending_operation_key"] == key
    assert (await pending(s, result)).command_bytes == original
    assert (await pending(s, result)).outcome_state != "DURABLE"
    # A status poll is not permission to create a new stable ordinal/sequence.
    assert not any(e.command.route == ACTIVATE_ROUTE for e in (
        await s.spool.get(k) for k in _keys(s)))
    return key


def _keys(s):
    with s.t.queue.connect(write=False) as db:
        return [row[0] for row in db.execute(
            "SELECT operation_key FROM rkb_control_operations ORDER BY sequence")]


@pytest.mark.asyncio
@pytest.mark.parametrize("total_inputs", (2, 3))
async def test_native_ordinal_progress_waits_for_remote_outcome_not_only_applied(
    native_intake, total_inputs,
):
    """CURRENT behavior: per-ordinal DURABLE barrier, not a new Dataset upload.

    At two/three ordinals, the serial intake has respectively 5/6 distinct
    durable outcome gates (stage+seal+N materialize+activate). These are
    logical coverage barriers, NOT proof of 5/6 actual Dataset versions.
    """
    s = native_intake
    result = await start(s)
    model = batch(s, result, count=1)
    await stage(s, result, model)
    first = await pending(s, result)
    assert first.command.route == STAGE_ROUTE
    assert operation_count(s) == 1
    await finish(s, result, publish=False)
    assert (await call(s, result, "status")).state == "processing"
    assert state(s, result)["batch_count"] == 0
    assert operation_count(s) == 1
    await s.spool.publish_outcome(first.command.operation_key, writer_generation="writer-1")
    assert (await call(s, result, "status")).state == "staged"
    assert state(s, result)["batch_count"] == 1

    assert (await call(s, result, "validate")).state == "processing"
    sealed = await pending(s, result)
    assert sealed.command.route == SEAL_ROUTE
    assert operation_count(s) == 2
    await finish(s, result, publish=False)
    assert (await call(s, result, "status")).state == "processing"
    assert state(s, result)["phase"] == "staging"
    assert operation_count(s) == 2
    await s.spool.publish_outcome(sealed.command.operation_key, writer_generation="writer-1")
    assert (await call(s, result, "status")).state == "ready"
    assert state(s, result)["phase"] == "sealed"

    assert (await call(s, result, "finalize")).state == "processing"
    for ordinal in range(total_inputs):
        entry = await pending(s, result)
        assert entry.command.route == MATERIALIZE_ROUTE
        assert json.loads(entry.command.payload)["ordinal"] == ordinal
        key = entry.command.operation_key
        exact_command = entry.command_bytes
        commands_before = operation_count(s)
        assert commands_before == ordinal + 3
        # APPLIED/PERSISTENCE_PENDING: real native controller does not advance.
        await finish(s, result, publish=False, total_inputs=total_inputs)
        assert (await call(s, result, "status")).state == "processing"
        assert state(s, result)["next_ordinal"] == ordinal
        assert state(s, result)["pending_operation_key"] == key
        assert operation_count(s) == commands_before
        assert (await pending(s, result)).command_bytes == exact_command
        # Only verified remote outcome frees this ordinal for the next one.
        await s.spool.publish_outcome(key, writer_generation="writer-1")
        next_response = await call(s, result, "status")
        assert next_response.state == "processing"
        assert state(s, result)["next_ordinal"] == ordinal + 1
        assert operation_count(s) == commands_before + 1
        next_entry = await pending(s, result)
        if ordinal + 1 < total_inputs:
            assert next_entry.command.route == MATERIALIZE_ROUTE
            assert json.loads(next_entry.command.payload)["ordinal"] == ordinal + 1
        else:
            assert next_entry.command.route == ACTIVATE_ROUTE
            assert state(s, result)["phase"] == "ready_activation"

    activation = await pending(s, result)
    assert activation.command.route == ACTIVATE_ROUTE
    await finish(s, result, publish=False, total_inputs=total_inputs)
    assert (await call(s, result, "status")).state == "processing"
    assert state(s, result)["phase"] == "ready_activation"
    assert operation_count(s) == total_inputs + 3
    assert state(s, result)["pending_operation_key"] == activation.command.operation_key
    await s.spool.publish_outcome(
        activation.command.operation_key, writer_generation="writer-1")
    terminal = await call(s, result, "status")
    assert terminal.state == "finalized"
    assert state(s, result)["phase"] == "finalized"
    assert state(s, result)["next_ordinal"] == total_inputs
    assert operation_count(s) == total_inputs + 3
    assert len(set(_keys(s))) == total_inputs + 3
    assert all((await s.spool.get(k)).outcome_state == "DURABLE" for k in _keys(s))
    assert (await call(s, result, "finalize")).state == "finalized"
    assert operation_count(s) == total_inputs + 3
    # No native output is described as a Telegram send/receipt.
    assert all((await s.spool.get(k)).command.route in
               {STAGE_ROUTE, SEAL_ROUTE, MATERIALIZE_ROUTE, ACTIVATE_ROUTE}
               for k in _keys(s))
