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
from mcp.server.mcpserver.exceptions import ToolError

from regional_knowledge.source_activation import ACTIVATE_ROUTE, MATERIALIZE_ROUTE
from regional_knowledge.source_stage import SEAL_ROUTE, STAGE_ROUTE
from test_kaggle_native_source_importer import (
    native_intake, start, batch, stage, call, finish, pending, state, operation_count,
)
from test_kaggle_public_backend import target


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
    entries = [await s.spool.get(k) for k in _keys(s)]
    assert all(entry.outcome_state == "DURABLE" for entry in entries)
    assert (await call(s, result, "finalize")).state == "finalized"
    assert operation_count(s) == total_inputs + 3
    # No native output is described as a Telegram send/receipt.
    entries = [await s.spool.get(k) for k in _keys(s)]
    assert all(entry.command.route in
               {STAGE_ROUTE, SEAL_ROUTE, MATERIALIZE_ROUTE, ACTIVATE_ROUTE}
               for entry in entries)


@pytest.mark.asyncio
@pytest.mark.parametrize('barrier', ('unpublished_outcome', 'history_facts_pending', 'intake_coverage'))
async def test_stage_exact_has_its_own_pending_and_coverage_allocation_guards(
    native_intake, barrier,
):
    """Changing only _drive cannot bypass scope/facts or native intake coverage.

    This characterizes the current source, not a proposed permission to proceed
    with uncovered effects. Real spool metadata is produced by actual apply and
    publication operations. Only the existing synthetic GEO proof seam changes.
    """
    from regional_knowledge.kaggle_native_source_bindings import resolved_native_stage_command
    from regional_knowledge.source_stage import digest

    s = native_intake
    result = await start(s)
    await stage(s, result, batch(s, result, count=1))
    first = await pending(s, result)
    await finish(s, result, publish=barrier != 'unpublished_outcome')
    applied = await s.spool.get(first.command.operation_key)
    assert applied.apply_state == 'APPLIED'
    if barrier == 'unpublished_outcome':
        assert applied.outcome_state != 'DURABLE'
        scope = await s.spool.scope_state()
        facts = await s.spool.read_facts(None)
        assert scope['pending'] is True and facts.pending is True
        assert scope['unknown'] is False and facts.unknown is False
        expected_reason = 'prior_write_pending'
    else:
        assert applied.outcome_state == 'DURABLE'
        await call(s, result, 'status')
        assert (await s.spool.scope_state())['pending'] is False
        if barrier == 'history_facts_pending':
            from regional_knowledge.control_spool import SourceChange
            # Synthetic native-history observation enters the actual compact
            # recovery projection. No read_facts method or flag is replaced.
            s.spool._replace_source_heads([SourceChange(result.document_id,
                first.command.sequence, first.command_sha256, False, True, False)])
            facts = await s.spool.read_facts(None)
            assert (await s.spool.scope_state())['pending'] is False
            assert facts.pending and not facts.unknown
            assert facts.scope_sequence <= facts.covered_sequence
            assert s.spool.facts_current(facts)
            assert s.importer._intake_coverage_current(s.startup.binding)
        else:
            s.geo_state['current'] = False
            assert not s.importer._intake_coverage_current(s.startup.binding)
        expected_reason = 'source_history_unavailable'

    row = s.importer._row(s.principal.subject, result.ingestion_id)
    native = s.importer.native_intake
    payload = json.loads(first.command.payload)
    payload.update(batch_index=1, previous_batch_sha256=digest(first.command.payload))
    before = operation_count(s)
    # Direct allocation entry bypasses _drive entirely and still refuses.
    async with s.importer._access(s.principal) as access:
        with pytest.raises(ToolError, match=expected_reason):
            await native._stage_exact(access, row, kind='stage',
                key=native._operation(row, 'stage', 1), route=STAGE_ROUTE,
                payload=payload,
                builder=lambda **values: resolved_native_stage_command(route=STAGE_ROUTE, **values))
    assert operation_count(s) == before
    assert (await s.spool.get(first.command.operation_key)).command_bytes == first.command_bytes
    assert state(s, result)['phase'] != 'finalized'
