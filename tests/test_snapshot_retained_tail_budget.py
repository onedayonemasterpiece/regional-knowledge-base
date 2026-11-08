"""Fictional journal bytes only; no corpus, provider, PostgreSQL or model."""
from dataclasses import replace
from types import SimpleNamespace
import time

import pytest

from regional_knowledge.kaggle_pg_snapshot_native import SnapshotLimits, SnapshotError
from regional_knowledge.pg_snapshot_recovery import PhysicalSnapshotRecovery
from regional_knowledge.recovery_journal import ObjectRef
from test_recovery_journal import command


class SyntheticTail:
    def __init__(self, count):
        self.count = count
        self.total_bytes = 0

    def ref(self, sequence):
        return ObjectRef('synthetic/' + str(sequence), 'a' * 64, 1, sequence)

    async def read_segment(self, ref):
        sequence = ref.last_sequence
        raw = command(sequence, payload=b'{"synthetic":"' + b'x' * 18000 + b'"}').encode()
        self.total_bytes += len(raw)
        return self.ref(sequence - 1) if sequence > 1 else None, (raw,)


def recovery(journal, limits=SnapshotLimits()):
    return PhysicalSnapshotRecovery(journal, None, None, registry=None, history_verifier=None,
        maintenance_guard=lambda _: None, current=lambda _: True,
        private_storage_current=lambda: True, limits=limits)


@pytest.mark.asyncio
async def test_retained_1861_command_tail_fits_bounded_default_without_dropping_bytes():
    journal = SyntheticTail(1861)
    started = time.perf_counter()
    tail = await recovery(journal)._tail(SimpleNamespace(reference=journal.ref(1861)), None, 0)
    assert 43059574 <= journal.total_bytes <= SnapshotLimits().max_tail_bytes
    assert [entry[0].sequence for entry in tail] == list(range(1, 1862))
    assert all(entry[0].payload == b'{"synthetic":"' + b'x' * 18000 + b'"}' for entry in tail)
    assert SnapshotLimits().max_tail_commands == 2048
    assert SnapshotLimits().timeout_seconds == 900
    print({'bytes': journal.total_bytes, 'commands': len(tail), 'elapsed_seconds': time.perf_counter() - started})


@pytest.mark.asyncio
@pytest.mark.parametrize('limits', [replace(SnapshotLimits(), max_tail_commands=1),
                                  replace(SnapshotLimits(), max_tail_bytes=30000)])
async def test_tail_command_and_byte_guards_still_reject_over_budget(limits):
    journal = SyntheticTail(2)
    with pytest.raises(SnapshotError, match='snapshot_replay_budget'):
        await recovery(journal, limits)._tail(SimpleNamespace(reference=journal.ref(2)), None, 0)
