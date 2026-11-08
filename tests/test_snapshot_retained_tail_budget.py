"""Fictional journal bytes only; no corpus, provider, PostgreSQL or model."""
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
    tail = [item async for item in recovery(journal)._tail(SimpleNamespace(reference=journal.ref(1861)), None, 0)]
    assert journal.total_bytes >= 2 * 43059574  # validation pass plus streaming replay
    assert [entry[0].sequence for entry in tail] == list(range(1, 1862))
    assert all(entry[0].payload == b'{"synthetic":"' + b'x' * 18000 + b'"}' for entry in tail)
    assert SnapshotLimits().timeout_seconds == 900
    print({'bytes': journal.total_bytes, 'commands': len(tail), 'elapsed_seconds': time.perf_counter() - started})


@pytest.mark.asyncio
async def test_tail_gap_is_detected_before_streaming_replay():
    journal = SyntheticTail(2)
    original = journal.read_segment

    async def gapped(ref):
        previous, raws = await original(ref)
        return previous, (command(4).encode(),) if ref.last_sequence == 1 else raws

    journal.read_segment = gapped
    emitted=[]
    with pytest.raises(SnapshotError, match='snapshot_tail_gap'):
        async for item in recovery(journal)._tail(SimpleNamespace(reference=journal.ref(2)), None, 0):
            emitted.append(item)
    assert emitted==[]
