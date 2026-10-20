from __future__ import annotations

import asyncio
import hashlib
import json
from dataclasses import replace

import pytest

from regional_knowledge.recovery_journal import (
    AcceptedSeal, ExactCommand, JournalConflict, JournalCorrupt, JournalLimit,
    JournalUnknown, RecoveryJournal, StaleWriter, StoredObject, StoreUnavailable,
)


class ConditionalMemoryStore:
    """Deterministic native-condition fixture, not a production adapter."""

    def __init__(self):
        self.objects = {}
        self.version = 0
        self.writer = "writer-1"
        self.faults = []
        self.read_failures = set()
        self.reads = []
        self.cas_gate = None
        self.cas_waiters = 0
        self.fail_head_read_after_cas = False

    def write(self, key, data):
        self.version += 1
        self.objects[key] = StoredObject(data, str(self.version))

    def fault(self, stage, key):
        for index, (expected, suffix) in enumerate(self.faults):
            if expected == stage and suffix in key:
                self.faults.pop(index)
                raise StoreUnavailable("synthetic lost ACK")

    async def read(self, key, *, max_bytes):
        self.reads.append(key)
        if any(suffix in key for suffix in self.read_failures):
            raise StoreUnavailable("synthetic read outage")
        item = self.objects.get(key)
        if item is not None and len(item.data) > max_bytes:
            raise JournalLimit("bounded fixture read")
        return item

    async def put_if_absent(self, key, data):
        self.fault("put-before", key)
        inserted = key not in self.objects
        if inserted:
            self.write(key, data)
        self.fault("put-after", key)
        return inserted

    async def compare_and_swap(self, key, data, *, expected_token, writer_generation):
        if self.cas_gate is not None:
            self.cas_waiters += 1
            if self.cas_waiters == 2:
                self.cas_gate.set()
            await self.cas_gate.wait()
        # Fence and CAS are evaluated at the same atomic effect boundary.
        if self.writer != writer_generation:
            raise StaleWriter("synthetic retired writer")
        self.fault("cas-before", key)
        item = self.objects.get(key)
        if (item.token if item else None) != expected_token:
            return False
        self.write(key, data)
        if self.fail_head_read_after_cas:
            self.read_failures.add("/head")
        self.fault("cas-after", key)
        return True


def command(sequence=1, **changes):
    fields = dict(
        sequence=sequence, operation_key=f"request-{sequence}/apply", request_id=f"request-{sequence}",
        person_id="verified-person", route="personal", logical_time="2026-10-20T11:00:00Z",
        schema_revision="schema-v1", function_revision="functions-v1", executor_revision="executor-v1",
        ids={"variant_id": f"chosen-variant-{sequence}"}, expected_revisions={"story": "7"},
        payload=b'{"body":"exact previously selected model body"}',
    )
    fields.update(changes)
    return ExactCommand(**fields)


def fixture(**limits):
    store = ConditionalMemoryStore()
    return store, RecoveryJournal(store, stream="synthetic-only", **limits)


async def seal(journal, commands, predecessor=None, writer="writer-1"):
    return await journal.seal(commands, predecessor=predecessor, writer_generation=writer)


@pytest.mark.asyncio
async def test_exact_group_roundtrip_duplicate_and_continuity():
    store, journal = fixture()
    commands = [command(), command(2, kind="schema_transition", payload=b"exact migration-v2 SQL")]
    receipt = await seal(journal, commands)
    original_head = await journal.head()
    assert await seal(journal, commands) == receipt
    await seal(journal, [command(3)], original_head)
    assert await seal(journal, commands) == receipt  # Lost ACK followed by a later publication.
    assert (await journal.head()).reference.last_sequence == 3
    await journal.verify_chain(await journal.head())
    encoded = commands[0].encode()
    persisted = [item.data for key, item in store.objects.items() if "/commands/" in key]
    assert encoded in persisted
    assert receipt.command_hashes[0] == hashlib.sha256(encoded).hexdigest()
    assert json.loads(encoded)["payload_length"] == len(commands[0].payload)
    previous, restored = await journal.read_segment(receipt.manifest)
    assert previous is None
    assert restored == tuple(item.encode() for item in commands)


@pytest.mark.asyncio
@pytest.mark.parametrize("stage,target", [
    ("put-after", "/commands/"), ("put-after", "/segments/"),
    ("put-after", "/manifests/"), ("cas-after", "/head"),
])
async def test_lost_write_ack_returns_only_after_exact_remote_readback(stage, target):
    store, journal = fixture()
    store.faults.append((stage, target))
    receipt = await seal(journal, [command()])
    assert receipt.manifest == (await journal.head()).reference
    assert not store.faults


@pytest.mark.asyncio
@pytest.mark.parametrize("target", ["/commands/", "/segments/", "/manifests/"])
async def test_crash_before_upload_keeps_pending_and_exact_retry_completes(target):
    store, journal = fixture()
    store.faults.append(("put-before", target))
    with pytest.raises(JournalUnknown):
        await seal(journal, [command()])
    assert await journal.head() is None
    # Surviving command/segment objects alone are never accepted.
    receipt = await seal(journal, [command()])
    assert receipt.manifest == (await journal.head()).reference


@pytest.mark.asyncio
async def test_crash_after_seal_before_cas_does_not_accept_or_allow_payload_drift():
    store, journal = fixture()
    store.faults.append(("cas-before", "/head"))
    with pytest.raises(JournalUnknown):
        await seal(journal, [command()])
    assert await journal.head() is None
    with pytest.raises(JournalConflict):
        await seal(journal, [command(payload=b"different accepted model choice")])
    await seal(journal, [command()])


@pytest.mark.asyncio
async def test_unreadable_upload_ack_is_unknown_even_if_remote_bytes_exist():
    store, journal = fixture()
    store.read_failures.add("/segments/")
    with pytest.raises(JournalUnknown):
        await seal(journal, [command()])
    assert await journal.head() is None
    assert any("/segments/" in key for key in store.objects)
    store.read_failures.clear()
    await seal(journal, [command()])


@pytest.mark.asyncio
async def test_lost_cas_ack_and_unavailable_readback_remain_unknown_until_reconciled():
    store, journal = fixture()
    store.faults.append(("cas-after", "/head"))
    store.fail_head_read_after_cas = True
    with pytest.raises(JournalUnknown):
        await seal(journal, [command()])
    assert journal.head_key in store.objects  # Publication happened, but no receipt was issued.
    store.read_failures.clear()
    store.fail_head_read_after_cas = False
    receipt = await seal(journal, [command()])
    assert receipt.manifest == (await journal.head()).reference


@pytest.mark.asyncio
async def test_cas_race_has_exactly_one_winner_and_never_promotes_orphan():
    store, journal = fixture()
    store.cas_gate = asyncio.Event()
    candidates = [command(operation_key="a"), command(operation_key="b")]
    results = await asyncio.gather(*(seal(journal, [candidate]) for candidate in candidates),
                                   return_exceptions=True)
    assert sum(isinstance(item, AcceptedSeal) for item in results) == 1
    assert sum(isinstance(item, JournalConflict) for item in results) == 1
    head = await journal.head()
    winner = next(item for item in results if isinstance(item, AcceptedSeal))
    assert head.reference == winner.manifest
    assert len([key for key in store.objects if "/segments/" in key]) == 2
    assert head.reference.last_sequence == 1


@pytest.mark.asyncio
async def test_stale_writer_is_fenced_at_cas_even_with_uploaded_objects():
    store, journal = fixture()
    store.writer = "writer-2"
    with pytest.raises(StaleWriter):
        await seal(journal, [command()])
    assert await journal.head() is None
    await seal(journal, [command()], writer="writer-2")


@pytest.mark.asyncio
@pytest.mark.parametrize("change", [
    {"payload": b"changed"}, {"ids": {"variant_id": "new-model-choice"}},
    {"logical_time": "2026-10-21"}, {"executor_revision": "executor-v2"},
    {"expected_revisions": {"story": "8"}},
])
async def test_same_key_with_changed_exact_identity_conflicts(change):
    _, journal = fixture()
    await seal(journal, [command()])
    with pytest.raises(JournalConflict):
        await seal(journal, [command(**change)])


@pytest.mark.asyncio
async def test_operation_key_cannot_be_resequenced_in_later_segment():
    _, journal = fixture()
    await seal(journal, [command()])
    with pytest.raises(JournalConflict):
        await seal(journal, [replace(command(), sequence=2)], await journal.head())
    assert (await journal.head()).reference.last_sequence == 1


@pytest.mark.asyncio
async def test_hole_overlap_duplicate_key_and_budgets_fail_before_effects():
    store, journal = fixture(max_commands=2, max_segment_bytes=1200)
    for commands, error in [
        ([command(2)], JournalConflict),
        ([command(), command(3)], JournalConflict),
        ([command(), command(2, operation_key=command().operation_key)], JournalConflict),
        ([command(), command(2), command(3)], JournalLimit),
        ([command(payload=b"x" * 2000)], JournalLimit),
    ]:
        with pytest.raises(error):
            await seal(journal, commands)
        assert store.objects == {}


@pytest.mark.asyncio
async def test_restore_detects_missing_predecessor_and_corrupt_hash():
    store, journal = fixture()
    await seal(journal, [command()])
    first = await journal.head()
    await seal(journal, [command(2)], first)
    head = await journal.head()
    original = store.objects.pop(first.reference.key)
    # Current node is intact; restoring the retained chain is a separate proof.
    assert await journal.head() == head
    with pytest.raises(JournalCorrupt, match="missing or hash"):
        await journal.verify_chain(head)
    store.objects[first.reference.key] = StoredObject(original.data + b" ", original.token)
    with pytest.raises(JournalCorrupt, match="missing or hash"):
        await journal.verify_chain(head)


@pytest.mark.asyncio
async def test_payload_checksum_and_sequence_are_verified_not_just_outer_hashes():
    _, journal = fixture()
    value = json.loads(command().encode())
    value["payload_sha256"] = "0" * 64
    with pytest.raises(JournalCorrupt):
        journal._validate_command(value)
    value = json.loads(command().encode())
    value["sequence"] = True
    with pytest.raises(JournalCorrupt):
        journal._validate_command(value)


@pytest.mark.asyncio
async def test_self_consistent_outer_hashes_cannot_hide_sequence_hole():
    store, journal = fixture()
    await seal(journal, [command()])
    head = await journal.head()
    node = json.loads(store.objects[head.reference.key].data)
    segment_ref = node["segment"]
    segment = json.loads(store.objects[segment_ref["key"]].data)
    segment["first_sequence"] = 7
    # Recompute the enclosing checksums to exercise semantic continuity validation.
    def encode(value):
        return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()

    segment_bytes = encode(segment)
    store.write(segment_ref["key"], segment_bytes)
    segment_ref.update(sha256=hashlib.sha256(segment_bytes).hexdigest(), length=len(segment_bytes))
    node_bytes = encode(node)
    store.write(head.reference.key, node_bytes)
    ref = replace(head.reference, sha256=hashlib.sha256(node_bytes).hexdigest(), length=len(node_bytes))
    with pytest.raises(JournalCorrupt, match="hole, overlap"):
        await journal.read_segment(ref)


@pytest.mark.asyncio
async def test_ephemeral_query_and_invalid_bounds_are_rejected():
    store, journal = fixture()
    with pytest.raises(ValueError, match="read_query"):
        await seal(journal, [command(kind="read_query")])
    assert not store.objects
    for limit in (True, 1.5, 0, -1):
        with pytest.raises(ValueError):
            RecoveryJournal(store, stream="fixture", max_commands=limit)


@pytest.mark.asyncio
async def test_explicit_recovery_budget_and_constant_append_reads():
    store, journal = fixture(max_reconcile_hops=1)
    await seal(journal, [command()])
    first = await journal.head()
    await seal(journal, [command(2)], first)
    second = await journal.head()
    store.reads.clear()
    await seal(journal, [command(3)], second)
    # Successful append does not read old manifest nodes or list object history.
    assert first.reference.key not in store.reads
    with pytest.raises(JournalUnknown, match="budget"):
        await journal.verify_chain(await journal.head())
    await journal.verify_chain(await journal.head(), stop=second.reference)


@pytest.mark.asyncio
async def test_outcome_readback_is_distinct_from_acceptance_and_idempotent():
    store, journal = fixture()
    cmd = command()
    accepted = await seal(journal, [cmd])
    assert not any("/outcomes/" in key for key in store.objects)
    store.faults.append(("put-after", "/outcomes/"))
    fields = dict(command=cmd, database_generation="original-db-generation", outcome=b"exact committed body/status/cursor")
    receipt = await journal.record_outcome(accepted, **fields)
    assert receipt == await journal.record_outcome(accepted, **fields)
    assert receipt.command_sha256 == accepted.command_hashes[0]
    with pytest.raises(JournalConflict):
        await journal.record_outcome(accepted, **{**fields, "outcome": b"recomputed body"})
    with pytest.raises(JournalConflict):
        await journal.record_outcome(accepted, **{**fields, "database_generation": "restored-new-db"})
    with pytest.raises(JournalConflict):
        await journal.record_outcome(accepted, **{**fields, "command": command(payload=b"different")})


@pytest.mark.asyncio
async def test_unverified_remote_outcome_cannot_produce_receipt():
    store, journal = fixture()
    accepted = await seal(journal, [command()])
    store.read_failures.add("/outcomes/")
    with pytest.raises(JournalUnknown):
        await journal.record_outcome(accepted, command=command(), database_generation="db-1", outcome=b"committed")


@pytest.mark.asyncio
async def test_outcome_budget_rejects_before_upload():
    store, journal = fixture(max_outcome_bytes=32)
    accepted = await seal(journal, [command()])
    with pytest.raises(JournalLimit):
        await journal.record_outcome(accepted, command=command(), database_generation="db-1", outcome=b"x")
    assert not any("/outcomes/" in key for key in store.objects)
