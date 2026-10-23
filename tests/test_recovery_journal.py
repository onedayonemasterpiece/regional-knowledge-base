from __future__ import annotations

import asyncio
import hashlib
import json
from dataclasses import replace

import pytest

import regional_knowledge.recovery_journal as journal_module
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
        if self.fail_head_read_after_cas and key.endswith("/head"):
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
async def test_executor_acceptance_proof_verifies_exact_published_command():
    store, journal = fixture()
    original = command()
    accepted = await seal(journal, [original])
    assert await journal.verify_accepted(accepted, original) == hashlib.sha256(original.encode()).hexdigest()
    with pytest.raises(JournalConflict):
        await journal.verify_accepted(accepted, replace(original, payload=b'different command'))
    store.reads.clear()
    with pytest.raises(ValueError):
        await journal.verify_accepted(replace(accepted, command_hashes=('x' * 64,)), original)
    assert not store.reads
    store.read_failures.add('/accepted/')
    with pytest.raises(JournalUnknown):
        await journal.verify_accepted(accepted, original)


@pytest.mark.asyncio
async def test_locate_exact_acceptance_is_read_only_and_distinguishes_observed_absence():
    store,journal=fixture()
    original=command()
    assert await journal.locate_accepted(original) is None
    assert not store.objects
    accepted=await seal(journal,[original])
    version=store.version
    assert await journal.locate_accepted(original)==accepted
    assert store.version==version
    with pytest.raises(JournalConflict):await journal.locate_accepted(replace(original,payload=b'different'))
    assert await journal.locate_accepted(command(2)) is None
    assert store.version==version


@pytest.mark.asyncio
async def test_locate_current_head_crash_gap_proves_acceptance_without_index_repair():
    store,journal=fixture()
    original=command()
    store.fail_head_read_after_cas=True
    with pytest.raises(JournalUnknown):await seal(journal,[original])
    store.fail_head_read_after_cas=False;store.read_failures.clear()
    assert not any('/accepted/' in key for key in store.objects)
    version=store.version
    located=await journal.locate_accepted(original)
    assert located is not None and await journal.verify_accepted(located,original)==hashlib.sha256(original.encode()).hexdigest()
    assert store.version==version and not any('/accepted/' in key for key in store.objects)


@pytest.mark.asyncio
async def test_locate_unpublished_orphan_never_creates_accepted_proof_and_history_gap_is_unknown():
    store,journal=fixture()
    store.faults.append(('cas-before','/head'))
    with pytest.raises(JournalUnknown):await seal(journal,[command()])
    version=store.version
    assert await journal.locate_accepted(command()) is None
    assert store.version==version
    await seal(journal,[command()])
    await seal(journal,[command(2)],await journal.head())
    del store.objects[journal.prefix+'/accepted/sequences/1']
    version=store.version
    with pytest.raises(JournalUnknown):await journal.locate_accepted(command())
    assert store.version==version


@pytest.mark.asyncio
async def test_locate_accepted_locator_poison_or_unavailable_is_not_absence():
    store,journal=fixture()
    await seal(journal,[command()])
    key=journal.prefix+'/accepted/sequences/1'
    raw=json.loads(store.objects[key].data);raw['operation_key_sha256']='0'*64
    store.write(key,json.dumps(raw,sort_keys=True,separators=(',',':')).encode())
    with pytest.raises(JournalCorrupt):await journal.locate_accepted(command())
    store.read_failures.add('/accepted/sequences/')
    with pytest.raises(JournalUnknown):await journal.locate_accepted(command())


@pytest.mark.asyncio
async def test_executor_rejects_receipt_for_uploaded_unpublished_orphan():
    store, journal = fixture()
    original = command()
    store.faults.append(('cas-before', '/head'))
    with pytest.raises(JournalUnknown):
        await seal(journal, [original])
    key = next(key for key in store.objects if '/manifests/' in key)
    reference = journal._ref(key, store.objects[key].data, 1)
    forged = AcceptedSeal(reference, (hashlib.sha256(original.encode()).hexdigest(),))
    with pytest.raises(JournalConflict):
        await journal.verify_accepted(forged, original)


@pytest.mark.asyncio
async def test_old_accepted_outcome_does_not_age_out_of_reconciliation_budget():
    store, journal = fixture(max_reconcile_hops=4)
    first = await seal(journal, [command()])
    for sequence in range(2, 8):
        await seal(journal, [command(sequence)], await journal.head())
    store.reads.clear()
    await journal.record_outcome(first, command=command(), database_generation="db-1",
                                 outcome=b"committed", writer_generation="writer-1")
    assert len(store.reads) <= 8
    assert sum("/segments/" in key for key in store.reads) == 1


@pytest.mark.asyncio
async def test_dead_unaccepted_binding_can_move_after_other_operation_occupies_sequence():
    store, journal = fixture()
    original = command(operation_key="X")
    store.faults.append(("cas-before", "/head"))
    with pytest.raises(JournalUnknown):
        await seal(journal, [original])
    store.writer = "writer-2"
    await seal(journal, [command(operation_key="Y")], writer="writer-2")
    accepted = await seal(journal, [replace(original, sequence=2)], await journal.head(), writer="writer-2")
    assert accepted.manifest.last_sequence == 2
    with pytest.raises(JournalConflict):
        await seal(journal, [replace(original, sequence=3)], await journal.head(), writer="writer-2")


@pytest.mark.asyncio
async def test_deep_json_segment_is_typed_corruption():
    store, journal = fixture()
    accepted = await seal(journal, [command()])
    node = json.loads(store.objects[accepted.manifest.key].data)
    deep = b'{"deep":' + b"[" * 100000 + b"0" + b"]" * 100000 + b"}"
    node["segment"]["length"] = len(deep)
    node["segment"]["sha256"] = hashlib.sha256(deep).hexdigest()
    store.write(node["segment"]["key"], deep)
    data = journal_module._encode(node)
    store.write(accepted.manifest.key, data)
    ref = replace(accepted.manifest, sha256=hashlib.sha256(data).hexdigest(), length=len(data))
    with pytest.raises(JournalCorrupt):
        await journal.read_segment(ref)


@pytest.mark.asyncio
async def test_head_index_crash_gap_is_repaired_before_another_append_advances():
    store, journal = fixture(max_reconcile_hops=4)
    store.faults.append(("cas-after", "/head"))
    store.fail_head_read_after_cas = True
    with pytest.raises(JournalUnknown):
        await seal(journal, [command()])
    store.read_failures.clear()
    store.fail_head_read_after_cas = False
    first_head = await journal.head()
    assert await journal._read(journal._accepted_key(first_head.reference), 8192) is None
    _, encoded = await journal.read_segment(first_head.reference)
    first = AcceptedSeal(first_head.reference, tuple(hashlib.sha256(data).hexdigest() for data in encoded))
    restarted = RecoveryJournal(store, stream="synthetic-only", max_reconcile_hops=4)
    for sequence in range(2, 9):
        await seal(restarted, [command(sequence)], await restarted.head())
    assert await restarted._indexed(first.manifest)
    store.reads.clear()
    assert first == await seal(restarted, [command()])
    assert len(store.reads) < 20  # Independent of the seven later groups.
    store.reads.clear()
    await restarted.record_outcome(first, command=command(), database_generation="db-1",
                                   outcome=b"recorded", writer_generation="writer-1")
    assert len(store.reads) <= 8


@pytest.mark.asyncio
async def test_failed_predecessor_index_repair_cannot_advance_head():
    store, journal = fixture()
    store.faults.append(("cas-before", "/accepted/sequences/"))
    with pytest.raises(JournalUnknown):
        await seal(journal, [command()])
    head = await journal.head()
    store.faults.append(("cas-before", "/accepted/sequences/"))
    with pytest.raises(JournalUnknown):
        await seal(journal, [command(2)], head)
    assert await journal.head() == head
    await seal(journal, [command(2)], head)


@pytest.mark.asyncio
async def test_unpublished_orphan_and_forged_seal_cannot_poison_acceptance_index():
    store, journal = fixture()
    store.faults.append(("cas-before", "/head"))
    with pytest.raises(JournalUnknown):
        await seal(journal, [command()])
    key = next(key for key in store.objects if "/manifests/" in key)
    data = store.objects[key].data
    orphan = AcceptedSeal(journal_module.ObjectRef(key, hashlib.sha256(data).hexdigest(), len(data), 1),
                          (hashlib.sha256(command().encode()).hexdigest(),))
    with pytest.raises(JournalConflict):
        await journal.record_outcome(orphan, command=command(), database_generation="db-1",
                                     outcome=b"not accepted", writer_generation="writer-1")
    assert not any("/accepted/" in key or "/outcomes/" in key for key in store.objects)
    accepted = await seal(journal, [command()])
    forged = replace(accepted, command_hashes=("0" * 64,))
    with pytest.raises(JournalConflict):
        await journal.record_outcome(forged, command=command(), database_generation="db-1",
                                     outcome=b"forged", writer_generation="writer-1")


@pytest.mark.asyncio
async def test_legacy_index_bootstrap_has_durable_bounded_progress_after_restart():
    store, journal = fixture(max_reconcile_hops=2)
    first = await seal(journal, [command()])
    for sequence in range(2, 8):
        await seal(journal, [command(sequence)], await journal.head())
    # Synthetic pre-index history: preserve the actual head, manifests and bytes.
    for key in tuple(store.objects):
        if "/accepted/" in key:
            del store.objects[key]
    store.reads.clear()
    with pytest.raises(JournalUnknown, match="bootstrap_indexes"):
        await journal.record_outcome(first, command=command(), database_generation="db-1",
                                     outcome=b"recorded", writer_generation="writer-1")
    # The fallback checks manifest links, not every intermediate segment.
    assert sum("/segments/" in key for key in store.reads) == 1
    for expected_complete in (False, False, False, True):
        restarted = RecoveryJournal(store, stream="synthetic-only", max_reconcile_hops=2)
        assert await restarted.bootstrap_indexes(writer_generation="writer-1") is expected_complete
    assert await restarted.bootstrap_indexes(writer_generation="writer-1") is True
    store.reads.clear()
    await restarted.record_outcome(first, command=command(), database_generation="db-1",
                                   outcome=b"recorded", writer_generation="writer-1")
    assert len(store.reads) <= 8


@pytest.mark.asyncio
async def test_bootstrap_lost_progress_ack_retries_verified_page_and_fences_old_writer():
    class BootstrapStore(ConditionalMemoryStore):
        fail_progress = True

        async def compare_and_swap(self, key, data, **kwargs):
            if key.endswith("/acceptance-index-bootstrap") and kwargs["expected_token"] is not None and self.fail_progress:
                self.fail_progress = False
                raise StoreUnavailable("checkpoint upload not completed")
            return await super().compare_and_swap(key, data, **kwargs)

    store = BootstrapStore()
    journal = RecoveryJournal(store, stream="synthetic-only", max_reconcile_hops=1)
    await seal(journal, [command()])
    await seal(journal, [command(2)], await journal.head())
    for key in tuple(store.objects):
        if "/accepted/" in key:
            del store.objects[key]
    with pytest.raises(JournalUnknown, match="resume durable progress"):
        await journal.bootstrap_indexes(writer_generation="writer-1")
    store.writer = "writer-2"
    with pytest.raises(StaleWriter):
        await journal.bootstrap_indexes(writer_generation="writer-1")
    # Repeats the already indexed page safely, then continues from persisted cursor.
    assert await journal.bootstrap_indexes(writer_generation="writer-2") is False
    restarted = RecoveryJournal(store, stream="synthetic-only", max_reconcile_hops=1)
    assert await restarted.bootstrap_indexes(writer_generation="writer-2") is True


@pytest.mark.asyncio
async def test_rebind_preserves_exact_payload_and_preconditions_and_needs_occupied_proof():
    store, journal = fixture()
    original = command(operation_key="X")
    store.faults.append(("cas-before", "/head"))
    with pytest.raises(JournalUnknown):
        await seal(journal, [original])
    # No head/occupant proof: private binding alone does not authorize moving X.
    with pytest.raises(JournalUnknown, match="sequence proof"):
        await journal._bind_command(replace(original, sequence=2).encode(), "writer-1")
    await seal(journal, [command(operation_key="Y")])
    for changes in ({"payload": b"new model choice"}, {"expected_revisions": {"story": "8"}},
                    {"ids": {"variant_id": "new ID"}}, {"executor_revision": "executor-v2"}):
        with pytest.raises(JournalConflict):
            await seal(journal, [replace(original, sequence=2, **changes)], await journal.head())
    store.writer = "writer-2"
    with pytest.raises(StaleWriter):
        await journal._bind_command(replace(original, sequence=2).encode(), "writer-1")
    await seal(journal, [replace(original, sequence=2)], await journal.head(), writer="writer-2")


@pytest.mark.asyncio
async def test_delayed_orphan_rebind_with_old_token_visible_is_unknown_until_native_commit():
    class LateBindingStore(ConditionalMemoryStore):
        delay_rebind = True
        pending = None

        async def compare_and_swap(self, key, data, **kwargs):
            if "/commands/" in key and kwargs["expected_token"] is not None and self.delay_rebind:
                self.pending = key, data
                raise StoreUnavailable("old-token rebind still pending")
            return await super().compare_and_swap(key, data, **kwargs)

    store = LateBindingStore()
    journal = RecoveryJournal(store, stream="synthetic-only")
    original = command(operation_key="X")
    store.faults.append(("cas-before", "/head"))
    with pytest.raises(JournalUnknown):
        await seal(journal, [original])
    await seal(journal, [command(operation_key="Y")])
    predecessor = await journal.head()
    with pytest.raises(JournalUnknown):
        await seal(journal, [replace(original, sequence=2)], predecessor)
    store.write(*store.pending)
    store.delay_rebind = False
    accepted = await seal(journal, [replace(original, sequence=2)], predecessor)
    assert accepted.manifest.last_sequence == 2


@pytest.mark.asyncio
async def test_generic_false_cas_with_unchanged_head_is_unknown_not_definitive_conflict():
    class FalseHeadStore(ConditionalMemoryStore):
        pending = None

        async def compare_and_swap(self, key, data, **kwargs):
            if key.endswith("/head"):
                self.pending = (key, data)
                return False
            return await super().compare_and_swap(key, data, **kwargs)

    store = FalseHeadStore()
    journal = RecoveryJournal(store, stream="synthetic-only")
    with pytest.raises(JournalUnknown):
        await seal(journal, [command()])
    store.write(*store.pending)
    assert (await seal(journal, [command()])).manifest == (await journal.head()).reference


@pytest.mark.asyncio
async def test_exact_group_roundtrip_duplicate_and_continuity():
    store, journal = fixture()
    commands = [command(), command(2,payload=b"exact second data command")]
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
    ("cas-after", "/commands/"), ("put-after", "/segments/"),
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
    store.faults.append(("cas-before" if target == "/commands/" else "put-before", target))
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
    store.faults.append(("cas-after", "/outcomes/"))
    fields = dict(command=cmd, database_generation="original-db-generation",
                  outcome=b"exact committed body/status/cursor", writer_generation="writer-1")
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
        await journal.record_outcome(accepted, command=command(), database_generation="db-1",
                                     outcome=b"committed", writer_generation="writer-1")


@pytest.mark.asyncio
async def test_outcome_budget_rejects_before_upload():
    store, journal = fixture(max_outcome_bytes=32)
    accepted = await seal(journal, [command()])
    with pytest.raises(JournalLimit):
        await journal.record_outcome(accepted, command=command(), database_generation="db-1",
                                     outcome=b"x", writer_generation="writer-1")
    assert not any("/outcomes/" in key for key in store.objects)


@pytest.mark.asyncio
async def test_retired_writer_cannot_reserve_an_accepted_commands_outcome():
    store, journal = fixture()
    cmd = command()
    accepted = await seal(journal, [cmd])
    store.writer = "writer-2"
    fields = dict(command=cmd, database_generation="original-db-generation")
    with pytest.raises(StaleWriter):
        await journal.record_outcome(accepted, **fields, outcome=b"retired isolated outcome",
                                     writer_generation="writer-1")
    assert not any("/outcomes/" in key for key in store.objects)
    # Accepted old commands remain replayable under the current authority.
    receipt = await journal.record_outcome(accepted, **fields, outcome=b"reconciled committed outcome",
                                           writer_generation="writer-2")
    assert receipt.database_generation == "original-db-generation"
    store.writer = "writer-3"
    assert receipt == await journal.record_outcome(
        accepted, **fields, outcome=b"reconciled committed outcome", writer_generation="writer-3")
    with pytest.raises(StaleWriter):
        await journal.record_outcome(accepted, **fields, outcome=b"reconciled committed outcome",
                                     writer_generation="writer-2")


@pytest.mark.asyncio
async def test_outcome_cas_unknown_reconciles_exact_publication_only():
    store, journal = fixture()
    accepted = await seal(journal, [command()])
    fields = dict(command=command(), database_generation="db-1", outcome=b"committed",
                  writer_generation="writer-1")
    store.faults.append(("cas-before", "/outcomes/"))
    with pytest.raises(JournalUnknown):
        await journal.record_outcome(accepted, **fields)
    assert not any("/outcomes/" in key for key in store.objects)
    store.faults.append(("cas-after", "/outcomes/"))
    store.read_failures.add("/outcomes/")
    with pytest.raises(JournalUnknown):
        await journal.record_outcome(accepted, **fields)
    assert any("/outcomes/" in key for key in store.objects)
    store.read_failures.clear()
    receipt = await journal.record_outcome(accepted, **fields)
    assert receipt.outcome_sha256 == hashlib.sha256(fields["outcome"]).hexdigest()


def forbid_encoding(monkeypatch):
    def fail(*args, **kwargs):
        raise AssertionError("oversized input reached base64/JSON encoding")
    monkeypatch.setattr(journal_module, "_encode", fail)
    monkeypatch.setattr(journal_module.base64, "b64encode", fail)


@pytest.mark.asyncio
@pytest.mark.parametrize("changes", [
    {"payload": b"x" * 2049},
    {"operation_key": "x" * 2049},
    {"ids": {"chosen": "x" * 2049}},
    {"expected_revisions": {"x" * 2049: "revision"}},
    {"person_id": "p" * 1100, "route": "r" * 1100},
    {"route": "𐀀" * 600},
])
async def test_segment_preflight_rejects_payload_and_aggregate_metadata_before_encoding(monkeypatch, changes):
    store, journal = fixture(max_segment_bytes=2048)
    forbid_encoding(monkeypatch)
    with pytest.raises(JournalLimit):
        await seal(journal, [command(**changes)])
    assert not store.objects


@pytest.mark.asyncio
async def test_segment_preflight_applies_to_whole_batch_before_any_encoding(monkeypatch):
    store, journal = fixture(max_segment_bytes=2048)
    forbid_encoding(monkeypatch)
    with pytest.raises(JournalLimit):
        await seal(journal, [command(payload=b"x" * 800), command(2, payload=b"x" * 800)])
    assert not store.objects


@pytest.mark.asyncio
async def test_segment_preflight_rejects_mapping_cardinality_before_iteration(monkeypatch):
    class OversizedMapping(dict):
        def __len__(self):
            return 2049

        def items(self):
            raise AssertionError("oversized mapping was traversed")

    store, journal = fixture(max_segment_bytes=2048)
    forbid_encoding(monkeypatch)
    with pytest.raises(JournalLimit):
        await seal(journal, [command(ids=OversizedMapping())])
    assert not store.objects


@pytest.mark.asyncio
@pytest.mark.parametrize("changes", [
    {"outcome": b"x" * 2049},
    {"database_generation": "x" * 2049},
    {"command": command(operation_key="x" * 2049)},
    {"command": command(payload=b"x" * 2049)},
    {"command": command(ids={"id": "x" * 2049})},
])
async def test_outcome_preflight_rejects_before_command_or_outcome_encoding(monkeypatch, changes):
    store, journal = fixture(max_segment_bytes=2048, max_outcome_bytes=2048)
    accepted = await seal(journal, [command()])
    original_objects = dict(store.objects)
    fields = dict(command=command(), outcome=b"committed", database_generation="db-1",
                  writer_generation="writer-1")
    fields.update(changes)
    forbid_encoding(monkeypatch)
    with pytest.raises(JournalLimit):
        await journal.record_outcome(accepted, **fields)
    assert store.objects == original_objects


@pytest.mark.asyncio
async def test_head_generation_preflight_and_final_cap_prevent_unreadable_publication(monkeypatch):
    store, journal = fixture()
    with monkeypatch.context() as guarded:
        forbid_encoding(guarded)
        with pytest.raises(JournalLimit):
            await seal(journal, [command()], writer="w" * 4097)
    assert not store.objects
    # JSON escaping can exceed the final head cap even when UTF-8 fits.
    with pytest.raises(JournalLimit, match="head"):
        await seal(journal, [command()], writer="\x00" * 1000)
    assert not store.objects
    assert await journal.head() is None
    await seal(journal, [command()])
    assert await journal.head() is not None
