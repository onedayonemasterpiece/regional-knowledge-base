"""Bounded immutable seals and a fenced CAS head, not a domain queue.

No production transport implements ConditionalRecoveryStore yet. In particular,
ObjectStore.put_bytes cannot implement its conditional operations safely.
"""
from __future__ import annotations

import base64
import hashlib
import json
from dataclasses import dataclass
from typing import Any, Mapping, Protocol, Sequence


class JournalError(RuntimeError):
    pass


class JournalConflict(JournalError):
    """A key, sequence or immutable object has a different identity."""


class JournalUnknown(JournalError):
    """Remote publication is unproved; retain the exact pending operation."""


class JournalCorrupt(JournalError):
    """Recovery must stop until the missing/corrupt chain is reconciled."""


class JournalLimit(JournalError):
    pass


class StoreUnavailable(OSError):
    """Transport failed, possibly after the provider committed the write."""


class StaleWriter(JournalError):
    pass


@dataclass(frozen=True)
class StoredObject:
    data: bytes
    token: str


class ConditionalRecoveryStore(Protocol):
    """Strong reads and native atomic conditions, scoped to one authority.

    read must enforce max_bytes and return None only for verified absence.
    Immutable writes never overwrite, even after a lost ACK. CAS must atomically
    validate writer_generation at publication, not via an earlier local check.
    False means a failed condition; uncertain transport raises StoreUnavailable.
    Tokens are opaque provider versions, never a locally calculated substitute.
    """

    async def read(self, key: str, *, max_bytes: int) -> StoredObject | None: ...
    async def put_if_absent(self, key: str, data: bytes) -> bool: ...
    async def compare_and_swap(
        self, key: str, data: bytes, *, expected_token: str | None,
        writer_generation: str,
    ) -> bool: ...


def _encode(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def _hash(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _decode(data: bytes) -> dict[str, Any]:
    try:
        value = json.loads(data)
        if not isinstance(value, dict) or _encode(value) != data:
            raise ValueError("noncanonical object")
        return value
    except (ValueError, TypeError, UnicodeError) as exc:
        raise JournalCorrupt("invalid canonical journal object") from exc


@dataclass(frozen=True)
class ExactCommand:
    sequence: int
    operation_key: str
    request_id: str
    person_id: str
    route: str
    logical_time: str
    schema_revision: str
    function_revision: str
    executor_revision: str
    ids: Mapping[str, str]
    expected_revisions: Mapping[str, str]
    payload: bytes
    kind: str = "domain_command"

    def encode(self) -> bytes:
        if type(self.sequence) is not int or self.sequence < 1:
            raise ValueError("sequence must be a positive integer")
        fields = (self.operation_key, self.request_id, self.person_id, self.route,
                  self.logical_time, self.schema_revision, self.function_revision,
                  self.executor_revision, self.kind)
        if any(not isinstance(value, str) or not value for value in fields):
            raise ValueError("exact command identity and revisions are required")
        if self.kind not in {"domain_command", "durable_intake", "schema_transition",
                             "control", "provider_intent"}:
            raise ValueError("unsupported durable command kind; read_query is ephemeral")
        if not isinstance(self.payload, bytes):
            raise ValueError("payload must preserve exact bytes")
        for mapping in (self.ids, self.expected_revisions):
            if any(not isinstance(k, str) or not isinstance(v, str)
                   for k, v in mapping.items()):
                raise ValueError("IDs and revision preconditions must be strings")
        return _encode({
            "codec": "rkb.exact-command.v1", "sequence": self.sequence,
            "operation_key": self.operation_key, "request_id": self.request_id,
            "person_id": self.person_id, "route": self.route,
            "logical_time": self.logical_time, "schema_revision": self.schema_revision,
            "function_revision": self.function_revision,
            "executor_revision": self.executor_revision, "kind": self.kind,
            "ids": dict(self.ids), "expected_revisions": dict(self.expected_revisions),
            "payload_b64": base64.b64encode(self.payload).decode("ascii"),
            "payload_sha256": _hash(self.payload), "payload_length": len(self.payload),
        })


@dataclass(frozen=True)
class ObjectRef:
    key: str
    sha256: str
    length: int
    last_sequence: int

    def wire(self) -> dict[str, Any]:
        return {"key": self.key, "sha256": self.sha256, "length": self.length,
                "last_sequence": self.last_sequence}


@dataclass(frozen=True)
class VerifiedHead:
    reference: ObjectRef
    token: str


@dataclass(frozen=True)
class AcceptedSeal:
    """Only a remote journal acceptance receipt; not a PG/domain outcome."""
    manifest: ObjectRef
    command_hashes: tuple[str, ...]


@dataclass(frozen=True)
class RemoteOutcomeReceipt:
    """Verified exact outcome bytes; caller still owns domain commit evidence."""
    operation_key: str
    command_sha256: str
    outcome_sha256: str
    key: str
    length: int
    database_generation: str


class RecoveryJournal:
    def __init__(self, store: ConditionalRecoveryStore, *, stream: str,
                 max_segment_bytes: int = 1024 * 1024, max_commands: int = 128,
                 max_outcome_bytes: int = 1024 * 1024,
                 max_reconcile_hops: int = 128) -> None:
        limits = (max_segment_bytes, max_commands, max_outcome_bytes, max_reconcile_hops)
        if (not isinstance(stream, str) or not stream or len(stream.encode()) > 512
                or any(type(limit) is not int or limit < 1 for limit in limits)):
            raise ValueError("stream and positive finite limits are required")
        self.store = store
        self.stream = stream
        self.prefix = "recovery/v1/" + _hash(stream.encode())
        self.head_key = self.prefix + "/head"
        self.max_segment_bytes = max_segment_bytes
        self.max_commands = max_commands
        self.max_outcome_bytes = max_outcome_bytes
        self.max_reconcile_hops = max_reconcile_hops

    def _ref(self, key: str, data: bytes, last_sequence: int) -> ObjectRef:
        return ObjectRef(key, _hash(data), len(data), last_sequence)

    def _parse_ref(self, value: Any) -> ObjectRef:
        try:
            ref = ObjectRef(**value)
            if (not isinstance(ref.key, str) or not ref.key.startswith(self.prefix + "/")
                    or not isinstance(ref.sha256, str) or len(ref.sha256) != 64
                    or any(c not in "0123456789abcdef" for c in ref.sha256)
                    or type(ref.length) is not int or ref.length < 1
                    or type(ref.last_sequence) is not int or ref.last_sequence < 1):
                raise ValueError("invalid reference")
            return ref
        except (TypeError, ValueError) as exc:
            raise JournalCorrupt("invalid stream reference") from exc

    async def _read(self, key: str, limit: int) -> StoredObject | None:
        try:
            value = await self.store.read(key, max_bytes=limit)
        except StoreUnavailable as exc:
            raise JournalUnknown("remote read unavailable; preserve pending identity") from exc
        if value is not None and len(value.data) > limit:
            raise JournalCorrupt("store violated bounded read contract")
        return value

    async def _verified(self, ref: ObjectRef, limit: int) -> bytes:
        if ref.length > limit:
            raise JournalLimit("referenced object exceeds configured byte budget")
        value = await self._read(ref.key, limit)
        if value is None or len(value.data) != ref.length or _hash(value.data) != ref.sha256:
            raise JournalCorrupt("immutable object missing or hash/length mismatch")
        return value.data

    async def _immutable(self, key: str, data: bytes, limit: int) -> None:
        if len(data) > limit:
            raise JournalLimit("object exceeds configured byte budget")
        try:
            await self.store.put_if_absent(key, data)
        except StoreUnavailable:
            pass  # A lost upload ACK is settled only by exact remote bytes.
        value = await self._read(key, limit)
        if value is None:
            raise JournalUnknown("immutable upload is unproved")
        if value.data != data:
            raise JournalConflict("immutable key already has different exact bytes")

    def _validate_command(self, item: Any) -> bytes:
        try:
            data = _encode(item)
            if item["codec"] != "rkb.exact-command.v1":
                raise ValueError("unsupported command codec")
            payload = base64.b64decode(item["payload_b64"], validate=True)
            command = ExactCommand(
                **{key: item[key] for key in (
                    "sequence", "operation_key", "request_id", "person_id", "route",
                    "logical_time", "schema_revision", "function_revision",
                    "executor_revision", "ids", "expected_revisions", "kind")},
                payload=payload,
            )
            if command.encode() != data:
                raise ValueError("command payload hash/length/schema mismatch")
            return data
        except (KeyError, TypeError, ValueError, AttributeError) as exc:
            raise JournalCorrupt("invalid exact command") from exc

    async def read_segment(self, ref: ObjectRef) -> tuple[ObjectRef | None, tuple[bytes, ...]]:
        """Verify one immutable seal and return predecessor plus exact envelopes.

        A standalone orphan can pass this check. Establish publication/chain
        membership before replay; return bytes unchanged to the pinned decoder.
        """
        node = _decode(await self._verified(ref, 8192))
        try:
            if node["codec"] != "rkb.manifest.v1" or node["stream"] != self.stream:
                raise JournalCorrupt("manifest stream/codec mismatch")
            previous = self._parse_ref(node["previous"]) if node["previous"] is not None else None
            segment_ref = self._parse_ref(node["segment"])
            if segment_ref.last_sequence != ref.last_sequence:
                raise JournalCorrupt("manifest cursor mismatch")
            segment = _decode(await self._verified(segment_ref, self.max_segment_bytes))
            if (segment["codec"] != "rkb.segment.v1" or segment["stream"] != self.stream
                    or segment["previous"] != node["previous"]):
                raise JournalCorrupt("segment predecessor/codec/stream mismatch")
            commands = tuple(self._validate_command(item) for item in segment["commands"])
            if not 1 <= len(commands) <= self.max_commands or segment["count"] != len(commands):
                raise JournalCorrupt("invalid segment count")
            first = previous.last_sequence + 1 if previous else 1
            sequences = [json.loads(command)["sequence"] for command in commands]
            if (sequences != list(range(first, first + len(commands)))
                    or segment["first_sequence"] != first
                    or segment["last_sequence"] != sequences[-1]
                    or sequences[-1] != ref.last_sequence):
                raise JournalCorrupt("journal hole, overlap or sequence mismatch")
            keys = [json.loads(command)["operation_key"] for command in commands]
            if len(set(keys)) != len(keys):
                raise JournalCorrupt("duplicate operation keys in segment")
            return previous, commands
        except (KeyError, TypeError, ValueError) as exc:
            raise JournalCorrupt("invalid manifest/segment schema") from exc

    async def head(self) -> VerifiedHead | None:
        """Read current node only. Recovery must additionally verify_chain()."""
        value = await self._read(self.head_key, 4096)
        if value is None:
            return None
        pointer = _decode(value.data)
        if (pointer.get("codec") != "rkb.head.v1" or pointer.get("stream") != self.stream
                or not pointer.get("writer_generation")):
            raise JournalCorrupt("invalid head identity")
        ref = self._parse_ref(pointer.get("manifest"))
        await self.read_segment(ref)
        return VerifiedHead(ref, value.token)

    async def verify_chain(self, head: VerifiedHead, *, stop: ObjectRef | None = None) -> None:
        """Bounded restore check to genesis or an independently verified base ref.

        Hitting the hop limit is UNKNOWN, never proof of complete recovery.
        No scanning/listing is performed during an ordinary successful append.
        """
        ref = head.reference
        for _ in range(self.max_reconcile_hops):
            if stop is not None and ref == stop:
                return
            previous, _ = await self.read_segment(ref)
            if stop is not None and previous == stop:
                return
            if previous is None:
                if stop is None:
                    return
                raise JournalCorrupt("verified base is not an ancestor")
            ref = previous
        raise JournalUnknown("recovery traversal budget exhausted")

    async def _published(self, target: ObjectRef) -> AcceptedSeal:
        head = await self.head()
        ref = head.reference if head else None
        for _ in range(self.max_reconcile_hops):
            if ref is None or ref.last_sequence < target.last_sequence:
                raise JournalConflict("candidate is not in the published chain")
            previous, commands = await self.read_segment(ref)
            if ref == target:
                return AcceptedSeal(ref, tuple(_hash(command) for command in commands))
            if ref.last_sequence == target.last_sequence:
                raise JournalConflict("another segment owns the candidate sequence")
            ref = previous
        raise JournalUnknown("publication reconciliation budget exhausted")

    async def seal(self, commands: Sequence[ExactCommand], *,
                   predecessor: VerifiedHead | None, writer_generation: str) -> AcceptedSeal:
        """Seal an already formed bounded batch; never execute a domain command."""
        if not writer_generation:
            raise ValueError("writer generation is required")
        if not 1 <= len(commands) <= self.max_commands:
            raise JournalLimit("command count exceeds segment budget")
        encoded = tuple(command.encode() for command in commands)
        previous = predecessor.reference if predecessor else None
        first = previous.last_sequence + 1 if previous else 1
        if [command.sequence for command in commands] != list(range(first, first + len(commands))):
            raise JournalConflict("segment must continue the exact predecessor")
        if len({command.operation_key for command in commands}) != len(commands):
            raise JournalConflict("duplicate operation key in batch")
        segment = _encode({
            "codec": "rkb.segment.v1", "stream": self.stream,
            "previous": previous.wire() if previous else None,
            "first_sequence": first, "last_sequence": commands[-1].sequence,
            "count": len(commands), "commands": [json.loads(data) for data in encoded],
        })
        if len(segment) > self.max_segment_bytes:
            raise JournalLimit("segment exceeds byte budget")
        segment_ref = self._ref(self.prefix + "/segments/" + _hash(segment), segment,
                                commands[-1].sequence)
        node = _encode({"codec": "rkb.manifest.v1", "stream": self.stream,
                        "previous": previous.wire() if previous else None,
                        "segment": segment_ref.wire()})
        node_ref = self._ref(self.prefix + "/manifests/" + _hash(node), node,
                             commands[-1].sequence)
        current = await self.head()
        if current != predecessor:
            return await self._published(node_ref)  # Exact duplicate or a fork; never overwrite.
        # Immutable key bindings retain exact identity even when a crash leaves
        # an unaccepted orphan. Retrying must keep IDs, sequence and bytes.
        for command, data in zip(commands, encoded, strict=True):
            key = self.prefix + "/commands/" + _hash(command.operation_key.encode())
            await self._immutable(key, data, self.max_segment_bytes)
        await self._immutable(segment_ref.key, segment, self.max_segment_bytes)
        await self._immutable(node_ref.key, node, 8192)
        pointer = _encode({"codec": "rkb.head.v1", "stream": self.stream,
                           "writer_generation": writer_generation, "manifest": node_ref.wire()})
        try:
            swapped = await self.store.compare_and_swap(
                self.head_key, pointer, expected_token=predecessor.token if predecessor else None,
                writer_generation=writer_generation,
            )
        except StoreUnavailable:
            # Readback may prove success after a lost CAS ACK. Otherwise preserve UNKNOWN.
            try:
                return await self._published(node_ref)
            except JournalConflict as exc:
                raise JournalUnknown("CAS result unproved; retain exact candidate") from exc
        try:
            return await self._published(node_ref)
        except JournalConflict as exc:
            if swapped:
                raise JournalUnknown("acknowledged publication lacks verified continuity") from exc
            raise

    async def record_outcome(self, seal: AcceptedSeal, *, command: ExactCommand,
                             database_generation: str, outcome: bytes) -> RemoteOutcomeReceipt:
        """Persist a caller-verified committed outcome; does not verify a PG commit.

        The caller supplies canonical domain outcome bytes (IDs/revisions/body,
        status and applied cursor), never a freshly rerun model result.
        """
        if not database_generation or not isinstance(outcome, bytes):
            raise ValueError("original database generation and exact bytes are required")
        command_hash = _hash(command.encode())
        accepted = await self._published(seal.manifest)
        if accepted != seal or command_hash not in accepted.command_hashes:
            raise JournalConflict("outcome does not match an accepted command")
        data = _encode({"codec": "rkb.outcome.v1", "stream": self.stream,
                        "command_sha256": command_hash, "sequence": command.sequence,
                        "operation_key": command.operation_key,
                        "database_generation": database_generation,
                        "outcome_b64": base64.b64encode(outcome).decode("ascii"),
                        "outcome_sha256": _hash(outcome), "outcome_length": len(outcome)})
        key = self.prefix + "/outcomes/" + _hash(command.operation_key.encode())
        await self._immutable(key, data, self.max_outcome_bytes)
        return RemoteOutcomeReceipt(command.operation_key, command_hash, _hash(outcome),
                                    key, len(data), database_generation)
