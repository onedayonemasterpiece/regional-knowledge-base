"""Bounded immutable seals and a fenced CAS head, not a domain queue.

The native S3 adapter is separate; provider semantics and product wiring remain
unverified. ObjectStore.put_bytes cannot implement conditional operations safely.
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


def _reserve_encoding_bytes(remaining: int, size: int) -> int:
    if size > remaining:
        raise JournalLimit("input exceeds configured encoding byte budget")
    return remaining - size


def _reserve_text(remaining: int, value: str) -> int:
    if not isinstance(value, str):
        raise ValueError("journal identity and metadata must be strings")
    # Reject large strings without allocating UTF-8. Encoding a surviving string
    # allocates at most four times the remaining configured budget.
    _reserve_encoding_bytes(remaining, len(value))
    return _reserve_encoding_bytes(remaining, len(value.encode("utf-8")))


def _reserve_integer(remaining: int, value: int) -> int:
    if type(value) is not int or value < 1:
        raise ValueError("sequence must be a positive integer")
    # A cheap lower bound on decimal digits, without converting a huge integer.
    return _reserve_encoding_bytes(remaining, (value.bit_length() - 1) // 4 + 1)


def _preflight_command(command: ExactCommand, remaining: int) -> int:
    """Charge an aggregate lower bound before any base64/JSON allocation."""
    if not isinstance(command.payload, bytes):
        raise ValueError("payload must preserve exact bytes")
    remaining = _reserve_encoding_bytes(remaining, 4 * ((len(command.payload) + 2) // 3))
    remaining = _reserve_integer(remaining, command.sequence)
    for value in (command.operation_key, command.request_id, command.person_id,
                  command.route, command.logical_time, command.schema_revision,
                  command.function_revision, command.executor_revision, command.kind):
        remaining = _reserve_text(remaining, value)
    for mapping in (command.ids, command.expected_revisions):
        if not isinstance(mapping, Mapping):
            raise ValueError("IDs and revision preconditions must be mappings")
        # Even empty JSON string pairs require five bytes ("":""). Check
        # cardinality before traversing or copying a potentially huge mapping.
        remaining = _reserve_encoding_bytes(remaining, 5 * len(mapping))
        for key, value in mapping.items():
            remaining = _reserve_text(remaining, key)
            remaining = _reserve_text(remaining, value)
    return remaining


def _decode(data: bytes) -> dict[str, Any]:
    try:
        value = json.loads(data)
        if not isinstance(value, dict) or _encode(value) != data:
            raise ValueError("noncanonical object")
        return value
    except (ValueError, TypeError, UnicodeError, RecursionError) as exc:
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

    async def remote_outcome(self, operation_key):
        key=self.prefix+'/outcomes/'+_hash(operation_key.encode())
        value=await self._read(key,self.max_outcome_bytes)
        covered=getattr(self.store,'remote_durable',None)
        if value is not None and covered is not None and not covered(key,value.data):
            return None
        return value

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

    async def _manifest(self, ref: ObjectRef) -> tuple[ObjectRef | None, ObjectRef]:
        node = _decode(await self._verified(ref, 8192))
        try:
            if node["codec"] != "rkb.manifest.v1" or node["stream"] != self.stream:
                raise JournalCorrupt("manifest stream/codec mismatch")
            previous = self._parse_ref(node["previous"]) if node["previous"] is not None else None
            segment_ref = self._parse_ref(node["segment"])
            if (segment_ref.last_sequence != ref.last_sequence
                    or (previous is not None and previous.last_sequence >= ref.last_sequence)):
                raise JournalCorrupt("manifest cursor mismatch")
            return previous, segment_ref
        except (KeyError, TypeError, ValueError) as exc:
            raise JournalCorrupt("invalid manifest schema") from exc

    async def read_segment(self, ref: ObjectRef) -> tuple[ObjectRef | None, tuple[bytes, ...]]:
        """Verify bytes; an orphan is not thereby proven accepted."""
        previous, segment_ref = await self._manifest(ref)
        try:
            segment = _decode(await self._verified(segment_ref, self.max_segment_bytes))
            if (segment["codec"] != "rkb.segment.v1" or segment["stream"] != self.stream
                    or segment["previous"] != (previous.wire() if previous else None)):
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

    def _accepted_key(self, ref: ObjectRef) -> str:
        return self.prefix + "/accepted/manifests/" + ref.sha256

    def _accepted_data(self, ref: ObjectRef) -> bytes:
        return _encode({"codec": "rkb.accepted-manifest.v1", "stream": self.stream,
                        "manifest": ref.wire()})

    async def _indexed(self, ref: ObjectRef) -> bool:
        value = await self._read(self._accepted_key(ref), 8192)
        if value is None:
            return False
        if value.data != self._accepted_data(ref):
            raise JournalCorrupt("accepted manifest proof differs from exact reference")
        return True

    async def _fenced_exact(self, key: str, data: bytes, *, expected_token: str | None,
                            writer_generation: str, limit: int = 8192) -> None:
        if len(data) > limit:
            raise JournalLimit("conditional binding exceeds configured read byte budget")
        try:
            await self.store.compare_and_swap(
                key, data, expected_token=expected_token, writer_generation=writer_generation,
            )
        except StoreUnavailable:
            pass
        value = await self._read(key, limit)
        if value is None:
            raise JournalUnknown("fenced publication is unproved")
        if value.data != data:
            if expected_token is not None and value.token == expected_token:
                raise JournalUnknown("conditional publication may still commit against the original token")
            raise JournalConflict("conditional binding has different exact bytes")

    async def _index_published(self, ref: ObjectRef, commands: tuple[bytes, ...], *,
                                writer_generation: str) -> None:
        """Internal: caller proved membership from head, index or bootstrap chain.

        Complete sequence proofs before the manifest proof. Never index a caller
        AcceptedSeal or an uploaded orphan without independent membership proof.
        """
        for data in commands:
            command = _decode(data)
            entry = _encode({"codec": "rkb.accepted-sequence.v1", "stream": self.stream,
                             "manifest": ref.wire(), "sequence": command["sequence"],
                             "command_sha256": _hash(data),
                             "operation_key_sha256": _hash(command["operation_key"].encode())})
            await self._fenced_exact(
                self.prefix + "/accepted/sequences/" + str(command["sequence"]), entry,
                expected_token=None, writer_generation=writer_generation,
            )
        await self._fenced_exact(self._accepted_key(ref), self._accepted_data(ref),
                                 expected_token=None, writer_generation=writer_generation)

    async def _accepted_commands(self, ref: ObjectRef) -> AcceptedSeal:
        _, commands = await self.read_segment(ref)
        return AcceptedSeal(ref, tuple(_hash(command) for command in commands))

    async def _published(self, target: ObjectRef) -> AcceptedSeal:
        if await self._indexed(target):
            return await self._accepted_commands(target)
        head = await self.head()
        ref = head.reference if head else None
        for _ in range(self.max_reconcile_hops):
            if ref is None or ref.last_sequence < target.last_sequence:
                raise JournalConflict("candidate is not in the published chain")
            if ref == target:
                return await self._accepted_commands(ref)
            if ref.last_sequence == target.last_sequence:
                raise JournalConflict("another segment owns the candidate sequence")
            previous, _ = await self._manifest(ref)
            ref = previous
        raise JournalUnknown("acceptance index absent; bounded bootstrap_indexes is required")

    async def verify_accepted(self, seal: AcceptedSeal, command: ExactCommand) -> str:
        """Prove exact remote acceptance before a trusted domain executor starts.

        A supplied receipt is a locator, never authority. This establishes only
        journal membership; current writer, person and SQL preconditions remain
        the executor's responsibility. It does not apply or deliver anything.
        """
        if not isinstance(seal, AcceptedSeal) or not isinstance(command, ExactCommand):
            raise ValueError("exact command and acceptance locator are required")
        if (not isinstance(seal.manifest, ObjectRef) or not isinstance(seal.command_hashes, tuple)
                or not 1 <= len(seal.command_hashes) <= self.max_commands
                or any(not isinstance(value, str) or len(value) != 64
                       or any(c not in "0123456789abcdef" for c in value)
                       for value in seal.command_hashes)):
            raise ValueError("bounded acceptance command hashes are required")
        self._parse_ref(seal.manifest.wire())
        _preflight_command(command, self.max_segment_bytes)
        encoded = command.encode()
        if len(encoded) > self.max_segment_bytes:
            raise JournalLimit("command exceeds segment byte budget")
        command_hash = _hash(encoded)
        accepted = await self._published(seal.manifest)
        if accepted != seal or command_hash not in accepted.command_hashes:
            raise JournalConflict("command does not match an accepted publication")
        return command_hash

    async def locate_accepted(self, command: ExactCommand) -> AcceptedSeal | None:
        """Read-only reconciliation of one exact uncertain publication.

        None proves only that the observed authoritative head is below this
        sequence (including an empty stream), not that an in-flight CAS cannot
        later commit. Missing historical proof/transport remains UNKNOWN. This
        never seals, repairs indexes, rebinds commands or authorizes new work.
        """
        if not isinstance(command,ExactCommand):raise ValueError('exact command required')
        _preflight_command(command,self.max_segment_bytes)
        encoded=command.encode()
        if len(encoded)>self.max_segment_bytes:raise JournalLimit('command exceeds segment byte budget')
        value=await self._read(self.prefix+'/accepted/sequences/'+str(command.sequence),8192)
        if value is not None:
            entry=_decode(value.data)
            try:
                if (entry['codec']!='rkb.accepted-sequence.v1' or entry['stream']!=self.stream
                        or type(entry['sequence']) is not int or entry['sequence']!=command.sequence):
                    raise JournalCorrupt('invalid accepted sequence locator')
                ref=self._parse_ref(entry['manifest'])
                accepted=await self._published(ref)
                previous,commands=await self.read_segment(ref)
                first=previous.last_sequence+1 if previous else 1
                if not first<=command.sequence<=ref.last_sequence:
                    raise JournalCorrupt('accepted locator is outside its segment')
                actual=commands[command.sequence-first]
                actual_command=_decode(actual)
                if (entry['command_sha256']!=_hash(actual)
                        or entry['operation_key_sha256']!=_hash(actual_command['operation_key'].encode())):
                    raise JournalCorrupt('accepted locator hash/key differs from sealed command')
                if actual!=encoded:raise JournalConflict('another exact accepted command occupies this sequence')
                return accepted
            except (KeyError,TypeError,ValueError) as exc:
                raise JournalCorrupt('invalid accepted sequence locator schema') from exc
        head=await self.head()
        if head is None or head.reference.last_sequence<command.sequence:return None
        previous,commands=await self.read_segment(head.reference)
        first=previous.last_sequence+1 if previous else 1
        if command.sequence<first:
            raise JournalUnknown('historical sequence locator absent; bounded index bootstrap required')
        # Modern predecessor-first publication leaves at most the current head
        # unindexed after a crash. Its native head CAS is acceptance evidence.
        if commands[command.sequence-first]!=encoded:
            raise JournalConflict('another exact accepted command occupies this sequence')
        return AcceptedSeal(head.reference,tuple(_hash(data) for data in commands))

    async def _finish_seal(self, target: ObjectRef, writer_generation: str) -> AcceptedSeal:
        accepted = await self._published(target)
        _, commands = await self.read_segment(target)
        await self._index_published(target, commands, writer_generation=writer_generation)
        return accepted

    async def _occupied_command(self, sequence: int) -> dict[str, Any]:
        value = await self._read(self.prefix + "/accepted/sequences/" + str(sequence), 8192)
        if value is None:
            raise JournalUnknown("sequence proof absent; repair current head or bootstrap_indexes")
        entry = _decode(value.data)
        try:
            if (entry["codec"] != "rkb.accepted-sequence.v1" or entry["stream"] != self.stream
                    or entry["sequence"] != sequence):
                raise JournalCorrupt("invalid accepted sequence proof")
            ref = self._parse_ref(entry["manifest"])
            if not await self._indexed(ref):
                raise JournalUnknown("accepted sequence manifest proof is incomplete")
            previous, commands = await self.read_segment(ref)
            first = previous.last_sequence + 1 if previous else 1
            if not first <= sequence <= ref.last_sequence:
                raise JournalCorrupt("accepted sequence is outside its sealed range")
            data = commands[sequence - first]
            command = _decode(data)
            if (entry["command_sha256"] != _hash(data)
                    or entry["operation_key_sha256"] != _hash(command["operation_key"].encode())):
                raise JournalCorrupt("accepted sequence command identity differs")
            return command
        except (KeyError, TypeError, ValueError) as exc:
            raise JournalCorrupt("invalid accepted sequence proof schema") from exc

    async def _bind_command(self, data: bytes, writer_generation: str) -> None:
        command = _decode(data)
        key = self.prefix + "/commands/" + _hash(command["operation_key"].encode())
        old = await self._read(key, self.max_segment_bytes)
        if old is not None and old.data == data:
            return
        if old is not None:
            original = _decode(self._validate_command(_decode(old.data)))
            old_identity = {k: v for k, v in original.items() if k != "sequence"}
            new_identity = {k: v for k, v in command.items() if k != "sequence"}
            if old_identity != new_identity or original["sequence"] == command["sequence"]:
                raise JournalConflict("operation identity differs from its exact original command")
            occupied = await self._occupied_command(original["sequence"])
            if occupied["operation_key"] == original["operation_key"]:
                raise JournalConflict("an accepted operation cannot be resequenced")
            # Another accepted command permanently occupies the old sequence:
            # the orphan's old head precondition can never succeed now.
        await self._fenced_exact(key, data, expected_token=old.token if old else None,
                                 writer_generation=writer_generation, limit=self.max_segment_bytes)

    async def bootstrap_indexes(self, *, writer_generation: str) -> bool:
        """Repair legacy history in bounded, durably checkpointed pages.

        Return True only after reaching genesis. There are no caller-provided
        ancestry references: the initial anchor is the actual published head;
        later cursors are fenced checkpoints of its verified predecessor chain.
        Appends with the new implementation repair their own predecessor first.
        """
        if not isinstance(writer_generation, str) or not writer_generation:
            raise ValueError("writer generation is required")
        _reserve_text(4096, writer_generation)
        key = self.prefix + "/acceptance-index-bootstrap"
        state = await self._read(key, 8192)
        if state is None:
            head = await self.head()
            if head is None:
                return True
            initial = _encode({"codec": "rkb.acceptance-bootstrap.v1", "stream": self.stream,
                               "anchor": head.reference.wire(), "next": head.reference.wire()})
            await self._fenced_exact(key, initial, expected_token=None,
                                     writer_generation=writer_generation)
            state = await self._read(key, 8192)
            if state is None:
                raise JournalUnknown("bootstrap checkpoint is unavailable")
        progress = _decode(state.data)
        if (progress.get("codec") != "rkb.acceptance-bootstrap.v1"
                or progress.get("stream") != self.stream or "next" not in progress):
            raise JournalCorrupt("invalid acceptance bootstrap checkpoint")
        anchor = self._parse_ref(progress.get("anchor"))
        if not await self._indexed(anchor):
            head = await self.head()
            if head is None or head.reference != anchor:
                raise JournalUnknown("bootstrap anchor has no verified publication proof")
        ref = self._parse_ref(progress["next"]) if progress.get("next") is not None else None
        if ref is not None and (ref.last_sequence > anchor.last_sequence
                                or (ref.last_sequence == anchor.last_sequence and ref != anchor)):
            raise JournalCorrupt("bootstrap cursor is ahead of its anchor")
        for _ in range(self.max_reconcile_hops):
            if ref is None:
                break
            previous, commands = await self.read_segment(ref)
            await self._index_published(ref, commands, writer_generation=writer_generation)
            ref = previous
        updated = _encode({"codec": "rkb.acceptance-bootstrap.v1", "stream": self.stream,
                           "anchor": anchor.wire(), "next": ref.wire() if ref else None})
        try:
            await self._fenced_exact(key, updated, expected_token=state.token,
                                     writer_generation=writer_generation)
        except (JournalConflict, JournalUnknown) as exc:
            raise JournalUnknown("bootstrap checkpoint not advanced; resume durable progress") from exc
        return ref is None

    async def seal(self, commands: Sequence[ExactCommand], *,
                   predecessor: VerifiedHead | None, writer_generation: str) -> AcceptedSeal:
        """Seal an already formed bounded batch; never execute a domain command."""
        if not isinstance(writer_generation, str) or not writer_generation:
            raise ValueError("writer generation is required")
        _reserve_text(4096, writer_generation)
        if not 1 <= len(commands) <= self.max_commands:
            raise JournalLimit("command count exceeds segment budget")
        previous = predecessor.reference if predecessor else None
        remaining = self.max_segment_bytes
        if previous is not None:
            remaining = _reserve_text(remaining, previous.key)
            remaining = _reserve_text(remaining, previous.sha256)
            remaining = _reserve_integer(remaining, previous.length)
            remaining = _reserve_integer(remaining, previous.last_sequence)
        for command in commands:
            remaining = _preflight_command(command, remaining)
        encoded_items = []
        encoded_bytes = 0
        for command in commands:
            data = command.encode()
            encoded_bytes += len(data)
            if encoded_bytes > self.max_segment_bytes:
                raise JournalLimit("commands exceed segment byte budget")
            encoded_items.append(data)
        encoded = tuple(encoded_items)
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
        pointer = _encode({"codec": "rkb.head.v1", "stream": self.stream,
                           "acceptance_index": "predecessor-first-v1",
                           "writer_generation": writer_generation, "manifest": node_ref.wire()})
        if len(pointer) > 4096:
            raise JournalLimit("head exceeds configured read byte budget")
        current = await self.head()
        if current != predecessor:
            return await self._finish_seal(node_ref, writer_generation)
        if current is not None:
            # Crash gap is at most the current head: never advance it until all
            # of its accepted manifest/sequence proofs have been read back.
            _, current_commands = await self.read_segment(current.reference)
            await self._index_published(current.reference, current_commands,
                                        writer_generation=writer_generation)
        for data in encoded:
            await self._bind_command(data, writer_generation)
        await self._immutable(segment_ref.key, segment, self.max_segment_bytes)
        await self._immutable(node_ref.key, node, 8192)
        try:
            swapped = await self.store.compare_and_swap(
                self.head_key, pointer, expected_token=predecessor.token if predecessor else None,
                writer_generation=writer_generation,
            )
        except StoreUnavailable:
            # Readback may prove success after a lost CAS ACK. Otherwise preserve UNKNOWN.
            try:
                return await self._finish_seal(node_ref, writer_generation)
            except JournalConflict as exc:
                raise JournalUnknown("CAS result unproved; retain exact candidate") from exc
        try:
            return await self._finish_seal(node_ref, writer_generation)
        except JournalConflict as exc:
            if swapped or await self.head() == predecessor:
                raise JournalUnknown("acknowledged publication lacks verified continuity") from exc
            raise

    async def record_outcome(self, seal: AcceptedSeal, *, command: ExactCommand,
                             database_generation: str, outcome: bytes,
                             writer_generation: str) -> RemoteOutcomeReceipt:
        """Persist a caller-verified committed outcome; does not verify a PG commit.

        The caller supplies canonical domain outcome bytes (IDs/revisions/body,
        status and applied cursor), never a freshly rerun model result.
        """
        if (not isinstance(database_generation, str) or not database_generation or not isinstance(outcome, bytes)
                or not isinstance(writer_generation, str) or not writer_generation):
            raise ValueError("original database generation, exact bytes and writer generation are required")
        remaining = _reserve_encoding_bytes(self.max_outcome_bytes, 4 * ((len(outcome) + 2) // 3))
        remaining = _reserve_text(remaining, database_generation)
        remaining = _reserve_text(remaining, command.operation_key)
        _reserve_text(remaining, self.stream)
        _reserve_text(4096, writer_generation)
        _preflight_command(command, self.max_segment_bytes)
        encoded_command = command.encode()
        if len(encoded_command) > self.max_segment_bytes:
            raise JournalLimit("command exceeds segment byte budget")
        command_hash = _hash(encoded_command)
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
        if len(data) > self.max_outcome_bytes:
            raise JournalLimit("outcome exceeds configured byte budget")
        try:
            # A canonical outcome binding is publication, even before product
            # delivery. A retired writer may upload orphan seal bytes, but must
            # not reserve this operation's outcome against the active writer.
            await self.store.compare_and_swap(
                key, data, expected_token=None, writer_generation=writer_generation,
            )
        except StoreUnavailable:
            pass  # Exact readback can reconcile an already committed create.
        value = await self._read(key, self.max_outcome_bytes)
        if value is None:
            raise JournalUnknown("outcome publication is unproved")
        if value.data != data:
            raise JournalConflict("outcome key already has different exact bytes")
        covered=getattr(self.store,'remote_durable',None)
        if covered is not None and not covered(key,data):
            raise JournalUnknown('outcome_pending_dataset_checkpoint')
        return RemoteOutcomeReceipt(command.operation_key, command_hash, _hash(outcome),
                                    key, len(data), database_generation)
