"""Finite local replay of an accepted journal; no controller or provider effects.

The caller installs the historical schema/code and supplies its trusted resolver
and materializer. This module never substitutes current handlers for history.
It is not a live writer or a replacement for normal recovery orchestration.
"""
from dataclasses import dataclass
import base64
import hashlib
import json
import logging
import importlib
from pathlib import Path
import sqlite3
import sys
import types
from psycopg.errors import QueryCanceled

from .kaggle_apply import ApplyConflict, apply_accepted
from .recovery_journal import AcceptedSeal, ExactCommand, ObjectRef, RecoveryJournal, StoredObject


LOG = logging.getLogger(__name__)
STATES = ("APPLIED", "REJECTED_PRECONDITION", "NOT_SENT", "SENT_UNKNOWN")


def outcome_difference(expected, actual):
    """Report field names only; receipt values may contain private book text."""
    before, after = json.loads(expected), json.loads(actual)
    fields = sorted(key for key in before.keys() | after.keys()
                    if before.get(key) != after.get(key))
    before = json.loads(base64.b64decode(before['result_b64']))
    after = json.loads(base64.b64decode(after['result_b64']))
    result_fields = sorted(key for key in before.keys() | after.keys()
                           if before.get(key) != after.get(key))
    return fields, result_fields


def historical_materializer(root, fingerprint):
    """Load prepared immutable source, sharing only executor boundary types.

    root is a caller-verified source archive extracted outside provider outputs.
    No journal payload is evaluated as Python. Model loading stays with the caller.
    """
    if (len(fingerprint) != 64
            or any(char not in "0123456789abcdef" for char in fingerprint)):
        raise ValueError("historical materializer fingerprint required")
    name = "_rkb_history_" + fingerprint
    if name not in sys.modules:
        package = types.ModuleType(name)
        package.__path__ = [str(Path(root).resolve() / "src" / "regional_knowledge")]
        sys.modules[name] = package
        for boundary in ("recovery_journal", "kaggle_apply", "kaggle_bootstrap"):
            sys.modules[name + "." + boundary] = importlib.import_module(
                "regional_knowledge." + boundary)
    module = importlib.import_module(name + ".kaggle_source_materializer")
    if module.materializer_fingerprint() != fingerprint:
        raise ApplyConflict("historical source differs from accepted materializer")
    return module


@dataclass(frozen=True)
class ReplayRecord:
    command: ExactCommand
    seal: AcceptedSeal
    state: str
    outcome: bytes | None


class SnapshotInput:
    """Read-only local export adapter. Never instantiates provider clients."""

    def __init__(self, path, *, storage, stream):
        self.connection = sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True)
        self.connection.row_factory = sqlite3.Row
        self.storage = storage
        self.journal = RecoveryJournal(self, stream=stream)

    def close(self):
        self.connection.close()

    async def read(self, key, *, max_bytes):
        row = self.connection.execute("""SELECT data, token FROM rkb_dataset_recovery_objects
            WHERE storage=? AND key=?""", (self.storage, key)).fetchone()
        if row is None:
            return None
        if len(row[0]) > max_bytes:
            raise ApplyConflict("local object exceeds the journal read budget")
        return StoredObject(bytes(row[0]), row[1])

    async def records(self):
        for row in self.connection.execute("SELECT * FROM rkb_control_operations ORDER BY sequence"):
            if row["seal_state"] != "ACCEPTED":
                raise ApplyConflict("input contains an operation without journal acceptance")
            wire = json.loads(row["seal_bytes"])
            seal = AcceptedSeal(ObjectRef(**wire["manifest"]), tuple(wire["command_hashes"]))
            raw = row["command_bytes"]
            if raw is None:
                _, commands = await self.journal.read_segment(seal.manifest)
                matches = [data for data in commands
                           if json.loads(data)["operation_key"] == row["operation_key"]]
                if len(matches) != 1:
                    raise ApplyConflict("retired command is missing from its sealed segment")
                raw = matches[0]
            doc = json.loads(raw)
            command = ExactCommand(**{key: doc[key] for key in (
                "sequence", "operation_key", "request_id", "person_id", "route", "logical_time",
                "schema_revision", "function_revision", "executor_revision", "ids",
                "expected_revisions", "kind")},
                payload=base64.b64decode(doc["payload_b64"], validate=True))
            if (command.encode() != bytes(raw) or command.operation_key != row["operation_key"]
                    or command.sequence != row["sequence"]
                    or hashlib.sha256(raw).hexdigest() != row["command_sha256"]):
                raise ApplyConflict("control record differs from its exact command")
            outcome = row["outcome_bytes"]
            if outcome is None and row["remote_receipt_bytes"] is not None:
                ref = json.loads(row["remote_receipt_bytes"])
                obj = await self.read(ref["key"], max_bytes=self.journal.max_outcome_bytes)
                if obj is None:
                    raise ApplyConflict("historical outcome object is missing")
                envelope = json.loads(obj.data)
                outcome = base64.b64decode(envelope["outcome_b64"], validate=True)
            if outcome is not None:
                outcome = bytes(outcome)
                if hashlib.sha256(outcome).hexdigest() != row["outcome_sha256"]:
                    raise ApplyConflict("historical outcome digest differs")
            yield ReplayRecord(command, seal, row["apply_state"], outcome)


def _historical_outcome(record):
    if record.state not in STATES:
        raise ValueError("unsupported historical application state")
    if record.state in ("NOT_SENT", "SENT_UNKNOWN"):
        if record.outcome is not None:
            raise ApplyConflict("unresolved operation has a terminal outcome")
        return None
    if not isinstance(record.outcome, bytes):
        raise ApplyConflict("historical outcome bytes required")
    outcome = json.loads(record.outcome)
    command = record.command
    status = "applied" if record.state == "APPLIED" else "rejected_precondition"
    expected = dict(codec="rkb.applied-outcome.v1", operation_key=command.operation_key,
        command_sha256=hashlib.sha256(command.encode()).hexdigest(), cursor=command.sequence,
        status=status, ids=dict(command.ids), request_id=command.request_id,
        person_id=command.person_id, route=command.route, logical_time=command.logical_time,
        before=[command.schema_revision, command.function_revision, command.executor_revision])
    if any(outcome.get(key) != value for key, value in expected.items()):
        raise ApplyConflict("historical outcome does not bind the exact command")
    return outcome


async def _existing(connection, command, database_generation):
    async with connection.transaction():
        row = await (await connection.execute("""SELECT command_bytes, outcome_bytes,
            database_generation FROM rkb_v4.applied_ops WHERE operation_key=%s OR sequence=%s""",
            (command.operation_key, command.sequence))).fetchall()
    if not row:
        return None
    if (len(row) != 1 or bytes(row[0][0]) != command.encode()
            or row[0][2] != database_generation):
        raise ApplyConflict("restored operation identity conflicts")
    return bytes(row[0][1])


async def _restore_rejection(connection, record, *, database_generation, writer_generation):
    """Restore the original rejected receipt, without re-evaluating old content."""
    command = record.command
    outcome = _historical_outcome(record)
    async with connection.transaction():
        state = await (await connection.execute("""SELECT database_generation, writer_generation,
            applied_seq FROM rkb_v4.recovery_state WHERE singleton FOR UPDATE""")).fetchone()
        if state != (database_generation, writer_generation, command.sequence - 1):
            raise ApplyConflict("rejection restore requires the exact next historical sequence")
        await connection.execute("""INSERT INTO rkb_v4.applied_ops
            (operation_key, sequence, command_bytes, command_sha256, accepted_manifest_sha256,
             database_generation, writer_generation, status, outcome_bytes, outcome_sha256,
             handler_sha256) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (command.operation_key, command.sequence, command.encode(), outcome["command_sha256"],
             record.seal.manifest.sha256, database_generation, writer_generation,
             "rejected_precondition", record.outcome, hashlib.sha256(record.outcome).hexdigest(),
             outcome["handler_sha256"]))
        await connection.execute("UPDATE rkb_v4.recovery_state SET applied_seq=%s WHERE singleton",
                                 (command.sequence,))


async def rebuild(connection, *, journal, records, database_generation, writer_generation,
                  resolve, prepare, keep_going=lambda: True):
    """Replay a finite ordered input, preserving unresolved suffix and exact receipts.

    resolve(command) installs/selects historical schema/functions and returns its
    registry. prepare(command) uses that same historical local materializer. Both
    are trusted notebook code, never serialized callbacks or journal instructions.
    A missing dependency terminates this batch; it never weakens domain guards.
    keep_going is the finite caller's time budget, checked between transactions.
    A partial report counts only visited records and identifies the next sequence.
    """
    head = await journal.head()
    if head is None:
        raise ApplyConflict("accepted journal is empty")
    await journal.verify_chain(head)
    report = dict.fromkeys(STATES, 0)
    report["restored_prefix"] = 0
    unresolved = False
    last = 0
    async for record in records:
        command = record.command
        if not keep_going():
            return dict(report, next_sequence=command.sequence,
                        accepted_last_sequence=head.reference.last_sequence)
        if command.sequence != last + 1:
            raise ApplyConflict("batch must start at genesis and preserve sequence")
        last = command.sequence
        expected = _historical_outcome(record)
        await journal.verify_accepted(record.seal, command)
        report[record.state] += 1
        if expected is None:
            unresolved = True
            continue
        if unresolved:
            raise ApplyConflict("terminal operation follows an unresolved historical gap")
        existing = await _existing(connection, command, database_generation)
        if existing is not None:
            if existing != record.outcome:
                raise ApplyConflict("restored receipt differs from historical bytes")
        elif record.state == "REJECTED_PRECONDITION":
            await _restore_rejection(connection, record, database_generation=database_generation,
                                     writer_generation=writer_generation)
        else:
            registry = await resolve(command)
            handler = registry.resolve(command)
            if (handler.revisions.wire() != (command.schema_revision, command.function_revision,
                                             command.executor_revision)
                    or handler.fingerprint != expected["handler_sha256"]):
                raise ApplyConflict("exact historical handler unavailable")
            try:
                await prepare(command)
            except QueryCanceled:
                if keep_going():
                    raise
                report[record.state] -= 1
                return dict(report, next_sequence=command.sequence,
                            accepted_last_sequence=head.reference.last_sequence)
            receipt = await apply_accepted(connection, journal=journal, seal=record.seal,
                command=command, database_generation=database_generation,
                writer_generation=writer_generation, registry=registry)
            if receipt.outcome != record.outcome:
                fields, result_fields = outcome_difference(record.outcome, receipt.outcome)
                LOG.error("offline_pg_rebuild_outcome_mismatch sequence=%s fields=%s result_fields=%s",
                          command.sequence, fields, result_fields)
                raise ApplyConflict("replayed outcome differs; partial output must not be published")
        report["restored_prefix"] = command.sequence
        LOG.info("offline_pg_rebuild sequence=%s historical_state=%s", command.sequence, record.state)
    if last != head.reference.last_sequence:
        raise ApplyConflict("batch omits part of the accepted journal")
    return report
