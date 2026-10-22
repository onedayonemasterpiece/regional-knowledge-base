"""Retained native update boundary; no wire endpoint, loader, SQL RPC or permit.

The native host supplies reviewed bindings and existing admission/accepted-write
callbacks. All dispatchers must capture ``bindings()``; changing this aggregate
cannot affect model/PG ownership. v105 does not contain or wire this owner.
"""
from __future__ import annotations

import asyncio
import base64
from dataclasses import dataclass
import hashlib
import inspect
import json
from pathlib import PurePosixPath
from types import MappingProxyType
from uuid import UUID

from .kaggle_apply import ApplyReceipt, HandlerRegistry, Revisions
from .kaggle_bootstrap import PreparedRuntime
from .kaggle_warm_maintenance import WarmHandlerPatchApproval
from .recovery_journal import ExactCommand


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'),
                      ensure_ascii=False, allow_nan=False).encode()


def _digest(value):
    if type(value) is not str or len(value) != 64 or set(value) - set('0123456789abcdef'):
        raise NativeUpdateRejected('native_update_pin')
    return value


class NativeUpdateRejected(RuntimeError):
    """Fixed diagnostic only; no source, private paths or native SQL text."""


@dataclass(frozen=True)
class NativeResidentObjects:
    encoder: object
    postgres_owner: object
    reader_pool: object
    writer_pool: object

    def __post_init__(self):
        if any(value is None for value in vars(self).values()):
            raise NativeUpdateRejected('native_update_resident_objects')


@dataclass(frozen=True)
class NativeBindings:
    resources: NativeResidentObjects
    prepared: PreparedRuntime
    registry: HandlerRegistry
    recovery_handler: object
    revisions: Revisions
    code_sha256: str
    code_inventory: tuple[tuple[str, str, int, str], ...]

    def __post_init__(self):
        if (type(self.resources) is not NativeResidentObjects
                or type(self.prepared) is not PreparedRuntime
                or type(self.registry) is not HandlerRegistry
                or type(self.revisions) is not Revisions
                or not callable(self.recovery_handler)):
            raise NativeUpdateRejected('native_update_bindings')
        _digest(self.code_sha256)
        if (type(self.code_inventory) is not tuple or not self.code_inventory
                or self.code_inventory != tuple(sorted(self.code_inventory))
                or any(type(row) is not tuple or len(row) != 4 or row[0] != 'code'
                       or type(row[1]) is not str or type(row[2]) is not int or row[2] < 1
                       for row in self.code_inventory)
                or len({row[1] for row in self.code_inventory}) != len(self.code_inventory)
                or _sha(_canonical(self.code_inventory)) != self.code_sha256):
            raise NativeUpdateRejected('native_update_code_inventory')
        ready = self.prepared.readiness
        if (tuple(getattr(ready, key) for key in
                  ('schema_revision', 'function_revision', 'executor_revision')) != self.revisions.wire()
                or ready.hashes['code'] != self.code_sha256):
            raise NativeUpdateRejected('native_update_readiness_binding')


@dataclass(frozen=True)
class NativeUpdateBinding:
    run_id: str
    runtime_incarnation: str
    connection_generation: int
    native_generation: int

    def __post_init__(self):
        if (any(type(v) is not str or not 1 <= len(v.encode()) <= 256
                for v in (self.run_id, self.runtime_incarnation))
                or type(self.connection_generation) is not int or self.connection_generation < 1
                or type(self.native_generation) is not int or self.native_generation < 0):
            raise NativeUpdateRejected('native_update_binding')


@dataclass(frozen=True)
class NativeUpdateHead:
    sequence: int
    sha256: str
    applied_sequence: int
    pending_writes: int
    publication_pending: bool
    unknown: bool
    pending_operation_sha256: tuple[str, ...] = ()
    unknown_operation_sha256: tuple[str, ...] = ()

    def require_settled(self):
        _digest(self.sha256)
        if (type(self.sequence) is not int or self.sequence < 0
                or type(self.applied_sequence) is not int
                or self.applied_sequence != self.sequence
                or type(self.pending_writes) is not int or self.pending_writes != 0
                or self.publication_pending is not False or self.unknown is not False
                or self.pending_operation_sha256 or self.unknown_operation_sha256):
            raise NativeUpdateRejected('native_update_unsettled_writes')


@dataclass(frozen=True)
class NativeUpdatePlan:
    operation_key: str
    artifact: WarmHandlerPatchApproval
    origin_code_sha256: str
    origin: Revisions
    target: Revisions
    head: NativeUpdateHead
    registry_sha256: str
    accepted_head_sha256: str
    sql_pins: tuple[tuple[str, str], ...] = ()
    transitions: tuple[ExactCommand, ...] = ()
    host_abi: str = 'rkb.native-update-host.v1'

    def __post_init__(self):
        if (type(self.artifact) is not WarmHandlerPatchApproval
                or type(self.origin) is not Revisions or type(self.target) is not Revisions
                or type(self.head) is not NativeUpdateHead
                or type(self.sql_pins) is not tuple or type(self.transitions) is not tuple
                or len(self.sql_pins) > 2 or len(self.transitions) > 1):
            raise NativeUpdateRejected('native_update_plan')
        if type(self.operation_key) is not str or not 1 <= len(self.operation_key.encode()) <= 240:
            raise NativeUpdateRejected('native_update_operation_key')
        if self.host_abi != 'rkb.native-update-host.v1':
            raise NativeUpdateRejected('native_update_host_abi')
        _digest(self.origin_code_sha256)
        _digest(self.registry_sha256)
        _digest(self.accepted_head_sha256)
        self.head.require_settled()
        if bool(self.sql_pins) != bool(self.transitions) or bool(self.transitions) != (self.origin != self.target):
            raise NativeUpdateRejected('native_update_ordered_transition_required')
        names = []
        for name, digest in self.sql_pins:
            if name not in ('sql/012_geo_dependency_bound.sql', 'sql/013_source_stage.sql',
                            'sql/014_source_stage_activation.sql'):
                raise NativeUpdateRejected('native_update_sql_artifact')
            _digest(digest)
            names.append(name)
        if len(set(names)) != len(names):
            raise NativeUpdateRejected('native_update_sql_artifact')
        for index, command in enumerate(self.transitions):
            if (type(command) is not ExactCommand or command.kind != 'schema_transition'
                    or command.route not in ('geo.dependencies.upgrade.v2', 'source.native.upgrade.v1')
                    or command.sequence != self.head.sequence + index + 1):
                raise NativeUpdateRejected('native_update_ordered_transition_required')
        if not self.transitions and self.accepted_head_sha256 != self.head.sha256:
            raise NativeUpdateRejected('native_update_head_changed')


@dataclass(frozen=True)
class NativeTransitionReadback:
    """Trusted native observer result, never caller-supplied absence/success.

    An unapplied hash requires the actual immutable ledger's predecessor cursor,
    origin semantics and exact command absence, plus the ordered prepare pins.
    Missing ACK/receipt alone cannot construct this result.
    """
    receipts: tuple[ApplyReceipt, ...]
    native_sequence: int
    revisions: Revisions
    unapplied_command_sha256: tuple[str, ...] = ()

    def __post_init__(self):
        if (type(self.receipts) is not tuple or len(self.receipts) > 1
                or any(type(receipt) is not ApplyReceipt for receipt in self.receipts)
                or type(self.native_sequence) is not int or self.native_sequence < 0
                or type(self.revisions) is not Revisions
                or type(self.unapplied_command_sha256) is not tuple
                or len(self.unapplied_command_sha256) > 1):
            raise NativeUpdateRejected('native_update_transition_readback')
        for digest in self.unapplied_command_sha256:
            _digest(digest)


def _artifact_files(raw, approval):
    """Existing source_bundle canonical base64 file map; never execute it."""
    if (type(raw) is not bytes or not 1 <= len(raw) <= 3 * 1024 * 1024
            or len(raw) != approval.blob.size or _sha(raw) != approval.blob.sha256):
        raise NativeUpdateRejected('native_update_source_artifact')
    try:
        encoded = json.loads(raw)
        if type(encoded) is not dict or not encoded or _canonical(encoded) != raw:
            raise ValueError
        files = {}
        for name, value in encoded.items():
            path = PurePosixPath(name)
            if (type(name) is not str or path.is_absolute() or '..' in path.parts
                    or str(path) != name or len(path.parts) != 2
                    or not ((path.parts[0] == 'regional_knowledge' and path.suffix == '.py')
                            or (path.parts[0] == 'sql' and path.suffix == '.sql'))
                    or type(value) is not str):
                raise ValueError
            files[name] = base64.b64decode(value, validate=True)
        if sum(map(len, files.values())) > 2 * 1024 * 1024:
            raise ValueError
        return files
    except (ValueError, TypeError, UnicodeError, RecursionError):
        raise NativeUpdateRejected('native_update_source_artifact') from None


def updated_code_inventory(origin, files):
    """Use the EXISTING VerifiedArtifacts.role_hash row format, including native code.

    Source bundle identity is not the loaded code hash. Keep native tar/manifest
    rows and replace only the content-addressed Python/SQL source bundle rows.
    The fixed host ABI cannot replace itself during a compatible update.
    """
    prefixes = {row[1].split('/')[0] for row in origin
                if row[1].startswith('rkb-target-')}
    if len(prefixes) != 1:
        raise NativeUpdateRejected('native_update_source_inventory')
    prefix = next(iter(prefixes))
    _digest(prefix.removeprefix('rkb-target-'))
    old = {name[len(prefix) + 1:]: digest for role, name, size, digest in origin
           if name.startswith(prefix + '/')}
    host = 'regional_knowledge/kaggle_native_update.py'
    frozen = (host, 'regional_knowledge/kaggle_apply.py',
              'regional_knowledge/kaggle_bootstrap.py', 'regional_knowledge/recovery_journal.py')
    if host not in old:
        raise NativeUpdateRejected('native_update_host_hook_not_installed')
    if any(name not in files or _sha(files[name]) != old[name]
           for name in frozen if name in old):
        raise NativeUpdateRejected('native_update_host_abi_changed')
    manifest = {name: _sha(raw) for name, raw in sorted(files.items())}
    target = 'rkb-target-' + _sha(_canonical(manifest))
    rows = [row for row in origin if not row[1].startswith(prefix + '/')]
    rows.extend(('code', target + '/' + name, len(raw), manifest[name])
                for name, raw in files.items())
    return tuple(sorted(rows))


class NativeUpdateOwner:
    """Host-only state machine using the retained compute permit and writer.

    ``quiesce`` is the existing host admission/drain context, not a new lease.
    ``apply_transitions`` must use the existing accepted journal/prepare/apply
    path and return independently reconciled ApplyReceipts. It must not run
    supplied SQL or infer nonexecution after a lost ACK. ``validate`` and
    ``probe`` belong to the reviewed native composition, never the wire caller.
    """
    def __init__(self, active, *, current, quiesce, validate, probe, apply_transitions,
                 reconcile_transitions):
        if type(active) is not NativeBindings or not all(callable(x) for x in
                (current, quiesce, validate, probe, apply_transitions, reconcile_transitions)):
            raise TypeError('retained native host callbacks required')
        if inspect.iscoroutinefunction(current) or inspect.iscoroutinefunction(getattr(current, '__call__', None)):
            raise TypeError('synchronous retained current guard required')
        self.active = active
        self.current, self.quiesce = current, quiesce
        self.validate, self.probe, self.apply_transitions = validate, probe, apply_transitions
        self.reconcile_transitions = reconcile_transitions
        self.generation = 0
        self.state = 'active'
        self.staged = None
        self.previous = None
        self._pending_ack_key = None
        self._lock = asyncio.Lock()

    def _current(self, binding):
        if type(binding) is not NativeUpdateBinding or binding.native_generation != self.generation:
            raise NativeUpdateRejected('native_update_stale_generation')
        verdict = self.current(binding)  # Existing run/incarnation/socket/permit/writer checks.
        if verdict is not None and verdict is not True:
            raise NativeUpdateRejected('native_update_current_binding')

    def bindings(self):
        if self.state in ('quiescing', 'activating', 'reconcile_required', 'resume_ready', 'awaiting_ack'):
            raise NativeUpdateRejected('native_update_dispatch_fenced')
        return self.active

    def receipt(self):
        return dict(state=self.state, generation=self.generation,
                    code_sha256=self.active.code_sha256,
                    revisions=list(self.active.revisions.wire()),
                    acknowledgement_key=self._pending_ack_key,
                    staged_sha256=None if self.staged is None else self.staged[1].code_sha256)

    async def stage(self, binding, plan, candidate, artifact_bytes):
        async with self._lock:
            self._current(binding)
            if self.state not in ('active', 'staged') or type(plan) is not NativeUpdatePlan:
                raise NativeUpdateRejected('native_update_stage_state')
            if (type(candidate) is not NativeBindings or candidate.resources is not self.active.resources
                    or plan.origin_code_sha256 != self.active.code_sha256
                    or plan.origin != self.active.revisions or plan.target != candidate.revisions):
                raise NativeUpdateRejected('native_update_resident_context_changed')
            old_ready, ready = self.active.prepared.readiness, candidate.prepared.readiness
            if (ready.database_generation != old_ready.database_generation
                    or any(ready.hashes[key] != old_ready.hashes[key] for key in
                           ('model', 'dependency', 'launch_config', 'executed_source'))
                    or any(candidate.registry.handlers.get(key) != handler
                           for key, handler in self.active.registry.handlers.items())):
                raise NativeUpdateRejected('native_update_resident_context_changed')
            if (not plan.transitions and ready.hashes['schema'] != old_ready.hashes['schema']
                    or ready.applied_seq != plan.head.sequence + len(plan.transitions)):
                raise NativeUpdateRejected('native_update_readiness_binding')
            files = _artifact_files(artifact_bytes, plan.artifact)
            if updated_code_inventory(self.active.code_inventory, files) != candidate.code_inventory:
                raise NativeUpdateRejected('native_update_code_pin')
            if any(name not in files or _sha(files[name]) != digest for name, digest in plan.sql_pins):
                raise NativeUpdateRejected('native_update_sql_pin')
            if _sha(_canonical(sorted(h.fingerprint for h in candidate.registry.handlers.values()))) != plan.registry_sha256:
                raise NativeUpdateRejected('native_update_registry_pin')
            revision = plan.origin
            for command in plan.transitions:
                handler = candidate.registry.resolve(command)
                if handler.revisions != revision or handler.transition is None:
                    raise NativeUpdateRejected('native_update_transition_registry')
                payload = json.loads(command.payload)
                pins = dict(plan.sql_pins)
                if command.route == 'geo.dependencies.upgrade.v2':
                    if tuple(pins) != ('sql/012_geo_dependency_bound.sql',):
                        raise NativeUpdateRejected('native_update_sql_pin')
                    artifact_sha = pins.get('sql/012_geo_dependency_bound.sql')
                else:
                    names = ('013_source_stage.sql', '014_source_stage_activation.sql')
                    if tuple(pins) != tuple('sql/' + name for name in names):
                        raise NativeUpdateRejected('native_update_sql_pin')
                    artifact_sha = _sha(_canonical(dict(codec='rkb.native-source-upgrade.v1',
                        artifacts=[dict(name=name, sha256=pins['sql/' + name]) for name in names])))
                if artifact_sha != handler.transition.artifact_sha256 or payload.get('artifact_sha256') != artifact_sha:
                    raise NativeUpdateRejected('native_update_sql_pin')
                expected = dict(schema=revision.schema, function=revision.function,
                    executor=revision.executor, migration=handler.transition.predecessor or '',
                    installer_person_id=command.person_id)
                exact_payload = dict(migration_id=handler.transition.migration_id,
                    artifact_sha256=artifact_sha, predecessor=handler.transition.predecessor)
                if (command.expected_revisions != expected or command.payload != _canonical(exact_payload)
                        or str(UUID(command.person_id)) != command.person_id):
                    raise NativeUpdateRejected('native_update_transition_command')
                if command.route == 'geo.dependencies.upgrade.v2':
                    if set(command.ids) != {'manifest_id'}:
                        raise NativeUpdateRejected('native_update_transition_command')
                    _digest(command.ids['manifest_id'])
                elif command.ids != {}:
                    raise NativeUpdateRejected('native_update_transition_command')
                revision = handler.transition.target
            if revision != plan.target:
                raise NativeUpdateRejected('native_update_transition_registry')
            if not plan.transitions and candidate.registry != self.active.registry:
                raise NativeUpdateRejected('native_update_unversioned_registry_change')
            await self.validate(candidate, MappingProxyType(files))
            self._current(binding)
            self.staged = (plan, candidate)
            self.state = 'staged'
            return self.receipt()

    def _head(self, head, plan, *, reconciling=False):
        if type(head) is not NativeUpdateHead:
            raise NativeUpdateRejected('native_update_head_readback')
        end = plan.head.sequence + len(plan.transitions)
        expected = tuple(_sha(c.encode()) for c in plan.transitions
                         if c.sequence > head.applied_sequence)
        known = {_sha(c.encode()) for c in plan.transitions}
        if (type(head.sequence) is not int or head.sequence != end
                or type(head.applied_sequence) is not int
                or not plan.head.sequence <= head.applied_sequence <= end
                or head.sha256 != plan.accepted_head_sha256
                or type(head.pending_writes) is not int or head.pending_writes != len(expected)
                or head.pending_operation_sha256 != expected
                or head.publication_pending is not False
                or type(head.unknown) is not bool
                or type(head.pending_operation_sha256) is not tuple
                or type(head.unknown_operation_sha256) is not tuple
                or bool(head.unknown_operation_sha256) != head.unknown
                or (head.unknown and not reconciling)
                or not set(head.unknown_operation_sha256) <= known):
            raise NativeUpdateRejected('native_update_unsettled_writes')

    def _receipts(self, plan, candidate, receipts):
        from .kaggle_source_commands import decode_receipt, receipt_wire
        if type(receipts) is not tuple or len(receipts) != len(plan.transitions):
            raise NativeUpdateRejected('native_update_transition_receipt')
        for command, receipt in zip(plan.transitions, receipts, strict=True):
            handler = candidate.registry.resolve(command)
            if (type(receipt) is not ApplyReceipt or receipt.status != 'applied'
                    or receipt.database_generation != candidate.prepared.readiness.database_generation
                    or receipt.command_sha256 != _sha(command.encode())
                    or receipt.operation_key != command.operation_key or receipt.sequence != command.sequence):
                raise NativeUpdateRejected('native_update_transition_receipt')
            _, outcome = decode_receipt(receipt_wire(receipt))
            if (outcome['handler_sha256'] != handler.fingerprint
                    or outcome['before'] != list(handler.revisions.wire())
                    or outcome['after'] != list(handler.transition.target.wire())
                    or any(outcome[key] != getattr(command, key) for key in
                           ('ids', 'request_id', 'person_id', 'route', 'logical_time'))):
                raise NativeUpdateRejected('native_update_transition_receipt')

    def _cutover(self, plan, candidate):
        self.previous = (self.active, plan)
        self.active = candidate
        self.generation += 1
        self.staged = None
        self._pending_ack_key = plan.operation_key + ':activate'
        self.state = 'awaiting_ack'

    def acknowledge(self, binding, *, operation_key, code_sha256):
        """Host calls ONLY after matching existing durable RESULT_ACK + adoption.

        This is not a new authorization source: the WSS owner must already have
        matched job/claim/request to the receipt and current runtime adoption.
        """
        self._current(binding)
        if self._lock.locked():
            raise NativeUpdateRejected('native_update_ack_before_boundary')
        if (self.state != 'awaiting_ack' or operation_key != self._pending_ack_key
                or code_sha256 != self.active.code_sha256):
            raise NativeUpdateRejected('native_update_ack_binding')
        self._pending_ack_key = None
        self.state = 'active'
        return self.receipt()

    async def activate(self, binding):
        async with self._lock:
            self._current(binding)
            if self.state not in ('staged', 'resume_ready') or self.staged is None:
                raise NativeUpdateRejected('native_update_no_validated_stage')
            plan, candidate = self.staged
            was_resume = self.state == 'resume_ready'
            self.state = 'quiescing'
            transition_started = False
            try:
                async with self.quiesce(binding) as head:
                    self._current(binding)
                    self._head(head, plan)
                    self.state = 'activating'
                    if plan.transitions:
                        transition_started = True
                        receipts = await self.apply_transitions(plan.transitions, candidate)
                        self._current(binding)
                        self._receipts(plan, candidate, receipts)
                    await self.probe(candidate)
                    self._current(binding)
                    self._cutover(plan, candidate)  # One synchronous aggregate cutover.
            except BaseException:
                # Unknown/committed schema must NEVER reopen old schema bindings.
                self.state = ('reconcile_required' if transition_started or self.active is candidate
                              else 'resume_ready' if was_resume else 'staged')
                if self.active is candidate:
                    self.staged = (plan, candidate)
                raise
            return self.receipt()

    async def reconcile(self, binding):
        """Read back the SAME accepted transitions; never submit them again."""
        async with self._lock:
            self._current(binding)
            if self.state != 'reconcile_required' or self.staged is None:
                raise NativeUpdateRejected('native_update_reconcile_state')
            plan, candidate = self.staged
            try:
                async with self.quiesce(binding) as head:
                    self._current(binding)
                    self._head(head, plan, reconciling=True)
                    progress = await self.reconcile_transitions(plan.transitions, candidate)
                    self._current(binding)
                    if type(progress) is not NativeTransitionReadback:
                        raise NativeUpdateRejected('native_update_transition_readback')
                    unapplied = tuple(_sha(c.encode()) for c in plan.transitions)
                    if (plan.transitions and progress.receipts == ()
                            and progress.native_sequence == plan.head.sequence
                            and progress.revisions == plan.origin
                            and progress.unapplied_command_sha256 == unapplied):
                        # Explicit later activate may resume these SAME accepted
                        # bytes; no automatic resubmission or fresh operation.
                        self.state = 'resume_ready'
                        return self.receipt()
                    if (progress.native_sequence != plan.head.sequence + len(plan.transitions)
                            or progress.revisions != plan.target or progress.unapplied_command_sha256):
                        raise NativeUpdateRejected('native_update_transition_readback')
                    self._receipts(plan, candidate, progress.receipts)
                    await self.probe(candidate)
                    self._current(binding)
                    if self.active is candidate:
                        self.staged = None
                        self.state = 'awaiting_ack'
                    else:
                        self._cutover(plan, candidate)
            except BaseException:
                self.staged = (plan, candidate)
                self.state = 'reconcile_required'
                raise
            return self.receipt()

    async def rollback(self, binding):
        async with self._lock:
            self._current(binding)
            if self.state == 'staged':
                self.staged = None
                self.state = 'active'
                return self.receipt()
            if self.state != 'active' or self.previous is None:
                raise NativeUpdateRejected('native_update_rollback_requires_reconciliation')
            previous, plan = self.previous
            if previous.revisions != self.active.revisions:
                raise NativeUpdateRejected('native_update_committed_schema_not_reversible')
            self.state = 'quiescing'
            try:
                async with self.quiesce(binding) as head:
                    self._current(binding)
                    head.require_settled()
                    if head != plan.head:
                        raise NativeUpdateRejected('native_update_head_changed')
                    await self.probe(previous)
                    self._current(binding)
                    self.active = previous
                    self.generation += 1
                    self.previous = None
                    self._pending_ack_key = plan.operation_key + ':rollback'
                    self.state = 'awaiting_ack'
            finally:
                if self.state == 'quiescing':
                    self.state = 'active'
            return self.receipt()
