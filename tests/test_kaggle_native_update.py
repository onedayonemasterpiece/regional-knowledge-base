"""Host seam only: actual binding/registry types, synthetic resources and ledger.

No PostgreSQL, model, source execution, provider, credentials or WSS deployment.
"""
import asyncio
import base64
from contextlib import asynccontextmanager
from dataclasses import replace
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from regional_knowledge.geo_manifest_commands import (
    GEO_REVISIONS, GEO_V2_REVISIONS, geo_dependency_repair_commands,
    geo_manifest_handler, plan_geo_manifest,
)
from regional_knowledge.geo_whitelist import build_geo_whitelist
from regional_knowledge.kaggle_apply import ApplyReceipt, HandlerRegistry
from regional_knowledge.kaggle_bootstrap import CandidateReadiness, PreparedRuntime
from regional_knowledge.kaggle_geo_upgrade import GeoUpgradePins
from regional_knowledge.kaggle_native_update import (
    NativeBindings, NativeResidentObjects, NativeUpdateHead, NativeUpdateBinding,
    NativeUpdateOwner, NativeUpdatePlan, NativeUpdateRejected, NativeTransitionReadback,
    updated_code_inventory,
)
from regional_knowledge.kaggle_pg_snapshot_native import BlobRef
from regional_knowledge.kaggle_readiness import RUNTIME_HASHES
from regional_knowledge.kaggle_warm_maintenance import WarmHandlerPatchApproval
from test_geo_manifest_commands import commands
from test_geo_migration import snapshot


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


async def handler(*args, **kwargs):
    return {'fixture': True}


def bindings(resources, registry, revisions, inventory, sequence):
    code = sha(canonical(inventory))
    readiness = CandidateReadiness(
        dict({key: 'a' * 64 for key in RUNTIME_HASHES}, code=code),
        'synthetic-db', *revisions.wire(), sequence, sequence, 0, True, True)
    return NativeBindings(resources, PreparedRuntime(handler, readiness), registry,
                          handler, revisions, code, inventory)


class Host:
    def __init__(self, *, schema=False):
        self.resources = NativeResidentObjects(*(object() for _ in range(4)))
        registry = HandlerRegistry([geo_manifest_handler(
            revisions=GEO_REVISIONS, check_sha256='a' * 64, apply_sha256='b' * 64)])
        originals = commands(plan_geo_manifest(build_geo_whitelist(**snapshot())))
        self.head = NativeUpdateHead(originals[-1].sequence, 'c' * 64,
                                      originals[-1].sequence, 0, False, False)
        host_name = 'regional_knowledge/kaggle_native_update.py'
        host_bytes = Path(__file__).resolve().parents[1].joinpath('src', host_name).read_bytes()
        old_files = {host_name: host_bytes, 'regional_knowledge/reviewed_update.py': b'old source\n'}
        prefix = 'rkb-target-' + sha(canonical({name: sha(raw) for name, raw in sorted(old_files.items())}))
        old_inventory = tuple(sorted([('code', 'native/runtime.tar', 7, 'a' * 64)] + [
            ('code', prefix + '/' + name, len(raw), sha(raw)) for name, raw in old_files.items()]))
        self.old = bindings(self.resources, registry, GEO_REVISIONS, old_inventory, self.head.sequence)
        files = {host_name: host_bytes, 'regional_knowledge/reviewed_update.py': b'raise AssertionError("must never execute")\n'}
        transitions = ()
        sql_pins = ()
        revisions = GEO_REVISIONS
        if schema:
            name = 'sql/012_geo_dependency_bound.sql'
            raw = (Path(__file__).resolve().parents[1] / name.replace('sql/', 'sql/kaggle/')).read_bytes()
            files[name] = raw
            pins = GeoUpgradePins(sha(raw), *('e' * 64 for _ in range(6)))
            registry = pins.registry(registry)
            outcomes = {c.operation_key: 'applied' for c in originals}
            for c in (originals[1], originals[-1]):
                outcomes[c.operation_key] = 'rejected_precondition'
            transitions = geo_dependency_repair_commands(
                originals, settled_outcomes=outcomes, artifact_sha256=sha(raw))[:1]
            sql_pins = ((name, sha(raw)),)
            revisions = GEO_V2_REVISIONS
        self.raw = canonical({name: base64.b64encode(raw).decode() for name, raw in files.items()})
        inventory = updated_code_inventory(old_inventory, files)
        self.candidate = bindings(self.resources, registry, revisions, inventory,
                                  self.head.sequence + len(transitions))
        approval = WarmHandlerPatchApproval(
            BlobRef('recovery/v1/fixture/blobs/' + sha(self.raw), len(self.raw), sha(self.raw), 'fixture-etag'),
            'b' * 40)
        self.plan = NativeUpdatePlan('fixture-native-update', approval, self.old.code_sha256,
            self.old.revisions, revisions, self.head,
            sha(canonical(sorted(h.fingerprint for h in registry.handlers.values()))),
            'f' * 64 if transitions else self.head.sha256, sql_pins, transitions)
        if transitions:
            # Dev has already accepted the exact barrier command. Kaggle must
            # neither seal it itself nor reject this declared pending write.
            self.head = replace(self.head, sequence=self.head.sequence + 1, sha256='f' * 64,
                pending_writes=1, pending_operation_sha256=tuple(sha(c.encode()) for c in transitions))
        self.connection_generation = 2
        self.quiesced = False
        self.validate_calls = self.apply_calls = self.reconcile_calls = self.probe_calls = 0
        self.ledger = ()
        self.schema = GEO_REVISIONS
        self.lose_ack = self.fail_probe = self.stale_validate = self.stale_probe = False
        self.fail_before_commit = False
        self.fail_exit_once = False
        self.owner = NativeUpdateOwner(self.old, current=self.current, quiesce=self.quiesce,
            validate=self.validate, probe=self.probe, apply_transitions=self.apply,
            reconcile_transitions=self.reconcile)

    def binding(self):
        return NativeUpdateBinding('fixture-run', 'fixture-incarnation', self.connection_generation,
                                   self.owner.generation)

    def current(self, binding):
        if binding.connection_generation != self.connection_generation:
            raise NativeUpdateRejected('fixture_stale_connection')

    @asynccontextmanager
    async def quiesce(self, binding):
        self.quiesced = True
        try:
            with pytest.raises(NativeUpdateRejected, match='dispatch_fenced'):
                self.owner.bindings()
            yield self.head
            if self.fail_exit_once:
                self.fail_exit_once = False
                raise NativeUpdateRejected('fixture_quiesce_exit_failed')
        finally:
            self.quiesced = False

    async def validate(self, candidate, files):
        self.validate_calls += 1
        assert not self.quiesced and self.apply_calls == 0
        assert candidate.resources is self.resources
        assert files['regional_knowledge/reviewed_update.py'].startswith(b'raise AssertionError')
        await asyncio.sleep(0)
        if self.stale_validate:
            self.connection_generation += 1

    async def probe(self, candidate):
        self.probe_calls += 1
        assert self.quiesced and candidate.resources is self.resources
        assert self.schema == candidate.revisions
        await asyncio.sleep(0)
        if self.stale_probe:
            self.connection_generation += 1
        if self.fail_probe:
            raise NativeUpdateRejected('fixture_probe_failed')

    async def apply(self, transitions, candidate):
        assert self.quiesced
        self.apply_calls += 1
        assert not self.ledger, 'lost commit ACK must reconcile rather than resubmit'
        if self.fail_before_commit:
            raise TimeoutError('synthetic interruption before commit')
        receipts = []
        for command in transitions:
            spec = candidate.registry.resolve(command)
            body = canonical(dict(codec='rkb.applied-outcome.v1', operation_key=command.operation_key,
                command_sha256=sha(command.encode()), cursor=command.sequence, status='applied',
                ids=command.ids, request_id=command.request_id, person_id=command.person_id,
                route=command.route, logical_time=command.logical_time,
                before=list(spec.revisions.wire()), after=list(spec.transition.target.wire()),
                handler_sha256=spec.fingerprint, result_b64=base64.b64encode(b'{}').decode(),
                result_sha256=sha(b'{}')))
            receipts.append(ApplyReceipt('synthetic-db', command.operation_key, command.sequence,
                sha(command.encode()), 'applied', body, sha(body), False))
        self.ledger = tuple(receipts)
        self.schema = candidate.revisions
        if self.lose_ack:
            self.head = replace(self.head, unknown=True,
                unknown_operation_sha256=tuple(sha(c.encode()) for c in transitions))
            raise TimeoutError('synthetic lost ACK after commit')
        self.head = replace(self.head, applied_sequence=self.head.sequence,
            pending_writes=0, pending_operation_sha256=(), unknown=False, unknown_operation_sha256=())
        return self.ledger

    async def reconcile(self, transitions, candidate):
        assert self.quiesced
        self.reconcile_calls += 1
        if self.ledger:
            self.head = replace(self.head, applied_sequence=self.head.sequence,
                pending_writes=0, pending_operation_sha256=(), unknown=False, unknown_operation_sha256=())
            return NativeTransitionReadback(self.ledger, self.ledger[-1].sequence, self.schema)
        return NativeTransitionReadback((), self.plan.head.sequence, self.schema,
            tuple(sha(c.encode()) for c in transitions))

    async def stage(self):
        return await self.owner.stage(self.binding(), self.plan, self.candidate, self.raw)

    def acknowledge(self):
        return self.owner.acknowledge(self.binding(),
            operation_key=self.owner.receipt()['acknowledgement_key'],
            code_sha256=self.owner.active.code_sha256)


@pytest.mark.asyncio
async def test_stage_validate_quiesce_activate_ack_and_code_rollback_keep_resident_objects():
    h = Host()
    await h.stage()
    assert h.owner.bindings() is h.old and h.validate_calls == 1
    result = await h.owner.activate(h.binding())
    assert result['state'] == 'awaiting_ack' and result['generation'] == 1
    with pytest.raises(NativeUpdateRejected, match='dispatch_fenced'):
        h.owner.bindings()
    h.acknowledge()
    assert h.owner.bindings() is h.candidate
    assert h.owner.bindings().resources is h.old.resources is h.resources
    await h.owner.rollback(h.binding())
    assert h.owner.receipt()['state'] == 'awaiting_ack'
    h.acknowledge()
    assert h.owner.bindings() is h.old and h.owner.generation == 2
    assert h.apply_calls == 0


@pytest.mark.asyncio
@pytest.mark.parametrize('change', ['source', 'sql', 'registry', 'resources', 'model', 'old_handler'])
async def test_invalid_stage_never_reaches_validation_or_resources(change):
    h = Host(schema=True)
    if change == 'source':
        h.raw += b' '
    elif change == 'sql':
        h.plan = replace(h.plan, sql_pins=((h.plan.sql_pins[0][0], 'f' * 64),))
    elif change == 'registry':
        h.plan = replace(h.plan, registry_sha256='f' * 64)
    elif change == 'resources':
        h.candidate = replace(h.candidate, resources=NativeResidentObjects(*(object() for _ in range(4))))
    elif change == 'model':
        ready = h.candidate.prepared.readiness
        ready = replace(ready, hashes=dict(ready.hashes, model='f' * 64))
        h.candidate = replace(h.candidate, prepared=replace(h.candidate.prepared, readiness=ready))
    else:
        transition = h.candidate.registry.resolve(h.plan.transitions[0])
        h.candidate = replace(h.candidate, registry=HandlerRegistry([transition]))
    with pytest.raises(NativeUpdateRejected):
        await h.stage()
    assert h.owner.bindings() is h.old
    assert h.validate_calls == h.apply_calls == h.probe_calls == 0


@pytest.mark.asyncio
async def test_stale_connection_after_validation_cannot_stage():
    h = Host()
    h.stale_validate = True
    with pytest.raises(NativeUpdateRejected, match='stale_connection'):
        await h.stage()
    assert h.owner.staged is None and h.owner.bindings() is h.old


@pytest.mark.asyncio
@pytest.mark.parametrize('change', ['pending', 'publication', 'unknown', 'cursor', 'head'])
async def test_unsettled_or_changed_head_blocks_before_any_schema_or_code_effect(change):
    h = Host(schema=True)
    await h.stage()
    update = dict(pending=dict(pending_writes=2), publication=dict(publication_pending=True),
        unknown=dict(unknown=True), cursor=dict(applied_sequence=h.plan.head.sequence - 1),
        head=dict(sha256='9' * 64))[change]
    h.head = replace(h.head, **update)
    with pytest.raises(NativeUpdateRejected):
        await h.owner.activate(h.binding())
    assert h.owner.bindings() is h.old and h.apply_calls == h.probe_calls == 0
    assert h.owner.generation == 0


@pytest.mark.asyncio
async def test_schema_commit_then_lost_ack_reconciles_exact_ledger_without_reapplying():
    h = Host(schema=True)
    await h.stage()
    h.lose_ack = True
    with pytest.raises(TimeoutError):
        await h.owner.activate(h.binding())
    assert h.owner.state == 'reconcile_required'
    with pytest.raises(NativeUpdateRejected, match='dispatch_fenced'):
        h.owner.bindings()
    with pytest.raises(NativeUpdateRejected, match='reconciliation'):
        await h.owner.rollback(h.binding())
    await h.owner.reconcile(h.binding())
    h.acknowledge()
    assert h.owner.bindings() is h.candidate and h.schema == GEO_V2_REVISIONS
    assert h.apply_calls == h.reconcile_calls == 1
    assert h.owner.bindings().resources is h.resources
    with pytest.raises(NativeUpdateRejected, match='not_reversible'):
        await h.owner.rollback(h.binding())


@pytest.mark.asyncio
async def test_probe_failure_after_schema_commit_stays_fenced_until_actual_readback():
    h = Host(schema=True)
    await h.stage()
    h.fail_probe = True
    with pytest.raises(NativeUpdateRejected, match='probe_failed'):
        await h.owner.activate(h.binding())
    assert h.owner.state == 'reconcile_required' and h.apply_calls == 1
    h.fail_probe = False
    await h.owner.reconcile(h.binding())
    h.acknowledge()
    assert h.owner.generation == 1 and h.apply_calls == 1


@pytest.mark.asyncio
async def test_stale_probe_and_wrong_generation_or_ack_never_reopen_dispatch():
    h = Host(schema=True)
    binding = h.binding()
    await h.stage()
    h.stale_probe = True
    with pytest.raises(NativeUpdateRejected, match='stale_connection'):
        await h.owner.activate(binding)
    assert h.owner.state == 'reconcile_required'
    h.stale_probe = False
    await h.owner.reconcile(h.binding())
    with pytest.raises(NativeUpdateRejected, match='stale_generation'):
        h.owner.acknowledge(binding, operation_key='fixture-native-update:activate',
                            code_sha256=h.candidate.code_sha256)
    with pytest.raises(NativeUpdateRejected, match='ack_binding'):
        h.owner.acknowledge(h.binding(), operation_key='foreign', code_sha256=h.candidate.code_sha256)
    with pytest.raises(NativeUpdateRejected, match='dispatch_fenced'):
        h.owner.bindings()
    h.acknowledge()
    assert h.owner.bindings() is h.candidate


@pytest.mark.asyncio
async def test_code_probe_failure_can_discard_stage_and_keeps_old_bindings():
    h = Host()
    await h.stage()
    h.fail_probe = True
    with pytest.raises(NativeUpdateRejected, match='probe_failed'):
        await h.owner.activate(h.binding())
    assert h.owner.bindings() is h.old and h.owner.state == 'staged'
    await h.owner.rollback(h.binding())
    assert h.owner.staged is None and h.owner.bindings() is h.old


@pytest.mark.asyncio
async def test_verified_native_predecessor_allows_explicit_same_command_resume_after_no_commit():
    h = Host(schema=True)
    await h.stage()
    h.fail_before_commit = True
    with pytest.raises(TimeoutError):
        await h.owner.activate(h.binding())
    with pytest.raises(NativeUpdateRejected, match='no_validated_stage'):
        await h.owner.activate(h.binding())
    await h.owner.reconcile(h.binding())
    assert h.owner.state == 'resume_ready' and not h.ledger
    with pytest.raises(NativeUpdateRejected, match='dispatch_fenced'):
        h.owner.bindings()
    h.fail_before_commit = False
    await h.owner.activate(h.binding())
    h.acknowledge()
    assert h.apply_calls == 2 and len(h.ledger) == 1
    assert h.owner.bindings() is h.candidate


@pytest.mark.asyncio
async def test_foreign_unknown_write_blocks_even_readback_reconciliation():
    h = Host(schema=True)
    await h.stage()
    h.lose_ack = True
    with pytest.raises(TimeoutError):
        await h.owner.activate(h.binding())
    h.head = replace(h.head, unknown_operation_sha256=('9' * 64,))
    with pytest.raises(NativeUpdateRejected, match='unsettled_writes'):
        await h.owner.reconcile(h.binding())
    assert h.reconcile_calls == 0 and h.owner.state == 'reconcile_required'


@pytest.mark.asyncio
async def test_cutover_exit_failure_reconciles_without_second_generation_or_resource_swap():
    h = Host()
    await h.stage()
    h.fail_exit_once = True
    with pytest.raises(NativeUpdateRejected, match='exit_failed'):
        await h.owner.activate(h.binding())
    assert h.owner.generation == 1 and h.owner.state == 'reconcile_required'
    assert h.owner.active is h.candidate
    await h.owner.reconcile(h.binding())
    assert h.owner.generation == 1
    h.acknowledge()
    assert h.owner.bindings() is h.candidate and h.owner.bindings().resources is h.resources


@pytest.mark.asyncio
async def test_false_guard_cannot_stage_even_with_exact_artifact_pins():
    h = Host()
    h.owner.current = lambda binding: False
    with pytest.raises(NativeUpdateRejected, match='current_binding'):
        await h.stage()
    assert h.validate_calls == 0 and h.owner.active is h.old


@pytest.mark.asyncio
async def test_missing_native_readback_never_becomes_permission_to_retry():
    h = Host(schema=True)
    await h.stage()
    h.fail_before_commit = True
    with pytest.raises(TimeoutError):
        await h.owner.activate(h.binding())
    async def no_proof(*args):
        return None
    h.owner.reconcile_transitions = no_proof
    with pytest.raises(NativeUpdateRejected, match='transition_readback'):
        await h.owner.reconcile(h.binding())
    assert h.owner.state == 'reconcile_required' and h.apply_calls == 1


@pytest.mark.asyncio
async def test_foreign_pending_hash_cannot_hide_inside_declared_barrier_count():
    h = Host(schema=True)
    await h.stage()
    h.head = replace(h.head, pending_operation_sha256=('9' * 64,))
    with pytest.raises(NativeUpdateRejected, match='unsettled_writes'):
        await h.owner.activate(h.binding())
    assert h.apply_calls == 0 and h.owner.bindings() is h.old


def test_loaded_code_pin_uses_existing_full_inventory_format_and_preserves_native_rows():
    from regional_knowledge.kaggle_bootstrap import VerifiedArtifacts
    h = Host()
    observed = VerifiedArtifacts(Path('/synthetic-not-a-verification'), (), h.candidate.code_inventory)
    assert observed.role_hash({'code'}) == h.candidate.code_sha256
    assert ('code', 'native/runtime.tar', 7, 'a' * 64) in h.candidate.code_inventory
    files = json.loads(h.raw)
    source_id = sha(canonical({name: sha(base64.b64decode(raw)) for name, raw in sorted(files.items())}))
    assert source_id != h.candidate.code_sha256


def test_current_host_hook_and_frozen_abi_cannot_be_installed_or_changed_by_this_seam():
    h = Host()
    files = {name: base64.b64decode(raw) for name, raw in json.loads(h.raw).items()}
    old = tuple(row for row in h.old.code_inventory
                if not row[1].endswith('/regional_knowledge/kaggle_native_update.py'))
    with pytest.raises(NativeUpdateRejected, match='host_hook_not_installed'):
        updated_code_inventory(old, files)
    files['regional_knowledge/kaggle_native_update.py'] = b'changed host ABI\n'
    with pytest.raises(NativeUpdateRejected, match='host_abi_changed'):
        updated_code_inventory(h.old.code_inventory, files)


@pytest.mark.asyncio
async def test_native_artifact_change_cannot_hide_under_a_new_source_hash():
    h = Host()
    bad = tuple(sorted((role, name, size, 'f' * 64 if name == 'native/runtime.tar' else digest)
                       for role, name, size, digest in h.candidate.code_inventory))
    h.candidate = bindings(h.resources, h.candidate.registry, h.candidate.revisions,
                           bad, h.candidate.prepared.readiness.applied_seq)
    with pytest.raises(NativeUpdateRejected, match='code_pin'):
        await h.stage()
    assert h.validate_calls == 0


@pytest.mark.asyncio
async def test_failed_resume_preflight_keeps_dispatch_fenced():
    h = Host(schema=True)
    await h.stage()
    h.fail_before_commit = True
    with pytest.raises(TimeoutError):
        await h.owner.activate(h.binding())
    await h.owner.reconcile(h.binding())
    h.head = replace(h.head, pending_operation_sha256=('9' * 64,))
    with pytest.raises(NativeUpdateRejected, match='unsettled_writes'):
        await h.owner.activate(h.binding())
    assert h.owner.state == 'resume_ready'
    with pytest.raises(NativeUpdateRejected, match='dispatch_fenced'):
        h.owner.bindings()


@pytest.mark.asyncio
async def test_cancellation_after_commit_requires_reconciliation_without_reapply():
    h = Host(schema=True)
    await h.stage()
    async def cancelled_after_commit(*args):
        await h.apply(*args)
        raise asyncio.CancelledError()
    h.owner.apply_transitions = cancelled_after_commit
    with pytest.raises(asyncio.CancelledError):
        await h.owner.activate(h.binding())
    assert h.owner.state == 'reconcile_required' and not h.quiesced
    await h.owner.reconcile(h.binding())
    h.acknowledge()
    assert h.apply_calls == 1 and h.owner.bindings() is h.candidate


@pytest.mark.asyncio
async def test_ack_cannot_open_dispatch_before_quiescence_boundary_returns():
    h = Host()
    original = h.quiesce
    @asynccontextmanager
    async def premature_ack(binding):
        async with original(binding) as head:
            yield head
            with pytest.raises(NativeUpdateRejected, match='ack_before_boundary'):
                h.acknowledge()
            with pytest.raises(NativeUpdateRejected, match='dispatch_fenced'):
                h.owner.bindings()
    h.owner.quiesce = premature_ack
    await h.stage()
    await h.owner.activate(h.binding())
    h.acknowledge()
    assert h.owner.bindings() is h.candidate
