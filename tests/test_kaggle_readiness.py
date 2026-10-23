from dataclasses import replace
from hashlib import sha256

import pytest

from regional_knowledge.kaggle_readiness import (
    ArtifactManifest, ArtifactRequirement, CapabilityReceipt, DownloadedArtifact,
    RUNTIME_HASHES, RunIdentity, RuntimeObservation, TerminalObservation,
    validate_capabilities, validate_runtime, validate_terminal, verify_outputs,
)


@pytest.fixture
def identity():
    return RunIdentity("fixture/notebook", "7", "execution-1", "run-1", "nonce-1", "instance-1")


@pytest.fixture
def capabilities():
    return CapabilityReceipt("fixture-sdk", "1.0", "fixture-adapter", "local-fixture",
                             exact_status=True, status_identifies_execution=True)


def test_ready_requires_exact_identity_hashes_and_authenticated_database(identity):
    hashes = {key: sha256(key.encode()).hexdigest() for key in RUNTIME_HASHES}
    runtime = RuntimeObservation(identity, hashes, True, "ready", True)
    assert validate_runtime(identity, hashes, runtime).ok
    for changed in (replace(runtime, authenticated=False), replace(runtime, database_ready=False),
                    replace(runtime, hashes={**hashes, "launch_config": "0" * 64}),
                    replace(runtime, identity=replace(identity, provider_version="8"))):
        assert not validate_runtime(identity, hashes, changed).ok
    assert validate_runtime(identity, hashes, runtime,
                            competing_incarnations=["instance-2"]).issues == ("competing_runtime_incarnation",)


def test_controller_cannot_skip_hash_attestation(identity):
    with pytest.raises(ValueError):
        validate_runtime(identity, {}, RuntimeObservation(identity, {}, True, "ready", True))


def outputs(identity):
    payload = b"small deterministic probe"
    required = ArtifactRequirement("probe/result.json", len(payload), sha256(payload).hexdigest())
    return [required], ArtifactManifest(identity, (required,)), {
        required.name: DownloadedArtifact(identity, payload, True),
    }


def test_all_required_bytes_and_native_readback_verified(identity):
    required, manifest, downloaded = outputs(identity)
    assert verify_outputs(identity, required, manifest, downloaded).ok
    assert "required_artifact_missing" in verify_outputs(identity, required, manifest, {}).issues
    assert not verify_outputs(identity, required, None, downloaded).ok
    assert not verify_outputs(identity, required, replace(manifest, members=()), downloaded).ok
    member = required[0].name
    for changed in (replace(downloaded[member], content=b"corrupt"),
                    replace(downloaded[member], native_readback_verified=False),
                    replace(downloaded[member], identity=replace(identity, provider_version="8"))):
        assert not verify_outputs(identity, required, manifest, {member: changed}).ok


def test_latest_version_complete_and_output_do_not_confirm_original(identity, capabilities):
    latest = replace(identity, provider_version="8", provider_execution_id="execution-2")
    required, manifest, downloaded = outputs(latest)
    assert not verify_outputs(identity, required, manifest, downloaded).ok
    assert not terminal(identity, capabilities, TerminalObservation(latest, "complete", 20, execution_binding_verified=True)).ok


def terminal(identity, capabilities, observation, **kwargs):
    return validate_terminal(identity, capabilities, observation, last_heartbeat_at=10,
                             now=30, max_age_seconds=30, **kwargs)


@pytest.mark.parametrize("source", ["stop_ack", "delete_ack", "resource_absent", "lease_expired", "child_exit", "credential_revoked"])
def test_ack_delete_ttl_and_local_exit_leave_termination_unknown(identity, capabilities, source):
    observed = TerminalObservation(identity, "complete", 20, source, True)
    assert not terminal(identity, capabilities, observed).ok


def test_terminal_requires_fresh_exact_execution_evidence(identity, capabilities):
    observed = TerminalObservation(identity, "complete", 20, execution_binding_verified=True)
    assert terminal(identity, capabilities, observed).ok
    for changed in (replace(observed, observed_at=9), replace(observed, observed_at=40),
                    replace(observed, observed_at=float("nan")), replace(observed, status="running"),
                    replace(observed, execution_binding_verified=False),
                    replace(observed, identity=replace(identity, runtime_instance_id="instance-2"))):
        assert not terminal(identity, capabilities, changed).ok
    assert not terminal(identity, capabilities, observed, contradictory_runtime_evidence=True).ok
    assert not terminal(identity, capabilities, None).ok
    assert not terminal(identity, replace(capabilities, status_identifies_execution=False), observed).ok
    assert not terminal(identity, replace(capabilities, installed_version=""), observed).ok
    assert not validate_terminal(identity, capabilities, observed, last_heartbeat_at=10,
                                 now=100, max_age_seconds=30).ok


def test_unknown_execution_id_is_not_fabricated_and_requires_version_semantics(identity, capabilities):
    identity = replace(identity, provider_execution_id=None)
    observed = TerminalObservation(identity, "complete", 20, execution_binding_verified=True)
    assert terminal(identity, capabilities, observed).ok
    assert not terminal(identity, replace(capabilities, status_identifies_execution=False), observed).ok
    assert identity.provider_execution_id is None


@pytest.mark.parametrize("name", ["../escape", "/absolute", "a/../b", "a//b", "a\\b", "./a"])
def test_unsafe_artifact_paths_rejected(name):
    with pytest.raises(ValueError):
        ArtifactRequirement(name, 0, sha256(b"").hexdigest())


def test_inventory_and_byte_budgets(identity):
    required, manifest, downloaded = outputs(identity)
    assert verify_outputs(identity, required, manifest, downloaded, max_total_bytes=1).issues == ("artifact_bytes_budget_exceeded",)
    extra = ArtifactRequirement("second", 0, sha256(b"").hexdigest())
    assert verify_outputs(identity, [*required, extra], manifest, downloaded, max_artifacts=1).issues == ("artifact_count_budget_exceeded",)
    assert not verify_outputs(identity, required, replace(manifest, members=manifest.members * 2), downloaded).ok
    with pytest.raises(ValueError):
        verify_outputs(identity, [], manifest, downloaded)
    with pytest.raises(ValueError):
        verify_outputs(identity, required * 2, manifest, downloaded)


def test_capability_unknown_is_not_universal_sdk_error():
    assert validate_capabilities(CapabilityReceipt(), required=()).issues == ("capability_evidence_missing",)
    with pytest.raises(ValueError):
        validate_capabilities(CapabilityReceipt(), required=("imaginary_cancel_api",))
