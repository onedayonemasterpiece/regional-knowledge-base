"""Pure validation helpers for the existing Kaggle controller; no provider I/O.

Capability flags are attestations supplied by an adapter's Stage 0 probe, not
capabilities inferred from a method name or an installed dependency.
"""
from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
import math
from pathlib import PurePosixPath
import re
from typing import Mapping, Sequence
from types import MappingProxyType
import json

_HASH = re.compile(r"[0-9a-f]{64}\Z")
RUNTIME_HASHES = frozenset({"code", "launch_config", "executed_source", "dependency", "model", "schema"})
# Full first-book metadata (33 handlers/55 wheels) measures about74.5KiB.
# Controller assembly and resident observation must accept the same bytes.
# The private target-file, WSS and source budgets have separate bounds.
MAX_LAUNCH_CONFIG_BYTES = 96 * 1024
TERMINAL_STATUSES = frozenset({"complete", "failed", "cancelled"})


@dataclass(frozen=True)
class Validation:
    issues: tuple[str, ...] = ()

    @property
    def ok(self) -> bool:
        return not self.issues


@dataclass(frozen=True)
class RunIdentity:
    owner_slug: str
    provider_version: str
    provider_execution_id: str | None
    run_id: str
    launch_nonce: str
    runtime_instance_id: str

    def __post_init__(self) -> None:
        if any(not isinstance(v, str) or not v.strip() for v in (
            self.owner_slug, self.provider_version, self.run_id,
            self.launch_nonce, self.runtime_instance_id,
        )):
            raise ValueError("incomplete run identity")
        if self.provider_execution_id is not None and not self.provider_execution_id.strip():
            raise ValueError("empty provider execution identity")


@dataclass(frozen=True)
class CapabilityReceipt:
    package: str = ""
    installed_version: str = ""
    adapter: str = ""
    evidence_ref: str = ""
    exact_status: bool = False
    # Proven on selected version: status identifies one execution, even if the
    # provider exposes only a version label rather than an execution ID.
    status_identifies_execution: bool = False
    version_qualified_outputs: bool = False
    complete_output_pagination: bool = False
    scoped_stop: bool = False
    writable_paths_verified: bool = False

    @property
    def attested(self) -> bool:
        return all((self.package, self.installed_version, self.adapter, self.evidence_ref))


def validate_capabilities(receipt: CapabilityReceipt, *, required: Sequence[str]) -> Validation:
    allowed = {"exact_status", "status_identifies_execution", "version_qualified_outputs",
               "complete_output_pagination", "scoped_stop", "writable_paths_verified"}
    if set(required) - allowed:
        raise ValueError("unknown capability requirement")
    issues = [] if receipt.attested else ["capability_evidence_missing"]
    issues.extend(f"capability_unverified:{name}" for name in required if not getattr(receipt, name))
    return Validation(tuple(issues))


@dataclass(frozen=True)
class RuntimeObservation:
    identity: RunIdentity
    hashes: Mapping[str, str]
    authenticated: bool
    phase: str
    database_ready: bool = False


def validate_runtime(
    expected: RunIdentity, expected_hashes: Mapping[str, str], observed: RuntimeObservation,
    *, competing_incarnations: Sequence[str] = (),
) -> Validation:
    if set(expected_hashes) != RUNTIME_HASHES or any(not _HASH.fullmatch(v) for v in expected_hashes.values()):
        raise ValueError("expected runtime hashes must contain all six SHA256 values")
    issues = []
    if expected != observed.identity:
        issues.append("runtime_identity_mismatch")
    if any(instance != expected.runtime_instance_id for instance in competing_incarnations):
        issues.append("competing_runtime_incarnation")
    if not observed.authenticated:
        issues.append("runtime_unauthenticated")
    if observed.phase != "ready" or not observed.database_ready:
        issues.append("runtime_not_ready")
    if dict(expected_hashes) != dict(observed.hashes):
        issues.append("loaded_hashes_mismatch")
    return Validation(tuple(issues))


@dataclass(frozen=True)
class SearchRuntimeExpectation:
    """Small controller-owned run binding, never corpus or caller authority."""
    hashes: Mapping[str, str]
    database_generation: str
    schema_revision: str
    function_revision: str
    executor_revision: str
    min_data_seq: int = 0

    def __post_init__(self):
        if (not isinstance(self.hashes, Mapping) or set(self.hashes) != RUNTIME_HASHES
                or any(not isinstance(v, str) or not _HASH.fullmatch(v) for v in self.hashes.values())):
            raise ValueError('all six exact expected runtime hashes required')
        for value in (self.database_generation, self.schema_revision,
                      self.function_revision, self.executor_revision):
            if not isinstance(value, str) or not 1 <= len(value.encode('utf-8')) <= 256:
                raise ValueError('bounded expected runtime version required')
        if type(self.min_data_seq) is not int or not 0 <= self.min_data_seq < 2**63:
            raise ValueError('bounded expected data cursor required')
        object.__setattr__(self, 'hashes', MappingProxyType(dict(self.hashes)))

    def wire(self):
        return {'hashes': dict(self.hashes), 'database_generation': self.database_generation,
                'schema_revision': self.schema_revision, 'function_revision': self.function_revision,
                'executor_revision': self.executor_revision, 'min_data_seq': self.min_data_seq}


def validate_search_runtime(expected: SearchRuntimeExpectation, observed: Mapping,
                            identity: RunIdentity) -> Validation:
    """Validate current authenticated READY facts against persisted expectations.

    The channel supplies identity from its current run/nonce/incarnation/native
    version, not from a readiness payload. Actual artifact/probe observation is
    the worker factory's responsibility; matching strings alone do not prove it.
    """
    fields = {'hashes', 'database_generation', 'schema_revision', 'function_revision',
              'executor_revision', 'applied_seq', 'projection_seq', 'visibility_epoch',
              'text_search_ready', 'dense_search_ready', 'archive_read_ready', 'proof_ready'}
    if (type(expected) is not SearchRuntimeExpectation or not isinstance(observed, Mapping)
            or set(observed) != fields):
        raise ValueError('exact target readiness shape required')
    if len(json.dumps(dict(observed), allow_nan=False).encode('utf-8')) > 16384:
        raise ValueError('bounded target readiness required')
    actual_hashes = observed['hashes']
    if (not isinstance(actual_hashes, dict) or set(actual_hashes) != RUNTIME_HASHES
            or any(not isinstance(v, str) or not _HASH.fullmatch(v) for v in actual_hashes.values())):
        raise ValueError('six observed runtime hashes required')
    for key in ('database_generation', 'schema_revision', 'function_revision', 'executor_revision'):
        value = observed[key]
        if not isinstance(value, str) or not 1 <= len(value.encode('utf-8')) <= 256:
            raise ValueError('bounded observed runtime version required')
    for key in ('applied_seq', 'projection_seq', 'visibility_epoch'):
        value = observed[key]
        if value is None and key != 'applied_seq':
            continue
        if type(value) is not int or not 0 <= value < 2**63:
            raise ValueError('strict observed runtime cursor required')
    for key in ('text_search_ready', 'dense_search_ready'):
        if type(observed[key]) is not bool:
            raise ValueError('explicit search readiness required')
    for key in ('archive_read_ready', 'proof_ready'):
        if observed[key] is not None and type(observed[key]) is not bool:
            raise ValueError('explicit independent artifact readiness required')
    database_ready = (observed['text_search_ready'] and observed['dense_search_ready']
        and observed['visibility_epoch'] is not None
        and observed['projection_seq'] == observed['applied_seq']
        and observed['applied_seq'] >= expected.min_data_seq)
    result = validate_runtime(identity, expected.hashes,
        RuntimeObservation(identity, actual_hashes, True, 'ready', database_ready))
    issues = list(result.issues)
    if observed['database_generation'] != expected.database_generation:
        issues.append('database_generation_mismatch')
    if any(observed[key] != getattr(expected, key) for key in
           ('schema_revision', 'function_revision', 'executor_revision')):
        issues.append('semantic_revision_mismatch')
    return Validation(tuple(issues))


@dataclass(frozen=True)
class ArtifactRequirement:
    name: str
    size: int
    sha256: str

    def __post_init__(self) -> None:
        path = PurePosixPath(self.name)
        if (not self.name or "\\" in self.name or path.is_absolute()
                or any(part in {"", ".", ".."} for part in self.name.split("/"))
                or not isinstance(self.size, int) or isinstance(self.size, bool) or self.size < 0
                or not _HASH.fullmatch(self.sha256)):
            raise ValueError("invalid artifact requirement")


@dataclass(frozen=True)
class ArtifactManifest:
    identity: RunIdentity
    members: tuple[ArtifactRequirement, ...]


@dataclass(frozen=True)
class DownloadedArtifact:
    identity: RunIdentity
    content: bytes
    native_readback_verified: bool = False


def verify_outputs(
    expected: RunIdentity, required: Sequence[ArtifactRequirement],
    manifest: ArtifactManifest | None, downloaded: Mapping[str, DownloadedArtifact],
    *, max_artifacts: int = 128, max_total_bytes: int = 16 * 1024 * 1024,
) -> Validation:
    """Verify small Stage 0 outputs, retaining independence from provider COMPLETE.

    The caller bounds transport before constructing byte objects. These limits
    additionally bound hashing and inventory work; this function reads no paths.
    """
    if max_artifacts <= 0 or max_total_bytes < 0:
        raise ValueError("invalid output budget")
    if not required:
        raise ValueError("controller must declare required outputs")
    if len(required) > max_artifacts or len(downloaded) > max_artifacts or (
        manifest is not None and len(manifest.members) > max_artifacts
    ):
        return Validation(("artifact_count_budget_exceeded",))
    if sum(item.size for item in required) > max_total_bytes or sum(
        len(item.content) for item in downloaded.values()
    ) > max_total_bytes:
        return Validation(("artifact_bytes_budget_exceeded",))
    if len({item.name for item in required}) != len(required):
        raise ValueError("duplicate controller artifact requirement")
    if manifest is None:
        return Validation(("manifest_missing",))
    issues = []
    if manifest.identity != expected:
        issues.append("manifest_identity_mismatch")
    members = {item.name: item for item in manifest.members}
    if len(members) != len(manifest.members):
        issues.append("duplicate_manifest_member")
    # Error codes deliberately contain no artifact names or payloads.
    for requirement in required:
        if members.get(requirement.name) != requirement:
            issues.append("required_manifest_member_missing_or_mismatched")
        actual = downloaded.get(requirement.name)
        if actual is None:
            issues.append("required_artifact_missing")
            continue
        if actual.identity != expected or not actual.native_readback_verified:
            issues.append("artifact_readback_unverified")
        if len(actual.content) != requirement.size or sha256(actual.content).hexdigest() != requirement.sha256:
            issues.append("artifact_bytes_mismatch")
    return Validation(tuple(dict.fromkeys(issues)))


@dataclass(frozen=True)
class TerminalObservation:
    identity: RunIdentity
    status: str
    observed_at: float
    source: str = "provider_status"
    # Adapter must bind an execution even when a version can run repeatedly.
    execution_binding_verified: bool = False


def validate_terminal(
    expected: RunIdentity, capabilities: CapabilityReceipt, observed: TerminalObservation | None,
    *, last_heartbeat_at: float, now: float, max_age_seconds: float,
    contradictory_runtime_evidence: bool = False,
) -> Validation:
    """A failed validation means termination_unknown and capacity stays owned.

    Stop ACK, deletion, TTL, child exit and credential revocation are intentionally
    not terminal observations. Telegram use release is a separate controller gate.
    """
    if not all(math.isfinite(value) for value in (last_heartbeat_at, now, max_age_seconds)) or max_age_seconds <= 0:
        raise ValueError("invalid terminal observation time budget")
    issues = list(validate_capabilities(
        capabilities, required=("exact_status", "status_identifies_execution"),
    ).issues)
    if observed is None:
        issues.append("termination_unknown")
        return Validation(tuple(issues))
    if observed.identity != expected:
        issues.append("terminal_identity_mismatch")
    if observed.source != "provider_status" or not observed.execution_binding_verified:
        issues.append("terminal_execution_unverified")
    if observed.status not in TERMINAL_STATUSES:
        issues.append("provider_not_terminal")
    if (not math.isfinite(observed.observed_at) or observed.observed_at <= last_heartbeat_at
            or observed.observed_at > now or now - observed.observed_at > max_age_seconds):
        issues.append("terminal_observation_not_fresh")
    if contradictory_runtime_evidence:
        issues.append("terminal_runtime_conflict")
    return Validation(tuple(issues))
