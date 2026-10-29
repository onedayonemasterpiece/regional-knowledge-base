"""Explicit resident preparation; no downloads, pool/PG creation or authority.

The owner supplies immutable local artifacts, the already owned initialized PG
lease and a native schema/readiness probe. Import performs no artifact/model load
and opens no PG/provider connection.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field, fields
import hashlib
import importlib.metadata
import inspect
import json
import os
from pathlib import Path
import platform
import re
import stat
import sys
import threading
from types import MappingProxyType

from .bge_contract import REVISION, SPACE, TOKENIZER_SHA256
from .kaggle_readiness import ArtifactRequirement, RUNTIME_HASHES
from .kaggle_query import PinnedTokenizer
from .kaggle_worker_query import KaggleWorkerQuery
from .kaggle_schema_migrate import latest_schema_version

REQUIRED_DEPENDENCIES = frozenset({'torch', 'transformers', 'tokenizers', 'huggingface-hub',
                                 'psycopg', 'psycopg-pool', 'aiohttp', 'pydantic'})


class BootstrapUnavailable(RuntimeError):
    """Fixed public reason only; never include artifact paths or private values."""


def _json(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False,
                      separators=(',', ':')).encode('utf-8')


def _sha(value):
    return hashlib.sha256(value).hexdigest()


def _text(value):
    return isinstance(value, str) and 0 < len(value.encode('utf-8')) <= 256


def _file_hash(path, size, *, dir_fd=None):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=dir_fd)
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode) or before.st_size != size:
            raise BootstrapUnavailable('artifact_size_mismatch')
        digest = hashlib.sha256()
        remaining = size
        while remaining:
            chunk = os.read(fd, min(1024 * 1024, remaining))
            if not chunk:
                raise BootstrapUnavailable('artifact_size_mismatch')
            digest.update(chunk)
            remaining -= len(chunk)
        after = os.fstat(fd)
        if os.read(fd, 1) or (before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
                after.st_size, after.st_mtime_ns, after.st_ctime_ns):
            raise BootstrapUnavailable('artifact_changed')
        return digest.hexdigest()
    finally:
        os.close(fd)


@dataclass(frozen=True)
class PreparedArtifact:
    requirement: ArtifactRequirement
    role: str

    def __post_init__(self):
        if type(self.requirement) is not ArtifactRequirement or self.role not in {
                'code', 'tokenizer', 'weight', 'model_config'}:
            raise ValueError('explicit prepared artifact role required')


@dataclass(frozen=True)
class VerifiedArtifacts:
    root: Path
    entries: tuple[PreparedArtifact, ...]
    observed: tuple[tuple[str, str, int, str], ...]

    def role_hash(self, roles):
        return _sha(_json([row for row in self.observed if row[0] in roles]))

    @property
    def model_directory(self):
        return _model_directory(self)


def verify_artifacts(root, entries, *, max_total_bytes):
    """Stream exact declared files only, without following any path symlink.

    Controller-owned content-addressed directories must remain immutable across
    verification and load; verification cannot fence arbitrary filesystem writers.
    """
    try:
        root = Path(root)
        entries = tuple(entries)
        if (not root.is_absolute() or root.resolve(strict=True) != root or not root.is_dir()
                or type(max_total_bytes) is not int or max_total_bytes < 1
                or not entries or any(type(e) is not PreparedArtifact for e in entries)
                or len({e.requirement.name for e in entries}) != len(entries)
                or sum(e.requirement.size for e in entries) > max_total_bytes):
            raise BootstrapUnavailable('artifact_inventory_invalid')
        roles = [e.role for e in entries]
        if roles.count('tokenizer') != 1 or not {'code', 'weight', 'model_config'} <= set(roles):
            raise BootstrapUnavailable('artifact_inventory_incomplete')
        observed = []
        root_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            for entry in entries:
                requirement = entry.requirement
                parent = os.dup(root_fd)
                try:
                    parts = requirement.name.split('/')
                    for part in parts[:-1]:
                        next_fd = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
                        os.close(parent)
                        parent = next_fd
                    digest = _file_hash(parts[-1], requirement.size, dir_fd=parent)
                finally:
                    os.close(parent)
                if digest != requirement.sha256:
                    raise BootstrapUnavailable('artifact_hash_mismatch')
                if entry.role == 'tokenizer' and (parts[-1] != 'tokenizer.json' or digest != TOKENIZER_SHA256):
                    raise BootstrapUnavailable('tokenizer_artifact_mismatch')
                observed.append((entry.role, requirement.name, requirement.size, digest))
        finally:
            os.close(root_fd)
        verified = VerifiedArtifacts(root, entries, tuple(sorted(observed)))
        _model_directory(verified)
        return verified
    except BootstrapUnavailable:
        raise
    except (OSError, ValueError, TypeError):
        raise BootstrapUnavailable('artifact_verification_failed') from None


def _model_directory(artifacts):
    tokenizer = next(e.requirement.name for e in artifacts.entries if e.role == 'tokenizer')
    directory = artifacts.root / Path(tokenizer).parent
    expected = {artifacts.root / e.requirement.name for e in artifacts.entries if e.role != 'code'}
    if any(not path.is_relative_to(directory) for path in expected) or directory / 'config.json' not in expected:
        raise BootstrapUnavailable('model_inventory_binding_invalid')
    observed, pending, visited = set(), [directory], 0
    while pending:
        current = pending.pop()
        visited += 1
        if visited > 128:
            raise BootstrapUnavailable('model_inventory_bound')
        with os.scandir(current) as children:
            for item in children:
                if item.is_symlink() or len(observed) + len(pending) > 128:
                    raise BootstrapUnavailable('model_inventory_binding_invalid')
                if item.is_dir(follow_symlinks=False):
                    pending.append(Path(item.path))
                elif item.is_file(follow_symlinks=False):
                    observed.add(Path(item.path))
                else:
                    raise BootstrapUnavailable('model_inventory_binding_invalid')
    if observed != expected:
        raise BootstrapUnavailable('model_inventory_binding_invalid')
    return directory


def _loaded_code_paths():
    from . import bge_contract, kaggle_protocol, kaggle_query, kaggle_search, kaggle_readiness, kaggle_worker_query, kaggle_database_probe
    return tuple(Path(module.__file__).resolve() for module in (
        sys.modules[__name__], bge_contract, kaggle_protocol, kaggle_query,
        kaggle_search, kaggle_readiness, kaggle_worker_query, kaggle_database_probe))


def _load_tokenizer_reference(artifacts, model_directory):
    """Materialize HF defaults from the independently verified local inventory.

    Old tokenizer JSON is not the backend's current serialization. Both the
    parser and HF configuration participate in the protected loaded behavior.
    This creates a tokenizer only, never a model or a network download.
    """
    protected = {'transformers': '5.16.1', 'tokenizers': '0.23.1', 'huggingface-hub': '1.29.0'}
    if any(importlib.metadata.version(name) != version for name, version in protected.items()):
        raise ValueError('tokenizer dependency mismatch')
    for entry in artifacts.entries:
        if entry.role in {'tokenizer', 'model_config'}:
            requirement = entry.requirement
            path = artifacts.root / requirement.name
            if (path.resolve(strict=True) != path or requirement.size > 64 * 1024 * 1024
                    or _file_hash(path, requirement.size) != requirement.sha256
                    or entry.role == 'tokenizer' and requirement.sha256 != TOKENIZER_SHA256):
                raise ValueError('tokenizer input mismatch')
    from transformers import AutoTokenizer
    return AutoTokenizer.from_pretrained(str(model_directory), use_fast=True,
        tokenizer_file=str(model_directory / 'tokenizer.json'), local_files_only=True,
        trust_remote_code=False)


def _verify_tokenizer_reference(actual, reference):
    """Compare complete materialized state and real single-input encodings."""
    if (type(actual) is not type(reference)
            or any(tokenizer.backend_tokenizer.truncation is not None for tokenizer in (actual, reference))):
        raise ValueError('tokenizer implementation mismatch')
    names = ('special_tokens_map', 'all_special_ids', 'all_special_tokens', 'model_input_names',
        'padding_side', 'truncation_side', 'model_max_length', 'clean_up_tokenization_spaces',
        'split_special_tokens')
    metadata = {name: getattr(reference, name, None) for name in names}
    if {name: getattr(actual, name, None) for name in names} != metadata:
        raise ValueError('tokenizer configuration mismatch')
    # Exercise the worker's batch1 input and the exact counter's special tokens.
    # Explicit per-call options normalize padding on BOTH handles; no backend
    # fields, normalizers, pre-tokenizers, decoders or added tokens are ignored.
    for text in ('readiness', 'Кёнигсберг — история\n1945.', 'Straße\t  Königsberg',
                 'e\u0301 é\u00a0中🙂', '<s> текст </s> <mask>', '  a\n\n b\t '):
        expected = reference([text], padding=True, truncation=False)
        observed = actual([text], padding=True, truncation=False)
        if dict(observed) != dict(expected):
            raise ValueError('tokenizer batch encoding mismatch')
        ids = reference.encode(text, add_special_tokens=True, truncation=False, padding=False)
        loaded_ids = actual.encode(text, add_special_tokens=True, truncation=False, padding=False)
        if loaded_ids != ids or expected['input_ids'] != [ids]:
            raise ValueError('tokenizer exact encoding mismatch')
    states = []
    for tokenizer in (actual, reference):
        rendered = tokenizer.backend_tokenizer.to_str()
        if not isinstance(rendered, str) or len(rendered.encode()) > 64 * 1024 * 1024:
            raise ValueError('tokenizer state bound')
        state = json.loads(rendered)
        if state.get('padding') is not None or state.get('truncation') is not None:
            raise ValueError('tokenizer count settings mismatch')
        states.append(state)
    if states[0] != states[1]:
        raise ValueError('tokenizer materialized state mismatch')
    return _sha(_json({'backend': states[1], 'configuration': metadata}))


@dataclass(frozen=True)
class ResidentEncoder:
    """Resident encoder and exact counter verified against its loaded tokenizer."""
    encode: object
    tokenizer: object
    artifacts: VerifiedArtifacts
    model_directory: Path
    model_load_count: int
    tokenizer_fingerprint: str = field(init=False)
    _tokenizer_lock: object = field(default_factory=threading.Lock, init=False, repr=False, compare=False)
    _counter_lock: object = field(default_factory=threading.Lock, init=False, repr=False, compare=False)
    _counter: object = field(init=False, repr=False, compare=False)

    def __post_init__(self):
        if type(self.artifacts) is not VerifiedArtifacts:
            raise BootstrapUnavailable('loaded_tokenizer_binding_invalid')
        model_directory = Path(self.model_directory)
        if (not callable(self.encode) or model_directory != _model_directory(self.artifacts)
                or Path(getattr(self.tokenizer, 'name_or_path', '')).resolve() != model_directory
                or getattr(self.tokenizer, 'is_fast', False) is not True
                or getattr(getattr(self.tokenizer, 'backend_tokenizer', None), 'truncation', True) is not None
                or not callable(getattr(self.tokenizer, 'encode', None))
                or type(self.model_load_count) is not int or self.model_load_count < 1):
            raise BootstrapUnavailable('loaded_tokenizer_binding_invalid')
        try:
            reference = _load_tokenizer_reference(self.artifacts, model_directory)
            fingerprint = _verify_tokenizer_reference(self.tokenizer, reference)
            object.__setattr__(self, 'tokenizer_fingerprint', fingerprint)
            object.__setattr__(self, '_counter', reference)
        except Exception:
            raise BootstrapUnavailable('loaded_tokenizer_settings_mismatch') from None

    def __call__(self, text):
        # Actual model forwards still serialize, including tokenizer mutation.
        with self._tokenizer_lock:
            return self.encode(text)

    async def count(self, text):
        def exact():
            with self._counter_lock:
                ids = self._counter.encode(text, add_special_tokens=True, truncation=False, padding=False)
                if not isinstance(ids, list) or any(type(token) is not int for token in ids):
                    raise BootstrapUnavailable('tokenizer_count_invalid')
                return len(ids)
        task = asyncio.create_task(asyncio.to_thread(exact))
        cancelled = False
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                cancelled = True
            except Exception:
                break
        if cancelled:
            if not task.cancelled():
                task.exception()
            raise asyncio.CancelledError
        return task.result()


@dataclass(frozen=True)
class CandidateReadiness:
    hashes: object
    database_generation: str
    applied_seq: int
    projection_seq: int | None
    visibility_epoch: int | None
    text_search_ready: bool
    dense_search_ready: bool
    archive_read_ready: bool | None = None
    proof_ready: bool | None = None
    schema_version: int = field(default_factory=latest_schema_version)

    def __post_init__(self):
        hashes = dict(self.hashes)
        if (type(self.schema_version) is not int or self.schema_version<1
                or set(hashes) != RUNTIME_HASHES or any(not isinstance(v, str) or re.fullmatch('[0-9a-f]{64}', v) is None for v in hashes.values())
                or any(not _text(getattr(self, k)) for k in ('database_generation',))
                or type(self.applied_seq) is not int or not 0 <= self.applied_seq <= 2**63-1
                or any(v is not None and (type(v) is not int or not 0 <= v <= 2**63-1) for v in (self.projection_seq, self.visibility_epoch))
                or self.projection_seq is not None and self.projection_seq > self.applied_seq
                or type(self.text_search_ready) is not bool or type(self.dense_search_ready) is not bool
                or any(v is not None and type(v) is not bool for v in (self.archive_read_ready, self.proof_ready))
                or (self.text_search_ready or self.dense_search_ready) and self.projection_seq != self.applied_seq):
            raise ValueError('bounded observed readiness required')
        object.__setattr__(self, 'hashes', MappingProxyType(hashes))

    def to_wire(self):
        result = {f.name: (dict(self.hashes) if f.name == 'hashes' else getattr(self, f.name)) for f in fields(self)}
        CandidateReadiness(**result)  # Revalidate even a forcibly modified instance.
        if len(_json(result)) > 16384:
            raise ValueError('readiness payload bound')
        return result


@dataclass
class PreparedRuntime:
    query_handler: object
    readiness: CandidateReadiness
    source_handler: object = None
    bootstrap_observation: object = None
    source_history_handler: object = None
    original_intake_handler: object = None
    read_event_drain: object = None

    def __post_init__(self):
        if self.read_event_drain is not None and (not callable(self.read_event_drain) or inspect.iscoroutinefunction(self.read_event_drain)):
            raise BootstrapUnavailable('read_event_drain_invalid')
        if (not (inspect.iscoroutinefunction(self.query_handler)
                 or inspect.iscoroutinefunction(getattr(self.query_handler, '__call__', None)))
                or type(self.readiness) is not CandidateReadiness
                or not self.readiness.text_search_ready or not self.readiness.dense_search_ready
                or self.readiness.visibility_epoch is None):
            raise BootstrapUnavailable('database_not_ready')
        if self.source_handler is not None and not (
                inspect.iscoroutinefunction(self.source_handler)
                or inspect.iscoroutinefunction(getattr(self.source_handler,'__call__',None))):
            raise BootstrapUnavailable('source_handler_invalid')
        if self.source_history_handler is not None and (not (
                inspect.iscoroutinefunction(self.source_history_handler)
                or inspect.iscoroutinefunction(getattr(self.source_history_handler,'__call__',None)))
                or not callable(getattr(self.source_history_handler,'attached',None))
                or not callable(getattr(self.source_history_handler,'detached',None))):
            raise BootstrapUnavailable('source_history_handler_invalid')
        if self.original_intake_handler is not None and (not (
                inspect.iscoroutinefunction(self.original_intake_handler)
                or inspect.iscoroutinefunction(getattr(self.original_intake_handler,'__call__',None)))
                or not callable(getattr(self.original_intake_handler,'attached',None))
                or not callable(getattr(self.original_intake_handler,'detached',None))):
            raise BootstrapUnavailable('original_intake_handler_invalid')


def observe_dependencies(packages):
    return {'python':sys.version,'machine':platform.machine(),'libc':list(platform.libc_ver()),
        'packages':{p:importlib.metadata.version(p) for p in packages}}


async def prepare_resident_runtime(encoder, *, acquire, probe_database, launch_config,
                                   executed_source, source_binding_verified, dependency_packages,
                                   telemetry=None):
    """Compose owned initialized resources; native PG/schema probe is explicit.

    probe_database(acquire=...) returns (catalog_bytes, readiness_fields) from
    actual bounded native checks; it must not initialize/migrate an unknown DB.
    Source path equivalence to provider-executed bytes is a Stage0 attestation.
    """
    if type(encoder) is not ResidentEncoder or source_binding_verified is not True:
        raise BootstrapUnavailable('resident_source_binding_unverified')
    try:
        def observe():
            verified = verify_artifacts(encoder.artifacts.root, encoder.artifacts.entries,
                max_total_bytes=max(1, sum(e.requirement.size for e in encoder.artifacts.entries)))
            code_paths = {verified.root / e.requirement.name for e in verified.entries if e.role == 'code'}
            if not set(_loaded_code_paths()) <= code_paths:
                raise BootstrapUnavailable('loaded_code_binding_invalid')
            source = Path(executed_source)
            if not source.is_absolute() or source.resolve(strict=True) != source or source.stat().st_size > 8 * 1024 * 1024:
                raise BootstrapUnavailable('executed_source_unverified')
            packages = tuple(dependency_packages)
            if (not 1 <= len(packages) <= 64 or len(set(packages)) != len(packages)
                    or any(not _text(p) for p in packages) or not REQUIRED_DEPENDENCIES <= set(packages)):
                raise BootstrapUnavailable('dependency_inventory_invalid')
            dependencies = observe_dependencies(packages)
            config_bytes = _json(launch_config)
            return {'code': verified.role_hash({'code'}), 'launch_config': _sha(config_bytes),
                'executed_source': _file_hash(source, source.stat().st_size), 'dependency': _sha(_json(dependencies)),
                'model': _sha(_json({'revision': REVISION, 'space': SPACE,
                    'artifacts': verified.role_hash({'weight', 'model_config', 'tokenizer'})}))}
        hashes = await asyncio.to_thread(observe)
        _, fields = await probe_database(acquire=acquire)
        # Compatibility hash slot until PR-3 retires the revision protocol.
        # Derived entirely from the observed integer, never from PG catalogs.
        hashes['schema'] = _sha(_json(fields.get('schema_version',latest_schema_version())))
        readiness = CandidateReadiness(hashes=hashes, **fields)
        tokenizer = PinnedTokenizer(encoder.count, REVISION, TOKENIZER_SHA256, SPACE)
        return PreparedRuntime(KaggleWorkerQuery(acquire=acquire, tokenizer=tokenizer, telemetry=telemetry), readiness)
    except asyncio.CancelledError:
        raise
    except BootstrapUnavailable:
        raise
    except Exception:
        raise BootstrapUnavailable('runtime_preparation_failed') from None
