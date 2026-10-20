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
                or not 1 <= len(entries) <= 128 or any(type(e) is not PreparedArtifact for e in entries)
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
    from . import bge_contract, kaggle_protocol, kaggle_query, kaggle_search, kaggle_readiness, kaggle_worker_query
    return tuple(Path(module.__file__).resolve() for module in (
        sys.modules[__name__], bge_contract, kaggle_protocol, kaggle_query,
        kaggle_search, kaggle_readiness, kaggle_worker_query))


@dataclass(frozen=True)
class ResidentEncoder:
    """Callable existing encoder plus its actual locally loaded tokenizer handle."""
    encode: object
    tokenizer: object
    artifacts: VerifiedArtifacts
    model_directory: Path
    model_load_count: int
    _tokenizer_lock: object = field(default_factory=threading.Lock, init=False, repr=False, compare=False)

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
            entry = next(e.requirement for e in self.artifacts.entries if e.role == 'tokenizer')
            if entry.size > 64 * 1024 * 1024:
                raise ValueError
            rendered = self.tokenizer.backend_tokenizer.to_str()
            if not isinstance(rendered, str) or len(rendered.encode()) > 64 * 1024 * 1024:
                raise ValueError
            actual = json.loads(rendered)
            pinned = json.loads((self.artifacts.root / entry.name).read_bytes())
            # Padding is a per-call batch setting, explicitly disabled by count.
            actual.pop('padding', None); pinned.pop('padding', None)
            if actual != pinned:
                raise ValueError
        except Exception:
            raise BootstrapUnavailable('loaded_tokenizer_settings_mismatch') from None

    def __call__(self, text):
        # Fast tokenizer calls can change backend padding/truncation settings.
        # Count and actual inference use the same resident handle serially.
        with self._tokenizer_lock:
            return self.encode(text)

    async def count(self, text):
        def exact():
            with self._tokenizer_lock:
                ids = self.tokenizer.encode(text, add_special_tokens=True, truncation=False, padding=False)
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
    schema_revision: str
    function_revision: str
    executor_revision: str
    applied_seq: int
    projection_seq: int | None
    visibility_epoch: int | None
    text_search_ready: bool
    dense_search_ready: bool
    archive_read_ready: bool | None = None
    proof_ready: bool | None = None

    def __post_init__(self):
        hashes = dict(self.hashes)
        if (set(hashes) != RUNTIME_HASHES or any(not isinstance(v, str) or re.fullmatch('[0-9a-f]{64}', v) is None for v in hashes.values())
                or any(not _text(getattr(self, k)) for k in ('database_generation', 'schema_revision', 'function_revision', 'executor_revision'))
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


@dataclass(frozen=True)
class PreparedRuntime:
    query_handler: object
    readiness: CandidateReadiness

    def __post_init__(self):
        if (not (inspect.iscoroutinefunction(self.query_handler)
                 or inspect.iscoroutinefunction(getattr(self.query_handler, '__call__', None)))
                or type(self.readiness) is not CandidateReadiness
                or not self.readiness.text_search_ready or not self.readiness.dense_search_ready
                or self.readiness.visibility_epoch is None):
            raise BootstrapUnavailable('database_not_ready')


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
            dependencies = {'python': sys.version, 'machine': platform.machine(), 'libc': platform.libc_ver(),
                'packages': {p: importlib.metadata.version(p) for p in packages}}
            config_bytes = _json(launch_config)
            if len(config_bytes) > 65536:
                raise BootstrapUnavailable('launch_config_bound')
            return {'code': verified.role_hash({'code'}), 'launch_config': _sha(config_bytes),
                'executed_source': _file_hash(source, source.stat().st_size), 'dependency': _sha(_json(dependencies)),
                'model': _sha(_json({'revision': REVISION, 'space': SPACE,
                    'artifacts': verified.role_hash({'weight', 'model_config', 'tokenizer'})}))}
        hashes = await asyncio.to_thread(observe)
        catalog_bytes, fields = await probe_database(acquire=acquire)
        if not isinstance(catalog_bytes, bytes) or not 1 <= len(catalog_bytes) <= 1024 * 1024:
            raise BootstrapUnavailable('catalog_observation_invalid')
        hashes['schema'] = _sha(catalog_bytes)
        readiness = CandidateReadiness(hashes=hashes, **fields)
        tokenizer = PinnedTokenizer(encoder.count, REVISION, TOKENIZER_SHA256, SPACE)
        return PreparedRuntime(KaggleWorkerQuery(acquire=acquire, tokenizer=tokenizer, telemetry=telemetry), readiness)
    except asyncio.CancelledError:
        raise
    except BootstrapUnavailable:
        raise
    except Exception:
        raise BootstrapUnavailable('runtime_preparation_failed') from None
