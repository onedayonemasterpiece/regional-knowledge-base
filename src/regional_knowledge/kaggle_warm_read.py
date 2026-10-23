"""Narrow in-process read handler activation for an already owned Kaggle runtime.

This is an internal, privileged runtime boundary, NOT a public MCP action,
remote shell or artifact transfer. A trusted maintenance owner must put an
immutable, source-reviewed module in an exclusive private directory before
stage(). No mutation executor, tokenizer, BGE model or PG pool is replaced.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from .kaggle_schema_migrate import latest_schema_version
import hashlib
from types import ModuleType
import inspect
import os
from pathlib import Path
import re
import stat
import sys


_HEX64 = re.compile(r"[0-9a-f]{64}\Z")
MAX_MODULE_BYTES = 512 * 1024
MAX_WARM_ACTIVATIONS = 8


class WarmHandlerRejected(RuntimeError):
    pass


@dataclass(frozen=True)
class ReadHandlerCompatibility:
    database_generation: str
    model_sha256: str
    dependency_sha256: str
    schema_version: int = field(default_factory=latest_schema_version)
    codec: str = "rkb.query.v2"

    def __post_init__(self):
        if any(type(getattr(self, field)) is not str or not getattr(self, field)
               for field in ("database_generation", "codec")):
            raise WarmHandlerRejected("read_handler_context_invalid")
        if (type(self.schema_version) is not int or self.schema_version<1
                or _HEX64.fullmatch(self.model_sha256) is None
                or _HEX64.fullmatch(self.dependency_sha256) is None
                or self.codec != "rkb.query.v2"):
            raise WarmHandlerRejected("read_handler_context_invalid")


def _async_handler(handler):
    return inspect.iscoroutinefunction(handler) or inspect.iscoroutinefunction(
        getattr(handler, "__call__", None))


@dataclass(frozen=True)
class _Candidate:
    handler: object
    sha256: str
    module_name: str


class ReadHandlerSlot:
    """One switchable callback around the SAME existing PG/model context.

    A stage must bind verified *actual source bytes* to the imported module;
    the caller supplies already pinned owned PG/tokenizer/telemetry references.
    Inflight readers finish under their captured handler before cutover.
    Only this internal callback is swapped; durable executors are untouched.
    """
    def __init__(self, handler, *, loaded_sha256: str, compatibility: ReadHandlerCompatibility):
        if not _async_handler(handler) or _HEX64.fullmatch(loaded_sha256) is None:
            raise WarmHandlerRejected("read_handler_initial_invalid")
        if type(compatibility) is not ReadHandlerCompatibility:
            raise WarmHandlerRejected("read_handler_context_invalid")
        self._handler = handler
        # Actual KaggleWorkerQuery has these already-owned references. A new
        # candidate may change its stateless read logic, never the pool,
        # tokenizer, telemetry or source-provenance requirement.
        names=('acquire','tokenizer','telemetry','source_provenance')
        self._pinned={key:getattr(handler,key) for key in names} if all(
            hasattr(handler,key) for key in names) else None
        self._hash = loaded_sha256
        self._active_module: str | None = None
        self._compatibility = compatibility
        self._condition = asyncio.Condition()
        self._change = asyncio.Lock()
        self._switching = False
        self._inflight = 0
        self._generation = 0
        self._stage: _Candidate | None = None
        self._previous: _Candidate | None = None
        self._activations = 0
        self._closed = False

    async def __call__(self, frame, *, encode, lease_current=None):
        async with self._condition:
            while self._switching:
                await self._condition.wait()
            if self._closed:
                raise WarmHandlerRejected("read_handler_closed")
            current = self._handler
            self._inflight += 1
        try:
            if lease_current is not None:
                # SQL_HEALTH owns the transport's live connection/resource
                # check. Keep that exact callback through the resident slot.
                return await current(frame, encode=encode, lease_current=lease_current)
            return await current(frame, encode=encode)
        finally:
            async with self._condition:
                self._inflight -= 1
                self._condition.notify_all()

    def receipt(self):
        return dict(loaded_sha256=self._hash,
                    loaded_kind='module' if self._active_module else 'verified_bundle',
                    generation=self._generation,
                    database_generation=self._compatibility.database_generation,
                    schema_version=self._compatibility.schema_version,
                    model_sha256=self._compatibility.model_sha256,
                    dependency_sha256=self._compatibility.dependency_sha256,
                    codec=self._compatibility.codec, inflight=self._inflight,
                    switching=self._switching,
                    staged_sha256=self._stage.sha256 if self._stage else None)

    async def stage(self, *, path: Path, trusted_root: Path, expected_sha256: str,
                    compatibility: ReadHandlerCompatibility, constructor: dict,
                    self_test):
        """Load exact owner-provided local bytes, without granting file access.

        The control/file-transfer owner, not this slot, authenticates the
        request and stages immutable bytes under a trusted private root.
        Candidate constructors accept only retained owned resource references.
        """
        if (type(compatibility) is not ReadHandlerCompatibility
                or compatibility != self._compatibility
                or _HEX64.fullmatch(expected_sha256) is None
                or not callable(self_test)
                or not inspect.iscoroutinefunction(self_test)
                or type(constructor) is not dict):
            raise WarmHandlerRejected("read_handler_incompatible")
        root = Path(trusted_root)
        path = Path(path)
        if (not root.is_absolute() or not path.is_absolute()
                or path.parent != root or path.suffix != '.py'
                or root.resolve(strict=True) != root or path.resolve(strict=True) != path):
            raise WarmHandlerRejected('read_handler_untrusted_path')
        # Read one immutable-by-identity descriptor; never execute a second
        # path read after verifying the hash. No symlink/subdirectory escape.
        root_fd=os.open(root,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
        try:
            root_stat=os.fstat(root_fd)
            if (not stat.S_ISDIR(root_stat.st_mode) or root_stat.st_uid!=os.getuid()
                    or root_stat.st_mode & 0o077):
                raise WarmHandlerRejected('read_handler_invalid_file')
            leaf_fd=os.open(path.name,os.O_RDONLY|os.O_NOFOLLOW,dir_fd=root_fd)
            try:
                leaf_stat=os.fstat(leaf_fd)
                if (not stat.S_ISREG(leaf_stat.st_mode) or leaf_stat.st_uid!=os.getuid()
                        or leaf_stat.st_mode & 0o077
                        or not 1 <= leaf_stat.st_size <= MAX_MODULE_BYTES):
                    raise WarmHandlerRejected('read_handler_invalid_file')
                chunks=[];remaining=leaf_stat.st_size
                while remaining:
                    chunk=os.read(leaf_fd,remaining)
                    if not chunk:break
                    chunks.append(chunk);remaining-=len(chunk)
                data=b''.join(chunks)
                if len(data)!=leaf_stat.st_size:
                    raise WarmHandlerRejected('read_handler_bytes_changed')
            finally:os.close(leaf_fd)
        finally:os.close(root_fd)
        if hashlib.sha256(data).hexdigest()!=expected_sha256:
            raise WarmHandlerRejected('read_handler_bytes_mismatch')
        code=compile(data,str(path),'exec')
        async with self._change:
            if self._closed or self._activations >= MAX_WARM_ACTIVATIONS:
                raise WarmHandlerRejected("read_handler_restart_required")
            if expected_sha256 == self._hash:
                return self.receipt()
            name = 'regional_knowledge._warm_' + expected_sha256
            module=ModuleType(name)
            module.__package__='regional_knowledge'
            module.__file__=str(path)
            try:
                sys.modules[name] = module
                # The bytes executed are EXACTLY those read and SHA-checked
                # from the descriptor above: no importlib second file read.
                exec(code,module.__dict__)
                factory = getattr(module, "KaggleWorkerQuery", None)
                if not callable(factory):
                    raise WarmHandlerRejected("read_handler_factory_invalid")
                candidate = factory(**constructor)
                if not _async_handler(candidate):
                    raise WarmHandlerRejected('read_handler_candidate_not_async')
                if self._pinned is not None and any(
                    (getattr(candidate,key,None)!=value if key=='source_provenance'
                     else getattr(candidate,key,None) is not value)
                    for key,value in self._pinned.items()
                ):
                    raise WarmHandlerRejected('read_handler_context_changed')
                await asyncio.wait_for(self_test(candidate), 3.0)
            except BaseException:
                sys.modules.pop(name, None)
                raise
            if self._stage is not None and self._stage.module_name != name:
                sys.modules.pop(self._stage.module_name,None)
            self._stage = _Candidate(candidate, expected_sha256, name)
            return self.receipt()

    async def _switch(self, candidate: _Candidate, *, expected_sha256: str,
                      probe, timeout_seconds: float):
        if (_HEX64.fullmatch(expected_sha256) is None or expected_sha256 != self._hash
                or type(timeout_seconds) not in (int, float)
                or not 0 < timeout_seconds <= 10
                or not inspect.iscoroutinefunction(probe)):
            raise WarmHandlerRejected("read_handler_stale_activation")
        if self._closed:
            raise WarmHandlerRejected("read_handler_closed")
        async with self._condition:
            self._switching = True
            self._condition.notify_all()
        old = _Candidate(self._handler, self._hash, self._active_module or '')
        try:
            async with asyncio.timeout(timeout_seconds):
                async with self._condition:
                    while self._inflight:
                        await self._condition.wait()
                self._handler, self._hash = candidate.handler, candidate.sha256
                self._active_module=candidate.module_name or None
                # A bounded usefulness probe is part of activation, before
                # opening new dispatches. It calls the selected handler
                # directly, not through this paused gate.
                await probe(candidate.handler)
            self._previous = old
            self._generation += 1
            self._activations += 1
            self._stage = None
        except BaseException:
            self._handler, self._hash = old.handler, old.sha256
            self._active_module=old.module_name or None
            raise
        finally:
            async with self._condition:
                self._switching = False
                self._condition.notify_all()
        return self.receipt()

    async def activate(self, *, staged_sha256: str, expected_loaded_sha256: str,
                       probe, timeout_seconds: float = 5.0):
        async with self._change:
            candidate = self._stage
            if candidate is None or candidate.sha256 != staged_sha256:
                raise WarmHandlerRejected("read_handler_no_verified_stage")
            return await self._switch(candidate, expected_sha256=expected_loaded_sha256,
                                      probe=probe, timeout_seconds=timeout_seconds)

    async def rollback(self, *, expected_loaded_sha256: str, probe,
                       timeout_seconds: float = 5.0):
        async with self._change:
            previous = self._previous
            if previous is None:
                raise WarmHandlerRejected("read_handler_no_rollback")
            result = await self._switch(previous, expected_sha256=expected_loaded_sha256,
                                        probe=probe, timeout_seconds=timeout_seconds)
            self._previous = None  # A rollback is not a second undo stack.
            return result

    def close(self):
        self._closed = True
