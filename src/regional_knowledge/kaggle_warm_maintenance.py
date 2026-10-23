"""Trusted controller stages bounded stateless Python over the existing WSS.

Run identity, reviewed source commit, exact SHA and slot generation remain
required. This maintenance operation is not exposed by the product MCP.
"""
from __future__ import annotations

import inspect
import hashlib
from dataclasses import dataclass
import os
from pathlib import Path
import re

from .kaggle_pg_snapshot_native import BlobRef
from .kaggle_warm_read import ReadHandlerSlot, ReadHandlerCompatibility, WarmHandlerRejected, MAX_MODULE_BYTES


_SHA64 = re.compile(r"[0-9a-f]{64}\Z")
_COMMIT40 = re.compile(r"[0-9a-f]{40}\Z")


class WarmMaintenanceRejected(RuntimeError):
    """Fixed non-secret refusal; neither source nor signed URL is included."""


@dataclass(frozen=True)
class WarmHandlerPatchApproval:
    """Trusted controller-issued pin; not a proof of upstream code review.

    The controller/deployer independently proves the exact reviewed commit.
    Used by the retained schema-bundle compatibility path; read-handler
    staging now carries reviewed source inline.
    """
    blob: BlobRef
    reviewed_commit: str

    def __post_init__(self):
        if (type(self.blob) is not BlobRef or type(self.reviewed_commit) is not str
                or _COMMIT40.fullmatch(self.reviewed_commit) is None):
            raise WarmMaintenanceRejected("read_handler_approval_invalid")

    def wire(self):
        return dict(blob=dict(key=self.blob.key,size=self.blob.size,
            sha256=self.blob.sha256,native_version=self.blob.native_version),
            reviewed_commit=self.reviewed_commit)

    @classmethod
    def from_wire(cls,value):
        if (type(value) is not dict or set(value)!={'blob','reviewed_commit'}
                or type(value['blob']) is not dict
                or set(value['blob'])!={'key','size','sha256','native_version'}):
            raise WarmMaintenanceRejected("read_handler_approval_invalid")
        try:
            return cls(blob=BlobRef(**value['blob']),reviewed_commit=value['reviewed_commit'])
        except (TypeError,ValueError):
            raise WarmMaintenanceRejected("read_handler_approval_invalid") from None


def inline_handler_bytes(source,digest,reviewed_commit):
    """Validate the trusted maintenance input before retaining or executing it."""
    if (type(source) is not str or type(digest) is not str
            or _SHA64.fullmatch(digest) is None or type(reviewed_commit) is not str
            or _COMMIT40.fullmatch(reviewed_commit) is None):
        raise WarmMaintenanceRejected("read_handler_approval_invalid")
    try:
        content=source.encode('utf-8')
    except UnicodeError:
        raise WarmMaintenanceRejected("read_handler_approval_invalid") from None
    if not 1<=len(content)<=MAX_MODULE_BYTES:
        raise WarmMaintenanceRejected("read_handler_artifact_budget")
    if hashlib.sha256(content).hexdigest()!=digest:
        raise WarmMaintenanceRejected("read_handler_bytes_mismatch")
    return content


class WarmHandlerMaintenance:
    """Retained same-worker operation; stages inline reviewed handler bytes.

    Candidate construction and all resource/probe callbacks stay in the
    already running model-owning TargetRuntime. Only this stateless callback
    can change. A lost ACK leaves original operation UNKNOWN until an
    independent status readback proves the exact loaded SHA/generation.
    """
    def __init__(self,*,slot:ReadHandlerSlot,workspace,constructor,self_test,probe,current):
        if (type(slot) is not ReadHandlerSlot or not callable(workspace) or not callable(current) or type(constructor) is not dict
                or not inspect.iscoroutinefunction(self_test)
                or not inspect.iscoroutinefunction(probe)):
            raise TypeError("trusted retained warm handler maintenance owner required")
        self.slot,self.workspace=slot,workspace
        self.constructor,self.self_test,self.probe=constructor,self_test,probe
        self.current=current  # retained exact WSS run/incarnation/generation gate

    def _current(self,expected_sha,expected_generation):
        receipt=self.slot.receipt()
        if (type(expected_sha) is not str or _SHA64.fullmatch(expected_sha) is None
                or type(expected_generation) is not int or expected_generation<0
                or receipt['loaded_sha256']!=expected_sha
                or receipt['generation']!=expected_generation
                or receipt['switching']):
            raise WarmMaintenanceRejected("read_handler_stale_operation")
        return receipt

    async def __call__(self,phase,args,operation_key,frame):
        if (type(args) is not dict or type(operation_key) is not str
                or not 1<=len(operation_key.encode())<=256 or type(frame) is not dict):
            raise WarmMaintenanceRejected("read_handler_maintenance_arguments")
        self.current(frame)
        if phase=='read_handler_stage':
            if set(args)!={'handler_source','handler_sha256','reviewed_commit',
                    'expected_loaded_sha256','expected_generation'}:
                raise WarmMaintenanceRejected("read_handler_maintenance_arguments")
            self._current(args['expected_loaded_sha256'],args['expected_generation'])
            content=inline_handler_bytes(args['handler_source'],args['handler_sha256'],args['reviewed_commit'])
            if 'download_ticket' in frame:
                raise WarmMaintenanceRejected("read_handler_maintenance_arguments")
            root=Path(self.workspace(operation_key,'read_handler'))
            if (not root.is_absolute() or root.exists() or root==root.parent
                    or root.name in ('.','..')):
                raise WarmMaintenanceRejected("read_handler_private_workspace")
            self.current(frame)
            root.mkdir(mode=0o700)
            target=root/'handler.py'
            try:
                descriptor=os.open(target,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
                with os.fdopen(descriptor,'wb') as output:
                    output.write(content)
                self.current(frame)
                self._current(args['expected_loaded_sha256'],args['expected_generation'])
                async def current_self_test(candidate):
                    self.current(frame)
                    await self.self_test(candidate)
                    self.current(frame)
                return await self.slot.stage(path=target,trusted_root=root,
                    expected_sha256=args['handler_sha256'],
                    compatibility=self.slot._compatibility,
                    constructor=self.constructor,self_test=current_self_test)
            except BaseException:
                target.unlink(missing_ok=True)
                root.rmdir()
                raise
        if phase=='read_handler_activate':
            if set(args)!={'staged_sha256','expected_loaded_sha256','expected_generation'}:
                raise WarmMaintenanceRejected("read_handler_maintenance_arguments")
            self._current(args['expected_loaded_sha256'],args['expected_generation'])
            if type(args['staged_sha256']) is not str or _SHA64.fullmatch(args['staged_sha256']) is None:
                raise WarmMaintenanceRejected("read_handler_artifact_budget")
            async def current_probe(candidate):
                self.current(frame)
                await self.probe(candidate)
                self.current(frame)
            try:
                return await self.slot.activate(staged_sha256=args['staged_sha256'],
                    expected_loaded_sha256=args['expected_loaded_sha256'],probe=current_probe)
            except WarmHandlerRejected as exc:
                raise WarmMaintenanceRejected(str(exc)) from None
        if phase=='read_handler_rollback':
            if set(args)!={'expected_loaded_sha256','expected_generation'}:
                raise WarmMaintenanceRejected("read_handler_maintenance_arguments")
            self._current(args['expected_loaded_sha256'],args['expected_generation'])
            async def current_probe(candidate):
                self.current(frame)
                await self.probe(candidate)
                self.current(frame)
            try:
                return await self.slot.rollback(expected_loaded_sha256=args['expected_loaded_sha256'],
                    probe=current_probe)
            except WarmHandlerRejected as exc:
                raise WarmMaintenanceRejected(str(exc)) from None
        raise WarmMaintenanceRejected("read_handler_maintenance_phase")
