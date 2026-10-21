"""Narrow authenticated warm read-handler maintenance on the existing recovery WSS.

This is NOT a public code-upload RPC: the existing controller must select a
source-reviewed commit and immutable object, issue a scoped GET ticket, and
hold the run-authenticated recovery compute permit. WSS carries only pins and
the existing signed ticket; no Python source, local path, constructor, SQL or
model/PG handle can be chosen by a user request.
"""
from __future__ import annotations

import inspect
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
    This type only prevents an arbitrary path/body from crossing the WSS wire.
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


class WarmHandlerMaintenance:
    """Retained same-worker operation; downloads through the existing S3 ticket.

    Candidate construction and all resource/probe callbacks stay in the
    already running model-owning TargetRuntime. Only this stateless callback
    can change. A lost ACK leaves original operation UNKNOWN until an
    independent status readback proves the exact loaded SHA/generation.
    """
    def __init__(self,*,slot:ReadHandlerSlot,download,workspace,constructor,self_test,probe,current):
        if (type(slot) is not ReadHandlerSlot or not callable(download)
                or not callable(workspace) or not callable(current) or type(constructor) is not dict
                or not inspect.iscoroutinefunction(self_test)
                or not inspect.iscoroutinefunction(probe)):
            raise TypeError("trusted retained warm handler maintenance owner required")
        self.slot,self.download,self.workspace=slot,download,workspace
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
            if set(args)!={'artifact','expected_loaded_sha256','expected_generation'}:
                raise WarmMaintenanceRejected("read_handler_maintenance_arguments")
            self._current(args['expected_loaded_sha256'],args['expected_generation'])
            artifact=WarmHandlerPatchApproval.from_wire(args['artifact'])
            if artifact.blob.size>MAX_MODULE_BYTES or not 1<=artifact.blob.size:
                raise WarmMaintenanceRejected("read_handler_artifact_budget")
            ticket=frame.get('download_ticket')
            if type(ticket) is not dict:
                raise WarmMaintenanceRejected("read_handler_transfer_required")
            # download() is the retained NativeSnapshotTransfer adapter. It
            # independently checks operation-key, HTTPS origin, conditional
            # ETag, exact size/SHA and writes an owned O_EXCL private file.
            source=self.download(artifact.blob,operation_key,
                ticket=ticket,transfer_operation=operation_key)
            if inspect.isawaitable(source):source=await source
            source=Path(source)
            root=Path(self.workspace(operation_key,'read_handler'))
            if (not root.is_absolute() or not source.is_absolute()
                    or source.parent!=root.parent or root.exists()
                    or root==root.parent or root.name in ('.','..')):
                raise WarmMaintenanceRejected("read_handler_private_workspace")
            try:
                self.current(frame)  # Revalidate AFTER potentially slow transfer.
                self._current(args['expected_loaded_sha256'],args['expected_generation'])
            except BaseException:
                # The verified download target belongs to this exact private
                # operation. A stale attachment must not retain disposable
                # candidate bytes or execute any of them.
                source.unlink(missing_ok=True)
                raise
            root.mkdir(mode=0o700)
            target=root/'handler.py'
            try:
                # Atomic rename inside the same private parent; stage() opens
                # the SAME new file via O_NOFOLLOW and SHA-verifies its bytes.
                os.rename(source,target)
                async def current_self_test(candidate):
                    self.current(frame)
                    await self.self_test(candidate)
                    self.current(frame)
                return await self.slot.stage(path=target,trusted_root=root,
                    expected_sha256=artifact.blob.sha256,
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
