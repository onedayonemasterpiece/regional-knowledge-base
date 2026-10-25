"""Owned backup/transfer tasks behind short existing recovery jobs, never BGE work."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
import logging

from .kaggle_pg_snapshot_native import BlobRef, SnapshotError, sha
from .native_snapshot_transfer import SnapshotTicket

LOG=logging.getLogger(__name__)

# Only exact SnapshotError literals emitted by kaggle_pg_snapshot_native may be
# classified. Never log exception text, arguments or substrings of a message.
_SNAPSHOT_FAILURE_CODES={
    'snapshot_exact_runtime_versions_required':'versions',
    'snapshot_native_version_mismatch':'versions',
    'snapshot_resident_model_tokenizer_changed':'versions',
    'native_snapshot_binary_version':'versions',
    'native_backup_current_prefix':'prefix',
    'snapshot_applied_prefix_gap':'prefix',
    'snapshot_applied_cursor_differs':'prefix',
    'snapshot_disk_budget':'disk',
    'snapshot_archive_byte_budget':'archive',
    'snapshot_required_native_members_missing':'archive',
    'snapshot_native_inventory_differs':'archive',
    'snapshot_tar_header_budget':'archive',
    'snapshot_tar_metadata_budget':'archive',
    'snapshot_unsafe_archive_member':'archive',
    'snapshot_native_command_failed':'subprocess',
    'snapshot_native_output_bound':'subprocess',
}


@dataclass
class _Operation:
    binding: tuple
    attached: asyncio.Event = field(default_factory=asyncio.Event)
    task: object = None
    upload: object = None
    upload_result: object = None
    upload_task: object = None
    failure_logged: bool = False


class SnapshotIo:
    def __init__(self,*,factory,tickets,transfer):
        self.factory,self.tickets,self.transfer=factory,tickets,transfer
        self.operations={};self.closed=False
        self._slots=asyncio.Semaphore(2)

    def rebind(self,binding):
        self.factory._current(binding)
        for operation in self.operations.values():
            if operation.binding[:2]!=binding[:2]:raise SnapshotError('snapshot_io_identity_changed')
            operation.binding=binding;operation.attached.set()

    async def _current(self,operation):
        while not self.closed:
            operation.attached.clear()
            try:self.factory._current(operation.binding);return
            except SnapshotError:await operation.attached.wait()
        raise SnapshotError('snapshot_io_closed')

    def start(self,key,binding,action,*,wait_for_capacity=False):
        self.factory._current(binding)
        handle=sha(key.encode())
        if handle not in self.operations:
            # Match the original owner's retained request bound: supported
            # books can exceed 256 pages. Running I/O remains capped at two.
            if len(self.operations)>=4096 or (not wait_for_capacity
                    and sum(not op.task.done() for op in self.operations.values())>=2):
                raise SnapshotError('snapshot_io_capacity')
            operation=_Operation(binding)
            async def run():
                async with asyncio.timeout(self.factory.limits.timeout_seconds):
                    # A page may retain its same short pending handle while
                    # these existing I/O slots are busy. It holds no BGE permit.
                    async with self._slots:
                        await self._current(operation)
                        result=await action(operation)
                        await self._current(operation)
                        return result
            operation.task=asyncio.create_task(run())
            self.operations[handle]=operation
        return dict(status='pending',handle=handle)

    def capture(self,key,binding,*,workspace,versions,expected_sequence):
        async def run(operation):
            async def upload(path,*,size,sha256):
                operation.upload_result=asyncio.get_running_loop().create_future()
                operation.upload=(path,size,sha256)
                return await operation.upload_result
            return await self.factory.capture(binding,workspace=workspace,versions=versions,
                expected_sequence=expected_sequence,upload_callback=upload,
                before_effect=lambda:self._current(operation))
        return self.start(key,binding,run)

    def _operation(self,handle,binding):
        self.factory._current(binding)
        operation=self.operations.get(handle)
        if operation is None or operation.binding[:2]!=binding[:2]:
            raise SnapshotError('snapshot_io_operation_unknown')
        operation.binding=binding;operation.attached.set()
        return operation

    def status(self,handle,binding):
        operation=self._operation(handle,binding)
        if operation.task.done():
            try:return dict(status='complete',value=operation.task.result())
            except Exception as error:
                if not operation.failure_logged:
                    operation.failure_logged=True
                    literal=(error.args[0] if type(error) is SnapshotError and len(error.args)==1
                             and type(error.args[0]) is str else None)
                    LOG.warning('snapshot_io failed code=%s',
                                _SNAPSHOT_FAILURE_CODES.get(literal,'unclassified'))
                raise SnapshotError('snapshot_io_failed') from None
        return dict(status='upload_required' if operation.upload and operation.upload_task is None else 'pending',
                    handle=handle)

    async def upload(self,handle,binding,operation_key):
        operation=self._operation(handle,binding)
        if operation.upload_task is not None:return dict(status='started')
        if operation.upload is None:raise SnapshotError('snapshot_io_upload_not_prepared')
        path,size,digest=operation.upload
        ticket=SnapshotTicket.from_wire(await self.tickets.upload_ticket(size=size,sha256=digest))
        if (ticket.size,ticket.sha256)!=(size,digest):raise SnapshotError('snapshot_io_upload_bytes_changed')
        async def transfer():
            try:
                await self._current(operation)
                etag=await self.transfer.upload(ticket,path,operation_key=operation_key)
                await self._current(operation)
                operation.upload_result.set_result(BlobRef(ticket.key,size,digest,etag))
            except BaseException as error:
                if not operation.upload_result.done():operation.upload_result.set_exception(error)
        operation.upload_task=asyncio.create_task(transfer())
        return dict(status='started')

    async def aclose(self):
        self.closed=True
        tasks=[task for op in self.operations.values() for task in (op.task,op.upload_task) if task is not None]
        for task in tasks:
            if not task.done():task.cancel()
        await asyncio.gather(*tasks,return_exceptions=True)
