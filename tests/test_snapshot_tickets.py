from contextlib import asynccontextmanager
from dataclasses import replace
import hashlib
import io
from types import SimpleNamespace

import pytest

from regional_knowledge.kaggle_pg_snapshot_native import SnapshotError
from regional_knowledge.native_snapshot_transfer import (
    NativeSnapshotTransfer, SnapshotTicket, SnapshotTransferError,
)
from regional_knowledge.recovery_s3 import S3ConditionalRecoveryStore
from regional_knowledge.snapshot_tickets import SnapshotTicketIssuer


@pytest.fixture
def deployment():
    raw = b'synthetic archive'; binding = SimpleNamespace(writer_generation='current')
    signs = []; state = {'current': True, 'raw': raw}
    class Client:
        meta = SimpleNamespace(service_model=SimpleNamespace(operation_model=lambda _: SimpleNamespace(
            input_shape=SimpleNamespace(members={'IfMatch': None, 'IfNoneMatch': None}))))
        def generate_presigned_url(self, method, **kwargs):
            signs.append((method, kwargs))
            return 'https://storage.example/private?signature=synthetic'
        def head_object(self, **kwargs):
            return {'ContentLength': len(raw), 'ETag': '"native-version"'}
        def get_object(self, **kwargs):
            return {'ContentLength': len(raw), 'ETag': '"native-version"', 'Body': io.BytesIO(state['raw'])}
    @asynccontextmanager
    async def guard(generation):
        if generation != 'current': raise PermissionError('wrong generation')
        yield
    store = S3ConditionalRecoveryStore(client=Client(), bucket='existing-fixture',
        prefix='rkb/fixed-stream/', writer_guard=guard)
    issuer = SnapshotTicketIssuer(store, current=lambda b: b is binding and state['current'],
        allowed_origins=frozenset({'https://storage.example'}), max_single_bytes=1024, clock=lambda: 100)
    return issuer, binding, raw, signs, state


@pytest.mark.asyncio
async def test_exact_conditional_ticket_and_lost_ack_native_reconciliation(deployment):
    issuer, binding, raw, signs, state = deployment
    digest = hashlib.sha256(raw).hexdigest()
    ticket = await issuer.issue_upload(binding, 'operation', size=len(raw), sha256=digest)
    assert SnapshotTicket.from_wire(ticket.wire()) == ticket
    assert 'signature' not in repr(ticket)
    params = signs[0][1]['Params']
    assert params['IfNoneMatch'] == '*' and params['ContentLength'] == len(raw)
    ref = await issuer.reconcile_upload(binding, 'operation', size=len(raw), sha256=digest)
    download = await issuer.issue_download(binding, 'restore-operation', ref)
    assert signs[-1][1]['Params']['IfMatch'] == ref.native_version
    assert dict(download.headers) == {'If-Match': '"native-version"'}
    state['raw'] = b'changed archive'
    with pytest.raises(SnapshotError):
        await issuer.reconcile_upload(binding, 'operation', size=len(raw), sha256=digest)


@pytest.mark.asyncio
async def test_current_binding_and_bound_before_presign(deployment):
    issuer, binding, raw, signs, state = deployment
    state['current'] = False
    with pytest.raises(SnapshotError):
        await issuer.issue_upload(binding, 'operation', size=len(raw), sha256='a' * 64)
    assert not signs
    state['current'] = True
    with pytest.raises(SnapshotError):
        await issuer.issue_upload(binding, 'operation', size=1025, sha256='a' * 64)
    assert not signs


@pytest.mark.asyncio
async def test_ticket_cannot_change_operation_origin_method_or_expiry(deployment):
    issuer, binding, raw, _, _ = deployment
    ticket = await issuer.issue_upload(binding, 'operation', size=len(raw), sha256=hashlib.sha256(raw).hexdigest())
    transfer = NativeSnapshotTransfer(allowed_origins=frozenset({'https://storage.example'}),
        max_bytes=1024, clock=lambda: 200)
    assert transfer._check(ticket, 'PUT', 'operation') > 0
    for value, method, key in [(ticket, 'GET', 'operation'), (ticket, 'PUT', 'other'),
            (replace(ticket, expires_at=199), 'PUT', 'operation'),
            (replace(ticket, url='https://other.example/private'), 'PUT', 'operation')]:
        with pytest.raises(SnapshotTransferError): transfer._check(value, method, key)
    with pytest.raises(SnapshotTransferError):
        replace(ticket, headers=tuple((k,v) for k,v in ticket.headers if k != 'If-None-Match'))
    with pytest.raises(SnapshotTransferError):
        transfer._path('/kaggle/working/private-corpus')
