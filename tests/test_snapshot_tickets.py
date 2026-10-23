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
async def test_real_kaggle_json_upload_uses_scoped_origin_and_object_generation(tmp_path,monkeypatch):
    import aiohttp,json
    from pathlib import Path
    raw=b'synthetic physical archive';path=tmp_path/'pg.blob';path.write_bytes(raw)
    key='capture';digest=hashlib.sha256(raw).hexdigest();sent=[]
    ticket=SnapshotTicket('PUT',hashlib.sha256(key.encode()).hexdigest(),'dataset-blobs/capture/pg.blob',
        len(raw),digest,200,'https://www.googleapis.com/upload/storage/v1/b/owned/o?upload_id=opaque',
        (('Content-Length',str(len(raw))),('Content-Type','application/octet-stream')),upload_kind='kaggle-dataset')
    class Content:
        done=False
        async def read(self,size):
            if self.done:return b''
            self.done=True
            return json.dumps(dict(kind='storage#object',generation='123456789')).encode()
    class Response:
        status=200;headers={};content=Content()
        def __init__(self,data):self.data=data
        async def __aenter__(self):
            sent.append(b''.join([chunk async for chunk in self.data]));return self
        async def __aexit__(self,*args):pass
    class Session:
        def __init__(self,**kwargs):assert kwargs['trust_env'] is False
        async def __aenter__(self):return self
        async def __aexit__(self,*args):pass
        def put(self,url,*,data,headers,allow_redirects):
            assert allow_redirects is False and 'If-None-Match' not in headers
            return Response(data)
    monkeypatch.setattr(aiohttp,'ClientSession',Session)
    transfer=NativeSnapshotTransfer(allowed_origins=frozenset({'https://www.googleapis.com'}),max_bytes=1024,clock=lambda:100)
    monkeypatch.setattr(transfer,'_path',lambda value:Path(value))
    assert await transfer.upload(ticket,path,operation_key=key)=='gcs-generation:123456789'
    assert sent==[raw]


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
