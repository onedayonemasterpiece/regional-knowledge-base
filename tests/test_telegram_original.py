"""Synthetic Telegram peer only: no credentials, connection or private books."""
import asyncio
from dataclasses import replace
from types import SimpleNamespace

import pytest

from regional_knowledge.contracts import TelegramOriginalInput
from regional_knowledge.original_intake import OriginalIntakeError, canonical, digest
from regional_knowledge.telegram_original import TelegramOriginalLocator, NativeTelegramOriginalDownloader
from test_kaggle_original_intake import native, frame, spec, PDF


def locator(data=PDF):
    return TelegramOriginalLocator(-1001234567890, 2, 10, 123456789, digest(data), len(data))


class Iterator:
    def __init__(self, chunks):
        self.chunks=iter(chunks);self.closed=False
    def __aiter__(self):return self
    async def __anext__(self):
        try:chunk=next(self.chunks)
        except StopIteration:raise StopAsyncIteration
        if isinstance(chunk,BaseException):raise chunk
        return chunk
    async def close(self):self.closed=True


class Client:
    def __init__(self,data=PDF):
        self.fetches=0;self.downloads=0;self.data=data;self.iterator=None
        self.sent=None;self.send_calls=0;self.send_error=False
        loc=locator(data)
        self.message=SimpleNamespace(id=loc.message_id,chat_id=loc.chat_id,
            reply_to=SimpleNamespace(reply_to_top_id=loc.topic_id),
            document=SimpleNamespace(id=loc.document_id,size=loc.size))
        self.chunks=[data]
    async def get_messages(self,chat_id,*,ids):
        assert chat_id==locator().chat_id
        self.fetches+=1
        if self.sent is not None and ids==self.sent.id:return self.sent
        return self.message
    async def iter_messages(self,chat_id,*,search,limit):
        assert chat_id==locator().chat_id and limit==10
        if self.sent is not None and self.sent.message==search:yield self.sent

    async def send_file(self,chat_id,path,*,caption,reply_to,force_document,mime_type,attributes):
        from pathlib import Path
        assert chat_id==locator().chat_id and reply_to==2 and force_document
        assert Path(path).read_bytes()==self.data
        self.sent_file_name,self.sent_mime_type=attributes[0].file_name,mime_type
        assert not self.sent_file_name.endswith('.source')
        self.send_calls+=1
        self.sent=SimpleNamespace(id=11,chat_id=chat_id,message=caption,
            reply_to=SimpleNamespace(reply_to_top_id=2),
            document=SimpleNamespace(id=987654321,size=len(self.data)))
        if self.send_error:raise TimeoutError('lost send acknowledgment')
        return self.sent

    def iter_download(self,document,*,request_size,chunk_size):
        assert document in (self.message.document, getattr(self.sent,'document',None))
        assert request_size==chunk_size==65536
        self.downloads+=1;self.iterator=Iterator(self.chunks);return self.iterator


def downloader(client,*,current=lambda:True):
    loc=locator()
    return NativeTelegramOriginalDownloader(client=client,chat_id=loc.chat_id,topic_id=loc.topic_id,current=current)


@pytest.mark.asyncio
async def test_direct_document_stream_and_exact_pin(tmp_path):
    client=Client();result=await downloader(client).download(locator(),tmp_path,max_bytes=len(PDF),current=lambda:True)
    assert result.path.read_bytes()==PDF and result.sha256==digest(PDF) and result.size_bytes==len(PDF)
    assert client.fetches==client.downloads==1 and client.iterator.closed


@pytest.mark.asyncio
@pytest.mark.parametrize('field,value',[('id',11),('chat_id',-1009876543210),('topic',3),('document_id',987),('size',1),('missing',None)])
async def test_changed_document_metadata_never_downloads(tmp_path,field,value):
    client=Client()
    if field=='topic':client.message.reply_to.reply_to_top_id=value
    elif field=='document_id':client.message.document.id=value
    elif field=='size':client.message.document.size=value
    elif field=='missing':client.message=None
    else:setattr(client.message,field,value)
    with pytest.raises(OriginalIntakeError,match='document_changed'):
        await downloader(client).download(locator(),tmp_path,max_bytes=len(PDF),current=lambda:True)
    assert client.downloads==0 and not list(tmp_path.iterdir())


@pytest.mark.asyncio
@pytest.mark.parametrize('chunks',[[PDF+b'excess'],[PDF[:-1]],[b'x'*len(PDF)],[b''],[b'x'*65537]])
async def test_stream_bound_hash_and_short_read_cleanup(tmp_path,chunks):
    client=Client();client.chunks=chunks
    with pytest.raises(OriginalIntakeError):
        await downloader(client).download(locator(),tmp_path,max_bytes=len(PDF),current=lambda:True)
    assert client.iterator.closed and not list(tmp_path.iterdir())


@pytest.mark.asyncio
async def test_cancellation_and_lease_loss_cleanup(tmp_path):
    client=Client();client.chunks=[PDF[:5],asyncio.CancelledError()]
    with pytest.raises(asyncio.CancelledError):
        await downloader(client).download(locator(),tmp_path,max_bytes=len(PDF),current=lambda:True)
    assert client.iterator.closed and not list(tmp_path.iterdir())
    checks=iter([True,True,False]);client=Client()
    with pytest.raises(OriginalIntakeError,match='lease_changed'):
        await downloader(client,current=lambda:next(checks)).download(locator(),tmp_path,max_bytes=len(PDF),current=lambda:True)
    assert client.iterator.closed and not list(tmp_path.iterdir())


@pytest.mark.asyncio
@pytest.mark.parametrize('minimal',[False,True])
async def test_foreign_scope_and_expired_dispatch_do_not_read(tmp_path,minimal):
    for loc,current in [(replace(locator(),topic_id=3),lambda:True),(locator(),lambda:False)]:
        if minimal:loc=replace(loc,document_id=None,sha256=None,size=None)
        client=Client()
        with pytest.raises(OriginalIntakeError):
            await downloader(client).download(loc,tmp_path,max_bytes=len(PDF),current=current)
        assert client.fetches==0


@pytest.mark.asyncio
async def test_native_acquire_uses_bound_locator_without_url_or_proxy(native):
    loc=locator();client=Client();native.handler.telegram_downloader=downloader(client)
    s=spec(file_id=loc.file_id,file_name=loc.file_name,mime_type=loc.mime_type,
        expected_sha256=loc.sha256,expected_size=loc.size)
    f=frame('original_acquire',dict(spec=s.wire(),telegram_locator=loc.wire()))
    result=await native.handler(f);record=result['value']
    assert record['sha256']==loc.sha256 and record['size']==loc.size and record['format']=='pdf'
    assert record['spec']==s.wire() and native.parser==['inspect'] and native.download.calls==0
    assert not native.transfer.calls and client.fetches==1
    assert await native.handler(f)==result and client.fetches==1
    changed=dict(f,arguments=dict(f['arguments'],telegram_locator=replace(loc,message_id=11).wire()))
    with pytest.raises(OriginalIntakeError,match='request_hash'):await native.handler(changed)


@pytest.mark.asyncio
async def test_native_binding_missing_and_mixed_transport_refused(native):
    loc=locator();s=spec(file_id=loc.file_id,expected_sha256=loc.sha256,expected_size=loc.size)
    f=frame('original_acquire',dict(spec=s.wire(),telegram_locator=loc.wire()))
    assert (await native.handler(f))['value']=={'error':'original_telegram_binding_required'}
    mixed=frame('original_acquire',f['arguments'],key='mixed',private={'download_url':'https://attachment.invalid/private'})
    assert (await native.handler(mixed))['value']=={'error':'original_acquire_contract'}
    assert native.download.calls==0 and not list(native.handler.root.iterdir())


def test_strict_public_locator_has_no_credential_or_url_fields():
    loc=locator();assert TelegramOriginalInput(**loc.wire()).locator()==loc
    for extra in ({'session':'private'},{'download_url':'https://example.invalid'}, {'message_id':True}, {'document_id':0}):
        with pytest.raises(ValueError):TelegramOriginalInput(**dict(loc.wire(),**extra))


@pytest.mark.asyncio
async def test_mcp_keeps_attachment_fileparams_and_refuses_legacy_telegram(monkeypatch):
    from regional_knowledge.server import build_server
    monkeypatch.setenv('RKB_DEV_NOAUTH','1')
    server=build_server()
    tool=next(t for t in server._tool_manager.list_tools() if t.name=='book_ingest')
    assert tool.meta['openai/fileParams']==['file']
    schema=tool.fn_metadata.arg_model.model_json_schema()
    assert 'telegram_original' in schema['properties']
    with pytest.raises(Exception,match='telegram_original_requires_target_start_without_attachment'):
        await tool.fn(command='start',telegram_original=TelegramOriginalInput(**locator().wire()))


@pytest.mark.asyncio
async def test_native_archive_original_presence_and_unknown_never_resends(native):
    """Distinguishes S3-only main: Kaggle sends a real document through one fake Telethon client."""
    from test_kaggle_original_intake import acquire,INGEST
    n=native;client=Client()
    n.handler.telegram_downloader=downloader(client)
    _,record=await acquire(n)
    args=dict(handle=record['handle'],original_sha256=record['sha256'])
    send=frame('original_archive',args,key='telegram-new-source')
    result=(await n.handler(send))['value']
    from regional_knowledge.telegram_original import TelegramOriginalLocator
    saved=TelegramOriginalLocator.from_wire(result['telegram_locator'])
    assert (saved.chat_id,saved.topic_id,saved.message_id,saved.document_id)==(
        locator().chat_id,2,11,987654321)
    assert result['key'].startswith('telegram/') and client.send_calls==1
    assert (await n.handler(send))['value']==result and client.send_calls==1
    assert await downloader(client).presence(saved,current=lambda:True)
    assert (await downloader(client).archive(n.handler._records[record['handle']]['path'],
        sha256=record['sha256'],size=record['size'],ingestion_id=INGEST,
        current=lambda:True))==saved
    assert client.send_calls==1
    # Provider can have committed and lost its send ACK. The same native
    # operation is UNKNOWN and must not issue an automatic second send.
    client.sent=None;client.send_error=True
    unknown=frame('original_archive',args,key='telegram-unknown-operation')
    first=(await n.handler(unknown))['value']
    assert first['error']=='original_telegram_send_unknown'
    assert (await n.handler(unknown))['value']==first and client.send_calls==2


@pytest.mark.asyncio
@pytest.mark.parametrize('legacy',[False,True])
async def test_native_restore_reads_registered_telegram_document_without_rewriting_receipt(native,legacy):
    """One synthetic upload followed by an actual Kaggle-native Telegram restore."""
    from test_kaggle_original_intake import acquire, SOURCE, INGEST, REQUEST
    from regional_knowledge.original_intake import VerifiedOriginalReceipt
    client=Client();n=native;n.handler.telegram_downloader=downloader(client)
    _,record=await acquire(n)
    args=dict(handle=record['handle'],original_sha256=record['sha256'])
    value=(await n.handler(frame('original_archive',args,key='restore-upload')))['value']
    loc=TelegramOriginalLocator.from_wire(value['telegram_locator'])
    receipt=VerifiedOriginalReceipt(SOURCE,INGEST,REQUEST,1,record['sha256'],record['size'],
        value['key'],value['native_version'],'pdf',record['page_count'],record['parser_sha256'],
        record['acquisition_sha256'],value['native_verification_sha256'])
    if legacy:
        receipt=replace(receipt,key='originals/legacy/exact-source',native_version='old-etag')
    immutable=receipt.wire()
    n.handler._records.clear()
    restored=frame('original_acquire',dict(spec=record['spec'],restore_receipt=immutable,
        restore_nonce='a'*48,telegram_locator=loc.wire()),key='restore-from-topic2')
    result=(await n.handler(restored))['value']
    assert result['restored_from_receipt_sha256']==receipt.receipt_sha256
    assert result['sha256']==record['sha256'] and result['parser_sha256']==record['parser_sha256']
    assert receipt.wire()==immutable and client.send_calls==client.downloads==1
    assert n.download.calls==1  # initial fixture acquisition; restore never fetched S3 or Dev


def test_public_document_id_decimal_string_preserves_64_bits():
    exact=5379726664597024218
    model=TelegramOriginalInput(**dict(locator().wire(),document_id=str(exact)))
    assert model.locator().document_id==exact
    for bad in ('5.3e18', '05379726664597024218', ' 5379726664597024218'):
        with pytest.raises(ValueError):TelegramOriginalInput(**dict(locator().wire(),document_id=bad))
