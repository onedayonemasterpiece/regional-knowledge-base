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
        loc=locator(data)
        self.message=SimpleNamespace(id=loc.message_id,chat_id=loc.chat_id,
            reply_to=SimpleNamespace(reply_to_top_id=loc.topic_id),
            document=SimpleNamespace(id=loc.document_id,size=loc.size))
        self.chunks=[data]
    async def get_messages(self,chat_id,*,ids):
        assert chat_id==locator().chat_id and ids==locator().message_id
        self.fetches+=1;return self.message
    def iter_download(self,document,*,request_size,chunk_size):
        assert document is self.message.document and request_size==chunk_size==65536
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
async def test_foreign_scope_and_expired_dispatch_do_not_read(tmp_path):
    for loc,current in [(replace(locator(),topic_id=3),lambda:True),(locator(),lambda:False)]:
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
