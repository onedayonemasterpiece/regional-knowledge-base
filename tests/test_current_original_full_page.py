"""Fictional old-pin fragments through actual MCP Context and issuance boundary."""
import copy
import json
from dataclasses import replace
from types import MethodType, SimpleNamespace
from uuid import UUID, uuid5

import pytest
from starlette.requests import Request

from regional_knowledge.backend import RenderedPage, RenderedPageBatch
from regional_knowledge.original_intake import canonical, digest, decode_page_cursor, encode_page_cursor
from regional_knowledge.read_issuance import MAX_RESPONSE_BYTES, ReadIssuanceMiddleware
from regional_knowledge.server import build_server
from test_kaggle_native_source_importer import native_intake, start, target, auth, people


def old_fragments(receipt, *, characters=120):
    blocks=[dict(block_index=i,bbox=dict(left=10,top=10,right=990,bottom=990),
        text=(f'Fictional old paragraph {i}. '+('x'*characters)+'\n')) for i in range(126)]
    native=''.join(b['text'] for b in blocks);cursor=dict(block=0,offset=0,text=0);parts=[]
    while True:
        position,offset=cursor['block'],cursor['offset'];segments=[]
        for _ in range(12):
            if position==len(blocks):break
            b=blocks[position];end=min(offset+1000,len(b['text']))
            segments.append(dict(block_index=position,bbox=b['bbox'],text=b['text'][offset:end],
                original_length=len(b['text']),offset=offset,end=end,truncated=end<len(b['text']),
                text_sha256=digest(b['text'].encode())))
            if end==len(b['text']):position+=1;offset=0
            else:offset=end
        end=min(cursor['text']+12000,len(native));complete=position==len(blocks) and end==len(native)
        part=dict(format='pdf',page_count=receipt.page_count,physical_page_index=0,width=595.,height=842.,
            page_id=str(uuid5(UUID(receipt.source_id),'page:1:0')),original_sha256=receipt.sha256,
            parser_sha256=receipt.parser_sha256,image_size=14,image_sha256=digest(b'synthetic-jpeg'),
            mime_type='image/jpeg',preview_render=dict(policy='fictional-old-preview',width=700,height=1000),
            native_cursor=cursor,next_native=None if complete else dict(block=position,offset=offset,text=end),
            native_text=native[cursor['text']:end],native_blocks=segments,
            native_text_info=dict(original_length=len(native),block_count=len(blocks),offset=cursor['text'],end=end,
                complete=complete,truncated=end<len(native),blocks_truncated=position<len(blocks),
                requires_visual_review=True,encoding_notice=None,native_text_sha256=digest(native.encode()),
                native_blocks_sha256=digest(canonical(blocks))))
        part['page_receipt_sha256']=digest(canonical(part));parts.append(part)
        if complete:break
        cursor=part['next_native']
    return parts,native


async def setup_page(s, monkeypatch, *, characters=120, gap=None, accepted=None):
    import regional_knowledge.server as server_module
    book=await start(s,mime_type='application/pdf',file_id='fictional-old-pin-pdf')
    receipt=s.originals.receipts[book.ingestion_id];parts,native=old_fragments(receipt,characters=characters)
    originals=s.originals;originals._page_chunks={};originals._page_proofs={};calls=[]
    immutable=[canonical(p) for p in parts]
    async def pages(self,context,ingestion_id,cursor=None,batch_size=1,*,page_read=None,_page_image=None):
        await self.spool._authorize(context)
        index,fragment=decode_page_cursor(cursor);assert index==0 and batch_size==1
        raw=next(p for p in parts if p['native_cursor']==fragment);part=copy.deepcopy(raw);calls.append(fragment)
        chunks=self._page_chunks.setdefault(ingestion_id,{}).setdefault(index,{})
        if index not in self._page_proofs.get(ingestion_id,{}):
            chunks[canonical(fragment).decode()]=canonical(part)
            self._assemble_available(ingestion_id,index,chunks)
        if gap is not None and len(calls)==1:await gap(s)
        await self.spool._authorize(context)
        return RenderedPageBatch((RenderedPage(part['page_id'],index,None,'image/jpeg',b'synthetic-jpeg',
            part['native_text'],tuple(part['native_blocks']),dict(part['native_text_info'],
                original_sha256=receipt.sha256,page_receipt_sha256=part['page_receipt_sha256'],
                source_image_receipt_sha256='a'*64,image_sha256=part['image_sha256'])),),
            encode_page_cursor(index,part['next_native']) if part['next_native'] else '1')
    originals.pages=MethodType(pages,originals)
    async def accepted_page(*args):return accepted
    monkeypatch.setattr('regional_knowledge.source_page_progress.read_accepted_model_page',accepted_page)
    monkeypatch.setattr(server_module,'_principal',lambda:s.principal)
    return SimpleNamespace(book=book,parts=parts,native=native,immutable=immutable,calls=calls,
        server=build_server(target_startup=s.startup),original_receipt=receipt)


async def issue(f, *, cursor=None, before_send=None):
    messages=[];request_id='actual-fictional-old-page-request'
    async def app(scope,receive,send):
        context=SimpleNamespace(request_context=SimpleNamespace(request=Request(scope),request_id=request_id))
        await f.server.call_tool('book_pages',dict(ingestion_id=f.book.ingestion_id,cursor=cursor),context)
        if before_send:await before_send()
        body=json.dumps(dict(jsonrpc='2.0',id=request_id,result={})).encode()
        await send(dict(type='http.response.start',status=200,headers=[(b'content-type',b'application/json')]))
        await send(dict(type='http.response.body',body=body))
    async def receive():return {'type':'http.disconnect'}
    async def send(message):messages.append(message)
    await ReadIssuanceMiddleware(app)({'type':'http','method':'POST','path':'/mcp','headers':[]},receive,send)
    body=b''.join(m.get('body',b'') for m in messages)
    return json.loads(body),body


@pytest.mark.asyncio
async def test_old_126_blocks_one_actual_mcp_result_and_cached_proof_reused(native_intake,monkeypatch):
    s=native_intake;f=await setup_page(s,monkeypatch)
    result,body=await issue(f);content=result['result']['content'];manifest=json.loads(content[0]['text'])
    assert len(content)==2 and content[1]['data']=='c3ludGhldGljLWpwZWc='
    page=manifest['pages'][0]
    assert len(page['native_blocks'])==126 and page['native_text']==f.native
    assert page['native_text_info']['complete'] and manifest['next_cursor']=='1'
    assert len(f.calls)==len(f.parts) and f.immutable==[canonical(p) for p in f.parts]
    assert s.originals.receipts[f.book.ingestion_id]==f.original_receipt
    before=len(f.calls);again,_=await issue(f)
    assert len(f.calls)==before+1 and again==result
    tool=next(t for t in await f.server.list_tools() if t.name=='book_pages')
    assert set(tool.input_schema['properties'])=={'ingestion_id','cursor','batch_size'}


@pytest.mark.asyncio
async def test_explicit_pc1_and_direct_internal_call_keep_old_fragment(native_intake,monkeypatch):
    s=native_intake;f=await setup_page(s,monkeypatch)
    batch=await s.importer.book_pages(principal=s.principal,ingestion_id=f.book.ingestion_id)
    assert len(batch.pages[0].native_blocks)==12 and batch.next_cursor.startswith('pc1.')
    result,_=await issue(f,cursor=batch.next_cursor);manifest=json.loads(result['result']['content'][0]['text'])
    assert len(manifest['pages'][0]['native_blocks'])==12 and manifest['next_cursor'].startswith('pc1.')
    assert len(f.calls)==2 and f.immutable==[canonical(p) for p in f.parts]


@pytest.mark.asyncio
async def test_oversized_complete_with_accepted_page_keeps_original_fragment_under_actual_gate(native_intake,monkeypatch):
    s=native_intake;accepted={'fictional_already_accepted':'a'*200000}
    f=await setup_page(s,monkeypatch,characters=2500,accepted=accepted)
    result,body=await issue(f);manifest=json.loads(result['result']['content'][0]['text'])
    assert len(body)<=MAX_RESPONSE_BYTES+4*((len(b'synthetic-jpeg')+2)//3)
    assert manifest['accepted_page']==accepted and manifest['next_cursor'] is None
    page=manifest['pages'][0]
    assert page['native_text']==f.parts[0]['native_text'] and len(page['native_blocks'])==12
    assert not page['native_text_info']['complete']
    # accepted_page intentionally clears the cursor for semantic correction;
    # ordinary pages retain their exact original pc1 cursor instead.
    f=await setup_page(s,monkeypatch,characters=2500)
    result,_=await issue(f);manifest=json.loads(result['result']['content'][0]['text'])
    assert manifest['next_cursor']==encode_page_cursor(0,f.parts[0]['next_native'])
    assert f.immutable==[canonical(p) for p in f.parts]


@pytest.mark.asyncio
@pytest.mark.parametrize('withdraw',['none','revoke','binding'])
async def test_old_pin_slow_drain_renews_authority_but_withdrawal_never_issues_pixels(native_intake,monkeypatch,withdraw):
    s=native_intake;resolver=s.importer.read_authority;resolver.clock=lambda:s.t.auth.now[0]
    async def gap(s):
        s.t.auth.now[0]+=resolver.ttl_seconds+1
        if withdraw=='revoke':
            await s.t.auth.provider.revoke_token(await s.t.auth.provider.load_access_token(s.principal.access_token))
        elif withdraw=='binding':
            with s.t.queue.connect() as db:db.execute('UPDATE runs SET channel_active=0 WHERE id=?',(s.t.run,))
    f=await setup_page(s,monkeypatch,gap=gap)
    if withdraw=='none':
        result,_=await issue(f)
        assert len(json.loads(result['result']['content'][0]['text'])['pages'][0]['native_blocks'])==126
    else:
        from mcp.server.mcpserver.exceptions import ToolError
        with pytest.raises((ToolError,PermissionError)):await issue(f)


@pytest.mark.asyncio
async def test_current_owner_withdrawal_after_cached_page_before_issuance_denies_whole_frame(native_intake,monkeypatch):
    s=native_intake;f=await setup_page(s,monkeypatch)
    await issue(f)
    async def revoke():await s.t.auth.provider.revoke_token(await s.t.auth.provider.load_access_token(s.principal.access_token))
    result,body=await issue(f,before_send=revoke)
    assert result['result']['isError'] and b'c3ludGhldGljLWpwZWc=' not in body and f.native.encode() not in body


@pytest.mark.asyncio
@pytest.mark.parametrize('withdraw',[False,True])
async def test_optional_continuation_timeout_keeps_first_fragment_but_final_withdrawal_denies(native_intake,monkeypatch,withdraw):
    from mcp.server.mcpserver.exceptions import ToolError
    s=native_intake;f=await setup_page(s,monkeypatch);original=s.originals.pages
    async def timeout(context,ingestion_id,cursor=None,batch_size=1,*,page_read=None,_page_image=None):
        if cursor and cursor.startswith('pc1.'):
            if withdraw:
                await s.t.auth.provider.revoke_token(await s.t.auth.provider.load_access_token(s.principal.access_token))
            raise TimeoutError('fictional ordinary continuation wait expired')
        return await original(context,ingestion_id,cursor,batch_size)
    s.originals.pages=timeout
    if withdraw:
        with pytest.raises(ToolError):await issue(f)
        return
    result,_=await issue(f);manifest=json.loads(result['result']['content'][0]['text'])
    page=manifest['pages'][0]
    assert manifest['next_cursor']==encode_page_cursor(0,f.parts[0]['next_native'])
    assert page['native_text']==f.parts[0]['native_text'] and len(page['native_blocks'])==12
    assert not page['native_text_info']['complete'] and len(result['result']['content'])==2
    assert f.immutable==[canonical(p) for p in f.parts]
    # A caller's explicit continuation is the original operation, not optional drain.
    with pytest.raises(ToolError) as error:await issue(f,cursor=manifest['next_cursor'])
    cause=error.value
    while cause.__cause__ is not None:cause=cause.__cause__
    assert json.loads(str(cause))['reason']=='transport_timeout'
