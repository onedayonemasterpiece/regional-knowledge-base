"""Full control/queue/native-file workflow with synthetic HTTP and parser peers."""
import asyncio
import io
import json
from types import SimpleNamespace
from uuid import UUID,uuid5

import pytest

from regional_knowledge.contracts import ChatFile
from regional_knowledge.control_spool import ControlSpool
from regional_knowledge.kaggle_apply import HandlerRegistry
from regional_knowledge.kaggle_public_backend import current_binding
from regional_knowledge.kaggle_recovery_dispatch import RecoveryPhaseDispatch
from regional_knowledge.original_intake_controller import OriginalIntakeController
from regional_knowledge.original_intake import digest,canonical
from regional_knowledge.original_intake import OriginalIntakeError
from test_queue_writer import owner
from test_read_authority import auth
from test_participants import people
from test_control_spool import handler
from test_kaggle_original_intake import native,PDF,IMAGE,SOURCE,INGEST
from test_recovery_s3 import BotoFailure


@pytest.mark.asyncio
@pytest.mark.parametrize('continued',[False,True])
@pytest.mark.parametrize('telegram,mime_type',[(False,'application/pdf'),(False,''),(True,None)])
async def test_existing_queue_receipts_url_privacy_and_native_original_readback(owner,native,continued,telegram,mime_type):
    e,n=owner,native
    if continued:
        import base64
        first='actual fixture span';tail=' plus complete later block';full=first+tail
        boxes=[dict(left=0,top=0,right=500,bottom=100),dict(left=0,top=100,right=500,bottom=200)]
        blocks=[dict(block_index=i,bbox=boxes[i],text=t) for i,t in enumerate((first,tail))]
        async def continued_parser(path,action,index=0,*,cursor=None):
            if action=='inspect':return dict(format='pdf',page_count=1)
            position=0 if cursor is None else cursor['block'];body=(first,tail)[position]
            offset=0 if position==0 else len(first)
            value=dict(format='pdf',page_count=1,physical_page_index=0,width=100,height=200,
                native_cursor=dict(block=position,offset=0,text=offset),
                next_native=dict(block=1,offset=0,text=len(first)) if position==0 else None,
                native_text=body,native_text_info=dict(original_length=len(full),offset=offset,end=offset+len(body),
                    truncated=position==0,requires_visual_review=True,encoding_notice=None,block_count=2,
                    blocks_truncated=position==0,complete=position==1,native_text_sha256=digest(full.encode()),
                    native_blocks_sha256=digest(canonical(blocks))),native_blocks=[dict(blocks[position],
                        original_length=len(body),offset=0,end=len(body),truncated=False,text_sha256=digest(body.encode()))])
            if action=='page':value.update(image_b64=base64.b64encode(IMAGE).decode(),image_sha256=digest(IMAGE),
                preview_render=dict(width=100,height=200,scale=1,jpeg_quality=82,preview_only=True))
            return value
        n.handler.parser=continued_parser
    spool=ControlSpool(e.q,e.auth.p.registry,e.j,HandlerRegistry([handler()]))
    binding=current_binding(e.q)
    current=lambda b:b==binding and e.w.current(database_generation='synthetic-db',writer_generation='writer-1')
    with e.q.connect(write=False) as db:run=db.execute('SELECT * FROM runs WHERE id=?',(e.run,)).fetchone()
    bridge=RecoveryPhaseDispatch(e.q,database=SimpleNamespace(database_generation='synthetic-db',writer_generation='writer-1'),
        identity=(e.run,run['launch_nonce'],binding.runtime_incarnation),binding_current=current)
    n.handler.identity=bridge.identity;n.handler.database=bridge.database
    n.handler.current=lambda b:b==(binding.run_id,binding.runtime_incarnation,binding.connection_generation)
    n.handler.attached(e.generation)
    native_reads=[];uploads=[]
    async def upload(ticket,path,*,operation_key):
        uploads.append(ticket.key);e.c.objects[ticket.key]=(path.read_bytes(),'native-etag')
        if len(uploads)==1:raise TimeoutError('lost upload acknowledgment')
        return 'native-etag'
    async def download(ticket,path,*,operation_key):
        native_reads.append(ticket.key);data=e.c.objects[ticket.key][0]
        assert digest(data)==ticket.sha256;path.write_bytes(data);return path
    n.transfer.upload,n.transfer.download=upload,download
    head_unknown=[True]
    def head(**kw):
        if head_unknown.pop() if head_unknown else False:raise TimeoutError('private HEAD error')
        if kw['Key'] not in e.c.objects:raise BotoFailure('NoSuchKey',404)
        data,version=e.c.objects[kw['Key']]
        return dict(ContentLength=len(data),ETag=version)
    e.c.head_object=head
    e.c.generate_presigned_url=lambda *a,**k:'https://archive.invalid/signed-private-ticket'
    original_get=e.c.get_object
    dev_body_reads=[]
    def get(**kw):
        if kw['Key'].startswith('originals/'):
            dev_body_reads.append(kw['Key'])
            assert '/pages/' in kw['Key'],'Dev attempted original bulk download'
        return original_get(**kw)
    e.c.get_object=get
    controller=OriginalIntakeController(queue=e.q,spool=spool,participants=e.auth.p.registry,
        object_store=SimpleNamespace(client=e.c,bucket=e.s.bucket),binding_current=current,
        current_binding=lambda:current_binding(e.q),allowed_origins=frozenset({'https://archive.invalid'}))
    stop=asyncio.Event();phases=[]
    async def peer():
        while not stop.is_set():
            work=await e.a.next_channel_work(e.run,e.token,e.generation)
            if work is None:await asyncio.sleep(.001);continue
            e.a.mark_work_sent(work)
            f=dict(type='DISPATCH',protocol_version=2,connection_generation=e.generation,
                **dict(work.document_payload),compute_claim=work.compute_claim,dispatch_generation=work.dispatch_generation)
            phases.append(f['phase'])
            receipt=await n.handler(f)
            await bridge.receive(dict(type='RECOVERY_PHASE_RESULT',job_id=f['id'],claim=f['claim'],receipt=receipt,
                **{k:f[k] for k in ('run_id','launch_nonce','runtime_incarnation','compute_claim','dispatch_generation')}),
                run_id=e.run,token=e.token,generation=e.generation)
            r=work.resource
            await e.a.resource_released(e.run,e.token,e.generation,runtime_incarnation=r.runtime_incarnation,
                resource_kind='document',resource_id=r.resource_id,compute_claim=r.compute_claim,dispatch_generation=r.dispatch_generation)
    task=asyncio.create_task(peer())
    try:
        if telegram:
            from test_telegram_original import Client, locator
            from regional_knowledge.telegram_original import NativeTelegramOriginalDownloader
            client=Client(PDF);file=locator(PDF)
            n.handler.telegram_downloader=NativeTelegramOriginalDownloader(client=client,
                chat_id=file.chat_id,topic_id=file.topic_id,current=lambda:True)
        else:
            file=ChatFile(download_url='https://attachment.invalid/private',file_id='file',mime_type=mime_type)
        status=await controller.begin(e.auth.p.admin,operation_id='original-fixture',source_id=SOURCE,ingestion_id=INGEST,
            file=file,max_bytes=len(PDF))
        assert not status.verified and phases==['original_acquire'] and not uploads
        status=await controller.status(e.auth.p.admin,INGEST)
        assert not status.verified and phases==['original_acquire','original_archive']
        status=await controller.status(e.auth.p.admin,INGEST)
        assert status.verified and status.receipt.original['sha256']==digest(PDF)
        assert phases==['original_acquire','original_archive','original_verify']
        assert len(uploads)==1  # Reconciliation does not PUT the original twice.
        assert len(native_reads)==1 and not dev_body_reads
        assert (await controller.begin(e.auth.p.admin,operation_id='original-fixture',source_id=SOURCE,ingestion_id=INGEST,
            file=file,max_bytes=len(PDF))).receipt==status.receipt
        assert n.download.calls==(0 if telegram else 1)
        if telegram:
            assert client.fetches==1 and client.iterator.closed
            from dataclasses import replace
            with pytest.raises(OriginalIntakeError,match='operation_changed'):
                await controller.begin(e.auth.p.admin,operation_id='original-fixture',source_id=SOURCE,ingestion_id=INGEST,
                    file=replace(file,message_id=file.message_id+1),max_bytes=len(PDF))
            assert client.fetches==1
        batch=await controller.pages(e.auth.p.admin,INGEST)
        assert batch.pages[0].data==IMAGE and batch.pages[0].native_text=='actual fixture span'
        assert batch.next_cursor.startswith('pc1.') if continued else batch.next_cursor is None
        assert bool(controller._page_chunks.get(INGEST))==continued
        # A first image is usable without fetching the rest of native text.
        # The metadata is durable across a controller restart and cannot mint
        # evidence for another page or a caller-supplied image hash.
        image_ref=batch.pages[0].native_text_info['source_image_receipt_sha256']
        before_phases=tuple(phases)
        restarted=OriginalIntakeController(queue=e.q,spool=spool,participants=e.auth.p.registry,
            object_store=SimpleNamespace(client=e.c,bucket=e.s.bucket),binding_current=current,
            current_binding=lambda:current_binding(e.q),allowed_origins=frozenset({'https://archive.invalid'}))
        issued=await restarted.selected_image_receipts(e.auth.p.admin,INGEST,((0,image_ref),))
        assert issued[0]['image_sha256']==digest(IMAGE)
        assert await restarted.selected_image_receipts(e.auth.p.admin,INGEST,((0,None),))==issued
        with pytest.raises(OriginalIntakeError):
            await restarted.selected_image_receipts(e.auth.p.admin,INGEST,((1,None),))
        assert 'native_text' not in issued[0] and tuple(phases)==before_phases
        with pytest.raises(OriginalIntakeError,match='receipt_changed'):
            await restarted.selected_image_receipts(e.auth.p.admin,INGEST,((0,'f'*64),))
        proof=await controller.native_pages(e.auth.p.admin,INGEST)
        assert proof.original_receipt_sha256==status.receipt.receipt_sha256
        assert proof.pages[0]['original_sha256']==digest(PDF)
        assert INGEST not in controller._page_chunks
        if continued:
            assert proof.pages[0]['native_text']==full and len(proof.pages[0]['native_blocks'])==2
            assert proof.pages[0]['native_text_info']['complete'] and not proof.pages[0]['native_text_info']['truncated']
        first_proof=proof.sha256;first_original=status.receipt.receipt_sha256
        old_handle=controller._row(e.auth.p.owner.subject,INGEST)['acquisition_json']
        # Drop only ephemeral native capabilities. The verified object and exact
        # logical source receipt remain durable; recovery must GET, never PUT.
        n.handler._records.clear();controller._page_proofs.clear();controller._page_chunks.clear()
        upload_count=len(uploads);download_count=n.download.calls
        await controller._restore_handle(e.auth.p.admin,e.auth.p.owner.subject,binding,status.receipt,json.loads(old_handle))
        recovered=await controller.pages(e.auth.p.admin,INGEST)
        assert recovered.pages[0].data==IMAGE
        assert len(uploads)==upload_count and n.download.calls==download_count
        assert (await controller.verified(e.auth.p.admin,INGEST)).receipt_sha256==first_original
        assert (await controller.native_pages(e.auth.p.admin,INGEST)).sha256==first_proof
        assert controller._row(e.auth.p.owner.subject,INGEST)['acquisition_json']!=old_handle
        async def djvu(url,folder,*,max_bytes):
            data=b'AT&TFORM'+b'\0'*4+b'DJVU';path=folder/'native.djvu';path.write_bytes(data)
            return SimpleNamespace(path=path,sha256=digest(data),size_bytes=len(data))
        n.handler.downloader.download=djvu
        from regional_knowledge.kaggle_original_parser import parse as pinned_parser
        from regional_knowledge.original_intake import OriginalIntakeRejected
        async def actual_djvu_inspection(path,action,index=0,*,cursor=None):
            # The real Python -I parser wrapper translates its static terminal
            # child code into OriginalIntakeRejected on this trust boundary.
            try:return pinned_parser(path,action,index,cursor=cursor)
            except ValueError as exc:
                if str(exc)=='original_djvu_invalid':
                    raise OriginalIntakeRejected('original_djvu_invalid') from None
                raise
        n.handler.parser=actual_djvu_inspection
        unsupported=str(uuid5(UUID(INGEST),'djvu-intake'))
        file=ChatFile(download_url='https://attachment.invalid/private',file_id='djvu-fixture',mime_type='image/vnd.djvu')
        rejected=await controller.begin(e.auth.p.admin,operation_id='unsupported-djvu',source_id=str(uuid5(UUID(SOURCE),'djvu')),
            ingestion_id=unsupported,file=file,max_bytes=len(PDF))
        assert rejected.state=='rejected' and rejected.reason=='original_djvu_invalid'
        assert (await controller.status(e.auth.p.admin,unsupported)).state=='rejected'
        assert await controller.verified(e.auth.p.admin,unsupported) is None
        assert dev_body_reads and all('/pages/' in key for key in dev_body_reads)
        with e.q.connect(write=False) as db:
            rows=db.execute("SELECT identity,result FROM jobs WHERE kind='recovery_phase'").fetchall()
            assert 'attachment.invalid' not in str([tuple(row) for row in rows]) and 'signed-private-ticket' not in str([tuple(row) for row in rows])
        await e.w.retire('writer-1')
        with pytest.raises(Exception):await controller.verified(e.auth.p.admin,INGEST)
    finally:
        stop.set();await task;await bridge.aclose();spool.close();await n.handler.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize('response',[TimeoutError('private provider failure'),BotoFailure('AccessDenied',403),
    {'ContentLength':999,'ETag':'version'}])
async def test_unknown_or_conflicting_head_never_authorizes_a_put(response):
    controller=object.__new__(OriginalIntakeController);puts=[]
    def head(**kw):
        if isinstance(response,Exception):raise response
        return response
    controller.store=SimpleNamespace(client=SimpleNamespace(head_object=head),bucket='existing-private')
    controller.spool=SimpleNamespace(journal=SimpleNamespace(stream='existing-stream'))
    async def forbidden(*a,**k):puts.append(True);pytest.fail('unknown HEAD was treated as absent')
    controller._ticket=forbidden
    record=dict(handle='retained',sha256=digest(PDF),size=len(PDF),spec={'source_id':SOURCE})
    with pytest.raises(OriginalIntakeError):await controller._archive(object(),record)
    assert not puts


@pytest.mark.asyncio
async def test_large_original_page_releases_chunks_and_keeps_proof():
    controller=object.__new__(OriginalIntakeController)
    controller._page_chunks={};controller._page_proofs={}
    receipt=SimpleNamespace(source_id=SOURCE,page_count=139,sha256=digest(PDF),parser_sha256='parser')
    cursor=dict(block=0,offset=0,text=0)
    page=dict(page_id=str(uuid5(UUID(SOURCE),'page:1:0')),original_sha256=receipt.sha256,
        parser_sha256=receipt.parser_sha256,native_cursor=cursor,next_native=None,
        native_text='page text',native_blocks=[],native_text_info={})
    page['page_receipt_sha256']=digest(canonical(page))

    async def authorize(_context):return 'actor','binding'
    async def verified(_context,_ingestion_id):return receipt
    async def page_data(_binding,record,_index,native_cursor,*,restore):
        assert native_cursor==cursor
        return record,page,IMAGE

    controller._authorize=authorize
    controller.verified=verified
    controller._row=lambda _actor,_ingestion_id:{'acquisition_json':json.dumps({'sha256':receipt.sha256})}
    controller._page_data=page_data
    controller._record_issued_image=lambda _receipt,_page:dict(
        source_image_receipt_sha256='issued',image_sha256=digest(IMAGE))

    batch=await controller.pages(None,INGEST)
    assert batch.pages[0].data==IMAGE and batch.pages[0].native_text=='page text'
    assert batch.next_cursor=='1'
    assert controller._page_chunks=={}
    assert 0 in controller._page_proofs[INGEST]
