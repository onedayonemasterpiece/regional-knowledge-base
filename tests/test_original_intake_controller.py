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

def legacy_s3_original_fixture(controller):
    """Historical original S3 receipt fixture; active intake uses Telegram instead."""
    archive=controller._archive
    async def old_archive(binding,record,*,page=None):
        if page is not None:
            return await archive(binding,record,page=page)
        args=dict(handle=record['handle'],original_sha256=record['sha256'])
        key='originals/v1/'+digest(controller.spool.journal.stream.encode())+'/'+record['spec']['source_id']+'/'+record['sha256']
        version=await controller._head(key,record['size'])
        if version is None:
            operation=digest(canonical(dict(phase='original_archive',args=args)))
            ticket=await controller._ticket(binding,operation,'PUT',key,record['size'],record['sha256'])
            native=await controller._phase(binding,'original_archive',args,{'ticket':ticket},operation_key=operation)
            version=await controller._head(key,record['size'],native['native_version'])
        operation=digest(canonical(dict(phase='original_verify',args=args)))
        ticket=await controller._ticket(binding,operation,'GET',key,record['size'],record['sha256'],version)
        receipt=await controller._phase(binding,'original_verify',args,{'ticket':ticket})
        from test_telegram_original import locator
        receipt['telegram_locator']=locator(PDF).wire()  # existing synthetic topic-2 copy
        return receipt
    controller._archive=old_archive



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
    legacy_s3_original_fixture(controller)
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
        # Before the historical sidecar migration, legacy restores remain S3-based.
        with e.q.connect() as db:
            db.execute('UPDATE rkb_original_intakes SET telegram_locator_json=NULL WHERE ingestion_id=?',(INGEST,))
        old_handle=controller._row(e.auth.p.owner.subject,INGEST)['acquisition_json']
        # Drop only ephemeral native capabilities. The verified object and exact
        # logical source receipt remain durable; recovery must GET, never PUT.
        n.handler._records.clear();controller._page_proofs.clear();controller._page_chunks.clear()
        # Simulate a new runtime's upgraded encoder after the original JPEG
        # and issuance receipt were already durably archived.
        previous_parser=n.handler.parser
        async def upgraded_parser(path,action,index=0,*,cursor=None):
            result=await previous_parser(path,action,index,cursor=cursor)
            if action=='page':
                import base64
                changed=b'new-high-resolution-render-different-from-issued-image'
                result.update(image_b64=base64.b64encode(changed).decode(),image_sha256=digest(changed),
                    preview_render=dict(policy='rkb.model-page.v2',width=1600,height=3200,scale=2,
                        jpeg_quality=85,byte_limit=8*1024*1024,image_bytes=len(changed),preview_only=True))
            return result
        n.handler.parser=upgraded_parser
        upload_count=len(uploads);download_count=n.download.calls
        await controller._restore_handle(e.auth.p.admin,e.auth.p.owner.subject,binding,status.receipt,json.loads(old_handle))
        recovered=await controller.pages(e.auth.p.admin,INGEST)
        assert recovered.pages[0].data==IMAGE
        assert recovered.pages[0].native_text_info['source_image_receipt_sha256']==image_ref
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
@pytest.mark.parametrize('upload_outcome',['success','lost_ack','missing_after_failure'])
async def test_new_page_uses_two_durable_phases_and_two_page_heads(owner,native,upload_outcome):
    e,n=owner,native
    spool=ControlSpool(e.q,e.auth.p.registry,e.j,HandlerRegistry([handler()]))
    binding=current_binding(e.q)
    current=lambda b:b==binding and e.w.current(database_generation='synthetic-db',writer_generation='writer-1')
    with e.q.connect(write=False) as db:run=db.execute('SELECT * FROM runs WHERE id=?',(e.run,)).fetchone()
    bridge=RecoveryPhaseDispatch(e.q,database=SimpleNamespace(database_generation='synthetic-db',writer_generation='writer-1'),
        identity=(e.run,run['launch_nonce'],binding.runtime_incarnation),binding_current=current)
    n.handler.identity=bridge.identity;n.handler.database=bridge.database
    n.handler.current=lambda b:b==(binding.run_id,binding.runtime_incarnation,binding.connection_generation)
    n.handler.attached(e.generation)
    heads=[];native_reads=[];phases=[];page_uploads=[]
    async def upload(ticket,path,*,operation_key):
        assert ticket.operation_ref==digest(operation_key.encode())
        if '/pages/' in ticket.key:
            page_uploads.append(operation_key)
            if len(page_uploads)==1 and upload_outcome!='success':
                from regional_knowledge.native_snapshot_transfer import SnapshotTransferError
                if upload_outcome=='lost_ack':e.c.objects[ticket.key]=(path.read_bytes(),'native-etag')
                raise SnapshotTransferError('snapshot_transfer_upload_unknown')
        e.c.objects[ticket.key]=(path.read_bytes(),'native-etag')
        return 'native-etag'
    async def download(ticket,path,*,operation_key):
        native_reads.append(ticket.key);path.write_bytes(e.c.objects[ticket.key][0]);return path
    def head(**kw):
        heads.append(kw['Key'])
        if kw['Key'] not in e.c.objects:raise BotoFailure('NoSuchKey',404)
        data,version=e.c.objects[kw['Key']]
        return dict(ContentLength=len(data),ETag=version)
    n.transfer.upload,n.transfer.download=upload,download
    e.c.head_object=head
    e.c.generate_presigned_url=lambda *a,**k:'https://archive.invalid/signed-private-ticket'
    controller=OriginalIntakeController(queue=e.q,spool=spool,participants=e.auth.p.registry,
        object_store=SimpleNamespace(client=e.c,bucket=e.s.bucket),binding_current=current,
        current_binding=lambda:current_binding(e.q),allowed_origins=frozenset({'https://archive.invalid'}))
    legacy_s3_original_fixture(controller)
    stop=asyncio.Event()
    async def peer():
        while not stop.is_set():
            work=await e.a.next_channel_work(e.run,e.token,e.generation)
            if work is None:await asyncio.sleep(.001);continue
            e.a.mark_work_sent(work)
            f=dict(type='DISPATCH',protocol_version=2,connection_generation=e.generation,
                **dict(work.document_payload),compute_claim=work.compute_claim,dispatch_generation=work.dispatch_generation)
            phases.append(f['phase']);receipt=await n.handler(f)
            await bridge.receive(dict(type='RECOVERY_PHASE_RESULT',job_id=f['id'],claim=f['claim'],receipt=receipt,
                **{k:f[k] for k in ('run_id','launch_nonce','runtime_incarnation','compute_claim','dispatch_generation')}),
                run_id=e.run,token=e.token,generation=e.generation)
            r=work.resource
            await e.a.resource_released(e.run,e.token,e.generation,runtime_incarnation=r.runtime_incarnation,
                resource_kind='document',resource_id=r.resource_id,compute_claim=r.compute_claim,dispatch_generation=r.dispatch_generation)
    task=asyncio.create_task(peer())
    try:
        status=await controller.begin(e.auth.p.admin,operation_id='page-phases',source_id=SOURCE,ingestion_id=INGEST,
            file=ChatFile(download_url='https://attachment.invalid/private',file_id='file'),max_bytes=len(PDF))
        assert status.verified and phases==['original_acquire','original_archive','original_verify']
        assert native_reads==[status.receipt.key]
        heads.clear();phases.clear()
        if upload_outcome!='success':
            with pytest.raises(OriginalIntakeError,match='original_native_receipt_unavailable'):
                await controller.pages(e.auth.p.admin,INGEST)
            with e.q.connect(write=False) as db:
                assert db.execute('SELECT count(*) FROM rkb_original_image_issuance').fetchone()[0]==0
        page=await controller.pages(e.auth.p.admin,INGEST)
        assert page.pages[0].data==IMAGE
        retried=upload_outcome=='missing_after_failure'
        assert phases==['original_pages','original_archive']+(['original_archive'] if retried else [])
        assert len(page_uploads)==len(set(page_uploads))==1+int(retried)
        assert len([key for key in heads if '/pages/' in key])==2+int(retried)
        assert [key for key in heads if '/pages/' not in key]==[status.receipt.key]*(1+int(upload_outcome!='success'))
        assert native_reads==[status.receipt.key]
        heads.clear();phases.clear()
        repeated=await controller.pages(e.auth.p.admin,INGEST)
        assert repeated==page and not phases
        assert len([key for key in heads if '/pages/' in key])==1
        assert [key for key in heads if '/pages/' not in key]==[status.receipt.key]
        assert native_reads==[status.receipt.key]
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
    page=dict(physical_page_index=0,image_sha256=digest(IMAGE),image_size=len(IMAGE))
    with pytest.raises(OriginalIntakeError):await controller._archive(object(),record,page=page)
    assert not puts


@pytest.mark.asyncio
@pytest.mark.parametrize('state',['pending','claimed','done'])
async def test_archive_preserves_unsettled_or_nontransfer_operation_key(owner,state):
    e=owner;binding=SimpleNamespace(run_id=e.run)
    record=dict(handle='retained',sha256=digest(PDF),size=len(PDF),spec={'source_id':SOURCE})
    page=dict(physical_page_index=0,image_sha256=digest(IMAGE),image_size=len(IMAGE))
    args=dict(handle=record['handle'],original_sha256=record['sha256'],page_index=0)
    key=digest(canonical(dict(phase='original_archive',args=args)))
    result=json.dumps({'value':{'error':'original_intake_unavailable','error_class':'ValueError'}}) if state=='done' else None
    with e.q.connect() as db:
        db.execute('INSERT INTO jobs(id,actor,idempotency,kind,texts,identity,state,created,updated,result) VALUES(?,?,?,?,?,?,?,?,?,?)',
            ('prior-archive','maintenance:'+e.run,'recovery:'+key,'recovery_phase','[]','{}',state,0,0,result))
    controller=object.__new__(OriginalIntakeController)
    controller.queue=e.q;controller.spool=SimpleNamespace(journal=SimpleNamespace(stream='fixture'))
    heads=[]
    async def head(*args):
        heads.append(args);return None if len(heads)==1 else 'etag'
    async def ticket(actual_binding,operation,*args):
        assert operation==key;return {'operation':operation}
    async def phase(actual_binding,name,actual_args,private,*,operation_key):
        assert operation_key==key and private['ticket']['operation']==key
        return dict(key=heads[0][0],size=len(IMAGE),sha256=digest(IMAGE),native_version='etag')
    controller._head=head;controller._ticket=ticket;controller._phase=phase
    assert (await controller._archive(binding,record,page=page))['native_version']=='etag'


@pytest.mark.asyncio
async def test_large_original_page_releases_chunks_and_keeps_proof():
    controller=object.__new__(OriginalIntakeController)
    controller._page_chunks={};controller._page_proofs={}
    receipt=SimpleNamespace(source_id=SOURCE,page_count=139,sha256=digest(PDF),parser_sha256='parser')
    cursor=dict(block=0,offset=0,text=0)
    native='page text'
    info=dict(original_length=len(native),block_count=0,
        native_text_sha256=digest(native.encode()),native_blocks_sha256=digest(canonical([])),
        encoding_notice=None,offset=0,end=len(native),complete=True)
    page=dict(format='pdf',page_count=receipt.page_count,physical_page_index=0,width=40,height=50,
        page_id=str(uuid5(UUID(SOURCE),'page:1:0')),original_sha256=receipt.sha256,
        parser_sha256=receipt.parser_sha256,image_size=len(IMAGE),image_sha256=digest(IMAGE),
        mime_type='image/jpeg',preview_render=False,native_cursor=cursor,next_native=None,
        native_text=native,native_blocks=[],native_text_info=info)
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


@pytest.mark.asyncio
@pytest.mark.parametrize('case',[
    'native_error','missing_result','old_success','synthetic_expiry',
    'missing_terminal','terminal_identity','terminal_provider','terminal_version','terminal_nonce',
    'conflict','pending_spec','pending_receipt','stale_binding','cas',
    'empty_identity','object_identity','missing_identity','missing_args','missing_spec','missing_receipt',
])
async def test_verified_restore_rebinds_only_after_native_terminal(owner,native,monkeypatch,caplog,case):
    """Actual queue/native restore and successor dispatch; all bytes are synthetic."""
    from types import MappingProxyType
    from regional_knowledge import kaggle_provider_terminal as terminal
    from regional_knowledge.kaggle_readiness import SearchRuntimeExpectation
    e,n=owner,native
    state=SimpleNamespace(run=e.run,token=e.token,generation=e.generation,
        binding=current_binding(e.q),current=True)
    current=lambda b:state.current and b==state.binding
    spool=ControlSpool(e.q,e.auth.p.registry,e.j,HandlerRegistry([handler()]))
    controller=OriginalIntakeController(queue=e.q,spool=spool,participants=e.auth.p.registry,
        object_store=SimpleNamespace(client=e.c,bucket=e.s.bucket),binding_current=current,
        current_binding=lambda:state.binding,allowed_origins=frozenset({'https://archive.invalid'}))
    legacy_s3_original_fixture(controller)
    def bridge():
        with e.q.connect(write=False) as db:
            run=db.execute('SELECT * FROM runs WHERE id=?',(state.run,)).fetchone()
        value=RecoveryPhaseDispatch(e.q,database=SimpleNamespace(database_generation='synthetic-db',writer_generation='writer-1'),
            identity=(state.run,run['launch_nonce'],state.binding.runtime_incarnation),binding_current=current)
        n.handler.identity=value.identity;n.handler.database=value.database
        n.handler.current=lambda b:current(state.binding) and b==(state.run,state.binding.runtime_incarnation,state.generation)
        n.handler.attached(state.generation)
        return value
    state.bridge=bridge()
    async def upload(ticket,path,*,operation_key):
        e.c.objects[ticket.key]=(path.read_bytes(),'native-etag');return 'native-etag'
    async def download(ticket,path,*,operation_key):
        path.write_bytes(e.c.objects[ticket.key][0]);return path
    n.transfer.upload,n.transfer.download=upload,download
    e.c.generate_presigned_url=lambda *a,**k:'https://archive.invalid/synthetic-ticket'
    def head(**kw):
        if kw['Key'] not in e.c.objects:raise BotoFailure('NoSuchKey',404)
        data,version=e.c.objects[kw['Key']]
        return dict(ContentLength=len(data),ETag=version)
    e.c.head_object=head
    stop=asyncio.Event()
    async def peer():
        while not stop.is_set():
            work=await e.a.next_channel_work(state.run,state.token,state.generation)
            if work is None:await asyncio.sleep(.001);continue
            e.a.mark_work_sent(work)
            f=dict(type='DISPATCH',protocol_version=2,connection_generation=state.generation,
                **dict(work.document_payload),compute_claim=work.compute_claim,dispatch_generation=work.dispatch_generation)
            receipt=await n.handler(f)
            await state.bridge.receive(dict(type='RECOVERY_PHASE_RESULT',job_id=f['id'],claim=f['claim'],receipt=receipt,
                **{k:f[k] for k in ('run_id','launch_nonce','runtime_incarnation','compute_claim','dispatch_generation')}),
                run_id=state.run,token=state.token,generation=state.generation)
            r=work.resource
            await e.a.resource_released(state.run,state.token,state.generation,runtime_incarnation=r.runtime_incarnation,
                resource_kind='document',resource_id=r.resource_id,compute_claim=r.compute_claim,dispatch_generation=r.dispatch_generation)
    task=asyncio.create_task(peer())
    try:
        status=await controller.begin(e.auth.p.admin,operation_id='restore-fixture',source_id=SOURCE,ingestion_id=INGEST,
            file=ChatFile(download_url='https://attachment.invalid/private',file_id='fixture',mime_type='application/pdf'),max_bytes=len(PDF))
        assert status.verified
        with e.q.connect() as db:
            db.execute('UPDATE rkb_original_intakes SET telegram_locator_json=NULL WHERE ingestion_id=?',(INGEST,))
        row=controller._row(e.auth.p.owner.subject,INGEST);original=dict(row)
        old_record=json.loads(row['acquisition_json']);parser=n.handler.parser
        async def unavailable(*a,**kw):raise OriginalIntakeError('original_parser_unavailable')
        n.handler._records.clear()
        if case=='old_success':
            phase=controller._phase
            async def lost_success(*a,**kw):
                await phase(*a,**kw)
                raise OriginalIntakeError('synthetic_lost_delivery')
            monkeypatch.setattr(controller,'_phase',lost_success)
        else:n.handler.parser=unavailable
        with pytest.raises(OriginalIntakeError,match='synthetic_lost_delivery|original_native_receipt_unavailable'):
            await controller._restore_handle(e.auth.p.admin,e.auth.p.owner.subject,state.binding,status.receipt,old_record)
        pending=controller._row(e.auth.p.owner.subject,INGEST)['restore_json'];args=json.loads(pending)['args']
        operation=digest(canonical(dict(phase='original_acquire',args=args)))
        with e.q.connect(write=False) as db:
            old_job=dict(db.execute('SELECT * FROM jobs WHERE actor=? AND idempotency=?',
                ('maintenance:'+state.run,'recovery:'+operation)).fetchone())
        value=json.loads(old_job['result'])['value']
        if case=='old_success':
            assert value['restored_from_receipt_sha256']==status.receipt.receipt_sha256
            monkeypatch.setattr(controller,'_phase',phase)
        else:assert value=={'error':'original_parser_unavailable'}
        stop.set();await task
        await state.bridge.aclose();await e.w.retire('writer-1')
        with e.q.connect(write=False) as db:old_run=dict(db.execute('SELECT * FROM runs WHERE id=?',(state.run,)).fetchone())
        e.q.clock=lambda:1001.
        proof=terminal._NativeTerminalProof(MappingProxyType({k:old_run[k] for k in terminal._SNAPSHOT}),
            1001.,1001.,'COMPLETE',_seal=terminal._OBSERVED)
        assert e.q.retire_provider_reservation(proof)
        e.q.clock=lambda:4000.
        e.q.maintenance();state.run=e.q.request_startup()['run_id']
        assert state.run!=e.run
        nonce=e.q.prepare_channel(state.run)
        expected=SearchRuntimeExpectation({key:'a'*64 for key in ('code','model','launch_config','executed_source','dependency','schema')},'synthetic-db',0)
        e.q.expect_search_runtime(state.run,expected)
        state.token=e.q.launch_claim(state.run);e.q.launch_record(state.run,'owner/stable-slug',provider_version=89)
        state.generation=e.q.attach_channel(state.run,state.token,nonce,'successor-incarnation')['connection_generation']
        observed=dict(hashes=dict(expected.hashes),database_generation='synthetic-db',schema_version=expected.schema_version,
            applied_seq=0,projection_seq=0,visibility_epoch=0,text_search_ready=True,dense_search_ready=True,
            archive_read_ready=None,proof_ready=None)
        assert e.q.accept_search_runtime(state.run,state.token,state.generation,observed).ok
        e.q.heartbeat(state.run,state.token,ready=True,connection_generation=state.generation)
        await e.a.resource_state(state.run,state.token,state.generation,runtime_incarnation='successor-incarnation',state='idle')
        state.binding=current_binding(e.q)
        # A successor is a fresh native process, with no old operation cache.
        old_native=n.handler;workspace=old_native.root.with_name('successor');workspace.mkdir(mode=0o700)
        await old_native.aclose()
        n.handler=type(old_native)(config=old_native.config,database=old_native.database,identity=old_native.identity,
            workspace=workspace,current=lambda _:False,downloader=n.download,parser=parser,transfer=n.transfer)
        state.bridge=bridge()
        with e.q.connect() as db:
            if case=='missing_terminal':db.execute('DELETE FROM bge_provider_terminal_receipts WHERE run_id=?',(e.run,))
            elif case.startswith('terminal_'):
                changed=proof.receipt()
                key={'terminal_identity':'runtime_incarnation','terminal_provider':'provider_ref',
                    'terminal_version':'provider_version','terminal_nonce':'launch_nonce'}[case]
                changed['identity'][key]=90 if key=='provider_version' else 'different-identity'
                db.execute('UPDATE bge_provider_terminal_receipts SET receipt_json=? WHERE run_id=?',(canonical(changed).decode(),e.run))
            elif case=='conflict':db.execute('UPDATE runs SET attachment_conflict=1 WHERE id=?',(e.run,))
            elif case=='synthetic_expiry':
                db.execute('UPDATE jobs SET result=? WHERE id=?',
                    (canonical({'value':{'error':'run_expired'}}).decode(),old_job['id']))
            elif case=='missing_result':db.execute('DELETE FROM jobs WHERE id=?',(old_job['id'],))
            elif case in ('pending_spec','pending_receipt','empty_identity','object_identity',
                    'missing_identity','missing_args','missing_spec','missing_receipt'):
                changed=json.loads(pending)
                if case=='pending_spec':changed['args']['spec']['file_id']='different-original'
                elif case=='pending_receipt':changed['args']['restore_receipt']['native_version']='different-version'
                elif case=='empty_identity':changed['identity']=[]
                elif case=='object_identity':changed['identity']={}
                elif case=='missing_identity':del changed['identity']
                elif case=='missing_args':del changed['args']
                elif case=='missing_spec':del changed['args']['spec']
                else:del changed['args']['restore_receipt']
                pending=canonical(changed).decode()
                db.execute('UPDATE rkb_original_intakes SET restore_json=? WHERE ingestion_id=?',(pending,INGEST))
        with e.q.connect(write=False) as db:
            retained=db.execute('SELECT * FROM jobs WHERE id=?',(old_job['id'],)).fetchone()
            retained=None if retained is None else dict(retained)
        if case=='stale_binding':state.current=False
        if case=='cas':
            head=controller._head
            async def changed_pending(*a,**kw):
                result=await head(*a,**kw)
                with e.q.connect() as db:
                    other=json.loads(pending);other['args']['restore_nonce']='f'*48
                    db.execute('UPDATE rkb_original_intakes SET restore_json=? WHERE ingestion_id=?',(canonical(other).decode(),INGEST))
                return result
            monkeypatch.setattr(controller,'_head',changed_pending)
        stop.clear();task=asyncio.create_task(peer())
        if case not in ('native_error','missing_result','old_success','synthetic_expiry'):
            with pytest.raises(OriginalIntakeError,match='original_restore_unknown_requires_reconciliation'):
                await controller._restore_handle(e.auth.p.admin,e.auth.p.owner.subject,state.binding,status.receipt,old_record)
            with e.q.connect(write=False) as db:
                assert db.execute('SELECT count(*) FROM jobs WHERE actor=?',('maintenance:'+state.run,)).fetchone()[0]==0
            latest=controller._row(e.auth.p.owner.subject,INGEST)
            assert latest['acquisition_json']==original['acquisition_json'] and latest['receipt_json']==original['receipt_json']
            assert json.loads(latest['restore_json']).get('identity')==json.loads(pending).get('identity')
            if case!='cas':assert latest['restore_json']==pending
        else:
            restored=await controller._restore_handle(e.auth.p.admin,e.auth.p.owner.subject,state.binding,status.receipt,old_record)
            assert restored['handle']!=old_record['handle']
            assert (await controller.pages(e.auth.p.admin,INGEST)).pages[0].data==IMAGE
            latest=controller._row(e.auth.p.owner.subject,INGEST)
            assert latest['restore_json'] is None and latest['receipt_json']==original['receipt_json']
            with e.q.connect(write=False) as db:
                old=db.execute('SELECT * FROM jobs WHERE id=?',(old_job['id'],)).fetchone()
                assert (None if old is None else dict(old))==retained
                successor=dict(db.execute('SELECT * FROM jobs WHERE actor=? AND idempotency=?',
                    ('maintenance:'+state.run,'recovery:'+operation)).fetchone())
            before,after=json.loads(old_job['identity']),json.loads(successor['identity'])
            for key in ('arguments','operation_key','request_sha256'):assert before[key]==after[key]
            assert json.loads(successor['result'])['value']['restored_from_receipt_sha256']==status.receipt.receipt_sha256
            assert n.download.calls==1  # successor GETs the verified object, never a new attachment.
        events=[record.getMessage() for record in caplog.records
            if record.name=='regional_knowledge.original_intake_controller.restore']
        assert events==([] if case not in ('native_error','missing_result','old_success','synthetic_expiry') else [
            f'original_restore state=terminal_rebound old_run_id={e.run} run_id={state.run} reason=verified_original_restore'])
    finally:
        stop.set();await task;await state.bridge.aclose();spool.close();await n.handler.aclose()
