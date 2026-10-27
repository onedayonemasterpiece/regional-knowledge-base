"""Existing-owner original workflow; Dev handles metadata and scoped tickets only."""
from __future__ import annotations

import asyncio
import json
import logging
import secrets
import time
from uuid import UUID,uuid5

from .backend import RenderedPage,RenderedPageBatch
from .kaggle_original_parser import MODEL_PAGE_BYTE_LIMIT
from .contracts import ChatFile
from .telegram_original import TelegramOriginalLocator
from .kaggle_recovery_dispatch import RecoveryPhaseDispatch
from .native_snapshot_transfer import SnapshotTicket,origin
from .original_intake import (OriginalIntakeSpec,OriginalIntakeStatus,VerifiedOriginalReceipt,
    OriginalIntakeError,OriginalIntakeRejected,TERMINAL_REJECTIONS,NativePagesProof,PHASES,canonical,digest,text,uuid_value,
    decode_page_cursor,encode_page_cursor,assemble_native_page,issued_image_receipt)
from .recovery_s3 import _blocking,_missing

_LOG=logging.getLogger(__name__)
_RESTORE_LOG=logging.getLogger(__name__+'.restore')
_RESTORE_LOG.setLevel(logging.INFO)


class OriginalIntakeController:
    def __init__(self,*,queue,spool,participants,object_store,binding_current,current_binding,
                 allowed_origins,max_bytes=128*1024*1024):
        from .dataset_recovery_store import DatasetRecoveryStore
        recovery=spool.journal.store
        if type(recovery) is DatasetRecoveryStore:
            same_storage=(recovery.queue is queue and getattr(recovery.writer_guard,'queue',None) is queue)
        else:
            same_storage=(object_store.client is getattr(recovery,'client',None)
                and object_store.bucket==getattr(recovery,'bucket',None))
        if (spool.queue is not queue or spool.participants is not participants
                or not same_storage
                or not callable(binding_current) or not callable(current_binding)
                or type(allowed_origins) is not frozenset or not 1<=len(allowed_origins)<=4
                or any(origin(s)!=s for s in allowed_origins)
                or type(max_bytes) is not int or not 1<=max_bytes<=512*1024*1024):
            raise TypeError('same configured original intake owners required')
        self.queue,self.spool,self.participants=queue,spool,participants
        self.store,self.binding_current,self.current_binding=object_store,binding_current,current_binding
        self.allowed_origins,self.max_bytes=allowed_origins,max_bytes
        self._private={};self._lock=asyncio.Lock();self._page_proofs={};self._page_chunks={}
        self._storage=digest(canonical([self.store.client.meta.endpoint_url,self.store.bucket]))
        with queue.connect() as db:
            # Only bounded image identity metadata, not page text or pixels.
            # It survives controller restart so stage retry needs no re-render.
            db.execute('''CREATE TABLE IF NOT EXISTS rkb_original_image_issuance(
                ingestion_id TEXT NOT NULL,physical_page_index INTEGER NOT NULL,
                receipt_json TEXT NOT NULL,PRIMARY KEY(ingestion_id,physical_page_index))''')
            db.execute('''CREATE TABLE IF NOT EXISTS rkb_original_intakes(
                ingestion_id TEXT PRIMARY KEY,actor TEXT NOT NULL,operation_id TEXT NOT NULL,
                spec_json TEXT NOT NULL,storage_sha256 TEXT NOT NULL,acquisition_json TEXT,receipt_json TEXT,
                UNIQUE(actor,operation_id))''')
            if 'failure_code' not in {r['name'] for r in db.execute('PRAGMA table_info(rkb_original_intakes)')}:
                db.execute('ALTER TABLE rkb_original_intakes ADD COLUMN failure_code TEXT')
            if 'telegram_locator_json' not in {r['name'] for r in db.execute('PRAGMA table_info(rkb_original_intakes)')}:
                db.execute('ALTER TABLE rkb_original_intakes ADD COLUMN telegram_locator_json TEXT')
            if 'restore_json' not in {r['name'] for r in db.execute('PRAGMA table_info(rkb_original_intakes)')}:
                db.execute('ALTER TABLE rkb_original_intakes ADD COLUMN restore_json TEXT')

    async def _authorize(self,context):
        actor=await self.spool._authorize(context)
        binding=await asyncio.to_thread(self.current_binding)
        if not self.binding_current(binding):raise OriginalIntakeError('original_current_binding_required')
        return actor,binding

    def _row(self,actor,ingestion_id):
        uuid_value(ingestion_id)
        with self.queue.connect(write=False) as db:
            row=db.execute('SELECT * FROM rkb_original_intakes WHERE ingestion_id=? AND actor=?',(ingestion_id,actor)).fetchone()
            if row is None or row['storage_sha256']!=self._storage:raise OriginalIntakeError('original_owned_ingestion_required')
            return dict(row)

    def _bridge(self,binding):
        with self.queue.connect(write=False) as db:
            run=db.execute('SELECT * FROM runs WHERE id=?',(binding.run_id,)).fetchone()
            if run is None:raise OriginalIntakeError('original_runtime_unavailable')
            identity=(binding.run_id,run['launch_nonce'],binding.runtime_incarnation)
        bridge=getattr(self.queue,'_recovery_phases',None)
        if type(bridge) is not RecoveryPhaseDispatch or bridge.identity!=identity or not bridge.current(binding):
            raise OriginalIntakeError('original_maintenance_binding_changed')
        previous=getattr(bridge,'original_intake',None)
        if previous is not None and previous is not self:raise OriginalIntakeError('original_maintenance_owner_changed')
        bridge.original_intake=self
        return bridge

    async def dispatch_transfer(self,identity,binding):
        if identity['phase'] not in PHASES or not self.binding_current(binding):raise PermissionError('original transfer binding unavailable')
        if identity['phase']=='original_pages':return {}
        if identity['phase']=='original_archive' and 'page_index' not in identity['arguments']:
            return None
        if identity['phase']=='original_acquire' and set(identity['arguments'])=={'spec','telegram_locator'}:
            # The immutable document locator is already in the request hash;
            # no ephemeral URL/session or separate private transfer is needed.
            TelegramOriginalLocator.from_wire(identity['arguments']['telegram_locator'])
            return None
        private=self._private.get(identity['operation_key'])
        if private is None:raise OriginalIntakeError('original_ephemeral_ticket_requires_retry')
        return private

    async def _phase(self,binding,phase,args,private=None,*,operation_key=None):
        key=operation_key or digest(canonical(dict(phase=phase,args=args)))
        if private is not None:self._private[key]=private
        value=await self._bridge(binding).request(binding,phase,args,operation_key=key)
        self._private.pop(key,None)
        if type(value) is not dict:raise OriginalIntakeError('original_native_receipt_invalid')
        if 'error' in value:
            code=value.get('error')
            if value.get('terminal') is True and code in TERMINAL_REJECTIONS:raise OriginalIntakeRejected(code)
            if code in ('original_retained_bytes_unavailable','original_djvu_decoder_required','original_telegram_binding_required'):
                raise OriginalIntakeError(code)
            raise OriginalIntakeError('original_native_receipt_unavailable')
        return value

    async def _head(self,key,size,version=None):
        def read():
            try:result=self.store.client.head_object(Bucket=self.store.bucket,Key=key)
            except Exception as exc:
                if _missing(exc):return None
                raise OriginalIntakeError('original_storage_head_unknown') from None
            etag=result.get('ETag');length=result.get('ContentLength')
            if (type(length) is not int or (length!=size if size is not None else not 1<=length<=MODEL_PAGE_BYTE_LIMIT)
                    or type(etag) is not str or not 1<=len(etag)<=512):
                raise OriginalIntakeError('original_storage_receipt_unverified')
            if version is not None and etag!=version:raise OriginalIntakeError('original_storage_version_changed')
            return (etag,length) if size is None else etag
        return await _blocking(read)

    async def _ticket(self,binding,operation,method,key,size,sha256,version=None):
        if not self.binding_current(binding):raise OriginalIntakeError('original_current_binding_required')
        params=dict(Bucket=self.store.bucket,Key=key)
        headers={'Content-Length':str(size),'Content-Type':'application/octet-stream','If-None-Match':'*'}
        if method=='PUT':params.update(ContentLength=size,ContentType='application/octet-stream',IfNoneMatch='*')
        else:params['IfMatch']=version;headers={'If-Match':version}
        url=await _blocking(lambda:self.store.client.generate_presigned_url('put_object' if method=='PUT' else 'get_object',
            Params=params,ExpiresIn=120,HttpMethod=method))
        if not self.binding_current(binding) or origin(url) not in self.allowed_origins:
            raise OriginalIntakeError('original_ticket_origin_or_generation')
        return SnapshotTicket(method,digest(operation.encode()),key,size,sha256,time.time()+120,url,tuple(headers.items())).wire()

    async def _archive(self,binding,record,*,page=None):
        args=dict(handle=record['handle'],original_sha256=record['sha256'])
        if page is None:
            receipt=await self._phase(binding,'original_archive',args)
            locator=TelegramOriginalLocator.from_wire(receipt['telegram_locator'])
            if ((locator.sha256,locator.size)!=(record['sha256'],record['size'])
                    or receipt.get('key')!='telegram/'+digest(canonical(locator.wire()))
                    or receipt.get('acquisition_sha256')!=record['acquisition_sha256']):
                raise OriginalIntakeError('original_telegram_archive_receipt_changed')
            return receipt
        sha256,size=record['sha256'],record['size']
        key='originals/v1/'+digest(self.spool.journal.stream.encode())+'/'+record['spec']['source_id']+'/'+sha256
        if page is not None:
            args['page_index']=page['physical_page_index'];sha256,size=page['image_sha256'],page['image_size']
            if type(size) is not int or not 1<=size<=MODEL_PAGE_BYTE_LIMIT:raise OriginalIntakeRejected('original_render_byte_bound')
            key+='/pages/'+str(args['page_index'])+'/'+sha256
        # HEAD resolves a lost PUT ACK before issuing another conditional PUT.
        version=await self._head(key,size)
        if version is None:
            operation=digest(canonical(dict(phase='original_archive',args=args)))
            # Only a settled transfer failure plus a missing object permits a
            # fresh conditional PUT. Pending/unknown work retains its key.
            root=operation
            with self.queue.connect(write=False) as db:
                for attempt in range(8):
                    operation=root if attempt==0 else root+':upload-retry:'+str(attempt)
                    row=db.execute('SELECT state,result FROM jobs WHERE actor=? AND idempotency=?',
                        ('maintenance:'+binding.run_id,'recovery:'+operation)).fetchone()
                    if row is None or row['state']!='done':break
                    value=json.loads(row['result'])['value']
                    if (value.get('error'),value.get('error_class'))!=('original_intake_unavailable','SnapshotTransferError'):break
                else:raise OriginalIntakeError('original_archive_retry_bound')
            if attempt:_LOG.info('original_archive state=retry run_id=%s attempt=%s operation_sha256=%s',binding.run_id,attempt,digest(operation.encode()))
            ticket=await self._ticket(binding,operation,'PUT',key,size,sha256)
            receipt=await self._phase(binding,'original_archive',args,{'ticket':ticket},operation_key=operation)
            if (receipt.get('key'),receipt.get('size'),receipt.get('sha256'))!=(key,size,sha256):
                raise OriginalIntakeError('original_archive_receipt_changed')
            version=await self._head(key,size,receipt.get('native_version'))
        if page is not None:return dict(key=key,native_version=version)
        operation=digest(canonical(dict(phase='original_verify',args=args)))
        ticket=await self._ticket(binding,operation,'GET',key,size,sha256,version)
        receipt=await self._phase(binding,'original_verify',args,{'ticket':ticket})
        if ((receipt.get('key'),receipt.get('size'),receipt.get('sha256'),receipt.get('native_version'))!=(key,size,sha256,version)
                or receipt.get('acquisition_sha256')!=record['acquisition_sha256']):
            raise OriginalIntakeError('original_verification_receipt_changed')
        if await self._head(key,size,version) is None:raise OriginalIntakeError('original_verified_object_missing')
        return receipt

    async def begin(self,context,*,operation_id,source_id,ingestion_id,file,max_bytes=128*1024*1024):
        actor,binding=await self._authorize(context)
        text(operation_id);uuid_value(source_id);uuid_value(ingestion_id)
        telegram=type(file) is TelegramOriginalLocator
        if not isinstance(file,ChatFile) and not telegram:raise OriginalIntakeError('original_chat_file_required')
        locator_json=canonical(file.wire()).decode() if telegram else None
        if type(max_bytes) is not int or not 1<=max_bytes<=self.max_bytes:raise OriginalIntakeError('original_byte_bound')
        # The caller may now offer a larger size/page budget, but it must
        # never mutate an already persisted operation's canonical input.
        # Historical 2MiB/32-page jobs remain valid exact replay inputs.
        request_id=str(uuid5(UUID(ingestion_id),'original:'+operation_id))
        spec=OriginalIntakeSpec(source_id,ingestion_id,request_id,file.file_id,
            file_name=file.file_name,mime_type=file.mime_type,max_bytes=max_bytes,
            **(dict(expected_sha256=file.sha256,expected_size=file.size) if telegram else {}))
        async with self._lock:
            with self.queue.connect() as db:
                prior=db.execute('SELECT * FROM rkb_original_intakes WHERE ingestion_id=?',(ingestion_id,)).fetchone()
                if prior is not None:
                    stored=OriginalIntakeSpec.from_wire(json.loads(prior['spec_json']))
                    if (prior['actor']!=actor or prior['operation_id']!=operation_id
                            or prior['storage_sha256']!=self._storage
                            or telegram and prior['telegram_locator_json']!=locator_json
                            or telegram and (stored.expected_sha256,stored.expected_size)!=(spec.expected_sha256,spec.expected_size)
                            or (stored.source_id,stored.ingestion_id,stored.request_id,stored.file_id,
                                stored.file_name,stored.mime_type)!=(source_id,ingestion_id,request_id,
                                    file.file_id,file.file_name,file.mime_type)
                            or stored.max_bytes>self.max_bytes):
                        raise OriginalIntakeError('original_operation_changed')
                    spec=stored
                else:
                    db.execute('INSERT INTO rkb_original_intakes(ingestion_id,actor,operation_id,spec_json,storage_sha256,telegram_locator_json) VALUES(?,?,?,?,?,?)',
                        (ingestion_id,actor,operation_id,canonical(spec.wire()).decode(),self._storage,locator_json))
            row=self._row(actor,ingestion_id)
            try:
                if row['failure_code'] is not None:return OriginalIntakeStatus(ingestion_id,'rejected',reason=row['failure_code'])
                if row['receipt_json'] is not None:return await self.status(context,ingestion_id)
                if row['acquisition_json'] is None:
                    args={'spec':spec.wire()}
                    if telegram:args['telegram_locator']=file.wire()
                    record=await self._phase(binding,'original_acquire',args,
                        None if telegram else {'download_url':str(file.download_url)})
                    if record.get('spec')!=spec.wire():raise OriginalIntakeError('original_acquisition_changed')
                    with self.queue.connect() as db:db.execute('UPDATE rkb_original_intakes SET acquisition_json=? WHERE ingestion_id=?',
                        (canonical(record).decode(),ingestion_id))
                else:record=json.loads(row['acquisition_json'])
                receipt=await self._complete(context,binding,record)
                return OriginalIntakeStatus(ingestion_id,'verified',receipt)
            except asyncio.CancelledError:raise
            except OriginalIntakeRejected as exc:return self._reject(ingestion_id,exc.code)
            except OriginalIntakeError as exc:
                if str(exc)=='original_telegram_binding_required':
                    return OriginalIntakeStatus(ingestion_id,'pending',reason=str(exc))
                return OriginalIntakeStatus(ingestion_id,'pending',reason='original_intake_unknown_retry_same_operation')
            except Exception:
                _LOG.warning('original_intake state=pending run_id=%s ingestion_id=%s reason=receipt_unresolved',binding.run_id,ingestion_id)
                return OriginalIntakeStatus(ingestion_id,'pending',reason='original_intake_unknown_retry_same_operation')

    def _reject(self,ingestion_id,code):
        if code not in TERMINAL_REJECTIONS:raise OriginalIntakeError('original_rejection_invalid')
        with self.queue.connect() as db:db.execute('UPDATE rkb_original_intakes SET failure_code=? WHERE ingestion_id=?',(code,ingestion_id))
        return OriginalIntakeStatus(ingestion_id,'rejected',reason=code)

    async def _complete(self,context,binding,record):
        native=await self._archive(binding,record);spec=OriginalIntakeSpec.from_wire(record['spec'])
        receipt=VerifiedOriginalReceipt(spec.source_id,spec.ingestion_id,spec.request_id,1,record['sha256'],record['size'],
            native['key'],native['native_version'],record['format'],record['page_count'],record['parser_sha256'],
            record['acquisition_sha256'],native['native_verification_sha256'])
        await self._authorize(context)
        with self.queue.connect() as db:db.execute(
            'UPDATE rkb_original_intakes SET receipt_json=?,telegram_locator_json=COALESCE(telegram_locator_json,?) WHERE ingestion_id=?',
            (canonical(receipt.wire()).decode(),canonical(native['telegram_locator']).decode(),spec.ingestion_id))
        _LOG.info('original_intake state=verified run_id=%s ingestion_id=%s',binding.run_id,spec.ingestion_id)
        return receipt

    async def verified(self,context,ingestion_id):
        actor,binding=await self._authorize(context);row=self._row(actor,ingestion_id)
        if row['receipt_json'] is None:return None
        receipt=VerifiedOriginalReceipt.from_wire(json.loads(row['receipt_json']))
        if receipt.key.startswith('telegram/'):
            locator=TelegramOriginalLocator.from_wire(json.loads(row['telegram_locator_json']))
            if (receipt.key!='telegram/'+digest(canonical(locator.wire()))
                    or (receipt.sha256,receipt.size)!=(locator.sha256,locator.size)):
                raise OriginalIntakeError('original_telegram_receipt_changed')
        elif await self._head(receipt.key,receipt.size,receipt.native_version) is None:
            raise OriginalIntakeError('original_verified_object_missing')
        if not self.binding_current(binding):raise OriginalIntakeError('original_current_binding_required')
        return receipt

    async def status(self,context,ingestion_id):
        actor,binding=await self._authorize(context);row=self._row(actor,ingestion_id)
        if row['failure_code'] is not None:return OriginalIntakeStatus(ingestion_id,'rejected',reason=row['failure_code'])
        receipt=await self.verified(context,ingestion_id)
        if receipt is not None:
            if row['telegram_locator_json'] is not None:
                return OriginalIntakeStatus(ingestion_id,'verified',receipt)
            return OriginalIntakeStatus(ingestion_id,'pending',reason='original_telegram_archive_pending')
        try:
            record=json.loads(row['acquisition_json']) if row['acquisition_json'] else None
            if record is None:
                spec=json.loads(row['spec_json']);args={'spec':spec}
                if row['telegram_locator_json'] is not None:
                    args['telegram_locator']=TelegramOriginalLocator.from_wire(json.loads(row['telegram_locator_json'])).wire()
                key=digest(canonical(dict(phase='original_acquire',args=args)))
                with self.queue.connect(write=False) as db:
                    job=db.execute('SELECT * FROM jobs WHERE actor=? AND idempotency=?',
                        ('maintenance:'+binding.run_id,'recovery:'+key)).fetchone()
                if job is None or job['state']!='done':
                    reason='original_acquisition_pending' if key in self._private else 'original_attachment_retry_same_file_required'
                    return OriginalIntakeStatus(ingestion_id,'pending',reason=reason)
                identity=json.loads(job['identity']);result=json.loads(job['result'])
                if (result.get('value',{}).get('terminal') is True
                        and result['value'].get('error') in TERMINAL_REJECTIONS):
                    return self._reject(ingestion_id,result['value']['error'])
                if (identity['phase']!='original_acquire' or identity['arguments']!=args
                        or result['request_sha256']!=identity['request_sha256'] or result['phase']!='original_acquire'
                        or result['value'].get('spec')!=spec):
                    raise OriginalIntakeError('original_acquisition_receipt_unavailable')
                record=result['value']
                with self.queue.connect() as db:db.execute('UPDATE rkb_original_intakes SET acquisition_json=? WHERE ingestion_id=?',
                    (canonical(record).decode(),ingestion_id))
            receipt=await self._complete(context,binding,record)
            return OriginalIntakeStatus(ingestion_id,'verified',receipt)
        except asyncio.CancelledError:raise
        except OriginalIntakeRejected as exc:return self._reject(ingestion_id,exc.code)
        except Exception:return OriginalIntakeStatus(ingestion_id,'pending',reason='original_intake_unknown_retry_same_operation')

    async def pages(self,context,ingestion_id,cursor=None,batch_size=1):
        actor,binding=await self._authorize(context);row=self._row(actor,ingestion_id)
        receipt=await self.verified(context,ingestion_id)
        if receipt is None:raise OriginalIntakeError('original_verified_receipt_required')
        if batch_size!=1:raise OriginalIntakeError('original_one_page_batch_required')
        index,native_cursor=decode_page_cursor(cursor)
        if index<0 or index>=receipt.page_count:raise OriginalIntakeError('original_page_cursor_invalid')
        record=json.loads(row['acquisition_json'])
        record,page,data=await self._page_data(binding,record,index,native_cursor,
            restore=lambda prior:self._restore_handle(context,actor,binding,receipt,prior))
        await self._authorize(context)
        expected_page=str(uuid5(UUID(receipt.source_id),f'page:1:{index}'))
        if (page.get('page_id')!=expected_page or page.get('original_sha256')!=receipt.sha256
                or page.get('parser_sha256')!=receipt.parser_sha256 or page.get('native_cursor')!=native_cursor
                or page.get('page_receipt_sha256')!=digest(canonical({k:v for k,v in page.items() if k!='page_receipt_sha256'}))):
            raise OriginalIntakeError('original_page_provenance_changed')
        if index not in self._page_proofs.get(ingestion_id,{}):
            chunks=self._page_chunks.setdefault(ingestion_id,{}).setdefault(index,{})
            part_key=canonical(native_cursor).decode();raw=canonical(page)
            if part_key in chunks and chunks[part_key]!=raw:
                self._page_proofs.get(ingestion_id,{}).pop(index,None)
                raise OriginalIntakeError('original_page_fragment_changed')
            chunks[part_key]=raw
            self._assemble_available(ingestion_id,index,chunks)
        issued=self._record_issued_image(receipt,page)
        next_cursor=(encode_page_cursor(index,page['next_native']) if page['next_native'] is not None
            else str(index+1) if index+1<receipt.page_count else None)
        return RenderedPageBatch((RenderedPage(page['page_id'],index,None,'image/jpeg',data,
            page['native_text'],tuple(page['native_blocks']),dict(page['native_text_info'],
                original_sha256=record['sha256'],page_receipt_sha256=page['page_receipt_sha256'],
                source_image_receipt_sha256=issued['source_image_receipt_sha256'],
                image_sha256=issued['image_sha256'])),),
            next_cursor)

    async def _page_data(self,binding,record,index,native_cursor,*,restore):
        args=dict(handle=record['handle'],original_sha256=record['sha256'],index=index,cursor=native_cursor)
        try:page=await self._phase(binding,'original_pages',args)
        except OriginalIntakeError as exc:
            if str(exc)!='original_retained_bytes_unavailable':raise
            record=await restore(record)
            page=await self._phase(binding,'original_pages',dict(args,handle=record['handle']))
        with self.queue.connect(write=False) as db:
            old=db.execute('SELECT receipt_json FROM rkb_original_image_issuance WHERE ingestion_id=? AND physical_page_index=?',
                (record['spec']['ingestion_id'],index)).fetchone()
        prior=json.loads(old['receipt_json']) if old else None
        if prior is None:native=await self._archive(binding,record,page=page)
        else:
            # Re-rendered metadata is not permission to replace an issued image.
            if any(prior.get(k)!=page.get(k) for k in ('page_id','original_sha256','parser_sha256','width','height')):
                raise OriginalIntakeError('original_issued_image_changed')
            key='originals/v1/'+digest(self.spool.journal.stream.encode())+'/'+record['spec']['source_id']+'/'+record['sha256']+'/pages/'+str(index)+'/'+prior['image_sha256']
            observed=await self._head(key,None)
            if observed is None:raise OriginalIntakeError('original_render_storage_invalid')
            version,length=observed;native=dict(key=key,native_version=version)
            page.update(image_size=length,image_sha256=prior['image_sha256'],preview_render=prior['preview_render'])
            if 'preview_render' in page['native_text_info']:page['native_text_info']['preview_render']=prior['preview_render']
            page['page_receipt_sha256']=digest(canonical({k:v for k,v in page.items() if k!='page_receipt_sha256'}))
        started=time.monotonic()
        def derivative():
            result=self.store.client.get_object(Bucket=self.store.bucket,Key=native['key'],IfMatch=native['native_version'])
            body=result['Body']
            try:
                if result.get('ContentLength')!=page['image_size'] or page['image_size']>MODEL_PAGE_BYTE_LIMIT:
                    raise OriginalIntakeError('original_render_storage_invalid')
                data=body.read(page['image_size']+1)
                if len(data)!=page['image_size'] or digest(data)!=page['image_sha256']:
                    raise OriginalIntakeError('original_render_storage_invalid')
                return data
            finally:body.close()
        data=await _blocking(derivative)
        _LOG.info('original_page_read stage=image_downloaded run_id=%s ingestion_id=%s page_index=%s image_bytes=%s elapsed_ms=%.3f',
            binding.run_id,record['spec']['ingestion_id'],index,len(data),(time.monotonic()-started)*1000)
        return record,page,data

    def _record_issued_image(self,receipt,page):
        issued=issued_image_receipt(receipt,page)
        raw=canonical(issued).decode()
        with self.queue.connect() as db:
            previous=db.execute('SELECT receipt_json FROM rkb_original_image_issuance '
                'WHERE ingestion_id=? AND physical_page_index=?',
                (receipt.ingestion_id,issued['physical_page_index'])).fetchone()
            if previous is not None and previous['receipt_json']!=raw:
                raise OriginalIntakeError('original_issued_image_changed')
            db.execute('INSERT OR IGNORE INTO rkb_original_image_issuance VALUES(?,?,?)',
                (receipt.ingestion_id,issued['physical_page_index'],raw))
        return issued

    async def selected_image_receipts(self,context,ingestion_id,selections):
        """Read only already issued pages; never download, render or scan a book."""
        actor,_=await self._authorize(context)
        self._row(actor,ingestion_id)
        receipt=await self.verified(context,ingestion_id)
        if receipt is None or type(selections) is not tuple or not 1<=len(selections)<=8:
            raise OriginalIntakeError('original_selected_image_bound')
        result=[];seen=set()
        with self.queue.connect(write=False) as db:
            for index,expected in selections:
                if type(index) is not int or not 0<=index<receipt.page_count or index in seen:
                    raise OriginalIntakeError('original_selected_image_bound')
                seen.add(index)
                row=db.execute('SELECT receipt_json FROM rkb_original_image_issuance '
                    'WHERE ingestion_id=? AND physical_page_index=?',(ingestion_id,index)).fetchone()
                if row is None:raise OriginalIntakeError('original_source_image_not_issued')
                value=json.loads(row['receipt_json'])
                if (value!=issued_image_receipt(receipt,value)
                        or (expected is not None and value['source_image_receipt_sha256']!=expected)):
                    raise OriginalIntakeError('original_source_image_receipt_changed')
                result.append(value)
        await self._authorize(context)
        return tuple(result)

    def _assemble_available(self,ingestion_id,index,chunks):
        cursor=dict(block=0,offset=0,text=0);parts=[];seen=set()
        for _ in range(4096):
            key=canonical(cursor).decode()
            if key not in chunks:return
            if key in seen:raise OriginalIntakeError('original_page_cursor_did_not_advance')
            seen.add(key);part=json.loads(chunks[key]);parts.append(part);cursor=part['next_native']
            if cursor is None:
                self._page_proofs.setdefault(ingestion_id,{})[index]=canonical(assemble_native_page(parts))
                del self._page_chunks[ingestion_id][index]
                if not self._page_chunks[ingestion_id]:
                    del self._page_chunks[ingestion_id]
                return
        raise OriginalIntakeError('original_page_continuation_bound')

    @staticmethod
    def _restore_terminal(db,identity):
        """Persisted native proof, never a lease expiry or STOP acknowledgement."""
        from .kaggle_provider_terminal import CONTRACT
        try:
            prior,incarnation=identity
            run=db.execute('SELECT r.*,t.receipt_json FROM runs r JOIN bge_provider_terminal_receipts t '
                "ON t.run_id=r.id WHERE r.id=? AND r.launch_state='terminal'",(prior,)).fetchone()
            if run is None:return False
            proof=json.loads(run['receipt_json']);sealed=proof['identity']
            return (proof['contract']==CONTRACT and proof['status'] in ('COMPLETE','ERROR','CANCEL_ACKNOWLEDGED')
                and sealed['id']==prior and sealed['runtime_incarnation']==incarnation==run['runtime_incarnation']
                and all(sealed[k]==run[k] for k in ('provider_ref','provider_version','launch_nonce'))
                and not run['attachment_conflict'] and not run['channel_active'])
        except (TypeError,ValueError,KeyError):return False

    async def _restore_handle(self,context,actor,binding,receipt,old_record,*,recheck=None):
        async with self._lock:
            row=self._row(actor,receipt.ingestion_id)
            if row['receipt_json'] is None or VerifiedOriginalReceipt.from_wire(json.loads(row['receipt_json']))!=receipt:
                raise OriginalIntakeError('original_verified_restore_receipt_required')
            fresh=json.loads(row['acquisition_json'])
            if fresh['handle']!=old_record['handle']:return fresh
            if await self._head(receipt.key,receipt.size,receipt.native_version) is None:
                raise OriginalIntakeError('original_verified_object_missing')
            pending=json.loads(row['restore_json']) if row['restore_json'] else None
            # Reconnect may bind a fresh socket generation while the same
            # retained native transfer/result still owns its original claim.
            identity=[binding.run_id,binding.runtime_incarnation]
            if pending is not None and (type(pending) is not dict
                    or type(pending.get('identity')) not in (list,tuple) or len(pending['identity'])!=2
                    or any(type(v) is not str or not v for v in pending['identity']) or type(pending.get('args')) is not dict):
                raise OriginalIntakeError('original_restore_unknown_requires_reconciliation')
            if pending is None:
                pending=dict(identity=identity,args=dict(spec=json.loads(row['spec_json']),restore_receipt=receipt.wire(),
                    restore_nonce=secrets.token_hex(24)))
                with self.queue.connect() as db:db.execute('UPDATE rkb_original_intakes SET restore_json=? WHERE ingestion_id=?',
                    (canonical(pending).decode(),receipt.ingestion_id))
            elif pending['identity']!=identity:
                prior_run=pending['identity'][0]
                with self.queue.connect() as db:
                    if (prior_run==binding.run_id or not self.binding_current(binding)
                            or not self._restore_terminal(db,pending['identity'])
                            or canonical(pending['args'].get('spec'))!=canonical(json.loads(row['spec_json']))
                            or canonical(pending['args'].get('restore_receipt'))!=canonical(receipt.wire())):
                        raise OriginalIntakeError('original_restore_unknown_requires_reconciliation')
                    # Restore is a pinned GET/hash/parse into a local handle. Once the old
                    # execution is terminal, its lost/success/error job outcome is immaterial.
                    rebound=dict(pending,identity=identity)
                    changed=db.execute('UPDATE rkb_original_intakes SET restore_json=? '
                        'WHERE ingestion_id=? AND actor=? AND restore_json=? AND acquisition_json=? AND receipt_json=?',
                        (canonical(rebound).decode(),receipt.ingestion_id,actor,row['restore_json'],
                            row['acquisition_json'],row['receipt_json'])).rowcount
                    if changed!=1:raise OriginalIntakeError('original_restore_unknown_requires_reconciliation')
                    pending=rebound
                _RESTORE_LOG.info('original_restore state=terminal_rebound old_run_id=%s run_id=%s reason=verified_original_restore',prior_run,binding.run_id)
            args=pending['args'];operation=digest(canonical(dict(phase='original_acquire',args=args)))
            ticket=await self._ticket(binding,operation,'GET',receipt.key,receipt.size,receipt.sha256,receipt.native_version)
            restored=await self._phase(binding,'original_acquire',args,{'ticket':ticket})
            if (restored.get('spec')!=args['spec'] or restored.get('restored_from_receipt_sha256')!=receipt.receipt_sha256
                    or tuple(restored.get(k) for k in ('sha256','size','format','page_count','parser_sha256','acquisition_sha256'))!=(
                        receipt.sha256,receipt.size,receipt.format,receipt.page_count,receipt.parser_sha256,receipt.acquisition_sha256)):
                raise OriginalIntakeError('original_restore_receipt_changed')
            if recheck is None:await self._authorize(context)
            else:await recheck()
            with self.queue.connect() as db:db.execute('UPDATE rkb_original_intakes SET acquisition_json=?,restore_json=NULL WHERE ingestion_id=?',
                (canonical(restored).decode(),receipt.ingestion_id))
            return restored

    async def illustration_page(self,figure,binding):
        """Internal page renderer for the already authorized accepted-figure read.

        Whole-page pixels stay inside the crop callback. The stored importer
        selects the existing original; it never impersonates the requesting user.
        """
        async def recheck():
            if not self.binding_current(binding):
                raise OriginalIntakeError('original_current_binding_required')
        await recheck()
        with self.queue.connect(write=False) as db:
            row=db.execute('''SELECT * FROM rkb_original_intakes
                WHERE ingestion_id=? AND json_extract(spec_json,'$.source_id')=?
                AND receipt_json IS NOT NULL AND storage_sha256=?''',
                (figure['ingestion_id'],figure['source_id'],self._storage)).fetchone()
        if row is None:raise OriginalIntakeError('original_verified_receipt_required')
        receipt=VerifiedOriginalReceipt.from_wire(json.loads(row['receipt_json']))
        record=json.loads(row['acquisition_json'])
        _,_,data=await self._page_data(binding,record,figure['physical_page_index'],
            dict(block=0,offset=0,text=0),restore=lambda prior:self._restore_handle(
                None,row['actor'],binding,receipt,prior,recheck=recheck))
        return data

    async def native_pages(self,context,ingestion_id):
        receipt=await self.verified(context,ingestion_id)
        if receipt is None or receipt.page_count>32:raise OriginalIntakeError('original_bounded_page_proof_required')
        for index in range(receipt.page_count):
            if index not in self._page_proofs.get(ingestion_id,{}):
                cursor=str(index);seen=set()
                for _ in range(4096):
                    if cursor in seen:raise OriginalIntakeError('original_page_cursor_did_not_advance')
                    seen.add(cursor);batch=await self.pages(context,ingestion_id,cursor=cursor,batch_size=1)
                    cursor=batch.next_cursor
                    if index in self._page_proofs.get(ingestion_id,{}):break
                    if cursor is None or decode_page_cursor(cursor)[0]!=index:
                        raise OriginalIntakeError('original_page_continuation_required')
                else:raise OriginalIntakeError('original_page_continuation_bound')
        await self._authorize(context)
        return NativePagesProof(receipt.receipt_sha256,tuple(self._page_proofs[ingestion_id][i] for i in range(receipt.page_count)))
