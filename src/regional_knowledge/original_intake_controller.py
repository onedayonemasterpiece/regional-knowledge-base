"""Existing-owner original workflow; Dev handles metadata and scoped tickets only."""
from __future__ import annotations

import asyncio
import json
import logging
import time
from uuid import UUID,uuid5

from .backend import RenderedPage,RenderedPageBatch
from .contracts import ChatFile
from .kaggle_recovery_dispatch import RecoveryPhaseDispatch
from .native_snapshot_transfer import SnapshotTicket,origin
from .original_intake import (OriginalIntakeSpec,OriginalIntakeStatus,VerifiedOriginalReceipt,
    OriginalIntakeError,NativePagesProof,PHASES,canonical,digest,text,uuid_value)
from .recovery_s3 import _blocking

_LOG=logging.getLogger(__name__)


class OriginalIntakeController:
    def __init__(self,*,queue,spool,participants,object_store,binding_current,current_binding,
                 allowed_origins,max_bytes=128*1024*1024):
        if (spool.queue is not queue or spool.participants is not participants
                or object_store.client is not spool.journal.store.client or object_store.bucket!=spool.journal.store.bucket
                or not callable(binding_current) or not callable(current_binding)
                or type(allowed_origins) is not frozenset or not 1<=len(allowed_origins)<=4
                or any(origin(s)!=s for s in allowed_origins)
                or type(max_bytes) is not int or not 1<=max_bytes<=512*1024*1024):
            raise TypeError('same configured original intake owners required')
        self.queue,self.spool,self.participants=queue,spool,participants
        self.store,self.binding_current,self.current_binding=object_store,binding_current,current_binding
        self.allowed_origins,self.max_bytes=allowed_origins,max_bytes
        self._private={};self._lock=asyncio.Lock();self._page_proofs={}
        self._storage=digest(canonical([self.store.client.meta.endpoint_url,self.store.bucket]))
        with queue.connect() as db:
            db.execute('''CREATE TABLE IF NOT EXISTS rkb_original_intakes(
                ingestion_id TEXT PRIMARY KEY,actor TEXT NOT NULL,operation_id TEXT NOT NULL,
                spec_json TEXT NOT NULL,storage_sha256 TEXT NOT NULL,acquisition_json TEXT,receipt_json TEXT,
                UNIQUE(actor,operation_id))''')

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
        private=self._private.get(identity['operation_key'])
        if private is None:raise OriginalIntakeError('original_ephemeral_ticket_requires_retry')
        return private

    async def _phase(self,binding,phase,args,private=None):
        key=digest(canonical(dict(phase=phase,args=args)))
        if private is not None:self._private[key]=private
        value=await self._bridge(binding).request(binding,phase,args,operation_key=key)
        self._private.pop(key,None)
        if type(value) is not dict:raise OriginalIntakeError('original_native_receipt_invalid')
        if 'error' in value:raise OriginalIntakeError('original_native_receipt_unavailable')
        return value

    async def _head(self,key,size,version=None):
        def read():
            result=self.store.client.head_object(Bucket=self.store.bucket,Key=key)
            etag=result.get('ETag')
            if result.get('ContentLength')!=size or type(etag) is not str or not 1<=len(etag)<=512:
                raise OriginalIntakeError('original_storage_receipt_unverified')
            if version is not None and etag!=version:raise OriginalIntakeError('original_storage_version_changed')
            return etag
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
        sha256,size=record['sha256'],record['size']
        key='originals/v1/'+digest(self.spool.journal.stream.encode())+'/'+record['spec']['source_id']+'/'+sha256
        if page is not None:
            args['page_index']=page['physical_page_index'];sha256,size=page['image_sha256'],page['image_size']
            key+='/pages/'+str(args['page_index'])+'/'+sha256
        # HEAD resolves a lost PUT ACK before issuing another conditional PUT.
        try:version=await self._head(key,size)
        except Exception:
            operation=digest(canonical(dict(phase='original_archive',args=args)))
            ticket=await self._ticket(binding,operation,'PUT',key,size,sha256)
            receipt=await self._phase(binding,'original_archive',args,{'ticket':ticket})
            if (receipt.get('key'),receipt.get('size'),receipt.get('sha256'))!=(key,size,sha256):
                raise OriginalIntakeError('original_archive_receipt_changed')
            version=await self._head(key,size,receipt.get('native_version'))
        operation=digest(canonical(dict(phase='original_verify',args=args)))
        ticket=await self._ticket(binding,operation,'GET',key,size,sha256,version)
        receipt=await self._phase(binding,'original_verify',args,{'ticket':ticket})
        if ((receipt.get('key'),receipt.get('size'),receipt.get('sha256'),receipt.get('native_version'))!=(key,size,sha256,version)
                or receipt.get('acquisition_sha256')!=record['acquisition_sha256']):
            raise OriginalIntakeError('original_verification_receipt_changed')
        await self._head(key,size,version)
        return receipt

    async def begin(self,context,*,operation_id,source_id,ingestion_id,file,max_bytes=128*1024*1024):
        actor,binding=await self._authorize(context)
        text(operation_id);uuid_value(source_id);uuid_value(ingestion_id)
        if not isinstance(file,ChatFile):raise OriginalIntakeError('original_chat_file_required')
        if type(max_bytes) is not int or not 1<=max_bytes<=self.max_bytes:raise OriginalIntakeError('original_byte_bound')
        spec=OriginalIntakeSpec(source_id,ingestion_id,str(uuid5(UUID(ingestion_id),'original:'+operation_id)),file.file_id,
            file_name=file.file_name,mime_type=file.mime_type,max_bytes=max_bytes)
        async with self._lock:
            with self.queue.connect() as db:
                prior=db.execute('SELECT * FROM rkb_original_intakes WHERE ingestion_id=?',(ingestion_id,)).fetchone()
                encoded=canonical(spec.wire()).decode()
                if prior is not None:
                    if (prior['actor'],prior['operation_id'],prior['spec_json'],prior['storage_sha256'])!=(actor,operation_id,encoded,self._storage):
                        raise OriginalIntakeError('original_operation_changed')
                else:db.execute('INSERT INTO rkb_original_intakes VALUES(?,?,?,?,?,NULL,NULL)',(ingestion_id,actor,operation_id,encoded,self._storage))
            row=self._row(actor,ingestion_id)
            try:
                if row['receipt_json'] is not None:return await self.status(context,ingestion_id)
                if row['acquisition_json'] is None:
                    record=await self._phase(binding,'original_acquire',{'spec':spec.wire()},{'download_url':str(file.download_url)})
                    if record.get('spec')!=spec.wire():raise OriginalIntakeError('original_acquisition_changed')
                    with self.queue.connect() as db:db.execute('UPDATE rkb_original_intakes SET acquisition_json=? WHERE ingestion_id=?',
                        (canonical(record).decode(),ingestion_id))
                else:record=json.loads(row['acquisition_json'])
                receipt=await self._complete(context,binding,record)
                return OriginalIntakeStatus(ingestion_id,'verified',receipt)
            except asyncio.CancelledError:raise
            except Exception:
                _LOG.warning('original_intake state=pending run_id=%s ingestion_id=%s reason=receipt_unresolved',binding.run_id,ingestion_id)
                return OriginalIntakeStatus(ingestion_id,'pending',reason='original_intake_unknown_retry_same_operation')

    async def _complete(self,context,binding,record):
        native=await self._archive(binding,record);spec=OriginalIntakeSpec.from_wire(record['spec'])
        receipt=VerifiedOriginalReceipt(spec.source_id,spec.ingestion_id,spec.request_id,1,record['sha256'],record['size'],
            native['key'],native['native_version'],record['format'],record['page_count'],record['parser_sha256'],
            record['acquisition_sha256'],native['native_verification_sha256'])
        await self._authorize(context)
        with self.queue.connect() as db:db.execute('UPDATE rkb_original_intakes SET receipt_json=? WHERE ingestion_id=?',
            (canonical(receipt.wire()).decode(),spec.ingestion_id))
        _LOG.info('original_intake state=verified run_id=%s ingestion_id=%s',binding.run_id,spec.ingestion_id)
        return receipt

    async def verified(self,context,ingestion_id):
        actor,binding=await self._authorize(context);row=self._row(actor,ingestion_id)
        if row['receipt_json'] is None:return None
        receipt=VerifiedOriginalReceipt.from_wire(json.loads(row['receipt_json']))
        await self._head(receipt.key,receipt.size,receipt.native_version)
        if not self.binding_current(binding):raise OriginalIntakeError('original_current_binding_required')
        return receipt

    async def status(self,context,ingestion_id):
        actor,binding=await self._authorize(context);row=self._row(actor,ingestion_id)
        receipt=await self.verified(context,ingestion_id)
        if receipt is not None:return OriginalIntakeStatus(ingestion_id,'verified',receipt)
        try:
            record=json.loads(row['acquisition_json']) if row['acquisition_json'] else None
            if record is None:
                spec=json.loads(row['spec_json']);args={'spec':spec}
                key=digest(canonical(dict(phase='original_acquire',args=args)))
                with self.queue.connect(write=False) as db:
                    job=db.execute('SELECT * FROM jobs WHERE actor=? AND idempotency=?',
                        ('maintenance:'+binding.run_id,'recovery:'+key)).fetchone()
                if job is None or job['state']!='done':
                    reason='original_acquisition_pending' if key in self._private else 'original_attachment_retry_same_file_required'
                    return OriginalIntakeStatus(ingestion_id,'pending',reason=reason)
                identity=json.loads(job['identity']);result=json.loads(job['result'])
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
        except Exception:return OriginalIntakeStatus(ingestion_id,'pending',reason='original_intake_unknown_retry_same_operation')

    async def pages(self,context,ingestion_id,cursor=None,batch_size=1):
        actor,binding=await self._authorize(context);row=self._row(actor,ingestion_id)
        receipt=await self.verified(context,ingestion_id)
        if receipt is None:raise OriginalIntakeError('original_verified_receipt_required')
        if batch_size!=1:raise OriginalIntakeError('original_one_page_batch_required')
        try:index=0 if cursor is None else int(cursor)
        except ValueError:raise OriginalIntakeError('original_page_cursor_invalid') from None
        if index<0 or index>=receipt.page_count:raise OriginalIntakeError('original_page_cursor_invalid')
        record=json.loads(row['acquisition_json'])
        page=await self._phase(binding,'original_pages',dict(handle=record['handle'],original_sha256=record['sha256'],index=index))
        native=await self._archive(binding,record,page=page)
        def derivative():
            result=self.store.client.get_object(Bucket=self.store.bucket,Key=native['key'],IfMatch=native['native_version'])
            body=result['Body']
            try:
                if result.get('ContentLength')!=page['image_size'] or page['image_size']>8*1024*1024:
                    raise OriginalIntakeError('original_render_storage_invalid')
                data=body.read(page['image_size']+1)
                if len(data)!=page['image_size'] or digest(data)!=page['image_sha256']:
                    raise OriginalIntakeError('original_render_storage_invalid')
                return data
            finally:body.close()
        data=await _blocking(derivative)
        await self._authorize(context)
        expected_page=str(uuid5(UUID(receipt.source_id),f'page:1:{index}'))
        if (page.get('page_id')!=expected_page or page.get('original_sha256')!=receipt.sha256
                or page.get('parser_sha256')!=receipt.parser_sha256):raise OriginalIntakeError('original_page_provenance_changed')
        self._page_proofs.setdefault(ingestion_id,{})[index]=canonical(page)
        return RenderedPageBatch((RenderedPage(page['page_id'],index,None,'image/jpeg',data,
            page['native_text'],tuple(page['native_blocks']),dict(page['native_text_info'],
                original_sha256=record['sha256'],page_receipt_sha256=page['page_receipt_sha256'])),),
            str(index+1) if index+1<receipt.page_count else None)

    async def native_pages(self,context,ingestion_id):
        receipt=await self.verified(context,ingestion_id)
        if receipt is None or receipt.page_count>32:raise OriginalIntakeError('original_bounded_page_proof_required')
        for index in range(receipt.page_count):
            if index not in self._page_proofs.get(ingestion_id,{}):
                await self.pages(context,ingestion_id,cursor=str(index),batch_size=1)
        await self._authorize(context)
        return NativePagesProof(receipt.receipt_sha256,tuple(self._page_proofs[ingestion_id][i] for i in range(receipt.page_count)))
