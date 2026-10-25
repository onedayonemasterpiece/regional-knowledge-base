"""Dev retained maintenance owner using the existing durable jobs and admission.

No public enqueue surface, human impersonation, SQL RPC or second dispatcher.
The existing run authentication, document resource and durable RESULT_ACK own
assignment/reconnect semantics; a timeout leaves the same durable job available.
"""
from __future__ import annotations

import asyncio
import json
import logging
import uuid

from .bge_queue import StaleClaim
from .kaggle_recovery_phases import PHASES,b64,bounded,MAX_PHASE_RESULT_BYTES
from .kaggle_pg_snapshot_native import SnapshotError,encode,sha

_LOG=logging.getLogger(__name__)


class RecoveryPhaseDispatch:
    def __init__(self,queue,*,database,identity,binding_current,timeout_seconds=900,snapshot_tickets=None,dataset_checkpoint=None,rebind_current=None):
        if (queue._shared_reads is None or getattr(queue,'_recovery_phases',None) is not None
                or not callable(binding_current) or rebind_current is not None and not callable(rebind_current)
                or not isinstance(identity,tuple) or len(identity)!=3
                or type(timeout_seconds) not in (int,float) or not 0<timeout_seconds<=900):
            raise TypeError('configured current maintenance owner required')
        self.queue,self.database,self.identity=queue,database,identity
        self.binding_current,self.timeout=binding_current,timeout_seconds
        self.snapshot_tickets=snapshot_tickets
        self.dataset_checkpoint=dataset_checkpoint
        self.schema_context=None
        self.rebind_current=rebind_current
        self._bindings={};self._attachment_binding=None;self.closed=False
        queue._recovery_phases=self

    def current(self,binding):
        return (not self.closed and self.queue._recovery_phases is self and self.binding_current(binding))

    def bind_attachment(self,binding):
        if not self.current(binding):raise PermissionError('current maintenance binding required')
        self._attachment_binding=binding
        self.queue.shared_reads._event.set()

    def _run(self,db,run_id,token,generation):
        q=self.queue;run=q._authorize(db,run_id,token,q.clock());q._dispatch_authorize(run,generation)
        if (run_id,run['launch_nonce'],run['runtime_incarnation'])!=self.identity:
            raise PermissionError('recovery run identity changed')
        if db.execute('select current_run from control where id=1').fetchone()[0]!=run_id:
            raise PermissionError('recovery requires owned current run')
        return run

    def eligible(self,job,run,generation):
        binding=self._bindings.get(job['id'])
        if binding is None or not self.current(binding):
            binding=self._attachment_binding
            if binding is not None and self.current(binding):
                try:
                    identity=json.loads(job['identity'])
                    retained=(identity['run_id'],identity['launch_nonce'],identity['runtime_incarnation'])
                    valid=(job['kind']=='recovery_phase' and job['actor']=='maintenance:'+self.identity[0]
                        and retained==self.identity and identity['database_generation']==self.database.database_generation
                        and identity['writer_generation']==self.database.writer_generation
                        and identity['phase'] in PHASES and job['idempotency']=='recovery:'+identity['operation_key']
                        and identity['request_sha256']==sha(encode(dict(phase=identity['phase'],arguments=identity['arguments']))))
                except (KeyError,TypeError,ValueError):valid=False
                if valid:
                    self._bindings[job['id']]=binding
                    _LOG.warning('recovery_job_binding_restored run_id=%s generation=%s phase=%s job_id=%s',
                        self.identity[0],generation,identity['phase'],job['id'])
                else:binding=None
        return (binding is not None and self.current(binding)
                and (run['id'],run['launch_nonce'],run['runtime_incarnation'])==self.identity
                and job['kind']=='recovery_phase')

    def _enqueue(self,identity):
        q=self.queue;actor='maintenance:'+self.identity[0];key='recovery:'+identity['operation_key']
        encoded=encode(identity).decode();now=q.clock()
        with q.connect() as db:
            row=db.execute('select * from jobs where actor=? and idempotency=?',(actor,key)).fetchone()
            if row:
                if row['kind']!='recovery_phase' or row['identity']!=encoded:
                    raise SnapshotError('recovery_operation_changed')
                return row['id'],actor
            run=db.execute('select * from runs where id=?',(self.identity[0],)).fetchone()
            if run is None or (run['launch_nonce'],run['runtime_incarnation'])!=self.identity[1:]:
                raise PermissionError('recovery requires actual attached run')
            job=str(uuid.uuid4())
            db.execute('insert into jobs(id,actor,idempotency,kind,texts,identity,state,created,updated) '
                'values(?,?,?,?,?,?,?,?,?)',(job,actor,key,'recovery_phase','[]',encoded,'pending',now,now))
            return job,actor

    async def request(self,binding,phase,arguments,*,operation_key):
        if not self.current(binding):raise PermissionError('current maintenance binding required')
        # A validated current request also restores authority for retained
        # jobs of this exact runtime after socket or API-only reconnect.
        # eligible() validates each immutable persisted job; admission still
        # reconciles its actual compute claim before any resend.
        self.bind_attachment(binding)
        if phase not in PHASES or not isinstance(operation_key,str) or not 1<=len(operation_key.encode())<=256:
            raise SnapshotError('recovery_phase_contract')
        root=operation_key
        with self.queue.connect(write=False) as db:
            for attempt in range(256):
                row=db.execute('SELECT state,result FROM jobs WHERE actor=? AND idempotency=?',
                    ('maintenance:'+self.identity[0],'recovery:'+operation_key)).fetchone()
                if row is None or row['state']!='done':break
                value=json.loads(row['result'])['value']
                if not value.get('retryable') or not (value.get('error') or
                        phase=='schema_migrate' and value.get('status')=='rejected'):break
                operation_key=root+':retry:'+str(attempt+1)
            else:raise SnapshotError('migration_retry_budget')
        arguments=json.loads(encode(arguments));request_hash=sha(encode({'phase':phase,'arguments':arguments}))
        identity=bounded(dict(phase=phase,arguments=arguments,operation_key=operation_key,
            request_sha256=request_hash,database_generation=self.database.database_generation,
            writer_generation=self.database.writer_generation,run_id=self.identity[0],launch_nonce=self.identity[1],
            runtime_incarnation=self.identity[2]))
        job,actor=await asyncio.to_thread(self._enqueue,identity)
        _LOG.info('recovery_phase_queued run_id=%s generation=%s phase=%s job_id=%s',
            self.identity[0],getattr(binding,'connection_generation',None),phase,job)
        self._bindings[job]=binding;self.queue.shared_reads._event.set()
        result=await self.queue.wait_result(actor,job,self.timeout)
        if result is None:
            _LOG.warning('recovery_phase_timeout run_id=%s phase=%s operation_hash=%s',self.identity[0],phase,sha(operation_key.encode()))
            raise TimeoutError('recovery phase result timeout')
        # The durable result owns replay after completion. Only outstanding
        # work needs its attachment retained for transfer/dispatch authority.
        self._bindings.pop(job,None)
        if not self.current(binding):raise PermissionError('recovery binding expired before delivery')
        value=result['result']['value']
        if phase in ('capture','dataset_verify','original_pages') and isinstance(value,dict) and value.get('status')=='pending':
            handle=value.get('handle')
            if not isinstance(handle,str):raise SnapshotError('snapshot_io_receipt_invalid')
            async with asyncio.timeout(self.timeout):
                while True:
                    status=await self.request(binding,'snapshot_status',dict(handle=handle),
                        operation_key=sha(encode([operation_key,'status',uuid.uuid4().hex])))
                    if status.get('status')=='complete':return status['value']
                    if status.get('status')=='upload_required':
                        upload=await self.request(binding,'capture_upload',dict(handle=handle),
                            operation_key=operation_key+':upload')
                        if upload!={'status':'started'}:raise SnapshotError('snapshot_io_upload_pending')
                    elif status.get('status')!='pending':raise SnapshotError('snapshot_io_pending_reconciliation')
                    await asyncio.sleep(2)
        return value


    async def dispatch_payload(self,job_id,*,run_id,token,generation):
        with self.queue.connect(write=False) as db:
            run=self._run(db,run_id,token,generation)
            job=db.execute('select * from jobs where id=?',(job_id,)).fetchone()
            if (job is None or not self.eligible(job,run,generation) or job['state']!='claimed'
                    or job['run_id']!=run_id or job['lease_until']<=self.queue.clock()):
                raise StaleClaim('recovery assignment changed')
            identity=json.loads(job['identity'])
            payload=dict(id=job_id,claim=job['claim'],kind='recovery_phase',**identity)
        if identity['phase'] in ('prepare','drill_prepare','schema_bundle_stage','dataset_verify'):
            # Kernel metadata readback drops Dataset version qualifiers.
            # Resolve an exact-version SDK download ticket instead of claiming
            # a mounted version observation from our own intended pin.
            if self.snapshot_tickets is None:
                raise PermissionError('current signed artifact transfer owner required')
            from .kaggle_pg_snapshot_native import BlobRef
            if identity['phase']=='schema_bundle_stage':
                from .kaggle_warm_maintenance import WarmHandlerPatchApproval
                approval=WarmHandlerPatchApproval.from_wire(identity['arguments']['artifact'])
                from .kaggle_schema_live import MAX_SQL_BUNDLE_BYTES
                limit=MAX_SQL_BUNDLE_BYTES
                if not 1<=approval.blob.size<=limit:
                    raise SnapshotError('read_handler_artifact_budget')
                artifact=approval.blob
            else:
                artifact=BlobRef(**identity['arguments']['blob'])
            ticket=await self.snapshot_tickets.issue_download(self._bindings[job_id],
                identity['operation_key'],artifact)
            payload['download_ticket']=ticket.wire()
        from .original_intake import PHASES as ORIGINAL_PHASES
        if identity['phase'] in ORIGINAL_PHASES:
            original=getattr(self,'original_intake',None)
            if original is None:raise PermissionError('configured original intake owner unavailable')
            payload['original_transfer']=await original.dispatch_transfer(identity,self._bindings[job_id])
        _LOG.info('recovery_phase_dispatch run_id=%s generation=%s phase=%s job_id=%s queued_ms=%.3f',
            run_id,generation,identity['phase'],job_id,(self.queue.clock()-job['created'])*1000)
        return payload

    async def issue_upload(self,frame,*,run_id,token,generation):
        """Ticket intent stays on control while capture retains compute permit."""
        if self.snapshot_tickets is None:raise PermissionError('snapshot ticket issuer unavailable')
        with self.queue.connect(write=False) as db:
            run=self._run(db,run_id,token,generation)
            job=db.execute("select * from jobs where id=? and kind='recovery_phase'",(frame['job_id'],)).fetchone()
            if (job is None or job['run_id']!=run_id or job['claim']!=frame['claim']
                    or job['state']!='claimed' or not self.eligible(job,run,generation)):
                raise StaleClaim('snapshot transfer assignment changed')
            identity=json.loads(job['identity'])
            if (identity['phase'] not in ('capture','capture_upload') or frame['operation_key']!=identity['operation_key']
                    or tuple(frame[k] for k in ('run_id','launch_nonce','runtime_incarnation'))!=self.identity):
                raise PermissionError('snapshot transfer capture binding changed')
            binding=self._bindings[job['id']]
        ticket=await self.snapshot_tickets.issue_upload(binding,frame['operation_key'],size=frame['size'],sha256=frame['sha256'])
        if not self.current(binding):raise PermissionError('snapshot transfer binding expired')
        return ticket.wire()

    async def receive(self,frame,*,run_id,token,generation):
        arrived=self.queue.clock()
        bounded(frame['receipt'],MAX_PHASE_RESULT_BYTES)
        with self.queue.connect() as db:
            run=self._run(db,run_id,token,generation)
            job=db.execute("select * from jobs where id=? and kind='recovery_phase'",(frame['job_id'],)).fetchone()
            if job is None or job['run_id']!=run_id or job['claim']!=frame['claim']:
                raise StaleClaim('recovery assignment changed')
            _LOG.info('recovery_phase_receipt run_id=%s generation=%s job_id=%s arrived_at=%.6f',
                run_id,generation,job['id'],arrived)
            eligible=self.eligible(job,run,generation)
            if not eligible and self.rebind_current is not None:
                # A retained result can beat periodic startup maintenance to
                # the new socket. Rebind before emitting any terminal stale ACK.
                self.rebind_current()
                eligible=self.eligible(job,run,generation)
            if not eligible:raise StaleClaim('recovery assignment changed')
            if tuple(frame[k] for k in ('run_id','launch_nonce','runtime_incarnation'))!=self.identity:
                raise PermissionError('recovery receipt runtime changed')
            identity=json.loads(job['identity']);receipt=frame['receipt']
            if (not isinstance(receipt,dict) or set(receipt)!={'phase','request_sha256','value'}
                    or receipt['phase']!=identity['phase'] or receipt['request_sha256']!=identity['request_sha256']):
                raise SnapshotError('recovery_receipt_request_changed')
            encoded=encode(receipt).decode();status='accepted'
            if job['state']=='done':
                if job['result']!=encoded:raise SnapshotError('recovery_receipt_changed')
                status='duplicate'
            elif job['state']!='claimed':raise StaleClaim('recovery assignment no longer claimed')
            if identity['phase']=='schema_migrate' and receipt['value'].get('readiness') is not None:
                if receipt['value'].get('schema_version')!=identity['arguments']['target_schema_version']:
                    raise SnapshotError('migration_result_version_changed')
                self.queue._promote_schema_runtime(db,run,generation,receipt['value']['readiness'],
                    target_version=identity['arguments']['target_schema_version'])
                if self.schema_context is None:raise SnapshotError('migration_context_promotion_unavailable')
                self.schema_context(receipt['value']['schema_version'])
            if status=='accepted':db.execute("update jobs set state='done',result=?,updated=? where id=?",(encoded,self.queue.clock(),job['id']))
        self._bindings.pop(frame['job_id'],None)
        self.queue._notify_result(frame['job_id'])
        _LOG.info('recovery_phase_settled run_id=%s generation=%s phase=%s job_id=%s status=%s total_ms=%.3f receive_ms=%.3f',
            run_id,generation,receipt['phase'],frame['job_id'],status,
            (self.queue.clock()-job['created'])*1000,(self.queue.clock()-arrived)*1000)
        return dict(job_id=frame['job_id'],claim=frame['claim'],status=status)

    async def aclose(self):
        self.closed=True
        if self.queue._recovery_phases is self:self.queue._recovery_phases=None


class WssSnapshotPhases:
    """Typed adapter consumed directly by PhysicalSnapshotRecovery."""
    def __init__(self,dispatch,*,operation_key):
        if type(dispatch) is not RecoveryPhaseDispatch or not isinstance(operation_key,str) or not operation_key:
            raise TypeError('retained recovery dispatch and stable operation required')
        self.dispatch,self.operation=dispatch,operation_key

    async def _call(self,binding,phase,args):
        # Content-derived stable subkeys survive lost receipts/reconnects.
        key=sha(encode({'operation':self.operation,'phase':phase,'arguments':args}))
        result=await self.dispatch.request(binding,phase,args,operation_key=key)
        if phase=='capture' and result=={'status':'rejected','reason':'snapshot_capture_rejected'}:
            raise SnapshotError('snapshot_capture_rejected')
        if phase in ('read_handler_stage','read_handler_activate','read_handler_rollback'):
            if result=={'status':'rejected','reason':'read_handler_maintenance_rejected'}:
                # Failure is durably acknowledged, not a worker/PG/BGE crash.
                # Retry requires a fresh decision after loaded-SHA readback.
                raise SnapshotError('read_handler_maintenance_rejected')
        return result

    async def capture(self,binding,**args):return await self._call(binding,'capture',args)

    async def prepare(self,binding,*,operation_key,manifest_bytes,control_bytes,**args):
        return (await self._call(binding,'prepare',dict(args,manifest_b64=b64(manifest_bytes),
            control_b64=b64(control_bytes),restore_operation=operation_key)))['handle']

    async def apply(self,binding,handle,*,command_bytes,**args):
        return await self._call(binding,'apply',dict(args,handle=handle,command_b64=b64(command_bytes)))

    async def finish(self,binding,handle,*,control_bytes,**args):
        return (await self._call(binding,'finish',dict(args,handle=handle,control_b64=b64(control_bytes))))['reference']

    async def verify_initialized(self,binding,reference):
        # Readback is a fresh operation, never a cached historical observation.
        import secrets
        return await self.dispatch.request(binding,'verify',{'reference':reference},
            operation_key=sha(encode([self.operation,'verify',secrets.token_hex(24)])))

    async def read_handler_status(self,binding):
        """Fresh loaded-code readback on the same run-authenticated WSS permit."""
        import secrets
        return await self.dispatch.request(binding,'read_handler_status',{},
            operation_key=sha(encode([self.operation,'read_handler_status',secrets.token_hex(24)])))

    async def stage_read_handler(self,binding,*,handler_source,reviewed_commit,
            expected_loaded_sha256,expected_generation):
        """Send reviewed stateless source inline over the same authenticated WSS."""
        from .kaggle_warm_maintenance import inline_handler_bytes
        digest=sha(handler_source.encode('utf-8')) if type(handler_source) is str else ''
        inline_handler_bytes(handler_source,digest,reviewed_commit)
        return await self._call(binding,'read_handler_stage',dict(handler_source=handler_source,
            handler_sha256=digest,reviewed_commit=reviewed_commit,
            expected_loaded_sha256=expected_loaded_sha256,expected_generation=expected_generation))

    async def activate_read_handler(self,binding,*,staged_sha256,expected_loaded_sha256,expected_generation):
        return await self._call(binding,'read_handler_activate',dict(staged_sha256=staged_sha256,
            expected_loaded_sha256=expected_loaded_sha256,expected_generation=expected_generation))

    async def rollback_read_handler(self,binding,*,expected_loaded_sha256,expected_generation):
        return await self._call(binding,'read_handler_rollback',dict(
            expected_loaded_sha256=expected_loaded_sha256,expected_generation=expected_generation))
