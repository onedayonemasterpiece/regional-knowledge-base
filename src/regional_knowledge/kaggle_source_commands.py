"""Fixed accepted source commands on the existing durable WSS assignment.

The controller proves remote membership before dispatch. The notebook receives
only one captured proof, fixed SQL-function pins and resolved command bytes; no
cloud journal credential, arbitrary SQL or user bearer is transported.
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import time
from uuid import UUID

import psycopg

from .kaggle_apply import ApplyExecutionError, ApplyUnknown, ApplyReceipt, HandlerRegistry, HandlerSpec, PgFunction, Revisions, apply_accepted
from .recovery_journal import AcceptedSeal, ExactCommand, ObjectRef
from .source_import import SOURCE_IMPORT, IMPORT_DEPENDENCY, validate_import_command, source_import_handler

SOURCE_WITHDRAW='source.withdraw.v1'
MAX_SOURCE_FRAME_BYTES=2*1024*1024
MAX_SOURCE_RECEIPT_BYTES=16*1024
_LOG=logging.getLogger(__name__)


def canonical(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()


def digest(value):return hashlib.sha256(value).hexdigest()


def decode_command(data):
    if not isinstance(data,bytes) or not 1<=len(data)<=1048576:
        raise ValueError('bounded exact source command required')
    try:
        value=json.loads(data)
        command=ExactCommand(**{k:value[k] for k in ('sequence','operation_key','request_id','person_id','route',
            'logical_time','schema_revision','function_revision','executor_revision','ids','expected_revisions','kind')},
            payload=base64.b64decode(value['payload_b64'],validate=True))
        if command.encode()!=data:raise ValueError
        if command.schema_revision=='rkb-v4-sql001-014-v1':
            from .kaggle_native_source_bindings import validate_native_source_command
            validate_native_source_command(command)
        elif command.schema_revision in ('rkb-v4-sql001-011-v1','rkb-v4-sql001-012-v1'):
            from .kaggle_domain_bindings import validate_current_command
            validate_current_command(command)
        elif command.route==SOURCE_IMPORT:validate_import_command(command)
        elif command.route=='source.import.v2':
            from .source_import_v2 import validate_import_v2_command
            validate_import_v2_command(command)
        elif command.route=='source.import.v3':
            from .source_import_v3 import validate_import_v3_command
            validate_import_v3_command(command)
        elif command.route=='source.import.v4':
            from .source_import_v4 import validate_import_v4_command
            validate_import_v4_command(command)
        elif command.route in ('story.edit.v1','story.access.v1'):
            from .story_editorial import validate_story_edit_command,validate_story_access_command
            (validate_story_edit_command if command.route=='story.edit.v1' else validate_story_access_command)(command)
        elif command.route.startswith('disagreement.'):
            from .disagreement_native import validate_case_command
            validate_case_command(command)
        elif command.route=='geo.manifest.install.v1':
            from .geo_manifest_commands import validate_geo_manifest_command
            validate_geo_manifest_command(command)
        elif (command.route!=SOURCE_WITHDRAW or command.kind!='domain_command'
              or set(command.ids)!={'source_id'} or set(command.expected_revisions)!={'eligibility_revision'}
              or command.payload!=b'{"withdrawn":true}'):
            raise ValueError
        if 'source_id' in command.ids and str(UUID(command.ids['source_id']))!=command.ids['source_id']:raise ValueError
        if command.route==SOURCE_WITHDRAW:
            revision=command.expected_revisions['eligibility_revision']
            if not revision.isdecimal() or not revision.isascii() or str(int(revision))!=revision or not 1<=int(revision)<2**63:
                raise ValueError
        return command
    except (ValueError,KeyError,TypeError,UnicodeError,RecursionError):
        raise ValueError('invalid exact source command') from None


def seal_wire(seal):
    return dict(manifest=seal.manifest.wire(),command_hashes=list(seal.command_hashes))


def decode_seal(value,command_hash,sequence):
    try:
        if set(value)!={'manifest','command_hashes'} or value['command_hashes']!=[command_hash]:raise ValueError
        ref=ObjectRef(**value['manifest'])
        if (ref.last_sequence!=sequence or type(ref.length) is not int or not 1<=ref.length<=8192
                or not isinstance(ref.key,str) or not 1<=len(ref.key)<=2048
                or not ref.key.startswith('recovery/v1/') or '/manifests/' not in ref.key
                or not isinstance(ref.sha256,str) or len(ref.sha256)!=64
                or any(c not in '0123456789abcdef' for c in ref.sha256)):
            raise ValueError
        return AcceptedSeal(ref,(command_hash,))
    except (ValueError,TypeError,KeyError):
        raise ValueError('exact singleton accepted source proof required') from None


def receipt_wire(receipt):
    value={key:getattr(receipt,key) for key in ('database_generation','operation_key','sequence',
        'command_sha256','status','outcome_sha256','duplicate')}
    value['outcome_b64']=base64.b64encode(receipt.outcome).decode()
    if len(canonical(value))>MAX_SOURCE_RECEIPT_BYTES:raise ValueError('source native receipt bound')
    return value


def decode_receipt(value):
    try:
        if len(canonical(value))>MAX_SOURCE_RECEIPT_BYTES:raise ValueError
        value=dict(value)
        body=base64.b64decode(value.pop('outcome_b64'),validate=True)
        receipt=ApplyReceipt(**value,outcome=body)
        if (type(receipt.sequence) is not int or receipt.sequence<1 or type(receipt.duplicate) is not bool
                or receipt.status not in ('applied','rejected_precondition') or digest(body)!=receipt.outcome_sha256):
            raise ValueError
        outcome=json.loads(body)
        if (canonical(outcome)!=body or outcome.get('codec')!='rkb.applied-outcome.v1'
                or set(outcome)!={'codec','operation_key','command_sha256','cursor','status','ids','request_id',
                    'person_id','route','logical_time','before','after','handler_sha256','result_b64','result_sha256'}
                or outcome.get('command_sha256')!=receipt.command_sha256
                or outcome.get('operation_key')!=receipt.operation_key or outcome.get('cursor')!=receipt.sequence
                or outcome.get('status')!=receipt.status):raise ValueError
        if digest(base64.b64decode(outcome['result_b64'],validate=True))!=outcome['result_sha256']:raise ValueError
        return receipt,outcome
    except (ValueError,KeyError,TypeError,UnicodeError):
        raise ValueError('invalid bounded native source receipt') from None



def read_historical_contract(data,registry):
    """Read v1 evidence without installing or executing a historical handler."""
    from dataclasses import replace
    command=decode_command(data)
    handler=registry.handlers.get(command.route)
    if handler is None and command.route in ('source.import.v1','source.import.v2','source.import.v3'):
        from .source_activation import ACTIVATE_ROUTE
        handler=registry.handlers.get(ACTIVATE_ROUTE)
    if handler is None or handler.kind!=command.kind:raise ValueError('unsupported historical command contract')
    dependency=handler.dependency
    if dependency is not None and dependency.operation!=command.route:
        dependency=replace(dependency,operation=command.route)
    return command,handler,dependency


def verify_historical_receipt(command,recorded_hash,receipt):
    """The original canonical receipt, never the current implementation hash."""
    try:
        decoded,value=decode_receipt(receipt_wire(receipt))
        audit=[command.schema_revision,command.function_revision,command.executor_revision]
        return (value['handler_sha256']==recorded_hash and value['before']==value['after']==audit
            and decoded.command_sha256==digest(command.encode())
            and decoded.operation_key==command.operation_key and decoded.sequence==command.sequence
            and value['route']==command.route and value['request_id']==command.request_id
            and value['person_id']==command.person_id and value['logical_time']==command.logical_time
            and value['ids']==dict(command.ids))
    except (ValueError,TypeError,KeyError):return False


class ControllerAcceptedProof:
    """A single TLS-controller-attested command/seal; not a remote journal store.

    Only the authenticated internal dispatch boundary constructs this object.
    The controller has just called the actual journal.verify_accepted. This
    object cannot verify another command, upload, seal or publish any outcome.
    """
    def __init__(self,command_bytes,seal,command_sha256):
        command=decode_command(command_bytes)
        if digest(command_bytes)!=command_sha256:raise ValueError('source command hash mismatch')
        self._bytes=bytes(command_bytes);self._seal=seal;self._sha=command_sha256
        decode_seal(seal_wire(seal),command_sha256,command.sequence)

    async def verify_accepted(self,seal,command):
        if seal!=self._seal or command.encode()!=self._bytes:
            raise PermissionError('source dispatch proof does not authorize this exact command')
        return self._sha


class WorkerSourceExecutor:
    def __init__(self,*,acquire,registry,database,run_id,launch_nonce,runtime_incarnation,materializer=None):
        self.acquire,self.registry,self.database=acquire,registry,database
        if materializer is not None and not callable(materializer):raise TypeError('retained native materializer required')
        self.materializer=materializer
        self.identity=(run_id,launch_nonce,runtime_incarnation)

    async def __call__(self,frame):
        if (set(frame)!={'type','protocol_version','connection_generation','id','claim','kind','run_id',
                'launch_nonce','runtime_incarnation','database_generation','writer_generation','command_sha256',
                'handler_sha256','command_b64','accepted','compute_claim','dispatch_generation'}
                or frame.get('kind')!='source_command' or frame.get('type')!='DISPATCH'
                or tuple(frame.get(k) for k in ('run_id','launch_nonce','runtime_incarnation'))!=self.identity
                or frame.get('database_generation')!=self.database.database_generation
                or frame.get('writer_generation')!=self.database.writer_generation
                or type(frame.get('connection_generation')) is not int
                or type(frame.get('dispatch_generation')) is not int
                or not 1<=frame['dispatch_generation']<=frame['connection_generation']
                or not isinstance(frame.get('compute_claim'),str) or not 1<=len(frame['compute_claim'])<=256):
            raise PermissionError('source dispatch runtime binding mismatch')
        if len(canonical(frame))>MAX_SOURCE_FRAME_BYTES:raise ValueError('source dispatch frame bound')
        data=base64.b64decode(frame['command_b64'],validate=True)
        command=decode_command(data)
        seal=decode_seal(frame['accepted'],frame['command_sha256'],command.sequence)
        proof=ControllerAcceptedProof(data,seal,frame['command_sha256'])
        handler=self.registry.resolve(command)
        if frame.get('handler_sha256')!=handler.fingerprint:
            raise PermissionError('source registry binding mismatch')
        reject_reason=None
        if command.schema_revision=='rkb-v4-sql001-014-v1' and command.route in ('source.materialize.v1','source.stage.seal.v1','source.batch.v1'):
            if self.materializer is None:raise ValueError('native source materializer unavailable')
            # Verify this captured authenticated accepted intent BEFORE private
            # candidate effects, then apply canonical outcome in its own tx.
            await proof.verify_accepted(seal,command)
            from .kaggle_source_materializer import SourceMaterializationRejected
            try:
                await self.materializer(command,acquire=self.acquire,database=self.database)
            except SourceMaterializationRejected as error:
                # Deterministic invalid model input produces an ordinary
                # rejected-precondition receipt, not a fabricated completion.
                reason=str(error)
                if not 1<=len(reason)<=100 or any(c not in 'abcdefghijklmnopqrstuvwxyz0123456789_' for c in reason):
                    reason='invalid_model_input'
                _LOG.warning('native_source_materialization state=rejected route=%s sequence=%s reason=%s',
                    command.route,command.sequence,reason)
            except psycopg.OperationalError:
                # Connection/commit uncertainty cannot become a refusal.
                raise ApplyUnknown('native source preparation outcome unknown; resume original operation') from None
            except psycopg.Error as error:
                reject_reason='native_source_sql_rolled_back'
                _LOG.warning('native_source_materialization state=rolled_back route=%s sequence=%s error_type=%s sqlstate=%s',
                    command.route,command.sequence,type(error).__name__,getattr(error,'sqlstate',None))
        async with self.acquire() as connection:
            options=dict(journal=proof,seal=seal,command=command,
                database_generation=self.database.database_generation,writer_generation=self.database.writer_generation,
                registry=self.registry,max_outcome_bytes=8192)
            try:
                receipt=await apply_accepted(connection,**options,
                    **({'reject_reason':reject_reason} if reject_reason is not None else {}))
            except ApplyExecutionError:
                # apply_accepted has exited its failed PG transaction. A new
                # transaction records an actual no-effect receipt, never a
                # Python-fabricated result. An uncertain commit is ApplyUnknown
                # and cannot enter this branch. Duplicate lookup wins a race.
                receipt=await apply_accepted(connection,**options,reject_reason='native_source_sql_rolled_back')
                _LOG.warning('native_source_apply state=rolled_back_rejection_retained route=%s sequence=%s',
                    command.route,command.sequence)
        return receipt_wire(receipt)


class AcceptedSourceDispatch:
    """Controller bridge, with durable truth retained by the existing spool/jobs."""
    def __init__(self,spool,*,binding_current,clock=time.monotonic):
        from .control_spool import ControlSpool
        if not isinstance(spool,ControlSpool) or not callable(binding_current):
            raise TypeError('existing canonical spool and current runtime binding required')
        if getattr(spool.queue,'_source_commands',None) is not None:
            raise ValueError('source dispatch already configured')
        self.spool,self.queue,self.binding_current,self.clock=spool,spool.queue,binding_current,clock
        self._retry={}
        self._publications={}
        self.queue._source_commands=self

    def maintenance_blocks_dispatch(self):
        if self.spool._lock.locked() or getattr(self.spool,'_native_history_unsettled',False):return True
        # Restore has already computed these exact outcomes. Dataset capture
        # must finish before old jobs settle; redispatching their old writer
        # identity would disconnect the healthy recovered worker.
        with self.queue.connect(write=False) as db:
            return db.execute("""SELECT 1 FROM jobs j JOIN rkb_control_operations o
                ON o.operation_key=json_extract(j.identity,'$.operation_key')
                WHERE j.kind='source_command' AND j.state IN ('pending','claimed')
                AND o.apply_state IN ('APPLIED','REJECTED_PRECONDITION')
                LIMIT 1""").fetchone() is not None

    async def enqueue(self,context,key,*,writer_generation):
        entry=await self.spool.get(key)
        actor=await self.spool.authorize_command(context,entry.command)
        if entry.command.person_id!=actor or entry.accepted is None:
            raise PermissionError('original actor and remote accepted source command required')
        command_hash=await self.spool.journal.verify_accepted(entry.accepted,entry.command)
        if command_hash!=entry.command_sha256:raise PermissionError('accepted source bytes differ')
        if not isinstance(writer_generation,str) or not 1<=len(writer_generation)<=256:
            raise ValueError('exact current writer generation required')
        await self.queue.shared_reads.register_command_actor(context,entry.command)
        identity=dict(operation_key=key,command_sha256=command_hash,database_generation=entry.database_generation,
            writer_generation=writer_generation,sequence=entry.command.sequence)
        job=await asyncio.to_thread(self.queue._enqueue_source_command,self,actor,identity)
        self.queue.shared_reads.wake()
        return job

    async def authorize_job(self,context,job):
        entry,_=await self._entry(job,verify=False)
        return await self.spool.authorize_command(context,entry.command)

    async def _entry(self,job,*,verify=True):
        identity=json.loads(job['identity'])
        entry=await self.spool.get(identity['operation_key'])
        if (entry.accepted is None or entry.command.person_id!=job['actor']
                or entry.command_sha256!=identity['command_sha256']
                or entry.database_generation!=identity['database_generation'] or entry.command.sequence!=identity['sequence']):
            raise PermissionError('durable source job differs from exact accepted intent')
        if verify and await self.spool.journal.verify_accepted(entry.accepted,entry.command)!=entry.command_sha256:
            raise PermissionError('source journal acceptance differs')
        return entry,identity

    def _current(self,run,identity,generation):
        if not self.binding_current(run_id=run['id'],runtime_incarnation=run['runtime_incarnation'],
                connection_generation=generation,database_generation=identity['database_generation'],
                writer_generation=identity['writer_generation']):
            raise PermissionError('source executor binding is no longer current')

    async def dispatch_payload(self,job_id,*,run_id,token,generation):
        run,job=await asyncio.to_thread(self.queue._source_claim,self,run_id,token,generation,job_id)
        entry,identity=await self._entry(job)
        self._current(run,identity,generation)
        await self.spool.mark_apply_sent(identity['operation_key'])
        return dict(id=job['id'],claim=job['claim'],kind='source_command',run_id=run_id,
            launch_nonce=run['launch_nonce'],runtime_incarnation=run['runtime_incarnation'],
            database_generation=identity['database_generation'],writer_generation=identity['writer_generation'],
            command_sha256=entry.command_sha256,handler_sha256=entry.handler_sha256,
            command_b64=base64.b64encode(entry.command_bytes).decode(),accepted=seal_wire(entry.accepted))

    async def receive(self,frame,*,run_id,token,generation):
        run,job=await asyncio.to_thread(self.queue._source_claim,self,run_id,token,generation,frame['job_id'])
        entry,identity=await self._entry(job,verify=False)
        self._current(run,identity,generation)
        self.queue.shared_reads.require_durable_resource(run_id,run['runtime_incarnation'],generation,
            job['id'],frame['compute_claim'],frame['dispatch_generation'])
        if (frame['claim']!=job['claim'] or frame.get('run_id')!=run_id
                or frame.get('launch_nonce')!=run['launch_nonce']
                or frame.get('runtime_incarnation')!=run['runtime_incarnation']):
            raise PermissionError('source receipt assignment differs')
        receipt,outcome=decode_receipt(frame['receipt'])
        if (receipt.database_generation!=entry.database_generation or receipt.operation_key!=identity['operation_key']
                or receipt.sequence!=entry.command.sequence or receipt.command_sha256!=entry.command_sha256
                or outcome.get('handler_sha256')!=entry.handler_sha256 or outcome.get('route')!=entry.command.route
                or outcome.get('ids')!=dict(entry.command.ids) or outcome.get('person_id')!=entry.command.person_id
                or outcome.get('request_id')!=entry.command.request_id or outcome.get('logical_time')!=entry.command.logical_time
                or outcome.get('before')!=[entry.command.schema_revision,entry.command.function_revision,entry.command.executor_revision]
                or outcome.get('after')!=outcome.get('before')):
            raise PermissionError('source native receipt differs from accepted command')
        await self.spool.record_apply(identity['operation_key'],receipt)
        _LOG.info('source_native_receipt_retained command_sha256=%s sequence=%s remote_outcome_durable=false',entry.command_sha256,entry.command.sequence)
        return await self.poll_settlement(run_id=run_id,token=token,generation=generation,job_id=job['id'])

    async def poll_settlement(self,*,run_id,token,generation,job_id=None):
        # One bounded publication task belongs to this existing durable job.
        # Cloud latency never serializes the channel's read/control dispatcher.
        for published_id,(owner_run,task) in tuple(self._publications.items()):
            if owner_run==run_id and task.done() and (job_id is None or published_id==job_id):
                self._publications.pop(published_id,None)
                return task.result()
        try:
            _,job=await asyncio.to_thread(self.queue._source_claim,self,run_id,token,generation,job_id)
        except LookupError:return None
        if job['id'] in self._publications:return None
        entry,_=await self._entry(job,verify=False)
        if entry.apply_receipt is None or self._retry.get(job['id'],(0,0))[0]>self.clock():return None
        # The receiver and dispatcher can both pass the earlier check before
        # _entry yields. Reserve once after the last await; never overwrite the
        # task whose exact accepted settlement must reach the worker.
        if job['id'] in self._publications:return None
        if len(self._publications)>=4:raise RuntimeError('bounded source publication task capacity')
        task=asyncio.create_task(self.reconcile(run_id=run_id,token=token,generation=generation,job_id=job['id']))
        self._publications[job['id']]=(run_id,task)
        task.add_done_callback(lambda done:self.queue.shared_reads.wake())
        return None

    async def reconcile(self,*,run_id,token,generation,job_id=None):
        from .recovery_journal import JournalUnknown,StoreUnavailable
        from .bge_queue import StaleAttachment
        try:
            run,job=await asyncio.to_thread(self.queue._source_claim,self,run_id,token,generation,job_id)
        except LookupError:return None
        now=self.clock()
        if self._retry.get(job['id'],(0,0))[0]>now:return None
        entry,identity=await self._entry(job)
        self._current(run,identity,generation)
        if entry.apply_receipt is None:return None
        try:
            await self.spool.publish_outcome(identity['operation_key'],writer_generation=identity['writer_generation'])
        except (JournalUnknown,StoreUnavailable,TimeoutError) as exc:
            from .dataset_recovery_store import DatasetRecoveryStore
            if (type(self.spool.journal.store) is DatasetRecoveryStore
                    and type(exc) is JournalUnknown and exc.args==('outcome_pending_dataset_checkpoint',)):
                result=await asyncio.to_thread(self.queue._complete_source_command,self,run_id,token,generation,
                    job['id'],job['claim'],entry.command_sha256,remote_outcome_durable=False)
                self.queue.shared_reads.durable_result_settled(job['id'],job['claim'],result)
                _LOG.info('source_outcome_checkpoint_pending command_sha256=%s sequence=%s remote_outcome_durable=false',
                    entry.command_sha256,entry.command.sequence)
                return dict(job_id=job['id'],claim=job['claim'],status=result)
            failures=self._retry.get(job['id'],(0,0))[1]+1
            self._retry[job['id']]=(now+min(30,2**min(failures,5)),failures)
            _LOG.info('source_outcome_pending command_sha256=%s sequence=%s retry=%s',entry.command_sha256,entry.command.sequence,failures)
            return None
        self._retry.pop(job['id'],None)
        try:
            latest,_=await asyncio.to_thread(self.queue._source_claim,self,run_id,token,generation,job['id'])
            self._current(latest,identity,generation)
            result=await asyncio.to_thread(self.queue._complete_source_command,self,run_id,token,generation,
                job['id'],job['claim'],entry.command_sha256)
        except StaleAttachment:
            return None  # New attachment reconciles the durable remote receipt.
        self.queue.shared_reads.durable_result_settled(job['id'],job['claim'],result)
        _LOG.info('source_outcome_remote_durable command_sha256=%s sequence=%s status=%s',entry.command_sha256,entry.command.sequence,result)
        return dict(job_id=job['id'],claim=job['claim'],status=result)

    async def aclose(self):
        cancelled=False
        for _,task in tuple(self._publications.values()):
            while not task.done():
                try:await asyncio.shield(task)
                except asyncio.CancelledError:cancelled=True
                except Exception:break
            if not task.cancelled():task.exception()
        self._publications.clear()
        if self.queue._source_commands is self:self.queue._source_commands=None
        if cancelled:raise asyncio.CancelledError
