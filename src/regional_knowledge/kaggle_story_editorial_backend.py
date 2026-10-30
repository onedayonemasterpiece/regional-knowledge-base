"""Existing Story operations through one queue/spool/native authority path.

Only a complete caller-authored UpsertVariant is encoded. The saved command
contains that exact body/vector; replay never invokes the model again.
"""
import asyncio
import base64
from contextlib import asynccontextmanager
from datetime import datetime,timezone
import hashlib
import json
import logging
import struct
import time
from uuid import UUID,uuid4
from mcp.server.mcpserver.exceptions import ToolError
from .bge_contract import SPACE,REVISION,TOKENIZER_SHA256,validate_vector
from .contracts import Principal
from .participants import RequestAuthority,ParticipantError
from .kaggle_editorial_contracts import EditorialReadRequest
from .kaggle_protocol import TrustedActorContext
from .kaggle_public_backend import current_binding
from .kaggle_import_tokenizer import HfImportTokenCounter
from .kaggle_import_output import StoryEditorialOutputGate
from .story_contracts import UpsertVariant
from .source_import import _text,_uuid
from .story_editorial import ROLES,EDITORIAL_REVISIONS,resolved_story_edit_command,resolved_story_access_command
from .kaggle_domain_bindings import DOMAIN_REVISIONS,resolved_current_story_edit_command,resolved_current_story_access_command
from .source_activation import ACTIVATION_REVISIONS
from .kaggle_native_source_bindings import new_native_command


def canonical(value):return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
def digest(value):return hashlib.sha256(value).hexdigest()
_LOG=logging.getLogger(__name__)

def failure(reason,status='unavailable'):return ToolError(json.dumps(dict(status=status,reason=reason)))


class KaggleStoryEditorial:
    def __init__(self,startup,*,tokenizer):
        if type(tokenizer) is not HfImportTokenCounter or not startup.gate.editorial_supported():
            raise TypeError('exact native editorial registry and protected tokenizer required')
        self.startup=startup;self.queue=startup.config.queue;self.spool=startup.config.spool
        self.participants=startup.config.participants;self.read_authority=startup.authority
        self.gate=startup.gate;self.tokenizer=tokenizer;self.destination=startup.backend.destination
        if self.queue._story_editorial is not None:raise ValueError('one retained Story workflow required')
        self.queue._story_editorial=self;self._documents={};self.output_gate=StoryEditorialOutputGate(self)

    def _binding_current(self,binding):
        snapshot=getattr(self.startup,'snapshot',None)
        return self.startup.binding_current(binding) and (snapshot is None or snapshot.verified)

    async def _check(self,authority,context,binding):
        await self.read_authority.require(authority,'public_search')
        if (authority.public_only or authority.route not in ('owner','personal') or context.route!=authority.route
                or context.principal.subject!=authority.person_id):raise PermissionError('personal Story authority required')
        try:actor=await asyncio.to_thread(self.participants.require,context,'public_search')
        except ParticipantError:raise PermissionError('current Story participant capability required') from None
        if actor!=authority.person_id or not self._binding_current(binding):raise PermissionError('current Story authority required')

    @asynccontextmanager
    async def _access(self,principal):
        authority=await self.read_authority.resolve_oauth(principal.access_token,destination=self.destination)
        retained=False
        try:
            context=RequestAuthority(Principal(subject=authority.person_id,client_id=authority.client_id,
                issuer=authority.issuer,access_token=''),authority.route)
            binding=await asyncio.to_thread(current_binding,self.queue)
            await self._check(authority,context,binding)
            access=dict(authority=authority,context=context,binding=binding)
            yield access
            retained=access.get('retained',False)
        finally:
            if not retained:self.read_authority.release(authority)

    async def _preflight(self,principal,access):
        # A separate retained authority belongs to the exact document job until
        # its original two-second read deadline. HTTP status issuance cannot
        # prematurely release that job's authorization object.
        authority=await self.read_authority.resolve_oauth(principal.access_token,destination=self.destination)
        permit=ticket=gated=None;kept=False
        try:
            await self._check(authority,access['context'],access['binding'])
            b=access['binding'];deadline=time.monotonic()+2
            q=EditorialReadRequest(request_id='editorial-context-'+str(uuid4()),connection_generation=b.connection_generation,
                turn_generation=0,story_id=access['story_id'],view='edit_context',limit=1,min_data_seq=b.min_data_seq,
                deadline_ms=2000,actor=TrustedActorContext(person_id=authority.person_id,audience=authority.destination.key,
                    auth_epoch=authority.auth_revision,visibility_epoch=0,context_ref=authority.context_ref))
            permit=await self.gate.dispatch(authority,q,b)
            ticket=await self.queue.shared_reads.begin_read(authority,permit.query,dialog_ref=self.destination.key,deadline=deadline)
            async with asyncio.timeout_at(deadline):result=await asyncio.shield(ticket.result)
            gated=await self.gate.finish(permit,result);permit=None
            if gated.result.status!='ok':raise failure(gated.result.reason or 'source_history_unavailable')
            kept=True
            return dict(authority=authority,gated=gated,binding=b,context=access['context'],record=gated.result.record)
        finally:
            if ticket is not None:self.queue.shared_reads.cancel_read(ticket)
            if permit is not None:self.gate.abandon(permit)
            if not kept:
                if gated is not None:self.gate.release(gated.ticket)
                self.read_authority.release(authority)

    def _release_proof(self,proof):
        self.gate.release(proof['gated'].ticket);self.read_authority.release(proof['authority'])

    def _release_document(self,job_id):
        value=self._documents.pop(job_id,None)
        self.queue.shared_reads.release_editorial_document(self,job_id)
        if value:
            value['timer'].cancel();self._release_proof(value['proof'])

    async def authorize_document(self,job_id):
        value=self._documents.get(job_id)
        if value is None:raise PermissionError('exact retained editorial job required')
        proof=value['proof'];await self._check(proof['authority'],proof['context'],proof['binding'])
        if not self.gate.issuance_current(proof['gated'].ticket):raise PermissionError('current Story/source proof required')
        def exact_job():
            with self.queue.connect(write=False) as db:
                row=db.execute('SELECT actor,kind,identity,texts FROM jobs WHERE id=? AND idempotency=?',(job_id,value['key'])).fetchone()
                return dict(row) if row is not None else None
        job=await asyncio.to_thread(exact_job)
        if (job is None or job['actor']!=proof['authority'].person_id or job['kind']!='document'
                or json.loads(job['identity'])!=value['identity'] or len(json.loads(job['texts']))!=1
                or digest(json.loads(job['texts'])[0].encode())!=value['identity']['body_sha256']):
            raise PermissionError('exact editorial document binding required')
        if not self.gate.issuance_current(proof['gated'].ticket):raise PermissionError('Story proof changed during authorization')
        return proof['authority'].person_id

    async def _tokens(self,body):
        tokens=await self.tokenizer.counts([body])
        if len(tokens)!=1 or type(tokens[0]) is not int or not 1<=tokens[0]<=512:
            raise failure('complete_narrative_exceeds_512_tokens','invalid_query')
        return tokens[0]

    async def _vector(self,proof,operation,key,input_hash,tokens):
        identity=dict(codec='rkb.story-editorial-document.v1',person_id=proof['authority'].person_id,
            story_id=proof['record']['story_id'],story_revision=proof['record']['revision'],
            database_generation=proof['binding'].database_generation,body_sha256=digest(operation.body.encode()),
            input_sha256=input_hash,model_revision=REVISION,space=SPACE,tokenizer_sha256=TOKENIZER_SHA256)
        job_key='story-editorial-document:'+key
        job=await asyncio.to_thread(self.queue.lookup,proof['authority'].person_id,job_key)
        if job is None:
            if await asyncio.to_thread(self.queue.document_pending)>=64:raise failure('document_capacity')
            job_id=await asyncio.to_thread(self.queue.enqueue,proof['authority'].person_id,job_key,[operation.body],kind='document',identity=identity)
            job=await asyncio.to_thread(self.queue.lookup,proof['authority'].person_id,job_key)
        if job['identity']!=identity or job['kind']!='document':raise failure('idempotency_conflict','invalid_query')
        if job['state']!='done':
            self._release_document(job['id'])
            timer=asyncio.get_running_loop().call_later(2,self._release_document,job['id'])
            self._documents[job['id']]=dict(proof=proof,identity=identity,key=job_key,timer=timer)
            try:await self.queue.shared_reads.register_editorial_document(self,job['id'])
            except BaseException:self._release_document(job['id']);raise
            _LOG.info('story_editorial_document operation_hash=%s state=pending',digest(key.encode()))
            return None,tokens,True
        self._release_document(job['id'])
        value=job['result'];vectors=value.get('vectors')
        if value.get('space')!=SPACE or type(vectors) is not list or len(vectors)!=1:raise failure('document_vector_contract')
        raw=vectors[0]
        if type(raw) is not list or any(type(v) not in (int,float) or struct.unpack('<f',struct.pack('<f',float(v)))[0]!=v for v in raw):
            raise failure('exact_fp32_vector_required')
        return validate_vector(raw),tokens,False

    async def _entry(self,key):
        with self.spool._db() as db:
            row=db.execute('SELECT 1 FROM rkb_control_operations WHERE operation_key=?',(key,)).fetchone()
            compact=db.execute('SELECT 1 FROM rkb_control_receipts WHERE operation_key=?',(key,)).fetchone()
        if row:return await self.spool.get(key)
        if compact:raise failure('original_command_reconciliation_required')
        return None

    @staticmethod
    def _same_input(entry,request):
        c=entry.command;doc=json.loads(c.payload)
        actual=dict(story_id=c.ids['story_id'],expected_revision=int(c.expected_revisions['story_revision']),
            operation=doc['operation'],reason=doc['reason']) if c.route=='story.edit.v1' else dict(story_id=c.ids['story_id'],
            expected_revision=int(c.expected_revisions['story_revision']),grantee_user_id=doc['grantee_user_id'],capability=doc['capability'])
        if actual!=request:raise failure('idempotency_conflict','invalid_query')

    async def _continue(self,access,entry):
        await self._check(access['authority'],access['context'],access['binding'])
        if entry.accepted is None:
            head=await self.spool.journal.head()
            args=dict(writer_generation=self.startup.config.writer_generation)
            if (head.reference.last_sequence if head else 0)==entry.command.sequence-1:args['predecessor']=head
            elif entry.seal_state!='SENT_UNKNOWN':raise failure('prior_write_pending')
            await self.spool.seal(access['context'],entry.command.operation_key,**args)
            entry=await self.spool.get(entry.command.operation_key)
        if entry.outcome_state!='DURABLE':
            await self.startup.source_dispatch.enqueue(access['context'],entry.command.operation_key,
                writer_generation=self.startup.config.writer_generation)
        output=dict(status='completed' if entry.outcome_state=='DURABLE' and entry.apply_receipt.status=='applied' else
            'rejected_precondition' if entry.outcome_state=='DURABLE' else 'processing',story_id=entry.command.ids['story_id'],
            operation_id=entry.command.operation_key,remote_outcome_durable=entry.outcome_state=='DURABLE')
        if entry.outcome_state=='DURABLE':
            native=json.loads(entry.apply_receipt.outcome);body=json.loads(base64.b64decode(native['result_b64']))
            output.update({k:body[k] for k in ('revision','variant_id','variant_revision','body_sha256') if k in body})
        _LOG.info('story_editorial_operation operation_hash=%s sequence=%s state=%s',digest(entry.command.operation_key.encode()),entry.command.sequence,output['status'])
        return output

    async def mutate(self,principal,*,story_id,expected_revision,idempotency_key,operation=None,reason=None,
                     grantee_user_id=None,capability=None):
        try:
            if str(UUID(story_id))!=story_id or type(expected_revision) is not int or expected_revision<1 or type(idempotency_key) is not str or not 1<=len(idempotency_key)<=160:
                raise ValueError
            if operation is not None:
                if type(operation) is not UpsertVariant:raise ValueError
                operation=UpsertVariant.model_validate_json(canonical(operation.model_dump(mode='json')),strict=True)
                if reason is not None:_text(reason,3000)
                _text(operation.body,4000);_text(operation.audience,500);_text(operation.language,24)
                if operation.variant_id is not None:_uuid(operation.variant_id)
                for block in operation.blocks:
                    if block.assertion_id is not None:
                        _uuid(block.assertion_id)
                        if block.assertion_revision is None:raise ValueError
                    elif block.assertion_revision is not None:raise ValueError
            else:
                _uuid(grantee_user_id)
                if capability not in ROLES:raise ValueError
            request=dict(story_id=story_id,expected_revision=expected_revision,**(dict(operation=operation.model_dump(mode='json'),reason=reason)
                if operation is not None else dict(grantee_user_id=grantee_user_id,capability=capability)))
            async with self._access(principal) as access:
                access['story_id']=story_id;actor=access['authority'].person_id
                key='story-editorial:'+digest(canonical([actor,story_id,idempotency_key]));entry=await self._entry(key)
                if entry is not None:
                    if entry.command.person_id!=actor:raise PermissionError('original actor required')
                    self._same_input(entry,request)
                else:
                    proof=await self._preflight(principal,access);kept=False
                    try:
                        r=proof['record'];needed={'editor','publisher','manager'} if operation is not None else {'manager'}
                        if r['role'] not in needed:raise PermissionError('current per-Story editor/manager required')
                        if r['revision']!=expected_revision:raise failure('revision_conflict','invalid_query')
                        if operation is not None:
                            refs={x['variant_id']:x['revision'] for x in r['variant_refs']}
                            if operation.variant_id is not None and refs.get(operation.variant_id)!=operation.expected_variant_revision:
                                raise failure('variant_revision_conflict','invalid_query')
                            # Tokenizer-only process startup may take up to45s.
                            # Never extend or reuse the initial two-second read
                            # ticket across that work. Re-observe current native
                            # role/source/CAS before authorizing the exact job.
                            source=dict(r['source']);self._release_proof(proof);proof=None
                            tokens=await self._tokens(operation.body)
                            proof=await self._preflight(principal,access);r=proof['record']
                            if r['role'] not in needed:raise PermissionError('current per-Story editor required')
                            if r['revision']!=expected_revision:raise failure('revision_conflict','invalid_query')
                            if r['source']!=source:raise failure('source_context_changed')
                            refs={x['variant_id']:x['revision'] for x in r['variant_refs']}
                            if operation.variant_id is not None and refs.get(operation.variant_id)!=operation.expected_variant_revision:
                                raise failure('variant_revision_conflict','invalid_query')
                            vector,tokens,kept=await self._vector(proof,operation,key,digest(canonical(request)),tokens)
                            if vector is None:
                                output=dict(status='processing',reason='exact_narrative_vector_pending',story_id=story_id,operation_id=key,remote_outcome_durable=False)
                                access['retained']=await self.output_gate.retain(output,access['authority'],access['context'],access['binding'])
                                return output
                        async with self.spool._lock:
                            entry=await self._entry(key)
                            if entry is None:
                                await self._check(access['authority'],access['context'],access['binding'])
                                if not self.gate.issuance_current(proof['gated'].ticket):raise failure('source_context_changed')
                                scope=await self.spool.scope_state();head=await self.spool.journal.head()
                                sequence=head.reference.last_sequence if head else 0
                                facts=await self.spool.read_facts(None)
                                prefix=getattr(self.startup,'coverage',None)
                                applied_prefix=(prefix is not None and prefix is self.spool._source_anchor
                                    and prefix.binding==access['binding'] and prefix.current()
                                    and prefix.database_generation==access['binding'].database_generation
                                    and prefix.applied_sequence==prefix.accepted_sequence==sequence
                                    and self.spool._source_covered_sequence==sequence
                                    and prefix.head==head  # Includes the CAS token, not only the manifest.
                                    and self.spool.facts_current(facts))
                                if ((facts.pending and not applied_prefix) or scope['unknown']
                                        or scope['accepted_sequence']!=scope['sequence'] or sequence!=scope['accepted_sequence']):
                                    raise failure('prior_write_pending')
                                revisions=self._revisions(access['binding'])
                                builder=(resolved_current_story_edit_command if operation is not None else resolved_current_story_access_command) if revisions in (DOMAIN_REVISIONS,ACTIVATION_REVISIONS) else (
                                    resolved_story_edit_command if operation is not None else resolved_story_access_command)
                                c=builder(sequence=sequence+1,operation_key=key,request_id='editorial-'+str(uuid4()),person_id=actor,
                                    logical_time=datetime.now(timezone.utc).isoformat(),source_id=r['source']['source_id'],story_id=story_id,
                                    expected_revision=expected_revision,source_revision=r['source']['content_revision'],eligibility_revision=r['source']['eligibility_revision'],
                                    **(dict(operation=operation,reason=reason,embedding=vector,input_tokens=tokens) if operation is not None else dict(grantee_user_id=grantee_user_id,capability=capability)))
                                # Only this newly resolved intent is adapted; stored entries keep their original bytes.
                                if revisions==ACTIVATION_REVISIONS:c=new_native_command(c)
                                frozen,data,handler=self.spool._validate(c,access['binding'].database_generation)
                                await self.spool.authorize_command(access['context'],frozen)
                                entry=await self.spool._write(self.spool._stage,frozen,data,handler,access['binding'].database_generation)
                            self._same_input(entry,request)
                    finally:
                        if not kept and proof is not None:self._release_proof(proof)
                output=await self._continue(access,entry)
                access['retained']=await self.output_gate.retain(output,access['authority'],access['context'],access['binding'])
                return output
        except ToolError:raise
        except PermissionError:raise failure('access_denied','denied') from None
        except TimeoutError:raise failure('deadline_exceeded') from None
        except (ValueError,TypeError,KeyError):raise failure('invalid_input','invalid_query') from None

    def _revisions(self,binding):
        with self.queue.connect(write=False) as db:
            row=self.queue._runtime_row(db,binding.run_id)
            value=self.queue._runtime_expected(row)
            if (row['accepted_generation']!=binding.connection_generation
                    or value.database_generation!=binding.database_generation):raise failure('target_runtime_changed')
        if value.schema_version>=14:return ACTIVATION_REVISIONS
        if value.schema_version>=11:return DOMAIN_REVISIONS
        if value.schema_version>=9:return EDITORIAL_REVISIONS
        raise failure('unsupported_editorial_schema')

    async def aclose(self):
        for job in tuple(self._documents):self._release_document(job)
        if self.queue._story_editorial is self:self.queue._story_editorial=None
