"""Native Case product operations on the existing queue, authority and spool."""
from __future__ import annotations

import asyncio
import base64
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime,timezone
import hashlib
import json
import secrets
import time
from uuid import UUID,uuid5

from mcp.server.mcpserver.exceptions import ToolError
from .case_read_gate import CaseReadGate
from .contracts import Principal
from .control_spool import SpoolError
from .disagreement_native import CASE_REVISIONS,resolved_case_command,validate_case_payload
from .disagreement_contracts import (CaseCreate,CaseRevise,CaseState,CaseGrant,EvidenceAdd,ContributionAdd,
    ContributionCorrect,ConclusionAdd,PersonalDraft,DraftPublication)
from .kaggle_case_contracts import CaseReadRequest,canonical,exact_uuid
from .kaggle_domain_bindings import DOMAIN_REVISIONS,resolved_current_case_command
from .source_activation import ACTIVATION_REVISIONS
from .kaggle_native_source_bindings import new_native_command,required_native_source_capabilities
from .kaggle_protocol import TrustedActorContext
from .kaggle_public_backend import current_binding
from .participants import RequestAuthority,ParticipantError
from .read_issuance import retain_read_output
from .source_read_gate import GatedResult

_NAMESPACE=UUID('91db58b9-e2d1-42b6-9cd6-eb788c05a46b')
_TYPES=dict(create=CaseCreate,revise=CaseRevise,state=CaseState,grant=CaseGrant,add_evidence=EvidenceAdd,
    contribute=ContributionAdd,correct_contribution=ContributionCorrect,conclude=ConclusionAdd,
    draft_save=PersonalDraft,draft_publish=DraftPublication)


def failure(reason,status='unavailable'):
    return ToolError(json.dumps(dict(status=status,reason=reason)))


@dataclass(frozen=True,eq=False)
class _OperationOutput:
    result:dict
    ticket:object


class CaseOperationGate:
    def __init__(self,backend):self.backend=backend;self._tickets={}

    async def retain(self,result,authority,context,binding,command):
        if len(self._tickets)>=512:raise failure('output_capacity')
        witness=await self.backend._check(authority,context,binding,command)
        ticket=object();self._tickets[ticket]=(result,authority,context,binding,command,witness)
        gated=_OperationOutput(result,ticket)
        try:retained=retain_read_output(self,gated,lambda value:value)
        except BaseException:
            self.release(ticket)
            raise
        if not retained:
            self.release(ticket);raise failure('public_output_boundary_required')
        return result

    async def refresh(self,gated):
        retained=self._tickets.get(gated.ticket)
        if retained is None or retained[0] is not gated.result:raise PermissionError('unknown Case operation output')
        witness=await self.backend._check(*retained[1:5])
        self._tickets[gated.ticket]=(*retained[:5],witness)
        return gated

    def issuance_current(self,ticket):
        value=self._tickets.get(ticket)
        if value is None:return False
        _,authority,context,binding,command,witness=value
        resolver=self.backend.read_authority
        try:
            if not self.backend._binding_current(binding) or authority.expires_at<=resolver.clock():return False
            with resolver._contexts_lock:
                if resolver._contexts.get(authority.context_ref) is not authority:return False
            with resolver._read() as db:
                current=resolver._state(context.principal,context.route,context.link_id,context.link_revision,
                    authority.oauth_family_ref,connection=db)
                # Same fixed command capability set, with no owner fallback for
                # Case ACL/draft authorship. No awaits after this final check.
                from .disagreement_native import required_capabilities
                from .kaggle_domain_bindings import _view
                if command.schema_revision==ACTIVATION_REVISIONS.schema:
                    capabilities=required_native_source_capabilities(command)
                else:
                    original=_view(command,CASE_REVISIONS) if command.schema_revision==DOMAIN_REVISIONS.schema else command
                    capabilities=required_capabilities(original)
                for capability in capabilities:
                    self.backend.participants.require(context,capability,connection=db)
            self.backend._scope(authority,command)
            spool=self.backend.spool
            return (current.revision==authority.auth_revision and not spool._writing
                and witness[0] is spool._source_anchor and witness[1] is spool._source_version)
        except Exception:return False

    def release(self,ticket):
        value=self._tickets.pop(ticket,None)
        if value is not None:self.backend.read_authority.release(value[1])


class KaggleCasesBackend:
    current_read_output_required=True

    def __init__(self,startup,*,case_gate):
        config=startup.config
        if (type(case_gate) is not CaseReadGate or case_gate.spool is not config.spool
                or case_gate.authority is not startup.authority or config.queue.shared_reads.authority is not startup.authority):
            raise TypeError('same canonical Case authority, spool, queue and admission required')
        self.startup=startup;self.queue=config.queue;self.spool=config.spool;self.participants=config.participants
        self.read_authority=startup.authority;self.gate=case_gate;self.destination=startup.backend.destination
        self.output_gate=CaseOperationGate(self)

    def _binding_current(self,binding):
        # Case actor rights are checked in PG; unrelated history refresh is
        # not an availability condition for a current PG Case binding.
        return self.startup.binding_current(binding)

    async def _native(self,authority,binding,*,deadline=None,**selector):
        deadline=min(deadline,time.monotonic()+2) if deadline is not None else time.monotonic()+2
        permit=ticket=gated=None
        try:
            async with asyncio.timeout_at(deadline):
                query=CaseReadRequest(request_id='case-'+secrets.token_hex(16),connection_generation=binding.connection_generation,
                    turn_generation=0,min_data_seq=(binding.min_data_seq if selector.get('section')=='operation' else 0),
                    deadline_ms=max(1,int((deadline-time.monotonic())*1000)),
                    actor=TrustedActorContext(person_id=authority.person_id,audience=self.destination.key,
                        auth_epoch=authority.auth_revision,visibility_epoch=0,context_ref=authority.context_ref),**selector)
                permit=await self.gate.dispatch(authority,query,binding)
                ticket=await self.queue.shared_reads.begin_read(authority,permit.query,dialog_ref=self.destination.key,deadline=deadline)
                result=await asyncio.shield(ticket.result)
                gated=await self.gate.finish(permit,result);permit=None
                if gated.result.status!='ok':raise failure(gated.result.reason,gated.result.status)
                return gated
        except BaseException:
            if gated is not None:self.gate.release(gated.ticket)
            raise
        finally:
            if ticket is not None:self.queue.shared_reads.cancel_read(ticket)
            if permit is not None:self.gate.abandon(permit)

    @staticmethod
    def _render(result):
        return {**(result.record or {}),'status':result.status,'reason':result.reason,
            'observed_data_seq':result.observed_data_seq,'observed_projection_seq':result.observed_projection_seq}

    def _render_case(self,result):
        # Native immutable contribution records stay exactly as signed by PG.
        # The owner's later mistaken-account correction is a *current* policy
        # annotation, recomputed on the final MCP issuance after gate refresh.
        output=self._render(result)
        if output.get('section')!='contributions':return output
        items=output.get('items')
        if type(items) is not list or len(items)>20:
            raise failure('invalid_case_page')
        references=[]
        for item in items:
            attribution=item['payload']['attribution']
            if attribution['route']=='expert':
                references.append((item['actor_id'],attribution['link_id'],attribution['link_revision']))
        try:flags=self.participants.historical_attribution_review(tuple(references))
        except ParticipantError:raise failure('case_attribution_unavailable') from None
        for item in items:
            attribution=item['payload']['attribution']
            ref=(item['actor_id'],attribution['link_id'],attribution['link_revision'])
            item['attribution_review_required']=flags.get(ref,False)
        return output

    async def _read(self,principal,**selector):
        deadline=time.monotonic()+2
        authority=gated=None;retained=False
        try:
            async with asyncio.timeout_at(deadline):
                # PG owns the current Case role/draft ACL. Do not wait for the
                # unrelated global source-history maintenance proof.
                authority=await self.read_authority.resolve_oauth(principal.access_token,destination=self.destination)
                binding=await asyncio.to_thread(current_binding,self.queue)
                gated=await self._native(authority,binding,deadline=deadline,**selector)
                output=self._render_case(gated.result)
                if not retain_read_output(self.gate,gated,self._render_case,on_release=lambda:self.read_authority.release(authority)):
                    raise failure('public_output_boundary_required')
                retained=True;return output
        except ToolError:raise
        except TimeoutError:raise failure('deadline_exceeded') from None
        except PermissionError:raise failure('access_denied','denied') from None
        except (ValueError,TypeError):raise failure('invalid_input','invalid_query') from None
        finally:
            if not retained:
                if gated is not None:self.gate.release(gated.ticket)
                if authority is not None:self.read_authority.release(authority)

    def _scope(self,authority,command):
        doc=validate_case_payload(command.payload)
        if doc['action']=='create':
            anchor=self.spool._source_anchor
            state=getattr(anchor,'case_state',None)
            if state is not None and command.ids['case_id'] in state.cases:
                if state.cases[command.ids['case_id']]['owner']!=authority.person_id:raise PermissionError('Case creator differs')
            return
        selector=CaseReadRequest(request_id='case-operation-check',connection_generation=1,turn_generation=0,
            case_id=command.ids['case_id'],section='draft_get' if doc['action'] in ('draft_save','draft_publish') else 'case',
            draft_id=command.ids['object_id'] if doc['action'] in ('draft_save','draft_publish') else None,
            actor=TrustedActorContext(person_id=authority.person_id,audience=self.destination.key,auth_epoch=authority.auth_revision,
                visibility_epoch=0),min_data_seq=0,deadline_ms=1)
        # A new own draft may have no head yet. Case access is checked separately;
        # an existing draft must always belong to this exact actor.
        if doc['action']=='draft_save' and command.expected_revisions['object_version']=='0':
            selector=selector.model_copy(update={'section':'case','draft_id':None})
        anchor,case,role=self.gate._policy(selector,authority)
        if anchor is None:raise failure('case_history_unavailable')
        minimum=1 if doc['action']=='draft_save' else 4 if doc['action']=='grant' else 3 if doc['action'] in ('revise','state') or doc['action']=='conclude' and doc['command']['make_current'] else 2
        if role<minimum:raise PermissionError('current Case role required')

    async def _check(self,authority,context,binding,command):
        await self.read_authority.require(authority,'case_read')
        if not self._binding_current(binding):raise failure('target_runtime_changed')
        await self.spool.authorize_command(context,command)
        self._scope(authority,command)
        anchor=self.spool._source_anchor
        version=self.spool._source_version
        # A native/local grant change beyond this verified projection closes
        # operation receipts too. An old Case grant cannot survive until the
        # next periodic history refresh, including after its outcome is durable.
        def restriction_pending():
            with self.spool._db() as db:
                row=db.execute("SELECT EXISTS(SELECT 1 FROM rkb_control_operations WHERE source_id=? AND sequence>? "
                    "AND operation_key<>? AND json_extract(CAST(dependency_bytes AS TEXT),'$.restriction')=1)",
                    ('case:'+command.ids['case_id'],getattr(anchor,'applied_sequence',0),command.operation_key)).fetchone()
                return bool(row[0])
        from .control_spool import _database,_command
        if await _database(restriction_pending):raise failure('prior_write_pending')
        for raw,_,_ in getattr(anchor,'pending',()):
            pending=_command(raw)
            if (pending.route=='disagreement.access.v1' and pending.ids['case_id']==command.ids['case_id']
                    and pending.operation_key!=command.operation_key):raise failure('prior_write_pending')
        state=getattr(anchor,'case_state',None)
        case=state.cases.get(command.ids['case_id']) if state is not None else None
        if case is not None:
            doc=validate_case_payload(command.payload)
            if (doc['action']=='grant' and doc['command']['grantee']==authority.person_id
                    and doc['command']['capability']!='manager' and case['owner']!=authority.person_id
                    and command.sequence>getattr(anchor,'applied_sequence',0)):
                raise failure('prior_write_pending')
            query=CaseReadRequest(request_id='case-write-check',connection_generation=binding.connection_generation,turn_generation=0,
                case_id=command.ids['case_id'],actor=TrustedActorContext(person_id=authority.person_id,
                    audience=self.destination.key,auth_epoch=authority.auth_revision,visibility_epoch=0),min_data_seq=0,deadline_ms=1)
            # Existing Case ACL is actor-scoped. Only restrictions on this
            # Case or its accepted source dependencies may stop the write;
            # an unrelated incoming page does not make the Case stale.
            facts=await self.gate._facts(query,authority,anchor,case)
            # Even if a grant/revoke is staged WHILE reading source facts,
            # its exact Case restriction must close this writer request.
            if self.gate._case_restriction_after(command.ids['case_id'],getattr(anchor,'applied_sequence',0)):
                raise failure('prior_write_pending')
            if any(change.restriction for change in facts.sources.changes):
                raise failure('evidence_withdrawn')
        if (self.spool._writing or self.spool._source_anchor is not anchor
                or self.spool._source_version is not version):raise failure('prior_write_pending')
        return anchor,version

    @staticmethod
    def _key(actor,operation_key):
        if type(operation_key) is not str or not 1<=len(operation_key.encode())<=1024:raise ValueError('bounded operation key required')
        prefix='case-op:'+actor+':'
        suffix=operation_key.removeprefix(prefix)
        if operation_key.startswith(prefix) and len(suffix)==64 and not set(suffix)-set('0123456789abcdef'):
            return operation_key
        return 'case-op:'+actor+':'+hashlib.sha256(operation_key.encode()).hexdigest()

    async def _entry(self,key):
        try:return await self.spool.get(key)
        except SpoolError as exc:
            if str(exc)=='control operation not found':return None
            raise

    async def _collected(self,key):
        # Terminal compact receipts outlive the short raw intent spool.
        # Do not re-stage a collected operation under the same key.
        from .control_spool import _database
        def present():
            with self.spool._db() as db:
                return db.execute('SELECT 1 FROM rkb_control_receipts WHERE operation_key=?',(key,)).fetchone() is not None
        return await _database(present)

    @staticmethod
    def _reply(entry):
        command=entry.command
        result=dict(status='pending',reason='accepted_apply_or_remote_outcome_pending',operation_key=command.operation_key,
            case_id=command.ids['case_id'],object_id=command.ids['object_id'],sequence=command.sequence,
            command_sha256=entry.command_sha256,intent_remote_accepted=entry.accepted is not None,
            outcome_durability=entry.outcome_state,saved=False)
        if entry.outcome_state=='DURABLE' and entry.apply_receipt is not None:
            applied=entry.apply_receipt
            outcome=json.loads(applied.outcome)
            if (outcome['command_sha256']!=entry.command_sha256 or outcome['person_id']!=command.person_id
                    or outcome['status']!=applied.status):raise ValueError('exact Case outcome differs')
            result.update(status='saved' if applied.status=='applied' else 'rejected',reason=None,
                saved=applied.status=='applied',result=json.loads(base64.b64decode(outcome['result_b64'],validate=True)))
        return result

    @staticmethod
    def _verify_collected_retry(command,key,action,model,case_id,object_id,case_revision,object_version):
        """Compare the entire caller-controlled CAS envelope after raw GC."""
        original=validate_case_payload(command.payload)
        wanted_object=(case_id if action=='create' else
            exact_uuid(str(object_id)) if object_id is not None else str(uuid5(_NAMESPACE,key+':object')))
        if (action not in _TYPES or type(model) is not _TYPES[action]
                or original['action']!=action or original['command']!=model.model_dump(mode='json')
                or command.ids['case_id']!=case_id or command.ids['object_id']!=wanted_object
                or int(command.expected_revisions['case_revision'])!=case_revision
                or int(command.expected_revisions['object_version'])!=object_version):
            raise failure('operation_key_conflict','invalid_query')

    async def _recover_operation(self,authority,context,binding,key,case_id,*,expected_action=None,
                                 expected_command=None,expected_object_id=None,expected_case_revision=0,
                                 expected_object_version=0):
        """Read a collected intent; native-only or local-only evidence is not Saved.

        The compact remote receipt outlives raw spool bytes. Re-read its exact
        immutable journal objects and compare the native actor-scoped receipt;
        never stage, seal, publish or enqueue an operation on this path.
        """
        from .control_spool import _database,_command
        from .recovery_journal import RemoteOutcomeReceipt
        # CREATE resolves its case ID before durable acceptance from the
        # actor-bound operation key. A caller that lost the create response
        # cannot supply case_id after raw spool GC: reconstruct only this
        # original CREATE locator, then prove it against the native PG receipt,
        # the immutable command and the remotely sealed outcome below.
        inferred_create=case_id is None
        case_id=(str(uuid5(_NAMESPACE,key+':case')) if inferred_create
                 else exact_uuid(str(case_id)))
        gated=None
        def compact():
            with self.spool._db() as db:
                row=db.execute('SELECT command_sha256,receipt_bytes FROM rkb_control_receipts WHERE operation_key=?',(key,)).fetchone()
                return (row[0],bytes(row[1])) if row is not None else None
        try:
            async with asyncio.timeout(2):
                gated=await self._native(authority,binding,case_id=case_id,section='operation',operation_key=key)
                native=gated.result.record
                retained=await _database(compact)
                if retained is None:raise failure('remote_outcome_unproved')
                command_hash,receipt_bytes=retained
                remote=RemoteOutcomeReceipt(**json.loads(receipt_bytes))
                journal=self.spool.journal
                if (canonical(remote.__dict__)!=receipt_bytes or remote.operation_key!=key
                        or remote.command_sha256!=command_hash or native['command_sha256']!=command_hash
                        or remote.key!=journal.prefix+'/outcomes/'+hashlib.sha256(key.encode()).hexdigest()
                        or remote.database_generation!=binding.database_generation
                        or type(remote.length) is not int or not 1<=remote.length<=journal.max_outcome_bytes):
                    raise failure('remote_outcome_unproved')
                original=await journal._read(journal.prefix+'/commands/'+hashlib.sha256(key.encode()).hexdigest(),journal.max_segment_bytes)
                if original is None or hashlib.sha256(original.data).hexdigest()!=command_hash:
                    raise failure('remote_outcome_unproved')
                command=_command(original.data)
                if (command.operation_key!=key or command.person_id!=authority.person_id
                        or command.ids.get('case_id')!=case_id or not command.route.startswith('disagreement.')
                        or inferred_create and validate_case_payload(command.payload)['action']!='create'
                        or native['sequence']!=command.sequence or native['request_id']!=command.request_id
                        or native['state']!='applied' or native['outcome_durability']!='native_only'):
                    raise failure('remote_outcome_unproved')
                if expected_action is not None:
                    self._verify_collected_retry(command,key,expected_action,expected_command,case_id,
                        expected_object_id,expected_case_revision,expected_object_version)
                # Fixed historical command semantics and current route/actor
                # authority, including private draft authorship, are rechecked.
                await self._check(authority,context,binding,command)
                stored=await journal._read(remote.key,journal.max_outcome_bytes)
                if stored is None or len(stored.data)!=remote.length:raise failure('remote_outcome_unproved')
                envelope=json.loads(stored.data)
                outcome=base64.b64decode(envelope['outcome_b64'],validate=True)
                expected=dict(codec='rkb.outcome.v1',stream=journal.stream,command_sha256=command_hash,
                    sequence=command.sequence,operation_key=key,database_generation=binding.database_generation,
                    outcome_b64=base64.b64encode(outcome).decode(),outcome_sha256=remote.outcome_sha256,outcome_length=len(outcome))
                if canonical(expected)!=stored.data or hashlib.sha256(outcome).hexdigest()!=remote.outcome_sha256:
                    raise failure('remote_outcome_unproved')
                applied=json.loads(outcome)
                result_bytes=base64.b64decode(applied['result_b64'],validate=True)
                versions=(command.schema_revision,command.function_revision,command.executor_revision)
                expected_applied=dict(codec='rkb.applied-outcome.v1',operation_key=key,command_sha256=command_hash,
                    cursor=command.sequence,status='applied',ids=dict(command.ids),request_id=command.request_id,
                    person_id=authority.person_id,route=command.route,logical_time=command.logical_time,
                    before=list(versions),after=list(versions),handler_sha256=applied['handler_sha256'],
                    result_b64=base64.b64encode(result_bytes).decode(),result_sha256=hashlib.sha256(result_bytes).hexdigest())
                if (canonical(expected_applied)!=outcome
                        or type(applied['handler_sha256']) is not str or len(applied['handler_sha256'])!=64
                        or set(applied['handler_sha256'])-set('0123456789abcdef')
                        or json.loads(result_bytes)!=native['result']):
                    raise failure('remote_outcome_unproved')
                gated=await self.gate.refresh(gated)
                if gated.result.status!='ok':raise failure(gated.result.reason,gated.result.status)
                output=dict(status='saved',reason=None,operation_key=key,case_id=case_id,
                    object_id=command.ids['object_id'],sequence=command.sequence,command_sha256=command_hash,
                    intent_remote_accepted=True,outcome_durability='DURABLE',saved=True,result=native['result'])
                await self.output_gate.retain(output,authority,context,binding,command)
                return output
        except TimeoutError:raise failure('deadline_exceeded') from None
        except (ValueError,TypeError,KeyError):raise failure('remote_outcome_unproved') from None
        finally:
            if gated is not None:self.gate.release(gated.ticket)

    async def _mutate(self,principal,action,operation_key,command=None,*,case_id=None,object_id=None,
                      case_revision=0,object_version=0,status_only=False):
        authority=context=binding=entry=gated=None;retained=False;collected=False
        try:
            authority=await self.read_authority.resolve_oauth(principal.access_token,destination=self.destination)
            await self.read_authority.require(authority,'case_read')
            if authority.public_only or authority.route not in ('owner','personal','expert'):raise PermissionError('personal Case authority required')
            context=RequestAuthority(Principal(subject=authority.person_id,client_id=authority.client_id,issuer=authority.issuer,access_token=''),
                route=authority.route,link_id=authority.link_id,link_revision=authority.link_revision)
            binding=await asyncio.to_thread(current_binding,self.queue)
            key=self._key(authority.person_id,operation_key);entry=await self._entry(key)
            if entry is None:
                if status_only:
                    result=await self._recover_operation(authority,context,binding,key,case_id)
                    retained=True;return result
                if action not in _TYPES or type(command) is not _TYPES[action]:raise ValueError('exact Case command type required')
                if await self._collected(key):
                    result=await self._recover_operation(authority,context,binding,key,case_id,
                        expected_action=action,expected_command=command,expected_object_id=object_id,
                        expected_case_revision=case_revision,expected_object_version=object_version)
                    retained=True;return result
                if case_id is None:
                    if action!='create':raise ValueError('existing Case UUID required')
                    case_id=str(uuid5(_NAMESPACE,key+':case'))
                else:case_id=exact_uuid(str(case_id))
                object_id=case_id if action=='create' else exact_uuid(str(object_id)) if object_id is not None else str(uuid5(_NAMESPACE,key+':object'))
                dependencies=[]
                if action!='create':
                    gated=await self._native(authority,binding,case_id=case_id,section='edit_context',
                        candidate_source_id=str(command.document_id) if action=='add_evidence' else None)
                    prepared=gated.result.record;dependencies=prepared['source_dependencies']
                    if action=='add_evidence':
                        candidate=prepared['candidate_dependency'];dependencies=sorted(
                            {d['source_id']:d for d in [*dependencies,candidate]}.values(),key=lambda d:d['source_id'])
                    gated=await self.gate.refresh(gated)
                    if gated.result.status!='ok':raise failure(gated.result.reason)
                async with self.spool._lock:
                    entry=await self._entry(key)
                    if entry is None and await self._collected(key):
                        collected=True
                    if entry is None and not collected:
                        scope=await self.spool.scope_state()
                        # An already accepted unrelated page may be awaiting
                        # PG/Dataset outcome. Journal predecessor proof permits
                        # the next Case intent, not a fictitious applied/Saved
                        # receipt. UNKNOWN or an unaccepted seal still blocks.
                        if scope['unknown'] or scope['sequence']!=scope['accepted_sequence']:
                            raise failure('prior_write_pending')
                        head=await self.spool.journal.head();sequence=head.reference.last_sequence if head else 0
                        if sequence!=scope['accepted_sequence']:raise failure('case_history_unavailable')
                        revisions={h.revisions for h in self.spool.registry.handlers.values() if h.route.startswith('disagreement.')}
                        if revisions in ({DOMAIN_REVISIONS},{ACTIVATION_REVISIONS}):builder=resolved_current_case_command
                        elif revisions=={CASE_REVISIONS}:builder=resolved_case_command
                        else:raise failure('case_handler_versions_unavailable')
                        resolved=builder(sequence=sequence+1,operation_key=key,request_id='case-'+secrets.token_hex(16),
                            person_id=authority.person_id,logical_time=datetime.now(timezone.utc).isoformat(),case_id=case_id,
                            object_id=object_id,action=action,command=command,source_dependencies=dependencies,
                            case_revision=case_revision,object_version=object_version,
                            attribution=dict(person_id=authority.person_id,recorded_by=context.executor_id or context.principal.subject,
                                route=context.route,link_id=context.link_id,link_revision=context.link_revision,human_reading_confirmed=False))
                        # Adapt a newly resolved intent only; accepted replay entries are never relabelled.
                        if revisions=={ACTIVATION_REVISIONS}:resolved=new_native_command(resolved)
                        await self._check(authority,context,binding,resolved)
                        if gated is not None and not self.gate.issuance_current(gated.ticket):raise failure('prior_write_pending')
                        frozen,data,handler=self.spool._validate(resolved,binding.database_generation)
                        entry=await self.spool._write(self.spool._stage,frozen,data,handler,binding.database_generation)
                if collected:
                    result=await self._recover_operation(authority,context,binding,key,case_id,
                        expected_action=action,expected_command=command,expected_object_id=object_id,
                        expected_case_revision=case_revision,expected_object_version=object_version)
                    retained=True;return result
            if status_only and case_id is not None and entry.command.ids['case_id']!=exact_uuid(str(case_id)):
                raise failure('operation_key_conflict','invalid_query')
            if not status_only:
                original=validate_case_payload(entry.command.payload)
                if (original['action']!=action or original['command']!=json.loads(command.model_dump_json())
                        or case_id is not None and entry.command.ids['case_id']!=str(case_id)
                        or object_id is not None and entry.command.ids['object_id']!=str(object_id)
                        or int(entry.command.expected_revisions['case_revision'])!=case_revision
                        or int(entry.command.expected_revisions['object_version'])!=object_version):
                    raise failure('operation_key_conflict','invalid_query')
            await self._check(authority,context,binding,entry.command)
            if entry.accepted is None:
                if entry.seal_state=='SENT_UNKNOWN':
                    await self.spool.seal(context,key,writer_generation=self.startup.config.writer_generation)
                else:
                    head=await self.spool.journal.head()
                    if (head.reference.last_sequence if head else 0)!=entry.command.sequence-1:
                        raise failure('prior_write_pending')
                    await self.spool.seal(context,key,writer_generation=self.startup.config.writer_generation,predecessor=head)
                entry=await self.spool.get(key)
            if entry.outcome_state!='DURABLE':
                await self.startup.source_dispatch.enqueue(context,key,writer_generation=self.startup.config.writer_generation)
                entry=await self.spool.get(key)
            result=self._reply(entry)
            await self.output_gate.retain(result,authority,context,binding,entry.command)
            retained=True;return result
        except ToolError:raise
        except PermissionError:raise failure('access_denied','denied') from None
        except (ValueError,TypeError):raise failure('invalid_input','invalid_query') from None
        except Exception:raise failure('case_operation_pending' if entry is not None else 'not_ready') from None
        finally:
            if gated is not None:self.gate.release(gated.ticket)
            if authority is not None and not retained:self.read_authority.release(authority)

    async def disagreement_search(self,query,principal,*,place=None,source_id=None,after_case_id=None,limit=5):
        # Model interprets Case identity; this returns only bounded
        # actor-authorized question/place/source candidate records.
        if (type(query) is not str or len(query)>120
                or not any((query.strip(),place,source_id))
                or type(limit) is not int or not 1<=limit<=8):
            raise failure('invalid_input','invalid_query')
        return await self._read(principal,case_id='00000000-0000-0000-0000-000000000000',
            section='search',search_query=query.strip(),place=place,
            source_id=source_id,after_case_id=after_case_id,limit=limit)

    async def disagreement_get(self,case_id,principal,*,section='case',after_seq=0,limit=20,revision=None):
        return await self._read(principal,case_id=str(case_id),section=section,after_seq=after_seq,limit=limit,revision=revision)

    async def disagreement_edit(self,action,operation_key,principal,*,case_id=None,command=None,expected_revision=None):
        if action=='operation_status':return await self._mutate(principal,action,operation_key,case_id=case_id,status_only=True)
        if (action=='create' and (case_id is not None or expected_revision is not None)
                or action!='create' and case_id is None
                or action in ('revise','state','grant') and expected_revision is None):
            raise failure('invalid_input','invalid_query')
        return await self._mutate(principal,action,operation_key,command,case_id=case_id,
            case_revision=(expected_revision or 0) if action in ('revise','state','grant') else 0)

    async def disagreement_contribute(self,case_id,operation_key,command,principal,*,contribution_id=None,expected_version=None):
        action='contribute' if type(command) is ContributionAdd else 'correct_contribution'
        if action=='contribute' and (contribution_id is not None or expected_version is not None):raise failure('invalid_input','invalid_query')
        if action=='correct_contribution' and (contribution_id is None or expected_version is None):raise failure('invalid_input','invalid_query')
        return await self._mutate(principal,action,operation_key,command,case_id=case_id,object_id=contribution_id,object_version=expected_version or 0)

    async def disagreement_conclusion(self,case_id,action,principal,*,operation_key=None,command=None,expected_revision=None,
                                     draft_id=None,expected_version=None,version=None,after_id=None,limit=20):
        if action in ('draft_get','draft_list'):
            return await self._read(principal,case_id=str(case_id),section=action,draft_id=str(draft_id) if draft_id is not None else None,
                draft_version=version,after_draft=str(after_id) if after_id is not None else None,limit=limit)
        if action=='conclude':return await self._mutate(principal,action,operation_key,command,case_id=case_id,case_revision=expected_revision or 0)
        if action in ('draft_save','draft_publish') and draft_id is not None and expected_version is not None:
            return await self._mutate(principal,action,operation_key,command,case_id=case_id,object_id=draft_id,object_version=expected_version)
        raise failure('invalid_input','invalid_query')
