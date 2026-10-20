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
from .kaggle_protocol import TrustedActorContext
from .kaggle_public_backend import current_binding
from .participants import RequestAuthority
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
                original=_view(command,CASE_REVISIONS) if command.schema_revision==DOMAIN_REVISIONS.schema else command
                for capability in required_capabilities(original):
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
        return self.startup.binding_current(binding) and self.startup.reads_current()

    async def _native(self,authority,binding,*,deadline=None,**selector):
        deadline=min(deadline,time.monotonic()+2) if deadline is not None else time.monotonic()+2
        permit=ticket=gated=None
        try:
            async with asyncio.timeout_at(deadline):
                query=CaseReadRequest(request_id='case-'+secrets.token_hex(16),connection_generation=binding.connection_generation,
                    turn_generation=0,min_data_seq=binding.min_data_seq,deadline_ms=max(1,int((deadline-time.monotonic())*1000)),
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

    async def _read(self,principal,**selector):
        deadline=time.monotonic()+2
        authority=gated=None;retained=False
        try:
            async with asyncio.timeout_at(deadline):
                if not self.startup.reads_current():raise failure('case_history_unavailable')
                authority=await self.read_authority.resolve_oauth(principal.access_token,destination=self.destination)
                binding=await asyncio.to_thread(current_binding,self.queue)
                gated=await self._native(authority,binding,deadline=deadline,**selector)
                output=self._render(gated.result)
                if not retain_read_output(self.gate,gated,self._render,on_release=lambda:self.read_authority.release(authority)):
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
        minimum=4 if doc['action']=='grant' else 3 if doc['action'] in ('revise','state') or doc['action']=='conclude' and doc['command']['make_current'] else 2
        if role<minimum:raise PermissionError('current Case role required')

    async def _check(self,authority,context,binding,command):
        await self.read_authority.require(authority,'case_read')
        if not self._binding_current(binding):raise failure('target_runtime_changed')
        await self.spool.authorize_command(context,command)
        self._scope(authority,command)
        anchor=self.spool._source_anchor
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
            facts=await self.gate._facts(query,authority,anchor,case)
            if not self.gate._current(facts):raise failure('case_history_unavailable')
            if any(change.restriction for change in facts.sources.changes):raise failure('evidence_withdrawn')
        if self.spool._writing:raise failure('prior_write_pending')
        return self.spool._source_anchor,self.spool._source_version

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

    async def _mutate(self,principal,action,operation_key,command=None,*,case_id=None,object_id=None,
                      case_revision=0,object_version=0,status_only=False):
        authority=context=binding=entry=gated=None;retained=False
        try:
            if not self.startup.reads_current():raise failure('case_history_unavailable')
            authority=await self.read_authority.resolve_oauth(principal.access_token,destination=self.destination)
            await self.read_authority.require(authority,'case_read')
            if authority.public_only or authority.route not in ('owner','personal','expert'):raise PermissionError('personal Case authority required')
            context=RequestAuthority(Principal(subject=authority.person_id,client_id=authority.client_id,issuer=authority.issuer,access_token=''),
                route=authority.route,link_id=authority.link_id,link_revision=authority.link_revision)
            binding=await asyncio.to_thread(current_binding,self.queue)
            key=self._key(authority.person_id,operation_key);entry=await self._entry(key)
            if entry is None:
                if status_only:raise failure('operation_not_found','invalid_query')
                if action not in _TYPES or type(command) is not _TYPES[action]:raise ValueError('exact Case command type required')
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
                    if entry is None:
                        scope=await self.spool.scope_state()
                        if scope['pending'] or scope['unknown'] or scope['sequence']!=scope['accepted_sequence']:raise failure('prior_write_pending')
                        head=await self.spool.journal.head();sequence=head.reference.last_sequence if head else 0
                        if sequence!=scope['accepted_sequence']:raise failure('case_history_unavailable')
                        revisions={h.revisions for h in self.spool.registry.handlers.values() if h.route.startswith('disagreement.')}
                        if revisions=={DOMAIN_REVISIONS}:builder=resolved_current_case_command
                        elif revisions=={CASE_REVISIONS}:builder=resolved_case_command
                        else:raise failure('case_handler_versions_unavailable')
                        resolved=builder(sequence=sequence+1,operation_key=key,request_id='case-'+secrets.token_hex(16),
                            person_id=authority.person_id,logical_time=datetime.now(timezone.utc).isoformat(),case_id=case_id,
                            object_id=object_id,action=action,command=command,source_dependencies=dependencies,
                            case_revision=case_revision,object_version=object_version,
                            attribution=dict(person_id=authority.person_id,recorded_by=context.executor_id or context.principal.subject,
                                route=context.route,link_id=context.link_id,link_revision=context.link_revision,human_reading_confirmed=False))
                        await self._check(authority,context,binding,resolved)
                        if gated is not None and not self.gate.issuance_current(gated.ticket):raise failure('prior_write_pending')
                        frozen,data,handler=self.spool._validate(resolved,binding.database_generation)
                        entry=await self.spool._write(self.spool._stage,frozen,data,handler,binding.database_generation)
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

    async def disagreement_get(self,case_id,principal,*,section='case',after_seq=0,limit=20,revision=None):
        return await self._read(principal,case_id=str(case_id),section=section,after_seq=after_seq,limit=limit,revision=revision)

    async def disagreement_edit(self,action,operation_key,principal,*,case_id=None,command=None,expected_revision=None):
        if action=='operation_status':return await self._mutate(principal,action,operation_key,status_only=True)
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
