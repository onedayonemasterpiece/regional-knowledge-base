"""Internal RAM admission owned by BgeQueue's existing WSS controller.

No model, dispatcher, transport, or caller identity authority lives here. One
actual compute claim spans ephemeral QUERY and the existing durable job queue.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
import fcntl
import math
import logging
import os
import secrets
import time
from types import MappingProxyType

from .bge_queue import StaleAttachment
from .kaggle_protocol import Query, QueryResult, PassageReadRequest, PassageReadResult, StoryReadRequest, StoryReadResult, encode_envelope
from .kaggle_protocol import SqlHealthRequest, SqlHealthResult
from .kaggle_fact_contracts import FactReadRequest, FactReadResult
from .kaggle_source_read import SourceReadRequest, SourceReadResult
from .kaggle_case_contracts import CaseReadRequest, CaseReadResult
from .kaggle_geo_contracts import GeoReadRequest, GeoReadResult
from .kaggle_editorial_contracts import EditorialReadRequest, EditorialReadResult
from .kaggle_source_graph_read import SourceGraphReadRequest, SourceGraphReadResult
from .kaggle_acceptance_contracts import AcceptanceReadRequest,AcceptanceReadResult
from .read_authority import ReadAuthority, VerifiedReadAuthority
from .participants import RequestAuthority

_LOG=logging.getLogger(__name__)


class AdmissionUnavailable(RuntimeError):
    pass


class AdmissionLimit(AdmissionUnavailable):
    def __init__(self, code):
        self.code=code
        super().__init__(code)


@dataclass(frozen=True)
class ReadTicket:
    request_id: str
    connection_generation: int
    turn_generation: int
    result: asyncio.Future = field(repr=False, compare=False)


@dataclass(frozen=True)
class ResourceClaim:
    run_id: str
    runtime_incarnation: str
    compute_claim: str
    dispatch_generation: int
    resource_kind: str
    resource_id: str


@dataclass(frozen=True)
class ChannelWork:
    resource: ResourceClaim
    query: Query | SqlHealthRequest | PassageReadRequest | StoryReadRequest | FactReadRequest | SourceReadRequest | CaseReadRequest | GeoReadRequest | EditorialReadRequest | SourceGraphReadRequest | None = None
    document_payload: object | None = None
    native_request: object | None = None

    @property
    def kind(self):return self.resource.resource_kind

    @property
    def compute_claim(self):return self.resource.compute_claim

    @property
    def dispatch_generation(self):return self.resource.dispatch_generation


@dataclass
class _Read:
    ticket: ReadTicket
    authority: VerifiedReadAuthority
    query: Query | SqlHealthRequest | PassageReadRequest | StoryReadRequest | FactReadRequest | SourceReadRequest | CaseReadRequest | GeoReadRequest | EditorialReadRequest | SourceGraphReadRequest
    dialog: str
    deadline: float
    timer: object
    dispatched: ResourceClaim | None = None
    on_dispatch: object = None


def _identity(value):
    if not isinstance(value,str) or not 1<=len(value)<=256 or len(value.encode())>256:
        raise ValueError('bounded internal identity required')
    return value


class SharedReadAdmission:
    def __init__(self, queue, read_authority, *, monotonic_clock=time.monotonic,
                 max_reads=256, max_person_reads=8, max_dialog_reads=2, max_read_streak=8):
        if not isinstance(read_authority,ReadAuthority):
            raise TypeError('existing ReadAuthority required')
        if type(max_reads) is not int or not 1<=max_reads<=256:
            raise ValueError('bounded read capacity required')
        if type(max_read_streak) is not int or not 1<=max_read_streak<=256:
            raise ValueError('bounded document progress interval required')
        for value in (max_person_reads,max_dialog_reads):
            if type(value) is not int or not 1<=value<=256:
                raise ValueError('bounded canonical person/dialog RAM capacity required')
        self.queue,self.authority=queue,read_authority
        self.clock=monotonic_clock
        self.max_reads,self.max_read_streak=max_reads,max_read_streak
        self.max_person_reads=min(max_person_reads,max_reads)
        self.max_dialog_reads=min(max_dialog_reads,self.max_person_reads)
        self._lock=asyncio.Lock()
        self._event=asyncio.Event()
        self._loop=None
        self._reads={}
        self._documents={}
        self._commands={}
        self._editorial_documents=set()
        self._attachment=None
        self._state='unknown'
        self._work=None
        self._last_release=None
        self._document_result=None
        self._document_resource=None
        self._native_forward=None
        self._native_completed=None
        self._sent=False
        self._slot_version=object()
        self._probe=None
        self._person_turn={}
        self._dialog_turn={}
        self._turn=0
        self._read_streak=0
        # One process/instance owns RAM consumers. This is startup I/O, never a
        # per-read lock. The persistent bit also fences pre-existing instances.
        lock_path=queue.path.resolve().with_suffix(queue.path.suffix+'.admission.lock')
        self._fd=os.open(lock_path,os.O_RDWR|os.O_CREAT,0o600)
        try:
            fcntl.flock(self._fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
            with queue.connect() as db:
                db.execute('create index if not exists bge_admission_actor on jobs(state,kind,actor,created,id)')
                db.execute('create table if not exists bge_shared_admission(id integer primary key check(id=1),enabled integer not null,run_id text,incarnation text)')
                db.execute('insert or ignore into bge_shared_admission(id,enabled) values(1,1)')
                db.execute('update bge_shared_admission set enabled=1 where id=1')
        except BaseException:
            os.close(self._fd);self._fd=None
            raise

    def _active(self):
        if self._fd is None:raise AdmissionUnavailable('admission controller closed')
        loop=asyncio.get_running_loop()
        if self._loop is None:self._loop=loop
        elif self._loop is not loop:raise AdmissionUnavailable('one controller event loop required')

    def wake(self):
        if self._loop is not None:
            self._loop.call_soon_threadsafe(self._event.set)

    async def wait_channel_work(self, seconds=.25):
        self._active()
        if not isinstance(seconds,(float,int)) or not math.isfinite(seconds) or not 0<=seconds<=1:
            raise ValueError('bounded reconcile wait required')
        # Clear only when actually consuming the notification, so an intake
        # between next_channel_work and this wait cannot lose its wakeup.
        try:await asyncio.wait_for(self._event.wait(),seconds)
        except TimeoutError:pass
        self._event.clear()

    def _cancel(self, key):
        read=self._reads.pop(key,None)
        if read is not None:
            read.timer.cancel()
            if not read.ticket.result.done():read.ticket.result.cancel()
            self._event.set()
        # _work deliberately retains only the compute descriptor after this.

    def cancel_read(self, ticket):
        self._active()
        self._cancel_ticket(ticket)

    def _cancel_ticket(self, ticket):
        key=(ticket.connection_generation,ticket.request_id)
        read=self._reads.get(key)
        if read is not None and read.ticket is ticket:self._cancel(key)

    async def begin_read(self, authority, query, *, dialog_ref, deadline, on_dispatch=None):
        self._active();_identity(dialog_ref)
        now=self.clock()
        if (not isinstance(deadline,(float,int)) or isinstance(deadline,bool)
                or not math.isfinite(deadline) or not now<deadline<=now+2):
            raise ValueError('absolute remaining deadline must be at most two seconds')
        if not isinstance(query,(Query,SqlHealthRequest,PassageReadRequest,StoryReadRequest,FactReadRequest,SourceReadRequest,CaseReadRequest,GeoReadRequest,EditorialReadRequest,SourceGraphReadRequest)) or not isinstance(authority,VerifiedReadAuthority):
            raise TypeError('typed query and verified authority required')
        if on_dispatch is not None and not callable(on_dispatch):
            raise TypeError('internal dispatch observer must be callable')
        encode_envelope(query)
        deadline=min(deadline,now+query.deadline_ms/1000)
        # Parsing a TrustedActorContext never authenticates it. Bind every
        # projection back to the identical resolver-owned context instead.
        actor=query.actor
        if (actor.person_id!=authority.person_id or actor.context_ref!=authority.context_ref
                or actor.auth_epoch!=authority.auth_revision
                or actor.audience!=authority.destination.key):
            raise PermissionError('query authority projection mismatch')
        if getattr(query,'compute_claim',None) is not None:
            raise PermissionError('caller cannot choose a compute claim')
        await self.authority.require(authority,'case_read' if type(query) is CaseReadRequest else 'public_search')
        self._require_acceptance(authority,query)
        self._active()
        if self.clock()>=deadline:raise AdmissionUnavailable('read deadline elapsed')
        if self._attachment is None or query.connection_generation!=self._attachment[1]:
            raise StaleAttachment('read requires the current reconciled channel')
        key=(query.connection_generation,query.request_id)
        if key in self._reads:raise ValueError('duplicate active read identity')
        if len(self._reads)>=self.max_reads:raise AdmissionLimit('read_capacity')
        person_reads=[r for r in self._reads.values() if r.authority.person_id==authority.person_id]
        if len(person_reads)>=self.max_person_reads:raise AdmissionLimit('person_read_capacity')
        if sum(r.dialog==dialog_ref for r in person_reads)>=self.max_dialog_reads:
            raise AdmissionLimit('dialog_read_capacity')
        future=asyncio.get_running_loop().create_future()
        ticket=ReadTicket(query.request_id,query.connection_generation,query.turn_generation,future)
        timer=asyncio.get_running_loop().call_later(max(0,deadline-self.clock()),self._cancel,key)
        self._reads[key]=_Read(ticket,authority,query,dialog_ref,float(deadline),timer,
                              on_dispatch=on_dispatch)
        future.add_done_callback(lambda _:self._cancel_ticket(ticket))
        self._event.set()
        return ticket

    def _require_acceptance(self,authority,query):
        if type(query) is not AcceptanceReadRequest:return
        from .acceptance_observer import FrozenAcceptanceFixture
        fixture=getattr(self,'_acceptance_fixture',None)
        if type(fixture) is not FrozenAcceptanceFixture:
            raise PermissionError('acceptance observer inactive')
        fixture.require(self.authority,authority,query)
        b=fixture.binding
        if self._attachment!=(b.run_id,b.connection_generation,b.runtime_incarnation):
            raise PermissionError('acceptance runtime changed')

    async def register_document_actor(self, context: RequestAuthority):
        """Trusted intake hook; UUID rows alone never authorize document work.

        Accepted rows are retained when this RAM context is unavailable after a
        restart. Durable intake context restoration belongs to existing intake.
        """
        self._active()
        if not isinstance(context,RequestAuthority):raise TypeError('internal RequestAuthority required')
        actor=await asyncio.to_thread(self.authority.participants.require,context,'import')
        if len(set(self._documents)|set(self._commands)|{actor})>256:
            raise AdmissionUnavailable('document authority capacity exhausted')
        self._documents[actor]=context
        self._event.set()
        return actor

    async def register_command_actor(self,context,command):
        """Only the retained existing bridge may authorize its exact command.

        This context is distinct from document import authority. Registering a
        Story editor never makes that person's arbitrary document rows eligible.
        """
        self._active()
        bridge=self.queue._source_commands
        if bridge is None:raise PermissionError('retained accepted command bridge required')
        bridge.spool.registry.resolve(command)
        actor=await bridge.spool.authorize_command(context,command)
        if len(set(self._documents)|set(self._commands)|{actor})>256:
            raise AdmissionUnavailable('canonical durable actor capacity exhausted')
        self._commands[actor]=context
        self._event.set()
        return actor

    async def register_editorial_document(self,owner,job_id):
        """One exact document job from the retained native Story workflow."""
        self._active()
        if owner is None or owner is not self.queue._story_editorial:
            raise PermissionError('retained Story workflow required')
        if len(self._editorial_documents|{job_id})>64:raise AdmissionUnavailable('bounded editorial documents required')
        await owner.authorize_document(job_id)
        self._editorial_documents.add(job_id);self._event.set()

    def release_editorial_document(self,owner,job_id):
        if owner is self.queue._story_editorial:self._editorial_documents.discard(job_id)

    def _observe_attachment(self, run, generation):
        attachment=(run['id'],generation,run['runtime_incarnation'])
        if self._attachment is not None and self._attachment[0]==run['id'] and generation<self._attachment[1]:
            raise StaleAttachment('attachment observation regressed')
        if self._attachment!=attachment:
            self._changed_slot()
            for key in tuple(self._reads):self._cancel(key)
            if self._work is not None and not self._sent:
                self._work=None;self._state='unknown'
            self._attachment=attachment

    def _changed_slot(self):
        self._slot_version=object()
        self._probe=None

    async def _snapshot(self, run_id, token, generation, *, observe=True):
        run,owner,docs,resumed=await asyncio.to_thread(self.queue._admission_snapshot,run_id,token,generation,
                                                    document_actors=tuple(self._documents),command_actors=tuple(self._commands),
                                                    editorial_documents=tuple(self._editorial_documents))
        if owner['run_id'] is not None and (owner['run_id'],owner['incarnation'])!=(run_id,run['runtime_incarnation']):
            raise StaleAttachment('prior compute resource remains unknown')
        if (observe and self._work is not None and self._sent
                and self._work.resource.run_id!=run_id):
            prior=self._work.resource
            # The standalone provider reconciler may have retired the old
            # resource while this RAM controller stayed alive. Only its durable
            # exact-incarnation native receipt can release the sent compute
            # debt. No extra receipt read occurs on ordinary same-run queries.
            retired=await asyncio.to_thread(self.queue.provider_reservation_retired,
                                           prior.run_id,prior.runtime_incarnation)
            if retired:
                self._work=None;self._sent=False;self._state='unknown'
                self._last_release=None;self._document_result=None
                self._document_resource=None;self._native_forward=None;self._native_completed=None
                self._changed_slot()
        if observe:self._observe_attachment(run,generation)
        return run,owner,docs,resumed

    @staticmethod
    def _descriptor(run_id, incarnation, generation, resource_kind, resource_id, compute_claim, dispatch_generation):
        if resource_kind not in {'read','document'}:raise ValueError('invalid resource kind')
        for value in (resource_id,compute_claim,incarnation):_identity(value)
        if type(dispatch_generation) is not int or not 1<=dispatch_generation<=generation:
            raise StaleAttachment('invalid original dispatch generation')
        return ResourceClaim(run_id,incarnation,compute_claim,dispatch_generation,resource_kind,resource_id)

    async def resource_state(self, run_id, token, connection_generation, *, runtime_incarnation,
                             state, resource_kind=None,resource_id=None,compute_claim=None,dispatch_generation=None,
                             probe_nonce=None):
        self._active()
        async with self._lock:
            run,owner,_,resumed=await self._snapshot(run_id,token,connection_generation)
            if run['runtime_incarnation']!=runtime_incarnation:raise StaleAttachment('resource incarnation mismatch')
            if state not in {'idle','busy'}:raise ValueError('invalid resource state')
            if state=='idle' and any(v is not None for v in (resource_kind,resource_id,compute_claim,dispatch_generation)):
                raise ValueError('idle baseline cannot carry a claim')
            descriptor=(self._descriptor(run_id,runtime_incarnation,connection_generation,resource_kind,resource_id,compute_claim,dispatch_generation)
                        if state=='busy' else None)
            probed=False
            probe_record=self._probe
            if probe_nonce is not None:
                _identity(probe_nonce)
                probed=(self._probe is not None
                        and secrets.compare_digest(self._probe[0].encode(),probe_nonce.encode())
                        and self._probe[1]==self._attachment
                        and self._probe[2] is self._slot_version
                        and (owner['run_id'],owner['incarnation'])==(run_id,runtime_incarnation))
                if not probed:raise StaleAttachment('stale resource probe')
            # Persist ownership only at its first baseline. The worker reports
            # busy/idle for every assignment, so repeating this transaction
            # would make ordinary QUERY wait for the SQLite writer. _snapshot
            # already authenticates the current attachment and exact owner;
            # the one controller lock serializes terminal/owner transitions.
            if owner['run_id'] is None:
                await asyncio.to_thread(self.queue._admission_bind,run_id,token,connection_generation,runtime_incarnation)
            if probed and (self._probe is not probe_record or probe_record[2] is not self._slot_version):
                raise StaleAttachment('resource changed during probe reconciliation')
            if self._work is not None:
                if probed and descriptor is None:
                    # Fresh ordered same-worker observation also proves that a
                    # dispatch lost before receipt cannot still start later.
                    r=self._work.resource
                    if r.resource_kind=='read':self._cancel((r.dispatch_generation,r.resource_id))
                    self._work=None;self._sent=False
                elif descriptor!=self._work.resource:
                    if (not probed and self._last_release is not None
                            and self._last_release.dispatch_generation==connection_generation
                            and (descriptor is None or descriptor==self._last_release)):
                        # states() may have queued the old idle/busy notice
                        # behind RELEASED while admission reserved new work.
                        # This weak notice cannot retire or contradict that
                        # newer claim. Only a fresh ordered probe can do so.
                        return self.status()
                    raise StaleAttachment('actual compute claim still held')
            elif descriptor is not None:
                self._work=ChannelWork(descriptor);self._sent=True
            self._changed_slot()
            self._state='busy' if self._work is not None else 'idle'
            _LOG.info('bge_resource_reconciled run_id=%s generation=%s state=%s',run_id,connection_generation,self._state)
            self._event.set()
            return self.status()

    async def begin_resource_probe(self, run_id, token, connection_generation):
        """One fresh ordered worker observation bound to this exact slot.

        Send RESOURCE_PROBE on the current socket. Its worker reader must record
        incoming work before replying, and fence superseded readers. Ordinary
        queued idle frames cannot satisfy this challenge.
        """
        self._active()
        async with self._lock:
            run,owner,_,_=await self._snapshot(run_id,token,connection_generation)
            if (owner['run_id'],owner['incarnation'])!=(run_id,run['runtime_incarnation']):
                raise StaleAttachment('probe cannot erase another owned resource')
            nonce=secrets.token_urlsafe(32)
            self._probe=(nonce,self._attachment,self._slot_version)
            return nonce

    async def resource_released(self, run_id, token, connection_generation, *, runtime_incarnation,
                                resource_kind,resource_id,compute_claim,dispatch_generation):
        self._active()
        async with self._lock:
            run,owner,_,resumed=await self._snapshot(run_id,token,connection_generation)
            descriptor=self._descriptor(run_id,runtime_incarnation,connection_generation,resource_kind,resource_id,compute_claim,dispatch_generation)
            if (run['runtime_incarnation']!=runtime_incarnation
                    or (owner['run_id'],owner['incarnation'])!=(run_id,runtime_incarnation)):
                raise StaleAttachment('stale or unknown resource release')
            if self._last_release==descriptor:
                return False # exact replay never touches a newer reservation.
            if self._work is None and self._state=='unknown':
                self._last_release=descriptor
                self._remember_document_result(descriptor,resumed)
                self._changed_slot()
                return True # restart has no claim memory; baseline still needed.
            if self._work is None or not self._sent or self._work.resource!=descriptor:
                raise StaleAttachment('stale or unknown resource release')
            if self._work.native_request is not None:
                request=self._work.native_request
                self._native_completed=(request['job_id'],request['claim'],request['request_id'])
                self._native_forward=None
            elif (resumed is not None and resumed['kind'] in ('source_command','recovery_phase')
                    and descriptor.resource_kind=='document' and descriptor.resource_id!=resumed['id']):
                # A fresh controller recovered a busy per-vector descriptor.
                self._native_completed=(resumed['id'],resumed['claim'],descriptor.resource_id)
                if self._native_forward==self._native_completed:self._native_forward=None
            self._work=None;self._sent=False;self._state='idle'
            self._last_release=descriptor
            self._remember_document_result(descriptor,resumed)
            self._changed_slot()
            _LOG.info('bge_resource_released run_id=%s generation=%s dispatch_generation=%s kind=%s',run_id,connection_generation,dispatch_generation,resource_kind)
            self._event.set()
            return True

    def _remember_document_result(self, descriptor, resumed):
        if descriptor.resource_kind!='document':return
        if self._document_result is None:
            self._document_resource=descriptor
        if resumed is not None and resumed['id']==descriptor.resource_id:
            if self._document_result!=(resumed['id'],resumed['claim']):
                self._document_resource=descriptor
            self._document_result=(resumed['id'],resumed['claim'])

    def durable_result_settled(self, job_id, claim, status):
        """Internal queue.complete reconciliation, separate from actual compute."""
        self._active()
        if status not in {'accepted','duplicate','stale','checkpoint_pending'}:raise ValueError('invalid durable settlement status')
        if self._document_result==(job_id,claim):
            # Keep the last receipt's admission until a new document arrives:
            # a lost RESULT_ACK can replay it after independent QUERY releases.
            self._document_result=None
            self._native_forward=None;self._native_completed=None;self._event.set()
            return True
        return False

    def require_durable_resource(self,run_id,incarnation,generation,job_id,compute_claim,dispatch_generation):
        self._active()
        descriptor=self._descriptor(run_id,incarnation,generation,'document',job_id,compute_claim,dispatch_generation)
        if descriptor not in (self._last_release,self._document_resource) and not (self._work is not None and self._sent and self._work.resource==descriptor):
            raise StaleAttachment('source receipt has no actual admitted resource binding')

    async def request_native_forward(self,run_id,token,generation,*,job_id,claim,request_id):
        """A retained native job borrows the SAME slot for one real forward."""
        for value in (job_id,claim,request_id):_identity(value)
        if request_id==job_id:raise ValueError('distinct native vector request required')
        self._active()
        async with self._lock:
            run,_,_,job=await self._snapshot(run_id,token,generation)
            if (job is None or job['id']!=job_id or job['claim']!=claim
                    or job['kind'] not in ('source_command','recovery_phase')
                    or self._document_result!=(job_id,claim)):
                raise StaleAttachment('native forward requires retained admitted job')
            request=(job_id,claim,request_id)
            if request==self._native_completed:return False
            if self._native_forward not in (None,request):
                raise ValueError('one native forward request per job')
            self._native_forward=request;self._event.set()
            return True

    async def next_channel_work(self, run_id, token, connection_generation):
        self._active()
        async with self._lock:
            run,owner,docs,resumed=await self._snapshot(run_id,token,connection_generation)
            if (self._state!='idle' or self._work is not None
                    or (owner['run_id'],owner['incarnation'])!=(run_id,run['runtime_incarnation'])):return None
            ready=(run['status']=='ready' and run['provider_version'] is not None
                and (not run.get('runtime_required') or run.get('runtime_verified')))
            maintenance=getattr(self.queue,'_recovery_phases',None)
            now=self.clock()
            for key,read in tuple(self._reads.items()):
                if read.deadline<=now or read.ticket.result.done():self._cancel(key)
            waiting_result=False
            if self._document_result is not None:
                waiting_result=await asyncio.to_thread(self.queue._document_claim_waiting,*self._document_result)
                if not waiting_result:self._document_result=None
            eligible=[];authorized={}
            native=(resumed is not None and self._native_forward is not None
                    and (resumed['id'],resumed['claim'])==self._native_forward[:2])
            for doc in ([resumed] if native else [] if waiting_result else [resumed] if resumed else docs):
                if doc['kind']=='recovery_phase':
                    if maintenance is not None and maintenance.eligible(doc,run,connection_generation):eligible.append(doc)
                    continue
                if not ready:continue
                if doc['kind']=='source_command':
                    if (self.queue._source_commands is None
                            or self.queue._source_commands.maintenance_blocks_dispatch()):continue
                    context=self._commands.get(doc['actor'])
                    if context is not None and self.queue._source_commands is not None:
                        try:actor=await self.queue._source_commands.authorize_job(context,doc)
                        except (PermissionError,ValueError,LookupError):actor=None
                        if actor==doc['actor']:eligible.append(doc)
                    continue
                if doc['id'] in self._editorial_documents:
                    owner=self.queue._story_editorial
                    if owner is not None:
                        try:actor=await owner.authorize_document(doc['id'])
                        except (PermissionError,ValueError,LookupError):actor=None
                        if actor==doc['actor']:eligible.append(doc)
                    continue
                context=self._documents.get(doc['actor'])
                if context is not None:
                    if doc['actor'] not in authorized:
                        try:actor=await asyncio.to_thread(self.authority.participants.require,context,'import')
                        except PermissionError:actor=None
                        except Exception as exc:
                            from .participants import ParticipantError
                            if not isinstance(exc,ParticipantError):raise
                            actor=None
                        authorized[doc['actor']]=actor==doc['actor']
                    if authorized[doc['actor']]:eligible.append(doc)
            recovery=[d for d in eligible if d['kind']=='recovery_phase']
            if recovery:eligible=recovery
            reads=[r for r in self._reads.values() if r.dispatched is None] if ready else []
            if native and reads and self._read_streak<self.max_read_streak:
                eligible=[]  # QUERY first between vectors; preserve bounded native progress.
            if not reads and not eligible:return None
            # Person first, then dialog. Many dialogs cannot multiply a
            # person's share. Forced document turn bounds continuous read runs.
            people={r.authority.person_id for r in reads}|{d['actor'] for d in eligible}
            self._person_turn={p:n for p,n in self._person_turn.items() if p in people}
            if eligible and self._read_streak>=self.max_read_streak:
                selected_doc=eligible[0];read=None
            else:
                person=min(people,key=lambda p:(self._person_turn.get(p,0),p))
                person_reads=[r for r in reads if r.authority.person_id==person]
                person_docs=[d for d in eligible if d['actor']==person]
                selected_doc=person_docs[0] if person_docs and not person_reads else None
                read=min(person_reads,key=lambda r:(self._dialog_turn.get((person,r.dialog),0),r.deadline,r.ticket.request_id)) if selected_doc is None and person_reads else None
            if read is not None:
                try:
                    await self.authority.require(read.authority,'case_read' if type(read.query) is CaseReadRequest else 'public_search')
                    self._require_acceptance(read.authority,read.query)
                except PermissionError:
                    self._cancel((read.ticket.connection_generation,read.ticket.request_id));return None
                if read.deadline<=self.clock() or (read.ticket.connection_generation,read.ticket.request_id) not in self._reads:return None
                remaining=min(read.query.deadline_ms,int((read.deadline-self.clock())*1000))
                if remaining<1:self._cancel((read.ticket.connection_generation,read.ticket.request_id));return None
                resource=ResourceClaim(run_id,run['runtime_incarnation'],secrets.token_urlsafe(24),connection_generation,'read',read.ticket.request_id)
                updates={'deadline_ms':remaining}
                if 'compute_claim' in Query.model_fields:updates['compute_claim']=resource.compute_claim
                work=ChannelWork(resource,query=read.query.model_copy(update=updates))
                person=read.authority.person_id
                self._read_streak=min(self.max_read_streak,self._read_streak+1)
                self._dialog_turn[(person,read.dialog)]=self._turn+1
            else:
                if (selected_doc['kind']=='source_command'
                        and self.queue._source_commands.maintenance_blocks_dispatch()):return None
                if native and selected_doc['id']==self._native_forward[0]:
                    if selected_doc['kind']=='source_command':
                        self.queue._source_commands._current(run,json.loads(selected_doc['identity']),connection_generation)
                    request_id=self._native_forward[2]
                    resource=ResourceClaim(run_id,run['runtime_incarnation'],secrets.token_urlsafe(24),connection_generation,'document',request_id)
                    work=ChannelWork(resource,native_request=MappingProxyType(dict(
                        job_id=selected_doc['id'],claim=selected_doc['claim'],request_id=request_id)))
                    self._turn+=1;self._person_turn[selected_doc['actor']]=self._turn
                    self._read_streak=0;self._work=work;self._sent=False;self._state='reserved'
                    self._changed_slot()
                    return work
                # Reserve before the durable writer await. Cancellation of the
                # dispatcher cannot free an ambiguously committed assignment.
                resource=ResourceClaim(run_id,run['runtime_incarnation'],secrets.token_urlsafe(24),connection_generation,'document',selected_doc['id'])
                self._state='reserving';self._work=ChannelWork(resource);self._sent=False
                self._changed_slot()
                try:payload=await asyncio.to_thread(self.queue._claim_specific,run_id,token,connection_generation,selected_doc['id'])
                except BaseException:
                    self._state='unknown';raise
                if payload is None:self._work=None;self._state='idle';return None
                if payload['kind']=='recovery_phase':
                    payload=await maintenance.dispatch_payload(selected_doc['id'],
                        run_id=run_id,token=token,generation=connection_generation)
                    work=ChannelWork(resource,document_payload=MappingProxyType(payload))
                elif payload['kind']=='source_command':
                    payload=await self.queue._source_commands.dispatch_payload(selected_doc['id'],
                        run_id=run_id,token=token,generation=connection_generation)
                    work=ChannelWork(resource,document_payload=MappingProxyType(payload))
                else:
                    work=ChannelWork(resource,document_payload=MappingProxyType({**payload,'texts':tuple(payload['texts'])}))
                person=selected_doc['actor'];self._read_streak=0
            self._turn+=1;self._person_turn[person]=self._turn
            dialogs={(r.authority.person_id,r.dialog) for r in self._reads.values()}
            self._dialog_turn={d:n for d,n in self._dialog_turn.items() if d in dialogs}
            self._work=work;self._sent=False;self._state='reserved'
            self._changed_slot()
            return work

    def mark_work_sent(self, work):
        """Call BEFORE attempting socket send; an uncertain send retains debt."""
        self._active()
        if self._work is not work or self._state!='reserved' or self._sent:
            raise StaleAttachment('stale work reservation')
        if work.query is not None:
            key=(work.query.connection_generation,work.query.request_id)
            read=self._reads.get(key)
            if read is None or read.deadline<=self.clock():
                raise StaleAttachment('read consumer no longer active')
            read.dispatched=work.resource
        self._sent=True;self._state='busy'
        self._changed_slot()
        if work.query is not None and read.on_dispatch is not None:
            try:
                read.on_dispatch(work.resource)
            except Exception:
                # Optional lossy diagnostics never change compute ownership.
                _LOG.warning('bge_read_dispatch_diagnostic_failed')
        _LOG.info('bge_compute_sent run_id=%s dispatch_generation=%s kind=%s',work.resource.run_id,work.dispatch_generation,work.kind)

    def discard_unsent(self, work):
        """Only for work provably never handed to network/worker."""
        self._active()
        if self._work is not work or self._sent:raise StaleAttachment('compute may already be active')
        self._work=None;self._state='idle';self._event.set()
        self._changed_slot()

    def read_consumer_active(self, work):
        """Cancellation observation only; never releases actual compute."""
        self._active()
        if work.query is None:return False
        read=self._reads.get((work.dispatch_generation,work.resource.resource_id))
        return bool(read is not None and not read.ticket.result.done()
                    and read.deadline>self.clock()
                    and read.ticket.turn_generation==work.query.turn_generation
                    and (read.dispatched==work.resource or
                         (read.dispatched is None and self._work is work and self._state=='reserved')))

    async def finish_read(self, run_id, token, connection_generation, result):
        """Consumer delivery only. Source/withdrawal output gate remains separate."""
        self._active()
        if not isinstance(result,(QueryResult,SqlHealthResult,PassageReadResult,StoryReadResult,FactReadResult,SourceReadResult,CaseReadResult,GeoReadResult,EditorialReadResult,SourceGraphReadResult)):raise TypeError('typed query result required')
        encode_envelope(result)
        if isinstance(result,QueryResult) and result.status in {'ok','degraded'} and result.mode!='hybrid':
            raise ValueError('target reads require actual hybrid retrieval')
        run,_,_,_=await self._snapshot(run_id,token,connection_generation,observe=False)
        if self._attachment!=(run_id,connection_generation,run['runtime_incarnation']):return False
        key=(result.connection_generation,result.request_id)
        read=self._reads.get(key)
        if (read is None or result.connection_generation!=connection_generation
                or read.ticket.turn_generation!=result.turn_generation
                or read.dispatched is None
                or read.deadline<=self.clock()):return False
        if type(result) is not {Query:QueryResult,SqlHealthRequest:SqlHealthResult,AcceptanceReadRequest:AcceptanceReadResult,PassageReadRequest:PassageReadResult,StoryReadRequest:StoryReadResult,FactReadRequest:FactReadResult,SourceReadRequest:SourceReadResult,CaseReadRequest:CaseReadResult,GeoReadRequest:GeoReadResult,EditorialReadRequest:EditorialReadResult,SourceGraphReadRequest:SourceGraphReadResult}.get(type(read.query)):
            raise ValueError('read response operation type differs from admitted request')
        try:
            await self.authority.require(read.authority,'case_read' if type(read.query) is CaseReadRequest else 'public_search')
            self._require_acceptance(read.authority,read.query)
        except PermissionError:
            if self._reads.get(key) is read:self._cancel(key)
            return False
        # The async recheck can race cancellation or a channel replacement.
        run,_,_,_=await self._snapshot(run_id,token,connection_generation,observe=False)
        if (self._reads.get(key) is not read or read.deadline<=self.clock()
                or read.ticket.result.done()
                or self._attachment!=(run_id,connection_generation,run['runtime_incarnation'])):return False
        if result.status in {'ok','degraded'} and (result.observed_data_seq<read.query.min_data_seq
                or result.observed_projection_seq<read.query.min_data_seq):
            raise ValueError('read result violates minimum sequence')
        self._reads.pop(key);read.timer.cancel();read.ticket.result.set_result(result)
        return True

    async def confirm_terminal(self, expected, capabilities, observed, *, max_age_seconds):
        """Internal exact native proof; UNKNOWN/stop/deletion never free debt."""
        self._active()
        async with self._lock:
            result=await asyncio.to_thread(self.queue._admission_terminal,expected,capabilities,observed,
                                          max_age_seconds=max_age_seconds)
            if result.ok:
                for key in tuple(self._reads):self._cancel(key)
                self._work=None;self._sent=False;self._state='unknown';self._attachment=None
                self._last_release=None
                self._document_result=None
                self._document_resource=None;self._native_forward=None;self._native_completed=None
                self._changed_slot()
                self._event.set()
            return result

    def status(self):
        return {'state':self._state,'pending_reads':len(self._reads),
                'resource':self._work.resource if self._work else None,
                'document_result_pending':self._document_result is not None,
                'document_contexts':len(self._documents),'command_contexts':len(self._commands)}

    def close(self):
        """Shutdown only: never clears persistent ownership or compute debt."""
        for key in tuple(self._reads):self._cancel(key)
        if self._fd is not None:os.close(self._fd);self._fd=None
