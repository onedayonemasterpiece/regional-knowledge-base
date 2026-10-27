"""Compact owner control for native source stage commands.

The existing importer owns OAuth/output fencing and its per-import lock. Exact
model inputs live only in its existing bounded ControlSpool and then native PG;
this table retains counters, hashes and pending operation identity, not a graph.
"""
from __future__ import annotations

import asyncio
import base64
from datetime import datetime, timezone
import json
import logging
import sqlite3
import time
import hashlib
from dataclasses import dataclass
from functools import wraps
from uuid import UUID, uuid5

from .source_stage import (STAGE_ROUTE, BATCH_CODEC, ZERO_HASH,
    canonical, digest, encode_issued_image, validate_stage_payload)


_LOG=logging.getLogger(__name__)
_PACKET_WORK="phase='staging' AND packet_mode=1 AND (pending_kind IN ('stage','batch') OR enriched_batch_count<batch_count)"


@dataclass(frozen=True)
class _AcceptedPacketProof:
    """Internal accepted-parent scope; never an HTTP access or owner Principal."""
    owner: object
    binding: object
    ingestion_id: str
    source_id: str
    person_id: str
    batch_index: int
    parent_key: str
    parent_sha256: str

    @property
    def context(self):return self

    async def authorize(self,command=None):
        return await self.owner._authorize_packet(self,command)


def _failure(reason, *, status='unavailable'):
    from .kaggle_importer import failure
    return failure(reason, status=status)


def _timed_native_phase(phase):
    """Nested native handler duration, not full MCP or persisted Saved."""
    def decorate(method):
        @wraps(method)
        async def observed(self, access, row, *args, **kwargs):
            from .product_telemetry import emit_import_event
            started = time.monotonic()
            status = 'error'
            try:
                result = await method(self, access, row, *args, **kwargs)
                state = getattr(result, 'state', None)
                status = ('applied' if state == 'finalized' else 'failed' if state == 'failed'
                          else 'staged' if state == 'staged' else 'pending'
                          if state in ('ready','processing') else 'returned')
                return result
            except asyncio.CancelledError:
                status = 'cancelled'
                raise
            finally:
                # A malformed observer input must not replace the domain
                # result, cancellation or exception being returned.
                try:
                    emit_import_event(ingestion_id=row['ingestion_id'],source_id=row['source_id'],event='import.phase',phase=phase,
                        status=status,duration_ms=(time.monotonic()-started)*1000)
                except Exception:
                    pass
        return observed
    return decorate


class NativeSourceIntake:
    """Called under KaggleSourceImporter._lock and its existing access lease."""
    def __init__(self, importer):
        from .source_activation import ACTIVATION_REVISIONS
        if importer.import_revisions != ACTIVATION_REVISIONS:
            raise TypeError('explicit native schema14 importer required')
        self.importer = importer
        self.queue, self.spool, self.startup = importer.queue, importer.spool, importer.startup
        self._packet_wakes=set();self._packet_task=None;self._packet_cursor='';self._packet_closed=False
        self._initialize()
        if getattr(self.startup,'source_dispatch',None) is not None:
            self.startup.source_dispatch.packet_owner=self

    def _initialize(self):
        with self.queue.connect() as db:
            db.execute('''CREATE TABLE IF NOT EXISTS rkb_native_source_intake_state(
                ingestion_id TEXT PRIMARY KEY,person_id TEXT NOT NULL,source_id TEXT UNIQUE NOT NULL,
                batch_count INTEGER NOT NULL DEFAULT 0,last_batch_sha256 TEXT NOT NULL,
                last_request_sha256 TEXT,pending_operation_key TEXT,pending_kind TEXT,
                phase TEXT NOT NULL DEFAULT 'staging',seal_sha256 TEXT,
                rejected_seal_head TEXT,
                materializer_sha256 TEXT,next_ordinal INTEGER NOT NULL DEFAULT 0,
                total_inputs INTEGER,knowledge_sha256 TEXT,
                CHECK(batch_count BETWEEN 0 AND 2500),CHECK(next_ordinal>=0),
                CHECK((pending_operation_key IS NULL)=(pending_kind IS NULL)),
                CHECK(phase IN ('staging','sealed','materializing','ready_activation','finalized','failed')))''')
            # Source research can outlive its completed original upload.
            # The importer serializes acquisition; per-source progress keeps
            # its own primary/unique identity without an owner-wide stage slot.
            db.execute('DROP INDEX IF EXISTS rkb_native_one_owner_intake')
            columns={value[1] for value in db.execute('PRAGMA table_info(rkb_native_source_intake_state)')}
            if 'rejected_seal_head' not in columns:
                db.execute('ALTER TABLE rkb_native_source_intake_state ADD COLUMN rejected_seal_head TEXT')
            for name, declaration in (('packet_mode','INTEGER NOT NULL DEFAULT 0'),
                    ('published_batch_count','INTEGER NOT NULL DEFAULT 0'),('rejected_batch_index','INTEGER'),
                    ('read_completion_requested','INTEGER NOT NULL DEFAULT 0'),('enriched_batch_count','INTEGER NOT NULL DEFAULT 0'),
                    ('command_attempt','INTEGER NOT NULL DEFAULT 0'),('rejected_reason','TEXT')):
                if name not in columns:
                    db.execute(f'ALTER TABLE rkb_native_source_intake_state ADD COLUMN {name} {declaration}')
            for legacy in db.execute("UPDATE rkb_native_source_intake_state SET phase='failed',"
                    "rejected_reason='legacy_import_reimport_required',pending_operation_key=NULL,pending_kind=NULL "
                    "WHERE packet_mode=0 AND rejected_reason IS NOT 'legacy_import_reimport_required' RETURNING ingestion_id"):
                db.execute("UPDATE rkb_source_intake_control SET phase='failed' WHERE ingestion_id=?", legacy)
            db.execute('DROP INDEX IF EXISTS rkb_native_packet_work')

    async def _packet_proof(self,ingestion_id):
        with self.queue.connect(write=False) as db:
            state=db.execute('SELECT * FROM rkb_native_source_intake_state WHERE ingestion_id=? AND '+_PACKET_WORK,(ingestion_id,)).fetchone()
        if state is None:raise PermissionError('accepted packet no longer pending')
        state=dict(state);index=state['batch_count']-(state['pending_kind']!='stage')
        key=f"source-stage:{ingestion_id}:{index}:{state['command_attempt']}"
        parent=await self.spool.get(key)
        proof=_AcceptedPacketProof(self,self.startup.binding,ingestion_id,state['source_id'],state['person_id'],index,key,parent.command_sha256)
        await proof.authorize()
        return proof

    async def _authorize_packet(self,proof,command=None):
        if (type(proof) is not _AcceptedPacketProof or proof.owner is not self or self._packet_closed
                or not self.importer._binding_current(proof.binding)):
            raise PermissionError('accepted packet binding changed')
        parent=await self.spool.get(proof.parent_key);c=parent.command;doc=json.loads(c.payload)
        if (parent.accepted is None or parent.seal_state!='ACCEPTED' or parent.command_sha256!=proof.parent_sha256
                or c.route!=STAGE_ROUTE or c.person_id!=proof.person_id or c.ids!={'source_id':proof.source_id}
                or doc.get('ingestion_id')!=proof.ingestion_id or doc.get('batch_index')!=proof.batch_index
                or parent.database_generation!=proof.binding.database_generation
                or await self.spool.journal.verify_accepted(parent.accepted,c)!=proof.parent_sha256):
            raise PermissionError('accepted packet parent differs')
        from .participants import ParticipantError
        try:actor=await asyncio.to_thread(self.spool.participants.require_packet_continuation,proof)
        except ParticipantError:raise PermissionError('current packet import rights unavailable') from None
        state=await asyncio.to_thread(self._state,dict(ingestion_id=proof.ingestion_id,source_id=proof.source_id,person_id=actor))
        if (state is None or state['phase']!='staging' or not state['packet_mode']
                or state['pending_kind'] not in (None,'stage','batch')
                or proof.parent_key!=self._operation({'ingestion_id':proof.ingestion_id},'stage',f"{proof.batch_index}:{state['command_attempt']}")
                or (state['pending_kind']=='stage' and (state['pending_operation_key']!=proof.parent_key or state['batch_count']!=proof.batch_index))
                or (state['pending_kind']!='stage' and (state['batch_count']!=proof.batch_index+1
                    or state['last_batch_sha256']!=digest(c.payload) or state['enriched_batch_count']>=state['batch_count']))):
            raise PermissionError('accepted packet state differs')
        if command is not None:
            if command.encode()==c.encode() and state['pending_kind']=='stage':return actor
            from .source_batch import CODEC,ROUTE,resolved_command
            from .kaggle_source_materializer import materializer_fingerprint
            value=json.loads(command.payload);component=value.get('component')
            expected=dict(codec=CODEC,ingestion_id=proof.ingestion_id,batch_index=proof.batch_index,
                batch_count=proof.batch_index+1,last_batch_sha256=digest(c.payload),
                materializer_sha256=materializer_fingerprint(),component=component)
            if (parent.apply_state!='APPLIED' or parent.apply_receipt is None or parent.apply_receipt.status!='applied'
                    or command.route!=ROUTE or component not in ('text','semantic') or value!=expected
                    or command.person_id!=actor or command.ids!=c.ids or command.request_id!=c.request_id
                    or command.operation_key!=self._operation({'ingestion_id':proof.ingestion_id},'batch',f'{proof.batch_index}:{component}')
                    or state['batch_count']!=proof.batch_index+1 or state['last_batch_sha256']!=digest(c.payload)
                    or component=='semantic' and state['published_batch_count']!=state['batch_count']):
                raise PermissionError('accepted packet derivative differs')
            exact=resolved_command(sequence=command.sequence,operation_key=command.operation_key,request_id=c.request_id,
                person_id=actor,logical_time=command.logical_time,revisions=self.importer._revisions(proof.binding),
                source_id=proof.source_id,payload=expected)
            if exact.encode()!=command.encode():raise PermissionError('packet command contract differs')
        return actor

    async def authorize_packet_job(self,command):
        if command.route not in (STAGE_ROUTE,'source.batch.v1'):raise PermissionError('not a packet command')
        proof=await self._packet_proof(json.loads(command.payload)['ingestion_id'])
        return await proof.authorize(command)

    def kick_packet(self,ingestion_id):
        if self._packet_closed:return
        self._packet_wakes.add(ingestion_id)
        if self._packet_task is None or self._packet_task.done():
            self._packet_task=asyncio.create_task(self._continue_packets())

    async def recover_packets(self):
        # Keyset over compact progress rows; no payload/history enumeration.
        with self.queue.connect(write=False) as db:
            rows=db.execute('SELECT ingestion_id FROM rkb_native_source_intake_state WHERE '+_PACKET_WORK+
                ' AND ingestion_id>? ORDER BY ingestion_id LIMIT 16',(self._packet_cursor,)).fetchall()
        self._packet_cursor=rows[-1][0] if rows else ''
        for row in rows:self.kick_packet(row[0])

    async def _continue_packets(self):
        while self._packet_wakes:
            ingestion=self._packet_wakes.pop()
            try:
                async with self.importer._lock:
                    proof=await self._packet_proof(ingestion)
                    row=await asyncio.to_thread(self.importer._row,proof.person_id,ingestion)
                    state=await asyncio.to_thread(self._state,row)
                    if state['pending_operation_key']:
                        pending=await self.spool.get(state['pending_operation_key'])
                        if 'SENT_UNKNOWN' in (pending.seal_state,pending.apply_state):continue
                        await proof.authorize(pending.command)
                    state=await self._drive(proof,row,state)
                    await self._packet_progress(proof,row,state)
            except asyncio.CancelledError:raise
            except Exception as error:
                _LOG.info('native_packet_continuation state=paused ingestion_id=%s reason=%s',ingestion,type(error).__name__)

    async def close_packets(self):
        self._packet_closed=True
        self._packet_wakes.clear()
        if self._packet_task is not None:
            self._packet_task.cancel()
            try:await self._packet_task
            except asyncio.CancelledError:pass

    async def _check(self, access, row=None):
        if type(access) is _AcceptedPacketProof:
            actor=await access.authorize()
            if row is not None and row['person_id']!=actor:raise PermissionError('packet actor differs')
            return
        await self.importer._check(access.authority, access.context, access.binding)
        if row is not None and row['person_id']!=access.authority.person_id:
            raise _failure('owned_ingestion_required',status='denied')

    def _state(self, row, *, create=False):
        with self.queue.connect(write=create) as db:
            if create:
                try:
                    db.execute('''INSERT OR IGNORE INTO rkb_native_source_intake_state
                        (ingestion_id,person_id,source_id,last_batch_sha256,packet_mode) VALUES(?,?,?,?,1)''',
                        (row['ingestion_id'],row['person_id'],row['source_id'],ZERO_HASH))
                except sqlite3.IntegrityError:
                    raise _failure('another_native_ingestion_active') from None
            value=db.execute('SELECT * FROM rkb_native_source_intake_state WHERE ingestion_id=?',
                (row['ingestion_id'],)).fetchone()
            if value is None:
                if create: raise _failure('another_native_ingestion_active')
                return None
            value=dict(value)
            if value['person_id']!=row['person_id'] or value['source_id']!=row['source_id']:
                raise _failure('owned_ingestion_required',status='denied')
        return value

    def _update(self, row, **changes):
        allowed={'batch_count','last_batch_sha256','last_request_sha256','pending_operation_key',
            'pending_kind','phase','seal_sha256','materializer_sha256','next_ordinal','total_inputs','knowledge_sha256'}
        allowed.add('rejected_seal_head')
        allowed.update({'published_batch_count','enriched_batch_count','rejected_batch_index','read_completion_requested',
            'command_attempt','rejected_reason'})
        if not changes or set(changes)-allowed: raise ValueError('compact native progress fields required')
        with self.queue.connect() as db:
            n=db.execute('UPDATE rkb_native_source_intake_state SET '+','.join(k+'=?' for k in changes)
                +' WHERE ingestion_id=? AND person_id=? AND source_id=?',
                (*changes.values(),row['ingestion_id'],row['person_id'],row['source_id'])).rowcount
            if n!=1: raise _failure('owned_ingestion_required',status='denied')
        return self._state(row)

    @staticmethod
    def _operation(row, kind, ordinal=None):
        if kind=='stage': return f"source-stage:{row['ingestion_id']}:{ordinal}"
        if kind=='catalog': return f"source-catalog:{row['ingestion_id']}:{ordinal}"
        if kind=='batch': return f"source-packet:{row['ingestion_id']}:{ordinal}"
        if kind=='finish': return f"source-finish:{row['ingestion_id']}"

    async def _entry(self, key):
        from .control_spool import SpoolError
        try: return await self.spool.get(key)
        except SpoolError as error:
            if str(error)=='control operation not found': return None
            raise

    def _stage_key(self,row,state):
        ordinal=f"{state['batch_count']}:{state['command_attempt']}"
        return self._operation(row,'stage',ordinal)

    @staticmethod
    def _request_hash(doc):
        return digest(canonical({k:v for k,v in doc.items()
            if k not in ('codec','batch_index','previous_batch_sha256')}))

    def _inactive_prefix_current(self, access, row, scope):
        """Require this book's own accepted inactive predecessors."""
        proof=getattr(self.startup,'coverage',None)
        if (proof is None or not proof.current() or getattr(proof,'binding',None)!=access.binding
                or self.spool._source_anchor is not proof or scope['unknown']):
            return False
        from .source_history_verifier import INACTIVE_SOURCE_ROUTES
        with self.queue.connect(write=False) as db:
            rows=db.execute("SELECT * FROM rkb_control_operations WHERE source_id=? AND outcome_state<>'DURABLE'",
                (row['source_id'],)).fetchall()
        if not rows:return False
        for pending in rows:
            verified=self.spool._entry(pending)
            command=verified.command
            applied=(pending['apply_state']=='APPLIED' or
                command.route in (STAGE_ROUTE,'source.batch.v1','source.finish.v1','source.catalog.v1') and pending['apply_state']=='REJECTED_PRECONDITION')
            if (pending['person_id']!=row['person_id']
                    or pending['database_generation']!=access.binding.database_generation
                    or pending['seal_state']!='ACCEPTED' or not applied
                    or pending['outcome_state']!='NOT_SENT'
                    or verified.accepted is None or verified.apply_receipt is None
                    or (verified.apply_state,verified.apply_receipt.status) not in (
                        ('APPLIED','applied'),('REJECTED_PRECONDITION','rejected_precondition'))):
                return False
            if (command.route not in INACTIVE_SOURCE_ROUTES|{'source.batch.v1','source.finish.v1','source.catalog.v1'}
                    or json.loads(command.payload).get('ingestion_id')!=row['ingestion_id']):
                return False
        return proof.current()

    @staticmethod
    def _can_settle(state,entry):
        if entry.outcome_state=='DURABLE':return True
        if state['pending_kind']=='finish':
            # A native reading-completion commit is useful immediately at
            # APPLIED. Dataset Saved is a later, independently observed
            # checkpoint; UNKNOWN, unaccepted and unproved apply stay pending.
            return (entry.outcome_state=='NOT_SENT' and entry.accepted is not None and entry.seal_state=='ACCEPTED'
                and entry.apply_receipt is not None and
                (getattr(entry,'apply_state',None),entry.apply_receipt.status) in
                    (('APPLIED','applied'),('REJECTED_PRECONDITION','rejected_precondition')))
        if state['pending_kind'] in ('batch','catalog') or state['pending_kind']=='stage':
            return (entry.outcome_state=='NOT_SENT' and entry.accepted is not None and entry.seal_state=='ACCEPTED'
                and entry.apply_receipt is not None and
                (getattr(entry,'apply_state',None),entry.apply_receipt.status) in
                    (('APPLIED','applied'),('REJECTED_PRECONDITION','rejected_precondition')))

    async def _stage_exact(self, access, row, *, kind, key, route, payload, builder):
        if type(access) is _AcceptedPacketProof and (kind!='batch' or route!='source.batch.v1'):
            raise PermissionError('packet continuation cannot stage new input')
        async with self.spool._lock:
            entry=await self._entry(key)
            if entry is not None:
                if type(access) is _AcceptedPacketProof:await access.authorize(entry.command)
                if (entry.command.route!=route or entry.command.payload!=canonical(payload)
                        or entry.command.ids!={'source_id':row['source_id']}
                        or entry.command.person_id!=row['person_id']):
                    raise _failure('native_operation_payload_changed',status='invalid_query')
            else:
                await self._check(access)
                scope=await self.spool.scope_state([row['source_id']])
                if scope['unknown']:
                    raise _failure('prior_write_pending')
                continuation=(scope['pending'] and kind in ('stage','batch','finish','catalog')
                    and await asyncio.to_thread(self._inactive_prefix_current,access,row,scope))
                if scope['pending'] and not continuation:raise _failure('prior_write_pending')
                journal_scope=await self.spool.scope_state()
                if journal_scope['sequence']!=journal_scope['accepted_sequence']:
                    raise _failure('prior_write_pending')
                head=await self.spool.journal.head()
                sequence=head.reference.last_sequence if head else 0
                if sequence!=journal_scope['accepted_sequence']:
                    _LOG.warning('native_intake_admission reason=journal_prefix kind=%s journal_sequence=%s accepted_sequence=%s',
                        kind,sequence,scope['accepted_sequence'])
                    raise _failure('source_history_unavailable')
                command=builder(sequence=sequence+1,operation_key=key,request_id=row['request_id'],
                    person_id=row['person_id'],logical_time=datetime.now(timezone.utc).isoformat(),
                    revisions=self.importer._revisions(access.binding),source_id=row['source_id'],payload=payload)
                frozen,data,handler=self.spool._validate(command,access.binding.database_generation)
                if type(access) is _AcceptedPacketProof:
                    if kind!='batch':raise PermissionError('packet continuation cannot stage new input')
                    await access.authorize(frozen)
                await self._check(access)
                facts=await self.spool.read_facts([row['source_id']])
                await self._check(access)
                if (not self.importer._intake_coverage_current(access.binding)
                        or facts.database_generation!=access.binding.database_generation
                        or facts.scope_sequence>facts.covered_sequence or (facts.pending and not continuation) or facts.unknown
                        or not self.spool.facts_current(facts)):
                    _LOG.warning('native_intake_admission reason=history_proof kind=%s reads_current=%s imports_current=%s '
                        'anchor_current=%s binding_current=%s database_current=%s scope_sequence=%s covered_sequence=%s '
                        'pending=%s continuation=%s unknown=%s facts_current=%s',kind,self.startup.reads_current(),
                        self.startup.imports_current(),self.startup.coverage is self.spool._source_anchor,
                        self.startup.binding==access.binding,facts.database_generation==access.binding.database_generation,
                        facts.scope_sequence,facts.covered_sequence,facts.pending,continuation,facts.unknown,
                        self.spool.facts_current(facts))
                    raise _failure('source_history_unavailable')
                try:entry=await self.spool._write(self.spool._stage,frozen,data,handler,access.binding.database_generation)
                except sqlite3.OperationalError as error:
                    if kind=='stage' and getattr(error,'sqlite_errorcode',None)==sqlite3.SQLITE_BUSY:
                        raise _failure('source_stage_not_accepted_retry_same_stage') from None
                    raise
            # If the process exits between the spool commit and this compact
            # mark, the stable key is reconciled before any new allocation.
            observed=await asyncio.to_thread(self._update,row,pending_operation_key=key,pending_kind=kind)
            try:
                from .product_telemetry import import_metadata, emit_import_event
                import_metadata(ingestion_id=row['ingestion_id'],source_id=row['source_id'],operation_key=key,
                    batch_index=payload.get('batch_index'),command_sequence=entry.command.sequence,
                    packet_bytes=len(entry.command.payload),attempt=observed['command_attempt'],
                    batch_count=observed['batch_count'],published_batch_count=observed['published_batch_count'],
                    enriched_batch_count=observed['enriched_batch_count'])
                component=payload['component'] if kind=='batch' else kind
                import_metadata(component=component)
                if kind=='stage':
                    model=payload['model_payload'];pages=model['pages']
                    import_metadata(page_count=len(pages),physical_page_index=pages[0]['physical_page_index'],
                        physical_page_indices=[p['physical_page_index'] for p in pages],
                        region_count=sum(len(p['regions']) for p in pages),chunk_count=len(model['chunks']),
                        story_candidate_count=len(model.get('story_candidates',[])),
                        entity_candidate_count=len(model.get('entity_candidates',{}).get('entities',[])),
                        poi_candidate_count=len(model.get('poi_facts',[])),
                        unit_marker_count=sum(len(p.get('source_unit_boundaries',[])) for p in pages))
                emit_import_event(ingestion_id=row['ingestion_id'],source_id=row['source_id'],operation_key=key,event='import.phase',
                    phase='admission',status='staged',command_sequence=entry.command.sequence,
                    component=component,packet_bytes=len(entry.command.payload))
            except Exception: pass  # Observation cannot change a committed stage.
            return entry

    @staticmethod
    def _result(entry):
        if entry.apply_receipt is None: raise _failure('native_durable_receipt_missing')
        outer=json.loads(entry.apply_receipt.outcome)
        raw=base64.b64decode(outer['result_b64'],validate=True)
        if not 1<=len(raw)<=8192 or digest(raw)!=outer['result_sha256']:
            raise _failure('native_outcome_integrity_differs')
        value=json.loads(raw)
        if type(value) is not dict: raise _failure('native_outcome_contract')
        return value

    async def _settle(self, access, row, state, entry):
        await self._check(access)
        if not self._can_settle(state,entry): return state
        if entry.apply_receipt is None: raise _failure('native_durable_receipt_missing')
        if entry.apply_receipt.status not in ('applied','rejected_precondition'):
            raise _failure('native_durable_outcome_status_unknown')
        if entry.apply_receipt.status!='applied':
            if state['pending_kind']=='catalog':
                # Rejected descriptive corrections never fail an accepted source.
                return await asyncio.to_thread(self._update,row,pending_operation_key=None,pending_kind=None)
            if state['pending_kind'] in ('stage','batch','finish'):
                kind=state['pending_kind'];reason='native_source_packet_requires_review'
                raw=json.loads(entry.apply_receipt.outcome)
                body=base64.b64decode(raw['result_b64'],validate=True)
                if body and self._result(entry).get('reason')=='native_source_sql_rolled_back':
                    reason='native_source_sql_rolled_back'
                # One invalid packet remains an explicit review result. It
                # neither revokes prior packets nor closes the whole import.
                changes=dict(phase='staging',pending_operation_key=None,pending_kind=None,
                    rejected_batch_index=max(0,state['batch_count']-1),rejected_reason=reason,
                    command_attempt=state['command_attempt']+1,read_completion_requested=0)
                if kind=='batch':changes.update(published_batch_count=state['batch_count'],enriched_batch_count=state['batch_count'])
                return await asyncio.to_thread(self._update,row,**changes)
            await asyncio.to_thread(self.importer._save,row,phase='failed')
            return await asyncio.to_thread(self._update,row,phase='failed',pending_operation_key=None,pending_kind=None)
        result=self._result(entry); doc=json.loads(entry.command.payload)
        if result.get('source_id')!=row['source_id']:
            raise _failure('native_outcome_identity_differs')
        kind=state['pending_kind']; changes=dict(pending_operation_key=None,pending_kind=None)
        if kind=='stage' and result.get('ingestion_id')!=row['ingestion_id']:
            raise _failure('native_outcome_identity_differs')
        if kind=='catalog':
            if (result.get('ingestion_id')!=row['ingestion_id']
                    or result.get('catalog_revision')!=1 or result.get('accepted') is not True
                    or result.get('payload_sha256')!=digest(entry.command.payload)):
                raise _failure('native_catalog_outcome_differs')
            # The catalog projection changes, never accepted stage metadata or bytes.
        elif kind=='stage':
            if (result.get('batch_index')!=doc['batch_index'] or result.get('batch_sha256')!=digest(entry.command.payload)
                    or result.get('candidate_state')!='staged_inactive'):
                raise _failure('native_stage_outcome_differs')
            changes.update(batch_count=doc['batch_index']+1,last_batch_sha256=digest(entry.command.payload),
                last_request_sha256=self._request_hash(doc),phase='staging',rejected_seal_head=None,
                rejected_batch_index=None,rejected_reason=None,read_completion_requested=0)
            await asyncio.to_thread(self.importer._save,row,phase='staging',
                research_json=canonical({k:v for k,v in doc['research'].items() if k!='frontier_id'}).decode())
        elif kind=='batch':
            if (result.get('ingestion_id')!=row['ingestion_id'] or result.get('batch_index')!=doc['batch_index']
                    or result.get('component')!=doc['component']
                    or result.get('candidate_state')!='published' or result.get('accepted') is not True):
                raise _failure('native_packet_outcome_differs')
            counter='published_batch_count' if doc['component']=='text' else 'enriched_batch_count'
            changes.update({counter:doc['batch_index']+1},phase='staging',rejected_batch_index=None)
        elif kind=='finish':
            if result.get('read_complete') is not True or result.get('ingestion_id')!=row['ingestion_id']:
                raise _failure('native_read_completion_outcome_differs')
            changes['phase']='finalized'
            await asyncio.to_thread(self.importer._save,row,phase='finalized')
        else: raise _failure('native_pending_operation_invalid')
        updated=await asyncio.to_thread(self._update,row,**changes)
        if kind=='finish':
            # Reading completion is APPLIED in native PG, not yet Dataset
            # Saved. Schedule the existing capture owner, never wait for it
            # in the import tool call or manufacture an outcome receipt.
            checkpoint=getattr(self.startup,'dataset_checkpoints',None)
            if checkpoint is not None:
                checkpoint.kick(access.binding,planned_stop=True)
        _LOG.info('native_intake_progress ingestion_id=%s kind=%s remote_durable=%s next_ordinal=%s',
            row['ingestion_id'],kind,entry.outcome_state=='DURABLE',updated['next_ordinal'])
        # Applied != Saved: log only, do not modify actual remote receipt.
        try:
            from .product_telemetry import emit_import_event, import_metadata
            phase=doc['component'] if kind=='batch' else kind
            import_metadata(ingestion_id=row['ingestion_id'],source_id=row['source_id'],operation_key=entry.command.operation_key,
                batch_count=updated['batch_count'],published_batch_count=updated['published_batch_count'],
                enriched_batch_count=updated['enriched_batch_count'])
            emit_import_event(ingestion_id=row['ingestion_id'],source_id=row['source_id'],operation_key=entry.command.operation_key,
                event='import.phase',phase=phase,status='applied',command_sequence=entry.command.sequence,
                batch_count=updated['batch_count'],published_batch_count=updated['published_batch_count'],
                enriched_batch_count=updated['enriched_batch_count'])
        except Exception: pass  # Incomplete diagnostics cannot alter a settled result.
        return updated

    async def _drive(self, access, row, state):
        if not state['pending_operation_key']: return state
        await self._check(access)
        entry=await self._entry(state['pending_operation_key'])
        if entry is None: raise _failure('native_pending_operation_missing')
        if entry.accepted is None:
            from .control_spool import SpoolUnknown
            from .recovery_journal import JournalUnknown
            try:
                if entry.seal_state=='SENT_UNKNOWN':
                    # Stored original predecessor only. Never recalculate a
                    # sequence or blindly resend an uncertain operation.
                    await self.spool.seal(access.context,entry.command.operation_key,
                        writer_generation=self.startup.config.writer_generation)
                else:
                    head=await self.spool.journal.head()
                    if (head.reference.last_sequence if head else 0)!=entry.command.sequence-1:
                        raise _failure('prior_write_pending')
                    await self._check(access)
                    await self.spool.seal(access.context,entry.command.operation_key,predecessor=head,
                        writer_generation=self.startup.config.writer_generation)
            except (SpoolUnknown, JournalUnknown):
                await self._check(access)
                return state
        await self._check(access)
        entry=await self.spool.get(entry.command.operation_key)
        if self._can_settle(state,entry): return await self._settle(access,row,state,entry)
        if entry.apply_receipt is not None and entry.apply_state in ('APPLIED','REJECTED_PRECONDITION'):
            # Known native outcome waiting for Dataset durability is not new
            # compute demand. Keep the same pending intent and truthful status.
            return state
        await self.startup.source_dispatch.enqueue(access.context,entry.command.operation_key,
            writer_generation=self.startup.config.writer_generation)
        await self._check(access)
        return state

    async def applied_page_cursor(self, access, row):
        with self.queue.connect(write=False) as db:
            latest=db.execute("SELECT operation_key FROM rkb_control_operations WHERE source_id=? "
                "AND operation_key LIKE ? AND apply_state='APPLIED' ORDER BY sequence DESC LIMIT 1",
                (row['source_id'],f"source-stage:{row['ingestion_id']}:%")).fetchone()
        entry=await self._entry(latest[0]) if latest else None
        return str(max(p['physical_page_index'] for p in json.loads(entry.command.payload)['model_payload']['pages'])+1) if entry else None

    def _output(self, row, state):
        phase=state['phase']
        if phase=='failed': return self.importer._output(row,'failed',state['rejected_reason'] or 'Native command rejected; no successful source activation','blocker')
        if state['pending_operation_key']:
            return self.importer._output(row,'processing','Exact native command pending apply and durable outcome; resume same operation','wait',source_archive_status='verified')
        if phase=='staging':
            if state['enriched_batch_count']<state['batch_count'] or state.get('read_completion_requested'):
                return self.importer._output(row,'processing','Current packet or reading-completion intent pending; resume status',
                    'wait',source_archive_status='verified')
            if state.get('rejected_batch_index') is not None:
                reason=state.get('rejected_reason') or 'native_source_packet_requires_review'
                message=(reason+': current packet phase rejected; earlier accepted pages remain searchable.\n'
                    'Correct and retry this packet under the same ingestion.\n'
                    'For unreadable spans use [?] with needs_review=false; never guess.')
                return self.importer._output(row,'staged',message,
                    'continue_pages',source_archive_status='verified',warnings=[reason])
            return self.importer._output(row,'staged','Accepted packet published; continue reading pages. Dataset persistence is periodic.',
                'continue_pages',source_archive_status='verified')
        if phase=='finalized':
            return self.importer._output(row,'finalized',
                'Reading completed; accepted packets remain searchable and research continuation is retained','done',
                source_archive_status='verified',warnings=[])

    async def _tail_output(self, access, row, state):
        if (not state['batch_count']
                or state['pending_operation_key'] or state.get('rejected_batch_index') is not None
                or state['phase'] not in ('staging','finalized')):
            return None
        await self._check(access,row)
        restorer=getattr(self.startup,'restorer',None)
        if restorer is not None and not restorer.complete:
            # A status request must not create the lazy history dispatcher
            # before startup installs its retained restore/checkpoint owner.
            return self.importer._output(row,'processing','Source maintenance is starting; resume status',
                'wait',source_archive_status='verified',warnings=['source_maintenance_starting'])
        try:
            from .contracts import SourceTailRef
            value=await self.startup.history.source_tail_refs(access.binding,row['source_id'],row['ingestion_id'])
            if set(value)!={'tail_refs','has_more'} or type(value['has_more']) is not bool or len(value['tail_refs'])>4:
                raise ValueError('source_tail_response_invalid')
            refs=[SourceTailRef.model_validate(r) for r in value['tail_refs']]
            if value['has_more'] and not refs:raise ValueError('source_tail_response_invalid')
            for ref in refs:
                if (ref.page_id!=str(uuid5(UUID(row['source_id']),f'page:1:{ref.physical_page_index}'))
                        or ref.region_id!=str(uuid5(UUID(ref.page_id),'region:'+ref.region_key))):
                    raise ValueError('source_tail_scope_changed')
        except asyncio.CancelledError:
            raise
        except Exception as error:
            _LOG.warning('source_tail_query state=unavailable ingestion_id=%s error_type=%s',
                row['ingestion_id'],type(error).__name__)
            await self._check(access,row)
            return self.importer._output(row,'processing','Source tail coverage unavailable; resume status',
                'wait',source_archive_status='verified',warnings=['source_tail_coverage_unavailable'])
        await self._check(access,row)
        if refs:
            _LOG.info('source_tail_query state=pending ingestion_id=%s tails=%s has_more=%s',
                row['ingestion_id'],len(refs),value['has_more'])
            return self.importer._output(row,'needs_review',
                'Resume the exact pending spans with the next reviewed page; select small complete semantic passages, without splitting sentences; do not finalize',
                'continue_tail',source_archive_status='verified',tail_refs=refs,
                next_cursor=str(refs[0].physical_page_index+1),warnings=['more_source_tails'] if value['has_more'] else [])
        return None

    async def _recover_orphan(self, access, row, state, kind, key):
        if state['pending_operation_key']: return await self._drive(access,row,state)
        entry=await self._entry(key)
        if entry is None: return state
        expected_route={'stage':STAGE_ROUTE,'batch':'source.batch.v1','finish':'source.finish.v1','catalog':'source.catalog.v1'}[kind]
        if (entry.command.person_id!=row['person_id'] or entry.command.ids!={'source_id':row['source_id']}
                or entry.command.route!=expected_route):
            raise _failure('native_operation_identity_differs')
        state=await asyncio.to_thread(self._update,row,pending_operation_key=key,pending_kind=kind)
        return await self._drive(access,row,state)

    async def catalog_update(self, access, row, correction):
        """Separate exact catalog command on the existing owner-authorized stage path."""
        from .contracts import CatalogUpdateInput
        from .source_catalog_command import (ROUTE, CODEC, resolved_catalog_command,
            validate_catalog_payload)
        await self._check(access,row)
        update=CatalogUpdateInput.model_validate(correction).model_dump(mode='json',exclude_unset=True)
        doc=dict(codec=CODEC,ingestion_id=row['ingestion_id'],owner_person_id=row['person_id'],
            catalog_update=update)
        validate_catalog_payload(canonical(doc))
        key=self._operation(row,'catalog',digest(canonical(doc)))
        state=await asyncio.to_thread(self._state,row,create=True)
        if state['pending_operation_key']:
            state=await self._drive(access,row,state)
            if state['pending_operation_key'] or state['phase']=='failed':return self._output(row,state)
        state=await self._recover_orphan(access,row,state,'catalog',key)
        if not state['pending_operation_key']:
            await self._stage_exact(access,row,kind='catalog',key=key,route=ROUTE,
                payload=doc,builder=resolved_catalog_command)
            state=await asyncio.to_thread(self._state,row)
        state=await self._drive(access,row,state)
        if state['pending_operation_key']:return self._output(row,state)
        entry=await self._entry(key)
        if entry is None or entry.apply_receipt is None:
            return self.importer._output(row,'processing','Catalog outcome unconfirmed','wait')
        if entry.apply_receipt.status!='applied':
            return self.importer._output(row,'needs_review',
                'Catalog correction rejected; original source and accepted pages unchanged',
                'continue_pages',warnings=['catalog_update_rejected'])
        return self.importer._output(row,'finalized' if state['phase']=='finalized' else 'staged',
            'Catalog correction APPLIED; original stage metadata and accepted bytes unchanged',
            'done' if state['phase']=='finalized' else 'continue_pages')

    @_timed_native_phase('stage')
    async def stage(self, access, row, payload, source_import):
        from .contracts import StartMetadataInput
        from .kaggle_import_contracts import SourceResearchInput
        from .kaggle_native_source_bindings import resolved_native_stage_command
        await self._check(access,row)
        if type(payload) is dict and set(payload)=={'catalog_update'}:
            if source_import is not None: raise _failure('catalog_metadata_only_stage_required',status='invalid_query')
            return await self.catalog_update(access,row,payload['catalog_update'])
        raw_payload=canonical(payload)
        if len(raw_payload)>1024*1024: raise _failure('stage_envelope_bound',status='invalid_query')
        payload=json.loads(raw_payload)
        state=await asyncio.to_thread(self._state,row,create=True)
        pending=await self._entry(state['pending_operation_key'] or self._stage_key(row,state))
        if pending is not None and pending.command.route==STAGE_ROUTE and pending.outcome_state!='DURABLE':
            frozen=json.loads(pending.command.payload)
            # The image checksum in accepted bytes is server-issued, never
            # a value the model must echo on an exact lost-ACK retry.
            def without_image_echo(model):
                return {**model,'pages':[{k:v for k,v in page.items()
                    if k!='source_image_receipt_sha256'} for page in model['pages']]}
            if (without_image_echo(frozen['model_payload'])!=without_image_echo(payload)
                    or source_import is not None and source_import.research is not None
                    and {k:v for k,v in frozen['research'].items() if k!='frontier_id'}!=source_import.research.model_dump(mode='json')):
                raise _failure('native_pending_payload_changed',status='invalid_query')
        state=await self._recover_orphan(access,row,state,'stage',self._stage_key(row,state))
        if state['pending_operation_key'] or state['phase']=='failed': return self._output(row,state)
        if state['phase']!='staging': raise _failure('native_candidate_already_sealed',status='invalid_query')
        receipt=await self.importer._receipt(access,row)
        if receipt is None: return self.importer._output(row,'processing','Verified original pending','wait')
        if source_import is not None and source_import.public_rights is not None:
            if canonical(source_import.public_rights.model_dump(mode='json')).decode()!=row['rights_json']:
                raise _failure('original_public_rights_basis_is_immutable',status='invalid_query')
        research=source_import.research if source_import is not None else None
        if research is None:
            if not row['research_json']: raise _failure('model_authored_research_required',status='invalid_query')
            research=SourceResearchInput.model_validate_json(row['research_json'])
        if research.state=='pending':
            continuation=research.continuation
            if (continuation.get('tool')!='book_pages' or type(continuation.get('arguments')) is not dict
                    or continuation['arguments'].get('ingestion_id')!=row['ingestion_id']):
                raise _failure('research_requires_reachable_owned_page_continuation',status='invalid_query')
        limit=4
        if type(payload) is not dict or type(payload.get('pages')) is not list or not 1<=len(payload['pages'])<=limit:
            raise _failure('bounded_model_authored_selected_pages_required',status='invalid_query')
        selections=tuple((p['physical_page_index'],None) for p in payload['pages'])
        issued=await self.importer.originals.selected_image_receipts(access.context,row['ingestion_id'],selections)
        await self._check(access,row)
        # Fill the existing SQL/command integrity field from the controller's
        # authenticated issuance; model-supplied hashes are not authority.
        for page,image in zip(payload['pages'],issued,strict=True):
            page['source_image_receipt_sha256']=image['source_image_receipt_sha256']
        metadata=StartMetadataInput.model_validate_json(row['metadata_json']).model_dump(mode='json')
        bibliography='; '.join(filter(None,[', '.join(metadata['authors']),metadata['title'],
            str(metadata['publication_year']) if metadata['publication_year'] else None]))
        doc=dict(codec=BATCH_CODEC,ingestion_id=row['ingestion_id'],batch_index=state['batch_count'],
            previous_batch_sha256=state['last_batch_sha256'],source=dict(bibliography=bibliography,
                work_id=str(uuid5(UUID(row['source_id']),'work')),edition_id=str(uuid5(UUID(row['source_id']),'edition')),
                metadata=metadata,original_receipt_b64=base64.b64encode(canonical(receipt.wire())).decode(),
                public_rights=json.loads(row['rights_json'])),issued_images_b64=[encode_issued_image(v) for v in issued],
            model_payload=payload,research=dict(frontier_id=str(uuid5(UUID(row['ingestion_id']),'research')),
                **research.model_dump(mode='json')))
        validate_stage_payload(canonical(doc),source_id=row['source_id'])
        if (self._request_hash(doc)==state['last_request_sha256']
                and state.get('rejected_batch_index') is None):
            return self._output(row,await self._packet_progress(access,row,state))
        if state['enriched_batch_count']<state['batch_count']:
            # Only the newest packet is ever enriched; accepting the next one
            # now would silently drop the previous packet's entities/stories.
            state=await self._packet_progress(access,row,state)
            if state['pending_operation_key'] or state['enriched_batch_count']<state['batch_count']:
                return self.importer._output(row,'processing','Previous packet is still being applied; '
                    'call status, then resend this same stage','wait',source_archive_status='verified',
                    warnings=['previous_packet_applying'])
        builder=lambda **values: resolved_native_stage_command(route=STAGE_ROUTE,**values)
        await self._stage_exact(access,row,kind='stage',key=self._stage_key(row,state),
            route=STAGE_ROUTE,payload=doc,builder=builder)
        await asyncio.to_thread(self.importer._save,row,research_json=canonical(research.model_dump(mode='json')).decode(),phase='staging')
        state=await asyncio.to_thread(self._state,row)
        return self._output(row,await self._packet_progress(access,row,await self._drive(access,row,state)))

    async def _packet_progress(self, access, row, state):
        if (state['pending_operation_key'] or state['phase']!='staging'
                or state['enriched_batch_count']>=state['batch_count']):
            return state
        from .source_batch import CODEC,ROUTE,resolved_command
        from .kaggle_source_materializer import materializer_fingerprint
        scope=await self.spool.scope_state([row['source_id']])
        if (scope['unknown'] or scope['pending'] and not await asyncio.to_thread(self._inactive_prefix_current,access,row,scope)):
            return state
        idx=state['batch_count']-1
        component='text' if state['published_batch_count']<state['batch_count'] else 'semantic'
        key=self._operation(row,'batch',f'{idx}:{component}')
        state=await self._recover_orphan(access,row,state,'batch',key)
        if state['pending_operation_key'] or state['enriched_batch_count']>=state['batch_count']:
            return state
        if component=='text' and state['published_batch_count']>=state['batch_count']:
            return await self._packet_progress(access,row,state)
        doc=dict(codec=CODEC,ingestion_id=row['ingestion_id'],batch_index=idx,batch_count=state['batch_count'],
            last_batch_sha256=state['last_batch_sha256'],materializer_sha256=materializer_fingerprint(),component=component)
        await self._stage_exact(access,row,kind='batch',key=key,route=ROUTE,
            payload=doc,builder=resolved_command)
        return await self._drive(access,row,await asyncio.to_thread(self._state,row))

    @_timed_native_phase('seal')
    async def validate(self, access, row):
        await self._check(access,row)
        state=await asyncio.to_thread(self._state,row)
        if state is None: raise _failure('stage_before_validate',status='invalid_query')
        return self._output(row,await self._packet_progress(access,row,await self._drive(access,row,state)))

    @_timed_native_phase('materialize')
    async def finalize(self, access, row):
        await self._check(access,row)
        state=await asyncio.to_thread(self._state,row)
        if state is None: raise _failure('validate_before_finalize',status='invalid_query')
        if not state.get('read_completion_requested'):
            state=await asyncio.to_thread(self._update,row,read_completion_requested=1)
        state=await self._packet_progress(access,row,await self._drive(access,row,state))
        if state['pending_operation_key'] or state['phase'] in ('failed','finalized'):
            return self._output(row,state)
        if state['enriched_batch_count']<state['batch_count'] or not state['read_completion_requested']:
            return self._output(row,state)
        tails=await self._tail_output(access,row,state)
        if tails is not None:return tails
        from .source_batch import FINISH_ROUTE,resolved_command
        key=self._operation(row,'finish')+':'+str(state['command_attempt'])
        state=await self._recover_orphan(access,row,state,'finish',key)
        if state['pending_operation_key'] or state['phase']=='finalized':return self._output(row,state)
        scope=await self.spool.scope_state([row['source_id']])
        if (scope['unknown'] or scope['pending'] and not await asyncio.to_thread(self._inactive_prefix_current,access,row,scope)):
            return self._output(row,state)
        await self._stage_exact(access,row,kind='finish',key=key,route=FINISH_ROUTE,
            payload=dict(codec='rkb.source-finish.v1',ingestion_id=row['ingestion_id']),builder=resolved_command)
        return self._output(row,await self._drive(access,row,await asyncio.to_thread(self._state,row)))

    async def status(self, access, row):
        """Resume existing intent; a status read never invents a seal/finalize."""
        await self._check(access,row)
        state=await asyncio.to_thread(self._state,row)
        if state is None:
            receipt=await self.importer._receipt(access,row)
            if receipt is None and row['phase'] in ('acquiring','failed'):
                # No native stage exists yet. Read the same durable original
                # failure_code as start; do not turn terminal PDF rejection
                # back into perpetual processing on the next MCP status.
                original=await self.importer.originals.status(access.context,row['ingestion_id'])
                return await self.importer._original_pending(row,original)
            if receipt is not None and row['phase']=='acquiring':
                row=await asyncio.to_thread(self.importer._save,row,phase='staging')
            return self.importer._output(row,'staged' if receipt else 'processing',
                'Review issued pages and stage exact model input' if receipt else 'Verified original pending',
                'continue_pages' if receipt else 'wait',source_archive_status='verified' if receipt else 'pending')
        if state.get('read_completion_requested') and state['phase']!='finalized':
            return await self.finalize(access,row)
        state=await self._drive(access,row,state)
        state=await self._packet_progress(access,row,state)
        tails=await self._tail_output(access,row,state)
        return tails if tails is not None else self._output(row,state)
