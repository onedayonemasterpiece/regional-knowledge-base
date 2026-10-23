"""Compact owner control for native source stage/materialize/activate commands.

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
from functools import wraps
from uuid import UUID, uuid5

from .source_stage import (STAGE_ROUTE, SEAL_ROUTE, BATCH_CODEC, SEAL_CODEC, ZERO_HASH,
    canonical, digest, encode_issued_image, validate_stage_payload)


_LOG=logging.getLogger(__name__)


def _failure(reason, *, status='unavailable'):
    from .kaggle_importer import failure
    return failure(reason, status=status)


def _timed_native_phase(phase):
    """Outer tool-call duration, not provider ACK or persisted Saved."""
    def decorate(method):
        @wraps(method)
        async def observed(self, access, row, *args, **kwargs):
            from .product_telemetry import emit_product_event
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
                    ref = hashlib.sha256(str(row['ingestion_id']).encode()).hexdigest()
                    emit_product_event({'event':'import.phase','phase':phase,'status':status,
                        'operation_ref':ref,'duration_ms':(time.monotonic()-started)*1000})
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
        self._initialize()

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
            db.execute("CREATE UNIQUE INDEX IF NOT EXISTS rkb_native_one_owner_intake "
                "ON rkb_native_source_intake_state(person_id) WHERE phase NOT IN ('finalized','failed')")
            columns={value[1] for value in db.execute('PRAGMA table_info(rkb_native_source_intake_state)')}
            if 'rejected_seal_head' not in columns:
                db.execute('ALTER TABLE rkb_native_source_intake_state ADD COLUMN rejected_seal_head TEXT')
            for name, declaration in (('packet_mode','INTEGER NOT NULL DEFAULT 0'),
                    ('published_batch_count','INTEGER NOT NULL DEFAULT 0'),('rejected_batch_index','INTEGER'),
                    ('read_completion_requested','INTEGER NOT NULL DEFAULT 0'),('enriched_batch_count','INTEGER NOT NULL DEFAULT 0'),
                    ('command_attempt','INTEGER NOT NULL DEFAULT 0'),('rejected_reason','TEXT')):
                if name not in columns:
                    db.execute(f'ALTER TABLE rkb_native_source_intake_state ADD COLUMN {name} {declaration}')

    async def _check(self, access, row=None):
        await self.importer._check(access.authority, access.context, access.binding)
        if row is not None and row['person_id']!=access.authority.person_id:
            raise _failure('owned_ingestion_required',status='denied')

    async def retire_inactive_legacy(self, access):
        """An obsolete inactive intake must not monopolize ordinary start.

        Keep its accepted inputs and receipts. An uncertain operation or an
        already-published source is never retired by this compatibility path.
        """
        await self._check(access)
        with self.queue.connect(write=False) as db:
            pending_ids=[r[0] for r in db.execute('''SELECT ingestion_id FROM rkb_native_source_intake_state
                WHERE person_id=? AND packet_mode=0 AND pending_operation_key IS NOT NULL
                AND phase NOT IN ('finalized','failed')''',(access.authority.person_id,))]
        for ingestion_id in pending_ids:
            row=await asyncio.to_thread(self.importer._row,access.authority.person_id,ingestion_id)
            state=await asyncio.to_thread(self._state,row)
            entry=await self._entry(state['pending_operation_key'])
            if entry is not None and entry.outcome_state=='DURABLE':
                # Settle only a known receipt. Starting another import must
                # never dispatch an abandoned older command as a side effect.
                await self._settle(access,row,state,entry)
        with self.queue.connect(write=False) as db:
            ids=[r[0] for r in db.execute('''SELECT st.ingestion_id FROM rkb_native_source_intake_state st
                WHERE st.person_id=? AND st.packet_mode=0 AND st.phase NOT IN ('finalized','failed')
                AND st.pending_operation_key IS NULL
                AND NOT EXISTS(SELECT 1 FROM rkb_control_operations op WHERE op.source_id=st.source_id
                    AND (op.seal_state<>'ACCEPTED' OR op.apply_state NOT IN ('APPLIED','REJECTED_PRECONDITION')
                        OR op.outcome_state<>'DURABLE'))
                AND NOT EXISTS(SELECT 1 FROM rkb_control_operations op WHERE op.source_id=st.source_id
                    AND op.apply_state='APPLIED' AND json_extract(CAST(op.command_bytes AS TEXT),'$.route')='source.activate.v1')''',
                (access.authority.person_id,))]
        for ingestion_id in ids:
            row=await asyncio.to_thread(self.importer._row,access.authority.person_id,ingestion_id)
            await self._check(access,row)
            await asyncio.to_thread(self.importer._save,row,phase='failed')
            await asyncio.to_thread(self._update,row,phase='failed')
            _LOG.info('native_intake_progress ingestion_id=%s state=inactive_legacy_retired',ingestion_id)

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
    def _operation(row, kind, ordinal=None, *, head=None):
        if kind=='stage': return f"source-stage:{row['ingestion_id']}:{ordinal}"
        if kind=='batch': return f"source-packet:{row['ingestion_id']}:{ordinal}"
        if kind=='finish': return f"source-finish:{row['ingestion_id']}"
        if kind=='materialize': return f"source-materialize:{row['ingestion_id']}:{ordinal}"
        if kind=='seal':
            if type(head) is not str or len(head)!=64 or set(head)-set('0123456789abcdef'):
                raise ValueError('exact candidate seal head required')
            return f"source-stage-seal:{row['ingestion_id']}:{head}"
        return 'source-activate:'+row['ingestion_id']

    async def _entry(self, key):
        from .control_spool import SpoolError
        try: return await self.spool.get(key)
        except SpoolError as error:
            if str(error)=='control operation not found': return None
            raise

    def _stage_key(self,row,state):
        ordinal=(f"{state['batch_count']}:{state['command_attempt']}" if state.get('packet_mode') else state['batch_count'])
        return self._operation(row,'stage',ordinal)

    @staticmethod
    def _request_hash(doc):
        return digest(canonical({k:v for k,v in doc.items()
            if k not in ('codec','batch_index','previous_batch_sha256')}))

    def _inactive_prefix_current(self, access, row, scope):
        """Continue only this owned inactive prefix proved APPLIED by history."""
        proof=getattr(self.startup,'coverage',None)
        if (proof is None or not proof.current() or getattr(proof,'binding',None)!=access.binding
                or self.spool._source_anchor is not proof
                or getattr(proof,'accepted_sequence',None)!=scope['accepted_sequence']
                or getattr(proof,'applied_sequence',None)!=scope['accepted_sequence']):
            return False
        from .control_spool import _command
        from .source_history_verifier import INACTIVE_SOURCE_ROUTES
        with self.queue.connect(write=False) as db:
            rows=db.execute("SELECT command_bytes,source_id,person_id,database_generation,seal_state,apply_state,outcome_state "
                "FROM rkb_control_operations WHERE outcome_state<>'DURABLE'").fetchall()
        if not rows:return False
        for pending in rows:
            command=_command(pending['command_bytes'])
            applied=(pending['apply_state']=='APPLIED' or
                command.route in (STAGE_ROUTE,'source.batch.v1','source.finish.v1') and pending['apply_state']=='REJECTED_PRECONDITION')
            if (pending['source_id']!=row['source_id'] or pending['person_id']!=row['person_id']
                    or pending['database_generation']!=access.binding.database_generation
                    or pending['seal_state']!='ACCEPTED' or not applied
                    or pending['outcome_state']!='NOT_SENT'):
                return False
            if (command.route not in INACTIVE_SOURCE_ROUTES|{'source.batch.v1','source.finish.v1'}
                    or json.loads(command.payload).get('ingestion_id')!=row['ingestion_id']):
                return False
        return proof.current()

    @staticmethod
    def _can_settle(state,entry):
        if entry.outcome_state=='DURABLE':return True
        if state.get('packet_mode') and state['pending_kind']=='finish':
            return (entry.outcome_state=='NOT_SENT' and entry.accepted is not None and entry.seal_state=='ACCEPTED'
                and entry.apply_receipt is not None and getattr(entry,'apply_state',None)=='REJECTED_PRECONDITION'
                and entry.apply_receipt.status=='rejected_precondition')
        if state['pending_kind']=='batch' or state.get('packet_mode') and state['pending_kind']=='stage':
            return (entry.outcome_state=='NOT_SENT' and entry.accepted is not None and entry.seal_state=='ACCEPTED'
                and entry.apply_receipt is not None and
                (getattr(entry,'apply_state',None),entry.apply_receipt.status) in
                    (('APPLIED','applied'),('REJECTED_PRECONDITION','rejected_precondition')))
        return (state['pending_kind'] in ('stage','seal','materialize','batch')
            and entry.outcome_state=='NOT_SENT' and entry.accepted is not None
            and entry.seal_state=='ACCEPTED' and getattr(entry,'apply_state',None)=='APPLIED'
            and entry.apply_receipt is not None and entry.apply_receipt.status=='applied')

    async def _stage_exact(self, access, row, *, kind, key, route, payload, builder):
        async with self.spool._lock:
            entry=await self._entry(key)
            if entry is not None:
                if (entry.command.route!=route or entry.command.payload!=canonical(payload)
                        or entry.command.ids!={'source_id':row['source_id']}
                        or entry.command.person_id!=row['person_id']):
                    raise _failure('native_operation_payload_changed',status='invalid_query')
            else:
                await self._check(access)
                scope=await self.spool.scope_state()
                if scope['unknown'] or scope['sequence']!=scope['accepted_sequence']:
                    raise _failure('prior_write_pending')
                continuation=(scope['pending'] and kind in ('stage','seal','materialize','batch','finish')
                    and await asyncio.to_thread(self._inactive_prefix_current,access,row,scope))
                if scope['pending'] and not continuation:raise _failure('prior_write_pending')
                head=await self.spool.journal.head()
                sequence=head.reference.last_sequence if head else 0
                if sequence!=scope['accepted_sequence']:
                    _LOG.warning('native_intake_admission reason=journal_prefix kind=%s journal_sequence=%s accepted_sequence=%s',
                        kind,sequence,scope['accepted_sequence'])
                    raise _failure('source_history_unavailable')
                command=builder(sequence=sequence+1,operation_key=key,request_id=row['request_id'],
                    person_id=row['person_id'],logical_time=datetime.now(timezone.utc).isoformat(),
                    revisions=self.importer._revisions(access.binding),source_id=row['source_id'],payload=payload)
                frozen,data,handler=self.spool._validate(command,access.binding.database_generation)
                await self._check(access)
                facts=await self.spool.read_facts(None)
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
                entry=await self.spool._write(self.spool._stage,frozen,data,handler,access.binding.database_generation)
            # If the process exits between the spool commit and this compact
            # mark, the stable key is reconciled before any new allocation.
            await asyncio.to_thread(self._update,row,pending_operation_key=key,pending_kind=kind)
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
            if state.get('packet_mode') and state['pending_kind'] in ('stage','batch','finish'):
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
            if state['pending_kind']=='seal':
                # Schema14's native preflight rejects before creating the
                # immutable candidate. Preserve the rejected accepted receipt,
                # keep staging open, and never resend the unchanged failed head.
                await asyncio.to_thread(self.importer._save,row,phase='staging')
                return await asyncio.to_thread(self._update,row,phase='staging',pending_operation_key=None,
                    pending_kind=None,rejected_seal_head=state['last_batch_sha256'])
            await asyncio.to_thread(self.importer._save,row,phase='failed')
            return await asyncio.to_thread(self._update,row,phase='failed',pending_operation_key=None,pending_kind=None)
        result=self._result(entry); doc=json.loads(entry.command.payload)
        if result.get('source_id')!=row['source_id']:
            raise _failure('native_outcome_identity_differs')
        kind=state['pending_kind']; changes=dict(pending_operation_key=None,pending_kind=None)
        if kind in ('stage','seal') and result.get('ingestion_id')!=row['ingestion_id']:
            raise _failure('native_outcome_identity_differs')
        if kind=='stage':
            if (result.get('batch_index')!=doc['batch_index'] or result.get('batch_sha256')!=digest(entry.command.payload)
                    or result.get('candidate_state')!='staged_inactive'):
                raise _failure('native_stage_outcome_differs')
            changes.update(batch_count=doc['batch_index']+1,last_batch_sha256=digest(entry.command.payload),
                last_request_sha256=self._request_hash(doc),phase='staging',rejected_seal_head=None,
                rejected_batch_index=None,rejected_reason=None)
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
        elif kind=='seal':
            if (result.get('seal_sha256')!=digest(entry.command.payload) or result.get('candidate_state')!='sealed_inactive'
                    or result.get('batch_count')!=state['batch_count'] or result.get('last_batch_sha256')!=state['last_batch_sha256']):
                raise _failure('native_seal_outcome_differs')
            changes.update(seal_sha256=digest(entry.command.payload),phase='sealed')
        elif kind=='materialize':
            total=result.get('total_inputs'); completed=doc['ordinal']+1; knowledge=result.get('knowledge_sha256')
            if (type(total) is not int or not 1<=total<=280000 or type(result.get('ordinal')) is not int
                    or result.get('ordinal')!=doc['ordinal']
                    or not 1<=completed<=total
                    or state['total_inputs'] is not None and state['total_inputs']!=total
                    or type(knowledge) is not str or len(knowledge)!=64 or set(knowledge)-set('0123456789abcdef')
                    or state['knowledge_sha256'] is not None and state['knowledge_sha256']!=knowledge):
                raise _failure('native_materialization_outcome_differs')
            changes.update(materializer_sha256=doc['materializer_sha256'],next_ordinal=completed,
                total_inputs=total,knowledge_sha256=knowledge,phase='ready_activation' if completed==total else 'materializing')
        elif kind=='activate':
            if result.get('accepted') is not True or result.get('knowledge_sha256')!=state['knowledge_sha256']:
                raise _failure('native_activation_outcome_differs')
            changes['phase']='finalized'
            await asyncio.to_thread(self.importer._save,row,phase='finalized')
        else: raise _failure('native_pending_operation_invalid')
        updated=await asyncio.to_thread(self._update,row,**changes)
        _LOG.info('native_intake_progress ingestion_id=%s kind=%s remote_durable=%s next_ordinal=%s',
            row['ingestion_id'],kind,entry.outcome_state=='DURABLE',updated['next_ordinal'])
        # Applied != Saved: log only, do not modify actual remote receipt.
        from .product_telemetry import emit_product_event
        emit_product_event({'event':'import.phase','phase':{
            'stage':'stage','seal':'seal','materialize':'materialize',
            'activate':'activation','batch':'stage','finish':'saved'}[kind], 'status':'applied',
            'operation_ref':hashlib.sha256(entry.command.operation_key.encode()).hexdigest()})
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

    def _output(self, row, state):
        phase=state['phase']
        if phase=='failed': return self.importer._output(row,'failed','Native command rejected; no successful source activation','blocker')
        if state['pending_operation_key']:
            return self.importer._output(row,'processing','Exact native command pending apply and durable outcome; resume same operation','wait',source_archive_status='verified')
        if state.get('packet_mode') and phase=='staging':
            if state['enriched_batch_count']<state['batch_count'] or state.get('read_completion_requested'):
                return self.importer._output(row,'processing','Current packet or reading-completion intent pending; resume status',
                    'wait',source_archive_status='verified')
            if state.get('rejected_batch_index') is not None:
                reason=state.get('rejected_reason') or 'native_source_packet_requires_review'
                return self.importer._output(row,'staged',reason+': correct or retry this packet; earlier packets remain searchable',
                    'continue_pages',source_archive_status='verified',warnings=[reason])
            return self.importer._output(row,'staged','Accepted packet published; continue reading pages. Dataset persistence is periodic.',
                'continue_pages',source_archive_status='verified')
        if phase=='finalized':
            if state.get('packet_mode'):
                return self.importer._output(row,'finalized',
                    'Reading completed; accepted packets remain searchable and research continuation is retained','done',
                    source_archive_status='verified',warnings=[])
            return self.importer._output(row,'finalized','Native source activation outcome remotely durable; retrieval acceptance is separate','done',
                source_archive_status='verified',warnings=[] if self.startup.reads_current() else ['source_history_refresh_pending'])
        if phase=='staging' and state['rejected_seal_head']==state['last_batch_sha256']:
            return self.importer._output(row,'staged','native_source_stage_requires_review: correct or extend reviewed input before validating again',
                'blocker',source_archive_status='verified',warnings=['native_source_stage_requires_review'])
        if phase in ('sealed','materializing','ready_activation'):
            return self.importer._output(row,'ready' if phase=='sealed' else 'processing',
                'Accepted batch chain sealed; bounded native materialization and activation still required','finalize',source_archive_status='verified')
        return self.importer._output(row,'staged','Exact model batch retained natively; continue reviewed pages or seal the candidate','validate',source_archive_status='verified')

    async def _recover_orphan(self, access, row, state, kind, key):
        if state['pending_operation_key']: return await self._drive(access,row,state)
        entry=await self._entry(key)
        if entry is None: return state
        expected_route={'stage':STAGE_ROUTE,'seal':SEAL_ROUTE,'materialize':'source.materialize.v1','activate':'source.activate.v1','batch':'source.batch.v1','finish':'source.finish.v1'}[kind]
        if (entry.command.person_id!=row['person_id'] or entry.command.ids!={'source_id':row['source_id']}
                or entry.command.route!=expected_route):
            raise _failure('native_operation_identity_differs')
        state=await asyncio.to_thread(self._update,row,pending_operation_key=key,pending_kind=kind)
        return await self._drive(access,row,state)

    @_timed_native_phase('stage')
    async def stage(self, access, row, payload, source_import):
        from .contracts import StartMetadataInput
        from .kaggle_import_contracts import SourceResearchInput
        from .kaggle_native_source_bindings import resolved_native_stage_command
        await self._check(access,row)
        raw_payload=canonical(payload)
        if len(raw_payload)>1024*1024: raise _failure('stage_envelope_bound',status='invalid_query')
        payload=json.loads(raw_payload)
        state=await asyncio.to_thread(self._state,row,create=True)
        pending=await self._entry(state['pending_operation_key'] or self._stage_key(row,state))
        if pending is not None and pending.command.route==STAGE_ROUTE and pending.outcome_state!='DURABLE':
            frozen=json.loads(pending.command.payload)
            if frozen['model_payload']!=payload or (source_import is not None and source_import.research is not None
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
        limit=4 if state.get('packet_mode') else 8
        if type(payload) is not dict or type(payload.get('pages')) is not list or not 1<=len(payload['pages'])<=limit:
            raise _failure('bounded_model_authored_selected_pages_required',status='invalid_query')
        selections=tuple((p['physical_page_index'],p['source_image_receipt_sha256']) for p in payload['pages'])
        issued=await self.importer.originals.selected_image_receipts(access.context,row['ingestion_id'],selections)
        await self._check(access,row)
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
        if self._request_hash(doc)==state['last_request_sha256']:
            return self._output(row,await self._packet_progress(access,row,state))
        builder=lambda **values: resolved_native_stage_command(route=STAGE_ROUTE,**values)
        await self._stage_exact(access,row,kind='stage',key=self._stage_key(row,state),
            route=STAGE_ROUTE,payload=doc,builder=builder)
        await asyncio.to_thread(self.importer._save,row,research_json=canonical(research.model_dump(mode='json')).decode(),phase='staging')
        state=await asyncio.to_thread(self._state,row)
        return self._output(row,await self._packet_progress(access,row,await self._drive(access,row,state)))

    async def _packet_progress(self, access, row, state):
        if (not state.get('packet_mode') or state['pending_operation_key'] or state['phase']!='staging'
                or state['enriched_batch_count']>=state['batch_count']):
            return state
        from .source_batch import CODEC,ROUTE,resolved_command
        from .kaggle_source_materializer import materializer_fingerprint
        scope=await self.spool.scope_state()
        if (scope['unknown'] or scope['sequence']!=scope['accepted_sequence']
                or scope['pending'] and not await asyncio.to_thread(self._inactive_prefix_current,access,row,scope)):
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
        from .kaggle_native_source_bindings import resolved_native_stage_command
        await self._check(access,row)
        state=await asyncio.to_thread(self._state,row)
        if state is None: raise _failure('stage_before_validate',status='invalid_query')
        if state.get('packet_mode'):
            return self._output(row,await self._packet_progress(access,row,await self._drive(access,row,state)))
        state=await self._recover_orphan(access,row,state,'stage',self._operation(row,'stage',state['batch_count']))
        if state['pending_operation_key'] or state['phase']=='failed': return self._output(row,state)
        if state['phase']!='staging': return self._output(row,state)
        if not state['batch_count']: raise _failure('stage_before_validate',status='invalid_query')
        if state['rejected_seal_head']==state['last_batch_sha256']: return self._output(row,state)
        doc=dict(codec=SEAL_CODEC,ingestion_id=row['ingestion_id'],batch_count=state['batch_count'],last_batch_sha256=state['last_batch_sha256'])
        builder=lambda **values: resolved_native_stage_command(route=SEAL_ROUTE,**values)
        await self._stage_exact(access,row,kind='seal',key=self._operation(row,'seal',head=state['last_batch_sha256']),route=SEAL_ROUTE,payload=doc,builder=builder)
        state=await asyncio.to_thread(self._state,row)
        return self._output(row,await self._drive(access,row,state))

    @_timed_native_phase('materialize')
    async def finalize(self, access, row):
        from .source_activation import (ACTIVATE_ROUTE, ACTIVATION_CODEC, MATERIALIZE_ROUTE,
            MATERIALIZE_CODEC, resolved_activation_command, resolved_materialize_command)
        from .kaggle_source_materializer import materializer_fingerprint
        await self._check(access,row)
        state=await asyncio.to_thread(self._state,row)
        if state is None: raise _failure('validate_before_finalize',status='invalid_query')
        if state.get('packet_mode'):
            if not state.get('read_completion_requested'):
                state=await asyncio.to_thread(self._update,row,read_completion_requested=1)
            state=await self._packet_progress(access,row,await self._drive(access,row,state))
            if state['pending_operation_key'] or state['phase'] in ('failed','finalized'):
                return self._output(row,state)
            if state['enriched_batch_count']<state['batch_count'] or not state['read_completion_requested']:
                return self._output(row,state)
            from .source_batch import FINISH_ROUTE,resolved_command
            key=self._operation(row,'finish')+':'+str(state['command_attempt'])
            state=await self._recover_orphan(access,row,state,'finish',key)
            if state['pending_operation_key'] or state['phase']=='finalized':return self._output(row,state)
            scope=await self.spool.scope_state()
            if (scope['unknown'] or scope['sequence']!=scope['accepted_sequence']
                    or scope['pending'] and not await asyncio.to_thread(self._inactive_prefix_current,access,row,scope)):
                return self._output(row,state)
            await self._stage_exact(access,row,kind='finish',key=key,route=FINISH_ROUTE,
                payload=dict(codec='rkb.source-finish.v1',ingestion_id=row['ingestion_id']),builder=resolved_command)
            return self._output(row,await self._drive(access,row,await asyncio.to_thread(self._state,row)))
        state=await self._drive(access,row,state)
        if state['pending_operation_key'] or state['phase'] in ('failed','finalized'): return self._output(row,state)
        if state['phase']=='staging':
            if state['rejected_seal_head']==state['last_batch_sha256']: return self._output(row,state)
            state=await self._recover_orphan(access,row,state,'seal',self._operation(row,'seal',head=state['last_batch_sha256']))
            if state['rejected_seal_head']==state['last_batch_sha256']: return self._output(row,state)
            if state['phase']=='staging': raise _failure('validate_before_finalize',status='invalid_query')
            if state['pending_operation_key']: return self._output(row,state)
        orphan_kind='activate' if state['phase']=='ready_activation' else 'materialize'
        state=await self._recover_orphan(access,row,state,orphan_kind,
            self._operation(row,orphan_kind,state['next_ordinal']))
        if state['pending_operation_key'] or state['phase'] in ('failed','finalized'): return self._output(row,state)
        fingerprint=materializer_fingerprint()
        if state['materializer_sha256'] is not None and state['materializer_sha256']!=fingerprint:
            raise _failure('accepted_native_materializer_changed')
        doc=dict(ingestion_id=row['ingestion_id'],batch_count=state['batch_count'],
            last_batch_sha256=state['last_batch_sha256'],materializer_sha256=fingerprint)
        if state['phase']!='ready_activation':
            # One bounded native input command per poll. The existing queue can
            # serve QUERY between commands; never one whole-book inference call.
            kind='materialize'; route=MATERIALIZE_ROUTE; key=self._operation(row,kind,state['next_ordinal'])
            doc.update(codec=MATERIALIZE_CODEC,ordinal=state['next_ordinal']); builder=resolved_materialize_command
        else:
            kind='activate'; route=ACTIVATE_ROUTE; key=self._operation(row,kind)
            doc['codec']=ACTIVATION_CODEC; builder=resolved_activation_command
        scope=await self.spool.scope_state()
        if (scope['unknown'] or scope['sequence']!=scope['accepted_sequence']
                or scope['pending'] and (kind=='activate'
                    or not await asyncio.to_thread(self._inactive_prefix_current,access,row,scope))):
            return self.importer._output(row,'processing',
                'Inactive candidate is awaiting current-prefix verification or remote durability; resume the same finalize',
                'wait',source_archive_status='verified')
        await self._stage_exact(access,row,kind=kind,key=key,route=route,payload=doc,builder=builder)
        state=await asyncio.to_thread(self._state,row)
        return self._output(row,await self._drive(access,row,state))

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
            return self.importer._output(row,'staged' if receipt else 'processing',
                'Review issued pages and stage exact model input' if receipt else 'Verified original pending',
                'continue_pages' if receipt else 'wait',source_archive_status='verified' if receipt else 'pending')
        if state.get('packet_mode'):
            if state.get('read_completion_requested') and state['phase']!='finalized':
                return await self.finalize(access,row)
            state=await self._drive(access,row,state)
            return self._output(row,await self._packet_progress(access,row,state))
        if state['pending_kind'] in ('materialize','activate') or state['phase'] in ('materializing','ready_activation'):
            return await self.finalize(access,row)
        if state['phase']=='sealed' and await self._entry(self._operation(row,'materialize',state['next_ordinal'])) is not None:
            # A persisted spool command is the finalize intent even if the
            # controller crashed before marking it pending locally.
            return await self.finalize(access,row)
        if state['pending_operation_key']:
            return self._output(row,await self._drive(access,row,state))
        if state['phase']=='staging':
            state=await self._recover_orphan(access,row,state,'stage',self._operation(row,'stage',state['batch_count']))
            if (not state['pending_operation_key'] and state['batch_count']
                    and state['rejected_seal_head']!=state['last_batch_sha256']):
                state=await self._recover_orphan(access,row,state,'seal',
                    self._operation(row,'seal',head=state['last_batch_sha256']))
        return self._output(row,state)
