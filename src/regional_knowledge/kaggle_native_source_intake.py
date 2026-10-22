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
import sqlite3
from uuid import UUID, uuid5

from .source_stage import (STAGE_ROUTE, SEAL_ROUTE, BATCH_CODEC, SEAL_CODEC, ZERO_HASH,
    canonical, digest, encode_issued_image, validate_stage_payload)


def _failure(reason, *, status='unavailable'):
    from .kaggle_importer import failure
    return failure(reason, status=status)


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

    async def _check(self, access, row=None):
        await self.importer._check(access.authority, access.context, access.binding)
        if row is not None and row['person_id']!=access.authority.person_id:
            raise _failure('owned_ingestion_required',status='denied')

    def _state(self, row, *, create=False):
        with self.queue.connect(write=create) as db:
            if create:
                try:
                    db.execute('''INSERT OR IGNORE INTO rkb_native_source_intake_state
                        (ingestion_id,person_id,source_id,last_batch_sha256) VALUES(?,?,?,?)''',
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

    @staticmethod
    def _request_hash(doc):
        return digest(canonical({k:v for k,v in doc.items()
            if k not in ('codec','batch_index','previous_batch_sha256')}))

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
                if scope['pending'] or scope['unknown'] or scope['sequence']!=scope['accepted_sequence']:
                    raise _failure('prior_write_pending')
                head=await self.spool.journal.head()
                sequence=head.reference.last_sequence if head else 0
                if sequence!=scope['accepted_sequence']: raise _failure('source_history_unavailable')
                command=builder(sequence=sequence+1,operation_key=key,request_id=row['request_id'],
                    person_id=row['person_id'],logical_time=datetime.now(timezone.utc).isoformat(),
                    revisions=self.importer._revisions(access.binding),source_id=row['source_id'],payload=payload)
                frozen,data,handler=self.spool._validate(command,access.binding.database_generation)
                await self._check(access)
                facts=await self.spool.read_facts(None)
                await self._check(access)
                if (not self.importer._intake_coverage_current(access.binding)
                        or facts.database_generation!=access.binding.database_generation
                        or facts.scope_sequence>facts.covered_sequence or facts.pending or facts.unknown
                        or not self.spool.facts_current(facts)):
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
        if entry.outcome_state!='DURABLE': return state
        if entry.apply_receipt is None: raise _failure('native_durable_receipt_missing')
        if entry.apply_receipt.status not in ('applied','rejected_precondition'):
            raise _failure('native_durable_outcome_status_unknown')
        if entry.apply_receipt.status!='applied':
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
                last_request_sha256=self._request_hash(doc),phase='staging',rejected_seal_head=None)
            await asyncio.to_thread(self.importer._save,row,phase='staging',
                research_json=canonical({k:v for k,v in doc['research'].items() if k!='frontier_id'}).decode())
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
        return await asyncio.to_thread(self._update,row,**changes)

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
        if entry.outcome_state=='DURABLE': return await self._settle(access,row,state,entry)
        await self.startup.source_dispatch.enqueue(access.context,entry.command.operation_key,
            writer_generation=self.startup.config.writer_generation)
        await self._check(access)
        return state

    def _output(self, row, state):
        phase=state['phase']
        if phase=='failed': return self.importer._output(row,'failed','Native command rejected; no successful source activation','blocker')
        if state['pending_operation_key']:
            return self.importer._output(row,'processing','Exact native command pending apply and durable outcome; resume same operation','wait',source_archive_status='verified')
        if phase=='finalized':
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
        expected_route={'stage':STAGE_ROUTE,'seal':SEAL_ROUTE,'materialize':'source.materialize.v1','activate':'source.activate.v1'}[kind]
        if (entry.command.person_id!=row['person_id'] or entry.command.ids!={'source_id':row['source_id']}
                or entry.command.route!=expected_route):
            raise _failure('native_operation_identity_differs')
        state=await asyncio.to_thread(self._update,row,pending_operation_key=key,pending_kind=kind)
        return await self._drive(access,row,state)

    async def stage(self, access, row, payload, source_import):
        from .contracts import StartMetadataInput
        from .kaggle_import_contracts import SourceResearchInput
        from .kaggle_native_source_bindings import resolved_native_stage_command
        await self._check(access,row)
        raw_payload=canonical(payload)
        if len(raw_payload)>1024*1024: raise _failure('stage_envelope_bound',status='invalid_query')
        payload=json.loads(raw_payload)
        state=await asyncio.to_thread(self._state,row,create=True)
        pending=await self._entry(state['pending_operation_key'] or self._operation(row,'stage',state['batch_count']))
        if pending is not None and pending.command.route==STAGE_ROUTE and pending.outcome_state!='DURABLE':
            frozen=json.loads(pending.command.payload)
            if frozen['model_payload']!=payload or (source_import is not None and source_import.research is not None
                    and {k:v for k,v in frozen['research'].items() if k!='frontier_id'}!=source_import.research.model_dump(mode='json')):
                raise _failure('native_pending_payload_changed',status='invalid_query')
        state=await self._recover_orphan(access,row,state,'stage',self._operation(row,'stage',state['batch_count']))
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
        if type(payload) is not dict or type(payload.get('pages')) is not list or not 1<=len(payload['pages'])<=8:
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
        if self._request_hash(doc)==state['last_request_sha256']: return self._output(row,state)
        builder=lambda **values: resolved_native_stage_command(route=STAGE_ROUTE,**values)
        await self._stage_exact(access,row,kind='stage',key=self._operation(row,'stage',state['batch_count']),
            route=STAGE_ROUTE,payload=doc,builder=builder)
        await asyncio.to_thread(self.importer._save,row,research_json=canonical(research.model_dump(mode='json')).decode(),phase='staging')
        state=await asyncio.to_thread(self._state,row)
        return self._output(row,await self._drive(access,row,state))

    async def validate(self, access, row):
        from .kaggle_native_source_bindings import resolved_native_stage_command
        await self._check(access,row)
        state=await asyncio.to_thread(self._state,row)
        if state is None: raise _failure('stage_before_validate',status='invalid_query')
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

    async def finalize(self, access, row):
        from .source_activation import (ACTIVATE_ROUTE, ACTIVATION_CODEC, MATERIALIZE_ROUTE,
            MATERIALIZE_CODEC, resolved_activation_command, resolved_materialize_command)
        from .kaggle_source_materializer import materializer_fingerprint
        await self._check(access,row)
        state=await asyncio.to_thread(self._state,row)
        if state is None: raise _failure('validate_before_finalize',status='invalid_query')
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
        await self._stage_exact(access,row,kind=kind,key=key,route=route,payload=doc,builder=builder)
        state=await asyncio.to_thread(self._state,row)
        return self._output(row,await self._drive(access,row,state))

    async def status(self, access, row):
        """Resume existing intent; a status read never invents a seal/finalize."""
        await self._check(access,row)
        state=await asyncio.to_thread(self._state,row)
        if state is None:
            receipt=await self.importer._receipt(access,row)
            return self.importer._output(row,'staged' if receipt else 'processing',
                'Review issued pages and stage exact model input' if receipt else 'Verified original pending',
                'continue_pages' if receipt else 'wait',source_archive_status='verified' if receipt else 'pending')
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



class NativeSourceRevision2Intake(NativeSourceIntake):
    """Same owner spool/lease/UNKNOWN mechanics with a *separate* version2 state.

    Old rkb_native_source_intake_state and accepted source_id remain immutable.
    New controller records have their own ingestion_id but the SAME source_id
    and inherit the exact already-verified original receipt.
    """
    def __init__(self,importer):
        from .kaggle_native_revision2_upgrade import REVISION2_REVISIONS
        if importer.import_revisions!=REVISION2_REVISIONS:
            raise TypeError('installed schema17 required for accepted-source successor')
        self.importer=importer
        self.queue,self.spool,self.startup=importer.queue,importer.spool,importer.startup
        with self.queue.connect() as db:
            db.execute('''CREATE TABLE IF NOT EXISTS rkb_native_source_revision2_state(
                ingestion_id TEXT PRIMARY KEY,source_id TEXT UNIQUE NOT NULL,person_id TEXT NOT NULL,
                original_ingestion_id TEXT NOT NULL,predecessor_command_sha256 TEXT NOT NULL,
                batch_count INTEGER NOT NULL DEFAULT 0,last_batch_sha256 TEXT NOT NULL,
                last_request_sha256 TEXT,pending_operation_key TEXT,pending_kind TEXT,
                phase TEXT NOT NULL DEFAULT 'staging',seal_sha256 TEXT,rejected_seal_head TEXT,
                materializer_sha256 TEXT,next_ordinal INTEGER NOT NULL DEFAULT 0,
                total_inputs INTEGER,knowledge_sha256 TEXT,
                CHECK(batch_count BETWEEN 0 AND 2500),CHECK(next_ordinal>=0),
                CHECK((pending_operation_key IS NULL)=(pending_kind IS NULL)),
                CHECK(phase IN ('staging','sealed','materializing','ready_activation','finalized','failed')))''')
            db.execute('''CREATE UNIQUE INDEX IF NOT EXISTS rkb_native_one_revision2_owner
                ON rkb_native_source_revision2_state(person_id)
                WHERE phase NOT IN ('finalized','failed')''')

    def _state(self,row,*,create=False):
        with self.queue.connect(write=create) as db:
            if create:
                try:
                    db.execute('''INSERT OR IGNORE INTO rkb_native_source_revision2_state(
                        ingestion_id,source_id,person_id,original_ingestion_id,
                        predecessor_command_sha256,last_batch_sha256)
                        VALUES(?,?,?,?,?,?)''',(
                        row['ingestion_id'],row['source_id'],row['person_id'],
                        row['original_ingestion_id'],row['predecessor_command_sha256'],ZERO_HASH))
                except sqlite3.IntegrityError:
                    raise _failure('another_native_ingestion_active') from None
            value=db.execute('''SELECT * FROM rkb_native_source_revision2_state
                WHERE ingestion_id=? AND person_id=?''',
                (row['ingestion_id'],row['person_id'])).fetchone()
        if value is None:
            if create:raise _failure('another_native_ingestion_active')
            return None
        state=dict(value)
        if (state['source_id'],state['original_ingestion_id'],state['predecessor_command_sha256'])!=(
                row['source_id'],row['original_ingestion_id'],row['predecessor_command_sha256']):
            raise _failure('accepted_source_successor_changed')
        return state

    def _update(self,row,**changes):
        allowed={'batch_count','last_batch_sha256','last_request_sha256','pending_operation_key',
            'pending_kind','phase','seal_sha256','materializer_sha256','next_ordinal','total_inputs',
            'knowledge_sha256','rejected_seal_head'}
        if not changes or set(changes)-allowed:raise ValueError('exact compact revision2 progress only')
        with self.queue.connect() as db:
            result=db.execute('UPDATE rkb_native_source_revision2_state SET '
                +','.join(key+'=?' for key in changes)
                +' WHERE ingestion_id=? AND source_id=? AND person_id=?',
                (*changes.values(),row['ingestion_id'],row['source_id'],row['person_id']))
            if result.rowcount!=1:raise _failure('owned_successor_ingestion_required',status='denied')
        return self._state(row)

    @staticmethod
    def _operation(row,kind,ordinal=None,*,head=None):
        prefix='revision2:'+row['ingestion_id']
        if kind=='stage':return prefix+':stage:'+str(ordinal)
        if kind=='seal':
            if type(head) is not str or len(head)!=64 or set(head)-set('0123456789abcdef'):
                raise ValueError('exact successor seal head required')
            return prefix+':seal:'+head
        if kind=='materialize':return prefix+':materialize:'+str(ordinal)
        if kind=='activate':return 'revision2-activation:'+row['ingestion_id']
        raise ValueError('explicit successor operation kind required')

    async def _settle(self,access,row,state,entry):
        if entry.outcome_state=='DURABLE' and entry.apply_receipt is not None and entry.apply_receipt.status=='applied':
            result=self._result(entry)
            if result.get('source_revision',result.get('content_revision'))!=2:
                raise _failure('native_revision2_outcome_differs')
        return await super()._settle(access,row,state,entry)

    async def _recover_orphan(self,access,row,state,kind,key):
        from .source_stage import REVISION2_STAGE_ROUTE,REVISION2_SEAL_ROUTE
        from .source_activation import REVISION2_MATERIALIZE_ROUTE,REVISION2_ACTIVATE_ROUTE
        if state['pending_operation_key']:return await self._drive(access,row,state)
        entry=await self._entry(key)
        if entry is None:return state
        route=dict(stage=REVISION2_STAGE_ROUTE,seal=REVISION2_SEAL_ROUTE,
            materialize=REVISION2_MATERIALIZE_ROUTE,activate=REVISION2_ACTIVATE_ROUTE)[kind]
        if (entry.command.route!=route or entry.command.person_id!=row['person_id']
                or entry.command.ids!={'source_id':row['source_id']}
                or (entry.command.schema_revision,entry.command.function_revision,entry.command.executor_revision)
                  !=self.importer.import_revisions.wire()):
            raise _failure('revision2_accepted_command_identity_changed')
        state=await asyncio.to_thread(self._update,row,pending_operation_key=key,pending_kind=kind)
        return await self._drive(access,row,state)

    async def stage(self,access,row,payload,source_import):
        from .kaggle_import_contracts import SourceResearchInput
        from .contracts import StartMetadataInput
        from .source_stage import (REVISION2_STAGE_ROUTE,REVISION2_BATCH_CODEC,
            validate_revision2_stage_payload)
        from .kaggle_native_source_bindings import resolved_revision2_native_command
        await self._check(access,row)
        if type(payload) is not dict or type(payload.get('pages')) is not list or not 1<=len(payload['pages'])<=8:
            raise _failure('bounded_model_authored_selected_pages_required',status='invalid_query')
        state=await asyncio.to_thread(self._state,row,create=True)
        state=await self._recover_orphan(access,row,state,'stage',
            self._operation(row,'stage',state['batch_count']))
        if state['pending_operation_key'] or state['phase']=='failed':return self._output(row,state)
        if state['phase']!='staging':raise _failure('revision2_candidate_already_sealed',status='invalid_query')
        receipt=await self.importer._receipt(access,row)
        if receipt is None:raise _failure('verified_original_predecessor_required')
        if receipt.ingestion_id!=row['original_ingestion_id'] or receipt.source_id!=row['source_id']:
            raise _failure('original_same_source_rebind_changed')
        research=source_import.research if source_import is not None else None
        if research is None:
            if not row['research_json']:raise _failure('model_authored_research_required',status='invalid_query')
            research=SourceResearchInput.model_validate_json(row['research_json'])
        if research.state=='pending':
            continuation=research.continuation
            if (continuation.get('tool')!='book_pages' or type(continuation.get('arguments')) is not dict
                    or continuation['arguments'].get('ingestion_id')!=row['ingestion_id']):
                raise _failure('research_requires_reachable_owned_page_continuation',status='invalid_query')
        issued=await self.importer.originals.selected_image_receipts(access.context,
            row['original_ingestion_id'],tuple((p['physical_page_index'],p['source_image_receipt_sha256'])
                for p in payload['pages']))
        await self._check(access,row)
        metadata=StartMetadataInput.model_validate_json(row['metadata_json']).model_dump(mode='json')
        bibliography='; '.join(filter(None,[', '.join(metadata['authors']),metadata['title'],
            str(metadata['publication_year']) if metadata['publication_year'] else None]))
        doc=dict(codec=REVISION2_BATCH_CODEC,source_revision=2,prior_revision=1,
            ingestion_id=row['ingestion_id'],original_ingestion_id=row['original_ingestion_id'],
            predecessor_command_sha256=row['predecessor_command_sha256'],
            batch_index=state['batch_count'],previous_batch_sha256=state['last_batch_sha256'],
            source=dict(bibliography=bibliography,work_id=str(uuid5(UUID(row['source_id']),'work')),
                edition_id=str(uuid5(UUID(row['source_id']),'edition')),
                metadata=metadata,original_receipt_b64=base64.b64encode(canonical(receipt.wire())).decode(),
                public_rights=json.loads(row['rights_json'])),
            issued_images_b64=[encode_issued_image(value) for value in issued],
            model_payload=payload,
            research=dict(frontier_id=str(uuid5(UUID(row['ingestion_id']),'research')),
                **research.model_dump(mode='json')))
        validate_revision2_stage_payload(canonical(doc),source_id=row['source_id'])
        if self._request_hash(doc)==state['last_request_sha256']:return self._output(row,state)
        await self._stage_exact(access,row,kind='stage',
            key=self._operation(row,'stage',state['batch_count']),
            route=REVISION2_STAGE_ROUTE,payload=doc,
            builder=lambda **kw:resolved_revision2_native_command(route=REVISION2_STAGE_ROUTE,**kw))
        await asyncio.to_thread(self.importer._save,row,
            phase='staging',research_json=canonical(research.model_dump(mode='json')).decode())
        return self._output(row,await self._drive(access,row,await asyncio.to_thread(self._state,row)))

    async def validate(self,access,row):
        from .source_stage import REVISION2_SEAL_ROUTE,REVISION2_SEAL_CODEC
        from .kaggle_native_source_bindings import resolved_revision2_native_command
        await self._check(access,row)
        state=await asyncio.to_thread(self._state,row)
        if state is None:raise _failure('stage_before_validate',status='invalid_query')
        state=await self._recover_orphan(access,row,state,'stage',
            self._operation(row,'stage',state['batch_count']))
        if state['pending_operation_key'] or state['phase']=='failed':return self._output(row,state)
        if state['phase']!='staging':return self._output(row,state)
        if not state['batch_count']:raise _failure('stage_before_validate',status='invalid_query')
        if state['rejected_seal_head']==state['last_batch_sha256']:return self._output(row,state)
        doc=dict(codec=REVISION2_SEAL_CODEC,source_revision=2,prior_revision=1,
            ingestion_id=row['ingestion_id'],batch_count=state['batch_count'],
            last_batch_sha256=state['last_batch_sha256'],
            predecessor_command_sha256=row['predecessor_command_sha256'])
        await self._stage_exact(access,row,kind='seal',
            key=self._operation(row,'seal',head=state['last_batch_sha256']),
            route=REVISION2_SEAL_ROUTE,payload=doc,
            builder=lambda **kw:resolved_revision2_native_command(route=REVISION2_SEAL_ROUTE,**kw))
        return self._output(row,await self._drive(access,row,await asyncio.to_thread(self._state,row)))

    async def finalize(self,access,row):
        from .source_activation import (REVISION2_ACTIVATE_ROUTE,REVISION2_MATERIALIZE_ROUTE,
            REVISION2_ACTIVATION_CODEC,REVISION2_MATERIALIZE_CODEC)
        from .kaggle_native_source_bindings import resolved_revision2_native_command
        from .kaggle_source_materializer import materializer_fingerprint
        await self._check(access,row)
        state=await asyncio.to_thread(self._state,row)
        if state is None:raise _failure('validate_before_finalize',status='invalid_query')
        state=await self._drive(access,row,state)
        if state['pending_operation_key'] or state['phase'] in ('failed','finalized'):return self._output(row,state)
        if state['phase']=='staging':
            if state['rejected_seal_head']==state['last_batch_sha256']:return self._output(row,state)
            state=await self._recover_orphan(access,row,state,'seal',
                self._operation(row,'seal',head=state['last_batch_sha256']))
            if state['phase']=='staging':raise _failure('validate_before_finalize',status='invalid_query')
            if state['pending_operation_key']:return self._output(row,state)
        kind='activate' if state['phase']=='ready_activation' else 'materialize'
        state=await self._recover_orphan(access,row,state,kind,
            self._operation(row,kind,state['next_ordinal']))
        if state['pending_operation_key'] or state['phase'] in ('failed','finalized'):return self._output(row,state)
        fingerprint=materializer_fingerprint()
        if state['materializer_sha256'] is not None and state['materializer_sha256']!=fingerprint:
            raise _failure('accepted_native_materializer_changed')
        doc=dict(source_revision=2,prior_revision=1,ingestion_id=row['ingestion_id'],
            batch_count=state['batch_count'],last_batch_sha256=state['last_batch_sha256'],
            predecessor_command_sha256=row['predecessor_command_sha256'],
            materializer_sha256=fingerprint)
        if kind=='materialize':
            route=REVISION2_MATERIALIZE_ROUTE;doc.update(codec=REVISION2_MATERIALIZE_CODEC,
                ordinal=state['next_ordinal'])
        else:route=REVISION2_ACTIVATE_ROUTE;doc['codec']=REVISION2_ACTIVATION_CODEC
        await self._stage_exact(access,row,kind=kind,key=self._operation(row,kind,state['next_ordinal']),
            route=route,payload=doc,builder=lambda **kw:resolved_revision2_native_command(route=route,**kw))
        return self._output(row,await self._drive(access,row,await asyncio.to_thread(self._state,row)))
