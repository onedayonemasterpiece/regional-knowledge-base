"""Controller unit seams with real compact SQLite and synthetic spool receipts.

No native/provider/BGE effect. Full actual ControlSpool/worker/PG composition is
the parent's combined integration test; these tests target crash/UNKNOWN state.
"""
import asyncio
import base64
from contextlib import contextmanager
import json
from pathlib import Path
import sqlite3
import sys
from tempfile import TemporaryDirectory
from types import SimpleNamespace as NS
import unittest
from unittest.mock import patch

from regional_knowledge.source_stage import canonical,digest,STAGE_ROUTE,SEAL_ROUTE
from regional_knowledge.kaggle_native_source_intake import NativeSourceIntake
from test_source_stage import SOURCE,INGESTION,REQUEST,batch,original

ACTIVATE='source.activate.v1'; MATERIALIZE='source.materialize.v1'; VERSIONS=('synthetic','schema14','unit-seam')


class IntakeFailure(ValueError): pass
class SpoolError(RuntimeError): pass
class SpoolUnknown(SpoolError): pass
class JournalUnknown(RuntimeError): pass


class Research:
    def __init__(self,value): self.state=value['state']; self.continuation=value['continuation']
    @classmethod
    def model_validate_json(cls,value): return cls(json.loads(value))
    def model_dump(self,**kwargs): return dict(state=self.state,continuation=self.continuation)


def builder(route,**kwargs):
    return NS(route=route,payload=canonical(kwargs['payload']),ids={'source_id':kwargs['source_id']},
        person_id=kwargs['person_id'],operation_key=kwargs['operation_key'],sequence=kwargs['sequence'],
        logical_time=kwargs['logical_time'])


class Queue:
    def __init__(self,path): self.path=path
    @contextmanager
    def connect(self,write=True):
        db=sqlite3.connect(self.path); db.row_factory=sqlite3.Row
        try:
            if write: db.execute('BEGIN IMMEDIATE')
            yield db
            if write: db.commit()
        except BaseException:
            db.rollback(); raise
        finally: db.close()


class Spool:
    def __init__(self):
        self._lock=asyncio.Lock(); self.entries={}; self.seals=[]; self.sequence=0; self.accepted=0
        self.unknown_once=False; self.journal_unknown_once=False; self.external_pending=False; self.journal=NS(head=self.head)
    async def head(self): return NS(reference=NS(last_sequence=self.accepted)) if self.accepted else None
    async def get(self,key):
        if key not in self.entries: raise SpoolError('control operation not found')
        return self.entries[key]
    async def scope_state(self):
        return dict(pending=self.external_pending,unknown=False,sequence=self.sequence,accepted_sequence=self.accepted)
    def _validate(self,c,generation): return c,b'unit-exact-wire',None
    async def read_facts(self,scope):
        return NS(database_generation='db',scope_sequence=self.accepted,covered_sequence=self.accepted,pending=False,unknown=False)
    def facts_current(self,facts): return True
    async def _write(self,call,*args): return call(*args)
    def _stage(self,c,*args):
        self.sequence=c.sequence
        entry=NS(command=c,command_sha256=digest(c.payload),accepted=None,seal_state='NOT_SENT',outcome_state='NOT_SENT',apply_receipt=None)
        self.entries[c.operation_key]=entry; return entry
    async def seal(self,context,key,**kwargs):
        self.seals.append((key,kwargs)); entry=self.entries[key]
        if self.journal_unknown_once:
            self.journal_unknown_once=False; entry.seal_state='SENT_UNKNOWN'; self.accepted=entry.command.sequence
            raise JournalUnknown('synthetic lost journal acceptance ACK')
        if self.unknown_once:
            self.unknown_once=False; entry.seal_state='SENT_UNKNOWN'; self.accepted=entry.command.sequence
            raise SpoolUnknown('synthetic lost acceptance ACK')
        entry.accepted=object(); entry.seal_state='ACCEPTED'; self.accepted=entry.command.sequence


class NativeIntakeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp=TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.spool=Spool(); self.queue=Queue(str(Path(self.temp.name)/'control.sqlite'))
        self.events=[]; self.revoked=False; self.enqueued=[]; self.saved=[]
        source=batch(count=1); metadata=source['source']['metadata']
        self.row=dict(ingestion_id=INGESTION,source_id=SOURCE,person_id='owner',request_id=REQUEST,
            metadata_json=canonical(metadata).decode(),rights_json=canonical(source['source']['public_rights']).decode(),
            research_json=canonical({k:v for k,v in source['research'].items() if k!='frontier_id'}).decode(),phase='staging')
        self.payload=source['model_payload']
        self.issued=[]
        for value in source['issued_images_b64']:
            raw=base64.b64decode(value); self.issued.append({**json.loads(raw),'source_image_receipt_sha256':digest(raw)})
        self.access=NS(authority=NS(person_id='owner'),context=object(),binding=NS(database_generation='db'))
        async def check(*args):
            self.events.append('auth')
            if self.revoked: raise PermissionError('revoked synthetic lease')
        async def selected(*args): return self.issued
        async def receipt(*args): return original()
        async def enqueue(context,key,**kwargs): self.enqueued.append(key)
        def save(row,**changes):
            self.saved.append(changes); row.update(changes); return row
        def output(row,state,message,action,**kwargs): return NS(state=state,message=message,next_action=action,**kwargs)
        startup=NS(config=NS(writer_generation='writer'),source_dispatch=NS(enqueue=enqueue),reads_current=lambda:True)
        self.importer=NS(import_revisions=VERSIONS,queue=self.queue,spool=self.spool,startup=startup,
            _check=check,_receipt=receipt,originals=NS(selected_image_receipts=selected),_save=save,_output=output,
            _revisions=lambda binding:VERSIONS,_intake_coverage_current=lambda binding:True)
        # Stub only external contracts at this controller unit boundary. Native
        # payload validation and SQLite transitions below are actual code.
        modules={
            'regional_knowledge.source_activation':NS(ACTIVATION_REVISIONS=VERSIONS,ACTIVATE_ROUTE=ACTIVATE,
                ACTIVATION_CODEC='rkb.source-activate.v1',MATERIALIZE_ROUTE=MATERIALIZE,
                MATERIALIZE_CODEC='rkb.source-materialize.v1',resolved_activation_command=lambda **kw:builder(ACTIVATE,**kw),
                resolved_materialize_command=lambda **kw:builder(MATERIALIZE,**kw)),
            'regional_knowledge.kaggle_native_source_bindings':NS(resolved_native_stage_command=lambda route,**kw:builder(route,**kw)),
            'regional_knowledge.kaggle_source_materializer':NS(materializer_fingerprint=lambda:'f'*64),
            'regional_knowledge.source_batch':NS(ROUTE='source.batch.v1',FINISH_ROUTE='source.finish.v1',CODEC='rkb.source-batch.v1',
                resolved_command=lambda **kw:builder('source.finish.v1' if kw['payload']['codec']=='rkb.source-finish.v1' else 'source.batch.v1',**kw)),
            'regional_knowledge.kaggle_import_contracts':NS(SourceResearchInput=Research),
            'regional_knowledge.kaggle_importer':NS(failure=lambda reason,**kw:IntakeFailure(reason)),
            'regional_knowledge.control_spool':NS(SpoolError=SpoolError,SpoolUnknown=SpoolUnknown),
            'regional_knowledge.recovery_journal':NS(JournalUnknown=JournalUnknown),
        }
        self.mock=patch.dict(sys.modules,modules); self.mock.start(); self.addCleanup(self.mock.stop)
        self.helper=NativeSourceIntake(self.importer)
        # Existing fixtures exercise historical, already-created seal intents.
        # New intake tests below explicitly select the new packet behavior.
        self.helper._state(self.row,create=True)
        with self.queue.connect() as db:
            db.execute('UPDATE rkb_native_source_intake_state SET packet_mode=0')

    def state(self): return self.helper._state(self.row)
    def pending(self): return self.spool.entries[self.state()['pending_operation_key']]
    def finish(self,entry=None,*,status='applied',total=2,knowledge='e'*64):
        entry=entry or self.pending(); doc=json.loads(entry.command.payload)
        value=dict(source_id=SOURCE,ingestion_id=INGESTION)
        if entry.command.route==STAGE_ROUTE:
            value.update(batch_index=doc['batch_index'],batch_sha256=digest(entry.command.payload),candidate_state='staged_inactive')
        elif entry.command.route==SEAL_ROUTE:
            value.update(batch_count=doc['batch_count'],last_batch_sha256=doc['last_batch_sha256'],
                seal_sha256=digest(entry.command.payload),candidate_state='sealed_inactive')
        elif entry.command.route==MATERIALIZE:
            value=dict(source_id=SOURCE,ordinal=doc['ordinal'],total_inputs=total,knowledge_sha256=knowledge,
                input_sha256='a'*64,input_id='synthetic-input')
        elif entry.command.route=='source.batch.v1':
            value.update(batch_index=doc['batch_index'],component=doc['component'],candidate_state='published',accepted=True,knowledge_sha256=knowledge)
        elif entry.command.route=='source.finish.v1':
            value.update(read_complete=True)
        else: value=dict(source_id=SOURCE,accepted=True,knowledge_sha256=knowledge)
        raw=canonical(value); entry.apply_receipt=NS(status=status,outcome=canonical(dict(
            result_b64=base64.b64encode(raw).decode(),result_sha256=digest(raw))))
        entry.outcome_state='DURABLE'

    async def stage(self,payload=None): return await self.helper.stage(self.access,self.row,payload or self.payload,None)
    async def stage_and_seal(self):
        await self.stage(); self.finish(); await self.helper.validate(self.access,self.row)
        self.finish(); return await self.helper.validate(self.access,self.row)

    def packet_mode(self):
        with self.queue.connect() as db:db.execute('UPDATE rkb_native_source_intake_state SET packet_mode=1')

    async def test_packet_status_publishes_without_whole_book_validate(self):
        self.packet_mode();await self.stage();self.finish()
        await self.helper.status(self.access,self.row)
        self.assertEqual(self.pending().command.route,'source.batch.v1')
        self.finish();self.pending().outcome_state='NOT_SENT';self.pending().apply_state='APPLIED'
        result=await self.helper.status(self.access,self.row)
        self.assertEqual(result.next_action,'wait')
        self.assertEqual(self.state()['published_batch_count'],1)
        self.assertEqual(json.loads(self.pending().command.payload)['component'],'semantic')
        self.finish();result=await self.helper.status(self.access,self.row)
        self.assertEqual(result.next_action,'continue_pages')
        self.assertNotIn(SEAL_ROUTE,[e.command.route for e in self.spool.entries.values()])

    async def test_bad_packet_does_not_close_import_or_revoke_prior_packets(self):
        self.packet_mode();await self.stage();self.finish();await self.helper.status(self.access,self.row)
        self.finish(status='rejected_precondition');result=await self.helper.status(self.access,self.row)
        self.assertEqual(result.warnings,['native_source_packet_requires_review'])
        self.assertEqual(self.state()['phase'],'staging')
        changed=json.loads(canonical(self.payload));changed['pages'][0]['source_review_note']='Explicit model correction.'
        await self.stage(changed)
        self.assertEqual(json.loads(self.pending().command.payload)['batch_index'],1)

    async def test_packet_finalize_only_marks_reading_complete(self):
        self.packet_mode();await self.stage();self.finish();await self.helper.status(self.access,self.row)
        self.finish();await self.helper.status(self.access,self.row)
        self.finish();await self.helper.status(self.access,self.row)
        await self.helper.finalize(self.access,self.row)
        self.assertEqual(self.pending().command.route,'source.finish.v1')
        self.finish();result=await self.helper.status(self.access,self.row)
        self.assertEqual(result.state,'finalized')
        self.assertEqual({e.command.route for e in self.spool.entries.values()},
            {STAGE_ROUTE,'source.batch.v1','source.finish.v1'})

    async def test_stage_stores_only_compact_local_progress(self):
        result=await self.stage()
        self.assertEqual(result.state,'processing'); self.assertEqual(len(self.spool.entries),1)
        self.assertEqual(self.state()['batch_count'],0)
        with self.queue.connect(write=False) as db:
            columns={r[1] for r in db.execute('PRAGMA table_info(rkb_native_source_intake_state)')}
        self.assertFalse(columns & {'stage_json','model_payload','body','embedding','semantic_json'})
        self.assertTrue(all('stage_json' not in v and 'semantic_json' not in v for v in self.saved))

    async def test_exact_pending_retry_never_resequences(self):
        await self.stage(); key=self.state()['pending_operation_key']; await self.stage()
        self.assertEqual(self.state()['pending_operation_key'],key); self.assertEqual(len(self.spool.entries),1)

    async def test_pending_changed_text_is_rejected_not_silently_queued(self):
        await self.stage(); changed=json.loads(canonical(self.payload)); changed['pages'][0]['regions'][0]['source_text']='changed'
        with self.assertRaisesRegex(IntakeFailure,'pending_payload_changed'): await self.stage(changed)
        self.assertEqual(len(self.spool.entries),1)

    async def test_durable_exact_retry_is_idempotent_after_progress_advance(self):
        await self.stage(); self.finish(); result=await self.stage()
        self.assertEqual(result.state,'staged'); self.assertEqual(self.state()['batch_count'],1)
        self.assertEqual(len(self.spool.entries),1)

    async def test_explicit_later_page_correction_is_new_append_only_batch(self):
        await self.stage(); self.finish(); await self.stage()
        first=list(self.spool.entries.values())[0]; original_bytes=first.command.payload
        changed=json.loads(canonical(self.payload)); changed['pages'][0]['regions'][0]['source_text']='Explicit later correction'
        await self.stage(changed)
        self.assertEqual(len(self.spool.entries),2); self.assertEqual(first.command.payload,original_bytes)
        self.assertEqual(json.loads(self.pending().command.payload)['batch_index'],1)
        self.assertEqual(json.loads(self.pending().command.payload)['previous_batch_sha256'],digest(original_bytes))

    async def test_crash_gap_after_spool_commit_recovers_same_stable_operation(self):
        await self.stage(); key=self.state()['pending_operation_key']; self.finish()
        self.helper._update(self.row,pending_operation_key=None,pending_kind=None)
        result=await self.stage()
        self.assertEqual(result.state,'staged'); self.assertEqual(self.state()['batch_count'],1)
        self.assertEqual(list(self.spool.entries),[key])

    async def test_unknown_acceptance_reconciles_original_predecessor(self):
        self.spool.unknown_once=True
        self.assertEqual((await self.stage()).state,'processing')
        key=self.state()['pending_operation_key']; self.assertEqual(self.pending().seal_state,'SENT_UNKNOWN')
        await self.stage()
        self.assertEqual(self.spool.seals[-1][0],key)
        self.assertNotIn('predecessor',self.spool.seals[-1][1]); self.assertEqual(len(self.spool.entries),1)

    async def test_unknown_blocks_new_candidate_sequence(self):
        self.spool.unknown_once=True; await self.stage()
        await self.helper.validate(self.access,self.row)
        self.assertEqual(len(self.spool.entries),1); self.assertEqual(self.state()['batch_count'],0)

    async def test_journal_unknown_keeps_same_pending_key_and_reconciles(self):
        self.spool.journal_unknown_once=True
        self.assertEqual((await self.stage()).state,'processing')
        key=self.state()['pending_operation_key']; await self.helper.status(self.access,self.row)
        self.assertEqual(self.state()['pending_operation_key'],key); self.assertEqual(len(self.spool.entries),1)
        self.assertNotIn('predecessor',self.spool.seals[-1][1])

    async def test_materialize_count_matches_exact_280000_contract_boundary(self):
        await self.stage_and_seal(); await self.helper.finalize(self.access,self.row); self.finish(total=280000)
        await self.helper.finalize(self.access,self.row)
        self.assertEqual(self.state()['total_inputs'],280000); self.assertEqual(self.state()['next_ordinal'],1)

    async def test_one_active_ingestion_per_owner(self):
        await self.stage(); other={**self.row,'ingestion_id':REQUEST,'source_id':'00000000-0000-0000-0000-000000000008'}
        with self.assertRaisesRegex(IntakeFailure,'another_native_ingestion_active'): self.helper._state(other,create=True)

    async def test_foreign_owner_row_is_denied(self):
        with self.assertRaisesRegex(IntakeFailure,'owned_ingestion_required'):
            await self.helper.stage(self.access,{**self.row,'person_id':'other'},self.payload,None)
        self.assertFalse(self.spool.entries)

    async def test_revocation_during_selected_image_read_prevents_command(self):
        async def revoke(*args): self.revoked=True; return self.issued
        self.importer.originals.selected_image_receipts=revoke
        with self.assertRaises(PermissionError): await self.stage()
        self.assertFalse(self.spool.entries)

    async def test_global_unresolved_operation_blocks_allocation(self):
        self.spool.external_pending=True
        with self.assertRaisesRegex(IntakeFailure,'prior_write_pending'): await self.stage()
        self.assertFalse(self.spool.entries)

    async def test_validate_is_inactive_seal_not_finalized(self):
        result=await self.stage_and_seal()
        self.assertEqual(result.state,'ready'); self.assertEqual(result.next_action,'finalize')
        self.assertEqual(self.state()['phase'],'sealed')
        self.assertEqual([e.command.route for e in self.spool.entries.values()],[STAGE_ROUTE,SEAL_ROUTE])

    async def test_bounded_materialize_per_poll_and_finalized_only_after_durable_activation(self):
        await self.stage_and_seal()
        result=await self.helper.finalize(self.access,self.row)
        self.assertEqual(result.state,'processing'); self.assertEqual(self.pending().command.route,MATERIALIZE)
        self.assertEqual(json.loads(self.pending().command.payload)['ordinal'],0)
        self.finish(); await self.helper.finalize(self.access,self.row)
        self.assertEqual(json.loads(self.pending().command.payload)['ordinal'],1)
        self.finish(); result=await self.helper.finalize(self.access,self.row)
        self.assertEqual(self.pending().command.route,ACTIVATE); self.assertNotEqual(result.state,'finalized')
        self.finish(); result=await self.helper.finalize(self.access,self.row)
        self.assertEqual(result.state,'finalized'); self.assertEqual(self.state()['next_ordinal'],2)
        self.assertEqual(len(self.spool.entries),5)

    async def test_invalid_materialize_count_cannot_skip_to_activation(self):
        await self.stage_and_seal(); await self.helper.finalize(self.access,self.row); self.finish(total=0)
        with self.assertRaisesRegex(IntakeFailure,'materialization_outcome_differs'):
            await self.helper.finalize(self.access,self.row)
        self.assertEqual(self.state()['next_ordinal'],0)
        self.assertFalse(any(e.command.route==ACTIVATE for e in self.spool.entries.values()))

    async def test_materialize_crash_gap_recovers_stored_hash_before_new_code_fingerprint(self):
        await self.stage_and_seal(); await self.helper.finalize(self.access,self.row)
        entry=self.pending(); self.helper._update(self.row,pending_operation_key=None,pending_kind=None)
        sys.modules['regional_knowledge.kaggle_source_materializer'].materializer_fingerprint=lambda:'b'*64
        result=await self.helper.finalize(self.access,self.row)
        self.assertEqual(result.state,'processing'); self.assertEqual(self.pending(),entry)
        self.assertEqual(json.loads(entry.command.payload)['materializer_sha256'],'f'*64)
        self.assertEqual(len(self.spool.entries),3)

    async def test_known_rejection_does_not_advance_batch_count(self):
        await self.stage(); self.finish(status='rejected_precondition'); result=await self.stage()
        self.assertEqual(result.state,'failed'); self.assertEqual(self.state()['batch_count'],0)

    async def test_rejected_seal_keeps_staging_open_and_never_resends_unchanged_head(self):
        await self.stage(); self.finish(); await self.helper.validate(self.access,self.row)
        failed_key=self.state()['pending_operation_key']; head=self.state()['last_batch_sha256']
        self.finish(status='rejected_precondition'); result=await self.helper.validate(self.access,self.row)
        self.assertEqual(result.state,'staged'); self.assertEqual(result.next_action,'blocker')
        self.assertEqual(self.state()['rejected_seal_head'],head); self.assertEqual(self.state()['phase'],'staging')
        calls=len(self.spool.seals); size=len(self.spool.entries)
        await self.helper.validate(self.access,self.row); await self.helper.finalize(self.access,self.row)
        self.assertEqual(len(self.spool.seals),calls); self.assertEqual(len(self.spool.entries),size)
        self.assertEqual(self.spool.entries[failed_key].apply_receipt.status,'rejected_precondition')

    async def test_corrected_batch_gets_new_seal_key_preserving_old_rejection(self):
        await self.stage(); self.finish(); await self.helper.validate(self.access,self.row)
        old_key=self.state()['pending_operation_key']; self.finish(status='rejected_precondition')
        await self.helper.validate(self.access,self.row)
        changed=json.loads(canonical(self.payload)); changed['pages'][0]['regions'][0]['source_text']='Corrected before seal'
        await self.stage(changed); self.finish(); await self.helper.validate(self.access,self.row)
        self.assertEqual(self.state()['batch_count'],2); self.assertIsNone(self.state()['rejected_seal_head'])
        self.assertNotEqual(self.state()['pending_operation_key'],old_key)
        self.assertTrue(self.state()['pending_operation_key'].endswith(self.state()['last_batch_sha256']))
        self.assertEqual(self.spool.entries[old_key].apply_receipt.status,'rejected_precondition')

    async def test_unknown_seal_outcome_is_not_mislabeled_review_rejection(self):
        await self.stage(); self.finish(); await self.helper.validate(self.access,self.row)
        key=self.state()['pending_operation_key']; self.finish(status='unknown')
        with self.assertRaisesRegex(IntakeFailure,'outcome_status_unknown'):
            await self.helper.validate(self.access,self.row)
        self.assertEqual(self.state()['pending_operation_key'],key)
        self.assertIsNone(self.state()['rejected_seal_head'])

    async def test_status_settles_stage_without_inventing_validate_or_finalize(self):
        await self.stage(); self.finish(); result=await self.helper.status(self.access,self.row)
        self.assertEqual(result.state,'staged'); self.assertEqual(self.state()['batch_count'],1)
        self.assertEqual([e.command.route for e in self.spool.entries.values()],[STAGE_ROUTE])

    async def test_rejected_seal_waits_for_dataset_without_new_compute(self):
        await self.stage();self.finish();await self.helper.validate(self.access,self.row)
        entry=self.pending();self.finish(status='rejected_precondition')
        entry.outcome_state='NOT_SENT';entry.apply_state='REJECTED_PRECONDITION'
        enqueued=len(self.enqueued)
        result=await self.helper.status(self.access,self.row)
        self.assertEqual(result.state,'processing')
        self.assertEqual(len(self.enqueued),enqueued)
        self.assertEqual(self.state()['pending_operation_key'],entry.command.operation_key)
        self.assertEqual(entry.outcome_state,'NOT_SENT')

    async def test_status_of_sealed_candidate_does_not_begin_materialization(self):
        await self.stage_and_seal(); result=await self.helper.status(self.access,self.row)
        self.assertEqual(result.state,'ready'); self.assertEqual(result.next_action,'finalize')
        self.assertEqual(len(self.spool.entries),2)

    async def test_status_recovers_existing_finalize_intent_after_crash_gap(self):
        await self.stage_and_seal(); await self.helper.finalize(self.access,self.row)
        entry=self.pending(); self.helper._update(self.row,pending_operation_key=None,pending_kind=None)
        result=await self.helper.status(self.access,self.row)
        self.assertEqual(result.state,'processing'); self.assertEqual(self.pending(),entry)
        self.assertEqual(len(self.spool.entries),3)


    async def test_applied_inactive_stage_advances_without_claiming_remote_durability(self):
        await self.stage();entry=self.pending();self.finish(entry)
        entry.outcome_state='NOT_SENT';entry.apply_state='APPLIED'
        result=await self.helper.status(self.access,self.row)
        self.assertEqual(self.state()['batch_count'],1)
        self.assertIsNone(self.state()['pending_operation_key'])
        self.assertEqual(entry.outcome_state,'NOT_SENT')
        self.assertNotEqual(result.state,'finalized')

    async def test_applied_materialization_advances_ordinal_without_a_checkpoint_per_vector(self):
        await self.stage_and_seal();await self.helper.finalize(self.access,self.row)
        entry=self.pending();self.finish(entry,total=20)
        entry.outcome_state='NOT_SENT';entry.apply_state='APPLIED'
        state=await self.helper._drive(self.access,self.row,self.state())
        self.assertEqual(state['next_ordinal'],1)
        self.assertIsNone(state['pending_operation_key'])
        self.assertEqual(entry.outcome_state,'NOT_SENT')

    async def test_applied_activation_and_unknown_inactive_receipts_still_wait(self):
        await self.stage_and_seal();await self.helper.finalize(self.access,self.row)
        for ordinal in (0,1):
            self.finish();await self.helper.finalize(self.access,self.row)
        entry=self.pending();self.assertEqual(entry.command.route,ACTIVATE)
        self.finish(entry);entry.outcome_state='NOT_SENT';entry.apply_state='APPLIED'
        result=await self.helper.status(self.access,self.row)
        self.assertNotEqual(result.state,'finalized')
        self.assertIsNotNone(self.state()['pending_operation_key'])
        self.assertEqual(entry.outcome_state,'NOT_SENT')


if __name__=='__main__': unittest.main()
