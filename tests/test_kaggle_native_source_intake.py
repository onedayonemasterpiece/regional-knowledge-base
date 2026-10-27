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
        self.unknown_once=False; self.journal_unknown_once=False; self.external_pending=False; self.external_unknown=False
        self.own_unknown=False; self.own_pending=False; self.journal=NS(head=self.head)
    async def head(self): return NS(reference=NS(last_sequence=self.accepted)) if self.accepted else None
    async def get(self,key):
        if key not in self.entries: raise SpoolError('control operation not found')
        return self.entries[key]
    async def scope_state(self,source_ids=None):
        return dict(pending=self.external_pending if source_ids is None else self.own_pending,
            unknown=self.external_unknown if source_ids is None else self.own_unknown,
            sequence=self.sequence,accepted_sequence=self.accepted)
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
        # Original is already verified; pending/rejected cases concern stage receipts.
        async def original_status(*args): return NS(verified=True)
        async def enqueue(context,key,**kwargs): self.enqueued.append(key)
        def save(row,**changes):
            self.saved.append(changes); row.update(changes); return row
        def output(row,state,message,action,**kwargs): return NS(state=state,message=message,next_action=action,**kwargs)
        self.tails=[];self.tail_query_failed=False
        async def tails(*args):
            if self.tail_query_failed:raise TimeoutError('synthetic native query timeout')
            return dict(tail_refs=self.tails,has_more=False)
        startup=NS(config=NS(writer_generation='writer'),source_dispatch=NS(enqueue=enqueue),reads_current=lambda:True,
            history=NS(source_tail_refs=tails))
        self.importer=NS(import_revisions=VERSIONS,queue=self.queue,spool=self.spool,startup=startup,
            _check=check,_receipt=receipt,originals=NS(selected_image_receipts=selected,status=original_status),_save=save,_output=output,
            _revisions=lambda binding:VERSIONS,_intake_coverage_current=lambda binding:True)
        # Stub only external contracts at this controller unit boundary. Native
        # payload validation and SQLite transitions below are actual code.
        modules={
            'regional_knowledge.source_activation':NS(ACTIVATION_REVISIONS=VERSIONS,ACTIVATE_ROUTE=ACTIVATE,
                ACTIVATION_CODEC='rkb.source-activate.v1',MATERIALIZE_ROUTE=MATERIALIZE,
                MATERIALIZE_CODEC='rkb.source-materialize.v1',resolved_activation_command=lambda **kw:builder(ACTIVATE,**kw),
                resolved_materialize_command=lambda **kw:builder(MATERIALIZE,**kw)),
            'regional_knowledge.kaggle_native_source_bindings':NS(resolved_native_stage_command=lambda route,**kw:builder(route,**kw)),
            'regional_knowledge.source_catalog_command':NS(ROUTE='source.catalog.v1',CODEC='rkb.source-catalog.v1',
                resolved_catalog_command=lambda **kw:builder('source.catalog.v1',**kw),
                validate_catalog_payload=lambda raw:json.loads(raw)),
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
        elif entry.command.route=='source.catalog.v1':
            value.update(catalog_revision=1,accepted=True,payload_sha256=digest(entry.command.payload))
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


    async def test_packet_status_exposes_computed_tail_and_finalize_does_not_drop_it(self):
        from uuid import UUID,uuid5
        self.packet_mode();await self.stage();self.finish()
        await self.helper.status(self.access,self.row);self.finish()
        await self.helper.status(self.access,self.row);self.finish()
        self.tails=[dict(page_id=self.payload['pages'][0]['page_id'],physical_page_index=0,
            region_id=str(uuid5(UUID(self.payload['pages'][0]['page_id']),'region:tail')),region_key='tail',source_text_sha256='a'*64,start=0,end=10)]
        result=await self.helper.status(self.access,self.row)
        self.assertEqual(result.next_action,'continue_tail');self.assertEqual(result.next_cursor,'1')
        self.assertEqual(result.tail_refs[0].region_key,'tail')
        before=len(self.spool.entries)
        result=await self.helper.finalize(self.access,self.row)
        self.assertEqual(result.next_action,'continue_tail')
        self.assertEqual(len(self.spool.entries),before)
        self.assertIsNone(self.state()['pending_operation_key'])
        self.tails=[]
        result=await self.helper.status(self.access,self.row)
        self.assertEqual(self.pending().command.route,'source.finish.v1')

    async def test_status_waits_for_restore_owner_before_querying_native_tails(self):
        self.packet_mode();await self.stage();self.finish()
        await self.helper.status(self.access,self.row);self.finish()
        await self.helper.status(self.access,self.row);self.finish()
        self.importer.startup.restorer=NS(complete=False)
        calls=[]
        async def tails(*args):
            calls.append(args)
            return dict(tail_refs=[],has_more=False)
        self.importer.startup.history.source_tail_refs=tails
        result=await self.helper.status(self.access,self.row)
        self.assertEqual(result.next_action,'wait')
        self.assertEqual(result.warnings,['source_maintenance_starting'])
        self.assertEqual(calls,[])
        self.importer.startup.restorer.complete=True
        result=await self.helper.status(self.access,self.row)
        self.assertEqual(result.next_action,'continue_pages')
        self.assertEqual(len(calls),1)

    async def test_unknown_tail_query_never_claims_complete_coverage(self):
        self.packet_mode();await self.stage();self.finish()
        await self.helper.status(self.access,self.row);self.finish()
        await self.helper.status(self.access,self.row);self.finish()
        self.tail_query_failed=True
        result=await self.helper.status(self.access,self.row)
        self.assertEqual(result.next_action,'wait')
        self.assertEqual(result.warnings,['source_tail_coverage_unavailable'])

    async def test_tail_query_failure_still_rechecks_revoked_actor(self):
        self.packet_mode();await self.stage();self.finish()
        await self.helper.status(self.access,self.row);self.finish()
        await self.helper.status(self.access,self.row);self.finish()
        async def revoked(*args):
            self.revoked=True
            raise TimeoutError('synthetic native query timeout')
        self.importer.startup.history.source_tail_refs=revoked
        with self.assertRaises(PermissionError):await self.helper.status(self.access,self.row)

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

    async def test_rejected_packet_stage_keeps_same_import_and_next_explicit_stage_can_retry(self):
        self.packet_mode();await self.stage();old_key=self.state()['pending_operation_key']
        self.finish(status='rejected_precondition')
        entry=self.spool.entries[old_key];raw=canonical({'reason':'native_source_sql_rolled_back'})
        entry.apply_receipt.outcome=canonical(dict(result_b64=base64.b64encode(raw).decode(),result_sha256=digest(raw)))
        result=await self.helper.status(self.access,self.row)
        self.assertEqual(result.warnings,['native_source_sql_rolled_back'])
        self.assertEqual(self.state()['phase'],'staging');self.assertEqual(self.state()['batch_count'],0)
        self.assertIsNone(self.state()['pending_operation_key'])
        # No automatic resend on status, but an explicit normal stage is a
        # new intent after a known no-effect outcome, on the SAME import.
        count=len(self.spool.entries);await self.helper.status(self.access,self.row)
        self.assertEqual(len(self.spool.entries),count)
        await self.stage()
        self.assertNotEqual(self.state()['pending_operation_key'],old_key)
        self.assertEqual(json.loads(self.pending().command.payload)['batch_index'],0)
        self.assertEqual(entry.apply_receipt.status,'rejected_precondition')

    async def test_finish_refusal_does_not_auto_resend_or_close_the_import(self):
        self.packet_mode();await self.stage();self.finish();await self.helper.status(self.access,self.row)
        self.finish();await self.helper.status(self.access,self.row)
        self.finish();await self.helper.status(self.access,self.row)
        await self.helper.finalize(self.access,self.row)
        old_key=self.state()['pending_operation_key'];self.finish(status='rejected_precondition')
        count=len(self.spool.entries);result=await self.helper.status(self.access,self.row)
        self.assertEqual(result.state,'staged');self.assertEqual(len(self.spool.entries),count)
        self.assertEqual(self.state()['read_completion_requested'],0)
        await self.helper.finalize(self.access,self.row)
        self.assertNotEqual(self.state()['pending_operation_key'],old_key)


    async def test_independent_sources_keep_independent_owner_progress(self):
        await self.stage()
        before=self.state()
        other={**self.row,'ingestion_id':REQUEST,'source_id':'00000000-0000-0000-0000-000000000008'}
        created=self.helper._state(other,create=True)
        self.assertEqual(created['ingestion_id'],REQUEST)
        self.assertEqual(self.state(),before)

    async def test_foreign_owner_row_is_denied(self):
        with self.assertRaisesRegex(IntakeFailure,'owned_ingestion_required'):
            await self.helper.stage(self.access,{**self.row,'person_id':'other'},self.payload,None)
        self.assertFalse(self.spool.entries)


if __name__=='__main__': unittest.main()
