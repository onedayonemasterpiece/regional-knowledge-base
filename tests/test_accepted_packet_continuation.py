"""Accepted derivatives use real spool/journal/progress, never an HTTP lease."""
import asyncio
import base64
from dataclasses import replace
import json
from types import SimpleNamespace as NS

import pytest
import pytest_asyncio

from regional_knowledge.bge_queue import BgeQueue
from regional_knowledge.control_spool import ControlSpool
from regional_knowledge.kaggle_current_registry import current_registry
from regional_knowledge.kaggle_native_source_bindings import resolved_native_stage_command
from regional_knowledge.kaggle_native_source_intake import NativeSourceIntake
from regional_knowledge.kaggle_importer import KaggleSourceImporter
from regional_knowledge.kaggle_source_commands import AcceptedSourceDispatch
from regional_knowledge.recovery_journal import RecoveryJournal
from regional_knowledge.source_activation import ACTIVATION_REVISIONS
from regional_knowledge.source_batch import resolved_command
from regional_knowledge.source_stage import STAGE_ROUTE,canonical,digest
from test_control_spool import receipt
from test_participants import people
from test_recovery_journal import ConditionalMemoryStore
from test_source_history_wss import pair
from test_source_stage import SOURCE,INGESTION,REQUEST,batch


@pytest_asyncio.fixture
async def packet(people,tmp_path):
    q=BgeQueue(tmp_path/'queue.sqlite');registry=current_registry()
    journal=RecoveryJournal(ConditionalMemoryStore(),stream='automatic-packet',max_outcome_bytes=8192)
    spool=ControlSpool(q,people.registry,journal,registry)
    native=pair(NS(q=q,spool=spool,registry=registry,journal=journal),extra_routes=('source.import',))
    row=dict(ingestion_id=INGESTION,source_id=SOURCE,person_id=people.owner.subject,request_id=REQUEST)
    producer_tasks=[];produced=[];settled=[];allow_native=asyncio.Event();queued=asyncio.Event()
    startup=NS(config=NS(writer_generation='writer-1'),binding=native.b,coverage=None)
    async def no_http(*args):raise AssertionError('expired/released HTTP access reused')
    importer=NS(queue=q,spool=spool,startup=startup,import_revisions=ACTIVATION_REVISIONS,
        _lock=asyncio.Lock(),_check=no_http,_revisions=lambda b:ACTIVATION_REVISIONS,
        _save=lambda row,**changes:row.update(changes) or row,
        _binding_current=lambda b:b is native.b,_intake_coverage_current=lambda b:native.v._coverage.current(),
        _row=lambda actor,ingestion:row if (actor,ingestion)==(row['person_id'],INGESTION) else None)
    bridge=object.__new__(AcceptedSourceDispatch);bridge.packet_owner=None;bridge.spool=spool
    q._source_commands=bridge
    startup.source_dispatch=bridge
    helper=NativeSourceIntake(importer);helper._state(row,create=True)

    async def apply(command):
        doc=json.loads(command.payload);kind='stage' if command.route==STAGE_ROUTE else doc['component']
        value=dict(source_id=SOURCE,ingestion_id=INGESTION,batch_index=0)
        if kind=='stage':value.update(batch_sha256=digest(command.payload),candidate_state='staged_inactive')
        else:value.update(component=kind,candidate_state='published',accepted=True)
        raw=canonical(value)
        outcome=receipt(command,outcome=canonical(dict(result_b64=base64.b64encode(raw).decode(),result_sha256=digest(raw))))
        await spool.record_apply(command.operation_key,outcome)
        entry=await spool.get(command.operation_key);handler=registry.resolve(command)
        native.pg.applied_seq=command.sequence
        native.pg.receipts.append((command.sequence,command.operation_key,entry.command_sha256,handler.fingerprint,
            entry.accepted.manifest.sha256,outcome.status,outcome.outcome_sha256,digest(outcome.outcome),len(outcome.outcome),
            'db-source',entry.command_sha256))
        startup.coverage=await native.v.extend(native.b)
        settled.append(kind)
        return entry

    async def enqueue(context,key,**kwargs):
        entry=await spool.get(key)
        await spool.authorize_command(context,entry.command)
        if key not in produced:
            produced.append(key)
            queued.set()
            async def complete():
                await allow_native.wait()
                entry=await apply((await spool.get(key)).command)
                bridge._packet_settled(entry)
            producer_tasks.append(asyncio.create_task(complete()))
    bridge.enqueue=enqueue
    await native.v.establish_genesis(native.b)
    command=resolved_native_stage_command(route=STAGE_ROUTE,sequence=1,
        operation_key=f'source-stage:{INGESTION}:0:0',request_id=REQUEST,person_id=row['person_id'],
        logical_time='2026-10-27T14:00:00Z',revisions=ACTIVATION_REVISIONS,source_id=SOURCE,payload=batch(count=1))
    frozen,data,handler=spool._validate(command,'db-source')
    await spool._write(spool._stage,frozen,data,handler,'db-source')
    await spool.seal(people.admin,command.operation_key,writer_generation='writer-1',predecessor=None)
    helper._update(row,pending_operation_key=command.operation_key,pending_kind='stage')
    parent=await apply(command)
    try:
        yield NS(helper=helper,importer=importer,spool=spool,q=q,native=native,row=row,parent=parent,
            startup=startup,bridge=bridge,produced=produced,settled=settled,apply=apply,people=people,allow_native=allow_native,queued=queued)
    finally:
        await startup.source_dispatch.packet_owner.close_packets()
        for task in producer_tasks:
            if not task.done():task.cancel()
        await asyncio.gather(*producer_tasks,return_exceptions=True)
        await native.n.aclose();spool.close()


@pytest.mark.asyncio
async def test_accepted_packet_finishes_without_http_and_keeps_authority_scoped(packet):
    from regional_knowledge.read_authority import ReadAuthority,TrustedReadDestination,ReadAuthorityError
    from regional_knowledge.bge_admission import SharedReadAdmission
    from regional_knowledge.kaggle_source_materializer import materializer_fingerprint
    e=packet;c=e.parent.command
    proof=await e.helper._packet_proof(INGESTION)
    job=dict(actor=e.row['person_id'],identity=json.dumps(dict(operation_key=c.operation_key,
        command_sha256=e.parent.command_sha256,database_generation='db-source',sequence=c.sequence)))
    assert await e.bridge.authorize_job(None,job)==e.row['person_id']
    for field,value in [('parent_sha256','f'*64),('source_id','00000000-0000-0000-0000-000000000099'),
            ('person_id','00000000-0000-0000-0000-000000000099'),('batch_index',1),
            ('ingestion_id','00000000-0000-0000-0000-000000000099'),('binding',None)]:
        with pytest.raises((PermissionError,AttributeError)):
            await replace(proof,**{field:value}).authorize()

    # The actual bridge must return a denial caught by shared admission,
    # rather than letting ParticipantError abort selection of unrelated reads.
    e.people.corpus.put('rkb_users',[{'id':e.people.owner.subject,'status':'disabled'}])
    with pytest.raises(PermissionError):await e.bridge.authorize_job(None,job)
    e.bridge._packet_settled(e.parent);await e.helper._packet_task
    assert not e.produced
    e.people.corpus.put('rkb_users',[{'id':e.people.owner.subject,'status':'active'}])

    # Fault injection in this synthetic fixture only: UNKNOWN must not enqueue
    # or redeliver the parent. Restore the exact retained receipt afterward.
    with e.q.connect() as db:
        saved=tuple(db.execute('SELECT apply_state,apply_bytes,outcome_bytes,outcome_sha256 '
            'FROM rkb_control_operations WHERE operation_key=?',(c.operation_key,)).fetchone())
        db.execute("UPDATE rkb_control_operations SET apply_state='SENT_UNKNOWN',apply_bytes=NULL,"
            'outcome_bytes=NULL,outcome_sha256=NULL WHERE operation_key=?',(c.operation_key,))
    e.bridge._packet_settled(e.parent);await e.helper._packet_task
    assert not e.produced
    with e.q.connect() as db:
        db.execute('UPDATE rkb_control_operations SET apply_state=?,apply_bytes=?,outcome_bytes=?,outcome_sha256=? '
            'WHERE operation_key=?',(*saved,c.operation_key))

    clock=[0.];provider=NS(issuer='https://fixture.invalid',resource='fixture',owner_subject=e.row['person_id'],
        personal_policy=NS(participants=e.people.registry))
    authority=ReadAuthority(e.people.registry,oauth_provider=provider,issuer=provider.issuer,resource=provider.resource,clock=lambda:clock[0])
    for mode in ('released','expired'):
        lease=authority._retain(person_id=e.row['person_id'],route='owner',current=authority._state(e.people.owner,'owner'),
            destination=TrustedReadDestination('mcp','personal','fixture'),expires_at=1000.,client_id=e.people.owner.client_id)
        if mode=='released':authority.release(lease)
        else:clock[0]=121.
        with pytest.raises(ReadAuthorityError):await authority.recheck(lease)

    # Simulate API owner reconstruction: no retained HTTP/actor context exists.
    await e.helper.close_packets()
    e.bridge._packet_settled(e.parent)
    assert not e.helper._packet_wakes
    e.helper=NativeSourceIntake(e.importer)
    with pytest.raises(PermissionError):await proof.authorize()
    await e.helper.recover_packets()
    e.bridge._packet_settled(e.parent)  # Coalesces with the restart wake.
    await asyncio.wait_for(e.queued.wait(),10)
    proof=await e.helper._packet_proof(INGESTION)
    doc=dict(codec='rkb.source-batch.v1',ingestion_id=INGESTION,batch_index=0,batch_count=1,
        last_batch_sha256=digest(c.payload),materializer_sha256=materializer_fingerprint(),component='text')
    command=resolved_command(sequence=2,operation_key=f'source-packet:{INGESTION}:0:text',request_id=REQUEST,
        person_id=e.row['person_id'],logical_time='2026-10-27T14:00:00Z',source_id=SOURCE,payload=doc)
    assert await proof.authorize(command)==e.row['person_id']
    original=object();contexts={e.row['person_id']:original}
    admission=NS(_active=lambda:None,queue=e.q,_documents={},_commands=contexts,_event=asyncio.Event())
    await SharedReadAdmission.register_command_actor(admission,proof,command)
    assert contexts=={e.row['person_id']:original}
    for changed in [replace(command,operation_key=command.operation_key+':other'),
            replace(command,ids={'source_id':'00000000-0000-0000-0000-000000000099'}),
            replace(command,payload=canonical(doc|{'ingestion_id':'00000000-0000-0000-0000-000000000099'})),
            replace(command,request_id='00000000-0000-0000-0000-000000000099'),
            replace(command,expected_revisions={'batch_head':'f'*64}),
            replace(command,operation_key=f'source-packet:{INGESTION}:0:semantic',payload=canonical(doc|{'component':'semantic'}))]:
        with pytest.raises(PermissionError):await e.spool.authorize_command(proof,changed)
    for kind,route in [('stage',STAGE_ROUTE),('finish','source.finish.v1')]:
        with pytest.raises(PermissionError):
            await e.helper._stage_exact(proof,e.row,kind=kind,key='forbidden',route=route,payload={},builder=None)

    e.allow_native.set()
    async def finished():
        while e.helper._state(e.row)['enriched_batch_count']!=1:
            await asyncio.sleep(.01)
    await asyncio.wait_for(finished(),10)
    assert e.settled==['stage','text','semantic']
    assert e.produced==[f'source-packet:{INGESTION}:0:text',f'source-packet:{INGESTION}:0:semantic']
    assert e.helper._state(e.row)['phase']=='staging'  # Never next stage or finish.
    for _ in range(4):e.bridge._packet_settled(e.parent)
    await e.helper._packet_task
    assert len(e.produced)==2
    with pytest.raises(PermissionError):await proof.authorize(command)
    # Historical settled jobs retain their independently authorized actor path.
    assert await e.bridge.authorize_job(e.people.admin,job)==e.row['person_id']
    with pytest.raises(PermissionError):await e.bridge.authorize_job(None,job)


@pytest.mark.asyncio
async def test_accepted_pending_stage_returns_next_read_cursor_without_status_poll(packet,monkeypatch):
    e=packet;e.importer._output=KaggleSourceImporter._output
    state=e.helper._state(e.row);key=e.parent.command.operation_key
    # The actual accepted spool entry has not advanced compact intake progress.
    assert state['pending_kind']=='stage' and state['batch_count']==0
    result=await e.helper._accepted_stage_output(e.row,state,key)
    assert (result.state,result.next_action,result.next_cursor)==('processing','continue_pages','1')
    assert 'not Dataset Saved' in result.message
    assert e.helper._output(e.row,state).next_action=='wait'  # Status/finalize unchanged.
    actual=await e.spool.get(key)
    async def read_entry(_):return current
    monkeypatch.setattr(e.helper,'_entry',read_entry)
    for current in [replace(actual,apply_state='NOT_SENT',apply_receipt=None),
            replace(actual,apply_state='SENT_UNKNOWN',apply_receipt=None)]:
        assert (await e.helper._accepted_stage_output(e.row,state,key)).next_action=='continue_pages'
    for current in [replace(actual,accepted=None,seal_state='NOT_SENT'),
            replace(actual,accepted=None,seal_state='SENT_UNKNOWN'),
            replace(actual,apply_state='REJECTED_PRECONDITION')]:
        assert (await e.helper._accepted_stage_output(e.row,state,key)).next_action=='wait'
    current=actual
    assert (await e.helper._accepted_stage_output(e.row,state|{'read_completion_requested':1},key)).next_action=='wait'
