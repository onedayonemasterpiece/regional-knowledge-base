"""Owned APPLIED candidate work may continue; activation/durability stay fenced."""
from types import SimpleNamespace as NS
from contextlib import closing
import json
import sqlite3
import pytest
import pytest_asyncio

from regional_knowledge.bge_queue import BgeQueue
from regional_knowledge.control_spool import ControlSpool
from regional_knowledge.kaggle_current_registry import current_registry
from regional_knowledge.kaggle_native_source_intake import NativeSourceIntake
from regional_knowledge.kaggle_native_source_bindings import resolved_native_stage_command
from regional_knowledge.recovery_journal import RecoveryJournal
from regional_knowledge.source_stage import STAGE_ROUTE,canonical,digest
from regional_knowledge.source_activation import ACTIVATION_REVISIONS,resolved_activation_command
from test_participants import people
from test_native_intake_history import commands
from test_source_stage import SOURCE,INGESTION,REQUEST,batch
from test_source_activation import activation_payload
from test_recovery_journal import ConditionalMemoryStore
from test_control_spool import receipt
from test_source_history_wss import pair


@pytest_asyncio.fixture
async def applied_candidate(people,tmp_path):
    q=BgeQueue(tmp_path/'queue.sqlite');store=ConditionalMemoryStore()
    journal=RecoveryJournal(store,stream='inactive-work-cycle',max_outcome_bytes=8192)
    registry=current_registry();spool=ControlSpool(q,people.registry,journal,registry)
    native=pair(NS(q=q,spool=spool,registry=registry,journal=journal),extra_routes=('source.import',))
    async def check(*args):pass
    startup=NS(config=NS(writer_generation='writer-1'),coverage=None)
    importer=NS(import_revisions=ACTIVATION_REVISIONS,queue=q,spool=spool,startup=startup,_check=check,
        _revisions=lambda binding:ACTIVATION_REVISIONS,
        _intake_coverage_current=lambda binding:native.v._coverage.current())
    helper=NativeSourceIntake(importer)
    row=dict(ingestion_id=INGESTION,source_id=SOURCE,person_id=people.owner.subject,request_id=REQUEST)
    access=NS(binding=native.b,authority=NS(person_id=people.owner.subject),context=people.admin)
    helper._state(row,create=True)
    try:
        await native.v.establish_genesis(native.b)
        command=commands(people.owner.subject)[0]
        frozen,data,handler=spool._validate(command,'db-source')
        await spool._write(spool._stage,frozen,data,handler,'db-source')
        accepted=await spool.seal(people.admin,command.operation_key,writer_generation='writer-1',predecessor=None)
        outcome=receipt(command);native.pg.applied_seq=1
        native.pg.receipts=[(1,command.operation_key,digest(command.encode()),handler.fingerprint,
            accepted.manifest.sha256,outcome.status,outcome.outcome_sha256,digest(outcome.outcome),len(outcome.outcome),
            'db-source',digest(command.encode()))]
        await spool.record_apply(command.operation_key,outcome)
        startup.coverage=await native.v.extend(native.b)
        yield NS(q=q,spool=spool,native=native,helper=helper,row=row,access=access,command=command,startup=startup)
    finally:
        await native.n.aclose();spool.close()


@pytest.mark.asyncio
async def test_exact_applied_inactive_prefix_can_allocate_next_owned_candidate(applied_candidate):
    e=applied_candidate;scope=await e.spool.scope_state()
    assert scope['pending'] and not scope['unknown']
    assert e.helper._inactive_prefix_current(e.access,e.row,scope)
    doc=batch(index=1,previous=digest(e.command.payload),start=1,count=1)
    entry=await e.helper._stage_exact(e.access,e.row,kind='stage',key='next-owned-stage',route=STAGE_ROUTE,
        payload=doc,builder=lambda **args:resolved_native_stage_command(route=STAGE_ROUTE,**args))
    assert entry.command.sequence==2 and entry.command.payload==canonical(doc)
    assert (await e.spool.get(e.command.operation_key)).outcome_state=='NOT_SENT'
    assert entry.outcome_state=='NOT_SENT'


@pytest.mark.asyncio
async def test_stage_busy_before_spool_write_is_not_accepted_and_same_stage_can_retry(applied_candidate,monkeypatch):
    from mcp.server.mcpserver.exceptions import ToolError
    e=applied_candidate;write=e.spool._write;blocked=[]
    async def locked_write(call,*args):
        if call==e.spool._stage and not blocked:
            blocked.append(True)
            with closing(sqlite3.connect(e.q.path)) as holder:
                holder.execute('BEGIN IMMEDIATE')
                return await write(call,*args)
        return await write(call,*args)
    monkeypatch.setattr(e.spool,'_write',locked_write)
    doc=batch(index=1,previous=digest(e.command.payload),start=1,count=1)
    args=dict(kind='stage',key='busy-stage',route=STAGE_ROUTE,payload=doc,
        builder=lambda **kw:resolved_native_stage_command(route=STAGE_ROUTE,**kw))
    with pytest.raises(ToolError) as caught:
        await e.helper._stage_exact(e.access,e.row,**args)
    assert json.loads(str(caught.value))==dict(status='unavailable',reason='source_stage_not_accepted_retry_same_stage')
    with e.q.connect(write=False) as db:
        assert db.execute("SELECT 1 FROM rkb_control_operations WHERE operation_key='busy-stage'").fetchone() is None
    assert e.helper._state(e.row)['pending_operation_key'] is None
    # A failed write invalidates only the cached history proof; refresh it
    # through the existing verifier before retrying the exact unchanged stage.
    e.startup.coverage=await e.native.v.extend(e.native.b)
    entry=await e.helper._stage_exact(e.access,e.row,**args)
    assert entry.command.payload==canonical(doc)
    assert e.helper._state(e.row)['pending_operation_key']=='busy-stage'


@pytest.mark.asyncio
async def test_stage_busy_after_spool_commit_is_not_reported_as_not_accepted(applied_candidate,monkeypatch):
    e=applied_candidate
    error=sqlite3.OperationalError('synthetic pending update contention')
    error.sqlite_errorcode=sqlite3.SQLITE_BUSY
    def fail(*args,**kwargs):raise error
    monkeypatch.setattr(e.helper,'_update',fail)
    doc=batch(index=1,previous=digest(e.command.payload),start=1,count=1)
    with pytest.raises(sqlite3.OperationalError) as caught:
        await e.helper._stage_exact(e.access,e.row,kind='stage',key='committed-stage',route=STAGE_ROUTE,
            payload=doc,builder=lambda **kw:resolved_native_stage_command(route=STAGE_ROUTE,**kw))
    assert caught.value is error
    assert (await e.spool.get('committed-stage')).command.payload==canonical(doc)
    assert e.helper._state(e.row)['pending_operation_key'] is None


@pytest.mark.parametrize('kind,code',[('stage',sqlite3.SQLITE_IOERR),('stage',sqlite3.SQLITE_ERROR),
    ('stage',sqlite3.SQLITE_LOCKED),('stage',sqlite3.SQLITE_BUSY_SNAPSHOT),('stage',None),('batch',sqlite3.SQLITE_BUSY)])
@pytest.mark.asyncio
async def test_stage_busy_classification_does_not_expand_to_other_failures(applied_candidate,monkeypatch,kind,code):
    e=applied_candidate;error=sqlite3.OperationalError('synthetic unclassified failure')
    if code is not None:error.sqlite_errorcode=code
    async def fail(*args):raise error
    monkeypatch.setattr(e.spool,'_write',fail)
    doc=batch(index=1,previous=digest(e.command.payload),start=1,count=1)
    with pytest.raises(sqlite3.OperationalError) as caught:
        await e.helper._stage_exact(e.access,e.row,kind=kind,key='unclassified-stage',route=STAGE_ROUTE,
            payload=doc,builder=lambda **kw:resolved_native_stage_command(route=STAGE_ROUTE,**kw))
    assert caught.value is error
    assert e.helper._state(e.row)['pending_operation_key'] is None


@pytest.mark.parametrize('field,value',[
    ('person_id','different-owner'),('source_id','00000000-0000-0000-0000-000000000099'),
    ('ingestion_id','00000000-0000-0000-0000-000000000099')])
@pytest.mark.asyncio
async def test_other_owner_source_or_ingestion_cannot_borrow_pending_prefix(applied_candidate,field,value):
    e=applied_candidate
    assert not e.helper._inactive_prefix_current(e.access,e.row|{field:value},await e.spool.scope_state())


@pytest.mark.asyncio
async def test_unknown_outcome_and_unproved_history_do_not_continue(applied_candidate):
    e=applied_candidate;proof=e.startup.coverage
    e.startup.coverage=None
    assert not e.helper._inactive_prefix_current(e.access,e.row,await e.spool.scope_state())
    e.startup.coverage=proof
    await e.spool._write(e.spool._outcome_sent,e.command.operation_key)
    e.startup.coverage=await e.native.v.extend(e.native.b)
    assert not e.helper._inactive_prefix_current(e.access,e.row,await e.spool.scope_state())


@pytest.mark.asyncio
async def test_activation_still_requires_remote_settlement_of_candidate_prefix(applied_candidate):
    from mcp.server.mcpserver.exceptions import ToolError
    e=applied_candidate;before=await e.spool.scope_state()
    with pytest.raises(ToolError,match='prior_write_pending'):
        await e.helper._stage_exact(e.access,e.row,kind='activate',key='must-not-activate',route='source.activate.v1',
            payload=activation_payload([batch(count=1)],digest(e.command.payload)),builder=resolved_activation_command)
    assert (await e.spool.scope_state())['sequence']==before['sequence']


@pytest.mark.asyncio
async def test_reading_finish_applied_not_dataset_saved_continues_and_forces_snapshot(applied_candidate,monkeypatch):
    """Native finish APPLIED is not cloud Saved and never waits for Dataset."""
    e=applied_candidate;requests=[]
    e.startup.dataset_checkpoints=NS(kick=lambda binding,planned_stop=False:
        requests.append((binding,planned_stop)))
    monkeypatch.setattr(e.helper,'_result',lambda entry:{
        'source_id':e.row['source_id'],'ingestion_id':e.row['ingestion_id'],
        'read_complete':True})
    monkeypatch.setattr(e.helper.importer,'_save',lambda row,**changes:row|changes,raising=False)
    state=e.helper._update(e.row,pending_operation_key='finish-key',pending_kind='finish')
    known=NS(outcome_state='NOT_SENT',accepted=object(),seal_state='ACCEPTED',
        apply_state='APPLIED',apply_receipt=NS(status='applied'),
        command=NS(payload='{}',operation_key='finish-key'))
    assert e.helper._can_settle(state,known)
    # A previously accepted but still claimed or UNKNOWN result is NOT applied.
    assert not e.helper._can_settle(state,NS(**(known.__dict__|{'apply_state':'SENT_UNKNOWN'})))
    assert not e.helper._can_settle(state,NS(**(known.__dict__|{'seal_state':'SENT_UNKNOWN'})))
    assert not e.helper._can_settle(state,NS(**(known.__dict__|{'apply_receipt':None})))
    assert not e.helper._can_settle(state,NS(**(known.__dict__|{'outcome_state':'SENT_UNKNOWN'})))
    result=await e.helper._settle(e.access,e.row,state,known)
    assert result['phase']=='finalized' and result['pending_operation_key'] is None
    assert requests==[(e.access.binding,True)]  # schedule only; no provider wait
    assert (await e.spool.get(e.command.operation_key)).outcome_state=='NOT_SENT'
