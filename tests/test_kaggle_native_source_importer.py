"""Actual OAuth/SQLite/ControlSpool14; synthetic originals and native receipts only.

No SQL, provider, model or image-renderer effects. These tests exercise the
public importer and real ordered journal rather than replacing the spool API.
"""
import base64
import copy
import json
from types import MethodType, SimpleNamespace
from uuid import UUID, uuid5

import pytest
import pytest_asyncio
from mcp.server.mcpserver.exceptions import ToolError

from regional_knowledge.contracts import ChatFile, Principal, StageChunkInput, StagePageInput
from regional_knowledge.control_spool import ControlSpool
from regional_knowledge.kaggle_apply import ApplyReceipt, HandlerRegistry
from regional_knowledge.kaggle_import_tokenizer import HfImportTokenCounter
from regional_knowledge.kaggle_native_source_bindings import native_source_handler
from regional_knowledge.kaggle_public_backend import current_binding
from regional_knowledge.kaggle_service_startup import TargetServiceConfig, TargetServiceStartup
from regional_knowledge.original_intake import OriginalIntakeError, OriginalIntakeStatus, VerifiedOriginalReceipt, issued_image_receipt
from regional_knowledge.original_intake_controller import OriginalIntakeController
from regional_knowledge.read_issuance import capture_read_output
from regional_knowledge.recovery_journal import RecoveryJournal
from regional_knowledge.source_activation import ACTIVATION_REVISIONS, ACTIVATE_ROUTE, MATERIALIZE_ROUTE
from regional_knowledge.source_stage import STAGE_ROUTE, SEAL_ROUTE, ZERO_HASH, canonical, digest
from test_kaggle_public_backend import target
from test_kaggle_service_startup import HistoryAdapter
from test_kaggle_source_importer import options
from test_participants import people
from test_read_authority import auth, issue
from test_recovery_journal import ConditionalMemoryStore


@pytest_asyncio.fixture
async def native_intake(target, tmp_path):
    """Own schema14 fixture: the old parametrized 7/8/9/11 fixture is unchanged."""
    t = target
    with t.queue.connect() as db:
        db.execute('DELETE FROM jobs')
        row = db.execute('SELECT expected_json,observed_json FROM bge_runtime_expectations WHERE run_id=?',
                         (t.run,)).fetchone()
        values = []
        for raw in row:
            value = json.loads(raw)
            value.update(schema_version=14)
            values.append(json.dumps(value))
        db.execute('UPDATE bge_runtime_expectations SET expected_json=?,observed_json=? WHERE run_id=?',
                   (*values,t.run))
    store = ConditionalMemoryStore()
    registry = HandlerRegistry([native_source_handler(route,check_sha256='a'*64,apply_sha256='b'*64)
                                for route in (STAGE_ROUTE,SEAL_ROUTE,MATERIALIZE_ROUTE,ACTIVATE_ROUTE,'source.batch.v1','source.finish.v1')])
    spool = ControlSpool(t.queue,t.auth.p.registry,RecoveryJournal(store,stream='native-intake-14'),registry)
    originals = object.__new__(OriginalIntakeController)
    originals.queue, originals.spool, originals.participants = t.queue, spool, t.auth.p.registry
    originals.calls, originals.receipts, originals.issued, originals.selections = [], {}, {}, []

    async def begin(self, context, *, operation_id, source_id, ingestion_id, file, max_bytes):
        assert context.route == 'owner' and context.principal.subject == t.auth.p.owner.subject
        self.calls.append((ingestion_id,file.file_id))
        if ingestion_id not in self.receipts:
            self.receipts[ingestion_id] = VerifiedOriginalReceipt(source_id,ingestion_id,
                str(uuid5(UUID(ingestion_id),'original:'+operation_id)),1,'1'*64,174000,
                'originals/synthetic/'+source_id,'synthetic-version','djvu',174,'2'*64,'3'*64,'4'*64)
        return OriginalIntakeStatus(ingestion_id,'verified',self.receipts[ingestion_id])

    async def verified(self, context, ingestion_id):
        return self.receipts.get(ingestion_id)

    async def status(self, context, ingestion_id):
        return OriginalIntakeStatus(ingestion_id,'verified',self.receipts[ingestion_id])

    async def selected_image_receipts(self, context, ingestion_id, selections):
        assert context.route == 'owner' and context.principal.subject == t.auth.p.owner.subject
        assert 1 <= len(selections) <= 8
        self.selections.append((ingestion_id,selections))
        values=[]
        for index,expected in selections:
            matches=[value for (owner,position,hash_),value in self.issued.items()
                     if owner==ingestion_id and position==index and (expected is None or hash_==expected)]
            if len(matches)!=1: raise OriginalIntakeError('original_source_image_not_issued')
            values.append(matches[0])
        return tuple(values)

    async def native_pages(self, *args, **kwargs):
        pytest.fail('native14 must not auto-fetch the whole original/native-text proof')

    for function in (begin,verified,status,selected_image_receipts,native_pages):
        setattr(originals,function.__name__,MethodType(function,originals))
    tokenizer = HfImportTokenCounter(tmp_path/'unloaded-tokenizer')
    token_calls = []

    async def counts(texts):
        token_calls.append(tuple(texts))
        pytest.fail('native14 model/tokenizer work must not run on Dev')

    tokenizer.counts = counts
    startup = TargetServiceStartup(TargetServiceConfig(t.auth.provider,t.auth.p.registry,t.queue,spool,
        HistoryAdapter(),'writer-1',lambda **binding:binding['database_generation']=='synthetic-db',
        import_originals=originals,import_tokenizer=tokenizer,import_parser_sha256='2'*64,
        import_launch_config_sha256='a'*64))
    startup.running = True
    startup.binding = current_binding(t.queue)
    startup.history.available = True
    startup.coverage = await startup.history.restore(startup.binding)
    # Synthetic native-history/GEO proof seam, as in the existing fresh11
    # importer fixture. Real read_facts, auth, spool, journal and queue remain.
    geo_state = {'current':True}
    startup.imports_current = lambda:startup.reads_current() and geo_state['current']
    tokens = await issue(t.auth.provider)
    principal = Principal(subject=t.auth.p.owner.subject,client_id=t.auth.provider.client.client_id,
        issuer=t.auth.provider.issuer,access_token=tokens.access_token)
    value = SimpleNamespace(t=t,startup=startup,spool=spool,store=store,originals=originals,
        importer=startup.importer,principal=principal,token_calls=token_calls,geo_state=geo_state)
    yield value
    await startup.source_dispatch.aclose()
    spool.close()


async def start(s, *, file_id='synthetic-174-djvu',mime_type='image/vnd.djvu'):
    return await s.importer.book_ingest(command='start',principal=s.principal,
        file=ChatFile(file_id=file_id,download_url='https://files.example.test/174.djvu?private=fixture',
            mime_type=mime_type,file_name='synthetic-174.djvu'),
        payload=dict(title='Synthetic 174-page scanned source',authors=['Synthetic author'],
            publication_year=1901,language='de',catalog={'kind':'book','publisher':'Synthetic publisher'}),
        source_import=options())


def batch(s, result, *, first=0, count=4):
    """Model-authored typed selected pages, not text certified by native OCR."""
    receipt = s.originals.receipts[result.ingestion_id]
    pages, chunks = [], []
    for index in range(first,min(first+count,receipt.page_count)):
        page_id = str(uuid5(UUID(result.document_id),f'page:1:{index}'))
        image = issued_image_receipt(receipt,dict(page_id=page_id,physical_page_index=index,
            original_sha256=receipt.sha256,parser_sha256=receipt.parser_sha256,width=595.,height=842.,
            image_sha256=digest(f'synthetic-issued-pixels-{index}'.encode()),mime_type='image/jpeg',
            preview_render=dict(width=724,height=1024,preview_only=True)))
        s.originals.issued[(result.ingestion_id,index,image['source_image_receipt_sha256'])] = image
        pages.append(StagePageInput.model_validate(dict(page_id=page_id,physical_page_index=index,
            source_image_receipt_sha256=image['source_image_receipt_sha256'],source_material='visual_reviewed',
            source_review_note='Synthetic model reading of this exact issued image.',
            knowledge_reviewed=True,story_candidates_reviewed=True,regions=[dict(region_key='body',kind='body',
                reading_order=0,bbox=dict(left=100,top=100,right=900,bottom=900),
                source_text=f'Explicit model transcription on synthetic scanned page {index}.')])).model_dump(mode='json'))
        chunks.append(StageChunkInput.model_validate(dict(chunk_key=f'page-{index}',title='Synthetic source passage',
            region_refs=[dict(page_id=page_id,region_key='body')])).model_dump(mode='json'))
    return dict(pages=pages,chunks=chunks,story_candidates=[])


async def call(s, result, command, **kwargs):
    return await s.importer.book_ingest(command=command,principal=s.principal,
        ingestion_id=result.ingestion_id,**kwargs)


async def stage(s, result, payload):
    return await call(s,result,'stage',payload=payload,
        source_import=options(research=True,ingestion_id=result.ingestion_id,state='pending'))


@pytest.mark.asyncio
async def test_djvu_hyphenated_mime_uses_the_same_original_intake(native_intake):
    result=await start(native_intake,mime_type='image/x-djvu')
    assert result.source_archive_status=='verified' and result.next_action=='continue_pages'


def state(s, result):
    row = s.importer._row(s.principal.subject,result.ingestion_id)
    return s.importer.native_intake._state(row)


async def pending(s, result):
    return await s.spool.get(state(s,result)['pending_operation_key'])


def operation_count(s):
    with s.t.queue.connect(write=False) as db:
        return db.execute('SELECT count(*) FROM rkb_control_operations').fetchone()[0]


async def finish(s, result, *, publish=True, total_inputs=2, status='applied'):
    """Synthetic worker result; actual ControlSpool/RecoveryJournal durability."""
    entry = await pending(s,result)
    command = entry.command
    doc = json.loads(command.payload)
    output = dict(source_id=result.document_id,ingestion_id=result.ingestion_id)
    if command.route == STAGE_ROUTE:
        output.update(batch_index=doc['batch_index'],batch_sha256=digest(command.payload),
                      candidate_state='staged_inactive')
    elif command.route == SEAL_ROUTE:
        output.update(batch_count=doc['batch_count'],last_batch_sha256=doc['last_batch_sha256'],
                      seal_sha256=digest(command.payload),candidate_state='sealed_inactive')
    elif command.route == MATERIALIZE_ROUTE:
        output = dict(source_id=result.document_id,ordinal=doc['ordinal'],total_inputs=total_inputs,
            knowledge_sha256='e'*64,input_sha256='f'*64,input_id='synthetic-native-input')
    elif command.route == 'source.batch.v1':
        output.update(batch_index=doc['batch_index'],component=doc['component'],candidate_state='published',accepted=True,knowledge_sha256='e'*64)
    elif command.route == 'source.finish.v1':
        output.update(read_complete=True)
    else:
        assert command.route == ACTIVATE_ROUTE
        output = dict(source_id=result.document_id,accepted=True,knowledge_sha256='e'*64)
    raw = canonical(output)
    outcome = canonical(dict(codec='rkb.applied-outcome.v1',operation_key=command.operation_key,
        command_sha256=entry.command_sha256,cursor=command.sequence,status=status,ids=dict(command.ids),
        request_id=command.request_id,person_id=command.person_id,route=command.route,logical_time=command.logical_time,
        before=list(ACTIVATION_REVISIONS.wire()),after=list(ACTIVATION_REVISIONS.wire()),
        handler_sha256=entry.handler_sha256,result_b64=base64.b64encode(raw).decode(),result_sha256=digest(raw)))
    receipt = ApplyReceipt('synthetic-db',command.operation_key,command.sequence,entry.command_sha256,
                          status,outcome,digest(outcome),False)
    await s.spool.record_apply(command.operation_key,receipt)
    if publish:
        await s.spool.publish_outcome(command.operation_key,writer_generation='writer-1')
    return entry


@pytest.mark.asyncio
@pytest.mark.parametrize('authored_cursor',[None,'4'])
async def test_status_after_pause_returns_next_unaccepted_physical_page(native_intake,authored_cursor):
    s=native_intake
    book=await start(s)
    payload=batch(s,book,first=0,count=4)
    if authored_cursor is None:
        assert (await stage(s,book,payload)).next_action=='wait'
    else:
        from regional_knowledge.kaggle_import_contracts import TargetSourceImportInput
        research=TargetSourceImportInput.model_validate({'research':dict(state='pending',
            continuation=dict(tool='book_pages',arguments=dict(
                ingestion_id=book.ingestion_id,cursor=authored_cursor,batch_size=1)))})
        assert (await call(s,book,'stage',payload=payload,source_import=research)).next_action=='wait'
    for component in ('stage','text','semantic'):
        await finish(s,book)
        status=await call(s,book,'status')
    assert status.state=='staged' and status.next_action=='continue_pages'
    assert status.next_cursor=='4'  # APPLIED physical 0–3; resumed book_pages starts at 4
    count=operation_count(s)
    # A later fresh status must recover the same continuation from durable
    # stage/research data without stage replay or a new import.
    resumed=await call(s,book,'status')
    assert resumed.next_cursor=='4' and resumed.next_action=='continue_pages'
    assert operation_count(s)==count
    row=s.importer._row(s.principal.subject,book.ingestion_id)
    assert row['stage_json'] is None  # no second model payload table

@pytest.mark.asyncio
async def test_applied_page_status_needs_no_second_original_head(native_intake, monkeypatch):
    s=native_intake
    book=await start(s)
    assert (await stage(s,book,batch(s,book,first=0,count=4))).next_action=='wait'
    for _ in range(3):
        await finish(s,book)
        await call(s,book,'status')

    verified_calls=[]
    async def unavailable_original_head(context, ingestion_id):
        verified_calls.append(ingestion_id)
        raise OriginalIntakeError('object_storage_head_unavailable')
    monkeypatch.setattr(s.originals,'verified',unavailable_original_head)
    before=operation_count(s)
    resumed=await call(s,book,'status')
    assert resumed.state=='staged' and resumed.next_action=='continue_pages'
    assert resumed.next_cursor=='4' and verified_calls==[]
    assert operation_count(s)==before
    # The actual page read must still prove the archived original.
    with pytest.raises(ToolError,match='native_original_page_unavailable'):
        await s.importer.book_pages(principal=s.principal,ingestion_id=book.ingestion_id,cursor='4')
    assert verified_calls==[book.ingestion_id]



@pytest.mark.asyncio
async def test_resume_cursor_ignores_newer_stage_not_yet_applied(native_intake):
    s=native_intake
    book=await start(s)
    assert (await stage(s,book,batch(s,book,first=0,count=4))).state=='processing'
    for _ in range(3):
        await finish(s,book)
        await call(s,book,'status')
    assert (await call(s,book,'status')).next_cursor=='4'
    assert (await stage(s,book,batch(s,book,first=4,count=2))).state=='processing'
    row=s.importer._row(s.principal.subject,book.ingestion_id)
    assert await s.importer.native_intake.applied_page_cursor(None,row)=='4'
    assert (await call(s,book,'status')).next_action=='wait'


@pytest.mark.asyncio
async def test_174_page_original_resumes_44_small_packets_without_dev_graph_or_vectors(native_intake):
    s = native_intake
    result = await start(s)
    assert s.originals.receipts[result.ingestion_id].page_count == 174
    previous, seen = ZERO_HASH, []
    for ordinal, first in enumerate(range(0,174,4)):
        payload = batch(s,result,first=first)
        assert 1 <= len(payload['pages']) <= 4
        assert (await stage(s,result,payload)).state == 'processing'
        entry = await pending(s,result)
        doc = json.loads(entry.command.payload)
        assert entry.command.route == STAGE_ROUTE
        assert (entry.command.schema_revision,entry.command.function_revision,entry.command.executor_revision) == ACTIVATION_REVISIONS.wire()
        assert doc['batch_index'] == ordinal and doc['previous_batch_sha256'] == previous
        assert doc['source']['metadata']['publication_year'] == 1901
        assert doc['model_payload'] == payload and doc['research']['state'] == 'pending'
        assert 'download_url' not in entry.command_bytes.decode() and 'private=fixture' not in entry.command_bytes.decode()
        previous = digest(entry.command.payload)
        seen.extend(p['physical_page_index'] for p in doc['model_payload']['pages'])
        await finish(s,result)
        assert (await call(s,result,'status')).state == 'processing'
        assert (await pending(s,result)).command.route=='source.batch.v1'
        await finish(s,result)
        assert (await call(s,result,'status')).state=='processing'
        assert json.loads((await pending(s,result)).command.payload)['component']=='semantic'
        await finish(s,result)
        assert (await call(s,result,'status')).state == 'staged'
        # An exact post-durability retry must not allocate batch ordinal+1.
        assert (await stage(s,result,payload)).state == 'staged'
        assert operation_count(s) == (ordinal+1)*3
        row = s.importer._row(s.principal.subject,result.ingestion_id)
        assert row['stage_json'] is None and row['semantic_json'] is None and row['native_proof_sha256'] is None
    assert seen == list(range(174)) and state(s,result)['batch_count'] == 44
    assert state(s,result)['last_batch_sha256'] == previous and not s.token_calls
    with s.t.queue.connect(write=False) as db:
        assert db.execute("SELECT count(*) FROM jobs WHERE kind<>'source_command'").fetchone()[0] == 0
        columns = {row[1] for row in db.execute('PRAGMA table_info(rkb_native_source_intake_state)')}
        assert not columns.intersection({'stage_json','semantic_json','model_payload','embedding','body'})
    assert len(s.originals.calls) == 1


@pytest.mark.asyncio
async def test_issued_native_page_without_model_sha_and_exact_replay(native_intake):
    s=native_intake; result=await start(s); payload=batch(s,result,count=1)
    issued_hash=payload['pages'][0].pop('source_image_receipt_sha256')
    assert (await stage(s,result,payload)).state=='processing'
    entry=await pending(s,result)
    frozen=entry.command_bytes; doc=json.loads(entry.command.payload)
    assert doc['model_payload']['pages'][0]['source_image_receipt_sha256']==issued_hash
    assert len(doc['issued_images_b64'])==1 and operation_count(s)==1
    assert (await stage(s,result,payload)).state=='processing'
    assert (await pending(s,result)).command_bytes==frozen
    guessed=copy.deepcopy(payload);guessed['pages'][0]['source_image_receipt_sha256']='0'*64
    assert (await stage(s,result,guessed)).state=='processing'
    assert (await pending(s,result)).command_bytes==frozen
    assert operation_count(s)==1
    await finish(s,result)
    assert (await call(s,result,'status')).state=='processing'
    assert (await s.spool.get(entry.command.operation_key)).command_bytes==frozen


@pytest.mark.asyncio
@pytest.mark.parametrize('case',['unissued','foreign_ingestion','forged_page_id'])
async def test_native_unissued_foreign_or_forged_page_denied_without_model_sha(native_intake,case):
    s=native_intake; result=await start(s); payload=batch(s,result,count=1)
    payload['pages'][0].pop('source_image_receipt_sha256')
    if case=='unissued':
        s.originals.issued.clear()
    elif case=='foreign_ingestion':
        (ingest,index,image_hash),value=s.originals.issued.popitem()
        s.originals.issued[('00000000-0000-0000-0000-000000000099',index,image_hash)]=value
    else:
        payload['pages'][0]['page_id']='00000000-0000-0000-0000-000000000099'
    with pytest.raises(ToolError):
        await stage(s,result,payload)
    assert operation_count(s)==0 and state(s,result)['pending_operation_key'] is None


@pytest.mark.asyncio
async def test_exact_pending_retry_preserves_request_and_rejects_changed_text(native_intake):
    s = native_intake; result = await start(s); payload = batch(s,result,count=1)
    await stage(s,result,payload)
    entry = await pending(s,result); exact = entry.command_bytes; request = entry.command.request_id
    for _ in range(2):
        assert (await stage(s,result,payload)).state == 'processing'
        assert (await pending(s,result)).command_bytes == exact
    changed = copy.deepcopy(payload)
    changed['pages'][0]['regions'][0]['source_text'] = 'Different model reading must be a later explicit batch.'
    with pytest.raises(ToolError,match='native_pending_payload_changed'):
        await stage(s,result,changed)
    assert operation_count(s) == 1 and (await pending(s,result)).command.request_id == request
    await finish(s,result); await call(s,result,'status')
    assert (await pending(s,result)).command.route=='source.batch.v1'
    await finish(s,result);await call(s,result,'status')
    await finish(s,result);await call(s,result,'status')
    await stage(s,result,changed)
    second = await pending(s,result)
    assert second.command.sequence == entry.command.sequence+3
    assert json.loads(second.command.payload)['previous_batch_sha256'] == digest(entry.command.payload)
    assert (await s.spool.get(entry.command.operation_key)).command_bytes == exact


@pytest.mark.asyncio
async def test_real_lost_acceptance_ack_keeps_unknown_key_then_read_only_reconciles(native_intake):
    s = native_intake; result = await start(s); payload = batch(s,result,count=1)
    s.store.fail_head_read_after_cas = True
    assert (await stage(s,result,payload)).state == 'processing'
    entry = await pending(s,result); exact = entry.command_bytes
    assert entry.seal_state == 'SENT_UNKNOWN' and entry.accepted is None
    assert (await call(s,result,'status')).state == 'processing'
    assert operation_count(s) == 1 and state(s,result)['batch_count'] == 0
    s.store.fail_head_read_after_cas = False
    s.store.read_failures.clear()
    writes = s.store.version
    assert (await call(s,result,'status')).state == 'processing'
    reconciled = await pending(s,result)
    assert reconciled.accepted is not None and reconciled.command_bytes == exact
    assert s.store.version == writes  # Locate existing head, not a new remote seal.
    assert operation_count(s) == 1 and state(s,result)['batch_count'] == 0
    with s.t.queue.connect(write=False) as db:
        assert db.execute("SELECT count(*) FROM jobs WHERE kind='source_command'").fetchone()[0] == 1


@pytest.mark.asyncio
async def test_packet_progress_and_reading_completion_keep_exact_durability_receipts(native_intake):
    s = native_intake; result = await start(s)
    await stage(s,result,batch(s,result,count=1))
    staged = await finish(s,result,publish=False)
    assert (await call(s,result,'status')).state == 'processing'
    assert state(s,result)['batch_count'] == 1 and operation_count(s) == 1
    assert (await s.spool.get(staged.command.operation_key)).outcome_state == 'NOT_SENT'
    await s.spool.publish_outcome(staged.command.operation_key,writer_generation='writer-1')
    assert (await call(s,result,'status')).state == 'processing'
    packet=await pending(s,result)
    assert packet.command.route=='source.batch.v1' and operation_count(s)==2
    await finish(s,result)
    assert (await call(s,result,'status')).state=='processing'
    assert json.loads((await pending(s,result)).command.payload)['component']=='semantic'
    await finish(s,result)
    assert (await call(s,result,'status')).state=='staged'
    assert state(s,result)['published_batch_count']==1
    assert (await call(s,result,'finalize')).state=='processing'
    completion=await pending(s,result)
    assert completion.command.route=='source.finish.v1'
    await finish(s,result,publish=False)
    # A native APPLIED reading completion is immediately usable; the cloud
    # Dataset receipt remains separately pending and never becomes fake Saved.
    assert (await call(s,result,'status')).state == 'finalized'
    unpersisted=await s.spool.get(completion.command.operation_key)
    assert unpersisted.outcome_state == 'NOT_SENT' and unpersisted.remote_outcome is None
    await s.spool.publish_outcome(completion.command.operation_key,writer_generation='writer-1')
    durable=await s.spool.get(completion.command.operation_key)
    assert durable.outcome_state == 'DURABLE' and durable.remote_outcome is not None
    assert (await call(s,result,'status')).state == 'finalized'
    assert (await call(s,result,'finalize')).state == 'finalized'
    assert operation_count(s) == 4 and not s.token_calls


@pytest.mark.asyncio
async def test_start_allows_same_file_retry_but_blocks_second_original_before_acquisition(native_intake):
    s = native_intake
    async def acquiring(self, context, *, operation_id, source_id, ingestion_id, file, max_bytes):
        self.calls.append((ingestion_id,file.file_id))
        return OriginalIntakeStatus(ingestion_id,'pending',reason='original_acquisition_pending')
    s.originals.begin=MethodType(acquiring,s.originals)
    first = await start(s)
    assert first.state=='processing'
    assert (await start(s)).ingestion_id == first.ingestion_id
    calls = len(s.originals.calls)
    with pytest.raises(ToolError,match='another_native_ingestion_active'):
        await start(s,file_id='second-unrelated-original')
    assert len(s.originals.calls) == calls


@pytest.mark.asyncio
async def test_normal_start_keeps_inactive_legacy_rows_and_accepted_bytes_unchanged(native_intake):
    s=native_intake;old=await start(s)
    row=s.importer._row(s.principal.subject,old.ingestion_id)
    s.importer.native_intake._state(row,create=True)
    with s.t.queue.connect() as db:
        db.execute('UPDATE rkb_native_source_intake_state SET packet_mode=0 WHERE ingestion_id=?',(old.ingestion_id,))
    await stage(s,old,batch(s,old,count=1))
    entry=await finish(s,old) # Known durable receipt, not yet settled in compact progress.
    frozen=entry.command_bytes
    before_row=s.importer._row(s.principal.subject,old.ingestion_id)
    before_state=state(s,old)
    new=await start(s)
    assert new.ingestion_id!=old.ingestion_id and new.document_id!=old.document_id
    assert s.importer._row(s.principal.subject,old.ingestion_id)==before_row
    assert state(s,old)==before_state
    assert (await s.spool.get(entry.command.operation_key)).command_bytes==frozen
    assert (await s.spool.get(entry.command.operation_key)).outcome_state=='DURABLE'
    assert len(s.originals.calls)==2


@pytest.mark.asyncio
async def test_new_upload_can_stage_after_foreign_uncertain_legacy_operation(native_intake):
    s=native_intake;old=await start(s)
    row=s.importer._row(s.principal.subject,old.ingestion_id)
    s.importer.native_intake._state(row,create=True)
    with s.t.queue.connect() as db:
        db.execute('UPDATE rkb_native_source_intake_state SET packet_mode=0 WHERE ingestion_id=?',(old.ingestion_id,))
    await stage(s,old,batch(s,old,count=1))
    entry=await pending(s,old)
    await s.spool.mark_apply_sent(entry.command.operation_key)
    before_row=s.importer._row(s.principal.subject,old.ingestion_id)
    before_state=state(s,old)
    again=await start(s)
    assert again.ingestion_id!=old.ingestion_id
    assert s.importer._row(s.principal.subject,old.ingestion_id)==before_row
    assert state(s,old)==before_state
    assert (await s.spool.get(entry.command.operation_key)).apply_state=='SENT_UNKNOWN'
    assert len(s.originals.calls)==2
    assert (await stage(s,again,batch(s,again,count=1))).state=='processing'
    next_entry=await pending(s,again)
    assert next_entry.command.sequence==entry.command.sequence+1
    assert (await s.spool.get(entry.command.operation_key)).command_bytes==entry.command_bytes
    assert (await s.spool.get(entry.command.operation_key)).apply_state=='SENT_UNKNOWN'
    with s.t.queue.connect(write=False) as db:
        assert db.execute('SELECT count(*) FROM rkb_source_intake_control').fetchone()[0] == 2


@pytest.mark.asyncio
async def test_revocation_during_selected_image_read_prevents_spool_acceptance(native_intake):
    s = native_intake; result = await start(s); payload = batch(s,result,count=1)
    payload['pages'][0].pop('source_image_receipt_sha256')
    selected = s.originals.selected_image_receipts
    async def revoke(*args):
        values = await selected(*args)
        await s.t.auth.provider.revoke_token(await s.t.auth.provider.load_access_token(s.principal.access_token))
        return values
    s.originals.selected_image_receipts = revoke
    with pytest.raises(ToolError,match='access_denied'):
        await stage(s,result,payload)
    assert operation_count(s) == 0 and not s.store.objects
    assert state(s,result)['pending_operation_key'] is None


@pytest.mark.asyncio
async def test_unknown_status_retains_current_owner_output_fence(native_intake):
    s = native_intake; result = await start(s)
    await stage(s,result,batch(s,result,count=1))
    entry = await pending(s,result)
    await s.spool.mark_apply_sent(entry.command.operation_key)
    with capture_read_output() as outputs:
        value = await call(s,result,'status')
        assert value.state == 'processing' and len(outputs) == 1
    retained = outputs[0]
    assert retained.gate.issuance_current(retained.gated.ticket)
    await s.t.auth.provider.revoke_token(await s.t.auth.provider.load_access_token(s.principal.access_token))
    assert not retained.gate.issuance_current(retained.gated.ticket)
    with pytest.raises(PermissionError):
        await retained.gate.refresh(retained.gated)
    retained.release()
    with pytest.raises(ToolError,match='access_denied'):
        await call(s,result,'status')
    assert operation_count(s) == 1 and (await pending(s,result)).command_bytes == entry.command_bytes


@pytest.mark.asyncio
@pytest.mark.parametrize('domain',['case','story_access','source_import','geo_manifest','native_stage'])
@pytest.mark.parametrize('phase',['accepted','applied','rejected_precondition','apply_unknown','outcome_unknown','unaccepted'])
async def test_verified_foreign_terminal_domain_does_not_hold_ordinary_book_stage(native_intake,domain,phase):
    """Real exact receipt/seal/owner/history gate, never a synthetic success flag."""
    s=native_intake;book=await start(s);pages=batch(s,book,count=1)
    actor=s.t.auth.p.owner.subject
    foreign_source='00000000-0000-0000-0000-000000000005'
    if domain=='case':
        from regional_knowledge.disagreement_native import CASE_EDIT,case_handler,resolved_case_command
        from test_disagreement_native import CASE,create
        handler=case_handler(CASE_EDIT,check_sha256='c'*64,apply_sha256='d'*64)
        command=resolved_case_command(sequence=1,operation_key='foreign-case-before-book-stage',
            request_id='foreign-case-request',person_id=actor,logical_time='2026-10-24T00:00:00Z',
            case_id=CASE,object_id=CASE,action='create',command=create(),
            source_dependencies=[],attribution=dict(person_id=actor,recorded_by=actor,
                route='owner',link_id=None,link_revision=None,human_reading_confirmed=False))
    elif domain=='story_access':
        from regional_knowledge.story_editorial import STORY_ACCESS,editorial_handler,resolved_story_access_command
        handler=editorial_handler(route=STORY_ACCESS,check_sha256='c'*64,apply_sha256='d'*64)
        command=resolved_story_access_command(sequence=1,operation_key='foreign-story-access-before-book-stage',
            request_id='foreign-story-access-request',person_id=actor,logical_time='2026-10-24T00:00:00Z',
            source_id=foreign_source,story_id='00000000-0000-0000-0000-000000000006',
            expected_revision=1,source_revision=1,eligibility_revision=1,
            grantee_user_id=actor,capability='viewer')
    elif domain=='source_import':
        from regional_knowledge.source_import import source_import_handler
        from test_source_import import VERSIONS,resolved
        handler=source_import_handler(revisions=VERSIONS,check_sha256='c'*64,apply_sha256='d'*64)
        command=resolved(sequence=1,operation_key='foreign-import-before-book-stage',
            request_id='foreign-import-request',person_id=actor,source_id=foreign_source)
    elif domain=='native_stage':
        from test_native_intake_history import commands
        command=commands(actor)[0]
        handler=s.spool.registry.resolve(command)
        assert json.loads(command.payload)['ingestion_id']!=book.ingestion_id
    else:
        from regional_knowledge.geo_manifest_commands import GEO_REVISIONS,GEO_MANIFEST_INSTALL,geo_manifest_handler
        from regional_knowledge.recovery_journal import ExactCommand
        handler=geo_manifest_handler(revisions=GEO_REVISIONS,check_sha256='c'*64,apply_sha256='d'*64)
        command=ExactCommand(sequence=1,operation_key='foreign-geo-before-book-stage',
            request_id='foreign-geo-request',person_id=actor,route=GEO_MANIFEST_INSTALL,
            logical_time='2026-10-24T00:00:00Z',
            schema_revision=GEO_REVISIONS.schema,function_revision=GEO_REVISIONS.function,
            executor_revision=GEO_REVISIONS.executor,
            ids={'manifest_id':'a'*64},
            expected_revisions=dict(manifest_state='staging',manifest_index_sha256='b'*64,
                installer_person_id=actor),
            payload=canonical(dict(codec='rkb.geo-manifest-command.v1',action='activate')))

    if domain!='native_stage':
        s.spool.registry=HandlerRegistry([*s.spool.registry.handlers.values(),handler])
    frozen,data,handler=s.spool._validate(command,'synthetic-db')
    assert s.spool._resource_key(command,handler.dependency)!=book.document_id
    await s.spool._write(s.spool._stage,frozen,data,handler,'synthetic-db')
    if phase!='unaccepted':
        await s.spool.seal(s.t.auth.p.admin,command.operation_key,
            predecessor=None,writer_generation='writer-1')
    if phase in ('applied','rejected_precondition','outcome_unknown'):
        raw=canonical(dict(verified_domain=domain))
        status='applied' if phase=='outcome_unknown' else phase
        receipt=ApplyReceipt('synthetic-db',command.operation_key,command.sequence,
            digest(data),status,raw,digest(raw),False)
        await s.spool.record_apply(command.operation_key,receipt)
    elif phase=='apply_unknown':
        await s.spool.mark_apply_sent(command.operation_key)
    if phase=='outcome_unknown':
        await s.spool._write(s.spool._outcome_sent,command.operation_key)

    binding=s.startup.binding
    s.startup.coverage=await s.spool.dataset_cursor(binding,control=dict(sequence=0,heads=[]),
        current=lambda candidate:candidate==binding)
    scope=await s.spool.scope_state()
    old=await s.spool.get(command.operation_key)
    assert scope['pending'] and old.outcome_state!='DURABLE' and old.remote_outcome is None
    if phase!='unaccepted':
        assert scope['accepted_sequence']==1
        assert s.startup.coverage.applied_sequence==(1 if phase in ('applied','rejected_precondition','outcome_unknown') else 0)
        assert old.accepted is not None
        assert (old.apply_receipt is None)==(phase in ('accepted','apply_unknown'))
        assert old.command_bytes==command.encode()
        result=await stage(s,book,pages)
        assert result.state=='processing'
        next_entry=await pending(s,book)
        assert next_entry.command.sequence==2 and next_entry.command.route==STAGE_ROUTE
        assert (await s.spool.get(command.operation_key)).command_bytes==command.encode()
        assert (await s.spool.get(command.operation_key)).outcome_state==('SENT_UNKNOWN' if phase=='outcome_unknown' else 'NOT_SENT')
    else:
        with pytest.raises(ToolError,match='prior_write_pending'):
            await stage(s,book,pages)
        assert operation_count(s)==1


@pytest.mark.asyncio
async def test_own_terminal_source_never_borrows_another_actor_or_ingestion(native_intake):
    s=native_intake;book=await start(s);payload=batch(s,book,count=1)
    assert (await stage(s,book,payload)).state=='processing'
    own=await pending(s,book)
    binding=s.startup.binding
    access=SimpleNamespace(binding=binding)
    native=s.importer.native_intake
    row=s.importer._row(s.principal.subject,book.ingestion_id)
    s.startup.coverage=await s.spool.dataset_cursor(binding,control=dict(sequence=0,heads=[]),
        current=lambda candidate:candidate==binding)
    scope=await s.spool.scope_state()
    assert not native._inactive_prefix_current(access,row,scope)  # unapplied, not a safe terminal
    await finish(s,book,publish=False)
    s.startup.coverage=await s.spool.dataset_cursor(binding,control=dict(sequence=0,heads=[]),
        current=lambda candidate:candidate==binding)
    scope=await s.spool.scope_state()
    assert scope['accepted_sequence']==scope['sequence']==s.startup.coverage.applied_sequence==1
    assert native._inactive_prefix_current(access,row,scope)  # unchanged owned ingestion
    foreign_actor=dict(row,person_id='00000000-0000-0000-0000-000000000099')
    other_ingestion=dict(row,ingestion_id='00000000-0000-0000-0000-000000000098')
    assert not native._inactive_prefix_current(access,foreign_actor,scope)
    assert not native._inactive_prefix_current(access,other_ingestion,scope)
    assert (await s.spool.get(own.command.operation_key)).command_bytes==own.command_bytes
    assert (await s.spool.get(own.command.operation_key)).outcome_state=='NOT_SENT'


@pytest.mark.asyncio
async def test_history_or_geo_loss_prevents_new_batch_without_rewriting_pending(native_intake):
    s = native_intake; result = await start(s); payload = batch(s,result,count=1)
    s.geo_state['current'] = False
    with pytest.raises(ToolError,match='source_history_unavailable'):
        await stage(s,result,payload)
    assert operation_count(s) == 0
    s.geo_state['current'] = True
    await stage(s,result,payload)
    entry = await pending(s,result)
    s.geo_state['current'] = False
    assert (await call(s,result,'status')).state == 'processing'
    assert (await pending(s,result)).command_bytes == entry.command_bytes and operation_count(s) == 1


@pytest.mark.asyncio
async def test_mcp_inline_stage_omits_absent_graph_and_rejects_malformed_graph(native_intake, monkeypatch):
    """MCP serialization must preserve the native schema14 optional-graph contract."""
    from starlette.requests import Request
    from pydantic import ValidationError

    from regional_knowledge.server import build_server
    from regional_knowledge.read_issuance import _SCOPE_KEY
    from regional_knowledge.source_stage import validate_stage_payload

    s = native_intake
    result = await start(s)
    payload = batch(s, result, count=1)
    monkeypatch.setattr("regional_knowledge.server._principal", lambda: s.principal)
    server = build_server(target_startup=s.startup)

    async def mcp_stage(**fields):
        request = Request({
            "type": "http", "method": "POST", "path": "/mcp", "headers": [],
            "app": SimpleNamespace(state=SimpleNamespace()), "state": {},
        })
        context = SimpleNamespace(request_context=SimpleNamespace(
            request=request, request_id="inline-graph-regression"))
        args = dict(command="stage", ingestion_id=result.ingestion_id,
            pages=payload["pages"], chunks=payload["chunks"],
            source_import=options(research=True, ingestion_id=result.ingestion_id,
                state="pending").model_dump(mode="json"), **fields)
        try:
            return await server.call_tool("book_ingest", args, context)
        finally:
            retained = request.scope.pop(_SCOPE_KEY, None)
            if retained:
                retained[1].release()

    # An explicitly malformed graph is rejected at the real typed MCP boundary.
    with pytest.raises((ToolError, ValidationError)):
        await mcp_stage(entity_candidates={"entities": "not-a-list"})
    assert operation_count(s) == 0

    # Absence means omission, not JSON null: actual native stage accepts the
    # retained model payload and source_stage validates the accepted envelope.
    await mcp_stage()
    entry = await pending(s, result)
    assert entry.command.route == STAGE_ROUTE
    stage_document = validate_stage_payload(entry.command.payload, source_id=result.document_id)
    assert "entity_candidates" not in stage_document["model_payload"]
    assert stage_document["model_payload"]["pages"] == [
        StagePageInput.model_validate(page).model_dump(mode="json", exclude_none=True)
        for page in payload["pages"]
    ]
    assert stage_document["model_payload"]["chunks"] == [
        StageChunkInput.model_validate(chunk).model_dump(mode="json", exclude_none=True)
        for chunk in payload["chunks"]
    ]
    assert operation_count(s) == 1


@pytest.mark.asyncio
async def test_status_releases_verified_upload_and_current_file_dedup_survives_other_book(native_intake):
    s=native_intake
    first=await start(s,file_id='first-upload')
    row=s.importer._row(s.principal.subject,first.ingestion_id)
    # Crash seam: receipt was persisted before its acquiring -> staging save.
    s.importer._save(row,phase='acquiring')
    initial=await call(s,first,'status')
    assert initial.source_archive_status=='verified' and initial.next_cursor is None
    assert s.importer._row(s.principal.subject,first.ingestion_id)['phase']=='staging'
    second=await start(s,file_id='second-upload')
    assert second.ingestion_id!=first.ingestion_id
    repeated=await start(s,file_id='first-upload')
    assert (repeated.ingestion_id,repeated.document_id)==(first.ingestion_id,first.document_id)
    assert s.importer._row(s.principal.subject,second.ingestion_id)['phase']=='staging'


@pytest.mark.asyncio
@pytest.mark.parametrize('component',['text','semantic'])
async def test_exact_packet_can_retry_after_terminal_rejection_only(native_intake,component):
    s=native_intake;book=await start(s);payload=batch(s,book,count=1)
    assert (await stage(s,book,payload)).state=='processing'
    await finish(s,book)
    assert (await call(s,book,'status')).state=='processing'
    if component=='semantic':
        await finish(s,book)
        assert (await call(s,book,'status')).state=='processing'
    rejected=await finish(s,book,status='rejected_precondition')
    assert (await call(s,book,'status')).state=='staged'
    assert state(s,book)['rejected_batch_index']==0
    before=operation_count(s)
    # Identical model input now asks for a real retry, not the old rejection.
    assert (await stage(s,book,payload)).state=='processing'
    retry=await pending(s,book)
    assert retry.command.route==STAGE_ROUTE
    assert retry.command.sequence==rejected.command.sequence+1
    assert retry.command.operation_key!=rejected.command.operation_key
    assert json.loads(retry.command.payload)['model_payload']==payload
    assert operation_count(s)==before+1
    old=await s.spool.get(rejected.command.operation_key)
    assert old.command_bytes==rejected.command_bytes and old.apply_state=='REJECTED_PRECONDITION'
    for _ in range(3):
        await finish(s,book)
        result=await call(s,book,'status')
    assert result.state=='staged' and state(s,book)['rejected_batch_index'] is None
    after=operation_count(s)
    assert (await stage(s,book,payload)).state=='staged'
    assert operation_count(s)==after  # Success/lost-ACK retries still deduplicate.
