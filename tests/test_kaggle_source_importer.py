"""Real owner auth, control queue and MCP contracts; native original/model are synthetic."""
import asyncio
import hashlib
from dataclasses import replace
import json
from types import MethodType, SimpleNamespace
from uuid import UUID, uuid5

import pytest
import pytest_asyncio
from mcp.server.mcpserver.exceptions import ToolError
from pydantic import ValidationError

from regional_knowledge.backend import RenderedPage, RenderedPageBatch
from regional_knowledge.bge_contract import SPACE
from regional_knowledge.contracts import ChatFile, Principal
from regional_knowledge.control_spool import ControlSpool
from regional_knowledge.kaggle_apply import HandlerRegistry
from regional_knowledge.kaggle_import_contracts import TargetSourceImportInput, PublicImportRights
from regional_knowledge.kaggle_import_tokenizer import HfImportTokenCounter
from regional_knowledge.kaggle_service_startup import TargetServiceConfig, TargetServiceStartup
from regional_knowledge.original_intake import NativePagesProof, OriginalIntakeStatus, VerifiedOriginalReceipt
from regional_knowledge.original_intake_controller import OriginalIntakeController
from regional_knowledge.read_issuance import capture_read_output
from regional_knowledge.recovery_journal import RecoveryJournal
from regional_knowledge.source_import_v2 import STORY_REVISIONS, source_import_v2_handler
from regional_knowledge.source_import_v3 import FACT_REVISIONS, source_import_v3_handler
from regional_knowledge.source_import_v4 import source_import_v4_handler
from regional_knowledge.story_editorial import EDITORIAL_REVISIONS
from regional_knowledge.kaggle_domain_bindings import DOMAIN_REVISIONS,current_handler
from regional_knowledge.original_intake import canonical
from test_kaggle_public_backend import target
from test_kaggle_service_startup import HistoryAdapter
from test_participants import people
from test_read_authority import auth, issue
from test_recovery_journal import ConditionalMemoryStore

TEXT = 'Exact synthetic harbour source evidence.'
BBOX = dict(left=10, top=20, right=900, bottom=100)


def options(*, research=False, ingestion_id=None, state='complete'):
    return TargetSourceImportInput.model_validate({
        'research': dict(state=state, continuation=({'tool':'book_pages', 'arguments':{'ingestion_id':ingestion_id}}
            if state=='pending' else {}))} if research else {
        'public_rights': dict(rights_status='licensed', evidence={'public_distribution':True, 'license':'synthetic-test'},
            policy_version='synthetic-rights-v1')})


@pytest_asyncio.fixture
async def intake(target, tmp_path, request):
    t=target
    versions={7:STORY_REVISIONS,8:FACT_REVISIONS,9:EDITORIAL_REVISIONS,11:DOMAIN_REVISIONS}[getattr(request,'param',7)]
    with t.queue.connect() as db:
        db.execute('DELETE FROM jobs')
        row=db.execute('SELECT expected_json,observed_json FROM bge_runtime_expectations WHERE run_id=?',(t.run,)).fetchone()
        # Exact future installed revisions; the hash/catalog observations remain
        # synthetic as in the existing target fixture.
        revision_values=dict(zip(('schema_revision','function_revision','executor_revision'),versions.wire()))
        values=[]
        for raw in row:
            value=json.loads(raw);value.update(revision_values);values.append(json.dumps(value))
        db.execute('UPDATE bge_runtime_expectations SET expected_json=?,observed_json=? WHERE run_id=?',(*values,t.run))
    if versions==DOMAIN_REVISIONS:
        handler=current_handler('source.import.v4',check_sha256='a'*64,apply_sha256='b'*64)
    elif versions==EDITORIAL_REVISIONS:
        handler=source_import_v4_handler(check_sha256='a'*64,apply_sha256='b'*64)
    else:
        builder=source_import_v3_handler if versions==FACT_REVISIONS else source_import_v2_handler
        handler=builder(revisions=versions,check_sha256='a'*64,apply_sha256='b'*64)
    spool=ControlSpool(t.queue,t.auth.p.registry,RecoveryJournal(ConditionalMemoryStore(),stream='synthetic-intake'),HandlerRegistry([handler]))
    originals=object.__new__(OriginalIntakeController)
    originals.queue,originals.spool,originals.participants=t.queue,spool,t.auth.p.registry
    originals.calls=[];originals.receipts={};originals.proofs={}
    async def begin(self, context, *, operation_id, source_id, ingestion_id, file, max_bytes):
        self.calls.append(('begin', ingestion_id, str(file.download_url)))
        receipt=VerifiedOriginalReceipt(source_id,ingestion_id,str(uuid5(UUID(ingestion_id),'original:'+operation_id)),
            1,'1'*64,401090,'originals/synthetic/'+source_id,'"synthetic-etag"','pdf',10,'2'*64,'3'*64,'4'*64)
        self.receipts[ingestion_id]=receipt
        pages=[]
        for index in range(10):
            block=dict(block_index=0,bbox=BBOX,text=TEXT,original_length=len(TEXT),truncated=False,
                text_sha256=hashlib.sha256(TEXT.encode()).hexdigest())
            page=dict(format='pdf',page_count=10,page_id=str(uuid5(UUID(source_id),f'page:1:{index}')),physical_page_index=index,
                original_sha256=receipt.sha256,parser_sha256=receipt.parser_sha256,width=595.,height=842.,
                image_size=len(b'synthetic-jpeg'),image_sha256=hashlib.sha256(b'synthetic-jpeg').hexdigest(),mime_type='image/jpeg',
                native_text=TEXT,native_text_info=dict(complete=True,truncated=False,blocks_truncated=False,
                    native_text_sha256=hashlib.sha256(TEXT.encode()).hexdigest(),native_blocks_sha256=hashlib.sha256(
                        canonical([{k:block[k] for k in ('block_index','bbox','text')}])).hexdigest()),
                native_blocks=[block],preview_render=dict(policy='rkb.pdf-preview.v1',width=724,height=1024,
                    scale=1.216319,max_edge=1024,jpeg_quality=80,byte_limit=256*1024,preview_only=True))
            page['page_receipt_sha256']=hashlib.sha256(canonical(page)).hexdigest();pages.append(page)
        self.proofs[ingestion_id]=NativePagesProof(receipt.receipt_sha256,
            tuple(json.dumps(page,sort_keys=True).encode() for page in pages))
        return OriginalIntakeStatus(ingestion_id,'verified',receipt)
    async def verified(self, context, ingestion_id):return self.receipts.get(ingestion_id)
    async def status(self, context, ingestion_id):return OriginalIntakeStatus(ingestion_id,'verified',self.receipts[ingestion_id])
    async def native_pages(self, context, ingestion_id):return self.proofs[ingestion_id]
    async def pages(self, context, ingestion_id, cursor=None, batch_size=1):
        index=int(cursor or 0);receipt=self.receipts[ingestion_id]
        return RenderedPageBatch((RenderedPage(str(uuid5(UUID(receipt.source_id),f'page:1:{index}')),index,None,
            'image/jpeg',b'synthetic-jpeg',TEXT,(),{}),),str(index+1) if index<9 else None)
    for fn in (begin,verified,status,native_pages,pages):setattr(originals,fn.__name__,MethodType(fn,originals))
    tokenizer=HfImportTokenCounter(tmp_path/'unloaded-tokenizer')
    token_calls=[]
    async def counts(texts):token_calls.append(tuple(texts));return [min(513,len(text.split())+2) for text in texts]
    tokenizer.counts=counts
    startup=TargetServiceStartup(TargetServiceConfig(t.auth.provider,t.auth.p.registry,t.queue,spool,
        HistoryAdapter(),'writer-1',lambda **binding: binding['database_generation']=='synthetic-db',
        import_originals=originals,import_tokenizer=tokenizer,import_parser_sha256='2'*64,import_launch_config_sha256='a'*64))
    tokens=await issue(t.auth.provider)
    principal=Principal(subject=t.auth.p.owner.subject,client_id=t.auth.provider.client.client_id,
        issuer=t.auth.provider.issuer,access_token=tokens.access_token)
    s=SimpleNamespace(t=t,startup=startup,spool=spool,originals=originals,tokenizer=tokenizer,principal=principal,
        importer=startup.importer,token_calls=token_calls)
    yield s
    await startup.source_dispatch.aclose();spool.close()


async def start(s,metadata=None):
    return await s.importer.book_ingest(command='start', principal=s.principal,
        file=ChatFile(file_id='synthetic-pdf',download_url='https://files.example.test/private.pdf?ticket=not-logged',
            mime_type='application/pdf',file_name='synthetic.pdf'), payload=metadata or {'title':'Synthetic source','language':'simple'},
        source_import=options())


def stage_payload(s, result, *, text=TEXT):
    pages=[];chunks=[]
    for native in s.originals.proofs[result.ingestion_id].pages:
        page_id=native['page_id'];index=native['physical_page_index']
        pages.append(dict(page_id=page_id,physical_page_index=index,source_material='visual_reviewed',
            knowledge_reviewed=True,source_review_note='Synthetic model review',regions=[dict(region_key='body',
                kind='body',bbox=BBOX,reading_order=0,source_text=text)]))
        chunks.append(dict(chunk_key='text-'+str(index),title='Source text',region_refs=[dict(page_id=page_id,region_key='body')]))
    return dict(pages=pages,chunks=chunks,story_candidates=[])


async def staged(s,metadata=None):
    result=await start(s,metadata)
    payload=stage_payload(s,result)
    for begin in (0,8):
        await s.importer.book_ingest(command='stage',principal=s.principal,ingestion_id=result.ingestion_id,
            payload={**payload,'pages':payload['pages'][begin:begin+8],'chunks':payload['chunks'][begin:begin+8]},
            source_import=options(research=True,ingestion_id=result.ingestion_id))
    return result


def test_public_rights_requires_existing_verified_distribution_policy():
    with pytest.raises(ValidationError):
        PublicImportRights(rights_status='unknown',evidence={'public_distribution':True},policy_version='x')
    with pytest.raises(ValidationError):
        PublicImportRights(rights_status='licensed',evidence={},policy_version='x')
    assert options().public_rights.rights_status=='licensed'


@pytest.mark.asyncio
@pytest.mark.parametrize('intake',[7,8,9,11],indirect=True)
async def test_owner_only_ten_page_stage_is_provenance_bound_and_batched(intake):
    s=intake;result=await staged(s)
    ready=await s.importer.book_ingest(command='validate',principal=s.principal,ingestion_id=result.ingestion_id)
    assert ready.state=='ready' and ready.next_action=='finalize'
    assert s.token_calls == [(TEXT,)*10]
    assert len(s.originals.calls)==1
    assert s.spool.queue is s.t.queue is s.originals.queue
    with s.t.queue.connect(write=False) as db:
        assert db.execute('SELECT count(*) FROM jobs').fetchone()[0]==0
    assert await s.importer.owned_row(s.principal,result.ingestion_id)==dict(id=result.ingestion_id,
        owner_user_id=s.principal.subject,document_id=result.document_id,staged_revision=1)


@pytest.mark.asyncio
@pytest.mark.parametrize('case',['invented_text','changed_bbox','truncated','wrong_original','wrong_receipt'])
async def test_unproven_native_material_never_stages_or_enqueues(intake,case):
    s=intake;result=await start(s);payload=stage_payload(s,result)
    payload={**payload,'pages':payload['pages'][:8],'chunks':payload['chunks'][:8]}
    if case=='invented_text':payload['pages'][0]['regions'][0]['source_text']='Invented private source text'
    if case=='changed_bbox':payload['pages'][0]['regions'][0]['bbox']={**BBOX,'left':11}
    if case=='wrong_receipt':
        s.originals.receipts[result.ingestion_id]=replace(s.originals.receipts[result.ingestion_id],request_id=str(uuid5(UUID(result.ingestion_id),'wrong')))
    if case in ('truncated','wrong_original'):
        proof=s.originals.proofs[result.ingestion_id];pages=list(proof.pages)
        if case=='truncated':pages[0]['native_blocks'][0]['truncated']=True
        else:pages[0]['original_sha256']='f'*64
        s.originals.proofs[result.ingestion_id]=NativePagesProof(proof.original_receipt_sha256,
            tuple(json.dumps(page).encode() for page in pages))
    with pytest.raises(ToolError):
        await s.importer.book_ingest(command='stage',principal=s.principal,ingestion_id=result.ingestion_id,
            payload=payload,source_import=options(research=True))
    assert not s.token_calls
    assert s.importer._row(s.principal.subject,result.ingestion_id)['stage_json'] is None


@pytest.mark.asyncio
async def test_personal_nonowner_and_html_are_rejected_before_native_acquire(intake):
    s=intake
    other=Principal(subject=s.t.auth.p.other.subject,client_id=s.principal.client_id,issuer=s.principal.issuer,
        access_token=s.t.auth.tokens.access_token)
    with pytest.raises(ToolError,match='access_denied'):
        await s.importer.book_ingest(command='start',principal=other,source_import=options())
    with pytest.raises(ToolError,match='target_pdf_only'):
        await s.importer.book_ingest(command='start',principal=s.principal,
            file=ChatFile(file_id='html',download_url='https://example.test/article',mime_type='text/html'),source_import=options())
    assert not s.originals.calls


@pytest.mark.asyncio
async def test_revoked_oauth_withholds_private_preview_at_final_output(intake):
    s=intake;result=await start(s)
    with capture_read_output() as calls:
        batch=await s.importer.book_pages(principal=s.principal,ingestion_id=result.ingestion_id)
        assert batch.pages[0].data==b'synthetic-jpeg'
    assert len(calls)==1
    retained=calls[0]
    assert retained.gate.issuance_current(retained.gated.ticket)
    await s.t.auth.provider.revoke_token(await s.t.auth.provider.load_access_token(s.principal.access_token))
    assert not retained.gate.issuance_current(retained.gated.ticket)
    with pytest.raises(PermissionError):await retained.gate.refresh(retained.gated)
    retained.release()
    assert not s.importer.output_gate._tickets


@pytest.mark.asyncio
async def test_embeddings_bind_exact_native_receipt_graph_and_document_text(intake):
    s=intake;result=await staged(s)
    await s.importer.book_ingest(command='validate',principal=s.principal,ingestion_id=result.ingestion_id)
    pending=await s.importer.book_ingest(command='finalize',principal=s.principal,ingestion_id=result.ingestion_id)
    assert pending.state=='processing'
    with s.t.queue.connect(write=False) as db:
        jobs=list(db.execute('SELECT * FROM jobs ORDER BY id'))
    assert len(jobs)==10 and all(job['kind']=='document' for job in jobs)
    receipt=s.originals.receipts[result.ingestion_id]
    for job in jobs:
        identity=json.loads(job['identity'])
        assert identity['original_receipt_sha256']==receipt.receipt_sha256
        assert identity['parser_sha256']==receipt.parser_sha256
        assert json.loads(job['texts'])==[TEXT]
    assert await s.spool.scope_state()==dict(sequence=0,accepted_sequence=0,operation_count=0,pending_count=0,
        unknown_count=0,pending=False,unknown=False,history_complete=False,scope_complete=False,scope_class='public_source')


@pytest.mark.asyncio
@pytest.mark.parametrize('nested',[False,True])
async def test_existing_mcp_fileparams_and_stage_json_use_target_owned_workflow(intake,monkeypatch,tmp_path,nested):
    from starlette.requests import Request
    from regional_knowledge.server import build_server
    from regional_knowledge.read_issuance import _SCOPE_KEY
    from test_stage_file import Downloader
    s=intake
    monkeypatch.setattr('regional_knowledge.server._principal',lambda:s.principal)
    monkeypatch.setenv('RKB_WORK_DIR',str(tmp_path/'stage-work'))
    server=build_server(target_startup=s.startup)
    tools={tool.name:tool for tool in server._tool_manager.list_tools()}
    assert tools['book_ingest'].meta['openai/fileParams']==['file']
    assert 'source_import' in tools['book_ingest'].parameters['properties']
    async def call(name, arguments):
        request=Request({'type':'http','method':'POST','path':'/mcp','headers':[],
            'app':SimpleNamespace(state=SimpleNamespace()),'state':{}})
        context=SimpleNamespace(request_context=SimpleNamespace(request=request,request_id='source-intake-mcp'))
        try:return await server.call_tool(name,arguments,context)
        finally:
            retained=request.scope.pop(_SCOPE_KEY,None)
            if retained:retained[1].release()
    arguments=dict(command='start',file=dict(file_id='synthetic-pdf',
        download_url='https://files.example.test/private.pdf',mime_type='application/pdf'),
        metadata={'title':'Synthetic source','language':'en'},source_import=options().model_dump(mode='json'))
    if nested:arguments['metadata']['source_import']=arguments.pop('source_import')
    await call('book_ingest',arguments)
    with s.t.queue.connect(write=False) as db:
        row=dict(db.execute('SELECT * FROM rkb_source_intake_control').fetchone())
    assert json.loads(row['metadata_json'])['language']=='en'
    assert 'source_import' not in json.loads(row['metadata_json'])
    result=SimpleNamespace(ingestion_id=row['ingestion_id'],document_id=row['source_id'])
    payload=stage_payload(s,result)
    stage=dict(ingestion_id=result.ingestion_id,source_revision=1,pages=payload['pages'][:8],chunks=payload['chunks'][:8])
    downloader=Downloader(json.dumps(stage).encode())
    s.startup.backend.file_downloader=downloader
    await call('book_ingest',dict(command='stage',ingestion_id=result.ingestion_id,
        file=dict(file_id='stage-json',download_url='https://files.example.test/stage.json',mime_type='application/json'),
        source_import=options(research=True,ingestion_id=result.ingestion_id,state='pending').model_dump(mode='json')))
    assert downloader.calls and not downloader.path.exists()
    stored=s.importer._row(s.principal.subject,result.ingestion_id)
    assert len(json.loads(stored['stage_json'])['pages'])==8
    await call('book_pages',dict(ingestion_id=result.ingestion_id,batch_size=4))
    assert not s.importer.output_gate._tickets
    assert len(s.originals.calls)==1


async def finish_document(s):
    a=s.startup.admission;t=s.t
    work=await a.next_channel_work(t.run,t.token,t.generation)
    assert work is not None and work.document_payload['kind']=='document'
    a.mark_work_sent(work);job=work.document_payload
    status=t.queue.complete(t.run,t.token,job['id'],job['claim'],SPACE,[[1.]+[0.]*1023],{},connection_generation=t.generation)
    a.durable_result_settled(job['id'],job['claim'],status)
    r=work.resource
    await a.resource_released(t.run,t.token,t.generation,runtime_incarnation=r.runtime_incarnation,
        resource_kind=r.resource_kind,resource_id=r.resource_id,compute_claim=r.compute_claim,
        dispatch_generation=r.dispatch_generation)
    return work


@pytest.mark.asyncio
@pytest.mark.parametrize('intake',[7,8],indirect=True)
async def test_source_and_authored_story_activate_only_after_exact_remote_outcome(intake):
    import base64
    import hashlib
    from regional_knowledge.kaggle_apply import ApplyReceipt
    from regional_knowledge.kaggle_source_commands import canonical,receipt_wire
    s=intake;result=await staged(s,dict(title='Synthetic source',authors=['A; B'],language='en'))
    first=s.originals.proofs[result.ingestion_id].pages[0]
    body='An independently authored short narrative with exact source evidence.'
    candidate=dict(candidate_key='harbour-finding',title='A harbour episode',summary='A separate editorial summary.',
        proposition=TEXT,evidence_refs=[dict(page_id=first['page_id'],region_key='body',original_excerpt=TEXT)],
        basic_variant=dict(body=body,language='en',blocks=[dict(start=0,end=len(body),evidence_indexes=[0])]))
    await s.importer.book_ingest(command='stage',principal=s.principal,ingestion_id=result.ingestion_id,
        payload={'story_candidates':[candidate]},source_import=options(research=True,state='pending',ingestion_id=result.ingestion_id))
    await s.importer.book_ingest(command='validate',principal=s.principal,ingestion_id=result.ingestion_id)
    waiting=await s.importer.book_ingest(command='finalize',principal=s.principal,ingestion_id=result.ingestion_id)
    assert waiting.state=='processing'
    for _ in range(11):await finish_document(s)
    command_pending=await s.importer.book_ingest(command='status',principal=s.principal,ingestion_id=result.ingestion_id)
    assert command_pending.state=='processing' and command_pending.next_action=='wait'
    entry=await s.spool.get('source-import:'+result.ingestion_id)
    assert entry.accepted is not None and entry.outcome_state!='DURABLE'
    value=json.loads(entry.command.payload);story=value['semantic']['stories'][0]
    assert story['default_variant']['body']==body and story['summary']!=body
    assert story['default_variant']['embedding']==[1.]+[0.]*1023
    assert value['original']==s.originals.receipts[result.ingestion_id].original
    expected_route='source.import.v4' if s.importer.import_revisions in (EDITORIAL_REVISIONS,DOMAIN_REVISIONS) else 'source.import.v3' if s.importer.import_revisions==FACT_REVISIONS else 'source.import.v2'
    assert entry.command.route==expected_route
    if expected_route=='source.import.v4':
        assert len(value['source_manifest']['pages'])==10 and len(value['source_manifest']['passages'])==10
        assert value['source_manifest']['metadata']['title']=='Synthetic source'
        assert value['source_manifest']['metadata']['language']=='en'
        assert value['source_manifest']['metadata']['authors']==['A; B']
    assert {p['language'] for p in value['passages']}=={'simple'}
    assert 'https://' not in entry.command.payload.decode() and 'download_url' not in entry.command.encode().decode()
    assert len(s.originals.calls)==1
    a=s.startup.admission;t=s.t
    work=await a.next_channel_work(t.run,t.token,t.generation)
    assert work.document_payload['kind']=='source_command'
    a.mark_work_sent(work);frame=dict(work.document_payload)
    command=entry.command;handler=s.spool.registry.resolve(command)
    outcome=canonical(dict(source_id=result.document_id,accepted=True))
    outcome=canonical(dict(codec='rkb.applied-outcome.v1',operation_key=command.operation_key,
        command_sha256=entry.command_sha256,cursor=command.sequence,status='applied',ids=dict(command.ids),
        request_id=command.request_id,person_id=command.person_id,route=command.route,logical_time=command.logical_time,
        before=list(s.importer.import_revisions.wire()),after=list(s.importer.import_revisions.wire()),handler_sha256=handler.fingerprint,
        result_b64=base64.b64encode(outcome).decode(),result_sha256=hashlib.sha256(outcome).hexdigest()))
    native=ApplyReceipt('synthetic-db',command.operation_key,command.sequence,entry.command_sha256,
        'applied',outcome,hashlib.sha256(outcome).hexdigest(),False)
    r=work.resource
    await a.resource_released(t.run,t.token,t.generation,runtime_incarnation=r.runtime_incarnation,
        resource_kind=r.resource_kind,resource_id=r.resource_id,compute_claim=r.compute_claim,dispatch_generation=r.dispatch_generation)
    entered,finish=asyncio.Event(),asyncio.Event();publish=s.spool.publish_outcome
    async def blocked(*args,**kwargs):entered.set();await finish.wait();return await publish(*args,**kwargs)
    s.spool.publish_outcome=blocked
    response=dict(type='SOURCE_COMMAND_RESULT',protocol_version=2,connection_generation=t.generation,
        job_id=frame['id'],claim=frame['claim'],receipt=receipt_wire(native),
        run_id=t.run,launch_nonce=t.nonce,runtime_incarnation='incarnation-a',
        compute_claim=work.compute_claim,dispatch_generation=work.dispatch_generation)
    assert await s.startup.source_dispatch.receive(response,run_id=t.run,token=t.token,generation=t.generation) is None
    await asyncio.wait_for(entered.wait(),2)
    not_durable=await s.importer.book_ingest(command='status',principal=s.principal,ingestion_id=result.ingestion_id)
    assert not_durable.state=='processing'
    finish.set()
    for _ in range(100):
        ack=await s.startup.source_dispatch.poll_settlement(run_id=t.run,token=t.token,generation=t.generation)
        if ack:break
        await asyncio.sleep(.01)
    assert ack['status']=='accepted'
    final=await s.importer.book_ingest(command='status',principal=s.principal,ingestion_id=result.ingestion_id)
    assert final.state=='finalized' and 'model_authored_research_pending' in final.warnings
    assert final.knowledge_extraction.retrieval_acceptance=='not_evaluated'
    assert final.knowledge_extraction.pending_candidates[0]['continuation']==value['research']['continuation']
    repeated=await s.importer.book_ingest(command='finalize',principal=s.principal,ingestion_id=result.ingestion_id)
    assert repeated==final and (await s.spool.get(command.operation_key)).command.encode()==command.encode()
    assert len(s.originals.calls)==1
    with t.queue.connect(write=False) as db:
        assert db.execute('SELECT count(*) FROM jobs').fetchone()[0]==12
    assert not s.importer.output_gate._tickets


@pytest.mark.asyncio
async def test_copied_rights_model_and_wrong_launch_pin_cannot_acquire_original(intake):
    s=intake
    forged=options().model_copy(update={'public_rights':options().public_rights.model_copy(update={'rights_status':'unknown'})})
    with pytest.raises(ToolError,match='invalid_small_source_input'):
        await s.importer.book_ingest(command='start',principal=s.principal,source_import=forged)
    s.importer.launch_config_sha256='f'*64
    with pytest.raises(ToolError,match='target_runtime_changed'):
        await start(s)
    assert not s.originals.calls


@pytest.mark.asyncio
async def test_native_terminal_validation_and_attachment_retry_are_reachable(intake):
    s=intake;result=await start(s)
    s.originals.receipts.clear()
    async def rejected(self,context,ingestion_id):
        return OriginalIntakeStatus(ingestion_id,'rejected',reason='original_page_count_limit')
    s.originals.status=MethodType(rejected,s.originals)
    output=await s.importer.book_ingest(command='status',principal=s.principal,ingestion_id=result.ingestion_id)
    assert output.state=='failed' and output.next_action=='blocker' and output.warnings==['original_page_count_limit']
    async def retry(self,context,ingestion_id):
        return OriginalIntakeStatus(ingestion_id,'pending',reason='original_attachment_retry_same_file_required')
    s.originals.status=MethodType(retry,s.originals)
    output=await s.importer.book_ingest(command='status',principal=s.principal,ingestion_id=result.ingestion_id)
    assert output.next_action=='blocker' and 'same file_id' in output.message
    assert output.source_archive_status=='pending'


@pytest.mark.asyncio
async def test_finalization_rechecks_native_proof_and_missing_tokenizer_is_unavailable(intake):
    s=intake;result=await staged(s)
    async def unavailable(_):raise FileNotFoundError('/private/operator/tokenizer-path')
    count=s.tokenizer.counts;s.tokenizer.counts=unavailable
    with pytest.raises(ToolError,match='pinned_tokenizer_unavailable') as error:
        await s.importer.book_ingest(command='validate',principal=s.principal,ingestion_id=result.ingestion_id)
    assert 'private/operator' not in str(error.value)
    s.tokenizer.counts=count
    await s.importer.book_ingest(command='validate',principal=s.principal,ingestion_id=result.ingestion_id)
    proof=s.originals.proofs[result.ingestion_id];pages=list(proof.pages)
    pages[0]['page_receipt_sha256']='f'*64
    s.originals.proofs[result.ingestion_id]=NativePagesProof(proof.original_receipt_sha256,
        tuple(json.dumps(page).encode() for page in pages))
    with pytest.raises(ToolError,match='native_original_provenance_changed'):
        await s.importer.book_ingest(command='finalize',principal=s.principal,ingestion_id=result.ingestion_id)
    with s.t.queue.connect(write=False) as db:
        assert db.execute('SELECT count(*) FROM jobs').fetchone()[0]==0


@pytest.mark.asyncio
async def test_same_attachment_retry_preserves_validation_and_document_job_identities(intake):
    s=intake;result=await staged(s)
    await s.importer.book_ingest(command='validate',principal=s.principal,ingestion_id=result.ingestion_id)
    duplicate=await start(s)
    assert duplicate.ingestion_id==result.ingestion_id and duplicate.state=='ready' and duplicate.next_action=='finalize'
    status=await s.importer.book_ingest(command='status',principal=s.principal,ingestion_id=result.ingestion_id)
    assert status.state=='ready'
    await s.importer.book_ingest(command='finalize',principal=s.principal,ingestion_id=result.ingestion_id)
    with s.t.queue.connect(write=False) as db:before=[row[0] for row in db.execute('SELECT id FROM jobs ORDER BY id')]
    assert (await start(s)).state=='processing'
    with s.t.queue.connect(write=False) as db:after=[row[0] for row in db.execute('SELECT id FROM jobs ORDER BY id')]
    assert before==after and len(after)==10
