"""174-page metadata shape, synthetic source text/pixels, no book/provider use.

The importer uses its real SQLite/auth/output seams. Only original acquisition
is the existing fixture seam; issued-image metadata uses the real controller.
"""
import json
from dataclasses import replace
from types import MethodType,SimpleNamespace
from uuid import UUID,uuid5

import pytest
from mcp.server.mcpserver.exceptions import ToolError

from regional_knowledge.original_intake import OriginalIntakeError,issued_image_receipt
from regional_knowledge.original_intake_controller import OriginalIntakeController
from regional_knowledge.kaggle_public_backend import current_binding
from regional_knowledge.stage_graph import StagedGraph
from test_kaggle_source_importer import intake,start,options,BBOX
from test_kaggle_public_backend import target
from test_read_authority import auth
from test_participants import people


def issuer(s,result):
    original=s.originals
    receipt=replace(original.receipts[result.ingestion_id],format='djvu',page_count=174)
    original.receipts[result.ingestion_id]=receipt
    store=s.spool.journal.store
    store.client=SimpleNamespace(meta=SimpleNamespace(endpoint_url='https://archive.invalid'))
    store.bucket='synthetic'
    controller=OriginalIntakeController(queue=s.t.queue,spool=s.spool,participants=s.t.auth.p.registry,
        object_store=SimpleNamespace(client=store.client,bucket=store.bucket),
        binding_current=lambda binding:s.startup.binding_current(binding),
        current_binding=lambda:current_binding(s.t.queue),allowed_origins=frozenset({'https://archive.invalid'}))
    async def verified(self,context,ingestion_id):
        await self._authorize(context)
        return original.receipts.get(ingestion_id)
    controller.verified=MethodType(verified,controller)
    # Native acquisition is synthetic; retain its exact owned control identity.
    with s.t.queue.connect() as db:
        db.execute('INSERT INTO rkb_original_intakes(ingestion_id,actor,operation_id,spec_json,storage_sha256) '
            'VALUES(?,?,?,?,?)',(result.ingestion_id,s.principal.subject,'synthetic','{}',controller._storage))
    original.selected_image_receipts=controller.selected_image_receipts
    async def forbidden(*args,**kwargs):
        pytest.fail('stage attempted hidden native fetch/render')
    original.native_pages=forbidden
    original.pages=forbidden
    return controller,receipt


def batch(controller,receipt,start_index,count=8,*,issue=True):
    pages=[];chunks=[]
    for index in range(start_index,min(start_index+count,receipt.page_count)):
        page_id=str(uuid5(UUID(receipt.source_id),f'page:1:{index}'))
        native=dict(page_id=page_id,physical_page_index=index,original_sha256=receipt.sha256,
            parser_sha256=receipt.parser_sha256,width=595.,height=842.,image_sha256='e'*64,
            mime_type='image/jpeg',preview_render=dict(width=724,height=1024,scale=1.216319,
                jpeg_quality=80,preview_only=True))
        # No native text exists: the source is a visually transcribed scan.
        issued=controller._record_issued_image(receipt,native) if issue else issued_image_receipt(receipt,native)
        pages.append(dict(page_id=page_id,physical_page_index=index,source_material='visual_reviewed',
            source_review_note='Synthetic model transcription from the issued image.',knowledge_reviewed=True,
            source_image_receipt_sha256=issued['source_image_receipt_sha256'],regions=[dict(region_key='body',
                kind='body',bbox=BBOX,reading_order=0,source_text=f'Model transcription of synthetic page {index}.')]))
        chunks.append(dict(chunk_key=f'page-{index}',title='Synthetic source text',
            region_refs=[dict(page_id=page_id,region_key='body')]))
    return dict(pages=pages,chunks=chunks,story_candidates=[])


@pytest.mark.asyncio
async def test_174_pages_stage_in_batches_without_native_text_or_book_fetch(intake):
    s=intake;result=await start(s);controller,receipt=issuer(s,result)
    for offset in range(0,174,8):
        payload=batch(controller,receipt,offset)
        args=dict(command='stage',principal=s.principal,ingestion_id=result.ingestion_id,
            payload=payload,source_import=options(research=True,ingestion_id=result.ingestion_id,state='pending'))
        await s.importer.book_ingest(**args)
        before=s.importer._row(s.principal.subject,result.ingestion_id)
        # Lost reply/resume: exact same batch adds no chunks or duplicate pages.
        await s.importer.book_ingest(**args)
        after=s.importer._row(s.principal.subject,result.ingestion_id)
        assert before['stage_json']==after['stage_json']
        assert before['semantic_json']==after['semantic_json']
    graph=StagedGraph.model_validate_json(after['stage_json'])
    assert len(graph.pages)==len(graph.chunks)==174
    assert graph.pages[-1].physical_page_index==173
    assert all(p.source_image_receipt_sha256 for p in graph.pages)
    assert after['native_proof_sha256'] is None
    assert not s.token_calls
    # This handoff does not relabel model text as frozen native-v1 provenance
    # or claim long-book activation. No embeddings/accepted command are made.
    with pytest.raises(ToolError,match='source_image_activation_revision_required'):
        await s.importer.book_ingest(command='validate',principal=s.principal,ingestion_id=result.ingestion_id)
    with s.t.queue.connect(write=False) as db:
        assert db.execute('SELECT count(*) FROM jobs').fetchone()[0]==0
    assert await s.importer._entry(after) is None


@pytest.mark.asyncio
@pytest.mark.parametrize('case',['unissued','forged','wrong_original','unreviewed'])
async def test_selected_stage_rejects_unissued_or_changed_provenance(intake,case):
    s=intake;result=await start(s);controller,receipt=issuer(s,result)
    payload=batch(controller,receipt,0,1,issue=case!='unissued')
    if case=='forged':payload['pages'][0]['source_image_receipt_sha256']='f'*64
    if case=='wrong_original':s.originals.receipts[result.ingestion_id]=replace(receipt,sha256='f'*64)
    if case=='unreviewed':
        payload['pages'][0].update(knowledge_reviewed=False,source_material='preview')
    with pytest.raises((ToolError,OriginalIntakeError)):
        await s.importer.book_ingest(command='stage',principal=s.principal,ingestion_id=result.ingestion_id,
            payload=payload,source_import=options(research=True))
    assert s.importer._row(s.principal.subject,result.ingestion_id)['stage_json'] is None


@pytest.mark.asyncio
async def test_envelope_remains_bounded_even_when_book_has_174_pages(intake):
    s=intake;result=await start(s);controller,receipt=issuer(s,result)
    with pytest.raises(ToolError,match='target_small_text_stage_only'):
        await s.importer.book_ingest(command='stage',principal=s.principal,ingestion_id=result.ingestion_id,
            payload=batch(controller,receipt,0,9),source_import=options(research=True))
