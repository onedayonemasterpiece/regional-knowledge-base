"""Synthetic reviewed pages only; no native-layer or fake persistence PASS."""
from copy import deepcopy
from types import SimpleNamespace as S
from uuid import UUID, uuid4, uuid5

import pytest

from regional_knowledge.model_source_provenance import build_model_source_provenance


BBOX={'left':10,'top':25,'right':650,'bottom':800}


def make_scan(page_count=3, *, with_illustration=False, source_format='pdf'):
    source_id=str(uuid4())
    receipt=S(source_id=source_id,ingestion_id=str(uuid4()),format=source_format,
              page_count=page_count,sha256='1'*64,receipt_sha256='2'*64)
    pages=[];figures=[];chunks=[]
    for i in range(page_count):
        page_id=uuid5(UUID(source_id),f'page:1:{i}')
        rid=uuid5(page_id,'region:body')
        body=S(region_key='body',region_id=rid,page_id=page_id,kind='body',bbox=BBOX,
               source_text=f'Model visually read source text on scan page {i}.')
        regions=[body];figure_ids=[]
        if with_illustration and i==0:
            fid=uuid5(page_id,'region:figure')
            cid=uuid5(page_id,'region:printed-caption')
            illustration_id=uuid5(page_id,'illustration:map')
            regions.extend([
                S(region_key='figure',region_id=fid,page_id=page_id,kind='figure',bbox=BBOX,source_text=''),
                S(region_key='printed-caption',region_id=cid,page_id=page_id,kind='caption',bbox=BBOX,
                  source_text='Map of an old fortress, printed on the original page.'),
            ])
            figure=S(illustration_key='map',illustration_id=illustration_id,page_id=page_id,source_region_id=fid,
                     caption_region_ids=[cid],visual_description='Model sees map details',
                     visual_description_provenance='model_observation',bbox=BBOX)
            figures.append(figure);figure_ids=[illustration_id]
        page=S(page_id=page_id,physical_page_index=i,source_material='visual_reviewed',
               source_review_note='Reviewed the actual attached source scan in the AI client',regions=regions)
        chunk=S(chunk_id=uuid5(page_id,'chunk:body'),text=body.source_text,
                page_ids=[page_id],region_ids=[rid],illustration_ids=figure_ids)
        pages.append(page);chunks.append(chunk)
    return receipt,S(revision=1,pages=pages,illustrations=figures,chunks=chunks)


@pytest.mark.parametrize('format', ['pdf','djvu'])
def test_image_only_scans_are_model_source_evidence_not_claimed_native_text(format):
    receipt,graph=make_scan(with_illustration=True,source_format=format)
    manifest, digest=build_model_source_provenance(receipt=receipt,graph=graph)
    assert manifest['codec']=='rkb.source-model-provenance.v1'
    assert manifest['format']==format
    assert all(p['observation_basis']=='model_client_visual_review' for p in manifest['pages'])
    assert all(r['transcription_basis']=='model_visual_review' for r in manifest['regions'])
    assert len(manifest['illustrations'])==1
    assert manifest['chunks'][0]['illustration_ids']==[str(graph.illustrations[0].illustration_id)]
    assert manifest['illustrations'][0]['visual_description_provenance']=='model_observation'
    assert 'native_block_index' not in manifest['regions'][0]
    assert 'native_text' not in manifest
    assert len(digest)==64
    repeated=build_model_source_provenance(receipt=receipt,graph=graph)
    assert repeated==(manifest,digest)


def test_realistic_139_page_long_djvu_keeps_full_page_and_chunk_inventory():
    receipt,graph=make_scan(139,source_format='djvu')
    manifest,digest=build_model_source_provenance(receipt=receipt,graph=graph)
    assert len(manifest['pages'])==139
    assert len(manifest['chunks'])==139
    assert len(manifest['regions'])==139
    assert manifest['pages'][138]['physical_page_index']==138
    assert manifest['chunks'][138]['source_body_sha256']


@pytest.mark.parametrize('corrupt',[
    'missing_page','wrong_page_id','wrong_page_index','no_model_review',
    'source_region_conflict','empty_region','invalid_bbox','figure_wrong_page',
    'caption_wrong_kind','unknown_figure','missing_source_region',
    'wrong_format','wrong_source_sha','invented_chunk','wrong_figure_id','wrong_region_id',
])
def test_model_stage_cannot_invent_identity_or_image_associations(corrupt):
    receipt,graph=make_scan(with_illustration=True)
    handlers={
        'missing_page':lambda:graph.pages.pop(),
        'wrong_page_id':lambda:setattr(graph.pages[0],'page_id',uuid4()),
        'wrong_page_index':lambda:setattr(graph.pages[0],'physical_page_index',2),
        'no_model_review':lambda:setattr(graph.pages[0],'source_material','unreviewed'),
        'source_region_conflict':lambda:setattr(graph.pages[0].regions[0],'page_id',uuid4()),
        'empty_region':lambda:setattr(graph.pages[0].regions[0],'source_text',''),
        'invalid_bbox':lambda:setattr(graph.pages[0].regions[0],'bbox',{'left':-1,'top':0,'right':900,'bottom':900}),
        'figure_wrong_page':lambda:setattr(graph.illustrations[0],'page_id',uuid4()),
        'caption_wrong_kind':lambda:setattr(graph.pages[0].regions[2],'kind','body'),
        'unknown_figure':lambda:setattr(graph.chunks[0],'illustration_ids',[uuid4()]),
        'missing_source_region':lambda:setattr(graph.chunks[0],'region_ids',[uuid4()]),
        'wrong_format':lambda:setattr(receipt,'format','html'),
        'wrong_source_sha':lambda:setattr(receipt,'sha256','NOT_A_SHA256'),
        'invented_chunk':lambda:setattr(graph.chunks[0],'text','invented text'),
        'wrong_figure_id':lambda:setattr(graph.illustrations[0],'illustration_id',uuid4()),
        'wrong_region_id':lambda:setattr(graph.pages[0].regions[0],'region_id',uuid4()),
    }
    handlers[corrupt]()
    with pytest.raises(ValueError):
        build_model_source_provenance(receipt=receipt,graph=graph)


def test_repeated_same_stage_is_immutable_and_no_generated_new_ids():
    receipt, graph=make_scan(8,with_illustration=True)
    first=build_model_source_provenance(receipt=receipt,graph=graph)
    assert build_model_source_provenance(receipt=receipt,graph=deepcopy(graph))==first
    graph.pages[0].regions[0].source_text+=' CORRECTED'
    with pytest.raises(ValueError, match='chunk text differs'):
        build_model_source_provenance(receipt=receipt,graph=graph)
    graph.chunks[0].text+=' CORRECTED'
    second=build_model_source_provenance(receipt=receipt,graph=graph)
    assert first[1]!=second[1]


def test_invalid_page_count_rejected_before_full_book_scan():
    receipt,graph=make_scan(1)
    receipt.page_count=2501
    with pytest.raises(ValueError):build_model_source_provenance(receipt=receipt,graph=graph)


def test_actual_image_only_pdf_has_no_native_layer_and_does_not_block_model_provenance(tmp_path):
    """Actual PDF binary from an embedded raster, not a fake native-text flag."""
    import hashlib
    import io
    fitz=pytest.importorskip('pymupdf')
    from PIL import Image, ImageDraw
    raster=Image.new('RGB',(440,230),'white')
    ImageDraw.Draw(raster).text((24,90),'Synthetic fortress map (raster only)',fill='black')
    png=io.BytesIO();raster.save(png,format='PNG')
    path=tmp_path/'model-read-scan.pdf'
    with fitz.open() as doc:
        page=doc.new_page(width=440,height=230)
        page.insert_image(page.rect,stream=png.getvalue())
        doc.save(path)
    with fitz.open(path) as doc:
        assert doc.page_count==1
        assert doc[0].get_text('text').strip()==''
        assert len(doc[0].get_images())==1
    receipt,graph=make_scan(1,with_illustration=True)
    receipt.sha256=hashlib.sha256(path.read_bytes()).hexdigest()
    # The region text is the caller-model's statement, not extracted from PDF.
    result,_=build_model_source_provenance(receipt=receipt,graph=graph)
    assert result['pages'][0]['original_sha256']==receipt.sha256
    assert result['regions'][0]['transcription_basis']=='model_visual_review'
    assert result['illustrations'][0]['source_region_id']
