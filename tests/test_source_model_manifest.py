"""Model-led scan manifest v2, no PostgreSQL/Kaggle side-effect claims."""
from copy import deepcopy
import hashlib
from uuid import uuid4
import pytest

from regional_knowledge.source_model_manifest import (
    CODEC, assemble_model_manifest, build_model_manifest, verify_model_manifest,
)
from regional_knowledge.search_material import iter_document_inputs
from test_model_source_provenance import make_scan

META = dict(title='Synthetic archive book', authors=['Synthetic Author'],
            publication_year=1885, language='de', catalog={'kind': 'book'})


async def inputs(graph, count=lambda texts: [17]*len(texts)):
    async def actual(values):
        return count(values)
    return [x async for x in iter_document_inputs(graph, graph.chunks, actual)]


@pytest.mark.asyncio
async def test_modelled_image_only_scan_manifest_preserves_exact_source_but_no_native_gate():
    original, graph = make_scan(2, with_illustration=True)
    rows=await inputs(graph)
    m=assemble_model_manifest(original=original, graph=graph,
                              metadata=META, passage_inputs=rows)
    assert m['codec']==CODEC
    assert m['source_id']==original.source_id
    assert len(m['pages'])==2 and len(m['passages'])==2
    assert m['regions'][0]['transcription_basis']=='model_visual_review'
    assert not any('native_block_index' in region for region in m['regions'])
    assert m['illustrations'][0]['visual_description_provenance']=='model_observation'
    assert m['passages'][0]['illustration_ids']==[str(graph.illustrations[0].illustration_id)]
    digest=hashlib.sha256(__import__('json').dumps(m,sort_keys=True,separators=(',', ':'),ensure_ascii=False,allow_nan=False).encode()).hexdigest()
    assert verify_model_manifest(m,original=original,graph=graph,metadata=META,passage_inputs=rows,sha256=digest)
    assert assemble_model_manifest(original=original,graph=deepcopy(graph),metadata=META,passage_inputs=rows)==m


@pytest.mark.asyncio
async def test_139_page_djvu_and_512_token_manifest_is_not_pilot_32_page_cap():
    original, graph=make_scan(139,source_format='djvu')
    calls=[]
    async def counter(texts):
        calls.append(len(texts))
        return [512]*len(texts)
    m,digest=await build_model_manifest(original=original,graph=graph,
                                       metadata=META,tokenizer_counts=counter)
    assert len(m['pages'])==139 and len(m['passages'])==139
    assert [len(x) for x in (m['pages'],m['passages'])]==[139,139]
    assert [p['input_tokens'] for p in m['passages']]==[512]*139
    assert calls==[32,32,32,32,11]
    assert len(digest)==64 and digest == hashlib.sha256(__import__('json').dumps(m,sort_keys=True,separators=(',', ':'),ensure_ascii=False,allow_nan=False).encode()).hexdigest()
    async def oversized(values):return [513]*len(values)
    with pytest.raises(ValueError,match='complete_source_input_exceeds_512_tokens'):
        await build_model_manifest(original=original,graph=graph,metadata=META,
                                   tokenizer_counts=oversized)


@pytest.mark.asyncio
@pytest.mark.parametrize('change', ['passage_hash','passage_figure','new_page','native_claim',
                                    'bad_metadata','discard_passage','new_chunk','altered_receipt'])
async def test_v2_readback_refuses_mutated_provenance_or_complete_input(change):
    original,graph=make_scan(3,with_illustration=True)
    rows=await inputs(graph)
    m,digest=await build_model_manifest(original=original,graph=graph,
                                       metadata=META,tokenizer_counts=lambda_17)
    corrupted=deepcopy(m)
    if change=='passage_hash':corrupted['passages'][0]['embedding_input_sha256']='0'*64
    elif change=='passage_figure':corrupted['passages'][0]['illustration_ids']=[]
    elif change=='new_page':corrupted['pages'][0]['page_id']=str(uuid4())
    elif change=='native_claim':corrupted['regions'][0]['native_block_index']=1
    elif change=='bad_metadata':corrupted['metadata']['title']='Forged book'
    elif change=='discard_passage':corrupted['passages'].pop()
    elif change=='new_chunk':corrupted['passages'].append(dict(corrupted['passages'][0]))
    elif change=='altered_receipt':corrupted['original']['receipt_sha256']='0'*64
    with pytest.raises(ValueError,match='drift'):
        verify_model_manifest(corrupted,original=original,graph=graph,metadata=META,
                              passage_inputs=rows,sha256=digest)


async def lambda_17(texts):return [17]*len(texts)


@pytest.mark.asyncio
async def test_rejected_incomplete_material_cannot_claim_512_or_mirror_pixels():
    original,graph=make_scan(2,with_illustration=True)
    rows=await inputs(graph)
    for change in ('drop','dup','tamper_text','tamper_hash','tamper_text_and_hash','wrong_figure','invalid_tokens'):
        bad=deepcopy(rows)
        if change=='drop':bad.pop()
        elif change=='dup':bad.append(deepcopy(bad[0]))
        elif change=='tamper_text':bad[0]['embedding_input']+=' unverified'
        elif change=='tamper_hash':bad[0]['embedding_input_sha256']='f'*64
        elif change=='tamper_text_and_hash':
            bad[0]['embedding_input'] += ' invented interpretation'
            bad[0]['embedding_input_sha256']=hashlib.sha256(bad[0]['embedding_input'].encode()).hexdigest()
        elif change=='wrong_figure':bad[0]['illustration_ids']=[]
        elif change=='invalid_tokens':bad[0]['input_tokens']=513
        with pytest.raises(ValueError):
            assemble_model_manifest(original=original,graph=graph,
                                    metadata=META,passage_inputs=bad)


def test_metadata_preserved_but_no_fabricated_public_rights():
    original,graph=make_scan(1)
    for invalid in ({'title':' '},{'title':'t','unknown':'invented'},
                    {'title':'t','publication_year':-1},
                    {'title':'t','authors':'A'},
                    {'title':'t','catalog':'not a catalog'}):
        with pytest.raises(ValueError):
            assemble_model_manifest(original=original,graph=graph,
                                    metadata=invalid,passage_inputs=[])


def test_metadata_cannot_be_rewritten_by_recomputing_untrusted_manifest_hash():
    import json
    import asyncio
    original, graph=make_scan(1)
    async def build():
        return await build_model_manifest(original=original,graph=graph,metadata=META,
                                          tokenizer_counts=lambda_17)
    manifest,_=asyncio.run(build())
    manifest['metadata']['title']='Other author or fabricated source title'
    recomputed=hashlib.sha256(json.dumps(manifest,sort_keys=True,separators=(',', ':'),
                                          ensure_ascii=False,allow_nan=False).encode()).hexdigest()
    passage_inputs=asyncio.run(inputs(graph))
    with pytest.raises(ValueError,match='drift'):
        verify_model_manifest(manifest,original=original,graph=graph,metadata=META,
                              passage_inputs=passage_inputs,sha256=recomputed)


@pytest.mark.asyncio
async def test_real_image_only_pdf_reaches_model_manifest_without_native_text(tmp_path):
    import io
    fitz = pytest.importorskip('pymupdf')
    from PIL import Image, ImageDraw
    raster = Image.new('RGB', (280, 190), 'white')
    ImageDraw.Draw(raster).text((18, 50), 'synthetic raster map', fill='black')
    data=io.BytesIO(); raster.save(data,format='PNG')
    file=tmp_path/'synthetic-raster-only.pdf'
    with fitz.open() as doc:
        page=doc.new_page(width=280,height=190)
        page.insert_image(page.rect,stream=data.getvalue())
        doc.save(file)
    with fitz.open(file) as doc:
        assert doc.page_count==1 and doc[0].get_text('text').strip()==''
        assert doc[0].get_images(full=True)
    receipt,graph=make_scan(1,with_illustration=True)
    receipt.sha256=hashlib.sha256(file.read_bytes()).hexdigest()
    manifest,digest=await build_model_manifest(original=receipt,graph=graph,
                                              metadata=META,tokenizer_counts=lambda_17)
    assert manifest['original']['sha256']==receipt.sha256
    assert manifest['regions'][0]['transcription_basis']=='model_visual_review'
    assert manifest['illustrations'][0]['caption_region_ids']
    assert len(digest)==64


def test_startmetadata_extra_transport_fields_do_not_change_catalogue():
    original, graph=make_scan(1)
    base={'title': 'V', 'duplicate_policy':'reuse', 'catalog_update':None, 'source_import':None}
    with pytest.raises(ValueError,match='complete and unique'):
        assemble_model_manifest(original=original,graph=graph,metadata=base,passage_inputs=[])
    for edit in ({'source_import':{'public_rights':{'rights_status':'unknown'}}},
                 {'catalog_update':{'title':'replacement'}},
                 {'duplicate_policy':'new_revision'}):
        with pytest.raises(ValueError):
            assemble_model_manifest(original=original,graph=graph,metadata={**base,**edit},passage_inputs=[])
