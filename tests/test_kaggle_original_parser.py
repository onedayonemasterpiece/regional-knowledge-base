"""Real installed PDF library on tiny synthetic documents; no provider/model."""
import base64
import hashlib

import pytest

from regional_knowledge.kaggle_original_parser import parse,MODEL_PAGE_BYTE_LIMIT,MODEL_PAGE_EDGES,MODEL_PAGE_QUALITIES
from regional_knowledge.original_intake import assemble_native_page,canonical,digest,OriginalIntakeError


def test_actual_pdf_parser_preserves_ten_physical_pages_and_render_provenance(tmp_path):
    fitz=pytest.importorskip('pymupdf')
    path=tmp_path/'synthetic-ten-pages.pdf'
    with fitz.open() as document:
        for index in range(10):
            page=document.new_page(width=300,height=400)
            page.insert_text((30,60),f'Synthetic native page {index+1}')
        document.save(path)
    assert parse(path,'inspect')=={'format':'pdf','page_count':10}
    result=parse(path,'page',9)
    image=base64.b64decode(result.pop('image_b64'),validate=True)
    assert image[:2]==b'\xff\xd8' and result['image_sha256']==hashlib.sha256(image).hexdigest()
    assert result['physical_page_index']==9 and result['page_count']==10
    assert 'Synthetic native page 10' in result['native_text']
    assert not result['native_text_info']['truncated']
    block=result['native_blocks'][0]
    assert 'Synthetic native page 10' in block['text']
    assert 0<=block['bbox']['left']<block['bbox']['right']<=1000
    assert 0<=block['bbox']['top']<block['bbox']['bottom']<=1000
    with pytest.raises(ValueError):parse(path,'page',10)


def test_actual_parser_marks_native_preview_truncation_instead_of_claiming_complete_text(tmp_path):
    fitz=pytest.importorskip('pymupdf')
    path=tmp_path/'synthetic-large-native-layer.pdf'
    with fitz.open() as document:
        page=document.new_page(width=1000,height=2000)
        # Repeated positioned text is actual PDF content, not supplied parser
        # metadata. Its native layer exceeds the explicit preview byte budget.
        for row in range(200):page.insert_text((10,10+row*9),'synthetic source region '*5,fontsize=5)
        document.save(path)
    result=parse(path,'page',0,legacy_fragments=True)
    assert len(result['native_text'])==12000
    assert result['native_text_info']['original_length']>12000
    assert result['native_text_info']['truncated'] and result['native_text_info']['requires_visual_review']
    assert result['native_text_info']['blocks_truncated'] and len(result['native_blocks'])==12


def test_actual_full_text_and_all_blocks_continue_without_rerender_or_silent_clipping(tmp_path):
    fitz=pytest.importorskip('pymupdf');path=tmp_path/'synthetic-full-fidelity.pdf'
    with fitz.open() as document:
        page=document.new_page(width=1000,height=2000)
        for row in range(200):page.insert_text((10,10+row*9),f'region-{row:03d} '+'complete source '*5,fontsize=5)
        document.save(path)
    with fitz.open(path) as document:
        expected=document[0].get_text('text',sort=False)
        expected_blocks=[str(b[4]) for b in document[0].get_text('blocks',sort=False) if len(b)>=7 and int(b[6])==0 and str(b[4]).strip()]
    first=parse(path,'page',0);image=base64.b64decode(first.pop('image_b64'))
    assert len(image)<=MODEL_PAGE_BYTE_LIMIT
    assert first['preview_render']['width']>0 and first['native_text_info']['preview_render']==first['preview_render']
    chunks=[];part=first
    while True:
        for key,value in dict(image_sha256=digest(image),image_size=len(image),mime_type='image/jpeg',
                page_id='00000000-0000-0000-0000-000000000001',original_sha256=digest(path.read_bytes()),
                parser_sha256='a'*64,preview_render=first['preview_render']).items():part[key]=value
        part['native_text_info']['preview_render']=first['preview_render']
        part['page_receipt_sha256']=digest(canonical(part));chunks.append(part)
        if part['next_native'] is None:break
        part=parse(path,'text',0,cursor=part['next_native'])
        assert 'image_b64' not in part and 'image_sha256' not in part
        assert len(chunks)<100
    complete=assemble_native_page(chunks)
    assert len(chunks)>1 and complete['native_text']==expected
    assert [b['text'] for b in complete['native_blocks']]==expected_blocks
    assert len(complete['native_blocks'])==200
    assert not complete['native_text_info']['truncated'] and not complete['native_text_info']['blocks_truncated']
    assert all(not b['truncated'] for b in complete['native_blocks'])
    with pytest.raises(OriginalIntakeError):assemble_native_page(chunks[:-1])
    with pytest.raises(OriginalIntakeError):assemble_native_page(chunks[:1]+chunks[2:])


def test_actual_single_long_native_block_has_exact_offset_continuation(tmp_path):
    fitz=pytest.importorskip('pymupdf');path=tmp_path/'synthetic-long-block.pdf'
    with fitz.open() as document:
        page=document.new_page(width=1000,height=2000)
        assert page.insert_textbox((10,10,990,1900),'source span '*2000,fontsize=6)>=0
        document.save(path)
    part=parse(path,'page');pieces=[];offsets=[]
    while True:
        for block in part['native_blocks']:pieces.append(block['text']);offsets.append(block['offset'])
        if part['next_native'] is None:break
        part=parse(path,'text',cursor=part['next_native'])
    assert max(offsets)>=1000
    with fitz.open(path) as document:
        expected=''.join(str(b[4]) for b in document[0].get_text('blocks',sort=False) if int(b[6])==0)
    assert ''.join(pieces)==expected


def test_actual_dense_image_preview_has_finite_policy_and_never_exceeds_output_bound(tmp_path):
    import random
    fitz=pytest.importorskip('pymupdf');path=tmp_path/'synthetic-dense-image.pdf'
    pixels=random.Random(1).randbytes(900*900*3)
    pixmap=fitz.Pixmap(fitz.csRGB,900,900,pixels,False)
    with fitz.open() as document:
        page=document.new_page(width=900,height=900);page.insert_image(page.rect,pixmap=pixmap);document.save(path)
    try:result=parse(path,'page')
    except ValueError as exc:
        assert str(exc)=='original_render_byte_bound'
    else:
        image=base64.b64decode(result['image_b64']);render=result['preview_render'];decoded=fitz.Pixmap(image)
        assert len(image)<=MODEL_PAGE_BYTE_LIMIT and result['width']==900 and result['height']==900
        assert render['width']==decoded.width and render['height']==decoded.height
        assert render['max_edge'] in MODEL_PAGE_EDGES and render['jpeg_quality'] in MODEL_PAGE_QUALITIES


def test_tiny_native_blocks_fit_one_fragment_without_changing_source(tmp_path):
    fitz=pytest.importorskip('pymupdf')
    path=tmp_path/'fictional-tiny-blocks.pdf'
    with fitz.open() as document:
        page=document.new_page(width=400,height=3000)
        for index in range(126):
            page.insert_text((20,20+index*20), 'fictional' if index<84 else 'tinytext', fontsize=5)
        document.save(path)
    result=parse(path,'text')
    assert result['native_text_info']['block_count']==126
    assert len(result['native_text'])==1218
    assert result['next_native'] is None
    assert len(result['native_blocks'])==126
    assert [b['block_index'] for b in result['native_blocks']]==list(range(126))
    assert ''.join(b['text'] for b in result['native_blocks'])==result['native_text']
    assert all(len(b['text'])<=1000 and not b['truncated'] for b in result['native_blocks'])


def test_historical_tiny_block_fragments_keep_exact_issued_bytes(tmp_path):
    fitz=pytest.importorskip('pymupdf');path=tmp_path/'fictional-historical.pdf'
    with fitz.open() as document:
        page=document.new_page(width=400,height=3000)
        for index in range(126):page.insert_text((20,20+index*20),'fictional' if index<84 else 'tinytext',fontsize=5)
        document.save(path)
    for cursor,expected in [(None,'6b84ecc251c461dbb59cf340db7925be50337edb5ad0d2fa87b255aa536cd609'),
            (dict(block=12,offset=0,text=1218),'1f7f8f1b8cbcb717577b139c9de6aa4c25cfa6123e63b3d5a8a1dccd607f3d14')]:
        # Canonical hashes captured from the pre-change parser on this fictional PDF.
        part=parse(path,'text',cursor=cursor,legacy_fragments=True)
        assert digest(canonical(part))==expected
        assert len(part['native_blocks'])==12


@pytest.mark.parametrize('texts', [['\x01😀'*500]*30, ['x']*1200])
def test_packed_native_fragments_respect_exact_wire_bound_and_resume_all_blocks(tmp_path,monkeypatch,texts):
    fitz=pytest.importorskip('pymupdf');path=tmp_path/'fictional-bound.pdf'
    with fitz.open() as document:document.new_page(width=300,height=400);document.save(path)
    native=''.join(texts)
    def get_text(self,kind,sort=False):
        return native if kind=='text' else [(10,10,20,20,text,index,0) for index,text in enumerate(texts)]
    monkeypatch.setattr(fitz.Page,'get_text',get_text)
    metadata=dict(page_id='00000000-0000-0000-0000-000000000001',original_sha256='a'*64,
        parser_sha256='b'*64,image_size=8000000,image_sha256='c'*64,mime_type='image/jpeg',
        preview_render=dict(policy='rkb.model-page.v2',width=3200,height=3200,scale=1.0,max_edge=3200,
            jpeg_quality=85,byte_limit=8*1024*1024,image_bytes=8000000,preview_only=True))
    cursor=None;parts=[]
    while True:
        part=parse(path,'text',cursor=cursor,page_metadata=metadata)
        wire=dict(part,**metadata);wire['native_text_info']=dict(part['native_text_info'],preview_render=metadata['preview_render'])
        wire['page_receipt_sha256']=digest(canonical(wire))
        assert len(canonical(dict(phase='original_pages',request_sha256='d'*64,value=wire)))<=120*1024
        assert len(part['native_text'])<=12000 and all(len(b['text'])<=1000 for b in part['native_blocks'])
        assert part['next_native']!=cursor or part['next_native'] is None
        parts.append(wire);cursor=part['next_native']
        if cursor is None:break
        assert len(parts)<100
    complete=assemble_native_page(parts)
    assert len(parts)>1 and complete['native_text']==native
    assert [b['text'] for b in complete['native_blocks']]==texts


def test_historical_long_block_continuation_preserves_exact_bytes(tmp_path,monkeypatch):
    fitz=pytest.importorskip('pymupdf');path=tmp_path/'fictional-old-long.pdf'
    with fitz.open() as document:document.new_page(width=300,height=400);document.save(path)
    text='fictional '*3000
    monkeypatch.setattr(fitz.Page,'get_text',lambda self,kind,sort=False:
        text if kind=='text' else [(10,10,20,20,text,0,0)])
    for cursor,expected in [(None,'40b723a45b19752411ab885366c6c94275112eae2ac3db8b93049631e5e03ab4'),
            (dict(block=0,offset=12000,text=12000),'ed7dc5d9e1284ec1ec3db9f1a7c98f699a2aefcdbbbe5f35b34f9d5c06be12ed')]:
        assert digest(canonical(parse(path,'text',cursor=cursor,legacy_fragments=True)))==expected
