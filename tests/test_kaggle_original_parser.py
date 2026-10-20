"""Real installed PDF library on tiny synthetic documents; no provider/model."""
import base64
import hashlib

import pytest

from regional_knowledge.kaggle_original_parser import parse
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
    result=parse(path,'page',0)
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
    assert len(image)<=256*1024
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
        assert len(image)<=256*1024 and result['width']==900 and result['height']==900
        assert render['width']==decoded.width and render['height']==decoded.height
        assert (render['max_edge'],render['jpeg_quality']) in {(1600,82),(1600,70),(1600,55),(1280,70),(1280,55)}
