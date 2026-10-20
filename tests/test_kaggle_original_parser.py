"""Real installed PDF library on tiny synthetic documents; no provider/model."""
import base64
import hashlib

import pytest

from regional_knowledge.kaggle_original_parser import parse


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
