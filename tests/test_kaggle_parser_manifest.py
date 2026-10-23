"""The parser lock is an additive exact closure; protected runtime never drifts."""
import hashlib
import json
from pathlib import Path


def test_pdf_manifest_preserves_all_fifty_original_wheel_pins():
    root=Path(__file__).resolve().parents[1]/'scripts/production/manifests'
    raw=(root/'kaggle-cp313-x86_64-wheels.json').read_bytes();base=json.loads(raw)
    pdf=json.loads((root/'kaggle-cp313-x86_64-pdf-wheels.json').read_bytes())
    assert pdf['base_manifest_sha256']==hashlib.sha256(raw).hexdigest()
    assert len(base['packages'])==50 and len(pdf['packages'])==51
    assert {p['package']:p for p in pdf['packages'] if p['package']!='pymupdf'}=={p['package']:p for p in base['packages']}
    parser=next(p for p in pdf['packages'] if p['package']=='pymupdf')
    assert parser['version']=='1.28.2' and parser['size']==25762500
    assert parser['sha256']=='397d6715c1f0df7548a92d0afd8ce370fc48fa47aeefac16be2bc04a16a8227f'
    assert parser['url'].startswith('https://files.pythonhosted.org/')
    assert pdf['wheel_bytes']==base['wheel_bytes']+parser['size']
    closure=pdf['parser_closure']
    assert closure['requires_dist']==[] and closure['packages']=={'pymupdf':'1.28.2'}
    assert closure['wheel_tag']=='cp310-abi3-manylinux_2_28_x86_64'
    assert not closure['smoke']['torch_installed'] and not closure['smoke']['model_forward_run']
    assert closure['smoke']['pdf_page_count']==10
    assert {'libmupdf.so.28.2','libmupdfcpp.so.28.2'}<=set(p['name'] for p in closure['native_elf'])
