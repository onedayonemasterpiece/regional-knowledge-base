"""Real isolated child and synthetic PDF; no original/provider/model calls."""
import importlib.metadata
import json
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
import venv

import pytest

from regional_knowledge import kaggle_original_native as native
from regional_knowledge.original_intake import OriginalIntakeError


@pytest.fixture
def child_fixture(tmp_path,monkeypatch):
    fitz=pytest.importorskip('pymupdf')
    isolated=tmp_path/'isolated'
    venv.EnvBuilder(with_pip=False).create(isolated)
    executable=isolated/'bin/python'
    pdf=tmp_path/'synthetic.pdf'
    with fitz.open() as doc:
        doc.new_page(width=300,height=400).insert_text((30,60),'Synthetic source page')
        doc.save(pdf)
    parser=Path(native.__file__).with_name('kaggle_original_parser.py')
    parser_bytes=parser.read_bytes()
    # Exercise the unchanged parser main/resource limits. Its private path gate
    # maps ONLY /kaggle/temp to this test-owned private directory in the child.
    script=tmp_path/'fixture_parser.py'
    script.write_text('import pathlib,runpy\n'
        'original=pathlib.Path.is_relative_to\n'
        'def private_fixture(self,*other):\n'
        f'    return original(self,{str(tmp_path)!r}) if other==("/kaggle/temp",) else original(self,*other)\n'
        'pathlib.Path.is_relative_to=private_fixture\n'
        f'runpy.run_path({str(parser)!r},run_name="__main__")\n')
    monkeypatch.setattr(native,'PRIVATE_ROOT',tmp_path)
    config=native.OriginalIntakeConfig(('https://archive.invalid',),
        (('PyMuPDF',importlib.metadata.version('PyMuPDF')),))
    owner=native.NativeOriginalIntake(config=config,database=SimpleNamespace(),
        identity=('synthetic-run','synthetic-nonce','synthetic-incarnation'),workspace=tmp_path,
        current=lambda binding:True)
    assert owner._parser_script(owner.parser_sha256)==parser
    assert owner._parser_script(owner.legacy_parser_sha256)==parser
    monkeypatch.setattr(owner,'_parser_script',lambda pin:script)
    yield SimpleNamespace(owner=owner,executable=executable,pdf=pdf,script=script,
        root=tmp_path,parser=parser,parser_bytes=parser_bytes,isolated=isolated)
    assert parser.read_bytes()==parser_bytes


@pytest.mark.asyncio
async def test_target_only_dependency_missing_before_fix_real_inspect_render_and_text_after(child_fixture,monkeypatch):
    f=child_fixture
    missing=subprocess.run([str(f.executable),'-I',str(f.script),'inspect',str(f.pdf),'0','0','0','0'],
        capture_output=True,text=True,timeout=15,env={'PATH':'/usr/bin:/bin','LANG':'C','LC_ALL':'C'})
    assert missing.returncode==1
    assert json.loads(missing.stdout)==dict(error='original_pdf_parser_unavailable',terminal=False)
    monkeypatch.setattr(sys,'executable',str(f.executable))
    assert await f.owner._parse(f.pdf,'inspect')==dict(format='pdf',page_count=1)
    page=await f.owner._parse(f.pdf,'page')
    text=await f.owner._parse(f.pdf,'text')
    assert page['native_text']==text['native_text']=='Synthetic source page\n'
    assert page['image_b64'] and page['image_sha256']
    assert page['preview_render']['byte_limit']==8*1024*1024


@pytest.mark.asyncio
async def test_current_directory_pythonpath_and_other_installed_version_cannot_shadow(child_fixture,monkeypatch):
    f=child_fixture
    evil=f.root/'shadow';evil.mkdir()
    (evil/'pymupdf.py').write_text('raise RuntimeError("untrusted path imported")\n')
    monkeypatch.chdir(evil);monkeypatch.setenv('PYTHONPATH',str(evil))
    site=next((f.isolated/'lib').glob('python*/site-packages'))
    (site/'pymupdf.py').write_text('raise RuntimeError("base version imported")\n')
    metadata=site/'pymupdf-0.0.1.dist-info';metadata.mkdir()
    (metadata/'METADATA').write_text('Metadata-Version: 2.1\nName: PyMuPDF\nVersion: 0.0.1\n')
    monkeypatch.setattr(sys,'executable',str(f.executable))
    assert await f.owner._parse(f.pdf,'inspect')==dict(format='pdf',page_count=1)


@pytest.mark.asyncio
@pytest.mark.parametrize('mismatch',['root','version'])
async def test_wrong_dependency_binding_rejects_before_parser_or_private_file_read(child_fixture,monkeypatch,caplog,mismatch):
    f=child_fixture;marker=f.root/'parser-entered'
    f.script.write_text(f'from pathlib import Path\nPath({str(marker)!r}).touch()\n')
    if mismatch=='version':f.owner._pdf_dependency_version='0.0.0'
    else:f.owner._pdf_dependency_root=str(f.root)
    monkeypatch.setattr(sys,'executable',str(f.executable))
    with pytest.raises(OriginalIntakeError,match='original_parser_unavailable'):
        await f.owner._parse(f.root/'nonexistent-private.pdf','inspect')
    assert not marker.exists()
    assert 'error_type=' in caplog.text
    assert str(f.root) not in caplog.text
    assert 'nonexistent-private' not in caplog.text


@pytest.mark.asyncio
async def test_normal_installed_venv_and_legacy_parser_pin_remain_usable(child_fixture):
    f=child_fixture
    assert await f.owner._parse(f.pdf,'inspect',parser_sha256=f.owner.legacy_parser_sha256)==dict(format='pdf',page_count=1)


@pytest.mark.asyncio
async def test_isolated_djvu_inspection_and_decoder_required_semantics_preserved(child_fixture,monkeypatch):
    from test_kaggle_djvu_inspect import fixture
    f=child_fixture;path=f.root/'synthetic.djvu';path.write_bytes(fixture(page_count=2))
    monkeypatch.setattr(sys,'executable',str(f.executable))
    assert await f.owner._parse(path,'inspect')==dict(format='djvu',page_count=2)
    with pytest.raises(OriginalIntakeError,match='original_djvu_decoder_required'):
        await f.owner._parse(path,'page')
