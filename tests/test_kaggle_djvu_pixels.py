"""One-page DjVu pixel seam tests. Synthetic native bundle != Kaggle acceptance."""
from copy import deepcopy
from hashlib import sha256
from io import BytesIO
from pathlib import Path

import pytest

import regional_knowledge.kaggle_djvu_pixels as pixels
from regional_knowledge.kaggle_djvu_inspect import inspect_djvu
from test_kaggle_djvu_inspect import fixture as djvu_fixture
from test_kaggle_native_bundle import fixture as native_fixture, prepare


@pytest.fixture
def archive(tmp_path,monkeypatch):
    root=tmp_path/"private"
    root.mkdir()
    data=djvu_fixture(2,dictionaries=1)
    path=root/"original.djvu"
    path.write_bytes(data)
    monkeypatch.setattr(pixels,"PRIVATE_ROOT",root)
    return path, inspect_djvu(path)


@pytest.fixture
def bundle(native_fixture,monkeypatch):
    executable=b"""#!/usr/bin/python3
import sys
args=sys.argv[1:]
if len(args)!=5 or args[0]!="-format=ppm" or not args[1].startswith("-page=") or not args[2].startswith("-size=") or args[4]!="-":
    sys.exit(3)
page=int(args[1].split("=")[1])
if page not in (1,2):sys.exit(4)
color=bytes([240,0,0] if page==1 else [0,0,240])
sys.stdout.buffer.write(b"P6\\n2 2\\n255\\n"+color*4)
"""+b"\n# "+b"x"*1500+b"\n"
    # A SCRIPT fake loader tests stdout/selected-page plumbing only.
    # The independent synthetic-ELF suite exercises the *real* closure
    # verifier; no fake script is accepted as verified DjVuLibre.
    loader=b"""#!/usr/bin/python3
import os,sys
arguments=sys.argv[1:]
if len(arguments)!=9 or arguments[:2]!=["--inhibit-cache","--library-path"]:
    sys.exit(5)
os.execv(arguments[3],arguments[3:])
"""+b"\n# "+b"x"*256+b"\n"
    lib=b"pinned synthetic library"*100
    value=native_fixture(
        entries=[("usr/bin/ddjvu","file",executable),
                 ("usr/bin/ld-linux-x86-64.so.2","file",loader),
                 ("usr/lib/x86_64-linux-gnu/libdjvulibre.so.21","file",lib)],
        files=[
            dict(path="usr/bin/ddjvu",size=len(executable),sha256=sha256(executable).hexdigest(),
                 package="fixture",member="usr/bin/ddjvu",executable=True),
            dict(path="usr/bin/ld-linux-x86-64.so.2",size=len(loader),sha256=sha256(loader).hexdigest(),
                 package="fixture",member="usr/bin/ld-linux-x86-64.so.2",executable=True),
            dict(path="usr/lib/x86_64-linux-gnu/libdjvulibre.so.21",size=len(lib),
                 sha256=sha256(lib).hexdigest(),package="fixture",
                 member="usr/lib/x86_64-linux-gnu/libdjvulibre.so.21",executable=False),
        ])
    cap=prepare(value)
    real=pixels._verified_decoder
    def synthetic_proof(candidate):
        # Explicitly fake ONLY closure verification for this subprocess/PPM
        # test. The trusted positive ELF fixtures live in test_kaggle_djvu_elf.
        if candidate is cap and cap.current():
            prefix=cap.prefix
            libs=str(prefix/"usr/lib/x86_64-linux-gnu")
            env={"PATH":"/usr/bin:/bin","LANG":"C","LC_ALL":"C",
                 "LD_LIBRARY_PATH":libs}
            return str(prefix/"usr/bin/ddjvu"),str(prefix/"usr/bin/ld-linux-x86-64.so.2"),libs,env
        return real(candidate)
    monkeypatch.setattr(pixels,"_verified_decoder",synthetic_proof)
    return cap


def test_strict_ppm_parses_only_bounded_actual_rgb_bytes():
    raw=b"P6\n# verified output\n2 2\n255\n"+bytes([255,0,0])*4
    w,h,rgb=pixels._read_ppm(raw,edge=1600)
    assert (w,h)==(2,2) and rgb==bytes([255,0,0])*4
    for invalid in (
        b"",b"P5\n2 2\n255\n"+b"\xff"*4,
        b"P6\n0 2\n255\n"+b"r"*12,
        b"P6\n2000 1\n255\n"+b"r"*6000,
        b"P6\n2 2\n65535\n"+b"r"*24,
        b"P6\n2 2\n255\n"+b"r"*11,
        b"P6\n2 2\n255\n"+b"r"*13,
        b"P6\n2 2\n255\n#"+b"r"*12,
    ):
        with pytest.raises(pixels.DjvuPixelsUnavailable):
            pixels._read_ppm(invalid,edge=1600)


@pytest.mark.asyncio
async def test_single_selected_page_produces_bounded_preview_and_exact_source_index(
        bundle,archive,monkeypatch):
    path,inv=archive
    seen=[]
    def fake_jpeg(rgb,width,height):
        seen.append((rgb,width,height))
        return b"\xff\xd8"+sha256(rgb).digest()+b"\xff\xd9",82
    monkeypatch.setattr(pixels,"_jpeg",fake_jpeg)
    first=await pixels.render_verified_djvu_page(
        bundle=bundle,original_path=path,original_bytes=path.stat().st_size,
        inventory=inv,index=0)
    second=await pixels.render_verified_djvu_page(
        bundle=bundle,original_path=path,original_bytes=path.stat().st_size,
        inventory=inv,index=1)
    assert [r["physical_page_index"] for r in (first,second)]==[0,1]
    assert [r["page_count"] for r in (first,second)]==[2,2]
    assert len(seen)==2
    assert seen[0][0]==bytes([240,0,0])*4
    assert seen[1][0]==bytes([0,0,240])*4
    for result in (first,second):
        assert result["format"]=="djvu" and result["image_sha256"]
        assert result["native_text"]=="" and result["native_blocks"]==[]
        assert result["native_text_info"]["requires_visual_review"]
        assert result["native_text_info"]["complete"]
        assert result["next_native"] is None
        assert result["preview_render"]["preview_only"]
        assert result["preview_render"]["width"]==2
        assert result["preview_render"]["height"]==2
        assert result["preview_render"]["max_edge"]==max(inv.pages[result["physical_page_index"]].width,inv.pages[result["physical_page_index"]].height)
        assert result["image_sha256"]==sha256(__import__("base64").b64decode(result["image_b64"])).hexdigest()


@pytest.mark.asyncio
async def test_real_pymupdf_encoder_returns_true_jpeg_bytes(bundle,archive):
    fitz=pytest.importorskip("pymupdf")
    path,inv=archive
    result=await pixels.render_verified_djvu_page(
        bundle=bundle,original_path=path,original_bytes=path.stat().st_size,
        inventory=inv,index=0)
    image=__import__("base64").b64decode(result["image_b64"])
    assert image.startswith(b"\xff\xd8") and image.endswith(b"\xff\xd9")
    p=fitz.Pixmap(image)
    assert (p.width,p.height)==(2,2) and 1<=len(image)<=pixels.MAX_JPEG_BYTES


@pytest.mark.asyncio
async def test_source_or_decoder_tamper_and_foreign_page_refuse_before_send(bundle,archive,monkeypatch,tmp_path):
    path,inv=archive
    monkeypatch.setattr(pixels,"_jpeg",lambda rgb,w,h:(b"\xff\xd8ok\xff\xd9",82))
    with pytest.raises(pixels.DjvuPixelsUnavailable,match="page_identity"):
        await pixels.render_verified_djvu_page(bundle=bundle,original_path=path,
            original_bytes=path.stat().st_size,inventory=inv,index=2)
    with pytest.raises(pixels.DjvuPixelsUnavailable,match="binding_changed"):
        await pixels.render_verified_djvu_page(bundle=bundle,original_path=path,
            original_bytes=path.stat().st_size-1,inventory=inv,index=0)
    symlink=path.parent/"shortcut.djvu"
    symlink.symlink_to(path)
    with pytest.raises(pixels.DjvuPixelsUnavailable):
        await pixels.render_verified_djvu_page(bundle=bundle,original_path=symlink,
            original_bytes=path.stat().st_size,inventory=inv,index=0)
    outside=tmp_path/"foreign.djvu"
    outside.write_bytes(path.read_bytes())
    with pytest.raises(pixels.DjvuPixelsUnavailable):
        await pixels.render_verified_djvu_page(bundle=bundle,original_path=outside,
            original_bytes=path.stat().st_size,inventory=inv,index=0)
    first=next(Path(pathstr) for pathstr,_,_,execute in bundle.pins if execute)
    first.chmod(0o700)
    first.write_bytes(b"tampered executable")
    with pytest.raises(pixels.DjvuPixelsUnavailable,match="bundle_not_verified|elf_closure_unverified"):
        await pixels.render_verified_djvu_page(bundle=bundle,original_path=path,
            original_bytes=path.stat().st_size,inventory=inv,index=0)


@pytest.mark.asyncio
async def test_no_unknown_decoder_fallback_and_unread_native_layer(bundle,archive,monkeypatch):
    path,inv=archive
    monkeypatch.setattr(pixels,"_jpeg",lambda rgb,w,h:(b"\xff\xd8ok\xff\xd9",82))
    with pytest.raises(pixels.DjvuPixelsUnavailable,match="bundle_not_verified|elf_closure_unverified"):
        await pixels.render_verified_djvu_page(bundle=object(),original_path=path,
            original_bytes=path.stat().st_size,inventory=inv,index=0)
    foreign=deepcopy(inv)
    from dataclasses import replace
    foreign=replace(foreign,pages=(replace(foreign.pages[0],text_chunks=1),foreign.pages[1]))
    # An OCR'd DjVu page still renders for visual transcription; the unread
    # layer is reported instead of being called absent.
    page=await pixels.render_verified_djvu_page(bundle=bundle,original_path=path,
        original_bytes=path.stat().st_size,inventory=foreign,index=0)
    assert page["native_text"]=="" and page["native_text_info"]["requires_visual_review"]
    assert page["native_text_info"]["encoding_notice"]=="djvu_native_layer_unread"

@pytest.mark.asyncio
@pytest.mark.parametrize('boundary', ['decoder_verification', 'final_verification'])
async def test_page_processing_keeps_control_loop_live(bundle,archive,monkeypatch,boundary):
    import asyncio
    import threading
    entered=threading.Event();release=threading.Event();decoded=False
    verify=pixels._verified_decoder
    current=type(bundle).current
    def blocked():
        entered.set()
        release.wait(1)  # Bound the failing pre-fix path without a hanging test.
    def decoder(candidate):
        if boundary=='decoder_verification':blocked()
        return verify(candidate)
    def checked(candidate):
        if candidate is bundle and decoded and boundary=='final_verification':blocked()
        return current(candidate)
    async def decode(*args):
        nonlocal decoded
        decoded=True
        return b'P6\n2 2\n255\n'+bytes([240,0,0])*4
    def jpeg(*args):
        return b'\xff\xd8fixture\xff\xd9',82
    monkeypatch.setattr(pixels,'_verified_decoder',decoder)
    monkeypatch.setattr(type(bundle),'current',checked)
    monkeypatch.setattr(pixels,'_decode_single',decode)
    monkeypatch.setattr(pixels,'_jpeg',jpeg)
    path,inventory=archive
    task=asyncio.create_task(pixels.render_verified_djvu_page(bundle=bundle,
        original_path=path,original_bytes=path.stat().st_size,inventory=inventory,index=0))
    try:
        assert await asyncio.to_thread(entered.wait,1)
        # The same loop must process control/read work before the slow boundary finishes.
        assert not task.done()
        await asyncio.sleep(0)
        assert not task.done()
        release.set()
        result=await task
        assert result['physical_page_index']==0 and result['image_sha256']
    finally:
        release.set()
        await task
