"""Bounded one-page DjVu pixel decoder guarded by a VerifiedNativeBundle.

This is an internal Kaggle-only adapter, not an MCP grant or archive owner.
Until an exact DjVuLibre .deb closure is installed in the native bundle, calls
fail closed; ambient /usr/bin/ddjvu and LD_LIBRARY_PATH are never fallbacks.
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
from pathlib import Path
import stat
import sys

from .kaggle_djvu_inspect import DjvuInventory
from .kaggle_native_bundle import VerifiedNativeBundle
from .kaggle_native_env import verified_native_environment

PRIVATE_ROOT = Path('/kaggle/temp')
MAX_ORIGINAL_BYTES = 128 * 1024 * 1024
MAX_RENDER_EDGE = 1600
MAX_RENDER_PIXELS = 2_560_000
MAX_PPM_BYTES = 8 * 1024 * 1024 + 1024
MAX_JPEG_BYTES = 256 * 1024


class DjvuPixelsUnavailable(ValueError):
    """Fixed code; never return decoder messages, file paths or source bytes."""


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _json(value) -> bytes:
    return json.dumps(value,sort_keys=True,separators=(',',':'),
                      ensure_ascii=False,allow_nan=False).encode()


def _verified_decoder(bundle: VerifiedNativeBundle):
    # VerifiedNativeBundle.current() verifies file hashes; this separate
    # static-ELF audit must verify the interpreter AND transitive DT_NEEDED
    # closure. A random pinned .so in the directory is not DjVuLibre proof.
    from .kaggle_djvu_elf import pinned_djvu_elf_closure, ElfClosureError
    try:
        binary,loader,library_path=pinned_djvu_elf_closure(bundle)
        env=verified_native_environment(bundle)
    except Exception:
        raise DjvuPixelsUnavailable('djvu_elf_closure_unverified') from None
    env['LD_LIBRARY_PATH']=library_path
    return binary,loader,library_path,env


def _read_ppm(raw: bytes, *, edge: int):
    """Strict P6/8-bit, bounded result; pixel bytes are never interpreted as header."""
    if type(raw) is not bytes or len(raw)>MAX_PPM_BYTES:
        raise DjvuPixelsUnavailable('djvu_render_output_bound')
    pos=0
    fields=[]
    for _ in range(4):
        while pos<len(raw):
            char=raw[pos]
            if char in b' \n\r\t':
                pos+=1
            elif char==35:
                close=raw.find(b'\n',pos)
                if close<0 or close-pos>512:raise DjvuPixelsUnavailable('djvu_ppm_header_invalid')
                pos=close+1
            else:break
        start=pos
        while pos<len(raw) and raw[pos] not in b' \n\r\t#':
            pos+=1
            if pos-start>32:raise DjvuPixelsUnavailable('djvu_ppm_header_invalid')
        if start==pos:raise DjvuPixelsUnavailable('djvu_ppm_header_invalid')
        fields.append(raw[start:pos])
    if pos>=len(raw) or raw[pos] not in b' \n\r\t':
        raise DjvuPixelsUnavailable('djvu_ppm_header_invalid')
    # The binary pixel stream begins immediately after the maxval delimiter.
    pos+=2 if raw[pos:pos+2]==b'\r\n' else 1
    magic,width_raw,height_raw,max_raw=fields
    try:
        width,height=int(width_raw),int(height_raw)
    except (ValueError,TypeError):
        raise DjvuPixelsUnavailable('djvu_ppm_header_invalid') from None
    if (magic!=b'P6' or max_raw!=b'255'
            or not 1<=width<=edge or not 1<=height<=edge
            or width*height>MAX_RENDER_PIXELS
            or len(raw)-pos!=width*height*3):
        raise DjvuPixelsUnavailable('djvu_ppm_geometry_or_bytes_invalid')
    return width,height,raw[pos:]


def _source_stat(path: Path, *, expected_bytes: int):
    if (not path.is_absolute() or not path.is_relative_to(PRIVATE_ROOT)
            or type(expected_bytes) is not int
            or not 16<=expected_bytes<=MAX_ORIGINAL_BYTES):
        raise DjvuPixelsUnavailable('djvu_private_original_binding_required')
    try:
        fd=os.open(path,os.O_RDONLY|os.O_CLOEXEC|os.O_NONBLOCK|getattr(os,'O_NOFOLLOW',0))
    except OSError:
        raise DjvuPixelsUnavailable('djvu_private_original_binding_changed') from None
    try:
        item=os.fstat(fd)
        if (not stat.S_ISREG(item.st_mode) or item.st_size!=expected_bytes
                or path.resolve(strict=True)!=path):
            raise DjvuPixelsUnavailable('djvu_private_original_binding_changed')
        return (item.st_dev,item.st_ino,item.st_size,item.st_mtime_ns,item.st_ctime_ns)
    finally:
        os.close(fd)


async def _decode_single(binary: str, loader: str, library_path: str,
                         environment: dict, original: Path,
                         index: int, edge: int) -> bytes:
    # A signed-inventory Python child sets hard CPU/AS/FD/core limits BEFORE
    # execve of the pinned decoder. Its command always selects one page.
    # In contrast, raw ddjvu without -page may decode an entire book.
    child=Path(__file__).with_name('kaggle_djvu_decoder_child.py')
    # Fixed static ELF closure is encoded in the loader and library path.
    # The child executes that exact verified loader, not ambient PT_INTERP.
    command=(sys.executable,'-I',str(child),loader,binary,str(original),
             str(index),str(edge),library_path)
    proc=None
    try:
        proc=await asyncio.create_subprocess_exec(*command,stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.DEVNULL,env=environment)
        fragments=[]
        used=0
        async with asyncio.timeout(25):
            while part:=await proc.stdout.read(65536):
                used+=len(part)
                if used>MAX_PPM_BYTES:
                    raise DjvuPixelsUnavailable('djvu_render_output_bound')
                fragments.append(part)
            status=await proc.wait()
        if status!=0 or used<16:
            raise DjvuPixelsUnavailable('djvu_decoder_failed')
        return b''.join(fragments)
    except asyncio.CancelledError:
        raise
    except DjvuPixelsUnavailable:
        raise
    except Exception:
        raise DjvuPixelsUnavailable('djvu_decoder_unavailable') from None
    finally:
        if proc is not None and proc.returncode is None:
            try:proc.kill()
            except ProcessLookupError:pass
            # asyncio's subprocess wait can otherwise remain pending after
            # SIGKILL while a full stdout pipe is paused. Close the pipe
            # transport first, then bound the final reap independently from
            # the main 25s processing timeout. Never wait without a deadline.
            transport=getattr(proc,'_transport',None)
            pipe=transport.get_pipe_transport(1) if transport is not None else None
            if pipe is not None:pipe.close()
            try:
                async with asyncio.timeout(2):
                    await proc.wait()
            except TimeoutError:
                if transport is not None:transport.close()
                raise DjvuPixelsUnavailable('djvu_decoder_cleanup_timeout') from None


def _jpeg(rgb: bytes, width: int, height: int):
    # PyMuPDF is the existing pinned native PDF/JPEG dependency; no ambient
    # Pillow, OCR/model or additional media uploader is required.
    import pymupdf
    image=pymupdf.Pixmap(pymupdf.csRGB,width,height,rgb,False)
    for quality in (82,70,55):
        blob=image.tobytes('jpeg',jpg_quality=quality)
        if 1<=len(blob)<=MAX_JPEG_BYTES:
            return blob,quality
    return None,None


async def render_verified_djvu_page(*, bundle: VerifiedNativeBundle,
                                    original_path: Path, original_bytes: int,
                                    inventory: DjvuInventory, index: int):
    """Render only the selected actual page, in the existing original_pages wire.

    The caller has already proved owner access and original SHA/receipt in
    NativeOriginalIntake. This helper rechecks private-file identity/size;
    it does not confer public/proof permissions or mark source imported/Saved.
    """
    if (type(inventory) is not DjvuInventory or type(index) is not int
            or not 0<=index<inventory.page_count or len(inventory.pages)!=inventory.page_count
            or inventory.pages[index].index!=index):
        raise DjvuPixelsUnavailable('djvu_page_identity_invalid')
    if inventory.pages[index].text_chunks:
        # An unread embedded native layer may not be silently called complete.
        raise DjvuPixelsUnavailable('djvu_embedded_text_layer_requires_decoder')
    # INFO orientation changes the meaning of the normalized source bbox.
    # Until the renderer owns and verifies the complete transform, never hand
    # callers an unrotated geometry/scale as though it were the source page.
    if inventory.pages[index].orientation_degrees != 0:
        raise DjvuPixelsUnavailable('djvu_orientation_transform_not_ready')
    path=Path(original_path)
    before=_source_stat(path,expected_bytes=original_bytes)
    if inventory.byte_length!=original_bytes:
        raise DjvuPixelsUnavailable('djvu_inventory_original_size_mismatch')
    binary,loader,library_path,env=_verified_decoder(bundle)
    output=None
    for edge in (1600,1280):
        raw=await _decode_single(binary,loader,library_path,env,path,index,edge)
        width,height,rgb=_read_ppm(raw,edge=edge)
        try:
            jpeg,quality=_jpeg(rgb,width,height)
        except (ImportError,RuntimeError,ValueError):
            raise DjvuPixelsUnavailable('djvu_jpeg_encoder_unavailable') from None
        if jpeg is not None:
            output=(jpeg,quality,edge,width,height)
            break
    if output is None:
        raise DjvuPixelsUnavailable('djvu_preview_jpeg_byte_bound')
    if _source_stat(path,expected_bytes=original_bytes)!=before or not bundle.current():
        raise DjvuPixelsUnavailable('djvu_source_or_decoder_changed')
    jpeg,quality,edge,width,height=output
    page=inventory.pages[index]
    render=dict(policy='rkb.djvu-preview.v1',width=width,height=height,
        scale=min(width/page.width,height/page.height),
        max_edge=edge,jpeg_quality=quality,byte_limit=MAX_JPEG_BYTES,
        preview_only=True)
    return dict(format='djvu',page_count=inventory.page_count,physical_page_index=index,
        width=page.width,height=page.height,
        native_cursor=dict(block=0,offset=0,text=0),next_native=None,
        native_text='',native_blocks=[],
        native_text_info=dict(original_length=0,offset=0,end=0,truncated=False,complete=True,
            native_text_sha256=_sha(b''),native_blocks_sha256=_sha(_json([])),
            requires_visual_review=True,encoding_notice='image_only_djvu_no_native_layer',
            block_count=0,blocks_truncated=False,preview_render=render),
        preview_render=render,image_b64=base64.b64encode(jpeg).decode('ascii'),
        image_sha256=_sha(jpeg))
