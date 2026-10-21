"""Fixed isolated PDF inspection/rendering child. Never recognition or OCR."""
from __future__ import annotations

import base64
import hashlib
import json
import os
from pathlib import Path
import sys


def _json(value):return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
def _hash(value):return hashlib.sha256(value).hexdigest()


def parse(path,action,index=0,*,cursor=None):
    # Inspect the actual file signature, not extension or caller-provided MIME.
    # This runs as a python -I child: a relative package import cannot be used.
    fd=os.open(path,os.O_RDONLY|os.O_NONBLOCK|getattr(os,'O_NOFOLLOW',0))
    try:header=os.pread(fd,16,0)
    finally:os.close(fd)
    if header.startswith(b'AT&TFORM') and header[12:16] in (b'DJVM',b'DJVU'):
        if action != 'inspect':
            # Metadata is NOT a render capability. A pinned DjVu decoder and
            # exact page-byte receipt are still required for book_pages.
            raise ValueError('original_djvu_decoder_required')
        import runpy
        inspector=runpy.run_path(str(Path(__file__).with_name('kaggle_djvu_inspect.py')))
        inventory=inspector['inspect_djvu'](path)
        return dict(format='djvu',page_count=inventory.page_count)
    import pymupdf as fitz
    with fitz.open(path) as document:
        total=int(document.page_count)
        if not 1<=total<=2500 or document.needs_pass:raise ValueError('original_pdf_invalid')
        if action=='inspect':return dict(format='pdf',page_count=total)
        if action not in ('page','text') or not 0<=index<total:raise ValueError('original_page_invalid')
        page=document.load_page(index);rect=page.rect
        width,height=float(rect.width),float(rect.height)
        if not 0<width<100000 or not 0<height<100000:raise ValueError('original_page_geometry_invalid')
        native=page.get_text('text',sort=False)
        missing=sorted({font[0] for font in page.get_fonts(full=True)
            if font[2]=='Type0' and document.xref_get_key(font[0],'ToUnicode')[0]=='null'})
        blocks=[]
        for ordinal,raw in enumerate(page.get_text('blocks',sort=False)):
            if len(raw)<7 or int(raw[6])!=0 or not str(raw[4]).strip():continue
            x0,y0,x1,y1=map(float,raw[:4]);box=fitz.Rect(x0,y0,x1,y1)
            if page.rotation:box=box*page.rotation_matrix
            x0,y0,x1,y1=box
            bbox={key:max(0,min(1000,round(value/extent*1000))) for key,value,extent in
                (('left',x0,width),('top',y0,height),('right',x1,width),('bottom',y1,height))}
            if bbox['left']>=bbox['right'] or bbox['top']>=bbox['bottom']:continue
            blocks.append(dict(block_index=ordinal,bbox=bbox,text=str(raw[4])))
        cursor=cursor or dict(block=0,offset=0,text=0)
        if (type(cursor) is not dict or set(cursor)!={'block','offset','text'}
                or any(type(v) is not int or v<0 for v in cursor.values())
                or cursor['block']>len(blocks) or cursor['text']>len(native)
                or (cursor['block']==len(blocks) and cursor['offset']!=0)
                or (cursor['block']<len(blocks) and cursor['offset']>=len(blocks[cursor['block']]['text']))):
            raise ValueError('original_page_cursor_invalid')
        position,offset=cursor['block'],cursor['offset'];segments=[]
        for _ in range(12):
            if position==len(blocks):break
            block=blocks[position];text=block['text'];end=min(offset+1000,len(text))
            segments.append(dict(block_index=block['block_index'],bbox=block['bbox'],text=text[offset:end],
                original_length=len(text),offset=offset,end=end,truncated=end<len(text),text_sha256=_hash(text.encode())))
            if end==len(text):position+=1;offset=0
            else:offset=end
        text_end=min(cursor['text']+12000,len(native));complete=position==len(blocks) and text_end==len(native)
        value=dict(format='pdf',page_count=total,physical_page_index=index,width=width,height=height,
            native_cursor=cursor,next_native=None if complete else dict(block=position,offset=offset,text=text_end),
            native_text=native[cursor['text']:text_end],native_text_info=dict(original_length=len(native),
                offset=cursor['text'],end=text_end,truncated=text_end<len(native),complete=complete,
                native_text_sha256=_hash(native.encode()),native_blocks_sha256=_hash(_json(blocks)),
                requires_visual_review=True,encoding_notice='cid_font_without_unicode_map' if missing else None,
                block_count=len(blocks),blocks_truncated=position<len(blocks)),native_blocks=segments)
        if action=='page':
            pixmap=None;last_edge=None
            for edge,quality in ((1600,82),(1600,70),(1600,55),(1280,70),(1280,55)):
                scale=min(2.,edge/max(width,height))
                if last_edge!=edge:pixmap=page.get_pixmap(matrix=fitz.Matrix(scale,scale),alpha=False);last_edge=edge
                image=pixmap.tobytes('jpeg',jpg_quality=quality)
                if len(image)<=256*1024:break
            else:raise ValueError('original_render_byte_bound')
            render=dict(policy='rkb.pdf-preview.v1',width=pixmap.width,height=pixmap.height,scale=scale,
                max_edge=edge,jpeg_quality=quality,byte_limit=256*1024,preview_only=True)
            value['preview_render']=render;value['native_text_info']['preview_render']=render
            value.update(image_b64=base64.b64encode(image).decode(),image_sha256=_hash(image))
        return value


def main():
    import resource
    resource.setrlimit(resource.RLIMIT_CPU,(30,30))
    resource.setrlimit(resource.RLIMIT_AS,(2*1024**3,2*1024**3))
    resource.setrlimit(resource.RLIMIT_FSIZE,(16*1024**2,16*1024**2))
    resource.setrlimit(resource.RLIMIT_NOFILE,(64,64))
    resource.setrlimit(resource.RLIMIT_CORE,(0,0))
    try:
        if len(sys.argv)!=7:raise ValueError
        path=Path(sys.argv[2])
        if not path.is_absolute() or not path.is_relative_to('/kaggle/temp') or path.resolve(strict=True)!=path:raise ValueError
        result=parse(path,sys.argv[1],int(sys.argv[3]),cursor=dict(zip(('block','offset','text'),map(int,sys.argv[4:]))))
        data=json.dumps(result,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
        if len(data)>12*1024*1024:raise ValueError
        sys.stdout.buffer.write(data);sys.stdout.buffer.flush()
    except Exception as exc:
        code=str(exc) if isinstance(exc,ValueError) and str(exc) in {
            'original_pdf_invalid','original_page_geometry_invalid','original_render_byte_bound',
            'original_djvu_decoder_required'} else 'original_pdf_parser_unavailable'
        os.write(1,json.dumps(dict(error=code,terminal=code not in {
            'original_pdf_parser_unavailable','original_djvu_decoder_required'})).encode())
        raise SystemExit(1)

if __name__=='__main__':main()
