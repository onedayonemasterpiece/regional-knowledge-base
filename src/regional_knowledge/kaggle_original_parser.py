"""Fixed isolated PDF inspection/rendering child. Never recognition or OCR."""
from __future__ import annotations

import base64
import hashlib
import json
import os
from pathlib import Path
import sys


def parse(path,action,index=0):
    import pymupdf as fitz
    with fitz.open(path) as document:
        total=int(document.page_count)
        if not 1<=total<=2500 or document.needs_pass:raise ValueError('original_pdf_invalid')
        if action=='inspect':return dict(format='pdf',page_count=total)
        if action!='page' or not 0<=index<total:raise ValueError('original_page_invalid')
        page=document.load_page(index);rect=page.rect
        width,height=float(rect.width),float(rect.height)
        if not 0<width<100000 or not 0<height<100000:raise ValueError('original_page_geometry_invalid')
        scale=min(2.,1600/max(width,height))
        image=page.get_pixmap(matrix=fitz.Matrix(scale,scale),alpha=False).tobytes('jpeg',jpg_quality=82)
        if len(image)>8*1024*1024:raise ValueError('original_render_byte_bound')
        native=page.get_text('text',sort=False)
        # This preview is never an automatic accepted passage. Its complete
        # length and truncation are explicit; model-authored staging is separate.
        preview=native[:12000]
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
            value=str(raw[4]);blocks.append(dict(block_index=ordinal,bbox=bbox,text=value[:1000],
                original_length=len(value),truncated=len(value)>1000))
        return dict(format='pdf',page_count=total,physical_page_index=index,width=width,height=height,
            image_b64=base64.b64encode(image).decode(),image_sha256=hashlib.sha256(image).hexdigest(),
            native_text=preview,native_text_info=dict(original_length=len(native),truncated=len(preview)<len(native),
                requires_visual_review=True,encoding_notice='cid_font_without_unicode_map' if missing else None,
                block_count=len(blocks),blocks_truncated=len(blocks)>12),native_blocks=blocks[:12])


def main():
    import resource
    resource.setrlimit(resource.RLIMIT_CPU,(30,30))
    resource.setrlimit(resource.RLIMIT_AS,(2*1024**3,2*1024**3))
    resource.setrlimit(resource.RLIMIT_FSIZE,(16*1024**2,16*1024**2))
    resource.setrlimit(resource.RLIMIT_NOFILE,(64,64))
    resource.setrlimit(resource.RLIMIT_CORE,(0,0))
    try:
        if len(sys.argv)!=4:raise ValueError
        path=Path(sys.argv[2])
        if not path.is_absolute() or not path.is_relative_to('/kaggle/temp') or path.resolve(strict=True)!=path:raise ValueError
        result=parse(path,sys.argv[1],int(sys.argv[3]))
        data=json.dumps(result,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
        if len(data)>12*1024*1024:raise ValueError
        sys.stdout.buffer.write(data);sys.stdout.buffer.flush()
    except Exception as exc:
        code=str(exc) if isinstance(exc,ValueError) and str(exc) in {
            'original_pdf_invalid','original_page_geometry_invalid','original_render_byte_bound'} else 'original_pdf_parser_unavailable'
        os.write(1,json.dumps(dict(error=code,terminal=code!='original_pdf_parser_unavailable')).encode())
        raise SystemExit(1)

if __name__=='__main__':main()
