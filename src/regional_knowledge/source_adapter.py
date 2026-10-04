"""Deterministic source containers. No recognition or semantic extraction."""
from __future__ import annotations
import asyncio, io, os, subprocess
from pathlib import Path
from .ingestion import PdfInfo, PdfRenderedPage, PyMuPdfProcessor, MAX_PAGES, PAGE_MAX_EDGE


def source_format(path: Path) -> str:
    with path.open('rb') as stream:
        header = stream.read(16)
    if header.startswith(b'%PDF-'):
        return 'pdf'
    if header[:8] == b'AT&TFORM' and header[12:16] in (b'DJVU', b'DJVM'):
        return 'djvu'
    raise ValueError('unsupported_source_container')


class DjVuProcessor:
    def command(self, executable, *arguments):
        root = Path(os.environ.get('RKB_DJVU_ROOT', '/usr' if Path('/usr/bin/ddjvu').exists() else '/home/dev/.local/opt/djvulibre'))
        env = dict(os.environ)
        env['LD_LIBRARY_PATH'] = str(root/'lib/x86_64-linux-gnu' if root==Path('/usr') else root/'usr/lib/x86_64-linux-gnu')
        result = subprocess.run([str(root/'bin'/executable if root==Path('/usr') else root/'usr/bin'/executable), *map(str, arguments)],
                                capture_output=True, timeout=45, env=env)
        if result.returncode:
            raise ValueError('djvu_container_or_render_invalid')
        return result.stdout

    async def inspect_file(self, path):
        count = int(await asyncio.to_thread(self.command, 'djvused', path, '-e', 'n'))
        if not 1 <= count <= MAX_PAGES:
            raise ValueError('source_page_count_limit')
        return PdfInfo(count)

    async def render_file(self, path, *, start, count, text_part=None):
        if start < 0 or not 1 <= count <= 8 or text_part is not None:
            raise ValueError('invalid_djvu_page_batch')
        info = await self.inspect_file(path)
        pages=[]
        for index in range(start, min(start+count, info.page_count)):
            data = await asyncio.to_thread(self.command, 'ddjvu', '-format=ppm',
                                           f'-page={index+1}', f'-size={PAGE_MAX_EDGE}x{PAGE_MAX_EDGE}', path, '-')
            from PIL import Image
            with Image.open(io.BytesIO(data)) as image:
                output=io.BytesIO();image.convert('RGB').save(output, format='JPEG', quality=82)
            # djvutxt only reads a text layer already present in the container.
            native = (await asyncio.to_thread(self.command,'djvutxt',f'-page={index+1}',path)).decode('utf-8').strip()
            pages.append(PdfRenderedPage(index,'image/jpeg',output.getvalue(),native[:24000] or None,(),
                {'original_length':len(native),'truncated':len(native)>24000,
                 'material':'embedded_text_hint','requires_visual_review':True}))
        return info.page_count,tuple(pages)


class SourceProcessor:
    def adapter(self,path):
        return PyMuPdfProcessor() if source_format(path)=='pdf' else DjVuProcessor()
    async def inspect_file(self,path):
        return await self.adapter(path).inspect_file(path)
    async def render_file(self,path,**kwargs):
        return await self.adapter(path).render_file(path,**kwargs)


def crop_sync(path,page_index,bbox):
    if source_format(path)=='pdf':
        from .stage_service import _crop_sync
        return _crop_sync(path,page_index,bbox)
    from PIL import Image
    processor=DjVuProcessor()
    data=processor.command('ddjvu','-format=ppm',f'-page={page_index+1}',f'-size={PAGE_MAX_EDGE}x{PAGE_MAX_EDGE}',path,'-')
    with Image.open(io.BytesIO(data)) as image:
        width,height=image.size
        box=tuple(round(v*d/1000) for v,d in zip((bbox['left'],bbox['top'],bbox['right'],bbox['bottom']),(width,height,width,height)))
        output=io.BytesIO();image.crop(box).save(output,format='PNG');return output.getvalue()
