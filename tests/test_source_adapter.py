import io,hashlib
from pathlib import Path
import pytest
from PIL import Image
from regional_knowledge.source_adapter import SourceProcessor,DjVuProcessor,source_format,crop_sync

@pytest.mark.asyncio
async def test_djvu_two_pages_are_rendered_without_recognition(tmp_path):
    processor=DjVuProcessor()
    files=[]
    for index,color in enumerate(('blue','red')):
        image=tmp_path/f'{index}.ppm';Image.new('RGB',(320,400),color).save(image)
        djvu=tmp_path/f'{index}.djvu';processor.command('c44',image,djvu);files.append(djvu)
    book=tmp_path/'control.djvu';processor.command('djvm','-c',book,*files)
    assert source_format(book)=='djvu'
    adapter=SourceProcessor();assert (await adapter.inspect_file(book)).page_count==2
    total,pages=await adapter.render_file(book,start=0,count=2)
    assert total==2 and len(pages)==2
    for page,color in zip(pages,((0,0,255),(255,0,0))):
        assert page.native_text is None
        with Image.open(io.BytesIO(page.data)) as image:
            observed=image.getpixel((10,10));assert all(abs(a-b)<10 for a,b in zip(observed,color))
    assert crop_sync(book,0,{'left':0,'top':0,'right':500,'bottom':500}).startswith(b'\x89PNG')
    with pytest.raises(ValueError):await adapter.render_file(book,start=0,count=9)


def test_container_signature_is_authoritative(tmp_path):
    path=tmp_path/'fake.pdf';path.write_bytes(b'not a container')
    with pytest.raises(ValueError):source_format(path)
