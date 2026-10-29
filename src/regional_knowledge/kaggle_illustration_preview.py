"""Public figure preview from the same verified-original page renderer as book_pages."""
from io import BytesIO
import math

from PIL import Image


def accepted_figure_crop(page, figure):
    """The same accepted bbox and display rotation for MCP and Kaggle PDF."""
    with Image.open(BytesIO(page)) as image:
        box=figure['bbox'];width,height=image.size
        crop=image.convert('RGB').crop((math.floor(box['left']*width/1000),
            math.floor(box['top']*height/1000),math.ceil(box['right']*width/1000),
            math.ceil(box['bottom']*height/1000)))
        rotation=figure.get('display_rotation_degrees',0)
        if rotation:crop=crop.rotate(-rotation,expand=True)
        return crop


def configure_illustration_preview(startup):
    originals=startup.config.import_originals
    if originals is None:return
    async def render(figure,binding,*,max_bytes):
        page=await originals.illustration_page(figure,binding)
        crop=accepted_figure_crop(page,figure)
        # Complete JPEG within the remaining MCP frame budget. Preserve
        # crop/orientation; reduce preview quality/resolution as needed.
        while True:
            for quality in (90,80,65,50,35,20):
                output=BytesIO();crop.save(output,format='JPEG',quality=quality)
                if output.tell()<=max_bytes:return output.getvalue()
            if crop.size==(1,1):raise ValueError('MCP metadata leaves no image capacity')
            crop=crop.resize((max(1,crop.width*3//4),max(1,crop.height*3//4)),Image.Resampling.LANCZOS)
    startup.backend.illustration_renderer=render
