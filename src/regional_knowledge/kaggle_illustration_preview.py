"""Public figure preview from the same verified-original page renderer as book_pages."""
from io import BytesIO
import math

from PIL import Image


def configure_illustration_preview(startup):
    originals=startup.config.import_originals
    if originals is None:return
    async def render(figure,binding):
        page=await originals.illustration_page(figure,binding)
        with Image.open(BytesIO(page)) as image:
            box=figure['bbox'];width,height=image.size
            crop=image.convert('RGB').crop((math.floor(box['left']*width/1000),
                math.floor(box['top']*height/1000),math.ceil(box['right']*width/1000),
                math.ceil(box['bottom']*height/1000)))
            rotation=figure.get('display_rotation_degrees',0)
            if rotation:crop=crop.rotate(-rotation,expand=True)
            output=BytesIO();crop.save(output,format='JPEG',quality=90)
        return output.getvalue()
    startup.backend.illustration_renderer=render
