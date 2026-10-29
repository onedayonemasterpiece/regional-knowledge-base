"""PDF illustrations use accepted figure positions and actual pixels, not nearest pictures."""
import base64
from io import BytesIO

from PIL import Image,ImageDraw
import pymupdf
import pytest

from regional_knowledge.kaggle_protocol import RESULT_MAX_BYTES
from regional_knowledge.source_unit_pdf import render_source_unit_pdf,SourceUnitPdfError
from regional_knowledge.source_unit_export import _pdf_figure_crop
from test_source_unit_pdf import inputs,flatten


def jpeg(color):
    image=Image.new('RGB',(900,450),'white')
    draw=ImageDraw.Draw(image)
    draw.rectangle((30,20,869,429),outline=color,width=9)
    for i in range(0,850,45):
        draw.line((i,70,i+50,370),fill=color,width=3)
    out=BytesIO();image.save(out,format='JPEG',quality=78)
    return out.getvalue()


def figure(*,order=1,label='123',text='Huldigung im Schloßhof',pixels=None):
    return dict(physical_page_index=0,reading_order=order,
        jpeg=pixels or jpeg('#a44216'),caption_text=text,
        caption_region_ids=[],page_label=label)


def test_figure_is_at_accepted_text_position_with_exact_caption_and_page():
    values=inputs([('body','VOR DER ABBILDUNG.'),('body','NACH DER ABBILDUNG.')])
    values['regions'][1].reading_order=3
    pdf=render_source_unit_pdf(**values,illustrations=[
        figure(),figure(order=2,label='124',text='Zweite Originalunterschrift',pixels=jpeg('#0d4967'))])
    assert len(base64.b64encode(pdf))<RESULT_MAX_BYTES-4096
    with pymupdf.open(stream=pdf,filetype='pdf') as doc:
        assert sum(len(page.get_images()) for page in doc)==2
        text=''.join(p.get_text() for p in list(doc)[1:])
        assert text.index('VOR DER ABBILDUNG.')<text.index('Huldigung im Schloßhof')
        assert text.index('Huldigung im Schloßhof')<text.index('Zweite Originalunterschrift')
        assert text.index('Zweite Originalunterschrift')<text.index('NACH DER ABBILDUNG.')
        assert 'Исходная страница: 123' in text
        assert 'Исходная страница: 124' in text
        assert text.count('Huldigung im Schloßhof')==1
        assert 'Страница PDF' not in text
    with pymupdf.open(stream=render_source_unit_pdf(**values),filetype='pdf') as no_images:
        assert all(not page.get_images() for page in no_images)
        author=''.join(p.get_text() for p in list(no_images)[1:])
        assert flatten('VOR DER ABBILDUNG.NACH DER ABBILDUNG.') in flatten(author)
        assert 'Huldigung im Schloßhof' not in author


def test_crop_matches_existing_public_illustration_geometry_without_page_leak():
    page=Image.new('RGB',(1000,1000),'blue')
    ImageDraw.Draw(page).rectangle((100,200,399,599),fill='red')
    src=BytesIO();page.save(src,format='JPEG',quality=85)
    figure_meta=dict(bbox=dict(left=100,top=200,right=400,bottom=600),
        display_rotation_degrees=90)
    crop=_pdf_figure_crop(src.getvalue(),figure_meta,90000)
    with Image.open(BytesIO(crop)) as image:
        assert image.format=='JPEG' and image.size==(400,300)
        r,g,b=image.getpixel((200,150))
        assert r>185 and g<90 and b<90


def test_no_fake_picture_can_replace_unrenderable_accepted_figure():
    values=inputs([('figure','X')])
    with pytest.raises(SourceUnitPdfError,match='pdf_illustration_original_unavailable'):
        render_source_unit_pdf(**values)
