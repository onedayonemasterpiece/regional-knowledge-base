"""Actual pixels via a verified source raster, not only caption/locator text.

Does not claim source authorization, Kaggle archive access or Telegram delivery.
"""
from copy import deepcopy
from hashlib import sha256
from io import BytesIO
from uuid import uuid4

import pytest
from PIL import Image, ImageDraw

from regional_knowledge.source_figure_crop import crop_verified_source_figure


def fixture():
    page = Image.new('RGB',(480,320),'white')
    draw = ImageDraw.Draw(page)
    draw.rectangle((0,0,238,319),fill=(240,20,20))
    draw.rectangle((242,0,479,319),fill=(20,20,240))
    raw=BytesIO();page.save(raw,format='JPEG',quality=96)
    data=raw.getvalue()
    pid=str(uuid4()); iid=str(uuid4())
    figure=dict(illustration_id=iid,page_id=pid,
                bbox=dict(left=0,top=0,right=500,bottom=1000),
                source_region_id=str(uuid4()),caption_region_ids=[],
                visual_description_provenance='model_observation')
    kwargs=dict(figure=figure,source_page_bytes=data,source_raster_sha256=sha256(data).hexdigest(),
                source_page_id=pid, original_sha256='f'*64)
    return kwargs


def test_delivers_real_left_hand_source_pixels_not_figure_description():
    args=fixture()
    desc, raw=crop_verified_source_figure(**args)
    with Image.open(BytesIO(raw)) as pic:
        assert pic.format=='JPEG'
        assert pic.size==(240,320)
        r,g,b=pic.getpixel((60,160))
        assert r>180 and b<70 and g<70
    assert desc['mime_type']=='image/jpeg'
    assert desc['bytes_sha256']==sha256(raw).hexdigest()
    assert desc['rendition']=='preview'
    assert desc['source_raster_sha256']==args['source_raster_sha256']
    assert desc['source_pixel_bounds']==[0,0,240,320]
    assert desc['source_frame']=='reviewed_page_normalized_bbox'


def test_right_hand_figure_and_rotation_do_not_modify_original_geometry():
    args=fixture()
    args['figure']['bbox']=dict(left=500,top=0,right=1000,bottom=500)
    args['figure']['display_rotation_degrees']=90
    old_bbox=deepcopy(args['figure']['bbox'])
    desc,raw=crop_verified_source_figure(**args)
    with Image.open(BytesIO(raw)) as pic:
        assert pic.size==(160,240)
        r,g,b=pic.getpixel((80,110))
        assert b>150 and r<80 and g<80
    assert desc['crop_pixels']==[160,240]
    assert args['figure']['bbox']==old_bbox
    assert desc['rendition']=='preview'


@pytest.mark.parametrize('corruption',[
    'changed_pixels','wrong_digest','wrong_original','wrong_page','missing_page',
    'outside','empty_bbox','float_bbox','wrong_rotation','wrong_rendition',
    'no_pixels','unsupported_image','oversized_image','bad_budget','highres_without_proof',
])
def test_invalid_source_raster_or_geometry_never_returns_pixels(corruption):
    args=fixture()
    if corruption=='changed_pixels':args['source_page_bytes']+=b'X'
    elif corruption=='wrong_digest':args['source_raster_sha256']='0'*64
    elif corruption=='wrong_original':args['original_sha256']='invalid'
    elif corruption=='wrong_page':args['source_page_id']=str(uuid4())
    elif corruption=='missing_page':args['figure']['page_id']=''
    elif corruption=='outside':args['figure']['bbox']['right']=1001
    elif corruption=='empty_bbox':args['figure']['bbox']['right']=0
    elif corruption=='float_bbox':args['figure']['bbox']['left']=0.25
    elif corruption=='wrong_rotation':args['figure']['display_rotation_degrees']=45
    elif corruption=='wrong_rendition':args['rendition']='Telegram_photo_verified_by_SHA'
    elif corruption=='highres_without_proof':args['rendition']='verified_high_resolution'
    elif corruption=='no_pixels':args['source_page_bytes']=b''
    elif corruption=='unsupported_image':
        raw=BytesIO();Image.new('RGB',(20,20)).save(raw,format='BMP')
        args['source_page_bytes']=raw.getvalue()
        args['source_raster_sha256']=sha256(args['source_page_bytes']).hexdigest()
    elif corruption=='oversized_image':args['max_source_pixels']=1
    elif corruption=='bad_budget':args['max_output_bytes']=-1
    with pytest.raises(ValueError):
        crop_verified_source_figure(**args)


def test_png_source_raster_renders_jpeg_correct_mime_and_bytes():
    args=fixture()
    with Image.open(BytesIO(args['source_page_bytes'])) as pic:
        tmp=BytesIO();pic.save(tmp,format='PNG')
    args['source_page_bytes']=tmp.getvalue()
    args['source_raster_sha256']=sha256(args['source_page_bytes']).hexdigest()
    desc,raw=crop_verified_source_figure(**args)
    assert raw[:3]==b'\xff\xd8\xff'
    assert desc['mime_type']=='image/jpeg'
