"""Verified byte-level figure crop for an authorized RKB source-page rendition.

This codec is not an independent archive, fetch route, public proof grant, or
storage adapter. Its caller must first authorize the current actor's purpose-
scoped illustration access and obtain a trusted original-bound page rendition
on Kaggle. `source_raster_sha256` is that *source rendition's* integrity check,
not a demand for Telegram photo readback equality. Raster previews are labelled
as previews; high-resolution delivery requires independently verified source
bytes and must never be inferred from an enlarged preview.
"""
from __future__ import annotations

from hashlib import sha256
from io import BytesIO
import math

from PIL import Image, UnidentifiedImageError

DEFAULT_MAX_SOURCE_PIXELS = 12_000_000
DEFAULT_MAX_OUTPUT_BYTES = 8 * 1024 * 1024


def crop_verified_source_figure(
    *, figure: dict, source_page_bytes: bytes, source_raster_sha256: str,
    source_page_id: str, original_sha256: str, rendition: str = 'preview',
    max_source_pixels: int = DEFAULT_MAX_SOURCE_PIXELS,
    max_output_bytes: int = DEFAULT_MAX_OUTPUT_BYTES,
):
    """Return actual JPEG pixels and a source-bound descriptor, no network I/O.

    Geometric bbox is normalized (0..1000) in the *reviewed page frame*;
    presentation rotation is applied to the crop after geometric localization.
    Do not confuse a source JPEG checksum with provider Telegram recompression.
    """
    if (type(figure) is not dict
            or type(source_page_bytes) is not bytes or not source_page_bytes
            or type(source_raster_sha256) is not str
            or len(source_raster_sha256) != 64
            or set(source_raster_sha256) - set('0123456789abcdef')
            or sha256(source_page_bytes).hexdigest() != source_raster_sha256
            or type(original_sha256) is not str or len(original_sha256) != 64
            or set(original_sha256) - set('0123456789abcdef')
            or not source_page_id or figure.get('page_id') != source_page_id
            or type(figure.get('illustration_id')) is not str
            or not figure.get('illustration_id')):
        raise ValueError('trusted source page and figure binding required')
    # No caller-provided word can upgrade a preview into an original-resolution
    # document. A verified high-resolution service path needs a separate
    # original-rendition receipt and its own validation before using this codec.
    if rendition != 'preview':
        raise ValueError('preview rendition only; high-resolution proof required')
    if (type(max_source_pixels) is not int or type(max_output_bytes) is not int
            or not 1 <= max_source_pixels <= 48_000_000
            or not 1024 <= max_output_bytes <= 64 * 1024 * 1024):
        raise ValueError('bounded figure decoding budget required')
    bbox = figure.get('bbox')
    if (type(bbox) is not dict or set(bbox) != {'left','top','right','bottom'}
            or any(type(v) is not int or not 0 <= v <= 1000 for v in bbox.values())
            or bbox['left'] >= bbox['right'] or bbox['top'] >= bbox['bottom']):
        raise ValueError('source figure bbox is invalid')
    rotation = figure.get('display_rotation_degrees', 0)
    if type(rotation) is not int or rotation not in (0, 90, 180, 270):
        raise ValueError('invalid presentation rotation')

    try:
        with Image.open(BytesIO(source_page_bytes)) as page:
            width, height = page.size
            if (not 1 <= width <= 20_000 or not 1 <= height <= 20_000
                    or width * height > max_source_pixels):
                raise ValueError('source rendition exceeds figure budget')
            if page.format not in ('JPEG','PNG','WEBP'):
                raise ValueError('source rendition image format unsupported')
            # bbox is in reviewed source pixel coordinates. EXIF 2/3/4
            # changes pixel orientation without changing image dimensions;
            # this codec must not silently normalize it after verification.
            # A new reviewed-frame receipt must be created upstream instead.
            if page.getexif().get(274, 1) != 1:
                raise ValueError('source rendition EXIF orientation requires upstream normalization')
            image = page.convert('RGB')
            bounds = (
                math.floor(bbox['left'] * width / 1000),
                math.floor(bbox['top'] * height / 1000),
                math.ceil(bbox['right'] * width / 1000),
                math.ceil(bbox['bottom'] * height / 1000),
            )
            if not (0 <= bounds[0] < bounds[2] <= width
                    and 0 <= bounds[1] < bounds[3] <= height):
                raise ValueError('nonempty source figure region required')
            crop = image.crop(bounds)
            if rotation:
                crop = crop.rotate(-rotation, expand=True)
            buffer = BytesIO()
            crop.save(buffer, format='JPEG', quality=92, optimize=True)
            result = buffer.getvalue()
    except (UnidentifiedImageError, OSError, Image.DecompressionBombError):
        raise ValueError('source rendition pixels unavailable') from None
    if not 1 <= len(result) <= max_output_bytes:
        raise ValueError('rendered figure output exceeds budget')
    descriptor = dict(illustration_id=figure['illustration_id'],
        page_id=source_page_id, original_sha256=original_sha256,
        source_raster_sha256=source_raster_sha256, rendition=rendition,
        source_pixel_bounds=list(bounds), original_page_pixels=[width, height],
        display_rotation_degrees=rotation,
        crop_pixels=[crop.width, crop.height],
        bytes_sha256=sha256(result).hexdigest(), byte_length=len(result),
        mime_type='image/jpeg', source_frame='reviewed_page_normalized_bbox')
    return descriptor, result
