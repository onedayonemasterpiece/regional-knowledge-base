"""RKB-Риф: red-first regressions from DOT-REVIEW-177.

Source/fixture-level checks only: never claim authorization, PG, Kaggle live,
Telegram delivery, or model inference from these tests.
"""
from copy import deepcopy
from hashlib import sha256
from io import BytesIO
from types import SimpleNamespace as S
from uuid import uuid5

import pytest
from PIL import Image, ImageDraw

from regional_knowledge.model_source_provenance import build_model_source_provenance
from regional_knowledge.search_material import prepare_document_inputs
from regional_knowledge.source_model_manifest import build_model_manifest, verify_model_manifest
from regional_knowledge.source_figure_crop import crop_verified_source_figure
from test_model_source_provenance import make_scan

META = dict(title='Figure-only source', authors=[], publication_year=None,
            language='de', catalog={'kind': 'book'})


async def counter(texts):
    return [min(512, len(t.split()) + 2) for t in texts]


def figure_only():
    original, graph = make_scan(1, with_illustration=True)
    figure = graph.illustrations[0]
    page = graph.pages[0]
    page.regions = [r for r in page.regions if r.region_id == figure.source_region_id]
    figure.caption_region_ids = []
    chunk = graph.chunks[0]
    chunk.region_ids = [figure.source_region_id]
    chunk.text = ''
    return original, graph


@pytest.mark.asyncio
async def test_figure_only_page_without_caption_keeps_labelled_visual_material_not_invented_transcription():
    receipt, graph = figure_only()
    provenance, _ = build_model_source_provenance(receipt=receipt, graph=graph)
    assert provenance['chunks'][0]['source_body_sha256'] == sha256(b'').hexdigest()
    assert provenance['regions'][0]['source_text'] == ''
    rows = await prepare_document_inputs(graph, graph.chunks, counter)
    assert rows[0]['body'] == ''
    assert rows[0]['embedding_input'].startswith('[Model observation; not printed source text]')
    assert rows[0]['input_tokens'] >= 1
    manifest, _ = await build_model_manifest(
        original=receipt, graph=graph, metadata=META, tokenizer_counts=counter)
    assert manifest['passages'][0]['source_body_sha256'] == sha256(b'').hexdigest()


@pytest.mark.asyncio
async def test_figure_only_page_with_no_printed_or_model_material_fails_without_inventing_text():
    _, graph = figure_only()
    graph.illustrations[0].visual_description = None
    with pytest.raises(ValueError, match='search material'):
        await prepare_document_inputs(graph, graph.chunks, counter)


@pytest.mark.asyncio
async def test_rotation_is_part_of_manifest_and_changes_exact_digest_and_crop():
    original, graph = make_scan(1, with_illustration=True)
    graph.illustrations[0].display_rotation_degrees = 90
    manifest, digest = await build_model_manifest(
        original=original, graph=graph, metadata=META, tokenizer_counts=counter)
    figure = manifest['illustrations'][0]
    assert figure['display_rotation_degrees'] == 90
    raster = Image.new('RGB', (480, 320), 'white')
    ImageDraw.Draw(raster).rectangle((0, 0, 238, 319), fill=(245, 20, 20))
    raw = BytesIO(); raster.save(raw, format='PNG')
    desc, pixels = crop_verified_source_figure(
        figure=figure, source_page_bytes=raw.getvalue(),
        source_raster_sha256=sha256(raw.getvalue()).hexdigest(),
        source_page_id=figure['page_id'], original_sha256=original.sha256)
    assert desc['display_rotation_degrees'] == 90
    with Image.open(BytesIO(pixels)) as crop:
        assert crop.width < crop.height
    changed = deepcopy(graph)
    changed.illustrations[0].display_rotation_degrees = 0
    other, other_digest = await build_model_manifest(
        original=original, graph=changed, metadata=META, tokenizer_counts=counter)
    assert digest != other_digest
    assert other['illustrations'][0]['display_rotation_degrees'] == 0
    with pytest.raises(ValueError, match='drift'):
        verify_model_manifest(manifest, original=original, graph=changed,
                              metadata=META,
                              passage_inputs=await prepare_document_inputs(changed, changed.chunks, counter),
                              sha256=digest)


@pytest.mark.asyncio
async def test_footnote_region_ids_roundtrip_and_same_text_reference_changes_digest():
    original, graph = make_scan(1)
    page, chunk = graph.pages[0], graph.chunks[0]
    text = 'Same footnote printed twice.'
    for key in ('footnote-a', 'footnote-b'):
        page.regions.append(S(region_key=key,
                              region_id=uuid5(page.page_id, 'region:' + key),
                              page_id=page.page_id, kind='footnote',
                              bbox=dict(left=10, top=800, right=600, bottom=900),
                              source_text=text))
    first, second = page.regions[-2:]
    chunk.footnote_region_ids = [first.region_id]
    chunk.text += '\n[Footnote] ' + text
    first_manifest, first_digest = await build_model_manifest(
        original=original, graph=graph, metadata=META, tokenizer_counts=counter)
    assert first_manifest['passages'][0]['footnote_region_ids'] == [str(first.region_id)]
    assert first_manifest['passages'][0]['source_body_sha256'] == sha256(chunk.text.encode()).hexdigest()
    changed = deepcopy(graph)
    changed.chunks[0].footnote_region_ids = [second.region_id]
    second_manifest, second_digest = await build_model_manifest(
        original=original, graph=changed, metadata=META, tokenizer_counts=counter)
    assert second_manifest['passages'][0]['footnote_region_ids'] == [str(second.region_id)]
    assert second_manifest['passages'][0]['source_body_sha256'] == first_manifest['passages'][0]['source_body_sha256']
    assert first_digest != second_digest
    assert first_manifest != second_manifest


@pytest.mark.parametrize('orientation', [2, 3, 4, 6])
def test_exif_nonidentity_cannot_silently_shift_source_bbox_before_crop(orientation):
    """JPEG EXIF2/3/4 change geometry even when dimensions stay equal."""
    page = Image.new('RGB', (480, 320), 'white')
    pen = ImageDraw.Draw(page)
    pen.rectangle((0, 0, 238, 319), fill=(240, 20, 20))
    pen.rectangle((242, 0, 479, 319), fill=(20, 20, 240))
    exif = page.getexif()
    exif[274] = orientation
    output = BytesIO()
    page.save(output, format='JPEG', quality=95, exif=exif)
    raw = output.getvalue()
    pid = '10000000-0000-0000-0000-000000000001'
    figure = dict(illustration_id='20000000-0000-0000-0000-000000000002',
                  page_id=pid, bbox=dict(left=0, top=0, right=500, bottom=1000),
                  display_rotation_degrees=0)
    with pytest.raises(ValueError, match='EXIF'):
        crop_verified_source_figure(
            figure=figure, source_page_bytes=raw,
            source_raster_sha256=sha256(raw).hexdigest(),
            source_page_id=pid, original_sha256='f' * 64)

@pytest.mark.asyncio
async def test_cross_page_text_and_figure_are_linked_without_invented_page_coverage():
    """A text passage on page B may cite a source-verified figure on page A."""
    from regional_knowledge.search_material import graph_material
    original, graph = make_scan(2, with_illustration=True)
    figure = graph.illustrations[0]
    text_chunk = graph.chunks[1]
    assert str(figure.page_id) != str(text_chunk.page_ids[0])
    text_chunk.illustration_ids = [figure.illustration_id]
    provenance, digest = build_model_source_provenance(receipt=original, graph=graph)
    assert provenance['chunks'][1]['illustration_ids'] == [str(figure.illustration_id)]
    assert provenance['chunks'][1]['page_ids'] == [str(text_chunk.page_ids[0])]
    text, _ = graph_material(graph, text_chunk)
    assert text_chunk.text in text
    assert '[Model observation; not printed source text]' in text
    manifest, manifest_hash = await build_model_manifest(
        original=original, graph=graph, metadata=META, tokenizer_counts=counter)
    row = next(p for p in manifest['passages'] if p['passage_id'] == str(text_chunk.chunk_id))
    assert row['illustration_ids'] == [str(figure.illustration_id)]
    assert row['page_ids'] == [str(text_chunk.page_ids[0])]
    assert any(x['illustration_id'] == str(figure.illustration_id)
               and x['page_id'] == str(figure.page_id) for x in manifest['illustrations'])
    assert len(digest) == len(manifest_hash) == 64


@pytest.mark.parametrize('foreign', [False, True])
def test_cross_page_relation_denies_unknown_or_foreign_source_figure(foreign):
    from uuid import uuid4
    receipt, graph = make_scan(2, with_illustration=True)
    chunk = graph.chunks[1]
    if foreign:
        _, outside = make_scan(1, with_illustration=True)
        graph.illustrations.append(outside.illustrations[0])
        chunk.illustration_ids = [outside.illustrations[0].illustration_id]
    else:
        chunk.illustration_ids = [uuid4()]
    with pytest.raises(ValueError, match='figure|illustration'):
        build_model_source_provenance(receipt=receipt, graph=graph)
