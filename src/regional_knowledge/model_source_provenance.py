"""Immutable source identity for model-read pages, including image-only scans.

This is a *staging provenance contract*, not a remote-durability receipt.
It contains neither proof that the model actually saw a page nor an authorization
decision. The caller must supply a previously verified original receipt and a
fully validated, owner-authorized model stage in the existing Kaggle runtime.
"""
from __future__ import annotations

import hashlib
import json
from uuid import UUID, uuid5


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'),
                      ensure_ascii=False, allow_nan=False).encode('utf-8')


def _sha(value):
    return hashlib.sha256(value).hexdigest()


def _kind(value):
    return getattr(value, 'value', value)


def build_model_source_provenance(*, receipt, graph):
    """Bind reviewed model regions to the exact verified original, not native OCR.

    No native-text equality or page-render pass is performed. Accepted-source
    activation remains a separate, versioned PostgreSQL command with durability
    and current authority checks; never treat this return value as Saved.
    """
    source_id = str(UUID(receipt.source_id))
    if (str(UUID(receipt.ingestion_id)) != receipt.ingestion_id
            or receipt.format not in ('pdf', 'djvu')
            or type(receipt.page_count) is not int
            or not 1 <= receipt.page_count <= 2500
            or type(receipt.sha256) is not str
            or len(receipt.sha256) != 64
            or set(receipt.sha256) - set('0123456789abcdef')
            or type(receipt.receipt_sha256) is not str
            or len(receipt.receipt_sha256) != 64
            or set(receipt.receipt_sha256) - set('0123456789abcdef')):
        raise ValueError('verified original identity required')
    if graph.revision != 1:
        raise ValueError('unsupported source review revision')
    if len(graph.pages) != receipt.page_count:
        raise ValueError('complete physical page coverage required')
    page_map = {}
    regions = {}
    seen_indexes = set()
    pages = []
    for page in graph.pages:
        index = page.physical_page_index
        if (type(index) is not int or index < 0 or index >= receipt.page_count
                or index in seen_indexes or str(page.page_id) != str(
                    uuid5(UUID(source_id), f'page:1:{index}'))):
            raise ValueError('staged model page must match original identity and order')
        seen_indexes.add(index)
        if (page.source_material != 'visual_reviewed'
                or not (page.source_review_note or '').strip()):
            raise ValueError('model page must be explicitly visually reviewed')
        page_id = str(page.page_id)
        page_map[page_id] = page
        pages.append(dict(page_id=page_id, physical_page_index=index,
                          original_sha256=receipt.sha256,
                          observation_basis='model_client_visual_review',
                          review_note_sha256=_sha(page.source_review_note.encode())))
        for region in page.regions:
            rid = str(region.region_id)
            expected_region = str(uuid5(UUID(page_id), f'region:{region.region_key}'))
            if rid != expected_region or rid in regions or str(region.page_id) != page_id:
                raise ValueError('source region identity or page conflict')
            if not str(region.source_text).strip() and _kind(region.kind) != 'figure':
                raise ValueError('non-figure model source text is empty')
            bbox = region.bbox.model_dump() if hasattr(region.bbox, 'model_dump') else region.bbox
            if (type(bbox) is not dict or set(bbox) != {'left','top','right','bottom'}
                    or any(type(v) is not int or not 0 <= v <= 1000 for v in bbox.values())
                    or bbox['left'] >= bbox['right'] or bbox['top'] >= bbox['bottom']):
                raise ValueError('reviewed model region bbox invalid')
            regions[rid] = dict(region_id=rid, page_id=page_id, kind=_kind(region.kind),
                                bbox=bbox, source_text_sha256=_sha(region.source_text.encode()),
                                transcription_basis='model_visual_review',
                                source_text=region.source_text)
    if seen_indexes != set(range(receipt.page_count)):
        raise ValueError('missing physical pages in reviewed source')

    figures = {}
    for figure in graph.illustrations:
        iid, page_id = str(figure.illustration_id), str(figure.page_id)
        expected_figure_id = str(uuid5(UUID(page_id), f'illustration:{figure.illustration_key}'))
        if iid != expected_figure_id:
            raise ValueError('figure identity must bind source page and key')
        src = regions.get(str(figure.source_region_id))
        if (iid in figures or page_id not in page_map or src is None
                or src['kind'] != 'figure' or src['page_id'] != page_id
                or src['bbox'] != (figure.bbox.model_dump() if hasattr(figure.bbox, 'model_dump') else figure.bbox)):
            raise ValueError('figure must have an exact source figure region and page')
        captions = []
        for ref in figure.caption_region_ids:
            c = regions.get(str(ref))
            if c is None or c['page_id'] != page_id or c['kind'] != 'caption':
                raise ValueError('printed caption must belong to original figure page')
            captions.append(str(ref))
        if figure.visual_description is not None and figure.visual_description_provenance != 'model_observation':
            raise ValueError('visual description is not printed source text')
        rotation = getattr(figure, 'display_rotation_degrees', 0)
        if type(rotation) is not int or rotation not in (0, 90, 180, 270):
            raise ValueError('invalid original-linked figure presentation rotation')
        figures[iid] = dict(illustration_id=iid, page_id=page_id,
                            source_region_id=str(figure.source_region_id),
                            caption_region_ids=captions,
                            visual_description=figure.visual_description,
                            visual_description_provenance=('model_observation'
                                if figure.visual_description is not None else None),
                            bbox=src['bbox'],
                            display_rotation_degrees=rotation)
    chunks = {}
    page_index = {str(p.page_id): p.physical_page_index for p in graph.pages}
    for chunk in graph.chunks:
        key = str(chunk.chunk_id)
        if key in chunks:
            raise ValueError('duplicate chunk identity')
        pids = [str(p) for p in chunk.page_ids]
        rids = [str(r) for r in chunk.region_ids]
        iids = [str(i) for i in chunk.illustration_ids]
        if (not pids or not rids or any(p not in page_map for p in pids)
                or any(r not in regions or regions[r]['page_id'] not in pids for r in rids)
                or any(i not in figures or figures[i]['page_id'] not in pids for i in iids)
                # A figure-only page has no printed text. Use exact labelled
                # model description/caption as indexed material, not invented
                # transcription; material() still rejects fully empty input.
                or (not chunk.text.strip() and not iids)):
            raise ValueError('chunk must bind reviewed source regions and linked figures')
        spans=[]
        reconstructed=[]
        for span in chunk.source_spans if hasattr(chunk,'source_spans') else ():
            region = regions.get(str(span.region_id))
            if (region is None or str(span.region_id) not in rids
                    or span.source_text_sha256 != region['source_text_sha256']
                    or not 0 <= span.start < span.end <= len(region['source_text'])):
                raise ValueError('source span must bind the exact reviewed transcription')
            reconstructed.append(region['source_text'][span.start:span.end].strip())
            spans.append(dict(region_id=str(span.region_id), start=span.start,end=span.end,
                              source_text_sha256=span.source_text_sha256))
        if not spans:
            reconstructed = [regions[r]['source_text'].strip() for r in rids
                             if regions[r]['source_text'].strip()]
        footnote_ids = [str(fid) for fid in getattr(chunk, 'footnote_region_ids', [])]
        if len(footnote_ids) != len(set(footnote_ids)) or set(footnote_ids).intersection(rids):
            raise ValueError('chunk footnote references must be unique and separate')
        for footnote_id in footnote_ids:
            footnote = regions.get(footnote_id)
            if (footnote is None or footnote['kind'] != 'footnote'
                    or footnote['page_id'] not in pids):
                raise ValueError('chunk footnote must bind reviewed source')
            reconstructed.append('[Footnote] ' + footnote['source_text'].strip())
        if '\n'.join(reconstructed).strip() != chunk.text.strip():
            raise ValueError('chunk text differs from model-reviewed source spans')
        chunks[key] = dict(chunk_id=key, page_ids=pids,
                           region_ids=rids, footnote_region_ids=footnote_ids,
                           illustration_ids=iids, source_spans=spans,
                           source_body_sha256=_sha(chunk.text.encode()))
    ordered_chunks=sorted(chunks.values(), key=lambda v:(
        min(page_index[p] for p in v['page_ids']), v['chunk_id']))
    for ordinal, chunk in enumerate(ordered_chunks):
        chunk['ordinal']=ordinal
    data = dict(codec='rkb.source-model-provenance.v1',
                source_id=source_id, source_revision=1,
                original_receipt_sha256=receipt.receipt_sha256,
                original_sha256=receipt.sha256, format=receipt.format,
                page_count=receipt.page_count,
                pages=sorted(pages, key=lambda v:v['physical_page_index']),
                regions=sorted(regions.values(), key=lambda v:v['region_id']),
                illustrations=sorted(figures.values(), key=lambda v:v['illustration_id']),
                chunks=ordered_chunks)
    return data, _sha(_canonical(data))
