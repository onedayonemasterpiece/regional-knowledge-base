"""Durable source catalogue and native page provenance, without original bytes.

The constructor consumes trusted intake receipts; the pure validator does not
turn a caller's geometry or hash into a native verification claim.
"""
from __future__ import annotations

import base64
import hashlib
import json
import math
from uuid import UUID, uuid5

from .source_import import _hash, _json, _text, _uuid

MANIFEST_CODEC = 'rkb.source-manifest.v1'
MAX_MANIFEST_BYTES = 512 * 1024
PAGE_KEYS = 'page_id physical_page_index printed_page_number width height page_receipt_sha256 image_sha256 native_text_sha256 native_blocks_sha256 preview_render'
REGION_KEYS = 'region_id page_id kind bbox reading_order column_id text_sha256 native_block_index native_block_sha256 start_offset end_offset'
KINDS = frozenset('heading body caption footnote figure table header footer page_number marginalia'.split())


def _keys(value, keys):
    if type(value) is not dict or set(value) != set(keys.split()):
        raise ValueError('exact source manifest object required')


def _integer(value, minimum, maximum):
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError('bounded source manifest integer required')


def _optional(value, maximum):
    if value is not None:
        _text(value, maximum, empty=True)


def _bbox(value):
    _keys(value, 'left top right bottom')
    for coordinate in value.values():
        _integer(coordinate, 0, 1000)
    if value['left'] >= value['right'] or value['top'] >= value['bottom']:
        raise ValueError('positive normalized native region required')


def _metadata(value):
    _keys(value, 'title authors publication_year language catalog')
    if len(_json(value)) > 16384:
        raise ValueError('bounded exact source metadata required')
    _optional(value['title'], 500)
    _optional(value['language'], 80)
    if type(value['authors']) is not list or len(value['authors']) > 50:
        raise ValueError('bounded exact authors required')
    for author in value['authors']:
        _text(author, 4000, empty=True)
    if value['publication_year'] is not None:
        _integer(value['publication_year'], 1, 3000)
    # Catalogue values are preserved verbatim. Dependency-bearing components
    # are not supported by the initial new-book workflow, never dropped.
    catalog = value['catalog']
    if catalog is not None:
        _keys(catalog, 'kind parent_id region_ids subtitle editors translators compilers publisher date edition volume issue identifiers form purpose searchable topics annotation annotation_origin annotation_page cover_page cover_kind physical_page_start physical_page_end')
        if catalog['kind'] != 'book' or catalog['parent_id'] is not None or catalog['region_ids'] != []:
            raise ValueError('source component dependencies require a separate contract')
        for key, bound in (('subtitle',500),('publisher',500),('date',80),('edition',100),('volume',100),
                           ('issue',100),('form',100),('purpose',100),('annotation',4000)):
            _optional(catalog[key], bound)
        for key in ('editors','translators','compilers','topics'):
            if type(catalog[key]) is not list or len(catalog[key]) > 50:
                raise ValueError('bounded catalogue list required')
            for text in catalog[key]:
                _text(text, 4000, empty=True)
        if type(catalog['searchable']) is not bool:
            raise ValueError('explicit catalogue searchability required')
        if type(catalog['identifiers']) is not list or len(catalog['identifiers']) > 30:
            raise ValueError('bounded exact identifiers required')
        for item in catalog['identifiers']:
            if type(item) is not dict or len(item) > 30:
                raise ValueError('bounded identifier object required')
            for key, text in item.items():
                _text(key, 4000, empty=True); _text(text, 4000, empty=True)
        if catalog['annotation_origin'] not in (None,'source','user','model') or (catalog['annotation'] and not catalog['annotation_origin']):
            raise ValueError('exact annotation provenance required')
        if catalog['cover_kind'] not in (None,'cover','title_page','issue_cover'):
            raise ValueError('exact cover kind required')
        for key in ('annotation_page','cover_page','physical_page_start','physical_page_end'):
            if catalog[key] is not None:
                _integer(catalog[key], 0, 2**31-1)
        if catalog['cover_page'] is not None and catalog['cover_kind'] is None:
            raise ValueError('cover provenance required')
        if (catalog['physical_page_start'] is not None and catalog['physical_page_end'] is not None
                and catalog['physical_page_start'] > catalog['physical_page_end']):
            raise ValueError('ordered physical page range required')


def validate_source_manifest(manifest, *, source_id, original, semantic, passages):
    """Check bounded, complete bindings; no model, transport, or authority."""
    _keys(manifest, 'codec source_id source_revision original_receipt_b64 native_pages_proof_sha256 metadata pages regions passages')
    if len(_json(manifest)) > MAX_MANIFEST_BYTES or manifest['codec'] != MANIFEST_CODEC:
        raise ValueError('bounded exact source manifest required')
    if _uuid(manifest['source_id']) != _uuid(source_id):
        raise ValueError('source manifest identity differs')
    _integer(manifest['source_revision'], 1, 1)
    _hash(manifest['native_pages_proof_sha256'])
    encoded = manifest['original_receipt_b64']
    if type(encoded) is not str or len(encoded) > 16384:
        raise ValueError('bounded original receipt required')
    try:
        raw = base64.b64decode(encoded, validate=True)
        receipt = json.loads(raw)
    except (ValueError, TypeError):
        raise ValueError('exact original receipt encoding required') from None
    if _json(receipt) != raw or base64.b64encode(raw).decode() != encoded:
        raise ValueError('canonical original receipt required')
    _keys(receipt, 'codec source_id ingestion_id request_id revision sha256 size key native_version format page_count parser_sha256 acquisition_sha256 native_verification_sha256')
    if (receipt['codec'] != 'rkb.original-receipt.v1' or receipt['source_id'] != source_id
            or type(receipt['revision']) is not int or receipt['revision'] != 1 or receipt['format'] != 'pdf'):
        raise ValueError('exact native PDF receipt required')
    for key in ('source_id','ingestion_id','request_id'):
        _uuid(receipt[key])
    for key in ('sha256','parser_sha256','acquisition_sha256','native_verification_sha256'):
        _hash(receipt[key])
    _integer(receipt['size'], 1, 512*1024*1024)
    _integer(receipt['page_count'], 1, 32)
    _text(receipt['key'], 1024); _text(receipt['native_version'], 512)
    if '://' in receipt['key'] or any(part in ('','.','..') for part in receipt['key'].split('/')):
        raise ValueError('stable original locator required')
    if original != dict(sha256=receipt['sha256'],size=receipt['size'],ref='archive:'+receipt['key'],
                        receipt_sha256=hashlib.sha256(raw).hexdigest()):
        raise ValueError('verified original receipt binding differs')
    _metadata(manifest['metadata'])
    pages = manifest['pages']
    if type(pages) is not list or len(pages) != receipt['page_count']:
        raise ValueError('complete native page inventory required')
    known_pages = set()
    for index, page in enumerate(pages):
        _keys(page, PAGE_KEYS)
        if (type(page['physical_page_index']) is not int or page['physical_page_index'] != index
                or page['page_id'] != str(uuid5(UUID(source_id), f'page:1:{index}'))):
            raise ValueError('exact physical page identity/order required')
        known_pages.add(page['page_id']); _optional(page['printed_page_number'], 80)
        for key in ('width','height'):
            if type(page[key]) not in (int,float) or not math.isfinite(page[key]) or not 0 < page[key] <= 100000:
                raise ValueError('native PDF point dimensions required')
        for key in ('page_receipt_sha256','image_sha256','native_text_sha256','native_blocks_sha256'):
            _hash(page[key])
        render = page['preview_render']
        _keys(render, 'policy width height scale max_edge jpeg_quality byte_limit preview_only')
        if render['policy'] != 'rkb.pdf-preview.v1' or render['preview_only'] is not True or render['byte_limit'] != 256*1024:
            raise ValueError('exact bounded preview provenance required')
        for key in ('width','height','max_edge'):
            _integer(render[key], 1, 20000)
        _integer(render['jpeg_quality'], 1, 100)
        if type(render['scale']) not in (int,float) or not math.isfinite(render['scale']) or not 0 < render['scale'] <= 100:
            raise ValueError('finite actual render scale required')
    regions = manifest['regions']
    accepted = {r['region_id']: r for r in semantic['regions']}
    if type(regions) is not list or not 1 <= len(regions) <= 800 or len(regions) != len(accepted):
        raise ValueError('complete semantic region provenance required')
    seen = set()
    for region in regions:
        _keys(region, REGION_KEYS)
        ident = _uuid(region['region_id']); exact = accepted.get(ident)
        if (ident in seen or exact is None or region['page_id'] not in known_pages
                or region['page_id'] != exact['page_id'] or region['text_sha256'] != exact['source_text_sha256']):
            raise ValueError('exact accepted source region binding required')
        seen.add(ident); _hash(region['text_sha256']); _hash(region['native_block_sha256'])
        if region['kind'] not in KINDS:
            raise ValueError('known source region kind required')
        _bbox(region['bbox']); _optional(region['column_id'], 120)
        for key in ('reading_order','native_block_index','start_offset','end_offset'):
            _integer(region[key], 0, 2**31-1)
        if region['end_offset']-region['start_offset'] != len(exact['source_text']) or not exact['source_text'].strip():
            raise ValueError('exact Unicode native span required')
    passage_map=manifest['passages'];accepted_passages={p['passage_id']:p for p in passages}
    if type(passage_map) is not list or not 1<=len(passage_map)<=100 or len(passage_map)!=len(accepted_passages):
        raise ValueError('complete source passage provenance required')
    seen=set()
    for passage in passage_map:
        _keys(passage,'passage_id region_ids footnote_region_ids page_ids source_spans')
        ident=_uuid(passage['passage_id'])
        if ident in seen or ident not in accepted_passages:
            raise ValueError('exact unique passage provenance required')
        seen.add(ident)
        for key,known,bound in (('region_ids',accepted,100),('footnote_region_ids',accepted,50),('page_ids',known_pages,20)):
            refs=passage[key]
            if (type(refs) is not list or len(refs)>bound or len(set(refs))!=len(refs)
                    or any(ref not in known for ref in refs)):
                raise ValueError('exact passage references required')
        regions=passage['region_ids'];footnotes=passage['footnote_region_ids']
        if (not regions or not passage['page_ids'] or set(regions).intersection(footnotes)
                or {accepted[r]['page_id'] for r in regions+footnotes}!=set(passage['page_ids'])):
            raise ValueError('complete source passage page/region mapping required')
        spans=passage['source_spans'];parts=[];selected=[];latest={}
        if type(spans) is not list or len(spans)>100:
            raise ValueError('bounded source spans required')
        for span in spans:
            _keys(span,'region_id start end source_text_sha256')
            region=accepted.get(span['region_id'])
            if region is None or span['region_id'] not in regions or span['source_text_sha256']!=region['source_text_sha256']:
                raise ValueError('exact source span region required')
            _integer(span['start'],0,len(region['source_text']));_integer(span['end'],1,len(region['source_text']))
            if span['start']>=span['end'] or latest.get(span['region_id'],0)>span['start']:
                raise ValueError('ordered nonoverlapping source spans required')
            latest[span['region_id']]=span['end'];selected.append(span['region_id'])
            parts.append(region['source_text'][span['start']:span['end']].strip())
        if spans and set(selected)!=set(regions):
            raise ValueError('exact source span coverage required')
        if not spans:parts=[accepted[r]['source_text'].strip() for r in regions if accepted[r]['source_text'].strip()]
        parts.extend('[Footnote] '+accepted[r]['source_text'].strip() for r in footnotes if accepted[r]['source_text'].strip())
        if '\n'.join(parts).strip()!=accepted_passages[ident]['body']:
            raise ValueError('source passage must reconstruct from exact accepted regions')
    return manifest


def build_source_manifest(*, metadata, receipt, proof, graph, semantic, passages):
    """Resolve physical provenance from verified full native page receipts."""
    from .contracts import StartMetadataInput
    from .original_intake import NativePagesProof, VerifiedOriginalReceipt
    from .stage_graph import StagedGraph
    if (type(metadata) is not StartMetadataInput or type(receipt) is not VerifiedOriginalReceipt
            or type(proof) is not NativePagesProof or type(graph) is not StagedGraph
            or proof.original_receipt_sha256 != receipt.receipt_sha256):
        raise ValueError('trusted typed source manifest inputs required')
    if metadata.catalog_update is not None or metadata.duplicate_policy != 'reuse':
        raise ValueError('new source metadata required')
    staged = {str(page.page_id): page for page in graph.pages}
    pages=[]; regions=[]
    for native in proof.pages:
        if (native['page_receipt_sha256'] != hashlib.sha256(_json({k:v for k,v in native.items() if k!='page_receipt_sha256'})).hexdigest()
                or native['original_sha256'] != receipt.sha256 or native['parser_sha256'] != receipt.parser_sha256
                or native['page_count'] != receipt.page_count or native['format'] != receipt.format):
            raise ValueError('native page receipt differs')
        info=native['native_text_info']; blocks=native['native_blocks']
        if (info.get('complete') is not True or info.get('truncated') is not False
                or info.get('blocks_truncated') is not False
                or hashlib.sha256(native['native_text'].encode()).hexdigest() != info['native_text_sha256']
                or hashlib.sha256(_json([{k:b[k] for k in ('block_index','bbox','text')} for b in blocks])).hexdigest() != info['native_blocks_sha256']):
            raise ValueError('complete native page proof required')
        page = staged.pop(native['page_id'], None)
        if page and page.physical_page_index!=native['physical_page_index']:
            raise ValueError('staged physical page index differs')
        pages.append(dict(page_id=native['page_id'],physical_page_index=native['physical_page_index'],
            printed_page_number=page.printed_page_number if page else None,width=native['width'],height=native['height'],
            page_receipt_sha256=native['page_receipt_sha256'],image_sha256=native['image_sha256'],
            native_text_sha256=info['native_text_sha256'],native_blocks_sha256=info['native_blocks_sha256'],
            preview_render=native['preview_render']))
        for region in page.regions if page else ():
            matches=[b for b in blocks if b['bbox']==region.bbox.model_dump() and not b['truncated']
                and b['original_length']==len(b['text']) and b['text'].count(region.source_text)==1
                and hashlib.sha256(b['text'].encode()).hexdigest()==b['text_sha256']]
            if len(matches)!=1:
                raise ValueError('unique exact native block required')
            block=matches[0];start=block['text'].index(region.source_text)
            regions.append(dict(region_id=str(region.region_id),page_id=str(region.page_id),kind=region.kind.value,
                bbox=region.bbox.model_dump(),reading_order=region.reading_order,column_id=region.column_id,
                text_sha256=hashlib.sha256(region.source_text.encode()).hexdigest(),native_block_index=block['block_index'],
                native_block_sha256=block['text_sha256'],start_offset=start,end_offset=start+len(region.source_text)))
    if staged:
        raise ValueError('staged page has no native proof')
    manifest=dict(codec=MANIFEST_CODEC,source_id=receipt.source_id,source_revision=1,
        original_receipt_b64=base64.b64encode(_json(receipt.wire())).decode(),native_pages_proof_sha256=proof.sha256,
        metadata={k:v for k,v in metadata.model_dump(mode='json').items() if k in ('title','authors','publication_year','language','catalog')},
        pages=sorted(pages,key=lambda p:p['physical_page_index']),regions=sorted(regions,key=lambda r:r['region_id']),
        passages=[dict(passage_id=str(c.chunk_id),region_ids=list(map(str,c.region_ids)),
            footnote_region_ids=list(map(str,c.footnote_region_ids)),page_ids=list(map(str,c.page_ids)),
            source_spans=[s.model_dump(mode='json') for s in c.source_spans]) for c in graph.chunks])
    return validate_source_manifest(manifest,source_id=receipt.source_id,original=receipt.original,semantic=semantic,passages=passages)
