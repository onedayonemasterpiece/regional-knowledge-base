"""Versioned image-origin activation; no native-text or vector transport claim.

Accepted commands pin the exact model-input batch chain and materializer code.
The native worker derives knowledge and embeddings beside the existing PG/BGE.
Only canonical knowledge (not floating-point embeddings) enters the outcome hash.
"""
from __future__ import annotations

import base64
import copy
import hashlib
import json
from uuid import UUID, uuid5

from .kaggle_apply import DependencySpec, HandlerSpec, PgFunction, Revisions
from .recovery_journal import ExactCommand
from .source_manifest import _bbox, _metadata, _optional
from .source_stage import canonical, digest, _keys, _uuid, _hash, _integer

ACTIVATE_ROUTE = 'source.activate.v1'
MATERIALIZE_ROUTE = 'source.materialize.v1'
MATERIALIZE_CODEC = 'rkb.source-materialize.v1'
ACTIVATION_CODEC = 'rkb.source-activate.v1'
PROJECTION_CODEC = 'rkb.source-activation-projection.v1'
IMAGE_MANIFEST_CODEC = 'rkb.source-image-manifest.v1'
ACTIVATION_REVISIONS = Revisions('rkb-v4-sql001-014-v1',
    'rkb-v4-native-source-activation-v1', 'rkb-python-apply-native-source-v1')
ACTIVATION_DEPENDENCY = DependencySpec(ACTIVATE_ROUTE, 'public_source', frozenset({'source_id'}), 'shared', False)
MAX_KNOWLEDGE_BYTES = 32 * 1024 * 1024
PAGE_KEYS = ('page_id physical_page_index printed_page_number width height source_image_receipt_sha256 '
             'issued_image_receipt_b64 image_sha256 preview_render source_material source_review_note')
REGION_KEYS = 'region_id region_key page_id kind bbox reading_order column_id text_sha256 source_text_provenance'


def knowledge_bytes(resolved):
    """Drop only derived embedding fields, preserving actual text/token counts."""
    value = copy.deepcopy(resolved)
    if type(value) is not dict or value.get('codec') != PROJECTION_CODEC:
        raise ValueError('exact native activation projection required')
    for passage in value['passages']:
        passage.pop('embedding', None)
    for story in value['semantic']['stories']:
        story['default_variant'].pop('embedding', None)
    raw = canonical(value)
    if not 1 <= len(raw) <= MAX_KNOWLEDGE_BYTES:
        raise ValueError('native activation knowledge capacity exceeded')
    return raw


def validate_activation_payload(payload):
    if type(payload) is not bytes or not 1 <= len(payload) <= 2048:
        raise ValueError('bounded native activation command required')
    doc = json.loads(payload)
    if canonical(doc) != payload:
        raise ValueError('canonical activation payload required')
    _keys(doc, 'codec ingestion_id batch_count last_batch_sha256 materializer_sha256')
    if doc['codec'] != ACTIVATION_CODEC:
        raise ValueError('explicit source activation codec required')
    _uuid(doc['ingestion_id']); _integer(doc['batch_count'], 1, 2500)
    _hash(doc['last_batch_sha256']); _hash(doc['materializer_sha256'])
    return doc


def validate_activation_command(command):
    if (type(command) is not ExactCommand or command.kind != 'domain_command'
            or command.route != ACTIVATE_ROUTE or set(command.ids) != {'source_id'}
            or command.expected_revisions != {'absent_source': 'true'}
            or (command.schema_revision, command.function_revision, command.executor_revision) != ACTIVATION_REVISIONS.wire()):
        raise ValueError('exact installed native source activation required')
    _uuid(command.ids['source_id'])
    return validate_activation_payload(command.payload)


def resolved_activation_command(*, sequence, operation_key, request_id, person_id,
                                logical_time, source_id, payload, revisions=ACTIVATION_REVISIONS):
    if revisions != ACTIVATION_REVISIONS:
        raise ValueError('explicit native source activation revisions required')
    command = ExactCommand(sequence=sequence, operation_key=operation_key, request_id=request_id,
        person_id=person_id, logical_time=logical_time, route=ACTIVATE_ROUTE,
        schema_revision=revisions.schema, function_revision=revisions.function,
        executor_revision=revisions.executor, ids={'source_id': source_id},
        expected_revisions={'absent_source': 'true'}, payload=canonical(payload))
    validate_activation_command(command)
    return command


def activation_handler(*, check_sha256, apply_sha256):
    return HandlerSpec('domain_command', ACTIVATE_ROUTE, ACTIVATION_REVISIONS,
        PgFunction('check_source_activate_v1', check_sha256),
        PgFunction('apply_source_activate_v1', apply_sha256), frozenset({'absent_source'}),
        dependency=ACTIVATION_DEPENDENCY)


def validate_image_manifest(manifest, *, source_id, original, semantic, passages, source_revision=1):
    from .original_intake import VerifiedOriginalReceipt
    _keys(manifest, 'codec source_id source_revision original_receipt_b64 metadata pages regions passages')
    if manifest['codec'] != IMAGE_MANIFEST_CODEC or manifest['source_id'] != _uuid(source_id) or manifest['source_revision'] != source_revision or type(source_revision) is not int or source_revision not in (1,2):
        raise ValueError('exact image-origin manifest identity required')
    raw = base64.b64decode(manifest['original_receipt_b64'], validate=True)
    receipt = VerifiedOriginalReceipt.from_wire(json.loads(raw))
    if canonical(receipt.wire()) != raw or receipt.source_id != source_id or receipt.original != original:
        raise ValueError('exact verified original receipt binding required')
    _metadata(manifest['metadata'])
    pages = manifest['pages']
    if type(pages) is not list or len(pages) != receipt.page_count or not 1 <= len(pages) <= 2500:
        raise ValueError('explicit complete reviewed page coverage required')
    known_pages = set()
    for index, page in enumerate(pages):
        _keys(page, PAGE_KEYS)
        expected = str(uuid5(UUID(source_id), f'page:1:{index}'))
        if (page['page_id'] != expected or type(page['physical_page_index']) is not int
                or page['physical_page_index'] != index or page['source_material'] != 'visual_reviewed'
                or not isinstance(page['source_review_note'], str) or not 1 <= len(page['source_review_note']) <= 500):
            raise ValueError('actual selected reviewed page identity required')
        image_raw = base64.b64decode(page['issued_image_receipt_b64'], validate=True)
        image = json.loads(image_raw)
        if (canonical(image) != image_raw or digest(image_raw) != page['source_image_receipt_sha256']
                or image.get('codec') != 'rkb.issued-source-image.v1'
                or image.get('page_id') != expected or image.get('physical_page_index') != index
                or image.get('original_receipt_sha256') != receipt.receipt_sha256
                or image.get('original_sha256') != receipt.sha256 or image.get('parser_sha256') != receipt.parser_sha256
                or any(page[key] != image.get(key) for key in ('width','height','image_sha256','preview_render'))):
            raise ValueError('issued image bytes differ from selected original/page')
        _optional(page['printed_page_number'], 80)
        known_pages.add(expected)
    accepted = {r['region_id']: r for r in semantic['regions']}
    regions = manifest['regions']
    if type(regions) is not list or len(regions) != len(accepted):
        raise ValueError('complete model-transcription region provenance required')
    seen = set()
    for region in regions:
        _keys(region, REGION_KEYS)
        ident = _uuid(region['region_id']); exact = accepted.get(ident)
        if ident != str(uuid5(UUID(region['page_id']), 'region:'+region['region_key'])):
            raise ValueError('exact deterministic staged region identity required')
        if (ident in seen or exact is None or region['page_id'] not in known_pages
                or region['page_id'] != exact['page_id'] or region['text_sha256'] != exact['source_text_sha256']
                or region['source_text_provenance'] != 'model_transcription'):
            raise ValueError('exact model-authored region binding required')
        seen.add(ident); _hash(region['text_sha256']); _bbox(region['bbox'])
        _integer(region['reading_order'], 0, 2**31 - 1); _optional(region['column_id'], 120)
    accepted_passages = {p['passage_id']: p for p in passages}; seen = set()
    if len(manifest['passages']) != len(accepted_passages):
        raise ValueError('complete source passage provenance required')
    for passage in manifest['passages']:
        _keys(passage, 'passage_id region_ids footnote_region_ids page_ids source_spans')
        ident = _uuid(passage['passage_id'])
        if ident in seen or ident not in accepted_passages:
            raise ValueError('unique exact passage provenance required')
        seen.add(ident)
        for key, known, bound in (('region_ids',accepted,100),('footnote_region_ids',accepted,50),('page_ids',known_pages,20)):
            refs = passage[key]
            if (type(refs) is not list or len(refs) > bound or len(set(refs)) != len(refs)
                    or any(ref not in known for ref in refs)):
                raise ValueError('exact selected passage references required')
        refs = passage['region_ids']; footnotes = passage['footnote_region_ids']
        if (not refs or not passage['page_ids'] or set(refs) & set(footnotes)
                or {accepted[r]['page_id'] for r in refs + footnotes} != set(passage['page_ids'])):
            raise ValueError('complete passage page/region binding required')
        spans = passage['source_spans']; parts = []; selected = []; latest = {}
        if type(spans) is not list or len(spans) > 100:
            raise ValueError('bounded exact model source spans required')
        for span in spans:
            _keys(span, 'region_id start end source_text_sha256')
            region = accepted.get(span['region_id'])
            if region is None or span['region_id'] not in refs or span['source_text_sha256'] != region['source_text_sha256']:
                raise ValueError('exact source span region required')
            _integer(span['start'], 0, len(region['source_text'])); _integer(span['end'], 1, len(region['source_text']))
            if span['start'] >= span['end'] or latest.get(span['region_id'], 0) > span['start']:
                raise ValueError('ordered nonoverlapping source spans required')
            latest[span['region_id']] = span['end']; selected.append(span['region_id'])
            parts.append(region['source_text'][span['start']:span['end']].strip())
        if spans and set(selected) != set(refs):
            raise ValueError('exact source span coverage required')
        if not spans:
            parts = [accepted[r]['source_text'].strip() for r in refs if accepted[r]['source_text'].strip()]
        parts.extend('[Footnote] ' + accepted[r]['source_text'].strip() for r in footnotes if accepted[r]['source_text'].strip())
        if '\n'.join(parts).strip() != accepted_passages[ident]['body']:
            raise ValueError('passage must reconstruct exact accepted model transcription')
    return manifest


def build_image_manifest(*, metadata, receipt, graph, semantic, passages, issued_images, source_revision=1):
    """No renderer, native text extraction, model inference or original fetch."""
    from .contracts import StartMetadataInput
    from .original_intake import VerifiedOriginalReceipt
    from .stage_graph import StagedGraph
    from .source_stage import encode_issued_image
    if type(metadata) is not StartMetadataInput or type(receipt) is not VerifiedOriginalReceipt or not isinstance(graph, StagedGraph):
        raise TypeError('typed image provenance inputs required')
    images = {i['page_id']: i for i in issued_images}
    pages = []; regions = []
    for page in sorted(graph.pages, key=lambda p:p.physical_page_index):
        image = images.get(str(page.page_id))
        if image is None or image['source_image_receipt_sha256'] != page.source_image_receipt_sha256:
            raise ValueError('exact issued image metadata required')
        pages.append(dict(page_id=str(page.page_id), physical_page_index=page.physical_page_index,
            printed_page_number=page.printed_page_number, width=image['width'], height=image['height'],
            source_image_receipt_sha256=page.source_image_receipt_sha256,
            issued_image_receipt_b64=encode_issued_image(image), image_sha256=image['image_sha256'],
            preview_render=image['preview_render'], source_material=page.source_material,
            source_review_note=page.source_review_note))
        for region in page.regions:
            regions.append(dict(region_id=str(region.region_id), region_key=region.region_key, page_id=str(page.page_id), kind=region.kind.value,
                bbox=region.bbox.model_dump(), reading_order=region.reading_order, column_id=region.column_id,
                text_sha256=digest(region.source_text.encode()), source_text_provenance='model_transcription'))
    manifest = dict(codec=IMAGE_MANIFEST_CODEC, source_id=receipt.source_id, source_revision=source_revision,
        original_receipt_b64=base64.b64encode(canonical(receipt.wire())).decode(),
        metadata={k:v for k,v in metadata.model_dump(mode='json').items() if k in ('title','authors','publication_year','language','catalog')},
        pages=pages, regions=sorted(regions,key=lambda r:r['region_id']),
        passages=[dict(passage_id=str(c.chunk_id),region_ids=list(map(str,c.region_ids)),
            footnote_region_ids=list(map(str,c.footnote_region_ids)),page_ids=list(map(str,c.page_ids)),
            source_spans=[s.model_dump(mode='json') for s in c.source_spans]) for c in graph.chunks])
    return validate_image_manifest(manifest,source_id=receipt.source_id,original=receipt.original,semantic=semantic,passages=passages,source_revision=source_revision)


def validate_materialize_payload(payload):
    if type(payload) is not bytes or not 1 <= len(payload) <= 2048:
        raise ValueError('bounded native materialization command required')
    doc = json.loads(payload)
    if canonical(doc) != payload:
        raise ValueError('canonical materialization command required')
    _keys(doc, 'codec ingestion_id batch_count last_batch_sha256 materializer_sha256 ordinal')
    if doc['codec'] != MATERIALIZE_CODEC:
        raise ValueError('explicit native materialization codec required')
    _integer(doc['ordinal'], 0, 279999)
    validate_activation_payload(canonical({k: (ACTIVATION_CODEC if k == 'codec' else v)
        for k, v in doc.items() if k != 'ordinal'}))
    return doc


def validate_materialize_command(command):
    if (type(command) is not ExactCommand or command.kind != 'domain_command'
            or command.route != MATERIALIZE_ROUTE or set(command.ids) != {'source_id'}
            or command.expected_revisions != {'absent_source': 'true'}
            or (command.schema_revision, command.function_revision, command.executor_revision) != ACTIVATION_REVISIONS.wire()):
        raise ValueError('exact installed native source materialization required')
    _uuid(command.ids['source_id'])
    return validate_materialize_payload(command.payload)


def resolved_materialize_command(*, sequence, operation_key, request_id, person_id,
                                 logical_time, source_id, payload, revisions=ACTIVATION_REVISIONS):
    if revisions != ACTIVATION_REVISIONS:
        raise ValueError('explicit native source materialization revisions required')
    command = ExactCommand(sequence=sequence, operation_key=operation_key, request_id=request_id,
        person_id=person_id, logical_time=logical_time, route=MATERIALIZE_ROUTE,
        schema_revision=revisions.schema, function_revision=revisions.function,
        executor_revision=revisions.executor, ids={'source_id': source_id},
        expected_revisions={'absent_source': 'true'}, payload=canonical(payload))
    validate_materialize_command(command)
    return command


def materialize_handler(*, check_sha256, apply_sha256):
    return HandlerSpec('domain_command', MATERIALIZE_ROUTE, ACTIVATION_REVISIONS,
        PgFunction('check_source_materialize_v1', check_sha256),
        PgFunction('apply_source_materialize_v1', apply_sha256), frozenset({'absent_source'}),
        dependency=DependencySpec(MATERIALIZE_ROUTE, 'public_source', frozenset({'source_id'}), 'shared', False))


# SQL017: additive processing-revision commands; rev1 accepted bytes untouched.
REVISION2_ACTIVATE_ROUTE='source.activate.v2'
REVISION2_MATERIALIZE_ROUTE='source.materialize.v2'
REVISION2_ACTIVATION_CODEC='rkb.source-activate.v2'
REVISION2_MATERIALIZE_CODEC='rkb.source-materialize.v2'


def validate_revision2_activation_payload(payload, *, materialize=False):
    if type(payload) is not bytes or not 1<=len(payload)<=2048:
        raise ValueError('bounded exact revision2 activation required')
    doc=json.loads(payload)
    if canonical(doc)!=payload:
        raise ValueError('exact canonical revision2 activation required')
    codec=REVISION2_MATERIALIZE_CODEC if materialize else REVISION2_ACTIVATION_CODEC
    fields='codec source_revision prior_revision ingestion_id batch_count last_batch_sha256 materializer_sha256 predecessor_command_sha256'
    if materialize:fields+=' ordinal'
    _keys(doc,fields)
    if (doc['codec']!=codec or type(doc['source_revision']) is not int or doc['source_revision']!=2
        or type(doc['prior_revision']) is not int or doc['prior_revision']!=1):
        raise ValueError('revision2 requires exact revision1 predecessor')
    _uuid(doc['ingestion_id']);_integer(doc['batch_count'],1,2500)
    _hash(doc['last_batch_sha256']);_hash(doc['materializer_sha256'])
    _hash(doc['predecessor_command_sha256'])
    if materialize:_integer(doc['ordinal'],0,279999)
    return doc


def validate_revision2_activation_command(command):
    from .kaggle_native_revision2_upgrade import REVISION2_REVISIONS
    if (type(command) is not ExactCommand or command.kind!='domain_command'
        or command.route not in (REVISION2_ACTIVATE_ROUTE,REVISION2_MATERIALIZE_ROUTE)
        or set(command.ids)!={'source_id'}
        or (command.schema_revision,command.function_revision,command.executor_revision)!=REVISION2_REVISIONS.wire()):
        raise ValueError('installed revision2 activation command required')
    _uuid(command.ids['source_id'])
    doc=validate_revision2_activation_payload(command.payload,
        materialize=command.route==REVISION2_MATERIALIZE_ROUTE)
    if command.expected_revisions!=dict(active_revision='1',
        predecessor_command_sha256=doc['predecessor_command_sha256'],owner_person_id=command.person_id):
        raise ValueError('exact previous source and owner required')
    return doc


def resolved_revision2_activation_command(*,route,source_id,payload,revisions=None,**identity):
    from .kaggle_native_revision2_upgrade import REVISION2_REVISIONS
    if revisions is None:revisions=REVISION2_REVISIONS
    if revisions!=REVISION2_REVISIONS or route not in (REVISION2_ACTIVATE_ROUTE,REVISION2_MATERIALIZE_ROUTE):
        raise ValueError('one supported revision2 materialization/activation route required')
    command=ExactCommand(**identity,route=route,schema_revision=revisions.schema,function_revision=revisions.function,
        executor_revision=revisions.executor,ids={'source_id':source_id},
        expected_revisions=dict(active_revision='1',owner_person_id=identity['person_id'],
            predecessor_command_sha256=payload['predecessor_command_sha256']),
        payload=canonical(payload))
    validate_revision2_activation_command(command)
    return command


def revision2_activation_handler(*,route,check_sha256,apply_sha256):
    from .kaggle_native_revision2_upgrade import REVISION2_REVISIONS
    if route not in (REVISION2_ACTIVATE_ROUTE,REVISION2_MATERIALIZE_ROUTE):
        raise ValueError('revision2 only')
    suffix='source_activate_revision2_v1' if route==REVISION2_ACTIVATE_ROUTE else 'source_materialize_revision2_v1'
    return HandlerSpec('domain_command',route,REVISION2_REVISIONS,
        PgFunction('check_'+suffix,check_sha256),PgFunction('apply_'+suffix,apply_sha256),
        frozenset({'active_revision','predecessor_command_sha256','owner_person_id'}),
        dependency=DependencySpec(route,'public_source',frozenset({'source_id'}),'shared',False))
