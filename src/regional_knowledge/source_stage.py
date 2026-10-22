"""Exact inactive native source staging, separate from frozen import v1-v4.

These commands preserve accepted model inputs. They do not authorize a caller,
certify model transcription as native text, run a model, or activate a source.
The caller must authenticate ownership and obtain issued receipts through the
existing original controller before sealing through ControlSpool.
"""
from __future__ import annotations

import base64
import hashlib
import json
from uuid import UUID, uuid5

STAGE_ROUTE = 'source.stage.v1'
SEAL_ROUTE = 'source.stage.seal.v1'
STAGE_REVISIONS = ('rkb-v4-sql001-013-v1', 'rkb-v4-source-stage-v1', 'rkb-python-apply-source-stage-v1')
BATCH_CODEC = 'rkb.source-stage-batch.v1'
SEAL_CODEC = 'rkb.source-stage-seal.v1'
ZERO_HASH = '0' * 64
# Leaves room for ExactCommand's base64 payload and bounded identity metadata.
MAX_BATCH_BYTES = 700 * 1024
MAX_BATCH_PAGES = 8
MAX_SOURCE_PAGES = 2500  # Existing verified-original limit, not a batch limit.
MODEL_KEYS = frozenset({'pages', 'chunks', 'story_candidates', 'entity_candidates',
                        'poi_facts', 'poi_media_links'})


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'),
                      ensure_ascii=False, allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(value).hexdigest()


def encode_issued_image(receipt):
    """Serialize an already authenticated issued receipt, not create authority.

    The controller's self-hash is retained on the model page. Its exact hashed
    body is sent to PG, so SQL never needs to imitate Python JSON canonicality.
    """
    if type(receipt) is not dict or 'source_image_receipt_sha256' not in receipt:
        raise ValueError('existing issued image receipt required')
    raw = canonical({k: v for k, v in receipt.items() if k != 'source_image_receipt_sha256'})
    if digest(raw) != receipt['source_image_receipt_sha256']:
        raise ValueError('issued image receipt integrity differs')
    return base64.b64encode(raw).decode()


def _keys(value, fields):
    if type(value) is not dict or set(value) != set(fields.split()):
        raise ValueError('exact source stage fields required')


def _hash(value):
    if type(value) is not str or len(value) != 64 or set(value) - set('0123456789abcdef'):
        raise ValueError('exact source stage hash required')
    return value


def _uuid(value):
    if type(value) is not str or str(UUID(value)) != value:
        raise ValueError('canonical source stage identity required')
    return value


def _text(value, maximum):
    if type(value) is not str or not 1 <= len(value) <= maximum or '\x00' in value:
        raise ValueError('bounded source stage text required')


def _integer(value, minimum, maximum):
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError('bounded source stage integer required')


def _decoded(encoded, maximum):
    if type(encoded) is not str or len(encoded) > 4 * ((maximum + 2) // 3):
        raise ValueError('bounded exact encoded receipt required')
    raw = base64.b64decode(encoded, validate=True)
    if not 1 <= len(raw) <= maximum or base64.b64encode(raw).decode() != encoded:
        raise ValueError('exact encoded receipt required')
    doc = json.loads(raw)
    if canonical(doc) != raw:
        raise ValueError('canonical receipt bytes required')
    return raw, doc


def _research(value):
    _keys(value, 'frontier_id state continuation')
    _uuid(value['frontier_id'])
    if (value['state'] not in ('pending', 'complete')
            or type(value['continuation']) is not dict
            or len(canonical(value['continuation'])) > 65536
            or value['state'] == 'pending' and not value['continuation']):
        raise ValueError('exact reachable research continuation required')


def _reject_derived(value):
    if type(value) is dict:
        if {'embedding', 'embeddings', 'input_tokens'}.intersection(value):
            raise ValueError('native source stage cannot carry derived vectors or token counts')
        for item in value.values():
            _reject_derived(item)
    elif type(value) is list:
        for item in value:
            _reject_derived(item)


def validate_stage_payload(payload, *, source_id):
    """Validate the envelope, retain all model data without semantic lowering.

The model payload is the existing stage input (not an invented normalized text
or story). Full domain validation/materialization is required before any later
public activation. No successful result here means "imported" or "searchable".
"""
    from .original_intake import VerifiedOriginalReceipt
    from .contracts import StartMetadataInput
    _uuid(source_id)
    if type(payload) is not bytes or not 1 <= len(payload) <= MAX_BATCH_BYTES:
        raise ValueError('bounded exact source stage bytes required')
    try:
        doc = json.loads(payload)
        if canonical(doc) != payload:
            raise ValueError('canonical source stage bytes required')
        _keys(doc, 'codec ingestion_id batch_index previous_batch_sha256 source issued_images_b64 model_payload research')
        if doc['codec'] != BATCH_CODEC:
            raise ValueError('versioned source stage codec required')
        _uuid(doc['ingestion_id'])
        _integer(doc['batch_index'], 0, MAX_SOURCE_PAGES - 1)
        _hash(doc['previous_batch_sha256'])
        if (doc['batch_index'] == 0) != (doc['previous_batch_sha256'] == ZERO_HASH):
            raise ValueError('exact predecessor required')
        source = doc['source']
        _keys(source, 'bibliography work_id edition_id original_receipt_b64 public_rights metadata')
        _text(source['bibliography'], 8000)
        _uuid(source['work_id']); _uuid(source['edition_id'])
        metadata = source['metadata']
        if (type(metadata) is not dict or len(canonical(metadata)) > 65536
                or StartMetadataInput.model_validate(metadata).model_dump(mode='json') != metadata):
            raise ValueError('exact complete source catalog metadata required')
        if type(source['public_rights']) is not dict or not source['public_rights'] or len(canonical(source['public_rights'])) > 65536:
            raise ValueError('exact accepted public rights declaration required')
        raw_original, original = _decoded(source['original_receipt_b64'], 16384)
        receipt = VerifiedOriginalReceipt.from_wire(original)
        if receipt.source_id != source_id or receipt.ingestion_id != doc['ingestion_id']:
            raise ValueError('original source/ingestion binding required')
        model = doc['model_payload']
        if type(model) is not dict or not {'pages', 'chunks'} <= set(model) or set(model) - MODEL_KEYS:
            raise ValueError('existing exact model stage input required')
        pages = model['pages']; images = doc['issued_images_b64']
        if (type(pages) is not list or not 1 <= len(pages) <= MAX_BATCH_PAGES
                or type(images) is not list or len(images) != len(pages)):
            raise ValueError('one to eight selected issued pages required')
        for key, limit in (('chunks', 100), ('story_candidates', 12), ('poi_facts', 32), ('poi_media_links', 32)):
            if key in model and (type(model[key]) is not list or len(model[key]) > limit):
                raise ValueError('per-batch model input bound exceeded')
        if 'entity_candidates' in model and (type(model['entity_candidates']) is not dict
                or len(canonical(model['entity_candidates'])) > 256 * 1024):
            raise ValueError('bounded exact entity candidate bundle required')
        _reject_derived(model)
        seen = set()
        for page, encoded in zip(pages, images, strict=True):
            raw_image, image = _decoded(encoded, 8192)
            _keys(image, 'codec original_receipt_sha256 page_id physical_page_index original_sha256 parser_sha256 width height image_sha256 mime_type preview_render')
            if type(page) is not dict:
                raise ValueError('exact selected page required')
            index = image['physical_page_index']
            _integer(index, 0, receipt.page_count - 1)
            if index in seen:
                raise ValueError('duplicate physical page in batch')
            seen.add(index)
            expected_id = str(uuid5(UUID(source_id), f'page:1:{index}'))
            expected_hash = digest(raw_image)
            if (image['codec'] != 'rkb.issued-source-image.v1'
                    or image['original_receipt_sha256'] != digest(raw_original)
                    or image['original_sha256'] != receipt.sha256
                    or image['parser_sha256'] != receipt.parser_sha256
                    or image['page_id'] != expected_id or page.get('page_id') != expected_id
                    or page.get('physical_page_index') != index
                    or type(page.get('physical_page_index')) is not int
                    or page.get('source_image_receipt_sha256') != expected_hash
                    or image['mime_type'] != 'image/jpeg'
                    or page.get('source_material') != 'visual_reviewed'):
                raise ValueError('selected page/image/original provenance mismatch')
            _hash(image['image_sha256'])
            _text(page.get('source_review_note'), 500)
            if any(type(image[k]) not in (int, float) or not 0 < image[k] <= 100000 for k in ('width', 'height')):
                raise ValueError('bounded issued-image dimensions required')
            if type(image['preview_render']) is not dict or len(canonical(image['preview_render'])) > 4096:
                raise ValueError('exact issued render metadata required')
        _research(doc['research'])
        return doc
    except (KeyError, TypeError, UnicodeError, RecursionError, json.JSONDecodeError) as error:
        raise ValueError('invalid exact source stage payload') from error


def validate_seal_payload(payload):
    if type(payload) is not bytes or not 1 <= len(payload) <= 1024:
        raise ValueError('bounded exact candidate seal required')
    doc = json.loads(payload)
    if canonical(doc) != payload:
        raise ValueError('canonical candidate seal required')
    _keys(doc, 'codec ingestion_id batch_count last_batch_sha256')
    if doc['codec'] != SEAL_CODEC:
        raise ValueError('versioned inactive candidate seal required')
    _uuid(doc['ingestion_id']); _integer(doc['batch_count'], 1, MAX_SOURCE_PAGES)
    _hash(doc['last_batch_sha256'])
    if doc['last_batch_sha256'] == ZERO_HASH:
        raise ValueError('nonempty candidate chain required')
    return doc


def validate_stage_command(command):
    from .recovery_journal import ExactCommand
    if (type(command) is not ExactCommand or command.kind != 'domain_command'
            or command.route not in (STAGE_ROUTE, SEAL_ROUTE)
            or (command.schema_revision, command.function_revision, command.executor_revision) != STAGE_REVISIONS
            or set(command.ids) != {'source_id'}):
        raise ValueError('exact native source stage command required')
    _uuid(command.ids['source_id'])
    doc = (validate_stage_payload(command.payload, source_id=command.ids['source_id'])
           if command.route == STAGE_ROUTE else validate_seal_payload(command.payload))
    head = doc['previous_batch_sha256'] if command.route == STAGE_ROUTE else doc['last_batch_sha256']
    if command.expected_revisions != {'stage_head': head}:
        raise ValueError('exact accepted stage predecessor required')
    return doc


def resolved_stage_command(*, route, revisions, source_id, payload, **identity):
    from .kaggle_apply import Revisions
    from .recovery_journal import ExactCommand
    if type(revisions) is not Revisions or revisions.wire() != STAGE_REVISIONS or route not in (STAGE_ROUTE, SEAL_ROUTE):
        raise ValueError('explicit installed native stage binding required')
    head = payload['previous_batch_sha256'] if route == STAGE_ROUTE else payload['last_batch_sha256']
    command = ExactCommand(**identity, route=route, schema_revision=revisions.schema,
        function_revision=revisions.function, executor_revision=revisions.executor,
        ids={'source_id': source_id}, expected_revisions={'stage_head': head}, payload=canonical(payload))
    validate_stage_command(command)
    if len(command.encode()) > 1024 * 1024:
        raise ValueError('exact stage command exceeds existing replay envelope')
    return command


def stage_handler(*, route, revisions, check_sha256, apply_sha256):
    from .kaggle_apply import DependencySpec, HandlerSpec, PgFunction, Revisions
    if type(revisions) is not Revisions or revisions.wire() != STAGE_REVISIONS or route not in (STAGE_ROUTE, SEAL_ROUTE):
        raise ValueError('explicit installed native stage binding required')
    suffix = 'source_stage_v1' if route == STAGE_ROUTE else 'source_stage_seal_v1'
    # Conservative existing write-scope fence only; this does not assert that
    # the source exists in public PG projections. No new dependency framework.
    return HandlerSpec('domain_command', route, revisions,
        PgFunction('check_' + suffix, check_sha256), PgFunction('apply_' + suffix, apply_sha256),
        frozenset({'stage_head'}),
        dependency=DependencySpec(route, 'public_source', frozenset({'source_id'}), 'shared', False))


async def load_stage_batch(connection, *, source_id, ingestion_id, batch_index):
    """Bounded native-only read of one immutable accepted batch.

    Caller supplies its existing checked, generation-fenced PG connection. This
    returns source/model inputs, never vectors or an activation authorization.
    """
    _uuid(source_id); _uuid(ingestion_id); _integer(batch_index, 0, MAX_SOURCE_PAGES - 1)
    row = await (await connection.execute('''SELECT b.payload_bytes,b.payload_sha256
        FROM rkb_v4.source_stage_batches b JOIN rkb_v4.source_stage_sessions s USING(source_id)
        JOIN rkb_v4.applied_ops op ON op.sequence=b.accepted_sequence AND op.status='applied'
        WHERE b.source_id=%s AND s.ingestion_id=%s AND b.batch_index=%s''',
        (source_id, ingestion_id, batch_index))).fetchone()
    if row is None:
        return None
    raw = bytes(row[0])
    if digest(raw) != row[1]:
        raise ValueError('persisted accepted source batch integrity differs')
    doc = validate_stage_payload(raw, source_id=source_id)
    if doc['ingestion_id'] != ingestion_id or doc['batch_index'] != batch_index:
        raise ValueError('persisted accepted source batch identity differs')
    return raw


# SQL017 is additive: revision-1 accepted command bytes/pins remain unchanged.
REVISION2_STAGE_ROUTE = 'source.stage.v2'
REVISION2_SEAL_ROUTE = 'source.stage.seal.v2'
REVISION2_BATCH_CODEC = 'rkb.source-stage-batch.v2'
REVISION2_SEAL_CODEC = 'rkb.source-stage-seal.v2'


def validate_revision2_stage_payload(payload, *, source_id):
    """Retain old physical page/issued-image identity while minting a new intake.

    This is a *processing* revision, not a second source or second original.
    Reuse the exact v1 source/image validator on a temporary local view whose
    ingestion_id is the immutable original's ID. No accepted v1 bytes are
    modified; the actual v2 command persists only the original payload.
    """
    from .original_intake import VerifiedOriginalReceipt
    if type(payload) is not bytes or not 1 <= len(payload) <= MAX_BATCH_BYTES:
        raise ValueError('bounded exact revision2 stage required')
    doc = json.loads(payload)
    if canonical(doc) != payload:
        raise ValueError('revision2 stage must preserve canonical bytes')
    _keys(doc, 'codec source_revision prior_revision ingestion_id original_ingestion_id '
        'predecessor_command_sha256 batch_index previous_batch_sha256 source '
        'issued_images_b64 model_payload research')
    if (doc['codec'] != REVISION2_BATCH_CODEC or type(doc['source_revision']) is not int
            or doc['source_revision'] != 2 or type(doc['prior_revision']) is not int
            or doc['prior_revision'] != 1):
        raise ValueError('explicit revision1 to revision2 only')
    _uuid(doc['ingestion_id']); _uuid(doc['original_ingestion_id'])
    if doc['ingestion_id'] == doc['original_ingestion_id']:
        raise ValueError('successor requires a distinct retained operation')
    _hash(doc['predecessor_command_sha256'])
    raw_original, original = _decoded(doc['source']['original_receipt_b64'], 16384)
    receipt = VerifiedOriginalReceipt.from_wire(original)
    if receipt.source_id != source_id or receipt.ingestion_id != doc['original_ingestion_id']:
        raise ValueError('same original/source actor binding required')
    # The issued page IDs are original-source identities, not new work IDs.
    view = {key:value for key,value in doc.items() if key not in
        {'source_revision','prior_revision','original_ingestion_id','predecessor_command_sha256'}}
    view['codec'] = BATCH_CODEC
    view['ingestion_id'] = doc['original_ingestion_id']
    validate_stage_payload(canonical(view),source_id=source_id)
    return doc


def validate_revision2_seal_payload(payload):
    doc=json.loads(payload)
    if canonical(doc)!=payload:
        raise ValueError('canonical revision2 seal required')
    _keys(doc,'codec source_revision prior_revision ingestion_id batch_count last_batch_sha256 predecessor_command_sha256')
    if doc['codec']!=REVISION2_SEAL_CODEC or type(doc['source_revision']) is not int or doc['source_revision']!=2 \
            or type(doc['prior_revision']) is not int or doc['prior_revision']!=1:
        raise ValueError('explicit revision2 seal required')
    _hash(doc['predecessor_command_sha256'])
    validate_seal_payload(canonical({**{k:v for k,v in doc.items() if k not in
        {'source_revision','prior_revision','predecessor_command_sha256'}},'codec':SEAL_CODEC}))
    return doc


def validate_revision2_stage_command(command):
    from .recovery_journal import ExactCommand
    from .kaggle_native_revision2_upgrade import REVISION2_REVISIONS
    if (type(command) is not ExactCommand or command.kind!='domain_command'
            or command.route not in (REVISION2_STAGE_ROUTE,REVISION2_SEAL_ROUTE)
            or (command.schema_revision,command.function_revision,command.executor_revision)!=REVISION2_REVISIONS.wire()
            or set(command.ids)!={'source_id'}):
        raise ValueError('exact installed revision2 stage command required')
    doc=(validate_revision2_stage_payload(command.payload,source_id=command.ids['source_id'])
        if command.route==REVISION2_STAGE_ROUTE else validate_revision2_seal_payload(command.payload))
    head=(doc['previous_batch_sha256'] if command.route==REVISION2_STAGE_ROUTE else doc['last_batch_sha256'])
    if command.expected_revisions != dict(active_revision='1',stage_head=head,
            predecessor_command_sha256=doc['predecessor_command_sha256'],owner_person_id=command.person_id):
        raise ValueError('exact owner, previous active revision and stage predecessor required')
    return doc


def resolved_revision2_stage_command(*,route,source_id,payload,revisions=None,**identity):
    from .kaggle_native_revision2_upgrade import REVISION2_REVISIONS
    from .recovery_journal import ExactCommand
    if revisions is None:revisions=REVISION2_REVISIONS
    if revisions!=REVISION2_REVISIONS or route not in (REVISION2_STAGE_ROUTE,REVISION2_SEAL_ROUTE):
        raise ValueError('versioned revision2 stage route required')
    head=payload['previous_batch_sha256'] if route==REVISION2_STAGE_ROUTE else payload['last_batch_sha256']
    command=ExactCommand(**identity,route=route,schema_revision=revisions.schema,function_revision=revisions.function,
        executor_revision=revisions.executor,ids={'source_id':source_id},
        expected_revisions=dict(active_revision='1',stage_head=head,
            predecessor_command_sha256=payload['predecessor_command_sha256'],owner_person_id=identity['person_id']),
        payload=canonical(payload))
    validate_revision2_stage_command(command)
    if len(command.encode())>1024*1024:raise ValueError('revision2 replay envelope too large')
    return command


def revision2_stage_handler(*,route,check_sha256,apply_sha256):
    from .kaggle_native_revision2_upgrade import REVISION2_REVISIONS
    from .kaggle_apply import HandlerSpec,PgFunction,DependencySpec
    if route not in (REVISION2_STAGE_ROUTE,REVISION2_SEAL_ROUTE):
        raise ValueError('exact revision2 stage handler required')
    suffix='source_stage_revision2_v1' if route==REVISION2_STAGE_ROUTE else 'source_stage_seal_revision2_v1'
    return HandlerSpec('domain_command',route,REVISION2_REVISIONS,
        PgFunction('check_'+suffix,check_sha256),PgFunction('apply_'+suffix,apply_sha256),
        frozenset({'active_revision','stage_head','predecessor_command_sha256','owner_person_id'}),
        dependency=DependencySpec(route,'public_source',frozenset({'source_id'}),'shared',False))


async def load_revision2_stage_batch(connection,*,source_id,ingestion_id,batch_index):
    _uuid(source_id);_uuid(ingestion_id);_integer(batch_index,0,MAX_SOURCE_PAGES-1)
    row=await (await connection.execute('''SELECT b.payload_bytes,b.payload_sha256
        FROM rkb_v4.source_revision2_batches b
        JOIN rkb_v4.source_revision2_sessions s USING(source_id)
        JOIN rkb_v4.applied_ops op ON op.sequence=b.accepted_sequence AND op.status='applied'
        WHERE b.source_id=%s AND s.ingestion_id=%s AND b.batch_index=%s''',
        (source_id,ingestion_id,batch_index))).fetchone()
    if row is None:return None
    raw=bytes(row[0])
    if digest(raw)!=row[1]:raise ValueError('immutable accepted revision2 batch SHA changed')
    doc=validate_revision2_stage_payload(raw,source_id=source_id)
    if doc['ingestion_id']!=ingestion_id or doc['batch_index']!=batch_index:
        raise ValueError('accepted revision2 source/revision identity changed')
    return raw
