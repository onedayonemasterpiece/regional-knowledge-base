"""Native figure rendering and mirror using the existing PG pools and Telegram lease."""
from __future__ import annotations

import asyncio
import base64
from dataclasses import asdict
from io import BytesIO
import json
import logging

from psycopg.types.json import Jsonb

from .kaggle_read_binding import TargetReadBinding
from .kaggle_public_illustration_native_read import NativePublicIllustrationReader
from .source_activation import media_crop_identity
from .source_figure_crop import crop_verified_source_figure
from .telegram_illustrations import NativeTelegramIllustrations
from .telegram_illustrations import IllustrationArchiveUnknown
from .original_intake import OriginalIntakeError, canonical, digest

ARCHIVE_SCOPE = 'rkb-native-illustrations-topic4'
_LOG = logging.getLogger(__name__)


class NativeIllustrationService:
    """One fixed operation; no model, connection pool, sender or state store of its own."""

    def __init__(self, intake, *, acquire, writer_acquire):
        self.intake = intake
        self.acquire, self.writer_acquire = acquire, writer_acquire

    def current(self, binding):
        owner = self.intake
        return (not owner.closed and binding.run_id == owner.identity[0]
                and binding.runtime_incarnation == owner.identity[2]
                and binding.connection_generation == owner.generation
                and owner.current((binding.run_id,binding.runtime_incarnation,
                                   binding.connection_generation)))

    async def __call__(self, frame, record):
        args = frame['arguments']
        if (set(args) != {'handle','original_sha256','source_id','illustration_id',
                          'actor_id','binding','pixels','nonce','recovery'}
                or type(args['pixels']) is not bool
                or record['spec']['source_id'] != args['source_id']
                or record['verified'] is None):
            raise OriginalIntakeError('public_illustration_contract')
        binding = TargetReadBinding(**args['binding'])
        if not self.current(binding):
            raise OriginalIntakeError('public_illustration_binding_changed')
        downloader = self.intake.telegram_downloader
        if downloader is None:
            raise OriginalIntakeError('original_telegram_binding_required')
        archive = NativeTelegramIllustrations(client=downloader._client,
            chat_id=downloader._chat_id,topic_id=4,
            current=lambda: self.current(binding) and downloader._current() is True,
            archive_scope=ARCHIVE_SCOPE)
        reader = NativePublicIllustrationReader(acquire=self.acquire,
            binding_current=self.current, archive_chat_id=archive.chat_id,
            archive_scope=ARCHIVE_SCOPE,packet_publications=True)
        source, figure, actor = args['source_id'],args['illustration_id'],args['actor_id']
        accepted = await reader.accepted_figure(source,figure,actor,binding)
        if accepted.archive_state['status'] != 'ready':
            await self._mirror(accepted,record,archive,binding,recovery=args['recovery'])
        snapshot = await reader(source,figure,actor,binding)
        _LOG.info('native_illustration phase=available run_id=%s source_id=%s illustration_id=%s',
            binding.run_id,source,figure)
        result = dict(snapshot=asdict(snapshot))
        if args['pixels']:
            proof = await archive.read_opaque(snapshot,actor)
            try:
                pixels = await archive.read_issued_bytes(proof)
                # The existing maintenance envelope is bounded. Emit an actual
                # crop preview, with its MIME, rather than truncating bytes or
                # changing the geometric scope. The registered photo is intact.
                data,mime = bounded_preview(pixels.data,pixels.mime_type)
                result.update(image_b64=base64.b64encode(data).decode('ascii'),
                              mime_type=mime)
            finally:
                archive.release_issued(proof)
        if not self.current(binding):
            raise OriginalIntakeError('public_illustration_binding_changed')
        return result

    async def _mirror(self, snapshot, record, archive, binding, *, recovery):
        source,figure = snapshot.source_id,snapshot.illustration_id
        crop_binding = snapshot.item['crop_binding']
        operation = 'illustration:'+source+':'+figure

        async def state(*, change=None, expected=None, locator=None):
            # Existing availability projection and durable maintenance dispatch
            # own recovery. UNKNOWN is committed before the external send.
            async with self.writer_acquire() as connection:
                async with connection.transaction():
                    async with connection.cursor() as cursor:
                        await cursor.execute('''SELECT true FROM rkb_v4.recovery_state r
                            JOIN rkb_v4.sources s ON s.source_id=%s::uuid
                            JOIN rkb_v4.active_revision a ON a.source_id=s.source_id
                            WHERE r.singleton AND r.database_generation=%s
                            AND r.writer_generation=%s AND s.accepted AND NOT s.withdrawn
                            AND EXISTS (SELECT 1 FROM rkb_v4.writer_generations w
                                WHERE w.generation=r.writer_generation AND NOT w.retired)
                            AND a.revision=1 FOR SHARE OF r''',
                            (source,self.intake.database.database_generation,
                             self.intake.database.writer_generation))
                        if await cursor.fetchone() is None or not self.current(binding):
                            raise OriginalIntakeError('public_illustration_source_changed')
                        await cursor.execute('''INSERT INTO rkb_v4.source_illustration_availability
                            (source_id,illustration_id,operation_key,binding_bytes,archive_chat_id,
                             archive_topic_id,version,status,photo,document)
                            VALUES(%s::uuid,%s::uuid,%s,%s,%s,4,0,'pending',NULL,NULL)
                            ON CONFLICT DO NOTHING''',
                            (source,figure,operation,canonical(crop_binding),archive.chat_id))
                        await cursor.execute('''SELECT version,status,photo,document,binding_bytes,
                            operation_key,archive_chat_id,archive_topic_id
                            FROM rkb_v4.source_illustration_availability
                            WHERE source_id=%s::uuid AND illustration_id=%s::uuid FOR UPDATE''',
                            (source,figure))
                        version,status,photo,document,stored,stored_operation,chat,topic = await cursor.fetchone()
                        if (media_crop_identity(json.loads(bytes(stored))) != media_crop_identity(crop_binding)
                                or chat != archive.chat_id or topic != 4):
                            raise OriginalIntakeError('public_illustration_geometry_changed')
                        if change:
                            if status not in expected:
                                return False
                            await cursor.execute('''UPDATE rkb_v4.source_illustration_availability
                                SET version=version+1,status=%s,photo=coalesce(%s,photo)
                                WHERE source_id=%s::uuid AND illustration_id=%s::uuid AND version=%s''',
                                (change,Jsonb(locator) if locator else None,source,figure,version))
                            return True
                        return dict(status=status,locator=photo or document,operation_key=stored_operation)

        async def load(key): return await state()
        async def begin(key):
            admitted=await state(change='unknown',expected=('pending',))
            _LOG.info('native_illustration phase=send_admitted run_id=%s illustration_id=%s admitted=%s',
                binding.run_id,figure,admitted)
            return admitted
        async def register(key,locator):
            if not await state(change='registered',expected=('unknown',),locator=locator):
                raise OriginalIntakeError('public_illustration_registration_changed')
            _LOG.info('native_illustration phase=registered run_id=%s illustration_id=%s',
                binding.run_id,figure)
        async def complete(key,locator):
            if not await state(change='ready',expected=('registered','ready'),locator=locator):
                raise OriginalIntakeError('public_illustration_availability_changed')

        current = await state()
        operation = current['operation_key']
        if current['status'] in ('pending','unknown') and recovery is not None:
            if recovery.get('status')!='ready':
                raise OriginalIntakeError('public_illustration_send_unknown')
            if media_crop_identity(recovery.get('binding'))!=media_crop_identity(crop_binding):
                raise OriginalIntakeError('public_illustration_geometry_changed')
            locator=recovery.get('photo')
            if locator is None:
                raise OriginalIntakeError('public_illustration_registered_photo_unavailable')
            locator=await archive.observe(locator)
            if not await state(change='registered',expected=('pending','unknown'),locator=locator):
                raise OriginalIntakeError('public_illustration_registration_changed')
            current=await state()
        crop = None
        if current['status'] == 'pending':
            page = await self.intake._parse_selected(record['path'],'page',
                snapshot.manifest_page['physical_page_index'],
                parser_sha256=record['parser_sha256'])
            image = base64.b64decode(page['image_b64'],validate=True)
            # The verified native original and accepted figure supply identity.
            # No Telegram rendition hash participates in mirror success.
            _,crop = crop_verified_source_figure(figure=snapshot.item,
                source_page_bytes=image,source_raster_sha256=digest(image),
                source_page_id=snapshot.item['page_id'],original_sha256=record['sha256'])
        try:
            await archive.send_one(operation_key=operation,crop=crop,mime_type='image/jpeg',
                kind='photo',load=load,begin=begin,register=register,complete=complete)
        except IllustrationArchiveUnknown:
            raise OriginalIntakeError('public_illustration_send_unknown') from None
        except Exception:
            if (await state())['status']=='unknown':
                raise OriginalIntakeError('public_illustration_send_unknown') from None
            raise


def bounded_preview(data, mime):
    """Preserve the exact crop frame while bounding a maintenance reply's bytes."""
    if len(data) <= 72*1024:
        return data,mime
    from PIL import Image
    with Image.open(BytesIO(data)) as source:
        image = source.convert('RGB')
        for edge in (1200,900,600,400):
            image.thumbnail((edge,edge))
            output = BytesIO()
            image.save(output,format='JPEG',quality=80,optimize=True)
            value = output.getvalue()
            if len(value) <= 72*1024:
                return value,'image/jpeg'
    raise OriginalIntakeError('public_illustration_preview_bound')
