"""Narrow topic-scoped media adapter borrowing the existing leased native client.

No client construction, credentials or extra uploader. Native send_one still
uses presence-only verification; a distinct owner-bound read_issued_bytes path
uses this SAME leased client's pre-existing bounded iter_download to deliver
real crop pixels. No downloaded bytes are used as a send success/ACK claim.
Durable effect state belongs to the existing source operation store.
An UNKNOWN send without a registered locator is NEVER blindly sent again.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
import hashlib
from io import BytesIO
import json
import secrets
import time
from uuid import UUID
from .source_activation import media_crop_identity


class IllustrationArchiveUnknown(RuntimeError):
    pass


class IllustrationArchiveReadUnavailable(PermissionError):
    """Current native archive/media read fails closed without private detail."""


@dataclass(frozen=True, eq=False)
class NativeIllustrationArchiveProof:
    """Native owner-local object: NOT serializable as a Dev IAM capability.

    A later trusted WSS bridge must retain its own Dev ArchiveOpaqueProof and
    map only verified source-scoped native results. The native object identity
    is never accepted as a bearer from a public client.
    """

    ref: str
    source_id: str
    illustration_id: str
    actor_id: str = field(repr=False)
    binding_sha256: str
    archive_scope: str = field(repr=False)
    owner: object = field(repr=False, compare=False)


@dataclass(frozen=True, eq=False)
class NativeIllustrationIssuedBytes:
    """Real bytes from the original leased Telethon client, never SQL metadata."""

    proof: NativeIllustrationArchiveProof = field(repr=False)
    data: bytes = field(repr=False)
    mime_type: str
    sha256: str


def _canonical(value):
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"),
                          ensure_ascii=False, allow_nan=False).encode("utf-8")
    except (TypeError, ValueError, OverflowError, RecursionError):
        raise IllustrationArchiveReadUnavailable("native_illustration_binding_invalid") from None


def _uuid(value):
    if type(value) is not str:
        return False
    try:
        return str(UUID(value)) == value
    except ValueError:
        return False


class NativeTelegramIllustrations:
    def __init__(self, *, client, chat_id, topic_id, current,
                 archive_scope=None, clock=time.monotonic,
                 proof_ttl_seconds=10.0, max_proofs=64):
        if (type(chat_id) is not int or not -(2**63)<chat_id<0
                or type(topic_id) is not int or topic_id!=4
                or not callable(current) or not callable(getattr(client,'get_messages',None))
                or not callable(getattr(client,'send_file',None))):
            raise ValueError('existing leased client and exact illustration archive scope required')
        if (archive_scope is not None and
                (type(archive_scope) is not str
                 or not 1 <= len(archive_scope.encode("utf-8")) <= 256)
                or not callable(clock)
                or type(proof_ttl_seconds) not in (int, float)
                or not 0 < proof_ttl_seconds <= 15
                or type(max_proofs) is not int or not 1 <= max_proofs <= 256):
            raise ValueError('bounded existing archive proof identity required')
        self.client,self.chat_id,self.topic_id,self.current=client,chat_id,topic_id,current
        # This is NOT another durable journal or sender. Only in-flight,
        # identity-bound private read proofs; no public ref can redeem them.
        self.archive_scope,self.clock=archive_scope,clock
        self.proof_ttl_seconds=float(proof_ttl_seconds)
        self.max_proofs=max_proofs
        self._issued_proofs={}

    def _check(self):
        if self.current() is not True:
            raise PermissionError('illustration Telegram lease changed')

    async def observe(self, locator):
        """Exact native presence/dimensions only; never download/hash media."""
        if (type(locator) is not dict or locator.get('chat_id')!=self.chat_id
                or locator.get('topic_id')!=self.topic_id
                or locator.get('kind') not in ('photo','document')
                or type(locator.get('message_id')) is not int or locator['message_id']<=0
                or type(locator.get('media_id')) is not int or not locator['media_id']):
            raise ValueError('exact registered illustration locator required')
        self._check()
        message=await self.client.get_messages(self.chat_id,ids=locator['message_id'])
        self._check()
        actual=self._locator(message,locator['kind'])
        if any(actual[k]!=locator[k] for k in ('chat_id','topic_id','message_id','media_id','kind')):
            raise ValueError('registered illustration media changed')
        return actual

    def _locator(self, message, kind):
        reply=getattr(message,'reply_to',None)
        topic=getattr(reply,'reply_to_top_id',None) or getattr(reply,'reply_to_msg_id',None)
        media=getattr(message,kind,None)
        if (getattr(message,'chat_id',None)!=self.chat_id or topic!=self.topic_id
                or type(getattr(message,'id',None)) is not int or message.id<=0
                or type(getattr(media,'id',None)) is not int or not media.id):
            raise ValueError('expected illustration archive message unavailable')
        if kind=='photo':
            dimensions=[(getattr(s,'w',0),getattr(s,'h',0)) for s in getattr(media,'sizes',())]
        else:
            dimensions=[(getattr(a,'w',0),getattr(a,'h',0)) for a in getattr(media,'attributes',())]
        dimensions=[(w,h) for w,h in dimensions if type(w) is int and type(h) is int and 0<w<=20000 and 0<h<=20000]
        if not dimensions:raise ValueError('observed illustration dimensions unavailable')
        w,h=max(dimensions,key=lambda p:p[0]*p[1])
        return dict(chat_id=self.chat_id,topic_id=self.topic_id,message_id=message.id,
            media_id=media.id,kind=kind,width=w,height=h)


    def _current_proof(self, proof):
        """Recheck object identity, registered owner, current lease and TTL."""
        self._check()
        entry=self._issued_proofs.get(id(proof))
        if (type(proof) is not NativeIllustrationArchiveProof
                or entry is None or entry[0] is not proof
                or proof.owner is not self
                or proof.archive_scope != self.archive_scope
                or entry[2] <= self.clock()):
            raise IllustrationArchiveReadUnavailable('native_illustration_proof_not_current')
        return entry[1]

    def _expire_proofs(self):
        now=self.clock()
        for key, entry in tuple(self._issued_proofs.items()):
            if entry[2] <= now:
                self._issued_proofs.pop(key, None)

    async def read_opaque(self, native, actor_id):
        """Mint proof only after live observation of an accepted private crop.

        The caller must already be the existing verified native PG reader and
        actor/SourceReadGate owner, never user JSON or a standalone public RPC.
        The Dev public gate performs independent actor/source recheck.
        """
        self._check()
        if (not callable(getattr(self.client, 'iter_download', None))
                or self.archive_scope is None
                or not _uuid(actor_id)
                or getattr(native, 'archive_scope', None) != self.archive_scope
                or getattr(native, 'accepted', None) is not True
                or getattr(native, 'withdrawn', None) is not False
                or type(getattr(native, 'source_revision', None)) is not int
                or native.source_revision != 1
                or not _uuid(getattr(native, 'source_id', None))
                or not _uuid(getattr(native, 'illustration_id', None))):
            raise IllustrationArchiveReadUnavailable('native_illustration_source_not_current')
        item=getattr(native, 'item', None)
        state=getattr(native, 'archive_state', None)
        if (type(item) is not dict or type(state) is not dict
                or set(state) != {'status','binding','operation_key','photo','document'}
                or state.get('status') != 'ready'
                or type(item.get('crop_binding')) is not dict
                or type(state.get('binding')) is not dict
                or media_crop_identity(state['binding']) != media_crop_identity(item['crop_binding'])):
            raise IllustrationArchiveReadUnavailable('native_illustration_binding_invalid')
        crop=item['crop_binding']
        if (crop.get('source_id') != native.source_id
                or crop.get('illustration_id') != native.illustration_id
                or crop.get('source_revision') != 1):
            raise IllustrationArchiveReadUnavailable('native_illustration_binding_invalid')
        # Native private crop evidence comes from the SAME accepted image
        # manifest page+figure. Do not rely on a user/model crop dict alone;
        # the Dev output gate also checks this after each awaited boundary.
        page=getattr(native,'manifest_page',None)
        region=getattr(native,'manifest_region',None)
        if (type(page) is not dict or type(region) is not dict
                or not _uuid(item.get('page_id'))
                or not _uuid(item.get('source_region_id'))
                or page.get('page_id') != item.get('page_id')
                or region.get('region_id') != item.get('source_region_id')
                or region.get('page_id') != item.get('page_id')
                or region.get('kind') != 'figure'
                or region.get('bbox') != item.get('bbox')
                or crop.get('page_id') != item.get('page_id')
                or crop.get('source_region_id') != item.get('source_region_id')
                or crop.get('bbox') != item.get('bbox')
                or crop.get('display_rotation_degrees') != item.get('display_rotation_degrees')
                or getattr(native,'archive_chat_id',None) != self.chat_id
                or getattr(native,'archive_topic_id',None) != self.topic_id):
            raise IllustrationArchiveReadUnavailable('native_illustration_figure_crop_changed')
        crop_bytes=_canonical(crop)
        if (len(crop_bytes) > 8192 or type(state.get('operation_key')) is not str
                or not 1 <= len(state['operation_key']) <= 256):
            raise IllustrationArchiveReadUnavailable('native_illustration_operation_changed')
        # Preserve a lossless document over a potentially resized Telegram
        # photo when both have been registered. Never choose a caller locator.
        locator=state['document'] if state['document'] is not None else state['photo']
        if (type(locator) is not dict or set(locator) !=
                {'chat_id','topic_id','message_id','media_id','kind','width','height'}
                or locator['chat_id'] != self.chat_id
                or locator['topic_id'] != self.topic_id):
            raise IllustrationArchiveReadUnavailable('native_illustration_locator_not_current')
        observed=await self.observe(locator)
        self._check()
        if observed != locator:
            raise IllustrationArchiveReadUnavailable('native_illustration_locator_changed')
        self._expire_proofs()
        if len(self._issued_proofs) >= self.max_proofs:
            raise IllustrationArchiveReadUnavailable('native_illustration_proof_capacity')
        proof=NativeIllustrationArchiveProof(
            ref='rkb-art-' + secrets.token_hex(32),
            source_id=native.source_id, illustration_id=native.illustration_id,
            actor_id=actor_id, binding_sha256=hashlib.sha256(crop_bytes).hexdigest(),
            archive_scope=self.archive_scope, owner=self)
        self._issued_proofs[id(proof)]=(proof, dict(locator),
            self.clock()+self.proof_ttl_seconds)
        self._current_proof(proof)
        return proof

    def issuance_current(self, proof):
        try:
            self._current_proof(proof)
            return True
        except Exception:
            return False

    def release_issued(self, proof):
        entry=self._issued_proofs.get(id(proof))
        if entry is not None and entry[0] is proof:
            self._issued_proofs.pop(id(proof),None)

    async def read_issued_bytes(self, proof):
        """Stream actual issued crop from the SAME leased Telethon client.

        Uses the verified NativeTelegramOriginalDownloader.iter_download
        64-KiB bounded pattern, not a new downloader/client/archive/transport.
        Native proof cannot cross WSS by Python object identity; root must
        bind an approved typed transfer receipt to its retained Dev-side proof.
        Never call this on an untrusted public ref, SQL READY or UNKNOWN.
        """
        locator=self._current_proof(proof)
        if not callable(getattr(self.client,'iter_download',None)):
            raise IllustrationArchiveReadUnavailable('native_illustration_transfer_missing')
        iterator=None
        try:
            async with asyncio.timeout(4):
                message=await self.client.get_messages(self.chat_id,ids=locator['message_id'])
                self._current_proof(proof)
                if self._locator(message,locator['kind']) != locator:
                    raise IllustrationArchiveReadUnavailable('native_illustration_locator_changed')
                media=getattr(message,locator['kind'],None)
                iterator=self.client.iter_download(
                    media,request_size=65536,chunk_size=65536)
                content=bytearray()
                sha=hashlib.sha256()
                async for part in iterator:
                    self._current_proof(proof)
                    if type(part) is memoryview:
                        part=part.tobytes()
                    if type(part) is not bytes or not 0 < len(part) <= 65536:
                        raise IllustrationArchiveReadUnavailable('native_illustration_chunk_invalid')
                    if len(content)+len(part)>256*1024:
                        raise IllustrationArchiveReadUnavailable('native_illustration_byte_limit')
                    content.extend(part)
                    sha.update(part)
                self._current_proof(proof)
                if not content:
                    raise IllustrationArchiveReadUnavailable('native_illustration_empty_media')
                # Verify this is actually a bounded image, not a user-provided
                # MIME label, provider JSON, truncated content or fake pixels.
                from PIL import Image, UnidentifiedImageError
                with Image.open(BytesIO(content)) as image:
                    mime={'PNG':'image/png','JPEG':'image/jpeg',
                          'WEBP':'image/webp'}.get(image.format)
                    width,height=image.size
                    if (mime is None or width != locator['width']
                            or height != locator['height']
                            or width*height > 16_777_216):
                        raise IllustrationArchiveReadUnavailable('native_illustration_pixels_changed')
                    image.verify()
                self._current_proof(proof)
                # Guard message/media identity AFTER the byte transfer too.
                after=await self.client.get_messages(self.chat_id,ids=locator['message_id'])
                self._current_proof(proof)
                if self._locator(after,locator['kind']) != locator:
                    raise IllustrationArchiveReadUnavailable('native_illustration_locator_changed')
                self._current_proof(proof)
                issued=NativeIllustrationIssuedBytes(proof=proof,data=bytes(content),
                    mime_type=mime,sha256=sha.hexdigest())
        except asyncio.CancelledError:
            raise
        except IllustrationArchiveReadUnavailable:
            raise
        except PermissionError:
            raise IllustrationArchiveReadUnavailable('native_illustration_lease_changed') from None
        except Exception:
            # Never expose private Telethon session/error details.
            raise IllustrationArchiveReadUnavailable('native_illustration_transfer_unavailable') from None
        finally:
            if iterator is not None:
                closer=getattr(iterator,'close',None)
                if callable(closer):
                    try:
                        async with asyncio.timeout(2):
                            await closer()
                    except Exception:
                        pass
        # Async generator cleanup also awaits on the leased native client.
        # Revocation/reconnect during close must invalidate even fully read
        # and decoded pixels before any caller can receive them.
        self._current_proof(proof)
        return issued

    async def send_one(self, *, operation_key, crop, mime_type, kind, load, begin, register, complete):
        """One existing durable effect; callbacks use exact-key compare-and-set.

        load returns pending/unknown/registered/ready. begin durably changes only
        pending→unknown BEFORE send, returning True to its single winning caller.
        register durably stores the returned locator BEFORE presence verification.
        These callbacks belong to the native source operation store, not a new
        queue. They must not retain a PG transaction across this method's awaits.
        """
        if (type(operation_key) is not str or not 1<=len(operation_key)<=256
                or type(crop) is not bytes or not 1<=len(crop)<=8*1024*1024
                or mime_type not in ('image/jpeg','image/png','image/webp')
                or kind not in ('photo','document')):
            raise ValueError('bounded exact illustration send required')
        self._check()
        state=await load(operation_key)
        self._check()
        if state['status'] in ('registered','ready'):
            actual=await self.observe(state['locator'])
            await complete(operation_key,actual)
            self._check()
            return actual
        if state['status']!='pending':
            raise IllustrationArchiveUnknown('reconcile the same unknown send; no blind resend')
        if await begin(operation_key) is not True:
            raise IllustrationArchiveUnknown('send already admitted; reconcile same operation')
        self._check()
        # A cancellation/lost reply from here leaves durable UNKNOWN. Do not
        # infer absence from a network exception or publish under another key.
        content=BytesIO(crop)
        content.name='illustration.'+{'image/jpeg':'jpg','image/png':'png','image/webp':'webp'}[mime_type]
        message=await self.client.send_file(self.chat_id,content,reply_to=self.topic_id,
            force_document=kind=='document',caption='RKB illustration '+operation_key)
        self._check()
        locator=self._locator(message,kind)
        await register(operation_key,locator)
        self._check()
        actual=await self.observe(locator)
        await complete(operation_key,actual)
        self._check()
        return actual


def needs_resolution_document(*, source_pixels, observed_photo):
    """No universal Telegram pixel limit; use this actual upload's dimensions."""
    if (type(source_pixels) not in (tuple,list) or len(source_pixels)!=2
            or any(type(v) is not int or not 0<v<=20000 for v in source_pixels)
            or observed_photo.get('kind')!='photo'
            or any(type(observed_photo.get(k)) is not int or observed_photo[k]<=0 for k in ('width','height'))):
        raise ValueError('actual source and returned photo dimensions required')
    return source_pixels[0]>observed_photo['width'] or source_pixels[1]>observed_photo['height']
