"""Compose public figure output over the existing original maintenance channel."""
from __future__ import annotations

import base64
import hashlib
import secrets
import time

from .kaggle_public_illustration_gate import (
    ArchiveOpaqueProof, IssuedArtifactBytes, NativeMediaSnapshot,
    PublicIllustrationOutputGate, PublicMediaUnavailable, _snapshot_identity,
    _SAFE_MEDIA_FIELDS,
)
from .kaggle_public_illustration_native_read import READ_SQL_SHA256
from .source_activation import media_crop_identity
from .source_read_gate import binding_key


def public_illustration_metadata(item, *, status):
    value = {key: data for key,data in item.items() if key in _SAFE_MEDIA_FIELDS}
    return dict(value,artifact_class='public_illustration',rendition='preview',status=status)


def validate_archive_state(state, *, binding, archive_chat_id, archive_topic_id):
    if (state.get('status') != 'ready'
            or media_crop_identity(state.get('binding')) != media_crop_identity(binding)
            or not any(state.get(kind) for kind in ('photo','document'))):
        raise ValueError('registered illustration unavailable')
    for kind in ('photo','document'):
        locator = state.get(kind)
        if locator is not None and (locator.get('kind') != kind
                or locator.get('chat_id') != archive_chat_id
                or locator.get('topic_id') != archive_topic_id):
            raise ValueError('illustration archive destination changed')
    return state


class PublicIllustrationBridge:
    """Private owner-bound receipts, never public locators or an additional transport."""

    def __init__(self, *, originals, source_gate):
        self.originals,self.source_gate = originals,source_gate
        self.bindings = {}
        self.proofs = {}

    def _expire(self):
        now = time.monotonic()
        self.proofs = {key: entry for key,entry in self.proofs.items() if entry[3] > now}
        self.bindings = {key: entry for key,entry in self.bindings.items() if entry[1] > now}

    async def native_read(self,source_id,illustration_id,actor_id,binding):
        self._expire()
        if len(self.bindings) >= 64:
            raise PublicMediaUnavailable('media_read_capacity')
        value = await self.originals.public_illustration(
            source_id,illustration_id,actor_id,binding)
        if self.source_gate.binding_current(binding) is not True:
            raise PublicMediaUnavailable('media_reader_lease_changed')
        snapshot = NativeMediaSnapshot(**value['snapshot'])
        self.bindings[(source_id,illustration_id,actor_id)] = (binding,time.monotonic()+30)
        return snapshot

    async def read_opaque(self,snapshot,actor_id):
        self._expire()
        key = (snapshot.source_id,snapshot.illustration_id,actor_id)
        entry = self.bindings.get(key)
        if entry is None or len(self.proofs) >= 64:
            raise PublicMediaUnavailable('media_read_binding_expired')
        binding = entry[0]
        value = await self.originals.public_illustration(*key,binding,pixels=True)
        current = NativeMediaSnapshot(**value['snapshot'])
        if (_snapshot_identity(current) != _snapshot_identity(snapshot)
                or self.source_gate.binding_current(binding) is not True):
            raise PublicMediaUnavailable('media_source_changed_during_image_download')
        proof = ArchiveOpaqueProof(ref='rkb-art-'+secrets.token_hex(32),
            source_id=snapshot.source_id,illustration_id=snapshot.illustration_id,
            actor_id=actor_id,binding_sha256=hashlib.sha256(
                repr(binding_key(binding)).encode()).hexdigest(),
            archive_scope=snapshot.archive_scope,owner=self)
        data = base64.b64decode(value['image_b64'],validate=True)
        pixels = IssuedArtifactBytes(proof=proof,data=data,mime_type=value['mime_type'],sha256=None)
        self.proofs[proof.ref] = (proof,pixels,binding,time.monotonic()+15)
        return proof

    def issuance_current(self,proof):
        entry = self.proofs.get(getattr(proof,'ref',None))
        return bool(entry is not None and entry[0] is proof
            and entry[3] > time.monotonic()
            and self.source_gate.binding_current(entry[2]) is True)

    async def read_issued_bytes(self,proof):
        if not self.issuance_current(proof):
            raise PublicMediaUnavailable('media_read_binding_expired')
        return self.proofs[proof.ref][1]


def configure_public_illustrations(startup):
    originals = startup.config.import_originals
    if originals is None:
        return
    backend = startup.backend
    if backend.public_media_gate is not None:
        return
    bridge = PublicIllustrationBridge(originals=originals,source_gate=backend.source_gate)
    gate = PublicIllustrationOutputGate(authority=backend.read_authority,
        source_gate=backend.source_gate,native_read=bridge.native_read,archive=bridge,
        render_metadata=public_illustration_metadata,validate_archive_state=validate_archive_state,
        native_reader_sha256=READ_SQL_SHA256)
    backend.configure_public_illustrations(gate)
