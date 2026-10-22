"""Existing shared WSS media read adapter: Dev-retained proof, native bytes.

No new HTTP/Telegram client, transport, IAM, read pool or durable journal.
The existing BgeQueue.shared_reads/ReadAuthority validates actor and
generation for every WSS request. A native proof/ref is never a Dev
capability: Dev creates and retains an independent object-scoped proof
only after a verified native *observe* result; the native *pixels* request
reads the current PG source and leased Telegram owner again.
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import secrets
import time
from dataclasses import dataclass
from uuid import UUID

from .kaggle_public_illustration_gate import (
    ArchiveOpaqueProof, IssuedArtifactBytes,
    PublicMediaUnavailable,
)
from .kaggle_media_native_contracts import NativeMediaSnapshot, canonical_media
from .kaggle_media_wss_contracts import IllustrationReadRequest, IllustrationReadResult
from .kaggle_protocol import TrustedActorContext
from .read_authority import VerifiedReadAuthority


@dataclass(frozen=True)
class _RetainedMediaProof:
    proof: ArchiveOpaqueProof
    actor: VerifiedReadAuthority
    binding: object
    native: NativeMediaSnapshot
    deadline: float


class NativeWssMediaBridge:
    """An optional owner-bound adapter over the existing admitted read lane."""

    _requires_verified_context=True

    def __init__(self, *, backend, archive_scope: str,
                 ttl_seconds: float=10, max_proofs: int=64, clock=time.monotonic):
        from .kaggle_public_backend import KaggleSourceBackend
        if (type(backend) is not KaggleSourceBackend
                and not isinstance(backend,KaggleSourceBackend)
                or not type(archive_scope) is str
                or not 1<=len(archive_scope.encode())<=256
                or type(ttl_seconds) not in (float,int)
                or not 0<ttl_seconds<=15
                or type(max_proofs) is not int or not 1<=max_proofs<=256
                or not callable(clock)):
            raise TypeError("existing shared media backend and exact archive scope required")
        self.backend,self.archive_scope=backend,archive_scope
        self.ttl,self.capacity,self.clock=float(ttl_seconds),max_proofs,clock
        self._proofs={}

    async def _call(self, action, source_id, illustration_id, verified, binding,
                    *, crop_sha256=None):
        from .kaggle_public_backend import TargetReadBinding
        if (type(verified) is not VerifiedReadAuthority
                or type(binding) is not TargetReadBinding
                or verified.destination != self.backend.destination
                or self.backend.source_gate.binding_current(binding) is not True):
            raise PublicMediaUnavailable("media_current_authority_or_lease_missing")
        await self.backend.read_authority.require(verified,"public_search")
        if self.backend.source_gate.binding_current(binding) is not True:
            raise PublicMediaUnavailable("media_current_lease_changed")
        queue=self.backend.worker_queue
        try:
            request=IllustrationReadRequest(
                request_id=secrets.token_urlsafe(24),
                connection_generation=binding.connection_generation,
                turn_generation=0,action=action,
                source_id=source_id,illustration_id=illustration_id,
                expected_crop_sha256=crop_sha256,
                database_generation=binding.database_generation,
                binding_run_id=binding.run_id,
                binding_runtime_incarnation=binding.runtime_incarnation,
                provider_version=binding.provider_version,
                archive_scope=self.archive_scope,
                actor=TrustedActorContext(
                    person_id=verified.person_id,
                    audience=verified.destination.key,
                    auth_epoch=verified.auth_revision,
                    visibility_epoch=0,
                    context_ref=verified.context_ref),
                min_data_seq=binding.min_data_seq,
                deadline_ms=1850,
            )
        except Exception:
            raise PublicMediaUnavailable("media_wss_request_invalid") from None
        ticket=None
        try:
            now=self.clock()
            ticket=await queue.shared_reads.begin_read(
                verified,request,dialog_ref=verified.destination.key,
                deadline=min(now+1.9, self.clock()+1.9))
            result=await asyncio.shield(ticket.result)
            await self.backend.read_authority.require(verified,"public_search")
            if (type(result) is not IllustrationReadResult
                    or result.status!="ok"
                    or result.action!=action
                    or (result.source_id,result.illustration_id)!=(source_id,illustration_id)
                    or result.observed_database_generation!=binding.database_generation
                    or self.backend.source_gate.binding_current(binding) is not True
                    or result.observed_data_seq is None
                    or result.observed_data_seq < binding.min_data_seq
                    or result.observed_projection_seq != result.observed_data_seq):
                raise PublicMediaUnavailable("media_wss_not_current")
            if crop_sha256 is not None and result.crop_sha256!=crop_sha256:
                raise PublicMediaUnavailable("media_wss_crop_changed")
            return result
        except asyncio.CancelledError:
            raise
        except PublicMediaUnavailable:
            raise
        except Exception:
            raise PublicMediaUnavailable("media_wss_unavailable") from None
        finally:
            if ticket is not None:
                queue.shared_reads.cancel_read(ticket)

    async def native_read(self, source_id, illustration_id, actor, binding):
        if type(actor) is not VerifiedReadAuthority:
            raise PublicMediaUnavailable("verified_media_actor_required")
        result=await self._call("snapshot",source_id,illustration_id,actor,binding)
        try:
            snapshot=NativeMediaSnapshot(**result.snapshot)
        except Exception:
            raise PublicMediaUnavailable("media_native_snapshot_invalid") from None
        if (snapshot.source_id!=source_id or snapshot.illustration_id!=illustration_id
                or snapshot.database_generation!=binding.database_generation):
            raise PublicMediaUnavailable("media_native_snapshot_changed")
        return snapshot

    def _expire(self):
        now=self.clock()
        for key,entry in tuple(self._proofs.items()):
            if entry.deadline <= now:self._proofs.pop(key,None)

    async def read_opaque(self, native, actor, binding):
        if (type(native) is not NativeMediaSnapshot
                or type(actor) is not VerifiedReadAuthority):
            raise PublicMediaUnavailable("media_native_proof_required")
        crop=native.item.get("crop_binding") if type(native.item) is dict else None
        if type(crop) is not dict:
            raise PublicMediaUnavailable("media_crop_binding_missing")
        digest=hashlib.sha256(canonical_media(crop)).hexdigest()
        observed=await self._call(
            "observe",native.source_id,native.illustration_id,
            actor,binding,crop_sha256=digest)
        if NativeMediaSnapshot(**observed.snapshot)!=native:
            raise PublicMediaUnavailable("media_source_changed_during_observe")
        self._expire()
        if len(self._proofs)>=self.capacity:
            raise PublicMediaUnavailable("media_archive_read_capacity")
        proof=ArchiveOpaqueProof(
            ref="rkb-art-"+secrets.token_hex(32),owner=self,
            source_id=native.source_id,
            illustration_id=native.illustration_id,
            actor_id=actor.person_id,
            binding_sha256=digest,
            archive_scope=self.archive_scope)
        self._proofs[id(proof)]=_RetainedMediaProof(
            proof,actor,binding,native,self.clock()+self.ttl)
        if not self.issuance_current(proof):
            self.release_issued(proof)
            raise PublicMediaUnavailable("media_current_scope_changed")
        return proof

    def issuance_current(self, proof):
        from .kaggle_public_backend import TargetReadBinding
        record=self._proofs.get(id(proof))
        if (type(proof) is not ArchiveOpaqueProof or record is None
                or record.proof is not proof or record.deadline<=self.clock()
                or proof.owner is not self
                or proof.actor_id != record.actor.person_id
                or proof.archive_scope!=self.archive_scope
                or record.native.database_generation != record.binding.database_generation):
            return False
        return self.backend.source_gate.binding_current(record.binding) is True

    def release_issued(self, proof):
        old=self._proofs.get(id(proof))
        if old is not None and old.proof is proof:self._proofs.pop(id(proof),None)

    async def read_issued_bytes(self, proof, actor, binding):
        record=self._proofs.get(id(proof))
        if (not self.issuance_current(proof)
                or record is None or record.actor is not actor
                or record.binding is not binding):
            raise PublicMediaUnavailable("media_archive_owner_changed")
        result=await self._call(
            "pixels",proof.source_id,proof.illustration_id,
            actor,binding,crop_sha256=proof.binding_sha256)
        if not self.issuance_current(proof):
            raise PublicMediaUnavailable("media_archive_lease_changed")
        try:
            raw=base64.b64decode(result.pixels_b64,validate=True)
            if (hashlib.sha256(raw).hexdigest()!=result.pixels_sha256
                    or result.crop_sha256!=proof.binding_sha256):
                raise ValueError
            return IssuedArtifactBytes(
                proof=proof,data=raw,mime_type=result.mime_type,
                sha256=result.pixels_sha256)
        except Exception:
            raise PublicMediaUnavailable("media_native_bytes_invalid") from None
