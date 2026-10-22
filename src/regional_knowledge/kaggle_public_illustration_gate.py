"""Unregistered, source-scoped PUBLIC illustration output gate.

SQL015 read_source_illustrations_v1 is a bounded metadata projection, not
authorization to disclose pixels or private Telegram locators. The existing
media leaf already owns public_illustration_metadata/validate_archive_state;
these two *exact existing functions* must be injected by the integration owner.
Do not create another renderer, IAM, PG pool, WSS query kind or sender here.

Native reads MUST originate in the existing actor-scoped Kaggle read channel,
from one PG snapshot joining active source/revision/visibility, the original,
the image manifest's page+figure region, media origins/candidate and persisted
archive availability. NativeMediaSnapshot is an INTERNAL transport result,
never a public request or a public MCP response. ArchiveReader independently
verifies actual archive access under its own current lease and returns ONLY a
non-bearer opaque artifact reference; this module never returns its locator.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import re
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from .read_authority import ReadAuthority, VerifiedReadAuthority
from .source_read_gate import SourceReadGate, binding_key


class PublicMediaUnavailable(PermissionError):
    """Fail closed: UNKNOWN, stale, missing permission, no exact archive proof."""


def _fail(code: str) -> None:
    raise PublicMediaUnavailable(code)


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"),
                          ensure_ascii=False, allow_nan=False).encode("utf-8")
    except (ValueError, TypeError, OverflowError, RecursionError):
        _fail("invalid_media_source_proof")


def _sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _hash(value: Any) -> str:
    return _sha(_canonical(value))


def _digest(value: Any) -> bool:
    return type(value) is str and re.fullmatch("[0-9a-f]{64}", value) is not None


def _uuid(value: Any) -> bool:
    if type(value) is not str:
        return False
    try:
        return str(UUID(value)) == value
    except ValueError:
        return False


class PublicOpaqueArtifact(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)
    kind: Literal["server_opaque_illustration"] = "server_opaque_illustration"
    ref: str = Field(pattern=r"^rkb-art-[0-9a-f]{64}$")


class PublicIllustrationFrame(BaseModel):
    """Serialized public surface; never place a Telegram URL, locator or receipt here."""

    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)
    codec: Literal["rkb.public-illustration.v1"] = "rkb.public-illustration.v1"
    status: Literal["ready"] = "ready"
    metadata: dict[str, Any]
    artifact: PublicOpaqueArtifact


@dataclass(frozen=True)
class NativeMediaSnapshot:
    """Internal, bounded, one-illustration native PG readback from SQL015+014.

    The existing media reader's PUBLIC row has data with crop_binding removed.
    The owner must additionally resolve PRIVATE source/page/region/crop/availability
    from the same canonical PG transaction; do not substitute model JSON.
    """

    source_id: str
    illustration_id: str
    source_revision: int
    eligibility_revision: int
    accepted: bool
    withdrawn: bool
    original_sha256: str
    origin_original_sha256: str
    media_candidate_sha256: str
    media_origin_sha256: str
    item: dict[str, Any] = field(repr=False)
    manifest_page: dict[str, Any] = field(repr=False)
    manifest_region: dict[str, Any] = field(repr=False)
    public_row: dict[str, Any] = field(repr=False)
    archive_state: dict[str, Any] = field(repr=False)
    archive_chat_id: int = field(repr=False)
    archive_topic_id: int = field(repr=False)
    archive_scope: str = field(repr=False)
    native_reader_sha256: str
    database_generation: str
    observed_data_seq: int
    observed_projection_seq: int
    observed_visibility_epoch: int


@dataclass(frozen=True, eq=False)
class ArchiveOpaqueProof:
    """Issued by the EXISTING separately authorized archive read/lease owner.

    No Telegram locator, download URL or credential is present. A bare ref is
    NOT a capability: the registered owner must retain and revalidate this very
    object synchronously at final issuance.
    """

    ref: str
    source_id: str
    illustration_id: str
    actor_id: str
    binding_sha256: str
    archive_scope: str
    owner: object = field(repr=False, compare=False)


@dataclass(frozen=True, eq=False)
class PublicMediaTicket:
    pass


@dataclass(frozen=True)
class IssuedPublicIllustration:
    frame: PublicIllustrationFrame
    ticket: PublicMediaTicket


@dataclass
class _Retained:
    actor: VerifiedReadAuthority
    binding: Any
    binding_identity: tuple
    source_facts: Any
    archive_proof: ArchiveOpaqueProof
    frame: PublicIllustrationFrame
    expires_at: float


# The existing media renderer only emits the following safe metadata fields.
# This is a second output whitelist, NOT a second model-to-metadata renderer.
_SAFE_MEDIA_FIELDS = frozenset({
    "illustration_key", "illustration_id", "page_id", "source_region_id", "bbox",
    "kind", "caption_region_ids", "nearby_region_ids", "caption_text",
    "visual_description", "visual_description_provenance",
    "visual_description_language", "display_rotation_degrees",
    "artifact_class", "rendition", "status",
})
_PRIVATE_MARKERS = frozenset({
    "crop_binding", "archive_locator", "archive_chat_id", "archive_topic_id",
    "telegram", "photo", "document", "source_raster_sha256", "original_sha256",
    "source_image_receipt_sha256", "media_id", "message_id", "token", "credential",
})


class PublicIllustrationOutputGate:
    """Single owner-composed output barrier over existing read/source/archive gates.

    native_read(source_id, illustration_id, person_id, binding) is an existing
    native read channel callback, NOT a new PG connection. archive.read_opaque
    must perform independently authorized exact media access/presence, and
    archive.issuance_current checks the retained exact proof/lease. No opaque
    artifact is issued until both have proved current.

    A future root MCP bridge must call issuance_current(ticket) synchronously
    immediately before emitting the frame (and release thereafter). Any await
    before output requires a fresh issue(), not replay of a cached frame.
    """

    def __init__(
        self, *, authority: ReadAuthority, source_gate: SourceReadGate,
        native_read: Callable[[str, str, str, Any], Awaitable[NativeMediaSnapshot]],
        archive: Any,
        render_metadata: Callable[..., dict[str, Any]],
        validate_archive_state: Callable[..., dict[str, Any]],
        native_reader_sha256: str,
        ttl_seconds: float = 15,
        max_retained: int = 64,
        clock: Callable[[], float] = time.monotonic,
    ):
        if (not isinstance(authority, ReadAuthority)
                or not isinstance(source_gate, SourceReadGate)
                or source_gate.authority is not authority
                or not callable(native_read) or not callable(render_metadata)
                or not callable(validate_archive_state)
                or not callable(getattr(archive, "read_opaque", None))
                or not callable(getattr(archive, "issuance_current", None))
                or not callable(clock) or not _digest(native_reader_sha256)
                or type(max_retained) is not int or not 1 <= max_retained <= 256
                or type(ttl_seconds) not in (int, float) or not 0 < ttl_seconds <= 30):
            raise TypeError("existing pinned read/renderer/archive owners are required")
        self.authority, self.source_gate = authority, source_gate
        self.native_read, self.archive = native_read, archive
        self.render_metadata, self.validate_archive_state = render_metadata, validate_archive_state
        self.reader_sha, self.ttl, self.capacity, self.clock = (
            native_reader_sha256, float(ttl_seconds), max_retained, clock)
        self._tickets: dict[PublicMediaTicket, _Retained] = {}

    def _lease(self, binding: Any, expected_key: tuple) -> None:
        try:
            if (binding_key(binding) != expected_key
                    or self.source_gate.binding_current(binding) is not True):
                _fail("media_reader_lease_changed")
        except (TypeError, AttributeError, ValueError):
            _fail("media_reader_lease_changed")

    async def _after_await(self, verified: VerifiedReadAuthority, binding: Any, key: tuple) -> None:
        # Both real methods re-evaluate the retained current OAuth/participant
        # identity; the second is a separate artifact-class authorization.
        person = await self.authority.require(verified, "public_search")
        if person != verified.person_id:
            _fail("current_media_person_changed")
        eligible = await self.authority.artifact_eligibility(verified, "public_illustration")
        if eligible is not True:
            _fail("public_media_artifact_denied")
        self._lease(binding, key)

    @staticmethod
    def _binding(source: NativeMediaSnapshot) -> dict[str, Any]:
        if type(source) is not NativeMediaSnapshot:
            _fail("untrusted_media_snapshot")
        binding = source.item.get("crop_binding") if type(source.item) is dict else None
        if type(binding) is not dict or set(binding) != {
            "source_id", "source_revision", "illustration_id", "page_id",
            "source_region_id", "bbox", "display_rotation_degrees",
            "source_image_receipt_sha256", "source_raster_sha256",
            "original_sha256", "rendition",
        }:
            _fail("invalid_media_crop_binding")
        return binding

    def _proof(self, row: NativeMediaSnapshot, *, source_id: str,
               illustration_id: str, expected_key: tuple) -> dict[str, Any]:
        if (type(row) is not NativeMediaSnapshot or
                row.source_id != source_id or row.illustration_id != illustration_id
                or not _uuid(row.source_id) or not _uuid(row.illustration_id)
                or row.accepted is not True or row.withdrawn is not False
                or type(row.source_revision) is not int or row.source_revision != 1
                or type(row.eligibility_revision) is not int or row.eligibility_revision < 1
                or not _digest(row.original_sha256)
                or row.origin_original_sha256 != row.original_sha256
                or not _digest(row.media_candidate_sha256)
                or row.media_candidate_sha256 != row.media_origin_sha256
                or row.native_reader_sha256 != self.reader_sha
                or type(row.database_generation) is not str
                or row.database_generation != expected_key[3]
                or type(row.observed_data_seq) is not int
                or type(row.observed_projection_seq) is not int
                or row.observed_data_seq != row.observed_projection_seq
                or row.observed_data_seq < expected_key[4]
                or type(row.observed_visibility_epoch) is not int
                or row.observed_visibility_epoch < 0
                or type(row.archive_chat_id) is not int or row.archive_chat_id >= 0
                or type(row.archive_topic_id) is not int or row.archive_topic_id != 4
                or type(row.archive_scope) is not str or not row.archive_scope
                or len(row.archive_scope) > 256):
            _fail("media_source_revision_or_origin_changed")
        if not all(type(value) is dict for value in
                   (row.item, row.public_row, row.manifest_page, row.manifest_region,
                    row.archive_state)):
            _fail("invalid_media_projection")
        item, page, region = row.item, row.manifest_page, row.manifest_region
        bbox = item.get("bbox")
        if (type(bbox) is not dict or
                set(bbox) != {"left", "top", "right", "bottom"} or
                any(type(x) is not int or not 0 <= x <= 20000 for x in bbox.values()) or
                not bbox["left"] < bbox["right"] or not bbox["top"] < bbox["bottom"]):
            _fail("invalid_media_figure_bbox")
        for key_name, maximum in (("caption_region_ids", 50), ("nearby_region_ids", 100)):
            refs = item.get(key_name)
            if (type(refs) is not list or len(refs) > maximum
                    or len(set(str(x) for x in refs)) != len(refs)
                    or any(not _uuid(ref) for ref in refs)):
                _fail("invalid_public_figure_references")
        for key_name, maximum in (
            ("illustration_key", 256), ("kind", 80), ("caption_text", 16000),
            ("visual_description", 8000), ("visual_description_language", 16),
        ):
            value = item.get(key_name)
            if type(value) is not str or not 1 <= len(value) <= maximum:
                _fail("invalid_public_figure_metadata")
        if item.get("display_rotation_degrees") not in (0, 90, 180, 270) or (
                type(item.get("display_rotation_degrees")) is not int):
            _fail("invalid_public_figure_rotation")
        crop = self._binding(row)
        if (item.get("illustration_id") != illustration_id
                or item.get("page_id") != page.get("page_id")
                or item.get("source_region_id") != region.get("region_id")
                or region.get("page_id") != page.get("page_id")
                or region.get("kind") != "figure"
                or region.get("bbox") != item.get("bbox")
                or item.get("visual_description_provenance") != "model_observation"
                or crop != dict(
                    source_id=source_id, source_revision=row.source_revision,
                    illustration_id=illustration_id, page_id=item.get("page_id"),
                    source_region_id=item.get("source_region_id"),
                    bbox=item.get("bbox"),
                    display_rotation_degrees=item.get("display_rotation_degrees"),
                    source_image_receipt_sha256=page.get("source_image_receipt_sha256"),
                    source_raster_sha256=page.get("image_sha256"),
                    original_sha256=row.original_sha256, rendition="preview",
                )):
            _fail("media_page_region_crop_mismatch")
        for k in ("source_image_receipt_sha256", "image_sha256"):
            if not _digest(page.get(k)):
                _fail("invalid_media_image_digest")
        if not all(_uuid(item.get(k)) for k in
                   ("illustration_id", "page_id", "source_region_id")):
            _fail("invalid_media_identity")
        public_row = row.public_row
        if (set(public_row) != {"source_id", "source_revision",
                               "eligibility_revision", "illustration_id",
                               "data", "archive_status"}
                or public_row["source_id"] != source_id
                or public_row["source_revision"] != row.source_revision
                or public_row["eligibility_revision"] != row.eligibility_revision
                or public_row["illustration_id"] != illustration_id
                or public_row["data"] != {k:v for k,v in item.items()
                                          if k != "crop_binding"}
                or public_row["archive_status"] != row.archive_state.get("status")):
            _fail("native_media_reader_projection_mismatch")
        try:
            archive = self.validate_archive_state(
                row.archive_state, binding=crop,
                archive_chat_id=row.archive_chat_id,
                archive_topic_id=row.archive_topic_id)
        except (ValueError, TypeError, KeyError):
            _fail("native_media_archive_binding_mismatch")
        if (type(archive) is not dict or archive != row.archive_state
                or row.archive_state.get("status") != "ready"):
            _fail("media_archive_not_verified")
        # Validate the renderer only after all private native checks. Nothing
        # from this private proof is copied into the public result except the
        # media leaf's sanitized public metadata.
        return crop

    @staticmethod
    def _history(facts: Any, row: NativeMediaSnapshot, key: tuple) -> None:
        if (facts is None or not getattr(facts,"history_complete",False)
                or getattr(facts,"pending",True) or getattr(facts,"unknown",True)
                or getattr(facts,"database_generation",None) != key[3]
                or type(getattr(facts,"scope_sequence",None)) is not int
                or getattr(facts,"scope_sequence") > row.observed_data_seq
                or getattr(facts,"scope_sequence") > row.observed_projection_seq
                or any(getattr(change,"restriction",False)
                       or change.sequence > row.observed_data_seq
                       for change in getattr(facts,"changes",()))):
            _fail("public_media_source_history_unavailable")

    def _source_current(self, facts: Any, binding: Any, key: tuple) -> None:
        self._lease(binding, key)
        if not self.source_gate.spool.facts_current(facts):
            _fail("media_source_fence_changed")

    async def issue(self, *, verified: VerifiedReadAuthority,
                    binding: Any, source_id: str,
                    illustration_id: str) -> IssuedPublicIllustration:
        if (type(verified) is not VerifiedReadAuthority or
                not _uuid(source_id) or not _uuid(illustration_id)):
            _fail("verified_actor_and_exact_media_selector_required")
        self._expire()
        if len(self._tickets) >= self.capacity:
            _fail("media_output_ticket_capacity")
        try:
            key = binding_key(binding)
            self._lease(binding, key)
            await self._after_await(verified, binding, key)
            first = await self.native_read(
                source_id, illustration_id, verified.person_id, binding)
            await self._after_await(verified, binding, key)
            crop = self._proof(first, source_id=source_id,
                               illustration_id=illustration_id, expected_key=key)
            facts = await self.source_gate.spool.read_facts(
                (source_id,), (source_id,))
            await self._after_await(verified, binding, key)
            self._history(facts, first, key)
            self._source_current(facts, binding, key)
            # No local source archive status constitutes access. The separately
            # leased owner must read/verify actual archive rights and mint an
            # exact opaque artifact (not a Telegram locator or bearer URL).
            proof = await self.archive.read_opaque(
                first, verified.person_id)
            await self._after_await(verified, binding, key)
            self._source_current(facts, binding, key)
            if (type(proof) is not ArchiveOpaqueProof
                    or proof.owner is not self.archive
                    or proof.actor_id != verified.person_id
                    or proof.source_id != source_id
                    or proof.illustration_id != illustration_id
                    or proof.archive_scope != first.archive_scope
                    or proof.binding_sha256 != _hash(crop)
                    or type(proof.ref) is not str
                    or re.fullmatch(r"rkb-art-[0-9a-f]{64}",proof.ref) is None
                    or self.archive.issuance_current(proof) is not True):
                _fail("independent_archive_access_not_proved")
            # A second current authoritative PG read is compulsory after the
            # archive await. Source version, withdrawal, manifest or availability
            # drift is never reconciled by silently returning an old cached frame.
            second = await self.native_read(
                source_id, illustration_id, verified.person_id, binding)
            await self._after_await(verified, binding, key)
            if second != first:
                _fail("media_source_changed_during_read")
            self._proof(second, source_id=source_id,
                        illustration_id=illustration_id, expected_key=key)
            final_facts = await self.source_gate.spool.read_facts(
                (source_id,), (source_id,))
            await self._after_await(verified, binding, key)
            self._history(final_facts, second, key)
            self._source_current(final_facts, binding, key)
            if self.archive.issuance_current(proof) is not True:
                _fail("media_archive_lease_changed")
            metadata = self.render_metadata(second.item, status="ready")
            expected_public = {
                k:v for k,v in second.item.items()
                if k in _SAFE_MEDIA_FIELDS
            }
            expected_public.update(artifact_class="public_illustration",
                                   rendition="preview", status="ready")
            if (type(metadata) is not dict
                    or not set(metadata) <= _SAFE_MEDIA_FIELDS
                    or metadata != expected_public
                    or any(k in metadata for k in _PRIVATE_MARKERS)
                    or metadata.get("illustration_id") != illustration_id
                    or metadata.get("page_id") != second.manifest_page.get("page_id")
                    or metadata.get("source_region_id") != second.manifest_region.get("region_id")
                    or metadata.get("bbox") != second.item.get("bbox")
                    or metadata.get("artifact_class") != "public_illustration"
                    or metadata.get("rendition") != "preview"
                    or metadata.get("status") != "ready"
                    or len(_canonical(metadata)) > 64*1024):
                _fail("untrusted_public_media_renderer")
            frame = PublicIllustrationFrame(metadata=metadata,
                artifact=PublicOpaqueArtifact(ref=proof.ref))
            ticket = PublicMediaTicket()
            self._tickets[ticket] = _Retained(
                verified, binding, key, final_facts, proof, frame,
                self.clock() + self.ttl)
            if not self.issuance_current(ticket):
                self.release(ticket)
                _fail("media_output_fence_changed")
            return IssuedPublicIllustration(frame, ticket)
        except asyncio.CancelledError:
            raise
        except PublicMediaUnavailable:
            raise
        except (PermissionError, ValueError, TypeError, KeyError, AttributeError):
            _fail("media_permission_or_native_read_unavailable")
        except Exception:
            # Never reflect native/archival provider exception text or private
            # locator/credentials into a public MCP error.
            _fail("media_native_or_archive_unavailable")

    def _expire(self):
        now = self.clock()
        for token, read in tuple(self._tickets.items()):
            if read.expires_at <= now:
                del self._tickets[token]

    def issuance_current(self, ticket: PublicMediaTicket) -> bool:
        """No await after this check and before actual public serialization."""
        retained = self._tickets.get(ticket)
        if retained is None or retained.expires_at <= self.clock():
            return False
        try:
            return bool(
                binding_key(retained.binding) == retained.binding_identity
                and self.source_gate.binding_current(retained.binding) is True
                and self.source_gate.spool.facts_current(retained.source_facts)
                and retained.archive_proof.owner is self.archive
                and self.archive.issuance_current(retained.archive_proof) is True)
        except (ValueError, TypeError, AttributeError, PermissionError):
            return False

    def release(self, ticket: PublicMediaTicket):
        self._tickets.pop(ticket, None)
