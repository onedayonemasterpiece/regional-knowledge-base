"""SQL014/015 one-transaction illustration snapshot for the EXISTING public gate.

This is a narrow Kaggle-native read *leaf*, not a new IAM, read pool, WSS route,
archive service, artifact issuer or output state machine. The root owner injects
its EXISTING verified reader-pool acquire() and binding_current(binding). The
existing PublicIllustrationOutputGate rechecks the actor and source history
around this callback, independently verifies archive access and owns issuance.

Private crop binding and Telegram locator NEVER leave this internal snapshot.
This module performs no provider action, send, image transfer or credential I/O.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import math
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

from psycopg.pq import TransactionStatus
from psycopg.rows import tuple_row

from .original_intake import canonical as _canonical
from .kaggle_read_binding import binding_key
from .source_activation import MEDIA_HASH_IDENTIFIERS,media_crop_identity

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
    original_sha256: str | None
    origin_original_sha256: str | None
    media_candidate_sha256: str | None
    media_origin_sha256: str | None
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


SQL014_SHA256 = "5f8257e9fe58e2e2bdd00f67042ad17b8db007c7873dcc04f52d53be3523e855"
SQL015_SHA256 = "7faacbf7fbb2b99f0dbdbd4339dea5138f134347e31a6be09577dff35be8c773"

# One exact private SELECT, within an existing PG17 tuple-row reader pool lease.
# No dynamic table/column names, search_path, model-selected joins or input SQL.
# SQL015's public read_source_illustrations_v1 is paged (up to 4) and strips
# crop_binding; this private exact-id read reconstructs the SAME projection
# plus immutable accepted source/manifest/figure/archive provenance in one
# REPEATABLE READ READ ONLY snapshot. A missing SELECT grant fails closed.
LEGACY_READ_SQL = """
SELECT
 s.source_id::text, i.illustration_id::text,
 active.revision, visibility.eligibility_revision, s.accepted, s.withdrawn,
 convert_from(manifest.original_receipt,'UTF8')::jsonb->>'sha256',
 imported.original_sha256,
 media.media_sha256, media_origin.media_sha256,
 i.data, page.value, region.value,
 availability.binding_bytes, availability.operation_key, availability.status,
 availability.photo, availability.document,
 availability.archive_chat_id, availability.archive_topic_id,
 recovery.database_generation, recovery.applied_seq,
 frontier.projection_seq, frontier.visibility_epoch
FROM rkb_v4.source_illustrations AS i
JOIN rkb_v4.sources AS s ON s.source_id=i.source_id
JOIN rkb_v4.active_revision AS active ON active.source_id=s.source_id
JOIN rkb_v4.source_visibility AS visibility ON visibility.source_id=s.source_id
JOIN rkb_v4.source_import_origins AS imported ON imported.source_id=s.source_id
 AND imported.revision=active.revision
JOIN rkb_v4.source_manifests AS manifest ON manifest.source_id=s.source_id
 AND manifest.source_revision=active.revision
JOIN rkb_v4.source_image_origins AS image_origin ON image_origin.source_id=s.source_id
JOIN rkb_v4.source_activation_candidates AS activation ON activation.source_id=s.source_id
JOIN rkb_v4.source_illustration_candidates AS media ON media.source_id=s.source_id
JOIN rkb_v4.source_illustration_origins AS media_origin ON media_origin.source_id=s.source_id
JOIN rkb_v4.applied_ops AS accepted ON accepted.operation_key=media_origin.operation_key
 AND accepted.sequence=media_origin.sequence
 AND accepted.command_sha256=media_origin.command_sha256
 AND accepted.status='applied'
JOIN rkb_v4.source_illustration_availability AS availability
 ON availability.source_id=i.source_id
 AND availability.illustration_id=i.illustration_id
JOIN LATERAL jsonb_array_elements(manifest.manifest->'pages') AS page(value)
 ON page.value->>'page_id'=i.data->>'page_id'
JOIN LATERAL jsonb_array_elements(manifest.manifest->'regions') AS region(value)
 ON region.value->>'region_id'=i.data->>'source_region_id'
CROSS JOIN rkb_v4.recovery_state AS recovery
CROSS JOIN rkb_v4.read_frontier AS frontier
WHERE s.source_id=%s::uuid AND i.illustration_id=%s::uuid
 AND s.accepted AND NOT s.withdrawn AND active.revision=1
 AND manifest.manifest->>'source_id'=s.source_id::text
 AND manifest.manifest->'source_revision'='1'::jsonb
 AND manifest.manifest->>'codec'='rkb.source-image-manifest.v1'
 AND image_origin.operation_key=media_origin.operation_key
 AND image_origin.command_sha256=media_origin.command_sha256
 AND accepted.database_generation=recovery.database_generation
 AND accepted.command_sha256=encode(sha256(accepted.command_bytes),'hex')
 AND page.value->>'page_id'=region.value->>'page_id'
 AND region.value->>'kind'='figure'
 AND region.value->'bbox'=i.data->'bbox'
 AND availability.status='ready'
 AND availability.archive_chat_id=%s::bigint
 AND availability.archive_topic_id=%s::integer
 AND recovery.singleton AND frontier.singleton
 AND frontier.text_ready AND frontier.pending_seq IS NULL
 AND frontier.projection_seq=recovery.applied_seq
LIMIT 2
""".strip()

# Packet publication already commits its accepted figure and exact manifest.
# It has no whole-book source_image/illustration_origins row. Do not require
# those historical receipts or compare media representation hashes for delivery.
BATCH_READ_SQL = """
SELECT s.source_id::text,i.illustration_id::text,
 active.revision,visibility.eligibility_revision,s.accepted,s.withdrawn,
 imported.original_sha256,imported.original_sha256,NULL::text,NULL::text,
 i.data,page.value,region.value,
 availability.binding_bytes,availability.operation_key,availability.status,
 availability.photo,availability.document,
 availability.archive_chat_id,availability.archive_topic_id,
 recovery.database_generation,recovery.applied_seq,
 frontier.projection_seq,frontier.visibility_epoch
FROM rkb_v4.source_illustrations i
JOIN rkb_v4.sources s USING(source_id)
JOIN rkb_v4.active_revision active USING(source_id)
JOIN rkb_v4.source_visibility visibility USING(source_id)
JOIN rkb_v4.source_import_origins imported ON imported.source_id=s.source_id
 AND imported.revision=active.revision
JOIN LATERAL (SELECT b.manifest FROM rkb_v4.source_batch_publications b
 WHERE b.source_id=i.source_id AND b.component='semantic'
 AND b.figure_ids @> ARRAY[i.illustration_id]
 ORDER BY b.batch_index DESC LIMIT 1) publication ON true
JOIN LATERAL jsonb_array_elements(publication.manifest->'pages') page(value)
 ON page.value->>'page_id'=i.data->>'page_id'
JOIN LATERAL jsonb_array_elements(publication.manifest->'regions') region(value)
 ON region.value->>'region_id'=i.data->>'source_region_id'
JOIN rkb_v4.source_illustration_availability availability
 ON availability.source_id=i.source_id AND availability.illustration_id=i.illustration_id
CROSS JOIN rkb_v4.recovery_state recovery
CROSS JOIN rkb_v4.read_frontier frontier
WHERE s.source_id=%s::uuid AND i.illustration_id=%s::uuid
 AND s.accepted AND NOT s.withdrawn AND active.revision=1
 AND page.value->>'page_id'=region.value->>'page_id'
 AND region.value->>'kind'='figure' AND region.value->'bbox'=i.data->'bbox'
 AND availability.status='ready' AND availability.archive_chat_id=%s::bigint
 AND availability.archive_topic_id=%s::integer
 AND recovery.singleton AND frontier.singleton AND frontier.text_ready
 AND frontier.pending_seq IS NULL AND frontier.projection_seq=recovery.applied_seq
LIMIT 2
""".strip()

READ_SQL = LEGACY_READ_SQL

# Stable public code fingerprint; never a caller-provided string or an
# assertion that a deployment has installed SQL015/this reader.
READ_SQL_SHA256 = hashlib.sha256((READ_SQL+'\n'+BATCH_READ_SQL).encode("utf-8")).hexdigest()

ACCEPTED_FIGURE_SQL = BATCH_READ_SQL.replace(
    'availability.binding_bytes,availability.operation_key,availability.status,',
    "coalesce(availability.binding_bytes,convert_to((i.data->'crop_binding')::text,'UTF8')),"
    "coalesce(availability.operation_key,'illustration:'||i.illustration_id::text),"
    "coalesce(availability.status,'pending'),"
).replace('availability.archive_chat_id,availability.archive_topic_id,',
          '%s::bigint,4::integer,').replace(
    'JOIN rkb_v4.source_illustration_availability availability',
    'LEFT JOIN rkb_v4.source_illustration_availability availability'
).replace(" AND availability.status='ready' AND availability.archive_chat_id=%s::bigint\n AND availability.archive_topic_id=%s::integer", '')

_FIELDS = (
    "source_id", "illustration_id", "source_revision", "eligibility_revision",
    "accepted", "withdrawn", "original_sha256", "origin_original_sha256",
    "media_candidate_sha256", "media_origin_sha256",
    "item", "manifest_page", "manifest_region",
    "archive_binding_bytes", "archive_operation_key", "archive_status",
    "photo", "document", "archive_chat_id", "archive_topic_id",
    "database_generation", "observed_data_seq", "observed_projection_seq",
    "observed_visibility_epoch",
)


class NativeIllustrationReadUnavailable(PermissionError):
    """No private source, provider or SQL detail is safe for public output."""


def _deny(reason: str = "native_media_source_unavailable") -> None:
    raise NativeIllustrationReadUnavailable(reason)


def _uuid(value) -> bool:
    if type(value) is not str:
        return False
    try:
        return str(UUID(value)) == value
    except ValueError:
        return False


def _row_to_snapshot(values, *, source_id: str, illustration_id: str,
                     reader_key: tuple, archive_chat_id: int,
                     archive_topic_id: int, archive_scope: str,
                     allow_pending: bool = False) -> NativeMediaSnapshot:
    if type(values) not in (tuple, list) or len(values) != len(_FIELDS):
        _deny()
    d = dict(zip(_FIELDS, values))
    if (d["source_id"] != source_id or d["illustration_id"] != illustration_id
            or d["accepted"] is not True or d["withdrawn"] is not False
            or type(d["source_revision"]) is not int or d["source_revision"] != 1
            or type(d["eligibility_revision"]) is not int or d["eligibility_revision"] < 1
            or d["database_generation"] != reader_key[3]
            or type(d["observed_data_seq"]) is not int
            or type(d["observed_projection_seq"]) is not int
            or d["observed_data_seq"] < reader_key[4]
            or d["observed_projection_seq"] != d["observed_data_seq"]
            or type(d["observed_visibility_epoch"]) is not int
            or d["observed_visibility_epoch"] < 0
            or d["archive_chat_id"] != archive_chat_id
            or d["archive_topic_id"] != archive_topic_id
            or d["archive_status"] not in (('pending','unknown','registered','ready','unavailable','failed')
                if allow_pending else ('ready',))):
        _deny()
    item, page, region = (d["item"], d["manifest_page"], d["manifest_region"])
    if any(type(v) is not dict for v in (item, page, region)):
        _deny()
    binding = item.get("crop_binding")
    required={
        "source_id", "source_revision", "illustration_id", "page_id",
        "source_region_id", "bbox", "display_rotation_degrees",
        "rendition"}
    if (type(binding) is not dict or not required<=binding.keys()
            or set(binding)-required-MEDIA_HASH_IDENTIFIERS):
        _deny()
    if (item.get("illustration_id") != illustration_id
            or page.get("page_id") != item.get("page_id")
            or region.get("region_id") != item.get("source_region_id")
            or region.get("page_id") != page.get("page_id")
            or region.get("kind") != "figure"
            or region.get("bbox") != item.get("bbox")
            or media_crop_identity(binding) != dict(
                source_id=source_id, source_revision=1,
                illustration_id=illustration_id, page_id=item.get("page_id"),
                source_region_id=item.get("source_region_id"),
                bbox=item.get("bbox"),
                display_rotation_degrees=item.get("display_rotation_degrees"),
                rendition="preview")):
        _deny()
    persisted = d["archive_binding_bytes"]
    if type(persisted) is memoryview:
        persisted = persisted.tobytes()
    if (type(persisted) is not bytes or not 1 <= len(persisted) <= 8192
            or type(d['archive_operation_key']) is not str
            or not 1<=len(d['archive_operation_key'])<=256):
        _deny()
    try:
        archived_binding=json.loads(persisted)
        if media_crop_identity(archived_binding)!=media_crop_identity(binding):
            _deny()
    except (ValueError,TypeError):
        _deny()
    photo, document = d["photo"], d["document"]
    if (photo is not None and type(photo) is not dict
            or document is not None and type(document) is not dict
            or d['archive_status']=='ready' and photo is None and document is None):
        _deny()
    archive_state = dict(status=d['archive_status'], binding=archived_binding,
                         operation_key=d["archive_operation_key"],
                         photo=photo, document=document)
    public_row = dict(
        source_id=source_id, source_revision=1,
        eligibility_revision=d["eligibility_revision"],
        illustration_id=illustration_id,
        data={k: v for k, v in item.items() if k != "crop_binding"},
        archive_status=d['archive_status'],
    )
    if (len(_canonical(item)) > 65536 or len(_canonical(public_row)) > 65536
            or len(_canonical(archive_state)) > 16384):
        _deny()
    return NativeMediaSnapshot(
        source_id=source_id,
        illustration_id=illustration_id,
        source_revision=1,
        eligibility_revision=d["eligibility_revision"],
        accepted=True, withdrawn=False,
        original_sha256=d["original_sha256"],
        origin_original_sha256=d["origin_original_sha256"],
        media_candidate_sha256=d["media_candidate_sha256"],
        media_origin_sha256=d["media_origin_sha256"],
        item=item, manifest_page=page, manifest_region=region,
        public_row=public_row, archive_state=archive_state,
        archive_chat_id=archive_chat_id, archive_topic_id=archive_topic_id,
        archive_scope=archive_scope, native_reader_sha256=READ_SQL_SHA256,
        database_generation=d["database_generation"],
        observed_data_seq=d["observed_data_seq"],
        observed_projection_seq=d["observed_projection_seq"],
        observed_visibility_epoch=d["observed_visibility_epoch"],
    )


class NativePublicIllustrationReader:
    """Pure adapter over the same PG reader-pool *acquire* that native search uses.

    Internal call signature matches PublicIllustrationOutputGate.native_read.
    Actor must be a *verified* gate-projected person ID. This reader cannot
    authenticate a caller, mint a public artifact or validate Telegram access.
    """

    def __init__(self, *, acquire, binding_current: Callable, archive_chat_id: int,
                 archive_scope: str, timeout_seconds: float = 2.0,
                 packet_publications: bool = False):
        if (not callable(acquire) or not callable(binding_current)
                or type(archive_chat_id) is not int
                or not -(2**63) < archive_chat_id < 0
                or type(archive_scope) is not str
                or not 1 <= len(archive_scope.encode("utf-8")) <= 256
                or type(timeout_seconds) not in (int, float)
                or not math.isfinite(timeout_seconds)
                or not 0 < timeout_seconds <= 2):
            raise ValueError("existing bounded reader and private archive owner scope required")
        self.acquire, self.current = acquire, binding_current
        self.archive_chat_id, self.archive_topic_id = archive_chat_id, 4
        self.archive_scope = archive_scope
        self.timeout_seconds = float(timeout_seconds)
        self.packet_publications = packet_publications
        self.native_reader_sha256 = READ_SQL_SHA256

    def _current(self, binding, key):
        try:
            if binding_key(binding) != key or self.current(binding) is not True:
                _deny("native_media_reader_lease_changed")
        except (ValueError, TypeError, AttributeError):
            _deny("native_media_reader_lease_changed")

    async def __call__(self, source_id: str, illustration_id: str,
                       actor_id: str, binding) -> NativeMediaSnapshot:
        return await self._read(source_id,illustration_id,actor_id,binding,allow_pending=False)

    async def accepted_figure(self, source_id, illustration_id, actor_id, binding):
        """Accepted geometry for the existing native mirror, before availability."""
        return await self._read(source_id,illustration_id,actor_id,binding,allow_pending=True)

    async def _read(self,source_id,illustration_id,actor_id,binding,*,allow_pending):
        if not _uuid(source_id) or not _uuid(illustration_id) or not _uuid(actor_id):
            _deny("native_media_verified_actor_and_source_required")
        try:
            key = binding_key(binding)
            self._current(binding, key)
            loop = asyncio.get_running_loop()
            deadline = loop.time() + self.timeout_seconds
            async with asyncio.timeout_at(deadline):
                async with self.acquire() as connection:
                    self._current(binding, key)
                    # Match the existing source-read transaction contract.
                    if (connection.closed or
                            connection.info.transaction_status != TransactionStatus.IDLE
                            or connection.row_factory is not tuple_row):
                        _deny()
                    async with connection.transaction():
                        self._current(binding, key)
                        async with connection.cursor() as cursor:
                            await cursor.execute(
                                "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                            self._current(binding, key)
                            await cursor.execute(
                                "SELECT set_config('statement_timeout',%s,true)",
                                (str(max(1, int((deadline-loop.time()) * 1000))),))
                            self._current(binding, key)
                            await cursor.execute(ACCEPTED_FIGURE_SQL if allow_pending else
                                (BATCH_READ_SQL if self.packet_publications else READ_SQL),
                                (self.archive_chat_id,source_id,illustration_id) if allow_pending else
                                (source_id, illustration_id, self.archive_chat_id,self.archive_topic_id))
                            self._current(binding, key)
                            rows = await cursor.fetchmany(2)
                            self._current(binding, key)
                            if len(rows) != 1:
                                _deny()
                            value = _row_to_snapshot(
                                rows[0], source_id=source_id,
                                illustration_id=illustration_id, reader_key=key,
                                archive_chat_id=self.archive_chat_id,
                                archive_topic_id=self.archive_topic_id,
                                archive_scope=self.archive_scope,allow_pending=allow_pending)
                        self._current(binding, key)
                    self._current(binding, key)
                self._current(binding, key)
                if loop.time() >= deadline:
                    _deny("native_media_read_deadline")
                return value
        except asyncio.CancelledError:
            raise
        except TimeoutError:
            _deny("native_media_read_deadline")
        except NativeIllustrationReadUnavailable:
            raise
        except Exception:
            # Fail closed on missing installed SELECT roles/function/schema,
            # cursor or snapshot error. Never leak SQL/private locators upstream.
            _deny()
