"""Version-covered canonical expert request records in the existing Dataset control store.

This leaf introduces no journal, PostgreSQL sequence, scheduler or provider effect.
Only the existing DatasetRecoveryStore fenced CAS can create a record. Readback
of identical locally staged bytes is NOT remote acceptance: only an exact
Dataset promotion/restore proves covered key + bytes.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import math
import secrets
from uuid import NAMESPACE_URL, UUID, uuid5

from .dataset_recovery_store import DatasetRecoveryStore
from .request_order import canonical as order_canonical


CODEC = "rkb.expert-request-record.v1"
PART = "control/expert-requests/"
MAX_RECORD_BYTES = 65536
RESTORE_PAGE_SIZE = 64  # Per-query memory bound; NOT a product-record ceiling.
FIELDS = frozenset({
    "codec", "request_id", "event_id", "event_hash", "message_key",
    "person_id", "link_id", "link_revision", "origin", "input_text",
    "output_scope", "created_at", "dialog_id", "dialog_seq",
})
ORIGIN_FIELDS = frozenset({
    "app_id", "team_id", "user_id", "channel_id", "channel_type",
    "thread_ts", "message_ts", "event_id",
})


class ExpertRecordError(ValueError):
    pass


def _fail():
    raise ExpertRecordError("expert_request_record_identity_changed")


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def _uuid(value):
    if type(value) is not str:
        _fail()
    try:
        if str(UUID(value)) != value:
            _fail()
    except (TypeError, ValueError):
        _fail()


def _unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            _fail()
        result[key] = value
    return result


def _identity(value):
    if type(value) is not dict or set(value) != FIELDS or value["codec"] != CODEC:
        _fail()
    origin = value["origin"]
    if type(origin) is not dict or set(origin) != ORIGIN_FIELDS:
        _fail()
    if (any(type(k) is not str or not 1 <= len(k.encode()) <= 256
            for k in origin.values())
            or origin["channel_type"] not in ("im", "group")
            or value["output_scope"] != ("public" if origin["channel_type"] == "group" else "personal")
            or type(value["event_id"]) is not str
            or not 1 <= len(value["event_id"].encode()) <= 256
            or origin["event_id"] != value["event_id"]
            or type(value["input_text"]) is not str
            or not 1 <= len(value["input_text"]) <= 12000
            or type(value["link_revision"]) is not int
            or value["link_revision"] < 1
            or type(value["dialog_seq"]) is not int or value["dialog_seq"] < 1
            or type(value["created_at"]) not in (int, float)
            or not math.isfinite(value["created_at"]) or value["created_at"] <= 0):
        _fail()
    for identifier in ("request_id", "person_id", "link_id"):
        _uuid(value[identifier])
    if (value["request_id"] != str(uuid5(
            NAMESPACE_URL, "rkb:expert-request:" + origin["app_id"] + ":"
            + origin["team_id"] + ":" + value["event_id"]))
            or type(value["event_hash"]) is not str
            or value["event_hash"] != hashlib.sha256(_canonical(
                {"origin": origin, "text": value["input_text"]})).hexdigest()
            or value["message_key"] != _canonical(
                [origin["team_id"], origin["channel_id"],
                 origin["message_ts"]]).decode()):
        _fail()
    tenant = hashlib.sha256(order_canonical(
        ["slack", origin["app_id"], origin["team_id"]]).encode()).hexdigest()
    conversation = order_canonical([origin["channel_id"], origin["thread_ts"]])
    expected_dialog = hashlib.sha256(order_canonical(
        ["slack", tenant, conversation, None]).encode()).hexdigest()
    if value["dialog_id"] != expected_dialog:
        _fail()
    return value


def from_bytes(raw):
    if type(raw) is not bytes or not 1 <= len(raw) <= MAX_RECORD_BYTES:
        _fail()
    try:
        value = json.loads(raw, object_pairs_hook=_unique,
                           parse_constant=lambda _: _fail())
        _identity(value)
        if _canonical(value) != raw:
            _fail()
        return value
    except (UnicodeError, TypeError, ValueError, OverflowError):
        _fail()


def from_row(row):
    try:
        origin = json.loads(row["origin"], object_pairs_hook=_unique)
        if _canonical(origin).decode() != row["origin"]:
            _fail()
        value = dict(
            codec=CODEC, request_id=row["id"], event_id=row["event_id"],
            event_hash=row["event_hash"], message_key=row["message_key"],
            person_id=row["person_id"], link_id=row["link_id"],
            link_revision=row["link_revision"], origin=origin,
            input_text=row["input_text"], output_scope=row["output_scope"],
            created_at=row["created_at"], dialog_id=row["dialog_id"],
            dialog_seq=row["dialog_seq"],
        )
        _identity(value)
        raw = _canonical(value)
        if len(raw) > MAX_RECORD_BYTES:
            _fail()
        return raw
    except (KeyError, TypeError, ValueError, OverflowError, UnicodeError):
        _fail()


class ExpertDatasetRequestRecords:
    """Fixed namespace over an ALREADY attached DatasetRecoveryStore."""

    def __init__(self, store):
        if type(store) is not DatasetRecoveryStore:
            raise TypeError("exact existing Dataset recovery store required")
        self.store = store
        self.prefix = store.prefix + PART

    def key(self, request_id):
        _uuid(request_id)
        return self.prefix + request_id

    def covered(self, row):
        raw = from_row(row)
        return self.store.remote_durable(self.key(row["id"]), raw)

    def effect_key(self, request_id, effect):
        """Only the original UUID and three fixed server-owned child names."""
        if effect not in ('result', 'send', 'receipt'):
            _fail()
        return self.key(request_id) + '/' + effect

    async def _stage_immutable(self, key, raw):
        """The unchanged atomic writer-fenced CAS/readback for an exact key."""
        self.store._key(key)
        if type(raw) is not bytes or not 1 <= len(raw) <= MAX_RECORD_BYTES:
            _fail()
        old = await asyncio.to_thread(self._read_bounded, key)
        if old is not None:
            if old[0] != raw:
                _fail()
            return await asyncio.to_thread(self.store.remote_durable, key, raw)
        await self._fenced_put(key, raw)
        current = await asyncio.to_thread(self._read_bounded, key)
        if current is None or current[0] != raw:
            _fail()
        return await asyncio.to_thread(self.store.remote_durable, key, raw)

    def _read_bounded(self, key):
        """Store.read's identical bounded SQLite lookup, executed off-loop."""
        self.store._key(key)  # Same existing stream-prefix validation.
        with self.store.queue.connect(write=False) as db:
            row = db.execute(
                'SELECT data,token FROM rkb_dataset_recovery_objects '
                'WHERE storage=? AND key=?',
                (self.store.storage_identity, key)).fetchone()
        if row is None:
            return None
        raw = bytes(row['data'])
        if len(raw) > MAX_RECORD_BYTES:
            _fail()
        return raw, row['token']

    def _atomic_current_put(self, key, raw):
        """Same SQLite CAS scope as DatasetStore._put + current writer fence.

        QueueWriterAuthority.__call__ only admits a writer BEFORE the await;
        current_run/retire can change while the worker waits for SQLite. A
        fresh _publication_binding() MUST run in the SAME BEGIN IMMEDIATE
        transaction as this exact immutable-key insert, so the lifecycle
        transition cannot overtake the write. No new permit, schema or journal.
        """
        from .recovery_journal import JournalLimit, JournalUnknown
        store=self.store
        store._key(key)
        if type(raw) is not bytes or len(raw)>1024*1024:
            raise JournalLimit('dataset_pending_object_budget')
        writer=store.writer_guard
        checker=getattr(writer,'_publication_binding',None)
        if not callable(checker):
            raise TypeError('existing atomic Dataset writer-binding check required')
        with store.queue.connect() as db:
            binding=checker(db,key=key)
            if binding['publication_json'] is not None:
                raise JournalUnknown('writer publication requires exact reconciliation')
            row=db.execute(
                'SELECT token FROM rkb_dataset_recovery_objects '
                'WHERE storage=? AND key=?',(store.storage_identity,key)).fetchone()
            if (row['token'] if row else None) is not None:
                return False
            size=db.execute(
                'SELECT coalesce(sum(length(data)),0) '
                'FROM rkb_dataset_recovery_objects WHERE storage=?',
                (store.storage_identity,)).fetchone()[0]
            if size+len(raw)>512*1024**2:
                raise JournalLimit('dataset_pending_control_spool_budget')
            token='pending:'+secrets.token_hex(24)
            db.execute(
                'INSERT INTO rkb_dataset_recovery_objects VALUES(?,?,?,?,NULL) '
                'ON CONFLICT(storage,key) DO UPDATE SET '
                'data=excluded.data,token=excluded.token',
                (store.storage_identity,key,raw,token))
            return True

    async def _fenced_put(self, key, raw):
        """Existing async writer guard + atomic SQLite-current CAS off-loop."""
        generation = self.store.writer_guard.writer_generation
        async with self.store.writer_guard(generation):
            worker = asyncio.create_task(
                asyncio.to_thread(self._atomic_current_put, key, raw))
            try:
                return await asyncio.shield(worker)
            except asyncio.CancelledError:
                # Do not release the cross-process fence before worker exits.
                while not worker.done():
                    try:
                        await asyncio.shield(worker)
                    except asyncio.CancelledError:
                        continue
                worker.result()
                raise

    async def stage(self, row):
        """Idempotent fixed-key CAS; never confuse local stage with coverage."""
        return await self._stage_immutable(self.key(row['id']), from_row(row))

    def covered_page(self, *, after=None, limit=RESTORE_PAGE_SIZE,
                     through_version=None):
        """Read ONE bounded keyset page; continuation is the last verified key.

        Only already-covered records count. No offset/whole-history scan,
        arbitrary product cap, or persisted secondary cursor. A reader can
        continue indefinitely in explicit fixed-size pages.
        """
        if type(limit) is not int or not 1 <= limit <= RESTORE_PAGE_SIZE:
            raise ExpertRecordError('expert_request_page_bound')
        if after is not None:
            if type(after) is not str or not after.startswith(self.prefix):
                _fail()
            _uuid(after[len(self.prefix):])
            if after != self.key(after[len(self.prefix):]):
                _fail()
        if through_version is not None and (
                type(through_version) is not int or through_version < 1):
            raise ExpertRecordError('expert_request_version_bound')
        query = ("SELECT key,data FROM rkb_dataset_recovery_objects "
                 "WHERE storage=? AND key LIKE ? AND length(key)=? "
                 "AND covered_version IS NOT NULL")
        # A result/send/receipt is a child of the same UUID path. Restore only
        # the parent intake records, not immutable effect children.
        args = [self.store.storage_identity, self.prefix + '%', len(self.prefix)+36]
        if through_version is not None:
            query += " AND covered_version<=?"
            args.append(through_version)
        if after is not None:
            query += " AND key>?"
            args.append(after)
        query += " ORDER BY key LIMIT ?"
        args.append(limit + 1)
        with self.store.queue.connect(write=False) as db:
            rows = db.execute(query, args).fetchall()
        more = len(rows) > limit
        values = []
        for row in rows[:limit]:
            key, raw = row['key'], bytes(row['data'])
            record = from_bytes(raw)
            if key != self.key(record['request_id']):
                _fail()
            # Do not accept a forged covered marker without exact bytes.
            if not self.store.remote_durable(key, raw):
                _fail()
            values.append(record)
        return tuple(values), (rows[limit - 1]['key'] if more else None)
