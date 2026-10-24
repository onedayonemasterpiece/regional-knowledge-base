"""RKB1-only source binding: existing private queue lease and injected native client.

This module has no Telegram transport, credential acquisition, WSS frame handling,
Kaggle calls or provider retries. The controller owns an exclusive scalar lease;
the Kaggle runtime borrows a client only after a separately authorized,
ephemeral WSS credential delivery. No credential is put in this queue.
"""
from __future__ import annotations

import base64
import binascii
import json
import re
from uuid import UUID

SLOT = "TELEGRAM_RKB1"
SOURCE_TOPIC = 2
_DEVICE_FIELDS = ("app_version", "device_model", "lang_code", "system_lang_code", "system_version")
_BUNDLE_FIELDS = frozenset(("session",) + _DEVICE_FIELDS)


class TelegramBindingUnavailable(PermissionError):
    """Stable, nonsecret refusal. Never format the input bundle or SDK error."""


def _identity(run_id, incarnation):
    if type(run_id) is not str or type(incarnation) is not str:
        raise TelegramBindingUnavailable("telegram_run_identity_required")
    try:
        if str(UUID(run_id)) != run_id:
            raise ValueError("noncanonical")
    except (ValueError, AttributeError):
        raise TelegramBindingUnavailable("telegram_run_identity_required") from None
    if not re.fullmatch(r"[A-Za-z0-9_-]{16,256}", incarnation):
        raise TelegramBindingUnavailable("telegram_run_identity_required")


class Rkb1Lease:
    """One scalar session slot in the EXISTING BgeQueue private SQLite database.

    A claim is bound to the currently authenticated WSS incarnation and writer.
    Reconnect may increment socket generation, but does not change the slot owner.
    Local timeout, heartbeat loss, Stop/cancel and status ERROR NEVER release it.
    Only the previously validated exact provider-terminal receipt can transfer it.
    """

    def __init__(self, queue, *, archive_chat_id, topic_id=SOURCE_TOPIC):
        if (not callable(getattr(queue, "connect", None))
                or not callable(getattr(queue, "clock", None))
                or not callable(getattr(queue, "_run_expired", None))
                or type(archive_chat_id) is not int or archive_chat_id >= 0
                or type(topic_id) is not int or topic_id != SOURCE_TOPIC):
            raise TelegramBindingUnavailable("telegram_archive_binding_required")
        self._queue = queue
        self.archive_chat_id = archive_chat_id
        self.topic_id = topic_id

    @staticmethod
    def _ensure_slot_table(db):
        db.execute("""CREATE TABLE IF NOT EXISTS rkb_telegram_session_lease(
            slot TEXT PRIMARY KEY CHECK(slot='TELEGRAM_RKB1'),
            run_id TEXT NOT NULL,
            incarnation TEXT NOT NULL,
            archive_chat_id INTEGER NOT NULL,
            topic_id INTEGER NOT NULL CHECK(topic_id=2))""")

    def _writer_current(self, db, run_id, incarnation):
        # Exactly the current native writer and authenticated run, NOT a
        # stale controller token, job claim, consumer-supplied boolean or TTL.
        exists = db.execute("""SELECT 1 FROM sqlite_master
            WHERE type='table' AND name='rkb_writer_control'""").fetchone()
        if exists is None:
            return False
        row = db.execute("""SELECT r.* FROM runs r
            JOIN control c ON c.current_run=r.id
            JOIN rkb_writer_control w ON w.id=1 AND w.run_id=r.id
            WHERE r.id=? AND r.runtime_incarnation=? AND r.status='ready'
              AND r.launch_state='dispatched' AND r.channel_active=1
              AND r.attachment_conflict=0 AND r.connection_generation>=1
              AND r.provider_version IS NOT NULL AND w.active=1""",
            (run_id, incarnation)).fetchone()
        return row is not None and not self._queue._run_expired(row, self._queue.clock())

    @staticmethod
    def _exact_terminal(db, prior):
        # This table is written by BgeQueue.retire_provider_reservation only
        # after a native execution-qualified observation and CAS validation.
        # A test may inject a synthetic table row; production must not.
        row = db.execute("""SELECT r.*, t.receipt_json FROM runs r
            JOIN bge_provider_terminal_receipts t ON t.run_id=r.id
            WHERE r.id=? AND r.launch_state='terminal'""", (prior["run_id"],)).fetchone()
        if row is None:
            return False
        try:
            # Dev-only validator: do not import the controller/provider SDK
            # when this module is shipped inside the Kaggle native bundle.
            from .kaggle_provider_terminal import CONTRACT as terminal_contract
            receipt = json.loads(row["receipt_json"])
            identity = receipt["identity"]
            return (receipt["contract"] == terminal_contract
                    and identity["id"] == prior["run_id"]
                    and identity["runtime_incarnation"] == prior["incarnation"]
                    and identity["provider_ref"] == row["provider_ref"]
                    and identity["provider_version"] == row["provider_version"]
                    and identity["launch_nonce"] == row["launch_nonce"]
                    and not row["attachment_conflict"])
        except (TypeError, ValueError, KeyError):
            return False

    def claim(self, run_id, incarnation):
        _identity(run_id, incarnation)
        with self._queue.connect() as db:
            if not self._writer_current(db, run_id, incarnation):
                raise TelegramBindingUnavailable("telegram_writer_binding_required")
            self._ensure_slot_table(db)
            prior = db.execute("SELECT * FROM rkb_telegram_session_lease WHERE slot=?",
                               (SLOT,)).fetchone()
            if prior is not None:
                if ((prior["archive_chat_id"], prior["topic_id"])
                        != (self.archive_chat_id, self.topic_id)):
                    raise TelegramBindingUnavailable("telegram_archive_scope_changed")
                same = (prior["run_id"], prior["incarnation"]) == (run_id, incarnation)
                if not same and not self._exact_terminal(db, prior):
                    raise TelegramBindingUnavailable("telegram_prior_execution_unknown")
                if same:
                    return True
            db.execute("""INSERT INTO rkb_telegram_session_lease
                (slot,run_id,incarnation,archive_chat_id,topic_id)
                VALUES(?,?,?,?,?)
                ON CONFLICT(slot) DO UPDATE SET
                run_id=excluded.run_id,incarnation=excluded.incarnation,
                archive_chat_id=excluded.archive_chat_id,topic_id=excluded.topic_id""",
                (SLOT, run_id, incarnation, self.archive_chat_id, self.topic_id))
            return True

    def current(self, run_id, incarnation):
        try:
            _identity(run_id, incarnation)
            with self._queue.connect(write=False) as db:
                if db.execute("""SELECT 1 FROM sqlite_master WHERE type='table'
                    AND name='rkb_telegram_session_lease'""").fetchone() is None:
                    return False
                row = db.execute("""SELECT 1 FROM rkb_telegram_session_lease
                    WHERE slot=? AND run_id=? AND incarnation=?
                      AND archive_chat_id=? AND topic_id=?""",
                    (SLOT, run_id, incarnation, self.archive_chat_id, self.topic_id)).fetchone()
                return row is not None and self._writer_current(db, run_id, incarnation)
        except TelegramBindingUnavailable:
            return False


def _read_ephemeral_bundle(bundle_b64):
    """Decode only the injected RKB1 bundle; never retain/serialize the session."""
    if type(bundle_b64) is not str or not 1 <= len(bundle_b64) <= 16384:
        raise TelegramBindingUnavailable("telegram_rkb1_bundle_invalid")
    try:
        raw = base64.b64decode(bundle_b64, validate=True)
        if len(raw) > 12288:
            raise ValueError("too large")
        def distinct(pairs):
            out = {}
            for name, value in pairs:
                if name in out:
                    raise ValueError("duplicate")
                out[name] = value
            return out
        values = json.loads(raw.decode("utf-8"), object_pairs_hook=distinct)
        if type(values) is not dict or frozenset(values) != _BUNDLE_FIELDS:
            raise ValueError("wrong fields")
        secret = values.pop("session")
        if (type(secret) is not str or not 1 <= len(secret) <= 8192
                or not secret.isascii() or any(c.isspace() for c in secret)):
            raise ValueError("session")
        for name in _DEVICE_FIELDS:
            value = values[name]
            if (type(value) is not str or not 1 <= len(value) <= 160
                    or any(ord(c) < 32 or ord(c) == 127 for c in value)):
                raise ValueError("metadata")
        return secret, values
    except (ValueError, UnicodeError, binascii.Error, TypeError):
        raise TelegramBindingUnavailable("telegram_rkb1_bundle_invalid") from None


def build_rkb1_downloader(
    *,
    source_name, bundle_b64, api_id, api_hash,
    run_id, runtime_incarnation, archive_chat_id, topic_id,
    authenticated_lease_current, dependency_current,
    session_factory, client_factory, downloader_factory,
):
    """Construct a borrowed downloader ONLY after the existing WSS authority.

    The injected callable is the native projection of the authenticated
    Dev-side Rkb1Lease, not a boolean from the model or an unauthenticated frame.
    No connection, login, message read or WSS send happens here. In production
    client_factory=Telethon.TelegramClient and session_factory=StringSession;
    the separately attested Stage0 wheel is required before this method.
    """
    _identity(run_id, runtime_incarnation)
    if (source_name != SLOT or type(archive_chat_id) is not int or archive_chat_id >= 0
            or type(topic_id) is not int or topic_id != SOURCE_TOPIC
            or not all(callable(v) for v in (authenticated_lease_current,
                dependency_current, session_factory, client_factory, downloader_factory))):
        raise TelegramBindingUnavailable("telegram_rkb1_binding_required")

    def current():
        try:
            return authenticated_lease_current(run_id, runtime_incarnation) is True
        except Exception:
            return False

    if not current():
        raise TelegramBindingUnavailable("telegram_rkb1_lease_not_current")
    try:
        pinned = dependency_current()
    except Exception:
        pinned = False
    if pinned is not True:
        raise TelegramBindingUnavailable("telegram_rkb1_dependency_unverified")

    if type(api_id) is str and re.fullmatch(r"[1-9][0-9]{0,11}", api_id):
        api_id = int(api_id)
    if (type(api_id) is not int or api_id < 1 or type(api_hash) is not str
            or not re.fullmatch(r"[0-9a-fA-F]{32}", api_hash)):
        raise TelegramBindingUnavailable("telegram_rkb1_api_config_invalid")

    secret, device = _read_ephemeral_bundle(bundle_b64)
    try:
        # The caller owns client connection/disconnection; there is NO fallback
        # identity, implicit start(), QR/password login or second transporter.
        session = session_factory(secret)
        client = client_factory(session, api_id, api_hash, **device,
                                auto_reconnect=False, connection_retries=0,
                                request_retries=0)
        if not all(callable(getattr(client, name, None))
                   for name in ("get_messages", "iter_download", "is_user_authorized")):
            raise TypeError("not a usable Telethon client")
        if not current():
            raise TelegramBindingUnavailable("telegram_rkb1_lease_not_current")
        return downloader_factory(client=client, chat_id=archive_chat_id,
                                  topic_id=SOURCE_TOPIC, current=current)
    except TelegramBindingUnavailable:
        raise
    except Exception:
        # Exception bodies can contain provider/session values; never chain
        # them into logs, job results or model-visible errors.
        raise TelegramBindingUnavailable("telegram_rkb1_client_unavailable") from None


async def check_rkb1_authorized(client, current):
    """Native pre-import gate; caller must separately own connection authority.

    The helper does not call connect/start. Tests use only synthetic clients.
    The existing downloader itself checks the exact message/document on use.
    """
    if not callable(current) or not callable(getattr(client, "is_user_authorized", None)):
        raise TelegramBindingUnavailable("telegram_rkb1_client_unavailable")
    if current() is not True:
        raise TelegramBindingUnavailable("telegram_rkb1_lease_not_current")
    try:
        authorized = await client.is_user_authorized()
    except Exception:
        raise TelegramBindingUnavailable("telegram_rkb1_authorization_unavailable") from None
    if authorized is not True or current() is not True:
        raise TelegramBindingUnavailable("telegram_rkb1_authorization_unavailable")
    return True
