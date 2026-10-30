"""Direct Telegram document ingress in Kaggle; no session creation or byte proxy.

The runtime owner supplies its already-authorized, exclusively leased Telethon
client. A caller locator is not an archive grant: the configured chat/topic and
live lease predicate are independently required. This module never logs in, reads credentials or closes the shared client;
archive sends use that same leased client.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, asdict
import hashlib
from pathlib import Path

from .file_ingress import DownloadedFile
from .original_intake import OriginalIntakeError, canonical, digest, hash_value


@dataclass(frozen=True)
class TelegramOriginalLocator:
    chat_id: int
    topic_id: int
    message_id: int
    document_id: int | None = None
    sha256: str | None = None
    size: int | None = None

    def __post_init__(self):
        if (type(self.chat_id) is not int or not -(2**63) < self.chat_id < 0
                or any(type(v) is not int or not 0 < v < 2**31
                       for v in (self.topic_id, self.message_id))
                or self.document_id is not None and (type(self.document_id) is not int
                    or not -(2**63) < self.document_id < 2**63 or self.document_id == 0)
                or self.size is not None and (type(self.size) is not int or not 5 <= self.size <= 512*1024*1024)):
            raise OriginalIntakeError('original_telegram_locator_invalid')
        if self.sha256 is not None:hash_value(self.sha256)

    def wire(self):
        return asdict(self)

    @classmethod
    def from_wire(cls, value):
        if type(value) is not dict or set(value) != set(cls.__dataclass_fields__):
            raise OriginalIntakeError('original_telegram_locator_invalid')
        return cls(**value)

    @property
    def file_id(self):
        return 'telegram:' + digest(canonical(self.wire()))

    @property
    def file_name(self):
        return 'telegram-original.pdf'

    @property
    def mime_type(self):
        return 'application/pdf'


@dataclass(frozen=True, slots=True)
class TelegramDownloadedFile(DownloadedFile):
    telegram_locator: TelegramOriginalLocator


class NativeTelegramOriginalDownloader:
    """Borrow the existing owner client; scope and lease are runtime capabilities."""

    def __init__(self, *, client, chat_id, topic_id, current):
        if (type(chat_id) is not int or not -(2**63) < chat_id < 0
                or type(topic_id) is not int or not 0 < topic_id < 2**31
                or not callable(current) or not callable(getattr(client, 'get_messages', None))
                or not callable(getattr(client, 'iter_download', None))):
            raise OriginalIntakeError('original_telegram_binding_required')
        self._client, self._current = client, current
        self._chat_id, self._topic_id = chat_id, topic_id

    def _check(self, locator, current):
        if self._current() is not True or current() is not True:
            raise OriginalIntakeError('original_telegram_lease_changed')
        if (locator.chat_id, locator.topic_id) != (self._chat_id, self._topic_id):
            raise OriginalIntakeError('original_telegram_scope_denied')

    async def presence(self, locator, *, current):
        """Metadata-only exact document check inside the leased topic."""
        if type(locator) is not TelegramOriginalLocator or not callable(current):
            raise OriginalIntakeError('original_telegram_locator_invalid')
        self._check(locator, current)
        try:
            message = await self._client.get_messages(self._chat_id, ids=locator.message_id)
        except Exception:
            raise OriginalIntakeError('original_telegram_presence_unknown') from None
        self._check(locator, current)
        document = getattr(message, 'document', None)
        reply = getattr(message, 'reply_to', None)
        topic = getattr(reply, 'reply_to_top_id', None) or getattr(reply, 'reply_to_msg_id', None)
        if (getattr(message, 'id', None) != locator.message_id
                or getattr(message, 'chat_id', None) != self._chat_id
                or topic != self._topic_id
                or type(getattr(document, 'id', None)) is not int
                or not -(2**63) < document.id < 2**63 or document.id == 0
                or type(getattr(document, 'size', None)) is not int
                or not 5 <= document.size <= 512*1024*1024
                or locator.document_id is not None and document.id != locator.document_id
                or locator.size is not None and document.size != locator.size):
            raise OriginalIntakeError('original_telegram_document_changed')
        return message

    async def find_archive(self, *, sha256, size, ingestion_id, current):
        """One bounded metadata observation; absence never authorizes a send."""
        if not callable(current) or type(ingestion_id) is not str:
            raise OriginalIntakeError('original_telegram_binding_required')
        if self._current() is not True or current() is not True:
            raise OriginalIntakeError('original_telegram_lease_changed')
        caption = 'RKB-original ' + ingestion_id
        try:
            # Exact caption reconciliation precedes provider send after lost ACK.
            async for candidate in self._client.iter_messages(self._chat_id, search=caption, limit=10):
                if getattr(candidate, 'message', None) != caption:
                    continue
                reply = getattr(candidate, 'reply_to', None)
                if (getattr(reply, 'reply_to_top_id', None) or getattr(reply, 'reply_to_msg_id', None)) != self._topic_id:
                    continue
                doc = getattr(candidate, 'document', None)
                locator = TelegramOriginalLocator(self._chat_id, self._topic_id,
                    candidate.id, getattr(doc, 'id', 0), sha256, size)
                await self.presence(locator, current=current)
                return locator
        except OriginalIntakeError:
            raise
        except Exception:
            raise OriginalIntakeError('original_telegram_presence_unknown') from None
        if self._current() is not True or current() is not True:
            raise OriginalIntakeError('original_telegram_lease_changed')
        return None

    async def archive(self, path, *, sha256, size, ingestion_id, current, previous=None, file_name=None, mime_type=None):
        """One document per immutable intake, using only the retained leased client."""
        if previous is not None:
            await self.presence(previous, current=current)
            return previous
        found = await self.find_archive(sha256=sha256, size=size, ingestion_id=ingestion_id, current=current)
        if found is not None:
            return found
        try:
            if self._current() is not True or current() is not True:
                raise OriginalIntakeError('original_telegram_lease_changed')
            from telethon.tl.types import DocumentAttributeFilename
            sent = await self._client.send_file(self._chat_id, str(path), caption='RKB-original ' + ingestion_id,
                reply_to=self._topic_id, force_document=True, mime_type=mime_type,
                attributes=[DocumentAttributeFilename(file_name or Path(path).name)])
            doc = getattr(sent, 'document', None)
            locator = TelegramOriginalLocator(self._chat_id, self._topic_id,
                getattr(sent, 'id', 0), getattr(doc, 'id', 0), sha256, size)
            await self.presence(locator, current=current)
        except OriginalIntakeError as exc:
            if str(exc)=='original_telegram_presence_unknown':
                raise OriginalIntakeError('original_telegram_send_unknown') from None
            raise
        except Exception:
            raise OriginalIntakeError('original_telegram_send_unknown') from None
        return locator

    async def download(self, locator, directory, *, max_bytes, current):
        if type(locator) is not TelegramOriginalLocator or not callable(current):
            raise OriginalIntakeError('original_telegram_locator_invalid')
        if (type(max_bytes) is not int or not 5 <= max_bytes <= 512*1024*1024
                or locator.size is not None and locator.size > max_bytes):
            raise OriginalIntakeError('original_telegram_byte_bound')
        path = Path(directory) / 'telegram-original.source'
        iterator = None
        acquired = False
        try:
            async with asyncio.timeout(120):
                message = await self.presence(locator, current=current)
                expected_size = message.document.size
                if expected_size > max_bytes:
                    raise OriginalIntakeError('original_telegram_byte_bound')
                iterator = self._client.iter_download(message.document, request_size=65536, chunk_size=65536)
                size = 0
                sha = hashlib.sha256()
                with path.open('xb') as output:
                    async for chunk in iterator:
                        self._check(locator, current)
                        if not isinstance(chunk, (bytes, memoryview)) or not 0 < len(chunk) <= 65536:
                            raise OriginalIntakeError('original_telegram_chunk_invalid')
                        size += len(chunk)
                        if size > expected_size:
                            raise OriginalIntakeError('original_telegram_byte_bound')
                        sha.update(chunk)
                        output.write(chunk)
                self._check(locator, current)
                if size != expected_size or locator.sha256 is not None and sha.hexdigest() != locator.sha256:
                    raise OriginalIntakeError('original_telegram_bytes_changed')
                # The existing native parser identifies PDF versus DjVu after
                # download; a Telegram locator does not assert a MIME type.
                acquired = True
                resolved = TelegramOriginalLocator(locator.chat_id, locator.topic_id, locator.message_id,
                    message.document.id, sha.hexdigest(), size)
                return TelegramDownloadedFile(path, sha.hexdigest(), size, resolved)
        except (OriginalIntakeError, asyncio.CancelledError):
            raise
        except Exception:
            # Provider exceptions may contain a private message or session data.
            raise OriginalIntakeError('original_telegram_download_unavailable') from None
        finally:
            if not acquired:
                path.unlink(missing_ok=True)
            if iterator is not None:
                # Telethon's iterator owns a borrowed download sender, not the
                # shared client. Closing it is required on early termination.
                try:
                    async with asyncio.timeout(2):
                        await iterator.close()
                except Exception:
                    pass
