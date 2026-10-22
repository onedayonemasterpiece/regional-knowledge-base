"""Ephemeral RKB1 binding on the existing authenticated worker channel.

No credential acquisition, durable job, provider retries, or second transport.
Production credentials are injected by the owning service, never launch data.
"""
from __future__ import annotations
import asyncio
import inspect
from .telegram_runtime_binding import (Rkb1Lease, SLOT, TelegramBindingUnavailable,
    build_rkb1_downloader, check_rkb1_authorized)
from .telegram_original import TelegramOriginalLocator, NativeTelegramOriginalDownloader

class Rkb1ChannelSource:
    def __init__(self, *, queue, locator, credentials):
        if type(locator) is not TelegramOriginalLocator or not callable(credentials):
            raise TypeError("exact archive locator and owned ephemeral supplier required")
        self.queue,self.locator,self.credentials=queue,locator,credentials
        self.lease=Rkb1Lease(queue,archive_chat_id=locator.chat_id,topic_id=locator.topic_id)
        self.startup=None
        self._archive_auth=None

    def bind(self,startup):
        if startup.config.queue is not self.queue or (self.startup is not None and self.startup is not startup):
            raise TypeError("same startup owner required")
        self.startup=startup

    def current(self,run_id,incarnation,generation):
        s=self.startup
        try:
            b=s.binding
            return bool(s.imports_current() and s.command_binding_current(
                run_id=run_id,runtime_incarnation=incarnation,connection_generation=generation,
                database_generation=b.database_generation,writer_generation=s.config.writer_generation))
        except Exception:
            return False

    async def grant(self,run_id,incarnation,generation,*,retained_client):
        if type(retained_client) is not bool or not self.current(run_id,incarnation,generation):
            return None
        await asyncio.to_thread(self.lease.claim,run_id,incarnation)
        if not self.current(run_id,incarnation,generation):return None
        result=dict(run_id=run_id,runtime_incarnation=incarnation,source_name=SLOT,
                    locator=self.locator.wire())
        if not retained_client:
            # Never invoke the supplier at attach, before READY, or on a stale socket.
            try:
                values=self.credentials()
                if inspect.isawaitable(values):values=await values
                if type(values) is not dict or set(values)!={"bundle_b64","api_id","api_hash"}:
                    raise ValueError("fields")
                result.update(values)
            except Exception:
                raise TelegramBindingUnavailable("telegram_credential_supplier_unavailable") from None
        if (not await asyncio.to_thread(self.lease.current,run_id,incarnation)
                or not self.current(run_id,incarnation,generation)):
            return None
        return result

    async def confirm_archive_auth(self,run_id,incarnation,generation,authorized):
        identity=(run_id,incarnation,generation)
        if not authorized:
            if self._archive_auth==identity:self._archive_auth=None
            return False
        if (not await asyncio.to_thread(self.lease.current,run_id,incarnation)
                or not self.current(*identity)):
            return False
        self._archive_auth=identity
        return True

    def archive_auth_confirmed(self,run_id,incarnation,generation):
        identity=(run_id,incarnation,generation)
        return self._archive_auth==identity and self.current(*identity)

class NativeRkb1Channel:
    """One retained client, fenced by this existing intake's socket generation."""
    def __init__(self,*,intake,dependency_current,session_factory=None,client_factory=None):
        self.intake=intake
        self.dependency_current=dependency_current
        self.session_factory,self.client_factory=session_factory,client_factory
        self.client=None
        self.downloader=None
        self.locator=None
        self.granted_generation=None
        self.authorized_generation=None
        self._lock=asyncio.Lock()
        self.closed=False
        self.failed_generation=None

    def _lease_current(self,*identity):
        i=self.intake
        return bool(not self.closed and not i.closed and self.granted_generation is not None
            and i.generation==self.granted_generation
            and i.current((i.identity[0],i.identity[2],i.generation))
            and (not identity or identity==(i.identity[0],i.identity[2])))

    def current(self,*identity):
        return (self.authorized_generation is not None
            and self.authorized_generation==self.granted_generation
            and self._lease_current(*identity))

    @property
    def retained_client(self):return self.client is not None

    async def receive(self,frame):
        async with self._lock:
            i=self.intake;generation=frame.get("connection_generation")
            if (self.closed or i.closed or type(generation) is not int or generation!=i.generation
                    or (frame.get("run_id"),frame.get("runtime_incarnation"))!=(i.identity[0],i.identity[2])
                    or frame.get("source_name")!=SLOT
                    or not i.current((i.identity[0],i.identity[2],generation))):
                raise TelegramBindingUnavailable("telegram_channel_binding_changed")
            locator=TelegramOriginalLocator.from_wire(frame["locator"])
            if self.locator is not None and locator!=self.locator:
                raise TelegramBindingUnavailable("telegram_archive_scope_changed")
            secret_fields={"bundle_b64","api_id","api_hash"}
            if self.client is not None and secret_fields & frame.keys():
                raise TelegramBindingUnavailable("telegram_duplicate_credential_delivery")
            self.granted_generation=generation
            self.authorized_generation=None
            try:
                if self.client is None:
                    if not secret_fields<=frame.keys():
                        raise TelegramBindingUnavailable("telegram_ephemeral_credential_required")
                    if self.session_factory is None or self.client_factory is None:
                        from telethon import TelegramClient
                        from telethon.sessions import StringSession
                        session_factory,client_factory=StringSession,TelegramClient
                    else:session_factory,client_factory=self.session_factory,self.client_factory
                    holder=[]
                    def make_client(*args,**kwargs):
                        value=client_factory(*args,**kwargs);holder.append(value);return value
                    def make_downloader(**kwargs):
                        kwargs['current']=self.current
                        return NativeTelegramOriginalDownloader(**kwargs)
                    self.downloader=build_rkb1_downloader(source_name=SLOT,
                        bundle_b64=frame["bundle_b64"],api_id=frame["api_id"],api_hash=frame["api_hash"],
                        run_id=i.identity[0],runtime_incarnation=i.identity[2],
                        archive_chat_id=locator.chat_id,topic_id=locator.topic_id,
                        authenticated_lease_current=self._lease_current,dependency_current=self.dependency_current,
                        session_factory=session_factory,client_factory=make_client,
                        downloader_factory=make_downloader)
                    self.client=holder[0]
                if not self._lease_current():raise TelegramBindingUnavailable("telegram_channel_binding_changed")
                async with asyncio.timeout(30):
                    if not self.client.is_connected():await self.client.connect()
                    await check_rkb1_authorized(self.client,self._lease_current)
                    # Metadata-only exact source/topic probe: no PDF download.
                    message=await self.client.get_messages(locator.chat_id,ids=locator.message_id)
                    reply=getattr(message,"reply_to",None)
                    topic=getattr(reply,"reply_to_top_id",None) or getattr(reply,"reply_to_msg_id",None)
                    doc=getattr(message,"document",None)
                    if (getattr(message,"id",None)!=locator.message_id
                            or getattr(message,"chat_id",None)!=locator.chat_id or topic!=locator.topic_id
                            or getattr(doc,"id",None)!=locator.document_id or getattr(doc,"size",None)!=locator.size):
                        raise TelegramBindingUnavailable("telegram_exact_original_unavailable")
                if not self._lease_current():raise TelegramBindingUnavailable("telegram_channel_binding_changed")
                self.locator=locator
                await i.bind_telegram_downloader(self.downloader,identity=i.identity,generation=generation)
                if not self._lease_current():raise TelegramBindingUnavailable("telegram_channel_binding_changed")
                self.authorized_generation=generation
                self.failed_generation=None
            except BaseException as exc:
                self.granted_generation=None
                self.authorized_generation=None
                self.failed_generation=generation
                if isinstance(exc,asyncio.CancelledError):raise
                raise TelegramBindingUnavailable("telegram_native_binding_unavailable") from None
            finally:
                # The frame is exclusively owned by the transport's reader.
                for key in secret_fields:frame.pop(key,None)

    async def aclose(self):
        self.closed=True;self.granted_generation=None;self.authorized_generation=None
        if self.client is not None:
            try:
                async with asyncio.timeout(5):await self.client.disconnect()
            except Exception:pass