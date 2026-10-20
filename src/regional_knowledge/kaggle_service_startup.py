"""Explicit controller startup for the Kaggle source vertical.

All owners are supplied by the existing controller. This module creates neither
credentials nor a corpus/storage authority, and never opens Kaggle's PG socket
from Dev. Native history observations belong to a fixed maintenance adapter.
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
import logging
import math
from typing import Callable, Mapping, Protocol, runtime_checkable

from mcp.server.mcpserver.exceptions import ToolError

from .bge_queue import BgeQueue
from .control_spool import ControlSpool
from .kaggle_public_backend import KaggleSourceBackend, TargetReadBinding, current_binding
from .kaggle_source_commands import AcceptedSourceDispatch
from .oauth_provider import RegionalOAuthProvider
from .participants import Participants
from .read_authority import ReadAuthority
from .source_read_gate import SourceReadGate

_LOG=logging.getLogger(__name__)


class SourceHistoryCoverage(Protocol):
    def current(self) -> bool: ...


class SourceHistorySession(Protocol):
    spool: ControlSpool
    routes: Mapping[str, Callable]
    coverage_seconds: float

    async def restore(self, binding: TargetReadBinding) -> SourceHistoryCoverage: ...
    async def extend(self, binding: TargetReadBinding) -> SourceHistoryCoverage: ...


@runtime_checkable
class NativeHistoryProofAdapter(Protocol):
    """Trusted fixed SOURCE_HISTORY maintenance observations, never SQL RPC.

    The adapter verifies remote journal bytes on Dev and native bounded
    prefix/catalog/ACL observations beside PG. Missing/unknown observations
    cannot return a current coverage capability. Binding is startup-only and
    must retain the actual route callables and supplied existing spool.
    """
    def bind(self, *, spool: ControlSpool, routes: Mapping[str, Callable],
             binding_current: Callable, writer_generation: str) -> SourceHistorySession: ...


@dataclass(frozen=True)
class TargetServiceConfig:
    oauth_provider: RegionalOAuthProvider
    participants: Participants
    queue: BgeQueue
    spool: ControlSpool
    history_adapter: NativeHistoryProofAdapter
    writer_generation: str
    writer_current: Callable
    destination_ref: str = 'rkb-source-http-response'
    refresh_seconds: float = 5
    writer_owner: object = None
    import_originals: object = None
    import_tokenizer: object = None
    import_parser_sha256: str | None = None
    import_launch_config_sha256: str | None = None

    def __post_init__(self):
        if (not isinstance(self.oauth_provider,RegionalOAuthProvider)
                or not isinstance(self.participants,Participants)
                or not isinstance(self.queue,BgeQueue) or not isinstance(self.spool,ControlSpool)
                or self.spool.queue is not self.queue or self.spool.participants is not self.participants):
            raise TypeError('existing canonical OAuth/participants/queue/control spool owners required')
        policy=self.oauth_provider.personal_policy
        if (policy is None or policy.participants is not self.participants
                or self.oauth_provider.owner_subject!=self.participants.owner):
            raise ValueError('configured current personal OAuth projection required')
        if not isinstance(self.history_adapter,NativeHistoryProofAdapter):
            raise TypeError('typed fixed native history proof adapter required')
        if (not callable(self.writer_current) or not isinstance(self.writer_generation,str)
                or not 1<=len(self.writer_generation.encode())<=256):
            raise ValueError('existing current writer authority required')
        if self.writer_owner is not None:
            from .queue_writer import QueueWriterAuthority
            if (type(self.writer_owner) is not QueueWriterAuthority or self.writer_owner.queue is not self.queue
                    or self.writer_owner.writer_generation!=self.writer_generation
                    or self.spool.journal.store.writer_guard is not self.writer_owner
                    or getattr(self.writer_current,'__self__',None) is not self.writer_owner):
                raise ValueError('same persisted writer owner must guard journal and current reads')
        if (not isinstance(self.destination_ref,str) or not 1<=len(self.destination_ref.encode())<=256
                or type(self.refresh_seconds) not in (int,float) or not math.isfinite(self.refresh_seconds)
                or not 0<self.refresh_seconds<=10):
            raise ValueError('bounded destination and maintenance interval required')
        if (len({value is None for value in (self.import_originals,self.import_tokenizer,
                self.import_parser_sha256,self.import_launch_config_sha256)}) != 1):
            raise TypeError('native originals and exact independent tokenizer must be supplied together')
        if self.import_originals is not None:
            from .original_intake_controller import OriginalIntakeController
            from .kaggle_import_tokenizer import HfImportTokenCounter
            import re
            if (type(self.import_originals) is not OriginalIntakeController
                    or type(self.import_tokenizer) is not HfImportTokenCounter
                    or self.import_originals.queue is not self.queue
                    or self.import_originals.spool is not self.spool
                    or self.import_originals.participants is not self.participants):
                raise TypeError('same existing native intake control owners required')
            if any(type(value) is not str or re.fullmatch('[0-9a-f]{64}',value) is None
                    for value in (self.import_parser_sha256,self.import_launch_config_sha256)):
                raise ValueError('independent target intake parser/config pins required')


class _SourceBackend(KaggleSourceBackend):
    def _importer(self):
        if self.startup.importer is None:
            raise ToolError('{"status":"unavailable","reason":"target_importer_unavailable"}')
        return self.startup.importer

    async def book_ingest(self, **kwargs):
        return await self._importer().book_ingest(**kwargs)

    async def book_pages(self, **kwargs):
        return await self._importer().book_pages(**kwargs)

    async def _ingestion_row(self, *, principal, ingestion_id):
        return await self._importer().owned_row(principal, ingestion_id)

    async def _search(self,*args,**kwargs):
        if not self.startup.reads_current():
            raise ToolError('{"status":"unavailable","reason":"source_history_unavailable"}')
        return await super()._search(*args,**kwargs)


class TargetServiceStartup:
    """One configured queue/admission/spool; ownership lasts one server lifespan.

    Public reads start closed. The WSS control route remains available while
    bounded background maintenance establishes or recovers exact history.
    Shutdown drains local remote-outcome publications, then releases local
    owners without claiming compute termination or clearing durable work.
    """
    def __init__(self,config: TargetServiceConfig):
        if type(config) is not TargetServiceConfig:
            raise TypeError('explicit typed target service configuration required')
        self.config=config
        admission=getattr(config.queue,'_shared_reads',None)
        if admission is None:
            authority=ReadAuthority(config.participants,oauth_provider=config.oauth_provider,
                issuer=config.oauth_provider.issuer,resource=config.oauth_provider.resource)
            admission=config.queue.configure_shared_reads(authority)
        else:
            authority=admission.authority
            if authority.provider is not config.oauth_provider or authority.participants is not config.participants:
                raise ValueError('existing admission belongs to another canonical authority')
        self.authority,self.admission=authority,admission
        self.binding=self.coverage=None
        self.running=False;self.closed=False;self._maintenance=None
        self.gate=SourceReadGate(authority,config.spool,binding_current=self.binding_current)
        self.backend=_SourceBackend(worker_queue=config.queue,read_authority=authority,
            source_gate=self.gate,destination_ref=config.destination_ref)
        self.backend.startup=self
        self.routes={'source.search':self.backend.search,'source.fetch':self.backend.fetch,
            'source.withdraw':config.spool.stage_source_withdraw,'source.import':config.spool.stage_source_import}
        if self.gate.story_supported():
            self.routes.update({'story.search':self.backend.story_search,'story.get':self.backend.story_get,
                'story.history':self.backend.story_history,'story.variants':self.backend.story_variants})
        if self.gate.facts_supported():
            self.routes.update({'source.fact_read':self.backend.source_fact_read,
                'source.calendar':self.backend.story_calendar,
                'source.observations':self.backend.story_observation_search})
        self.history=config.history_adapter.bind(spool=config.spool,routes=self.routes,
            binding_current=self.binding_current,writer_generation=config.writer_generation)
        if (self.history.spool is not config.spool or set(self.history.routes)!=set(self.routes)
                or any(self.history.routes[k] is not value for k,value in self.routes.items())
                or not 3*config.refresh_seconds<self.history.coverage_seconds<=120):
            raise ValueError('actual route closure and bounded history maintenance required')
        bridge=getattr(config.queue,'_source_commands',None)
        if bridge is not None:
            if bridge.spool is not config.spool:
                raise ValueError('existing source dispatcher belongs to another control spool')
            self.source_dispatch=bridge
        else:
            self.source_dispatch=AcceptedSourceDispatch(config.spool,binding_current=self.command_binding_current)
        self.importer = None
        if config.import_originals is not None:
            from .kaggle_importer import KaggleSourceImporter
            from .file_ingress import ChatFileDownloader
            self.importer = KaggleSourceImporter(self, originals=config.import_originals,
                tokenizer=config.import_tokenizer)
            # Only bounded stage JSON uses this Dev downloader. Original PDFs
            # are acquired by the configured native controller.
            self.backend.file_downloader = ChatFileDownloader()

    def binding_current(self,binding):
        try:
            return (not self.closed and type(binding) is TargetReadBinding
                and current_binding(self.config.queue)==binding
                and self.config.writer_current(database_generation=binding.database_generation,
                    writer_generation=self.config.writer_generation))
        except Exception:return False

    def command_binding_current(self,*,run_id,runtime_incarnation,connection_generation,
                                database_generation,writer_generation):
        try:
            binding=current_binding(self.config.queue)
            return (writer_generation==self.config.writer_generation and self.binding_current(binding)
                and (binding.run_id,binding.runtime_incarnation,binding.connection_generation,binding.database_generation)
                ==(run_id,runtime_incarnation,connection_generation,database_generation))
        except Exception:return False

    def reads_current(self):
        try:
            return bool(self.running and not self.closed and self.coverage is not None
                and self.coverage.current() and self.binding_current(self.binding))
        except Exception:return False

    async def refresh(self):
        """Maintenance only; ready reads never wait for native/journal writes."""
        if not self.running or self.closed:return False
        try:
            binding=await asyncio.to_thread(current_binding,self.config.queue)
            if self.config.writer_owner is not None:
                await self.config.writer_owner.ensure_current(self.config.spool.journal)
            if not self.binding_current(binding):raise PermissionError('current writer binding unavailable')
            if binding==self.binding and self.coverage is not None and self.coverage.current():
                coverage=await self.history.extend(self.binding)
            else:
                self.coverage=None;self.gate.invalidate();self.binding=binding
                coverage=await self.history.restore(binding)
            if not coverage.current() or not self.binding_current(self.binding):
                raise PermissionError('native history proof no longer current')
            self.coverage=coverage
            _LOG.info('target_source_routing state=ready connection_generation=%s',binding.connection_generation)
            return True
        except asyncio.CancelledError:
            self.coverage=None;self.gate.invalidate();raise
        except Exception as exc:
            self.coverage=None;self.gate.invalidate()
            _LOG.warning('target_source_routing state=closed reason=%s',type(exc).__name__)
            return False

    async def _maintain(self):
        while True:
            await asyncio.sleep(self.config.refresh_seconds)
            await self.refresh()

    @asynccontextmanager
    async def lifespan(self,server):
        if self.running or self.closed:raise RuntimeError('one target service lifespan required')
        self.running=True
        try:
            await self.refresh()
            self._maintenance=asyncio.create_task(self._maintain(),name='rkb-source-history-maintenance')
            yield self
        finally:
            self.running=False;self.coverage=None;self.gate.invalidate()
            if self._maintenance is not None:
                self._maintenance.cancel()
                try:await self._maintenance
                except asyncio.CancelledError:pass
            try:await self.source_dispatch.aclose()
            finally:
                self.closed=True
                self.admission.close()
                self.config.spool.close()
                if self.config.writer_owner is not None:self.config.writer_owner.close()
