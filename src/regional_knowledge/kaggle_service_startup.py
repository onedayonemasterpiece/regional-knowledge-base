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
import json
import math
import time
from typing import Callable, Mapping, Protocol, runtime_checkable

from mcp.server.mcpserver.exceptions import ToolError

from .bge_queue import BgeQueue
from .control_spool import ControlSpool
from .kaggle_public_backend import KaggleSourceBackend, TargetReadBinding, TargetUnavailable, current_binding, binding_is_current_reader
from .kaggle_source_commands import AcceptedSourceDispatch
from .oauth_provider import RegionalOAuthProvider
from .participants import Participants
from .read_authority import ReadAuthority
from .source_read_gate import SourceReadGate

_LOG=logging.getLogger(__name__)
GEO_RETRY_SECONDS=300


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
    geo_manifest_operation_keys: tuple[str,...] = ()
    source_restore: object = None
    source_snapshot: object = None
    geo_bootstrap: object = None
    dataset_seed: object = None
    telegram_runtime_source: object = None

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
        if (type(self.geo_manifest_operation_keys) is not tuple or len(self.geo_manifest_operation_keys)>256
                or len(set(self.geo_manifest_operation_keys))!=len(self.geo_manifest_operation_keys)
                or any(type(key) is not str or not 1<=len(key.encode())<=256 for key in self.geo_manifest_operation_keys)
                or len(self.geo_manifest_operation_keys)==1):
            raise ValueError('bounded exact GEO manifest operation keys required')
        if (len({value is None for value in (self.import_originals,self.import_tokenizer,
                self.import_parser_sha256,self.import_launch_config_sha256)}) != 1):
            raise TypeError('native originals and exact independent tokenizer must be supplied together')
        if self.source_restore is not None:
            from .kaggle_restore_startup import SourceRestoreConfig
            if type(self.source_restore) is not SourceRestoreConfig or self.writer_owner is None:
                raise TypeError('typed restore config and same persisted writer owner required')
        if self.source_restore is not None and self.source_snapshot is not None:
            raise ValueError('restore and first empty snapshot are distinct startup modes')
        if self.source_snapshot is not None:
            from .kaggle_snapshot_startup import SourceSnapshotConfig
            if type(self.source_snapshot) is not SourceSnapshotConfig or self.writer_owner is None:
                raise TypeError('typed first snapshot config and same persisted writer required')
        if self.geo_bootstrap is not None:
            from .kaggle_geo_install import SourceGeoInstallConfig
            if (type(self.geo_bootstrap) is not SourceGeoInstallConfig or not self.geo_manifest_operation_keys
                    or self.source_snapshot is None and self.source_restore is None):
                raise TypeError('fixed GEO bootstrap requires exact keys and verified recovery owner')
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
        if self.startup.snapshot is not None and not self.startup.snapshot.verified:
            raise ToolError('{"status":"unavailable","reason":"first_snapshot_unverified"}')
        if self.startup.importer is None:
            raise ToolError('{"status":"unavailable","reason":"target_importer_unavailable"}')
        return self.startup.importer

    async def book_ingest(self, **kwargs):
        return await self._importer().book_ingest(**kwargs)

    async def book_pages(self, **kwargs):
        return await self._importer().book_pages(**kwargs)

    async def _ingestion_row(self, *, principal, ingestion_id):
        return await self._importer().owned_row(principal, ingestion_id)

    async def story_edit(self,story_id,principal,*,expected_revision,operations,idempotency_key,reason=None):
        from .story_contracts import UpsertVariant
        if self.startup.editorial is None:raise ToolError('{"status":"unavailable","reason":"target_editorial_unavailable"}')
        if len(operations)!=1 or type(operations[0]) is not UpsertVariant:
            raise ToolError('{"status":"unavailable","reason":"unsupported_target_story_operation"}')
        return await self.startup.editorial.mutate(principal,story_id=story_id,expected_revision=expected_revision,
            idempotency_key=idempotency_key,operation=operations[0],reason=reason)

    async def story_access(self,story_id,principal,*,expected_revision,grantee_user_id,capability,idempotency_key):
        if self.startup.editorial is None:raise ToolError('{"status":"unavailable","reason":"target_editorial_unavailable"}')
        return await self.startup.editorial.mutate(principal,story_id=story_id,expected_revision=expected_revision,
            idempotency_key=idempotency_key,grantee_user_id=grantee_user_id,capability=capability)


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
        if self.gate.catalog_supported():
            self.routes['source.catalog']=self.backend.book_find
        if self.gate.story_supported():
            self.routes.update({'story.search':self.backend.story_search,'story.get':self.backend.story_get,
                'story.history':self.backend.story_history,'story.variants':self.backend.story_variants})
        if self.gate.facts_supported():
            self.routes.update({'source.fact_read':self.backend.source_fact_read,
                'source.calendar':self.backend.story_calendar,
                'source.observations':self.backend.story_observation_search,
                'source.observation_compare':self.backend.story_observation_compare})
        self.editorial=None
        editorial_available=self.gate.editorial_supported() and config.import_tokenizer is not None
        if editorial_available:self.routes.update({'story.edit':self.backend.story_edit,'story.access':self.backend.story_access})
        if self.gate.graph_supported():
            self.routes.update({'source.entity_read':self.backend.source_graph_fetch,
                'source.entity_list':self.backend.entity_list})
        self.geo_gate=self.geo_backend=None;self._geo_denied=None
        if config.geo_manifest_operation_keys:
            from .geo_read_gate import GeoReadGate
            from .kaggle_geo_backend import KaggleGeoBackend
            self.geo_gate=GeoReadGate(authority,config.spool,binding_current=self.binding_current)
            if not self.geo_gate.supported():
                raise ValueError('explicit fresh11 GEO handler required for GEO startup')
            self.geo_backend=KaggleGeoBackend(worker_queue=config.queue,read_authority=authority,
                geo_gate=self.geo_gate,destination_ref=config.destination_ref,current=self.reads_current)
            self.routes.update({'geo.identity_read':self.geo_backend.geo_lookup,
                'geo.members_read':self.geo_backend.graph_fetch})
        from .case_read_gate import CaseReadGate
        self.case_gate=CaseReadGate(authority,config.spool,binding_current=self.binding_current)
        self.cases_backend=None
        if self.case_gate.cases_supported():
            from .kaggle_cases_backend import KaggleCasesBackend
            self.cases_backend=KaggleCasesBackend(self,case_gate=self.case_gate)
            self.routes.update({'case.read':self.cases_backend.disagreement_get,
                'case.edit':self.cases_backend.disagreement_edit,'case.access':config.spool.stage_domain_command,
                'case.contribute':self.cases_backend.disagreement_contribute,
                'case.conclusion':self.cases_backend.disagreement_conclusion})
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
        if config.telegram_runtime_source is not None:
            from .telegram_runtime_channel import Rkb1ChannelSource
            owner=config.telegram_runtime_source
            if type(owner) is not Rkb1ChannelSource or owner.queue is not config.queue:
                raise TypeError('same owned RKB1 source required')
            owner.bind(self)
            config.queue._telegram_runtime_source=owner
        self.importer = None
        if config.import_originals is not None:
            from .kaggle_importer import KaggleSourceImporter
            from .file_ingress import ChatFileDownloader
            self.importer = KaggleSourceImporter(self, originals=config.import_originals,
                tokenizer=config.import_tokenizer)
            # Only bounded stage JSON uses this Dev downloader. Original PDFs
            # are acquired by the configured native controller.
            self.backend.file_downloader = ChatFileDownloader()
        if editorial_available:
            from .kaggle_story_editorial_backend import KaggleStoryEditorial
            self.editorial=KaggleStoryEditorial(self,tokenizer=config.import_tokenizer)

        self.restorer=None
        if config.source_restore is not None:
            from .kaggle_restore_startup import SourceRestoreCoordinator
            self.restorer=SourceRestoreCoordinator(self,config.source_restore)

        self.snapshot=None
        if config.source_snapshot is not None:
            from .kaggle_snapshot_startup import SourceSnapshotCoordinator
            self.snapshot=SourceSnapshotCoordinator(self,config.source_snapshot)
            config.spool.configure_intake_guard(lambda:not self.closed and self.snapshot.verified)

        self.geo_installer=None
        if config.geo_bootstrap is not None:
            from .kaggle_geo_install import SourceGeoInstaller
            self.geo_installer=SourceGeoInstaller(self,config.geo_bootstrap)
        self.dataset_seed=None
        if config.dataset_seed is not None:
            from .kaggle_dataset_seed import SourceDatasetSeed, DatasetSeedConfig
            from .kaggle_dataset_publisher import KaggleDatasetPublisher, authenticated_service_api
            if type(config.dataset_seed) is not DatasetSeedConfig:
                raise TypeError('typed Dataset seed required')
            api=authenticated_service_api()
            self.dataset_seed=SourceDatasetSeed(self,config.dataset_seed,KaggleDatasetPublisher(api,
                provider_ref=config.dataset_seed.provider_ref,owner=api.get_config_value(api.CONFIG_NAME_USER)))
        self.dataset_checkpoints=None
        if getattr(config.source_restore,'dataset_checkpoint',None) is not None:
            if self.dataset_seed is not None or self.snapshot is not None:
                raise ValueError('Dataset primary must own the existing checkpoint path exclusively')
            from .kaggle_dataset_checkpoint import SourceDatasetCheckpoints
            self.dataset_checkpoints=SourceDatasetCheckpoints(self)
        from .kaggle_schema_owner import SourceSchemaMigrations
        self.schema_migrations=SourceSchemaMigrations(self)
        config.spool._schema_intake_guard=self.schema_migrations.intake_allowed

    def promote_schema_context(self,version):
        """The authenticated migration result advances metadata in this owner."""
        from dataclasses import replace
        from .kaggle_pg_snapshot_native import encode
        adapter=self.config.history_adapter
        database=replace(adapter.database,schema_version=version)
        adapter.database=database
        self.history.database=database;self.history.schema_version=version
        self.history._native_prefix=None
        self.restorer.database=database
        self.restorer.dispatch.database=database
        proof=self.restorer.proof
        if proof is not None:
            control=json.loads(proof.control);control['schema_version']=version
            object.__setattr__(proof,'database',database)
            object.__setattr__(proof,'control',encode(control))

    def binding_current(self,binding):
        try:
            return (not self.closed and type(binding) is TargetReadBinding
                and binding_is_current_reader(self.config.queue,binding)
                and self.config.writer_current(database_generation=binding.database_generation,
                    writer_generation=self.config.writer_generation))
        except Exception:return False

    def command_binding_current(self,*,run_id,runtime_incarnation,connection_generation,
                                database_generation,writer_generation):
        try:
            binding=current_binding(self.config.queue)
            snapshot=getattr(self,'snapshot',None)
            schema=getattr(self,'schema_migrations',None)
            return ((schema is None or schema.writes_allowed()) and (snapshot is None or snapshot.verified) and writer_generation==self.config.writer_generation and self.binding_current(binding)
                and (binding.run_id,binding.runtime_incarnation,binding.connection_generation,binding.database_generation)
                ==(run_id,runtime_incarnation,connection_generation,database_generation))
        except Exception:return False

    def reads_current(self):
        try:
            return bool(self.running and not self.closed and (self.snapshot is None or self.snapshot.verified) and self.coverage is not None
                and self.coverage.current() and self.binding_current(self.binding))
        except Exception:return False

    def imports_current(self):
        # Exact fresh11 source imports require reviewed GEO identities first.
        # Ordinary reads and GEO command installation must remain available
        # while those same accepted operations are being reconciled.
        if not self.reads_current():return False
        if not self.gate.graph_supported():return True
        installer=getattr(self,'geo_installer',None)
        if installer is not None and not installer.current():return False
        keys=installer.effective_keys if installer is not None else self.config.geo_manifest_operation_keys
        return bool(self.geo_gate is not None and self.geo_gate.installation_current(self.binding,keys))

    def _coverage_idle(self):
        """Nothing new to verify and ample time left on the current proof.

        Re-observing an unchanged native prefix every tick only adds WSS/queue
        work and PG SHARE locks; the proof keeps its own deadline and any new
        accepted/applied command, pending restriction or write still extends.
        """
        c=self.coverage;spool=self.config.spool
        verifier=getattr(c,'verifier',None);deadline=getattr(c,'deadline',None)
        if verifier is None or deadline is None:return False
        margin=2*self.config.refresh_seconds+getattr(verifier,'timeout_seconds',10)
        return (deadline-verifier.clock()>margin and not c.pending and not spool._writing
                and spool._source_anchor is c
                and c.applied_sequence==c.accepted_sequence==spool._source_covered_sequence)

    async def refresh(self):
        if self.dataset_seed is not None and self.dataset_seed.published():
            # The one-time seed is finished. Do not keep polling S3 as a live
            # journal while the controller prepares the Dataset restore.
            self.coverage=None;self.gate.invalidate()
            return False
        """Maintenance only; ready reads never wait for native/journal writes."""
        if not self.running or self.closed:return False
        try:
            owner=self.config.writer_owner
            if owner is not None:
                # Read back an owned UNKNOWN publication even if its worker
                # disconnected, before restore/current-runtime checks reject it.
                with self.config.queue.connect(write=False) as db:
                    pending=db.execute('SELECT 1 FROM rkb_writer_control WHERE run_id=? AND writer_generation=? AND publication_json IS NOT NULL',
                        (owner.run_id,self.config.writer_generation)).fetchone()
                if pending:await owner.reconcile(self.config.spool.journal.store)
            if self.restorer is not None and not self.restorer.complete:
                await self.restorer.refresh()
            try:
                binding=await asyncio.to_thread(current_binding,self.config.queue)
            except TargetUnavailable:
                # The migrated worker can reconnect before its lost receipt
                # advances our observed version. Reconcile that original job
                # through existing pre-ready authority, then resume READY.
                self.schema_migrations.resume_attachment()
                raise
            if self.config.writer_owner is not None:
                await self.config.writer_owner.ensure_current(self.config.spool.journal)
            if not self.binding_current(binding):raise PermissionError('current writer binding unavailable')
            if binding==self.binding and self.coverage is not None and self.coverage.current():
                coverage=self.coverage if self._coverage_idle() else await self.history.extend(self.binding)
            else:
                self.coverage=None;self.gate.invalidate();self.binding=binding
                if self.geo_gate is not None:self.geo_gate.invalidate()
                self.case_gate.invalidate()
                coverage=await self.history.restore(binding)
            if not coverage.current() or not self.binding_current(self.binding):
                raise PermissionError('native history proof no longer current')
            self.coverage=coverage
            if self.schema_migrations.pending() is not None:
                # Dataset upload/readback may take minutes; keep ordinary
                # history/read coverage refreshing beside the same barrier.
                self.schema_migrations.kick(self.binding)
                return self.reads_current()
            if self.dataset_checkpoints is not None:
                from .kaggle_dataset_checkpoint import CheckpointPending
                try:
                    if await self.dataset_checkpoints.refresh(self.binding):
                        self.coverage=await self.history.extend(self.binding)
                except CheckpointPending as exc:
                    # The pointer did not move and the verified prefix is
                    # unchanged: keep reads on the current base, retry later.
                    _LOG.warning('target_dataset_checkpoint state=pending reason=%s',exc.args[0])
            if self.dataset_seed is not None:
                await self.dataset_seed.capture_once(self.binding)
                # Seed publication does not authorize an S3-backed importer.
                # Keep new writes pending until Dataset cold restore is wired.
                self.coverage=None;self.gate.invalidate()
                return False
            if self.snapshot is not None:self.snapshot.kick(self.binding)
            if self.geo_gate is not None and (self.geo_gate._manifest is None
                    or self.geo_gate._manifest.database!=binding.database_generation):
                # establish() holds the spool lock while re-reading every GEO
                # command; a denial for unchanged inputs repeats identically, so
                # retry only after spool/binding change or GEO_RETRY_SECONDS.
                inputs=(binding.database_generation,binding.connection_generation,
                    self.config.spool._source_covered_sequence)
                denied=self._geo_denied
                if denied is None or denied[0]!=inputs or time.monotonic()-denied[1]>=GEO_RETRY_SECONDS:
                    try:
                        await self.geo_gate.establish(self.config.geo_manifest_operation_keys,binding=self.binding)
                        self._geo_denied=None
                    except asyncio.CancelledError:raise
                    except Exception as exc:
                        self.geo_gate.invalidate()
                        self._geo_denied=(inputs,time.monotonic())
                        _LOG.warning('target_geo_routing state=closed reason=%s',type(exc).__name__)
            if self.geo_installer is not None:self.geo_installer.kick(self.binding)
            if self.reads_current():
                # Mark the original demand complete only after the full
                # verified history/read gate is current, never at BGE heartbeat.
                self.config.queue.finish_startup(binding.run_id)
            _LOG.info('target_source_routing state=ready connection_generation=%s',binding.connection_generation)
            return True
        except asyncio.CancelledError:
            self.coverage=None;self.gate.invalidate();raise
        except Exception as exc:
            self.coverage=None;self.gate.invalidate()
            code=exc.args[0] if len(exc.args)==1 and type(exc.args[0]) is str else ''
            if not 1<=len(code)<=100 or any(c not in 'abcdefghijklmnopqrstuvwxyz0123456789_' for c in code):
                code='unavailable'
            _LOG.warning('target_source_routing state=closed reason=%s code=%s',type(exc).__name__,code)
            return False

    async def _maintain(self,*,initial=False):
        if initial:await self.refresh()
        while True:
            await asyncio.sleep(self.config.refresh_seconds)
            await self.refresh()

    @asynccontextmanager
    async def lifespan(self,server):
        if self.running or self.closed:raise RuntimeError('one target service lifespan required')
        self.running=True
        try:
            if self.restorer is None:await self.refresh()
            self._maintenance=asyncio.create_task(self._maintain(initial=self.restorer is not None),name='rkb-source-history-maintenance')
            yield self
        finally:
            self.running=False;self.coverage=None;self.gate.invalidate()
            if self.geo_gate is not None:self.geo_gate.invalidate()
            self.case_gate.invalidate()
            if self._maintenance is not None:
                self._maintenance.cancel()
                try:await self._maintenance
                except asyncio.CancelledError:pass
            await self.schema_migrations.aclose()
            if self.geo_installer is not None:await self.geo_installer.aclose()
            if self.snapshot is not None:await self.snapshot.aclose()
            if self.restorer is not None:await self.restorer.aclose()
            if self.editorial is not None:await self.editorial.aclose()
            try:await self.source_dispatch.aclose()
            finally:
                self.closed=True
                self.admission.close()
                self.config.spool.close()
                if self.config.writer_owner is not None:self.config.writer_owner.close()
