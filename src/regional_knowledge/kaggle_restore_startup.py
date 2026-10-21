"""Dev pre-READY snapshot restore using the existing source service owners."""
from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass
import json
import re

from .kaggle_pg_snapshot_native import SnapshotLimits,SnapshotError,validate_versions
from .recovery_journal import ObjectRef


@dataclass(frozen=True)
class SourceRestoreConfig:
    manifest: ObjectRef
    operation_key: str
    versions: object
    launch_config_sha256: str
    allowed_origins: tuple[str,...]
    max_archive_bytes: int = 8*1024**3
    seconds: int = 900

    def __post_init__(self):
        from .native_snapshot_transfer import origin
        from types import MappingProxyType
        from collections.abc import Mapping
        if not isinstance(self.versions,Mapping):raise ValueError('exact native versions required')
        validate_versions(dict(self.versions))
        if (type(self.manifest) is not ObjectRef or type(self.manifest.length) is not int or not 1<=self.manifest.length<=1024**2
                or type(self.manifest.last_sequence) is not int or self.manifest.last_sequence<0
                or type(self.manifest.sha256) is not str or re.fullmatch('[0-9a-f]{64}',self.manifest.sha256) is None
                or type(self.operation_key) is not str or not 1<=len(self.operation_key.encode())<=256
                or type(self.launch_config_sha256) is not str or re.fullmatch('[0-9a-f]{64}',self.launch_config_sha256) is None
                or type(self.allowed_origins) is not tuple or not 1<=len(self.allowed_origins)<=4
                or len(set(self.allowed_origins))!=len(self.allowed_origins)
                or any(origin(value)!=value for value in self.allowed_origins)
                or type(self.max_archive_bytes) is not int or not 1<=self.max_archive_bytes<=8*1024**3
                or type(self.seconds) is not int or not 1<=self.seconds<=900):
            raise ValueError('exact bounded existing snapshot restore configuration required')
        object.__setattr__(self,'versions',MappingProxyType(dict(self.versions)))

    def wire(self):
        return dict(manifest=self.manifest.wire(),operation_key=self.operation_key,versions=dict(self.versions),
            launch_config_sha256=self.launch_config_sha256,allowed_origins=list(self.allowed_origins),
            max_archive_bytes=self.max_archive_bytes,seconds=self.seconds)

    @classmethod
    def from_wire(cls,value):
        if type(value) is not dict or set(value)!=set(cls.__dataclass_fields__) or type(value['allowed_origins']) is not list:
            raise ValueError('exact source restore configuration fields required')
        return cls(**dict(value,manifest=ObjectRef(**value['manifest']),allowed_origins=tuple(value['allowed_origins'])))


@dataclass(frozen=True)
class PreReadyRecoveryBinding:
    run_id: str
    runtime_incarnation: str
    connection_generation: int
    database_generation: str
    writer_generation: str


class SourceRestoreCoordinator:
    def __init__(self,startup,config):
        from .queue_writer import QueueWriterAuthority
        if type(config) is not SourceRestoreConfig or type(startup.config.writer_owner) is not QueueWriterAuthority:
            raise TypeError('same persisted source writer and explicit restore config required')
        self.startup,self.config=startup,config
        self.writer=startup.config.writer_owner
        self.database=startup.config.history_adapter.database
        self.journal=startup.config.spool.journal
        self.queue=startup.config.queue
        from .geo_manifest_commands import GEO_REVISIONS,GEO_V2_REVISIONS
        pins=getattr(startup.config,'geo_dependency_upgrade',None)
        ordered_upgrade=(pins is not None and pins.origin_catalog_sha256==self.config.versions['catalog_sha256']
            and tuple(self.config.versions[key] for key in ('schema_revision','function_revision','executor_revision'))==GEO_REVISIONS.wire()
            and tuple(getattr(self.database,key) for key in ('schema_revision','function_revision','executor_revision'))==GEO_V2_REVISIONS.wire())
        if (self.database.mode!='VERIFIED_INITIALIZED' or self.database.database_generation!=self.writer.database_generation
                or not ordered_upgrade and (self.config.versions['catalog_sha256']!=startup.history.catalog_hash
                    or any(self.config.versions[key]!=getattr(self.database,key) for key in
                        ('schema_revision','function_revision','executor_revision')))
                or not self.config.manifest.key.startswith(self.journal.prefix+'/snapshots/manifests/')):
            raise ValueError('restore versions/database/manifest must match exact source service')
        self.binding=None;self.dispatch=None;self.physical=None;self.proof=None;self.complete=False

    def _observe(self):
        with self.queue.connect(write=False) as db:
            self.queue._shared_owner(db)
            run=db.execute('SELECT * FROM runs WHERE id=?',(self.writer.run_id,)).fetchone()
            current=db.execute('SELECT current_run FROM control WHERE id=1').fetchone()
            runtime=self.queue._runtime_row(db,self.writer.run_id)
            if (run is None or current[0]!=run['id'] or run['status'] not in ('starting','ready')
                    or not run['channel_active'] or run['attachment_conflict'] or self.queue._run_expired(run,self.queue.clock())
                    or not run['runtime_incarnation'] or run['provider_version'] is None or runtime is None):
                raise SnapshotError('restore_current_attachment_unavailable')
            expected=json.loads(runtime['expected_json'])
            if (expected.get('hashes',{}).get('launch_config')!=self.config.launch_config_sha256
                    or expected.get('database_generation')!=self.database.database_generation
                    or any(expected.get(key)!=getattr(self.database,key) for key in
                        ('schema_revision','function_revision','executor_revision'))):
                raise SnapshotError('restore_pinned_launch_differs')
            return PreReadyRecoveryBinding(run['id'],run['runtime_incarnation'],run['connection_generation'],
                self.database.database_generation,self.database.writer_generation),run['launch_nonce']

    def current(self,binding):
        from .kaggle_public_backend import TargetReadBinding
        if type(binding) is TargetReadBinding:return self.startup.binding_current(binding)
        if type(binding) is not PreReadyRecoveryBinding:return False
        try:
            actual,_=self._observe()
            if binding!=actual:return False
            return self.writer.recovery_current(binding) or self.writer.current(
                database_generation=binding.database_generation,writer_generation=binding.writer_generation)
        except Exception:return False

    def _storage_current(self):
        from .native_snapshot_transfer import origin
        store=self.journal.store
        try:
            return (not self.startup.closed and store.writer_guard is self.writer
                and self.writer._storage(store)==self.writer._store_identity
                and origin(store.client.meta.endpoint_url) in self.config.allowed_origins)
        except Exception:return False

    @asynccontextmanager
    async def _maintenance(self,binding):
        async with self.startup.config.spool._lock:
            if not self.current(binding):raise SnapshotError('restore_maintenance_binding_changed')
            yield
            if not self.current(binding):raise SnapshotError('restore_maintenance_binding_changed')

    def _compose(self,binding,nonce):
        from .kaggle_recovery_dispatch import RecoveryPhaseDispatch,WssSnapshotPhases
        from .snapshot_tickets import SnapshotTicketIssuer
        from .pg_snapshot_recovery import PhysicalSnapshotRecovery,S3SnapshotTransport
        identity=(binding.run_id,nonce,binding.runtime_incarnation)
        existing=getattr(self.queue,'_recovery_phases',None)
        if existing is not None:raise SnapshotError('restore_requires_same_retained_maintenance_owner')
        tickets=SnapshotTicketIssuer(self.journal.store,current=self.current,
            allowed_origins=frozenset(self.config.allowed_origins),max_single_bytes=self.config.max_archive_bytes,
            ttl_seconds=min(600,self.config.seconds))
        self.dispatch=RecoveryPhaseDispatch(self.queue,database=self.database,identity=identity,
            binding_current=self.current,timeout_seconds=self.config.seconds,snapshot_tickets=tickets)
        self.physical=PhysicalSnapshotRecovery(self.journal,S3SnapshotTransport(self.journal.store),
            WssSnapshotPhases(self.dispatch,operation_key=self.config.operation_key),registry=self.startup.config.spool.registry,
            history_verifier=self.startup.history,maintenance_guard=self._maintenance,current=self.current,
            private_storage_current=self._storage_current,
            limits=SnapshotLimits(max_archive_bytes=self.config.max_archive_bytes,timeout_seconds=self.config.seconds))

    async def refresh(self):
        if self.complete:return True
        binding,nonce=self._observe()
        await self.writer.begin_restore(self.journal,binding,plan=self.config)
        if self.writer.current(database_generation=binding.database_generation,writer_generation=binding.writer_generation):
            # Exact persisted activation already completed. Ordinary accepted
            # writes may have advanced since then: never replay the old base.
            # Startup still performs full native history verification next.
            self.binding=binding
            if self.dispatch is None and getattr(self.startup,'dataset_seed',None) is not None:
                # API-only restart retains native PG/model; rebuild only this
                # process's maintenance facade for subsequent checkpoint capture.
                self._compose(binding,nonce)
            self.complete=True
            return True
        if self.dispatch is None:self._compose(binding,nonce)
        if self.binding is not None and self.binding.runtime_incarnation!=binding.runtime_incarnation:
            raise SnapshotError('restore_retained_incarnation_changed')
        self.binding=binding
        # UNKNOWN canonical publication is read back, never blindly repeated.
        await self.writer.reconcile(self.journal.store)
        if self.proof is None:
            self.proof=await self.physical.recover(self.config.manifest,binding,database=self.database,
                expected_versions=dict(self.config.versions),expected_final_catalog_sha256=self.startup.history.catalog_hash,
                identity=(binding.run_id,nonce,binding.runtime_incarnation),operation_key=self.config.operation_key)
        elif self.proof._binding!=binding:
            await self.physical.rebind(self.proof,binding)
        # Native finish releases worker prepare/READY concurrently. Until its
        # current accepted cursor exists this fails closed and the same proof
        # is retained for the next maintenance iteration.
        await self.writer.activate_recovered(self.journal,self.proof,binding)
        self.complete=True
        return True

    async def aclose(self):
        if self.dispatch is not None:await self.dispatch.aclose()
