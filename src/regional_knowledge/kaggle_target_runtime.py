"""Explicit controller launch contract and resident target lifecycle.

No provider calls, environment credentials, installation or model fallback.
Independent Stage0 pins remain required; generating source cannot establish them.
"""
from __future__ import annotations

from dataclasses import dataclass
import asyncio
import hashlib
import json
from pathlib import Path
import re

from .kaggle_owned_pg import OwnedDatabase, OwnedPostgres, canonical
from .kaggle_bootstrap import BootstrapUnavailable, PreparedRuntime, prepare_resident_runtime
from .kaggle_database_probe import NativeDatabaseProbe


def sha(data):
    return hashlib.sha256(data).hexdigest()


@dataclass(frozen=True)
class TargetLaunchSpec:
    database: OwnedDatabase
    artifact_root: str
    artifacts: tuple[dict, ...]
    dependency_packages: tuple[str, ...]
    dependency_sha256: str
    schema_sha256: str
    stage0_receipt: str
    initialized_receipt: dict | None = None
    min_data_seq: int = 0
    command_handlers: tuple[dict, ...] = ()
    stage0: object = None
    native_bundle: object = None
    bootstrap_only: bool = False
    bootstrap_seconds: int = 900

    def __post_init__(self):
        if type(self.database) is not OwnedDatabase:
            raise TypeError('typed owned database required')
        root = Path(self.artifact_root)
        if (not root.is_absolute() or '..' in root.parts or root != Path('/kaggle/working')
                or not 1 <= len(self.artifacts) <= 96):
            raise ValueError('bounded prepared Kaggle inventory required')
        from .kaggle_bootstrap import PreparedArtifact, REQUIRED_DEPENDENCIES
        from .kaggle_readiness import ArtifactRequirement
        entries = tuple(dict(e) for e in self.artifacts)
        for e in entries:
            PreparedArtifact(ArtifactRequirement(e['name'],e['size'],e['sha256']),e['role'])
        if len({e['name'] for e in entries}) != len(entries):
            raise ValueError('unique prepared inventory required')
        if (not REQUIRED_DEPENDENCIES <= set(self.dependency_packages)
                or len(set(self.dependency_packages)) != len(self.dependency_packages)
                or len(self.dependency_packages)>64
                or not isinstance(self.stage0_receipt,str) or not 1<=len(self.stage0_receipt.encode())<=256
                or type(self.bootstrap_only) is not bool or type(self.bootstrap_seconds) is not int
                or not 30<=self.bootstrap_seconds<=1800
                or (self.bootstrap_only and (self.database.mode!='NEW_EMPTY_GENERATION'
                    or self.dependency_sha256 is not None or self.schema_sha256 is not None))
                or (not self.bootstrap_only and any(type(v) is not str or not re.fullmatch('[0-9a-f]{64}',v)
                    for v in (self.dependency_sha256,self.schema_sha256)))
                or type(self.min_data_seq) is not int or not 0<=self.min_data_seq<2**63):
            raise ValueError('independent Stage0 dependency/schema pins required')
        if self.database.mode == 'VERIFIED_INITIALIZED':
            receipt = self.initialized_receipt
            if (not isinstance(receipt,dict) or receipt.get('database') != self.database.wire()
                    or receipt.get('schema_sha256') != self.schema_sha256
                    or receipt.get('stage0_receipt') != self.stage0_receipt):
                raise ValueError('exact independently verified initialized binding required')
        elif self.initialized_receipt is not None or self.min_data_seq != 0:
            raise ValueError('fresh generation has no initialized receipt or accepted cursor')
        # Freeze a detached canonical inventory; callers cannot mutate expected pins.
        object.__setattr__(self,'artifacts',tuple(json.loads(canonical(entries))))
        if self.command_handlers:
            from .kaggle_source_commands import fixed_registry
            fixed_registry(self.database,self.command_handlers)
        object.__setattr__(self,'command_handlers',tuple(json.loads(canonical(self.command_handlers))))
        if self.native_bundle is not None:
            from .kaggle_native_bundle import NativeBundleSpec
            if type(self.native_bundle) is not NativeBundleSpec:
                raise TypeError('typed pinned native bundle required')
            manifest=next((e for e in entries if e['name']==self.native_bundle.manifest_name),None)
            if manifest is None or manifest['role']!='code' or manifest['sha256']!=self.native_bundle.manifest_sha256:
                raise ValueError('native manifest must match exact top-level code inventory')
            if any(e['name'].startswith('rkb-native-') for e in entries):
                raise ValueError('native extraction prefix cannot be a Stage0 download')
            if self.database.mode=='NEW_EMPTY_GENERATION' and any(
                    not Path(path).is_relative_to(self.native_bundle.prefix(root)) for path in self.database.native_paths.values()):
                raise ValueError('native PostgreSQL paths must match content-addressed bundle prefix')
        if self.stage0 is not None:
            from .kaggle_stage0 import Stage0Spec
            if type(self.stage0) is not Stage0Spec:
                raise TypeError('typed pinned Stage0 preparation required')
            declared={e['name']:e for e in entries}
            if any(name.startswith('rkb-target-') for name in declared):
                raise ValueError('generated target source cannot be a Stage0 download')
            downloads={item.name:item for item in self.stage0.downloads}
            if set(downloads)!=set(declared):
                raise ValueError('Stage0 must pin every external prepared artifact exactly')
            if any((item.size,item.sha256)!=(declared[name]['size'],declared[name]['sha256'])
                   for name,item in downloads.items()):
                raise ValueError('Stage0 download differs from prepared artifact pin')
            if set(self.stage0.packages)!=set(self.dependency_packages):
                raise ValueError('Stage0 packages must match exact dependency hash inventory')
            protected={'torch':'2.11.0+cpu','transformers':'5.16.1','tokenizers':'0.23.1','huggingface-hub':'1.29.0'}
            if any(self.stage0.packages.get(name)!=version for name,version in protected.items()):
                raise ValueError('protected BGE runtime versions cannot change through Stage0')
            if any(declared[name]['role']!='code' for name in self.stage0.wheels):
                raise ValueError('Stage0 wheels require separate pinned code artifacts')
            if self.database.mode=='NEW_EMPTY_GENERATION' and self.native_bundle is None:
                native={str(root/e['name']) for e in entries if e['role']=='code'}
                if not set(self.database.native_paths.values())<=native:
                    raise ValueError('Stage0 exact native PostgreSQL path inventory required')
                for name in ('initdb','postgres','pg_config'):
                    relative=str(Path(self.database.native_paths[name]).relative_to(root))
                    if not downloads[relative].executable:
                        raise ValueError('Stage0 native PostgreSQL binaries must be executable')

    def wire(self):
        return dict(database=self.database.wire(),artifact_root=self.artifact_root,
            artifacts=list(self.artifacts),dependency_packages=list(self.dependency_packages),
            dependency_sha256=self.dependency_sha256,schema_sha256=self.schema_sha256,
            stage0_receipt=self.stage0_receipt,initialized_receipt=self.initialized_receipt,
            min_data_seq=self.min_data_seq,command_handlers=list(self.command_handlers),
            stage0=self.stage0.wire() if self.stage0 is not None else None,
            native_bundle=self.native_bundle.wire() if self.native_bundle is not None else None,
            bootstrap_only=self.bootstrap_only,bootstrap_seconds=self.bootstrap_seconds)


class TargetRuntime:
    """One owned pool/PG/model preparation across WSS reconnects, on one loop."""
    def __init__(self, config, *, executed_source, pool_factory=None, owner_factory=OwnedPostgres,
                 probe=None, prepare=prepare_resident_runtime, restored_database=None, recovery_worker=None):
        from psycopg_pool import AsyncConnectionPool
        self.config = config
        target = config['target_runtime']
        self.database = OwnedDatabase(**target['database'])
        self.target = target
        self.executed_source = executed_source
        self.pool_factory = pool_factory or AsyncConnectionPool
        self.owner = owner_factory(self.database)
        self.probe = probe or NativeDatabaseProbe()
        self.prepare_resident = prepare
        self.pool = None
        self.writer_pool = None
        self.prepared = None
        self.identity = None
        self.closed = False
        if restored_database is not None:
            from .kaggle_restored_database import VerifiedInitializedDatabase
            if type(restored_database) is not VerifiedInitializedDatabase or self.database.mode!='VERIFIED_INITIALIZED':
                raise BootstrapUnavailable('target_restore_capability_required')
        if recovery_worker is not None:
            from .kaggle_recovery_phases import WorkerRecoveryPhases
            if (type(recovery_worker) is not WorkerRecoveryPhases or restored_database is not None
                    or self.database.mode!='VERIFIED_INITIALIZED'):
                raise BootstrapUnavailable('target_restore_worker_required')
        self.recovery_worker=recovery_worker
        self._recovery_available=asyncio.Event()
        if recovery_worker is not None:self._recovery_available.set()
        self.restored_database=restored_database

    async def configure_recovery(self,encoder,*,run_id,launch_nonce,runtime_incarnation,
                                 native_bundle,tools,native_uid,native_gid,current,
                                 workspace_private_current,workspace,upload=None,download=None,
                                 ticket_channel=None,transfer=None,limits=None):
        """Trusted notebook composition after actual Stage0 and before prepare.

        The verified native bundle belongs to the same verified artifact root;
        callback objects retain attachment/transfer/private-output authority.
        No launch receipt dictionary supplies those permissions.
        """
        if (self.closed or self.identity is not None or self.recovery_worker is not None
                or self.restored_database is not None or self.database.mode!='VERIFIED_INITIALIZED'
                or (run_id,launch_nonce)!=(self.config['run_id'],self.config['launch_nonce'])
                or native_bundle.root!=encoder.artifacts.root):
            raise BootstrapUnavailable('target_recovery_configuration_changed')
        from .kaggle_recovery_phases import build_recovery_worker
        worker=await build_recovery_worker(native_bundle=native_bundle,database=self.database,
            identity=(run_id,launch_nonce,runtime_incarnation),tools=tools,native_uid=native_uid,
            native_gid=native_gid,current=current,workspace_private_current=workspace_private_current,
            upload=upload,download=download,workspace=workspace,limits=limits,
            ticket_channel=ticket_channel,transfer=transfer)
        self.recovery_worker=worker;self._recovery_available.set()
        return worker

    async def recovery(self,frame):
        """Register this handler before _load/prepare; same channel stays live."""
        if self.closed:raise BootstrapUnavailable('target_runtime_closed')
        async with asyncio.timeout(900):await self._recovery_available.wait()
        if self.closed:raise BootstrapUnavailable('target_runtime_closed')
        return await self.recovery_worker(frame)

    async def _owned_probe(self, *, acquire):
        # The native catalog hash and actual current writer binding have
        # independent checks. A receipt cannot substitute for a database read.
        catalog, fields = await self.probe(acquire=acquire)
        self._bootstrap_catalog=catalog
        async with asyncio.timeout(3):
            async with acquire() as connection:
                async with connection.transaction():
                    await connection.execute("SET LOCAL statement_timeout='2s'")
                    cursor = await connection.execute('''SELECT database_generation,writer_generation
                        FROM rkb_v4.recovery_state WHERE singleton LIMIT 2''')
                    row = await cursor.fetchone()
                    if (row != (self.database.database_generation,self.database.writer_generation)
                            or await cursor.fetchone() is not None):
                        raise BootstrapUnavailable('target_database_writer_binding_mismatch')
        return catalog,fields

    async def prepare(self, encoder, *, run_id, launch_nonce, runtime_incarnation):
        identity = (run_id,launch_nonce,runtime_incarnation)
        if self.closed or identity[:2] != (self.config['run_id'],self.config['launch_nonce']):
            raise BootstrapUnavailable('target_runtime_identity_mismatch')
        if self.prepared is not None:
            if identity != self.identity:
                raise BootstrapUnavailable('target_runtime_identity_mismatch')
            return self.prepared
        if self.identity is not None:
            raise BootstrapUnavailable('target_runtime_preparation_already_started')
        self.identity = identity
        # BgeWssTransport invokes prepare only after its authenticated ATTACH and
        # model load. Reconnect never re-enters this factory or replaces the pool.
        reader_kwargs=self.database.connection()
        if self.recovery_worker is not None:
            self.restored_database=await self.recovery_worker.wait_initialized(self.database,*identity)
        if self.restored_database is not None:
            reader_kwargs=await self.restored_database.verify_for(self.database,run_id,launch_nonce,runtime_incarnation)
            if (not isinstance(reader_kwargs,dict) or any(reader_kwargs.get(key)!=self.database.connection()[key]
                    for key in ('host','port','dbname','user'))):
                raise BootstrapUnavailable('target_restore_connection_binding_mismatch')
        else:
            native_bundle=None
            if self.target.get('native_bundle') is not None:
                from .kaggle_native_bundle import NativeBundleSpec,prepare_native_bundle
                bundle=NativeBundleSpec.from_wire(self.target['native_bundle'])
                inventory=tuple(dict(name=e.requirement.name,size=e.requirement.size,
                    sha256=e.requirement.sha256,role=e.role) for e in encoder.artifacts.entries)
                native_bundle=await asyncio.to_thread(prepare_native_bundle,encoder.artifacts.root,
                    bundle.manifest_name,bundle.manifest_sha256,inventory=inventory)
            options=dict(intent=self.target.get('initialization_intent'),artifacts=encoder.artifacts,sql_root=self.target['sql_root'])
            if native_bundle is not None:options['native_bundle']=native_bundle
            await self.owner.start_empty(**options)
        self.pool = self.pool_factory(kwargs=reader_kwargs,min_size=1,max_size=4,
            open=False,timeout=2,max_waiting=4)
        await self.pool.open(wait=True,timeout=10)
        prepared = await self.prepare_resident(encoder,acquire=self.pool.connection,
            probe_database=self._owned_probe,launch_config=self.config,executed_source=self.executed_source,
            source_binding_verified=True,dependency_packages=self.target['dependency_packages'])
        readiness = prepared.readiness
        if (readiness.database_generation != self.database.database_generation
                or any(getattr(readiness,key) != getattr(self.database,key) for key in
                       ('schema_revision','function_revision','executor_revision'))):
            raise BootstrapUnavailable('target_database_binding_mismatch')
        # Same per-invocation context is used by telemetry's native adapter.
        handler = prepared.query_handler
        async def invoke(frame, *, encode):
            from .query_telemetry import ReadRuntime, bind_read_runtime
            with bind_read_runtime(ReadRuntime(run_id,runtime_incarnation)):
                return await handler(frame,encode=encode)
        source_handler=None
        if self.target.get('command_handlers'):
            from .kaggle_source_commands import fixed_registry,WorkerSourceExecutor
            self.writer_pool=self.pool_factory(kwargs={**self.database.connection(),'user':'rkb_writer'},
                min_size=1,max_size=1,open=False,timeout=2,max_waiting=1)
            await self.writer_pool.open(wait=True,timeout=10)
            source_handler=WorkerSourceExecutor(acquire=self.writer_pool.connection,
                registry=fixed_registry(self.database,self.target['command_handlers']),database=self.database,
                run_id=run_id,launch_nonce=launch_nonce,runtime_incarnation=runtime_incarnation)
        observation=None
        if self.target.get('bootstrap_only'):
            import base64
            from .kaggle_bootstrap import observe_dependencies
            dependencies=await asyncio.to_thread(observe_dependencies,self.target['dependency_packages'])
            observation=dict(runtime_readiness=readiness.to_wire(),dependencies=dependencies,
                catalog_b64=base64.b64encode(self._bootstrap_catalog).decode())
        self.prepared = PreparedRuntime(invoke,readiness,source_handler,observation)
        return self.prepared

    async def aclose(self):
        if self.closed:
            return
        self.closed = True
        try:
            try:
                if self.writer_pool is not None:
                    await self.writer_pool.close(timeout=10)
            finally:
                if self.pool is not None:
                    await self.pool.close(timeout=10)
        finally:
            try:
                if self.restored_database is not None:
                    await self.restored_database.aclose()
            finally:
                await self.owner.aclose()
