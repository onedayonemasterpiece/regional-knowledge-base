"""One optional first-base capture on the existing source maintenance owner.

Capture seals an immutable candidate base. The verified current pointer is
published only after the retained physical restore/readiness/shutdown drill.
"""
from dataclasses import dataclass
from contextlib import asynccontextmanager
import asyncio
import json
import logging
import secrets
from collections.abc import Mapping
from types import MappingProxyType

from .kaggle_pg_snapshot_native import SnapshotError,SnapshotLimits,validate_versions,encode,sha
from .native_snapshot_transfer import origin

_LOG=logging.getLogger(__name__)


@dataclass(frozen=True)
class SourceSnapshotConfig:
    operation_key: str
    versions: object
    allowed_origins: tuple[str,...]
    max_archive_bytes: int = 8*1024**3
    max_unpacked_bytes: int = 16*1024**3
    seconds: int = 900

    def __post_init__(self):
        if not isinstance(self.versions,Mapping):raise ValueError('exact snapshot versions required')
        validate_versions(dict(self.versions))
        if (type(self.operation_key) is not str or not 1<=len(self.operation_key.encode())<=256
                or type(self.allowed_origins) is not tuple or not 1<=len(self.allowed_origins)<=4
                or len(set(self.allowed_origins))!=len(self.allowed_origins)
                or any(origin(value)!=value for value in self.allowed_origins)
                or type(self.max_archive_bytes) is not int or not 1<=self.max_archive_bytes<=8*1024**3
                or type(self.max_unpacked_bytes) is not int or not 1<=self.max_unpacked_bytes<=16*1024**3
                or type(self.seconds) is not int or not 1<=self.seconds<=900):
            raise ValueError('explicit bounded first snapshot owner required')
        object.__setattr__(self,'versions',MappingProxyType(dict(self.versions)))

    def wire(self):
        return dict(operation_key=self.operation_key,versions=dict(self.versions),allowed_origins=list(self.allowed_origins),
            max_archive_bytes=self.max_archive_bytes,max_unpacked_bytes=self.max_unpacked_bytes,seconds=self.seconds)

    @classmethod
    def from_wire(cls,value):
        if type(value) is not dict or set(value)!=set(cls.__dataclass_fields__) or type(value['allowed_origins']) is not list:
            raise ValueError('exact source snapshot configuration fields required')
        return cls(**dict(value,allowed_origins=tuple(value['allowed_origins'])))


@dataclass(frozen=True,eq=False)
class DrilledBaseProof:
    _owner: object
    manifest: object
    native_reference: str
    native_sha256: str
    identity: tuple
    binding: object

    async def verify_current(self):return await self._owner._verify_drilled(self)


class SourceSnapshotCoordinator:
    def __init__(self,startup,config):
        if type(config) is not SourceSnapshotConfig:raise TypeError('typed first snapshot owner required')
        self.startup,self.config=startup,config
        self.queue=startup.config.queue;self.writer=startup.config.writer_owner
        self.journal=startup.config.spool.journal;self.database=startup.config.history_adapter.database
        if (self.writer is None or self.database.mode!='NEW_EMPTY_GENERATION'
                or config.versions.get('schema_version')!=self.database.schema_version):
            raise ValueError('snapshot owner must match actual source service closure')
        self.task=None;self.reference=None;self.closed=False;self.verified=False;self._proofs=set();self.dispatch=None;self.physical=None;self._owns_dispatch=False
        with self.queue.connect() as db:
            db.execute('''CREATE TABLE IF NOT EXISTS rkb_source_base_capture(
                id INTEGER PRIMARY KEY CHECK(id=1),plan_json TEXT NOT NULL,identity_json TEXT NOT NULL,
                state TEXT NOT NULL CHECK(state IN ('pending','captured','verified')),manifest_json TEXT,pointer_sha256 TEXT)''')

    def current(self,binding):return not self.closed and self.startup.binding_current(binding)

    def _storage_current(self):
        try:
            store=self.journal.store
            return (store.writer_guard is self.writer and self.writer._storage(store)==self.writer._store_identity
                and origin(store.client.meta.endpoint_url) in self.config.allowed_origins)
        except Exception:return False

    @asynccontextmanager
    async def _maintenance(self,binding):
        if not self.current(binding):raise SnapshotError('snapshot_capture_binding_changed')
        # capture_once retains the actual spool exclusion across every phase;
        # the delegated locked base helper must not acquire it a second time.
        yield
        if not self.current(binding):raise SnapshotError('snapshot_capture_binding_changed')

    async def capture_once(self,binding,*,reconcile=False):
        from .kaggle_recovery_dispatch import RecoveryPhaseDispatch,WssSnapshotPhases
        from .snapshot_tickets import SnapshotTicketIssuer
        from .pg_snapshot_recovery import PhysicalSnapshotRecovery,S3SnapshotTransport
        if not self.current(binding) or not self._storage_current():raise SnapshotError('snapshot_capture_authority_unavailable')
        with self.queue.connect(write=False) as db:
            run=db.execute('SELECT launch_nonce FROM runs WHERE id=?',(binding.run_id,)).fetchone()
            runtime=self.queue._runtime_row(db,binding.run_id)
            expected=json.loads(runtime['expected_json'])
        if self.config.versions['model_sha256']!=expected['hashes']['model']:
            raise SnapshotError('snapshot_capture_model_pin_differs')
        identity=(binding.run_id,run['launch_nonce'],binding.runtime_incarnation)
        plan=encode(self.config.wire()).decode();identity_json=encode(identity).decode()
        with self.queue.connect() as db:
            prior=db.execute('SELECT * FROM rkb_source_base_capture WHERE id=1').fetchone()
            if prior is not None:
                if prior['plan_json']!=plan or prior['identity_json']!=identity_json:
                    raise SnapshotError('snapshot_capture_existing_owner_changed')
                if prior['state'] in ('captured','verified'):
                    from .recovery_journal import ObjectRef
                    reference=ObjectRef(**json.loads(prior['manifest_json']))
                elif reconcile:
                    # Only an already durable exact capture result may finish
                    # sealing after an UNKNOWN response. Never start a new copy.
                    args=dict(versions=dict(self.config.versions),expected_sequence=0)
                    phase_key=sha(encode(dict(operation=self.config.operation_key,phase='capture',arguments=args)))
                    job=db.execute("SELECT state,identity FROM jobs WHERE actor=? AND idempotency=?",
                        ('maintenance:'+binding.run_id,'recovery:'+phase_key)).fetchone()
                    if job is None or job['state']!='done' or json.loads(job['identity']).get('arguments')!=args:
                        raise SnapshotError('snapshot_capture_unknown_reconcile_same_operation')
                    reference=None
                else:raise SnapshotError('snapshot_capture_unknown_reconcile_same_operation')
            else:
                db.execute("INSERT INTO rkb_source_base_capture VALUES(1,?,?,'pending',NULL,NULL)",(plan,identity_json))
                reference=None
        bridge=getattr(self.queue,'_recovery_phases',None)
        tickets=SnapshotTicketIssuer(self.journal.store,current=self.current,
            allowed_origins=frozenset(self.config.allowed_origins),max_single_bytes=self.config.max_archive_bytes,
            ttl_seconds=min(600,self.config.seconds))
        if bridge is None:
            bridge=RecoveryPhaseDispatch(self.queue,database=self.database,identity=identity,
                binding_current=self.current,timeout_seconds=self.config.seconds,snapshot_tickets=tickets)
            self._owns_dispatch=True
        elif (type(bridge) is not RecoveryPhaseDispatch or bridge.database!=self.database
                or bridge.identity!=identity or not bridge.current(binding)):
            raise SnapshotError('snapshot_capture_maintenance_owner_changed')
        elif bridge.snapshot_tickets is None:
            bridge.snapshot_tickets=tickets
        elif bridge.snapshot_tickets.store is not self.journal.store:
            raise SnapshotError('snapshot_capture_storage_owner_changed')
        bridge.timeout=max(bridge.timeout,self.config.seconds)
        physical=PhysicalSnapshotRecovery(self.journal,S3SnapshotTransport(self.journal.store),
            WssSnapshotPhases(bridge,operation_key=self.config.operation_key),registry=self.startup.config.spool.registry,
            history_verifier=self.startup.history,maintenance_guard=self._maintenance,current=self.current,
            private_storage_current=self._storage_current,
            limits=SnapshotLimits(max_archive_bytes=self.config.max_archive_bytes,max_unpacked_bytes=self.config.max_unpacked_bytes,timeout_seconds=self.config.seconds))
        self.dispatch,self.physical=bridge,physical
        if prior is not None and prior['state']=='verified':
            _,blob,_=await physical._manifest(reference)
            await physical.transport.verify(blob)
            pointer=await self.journal._read(self.journal.prefix+'/snapshots/current',self.physical.limits.max_control_bytes)
            if pointer is None or sha(pointer.data)!=prior['pointer_sha256']:
                raise SnapshotError('snapshot_verified_pointer_changed')
            self.reference=reference;self.verified=True
            return reference
        async with self._maintenance(binding):
            async with self.startup.config.spool._lock:
                coverage=self.startup.coverage
                if (coverage is None or not coverage.current() or coverage.accepted_sequence!=0
                        or coverage.applied_sequence!=0 or coverage.pending or await self.journal.head() is not None):
                    raise SnapshotError('first_snapshot_requires_empty_drained_prefix')
                if reference is None:
                    reference=await physical._create_base_locked(binding,versions=dict(self.config.versions),
                        writer_generation=self.writer.writer_generation)
                    with self.queue.connect() as db:
                        db.execute("UPDATE rkb_source_base_capture SET state='captured',manifest_json=? WHERE id=1 AND plan_json=? AND identity_json=?",
                            (encode(reference.wire()).decode(),plan,identity_json))
                self.reference=reference
                proof=await self._drill(reference,binding,identity)
                pointer=await self._promote_drilled(proof)
                with self.queue.connect() as db:
                    db.execute("UPDATE rkb_source_base_capture SET state='verified',pointer_sha256=? WHERE id=1 AND plan_json=? AND identity_json=?",
                        (sha(pointer.data),plan,identity_json))
                self.verified=True
        _LOG.info('snapshot_base state=restore_drill_verified manifest_sha256=%s cursor=0',reference.sha256)
        return reference

    async def _phase(self,binding,phase,args):
        key=sha(encode(dict(operation=self.config.operation_key,phase=phase,arguments=args)))
        return await self.dispatch.request(binding,phase,args,operation_key=key)

    async def _drill(self,reference,binding,identity):
        import base64
        from .recovery_journal import ObjectRef
        from .kaggle_snapshot_drill import drill_database
        manifest,blob,_=await self.physical._manifest(reference)
        if manifest['applied_sequence']!=0 or manifest['journal_head'] is not None:
            raise SnapshotError('first_snapshot_empty_base_required')
        await self.physical.transport.verify(blob)
        control=manifest['source_control']
        raw=await self.journal._verified(ObjectRef(control['key'],control['sha256'],control['length'],0),self.physical.limits.max_control_bytes)
        database=drill_database(self.database,identity)
        prepared=await self._phase(binding,'drill_prepare',dict(manifest_b64=base64.b64encode(encode(manifest)).decode(),
            manifest_ref=reference.wire(),blob=manifest['blob'],control_b64=base64.b64encode(raw).decode(),
            expected_versions=dict(self.config.versions),database=database.wire(),identity=list(identity),
            restore_operation=sha(encode([self.config.operation_key,'empty-drill']))))
        finished=await self._phase(binding,'drill_finish',dict(handle=prepared['handle'],
            control_b64=base64.b64encode(raw).decode(),schema_version=self.config.versions['schema_version']))
        closed=await self._phase(binding,'drill_close',dict(reference=finished['reference']))
        self._check_drill(closed,reference,identity)
        proof=DrilledBaseProof(self,reference,closed['reference'],sha(encode({k:v for k,v in closed.items() if k!='reference'})),identity,binding)
        self._proofs.add(proof)
        return proof

    def _check_drill(self,value,reference,identity):
        from .kaggle_snapshot_drill import drill_database
        if (type(value) is not dict or value.get('codec')!='rkb.empty-base-drill.v1'
                or value.get('database')!=self.database.wire() or value.get('drill_database')!=drill_database(self.database,identity).wire()
                or value.get('identity')!=list(identity) or value.get('manifest_sha256')!=reference.sha256
                or value.get('applied_sequence')!=0 or value.get('schema_version')!=self.config.versions['schema_version']
                or value.get('versions')!=dict(self.config.versions)
                or any(type(value.get(k)) is not int or value[k]<1 for k in ('disk_free_before','owned_peak_bytes_bound'))):
            raise SnapshotError('first_snapshot_native_drill_proof_differs')

    async def _verify_drilled(self,proof):
        if type(proof) is not DrilledBaseProof or proof._owner is not self or proof not in self._proofs or not self.current(proof.binding):
            raise SnapshotError('first_snapshot_retained_drill_proof_required')
        value=await self.dispatch.request(proof.binding,'drill_verify',{'reference':proof.native_reference},
            operation_key=sha(encode([self.config.operation_key,'drill_verify',secrets.token_hex(24)])))
        self._check_drill(value,proof.manifest,proof.identity)
        if sha(encode({k:v for k,v in value.items() if k!='reference'}))!=proof.native_sha256:
            raise SnapshotError('first_snapshot_retained_native_proof_changed')
        return value

    async def _promote_drilled(self,proof):
        if not self.startup.config.spool._lock.locked():raise SnapshotError('first_snapshot_intake_barrier_required')
        await proof.verify_current()
        if await self.journal.head() is not None:raise SnapshotError('first_snapshot_prefix_changed')
        manifest,blob,_=await self.physical._manifest(proof.manifest)
        await self.physical.transport.verify(blob)
        data=encode(dict(codec='rkb.verified-snapshot-pointer.v1',stream=self.journal.stream,manifest=proof.manifest.wire(),
            verified_cursor=0,verified_head=None,writer_generation=self.writer.writer_generation,
            control_sha256=manifest['source_control']['sha256']))
        key=self.journal.prefix+'/snapshots/current'
        existing=await self.journal._read(key,self.physical.limits.max_control_bytes)
        if existing is not None and existing.data!=data:raise SnapshotError('first_snapshot_existing_pointer_differs')
        if existing is None:
            await self.journal._fenced_exact(key,data,expected_token=None,writer_generation=self.writer.writer_generation,
                limit=self.physical.limits.max_control_bytes)
        pointer=await self.journal._read(key,self.physical.limits.max_control_bytes)
        if pointer is None or pointer.data!=data or not self.current(proof.binding):raise SnapshotError('first_snapshot_pointer_unproved')
        # A lost provider ACK can leave the durable writer marker UNKNOWN even
        # after exact byte readback. Resolve it before enabling initial writes.
        await self.writer.reconcile(self.journal.store)
        return pointer

    async def reconcile(self):
        from .kaggle_public_backend import current_binding
        return await self.capture_once(current_binding(self.queue),reconcile=True)

    def kick(self,binding):
        if self.task is not None or self.closed:return
        async def once():
            try:await self.capture_once(binding)
            except asyncio.CancelledError:raise
            except Exception as exc:_LOG.warning('snapshot_base state=pending reason=%s',type(exc).__name__)
        self.task=asyncio.create_task(once(),name='rkb-first-snapshot-capture')

    async def aclose(self):
        self.closed=True
        if self.task is not None:
            self.task.cancel()
            await asyncio.gather(self.task,return_exceptions=True)
        if self._owns_dispatch and self.dispatch is not None:await self.dispatch.aclose()
