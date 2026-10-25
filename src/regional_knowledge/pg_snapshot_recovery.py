"""Dev controller snapshot/journal orchestration; never included in notebook code.

Native bytes move directly through scoped storage tickets. The injected native
adapter uses existing authenticated owned-worker WSS phases; it is not SQL RPC,
a second dispatcher, a provider client on the notebook or a Dev shadow corpus.
"""
from __future__ import annotations
from dataclasses import dataclass, asdict
import json
import logging
from .kaggle_pg_snapshot_native import SnapshotError, SnapshotLimits, BlobRef, encode, sha, validate_versions, _hash
from .recovery_journal import AcceptedSeal, ObjectRef, StoreUnavailable
from .control_spool import _command

_LOG=logging.getLogger(__name__)


class S3SnapshotTransport:
    """Stream through the already configured S3ConditionalRecoveryStore client.

    The existing prefix, bucket and writer guard remain the authority. No
    listing, credentials, ACL mutation or independent provider client exists.
    """
    def __init__(self,store):
        from .recovery_s3 import S3ConditionalRecoveryStore
        if not isinstance(store,S3ConditionalRecoveryStore):raise TypeError('existing restricted S3 store required')
        self.store=store

    async def verify(self,ref):
        """Independent bounded streaming readback without a Dev file copy."""
        from .recovery_s3 import _blocking
        s=self.store;s._key(ref.key)
        def read():
            body=None
            try:
                result=s.client.get_object(Bucket=s.bucket,Key=ref.key);body=result.get('Body')
                if result.get('ContentLength')!=ref.size or result.get('ETag')!=ref.native_version:
                    raise SnapshotError('snapshot_exact_version_unavailable')
                import hashlib
                h=hashlib.sha256();used=0
                while chunk:=body.read(min(1024*1024,ref.size-used+1)):
                    if not isinstance(chunk,bytes):raise SnapshotError('snapshot_remote_stream')
                    used+=len(chunk)
                    if used>ref.size:raise SnapshotError('snapshot_remote_length')
                    h.update(chunk)
                if used!=ref.size or h.hexdigest()!=ref.sha256:raise SnapshotError('snapshot_remote_hash')
            except SnapshotError:raise
            except Exception:raise StoreUnavailable('snapshot_readback_unavailable') from None
            finally:
                if body is not None:body.close()
        await _blocking(read)






@dataclass(frozen=True,eq=False)
class ControllerRecoveryProof:
    _factory: object
    native_reference: object
    database: object
    identity: tuple
    manifest: ObjectRef
    journal_head: object
    applied_sequence: int
    control: bytes
    _binding: object

    async def verify_for(self,database_spec,run_id,launch_nonce,runtime_incarnation):
        return await self._factory.verify_initialized(self,database_spec,(run_id,launch_nonce,runtime_incarnation))


class PhysicalSnapshotRecovery:
    """Existing authority + journal; native methods are fixed typed WSS phases."""
    def __init__(self,journal,transport,native,*,registry,history_verifier,
                 maintenance_guard,current,private_storage_current,limits=SnapshotLimits()):
        if not all(callable(c) for c in (maintenance_guard,current,private_storage_current)):
            raise TypeError('existing controller maintenance/storage authority required')
        self.journal,self.transport,self.native=journal,transport,native
        self.registry,self.history=registry,history_verifier
        self.maintenance_guard,self.current,self.private_storage_current=maintenance_guard,current,private_storage_current
        self.limits=limits;self._proofs=set()

    def _authority(self,binding):
        if not self.current(binding) or not self.private_storage_current():
            raise SnapshotError('snapshot_current_private_authority_required')

    async def create_base(self,binding,*,versions,writer_generation):
        validate_versions(versions);self._authority(binding)
        async with self.maintenance_guard(binding):
            async with self.history.spool._lock:
                return await self._create_base_locked(binding,versions=versions,writer_generation=writer_generation)

    async def _create_base_locked(self,binding,*,versions,writer_generation):
        """Internal owner path while its actual spool exclusion is retained."""
        validate_versions(versions);self._authority(binding)
        if not self.history.spool._lock.locked():raise SnapshotError('snapshot_intake_barrier_required')
        proof=self.history._coverage
        if (proof is None or not proof.current() or proof.pending
                or proof.accepted_sequence!=proof.applied_sequence
                or proof.accepted_sequence!=self.history.spool._source_covered_sequence
                or await self.journal.head()!=proof.head):
            raise SnapshotError('snapshot_requires_exact_drained_prefix')
        control=self._control(proof)
        descriptor=await self.native.capture(binding,versions=versions,expected_sequence=proof.applied_sequence)
        if (descriptor['database_generation']!=proof.database_generation
                or descriptor['writer_generation']!=writer_generation
                or descriptor['applied_sequence']!=proof.applied_sequence or descriptor['versions']!=versions):
            raise SnapshotError('snapshot_native_capture_binding')
        blob=BlobRef(**descriptor['blob'])
        if (not blob.key.startswith(self.journal.prefix+'/snapshots/blobs/')
                or blob.size>self.limits.max_archive_bytes):raise SnapshotError('snapshot_blob_scope')
        # Independent controller-side stream readback; bytes never land
        # in Dev corpus storage or the 512MiB canonical control spool.
        await self.transport.verify(blob)
        self._authority(binding)
        if await self.journal.head()!=proof.head:raise SnapshotError('snapshot_capture_prefix_changed')
        control_key=self.journal.prefix+'/source-base-control/'+sha(control)
        await self.journal._immutable(control_key,control,self.limits.max_control_bytes)
        manifest=dict(descriptor,codec='rkb.pg-physical-base.v1',stream=self.journal.stream,
            journal_head=proof.head.reference.wire() if proof.head else None,
            source_control={'key':control_key,'sha256':sha(control),'length':len(control)})
        data=encode(manifest);key=self.journal.prefix+'/snapshots/manifests/'+sha(data)
        await self.journal._immutable(key,data,self.limits.max_control_bytes)
        _LOG.info('snapshot_base_sealed manifest_hash=%s cursor=%s bytes=%s',sha(data),proof.applied_sequence,blob.size)
        return ObjectRef(key,sha(data),len(data),proof.applied_sequence)

    async def recover(self,manifest_ref,binding,*,database,expected_versions,identity,operation_key,
                      expected_schema_version=None):
        from .kaggle_pg_snapshot_native import compatible_versions
        self._authority(binding);validate_versions(expected_versions)
        expected_schema_version=database.schema_version if expected_schema_version is None else expected_schema_version
        if not isinstance(operation_key,str) or not 1<=len(operation_key.encode())<=256:
            raise SnapshotError('snapshot_stable_operation_key_required')
        if len(self._proofs)>=2:raise SnapshotError('snapshot_controller_proof_capacity')
        async with self.maintenance_guard(binding):
            manifest,blob,boundary=await self._manifest(manifest_ref)
            if (not compatible_versions(manifest['versions'],expected_versions) or manifest['database_generation']!=database.database_generation
                    or manifest['writer_generation']==database.writer_generation):
                raise SnapshotError('snapshot_restore_versions_or_writer_conflict')
            await self.transport.verify(blob)
            ref=manifest['source_control']
            control_bytes=await self.journal._verified(ObjectRef(ref['key'],ref['sha256'],ref['length'],
                manifest['applied_sequence']),self.limits.max_control_bytes)
            control=json.loads(control_bytes)
            if (encode(control)!=control_bytes or control.get('codec')!='rkb.source-base-control.v1'
                    or control.get('stream')!=self.journal.stream or control.get('sequence')!=manifest['applied_sequence']
                    or control.get('database_generation')!=database.database_generation
                    or control.get('journal_head')!=manifest['journal_head']):
                raise SnapshotError('snapshot_source_control_binding')
            intent=encode({'codec':'rkb.physical-recovery-intent.v1','stream':self.journal.stream,
                'operation_key':operation_key,'manifest':manifest_ref.wire(),'database':database.wire(),'identity':identity})
            await self.journal._fenced_exact(self.journal.prefix+'/snapshots/recovery-intents/'+sha(operation_key.encode()),
                intent,expected_token=None,writer_generation=database.writer_generation,limit=self.limits.max_control_bytes)
            handle=await self.native.prepare(binding,operation_key=operation_key,
                manifest_bytes=encode(manifest),manifest_ref=manifest_ref.wire(),
                blob=asdict(blob),control_bytes=control_bytes,expected_versions=expected_versions,
                database=database.wire(),identity=identity)
            head=await self.journal.head();tail=await self._tail(head,boundary,manifest['applied_sequence'])
            heads={row[0]:row for row in control['heads']};receipts=[];replayed=[]
            for command,seal in tail:
                self._authority(binding)
                handler=self.registry.resolve(command);dep=handler.dependency
                # Same closed route/dependency classification as native history;
                # independent Case/GEO commands keep their real resource IDs.
                classified,installed,_=self.history._classify(command.encode(),seal.manifest)
                if classified.encode()!=command.encode() or installed.fingerprint!=handler.fingerprint:
                    raise SnapshotError('snapshot_replay_scope_unsupported')
                if await self.journal.verify_accepted(seal,command)!=sha(command.encode()):
                    raise SnapshotError('snapshot_exact_acceptance_changed')
                # Adapter reuses the existing claimed accepted-command DISPATCH;
                # the worker receives this exact proof, never cloud credentials.
                wire=await self.native.apply(binding,handle,command_bytes=command.encode(),
                    accepted={'manifest':seal.manifest.wire(),'command_hashes':list(seal.command_hashes)},
                    handler_sha256=handler.fingerprint,original_pg_writer=manifest['writer_generation'])
                from .kaggle_source_commands import decode_receipt
                receipt,_=decode_receipt(wire)
                if ((receipt.database_generation,receipt.operation_key,receipt.sequence,receipt.command_sha256)
                        !=(database.database_generation,command.operation_key,command.sequence,sha(command.encode()))):
                    raise SnapshotError('snapshot_native_replay_receipt_changed')
                try:
                    remote=await self.journal.record_outcome(seal,command=command,database_generation=database.database_generation,
                        outcome=receipt.outcome,writer_generation=database.writer_generation)
                except Exception as exc:
                    from .dataset_recovery_store import DatasetRecoveryStore
                    from .recovery_journal import JournalUnknown
                    if (type(self.journal.store) is not DatasetRecoveryStore or type(exc) is not JournalUnknown
                            or exc.args!=('outcome_pending_dataset_checkpoint',)):raise
                    remote=None  # Replayed locally; still pending until the next Dataset.
                replayed.append((receipt,remote))
                receipts.append((receipt.sequence,receipt.command_sha256,receipt.outcome_sha256))

                if dep is not None and dep.scope_class=='public_source':
                    source=command.ids['source_id'];old=heads.get(source)
                    heads[source]=[source,command.sequence,sha(command.encode()),int(dep.restriction or old is not None and old[3]),0,0]
            if await self.journal.head()!=head:raise SnapshotError('snapshot_tail_changed_before_rollover')
            sequence=manifest['applied_sequence']+len(tail)
            for legacy_field in ('catalog_sha256','acl_sha256','closure_transitions'):
                control.pop(legacy_field,None)
            control.update(sequence=sequence,journal_head=head.reference.wire() if head else None,
                heads=[heads[k] for k in sorted(heads)],schema_version=expected_schema_version,
                registry_sha256=self.history.registry_hash,routes=sorted(self.history.routes))
            final_control=encode(control)
            if len(final_control)>self.limits.max_control_bytes:raise SnapshotError('snapshot_final_control_byte_budget')
            native_ref=await self.native.finish(binding,handle,expected_tail=tuple(receipts),
                final_head=head.reference.wire() if head else None,control_bytes=final_control,
                expected_schema_version=expected_schema_version)
            if await self.journal.head()!=head:raise SnapshotError('snapshot_tail_changed_after_readiness')
            self._authority(binding)
            proof=ControllerRecoveryProof(self,native_ref,database,identity,manifest_ref,head,sequence,final_control,binding)
            self._proofs.add(proof)
            await self.verify_initialized(proof,database,identity)
            # Recovered commands already have exact native and remote outcomes.
            # Reconcile retained local jobs before ordinary dispatch can select
            # their original writer identities on the new executor.
            spool=self.history.spool
            for receipt,remote in replayed:
                with spool.queue.connect(write=False) as db:
                    retained=db.execute('select 1 from rkb_control_operations where operation_key=?',
                                        (receipt.operation_key,)).fetchone()
                if retained is None:continue  # Compact history need not be recreated.
                await spool.record_apply(receipt.operation_key,receipt)
                if remote is None:continue
                await spool._write(spool._durable,receipt.operation_key,remote)
                jobs=await spool._write(spool._settle_recovered_jobs,receipt.operation_key)
                for job_id in jobs:spool.queue._notify_result(job_id)
                _LOG.info('snapshot_recovered_jobs_settled command_sha256=%s sequence=%s count=%s',
                          receipt.command_sha256,receipt.sequence,len(jobs))
            _LOG.info('snapshot_recovery_verified manifest_hash=%s operation_hash=%s cursor=%s replay_count=%s',
                manifest_ref.sha256,sha(operation_key.encode()),sequence,len(tail))
            return proof

    async def verify_initialized(self,proof,database,identity):
        if (type(proof) is not ControllerRecoveryProof or proof not in self._proofs
                or proof.database!=database or proof.identity!=identity):
            raise SnapshotError('controller_recovery_proof_unknown')
        self._authority(proof._binding)
        observed=await self.native.verify_initialized(proof._binding,proof.native_reference)
        if (observed.get('database')!=database.wire() or tuple(observed.get('identity',()))!=identity
                or observed.get('applied_sequence')!=proof.applied_sequence
                or observed.get('schema_version')!=json.loads(proof.control)['schema_version']):
            raise SnapshotError('controller_recovery_native_readback_changed')
        return observed

    async def rebind(self,proof,binding):
        """Reconcile the same retained recovery after control reconnection."""
        if type(proof) is not ControllerRecoveryProof or proof not in self._proofs:
            raise SnapshotError('controller_recovery_proof_unknown')
        self._authority(binding)
        observed=await self.native.verify_initialized(binding,proof.native_reference)
        if (observed.get('database')!=proof.database.wire() or tuple(observed.get('identity',()))!=proof.identity
                or observed.get('applied_sequence')!=proof.applied_sequence
                or observed.get('schema_version')!=json.loads(proof.control)['schema_version']
                or await self.journal.head()!=proof.journal_head):
            raise SnapshotError('controller_recovery_rebind_changed')
        self._authority(binding);object.__setattr__(proof,'_binding',binding)
        return proof

    async def promote(self,proof,binding,*,expected_token):
        self._authority(binding);await self.verify_initialized(proof,proof.database,proof.identity)
        if await self.journal.head()!=proof.journal_head:raise SnapshotError('snapshot_pointer_tail_changed')
        data=encode({'codec':'rkb.verified-snapshot-pointer.v1','stream':self.journal.stream,
            'manifest':proof.manifest.wire(),'verified_cursor':proof.applied_sequence,
            'verified_head':proof.journal_head.reference.wire() if proof.journal_head else None,
            'writer_generation':proof.database.writer_generation,'control_sha256':sha(proof.control)})
        key=self.journal.prefix+'/snapshots/current'
        existing=await self.journal._read(key,self.limits.max_control_bytes)
        if existing is not None and existing.data==data:return existing
        await self.journal._fenced_exact(key,data,expected_token=expected_token,
            writer_generation=proof.database.writer_generation,limit=self.limits.max_control_bytes)
        _LOG.info('snapshot_pointer_promoted manifest_hash=%s cursor=%s',proof.manifest.sha256,proof.applied_sequence)
        return await self.journal._read(key,self.limits.max_control_bytes)

    async def current_pointer(self):
        """Exact pointer identity and CAS token, without re-validating the base it names."""
        value=await self.journal._read(self.journal.prefix+'/snapshots/current',self.limits.max_control_bytes)
        if value is None:return None
        try:
            pointer=json.loads(value.data)
            if (encode(pointer)!=value.data or pointer['codec']!='rkb.verified-snapshot-pointer.v1'
                    or pointer['stream']!=self.journal.stream):raise ValueError
            ref=ObjectRef(**pointer['manifest'])
            if (not ref.key.startswith(self.journal.prefix+'/snapshots/manifests/')
                    or type(ref.last_sequence) is not int or ref.last_sequence<0):raise ValueError
        except (ValueError,TypeError,KeyError):raise SnapshotError('snapshot_pointer_invalid') from None
        return ref,value.token,pointer

    async def current_snapshot(self):
        """Return exact published artifact identity, never an initialization grant."""
        value=await self.journal._read(self.journal.prefix+'/snapshots/current',self.limits.max_control_bytes)
        if value is None:return None
        try:
            pointer=json.loads(value.data)
            if (encode(pointer)!=value.data or pointer['codec']!='rkb.verified-snapshot-pointer.v1'
                    or pointer['stream']!=self.journal.stream):raise ValueError
            ref=ObjectRef(**pointer['manifest'])
            if not ref.key.startswith(self.journal.prefix+'/snapshots/manifests/') or ref.last_sequence<0:raise ValueError
            await self._manifest(ref)
        except (ValueError,TypeError,KeyError):raise SnapshotError('snapshot_pointer_invalid') from None
        return ref,value.token,pointer

    def release(self,proof):
        self._proofs.discard(proof)
    def _control(self,proof):
        with self.history.spool._db() as db:
            rows=db.execute('SELECT source_id,sequence,command_sha256,restriction,pending,unknown '
                'FROM rkb_control_source_heads ORDER BY source_id LIMIT ?',
                (min(self.limits.max_receipts,self.limits.max_control_bytes//128)+1,)).fetchall()
        if len(rows)>min(self.limits.max_receipts,self.limits.max_control_bytes//128):
            raise SnapshotError('snapshot_control_head_budget')
        data=encode({'codec':'rkb.source-base-control.v1','stream':self.journal.stream,
            'database_generation':proof.database_generation,'sequence':proof.accepted_sequence,
            'journal_head':proof.head.reference.wire() if proof.head else None,
            'schema_version':self.history.schema_version,
            'registry_sha256':self.history.registry_hash,'routes':sorted(self.history.routes),
            'heads':[list(row) for row in rows]})
        if len(data)>self.limits.max_control_bytes:raise SnapshotError('snapshot_control_byte_budget')
        if any(row[4] or row[5] for row in rows):raise SnapshotError('snapshot_control_unsettled')
        return data
    async def _manifest(self,ref):
        data=await self.journal._verified(ref,self.limits.max_control_bytes)
        try:
            m=json.loads(data)
            validate_versions(m['versions'])
            if (encode(m)!=data or m['codec']!='rkb.pg-physical-base.v1' or m['stream']!=self.journal.stream
                    or m['applied_sequence']!=ref.last_sequence or type(m['applied_sequence']) is not int
                    or not 0<=m['applied_sequence']<=self.limits.max_receipts
                    or not 0<m['file_count']<=self.limits.max_files
                    or not 0<m['unpacked_bytes']<=self.limits.max_unpacked_bytes):raise ValueError
            blob=BlobRef(**m['blob'])
            from .dataset_recovery_store import DatasetSnapshotTransport
            dataset_blob=(type(self.transport) is DatasetSnapshotTransport and self.transport.matches(blob))
            if blob.size>self.limits.max_archive_bytes or not (blob.key.startswith(self.journal.prefix+'/snapshots/blobs/') or dataset_blob):
                raise ValueError
            for key in ('applied_prefix_sha256','inventory_sha256','backup_manifest_sha256'):_hash(m[key])
            boundary=self.journal._parse_ref(m['journal_head']) if m['journal_head'] is not None else None
            if (boundary.last_sequence if boundary else 0)!=m['applied_sequence']:raise ValueError
            control=m['source_control'];_hash(control['sha256'])
            if (not control['key'].startswith(self.journal.prefix+'/source-base-control/')
                    or type(control['length']) is not int or not 0<control['length']<=self.limits.max_control_bytes):raise ValueError
        except (ValueError,TypeError,KeyError):raise SnapshotError('snapshot_manifest_invalid') from None
        return m,blob,boundary
    async def _tail(self,head,base,sequence):
        ref=head.reference if head else None;tail=[];used=0
        while ref!=base:
            if ref is None:raise SnapshotError('snapshot_boundary_not_ancestor')
            previous,raws=await self.journal.read_segment(ref)
            seal=AcceptedSeal(ref,tuple(sha(raw) for raw in raws))
            for raw in raws:
                used+=len(raw)
                if used>self.limits.max_tail_bytes or len(tail)>=self.limits.max_tail_commands:
                    raise SnapshotError('snapshot_replay_budget')
                tail.append((_command(raw),seal))
            ref=previous
        tail.sort(key=lambda p:p[0].sequence)
        if [c.sequence for c,_ in tail]!=list(range(sequence+1,sequence+len(tail)+1)):
            raise SnapshotError('snapshot_tail_gap')
        return tail
