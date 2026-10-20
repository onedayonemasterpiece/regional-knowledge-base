"""Fixed empty-base restore drill beside the serving PG; no serving adoption."""
from dataclasses import dataclass,replace
import asyncio
import base64
import json
from pathlib import Path
import secrets
import shutil

from .kaggle_pg_snapshot_native import NativeSnapshotFactory,SnapshotError,BlobRef,encode,sha

DRILL_PHASES=frozenset({'drill_prepare','drill_finish','drill_close','drill_verify'})


def drill_database(database,identity):
    token=sha(encode(dict(identity=identity,database_generation=database.database_generation,writer_generation=database.writer_generation)))
    return replace(database,mode='VERIFIED_INITIALIZED',writer_generation='rkb-drill-'+token[:32],
        socket='/kaggle/temp/rkb-drill-'+token[:20]+'/socket',cluster=None,uid=None,gid=None,native_paths=None,
        ownership_evidence='Fixed owned temporary empty-base restore drill; never serving adoption')


@dataclass(eq=False)
class _NativeDrillProof:
    owner: object
    candidate: object
    wire: dict
    closed: bool = False


class EmptySnapshotDrill:
    def __init__(self,serving,*,download,workspace):
        if type(serving) is not NativeSnapshotFactory:raise TypeError('retained serving native owner required')
        self.serving,self.download,self.workspace=serving,download,workspace
        database=drill_database(serving.database,serving.identity)
        self.factory=NativeSnapshotFactory(tools=serving.tools,database=database,identity=serving.identity,
            registry=serving.registry,executor_factory=serving.executor_factory,current=serving.current,
            workspace_private_current=serving.workspace_private_current,native_uid=serving.native_uid,native_gid=serving.native_gid,
            limits=serving.limits,connect=serving.connect,spawn=serving.spawn,probe=serving.probe)
        self.factory.native_env=dict(serving.native_env);self.factory.model_pins=serving.model_pins
        self.sessions={};self.proofs={};self.closing={};self.prepared={}

    def can_reconcile(self,phase,args):
        if phase=='drill_finish':
            session=self.sessions.get(args.get('handle'))
            return session in self.factory._finish_intents
        return (phase=='drill_close' and args.get('reference') in self.closing
            or phase=='drill_verify' and args.get('reference') in self.proofs)

    async def __call__(self,phase,args,key,binding,frame):
        self.serving._current(binding)
        if type(args) is not dict:raise SnapshotError('drill_fixed_arguments')
        if phase=='drill_prepare':return await self._prepare(args,key,binding,frame)
        if phase=='drill_finish':
            if set(args)!={'handle','control_b64','catalog_sha256'}:raise SnapshotError('drill_fixed_arguments')
            session=self.sessions.get(args['handle'])
            if session is None:raise SnapshotError('drill_retained_session_unknown')
            session.binding=binding
            control=base64.b64decode(args['control_b64'],validate=True)
            if control!=session.control:raise SnapshotError('drill_empty_control_changed')
            cap=await self.factory.finish(session,expected_tail=(),final_head=None,
                control_bytes=control,expected_catalog_sha256=args['catalog_sha256'])
            return {'reference':self.factory.reference(cap)}
        if set(args)!={'reference'}:raise SnapshotError('drill_fixed_arguments')
        if phase=='drill_close':return await self._close(args['reference'],binding)
        proof=self.proofs.get(args['reference'])
        if proof is None or proof.owner is not self or not proof.closed or proof.candidate.process.returncode is None:
            raise SnapshotError('drill_shutdown_proof_unknown')
        await self._serving(binding,proof.wire['catalog_sha256'],proof.wire['system_identifier'])
        return dict(proof.wire,reference=args['reference'])

    async def _serving(self,binding,catalog_hash,system_identifier):
        self.serving._current(binding)
        if not callable(self.serving.acquire):raise SnapshotError('drill_serving_connection_unavailable')
        catalog,fields=await self.serving.probe(acquire=self.serving.acquire)
        if (sha(catalog)!=catalog_hash or fields['database_generation']!=self.serving.database.database_generation
                or fields['applied_seq']!=0 or fields['projection_seq']!=0
                or not fields['text_search_ready'] or not fields['dense_search_ready']):
            raise SnapshotError('drill_serving_empty_frontier_changed')
        async with self.serving.acquire() as connection:
            async with connection.transaction():
                await connection.execute("SET LOCAL statement_timeout='2000ms'")
                cursor=await connection.execute('SELECT writer_generation FROM rkb_v4.recovery_state WHERE singleton')
                if await cursor.fetchone()!=(self.serving.database.writer_generation,):raise SnapshotError('drill_serving_writer_changed')
                cursor=await connection.execute('SELECT system_identifier::text FROM pg_control_system()')
                if await cursor.fetchone()!=(system_identifier,):raise SnapshotError('drill_serving_system_changed')
                await self.serving._sql_readiness(connection)
        self.serving._current(binding)

    async def _prepare(self,args,key,binding,frame):
        expected={'manifest_b64','manifest_ref','blob','control_b64','expected_versions','database','identity','restore_operation'}
        if (set(args)!=expected or args['database']!=self.factory.database.wire() or tuple(args['identity'])!=self.factory.identity
                or 'download_ticket' not in frame):raise SnapshotError('drill_fixed_database_identity')
        raw=base64.b64decode(args['manifest_b64'],validate=True);control=base64.b64decode(args['control_b64'],validate=True)
        if max(len(raw),len(control))>self.factory.limits.max_control_bytes:raise SnapshotError('drill_metadata_bound')
        manifest=json.loads(raw);state=json.loads(control)
        if (sha(raw)!=args['manifest_ref']['sha256'] or manifest.get('blob')!=args['blob']
                or manifest.get('applied_sequence')!=0 or manifest.get('journal_head') is not None
                or state.get('sequence')!=0 or state.get('journal_head') is not None or state.get('heads')!=[]
                or manifest.get('writer_generation')!=self.serving.database.writer_generation):
            raise SnapshotError('drill_empty_base_required')
        if self.prepared:raise SnapshotError('drill_candidate_already_owned')
        await self._serving(binding,args['expected_versions']['catalog_sha256'],manifest['system_identifier'])
        free=shutil.disk_usage(Path(self.factory.database.socket).parent.parent).free
        needed=args['blob']['size']+manifest['unpacked_bytes']
        if free<needed:raise SnapshotError('drill_disk_budget')
        # Record before direct download; UNKNOWN never allocates a new candidate.
        self.prepared[key]=(manifest,free)
        archive=await self.download(BlobRef(**args['blob']),args['restore_operation'],ticket=frame['download_ticket'],transfer_operation=key)
        session=await self.factory.prepare(binding,operation_key=args['restore_operation'],manifest_bytes=raw,
            expected_manifest_sha256=sha(raw),archive_path=archive,control_bytes=control,expected_versions=args['expected_versions'],
            workspace=self.workspace(args['restore_operation'],'drill_restore'))
        handle=secrets.token_urlsafe(24);self.sessions[handle]=session
        return {'handle':handle,'disk_free_before':free,'owned_peak_bytes_bound':2*(args['blob']['size']+manifest['unpacked_bytes'])}

    async def _close(self,reference,binding):
        proof=self.closing.get(reference)
        if proof is None:
            await self.factory.verify_reference(binding,reference)
            cap=self.factory._references[reference]
            async with cap._candidate.acquire() as connection:
                async with connection.transaction():
                    await connection.execute("SET LOCAL statement_timeout='2000ms'")
                    await self.factory._sql_readiness(connection)
            manifest,free=next(iter(self.prepared.values()))
            proof=_NativeDrillProof(self,cap._candidate,dict(codec='rkb.empty-base-drill.v1',
                database=self.serving.database.wire(),drill_database=self.factory.database.wire(),identity=list(self.serving.identity),
                manifest_sha256=cap.manifest['sha256'],applied_sequence=0,catalog_sha256=json.loads(cap.control)['catalog_sha256'],
                versions=dict(manifest['versions']),system_identifier=manifest['system_identifier'],
                disk_free_before=free,owned_peak_bytes_bound=2*(manifest['blob']['size']+manifest['unpacked_bytes'])))
            self.closing[reference]=proof
        if not proof.closed:
            await proof.candidate.aclose()
            if proof.candidate.process.returncode is None:
                raise SnapshotError('drill_owned_shutdown_unknown')
            socket=Path(self.factory.database.socket)/('.s.PGSQL.'+str(self.factory.database.port))
            if socket.exists():raise SnapshotError('drill_owned_socket_remains')
            proof.closed=True
            for cap in tuple(self.factory._capabilities):
                if cap._candidate is proof.candidate:await cap.aclose()
        existing=next((ref for ref,p in self.proofs.items() if p is proof),None)
        reference=existing or secrets.token_urlsafe(24);self.proofs[reference]=proof
        await self._serving(binding,proof.wire['catalog_sha256'],proof.wire['system_identifier'])
        return dict(proof.wire,reference=reference)

    async def aclose(self):
        for candidate in tuple(self.factory._owned_candidates):
            await candidate.aclose();self.factory._owned_candidates.discard(candidate)
        self.factory._capabilities.clear()
