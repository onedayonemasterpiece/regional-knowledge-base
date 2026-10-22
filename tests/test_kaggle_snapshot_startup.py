"""First-base product owner over actual queue/admission/conditional S3 adapter."""
import asyncio
from dataclasses import replace
import json

import pytest
from mcp.server.mcpserver.exceptions import ToolError

from regional_knowledge.control_spool import SpoolDenied
from regional_knowledge.kaggle_snapshot_startup import SourceSnapshotConfig,SourceSnapshotCoordinator,DrilledBaseProof
from regional_knowledge.kaggle_source_owners import compose_source_service
from regional_knowledge.kaggle_source_commands import fixed_registry
from regional_knowledge.kaggle_public_backend import current_binding
from regional_knowledge.kaggle_pg_snapshot_native import encode,sha,SnapshotError
from regional_knowledge.kaggle_snapshot_drill import drill_database
from regional_knowledge.source_history_wss import WssNativeHistoryProofAdapter
from regional_knowledge.source_history_verifier import _Coverage
from regional_knowledge.source_read_gate import binding_key
from test_queue_writer import owner,auth,people
from test_kaggle_target_runtime import spec
from types import SimpleNamespace


@pytest.mark.asyncio
async def test_same_owner_captures_drills_closes_then_publishes_under_unchanged_serving_writer(owner):
    e=owner;database=spec(fresh=True).database
    versions=dict(schema_version=database.schema_version,server_version_num=170011,pgvector_version='0.8.2',catalog_sha256='a'*64,model_sha256='a'*64,
        tokenizer_sha256='b'*64,schema_revision='s1',function_revision='f1',executor_revision='e1')
    config=SourceSnapshotConfig('first-base-empty',versions,('https://existing-s3.fixture.invalid',),
        max_archive_bytes=128*1024**2,max_unpacked_bytes=256*1024**2,seconds=2)
    assert SourceSnapshotConfig.from_wire(config.wire()).wire()==config.wire()
    registry=fixed_registry(database,[dict(route='source.withdraw.v1',check_sha256='a'*64,apply_sha256='b'*64)])
    history=WssNativeHistoryProofAdapter(database=database, expected_observer_sha256='c' * 64)
    startup=compose_source_service(queue=e.q,oauth_provider=e.auth.provider,participants=e.auth.p.registry,
        object_store=SimpleNamespace(client=e.c,bucket=e.s.bucket),stream=e.j.stream,registry=registry,history_adapter=history,
        run_id=e.run,database_generation='synthetic-db',writer_generation='writer-1',source_snapshot=config)
    binding=current_binding(e.q);startup.binding=binding;startup.running=True
    coverage=_Coverage('synthetic-db',startup.history,binding,binding_key(binding),startup.history.clock()+30,'native-empty-prefix',None,0,0)
    startup.history._coverage=coverage;startup.coverage=coverage
    startup.config.spool._source_anchor=coverage;startup.config.spool._source_covered_sequence=0
    assert not startup.reads_current()
    assert not startup.command_binding_current(run_id=binding.run_id,runtime_incarnation=binding.runtime_incarnation,
        connection_generation=binding.connection_generation,database_generation=binding.database_generation,writer_generation='writer-1')
    with pytest.raises(SpoolDenied,match='configured control intake unavailable'):
        await startup.config.spool.stage_source_import(None,None,database_generation='synthetic-db')
    with pytest.raises(SpoolDenied,match='configured control intake unavailable'):
        await startup.config.spool.authorize_command(None,None)
    with pytest.raises(ToolError,match='first_snapshot_unverified'):startup.backend._importer()
    def sign(method,**kwargs):
        assert kwargs['HttpMethod'] in ('PUT','GET')
        params=kwargs['Params']
        assert params.get('IfNoneMatch')=='*' or params.get('IfMatch')=='"direct-native-version"'
        return 'https://existing-s3.fixture.invalid/private-direct-transfer?synthetic-scope'
    e.c.generate_presigned_url=sign
    task=asyncio.create_task(startup.snapshot.capture_once(binding));phases=[];native_proof=None
    pointer_key=e.j.prefix+'/snapshots/current';blob=b'synthetic-private-base'
    try:
        async with asyncio.timeout(3):
            while not task.done():
                work=await startup.admission.next_channel_work(e.run,e.token,e.generation)
                if work is None:await asyncio.sleep(.001);continue
                startup.admission.mark_work_sent(work)
                frame=dict(work.document_payload,compute_claim=work.compute_claim,dispatch_generation=work.dispatch_generation)
                phase=frame['phase'];phases.append(phase)
                assert startup.config.spool._lock.locked() and not startup.reads_current()
                assert pointer_key not in e.c.objects
                if phase=='capture':
                    pending_owner=SourceSnapshotCoordinator(startup,config)
                    with pytest.raises(SnapshotError,match='unknown_reconcile_same_operation'):
                        await pending_owner.capture_once(binding)
                    assert not pending_owner.verified
                    with e.q.connect(write=False) as db:
                        assert db.execute("SELECT COUNT(*) FROM jobs WHERE kind='recovery_phase'").fetchone()[0]==1
                    intent=dict(job_id=frame['id'],claim=frame['claim'],operation_key=frame['operation_key'],size=len(blob),sha256=sha(blob),
                        run_id=e.run,launch_nonce=startup.snapshot.dispatch.identity[1],runtime_incarnation=binding.runtime_incarnation,
                        compute_claim=frame['compute_claim'],dispatch_generation=frame['dispatch_generation'])
                    ticket=await startup.snapshot.dispatch.issue_upload(intent,run_id=e.run,token=e.token,generation=e.generation)
                    assert ticket['method']=='PUT' and ['If-None-Match','*'] in ticket['headers']
                    e.c.objects[ticket['key']]=(blob,'"direct-native-version"')
                    value=dict(database_generation='synthetic-db',writer_generation='writer-1',applied_sequence=0,
                        applied_prefix_sha256='a'*64,versions=versions,
                        blob=dict(key=ticket['key'],size=len(blob),sha256=sha(blob),native_version='"direct-native-version"'),
                        system_identifier='native-system',file_count=1,unpacked_bytes=1024,inventory_sha256='b'*64,
                        backup_manifest_sha256='c'*64,native_tools={})
                elif phase=='drill_prepare':
                    assert frame['arguments']['database']==drill_database(database,startup.snapshot.dispatch.identity).wire()
                    assert frame['download_ticket']['method']=='GET'
                    native_proof=dict(codec='rkb.empty-base-drill.v1',database=database.wire(),
                        drill_database=frame['arguments']['database'],identity=list(startup.snapshot.dispatch.identity),
                        manifest_sha256=frame['arguments']['manifest_ref']['sha256'],applied_sequence=0,schema_version=database.schema_version,
                        versions=versions,system_identifier='native-system',disk_free_before=2**30,
                        owned_peak_bytes_bound=2*(1024+len(blob)))
                    value=dict(handle='isolated-B-session',disk_free_before=2**30,owned_peak_bytes_bound=2*(1024+len(blob)))
                elif phase=='drill_finish':value={'reference':'actual-owned-B-capability'}
                elif phase=='drill_close':value=dict(native_proof,reference='retained-B-closed-proof')
                elif phase=='drill_verify':value=dict(native_proof,reference='retained-B-closed-proof')
                else:raise AssertionError(phase)
                receipt=dict(job_id=frame['id'],claim=frame['claim'],receipt=dict(phase=phase,request_sha256=frame['request_sha256'],value=value),
                    run_id=e.run,launch_nonce=startup.snapshot.dispatch.identity[1],runtime_incarnation=binding.runtime_incarnation,
                    compute_claim=frame['compute_claim'],dispatch_generation=frame['dispatch_generation'])
                await startup.snapshot.dispatch.receive(receipt,run_id=e.run,token=e.token,generation=e.generation)
                r=work.resource
                await startup.admission.resource_released(e.run,e.token,e.generation,runtime_incarnation=r.runtime_incarnation,
                    resource_kind='document',resource_id=r.resource_id,compute_claim=r.compute_claim,dispatch_generation=r.dispatch_generation)
            reference=await task
        assert phases==['capture','drill_prepare','drill_finish','drill_close','drill_verify']
        pointer=json.loads(e.c.objects[pointer_key][0])
        assert pointer['manifest']==reference.wire() and pointer['writer_generation']=='writer-1' and pointer['verified_cursor']==0
        assert startup.reads_current() and startup.snapshot.verified
        startup.config.spool._require_intake()
        assert startup.command_binding_current(run_id=binding.run_id,runtime_incarnation=binding.runtime_incarnation,
            connection_generation=binding.connection_generation,database_generation=binding.database_generation,writer_generation='writer-1')
        proof=next(iter(startup.snapshot._proofs))
        with pytest.raises(SnapshotError,match='retained_drill_proof_required'):await replace(proof).verify_current()
        restarted=SourceSnapshotCoordinator(startup,config)
        assert await restarted.capture_once(binding)==reference and restarted.verified
        assert len(phases)==5  # No backup/model/native drill repeated on API restart.
    finally:
        if not task.done():task.cancel()
        await asyncio.gather(task,return_exceptions=True)
        await startup.snapshot.aclose()
        if startup.snapshot.dispatch is not None:await startup.snapshot.dispatch.aclose()
        await startup.source_dispatch.aclose();startup.config.spool.close();startup.config.writer_owner.close()
