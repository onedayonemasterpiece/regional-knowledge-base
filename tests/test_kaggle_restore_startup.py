"""Real source-owner composition through pre-READY durable WSS assignments."""
import asyncio
from dataclasses import replace
from types import SimpleNamespace

import pytest

from regional_knowledge.kaggle_pg_snapshot_native import encode,sha
from regional_knowledge.kaggle_restore_startup import SourceRestoreConfig
from regional_knowledge.kaggle_source_owners import compose_source_service
from regional_knowledge.kaggle_source_commands import fixed_registry
from regional_knowledge.source_history_wss import WssNativeHistoryProofAdapter
from regional_knowledge.recovery_journal import ObjectRef
from test_queue_restore_writer import recovery_owner,owner,auth,people
from test_kaggle_target_runtime import spec


@pytest.mark.asyncio
async def test_api_restart_of_active_restore_recreates_capture_facade_without_pg_replay():
    from regional_knowledge.kaggle_restore_startup import SourceRestoreCoordinator
    coordinator=SourceRestoreCoordinator.__new__(SourceRestoreCoordinator)
    binding=SimpleNamespace(database_generation='db',writer_generation='writer')
    async def begin(*args,**kwargs):pass
    coordinator.writer=SimpleNamespace(begin_restore=begin,current=lambda **kwargs:True)
    coordinator.config=SimpleNamespace()
    coordinator.startup=SimpleNamespace(dataset_seed=object())
    coordinator.journal=object();coordinator.complete=False
    coordinator.dispatch=None;coordinator.physical=None
    coordinator._observe=lambda:(binding,'nonce')
    composed=[]
    def compose(observed,nonce):
        composed.append((observed,nonce));coordinator.dispatch=object();coordinator.physical=object()
    coordinator._compose=compose
    assert await coordinator.refresh()
    assert coordinator.complete and coordinator.physical is not None and composed==[(binding,'nonce')]
    assert await coordinator.refresh()
    assert len(composed)==1


@pytest.mark.asyncio
@pytest.mark.parametrize('dataset_input',[False,True])
async def test_production_owner_restores_before_ready_through_same_claim_and_ticket_owner(recovery_owner,dataset_input,monkeypatch):
    e=recovery_owner;database=replace(spec().database,writer_generation='writer-2')
    registry=fixed_registry(database,[dict(route='source.withdraw.v1',check_sha256='a'*64,apply_sha256='b'*64)])
    routes=['source.fetch','source.import','source.search','source.withdraw']
    control=encode(dict(codec='rkb.source-base-control.v1',stream=e.j.stream,database_generation='synthetic-db',sequence=0,
        journal_head=None,catalog_sha256='a'*64,acl_sha256='b'*64,
        registry_sha256=sha(encode(sorted(h.fingerprint for h in registry.handlers.values()))),routes=routes,heads=[]))
    ckey=e.j.prefix+'/source-base-control/'+sha(control)
    blob=b'synthetic-private-physical-base';blob_key=e.j.prefix+'/snapshots/blobs/'+'0'*64+'/'+sha(blob)
    manifest=encode(dict(codec='rkb.pg-physical-base.v1',stream=e.j.stream,database_generation='synthetic-db',writer_generation='writer-1',
        applied_sequence=0,journal_head=None,versions=dict(e.plan.versions),blob=dict(key=blob_key,size=len(blob),sha256=sha(blob),native_version='"native-blob"'),
        source_control=dict(key=ckey,sha256=sha(control),length=len(control)),applied_prefix_sha256='d'*64,
        file_count=1,unpacked_bytes=100,inventory_sha256='e'*64,backup_manifest_sha256='f'*64))
    mkey=e.j.prefix+'/snapshots/manifests/'+sha(manifest)
    e.old.c.objects.update({ckey:(control,'"native-control"'),blob_key:(blob,'"native-blob"'),mkey:(manifest,'"native-manifest"')})
    plan=replace(e.plan,manifest=ObjectRef(mkey,sha(manifest),len(manifest),0))
    dataset_downloads=[]
    if dataset_input:
        import base64
        import json
        from regional_knowledge.kaggle_dataset_base import DatasetCheckpoint,DatasetFile
        journal=encode(dict(codec='rkb.dataset-journal.v1',objects=[dict(key=k,token=v[1],
            data_b64=base64.b64encode(v[0]).decode()) for k,v in e.old.c.objects.items()
            if k.startswith(e.j.prefix+'/') and '/snapshots/' not in k]))
        def pin(name,data):return DatasetFile(name,len(data),sha(data))
        checkpoint=DatasetCheckpoint('fixture-owner','rkb-base',1,'synthetic-db',0,
            pin('base-manifest.json',manifest),pin(blob_key.rsplit('/',1)[-1],blob),
            pin('source-control.json',control),pin('journal-control.json',journal))
        class Publisher:
            ref='fixture-owner/rkb-base'
            def __init__(self,*args,**kwargs):pass
            def read_control(self,file,*,version):
                assert version==1 and file.name!=checkpoint.archive.name
                return {'base-manifest.json':manifest,'source-control.json':control,'journal-control.json':journal}[file.name]
            def snapshot_download_link(self,file,*,version):
                assert version==1 and file==checkpoint.archive
                dataset_downloads.append((file.name,version))
                return 'https://storage.googleapis.com/historical-version-1','"dataset-version1"'
        monkeypatch.setattr('regional_knowledge.kaggle_dataset_publisher.authenticated_service_api',
            lambda:SimpleNamespace(CONFIG_NAME_USER='user',get_config_value=lambda _: 'fixture-owner'))
        monkeypatch.setattr('regional_knowledge.kaggle_dataset_publisher.KaggleDatasetPublisher',Publisher)
        with e.q.connect() as db:
            db.execute('CREATE TABLE rkb_dataset_seed(operation_key TEXT PRIMARY KEY,provider_ref TEXT,state TEXT,package_json TEXT,version INTEGER)')
            db.execute('INSERT INTO rkb_dataset_seed VALUES(?,?,?,?,?)',('seed',Publisher.ref,'published',
                json.dumps(dict(manifest_sha256=sha(manifest),database_generation='synthetic-db',base_sequence=0)),1))
        plan=replace(plan,dataset_checkpoint=checkpoint,allowed_origins=('https://storage.googleapis.com',))
    signatures=[]
    def sign(method,**kwargs):
        signatures.append((method,kwargs))
        assert method=='get_object' and kwargs['Params']['IfMatch']=='"native-blob"'
        return 'https://existing-s3.fixture.invalid/private-exact-object?synthetic-ticket'
    e.old.c.generate_presigned_url=sign
    history=WssNativeHistoryProofAdapter(database=database,expected_catalog_sha256='a'*64,
        expected_acl_sha256='b'*64,expected_observer_sha256='c'*64)
    imports=None
    if dataset_input:
        from regional_knowledge.kaggle_import_composition import SourceImportOwners
        from regional_knowledge.kaggle_import_tokenizer import TokenizerProcessSpec
        imports=SourceImportOwners('a'*64,'b'*64,('https://existing-s3.fixture.invalid',),'/operator/tokenizer',
            TokenizerProcessSpec('/operator/versioned-venv/bin/python','c'*64,'d'*64))
    startup=compose_source_service(queue=e.q,oauth_provider=e.old.auth.provider,participants=e.old.auth.p.registry,
        object_store=SimpleNamespace(client=e.old.c,bucket=e.s.bucket),stream=e.j.stream,registry=registry,history_adapter=history,
        run_id=e.run,database_generation='synthetic-db',writer_generation='writer-2',source_restore=plan)
    if dataset_input:
        imported=imports.compose(queue=e.q,spool=startup.config.spool,participants=e.old.auth.p.registry,
            object_store=SimpleNamespace(client=e.old.c,bucket=e.s.bucket),writer=startup.config.writer_owner)
        assert imported['import_originals'].spool is startup.config.spool
        assert imported['import_originals'].store.client is e.old.c
    task=asyncio.create_task(startup.restorer.refresh());phases=[]
    try:
        async with asyncio.timeout(3):
            while not task.done():
                work=await startup.admission.next_channel_work(e.run,e.token,e.generation)
                if work is None:await asyncio.sleep(.001);continue
                startup.admission.mark_work_sent(work)
                frame=dict(work.document_payload)
                frame.update(compute_claim=work.compute_claim,dispatch_generation=work.dispatch_generation)
                phase=frame['phase'];phases.append(phase)
                assert not startup.reads_current()
                if phase=='prepare':
                    assert e.q.status()['state']=='starting'
                    if dataset_input:
                        assert frame['download_ticket']['headers']==[['If-Match','"dataset-version1"']]
                        assert 'dataset_base' not in frame and 'dataset_observation' not in frame
                    else:assert frame['download_ticket']['headers']==[['If-Match','"native-blob"']]
                    assert frame['arguments']['manifest_b64'] and frame['writer_generation']=='writer-2'
                    value={'handle':'native-retained-session'}
                elif phase=='finish':
                    assert frame['arguments']['handle']=='native-retained-session'
                    value={'reference':'retained-native-capability'}
                    ready=dict(hashes=dict(e.expected.hashes), database_generation='synthetic-db', schema_version=e.expected.schema_version, applied_seq=0, projection_seq=0, visibility_epoch=0, text_search_ready=True, dense_search_ready=True, archive_read_ready=None, proof_ready=None)
                    assert e.q.accept_search_runtime(e.run,e.token,e.generation,ready).ok
                    e.q.heartbeat(e.run,e.token,ready=True,connection_generation=e.generation)
                    assert not startup.config.writer_owner.current(database_generation='synthetic-db',writer_generation='writer-2')
                elif phase=='verify':
                    value=dict(database=database.wire(),identity=[e.run,e.nonce,e.b.runtime_incarnation],applied_sequence=0,schema_version=database.schema_version)
                else:raise AssertionError(phase)
                receipt=dict(job_id=frame['id'],claim=frame['claim'],receipt=dict(phase=phase,request_sha256=frame['request_sha256'],value=value),
                    run_id=e.run,launch_nonce=e.nonce,runtime_incarnation=e.b.runtime_incarnation,
                    compute_claim=frame['compute_claim'],dispatch_generation=frame['dispatch_generation'])
                settled=await startup.restorer.dispatch.receive(receipt,run_id=e.run,token=e.token,generation=e.generation)
                assert settled['status']=='accepted'
                r=work.resource
                await startup.admission.resource_released(e.run,e.token,e.generation,runtime_incarnation=r.runtime_incarnation,
                    resource_kind='document',resource_id=r.resource_id,compute_claim=r.compute_claim,dispatch_generation=r.dispatch_generation)
            assert await task
        assert phases==['prepare','finish','verify','verify','verify']
        assert bool(signatures)==(not dataset_input)
        assert bool(dataset_downloads)==dataset_input
        assert startup.config.writer_owner.current(database_generation='synthetic-db',writer_generation='writer-2')
        assert not startup.reads_current()  # Full native source history remains a separate mandatory gate.
        from regional_knowledge.kaggle_public_backend import current_binding
        assert startup.restorer.dispatch.current(current_binding(e.q))
        assert startup.config.spool.queue is startup.backend.worker_queue is e.q
        # API-only restart must not replay an already activated physical base,
        # even when later accepted writes have advanced the original head.
        from regional_knowledge.kaggle_restore_startup import SourceRestoreCoordinator
        from test_control_spool import withdraw
        later=replace(withdraw(e.old.auth.p.owner.subject),schema_revision='s1',function_revision='f1',executor_revision='e1')
        accepted=await startup.config.spool.journal.seal([later],predecessor=None,writer_generation='writer-2')
        assert await startup.config.spool.journal.verify_accepted(accepted,later)==sha(later.encode())
        advanced=dict(ready,applied_seq=1,projection_seq=1)
        assert e.q.accept_search_runtime(e.run,e.token,e.generation,advanced).ok
        if dataset_input:await startup.restorer.dispatch.aclose()  # Previous API owner is gone.
        restarted=SourceRestoreCoordinator(startup,plan)
        assert await restarted.refresh() and restarted.complete
        assert (restarted.dispatch is not None)==dataset_input
        assert phases==['prepare','finish','verify','verify','verify']
        await restarted.aclose()
    finally:
        if not task.done():task.cancel()
        await asyncio.gather(task,return_exceptions=True)
        await startup.restorer.aclose();await startup.source_dispatch.aclose();startup.config.spool.close();startup.config.writer_owner.close()


@pytest.mark.asyncio
async def test_lifespan_yields_wss_route_before_waiting_for_pre_ready_restore(recovery_owner,monkeypatch):
    # Real lifespan ordering with the concrete owner installed; its native
    # operation is held pending, so server startup must still finish.
    e=recovery_owner
    from regional_knowledge.kaggle_service_startup import TargetServiceStartup,TargetServiceConfig
    from regional_knowledge.control_spool import ControlSpool
    database=replace(spec().database,writer_generation='writer-2')
    registry=fixed_registry(database,[dict(route='source.withdraw.v1',check_sha256='a'*64,apply_sha256='b'*64)])
    spool=ControlSpool(e.q,e.old.auth.p.registry,e.j,registry)
    history=WssNativeHistoryProofAdapter(database=database,expected_catalog_sha256='a'*64,expected_acl_sha256='b'*64,expected_observer_sha256='c'*64)
    startup=TargetServiceStartup(TargetServiceConfig(e.old.auth.provider,e.old.auth.p.registry,e.q,spool,history,'writer-2',e.w.current,
        writer_owner=e.w,source_restore=e.plan))
    entered=asyncio.Event();blocked=asyncio.Event()
    async def pending():entered.set();await blocked.wait()
    monkeypatch.setattr(startup.restorer,'refresh',pending)
    async with asyncio.timeout(1):
        async with startup.lifespan(None):
            await entered.wait()
            assert startup.running and not startup.reads_current()
