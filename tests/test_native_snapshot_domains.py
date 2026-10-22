"""Opt-in same-schema native physical backup/restore + exact domain tail.

Requires the existing isolated offline Noble fixture. No launcher, BGE, real
corpus or provider/storage integration is included in this test.
"""
import asyncio
import base64
from contextlib import asynccontextmanager
from dataclasses import asdict,replace
import json
import os
from pathlib import Path
import subprocess

import psycopg
import pytest

from regional_knowledge.kaggle_apply import HandlerRegistry,apply_accepted
from regional_knowledge.kaggle_domain_bindings import (current_handler,DOMAIN_REVISIONS,
    resolved_current_import_v4_command,resolved_current_case_command,resolved_current_story_edit_command)
from regional_knowledge.kaggle_owned_pg import OwnedDatabase,configure_native_roles,DOMAIN_SQL_ARTIFACTS
from regional_knowledge.kaggle_database_probe import native_probe_for_schema
from regional_knowledge.kaggle_source_acl import inspect_acl
from regional_knowledge.kaggle_pg_snapshot_native import NativeSnapshotFactory,NativeBinary,PhysicalPgTools,SnapshotLimits,BlobRef,encode,sha,digest_file
from regional_knowledge.kaggle_source_commands import WorkerSourceExecutor,seal_wire
from test_geo_manifest_native import native,commands
from test_source_story_native import apply
from test_kaggle_apply import pinned
from test_source_graph import payload as graph_payload
from test_source_import import SOURCE
from test_source_import_v2 import STORY
from test_disagreement_native import ACTOR,CASE,create,attribution
from test_story_editorial import operation

pytestmark=pytest.mark.skipif(not os.getenv('RKB_NATIVE_RESTORE_FIXTURE'),reason='explicit isolated PG17 restore fixture only')


@pytest.mark.asyncio
async def test_actual_physical_base_domain_tail_and_same_candidate_proof(native,monkeypatch):
    import regional_knowledge.kaggle_pg_snapshot_native as snapshot
    import regional_knowledge.kaggle_owned_pg as owned
    root=Path(os.environ['RKB_NATIVE_RESTORE_FIXTURE']);container=os.environ['RKB_STORY_FIXTURE_CONTAINER']
    assert root.is_relative_to('/home/dev/artifacts/regional-knowledge-base') and (root/'.artifact.json').is_file()
    assert container=='rkb-v4-story-fixture-restore-review'
    base=native['connection'];probe=native_probe_for_schema(DOMAIN_REVISIONS.schema)
    routes=('source.import.v4','disagreement.edit.v1','story.edit.v1','geo.manifest.install.v1')
    handlers=[]
    for route in routes:
        prototype=current_handler(route,check_sha256='a'*64,apply_sha256='b'*64)
        c=await pinned(base,prototype.check.name);a=await pinned(base,prototype.apply.name)
        handlers.append(current_handler(route,check_sha256=c.definition_sha256,apply_sha256=a.definition_sha256))
    registry=native['registry']=HandlerRegistry(handlers)
    async with base.transaction():await configure_native_roles(base,selected_sql=DOMAIN_SQL_ARTIFACTS,dbname='rkb')
    @asynccontextmanager
    async def acquire():yield base
    catalog,fields=await probe(acquire=acquire)
    async with base.transaction():acl=await inspect_acl(base)
    monkeypatch.setattr(snapshot,'PRIVATE_RUNTIME_ROOT',root)
    monkeypatch.setattr(owned,'PRIVATE_RUNTIME_ROOT',root)
    monkeypatch.setattr(owned,'ensure_private_runtime_root',lambda **kw:root)
    binaries={};nativebin='/kaggle/working/native/usr/lib/postgresql/17/bin/'
    ld='/kaggle/working/native/usr/lib/x86_64-linux-gnu:/kaggle/working/native/lib/x86_64-linux-gnu:/kaggle/working/native/usr/lib/postgresql/17/lib'
    for name in ('postgres','pg_basebackup','pg_verifybackup'):
        path=root/'bin'/name;path.parent.mkdir(exist_ok=True)
        subprocess.run(['docker','cp',container+':'+nativebin+name,str(path)],check=True,capture_output=True,timeout=5)
        version=subprocess.run(['docker','exec','--env','LD_LIBRARY_PATH='+ld,container,nativebin+name,'--version'],check=True,capture_output=True,text=True,timeout=5).stdout.strip()
        binaries[name]=NativeBinary(str(path),path.stat().st_size,sha(path.read_bytes()),version)
    tools=PhysicalPgTools(**binaries)
    limits=SnapshotLimits(max_archive_bytes=128*1024**2,max_unpacked_bytes=128*1024**2,timeout_seconds=70)
    olddb=OwnedDatabase('VERIFIED_INITIALIZED', native['generation'], 'writer-1', str(root / 'pg/socket'), 'rkb', ownership_evidence='isolated-native-restore-fixture')
    newdb=replace(olddb,writer_generation='writer-2',socket=str(root/'r/socket'))
    binding=('synthetic-run','synthetic-incarnation',1);identity=('synthetic-run','synthetic-nonce','synthetic-incarnation')
    started=[]
    class Process:
        def __init__(self,p,data):self.p=p;self.data=data;self.stdout=p.stdout
        @property
        def returncode(self):return self.p.returncode
        async def wait(self):return await self.p.wait()
        def kill(self):self.p.kill()
        def send_signal(self,signal):
            pid=int((Path(self.data)/'postmaster.pid').read_text().splitlines()[0])
            subprocess.run(['docker','exec','--user','1001:1001',container,'kill','-INT',str(pid)],check=True,capture_output=True,timeout=5)
    async def spawn(*args,**kw):
        args=list(args);name=Path(args[0]).name;args[0]=nativebin+name
        args=[('/fixture-socket' if arg==olddb.socket else arg) for arg in args]
        command=['docker','exec','--user','1001:1001','--env','LD_LIBRARY_PATH='+ld,'--env','LANG=C','--env','LC_ALL=C',container,*args]
        p=await asyncio.create_subprocess_exec(*command,stdin=kw['stdin'],stdout=kw['stdout'],stderr=kw['stderr'])
        if name=='postgres' and '-D' in args:
            started.append(args[args.index('-D')+1]);return Process(p,started[-1])
        return p
    blobs={}
    async def upload(path,*,size,sha256):
        assert digest_file(path,limits.max_archive_bytes)==(size,sha256)
        ref=BlobRef('fixture/snapshots/'+sha256,size,sha256,'verified-local-fixture-bytes')
        blobs[ref.key]=path;return ref
    def factory(database,acquire=None):
        value=NativeSnapshotFactory(tools=tools,database=database,identity=identity,registry=registry,
            executor_factory=WorkerSourceExecutor,current=lambda b:b==binding,
            workspace_private_current=lambda p:Path(p).is_relative_to(root),native_uid=1001,native_gid=1001,
            acquire=acquire,upload_ticket=upload,limits=limits,spawn=spawn)
        # Docker executes every native child as the fixed fixture UID/GID;
        # the host's supplementary Docker group is not forwarded into children.
        value._identity=lambda:{}
        return value
    versions=dict(server_version_num=170011,pgvector_version='0.8.2',catalog_sha256=sha(catalog),model_sha256='b'*64,
        tokenizer_sha256='c'*64,schema_revision=DOMAIN_REVISIONS.schema,function_revision=DOMAIN_REVISIONS.function,executor_revision=DOMAIN_REVISIONS.executor)
    source=factory(olddb,acquire);restored=factory(newdb)
    descriptor=await source.capture(binding,workspace=root/'c',versions=versions,expected_sequence=0)
    assert descriptor['applied_sequence']==0 and descriptor['versions']==versions
    control=encode(dict(codec='rkb.source-base-control.v1',stream=native['journal'].stream,database_generation=native['generation'],
        sequence=0,journal_head=None,catalog_sha256=sha(catalog),acl_sha256=sha(acl),heads=[]))
    manifest=encode(dict(descriptor,codec='rkb.pg-physical-base.v1',stream=native['journal'].stream,journal_head=None,
        source_control=dict(key='fixture/source-control',sha256=sha(control),length=len(control))))
    blob=BlobRef(**descriptor['blob'])
    session=await restored.prepare(binding,operation_key='actual-native-restore-once',manifest_bytes=manifest,
        expected_manifest_sha256=sha(manifest),archive_path=blobs[blob.key],control_bytes=control,expected_versions=versions,workspace=root/'r')
    try:
        # These commands are accepted exactly once AFTER the physical base.
        payload,graph=graph_payload()
        from regional_knowledge.source_graph import resolve_source_graph
        payload['source_graph']=resolve_source_graph(graph,source_id=SOURCE,person_id=ACTOR,semantic=payload['semantic'])
        common=dict(person_id=ACTOR,logical_time='2026-10-20T20:00:00Z')
        tail=[resolved_current_import_v4_command(sequence=1,operation_key='restore-source',request_id='restore-source-1',source_id=SOURCE,payload=payload,**common),
            resolved_current_case_command(sequence=2,operation_key='restore-case',request_id='restore-case-2',case_id=CASE,object_id=CASE,
                action='create',command=create(),source_dependencies=[],attribution=attribution(),**common),
            resolved_current_story_edit_command(sequence=3,operation_key='restore-editorial',request_id='restore-editorial-3',source_id=SOURCE,story_id=STORY,
                expected_revision=1,source_revision=1,eligibility_revision=2,operation=operation(),embedding=[1.]+[0.]*1023,input_tokens=12,**common)]
        from regional_knowledge.geo_manifest_commands import plan_geo_manifest
        geo,_=commands()
        tail.extend(plan_geo_manifest(geo).commands(first_sequence=4,operation_key='restore-geo',request_id='restore-geo-4',**common))
        receipts=[]
        for command in tail:
            original=await apply(native,command);assert original.status=='applied'
            seal=await native['journal'].locate_accepted(command);handler=registry.resolve(command)
            frame=dict(type='DISPATCH',protocol_version=2,connection_generation=1,id='fixture-'+str(command.sequence),claim='fixture-claim',kind='source_command',
                run_id=identity[0],launch_nonce=identity[1],runtime_incarnation=identity[2],database_generation=newdb.database_generation,
                writer_generation='writer-1',command_sha256=sha(command.encode()),handler_sha256=handler.fingerprint,
                command_b64=base64.b64encode(command.encode()).decode(),accepted=seal_wire(seal),compute_claim='fixture-compute',dispatch_generation=1)
            replay=await restored.apply(session,frame)
            assert replay['outcome_sha256']==original.outcome_sha256
            duplicate=await restored.apply(session,frame)
            assert duplicate['duplicate'] and duplicate['outcome_sha256']==original.outcome_sha256
            receipts.append((command.sequence,replay['command_sha256'],replay['outcome_sha256']))
        head=await native['journal'].head();sequence=tail[-1].sequence
        final=encode(dict(json.loads(control),sequence=sequence,journal_head=head.reference.wire()))
        cap=await restored.finish(session,expected_tail=tuple(receipts),final_head=head.reference.wire(),control_bytes=final,expected_catalog_sha256=sha(catalog))
        assert (await cap.verify_for(newdb,*identity))==newdb.connection()
        async with session.candidate.acquire() as connection:
            async with connection.transaction():
                assert await(await connection.execute('SELECT count(*),max(sequence) FROM rkb_v4.applied_ops')).fetchone()==(sequence,sequence)
                assert await(await connection.execute('SELECT writer_generation FROM rkb_v4.recovery_state')).fetchone()==('writer-2',)
                assert await(await connection.execute('SELECT rkb_v4.verify_disagreement_origins_v1(%s::uuid[])',([CASE],))).fetchone()==(True,)
                story=(await(await connection.execute("SELECT rkb_v4.read_editorial_story_v1(%s,NULL,'get',0,10)",(STORY,))).fetchone())[0]
                assert story['body']==operation().body and story['default_variant_ref']['revision']==2
                assert await(await connection.execute('SELECT rkb_v4.verify_source_graph_origins_v1(%s::uuid[])',([SOURCE],))).fetchone()==(True,)
        receipt=dict(physical_base_bytes=blob.size,physical_unpacked_bytes=descriptor['unpacked_bytes'],physical_file_count=descriptor['file_count'],
            physical_sha256=blob.sha256,same_candidate=True,accepted_tail_count=len(tail),each_exact_replay_duplicate=True,
            final_applied_sequence=sequence,writer='writer-2',catalog_sha256=sha(catalog),source_case_editorial_geo=True,
            model_used=False,real_corpus_used=False,provider_calls=0)
        (root/'native-restore-proof.json').write_text(json.dumps(receipt,indent=2)+'\n')
        await cap.aclose()
    finally:
        for candidate in tuple(restored._owned_candidates):await candidate.aclose()
