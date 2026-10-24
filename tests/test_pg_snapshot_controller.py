"""Controller composition over synthetic fixed native phases; no provider/PG."""
import base64
from contextlib import asynccontextmanager
from dataclasses import asdict,replace
import json
import sys
from types import SimpleNamespace

import pytest

from regional_knowledge.kaggle_apply import ApplyReceipt
from regional_knowledge.kaggle_owned_pg import OwnedDatabase
from regional_knowledge.kaggle_pg_snapshot_native import BlobRef,SnapshotError,encode,sha
from regional_knowledge.pg_snapshot_recovery import PhysicalSnapshotRecovery,S3SnapshotTransport
from test_control_spool import env,accept,SOURCE,VERSIONS
from test_source_history_verifier import Pg,verifier
from test_source_read_gate import binding
from test_participants import people


def versions():
    return dict(schema_version=16,server_version_num=170006,pgvector_version='0.8.2',catalog_sha256=sha(b'catalog'),
        model_sha256='1'*64,tokenizer_sha256='2'*64,schema_revision=VERSIONS.schema,
        function_revision=VERSIONS.function,executor_revision=VERSIONS.executor)


class Transport:
    def __init__(self,blob):self.blob=blob;self.verifications=[]
    async def verify(self,ref):
        assert ref==self.blob;self.verifications.append(ref)


class Native:
    def __init__(self,e,blob):
        self.e,self.blob=e,blob;self.events=[];self.database=None;self.final=None
        self.archive_cursor=0
    async def capture(self,b,**kwargs):
        self.events.append('capture')
        return dict(database_generation='db-source',writer_generation='writer-1',applied_sequence=0,
            applied_prefix_sha256='a'*64,versions=kwargs['versions'],blob=asdict(self.blob),system_identifier='synthetic-system',
            file_count=3,unpacked_bytes=100,inventory_sha256='b'*64,backup_manifest_sha256='c'*64,
            native_tools={n:dict(path='/synthetic/'+n,size=1,sha256='d'*64,version_output='PG17')
                          for n in ('pg_basebackup','pg_verifybackup','postgres')})
    async def prepare(self,b,**kwargs):
        assert any('/recovery-intents/' in key for key in self.e.store.objects)
        self.events.append('prepare');self.database=kwargs['database']
        return dict(handle='retained-native-session',archived_sequence=self.archive_cursor)
    async def apply(self,b,handle,**kwargs):
        assert handle=='retained-native-session' and kwargs['command_bytes']==self.e.cmd.encode()
        assert kwargs['original_pg_writer']=='writer-1'
        assert set(kwargs)=={'command_bytes','accepted','handler_sha256','original_pg_writer'}
        self.events.append('apply');body=b'exact synthetic committed native outcome'
        c=self.e.cmd
        return dict(database_generation='db-source',operation_key=c.operation_key,sequence=c.sequence,
            command_sha256=sha(c.encode()),status='rejected_precondition',outcome_sha256=sha(body),
            duplicate=False,outcome_b64=base64.b64encode(body).decode())
    async def finish(self,b,handle,**kwargs):
        assert kwargs['expected_tail']==()
        assert self.e.journal.prefix+'/outcomes/'+sha(self.e.cmd.operation_key.encode()) not in self.e.store.objects
        self.events.append('rollover-and-readiness');self.final=json.loads(kwargs['control_bytes'])
        assert self.final['sequence']==self.archive_cursor
        assert self.final['journal_head']==kwargs['final_head']
        return 'opaque-native-capability-reference'
    async def verify_initialized(self,b,reference):
        assert reference=='opaque-native-capability-reference'
        return dict(database=self.database,identity=['run','nonce','inc'],applied_sequence=self.archive_cursor,schema_version=self.database['schema_version'])


@asynccontextmanager
async def guard(binding):yield


def decoder(wire):
    value=dict(wire);outcome=base64.b64decode(value.pop('outcome_b64'))
    assert sha(outcome)==value['outcome_sha256']
    return ApplyReceipt(**value,outcome=outcome),{}


@pytest.mark.asyncio
async def test_snapshot_prefix_becomes_ready_before_pending_tail_and_keeps_original_claim(env,monkeypatch):
    pg=Pg();b=binding();history=verifier(env,pg,b,monkeypatch)
    await history.establish_genesis(b)
    blob=BlobRef(env.journal.prefix+'/snapshots/blobs/'+'a'*64,123,'a'*64,'"native-version-1"')
    native=Native(env,blob);transport=Transport(blob)
    controller=PhysicalSnapshotRecovery(env.journal,transport,native,registry=env.registry,history_verifier=history,
        maintenance_guard=guard,current=lambda value:value is b,private_storage_current=lambda:True)
    base=await controller.create_base(b,versions=versions(),writer_generation='writer-1')
    assert base.last_sequence==0 and not any(key.endswith('/snapshots/current') for key in env.store.objects)
    await accept(env);env.store.writer='writer-2'
    old_identity=dict(operation_key=env.cmd.operation_key,command_sha256=sha(env.cmd.encode()),
        database_generation='db-source',writer_generation='writer-1',sequence=env.cmd.sequence)
    with env.q.connect() as db:
        db.execute("insert into jobs(id,actor,idempotency,kind,texts,identity,state,created,updated,attempt) values(?,?,?,?,?,?,?,?,?,?)",
            ('old-accepted-job',env.cmd.person_id,'source-command:'+env.cmd.operation_key,'source_command','[]',
             json.dumps(old_identity),'pending',1.,1.,1))
    await env.spool.mark_apply_sent(env.cmd.operation_key)
    monkeypatch.setitem(sys.modules,'regional_knowledge.kaggle_source_commands',SimpleNamespace(decode_receipt=decoder))
    database=OwnedDatabase('VERIFIED_INITIALIZED', 'db-source', 'writer-2', '/kaggle/temp/restore/socket', 'rkb', ownership_evidence='synthetic-owned-restore')
    proof=await controller.recover(base, b, database=database, expected_versions=versions(), identity=('run', 'nonce', 'inc'), operation_key='recovery-1')
    assert native.events==['capture','prepare','rollover-and-readiness']
    assert proof.applied_sequence==0 and json.loads(proof.control)['heads']==[]
    restored=await env.spool.get(env.cmd.operation_key)
    assert restored.apply_state=='SENT_UNKNOWN' and restored.outcome_state=='NOT_SENT'
    with env.q.connect(write=False) as db:
        job=db.execute("select state,result,identity from jobs where id='old-accepted-job'").fetchone()
        assert job['state']=='pending' and job['result'] is None
        assert json.loads(job['identity'])==old_identity  # Original writer/actor/bytes remain audit evidence.
    assert len(transport.verifications)==2
    with pytest.raises(SnapshotError,match='proof_unknown'):
        await replace(proof).verify_for(database,'run','nonce','inc')
    new_binding=binding();controller.current=lambda value:value is new_binding
    with pytest.raises(SnapshotError,match='current_private_authority'):
        await proof.verify_for(database,'run','nonce','inc')
    assert await controller.rebind(proof,new_binding) is proof
    pointer=await controller.promote(proof,new_binding,expected_token=None)
    assert json.loads(pointer.data)['verified_cursor']==0
    assert json.loads(pointer.data)['verified_head'] is None
    assert json.loads(pointer.data)['manifest']==base.wire()


@pytest.mark.asyncio
async def test_archive_ahead_committed_prefix_ready_with_remainder_still_accepted(env,monkeypatch):
    from test_control_spool import withdraw
    b=binding();history=verifier(env,Pg(),b,monkeypatch)
    await history.establish_genesis(b)
    blob=BlobRef(env.journal.prefix+'/snapshots/blobs/'+'a'*64,123,'a'*64,'"native-version-1"')
    native=Native(env,blob)
    controller=PhysicalSnapshotRecovery(env.journal,Transport(blob),native,
        registry=env.registry,history_verifier=history,maintenance_guard=guard,
        current=lambda value:value is b,private_storage_current=lambda:True)
    base=await controller.create_base(b,versions=versions(),writer_generation='writer-1')
    first=await accept(env)
    later=withdraw(env.p.owner.subject,2)
    second=await accept(env,later,predecessor=await env.journal.head())
    env.store.writer='writer-2'
    command=env.cmd
    native.archive_cursor=1
    database=OwnedDatabase('VERIFIED_INITIALIZED','db-source','writer-2',
        '/kaggle/temp/restore/socket','rkb',ownership_evidence='synthetic-owned-restore')
    proof=await controller.recover(base,b,database=database,expected_versions=versions(),
        identity=('run','nonce','inc'),operation_key='archive-ahead-accepted-original')
    assert proof.applied_sequence==1
    final=json.loads(proof.control)
    assert final['sequence']==1 and final['journal_head']==first.manifest.wire()
    assert final['heads'][0][:3]==[SOURCE,1,sha(command.encode())]
    assert native.events==['capture','prepare','rollover-and-readiness']
    assert (await env.spool.get(later.operation_key)).apply_receipt is None
    assert (await env.journal.verify_accepted(second,later))==sha(later.encode())
    assert json.loads((await controller.promote(proof,b,expected_token=None)).data)['verified_cursor']==1


@pytest.mark.asyncio
async def test_archive_ahead_rejects_cursor_beyond_accepted_tail(env,monkeypatch):
    b=binding();history=verifier(env,Pg(),b,monkeypatch)
    await history.establish_genesis(b)
    blob=BlobRef(env.journal.prefix+'/snapshots/blobs/'+'a'*64,123,'a'*64,'"native-version-1"')
    native=Native(env,blob)
    controller=PhysicalSnapshotRecovery(env.journal,Transport(blob),native,
        registry=env.registry,history_verifier=history,maintenance_guard=guard,
        current=lambda value:value is b,private_storage_current=lambda:True)
    base=await controller.create_base(b,versions=versions(),writer_generation='writer-1')
    await accept(env);env.store.writer='writer-2'
    native.archive_cursor=2
    database=OwnedDatabase('VERIFIED_INITIALIZED','db-source','writer-2',
        '/kaggle/temp/restore/socket','rkb',ownership_evidence='synthetic-owned-restore')
    with pytest.raises(SnapshotError,match='snapshot_archive_cursor_outside_accepted_tail'):
        await controller.recover(base,b,database=database,expected_versions=versions(),
            identity=('run','nonce','inc'),operation_key='archive-ahead-refuse-cursor')
    assert 'rollover-and-readiness' not in native.events


@pytest.mark.asyncio
async def test_tail_uses_exact_ancestor_and_original_revisions(env,monkeypatch):
    from test_control_spool import withdraw
    pg=Pg();b=binding();history=verifier(env,pg,b,monkeypatch)
    first=await accept(env)
    await accept(env,withdraw(env.p.owner.subject,2),predecessor=await env.journal.head())
    controller=PhysicalSnapshotRecovery(env.journal,None,None,registry=env.registry,history_verifier=history,
        maintenance_guard=guard,current=lambda value:True,private_storage_current=lambda:True)
    tail=await controller._tail(await env.journal.head(),first.manifest,1)
    assert len(tail)==1 and tail[0][0].sequence==2
    assert tail[0][0].schema_revision==VERSIONS.schema
    assert tail[0][1].manifest.last_sequence==2
    with pytest.raises(SnapshotError):await controller._tail(None,first.manifest,1)


@pytest.mark.asyncio
async def test_s3_blob_verification_streams_and_rejects_hash_version_and_private_errors():
    from test_recovery_s3 import fixture,PREFIX,Body,BotoFailure
    client,authority,store=fixture();data=b'synthetic-private-backup'
    client.objects[PREFIX+'blob']=(data,'"v1"')
    transport=S3SnapshotTransport(store);ref=BlobRef(PREFIX+'blob',len(data),sha(data),'"v1"')
    await transport.verify(ref)
    with pytest.raises(SnapshotError):await transport.verify(replace(ref,native_version='"v2"'))
    with pytest.raises(SnapshotError):await transport.verify(replace(ref,sha256='f'*64))
    client.get_failure=BotoFailure('AccessDenied',403)
    with pytest.raises(Exception) as caught:await transport.verify(ref)
    assert 'do-not-echo-provider-message' not in str(caught.value)
