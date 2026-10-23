"""Synthetic cross-slice acceptance: signed Slack -> actual grouped Dataset checkpoint.

Production spool/queue/CAS/source contracts are reused. Only the native
Kaggle capture and Dataset provider are fakes from the existing PR215 harness.
No PostgreSQL, network transport, Slack send, credential or live I/O.
"""
from types import SimpleNamespace as NS

import httpx
import pytest
from starlette.applications import Starlette
from starlette.routing import Route

from regional_knowledge.control_spool import ControlSpool
from regional_knowledge.kaggle_apply import HandlerRegistry
from regional_knowledge.kaggle_dataset_checkpoint import (
    CheckpointPending, SourceDatasetCheckpoints,
)
from regional_knowledge.expert_delivery_control import TargetExpertDeliveryControl
from regional_knowledge.expert_request_control import RequestControlError
from regional_knowledge.expert_request_recovery import from_row
from regional_knowledge.expert_slack_ingress import ExpertSlackIngress, PATH
from regional_knowledge.recovery_journal import RecoveryJournal
from test_control_spool import env, handler
from test_participants import people
from test_expert_request_control import _prepare, _event
from test_expert_delivery_recovery import lose_local_and_reopen
from test_kaggle_dataset_checkpoint import control_cycle
from test_queue_writer import owner
from test_read_authority import auth


def composition(owner, env, tmp_path, monkeypatch):
    """Actual BgeQueue/current QueueWriterAuthority; fake *only* native/provider I/O.

    Reuse the canonical real ready-run/maintenance writer fixture, including
    QueueWriterAuthority._publication_binding. The initial Dataset is an
    explicitly synthetic pre-verified archive transition, as in the existing
    test_real_writer_binding_rechecked_in_same_sqlite_cas_after_admission.
    """
    from dataclasses import replace
    import hashlib
    import json
    from regional_knowledge.bge_queue import BgeQueue
    from regional_knowledge.queue_writer import QueueWriterAuthority
    from regional_knowledge.dataset_recovery_store import DatasetRecoveryStore
    from regional_knowledge.kaggle_dataset_base import DatasetFile
    from regional_knowledge.kaggle_pg_snapshot_native import encode, sha
    from regional_knowledge.recovery_journal import ObjectRef, StaleWriter
    from regional_knowledge.kaggle_dataset_checkpoint import _WorkCycleCapture

    p=control_cycle(tmp_path,monkeypatch)
    # 'control_cycle' supplies ONLY its fake provider/native response harness.
    # Its prepared() Queue/Guard must never reach the composed RKB owner.
    assert not isinstance(p.q,BgeQueue)
    assert not isinstance(p.guard,QueueWriterAuthority)
    fake_publisher=p.store.publisher
    fixture=p.checkpoint
    q=owner.q
    s3_writer=owner.w
    assert isinstance(q,BgeQueue) and isinstance(s3_writer,QueueWriterAuthority)
    assert s3_writer.queue is q and s3_writer.current(
        database_generation=s3_writer.database_generation,
        writer_generation=s3_writer.writer_generation)
    # Independent real instance: its FIRST store binding is Dataset, never S3.
    # The S3 fixture owner remains bound to its original storage authority.
    writer=QueueWriterAuthority(q,run_id=s3_writer.run_id,
        database_generation=s3_writer.database_generation,
        writer_generation=s3_writer.writer_generation,stream=s3_writer.stream)
    assert writer is not s3_writer and writer.queue is q
    assert writer._publication_binding.__func__ is QueueWriterAuthority._publication_binding

    prefix='recovery/v1/'+hashlib.sha256(writer.stream.encode()).hexdigest()+'/'
    control=encode(dict(codec='rkb.source-base-control.v1',
                        database_generation=writer.database_generation,sequence=0))
    manifest=encode(dict(database_generation=writer.database_generation,applied_sequence=0,
        blob=dict(size=fixture.archive.size,sha256=fixture.archive.sha256),
        source_control=dict(key=prefix+'source-base-control/'+sha(control),
                            sha256=sha(control),length=len(control))))
    journal_bytes=encode(dict(codec='rkb.dataset-journal.v1',objects=[]))
    df=lambda name,data:DatasetFile(name,len(data),sha(data))
    checkpoint=replace(fixture,database_generation=writer.database_generation,
        base_sequence=0,manifest=df('base-manifest.json',manifest),
        control=df('source-control.json',control),
        journal=df('journal-control.json',journal_bytes))
    manifest_ref=ObjectRef(prefix+'snapshots/manifests/'+sha(manifest),
                           sha(manifest),len(manifest),0)
    class SyntheticDatasetPublisher:
        ref=fake_publisher.ref
        def read_control(self,file,*,version):
            if version==1:
                return dict(**{'base-manifest.json':manifest,
                    'source-control.json':control,
                    'journal-control.json':journal_bytes})[file.name]
            return fake_publisher.read_control(file,version=version)
        def reserve(self,**kwargs):return fake_publisher.reserve(**kwargs)
        def observe(self):return fake_publisher.observe()
        def finalize(self,**kwargs):return fake_publisher.finalize(**kwargs)
        def reconcile(self,**kwargs):return fake_publisher.reconcile(**kwargs)

    # Explicitly SYNTHETIC pre-verified Dataset genesis; NOT proof of native
    # S3-to-Dataset recovery/adoption. Change only this isolated queue fixture's
    # persisted genesis record, never either writer instance's private identity.
    target_id=hashlib.sha256(json.dumps(
        ['kaggle-dataset',checkpoint.owner_slug,checkpoint.dataset_slug,prefix],
        separators=(',',':')).encode()).hexdigest()
    with q.connect() as db:
        db.execute('UPDATE rkb_writer_control SET storage_sha256=? WHERE id=1',
                   (target_id,))
    store=DatasetRecoveryStore(q,checkpoint=checkpoint,manifest_ref=manifest_ref,
        publisher=SyntheticDatasetPublisher(),writer_guard=writer,stream=writer.stream)
    assert store.storage_identity==target_id
    assert writer.current(database_generation=writer.database_generation,
                          writer_generation=writer.writer_generation)
    # The original S3 instance is never rebound, reset or used for Dataset CAS.
    with pytest.raises(StaleWriter,match='writer cannot change existing storage authority'):
        s3_writer.bind_store(store)
    assert not s3_writer.current(database_generation=s3_writer.database_generation,
                                 writer_generation=s3_writer.writer_generation)
    journal=RecoveryJournal(store,stream=writer.stream)
    spool=ControlSpool(q,env.p.registry,journal,HandlerRegistry([handler()]))
    assert spool.queue is q and spool.journal.store is store
    assert store.queue is q and store.writer_guard is writer
    assert store.prefix==journal.prefix+'/'
    assert spool._source_covered_sequence==0
    p.q,p.guard,p.store,p.prefix,p.checkpoint,p.ref=q,writer,store,prefix,checkpoint,manifest_ref
    with q.connect(write=False) as db:
        bound=db.execute('SELECT runtime_incarnation,connection_generation FROM runs WHERE id=?',(owner.run,)).fetchone()
        assert bound and bound['runtime_incarnation']
    p.binding=NS(run_id=owner.run,runtime_incarnation=bound['runtime_incarnation'],
                 connection_generation=bound['connection_generation'],
                 writer_generation=writer.writer_generation)
    # Build new synthetic native-coverage metadata around the actual writer
    # guard, instead of overriding methods on a running runtime object.
    old_proof=p.proof
    p.proof=NS(applied_sequence=0,accepted_sequence=0,
        database_generation=writer.database_generation,head=old_proof.head,
        pending=old_proof.pending,
        current=lambda:writer.current(database_generation=writer.database_generation,
                                      writer_generation=writer.writer_generation))
    p.startup.coverage=p.proof
    p.startup.config.queue=q
    p.startup.config.spool=spool
    p.startup.config.writer_owner=writer
    p.startup.config.writer_generation=writer.writer_generation
    p.startup.restorer.physical.journal=journal
    # A fixture-local startup description is built with the genuine writer
    # fence from inception; no production runtime method is monkeypatched.
    original_startup=p.startup
    p.startup=NS(config=original_startup.config,history=original_startup.history,coverage=p.proof,
        restorer=original_startup.restorer,
        binding_current=lambda binding:(
            p.alive and binding is p.binding and writer.current(
                database_generation=writer.database_generation,
                writer_generation=writer.writer_generation)))
    async def fake_native_capture(batch,physical,bridge,binding,sequence):
        p.calls.append('capture')
        ticket=await batch.tickets.issue_upload(
            binding,batch.config.operation_key+':archive',
            size=20,sha256=sha(b'fixture archive'))
        return dict(database_generation=writer.database_generation,
            writer_generation=writer.writer_generation,
            applied_sequence=sequence,versions=dict(batch.config.versions),
            blob=dict(key=ticket.key,size=ticket.size,sha256=ticket.sha256,
                      native_version='fixture'))
    monkeypatch.setattr(_WorkCycleCapture,'_capture',fake_native_capture)
    p.owner=SourceDatasetCheckpoints(p.startup,clock=lambda:p.clock[0],quiet_seconds=30)
    assert p.owner.store is store
    local=NS(p=env.p,q=q,spool=spool,journal=journal)
    ctl,credentials,_=_prepare(local)
    assert ctl.dataset_records.store is store
    assert ctl.dataset_records.store.writer_guard is writer
    ingress=ExpertSlackIngress(ctl,signing_secret='test-signing-secret',
                               clock=lambda:1792000001.0)
    app=Starlette(routes=[Route(PATH,ingress.__call__,methods=['POST'])])
    return p,local,ctl,credentials,app


async def post(app,raw,headers):
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                 base_url='http://test') as client:
        return await client.post(PATH,content=raw,headers=headers)


def local_rows(people):
    with people.corpus.connect() as db:
        return [dict(r) for r in db.execute('SELECT * FROM rkb_expert_requests')]


@pytest.mark.asyncio
async def test_signed_ingress_quiet_capture_same_pg_seq_then_exact_ack_and_unknown(
        env,owner,tmp_path,monkeypatch):
    p,local,ctl,credentials,app=composition(owner,env,tmp_path,monkeypatch)
    try:
        raw,headers=_event()
        head=await local.journal.head()
        with p.q.connect(write=False) as db:
            sequence=db.execute('SELECT sequence FROM rkb_control_spool_meta '
                                'WHERE id=1').fetchone()[0]
        assert sequence==p.proof.applied_sequence==p.proof.accepted_sequence==0
        first=await post(app,raw,headers)
        assert first.status_code==503 and first.json()=={'error':'remote_intent_pending'}
        rows=local_rows(env.p)
        assert len(rows)==1 and rows[0]['state']=='received'
        rid=rows[0]['id']
        key=ctl.dataset_records.key(rid)
        original=from_row(ctl._row(rid))
        assert p.store.pending_control() is not None
        assert not p.store.remote_durable(key,original)
        assert not await p.owner.refresh(p.binding)
        p.clock[0]=29
        assert not await p.owner.refresh(p.binding) and p.calls==[]
        p.clock[0]=31
        assert await p.owner.refresh(p.binding)
        assert p.store.latest_checkpoint.dataset_version==2
        assert p.store.latest_checkpoint.base_sequence==sequence
        assert p.store.remote_durable(key,original)
        accepted=await post(app,raw,headers)
        assert accepted.status_code==200 and accepted.json()=={'ok':True}
        assert len(local_rows(env.p))==1
        lease=ctl.claim(credentials,request_id=rid)
        assert lease['state']=='leased'
        args=(credentials,rid,lease['generation'],lease['execution_token'])
        delivery=TargetExpertDeliveryControl(ctl,source_gate=lambda row,origin:True)
        assert delivery.prepare_result(*args,text='Synthetic original answer')['state']=='result_unsealed'
        assert await delivery.stage_dataset_result(*args) is False
        result_key=ctl.dataset_records.effect_key(rid,'result')
        with p.q.connect(write=False) as db:
            result_data=bytes(db.execute(
                'SELECT data FROM rkb_dataset_recovery_objects WHERE storage=? AND key=?',
                (p.store.storage_identity,result_key)).fetchone()[0])
        assert not p.store.remote_durable(result_key,result_data)
        p.clock[0]=32
        assert not await p.owner.refresh(p.binding)
        p.clock[0]=63
        assert await p.owner.refresh(p.binding)
        assert p.store.latest_checkpoint.dataset_version==3
        assert p.store.remote_durable(result_key,result_data)
        assert delivery.confirm_dataset_result(*args)['state']=='result_ready'
        assert await delivery.stage_dataset_send(*args) is False
        send_key=ctl.dataset_records.effect_key(rid,'send')
        with p.q.connect(write=False) as db:
            send_data=bytes(db.execute(
                'SELECT data FROM rkb_dataset_recovery_objects WHERE storage=? AND key=?',
                (p.store.storage_identity,send_key)).fetchone()[0])
        assert not p.store.remote_durable(send_key,send_data)
        p.clock[0]=64
        assert not await p.owner.refresh(p.binding)
        p.clock[0]=95
        assert await p.owner.refresh(p.binding)
        assert p.store.latest_checkpoint.dataset_version==4
        assert p.store.remote_durable(send_key,send_data)
        assert delivery.confirm_dataset_send(*args)['state']=='send_intent_ready'
        claimed=delivery.claim_send_once(*args)
        assert claimed['delivery_id']
        delivery.uncertain_delivery(credentials,rid,claimed['delivery_id'])
        restored,after_loss=lose_local_and_reopen(NS(
            env=local,saved=p,ctl=ctl,credentials=credentials))
        result=after_loss.reconcile_restored_dataset_delivery(credentials,rid)
        assert result['state']=='unknown' and result['retry_send_allowed'] is False
        assert restored.claim(credentials)=={'state':'empty'}
        with pytest.raises(RequestControlError):
            after_loss.claim_send_once(*args)
        assert await local.journal.head()==head
        with p.q.connect(write=False) as db:
            assert db.execute('SELECT sequence FROM rkb_control_spool_meta '
                              'WHERE id=1').fetchone()[0]==sequence
        assert p.calls.count('capture')==3
        assert p.calls.count('finalize')==3
        assert p.calls.count('verify')==3
    finally:
        local.spool.close()
        p.guard.close()  # Independent Dataset writer; S3 fixture owner cleans up itself.


@pytest.mark.asyncio
@pytest.mark.parametrize('lost',['finalize','verify'])
async def test_lost_publish_ack_retains_original_version_and_single_signed_event(
        env,owner,tmp_path,monkeypatch,lost):
    p,local,ctl,credentials,app=composition(owner,env,tmp_path,monkeypatch)
    try:
        raw,headers=_event(n=2)
        assert (await post(app,raw,headers)).status_code==503
        rid=local_rows(env.p)[0]['id']
        key=ctl.dataset_records.key(rid)
        original=from_row(ctl._row(rid))
        setattr(p,'fail_'+lost,True)
        assert not await p.owner.refresh(p.binding)
        p.clock[0]=31
        with pytest.raises(CheckpointPending):
            await p.owner.refresh(p.binding)
        assert p.store.latest_checkpoint.dataset_version==1
        assert not p.store.remote_durable(key,original)
        assert (await post(app,raw,headers)).status_code==503
        p.owner=SourceDatasetCheckpoints(p.startup,clock=lambda:p.clock[0],quiet_seconds=30)
        assert await p.owner.refresh(p.binding)
        assert p.store.latest_checkpoint.dataset_version==2
        assert p.store.remote_durable(key,original)
        assert (await post(app,raw,headers)).status_code==200
        assert len(local_rows(env.p))==1
        assert p.calls.count('capture')==1
        assert p.calls.count('finalize')==1
        assert len(p.saved)==1
        assert ctl.claim(credentials)['state']=='leased'
        assert not await p.owner.refresh(p.binding)
    finally:
        local.spool.close()
        p.guard.close()  # Independent Dataset writer; S3 fixture owner cleans up itself.

@pytest.mark.asyncio
@pytest.mark.parametrize('transition',['retire','current_run'])
async def test_real_same_owner_writer_transition_denies_new_slack_cas_and_checkpoint(
        env,owner,tmp_path,monkeypatch,transition):
    """Actual writer runtime retirement denies later CAS and checkpoint.

    Do not monkeypatch _publication_binding or create another permit.
    The local received row is never itself a durable acknowledgment.
    """
    from regional_knowledge.bge_queue import BgeQueue
    from regional_knowledge.queue_writer import QueueWriterAuthority

    p,local,ctl,credentials,app=composition(owner,env,tmp_path,monkeypatch)
    try:
        assert type(local.q) is BgeQueue
        assert type(p.guard) is QueueWriterAuthority
        assert local.spool.queue is p.store.queue is p.q is owner.q
        assert p.store.writer_guard is p.guard and p.guard is not owner.w
        assert not owner.w.current(database_generation=owner.w.database_generation,
                                   writer_generation=owner.w.writer_generation)
        assert p.guard._publication_binding.__func__ is QueueWriterAuthority._publication_binding

        raw,headers=_event(n=51)
        first=await post(app,raw,headers)
        assert first.status_code==503
        assert first.json()=={'error':'remote_intent_pending'}
        existing=next(row for row in local_rows(env.p) if row['event_id']=='Ev-51')
        existing_key=ctl.dataset_records.key(existing['id'])
        staged=await p.store.read(existing_key,max_bytes=65536)
        assert staged is not None and staged.token.startswith('pending:')
        assert not p.store.remote_durable(existing_key,staged.data)

        if transition=='retire':
            await p.guard.retire(p.guard.writer_generation)
        else:
            with p.q.connect() as db:
                db.execute('UPDATE control SET current_run=NULL WHERE id=1')
        assert not p.guard.current(database_generation=p.guard.database_generation,
                                   writer_generation=p.guard.writer_generation)
        raw2,headers2=_event(n=52)
        denied=await post(app,raw2,headers2)
        assert denied.status_code==503 and denied.json().get('ok') is not True
        newer=next(row for row in local_rows(env.p) if row['event_id']=='Ev-52')
        assert newer['state']=='received' and newer['remote_command_hash'] is None
        new_key=ctl.dataset_records.key(newer['id'])
        assert await p.store.read(new_key,max_bytes=65536) is None
        assert not ctl.confirm_dataset_coverage(newer['id'])
        assert ctl.claim(credentials)=={'state':'empty'}

        p.clock[0]=31
        with pytest.raises(CheckpointPending):
            await p.owner.refresh(p.binding,planned_stop=True)
        assert p.store.latest_checkpoint.dataset_version==1
        assert not p.store.remote_durable(existing_key,staged.data)
        assert p.calls==[] and p.saved=={}
        with p.q.connect(write=False) as db:
            assert db.execute('SELECT sequence FROM rkb_control_spool_meta '
                              'WHERE id=1').fetchone()[0]==0
    finally:
        local.spool.close()
        p.guard.close()  # Independent Dataset writer; S3 fixture owner cleans up itself.


@pytest.mark.asyncio
async def test_late_signed_child_after_capture_does_not_inherit_parents_saved_version(
        env,owner,tmp_path,monkeypatch):
    """A real signed child staged during native readback needs its OWN verified
    Dataset coverage; replay of the parent's 200 cannot mark it Saved.
    """
    p,local,ctl,credentials,app=composition(owner,env,tmp_path,monkeypatch)
    try:
        first_raw,first_headers=_event(n=61,thread='1792000000.100')
        child_raw,child_headers=_event(n=62,thread='1792000000.100')
        assert (await post(app,first_raw,first_headers)).status_code==503
        first=next(x for x in local_rows(env.p) if x['event_id']=='Ev-61')
        first_id=first['id']
        assert not await p.owner.refresh(p.binding)
        p.clock[0]=31

        async def late_child_after_capture():
            p.on_verify=None
            child_response=await post(app,child_raw,child_headers)
            assert child_response.status_code==503
            assert child_response.json()=={'error':'remote_intent_pending'}
        p.on_verify=late_child_after_capture
        assert await p.owner.refresh(p.binding)
        assert p.store.latest_checkpoint.dataset_version==2

        child=next(x for x in local_rows(env.p) if x['event_id']=='Ev-62')
        child_id=child['id']
        child_key=ctl.dataset_records.key(child_id)
        child_data=from_row(ctl._row(child_id))
        assert first_id!=child_id and child['state']=='received'
        # The new Dataset version invalidates the bounded restore traversal.
        # A signed parent retry (not a direct caller-Saved assertion) advances
        # the real request-control reconciliation before its HTTP 200.
        assert (await post(app,first_raw,first_headers)).status_code==200
        assert ctl.confirm_dataset_coverage(first_id)
        assert not ctl.confirm_dataset_coverage(child_id)
        assert not p.store.remote_durable(child_key,child_data)
        assert p.store.pending_control() is not None
        assert (await post(app,first_raw,first_headers)).status_code==200
        assert (await post(app,child_raw,child_headers)).status_code==503
        assert len(local_rows(env.p))==2

        p.clock[0]=32
        assert not await p.owner.refresh(p.binding)
        p.clock[0]=63
        assert await p.owner.refresh(p.binding)
        assert p.store.latest_checkpoint.dataset_version==3
        assert p.store.remote_durable(child_key,child_data)
        assert (await post(app,child_raw,child_headers)).status_code==200
        assert len(local_rows(env.p))==2
        assert p.calls.count('capture')==2
        assert p.calls.count('finalize')==2
        assert p.calls.count('verify')==2
        assert ctl.claim(credentials,request_id=child_id)=={'state':'deferred'}
        assert ctl.claim(credentials,request_id=first_id)['state']=='leased'
        with p.q.connect(write=False) as db:
            assert db.execute('SELECT sequence FROM rkb_control_spool_meta '
                              'WHERE id=1').fetchone()[0]==0
    finally:
        local.spool.close()
        p.guard.close()
