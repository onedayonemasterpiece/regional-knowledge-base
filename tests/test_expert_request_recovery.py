"""Real existing DatasetRecoveryStore CAS/promotion for bounded expert intake.

Only synthetic SQLite, in-process signed Slack HTTP, and synthetic Dataset
metadata. Never create a PostgreSQL, Kaggle, provider, or network operation.
"""
import base64
import hashlib
import json
from dataclasses import replace
from types import SimpleNamespace

import httpx
import pytest

from regional_knowledge.dataset_recovery_store import DatasetRecoveryStore
from regional_knowledge.bge_queue import BgeQueue
from regional_knowledge.control_spool import ControlSpool
from regional_knowledge.kaggle_apply import HandlerRegistry
from regional_knowledge.expert_request_recovery import (
    ExpertDatasetRequestRecords, ExpertRecordError, from_bytes, from_row,
)
from regional_knowledge.expert_request_control import (
    TargetExpertRequestControl, RequestControlError,
)
from regional_knowledge.kaggle_dataset_base import DatasetFile
from regional_knowledge.kaggle_pg_snapshot_native import encode, sha
from regional_knowledge.participant_contracts import AccountUnlink
from regional_knowledge.recovery_journal import ObjectRef, RecoveryJournal, StaleWriter
from regional_knowledge.queue_writer import QueueWriterAuthority
from test_queue_writer import owner
from test_read_authority import auth
from regional_knowledge.expert_slack_ingress import ExpertSlackIngress, PATH
from test_dataset_recovery_store import prepared
from test_kaggle_dataset_tickets import Queue
from test_control_spool import env, handler
from test_expert_request_control import _prepare, _event, _observation
from test_participants import change, people


def attached(env, tmp_path):
    """Existing expert participant/spool; existing Dataset store/guard, no journal writes."""
    (tmp_path/'dataset').mkdir()
    saved=prepared(tmp_path/'dataset')
    saved.guard.writer_generation='writer'
    # Use the exact SAME queue and stream as the existing ControlSpool,
    # rather than a disconnected fake control database.
    with env.q.connect() as db:
        db.execute('CREATE TABLE IF NOT EXISTS rkb_writer_control('
                   'id INTEGER PRIMARY KEY,storage_sha256 TEXT)')
        db.execute("INSERT OR IGNORE INTO rkb_writer_control VALUES(1,'original-s3-authority')")
    saved.store=DatasetRecoveryStore(env.q,checkpoint=saved.checkpoint,
        manifest_ref=saved.ref,publisher=saved.publisher,
        writer_guard=saved.guard,stream='stream')
    saved.q=env.q
    # The production owner is QueueWriterAuthority._publication_binding;
    # this fixture has a minimal explicit same-transaction equivalent, not
    # a blanket allow-all. Tests for real runtime/current_run changes use
    # the production QueueWriterAuthority separately.
    with env.q.connect() as db:
        db.execute('UPDATE rkb_writer_control SET storage_sha256=? WHERE id=1',
                   (saved.store.storage_identity,))
    def verified_fixture_binding(db,*,key):
        row=db.execute('SELECT storage_sha256 FROM rkb_writer_control WHERE id=1').fetchone()
        if (not saved.guard.current or row is None
                or row[0]!=saved.store.storage_identity
                or not key.startswith(saved.store.prefix)):
            raise PermissionError('stale writer')
        return {'publication_json':None}
    saved.guard._publication_binding=verified_fixture_binding
    env.journal=RecoveryJournal(saved.store,stream='stream')
    env.spool.journal=env.journal
    assert saved.store.queue is env.spool.queue
    assert saved.store.prefix == env.spool.journal.prefix + '/'
    ctl,credentials,link=_prepare(env)
    assert ctl.dataset_records is not None
    return SimpleNamespace(saved=saved,ctl=ctl,credentials=credentials,link=link,env=env)


def next_checkpoint(saved, captured, *, sequence=7, version=2):
    """Synthetic manifest+control and real DatasetRecoveryStore promotion path."""
    prefix=saved.prefix
    archived=saved.checkpoint.archive
    control=encode(dict(codec='rkb.source-base-control.v1',
                        database_generation='db',sequence=sequence))
    manifest=encode(dict(database_generation='db',applied_sequence=sequence,
        blob=dict(size=archived.size,sha256=archived.sha256),
        source_control=dict(key=prefix+'source-base-control/'+sha(control),
                            sha256=sha(control),length=len(control))))
    part=lambda name,data:DatasetFile(name,len(data),sha(data))
    checkpoint=replace(saved.checkpoint,dataset_version=version,base_sequence=sequence,
        manifest=part('base-manifest.json',manifest),
        control=part('source-control.json',control),
        journal=part('journal-control.json',captured))
    reference=ObjectRef(prefix+'snapshots/manifests/'+sha(manifest),
                        sha(manifest),len(manifest),sequence)
    class Publisher:
        ref=saved.publisher.ref
        def read_control(self,file,*,version):
            assert version==checkpoint.dataset_version
            return {
                'journal-control.json':captured,
                'base-manifest.json':manifest,
                'source-control.json':control,
            }[file.name]
    result=dict(dataset_ref=Publisher.ref,dataset_version=version,
                base_sequence=sequence,archive_sha256=archived.sha256,
                archive_size=archived.size)
    return checkpoint,reference,Publisher(),result


def imported_dataset(tmp_path, checkpoint, reference, publisher):
    """Real constructor import into a NEW synthetic local queue from v2 files."""
    from contextlib import asynccontextmanager

    q=BgeQueue(tmp_path/'recovered-dataset.sqlite')
    with q.connect() as db:
        db.execute('CREATE TABLE rkb_writer_control(id INTEGER PRIMARY KEY,storage_sha256 TEXT)')
        db.execute("INSERT INTO rkb_writer_control VALUES(1,'prior-source-owner')")
    class Guard:
        writer_generation='writer'
        def bind_store(self,store):self.store=store
        @asynccontextmanager
        async def __call__(self,generation):
            assert generation=='writer'
            yield
    guard=Guard()
    store=DatasetRecoveryStore(q,checkpoint=checkpoint,manifest_ref=reference,
                               publisher=publisher,writer_guard=guard,stream='stream')
    assert store.latest_checkpoint==checkpoint
    return store


@pytest.mark.asyncio
async def test_exact_immutable_fenced_cas_duplicate_and_changed_bytes_fail_closed(env,tmp_path):
    a=attached(env,tmp_path)
    ctl=a.ctl;stored=a.saved.store
    first=ctl.accept(_observation())
    assert first=={'request_id':first['request_id'],'state':'received','durable':False}
    assert ctl.claim(a.credentials)=={'state':'empty'}
    with env.q.connect(write=False) as db:
        initial=db.execute('SELECT sequence FROM rkb_control_spool_meta WHERE id=1').fetchone()[0]
    head=await env.journal.head()

    assert await ctl.stage_received_dataset(first['request_id']) is False
    row=ctl._row(first['request_id'])
    raw=from_row(row)
    record=from_bytes(raw)
    key=ctl.dataset_records.key(first['request_id'])
    assert key==stored.prefix+'control/expert-requests/'+first['request_id']
    assert record['link_id']==a.link.link_id
    staged=await stored.read(key,max_bytes=65536)
    assert staged and staged.data==raw and staged.token.startswith('pending:')
    assert not stored.remote_durable(key,raw)
    assert not ctl.confirm_dataset_coverage(first['request_id'])
    assert ctl.claim(a.credentials)=={'state':'empty'}

    assert await ctl.stage_received_dataset(first['request_id']) is False
    same=await stored.read(key,max_bytes=65536)
    assert same.token==staged.token and same.data==raw
    assert not await stored.compare_and_swap(
        key,b'forged',expected_token=None,writer_generation='writer')
    assert (await stored.read(key,max_bytes=65536)).data==raw
    with env.p.corpus.connect() as db:
        db.execute("UPDATE rkb_expert_requests SET input_text='different' WHERE id=?",
                   (first['request_id'],))
    with pytest.raises(ExpertRecordError,match='identity_changed'):
        await ctl.stage_received_dataset(first['request_id'])
    with pytest.raises(RequestControlError,match='request_record_conflict'):
        ctl.confirm_dataset_coverage(first['request_id'])
    assert (await stored.read(key,max_bytes=65536)).data==raw

    with env.q.connect(write=False) as db:
        after=db.execute('SELECT sequence FROM rkb_control_spool_meta WHERE id=1').fetchone()[0]
    assert initial==after and await env.journal.head()==head
    assert not env.store.objects  # no RecoveryJournal seal, no new PG head


@pytest.mark.asyncio
async def test_real_promote_only_exact_captured_bytes_enables_request_ack_and_claim(env,tmp_path):
    a=attached(env,tmp_path)
    ctl=a.ctl;store=a.saved.store
    one=ctl.accept(_observation())
    await ctl.stage_received_dataset(one['request_id'])
    first_raw=from_row(ctl._row(one['request_id']))
    first_key=ctl.dataset_records.key(one['request_id'])
    captured=store.export_control()
    # A new exact request after the capture must NOT inherit its coverage.
    late=ctl.accept(_observation(n=2,thread='1792000000.200'))
    await ctl.stage_received_dataset(late['request_id'])
    late_raw=from_row(ctl._row(late['request_id']))
    late_key=ctl.dataset_records.key(late['request_id'])
    before_head=await env.journal.head()
    with env.q.connect(write=False) as db:
        before_seq=db.execute('SELECT sequence FROM rkb_control_spool_meta WHERE id=1').fetchone()[0]
    checkpoint,ref,publisher,result=next_checkpoint(a.saved,captured)
    store.publisher=publisher

    with pytest.raises(Exception,match='dataset_native_snapshot_readback_changed'):
        store.promote(checkpoint,dict(result,archive_size=result['archive_size']+1),
                      manifest_ref=ref)
    assert not store.remote_durable(first_key,first_raw)
    assert ctl.claim(a.credentials)=={'state':'empty'}
    store.promote(checkpoint,result,manifest_ref=ref)
    assert store.remote_durable(first_key,first_raw)
    assert not store.remote_durable(late_key,late_raw)
    assert store.latest_checkpoint==checkpoint
    # Promotion changes the pinned coverage version. Replay one bounded
    # history page before turning received records into claimable pending.
    assert ctl.reconcile_dataset_coverage()==0
    assert ctl._restore_complete
    assert ctl.reconcile_dataset_coverage()==1
    assert ctl.confirm_dataset_coverage(one['request_id'])
    assert not ctl.confirm_dataset_coverage(late['request_id'])
    lease=ctl.claim(a.credentials,request_id=one['request_id'])
    assert lease['state']=='leased' and lease['generation']==1
    assert ctl.claim(a.credentials,request_id=late['request_id'])=={'state':'empty'}

    # A lost promotion ACK may cause the exact same promote to be repeated,
    # but it must never cover post-capture objects.
    store.promote(checkpoint,result,manifest_ref=ref)
    assert not store.remote_durable(late_key,late_raw)
    assert not ctl.confirm_dataset_coverage(late['request_id'])
    with env.q.connect(write=False) as db:
        after_seq=db.execute('SELECT sequence FROM rkb_control_spool_meta WHERE id=1').fetchone()[0]
    assert before_seq==after_seq and await env.journal.head()==before_head


@pytest.mark.asyncio
async def test_http_signed_retry_stays_503_until_exact_dataset_coverage_then_200(env,tmp_path):
    a=attached(env,tmp_path)
    ctl=a.ctl;store=a.saved.store
    ingress=ExpertSlackIngress(ctl,signing_secret='test-signing-secret',
                               clock=lambda:1792000001)
    from starlette.applications import Starlette
    from starlette.routing import Route
    app=Starlette(routes=[Route(PATH,ingress.__call__,methods=['POST'])])
    raw,headers=_event()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                 base_url='http://test') as client:
        for _ in range(2):
            response=await client.post(PATH,content=raw,headers=headers)
            assert response.status_code==503
            assert response.json()=={'error':'remote_intent_pending'}
        one=ctl.accept(_observation())
        assert ctl.claim(a.credentials)=={'state':'empty'}
        captured=store.export_control()
        checkpoint,ref,publisher,result=next_checkpoint(a.saved,captured)
        store.publisher=publisher
        store.promote(checkpoint,result,manifest_ref=ref)
        response=await client.post(PATH,content=raw,headers=headers)
        assert response.status_code==200 and response.json()=={'ok':True}
        repeat=await client.post(PATH,content=raw,headers=headers)
        assert repeat.status_code==200
    with env.p.corpus.connect() as db:
        rows=db.execute('SELECT state,remote_command_hash,dialog_seq FROM rkb_expert_requests').fetchall()
        assert len(rows)==1 and rows[0]['state']=='pending'
        assert rows[0]['remote_command_hash'] is None and rows[0]['dialog_seq']==1
    assert store.latest_checkpoint.dataset_version==2


@pytest.mark.asyncio
async def test_export_import_rebuild_holds_unknown_delivery_and_checks_current_identity(env,tmp_path):
    a=attached(env,tmp_path)
    ctl=a.ctl;store=a.saved.store
    one=ctl.accept(_observation())
    await ctl.stage_received_dataset(one['request_id'])
    raw=from_row(ctl._row(one['request_id']))
    capture=store.export_control()
    checkpoint,ref,publisher,result=next_checkpoint(a.saved,capture)
    store.publisher=publisher
    store.promote(checkpoint,result,manifest_ref=ref)
    recovered_store=imported_dataset(tmp_path,checkpoint,ref,publisher)
    key=ctl.dataset_records.key(one['request_id'])
    assert recovered_store.remote_durable(key,raw)
    assert (await recovered_store.read(key,max_bytes=65536)).data==raw
    # Export of the restored checkpoint may refresh its own manifest/control
    # token to dataset:2; the canonical expert request key+bytes stay exact.
    restored_objects=json.loads(recovered_store.export_control())['objects']
    request_object=next(item for item in restored_objects if item['key']==key)
    assert base64.b64decode(request_object['data_b64'])==raw
    # Actual import has verified ONLY captured bytes, not local late changes.
    env.q=recovered_store.queue
    env.journal=RecoveryJournal(recovered_store,stream='stream')
    env.spool=ControlSpool(env.q,env.p.registry,env.journal,
                           HandlerRegistry([handler()]))

    # Model a LOCAL request state loss, not a Slack provider event or PG effect.
    with env.p.corpus.connect() as db:
        db.execute('DELETE FROM rkb_expert_requests')
        db.execute('DELETE FROM rkb_request_dialogs')
        db.execute('DELETE FROM rkb_request_people')
    # A new request owner uses the recovered, independently covered bytes
    # and the SAME currently verified canonical person/link/capability.
    restored=TargetExpertRequestControl(env.p.registry,env.spool,
        app_id='A111',team_id='T111',bot_user_id='UBOT',
        allowed_channels=('C111',),clock=lambda:1792000001)
    row=restored._row(one['request_id'])
    assert from_row(row)==raw
    assert row['state']=='recovery_review_required'
    assert restored.dataset_records.covered(row)
    assert restored.claim(a.credentials)=={'state':'empty'}
    assert not restored.confirm_dataset_coverage(one['request_id'])
    assert restored.accept(_observation())['durable'] is False
    assert await restored.stage_received_dataset(one['request_id']) is False

    # Wrong binding AFTER crash cannot acquire the restored person's authority.
    with env.p.corpus.connect() as db:
        db.execute('DELETE FROM rkb_expert_requests')
        db.execute('DELETE FROM rkb_request_dialogs')
        db.execute('DELETE FROM rkb_request_people')
    change(env.p,AccountUnlink(link_id=a.link.link_id,expected_link_revision=2,
                              reason='synthetic wrong binding',
                              incorrect_binding=True))
    withheld=TargetExpertRequestControl(env.p.registry,env.spool,
        app_id='A111',team_id='T111',bot_user_id='UBOT',
        allowed_channels=('C111',),clock=lambda:1792000001)
    with env.p.corpus.connect() as db:
        assert db.execute('SELECT count(*) FROM rkb_expert_requests').fetchone()[0]==0
    assert withheld.claim(a.credentials)=={'state':'empty'}


    
@pytest.mark.asyncio
async def test_guarded_cas_lost_response_reuses_original_bytes_and_token(env,tmp_path,monkeypatch):
    a=attached(env,tmp_path)
    one=a.ctl.accept(_observation())
    store=a.saved.store
    records=a.ctl.dataset_records
    original=records._atomic_current_put
    calls=[]

    def write_then_lose_ack(key,data):
        # Fail after the atomic writer-current-checked insert has committed.
        calls.append((key,sha(data)))
        assert original(key,data)
        raise TimeoutError('synthetic lost local CAS response')

    monkeypatch.setattr(records,'_atomic_current_put',write_then_lose_ack)
    with pytest.raises(TimeoutError,match='lost local CAS response'):
        await a.ctl.stage_received_dataset(one['request_id'])
    assert len(calls)==1
    key=a.ctl.dataset_records.key(one['request_id'])
    first=await store.read(key,max_bytes=65536)
    assert first.data==from_row(a.ctl._row(one['request_id']))
    assert not store.remote_durable(key,first.data)
    monkeypatch.setattr(records,'_atomic_current_put',
                        lambda *_args,**_kwargs:pytest.fail('repeat must not write again'))
    assert await a.ctl.stage_received_dataset(one['request_id']) is False
    again=await store.read(key,max_bytes=65536)
    assert again.token==first.token and again.data==first.data
    assert a.ctl.claim(a.credentials)=={'state':'empty'}


@pytest.mark.asyncio
async def test_revocation_after_capture_blocks_dataset_claim_and_ingress_ack(env,tmp_path):
    a=attached(env,tmp_path)
    one=a.ctl.accept(_observation())
    await a.ctl.stage_received_dataset(one['request_id'])
    captured=a.saved.store.export_control()
    checkpoint,ref,publisher,result=next_checkpoint(a.saved,captured)
    a.saved.store.publisher=publisher
    a.saved.store.promote(checkpoint,result,manifest_ref=ref)
    change(env.p,AccountUnlink(link_id=a.link.link_id,expected_link_revision=2,
                              reason='synthetic wrong identity',
                              incorrect_binding=True))
    assert a.ctl.reconcile_dataset_coverage()==0
    assert a.ctl.reconcile_dataset_coverage()==0
    assert a.ctl._row(one['request_id'])['state']=='blocked'
    assert not a.ctl.confirm_dataset_coverage(one['request_id'])
    assert a.ctl.claim(a.credentials)=={'state':'empty'}



@pytest.mark.asyncio
async def test_520_covered_records_restore_by_bounded_keyset_without_arbitrary_cap(env,tmp_path):
    """The 513th historical request must not break constructor or claims."""
    a=attached(env,tmp_path)
    ctl=a.ctl;store=a.saved.store
    for n in range(1,521):
        request=ctl.accept(_observation(n=n))
        assert request['state']=='received'
        await ctl.stage_received_dataset(request['request_id'])
    checkpoint,reference,publisher,result=next_checkpoint(a.saved,store.export_control())
    store.publisher=publisher
    store.promote(checkpoint,result,manifest_ref=reference)
    assert store.latest_checkpoint.dataset_version==2
    # The received queue itself contains >512 records: one guarded claim
    # activates only a bounded page and must not raise a capacity error.
    outcomes=[]
    for _ in range(25):
        outcome=ctl.claim(a.credentials)
        outcomes.append(outcome['state'])
        assert outcome['state'] in ('deferred','empty','leased')
        if outcome['state']=='leased':
            break
    assert outcomes[-1]=='leased'
    # All requests share one thread; their UUID-key ordering is unrelated
    # to original dialog_seq. Rebuild must use captured sequence, never
    # assign() by page order.
    with env.p.corpus.connect() as db:
        db.execute('DELETE FROM rkb_expert_requests')
        db.execute('DELETE FROM rkb_request_dialogs')
        db.execute('DELETE FROM rkb_request_people')
    restored=TargetExpertRequestControl(env.p.registry,env.spool,
        app_id='A111',team_id='T111',bot_user_id='UBOT',
        allowed_channels=('C111',),clock=lambda:1792000001.0)
    assert not restored._restore_complete
    assert restored.claim(a.credentials)=={
        'state':'deferred','reason':'request_restore_pending'}
    with env.p.corpus.connect() as db:
        assert db.execute('SELECT COUNT(*) FROM rkb_expert_requests').fetchone()[0]==128
    pages=2  # Constructor page and the explicit claim continuation.
    while not restored._restore_complete:
        before=restored._restore_cursor
        restored.rebuild_covered_dataset_requests()
        assert restored._restore_cursor!=before or restored._restore_complete
        pages+=1
        assert pages<=10
    assert pages==9
    with env.p.corpus.connect() as db:
        got=db.execute(
            'SELECT COUNT(*),MIN(dialog_seq),MAX(dialog_seq),COUNT(DISTINCT dialog_seq) '
            'FROM rkb_expert_requests').fetchone()
        assert tuple(got)==(520,1,520,520)
        dialog=db.execute('SELECT next_seq,head_seq FROM rkb_request_dialogs').fetchone()
        assert tuple(dialog)==(520,1)
        assert db.execute(
            "SELECT COUNT(*) FROM rkb_expert_requests WHERE state='recovery_review_required'"
        ).fetchone()[0]==520
    # A giant historical covered set is not a permanent product-level cap.
    assert restored.claim(a.credentials)=={'state':'empty'}
    assert restored.reconcile_dataset_coverage()==0
    report=restored.reconcile_restored_request(restored._row(
        str(__import__('uuid').uuid5(__import__('uuid').NAMESPACE_URL,
            'rkb:expert-request:A111:T111:Ev-520')))['id'])
    assert report['state']=='recovery_review_required' and not report['claim_allowed']


@pytest.mark.asyncio
async def test_existing_delivery_receipt_cannot_reset_lost_send_intent_on_intake_only_restore(env,tmp_path):
    from regional_knowledge.expert_delivery_control import (
        TargetExpertDeliveryControl, verified_native_slack_receipt,
    )
    a=attached(env,tmp_path)
    first=a.ctl.accept(_observation())
    await a.ctl.stage_received_dataset(first['request_id'])
    snapshot=a.saved.store.export_control()
    checkpoint,ref,publisher,result=next_checkpoint(a.saved,snapshot)
    a.saved.store.publisher=publisher
    a.saved.store.promote(checkpoint,result,manifest_ref=ref)
    with env.p.corpus.connect() as db:
        db.execute('DELETE FROM rkb_expert_requests')
        db.execute('DELETE FROM rkb_request_dialogs')
        db.execute('DELETE FROM rkb_request_people')
    restored=TargetExpertRequestControl(env.p.registry,env.spool,
        app_id='A111',team_id='T111',bot_user_id='UBOT',
        allowed_channels=('C111',),clock=lambda:1792000001.0)
    status=restored.reconcile_restored_request(first['request_id'])
    assert status['state']=='recovery_review_required'
    assert status['missing_contract']=='accepted_original_result_and_send_intent_with_provider_disposition'
    assert not status['retry_send_allowed'] and not status['ack_allowed']
    delivery=TargetExpertDeliveryControl(restored,source_gate=lambda *_:True)
    proof=verified_native_slack_receipt(
        dict(app_id='A111',user='UBOT',channel='D111',
             thread_ts='1792000000.100',ts='1792000033.100',
             client_msg_id='unbound-original-marker',text='result unavailable'),
        app_id='A111',team_id='T111',bot_user_id='UBOT',
        channel_id='D111',thread_ts='1792000000.100')
    with pytest.raises(RequestControlError,match='delivery_unavailable'):
        delivery.receipt(a.credentials,first['request_id'],proof)
    assert restored.claim(a.credentials)=={'state':'empty'}
    assert not restored.confirm_dataset_coverage(first['request_id'])


@pytest.mark.asyncio
async def test_sqlite_writer_lock_does_not_extend_signed_slack_503_past_deadline(env,tmp_path):
    """Real SQLite BEGIN IMMEDIATE contention, not time.sleep in coroutine.

    Existing SQLite timeout is 5 s; HTTP must respond inside 2.5 s while
    guarded control staging can finish or fail independently.
    """
    import asyncio
    import sqlite3
    import threading
    import time
    from starlette.applications import Starlette
    from starlette.routing import Route

    a=attached(env,tmp_path)
    route=ExpertSlackIngress(a.ctl,signing_secret='test-signing-secret',
                             clock=lambda:1792000001.0)
    app=Starlette(routes=[Route(PATH,route.__call__,methods=['POST'])])
    raw,headers=_event(n=47)
    con=sqlite3.connect(a.saved.store.queue.path,timeout=0.1,check_same_thread=False)
    con.execute('BEGIN IMMEDIATE')
    released=threading.Event()
    def unlock():
        try: con.rollback()
        finally: released.set()
    timer=threading.Timer(3.2,unlock)
    timer.start()
    try:
        started=time.monotonic()
        async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url='http://test') as client:
            response=await client.post(PATH,content=raw,headers=headers)
        elapsed=time.monotonic()-started
        assert response.status_code==503
        assert elapsed<2.95, ('asyncio timeout was blocked by synchronous SQLite '
                              'I/O: '+str(elapsed))
    finally:
        timer.join(timeout=6)
        if not released.is_set():
            con.rollback()
        con.close()
    assert a.ctl.accept(_observation(n=47))['durable'] is False


@pytest.mark.asyncio
async def test_dataset_request_stage_uses_existing_writer_guard_not_unfenced_put(env,tmp_path):
    a=attached(env,tmp_path)
    row=a.ctl.accept(_observation(n=91))
    store=a.saved.store
    # A stopped/retired owner must reject even a local immutable first stage.
    a.saved.guard.current=False
    with pytest.raises(PermissionError,match='stale writer'):
        await a.ctl.stage_received_dataset(row['request_id'])
    key=a.ctl.dataset_records.key(row['request_id'])
    assert await store.read(key,max_bytes=65536) is None
    assert a.ctl.claim(a.credentials)=={'state':'empty'}



@pytest.mark.asyncio
async def test_promotion_during_restore_pages_rewinds_verified_version_cursor(env,tmp_path):
    a=attached(env,tmp_path)
    store=a.saved.store
    ctl=a.ctl
    for n in range(1,141):
        item=ctl.accept(_observation(n=n))
        await ctl.stage_received_dataset(item['request_id'])
    first=store.export_control()
    cp2,ref2,pub2,proof2=next_checkpoint(a.saved,first,version=2)
    store.publisher=pub2
    store.promote(cp2,proof2,manifest_ref=ref2)
    # New Dataset promotion invalidates the old admission traversal; explicit
    # bounded continuation must finish before another durable dialog turn.
    while not ctl._restore_complete or ctl._restore_version!=2:
        ctl.rebuild_covered_dataset_requests()
    extra=ctl.accept(_observation(n=141))
    await ctl.stage_received_dataset(extra['request_id'])
    after=store.export_control()
    cp3,ref3,pub3,proof3=next_checkpoint(a.saved,after,version=3)
    with env.p.corpus.connect() as db:
        db.execute('DELETE FROM rkb_expert_requests')
        db.execute('DELETE FROM rkb_request_dialogs')
        db.execute('DELETE FROM rkb_request_people')
    recovered=TargetExpertRequestControl(env.p.registry,env.spool,
        app_id='A111',team_id='T111',bot_user_id='UBOT',
        allowed_channels=('C111',),clock=lambda:1792000001.0)
    assert recovered._restore_version==2
    assert recovered._restore_cursor is not None
    with env.p.corpus.connect() as db:
        assert db.execute('SELECT COUNT(*) FROM rkb_expert_requests').fetchone()[0]==64
    # The old cursor and through_version=2 can exclude records that the new
    # Dataset promotion has now covered. A restart from key zero is mandatory.
    store.publisher=pub3
    store.promote(cp3,proof3,manifest_ref=ref3)
    assert store.latest_checkpoint.dataset_version==3
    recovered.rebuild_covered_dataset_requests()
    assert recovered._restore_version==3
    seen=0
    while not recovered._restore_complete:
        recovered.rebuild_covered_dataset_requests()
        seen+=1
        assert seen<=4
    with env.p.corpus.connect() as db:
        got=db.execute('SELECT COUNT(*),MIN(dialog_seq),MAX(dialog_seq) '
                       'FROM rkb_expert_requests').fetchone()
        assert tuple(got)==(141,1,141)
        assert db.execute(
            "SELECT COUNT(*) FROM rkb_expert_requests "
            "WHERE state='recovery_review_required'").fetchone()[0]==141
    assert recovered._row(extra['request_id'])['state']=='recovery_review_required'
    assert not recovered.confirm_dataset_coverage(extra['request_id'])
    assert recovered.claim(a.credentials)=={'state':'empty'}


@pytest.mark.asyncio
@pytest.mark.parametrize('transition',['current_run','retire'])
async def test_real_writer_binding_rechecked_in_same_sqlite_cas_after_admission(
        owner,tmp_path,monkeypatch,transition):
    """Actual QueueWriterAuthority current-run/retire race, no real provider."""
    import asyncio
    import threading
    from uuid import uuid4

    e=owner
    path=tmp_path/'synthetic-dataset'
    path.mkdir()
    fixture=prepared(path)
    prefix='recovery/v1/'+hashlib.sha256(e.w.stream.encode()).hexdigest()+'/'
    # Re-encode only the SAME fake Dataset control metadata under this writer
    # stream. No PG, Kaggle, tokens, network or owner service mutation.
    raw_journal=fixture.publisher.read_control(fixture.checkpoint.journal,version=1)
    journal=json.loads(raw_journal)
    for entry in journal['objects']:
        assert entry['key'].startswith(fixture.prefix)
        entry['key']=prefix+entry['key'][len(fixture.prefix):]
    journal=encode(journal)
    control=fixture.publisher.read_control(fixture.checkpoint.control,version=1)
    manifest=json.loads(fixture.publisher.read_control(fixture.checkpoint.manifest,version=1))
    manifest['source_control']['key']=prefix+'source-base-control/'+sha(control)
    manifest=encode(manifest)
    df=lambda name,data:DatasetFile(name,len(data),sha(data))
    cp=replace(fixture.checkpoint,
        manifest=df('base-manifest.json',manifest),
        journal=df('journal-control.json',journal))
    ref=ObjectRef(prefix+'snapshots/manifests/'+sha(manifest),
                  sha(manifest),len(manifest),7)
    class Publisher:
        ref=fixture.publisher.ref
        def read_control(self,file,*,version):
            assert version==1
            return {'base-manifest.json':manifest,
                    'source-control.json':control,
                    'journal-control.json':journal}[file.name]
    identity=hashlib.sha256(json.dumps(
        ['kaggle-dataset',cp.owner_slug,cp.dataset_slug,prefix],
        separators=(',',':')).encode()).hexdigest()
    # Simulate an already verified synthetic storage transition. Source
    # production authority transitions remain untouched.
    e.w._store_identity=None
    with e.q.connect() as db:
        db.execute('UPDATE rkb_writer_control SET storage_sha256=? WHERE id=1',
                   (identity,))
    store=DatasetRecoveryStore(e.q,checkpoint=cp,manifest_ref=ref,
        publisher=Publisher(),writer_guard=e.w,stream=e.w.stream)
    records=ExpertDatasetRequestRecords(store)
    key=records.key(str(uuid4()))
    data=b'exact-immutable-synthetic-request-bytes'
    entered=threading.Event()
    released=threading.Event()
    original=records._atomic_current_put
    def pause_worker(key,raw):
        entered.set()
        if not released.wait(timeout=4):
            raise TimeoutError('synthetic SQLite worker release not observed')
        return original(key,raw)
    monkeypatch.setattr(records,'_atomic_current_put',pause_worker)
    task=asyncio.create_task(records._fenced_put(key,data))
    try:
        assert await asyncio.to_thread(entered.wait,3)
        # The writer guard has admitted the operation and still owns flock.
        # Existing control/retire transition is independent of that flock.
        with e.q.connect() as db:
            if transition=='current_run':
                db.execute('UPDATE control SET current_run=NULL WHERE id=1')
            else:
                db.execute('UPDATE rkb_writer_control SET active=0 WHERE id=1')
        released.set()
        with pytest.raises(StaleWriter):
            await task
        assert await store.read(key,max_bytes=65536) is None
    finally:
        released.set()
        if not task.done():
            try: await task
            except BaseException: pass


def test_record_decoder_rejects_forged_identity_noncanonical_and_arbitrary_key(env,tmp_path):
    a=attached(env,tmp_path)
    ctl=a.ctl
    one=ctl.accept(_observation())
    raw=from_row(ctl._row(one['request_id']))
    assert from_bytes(raw)['request_id']==one['request_id']
    record=json.loads(raw)
    # A different but canonical person UUID is rejected only by the FRESH
    # Participants authority, never claimed from independent DTO bytes.
    assert from_bytes(encode(dict(record,person_id='00000000-0000-0000-0000-000000000001')))
    for field,value in [
        ('event_hash','0'*64),('dialog_seq',0),('link_revision',0),
        ('request_id','bad'),('output_scope','public')]:
        changed=dict(record,**{field:value})
        with pytest.raises(ExpertRecordError):
            from_bytes(encode(changed))
    with pytest.raises(ExpertRecordError):
        from_bytes(json.dumps(record,indent=2).encode())
    with pytest.raises(ExpertRecordError):
        from_bytes(raw[:-1]+b' ')
    with pytest.raises(ExpertRecordError):
        from_bytes(b'{"codec":"a","codec":"b"}')
    with pytest.raises(ExpertRecordError):
        ctl.dataset_records.key('../other')
