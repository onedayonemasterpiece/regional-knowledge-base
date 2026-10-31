import base64
from contextlib import asynccontextmanager
import hashlib
import json
from types import SimpleNamespace as NS

import pytest

from regional_knowledge.dataset_recovery_store import DatasetRecoveryStore,DatasetSnapshotTransport
from regional_knowledge.kaggle_dataset_base import DatasetCheckpoint,DatasetFile
from regional_knowledge.kaggle_pg_snapshot_native import BlobRef,encode,sha
from regional_knowledge.recovery_journal import ObjectRef,JournalUnknown
from test_kaggle_dataset_tickets import Queue
from test_control_spool import env,accept,people,receipt


def prepared(tmp_path):
    queue=Queue(tmp_path/'queue.sqlite');prefix='recovery/v1/'+sha(b'stream')+'/'
    original=b'original accepted command'
    journal=encode(dict(codec='rkb.dataset-journal.v1',objects=[dict(key=prefix+'commands/one',
        data_b64=base64.b64encode(original).decode(),token='native-original-token')]))
    control=encode(dict(codec='rkb.source-base-control.v1',database_generation='db',sequence=7))
    ckey=prefix+'source-base-control/'+sha(control)
    manifest=encode(dict(database_generation='db',applied_sequence=7,
        blob=dict(size=len(b'archive never read by Dev'),sha256=sha(b'archive never read by Dev')),
        source_control=dict(key=ckey,sha256=sha(control),length=len(control))))
    def pin(name,data):return DatasetFile(name,len(data),sha(data))
    checkpoint=DatasetCheckpoint('fixture-owner','rkb-pg-checkpoint',1,'db',7,
        pin('base-manifest.json',manifest),pin('pg.blob',b'archive never read by Dev'),
        pin('source-control.json',control),pin('journal-control.json',journal))
    mref=ObjectRef(prefix+'snapshots/manifests/'+sha(manifest),sha(manifest),len(manifest),7)
    class Guard:
        current=True
        def bind_store(self,store):self.store=store
        @asynccontextmanager
        async def __call__(self,generation):
            if not self.current or generation!='writer':raise PermissionError('stale writer')
            yield
    guard=Guard();reads=[]
    class Publisher:
        ref='fixture-owner/rkb-pg-checkpoint'
        def read_control(self,file,*,version):
            assert version==1 and file.name!='pg.blob'
            reads.append(file.name)
            return {'base-manifest.json':manifest,'source-control.json':control,'journal-control.json':journal}[file.name]
    with queue.connect() as db:
        db.execute('CREATE TABLE rkb_writer_control(id INTEGER PRIMARY KEY,storage_sha256 TEXT)')
        db.execute("INSERT INTO rkb_writer_control VALUES(1,'original-s3-authority')")
        db.execute('CREATE TABLE rkb_dataset_seed(operation_key TEXT PRIMARY KEY,provider_ref TEXT,state TEXT,package_json TEXT,version INTEGER)')
        db.execute('INSERT INTO rkb_dataset_seed VALUES(?,?,?,?,?)',('seed',Publisher.ref,'published',
            json.dumps(dict(manifest_sha256=sha(manifest),database_generation='db',base_sequence=7)),1))
    store=DatasetRecoveryStore(queue,checkpoint=checkpoint,manifest_ref=mref,publisher=Publisher(),writer_guard=guard,stream='stream')
    return NS(store=store,q=queue,guard=guard,prefix=prefix,checkpoint=checkpoint,ref=mref,reads=reads,publisher=Publisher())


@pytest.mark.asyncio
async def test_dataset_metadata_restores_original_objects_and_stages_new_bytes_pending(tmp_path):
    p=prepared(tmp_path);store=p.store
    assert sorted(p.reads)==['base-manifest.json','journal-control.json','source-control.json']
    old=await store.read(p.prefix+'commands/one',max_bytes=100)
    assert old.data==b'original accepted command' and old.token=='native-original-token'
    assert store.remote_durable(p.prefix+'commands/one',old.data)
    assert store.allows_storage_transition('original-s3-authority')
    assert not store.allows_storage_transition('another-authority')
    key=p.prefix+'outcomes/one'
    assert await store.compare_and_swap(key,b'new outcome',expected_token=None,writer_generation='writer')
    new=await store.read(key,max_bytes=100)
    assert new.token.startswith('pending:') and not store.remote_durable(key,new.data)
    assert not await store.compare_and_swap(key,b'overwrite',expected_token=None,writer_generation='writer')
    assert await store.compare_and_swap(key,b'reconciled',expected_token=new.token,writer_generation='writer')
    # An API-only restart retains the locally pending continuation.
    again=DatasetRecoveryStore(p.q,checkpoint=p.checkpoint,manifest_ref=p.ref,publisher=p.publisher,writer_guard=p.guard,stream='stream')
    assert (await again.read(key,max_bytes=100)).data==b'reconciled'
    assert len(p.reads)==3
    p.guard.current=False
    with pytest.raises(PermissionError):
        await store.compare_and_swap(key,b'stale',expected_token=new.token,writer_generation='writer')


@pytest.mark.asyncio
async def test_local_outcome_does_not_become_remote_receipt_until_checkpoint_covers_bytes(env):
    e=env;seal=await accept(e);native=receipt(e.cmd)
    e.store.remote_durable=lambda key,data:False
    with pytest.raises(JournalUnknown,match='outcome_pending_dataset_checkpoint'):
        await e.journal.record_outcome(seal,command=e.cmd,database_generation='db-source',
            outcome=native.outcome,writer_generation='writer-1')
    assert await e.journal.remote_outcome(e.cmd.operation_key) is None
    e.store.remote_durable=lambda key,data:True
    remote=await e.journal.record_outcome(seal,command=e.cmd,database_generation='db-source',
        outcome=native.outcome,writer_generation='writer-1')
    assert remote.command_sha256==sha(e.cmd.encode())
    assert (await e.journal.remote_outcome(e.cmd.operation_key)) is not None


@pytest.mark.asyncio
async def test_dataset_transport_requires_exact_archive_pin(tmp_path):
    p=prepared(tmp_path);pin=p.checkpoint.archive;transport=DatasetSnapshotTransport(p.store)
    blob=BlobRef('dataset-blobs/operation/'+pin.name,pin.size,pin.sha256,'native-etag')
    await transport.verify(blob)
    assert not transport.matches(BlobRef(blob.key,blob.size+1,blob.sha256,blob.native_version))


@pytest.mark.asyncio
@pytest.mark.parametrize('producer',['export_control','legacy_seed'])
async def test_native_verified_checkpoint_promotes_only_exact_captured_bytes_and_lost_ack_is_idempotent(tmp_path,producer):
    from dataclasses import replace
    from regional_knowledge.recovery_journal import JournalConflict
    p=prepared(tmp_path);store=p.store;key=p.prefix+'outcomes/new'
    assert await store.compare_and_swap(key,b'captured',expected_token=None,writer_generation='writer')
    captured=await store.read(key,max_bytes=100)
    with p.q.connect() as db:
        db.executemany('INSERT INTO rkb_dataset_recovery_objects VALUES(?,?,?,?,1)',
            [(store.storage_identity,p.prefix+'commands/covered-'+str(i),b'x'*16384,'token-'+str(i)) for i in range(128)])
    escaped=p.prefix+'commands/escaped-},{"data_b64":\\'
    assert await store.put_if_absent(escaped,b'pending escaped key')
    if producer=='export_control':
        journal=store.export_control()
    else:
        from regional_knowledge.kaggle_dataset_seed import SourceDatasetSeed
        class LegacyStore:
            bucket='fixture';prefix=p.prefix
            def __init__(self):self.client=self
            def list_objects_v2(self,**kwargs):
                with p.q.connect(write=False) as db:
                    rows=db.execute('SELECT key,length(data) size FROM rkb_dataset_recovery_objects WHERE storage=? ORDER BY key',
                        (store.storage_identity,)).fetchall()
                return dict(Contents=[dict(Key=r['key'],Size=r['size']) for r in rows],IsTruncated=False)
            async def read(self,key,**kwargs):return await store.read(key,**kwargs)
        seed=SourceDatasetSeed.__new__(SourceDatasetSeed)
        seed.startup=NS(config=NS(spool=NS(journal=NS(store=LegacyStore()))))
        journal=await seed._journal_control()
    control=encode(dict(codec='rkb.source-base-control.v1',database_generation='db',sequence=8))
    manifest=encode(dict(database_generation='db',applied_sequence=8,
        blob=dict(size=p.checkpoint.archive.size,sha256=p.checkpoint.archive.sha256),
        source_control=dict(key=p.prefix+'source-base-control/'+sha(control),sha256=sha(control),length=len(control))))
    ref=ObjectRef(p.prefix+'snapshots/manifests/'+sha(manifest),sha(manifest),len(manifest),8)
    checkpoint=replace(p.checkpoint,dataset_version=2,base_sequence=8,
        manifest=DatasetFile('base-manifest.json',len(manifest),sha(manifest)),
        control=DatasetFile('source-control.json',len(control),sha(control)),
        journal=DatasetFile('journal-control.json',len(journal),sha(journal)))
    original=p.publisher.read_control
    class Published:
        ref=p.publisher.ref
        def read_control(self,file,*,version):
            assert version==2
            return memoryview({'journal-control.json':journal,'base-manifest.json':manifest,'source-control.json':control}[file.name]).tobytes()
    store.publisher=Published()
    expected=dict(dataset_ref=Published.ref,dataset_version=2,base_sequence=8,
        archive_sha256=checkpoint.archive.sha256,archive_size=checkpoint.archive.size)
    with pytest.raises(JournalConflict,match='native_snapshot_readback_changed'):
        store.promote(checkpoint,dict(expected,archive_sha256='0'*64),manifest_ref=ref)
    assert not store.remote_durable(key,b'captured') and store.latest_checkpoint.dataset_version==1
    valid_journal=journal
    for invalid_item in (None,dict(key=p.prefix+'commands/one',token='native-original-token',data_b64='not base64!')):
        document=json.loads(valid_journal);document['objects'][0]=invalid_item
        journal=encode(document)
        broken=replace(checkpoint,journal=DatasetFile('journal-control.json',len(journal),sha(journal)))
        with pytest.raises((TypeError,ValueError)):
            store.promote(broken,expected,manifest_ref=ref)
        assert store.latest_checkpoint.dataset_version==1
    journal=valid_journal
    with p.q.connect() as db:
        db.execute('CREATE TABLE redundant_coverage_writes(key TEXT)')
        db.execute('''CREATE TRIGGER record_covered_rewrite AFTER UPDATE ON rkb_dataset_recovery_objects
            WHEN OLD.covered_version IS NOT NULL BEGIN
            INSERT INTO redundant_coverage_writes VALUES(OLD.key); END''')
    import tracemalloc
    tracemalloc.start()
    try:
        store.promote(checkpoint,expected,manifest_ref=ref)
        peak=tracemalloc.get_traced_memory()[1]
    finally:tracemalloc.stop()
    assert peak<len(journal)*1.5
    assert store.remote_durable(escaped,b'pending escaped key')
    assert store.remote_durable(key,b'captured') and store.latest_checkpoint==checkpoint
    with p.q.connect(write=False) as db:
        assert db.execute('SELECT count(*) FROM redundant_coverage_writes').fetchone()[0]==0
    assert store.remote_durable(p.prefix+'commands/one',b'original accepted command')
    assert await store.compare_and_swap(key,b'after checkpoint',expected_token=captured.token,writer_generation='writer')
    store.promote(checkpoint,expected,manifest_ref=ref)  # Lost promotion ACK.
    assert not store.remote_durable(key,b'after checkpoint')
    assert not store.remote_durable(key,b'captured')


@pytest.mark.asyncio
async def test_control_inventory_revision_is_exact_and_excludes_other_namespaces(tmp_path):
    p=prepared(tmp_path);store=p.store
    assert store.pending_control() is None
    assert await store.put_if_absent(p.prefix+'outcomes/one',b'pending outcome')
    assert store.pending_outcomes() and store.pending_control() is None
    key=p.prefix+'control/expert-requests/one/result'
    assert await store.compare_and_swap(key,b'exact bytes',expected_token=None,writer_generation='writer')
    first=store.pending_control();obj=await store.read(key,max_bytes=100)
    assert first==store.pending_control()
    assert not await store.compare_and_swap(key,b'no CAS',expected_token=None,writer_generation='writer')
    assert first==store.pending_control()
    assert await store.compare_and_swap(key,obj.data,expected_token=obj.token,writer_generation='writer')
    assert first!=store.pending_control()  # Late same-byte write has a new CAS identity.


@pytest.mark.asyncio
async def test_export_is_canonical_and_bounded_across_pages(tmp_path,monkeypatch):
    import regional_knowledge.dataset_recovery_store as module
    p=prepared(tmp_path);store=p.store
    monkeypatch.setattr(module,'CONTROL_PAGE_SIZE',2)
    for n in range(5):
        assert await store.put_if_absent(p.prefix+'control/expert-requests/'+str(n)+'/result',b'bytes')
    assert await store.put_if_absent(p.prefix+'snapshots/private',b'not exported')
    raw=store.export_control();value=json.loads(raw)
    assert raw==encode(value)
    assert not any('/snapshots/' in item['key'] for item in value['objects'])
    assert len([item for item in value['objects'] if '/control/' in item['key']])==5
    assert store.pending_control() is not None


@pytest.mark.asyncio
async def test_promotion_fences_writer_retirement_and_competing_pointer_after_readback(tmp_path):
    from dataclasses import replace
    from regional_knowledge.recovery_journal import JournalConflict
    p=prepared(tmp_path);store=p.store
    key=p.prefix+'control/expert-requests/one/result'
    assert await store.compare_and_swap(key,b'pending',expected_token=None,writer_generation='writer')
    journal=store.export_control()
    checkpoint=replace(p.checkpoint,dataset_version=2,
        journal=DatasetFile('journal-control.json',len(journal),sha(journal)))
    original=p.publisher;state={'current':True,'compete':False}
    class Publisher:
        ref=original.ref
        def read_control(self,file,*,version):
            assert version==2
            if file.name=='journal-control.json':return journal
            result=original.read_control(file,version=1)
            if file.name=='source-control.json':
                state['current']=False
                if state['compete']:
                    with p.q.connect() as db:
                        db.execute('UPDATE rkb_dataset_recovery_store SET latest_json=? WHERE storage=?',
                            (json.dumps(replace(checkpoint,dataset_version=3).wire()),store.storage_identity))
            return result
    store.publisher=Publisher()
    native=dict(dataset_ref=Publisher.ref,dataset_version=2,base_sequence=7,
        archive_sha256=checkpoint.archive.sha256,archive_size=checkpoint.archive.size)
    with pytest.raises(JournalConflict,match='writer_changed'):
        store.promote(checkpoint,native,manifest_ref=p.ref,current=lambda:state['current'])
    assert store.latest_checkpoint.dataset_version==1 and not store.remote_durable(key,b'pending')
    state.update(current=True,compete=True)
    with pytest.raises(JournalConflict,match='promotion_predecessor'):
        store.promote(checkpoint,native,manifest_ref=p.ref,current=lambda:state['current'])
    assert store.latest_checkpoint.dataset_version==3 and not store.remote_durable(key,b'pending')


def test_control_journal_over_64_mib_and_10000_objects_restores_byte_for_byte(tmp_path):
    """Pure SQLite/publisher fixture: no Dataset, PG, or remote effects."""
    from dataclasses import replace
    from test_kaggle_dataset_tickets import Queue

    p = prepared(tmp_path)
    data = b'journal payload-' + b'x' * 6000
    with p.q.connect() as db:
        db.executemany(
            'INSERT INTO rkb_dataset_recovery_objects VALUES(?,?,?,?,NULL)',
            ((p.store.storage_identity, p.prefix + f'control/bulk/{n:05d}',
              data + n.to_bytes(2, 'big'), f'original-token-{n}')
             for n in range(10001)),
        )
    exported = p.store.export_control()
    assert len(exported) > 64 * 1024**2
    assert len(json.loads(exported)['objects']) > 10000
    assert p.store.pending_control() is not None

    pin = DatasetFile('journal-control.json', len(exported), sha(exported))
    checkpoint = replace(p.checkpoint, journal=pin)
    assert DatasetCheckpoint.from_wire(checkpoint.wire()) == checkpoint

    class Publisher:
        ref = p.publisher.ref

        def read_control(self, file, *, version):
            assert version == checkpoint.dataset_version
            raw = exported if file.name == 'journal-control.json' else (
                p.publisher.read_control(file, version=version))
            assert (len(raw), sha(raw)) == (file.size, file.sha256)
            return raw

    restored_queue = Queue(tmp_path / 'restored.sqlite')
    with restored_queue.connect() as db:
        db.execute('CREATE TABLE rkb_writer_control(id INTEGER PRIMARY KEY,storage_sha256 TEXT)')
        db.execute("INSERT INTO rkb_writer_control VALUES(1,'original-s3-authority')")
    restored = DatasetRecoveryStore(restored_queue, checkpoint=checkpoint,
        manifest_ref=p.ref, publisher=Publisher(), writer_guard=p.guard, stream='stream')
    assert restored.export_control() == exported
    assert restored.pending_control() is None  # Saved objects are no longer pending.
    with restored_queue.connect(write=False) as db:
        row = db.execute('SELECT data,token,covered_version FROM rkb_dataset_recovery_objects '
            'WHERE storage=? AND key=?',
            (restored.storage_identity, p.prefix + 'control/bulk/10000')).fetchone()
    assert bytes(row['data']) == data + (10000).to_bytes(2, 'big')
    assert row['token'] == 'original-token-10000'
    assert row['covered_version'] == checkpoint.dataset_version
