import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace as NS

import pytest

from regional_knowledge.kaggle_dataset_publisher import KaggleDatasetPublisher
from regional_knowledge.kaggle_dataset_seed import DatasetSeedConfig, SourceDatasetSeed
from regional_knowledge.kaggle_pg_snapshot_native import SnapshotError, encode, sha
from test_kaggle_dataset_publisher import FakeApi
from test_kaggle_dataset_tickets import Queue


@pytest.mark.asyncio
async def test_nonempty_native_checkpoint_publishes_archive_without_dev_proxy(tmp_path, monkeypatch):
    queue = Queue(tmp_path/'queue.sqlite')
    api = FakeApi()
    publisher = KaggleDatasetPublisher(api, provider_ref='owner/rkb-pg', owner='owner')
    versions = dict(server_version_num=170010, pgvector_version='0.8.2', catalog_sha256='a'*64,
        model_sha256='b'*64, tokenizer_sha256='c'*64, schema_revision='schema11',
        function_revision='geo6', executor_revision='executor1')
    config = DatasetSeedConfig('owner/rkb-pg', 'seed-seven', versions)
    binding = NS(run_id='run', runtime_incarnation='inc', writer_generation='writer')
    @asynccontextmanager
    async def guard(generation):yield
    proof = NS(current=lambda:True, pending=(), accepted_sequence=7, applied_sequence=7,
        database_generation='db', head=None)
    control = encode(dict(codec='rkb.source-base-control.v1', sequence=7))
    bridge = NS(snapshot_tickets=None)
    captures = []
    async def capture(binding, **kwargs):
        captures.append(kwargs)
        assert kwargs['expected_sequence'] == 7
        # Native capture uploads the archive. Only its byte receipt returns.
        ticket = await bridge.snapshot_tickets.issue_upload(binding, 'native-capture-seven', size=99999, sha256='d'*64)
        assert ticket.key.endswith('.blob') and not ticket.key.endswith('.tar')
        return dict(database_generation='db', writer_generation='writer', applied_sequence=7,
            versions=versions, blob=dict(key=ticket.key, size=ticket.size, sha256=ticket.sha256, native_version='native-etag'))
    physical = NS(_control=lambda p:control, native=NS(capture=capture))
    async def head():return None
    startup = NS(config=NS(queue=queue, writer_owner=guard, writer_generation='writer',
        spool=NS(_lock=asyncio.Lock(), _source_covered_sequence=7, journal=NS(prefix='recovery/source', stream='source',head=head))),
        binding_current=lambda b:b is binding, history=NS(_coverage=proof),
        restorer=NS(complete=True, physical=physical, dispatch=bridge))
    seed = SourceDatasetSeed(startup, config, publisher)
    async def journal_control():return encode(dict(codec='rkb.dataset-journal.v1',objects=[]))
    seed._journal_control=journal_control
    controls = []
    def put(grant, data):
        assert grant.size < 2048 and grant.name in ('base-manifest.json', 'source-control.json','journal-control.json')
        assert len(data) == grant.size and sha(data) == grant.sha256
        controls.append(grant.name)
    monkeypatch.setattr('regional_knowledge.kaggle_dataset_seed._put_control', put)
    original_create = api.create_dataset
    def create(request):
        import json
        package = json.loads(request.description)
        api.files = [NS(name=f['name'], total_bytes=f['size']) for f in package['files']]
        original_create(request)
    api.create_dataset = create
    assert await seed.capture_once(binding) == 1
    assert await seed.capture_once(binding) == 1
    assert len(captures) == 1 and sorted(controls) == ['base-manifest.json', 'journal-control.json','source-control.json']
    assert api.mutations.count('create') == 1
    with queue.connect(write=False) as db:
        row = db.execute('SELECT * FROM rkb_dataset_seed').fetchone()
        assert row['state'] == 'published' and row['version'] == 1
    # Publication is still only the seed, never a native recovery capability.
    assert not hasattr(seed, 'verify_initialized')


@pytest.mark.asyncio
async def test_pending_prefix_cannot_be_replaced_by_empty_seed(tmp_path):
    queue = Queue(tmp_path/'queue.sqlite')
    @asynccontextmanager
    async def guard(generation):yield
    startup = NS(config=NS(queue=queue, writer_owner=guard, spool=NS(_lock=asyncio.Lock())),
        binding_current=lambda b:True, history=NS(_coverage=NS(current=lambda:True, pending=('pending',))),
        restorer=NS(complete=True, physical=NS(), dispatch=NS()))
    config = NS(operation_key='nonempty', provider_ref='owner/rkb-pg')
    seed = SourceDatasetSeed(startup, config, NS())
    with pytest.raises(SnapshotError, match='drained_prefix'):
        await seed.capture_once(NS())


@pytest.mark.asyncio
async def test_existing_geo_journal_window_excludes_archive_and_fits_control_budget():
    from regional_knowledge.recovery_journal import StoredObject
    import json
    read=[]
    class Store:
        bucket='fixture';prefix='recovery/fixed/'
        def __init__(self):self.client=self
        def list_objects_v2(self,**kwargs):
            return dict(Contents=[dict(Key=self.prefix+'commands/'+str(i),Size=90000) for i in range(43)]+
                [dict(Key=self.prefix+'snapshots/blobs/pg',Size=8*1024**3)],IsTruncated=False)
        async def read(self,key,**kwargs):
            assert '/snapshots/' not in key
            read.append(key)
            return StoredObject(b'x'*90000,'native-provider-version')
    seed=SourceDatasetSeed.__new__(SourceDatasetSeed)
    seed.startup=NS(config=NS(spool=NS(journal=NS(store=Store()))))
    raw=await seed._journal_control()
    assert 4*1024**2<len(raw)<16*1024**2
    assert len(json.loads(raw)['objects'])==len(read)==43
