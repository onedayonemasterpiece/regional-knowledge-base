import asyncio
import threading
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
        binding_current=lambda b:b is binding, coverage=proof,
        restorer=NS(complete=True, physical=physical, dispatch=bridge))
    seed = SourceDatasetSeed(startup, config, publisher)
    journal=encode(dict(codec='rkb.dataset-journal.v1',objects=[]))
    async def journal_control():return journal
    seed._journal_control=journal_control
    loop=asyncio.get_running_loop()
    def responsive_hash(data):
        if data==journal:
            responsive=threading.Event()
            loop.call_soon_threadsafe(responsive.set)
            assert responsive.wait(1), 'journal hashing blocked the API event loop'
        return sha(data)
    monkeypatch.setattr('regional_knowledge.kaggle_dataset_seed.sha',responsive_hash)
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
        binding_current=lambda b:True, coverage=NS(current=lambda:True, pending=("pending",)),
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


def test_control_upload_keeps_progress_past_one_socket_timeout(monkeypatch):
    import hashlib
    import http.client
    import io
    from regional_knowledge.kaggle_dataset_seed import _put_control
    data=b'x'*(2*1024*1024+17)
    digest=hashlib.sha256();sizes=[];headers=[]
    class SlowSocket:
        closed=False
        def sendall(self,part):
            if not headers:
                headers.append(bytes(part));return
            # A progressing link takes 20 seconds per MiB. A single 2 MiB
            # sendall exceeds 30 seconds, but bounded writes keep progressing.
            if len(part)/(1024*1024/20)>30:raise TimeoutError('whole-body send deadline')
            digest.update(part);sizes.append(len(part))
        def makefile(self,*args,**kwargs):
            return io.BytesIO(b'HTTP/1.1 201 Created\r\nContent-Length: 0\r\n\r\n')
        def close(self):self.closed=True
    socket=SlowSocket();result=http.client.HTTPSConnection('storage.googleapis.com',timeout=30)
    result.sock=socket
    def connection(host,*,timeout):
        assert host=='storage.googleapis.com' and timeout==30
        return result
    monkeypatch.setattr('regional_knowledge.kaggle_dataset_seed.http.client.HTTPSConnection',connection)
    grant=NS(size=len(data),sha256=sha(data),url='https://storage.googleapis.com/checkpoint')
    _put_control(grant,data)
    assert sum(sizes)==len(data) and digest.hexdigest()==grant.sha256
    assert sum(sizes)/(1024*1024/20)>30 and socket.closed
    assert b'Content-Length: '+str(len(data)).encode() in headers[0]
    assert b'Transfer-Encoding' not in headers[0]


@pytest.mark.asyncio
async def test_seed_large_journal_paginates_beyond_10000_and_uploads_exact_bytes(monkeypatch):
    """All S3 entries and HTTPS transfer are synthetic; only source code runs."""
    import base64
    import hashlib
    import json
    from regional_knowledge.recovery_journal import StoredObject
    from regional_knowledge.kaggle_dataset_seed import _put_control

    count = 10001
    prefix = 'recovery/fixture/'

    def payload(n):
        return b'control object' + b'z' * 6000 + n.to_bytes(2, 'big')

    class Store:
        bucket = 'fixture'
        prefix = 'recovery/fixture/'

        def __init__(self):
            self.client = self
            self.pages = 0

        def list_objects_v2(self, **kwargs):
            self.pages += 1
            assert kwargs['MaxKeys'] == 1000
            start = int(kwargs.get('ContinuationToken', '0'))
            end = min(start + 1000, count)
            result = dict(Contents=[
                dict(Key=prefix + f'commands/{n:05d}', Size=len(payload(n)))
                for n in range(start, end)], IsTruncated=end < count)
            if end < count:
                result['NextContinuationToken'] = str(end)
            return result

        async def read(self, key, *, max_bytes):
            n = int(key.rsplit('/', 1)[-1])
            raw = payload(n)
            assert max_bytes == 1024 * 1024 and len(raw) < max_bytes
            return StoredObject(raw, f'token-{n}')

    store = Store()
    seed = SourceDatasetSeed.__new__(SourceDatasetSeed)
    seed.startup = NS(config=NS(spool=NS(journal=NS(store=store))))
    journal = await seed._journal_control()
    assert store.pages == 11
    assert len(journal) > 64 * 1024**2
    objects = json.loads(journal)['objects']
    assert len(objects) == count
    assert all(base64.b64decode(item['data_b64']) ==
               payload(int(item['key'].rsplit('/', 1)[-1])) for item in objects)

    observed = {}

    class Connection:
        def request(self, method, path, *, body, headers):
            assert method == 'PUT' and headers['Content-Length'] == str(len(journal))
            digest=hashlib.sha256();size=0
            for part in body:
                assert 0<len(part)<=1024*1024
                digest.update(part);size+=len(part)
            observed.update(sha256=digest.hexdigest(),size=size)

        def getresponse(self):
            return NS(status=201)

        def close(self):
            pass

    monkeypatch.setattr('regional_knowledge.kaggle_dataset_seed.http.client.HTTPSConnection',
        lambda *a, **kw: Connection())
    grant = NS(name='journal-control.json', size=len(journal),
        sha256=sha(journal), url='https://storage.googleapis.com/checkpoint')
    _put_control(grant, journal)
    assert observed==dict(sha256=sha(journal),size=len(journal))
