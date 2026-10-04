import hashlib
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID, uuid4

import psycopg
import pytest
from psycopg.types.json import Jsonb

from test_graph_postgres import fixture, graph_db
from regional_knowledge.postgres_backend import PostgresBackend
from regional_knowledge.supabase_backend import LexicalOnlyEmbedder


class _UnusedStore:
    async def download_file(self, *args, **kwargs):
        raise AssertionError("reprocess must prove the verified archive path")


class _OnePageProcessor:
    async def inspect_file(self, path):
        assert Path(path).exists()
        return SimpleNamespace(page_count=1, title=None)


@pytest.mark.asyncio
async def test_book_find_and_archived_reprocess_are_acl_safe_idempotent_and_same_document(
    graph_db,
    tmp_path,
    monkeypatch,
):
    doc, owner, other, _, _, _ = fixture(graph_db)
    archived = b"%PDF-1.7\narchived-gause-control"
    sha = hashlib.sha256(archived).hexdigest()
    source_object = uuid4()

    with psycopg.connect(graph_db, autocommit=True) as db:
        db.execute(
            """
            update rkb_documents
               set title=%s,
                   authors=%s,
                   source_sha256=%s,
                   source_format='pdf',
                   source_filename='gause.pdf',
                   source_archive_status='verified',
                   source_archive_ref='entry_gause_control',
                   page_count=1
             where id=%s
            """,
            (
                "Кёнигсберг. История города",
                Jsonb(["Фриц Гаузе"]),
                sha,
                doc,
            ),
        )
        db.execute(
            """
            insert into rkb_objects(
                id,document_id,kind,object_key,sha256,mime_type,size_bytes,deleted_at
            ) values(%s,%s,'source_pdf','gc/source.pdf',%s,'application/pdf',%s,now())
            """,
            (source_object, doc, sha, len(archived)),
        )

    b = PostgresBackend(
        graph_db,
        embedder=LexicalOnlyEmbedder(),
        object_store=_UnusedStore(),
    )
    b.pdf_processor = _OnePageProcessor()
    monkeypatch.setenv("RKB_WORK_DIR", str(tmp_path))
    archive_reads = []

    async def archived_download(
        backend,
        principal,
        document_id,
        obj,
        path,
        *,
        require_archive=False,
    ):
        assert backend is b
        assert principal.subject == owner.subject
        assert document_id == str(doc)
        assert obj["sha256"] == sha
        assert require_archive is True
        archive_reads.append(document_id)
        Path(path).write_bytes(archived)

    monkeypatch.setattr(
        "regional_knowledge.source_archive.download_source",
        archived_download,
    )

    try:
        found = await b.book_find("Гаузе", owner)
        assert [item.document_id for item in found.results] == [str(doc)]
        assert found.results[0].authors == ["Фриц Гаузе"]
        assert (await b.book_find("Гаузе", other)).results == []

        with psycopg.connect(graph_db, autocommit=True) as db:
            db.execute(
                "insert into rkb_document_grants(document_id,grantee_user_id,role) values(%s,%s,'viewer')",
                (doc, UUID(other.subject)),
            )
        assert (await b.book_find("Гаузе", other)).results[0].document_id == str(doc)
        with pytest.raises(PermissionError, match="book_reprocess_owner_required"):
            await b.book_ingest(
                command="reprocess",
                principal=other,
                file=None,
                ingestion_id=None,
                cursor=None,
                payload=None,
                document_id=str(doc),
            )

        first = await b.book_ingest(
            command="reprocess",
            principal=owner,
            file=None,
            ingestion_id=None,
            cursor=None,
            payload=None,
            document_id=str(doc),
        )
        assert first.document_id == str(doc)
        assert first.state == "staged"
        assert first.next_action == "continue_pages"
        assert first.source_archive_status == "verified"
        assert archive_reads == [str(doc)]

        replay = await b.book_ingest(
            command="reprocess",
            principal=owner,
            file=None,
            ingestion_id=None,
            cursor=None,
            payload=None,
            document_id=str(doc),
        )
        assert replay.ingestion_id == first.ingestion_id
        assert replay.document_id == first.document_id
        assert archive_reads == [str(doc)]

        status = await b.book_ingest(
            command="status",
            principal=owner,
            file=None,
            ingestion_id=first.ingestion_id,
            cursor=None,
            payload=None,
        )
        assert status.next_action == "continue_pages"

        with psycopg.connect(graph_db) as db:
            job = db.execute(
                "select document_id,staged_revision,duplicate_policy,source_object_id from rkb_ingestion_jobs where id=%s",
                (UUID(first.ingestion_id),),
            ).fetchone()
            assert job == (doc, 2, "new_revision", source_object)
            assert db.execute(
                "select count(*) from rkb_documents where owner_user_id=%s",
                (UUID(owner.subject),),
            ).fetchone()[0] == 1

        with psycopg.connect(graph_db, autocommit=True) as db:
            db.execute(
                "update rkb_documents set source_archive_status='pending',source_archive_ref=null where id=%s",
                (doc,),
            )
        with pytest.raises(RuntimeError, match="attach the original PDF/DjVu again"):
            await b.book_ingest(
                command="reprocess",
                principal=owner,
                file=None,
                ingestion_id=None,
                cursor=None,
                payload=None,
                document_id=str(doc),
            )
    finally:
        await b.aclose()


def test_ingestion_next_action_is_model_actionable():
    review = PostgresBackend._ingestion_output(
        {"id": str(uuid4()), "state": "needs_review", "cursor": ""}, "review"
    )
    assert review.next_action == "continue_pages" and review.next_cursor is None
    output = PostgresBackend._ingestion_output(
        {"id": str(uuid4()), "document_id": str(uuid4()), "state": "ready", "cursor": None},
        "ready",
    )
    assert output.next_action == "finalize"
    output = PostgresBackend._ingestion_output(
        {"id": str(uuid4()), "document_id": str(uuid4()), "state": "staged", "cursor": None},
        "complete",
    )
    assert output.next_action == "validate"
    output = PostgresBackend._ingestion_output(
        {"id": str(uuid4()), "document_id": str(uuid4()), "state": "failed", "cursor": None},
        "failed",
    )
    assert output.next_action == "blocker"


async def archived_book(graph_db, tmp_path, monkeypatch):
    from test_stage_service import make_pdf
    from test_robust_import import Objects
    doc, owner, other, _, _, _ = fixture(graph_db)
    data = make_pdf()
    sha = hashlib.sha256(data).hexdigest()
    obj = uuid4()
    with psycopg.connect(graph_db, autocommit=True) as db:
        db.execute("update rkb_documents set title='Owned archive control',authors=%s,source_sha256=%s,source_format='pdf',source_archive_status='verified',source_archive_ref='owned-entry' where id=%s", (Jsonb(['Фриц Гаузе']), sha, doc))
        db.execute("insert into rkb_objects(id,document_id,kind,object_key,sha256,mime_type,size_bytes,deleted_at) values(%s,%s,'source_pdf',%s,%s,'application/pdf',%s,now())", (obj, doc, str(obj), sha, len(data)))
    class ArchiveOnlyStore(Objects):
        async def download_file(self, *args):
            raise AssertionError('archive path required')
    b = PostgresBackend(graph_db, embedder=LexicalOnlyEmbedder(), object_store=ArchiveOnlyStore())
    monkeypatch.setenv('RKB_WORK_DIR', str(tmp_path))
    control = {'data': data, 'reads': 0}
    async def download(backend, actor, document_id, source, path, *, require_archive=False):
        assert actor.subject == owner.subject and document_id == str(doc)
        assert source['sha256'] == sha
        assert require_archive or source['deleted_at']
        control['reads'] += 1
        Path(path).write_bytes(control['data'])
    monkeypatch.setattr('regional_knowledge.source_archive.download_source', download)
    async def reprocess():
        return await b.book_ingest(command='reprocess', principal=owner, file=None,
            ingestion_id=None, cursor=None, payload=None, document_id=str(doc))
    return b, doc, owner, other, control, reprocess


@pytest.mark.asyncio
async def test_reprocess_checks_sha_before_allocating_revision(graph_db, tmp_path, monkeypatch):
    b, doc, owner, _, control, reprocess = await archived_book(graph_db, tmp_path, monkeypatch)
    try:
        control['data'] = b'corrupted archive'
        with pytest.raises(RuntimeError, match='integrity mismatch'):
            await reprocess()
        with psycopg.connect(graph_db) as db:
            assert db.execute('select count(*) from rkb_ingestion_jobs where document_id=%s', (doc,)).fetchone()[0] == 0
            assert db.execute('select active_revision from rkb_documents where id=%s', (doc,)).fetchone()[0] == 1
    finally:
        await b.aclose()


@pytest.mark.asyncio
async def test_reprocess_lost_rpc_response_resumes_same_revision(graph_db, tmp_path, monkeypatch):
    import httpx
    b, doc, owner, _, control, reprocess = await archived_book(graph_db, tmp_path, monkeypatch)
    post = b.client.post
    lost = False
    async def lose_once(url, **kwargs):
        nonlocal lost
        result = await post(url, **kwargs)
        if url.endswith('/rpc/rkb_start_ingestion') and not lost:
            lost = True
            raise httpx.ReadError('lost committed response')
        return result
    monkeypatch.setattr(b.client, 'post', lose_once)
    try:
        with pytest.raises(httpx.ReadError):
            await reprocess()
        resumed = await reprocess()
        replay = await reprocess()
        assert resumed.ingestion_id == replay.ingestion_id and resumed.document_id == str(doc)
        with psycopg.connect(graph_db) as db:
            assert db.execute('select staged_revision,state from rkb_ingestion_jobs where document_id=%s', (doc,)).fetchall() == [(2, 'staged')]
            assert db.execute('select active_revision from rkb_documents where id=%s', (doc,)).fetchone()[0] == 1
        assert control['reads'] == 2
    finally:
        await b.aclose()


@pytest.mark.asyncio
async def test_reprocess_after_real_activation_can_start_following_revision(graph_db, tmp_path, monkeypatch):
    from uuid import uuid5
    from regional_knowledge.stage_service import finalize_ingestion
    b, doc, owner, _, control, reprocess = await archived_book(graph_db, tmp_path, monkeypatch)
    try:
        first = await reprocess()
        page_id = str(uuid5(doc, 'page:2:0'))
        payload = {'pages': [{'page_id': page_id, 'physical_page_index': 0,
            'source_material': 'visual_reviewed', 'source_review_note': 'Owned synthetic fixture reviewed',
            'regions': [{'region_key': 'body', 'kind': 'body', 'reading_order': 0,
                'bbox': {'left': 0, 'top': 0, 'right': 1000, 'bottom': 1000},
                'source_text': 'Owned synthetic control'}]}],
            'chunks': [{'chunk_key': 'body', 'title': 'Control',
                'region_refs': [{'page_id': page_id, 'region_key': 'body'}]}]}
        async def call(command, payload=None):
            return await b.book_ingest(command=command, principal=owner, file=None,
                ingestion_id=first.ingestion_id, cursor=None, payload=payload)
        assert (await call('stage', payload)).next_action == 'validate'
        assert (await call('status')).next_action == 'validate'
        assert (await call('validate')).next_action == 'finalize'
        finalized = await finalize_ingestion(b, principal=owner, ingestion_id=first.ingestion_id)
        assert finalized.state == 'finalized' and finalized.next_action == 'done'
        following = await reprocess()
        assert following.ingestion_id != first.ingestion_id and following.document_id == str(doc)
        with psycopg.connect(graph_db) as db:
            assert db.execute('select staged_revision from rkb_ingestion_jobs where id=%s', (UUID(following.ingestion_id),)).fetchone()[0] == 3
            assert db.execute('select active_revision from rkb_documents where id=%s', (doc,)).fetchone()[0] == 2
            assert db.execute('select count(*) from rkb_documents where owner_user_id=%s', (UUID(owner.subject),)).fetchone()[0] == 1
    finally:
        await b.aclose()


@pytest.mark.asyncio
async def test_rest_catalog_pages_and_deterministic_ranking(monkeypatch):
    import httpx
    from regional_knowledge.contracts import Principal
    from regional_knowledge.supabase_backend import SupabaseConfig, SupabaseRestBackend
    actor = Principal(subject=str(uuid4()), client_id='test', issuer='test', access_token='actor-token')
    rows = [{'id': str(uuid4()), 'title': f'Unrelated {n:03}', 'authors': []} for n in range(512)]
    matched = [
        {'id': '00000000-0000-0000-0000-000000000004', 'title': 'Other', 'authors': ['Гаузе']},
        {'id': '00000000-0000-0000-0000-000000000003', 'title': 'История Гаузе'},
        {'id': '00000000-0000-0000-0000-000000000002', 'title': 'Гаузе. История'},
        {'id': '00000000-0000-0000-0000-000000000001', 'title': 'Гаузе'},
    ]
    calls = []
    async def handler(request):
        assert request.headers['authorization'] == 'Bearer actor-token'
        assert request.url.params['order'] == 'title.asc,id.asc'
        offset = int(request.url.params['offset']); calls.append(offset)
        return httpx.Response(200, json=(rows + matched)[offset:offset+512])
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    b = SupabaseRestBackend(SupabaseConfig(url='https://test.example', anon_key='public'), client=client)
    try:
        output = await b.book_find(' ГАУЗЕ ', actor)
        assert [r.document_id for r in output.results] == [row['id'] for row in reversed(matched)]
        assert calls == [0, 512]
        assert len((await b.book_find('Гаузе', actor, limit=2)).results) == 2
        assert not (await b.book_find(' ', actor)).results
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_postgres_catalog_ranking_is_literal_bounded_and_acl_filtered(graph_db):
    doc, owner, other, _, _, _ = fixture(graph_db)
    ids = [uuid4() for _ in range(12)]
    titles = ['Гаузе', 'Гаузе. История', 'История Гаузе', 'Other'] + ['Гаузе. История'] * 8
    with psycopg.connect(graph_db, autocommit=True) as db:
        for index, (id, title) in enumerate(zip(ids, titles)):
            db.execute('insert into rkb_documents(id,owner_user_id,title,authors,source_sha256) values(%s,%s,%s,%s,%s)',
                (id, UUID(owner.subject), title, Jsonb(['Гаузе']), hashlib.sha256(str(index).encode()).hexdigest()))
    b = PostgresBackend(graph_db, embedder=LexicalOnlyEmbedder(), object_store=_UnusedStore())
    try:
        ranked = await b.book_find('ГАУЗЕ', owner, limit=100)
        assert len(ranked.results) == 8 and ranked.results[0].document_id == str(ids[0])
        tied = sorted([ids[1], *ids[4:]], key=str)
        assert [r.document_id for r in ranked.results[1:]] == [str(id) for id in tied[:7]]
        assert ranked == await b.book_find('ГАУЗЕ', owner, limit=100)
        assert not (await b.book_find('Гаузе', other)).results
        assert not (await b.book_find('%', owner)).results
        assert len((await b.book_find('Гаузе', owner, limit=1)).results) == 1
    finally:
        await b.aclose()


@pytest.mark.asyncio
async def test_rest_reprocess_preserves_original_identity_and_replay(tmp_path, monkeypatch):
    import httpx
    from regional_knowledge.contracts import Principal
    from regional_knowledge.supabase_backend import SupabaseConfig, SupabaseRestBackend
    owner = Principal(subject=str(uuid4()), client_id='test', issuer='test', access_token='actor-token')
    doc = {'id': str(uuid4()), 'owner_user_id': owner.subject, 'title': 'Archive control',
        'authors': ['Гаузе'], 'page_count': 1, 'active_revision': 1,
        'source_sha256': hashlib.sha256(b'owned source').hexdigest(),
        'source_format': 'pdf', 'source_archive_status': 'verified', 'source_archive_ref': 'entry'}
    obj = {'id': str(uuid4()), 'document_id': doc['id'], 'sha256': doc['source_sha256'], 'kind': 'source_pdf', 'deleted_at': '2026-10-04'}
    jobs = []
    async def handler(request):
        if request.url.path.endswith('rkb_documents'):
            assert request.headers['authorization'] == 'Bearer actor-token'
            return httpx.Response(200, json=[doc])
        if request.url.path.endswith('rkb_objects'):
            assert request.headers['authorization'] == 'Bearer service-token'
            return httpx.Response(200, json=[obj])
        if request.url.path.endswith('/rpc/rkb_start_ingestion'):
            import json
            payload = json.loads(request.content)
            assert payload['p_document_id'] == doc['id'] and payload['p_duplicate_policy'] == 'new_revision'
            job = {'id': payload['p_ingestion_id'], 'document_id': doc['id'], 'source_file_id': payload['p_source_file_id'], 'source_sha256': doc['source_sha256'], 'state': 'processing', 'cursor': '0'}
            jobs.append(job)
            return httpx.Response(200, json=[{'ingestion_id': job['id'], 'document_id': doc['id']}])
        if request.url.path.endswith('rkb_ingestion_jobs'):
            id = request.url.params.get('id', '').removeprefix('eq.')
            file_id = request.url.params.get('source_file_id', '').removeprefix('eq.')
            matched = [j for j in jobs if j['id'] == id or j['source_file_id'] == file_id]
            if request.method == 'PATCH':
                import json
                matched[0].update(json.loads(request.content))
            return httpx.Response(200, json=matched)
        raise AssertionError(request.url.path)
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    b = SupabaseRestBackend(SupabaseConfig(url='https://test.example', anon_key='public', service_role_key='service-token'), client=client, object_store=_UnusedStore())
    b.pdf_processor = _OnePageProcessor()
    monkeypatch.setenv('RKB_WORK_DIR', str(tmp_path))
    reads = []
    async def download(backend, actor, document_id, obj, path, *, require_archive=False):
        assert require_archive and actor.subject == owner.subject
        reads.append(document_id); Path(path).write_bytes(b'owned source')
    monkeypatch.setattr('regional_knowledge.source_archive.download_source', download)
    async def reprocess():
        return await b.book_ingest(command='reprocess', principal=owner, file=None, ingestion_id=None, cursor=None, payload=None, document_id=doc['id'])
    try:
        first = await reprocess(); replay = await reprocess()
        assert first.ingestion_id == replay.ingestion_id and first.document_id == doc['id']
        assert first.next_action == 'continue_pages' and len(jobs) == len(reads) == 1
        doc['source_archive_status'] = 'pending'
        with pytest.raises(RuntimeError, match='attach the original PDF/DjVu again'):
            await reprocess()
        assert len(jobs) == 1
    finally:
        await client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize('corrupt', [False, True])
async def test_require_archive_bypasses_even_available_local_source(tmp_path, monkeypatch, corrupt):
    import httpx
    from regional_knowledge.source_archive import download_source
    from regional_knowledge.contracts import Principal
    data = b'owned archive bytes'
    owner = Principal(subject=str(uuid4()), client_id='test', issuer='test', access_token='actor')
    class Store:
        async def download_file(self, *args):
            raise AssertionError('local source must not satisfy require_archive')
    async def handler(request):
        return httpx.Response(200, json=[{'owner_user_id': owner.subject,
            'source_archive_status': 'verified', 'source_archive_ref': 'entry'}])
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    backend = SimpleNamespace(client=client, config=SimpleNamespace(url='https://test'),
        _headers=lambda p: {}, object_store=Store())
    class Vibe:
        def __init__(self, path): self.owner = owner.subject
        async def close(self): pass
    async def read(vibe, ref):
        assert ref == 'entry'
        return b'corrupt' if corrupt else data
    monkeypatch.setenv('RKB_VIBEPUBLISH_GRANT_FILE', 'not-a-credential')
    monkeypatch.setattr('regional_knowledge.source_archive.VibePublishClient', Vibe)
    monkeypatch.setattr('regional_knowledge.source_archive.archive_bytes', read)
    target = tmp_path/'archive.pdf'
    try:
        if corrupt:
            with pytest.raises(ValueError, match='archived_source_integrity_mismatch'):
                await download_source(backend, owner, str(uuid4()),
                    {'object_key': 'available', 'sha256': hashlib.sha256(data).hexdigest()}, target, require_archive=True)
            assert not target.exists()
        else:
            await download_source(backend, owner, str(uuid4()),
                {'object_key': 'available', 'sha256': hashlib.sha256(data).hexdigest()}, target, require_archive=True)
            assert target.read_bytes() == data
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_status_guides_interrupted_finalization_resume_or_wait(graph_db, tmp_path, monkeypatch):
    import asyncio
    b, doc, owner, _, control, reprocess = await archived_book(graph_db, tmp_path, monkeypatch)
    try:
        start = await reprocess()
        with psycopg.connect(graph_db, autocommit=True) as db:
            db.execute("update rkb_ingestion_jobs set state='processing',cursor='finalize' where id=%s", (UUID(start.ingestion_id),))
        async def status():
            return await b.book_ingest(command='status', principal=owner, file=None,
                ingestion_id=start.ingestion_id, cursor=None, payload=None)
        assert (await status()).next_action == 'resume_finalize'
        pending = asyncio.get_running_loop().create_future()
        b._finalize_tasks[start.ingestion_id] = pending
        assert (await status()).next_action == 'wait'
        pending.cancel()
        assert (await status()).next_action == 'resume_finalize'
    finally:
        await b.aclose()


@pytest.mark.asyncio
async def test_reprocess_explicitly_selected_historical_duplicate_keeps_roots_separate(graph_db, tmp_path, monkeypatch):
    b, doc, owner, _, control, reprocess = await archived_book(graph_db, tmp_path, monkeypatch)
    sibling = uuid4()
    try:
        with psycopg.connect(graph_db, autocommit=True) as db:
            migration = (Path(__file__).parents[1] / 'sql/018_selected_source_revision.sql').read_text()
            db.execute(migration)
            db.execute(migration)
            db.execute('update rkb_documents set source_identity_primary=false where id=%s', (doc,))
            db.execute("insert into rkb_documents(id,owner_user_id,title,source_sha256,page_count,source_identity_primary,active_revision) select %s,owner_user_id,'Historical duplicate control',source_sha256,page_count,false,1 from rkb_documents where id=%s", (sibling, doc))
        result = await reprocess()
        assert result.document_id == str(doc)
        with pytest.raises(psycopg.errors.RaiseException, match='historical_source_identity_ambiguous'):
            await b.client.post(b.config.url+'/rest/v1/rpc/rkb_start_ingestion',
                headers=b._headers(owner), json={'p_ingestion_id': str(uuid4()),
                'p_document_id': str(uuid4()), 'p_title': 'Unselected attachment',
                'p_source_sha256': hashlib.sha256(control['data']).hexdigest(),
                'p_source_file_id': 'unselected', 'p_page_count': 1,
                'p_duplicate_policy': 'new_revision'})
        with psycopg.connect(graph_db) as db:
            assert db.execute('select document_id,staged_revision from rkb_ingestion_jobs where id=%s', (UUID(result.ingestion_id),)).fetchone() == (doc, 2)
            assert db.execute('select active_revision from rkb_documents where id=%s', (sibling,)).fetchone()[0] == 1
            assert db.execute('select count(*) from rkb_documents where owner_user_id=%s', (UUID(owner.subject),)).fetchone()[0] == 2
    finally:
        await b.aclose()
