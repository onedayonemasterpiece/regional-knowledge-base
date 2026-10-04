import asyncio
import math
import threading
import httpx
import pytest
from regional_knowledge.e5_contract import SPACE,validate_vector
from regional_knowledge.e5_service import SerialEncoder,QueueFull
from regional_knowledge.local_e5 import LocalE5Embedder
from regional_knowledge.supabase_backend import _embedder_from_env,LexicalOnlyEmbedder


def test_e5_dimension_normalization_and_finiteness():
    assert len(validate_vector([1]+[0]*383))==384
    for values in ([1]*768,[0]*384,[float('nan')]+[0]*383,[2]+[0]*383):
        with pytest.raises(ValueError):validate_vector(values)


def test_fast_tier_never_inherits_shared_keys_or_accepts_external_endpoint(monkeypatch):
    for key in ('RKB_EMBEDDING_ENDPOINT','RKB_EMBEDDING_API_KEY','RKB_EMBEDDING_MODEL','RKB_EMBEDDING_SPACE','RKB_FAST_E5_ENABLED'):monkeypatch.delenv(key,raising=False)
    monkeypatch.setenv('OPENAI_API_KEY','shared-test-key')
    assert isinstance(_embedder_from_env(),LexicalOnlyEmbedder)
    monkeypatch.setenv('RKB_FAST_E5_ENABLED','1')
    assert isinstance(_embedder_from_env(),LocalE5Embedder)
    for endpoint in ('https://127.0.0.1:8767','http://provider.example:8767','http://127.0.0.1:9999','http://user:secret@127.0.0.1:8767'):
        with pytest.raises(ValueError):LocalE5Embedder(endpoint)


@pytest.mark.asyncio
async def test_bounded_queue_single_inference_and_single_worker_start():
    gate=threading.Event();started=threading.Event();active=0;max_active=0
    def encode(texts,role):
        nonlocal active,max_active
        active+=1;max_active=max(max_active,active);started.set();gate.wait(1);active-=1
        return [[1]+[0]*383 for t in texts]
    q=SerialEncoder(encode,capacity=1)
    first=asyncio.create_task(q.submit(['a'],'query'))
    await asyncio.to_thread(started.wait,1)
    worker=q.worker;q.start();assert q.worker is worker
    second=asyncio.create_task(q.submit(['b'],'query'));await asyncio.sleep(.01)
    with pytest.raises(QueueFull):await q.submit(['c'],'query')
    gate.set();a,b=await asyncio.gather(first,second)
    assert max_active==1 and q.max_queue<=1 and q.rejected==1
    assert b['queue_wait_seconds']>0 and a['space']==SPACE
    await q.close()


@pytest.mark.asyncio
async def test_local_client_rejects_wrong_space_and_unavailable_has_safe_status():
    client=LocalE5Embedder()
    async def handler(request):return httpx.Response(200,json={'space':'wrong','vectors':[[1]+[0]*383]})
    await client.client.aclose();client.client=httpx.AsyncClient(transport=httpx.MockTransport(handler),base_url='http://127.0.0.1:8767')
    with pytest.raises(ValueError):await client.embed('query')
    await client.aclose()


def test_exact_query_and_passage_prefix_contract():
    from regional_knowledge.e5_contract import prepare_text
    assert prepare_text('query','город')=='query: город'
    assert prepare_text('passage','город')=='passage: город'
    with pytest.raises(ValueError):prepare_text('other','город')


@pytest.mark.asyncio
async def test_unavailable_fast_encoder_degrades_to_lexical_without_provider(monkeypatch):
    from regional_knowledge.supabase_backend import SupabaseRestBackend,SupabaseConfig
    from regional_knowledge.contracts import Principal
    import json
    calls=[]
    e=LocalE5Embedder()
    async def down(request):raise httpx.ConnectError('local unavailable')
    await e.client.aclose();e.client=httpx.AsyncClient(transport=httpx.MockTransport(down),base_url='http://127.0.0.1:8767')
    async def db(request):
        calls.append((request.url.path,json.loads(request.content)))
        return httpx.Response(200,json=[])
    dbclient=httpx.AsyncClient(transport=httpx.MockTransport(db))
    b=SupabaseRestBackend(SupabaseConfig(url='https://db.example',anon_key='test'),embedder=e,client=dbclient)
    result=await b.search('город',Principal(subject='11111111-1111-1111-1111-111111111111',client_id='test',issuer='test',access_token='test'))
    assert result.retrieval_mode=='lexical_only' and result.mode=='lexical_degraded'
    assert calls[0][0].endswith('/rkb_fast_e5_search') and calls[0][1]['query_embedding'] is None
    status=await e.status();assert not status['ready'] and status['retrieval_mode']=='lexical_only'
    await b.aclose();await dbclient.aclose()


def test_changed_passage_requires_reencoding_whole_batch_and_preserves_other_groups():
    import importlib.util
    from pathlib import Path
    import sys
    folder=Path(__file__).parents[1]/'scripts/production'
    sys.path.insert(0,str(folder))
    try:
        spec=importlib.util.spec_from_file_location('backfill_operator',folder/'backfill_e5.py')
        module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        rows=[dict(id=str(i),document_id='a' if i<5 else 'b',revision=1,text_sha256=str(i),existing_hash=None,existing_batch=None,existing_revision=None) for i in range(7)]
        groups=module.document_batches(rows)
        assert [len(group) for group in groups]==[4,1,2]
        for group in groups:
            fingerprint=module.batch_fingerprint(group)
            for row in group:row.update(existing_hash=row['text_sha256'],existing_batch=fingerprint,existing_revision=1)
        assert all(module.already_current(group,module.batch_fingerprint(group)) for group in groups)
        rows[2]['text_sha256']='changed'
        assert not module.already_current(groups[0],module.batch_fingerprint(groups[0]))
        assert all(module.already_current(group,module.batch_fingerprint(group)) for group in groups[1:])
    finally:sys.path.remove(str(folder))


@pytest.mark.asyncio
async def test_public_status_surface_returns_readiness_without_private_details(monkeypatch):
    from regional_knowledge.server import build_server
    from regional_knowledge.backend import UnavailableBackend
    from starlette.requests import Request
    backend=UnavailableBackend()
    e=LocalE5Embedder();backend.embedder=e
    async def health():return {'configured':True,'ready':True,'retrieval_mode':'fast_e5','space':SPACE,'model_path':'private','queue_depth':0}
    monkeypatch.setattr(e,'status',health)
    monkeypatch.setenv('RKB_DEV_NOAUTH','1')
    server=build_server(backend=backend)
    route=next(route for route in server._custom_starlette_routes if route.path=='/fast-tier/health')
    response=await route.endpoint(Request({'type':'http'}))
    import json
    assert json.loads(response.body)=={'configured':True,'ready':True,'retrieval_mode':'fast_e5'}
    await e.aclose()
