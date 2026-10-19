"""Atomic cross-producer reserve and one query-first E5/tokenizer worker."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
import threading

import pytest

from regional_knowledge.bge_queue import BgeQueue,BgeQueueCapacityError
from regional_knowledge.e5_service import SerialEncoder,QueueFull,LocalService
from regional_knowledge.e5_contract import SPACE
from test_expert_requests import queue
from test_expert_executor import sources


def test_all_bge_document_producers_share_atomic_query_reserve(tmp_path):
    queue=BgeQueue(tmp_path/'queue.sqlite')
    for i in range(254):queue.enqueue('source-indexer',str(i),['Synthetic passage'],kind='document')
    def document(i):
        try:return queue.enqueue('producer-'+str(i),'document-key',['Synthetic other passage'],kind='document')
        except BgeQueueCapacityError:return None
    with ThreadPoolExecutor(max_workers=8) as pool:accepted=list(pool.map(document,range(8)))
    assert sum(v is not None for v in accepted)==1
    query=queue.enqueue('searcher','interactive-query',['Synthetic query'])
    assert queue.enqueue('searcher','interactive-query',['Synthetic query'])==query
    # A reserved slot doesn't remove the original bounded computational cap.
    with pytest.raises(BgeQueueCapacityError):queue.enqueue('other-searcher','overflow',['Synthetic query'])
    with queue.connect() as db:
        assert db.execute("SELECT count(*) FROM jobs WHERE state!='done'").fetchone()[0]==256
        assert db.execute('SELECT count(*) FROM runs').fetchone()[0]==1
    # Existing document retries remain exact even when new documents must wait.
    assert queue.enqueue('source-indexer','0',['Synthetic passage'],kind='document')


@pytest.mark.asyncio
async def test_story_capacity_wait_preserves_its_durable_outbox(tmp_path):
    from test_story_vector_index import setup,seed
    from regional_knowledge.story_vector_index import StoryVectorIndexer,pending
    registry,owner,_=setup(tmp_path)
    ident=seed(registry,owner,'Synthetic capacity story','capacity-story')
    class Full:
        def enqueue(self,*args,**kwargs):raise BgeQueueCapacityError('capacity_wait')
    indexer=StoryVectorIndexer(registry.corpus,Full())
    original=pending(registry.corpus)
    assert original['story_id']==ident
    assert await indexer.tick()==0
    assert pending(registry.corpus)==original


@pytest.mark.asyncio
async def test_query_priority_token_counts_serialization_and_reserved_queue_slot():
    gate=threading.Event();started=threading.Event();calls=[];active=0;peak=0
    def enter(name):
        nonlocal active,peak
        active+=1;peak=max(peak,active);calls.append(name)
        if name=='passage-active':started.set();gate.wait(2)
    def encode(texts,role):
        nonlocal active
        enter(texts[0]);active-=1
        return [[1]+[0]*383 for _ in texts]
    def count(texts,role):
        nonlocal active
        enter('token-count');active-=1
        return {'e5_tokens':[10],'bge_tokens':[11]}
    queue=SerialEncoder(encode,capacity=4)
    LocalService(queue,token_counts=count,token_budget_ready=True)
    first=asyncio.create_task(queue.submit(['passage-active'],'passage'))
    assert await asyncio.to_thread(started.wait,1)
    passage=asyncio.create_task(queue.submit(['passage-next'],'passage'))
    counter=asyncio.create_task(queue.submit(['counts'],'passage',operation='token_count'))
    query=asyncio.create_task(queue.submit(['query-first'],'query'))
    await asyncio.sleep(0)
    with pytest.raises(QueueFull):await queue.submit(['excess-passage'],'passage')
    next_query=asyncio.create_task(queue.submit(['query-second'],'query'))
    await asyncio.sleep(0)
    gate.set()
    results=await asyncio.gather(first,passage,counter,query,next_query)
    assert calls==['passage-active','query-first','query-second','passage-next','token-count']
    assert peak==1 and queue.max_queue==4 and results[2]['bge_tokens']==[11]
    await queue.close()


@pytest.mark.asyncio
async def test_cancelled_search_does_not_consume_later_encoder_work():
    gate=threading.Event();started=threading.Event();calls=[]
    def encode(texts,role):
        calls.append(texts[0]);started.set();gate.wait(2)
        return [[1]+[0]*383]
    queue=SerialEncoder(encode)
    active=asyncio.create_task(queue.submit(['active'],'query'))
    assert await asyncio.to_thread(started.wait,1)
    abandoned=asyncio.create_task(queue.submit(['abandoned'],'query'))
    await asyncio.sleep(0);abandoned.cancel()
    await asyncio.gather(abandoned,return_exceptions=True)
    gate.set();await active;await queue.queue.join()
    assert calls==['active'] and queue.completed==1
    await queue.close()


@pytest.mark.asyncio
async def test_token_count_http_uses_the_same_bounded_worker():
    import httpx
    calls=[]
    def count(texts,role):calls.append((texts,role));return {'e5_tokens':[2],'bge_tokens':[3]}
    queue=SerialEncoder(lambda texts,role:[[1]+[0]*383])
    service=LocalService(queue,token_counts=count,token_budget_ready=True)
    server=await asyncio.start_server(service.handle,'127.0.0.1',0)
    port=server.sockets[0].getsockname()[1]
    try:
        async with httpx.AsyncClient(trust_env=False) as client:
            response=await client.post('http://127.0.0.1:'+str(port)+'/token-count',json={'space':SPACE,'role':'passage','texts':['Synthetic tokens']})
            assert response.status_code==200
            assert response.json()['bge_tokens']==[3]
        assert calls==[(['Synthetic tokens'],'passage')] and queue.completed==1
    finally:
        server.close();await server.wait_closed();await queue.close()


@pytest.mark.asyncio
async def test_e5_capacity_classification_preserves_authorized_evidence(sources):
    import httpx
    from regional_knowledge.local_e5 import LocalE5Embedder,E5QueueCapacityError
    s=sources;encoder=LocalE5Embedder()
    await encoder.client.aclose()
    async def pressure(request):
        if request.url.path=='/health':return httpx.Response(200,json={'ready':True,'space':SPACE})
        return httpx.Response(429,json={'error':'encoder_queue_full'})
    encoder.client=httpx.AsyncClient(transport=httpx.MockTransport(pressure),base_url='http://127.0.0.1:8767')
    s.backend.embedder=encoder
    for call in (encoder.embed('query'),encoder.embed_passages(['passage']),encoder.passage_token_counts(['passage'])):
        with pytest.raises(E5QueueCapacityError):await call
    found=await s.backend.search_evidence('Synthetic atlas',s.q.actors[1])
    assert found.wait_reason=='capacity_wait' and found.main_state!='unavailable'
    text=' '.join(item.text for item in found.evidence)
    assert 'PUBLIC_SENTINEL' in text and 'PRIVATE_SENTINEL' not in text
    assert (await encoder.status())['ready']
    await s.backend.aclose()


@pytest.mark.asyncio
async def test_arbitrary_provider_429_does_not_invent_e5_capacity():
    import httpx
    from regional_knowledge.local_e5 import LocalE5Embedder
    encoder=LocalE5Embedder();await encoder.client.aclose()
    encoder.client=httpx.AsyncClient(transport=httpx.MockTransport(lambda request:httpx.Response(429,json={'error':'unknown_failure'})),base_url='http://127.0.0.1:8767')
    try:
        with pytest.raises(httpx.HTTPStatusError):await encoder.embed('query')
    finally:await encoder.aclose()
