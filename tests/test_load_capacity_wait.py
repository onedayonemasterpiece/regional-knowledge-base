"""Actual bounded BGE queue pressure is waiting, not a provider outage."""
import pytest

from regional_knowledge.bge_queue import BgeQueue, BgeQueueCapacityError
from regional_knowledge.contracts import Principal, SearchOutput, SearchResult
from regional_knowledge.multilingual_retrieval import main_search
from test_expert_requests import queue
from test_expert_executor import sources


def full_queue(path):
    queue=BgeQueue(path)
    first=None
    for index in range(256):
        ident=queue.enqueue('synthetic-indexer','document-'+str(index),['Synthetic accepted text'],kind='document')
        first=first or ident
    return queue,first


@pytest.mark.asyncio
async def test_capacity_wait_retains_fast_evidence_without_false_unavailable(tmp_path,monkeypatch):
    path=tmp_path/'bge.sqlite';queue,first=full_queue(path)
    monkeypatch.setenv('RKB_BGE_QUEUE_PATH',str(path))
    with pytest.raises(BgeQueueCapacityError):queue.enqueue('expert','overflow',['Synthetic query'])
    # Exact replays remain available even when the computational queue is full.
    assert queue.enqueue('synthetic-indexer','document-0',['Synthetic accepted text'],kind='document')==first
    scopes=[]
    class Backend:
        async def search(self,query,principal,**kwargs):
            assert kwargs['_fast_only'] is True
            scopes.append(kwargs['document_ids'])
            return SearchOutput(results=[SearchResult(id='safe',title='Synthetic evidence',url='knowledge://chunk/safe')],retrieval_mode='fast_e5')
    actor=Principal(subject='expert',client_id='test',issuer='test',access_token='synthetic')
    found=await main_search(Backend(),'Synthetic query',actor,document_ids=['scoped-document'])
    assert found.main_state=='pending' and found.wait_reason=='capacity_wait' and found.main_job_id is None
    assert [r.id for r in found.results]==['safe'] and scopes==[['scoped-document']]
    with queue.connect() as db:
        assert db.execute('SELECT count(*) FROM jobs').fetchone()[0]==256
        assert db.execute('SELECT count(*) FROM runs').fetchone()[0]==1


@pytest.mark.asyncio
async def test_capacity_reason_survives_real_authorized_evidence_hydration(sources,tmp_path,monkeypatch):
    s=sources;path=tmp_path/'bge.sqlite';full_queue(path)
    monkeypatch.setenv('RKB_BGE_ENABLED','1')
    monkeypatch.setenv('RKB_BGE_QUEUE_PATH',str(path))
    result=await s.backend.search_evidence('Synthetic atlas',s.q.actors[1])
    assert result.main_state=='pending' and result.wait_reason=='capacity_wait'
    text=' '.join(e.text for e in result.evidence)
    assert 'PUBLIC_SENTINEL' in text and 'PRIVATE_SENTINEL' not in text
    await s.backend.aclose()
