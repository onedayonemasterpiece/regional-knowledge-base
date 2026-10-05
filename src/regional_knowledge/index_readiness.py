"""Actor-safe active-index readiness. No private titles/text or global counts."""
import asyncio,json,os,time
from pathlib import Path
from uuid import UUID
from .local_e5 import LocalE5Embedder
from .bge_queue import BgeQueue
from .contracts import IndexingStatus

def enabled():return os.getenv('RKB_AUTO_INDEX_ENABLED')=='1'

async def counts(backend,principal,document_id=None):
    if not hasattr(backend,'data_client'):raise RuntimeError('Postgres readiness requires actor bridge')
    target=UUID(str(document_id)) if document_id else None
    async with backend.data_client._connection(backend._headers(principal)) as db:
        if target and not await(await db.execute('select id from rkb_documents where id=%s',(target,))).fetchone():raise LookupError('document not found')
        return await(await db.execute('select * from rkb_index_counts(%s)',(target,))).fetchone()

def maintenance_state():
    try:
        data=json.loads(Path(os.environ['RKB_INDEXING_HEALTH_PATH']).read_text())
        if time.time()-data['updated_at']>120:return 'unavailable'
        return data['state'] if data['state'] in ('running','ready','degraded') else 'unavailable'
    except (KeyError,ValueError,OSError):return 'unavailable'

async def status(backend,principal,document_id=None):
    values=await counts(backend,principal,document_id);n=values['active_chunks'];e=values['e5_ready'];b=values['bge_ready']
    fast=await backend.embedder.status() if isinstance(backend.embedder,LocalE5Embedder) else {'ready':False}
    try:
        q=await asyncio.to_thread(BgeQueue(os.environ['RKB_BGE_QUEUE_PATH']).status)
        worker=q['state']
    except (KeyError,OSError,RuntimeError):worker='unavailable'
    owner=maintenance_state() if enabled() else 'disabled'
    warm_mode=os.getenv('RKB_BGE_WARM_MODE','bge')
    if warm_mode not in ('bge_lexical','e5_bge_lexical','bge','e5_bge'):
        warm_mode='bge'
    needs_e5=warm_mode in ('e5_bge_lexical','e5_bge')
    main_ready=(
        n and b==n and worker=='ready' and os.getenv('RKB_BGE_ENABLED')=='1'
        and (not needs_e5 or (e==n and fast['ready']))
    )
    mode=warm_mode if main_ready else 'fast_e5' if n and e==n and fast['ready'] else 'lexical_only'
    missing=e<n or b<n
    state='ready' if not missing else 'degraded' if owner in ('unavailable','degraded','disabled') or (e<n and not fast['ready']) or (b<n and worker in ('failed','unavailable')) else 'running' if owner=='running' or e>0 or b>0 else 'pending'
    return IndexingStatus(**values,e5_missing=n-e,bge_missing=n-b,indexing_state=state,bge_worker_state=worker,indexing_owner_state=owner,effective_retrieval_mode=mode)