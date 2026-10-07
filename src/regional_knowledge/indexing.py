"""Small active-index reconciler. Missing vectors + existing BGE jobs are state.

No new durable scheduler/queue, source parser, native model or paid provider.
Source reads use each document owner and ordinary actor RLS. E5 preserves the
per-document ordered batch4 contract; BGE preserves single-document job priority.
"""
import asyncio,hashlib,json,logging,os,time
from pathlib import Path
from uuid import UUID
from .contracts import Principal
from .local_e5 import LocalE5Embedder
from .e5_contract import SPACE as E5_SPACE,validate_vector as validate_e5
from .bge_contract import SPACE as BGE_SPACE,REVISION,validate_vector as validate_bge
from .bge_queue import BgeQueue
from .vector_policy import index_vector_spaces
log=logging.getLogger(__name__)
POLL_SECONDS=5
BGE_DOCUMENT_WINDOW=64
STALL_SECONDS=300

VALID_E5="e.chunk_id is not null and e.embedding_space=%s and e.revision=c.revision and e.text_sha256=c.text_sha256 and e.search_material_sha256=c.search_material_sha256"
VALID_BGE="b.chunk_id is not null and b.embedding_space=%s and b.model_revision=%s and b.revision=c.revision and b.text_sha256=c.text_sha256 and b.search_material_sha256=c.search_material_sha256"

def fingerprint(rows):
    return hashlib.sha256(json.dumps([(str(r['id']),r['text_sha256'],r['revision'],r['search_material_sha256']) if r.get('search_material_sha256',r['text_sha256'])!=r['text_sha256'] else (str(r['id']),r['text_sha256'],r['revision']) for r in rows],separators=(',',':')).encode()).hexdigest()

def bge_key(row):
    key='bge:'+BGE_SPACE+':'+str(row['id'])+':'+str(row['revision'])+':'+row['text_sha256']
    return key+':'+row['search_material_sha256'] if row.get('search_material_sha256',row['text_sha256'])!=row['text_sha256'] else key

def bge_identity(row):
    value={'chunk_id':str(row['id']),'revision':row['revision'],'text_sha256':row['text_sha256']}
    if row.get('search_material_sha256',row['text_sha256'])!=row['text_sha256']:value['search_material_sha256']=row['search_material_sha256']
    return value

def literal(vector):return '['+','.join(format(v,'.9g') for v in vector)+']'

class IndexReconciler:
    def __init__(self,backend,queue):
        if 'e5' in index_vector_spaces() and not isinstance(backend.embedder,LocalE5Embedder):raise RuntimeError('local E5 required when E5 indexing is enabled; no provider fallback')
        self.backend=backend;self.queue=queue;self.cursor=None
        self.mirror=None
        self.archive=None
        self.last_gc=0
        self.last_original_gc=0

    def actor(self,owner):return Principal(subject=str(owner),client_id='index-maintenance',issuer='internal-actor-bridge',access_token='internal-actor-bridge')

    def selected_revision(self):
        if hasattr(self.backend,'corpus'):
            return "(c.revision=d.active_revision or exists(select 1 from revision_publication rp where rp.document_id=d.id and rp.revision=c.revision and rp.state='pending'))"
        return 'c.revision=d.active_revision'

    async def documents(self):
        # System metadata selection only. No source bytes/locators leave this
        # boundary until the owner account and chunk are authorized below.
        spaces=index_vector_spaces()
        missing=[];params=[]
        if 'bge' in spaces:
            missing.append(f'not ({VALID_BGE})');params.extend((BGE_SPACE,REVISION))
        if 'e5' in spaces:
            missing.append(f'not ({VALID_E5})');params.append(E5_SPACE)
        predicate=' or '.join(missing)
        async with self.backend.data_client._connection({'x-rkb-service':'1'}) as db:
            rows=await(await db.execute(f'''select distinct d.id,d.owner_user_id from rkb_documents d
             join rkb_users u on u.id=d.owner_user_id and u.status='active'
             join rkb_chunks c on c.document_id=d.id and {self.selected_revision()}
             left join rkb_chunk_embeddings_e5 e on e.chunk_id=c.id
             left join rkb_chunk_embeddings_bge b on b.chunk_id=c.id
             where ({predicate})
             and (%s::uuid is null or d.id>%s) order by d.id limit 4''',(*params,self.cursor,self.cursor))).fetchall()
        if not rows:
            wrapped=self.cursor is not None;self.cursor=None
            if wrapped:return await self.documents()
        else:self.cursor=rows[-1]['id']
        return rows

    async def e5_groups(self,actor,document):
        async with self.backend.data_client._connection(self.backend._headers(actor)) as db:
            rows=await(await db.execute(f'''with ranked as (
             select c.id,c.document_id,c.revision,c.text_sha256,c.text_start,c.search_material,c.search_material_sha256,
              ((row_number() over(partition by c.revision order by c.text_start,c.id))-1)/4 batch,
              ({VALID_E5}) valid,e.batch_sha256 existing_batch
             from rkb_chunks c join rkb_documents d on d.id=c.document_id
             left join rkb_chunk_embeddings_e5 e on e.chunk_id=c.id
             where d.id=%s and {self.selected_revision()}), missing as (
              select distinct revision,batch from ranked where not valid order by revision,batch limit 2)
             select * from ranked where (revision,batch) in(select revision,batch from missing) order by revision,batch,text_start,id''',(E5_SPACE,document))).fetchall()
        groups=[]
        for row in rows:
            if not groups or (groups[-1][0]['revision'],groups[-1][0]['batch'])!=(row['revision'],row['batch']):groups.append([])
            groups[-1].append(row)
        return groups

    async def source(self,actor,row):
        if hasattr(self.backend,'corpus'):
            # Indexing needs the exact authorized passage, not fully hydrated
            # evidence metadata. Public fetch expands pages, regions and
            # illustrations and made large-book indexing scale with hydration.
            await asyncio.to_thread(self.backend.corpus.authorize,actor.subject,str(row['document_id']),owner=True)
            chunk=await asyncio.to_thread(self.backend.corpus.one,'rkb_chunks',str(row['id']))
            if not chunk or str(chunk['document_id'])!=str(row['document_id']) or chunk['revision']!=row['revision']:
                raise ValueError('publication source changed')
            result_text=chunk['source_text']
            text=chunk.get('search_material') if chunk.get('search_material') is not None else result_text
        else:
            result=await self.backend.fetch(str(row['id']),actor)
            result_text=result.text
            text=row.get('search_material') if row.get('search_material') is not None else result_text
        if hashlib.sha256(result_text.encode()).hexdigest()!=row['text_sha256']:raise ValueError('source hash mismatch')
        if hashlib.sha256(text.encode()).hexdigest()!=row.get('search_material_sha256',row['text_sha256']):raise ValueError('search material hash mismatch')
        return text

    async def legacy_anchor(self,db,row):
        # Compatibility for historical PG fixtures/releases with the v2 FK.
        # Production SQLite publication uses vector_plane.install instead.
        await db.execute('''insert into rkb_vector_items(chunk_id,document_id,revision,text_sha256,search_material_sha256,source_sha256,owner_user_id)
          select c.id,c.document_id,c.revision,c.text_sha256,c.search_material_sha256,d.source_sha256,d.owner_user_id
          from rkb_chunks c join rkb_documents d on d.id=c.document_id
          join rkb_users u on u.id=d.owner_user_id and u.status='active'
          where c.id=%s and c.revision=%s and c.text_sha256=%s and c.search_material_sha256=%s
          and c.revision=d.active_revision and d.owner_user_id=rkb_current_actor_id()
          on conflict(chunk_id) do nothing''',(row['id'],row['revision'],row['text_sha256'],row.get('search_material_sha256',row['text_sha256'])))

    async def install_e5(self,actor,group,vectors):
        if hasattr(self.backend,'corpus'):return await self.install_local(actor,group,vectors,E5_SPACE,batch_sha256=fingerprint(group))
        batch=fingerprint(group);written=0
        async with self.backend.data_client._connection({'x-rkb-service':'1'}) as db:
            await db.execute("select set_config('rkb.actor_id',%s,true)",(actor.subject,))
            for row,vector in zip(group,vectors,strict=True):
                await self.legacy_anchor(db,row)
                result=await db.execute(f'''insert into rkb_chunk_embeddings_e5(chunk_id,embedding_space,revision,text_sha256,batch_sha256,embedding,search_material_sha256)
                 select c.id,%s,c.revision,c.text_sha256,%s,%s::vector(384),c.search_material_sha256 from rkb_chunks c join rkb_documents d on d.id=c.document_id
                 join rkb_users u on u.id=d.owner_user_id and u.status='active'
                 where c.id=%s and c.revision=%s and c.text_sha256=%s and c.search_material_sha256=%s and {self.selected_revision()} and d.owner_user_id=rkb_current_actor_id() and rkb_can_read_document(c.document_id)
                 on conflict(chunk_id) do update set embedding_space=excluded.embedding_space,revision=excluded.revision,text_sha256=excluded.text_sha256,batch_sha256=excluded.batch_sha256,embedding=excluded.embedding,search_material_sha256=excluded.search_material_sha256,updated_at=now()
                 where rkb_chunk_embeddings_e5.embedding_space is distinct from excluded.embedding_space or rkb_chunk_embeddings_e5.revision is distinct from excluded.revision or rkb_chunk_embeddings_e5.text_sha256 is distinct from excluded.text_sha256 or rkb_chunk_embeddings_e5.search_material_sha256 is distinct from excluded.search_material_sha256 or rkb_chunk_embeddings_e5.batch_sha256 is distinct from excluded.batch_sha256''',(E5_SPACE,batch,literal(validate_e5(vector)),row['id'],row['revision'],row['text_sha256'],row.get('search_material_sha256',row['text_sha256'])))
                written+=result.rowcount
        return written

    async def e5(self,actor,document):
        written=0
        for group in await self.e5_groups(actor,document):
            if any(r['search_material_sha256']!=r['text_sha256'] for r in group):group=[r for r in group if not r['valid']]
            texts=[await self.source(actor,row) for row in group]
            vectors=await self.backend.embedder.embed_passages(texts)
            written+=await self.install_e5(actor,group,vectors)
        return written

    async def install_bge(self,actor,row,result):
        if result['space']!=BGE_SPACE or len(result['vectors'])!=1:raise ValueError('BGE result contract')
        vector=validate_bge(result['vectors'][0])
        if hasattr(self.backend,'corpus'):return await self.install_local(actor,[row],[vector],BGE_SPACE,model_revision=REVISION)
        async with self.backend.data_client._connection({'x-rkb-service':'1'}) as db:
            await db.execute("select set_config('rkb.actor_id',%s,true)",(actor.subject,))
            await self.legacy_anchor(db,row)
            cursor=await db.execute(f'''insert into rkb_chunk_embeddings_bge(chunk_id,embedding_space,model_revision,revision,text_sha256,embedding,search_material_sha256)
             select c.id,%s,%s,c.revision,c.text_sha256,%s::vector(1024),c.search_material_sha256 from rkb_chunks c join rkb_documents d on d.id=c.document_id
             join rkb_users u on u.id=d.owner_user_id and u.status='active'
             where c.id=%s and c.revision=%s and c.text_sha256=%s and c.search_material_sha256=%s and {self.selected_revision()} and d.owner_user_id=rkb_current_actor_id() and rkb_can_read_document(c.document_id)
             on conflict(chunk_id) do update set embedding_space=excluded.embedding_space,model_revision=excluded.model_revision,revision=excluded.revision,text_sha256=excluded.text_sha256,embedding=excluded.embedding,search_material_sha256=excluded.search_material_sha256,updated_at=now()
             where rkb_chunk_embeddings_bge.embedding_space is distinct from excluded.embedding_space or rkb_chunk_embeddings_bge.model_revision is distinct from excluded.model_revision or rkb_chunk_embeddings_bge.text_sha256 is distinct from excluded.text_sha256 or rkb_chunk_embeddings_bge.search_material_sha256 is distinct from excluded.search_material_sha256 or rkb_chunk_embeddings_bge.revision is distinct from excluded.revision''',(BGE_SPACE,REVISION,literal(vector),row['id'],row['revision'],row['text_sha256'],row.get('search_material_sha256',row['text_sha256'])))
            return cursor.rowcount

    async def install_bge_batch(self,actor,ready):
        if not ready:return 0
        rows=[];vectors=[]
        for row,result in ready:
            if result['space']!=BGE_SPACE or len(result['vectors'])!=1:raise ValueError('BGE result contract')
            rows.append(row);vectors.append(validate_bge(result['vectors'][0]))
        if hasattr(self.backend,'corpus'):
            return await self.install_local(actor,rows,vectors,BGE_SPACE,model_revision=REVISION)
        written=0
        for row,vector in zip(rows,vectors,strict=True):
            written+=await self.install_bge(actor,row,{'space':BGE_SPACE,'vectors':[vector]})
        return written

    async def bge(self,actor,document):
        async with self.backend.data_client._connection(self.backend._headers(actor)) as db:
            rows=await(await db.execute(f'''select c.id,c.document_id,c.revision,c.text_sha256,c.search_material,c.search_material_sha256 from rkb_chunks c join rkb_documents d on d.id=c.document_id
             left join rkb_chunk_embeddings_bge b on b.chunk_id=c.id where d.id=%s and {self.selected_revision()} and not ({VALID_BGE})
             order by c.text_start,c.id limit {BGE_DOCUMENT_WINDOW}''',(document,BGE_SPACE,REVISION))).fetchall()
        submitted=0;ready=[]
        capacity=max(0,BGE_DOCUMENT_WINDOW-await asyncio.to_thread(self.queue.document_pending))
        for row in rows:
            key=bge_key(row);job=await asyncio.to_thread(self.queue.lookup,actor.subject,key)
            if job:
                if job['kind']!='document' or job['identity']!=bge_identity(row):raise ValueError('BGE source identity mismatch')
                if job['state']=='done':ready.append((row,job['result']))
                continue
            # Reserve interactive capacity. Claims still strictly prioritize
            # queries over document jobs.
            if capacity<=0:continue
            text=await self.source(actor,row)
            async with self.backend.data_client._connection(self.backend._headers(actor)) as db:
                active=await(await db.execute(f'''select c.id from rkb_chunks c join rkb_documents d on d.id=c.document_id
                 where c.id=%s and c.revision=%s and c.text_sha256=%s and c.search_material_sha256=%s and {self.selected_revision()} and d.owner_user_id=rkb_current_actor_id() for share of d''',(row['id'],row['revision'],row['text_sha256'],row['search_material_sha256']))).fetchone()
                if not active:continue
                await asyncio.to_thread(self.queue.enqueue,actor.subject,key,[text],kind='document',identity=bge_identity(row));submitted+=1;capacity-=1
        return submitted,await self.install_bge_batch(actor,ready)

    async def install_local(self,actor,rows,vectors,space,**extra):
        from .sqlite_data import defaults
        from .sqlite_revision import manifest
        items=[]
        # No transaction spans the network. Freeze identities under local actor
        # authorization, then recheck the acknowledgement before local readiness.
        for row,vector in zip(rows,vectors,strict=True):
            c=self.backend.corpus.one('rkb_chunks',str(row['id']))
            d=self.backend.corpus.authorize(actor.subject,c['document_id'],owner=True)
            if any(c[k]!=row[k] for k in ('revision','text_sha256','search_material_sha256')):raise ValueError('publication source changed')
            items.append({'chunk_id':c['id'],'document_id':d['id'],'revision':c['revision'],'text_sha256':c['text_sha256'],'search_material_sha256':c['search_material_sha256'],'source_sha256':d['source_sha256'],'owner_user_id':d['owner_user_id'],'vector':vector,**extra})
        if not self.backend.vector_client:raise RuntimeError('vector service unavailable')
        ack=await self.backend.vector_client.install(items,space)
        if len(ack)!=len(items):raise ValueError('publication acknowledgement incomplete')
        table='rkb_chunk_embeddings_e5' if space==E5_SPACE else 'rkb_chunk_embeddings_bge'
        async with self.backend.data_client._connection(self.backend._headers(actor),write=True) as db:
            for item,a in zip(items,ack,strict=True):
                c=db.context.one('rkb_chunks',item['chunk_id']);d=db.context.one('rkb_documents',item['document_id'])
                if not c or d['source_sha256']!=item['source_sha256'] or a['embedding_space']!=space or str(a['chunk_id'])!=c['id'] or any(a[k]!=c[k] or item[k]!=c[k] for k in ('revision','text_sha256','search_material_sha256')):raise ValueError('publication acknowledgement mismatch')
                self.backend.corpus.put(table,[{**defaults(table),**{k:v for k,v in item.items() if k in ('chunk_id','revision','text_sha256','search_material_sha256')},'embedding_space':space,**extra}],connection=db.db)
        return len(items)

    async def tick(self):
        stats={'e5_written':0,'bge_submitted':0,'bge_written':0,'errors':[],'pending_documents':0,'phase_seconds':{}}
        started=time.monotonic()
        phase=time.monotonic();documents=await self.documents();stats['pending_documents']=len(documents);stats['phase_seconds']['document_scan']=time.monotonic()-phase
        phase=time.monotonic()
        for document in documents:
            actor=self.actor(document['owner_user_id'])
            # Publication work is the primary responsibility of this worker.
            # BGE is the production gate and always runs before optional E5.
            for space in index_vector_spaces():
                try:
                    if space=='e5':stats['e5_written']+=await self.e5(actor,document['id'])
                    else:
                        submitted,written=await self.bge(actor,document['id']);stats['bge_submitted']+=submitted;stats['bge_written']+=written
                except Exception as error:
                    stats['errors'].append(type(error).__name__)
                    log.warning(json.dumps({'event':'indexing_retry','space':space,'document_id':str(document['id']),'error_type':type(error).__name__}))
        if hasattr(self.backend,'corpus'):await self.backend.activate_pending()
        stats['phase_seconds']['publication']=time.monotonic()-phase

        # Ancillary maintenance follows publication so unrelated provider
        # latency cannot delay this pass's vector progress or activation.
        phase=time.monotonic()
        if time.time()-self.last_original_gc>60:
            from .original_cache import OriginalCache
            stats['original_cache']=await asyncio.to_thread(OriginalCache.from_env().cleanup)
            proof_cache=OriginalCache(os.getenv('RKB_PROOF_CACHE_DIR','/home/dev/.local/state/regional-knowledge-base/proofs'),max_bytes=64*1024*1024)
            stats['proof_cache']=await asyncio.to_thread(proof_cache.cleanup)
            self.last_original_gc=time.time()
        stats['phase_seconds']['cache_gc']=time.monotonic()-phase
        phase=time.monotonic()
        if not documents and os.getenv('RKB_VIBEPUBLISH_GRANT_FILE'):
            try:
                if self.mirror is None:
                    from .illustration_mirror import IllustrationMirror,VibePublishClient
                    self.mirror=IllustrationMirror(self.backend,VibePublishClient(os.environ['RKB_VIBEPUBLISH_GRANT_FILE']))
                if self.archive is None:
                    from .source_archive import SourceArchive
                    self.archive=SourceArchive(self.backend,self.mirror.client)
                stats['sources_verified']=await self.archive.tick()
                stats['mirrors_verified']=await self.mirror.tick()
            except Exception as error:
                log.warning(json.dumps({'event':'illustration_mirror_pass_retry','error_type':type(error).__name__}))
        stats['phase_seconds']['archive_mirror']=time.monotonic()-phase
        phase=time.monotonic()
        if not documents and os.getenv('RKB_STORAGE_GC_ENABLED')=='1' and time.time()-self.last_gc>60:
            from .storage_gc import collect
            await collect(self.backend,apply=True);self.last_gc=time.time()
        stats['phase_seconds']['storage_gc']=time.monotonic()-phase
        stats['phase_seconds']['total']=time.monotonic()-started
        if any(stats[k] for k in ('e5_written','bge_submitted','bge_written')) or stats['errors']:
            log.info(json.dumps({'event':'indexing_progress',**stats,'external_embedding_api_calls':0}))
        return stats

def write_health(stats):
    path=Path(os.environ['RKB_INDEXING_HEALTH_PATH']);path.parent.mkdir(parents=True,exist_ok=True)
    now=time.time();previous={}
    try:previous=json.loads(path.read_text())
    except (OSError,ValueError):pass
    progress=sum(int(stats.get(k,0)) for k in ('e5_written','bge_submitted','bge_written'))
    pending=int(stats.get('pending_documents',0))
    if progress or not previous.get('pending_documents'):last_progress=now
    else:last_progress=float(previous.get('last_progress_at',now))
    stalled=pending>0 and now-last_progress>STALL_SECONDS
    if stalled and not previous.get('stalled_seconds'):
        log.error(json.dumps({'event':'indexing_stalled','pending_documents':pending,'stalled_seconds':now-last_progress}))
    state='degraded' if stats.get('errors') or stalled else 'running' if pending or progress else 'ready'
    value={'updated_at':now,'pid':os.getpid(),'state':state,
           'error_type':stats['errors'][0] if stats.get('errors') else 'indexing_stalled' if stalled else None,
           'pending_documents':pending,'last_progress_at':last_progress,
           'stalled_seconds':max(0,now-last_progress) if stalled else 0,
           'phase_seconds':stats.get('phase_seconds',{})}
    temp=path.with_name(path.name+'.new');temp.write_text(json.dumps(value));temp.chmod(0o600);temp.replace(path)

async def run_loop(backend,queue):
    if not hasattr(backend,"corpus"):return await _legacy_run_loop(backend,queue)
    import fcntl
    worker=IndexReconciler(backend,queue)
    lock_path=backend.corpus.path.with_suffix('.indexing.lock')
    with lock_path.open('a') as owner:
        lock_path.chmod(0o600)
        try:fcntl.flock(owner,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:raise RuntimeError('indexing owner already running')
        try:
            while True:
                try:stats=await worker.tick()
                except Exception as error:
                    stats={'e5_written':0,'bge_submitted':0,'bge_written':0,'errors':[type(error).__name__]}
                    log.warning(json.dumps({'event':'indexing_pass_failed','error_type':type(error).__name__}))
                write_health(stats)
                await asyncio.sleep(POLL_SECONDS)
        finally:
            if worker.mirror:await worker.mirror.client.close()

async def _legacy_run_loop(backend,queue):
    worker=IndexReconciler(backend,queue);await backend.data_client._ensure_open()
    # One independent connection owns LISTEN and a session advisory mutex.
    # It never holds source rows, model calls or private queue texts.
    async with backend.data_client.pool.connection() as listener:
        await listener.set_autocommit(True)
        lock=await(await listener.execute("select pg_try_advisory_lock(hashtext('rkb-index-maintenance')) locked")).fetchone()
        if not lock['locked']:raise RuntimeError('indexing owner already running')
        try:
            await listener.execute('listen rkb_index_activation')
            while True:
                try:stats=await worker.tick()
                except Exception as error:
                    stats={'e5_written':0,'bge_submitted':0,'bge_written':0,'errors':[type(error).__name__]};log.warning(json.dumps({'event':'indexing_pass_failed','error_type':type(error).__name__}))
                write_health(stats)
                async for _ in listener.notifies(timeout=POLL_SECONDS,stop_after=1):
                    log.info(json.dumps({'event':'indexing_activation_wakeup'}))
        finally:
            if worker.mirror:await worker.mirror.client.close()
            await listener.execute('unlisten rkb_index_activation');await listener.execute("select pg_advisory_unlock(hashtext('rkb-index-maintenance'))");await listener.set_autocommit(False)

async def main():
    from .supabase_backend import backend_from_env
    logging.basicConfig(level=logging.INFO);backend=backend_from_env()
    try:await run_loop(backend,BgeQueue(os.environ['RKB_BGE_QUEUE_PATH']))
    finally:await backend.aclose()
if __name__=='__main__':asyncio.run(main())
