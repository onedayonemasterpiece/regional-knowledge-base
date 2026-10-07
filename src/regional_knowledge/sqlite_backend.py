"""SQLite corpus/ACL authority with a separate minimal remote vector plane."""
import asyncio
import os
from uuid import UUID
from .postgres_backend import PostgresBackend,_DbResponse
from .supabase_backend import SupabaseRestBackend,SupabaseConfig
from .sqlite_corpus import SQLiteCorpus
from .sqlite_data import SQLiteDataClient
from .vector_plane import RemoteVectorClient
from .contracts import BookFindOutput,BookFindResult
from .rank_fusion import fuse

class SQLiteBackend(PostgresBackend):
    def __init__(self,dsn=None,*,corpus_path,embedder,object_store,pool_min_size=0,pool_max_size=4,public_base_url=None):
        self.corpus=SQLiteCorpus(corpus_path)
        self.data_client=SQLiteDataClient(self.corpus,self)
        self.vector_client=RemoteVectorClient(dsn,min_size=0,max_size=pool_max_size) if dsn else None
        self.bge_query_embedder=None
        if os.getenv('RKB_BGE_QUERY_LOCAL_ENABLED')=='1':
            from .local_bge import LocalBGEQueryEmbedder
            self.bge_query_embedder=LocalBGEQueryEmbedder()
        SupabaseRestBackend.__init__(self,SupabaseConfig(url='sqlite://regional-knowledge.internal',anon_key='local',service_role_key='local',public_base_url=public_base_url),embedder=embedder,object_store=object_store,client=self.data_client)

    async def aclose(self):
        await SupabaseRestBackend.aclose(self)
        if self.vector_client:await self.vector_client.aclose()
        if self.bge_query_embedder:await self.bge_query_embedder.aclose()

    async def local_rpc(self,name,p,headers):
        if name in ('rkb_fast_e5_search','rkb_hybrid_search','rkb_multilingual_rankings'):return await self.local_rankings(name,p,headers)
        from . import sqlite_revision as revisions
        async with self.data_client._connection(headers,write=name!='rkb_author_authority_for_names') as db:
            ctx=db.context
            if name=='rkb_start_ingestion':result=revisions.start(ctx,p)
            elif name=='rkb_insert_chunks':result=revisions.insert_chunks(ctx,p)
            elif name=='rkb_activate_revision':result=revisions.activate(ctx,p)
            elif name=='rkb_author_authority_for_names':
                profiles=[x for x in ctx.rows('rkb_author_profiles') if x['verification_state']=='verified'];result=[]
                for name in p.get('p_names') or []:
                    for profile in profiles:
                        if name.strip().lower() in [x.strip().lower() for x in [profile['canonical_name'],*profile['aliases']]]:
                            for a in ctx.rows('rkb_author_authority'):
                                if a['author_id']==profile['id'] and a['is_active'] is True and a['subject']==p.get('p_subject') and a['geography']==p.get('p_geography'):result.append({'input_name':name,'author_id':a['author_id'],'score':a['score'],'policy_version':a['policy_version']})
            else:raise ValueError('unsupported local RPC: '+name)
        if name=='rkb_insert_chunks':await asyncio.to_thread(self.corpus.build_fragments,p['p_document_id'],p['p_revision'])
        return _DbResponse(result)

    async def activate_pending(self):
        with self.corpus.connect() as db:pending=[dict(r) for r in db.execute("select * from revision_publication where state='pending'")]
        for r in pending:
            d=self.corpus.one('rkb_documents',r['document_id'])
            await self.local_rpc('rkb_activate_revision',{'p_document_id':r['document_id'],'p_ingestion_id':r['ingestion_id'],'p_revision':r['revision'],'p_poi_events':__import__('json').loads(r['poi_events'])},{'x-rkb-actor':d['owner_user_id']})

    def search_visible(self,document_id):
        document=self.corpus.one('rkb_documents',str(document_id))
        if not document:return False
        seen=set()
        while document:
            catalog=document.get('catalog') or {}
            if catalog.get('searchable',True) is False:return False
            parent=catalog.get('parent_id') or document.get('parent_id')
            if not parent:return True
            parent=str(parent)
            if parent in seen:return False
            seen.add(parent)
            document=self.corpus.one('rkb_documents',parent)
        return False

    async def automatic_aliases(self,query,principal):
        from .entity_graph import normalize_alias
        normalized=normalize_alias(query)
        words=normalized.split()
        spans=[]
        for width in range(1,min(4,len(words))+1):
            for start in range(0,len(words)-width+1):
                value=' '.join(words[start:start+width])
                if len(value)>=3:spans.append(value)
        spans=list(dict.fromkeys(spans))[:64]
        if not spans:return []
        async with self.data_client._connection(self._headers(principal)) as db:
            matched=await(await db.execute(
                'select entity_id from rkb_entity_aliases where normalized_value=any(%s) limit 20',
                (spans,),
            )).fetchall()
            entity_ids=list(dict.fromkeys(str(row['entity_id']) for row in matched))
            if not entity_ids:return []
            aliases=await(await db.execute(
                'select entity_id,value,alias_type,document_id,revision from rkb_entity_aliases where entity_id=any(%s) order by entity_id,id limit 100',
                (entity_ids,),
            )).fetchall()
            entities=await(await db.execute(
                'select id,canonical_label,document_id,revision from rkb_entities where id=any(%s) order by id limit 20',
                (entity_ids,),
            )).fetchall()
        active={}
        for entity in entities:
            doc=self.corpus.one('rkb_documents',str(entity['document_id']))
            if doc and int(doc.get('active_revision') or 0)==int(entity['revision']) and self.search_visible(entity['document_id']):
                active[str(entity['id'])]=entity
        result=[];seen=set()
        for entity_id in entity_ids:
            entity=active.get(entity_id)
            if not entity:continue
            canonical=str(entity['canonical_label']).strip()
            if canonical and canonical.casefold() not in seen:
                result.append({'name':canonical,'kind':'historical'});seen.add(canonical.casefold())
            for alias in aliases:
                if str(alias['entity_id'])!=entity_id:continue
                doc=self.corpus.one('rkb_documents',str(alias['document_id']))
                if not doc or int(doc.get('active_revision') or 0)!=int(alias['revision']) or not self.search_visible(alias['document_id']):continue
                value=str(alias['value']).strip();key=value.casefold()
                if not value or key in seen:continue
                kind=alias.get('alias_type') if alias.get('alias_type') in ('current','historical') else 'historical'
                result.append({'name':value,'kind':kind});seen.add(key)
                if len(result)>=20:return result
        return result

    async def local_rankings(self,name,payload,headers):
        actor=self.data_client.actor(headers)
        if actor is None:raise PermissionError('actor required')
        async with self.data_client._connection(headers) as db:
            docs=await(await db.execute('select id,active_revision from rkb_documents')).fetchall()
        allowed={str(d['id']):d['active_revision'] for d in docs if self.search_visible(d['id'])}
        depth=max(1,min(int(payload.get('depth',100)),100)) if name=='rkb_multilingual_rankings' else max(20,max(1,min(int(payload.get('match_count',8)),20))*5)
        query=payload.get('query_text','')
        include_lexical=bool(payload.get('include_lexical',True))
        timeout_ms=max(10,min(int(payload.get('lexical_timeout_ms',os.getenv('RKB_LEXICAL_BUDGET_MS','100'))),1000))
        rows=[]
        if include_lexical:
            rows.extend(await asyncio.to_thread(
                self.corpus.lexical,str(actor),query,depth=depth,allowed=allowed,timeout_ms=timeout_ms
            ))
        for alias in (payload.get('aliases') or [])[:20]:
            for row in await asyncio.to_thread(
                self.corpus.lexical,str(actor),alias['name'],depth=depth,phrase=True,
                allowed=allowed,timeout_ms=timeout_ms
            ):
                rows.append({**row,'branch':'exact_current_alias' if alias.get('kind')=='current' else 'exact_historical_alias' if alias.get('kind')=='historical' else 'exact_alias','matched_alias':alias['name']})
        e5=payload.get('e5_vector') if name=='rkb_multilingual_rankings' else payload.get('query_embedding')
        es=payload.get('e5_space') if name=='rkb_multilingual_rankings' else payload.get('query_embedding_space')
        try:
            if (e5 or payload.get('bge_vector')) and self.vector_client:
                candidates=await self.vector_client.candidates(
                    str(actor),allowed,e5,es,
                    payload.get('bge_vector'),payload.get('bge_space'),depth
                )
                metadata=await asyncio.to_thread(
                    self.corpus.candidate_metadata_ids,
                    [candidate['chunk_id'] for candidate in candidates],
                    allowed,
                )
                by_id={c['chunk_id']:c for c in metadata}
                for candidate in candidates:
                    c=by_id.get(str(candidate['chunk_id']))
                    if c and allowed.get(c['document_id'])==c['revision'] and all(
                        c[k]==candidate[k] for k in ('revision','text_sha256','search_material_sha256')
                    ):
                        rows.append(dict(candidate))
        except Exception as error:
            import logging
            logging.getLogger(__name__).warning(
                "vector_candidates_unavailable error_type=%s",type(error).__name__
            )
        if name=='rkb_multilingual_rankings':
            return _DbResponse([{**r,'matched_alias':r.get('matched_alias')} for r in rows])
        ids,_=fuse(rows,['e5',*(['lexical'] if include_lexical else [])],limit=max(1,min(int(payload.get('match_count',8)),20)))
        mode='fast_e5' if any(r['branch']=='e5' for r in rows) else 'lexical_only'
        return _DbResponse([{'chunk_id':ident,'title':self.corpus.one('rkb_chunks',ident)['title'],'retrieval_mode':mode} for ident in ids],retrieval_mode=mode)

    async def book_find(self,query,principal,*,limit=8):
        result=await self.catalog(principal,query,limit=max(1,min(limit,8)))
        return BookFindOutput(query=query,results=[BookFindResult(document_id=r['id'],title=r['title'],authors=r.get('authors') or [],publication_year=r.get('publication_year'),active_revision=r['active_revision'],source_format=r.get('source_format'),source_archive_status=r.get('source_archive_status')) for r in result['items']])

    async def catalog(self,principal,query='',*,limit=20,cursor=None,kind=None,document_id=None):
        from .catalog_components import visible
        components=visible(self.corpus,principal.subject)
        async with self.data_client._connection(self._headers(principal)) as db:
            docs=await(await db.execute('select id from rkb_documents')).fetchall()
        items=[self.catalog_details(self.corpus.one('rkb_documents',str(r['id']))) for r in docs]+components
        if document_id:
            row=next((r for r in items if r['id']==document_id),None)
            if not row:raise LookupError('document_not_found')
            return row
        offset=int(cursor or 0)
        if offset<0 or not 1<=limit<=100:raise ValueError('invalid pagination')
        terms=query.casefold().split()
        items=[r for r in items if (not kind or r['catalog']['kind']==kind) and all(t in __import__('json').dumps(r,ensure_ascii=False).casefold() for t in terms)]
        items.sort(key=lambda r:(r['title'].casefold(),r['id']))
        return {'items':items[offset:offset+limit],'next_cursor':str(offset+limit) if len(items)>offset+limit else None}

    @staticmethod
    def catalog_details(row):
        fields=('id','title','authors','publication_year','language','active_revision','page_count','source_format','source_archive_status')
        return {**{k:row.get(k) for k in fields},'catalog':{'kind':'book',**(row.get('catalog') or {})}}

    async def _pending_vector_progress(self,row):
        from .e5_contract import SPACE as E5_SPACE
        from .bge_contract import SPACE as BGE_SPACE,REVISION
        document=str(row['document_id']);revision=int(row['staged_revision'])
        def read():
            with self.corpus.connect() as db:
                value=db.execute('''select count(*) chunks,
                  coalesce(sum(case when e.row_key is not null
                    and json_extract(e.payload,'$.embedding_space')=?
                    and json_extract(e.payload,'$.revision')=t.revision
                    and json_extract(e.payload,'$.text_sha256')=t.text_sha256
                    and json_extract(e.payload,'$.search_material_sha256')=t.search_material_sha256
                    then 1 else 0 end),0) e5_ready,
                  coalesce(sum(case when b.row_key is not null
                    and json_extract(b.payload,'$.embedding_space')=?
                    and json_extract(b.payload,'$.model_revision')=?
                    and json_extract(b.payload,'$.revision')=t.revision
                    and json_extract(b.payload,'$.text_sha256')=t.text_sha256
                    and json_extract(b.payload,'$.search_material_sha256')=t.search_material_sha256
                    then 1 else 0 end),0) bge_ready
                  from chunk_text t
                  left join corpus_rows e on e.table_name='rkb_chunk_embeddings_e5' and e.row_key=t.chunk_id
                  left join corpus_rows b on b.table_name='rkb_chunk_embeddings_bge' and b.row_key=t.chunk_id
                  where t.document_id=? and t.revision=?''',
                  (E5_SPACE,BGE_SPACE,REVISION,document,revision)).fetchone()
                return dict(value)
        return await asyncio.to_thread(read)

    async def _pending_vector_message(self,row,principal):
        from .vector_policy import required_vector_spaces
        from .index_readiness import maintenance_state
        self.corpus.authorize(principal.subject,str(row['document_id']),owner=True)
        counts=await self._pending_vector_progress(row)
        required=required_vector_spaces();parts=[]
        for space in required:
            ready=int(counts[space+'_ready'])
            parts.append(f"{space.upper()} {ready}/{int(counts['chunks'])}")
        spaces='+'.join(space.upper() for space in required)
        progress=', '.join(parts)
        return (
            f"Waiting for required {spaces} publication "
            f"(revision {int(row['staged_revision'])}: {progress}; indexer {maintenance_state()}); "
            "previous revision remains selected"
        )

    async def book_ingest(self,**kwargs):
        payload=kwargs.get('payload') or {}
        if kwargs.get('command')=='start' and (payload.get('catalog') or {}).get('parent_id') and kwargs.get('file') is None:
            from .catalog_components import create
            return create(self.corpus,kwargs['principal'].subject,payload)
        if kwargs.get('ingestion_id') and kwargs.get('command') in ('status','finalize','continue_pages','validate'):
            row=await self._ingestion_row(principal=kwargs['principal'],ingestion_id=kwargs['ingestion_id'])
            if row and row.get('cursor')=='vectors':
                from .vector_policy import required_vector_spaces
                required='+'.join(space.upper() for space in required_vector_spaces())
                return self._ingestion_output(row,f'Waiting for required {required} publication; previous revision remains selected')
        result=await super().book_ingest(**kwargs)
        if result.document_id and payload.get('catalog'):
            from .contracts import CatalogMetadata
            metadata=CatalogMetadata.model_validate(payload['catalog']).model_dump(mode='json',exclude_none=True)
            document=self.corpus.authorize(kwargs['principal'].subject,result.document_id,owner=True)
            if metadata.get('parent_id'):
                parent=self.corpus.authorize(kwargs['principal'].subject,metadata['parent_id'])
                if (parent.get('catalog') or {}).get('kind')!='journal_issue':raise ValueError('article parent must be a journal issue')
            self.corpus.put('rkb_documents',[{**document,'catalog':metadata}])
        return result