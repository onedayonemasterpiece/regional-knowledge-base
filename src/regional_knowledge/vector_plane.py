"""Remote Supabase vector-only client. Local actor admission is authoritative.

No end-user bearer or corpus payload enters this client. RLS uses the existing
application actor bridge plus server-computed document/revision scope, never auth.uid().
"""
import asyncio
import json
from contextlib import asynccontextmanager
from uuid import UUID
from psycopg import sql
from .postgres_backend import PostgresDataClient
from .e5_contract import SPACE as E5_SPACE,validate_vector as e5_validate
from .bge_contract import SPACE as BGE_SPACE,REVISION,validate_vector as bge_validate

class RemoteVectorClient(PostgresDataClient):
    @asynccontextmanager
    async def _connection(self,headers):
        if self.pool.closed:await self.pool.open(wait=False)
        async with self.pool.connection(timeout=3) as db:
            async with db.transaction():
                await db.execute("set local statement_timeout='5000ms'")
                actor=(headers or {}).get('x-rkb-actor')
                if actor:
                    actor=str(UUID(actor))
                    raw_scope=json.loads(headers.get('x-rkb-vector-revisions','{}'))
                    if not isinstance(raw_scope,dict):raise PermissionError('vector revision scope required')
                    scope={str(UUID(key)):int(value) for key,value in raw_scope.items()}
                    if any(value<0 for value in scope.values()):raise PermissionError('invalid vector revision scope')
                    await db.execute('set local role rkb_app')
                    await db.execute(
                        "select set_config('rkb.actor_id',%s,true),"
                        "set_config('rkb.vector_documents',%s,true),"
                        "set_config('rkb.vector_revisions',%s,true)",
                        (actor,','.join(scope),json.dumps(scope,separators=(',',':'))),
                    )
                elif (headers or {}).get('x-rkb-service')!='1':raise PermissionError('vector service context required')
                yield db

    async def candidates(self,actor,revisions,e5,es,bge,bs,depth):
        """Return candidates inside a compact server-authorized document/revision scope."""
        headers={'x-rkb-actor':actor,'x-rkb-vector-revisions':json.dumps(revisions)}
        async with self._connection(headers) as db:
            rows=await(await db.execute(
                'select * from rkb_vector_candidates_v3(%s,%s,%s,%s,%s)',
                (e5,es,bge,bs,depth),
            )).fetchall()
            return [dict(row) for row in rows]

    async def install(self,items,space):
        table='rkb_chunk_embeddings_e5' if space==E5_SPACE else 'rkb_chunk_embeddings_bge' if space==BGE_SPACE else None
        if table is None:raise ValueError('embedding space mismatch')
        results=[]
        async with self._connection({'x-rkb-service':'1'}) as db:
            for item in items:
                vector=e5_validate(item['vector']) if space==E5_SPACE else bge_validate(item['vector'])
                anchor=await(await db.execute('''insert into rkb_vector_items(chunk_id,document_id,revision,text_sha256,search_material_sha256,source_sha256,owner_user_id)
                  values(%s,%s,%s,%s,%s,%s,%s) on conflict(chunk_id) do update set
                  document_id=excluded.document_id,revision=excluded.revision,text_sha256=excluded.text_sha256,
                  search_material_sha256=excluded.search_material_sha256,source_sha256=excluded.source_sha256,owner_user_id=excluded.owner_user_id
                  where rkb_vector_items.document_id=excluded.document_id and rkb_vector_items.revision=excluded.revision
                  and rkb_vector_items.text_sha256=excluded.text_sha256 and rkb_vector_items.search_material_sha256=excluded.search_material_sha256
                  and rkb_vector_items.source_sha256=excluded.source_sha256 and rkb_vector_items.owner_user_id=excluded.owner_user_id returning chunk_id''',
                  (UUID(item['chunk_id']),UUID(item['document_id']),item['revision'],item['text_sha256'],item['search_material_sha256'],item['source_sha256'],UUID(item['owner_user_id'])))).fetchone()
                if not anchor:raise ValueError('immutable vector anchor mismatch')
                cols=['chunk_id','embedding_space','revision','text_sha256','search_material_sha256','embedding']
                values=[UUID(item['chunk_id']),space,item['revision'],item['text_sha256'],item['search_material_sha256'],'['+','.join(format(v,'.9g') for v in vector)+']']
                if space==E5_SPACE:cols+=['batch_sha256'];values+=[item['batch_sha256']]
                else:cols+=['model_revision'];values+=[REVISION]
                slots=[sql.Placeholder() for _ in cols];slots[5]=sql.SQL('%s::vector('+('384' if space==E5_SPACE else '1024')+')')
                query=sql.SQL('insert into {}({}) values({}) on conflict(chunk_id) do update set {} returning chunk_id,embedding_space,revision,text_sha256,search_material_sha256').format(
                  sql.Identifier(table),sql.SQL(',').join(map(sql.Identifier,cols)),sql.SQL(',').join(slots),
                  sql.SQL(',').join(sql.SQL('{}=excluded.{}').format(sql.Identifier(c),sql.Identifier(c)) for c in cols[1:]))
                row=await(await db.execute(query,values)).fetchone();results.append(dict(row))
        return results