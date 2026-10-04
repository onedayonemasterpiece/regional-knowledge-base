"""Operator status: loopback readiness and active vector counts, no document details."""
import asyncio,json
from operator_env import load_service_env
from regional_knowledge.supabase_backend import backend_from_env
from regional_knowledge.local_e5 import LocalE5Embedder
from regional_knowledge.e5_contract import SPACE

async def status():
    load_service_env();b=backend_from_env();encoder=LocalE5Embedder()
    try:
        result=await encoder.status()
        result['configured']=isinstance(b.embedder,LocalE5Embedder)
        async with b.data_client._connection({'x-rkb-service':'1'}) as c:
            row=await(await c.execute('''select count(*) as active_chunks,count(e.chunk_id) filter(where e.embedding_space=%s and e.text_sha256=c.text_sha256 and e.revision=c.revision) as active_e5_vectors
              from public.rkb_chunks c join public.rkb_documents d on d.id=c.document_id left join public.rkb_chunk_embeddings_e5 e on e.chunk_id=c.id where c.revision=d.active_revision''',(SPACE,))).fetchone()
        return {**result,**row,'degraded':not result['configured'] or not result['ready'],'external_embedding_calls':0}
    finally:await encoder.aclose();await b.aclose()
if __name__=='__main__':print(json.dumps(asyncio.run(status()),indent=2))
