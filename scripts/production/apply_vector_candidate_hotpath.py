"""Idempotently install and verify the vector candidate v4 hot path."""
import asyncio,os
from pathlib import Path
from operator_env import load_service_env
from regional_knowledge.vector_plane import RemoteVectorClient

async def main():
    load_service_env()
    client=RemoteVectorClient(os.environ['KB_SUPABASE_SESSION_CONNECTION'],min_size=0,max_size=1)
    try:
        async with client._connection({'x-rkb-service':'1'}) as db:
            sql=Path('sql/022_vector_candidate_hotpath.sql').read_text().strip()
            await db.execute(sql.removeprefix('begin;').removesuffix('commit;'))
            row=await(await db.execute(
                "select to_regprocedure('public.rkb_vector_candidates_v4(text,text,text,text,integer)')::text name"
            )).fetchone()
            if not row or not row['name']:
                raise ValueError('vector candidate v4 missing after apply')
        print('rkb_vector_candidates_v4 ready')
    finally:
        await client.aclose()

if __name__=='__main__':
    asyncio.run(main())
