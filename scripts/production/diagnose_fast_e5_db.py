"""Measure the DB boundary under the ordinary owner ACL, without changing rows."""
import argparse,asyncio,json,time
from pathlib import Path
from operator_env import load_service_env
from accept_fast_e5 import principal,distribution
from regional_knowledge.supabase_backend import backend_from_env
from regional_knowledge.e5_contract import SPACE

async def run(output):
    load_service_env();backend=backend_from_env();actor=principal();rows=[]
    query='Почему крепость Кёнигсберг получила своё название и какую роль играл король Оттокар?'
    try:
        vector=await backend.embedder.embed(query);literal='['+','.join(format(value,'.9g') for value in vector)+']'
        for _ in range(10):
            start=time.monotonic()
            async with backend.data_client._connection(backend._headers(actor)) as connection:
                context=time.monotonic()-start;start=time.monotonic()
                await connection.execute('select 1');rtt=time.monotonic()-start;start=time.monotonic()
                plan=await(await connection.execute('explain(analyze,format json) select * from public.rkb_fast_e5_search(%s,%s,%s,8)',(query,literal,SPACE))).fetchone()
                sql_seconds=time.monotonic()-start
                details=next(iter(plan.values()))[0]
            rows.append({'pool_plus_actor_context_seconds':context,'select1_roundtrip_seconds':rtt,'rpc_roundtrip_seconds':sql_seconds,'server_execution_seconds':details['Execution Time']/1000})
        result={'db_pool_max':backend.data_client.pool.max_size,'samples':rows,'seconds':{key:distribution([row[key] for row in rows]) for key in rows[0]},'pool_stats':backend.data_client.pool.get_stats()}
        output.write_text(json.dumps(result,indent=2));print(json.dumps(result['seconds']))
    finally:await backend.aclose()

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('output',type=Path);asyncio.run(run(parser.parse_args().output))
