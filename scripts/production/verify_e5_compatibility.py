"""Recheck the retained E5 fixture and stored batch4 vectors, no model survey."""
import argparse,asyncio,json,math,hashlib
from pathlib import Path
import httpx
from operator_env import load_service_env
from regional_knowledge.e5_contract import SPACE
from regional_knowledge.supabase_backend import backend_from_env

def compare(left,right):
    dot=sum(a*b for a,b in zip(left,right));norm=math.sqrt(sum(a*a for a in left)*sum(b*b for b in right))
    return max(abs(a-b) for a,b in zip(left,right)),dot/norm

async def run(args):
    load_service_env();fixture=json.loads(args.fixture.read_text());checks=[]
    async with httpx.AsyncClient(base_url='http://127.0.0.1:8767',timeout=4,trust_env=False) as client:
        for case in fixture['cases']:
            for role,key in [('query','query_vector'),('passage','document_vector')]:
                response=await client.post('/embed',json={'space':SPACE,'role':role,'texts':[case['text']]});response.raise_for_status()
                delta,cosine=compare(response.json()['vectors'][0],case[key]);checks.append({'role':role,'max_absolute_difference':delta,'cosine':cosine})
    result={'fixed_strings':len(fixture['cases']),'roles_checked':len(checks),'max_absolute_difference':max(row['max_absolute_difference'] for row in checks),'minimum_cosine':min(row['cosine'] for row in checks)}
    assert result['max_absolute_difference']<=fixture['comparison']['absolute_component_tolerance'] and result['minimum_cosine']>=fixture['comparison']['cosine_min']
    if args.lab:
        import subprocess
        corpus=[json.loads(line) for line in (args.lab/'corpus.jsonl').read_text().splitlines()]
        # Keep ML/numpy out of the MCP runtime. The retained benchmark venv only
        # decodes its own NPY file; no inference or package installation occurs.
        saved=json.loads(subprocess.check_output([str(args.lab/'venv/bin/python'),'-c','import json,numpy,sys; print(json.dumps(numpy.load(sys.argv[1]).tolist()))',str(args.lab/'e5-document-vectors.npy')],text=True));backend=backend_from_env()
        try:
            async with backend.data_client._connection({'x-rkb-service':'1'}) as connection:
                rows=await(await connection.execute('select e.chunk_id,e.embedding::text,e.text_sha256 from public.rkb_chunk_embeddings_e5 e where e.chunk_id=any(%s::uuid[])',([row['id'] for row in corpus],))).fetchall()
            by_id={str(row['chunk_id']):row for row in rows};metrics=[]
            for i,row in enumerate(corpus):
                live=by_id[row['id']];assert live['text_sha256']==hashlib.sha256(row['text'].encode()).hexdigest();metrics.append(compare(json.loads(live['embedding']),saved[i]))
            result['stored_document_batch4']={'vectors':len(metrics),'max_absolute_difference':max(pair[0] for pair in metrics),'minimum_cosine':min(pair[1] for pair in metrics)}
            assert result['stored_document_batch4']['minimum_cosine']>=.999999
        finally:await backend.aclose()
    args.output.write_text(json.dumps(result,indent=2));print(json.dumps(result))

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('fixture',type=Path);parser.add_argument('output',type=Path);parser.add_argument('--lab',type=Path);asyncio.run(run(parser.parse_args()))
