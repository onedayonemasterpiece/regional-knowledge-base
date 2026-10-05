"""Same frozen HARD6/complete-answer inputs over legacy and local authority."""
import ast,asyncio,json,os,time
from pathlib import Path
from contextlib import asynccontextmanager
from operator_env import load_service_env
from regional_knowledge.postgres_backend import PostgresBackend
from regional_knowledge.sqlite_backend import SQLiteBackend
from regional_knowledge.supabase_backend import _embedder_from_env,_object_store_from_env
from regional_knowledge.contracts import Principal

async def run():
    load_service_env();os.environ['RKB_BGE_WARM_MODE']='bge'
    inputs=json.loads(Path(os.environ['RKB_FROZEN_CASES']).read_text());cases=inputs['cases']
    baseline=json.loads(Path(os.environ['RKB_FROZEN_BASELINE']).read_text()) if os.getenv('RKB_FROZEN_BASELINE') else None
    old=None if baseline else PostgresBackend(os.environ['KB_SUPABASE_SESSION_CONNECTION'],embedder=_embedder_from_env(),object_store=_object_store_from_env(),pool_max_size=2)
    new=SQLiteBackend(os.environ['KB_SUPABASE_SESSION_CONNECTION'],corpus_path='/home/dev/.local/state/regional-knowledge-base/corpus.sqlite3',embedder=_embedder_from_env(),object_store=_object_store_from_env(),pool_max_size=2)
    original=old.data_client._connection if old else None
    @asynccontextmanager
    async def scoped(headers):
        async with original(headers) as db:
            if (headers or {}).get('x-rkb-actor'):
                docs=await(await db.execute('select id from rkb_documents')).fetchall()
                await db.execute("select set_config('rkb.vector_documents',%s,true)",(','.join(str(d['id']) for d in docs),))
            yield db
    if old:old.data_client._connection=scoped
    doc=new.corpus.one('rkb_documents',inputs['document_id'])
    actor=Principal(subject=doc['owner_user_id'],client_id='frozen-hard6',issuer='application-actor-bridge',access_token=<redacted>
    report={'revision':doc['active_revision'],'source_sha256':doc['source_sha256'],'cases':[]}
    try:
        for case in cases:
            item={'name':case['name'],'query':case['query'],'required_groups':case['required_groups']}
            if baseline:
                previous=next(c for c in baseline['cases'] if c['name']==case['name'])
                assert previous['query']==case['query'] and previous['required_groups']==case['required_groups']
                item['before']=previous['before']
            for label,backend in ([('after',new)] if baseline else [('before',old),('after',new)]):
                t=time.monotonic();res=await backend.search(case['query'],actor,match_count=8)
                if res.main_job_id and res.main_state!='ready':
                    from regional_knowledge.bge_queue import BgeQueue
                    from regional_knowledge.multilingual_retrieval import wait_result
                    await wait_result(BgeQueue(os.environ['RKB_BGE_QUEUE_PATH']),actor.subject,res.main_job_id,90)
                    res=await backend.search(case['query'],actor,match_count=8,main_job_id=res.main_job_id)
                evidence=[await backend.fetch(hit.id,actor) for hit in res.results]
                text='\n'.join(f.text for f in evidence if (f.metadata or {}).get('document_id')==doc['id']).casefold()
                complete=all(all(token.casefold() in text for token in group) for group in case['required_groups'])
                item[label]={'mode':res.retrieval_mode,'complete':complete,'target_present':case['target'] in [f.id for f in evidence],'ids':[f.id for f in evidence],'seconds':time.monotonic()-t,'citations':[f.model_dump(mode='json') for f in evidence]}
            report['cases'].append(item)
            Path(os.environ['RKB_ACCEPTANCE_DIR'],'hard6.json').write_text(json.dumps(report,ensure_ascii=False))
        report['before_complete']=sum(c['before']['complete'] for c in report['cases']);report['after_complete']=sum(c['after']['complete'] for c in report['cases']);report['regressions']=[c['name'] for c in report['cases'] if c['before']['complete'] and not c['after']['complete']]
        Path(os.environ['RKB_ACCEPTANCE_DIR'],'hard6.json').write_text(json.dumps(report,ensure_ascii=False))
        print(json.dumps({k:v for k,v in report.items() if k!='cases'}))
        if report['regressions'] or report['after_complete']!=len(cases) or not all(c['after']['target_present'] for c in report['cases']):raise ValueError('frozen relevance/complete answer regression')
    finally:
        if old:await old.aclose()
        await new.aclose()
if __name__=='__main__':asyncio.run(run())