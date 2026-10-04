"""Actual Kaggle/E5 encoding and ACL-aware seven-mode ranking ablation.

Private fixtures/vectors/per-case IDs remain managed evidence. Unknown pooled
relevance is not silently scored as an independently judged negative.
"""
import argparse,asyncio,json,math,os,time,uuid,hashlib
from pathlib import Path
from operator_env import load_service_env
from accept_fast_e5 import principal,distribution
from regional_knowledge.bge_queue import BgeQueue
from regional_knowledge.bge_contract import SPACE as BGE_SPACE
from regional_knowledge.e5_contract import SPACE as E5_SPACE
from regional_knowledge.multilingual_retrieval import wait_result
from regional_knowledge.rank_fusion import MODES,fuse
from regional_knowledge.supabase_backend import backend_from_env

def metrics(cases,rankings):
    positive=[case for case in cases if not case['unanswerable']]
    result={'answerable':len(positive),'recall':{str(k):sum(sum(bool(set(group)&set(rankings[case['id']][:k])) for group in case['evidence_groups'])/len(case['evidence_groups']) for case in positive)/len(positive) for k in (1,5,10)},'mrr':sum(next((1/(i+1) for i,chunk in enumerate(rankings[case['id']]) if chunk in sum(case['evidence_groups'],[])),0) for case in positive)/len(positive)}
    multi=[case for case in positive if len(case['evidence_groups'])>1]
    result['multi_complete_at10']=sum(all(set(group)&set(rankings[case['id']][:10]) for group in case['evidence_groups']) for case in multi);result['multi_count']=len(multi)
    result['unsupported']={case['id']:{'returned_count':len(rankings[case['id']][:10]),'generated_answer':False,'abstention_calibrated':False} for case in cases if case['unanswerable']}
    return result

async def encode(backend,actor,fixture,output):
    queue=BgeQueue(os.environ['RKB_BGE_QUEUE_PATH']);rows=json.loads(output.read_text()) if output.exists() else {}
    for case in fixture['cases']:
        started=time.monotonic()
        query_hash=hashlib.sha256(case['query'].encode()).hexdigest()
        if rows.get(case['id'],{}).get('query_sha256')==query_hash:continue
        async def bge():
            job=await asyncio.to_thread(queue.enqueue,actor.subject,'ablation-query:'+str(uuid.uuid4()),[case['query']],identity={'fixture_case':case['id']})
            result=await wait_result(queue,actor.subject,job,60)
            if not result:raise TimeoutError('BGE benchmark query pending')
            return result['result']
        # Execute the independent encoders concurrently; raw vectors are only
        # passed to their matching spaces, never compared to each other.
        e5,main=await asyncio.gather(backend.embedder.embed(case['query']),bge())
        rows[case['id']]={'query_sha256':query_hash,'e5':e5,'bge':main['vectors'][0],'bge_queue_seconds':main['queue_seconds'],'bge_encoder_seconds':main['encoder_seconds'],'dual_wall_seconds':time.monotonic()-started}
        output.write_text(json.dumps(rows));print('encoded',case['id'],flush=True)

async def ablate(backend,actor,fixture,vectors,output):
    result={'modes':{},'scope':'all ACL-visible active chunks; explicit matching BGE/E5 spaces','labels':'source-verified positive evidence; pooled unjudged candidates remain unknown','per_case':{},'database_seconds':[]}
    rankings={mode:{} for mode in MODES};alias_rankings={};branch_counts={}
    literal=lambda vector:'['+','.join(format(value,'.9g') for value in vector)+']'
    for case in fixture['cases']:
        started=time.monotonic();vector=vectors[case['id']]
        if vector['query_sha256']!=hashlib.sha256(case['query'].encode()).hexdigest():raise ValueError('stale benchmark query encoding')
        response=await backend.client.post(f'{backend.config.url.rstrip("/")}/rest/v1/rpc/rkb_multilingual_rankings',headers=backend._headers(actor),json={'query_text':case['query'],'bge_vector':literal(vector['bge']),'bge_space':BGE_SPACE,'e5_vector':literal(vector['e5']),'e5_space':E5_SPACE,'aliases':fixture.get('aliases',[]) if any('name' in name or 'poi' in name for name in case['classes']) else [],'depth':100})
        response.raise_for_status();rows=response.json();result['database_seconds'].append(time.monotonic()-started)
        branch_counts[case['id']]={branch:sum(row['branch']==branch for row in rows) for branch in ('e5','bge','lexical','exact_current_alias','exact_historical_alias','exact_alias')}
        diagnostics={}
        for mode,branches in MODES.items():rankings[mode][case['id']],diagnostics[mode]=fuse(rows,branches,limit=20)
        alias_rankings[case['id']],alias_signals=fuse(rows,['bge','lexical','exact_current_alias','exact_historical_alias','exact_alias'],limit=20)
        result['per_case'][case['id']]={'rankings':{mode:rankings[mode][case['id']] for mode in MODES},'branch_counts':branch_counts[case['id']],'bge_alias_diagnostics':alias_signals}
    for mode,ranking in rankings.items():
        result['modes'][mode]={'all':metrics(fixture['cases'],ranking),'languages':{language:metrics([case for case in fixture['cases'] if case['query_language']==language],ranking) for language in ('de','ru')},'graph_cases':metrics([case for case in fixture['cases'] if any('person' in label or 'participant' in label for label in case['classes'])],ranking)}
        result['modes'][mode]['families']={name:metrics([case for case in fixture['cases'] if ('short_entity_query' in case['classes'])==short],ranking) for name,short in [('natural_questions',False),('short_entity_queries',True)]}
    result['alias_fusion']=metrics(fixture['cases'],alias_rankings)
    result['top10_overlap']={mode:sum(len(set(ranking[case['id']][:10])&set(rankings['bge'][case['id']][:10]))/10 for case in fixture['cases'])/len(fixture['cases']) for mode,ranking in rankings.items()}
    paired=[case for case in fixture['cases'] if case.get('pair') and case['query_language']=='de' and not case['unanswerable']]
    result['paired_language_gap']={}
    for mode,ranking in rankings.items():
        de=metrics(paired,ranking)
        ru_cases=[next(other for other in fixture['cases'] if other.get('pair')==case['pair'] and other['query_language']=='ru') for case in paired]
        ru=metrics(ru_cases,ranking)
        result['paired_language_gap'][mode]={'needs':len(paired),'de':de,'ru':ru,'recall10_de_minus_ru':de['recall']['10']-ru['recall']['10'],'mrr_de_minus_ru':de['mrr']-ru['mrr']}
        natural_de=[case for case in paired if 'short_entity_query' not in case['classes']]
        natural_ru=[next(other for other in fixture['cases'] if other.get('pair')==case['pair'] and other['query_language']=='ru') for case in natural_de]
        natural_de_score=metrics(natural_de,ranking);natural_ru_score=metrics(natural_ru,ranking)
        result['paired_language_gap'][mode]['natural_question_pairs']={'needs':len(natural_de),'de':natural_de_score,'ru':natural_ru_score,'recall10_de_minus_ru':natural_de_score['recall']['10']-natural_ru_score['recall']['10'],'mrr_de_minus_ru':natural_de_score['mrr']-natural_ru_score['mrr']}
    result['database_distribution_seconds']=distribution(result['database_seconds']);output.write_text(json.dumps(result,indent=2));print(json.dumps({mode:values['all'] for mode,values in result['modes'].items()}))

async def run(args):
    load_service_env();backend=backend_from_env();actor=principal();fixture=json.loads(args.fixture.read_text())
    try:
        if args.phase=='encode':await encode(backend,actor,fixture,args.vectors)
        else:await ablate(backend,actor,fixture,json.loads(args.vectors.read_text()),args.output)
    finally:await backend.aclose()

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('phase',choices=['encode','ablate']);parser.add_argument('fixture',type=Path);parser.add_argument('vectors',type=Path);parser.add_argument('output',type=Path);asyncio.run(run(parser.parse_args()))
