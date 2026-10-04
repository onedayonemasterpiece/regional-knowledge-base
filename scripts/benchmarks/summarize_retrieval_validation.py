"""Recompute quality/coverage/batch sensitivity from retained matrices, no inference."""
import argparse,hashlib,json
from pathlib import Path
import numpy as np
from retrieval_validation_pool import ranking
from retrieval_validation_metrics import graded_metrics,evidence_recall
from small_embedding_metrics import rrf


def mrr(q,r):
    positive=set(sum(q['evidence_groups'],[]))
    return next((1/(i+1) for i,c in enumerate(r) if c in positive),0)


def quality(qs,ranks):
    qs=[q for q in qs if q['evidence_groups']]
    multi=[q for q in qs if len(q['evidence_groups'])>1]
    return {'n':len(qs),**{f'recall_at_{k}':float(np.mean([evidence_recall(q,ranks[q['id']],k) for q in qs])) for k in (1,5,10)},'mrr':float(np.mean([mrr(q,ranks[q['id']]) for q in qs])),'multi_n':len(multi),'all_evidence_at_10':float(np.mean([evidence_recall(q,ranks[q['id']])==1 for q in multi])) if multi else None}


def main():
    p=argparse.ArgumentParser();p.add_argument('lab',type=Path);p.add_argument('output',type=Path);a=p.parse_args()
    old=json.loads(Path('scripts/benchmarks/small_embedding_questions.json').read_text())['questions'];new=json.loads(Path('scripts/benchmarks/retrieval_validation_questions.v1.json').read_text())['questions'];review=json.loads(Path('scripts/benchmarks/retrieval_validation_original_qrels.v2.json').read_text());revised=json.loads(json.dumps(old))
    for change in review['changes']:next(q for q in revised if q['id']==change['question_id'])['evidence_groups']=change['evidence_groups']
    qs=revised+new;corpus=[json.loads(l) for l in (a.lab/'corpus.jsonl').read_text().splitlines()];ids=[r['id'] for r in corpus];ranks=json.loads((a.output/'rankings.json').read_text());pool=json.loads((a.output/'blind-pool.json').read_text());jf=json.loads(Path('scripts/benchmarks/retrieval_validation_judgments.v1.json').read_text());judgments={q['id']:{x['chunk_id']:x['grade'] for x in jf['judgments'] if x['question_id']==q['id']} for q in qs};pools={q['id']:[x['chunk_id'] for x in pool if x['question_id']==q['id']] for q in qs}
    result={'schema':'rkb-retrieval-validation-summary.v1','corpus_sha256':hashlib.sha256((a.lab/'corpus.jsonl').read_bytes()).hexdigest(),'question_freeze':json.loads((a.output/'questions-freeze.json').read_text()),'pool':{'pairs':len(pool),'judged_pairs':sum(x['chunk_id'] in judgments[x['question_id']] for x in pool),'complete_questions':jf['fully_judged_questions']},'quality':{},'graded':{},'batch':{},'per_query':[]}
    slices={'original_historical':old,'original_review_v2':revised,'new_answerable':[q for q in new if q['status']=='answerable'],'new_multi':[q for q in new if 'multi' in q['classes']],'new_paraphrase':[q for q in new if 'paraphrase' in q['classes']],'original_keyword':[q for q in revised if 'keyword_control' in q['classes']], 'original_natural':[q for q in revised[:24]], 'original_multi':[q for q in revised if 'multi' in q['classes']], 'original_paraphrase':[q for q in revised if 'paraphrase' in q['classes']]}
    for method in ('lexical','e5','gemma','e5_fused','gemma_fused'):
        result['quality'][method]={s:quality(subset,ranks[method]) for s,subset in slices.items()}
        rows={q['id']:{str(k):graded_metrics(ranks[method][q['id']],judgments[q['id']],k=k,pool=pools[q['id']]) for k in (3,5,10)} for q in qs};result['graded'][method]={'per_query':rows,'complete_pool_subset_n':len(jf['fully_judged_questions'])}
        subset=jf['fully_judged_questions'];result['graded'][method]['complete_pool_subset']={str(k):{'precision_direct':float(np.mean([rows[q][str(k)]['precision_direct'] for q in subset])),'precision_useful':float(np.mean([rows[q][str(k)]['precision_useful'] for q in subset])),'ndcg_pooled':float(np.mean([rows[q][str(k)]['ndcg_pooled'] for q in subset]))} for k in (3,5,10)}
    for model in ('e5','gemma'):
        v=np.load(a.output/f'{model}-batch1-document-vectors.npy');queries=np.concatenate([np.load(a.lab/f'{model}-query-vectors.npy'),np.load(a.output/f'{model}-new-query-vectors.npy')]);batch1={q['id']:ranking(v,vec,ids) for q,vec in zip(qs,queries)};stats=json.loads((a.output/f'{model}-batch-consistency.json').read_text());stats['query_count']=len(qs);stats['top1_changed']=sum(batch1[q['id']][0]!=ranks[model][q['id']][0] for q in qs);stats['top10_overlap_mean']=float(np.mean([len(set(batch1[q['id']][:10])&set(ranks[model][q['id']][:10]))/10 for q in qs]));stats['batch1_quality']={s:quality(subset,batch1) for s,subset in slices.items()};stats['batch1_fused_quality']={s:quality(subset,{qid:rrf(r,ranks['lexical'][qid]) for qid,r in batch1.items()}) for s,subset in slices.items()};stats['per_query']=[{'question_id':q['id'],'top1_changed':batch1[q['id']][0]!=ranks[model][q['id']][0],'top10_overlap':len(set(batch1[q['id']][:10])&set(ranks[model][q['id']][:10]))/10,'batch1_recall10':evidence_recall(q,batch1[q['id']]),'batch4_recall10':evidence_recall(q,ranks[model][q['id']]),'known_evidence_ranks':{c:{'batch1':batch1[q['id']].index(c)+1,'batch4':ranks[model][q['id']].index(c)+1} for c in set(sum(q['evidence_groups'],[]))}} for q in qs];result['batch'][model]=stats
    for q in qs:
        row={'id':q['id'],'status':q.get('status','negative' if q.get('unanswerable') else 'answerable'),'classes':q['classes'],'e5_recall10':evidence_recall(q,ranks['e5'][q['id']]),'gemma_recall10':evidence_recall(q,ranks['gemma'][q['id']]),'e5_mrr':mrr(q,ranks['e5'][q['id']]) if q['evidence_groups'] else None,'gemma_mrr':mrr(q,ranks['gemma'][q['id']]) if q['evidence_groups'] else None,'top10_overlap':len(set(ranks['e5'][q['id']][:10])&set(ranks['gemma'][q['id']][:10]))/10}
        if q.get('correction_groups'):row['correction_at_10']={m:bool(set(sum(q['correction_groups'],[]))&set(ranks[m][q['id']][:10])) for m in ('e5','gemma')}
        result['per_query'].append(row)
    positives=slices['new_answerable'];families=sorted({q['family'] for q in positives});rng=np.random.default_rng(20261004);delta=[]
    for family in families:
        fq=[q for q in positives if q['family']==family];delta.append(np.mean([evidence_recall(q,ranks['gemma'][q['id']])-evidence_recall(q,ranks['e5'][q['id']]) for q in fq]))
    boot=np.asarray(delta)[rng.integers(0,len(delta),size=(10000,len(delta)))].mean(axis=1);result['new_family_bootstrap']={'unit':'fact family','families':len(families),'replicates':10000,'seed':20261004,'gemma_minus_e5_recall10':float(np.mean(delta)),'percentile_95_interval':[float(x) for x in np.percentile(boot,[2.5,97.5])],'scope':'model-assisted sparse source-selected labels, not population certainty'}
    mrr_delta=np.asarray([np.mean([mrr(q,ranks['gemma'][q['id']])-mrr(q,ranks['e5'][q['id']]) for q in positives if q['family']==f]) for f in families]); mrr_boot=mrr_delta[rng.integers(0,len(families),size=(10000,len(families)))].mean(axis=1);result['new_family_bootstrap']['gemma_minus_e5_mrr']=float(mrr_delta.mean());result['new_family_bootstrap']['mrr_percentile_95_interval']=[float(x) for x in np.percentile(mrr_boot,[2.5,97.5])]
    (a.output/'validation-summary.json').write_text(json.dumps(result,indent=2)+'\n');public=json.loads(json.dumps(result))
    for method, data in public['graded'].items():
        rows=data['per_query'];data['new_answerable_coverage']={str(k):{'judgment_coverage_mean':float(np.mean([rows[q['id']][str(k)]['judgment_coverage'] for q in positives])), 'direct_precision_macro_bounds':[float(np.mean([rows[q['id']][str(k)]['precision_direct_bounds'][i] for q in positives])) for i in (0,1)], 'exact_precision_questions':sum(rows[q['id']][str(k)]['precision_direct'] is not None for q in positives)} for k in (3,5,10)}
        data['per_query']={qid:row for qid,row in rows.items() if qid in jf['fully_judged_questions']}
    Path('docs/reports/embedding-retrieval-validation-followup-20261004.summary.json').write_text(json.dumps(public,indent=2)+'\n')
    print(json.dumps({k:result[k] for k in ('pool','quality','new_family_bootstrap')},indent=2))
if __name__=='__main__':main()
