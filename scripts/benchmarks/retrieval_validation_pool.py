"""Create source/rank-blind pools; private passages stay in managed evidence."""
import argparse,hashlib,json
from pathlib import Path
import numpy as np
from small_embedding_metrics import rrf

def ranking(vectors,q,ids):
 scores=vectors@q
 return sorted(ids,key=lambda x:(-float(scores[ids.index(x)]),x))

def main():
 p=argparse.ArgumentParser();p.add_argument('lab',type=Path);p.add_argument('output',type=Path);a=p.parse_args();corpus=[json.loads(l) for l in (a.lab/'corpus.jsonl').read_text().splitlines()];ids=[r['id'] for r in corpus];by={r['id']:r for r in corpus}
 old=json.loads(Path('scripts/benchmarks/small_embedding_questions.json').read_text())['questions'];new=json.loads(Path('scripts/benchmarks/retrieval_validation_questions.v1.json').read_text())['questions'];qs=old+new;lex={**json.loads((a.lab/'lexical.json').read_text())['rankings'],**json.loads((a.output/'lexical.json').read_text())['rankings']};ranks={'lexical':lex}
 for model in ('e5','gemma'):
  d=np.load(a.lab/f'{model}-document-vectors.npy');q=np.concatenate([np.load(a.lab/f'{model}-query-vectors.npy'),np.load(a.output/f'{model}-new-query-vectors.npy')]);v={x['id']:ranking(d,v,ids) for x,v in zip(qs,q)};ranks[model]=v;ranks[model+'_fused']={qid:rrf(r,lex[qid]) for qid,r in v.items()}
 (a.output/'rankings.json').write_text(json.dumps(ranks))
 pool=[]
 for q in qs:
  candidates=set(sum([ranks[m][q['id']][:10] for m in ('e5','gemma','lexical')],[]))
  for cid in candidates:
   key=hashlib.sha256((q['id']+':'+cid).encode()).hexdigest()[:16];r=by[cid];pool.append({'blind_id':key,'question_id':q['id'],'query':q['query'],'chunk_id':cid,'physical_pages':[p['physical_page_index']+1 for p in r['pages']],'text':r['text']})
 pool.sort(key=lambda x:x['blind_id']);(a.output/'blind-pool.json').write_text(json.dumps(pool,ensure_ascii=False,indent=2));(a.output/'blind-pool-freeze.json').write_text(json.dumps({'sha256':hashlib.sha256((a.output/'blind-pool.json').read_bytes()).hexdigest(),'pairs':len(pool),'models_or_ranks_in_pool':False},indent=2));print('pool',len(pool),'unique passages',len({x['chunk_id'] for x in pool}))
if __name__=='__main__':main()
