"""Dense-only evaluation with pre-frozen source-span labels and held-out families.

Ranking is exact normalized dot product over the entire experiment corpus. It
never imports/calls lexical search, aliases, query expansion or a reranker.
Unknown alternative relevant passages are not negative labels; these are
known-proof retrieval rates, not exhaustive precision or answer correctness.
"""
from __future__ import annotations
import argparse,collections,json,time
from pathlib import Path
from chunk_size_audit import load,dump,norm,sha,stats


def covers(intervals,start,end):
    cursor=start
    for a,b in sorted(intervals):
        if b<=cursor:continue
        if a>cursor:return False
        cursor=max(cursor,b)
        if cursor>=end:return True
    return False


def retrieve_metric(items,order,case,page_text,budget=5000):
    proof=case['proof'];at=page_text.index(proof);end=at+len(proof)
    singles=set();pieces={}
    for i,x in enumerate(items):
        if x['doc']!=case['doc'] or case['page'] not in x['pages']:continue
        if proof in norm(x['text']):singles.add(i)
        if 'start' in x and len(x['pages'])==1 and x['pages'][0]==case['page']:
            a=max(x['start'],at);b=min(x['end'],end)
            if a<b:pieces[i]=(a,b)
    def hit(selected):
        return bool(singles.intersection(selected)) or covers([pieces[i] for i in selected if i in pieces],at,end)
    rank=next((j for j,i in enumerate(order,1) if i in singles),None)
    selected=[];size=0
    for i in order:
        n=len(items[i]['text'])
        if size+n>budget:break
        selected.append(i);size+=n
    return {'id':case['id'],'family':case['family'],'split':case['split'],'language':case['language'],'document':case['doc'],'rank':rank,'single_proof_exists':bool(singles),'single_hit1':rank==1,'single_hit5':rank is not None and rank<=5,'single_hit10':rank is not None and rank<=10,'span_hit1':hit(order[:1]),'span_hit5':hit(order[:5]),'span_hit10':hit(order[:10]),'span_hit5000chars':hit(selected),'chars_top5':sum(len(items[i]['text']) for i in order[:5]),'budget_chunks':len(selected),'budget_used':size,'top10_ids':[items[i]['id'] for i in order[:10]]}


def summary(rows):
    if not rows:return {'n':0}
    keys=('single_hit1','single_hit5','single_hit10','span_hit1','span_hit5','span_hit10','span_hit5000chars','single_proof_exists')
    result={'n':len(rows),'families':len({r['family'] for r in rows})}
    result.update({k:sum(bool(r[k]) for r in rows)/len(rows) for k in keys})
    result['mrr10']=sum(1/r['rank'] if r['rank'] and r['rank']<=10 else 0 for r in rows)/len(rows)
    result['mean_top5_chars']=sum(r['chars_top5'] for r in rows)/len(rows)
    return result


def evaluate(lab,variants,split):
    import numpy as np
    allcases=load(lab/'gold.json')['cases'];positions=[i for i,c in enumerate(allcases) if split=='all' or c['split']==split];cases=[allcases[i] for i in positions]
    pages={(p['doc'],p['page']):p['text'] for p in load(lab/'source-pages.json')};results={};details={}
    for variant in variants:
        items=load(lab/(variant+'.json'));orders={};times={}
        for model in ('e5','bge'):
            dp=lab/(variant+'-'+model+'.npy');qp=lab/('queries-'+model+'.npy')
            if not dp.exists() or not qp.exists():continue
            d=np.load(dp);q=np.load(qp)[positions]
            if d.shape[0]!=len(items) or q.shape[1]!=d.shape[1]:raise ValueError('matrix alignment mismatch')
            if not np.isfinite(d).all() or not np.isfinite(q).all():raise ValueError('nonfinite matrix')
            if np.max(np.abs(np.linalg.norm(d,axis=1)-1))>.003:raise ValueError('not normalized')
            ranking=[];timing=[]
            for vector in q:
                t=time.monotonic();r=np.argsort(-(d@vector),kind='stable');timing.append(time.monotonic()-t);ranking.append(r[:100].tolist())
            orders[model]=ranking;times[model]=stats(timing)
        if 'e5' in orders and 'bge' in orders:
            fusion=[]
            for e,b in zip(orders['e5'],orders['bge']):
                sums={}
                for branch in (e,b):
                    for rank,i in enumerate(branch,1):sums[i]=sums.get(i,0)+1/(60+rank)
                fusion.append(sorted(sums,key=lambda i:(-sums[i],items[i]['id']))[:100])
            orders['e5_bge']=fusion
        for method,ranking in orders.items():
            rows=[retrieve_metric(items,order,c,pages[c['doc'],c['page']]) for c,order in zip(cases,ranking)]
            key=variant+':'+method;details[key]=rows
            results[key]={'aggregate':summary(rows),'by_doc_lang':{doc+':'+lang:summary([r for r in rows if r['document']==doc and r['language']==lang]) for doc in dict.fromkeys(c['doc'] for c in cases) for lang in ('ru','de')},'ranking_seconds':times.get(method),'chunks':len(items)}
    out={'split':split,'gold_sha256':sha((lab/'gold.json').read_bytes()),'method':'dense-only exact cosine, no lexical/aliases/expansion; single-proof and union-of-ranked-source-spans; 5000-character retrieval budget supplementary metric','results':results}
    dump(lab/('score-'+split+'.json'),out);dump(lab/('details-'+split+'.json'),details)
    print(json.dumps(out,ensure_ascii=False,indent=2))


def main():
    p=argparse.ArgumentParser();p.add_argument('--lab',required=True);p.add_argument('--variants',default='current,current_norm,c700,c1000,c1400,c1800,t256');p.add_argument('--split',choices=['dev','test','all'],default='dev');p.add_argument('--select');p.add_argument('--reason');a=p.parse_args();lab=Path(a.lab)
    if a.select:
        if (lab/'selection.json').exists():raise ValueError('selection already frozen')
        if not a.reason:raise ValueError('selection reason required')
        dump(lab/'selection.json',{'variant':a.select,'reason':a.reason,'selected_at':time.time(),'gold_sha256':sha((lab/'gold.json').read_bytes()),'dev_result_sha256':sha((lab/'score-dev.json').read_bytes())})
        print('selection frozen');return
    if a.split!='dev' and not (lab/'selection.json').exists():raise ValueError('freeze selection from development results before reading held-out rankings')
    evaluate(lab,a.variants.split(','),a.split)
if __name__=='__main__':main()
