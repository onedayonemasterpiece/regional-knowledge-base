"""Post-selection lexical ablation and paired-family uncertainty, no inference."""
from __future__ import annotations
import argparse,json
from pathlib import Path
import numpy as np
from chunk_size_audit import load,dump,lexical,stats
from chunk_audit_score import retrieve_metric,summary


def main():
    p=argparse.ArgumentParser();p.add_argument('--lab',required=True);a=p.parse_args();lab=Path(a.lab)
    selection=load(lab/'selection.json')['variant'];fixture=load(lab/'gold.json');cases=fixture['cases']
    pages={(p['doc'],p['page']):p['text'] for p in load(lab/'source-pages.json')}
    aggregate={}
    for key in dict.fromkeys(['current',selection]):
        items=load(lab/(key+'.json'));lex,times=lexical(items,cases);rankings={'lexical_only':lex}
        for model in ('e5','bge'):
            dfile=lab/(key+'-'+model+'.npy');qfile=lab/('queries-'+model+'.npy')
            if not dfile.exists() or not qfile.exists():continue
            d=np.load(dfile);q=np.load(qfile)
            if d.shape[0]!=len(items) or q.shape[0]!=len(cases):raise ValueError('complete aligned matrix required')
            rankings[model]=[np.argsort(-(d@v),kind='stable')[:100].tolist() for v in q]
        def fuse(branches):
            output=[]
            for qi in range(len(cases)):
                scores={}
                for branch in branches:
                    for rank,i in enumerate(rankings[branch][qi],1):scores[i]=scores.get(i,0)+1/(60+rank)
                output.append(sorted(scores,key=lambda i:(-scores[i],items[i]['id']))[:100])
            return output
        for model in ('e5','bge'):
            if model in rankings:rankings[model+'_lexical']=fuse([model,'lexical_only'])
        if all(m in rankings for m in ('e5','bge')):
            rankings['e5_bge']=fuse(['e5','bge']);rankings['e5_bge_lexical']=fuse(['e5','bge','lexical_only'])
        out={}
        for name,ranks in rankings.items():
            rows=[retrieve_metric(items,r,c,pages[c['doc'],c['page']]) for r,c in zip(ranks,cases)]
            out[name]={s:summary([r for r in rows if s=='all' or r['split']==s]) for s in ('dev','test','all')}
        aggregate[key]={'chunks':len(items),'lexical_nonempty':sum(bool(r) for r in lex),'lexical_seconds':stats(times),'methods':out}
    dump(lab/'hybrid-ablation.json',aggregate)
    data=load(lab/'details-test.json');rng=np.random.default_rng(20261005);boot={}
    for name in [selection+':e5','current:bge','current:e5_bge']:
        if name not in data or name=='current:e5':continue
        left={r['id']:r for r in data['current:e5']};right={r['id']:r for r in data[name]}
        if set(left)!=set(right):raise ValueError('paired cases mismatch')
        families=sorted({r['family'] for r in left.values()})
        for metric in ('single_hit5','single_hit10','span_hit5000chars','mrr10'):
            def value(r):return (1/r['rank'] if r['rank'] and r['rank']<=10 else 0) if metric=='mrr10' else float(r[metric])
            differences=np.asarray([np.mean([value(right[k])-value(r) for k,r in left.items() if r['family']==f]) for f in families])
            samples=differences[rng.integers(0,len(families),size=(10000,len(families)))].mean(axis=1)
            boot[name+':'+metric]={'families':len(families),'estimate':float(differences.mean()),'percentile95':[float(x) for x in np.percentile(samples,[2.5,97.5])]}
    result={'method':'Paired fact-family bootstrap, 10000 resamples, seed20261005, held-out only. RU/DE versions remain clustered. Intervals describe this small source-selected fixture, not population performance. Comparisons are each named method minus current E5.','comparisons':boot}
    dump(lab/'bootstrap.json',result)
    print(json.dumps({'hybrid':aggregate,'bootstrap':result},ensure_ascii=False,indent=2))
if __name__=='__main__':main()
