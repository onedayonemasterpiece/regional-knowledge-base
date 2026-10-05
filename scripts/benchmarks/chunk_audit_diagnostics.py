"""Check matrix ordering and deployed/local encoder agreement before conclusions."""
import argparse,json,time
from pathlib import Path
import numpy as np
from chunk_size_audit import load,dump,norm,sha
from regional_knowledge.e5_service import PinnedEncoder

def main():
    p=argparse.ArgumentParser();p.add_argument('--lab',required=True);p.add_argument('--model-dir',required=True);a=p.parse_args();lab=Path(a.lab)
    items=load(lab/'current.json');cases=load(lab/'gold.json')['cases'];q=np.load(lab/'queries-e5.npy');d=np.load(lab/'current-e5.npy')
    ordered=[x['id'] for did in dict.fromkeys(x['doc'] for x in items) for x in items if x['doc']==did]
    if ordered!=[x['id'] for x in items]:raise ValueError('matrix ordering is wrong')
    encoder=PinnedEncoder(a.model_dir);rows=[]
    for ci,c in enumerate(cases):
        if c['id'] not in ('B01-ru','B01-de','B09-ru','B09-de','G01-ru','G01-de'):continue
        local=np.asarray(encoder.encode([c['query']],'query')[0],dtype=np.float32)
        agreement=float(local@q[ci]);scores=d@q[ci];order=np.argsort(-scores,kind='stable')
        good=[i for i,x in enumerate(items) if x['doc']==c['doc'] and c['page'] in x['pages'] and c['proof'] in norm(x['text'])]
        if not good:raise ValueError('missing diagnostic known span')
        target=good[0];docindices=[i for i,x in enumerate(items) if x['doc']==items[target]['doc']];pos=docindices.index(target);group=docindices[(pos//4)*4:(pos//4)*4+4]
        dv=np.asarray(encoder.encode([items[i]['material'] for i in group],'passage'),dtype=np.float32)[group.index(target)]
        row={'case':c['id'],'deployed_query_local_cosine':agreement,'same_batch_passage_cosine':float(dv@d[target]),'same_batch_self_nearest':items[int(np.argmax(d@dv))]['id']==items[target]['id'],'known_span_rank':int(np.where(order==target)[0][0])+1,'known_span_cosine':float(scores[target]),'top3':[{'title':items[int(i)].get('title'),'doc':items[int(i)]['doc'],'pages':items[int(i)]['pages'],'score':float(scores[int(i)]),'text':items[int(i)]['text'][:400]} for i in order[:3]]}
        rows.append(row)
    result={'matrix_order_check':True,'rows':rows,'local_deployed_agreement':all(x['deployed_query_local_cosine']>.99999 and x['same_batch_passage_cosine']>.99999 for x in rows)}
    dump(lab/'encoder-consistency.json',result);print(json.dumps(result,ensure_ascii=False,indent=2))
if __name__=='__main__':main()
