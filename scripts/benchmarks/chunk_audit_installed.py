"""Compare installed owner-scoped E5 vectors with the unchanged-text lab baseline."""
import argparse,json
from pathlib import Path
import numpy as np
from chunk_size_audit import load,dump,norm,stats

def main():
    p=argparse.ArgumentParser();p.add_argument('--lab',required=True);a=p.parse_args();lab=Path(a.lab)
    items=load(lab/'current.json');installed=load(lab/'installed-e5-private.json');dv=np.load(lab/'current-e5.npy');qv=np.load(lab/'queries-e5.npy')
    indexes=[i for i,x in enumerate(items) if x['id'] in installed['vectors']]
    v=np.asarray([installed['vectors'][items[i]['id']]['vector'] for i in indexes],dtype=np.float32)
    local=dv[indexes];cos=np.sum(v*local,axis=1)
    rows=[]
    for ci,c in enumerate(load(lab/'gold.json')['cases']):
        if c['split']!='dev' or c['doc'] not in installed['active']:continue
        proof={j for j,i in enumerate(indexes) if items[i]['doc']==c['doc'] and c['page'] in items[i]['pages'] and c['proof'] in norm(items[i]['text'])}
        old=np.argsort(-(v@qv[ci]));new=np.argsort(-(local@qv[ci]))
        rows.append({'id':c['id'],'installed_rank':next((k for k,j in enumerate(old,1) if j in proof),None),'lab_same_scope_rank':next((k for k,j in enumerate(new,1) if j in proof),None),'top10_overlap':len(set(old[:10])&set(new[:10]))/10})
    out={'installed_rows':len(indexes),'cosine_installed_vs_lab':stats(cos.tolist()),'under_0999':int(np.sum(cos<.999)),'under_099':int(np.sum(cos<.99)),'dev_queries_only':rows,'production_read_only':True,'note':'Same source bytes/space; batching order is not assumed identical. Re-encoding reproducibility was checked separately.'}
    dump(lab/'installed-parity.json',out);print(json.dumps(out,indent=2))
if __name__=='__main__':main()
