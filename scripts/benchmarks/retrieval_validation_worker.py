"""Narrow follow-up: reuse weights/batch4; encode new queries and batch1 only."""
import os
for key in ('OMP_NUM_THREADS','OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','NUMEXPR_NUM_THREADS'):os.environ[key]='1'
os.environ['TOKENIZERS_PARALLELISM']='false'
import argparse,json,hashlib,time
from pathlib import Path
import numpy as np
from small_embedding_worker import Encoder

def main():
 p=argparse.ArgumentParser();p.add_argument('lab',type=Path);p.add_argument('output',type=Path);p.add_argument('candidate',choices=['e5','gemma']);a=p.parse_args()
 f=Path('scripts/benchmarks/retrieval_validation_questions.v1.json');freeze=json.loads((a.output/'questions-freeze.json').read_text());assert hashlib.sha256(f.read_bytes()).hexdigest()==freeze['sha256']
 corpus=[json.loads(l) for l in (a.lab/'corpus.jsonl').read_text().splitlines()];assert hashlib.sha256((a.lab/'corpus.jsonl').read_bytes()).hexdigest()==json.loads(f.read_text())['corpus_sha256']
 cg=Path('/sys/fs/cgroup')/Path('/proc/self/cgroup').read_text().strip().split('::')[-1].lstrip('/');limits={k:(cg/k).read_text().strip() for k in ('memory.max','memory.swap.max','cpu.max')};assert limits['memory.max']=='1073741824' and limits['memory.swap.max']=='0' and int(limits['cpu.max'].split()[0])<=int(limits['cpu.max'].split()[1])
 e=Encoder(a.candidate,a.lab);queries=json.loads(f.read_text())['questions'];qvec=np.concatenate([e.encode([q['query']]) for q in queries]);np.save(a.output/f'{a.candidate}-new-query-vectors.npy',qvec)
 start=time.monotonic();vectors=[]
 for i,r in enumerate(corpus):
  vectors.append(e.encode([r['text']],query=False,titles=[r['title']]))
  if i%100==0:print(a.candidate,i,flush=True)
 v=np.concatenate(vectors);np.save(a.output/f'{a.candidate}-batch1-document-vectors.npy',v);saved=np.load(a.lab/f'{a.candidate}-document-vectors.npy');cos=(v*saved).sum(axis=1)/(np.linalg.norm(v,axis=1)*np.linalg.norm(saved,axis=1));diff=np.abs(v-saved)
 result={'candidate':a.candidate,'status':'success','freeze_sha256':freeze['sha256'],'limits':limits,'contract':e.contract,'model':json.loads((a.lab/'models'/a.candidate/'manifest.json').read_text()),'batch1_seconds':time.monotonic()-start,'max_component_difference':float(diff.max()),'min_cosine':float(cos.min()),'cosine_p50':float(np.median(cos)),'fixed_tolerances':{'component_max':.002,'cosine_min':.999},'compatibility_pass':bool(diff.max()<=.002 and cos.min()>=.999),'memory_peak_bytes':int((cg/'memory.peak').read_text())}
 (a.output/f'{a.candidate}-batch-consistency.json').write_text(json.dumps(result,indent=2));print(a.candidate,'done',result['max_component_difference'],result['min_cosine'],flush=True)
if __name__=='__main__':main()
