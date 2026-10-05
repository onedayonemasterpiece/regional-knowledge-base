"""Private BGE size audit through the existing pinned CPU worker.

Uses owner-scoped cached vectors when byte-identical. New inference is bounded to
8 outstanding document jobs (2 queries); no corpus/revision/index/ACL mutation.
Results remain in the managed audit. Retries reuse deterministic per-text keys.
"""
from __future__ import annotations
import argparse,collections,json,os,sqlite3,time
from pathlib import Path
import numpy as np
from chunk_size_audit import load,dump,ro,sha,stats
from regional_knowledge.bge_contract import SPACE,validate_vector
from regional_knowledge.bge_queue import BgeQueue


def main():
    p=argparse.ArgumentParser();p.add_argument('--lab',required=True);p.add_argument('--queue',required=True);p.add_argument('--database',required=True);p.add_argument('--variants',default='current,c1000');p.add_argument('--seconds',type=int,default=3000);a=p.parse_args()
    lab=Path(a.lab);manifest=load(lab/'manifest.json');actor=manifest['actor']
    with ro(a.database) as db:
        user=db.execute("select payload from corpus_rows where table_name='rkb_users' and row_key=?",(actor,)).fetchone()
        if not user or json.loads(user[0])['status']!='active':raise PermissionError('active owner required')
        for document in manifest['documents']:
            row=json.loads(db.execute("select payload from corpus_rows where table_name='rkb_documents' and row_key=?",(document['id'],)).fetchone()[0])
            if row['owner_user_id']!=actor or row['source_sha256']!=document['source_sha256']:raise PermissionError('source ownership/identity changed')
    cases=load(lab/'gold.json')['cases'];sets={'queries':[{'material':c['query']} for c in cases]}
    sets.update({key:load(lab/(key+'.json')) for key in a.variants.split(',') if key})
    wanted={(('query' if key=='queries' else 'document'),sha(x['material'].encode())) for key,items in sets.items() for x in items}
    cache={};timings={};reused=0
    with ro(a.queue) as db:
        for row in db.execute("select kind,texts,result from jobs where actor=? and state='done' and result is not null order by updated",(actor,)):
            texts=json.loads(row['texts']);result=json.loads(row['result'])
            key=(row['kind'],sha(texts[0].encode())) if len(texts)==1 else None
            if key in wanted and result.get('space')==SPACE and len(result.get('vectors',[]))==1:
                cache[key]=validate_vector(result['vectors'][0]);timings[key]={k:result.get(k) for k in ('encoder_seconds','queue_seconds')}
    reused=len(cache);queue=BgeQueue(a.queue);deadline=time.monotonic()+a.seconds;submitted=0;completed=0
    report={'space':SPACE,'gold_sha256':sha((lab/'gold.json').read_bytes()),'cache_hits':reused,'variants':{},'started_at':time.time(),'new_submitted':0,'completed':0}
    pending={};seq=[]
    for role in ('query','document'):
        for key in sorted(wanted):
            if key[0]!=role or key in cache:continue
            text=next(x['material'] for sk,items in sets.items() if (sk=='queries')==(role=='query') for x in items if sha(x['material'].encode())==key[1])
            seq.append((key,text))
    index=0;last_write=0
    def save():
        nonlocal last_write
        for name,items in sets.items():
            role='query' if name=='queries' else 'document';keys=[(role,sha(x['material'].encode())) for x in items]
            n=sum(k in cache for k in keys);report['variants'][name]={'required':len(keys),'ready':n}
            file=lab/(name+'-bge.npy')
            if n==len(keys) and not file.exists():
                data=np.asarray([cache[k] for k in keys],dtype=np.float32)
                if data.shape!=(len(keys),1024) or not np.isfinite(data).all():raise ValueError('invalid BGE matrix')
                tmp=file.with_suffix('.npy.new')
                with tmp.open('wb') as f:np.save(f,data)
                tmp.chmod(0o600);os.replace(tmp,file)
                report['variants'][name]['matrix_sha256']=sha(file.read_bytes())
        report.update(new_submitted=submitted,completed=completed,pending=len(pending),elapsed_seconds=a.seconds-max(0,deadline-time.monotonic()),worker=queue.status())
        report['query_encoder_seconds']=stats([v['encoder_seconds'] for k,v in timings.items() if k[0]=='query' and v.get('encoder_seconds') is not None])
        report['document_encoder_seconds']=stats([v['encoder_seconds'] for k,v in timings.items() if k[0]=='document' and v.get('encoder_seconds') is not None])
        dump(lab/'bge-progress.json',report);last_write=time.monotonic()
        print(json.dumps(report,ensure_ascii=False),flush=True)
    save()
    while (index<len(seq) or pending) and time.monotonic()<deadline:
        while index<len(seq) and len(pending)<(2 if seq[index][0][0]=='query' else 8):
            key,text=seq[index]
            if key[0]=='document' and queue.document_pending()>=48:break
            identity={'audit':'chunk-size-20261005','material_sha256':key[1],'space':SPACE}
            ident=queue.enqueue(actor,'audit-chunk-size:'+key[0]+':'+key[1],[text],kind=key[0],identity=identity)
            pending[key]=ident;submitted+=1;index+=1
        for key,ident in list(pending.items()):
            row=queue.result(actor,ident)
            if row['state']=='done':
                r=row['result']
                if r.get('space')!=SPACE:raise ValueError('space mismatch')
                cache[key]=validate_vector(r['vectors'][0]);timings[key]={k:r.get(k) for k in ('encoder_seconds','queue_seconds')};pending.pop(key);completed+=1
        if time.monotonic()-last_write>=30:save()
        if pending or index<len(seq):time.sleep(.5)
    save()
    if any(v['ready']!=v['required'] for v in report['variants'].values()):
        print('BOUNDED_RUN_INCOMPLETE: resume with the same lab; no quality metric may use a partial matrix',flush=True)
        raise SystemExit(2)
if __name__=='__main__':main()
