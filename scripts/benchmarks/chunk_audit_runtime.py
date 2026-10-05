"""Measure inference and indexed lexical latency without changing production data."""
from __future__ import annotations
import argparse,json,os,re,shutil,sqlite3,time,urllib.request
from pathlib import Path
import numpy as np
from chunk_size_audit import load,dump,stats,sha,ro
from regional_knowledge.e5_contract import SPACE,validate_vector


def queries(lab):
    dest=lab/'queries-e5.npy'
    if dest.exists():print('queries-e5.npy already exists; preserved');return
    vectors=[];wall=[];encoder=[];queues=[]
    for case in load(lab/'gold.json')['cases']:
        body=json.dumps({'space':SPACE,'role':'query','texts':[case['query']]}).encode()
        req=urllib.request.Request('http://127.0.0.1:8767/embed',data=body,headers={'Content-Type':'application/json'},method='POST')
        t=time.monotonic()
        with urllib.request.urlopen(req,timeout=6) as r:payload=json.load(r)
        wall.append(time.monotonic()-t)
        if payload.get('space')!=SPACE:raise ValueError('space mismatch')
        vectors.append(validate_vector(payload['vectors'][0]));encoder.append(payload['encoder_seconds']);queues.append(payload['queue_wait_seconds'])
        time.sleep(.05)
    tmp=dest.with_suffix('.npy.new')
    with tmp.open('wb') as f:np.save(f,np.asarray(vectors,dtype=np.float32))
    tmp.chmod(0o600);os.replace(tmp,dest)
    result={'queries':len(vectors),'loopback_wall_seconds':stats(wall),'encoder_seconds':stats(encoder),'queue_seconds':stats(queues),'matrix_sha256':sha(dest.read_bytes()),'load_note':'Measured while isolated offline E5 indexing benchmark may be using another CPU; not idle-host latency.'}
    dump(lab/'e5-query-times.json',result);print(json.dumps(result,indent=2))


def fts(lab,sizes):
    items=load(lab/'c1000.json');cases=load(lab/'gold.json')['cases']
    controls=[('rare_de','Kulmer Handfeste'),('frequent_de','Recht'),('very_frequent_de','und'),('rare_ru','Альбертина'),('two_ru','Кёнигсберг университет')]
    out={'method':'SQLite FTS5 unicode61 AND BM25, with selected document/revision join and LIMIT 100. Expanded sizes repeat the actual 3-source text with distinct IDs: storage/latency stress, not quality evidence or a realistic diverse-corpus growth model. Per-size cold means first application query after build, not an OS cache eviction.','sqlite_version':sqlite3.sqlite_version,'sizes':{}}
    for count in sizes:
        if shutil.disk_usage(lab).free<2*2**30:raise RuntimeError('insufficient disk for bounded lexical stress; no cleanup attempted')
        path=lab/f'fts-scale-{count}.sqlite';first=not path.exists();db=sqlite3.connect(path);db.execute('pragma cache_size=-32768')
        if first:
            db.executescript("create table chunk_text(rowid integer primary key,chunk_id text unique,document_id text,revision integer,search_material text);create index doc_rev on chunk_text(document_id,revision);create virtual table chunk_fts using fts5(search_material,content='chunk_text',content_rowid='rowid',tokenize='unicode61');")
            t=time.monotonic()
            with db:
                db.executemany('insert into chunk_text values(?,?,?,?,?)',((i+1,str(i),f'doc-{i//600}',1,items[i%len(items)]['material']) for i in range(count)))
                db.execute("insert into chunk_fts(chunk_fts) values('rebuild')")
            build=time.monotonic()-t
        else:build=None
        db.execute('create temp table allowed(document_id text primary key,revision integer)')
        db.execute('insert into allowed select distinct document_id,revision from chunk_text')
        sql='select t.chunk_id from chunk_fts join chunk_text t on t.rowid=chunk_fts.rowid join allowed a on a.document_id=t.document_id and a.revision=t.revision where chunk_fts match ? order by bm25(chunk_fts),t.chunk_id limit 100'
        rows=[]
        for label,query in controls+[(c['id'],c['query']) for c in cases]:
            expression=' AND '.join('"'+t+'"' for t in re.findall(r'[^\W_]+',query,flags=re.UNICODE)[:64])
            times=[];n=0
            for _ in range(4):
                t=time.monotonic();result=db.execute(sql,(expression,)).fetchall();times.append(time.monotonic()-t);n=len(result)
            rows.append({'label':label,'results':n,'first_seconds':times[0],'warm_seconds':stats(times[1:])})
        naturals=rows[len(controls):]
        out['sizes'][str(count)]={'rows':count,'bytes':path.stat().st_size,'build_seconds':build,'controls':rows[:len(controls)],'natural_queries':len(naturals),'natural_nonempty':sum(r['results']>0 for r in naturals),'natural_warm_p50_per_query_seconds':stats([r['warm_seconds']['p50'] for r in naturals]),'plan':[list(r) for r in db.execute('explain query plan '+sql,('"Kulmer" AND "Handfeste"',)).fetchall()]}
        db.close();path.chmod(0o600);dump(lab/'fts-scale.json',out);print(json.dumps({'size':count,**out['sizes'][str(count)]},ensure_ascii=False,indent=2),flush=True)


def main():
    p=argparse.ArgumentParser();p.add_argument('phase',choices=['queries','fts']);p.add_argument('--lab',required=True);p.add_argument('--sizes',default='2250,10000,100000');a=p.parse_args();lab=Path(a.lab)
    if not (lab/'.artifact.json').exists():raise ValueError('managed lab required')
    if a.phase=='queries':queries(lab)
    else:fts(lab,[int(v) for v in a.sizes.split(',')])
if __name__=='__main__':main()
