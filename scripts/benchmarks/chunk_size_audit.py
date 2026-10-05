"""Reproducible, private, read-only corpus experiment; no production index writes.

Phases: prepare -> gold (freeze before query inference/ranking) -> e5 -> assess.
Corpus/queries/vectors stay in a managed retained directory, never in Git.
Character-window variants hold source text and page boundaries constant. They
are controlled size experiments, NOT an automatic replacement for semantic review.
"""
from __future__ import annotations
import argparse, collections, hashlib, json, os, re, shutil, sqlite3, subprocess, sys, time, unicodedata
from contextlib import closing
from pathlib import Path
from typing import Any

os.environ.update(OMP_NUM_THREADS='1', OPENBLAS_NUM_THREADS='1', MKL_NUM_THREADS='1', NUMEXPR_NUM_THREADS='1', TOKENIZERS_PARALLELISM='false', HF_HUB_OFFLINE='1')

def norm(text: str) -> str:
    text=unicodedata.normalize('NFKC',text).replace('\ufffe','').replace('\u00ad','')
    text=re.sub(r'(?<=[^\W\d_])[-=]\s*\n\s*(?=[^\W\d_])','',text,flags=re.UNICODE)
    return ' '.join(text.split())

def sha(value: bytes) -> str:return hashlib.sha256(value).hexdigest()

def dump(path: Path, data: Any) -> None:
    raw=json.dumps(data,ensure_ascii=False,sort_keys=True,indent=2).encode()
    tmp=path.with_suffix(path.suffix+'.new')
    with tmp.open('wb') as f:f.write(raw);f.flush();os.fsync(f.fileno())
    tmp.chmod(0o600);os.replace(tmp,path)

def load(path: Path) -> Any:return json.loads(path.read_text())

def ro(path: str|Path) -> sqlite3.Connection:
    db=sqlite3.connect('file:'+str(Path(path).resolve())+'?mode=ro',uri=True,timeout=5)
    db.row_factory=sqlite3.Row
    return db

def stats(values: list[float|int]) -> dict:
    import numpy as np
    if not values:return {'n':0}
    return {'n':len(values),'min':float(min(values)),'p50':float(np.percentile(values,50)),'p95':float(np.percentile(values,95)),'max':float(max(values)),'mean':float(np.mean(values))}

def tokens(path: str|Path):
    from tokenizers import Tokenizer
    tok=Tokenizer.from_file(str(path));tok.no_truncation();tok.no_padding();return tok

def split_size(text: str, cap: int, tok=None) -> list[tuple[int,int]]:
    # Preserves every non-whitespace source character; never splits a word.
    points=[0];pos=0
    while pos<len(text):
        if tok is None:
            end=min(len(text),pos+cap)
        else:
            rest=text[pos:];enc=tok.encode('passage: '+rest)
            if len(enc.ids)<=cap:end=len(text)
            else:
                offsets=[b-9 for a,b in enc.offsets[1:cap-1] if b>9]
                end=pos+max(offsets)
        if end<len(text):
            lower=pos+max(1,int((end-pos)*.55))
            # Avoid common German scholarly abbreviations as sentence ends.
            candidates=[]
            for m in re.finditer(r'[.!?;:]\s+',text[lower:end]):
                upto=text[:lower+m.start()+1]
                if re.search(r'(?:\b(?:S|No|Nr|Art|Bd|Th|St|Dr|vgl|resp|bzw)|\d)\.$',upto,re.I):continue
                candidates.append(lower+m.end())
            if candidates:end=candidates[-1]
            else:
                boundary=text.rfind(' ',pos+1,end)
                if boundary>pos:end=boundary+1
        if end<=pos:raise ValueError('chunker did not advance')
        points.append(end);pos=end
    return [(a,b) for a,b in zip(points,points[1:]) if text[a:b].strip()]

def prepare(args):
    if shutil.disk_usage('/home/dev').free<100*2**20:raise RuntimeError('need at least 100 MiB free; no downloads or cleanup attempted')
    result=subprocess.run(['/home/dev/.local/bin/dev-artifacts','new','regional-knowledge-base','vector-only-chunk-audit-20261005','--retain','--reason','User-requested chunk-size and vector-only retrieval audit'],text=True,capture_output=True,check=True)
    options=[Path(x.strip()) for x in result.stdout.splitlines() if x.strip().startswith('/home/dev/artifacts/')]
    if len(options)!=1:raise RuntimeError('managed artifact path not returned')
    lab=options[0];lab.chmod(0o700)
    selected=dict(item.rsplit(':',1) for item in args.documents.split(','))
    pages=[];current=[];meta=[]
    with closing(ro(args.database)) as db:
        user=db.execute("select payload from corpus_rows where table_name='rkb_users' and row_key=?",(args.actor,)).fetchone()
        if not user or json.loads(user[0]).get('status')!='active':raise PermissionError('active connected owner required')
        for did,revision in selected.items():
            doc=json.loads(db.execute("select payload from corpus_rows where table_name='rkb_documents' and row_key=?",(did,)).fetchone()[0])
            if doc['owner_user_id']!=args.actor:raise PermissionError('selected document not owned by actor')
            rev=int(revision)
            meta.append({k:doc.get(k) for k in ('id','title','language','active_revision','page_count','source_sha256')}|{'sampled_revision':rev})
            prows=[json.loads(r[0]) for r in db.execute("select payload from corpus_rows where table_name='rkb_pages' and document_id=? and revision=?",(did,rev))]
            by_page={p['id']:p for p in prows}
            for p in sorted(prows,key=lambda p:p['physical_page_index']):
                regions=[json.loads(r[0]) for r in db.execute("select payload from corpus_rows where table_name='rkb_regions' and json_extract(payload,'$.page_id')=?",(p['id'],))]
                regions.sort(key=lambda r:(r['reading_order'],r['id']))
                text=norm('\n'.join(r.get('source_text','') for r in regions if r['kind'] in ('body','heading','caption','footnote','table','marginalia')))
                if text:pages.append({'doc':did,'revision':rev,'page':p['physical_page_index'],'text':text,'region_kinds':dict(collections.Counter(r['kind'] for r in regions))})
            rows=db.execute('select t.*,c.payload from chunk_text t join corpus_rows c on c.table_name=\'rkb_chunks\' and c.row_key=t.chunk_id where t.document_id=? and t.revision=? order by t.rowid',(did,rev)).fetchall()
            for row in rows:
                c=json.loads(row['payload']);source=row['source_text'];material=row['search_material']
                if sha(source.encode())!=row['text_sha256'] or sha(material.encode())!=row['search_material_sha256']:raise ValueError('source/hash mismatch')
                current.append({'id':row['chunk_id'],'doc':did,'revision':rev,'pages':[by_page[p]['physical_page_index'] for p in c.get('page_ids',[]) if p in by_page],'text':source,'material':material,'title':c.get('title',''),'source_hash':row['text_sha256'],'material_hash':row['search_material_sha256']})
    dump(lab/'source-pages.json',pages);dump(lab/'current.json',current)
    e5=tokens(args.e5_tokenizer);bge=tokens(args.bge_tokenizer)
    variants={'current':current}
    for key,cap,tokenizer in [('c700',700,None),('c1000',1000,None),('c1400',1400,None),('c1800',1800,None),('t256',256,e5)]:
        items=[]
        for p in pages:
            for k,(a,b) in enumerate(split_size(p['text'],cap,tokenizer)):
                text=p['text'][a:b].strip()
                items.append({'id':f'{p["doc"]}:{p["page"]}:{key}:{k}','doc':p['doc'],'revision':p['revision'],'pages':[p['page']],'text':text,'material':text,'start':a,'end':b})
        variants[key]=items;dump(lab/(key+'.json'),items)
    summary={}
    for key,items in variants.items():
        e5_len=[len(e5.encode('passage: '+x['material']).ids) for x in items]
        bge_len=[len(bge.encode(x['material']).ids) for x in items]
        summary[key]={'chunks':len(items),'chars':stats([len(x['text']) for x in items]),'e5_tokens':stats(e5_len),'bge_tokens':stats(bge_len),'e5_over512':sum(n>512 for n in e5_len),'bge_over512':sum(n>512 for n in bge_len),'by_document':{d['id']:{'chunks':sum(x['doc']==d['id'] for x in items),'e5_over512':sum(n>512 for x,n in zip(items,e5_len) if x['doc']==d['id']),'bge_over512':sum(n>512 for x,n in zip(items,bge_len) if x['doc']==d['id'])} for d in meta}}
    manifest={'created_at':time.time(),'actor':args.actor,'documents':meta,'source_sha256':sha((lab/'source-pages.json').read_bytes()),'git_sha':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),'variants':list(variants),'scope':'Selected owned revisions, including explicitly pending Brünneck. No production mutation. Character variants normalize whitespace and preserve page boundaries; no claim of semantic reimport.','e5_tokenizer_sha256':sha(Path(args.e5_tokenizer).read_bytes()),'bge_tokenizer_sha256':sha(Path(args.bge_tokenizer).read_bytes()),'shape':summary}
    dump(lab/'manifest.json',manifest)
    print(json.dumps({'lab':str(lab),'documents':meta,'shape':summary},ensure_ascii=False,indent=2))

def gold(args):
    lab=Path(args.lab)
    if (lab/'gold.json').exists():raise RuntimeError('gold already frozen; use a separately versioned fixture instead')
    page_map={(p['doc'],p['page']):p['text'] for p in load(lab/'source-pages.json')}
    data=[]
    for raw in args.case:
        case=json.loads(raw)
        proof=norm(case['proof']);source=page_map.get((case['doc'],case['page']),'')
        if proof not in source:raise ValueError('gold evidence not found: '+case['id'])
        if len(proof)<25:raise ValueError('proof must be a meaningful preselected span')
        case['proof']=proof;case['proof_source_sha256']=sha(source.encode());data.append(case)
    ids=[x['id'] for x in data]
    if len(ids)!=len(set(ids)) or len(ids)<8:raise ValueError('unique >=8 gold cases required')
    dump(lab/'gold.json',{'frozen_at':time.time(),'source_sha256':load(lab/'manifest.json')['source_sha256'],'method':'Model-authored natural questions with exact pre-ranking known sufficient source spans. Same fact/language variants share family and split. Known-proof retrieval, not exhaustive precision or generated-answer correctness.','cases':data})
    print(json.dumps({'gold_cases':len(data),'families':len({x['family'] for x in data}),'sha256':sha((lab/'gold.json').read_bytes()),'splits':dict(collections.Counter(x['split'] for x in data))}))

def e5_encode(args):
    import numpy as np
    from regional_knowledge.e5_service import PinnedEncoder
    lab=Path(args.lab);model=PinnedEncoder(args.model_dir)
    fixture=load(lab/'gold.json');keys=args.variants.split(',') if args.variants else load(lab/'manifest.json')['variants']
    output={}
    for key in keys:
        items=load(lab/(key+'.json'));file=lab/(key+'-e5.npy');times=[];begin=time.monotonic()
        if file.exists():
            matrix=np.load(file);assert matrix.shape==(len(items),384)
            print(json.dumps({'phase':'e5_reused','variant':key,'chunks':len(items)}),flush=True);continue
        arrays=[]
        # Ordered per-document batch4 is part of the pinned INT8 space.
        for did in dict.fromkeys(x['doc'] for x in items):
            part=[x for x in items if x['doc']==did]
            for start in range(0,len(part),4):
                texts=[x['material'] for x in part[start:start+4]];t=time.monotonic()
                arrays.extend(model.encode(texts,'passage'));times.append(time.monotonic()-t)
        matrix=np.asarray(arrays,dtype=np.float32)
        assert matrix.shape==(len(items),384) and np.isfinite(matrix).all()
        tmp=file.with_suffix('.npy.new')
        with tmp.open('wb') as f:np.save(f,matrix)
        tmp.chmod(0o600);os.replace(tmp,file)
        output[key]={'chunks':len(items),'seconds':time.monotonic()-begin,'batch_times':stats(times),'vector_sha256':sha(file.read_bytes())}
        dump(lab/'e5-progress.json',output)
        print(json.dumps({'phase':'e5_encoded','variant':key,**output[key]}),flush=True)
    qfile=lab/'queries-e5.npy'
    if not qfile.exists():
        vectors=[];times=[]
        for case in fixture['cases']:
            t=time.monotonic();vectors.append(model.encode([case['query']],'query')[0]);times.append(time.monotonic()-t)
        with qfile.open('wb') as f:np.save(f,np.asarray(vectors,dtype=np.float32))
        qfile.chmod(0o600);dump(lab/'e5-query-times.json',stats(times))
    print(json.dumps({'phase':'e5_complete','variants':keys,'queries':len(fixture['cases'])}),flush=True)

def lexical(items,cases):
    db=sqlite3.connect(':memory:')
    db.execute("create virtual table ft using fts5(text,tokenize='unicode61')")
    db.executemany('insert into ft(rowid,text) values(?,?)',((i+1,x['material']) for i,x in enumerate(items)))
    rankings=[];times=[]
    for case in cases:
        terms=re.findall(r'[^\W_]+',case['query'],flags=re.UNICODE)[:64]
        expression=' AND '.join('"'+t+'"' for t in terms)
        t=time.monotonic();rows=db.execute('select rowid from ft where ft match ? order by bm25(ft),rowid limit 100',(expression,)).fetchall();times.append(time.monotonic()-t)
        rankings.append([r[0]-1 for r in rows])
    db.close();return rankings,times

def metrics(rows):
    if not rows:return {}
    out={'n':len(rows)}
    for k in (1,3,5,10):out['known_proof_hit@'+str(k)]=sum(x['rank'] is not None and x['rank']<=k for x in rows)/len(rows)
    out['mrr@10']=sum(1/x['rank'] if x['rank'] is not None and x['rank']<=10 else 0 for x in rows)/len(rows)
    return out

def assess(args):
    import numpy as np
    lab=Path(args.lab);cases=load(lab/'gold.json')['cases'];results={};details={}
    for key in (args.variants.split(',') if args.variants else load(lab/'manifest.json')['variants']):
        items=load(lab/(key+'.json'));bodies=[norm(x['text']) for x in items]
        relevant=[{i for i,x in enumerate(items) if x['doc']==c['doc'] and c['page'] in x['pages'] and c['proof'] in bodies[i]} for c in cases]
        lex,lex_times=lexical(items,cases)
        rankings={'lexical':lex};timings={};arrays={}
        for model in ('e5','bge'):
            dfile=lab/(key+'-'+model+'.npy');qfile=lab/('queries-'+model+'.npy')
            if dfile.exists() and qfile.exists():
                d=np.load(dfile);q=np.load(qfile)
                if d.shape[0]!=len(items) or q.shape[0]!=len(cases):raise ValueError('matrix/corpus alignment mismatch')
                if not np.isfinite(d).all() or not np.isfinite(q).all():raise ValueError('nonfinite vectors')
                dr=[];ts=[]
                for v in q:
                    t=time.monotonic();scores=d@v;idx=np.argsort(-scores,kind='stable');ts.append(time.monotonic()-t);dr.append(idx[:100].tolist())
                rankings[model]=dr;timings[model]=stats(ts)
        def fusion(branches):
            out=[]
            for q in range(len(cases)):
                scores={}
                for b in branches:
                    for rank,i in enumerate(rankings[b][q],1):scores[i]=scores.get(i,0)+1/(60+rank)
                out.append(sorted(scores,key=lambda i:(-scores[i],items[i]['id']))[:100])
            return out
        if 'e5' in rankings:rankings['e5_lexical']=fusion(['e5','lexical'])
        if 'bge' in rankings:rankings['bge_lexical']=fusion(['bge','lexical'])
        if all(m in rankings for m in ('e5','bge')):
            rankings['e5_bge']=fusion(['e5','bge']);rankings['e5_bge_lexical']=fusion(['e5','bge','lexical'])
        methods={}
        for method,ranks in rankings.items():
            rows=[]
            for c,good,ordered in zip(cases,relevant,ranks):
                position=next((rank for rank,i in enumerate(ordered,1) if i in good),None)
                rows.append({'id':c['id'],'family':c['family'],'split':c['split'],'language':c['language'],'document':c['doc'],'rank':position,'proof_exists_in_any_single_chunk':bool(good),'top10_ids':[items[i]['id'] for i in ordered[:10]],'top10_characters':sum(len(items[i]['text']) for i in ordered[:10])})
            methods[method]={'all':metrics(rows),'by_split':{s:metrics([r for r in rows if r['split']==s]) for s in ('dev','test')},'by_language':{s:metrics([r for r in rows if r['language']==s]) for s in ('ru','de')},'by_document':{d:metrics([r for r in rows if r['document']==d]) for d in dict.fromkeys(r['document'] for r in rows)}}
            details[key+':'+method]=rows
        results[key]={'methods':methods,'query_dot_sort_seconds':timings,'lexical_seconds':stats(lex_times),'lexical_nonempty':sum(bool(r) for r in lex),'proof_available':sum(bool(r) for r in relevant),'total_cases':len(cases)}
    dump(lab/'results.json',results);dump(lab/'per-query.json',details)
    print(json.dumps(results,ensure_ascii=False,indent=2))

def main():
    p=argparse.ArgumentParser();p.add_argument('phase',choices=['prepare','gold','e5','assess']);p.add_argument('--lab');p.add_argument('--database');p.add_argument('--actor');p.add_argument('--documents');p.add_argument('--e5-tokenizer');p.add_argument('--bge-tokenizer');p.add_argument('--model-dir');p.add_argument('--variants');p.add_argument('--case',action='append',default=[]);args=p.parse_args()
    if args.phase!='prepare' and not args.lab:p.error('--lab required')
    {'prepare':prepare,'gold':gold,'e5':e5_encode,'assess':assess}[args.phase](args)
if __name__=='__main__':main()
