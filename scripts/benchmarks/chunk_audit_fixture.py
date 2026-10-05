"""Fixture/control helpers for the private size experiment; no production writes."""
from __future__ import annotations
import argparse,collections,json,time
from pathlib import Path
from types import SimpleNamespace
from chunk_size_audit import load,dump,norm,gold,stats,tokens,sha

def main():
 p=argparse.ArgumentParser();p.add_argument('phase',choices=['controls','freeze','inspect']);p.add_argument('--lab',required=True);p.add_argument('--family',action='append',default=[]);p.add_argument('--e5-tokenizer');p.add_argument('--bge-tokenizer');p.add_argument('--pages');p.add_argument('--document');p.add_argument('--file');a=p.parse_args();lab=Path(a.lab)
 if a.phase=='controls':
  items=load(lab/'current.json');clean=[{**x,'text':norm(x['text']),'material':norm(x['material'])} for x in items];dump(lab/'current_norm.json',clean)
  m=load(lab/'manifest.json');m['variants']+=['current_norm'] if 'current_norm' not in m['variants'] else []
  e=tokens(a.e5_tokenizer);b=tokens(a.bge_tokenizer)
  en=[len(e.encode('passage: '+x['material']).ids) for x in clean];bn=[len(b.encode(x['material']).ids) for x in clean]
  m['shape']['current_norm']={'chunks':len(clean),'chars':stats([len(x['text']) for x in clean]),'e5_tokens':stats(en),'bge_tokens':stats(bn),'e5_over512':sum(x>512 for x in en),'bge_over512':sum(x>512 for x in bn)}
  m['normalization_control']='Same current chunk boundaries and metadata; NFKC, whitespace and line-hyphen normalization only. Separates preprocessing from size.';dump(lab/'manifest.json',m)
  print(json.dumps({k:{kk:v for kk,v in val.items() if kk!='by_document'} for k,val in m['shape'].items()},indent=2))
 elif a.phase=='freeze':
  cases=[]
  for raw in a.family:
   f=json.loads(raw)
   for lang in ('ru','de'):
    if f.get(lang):cases.append(json.dumps({'id':f['id']+'-'+lang,'family':f['id'],'doc':f['doc'],'page':f['page'],'split':f['split'],'query':f[lang],'language':lang,'proof':f['proof']},ensure_ascii=False))
  gold(SimpleNamespace(lab=a.lab,case=cases))
 elif a.pages:
  wanted={int(x) for x in a.pages.split(',')};rows=[x for x in load(lab/'source-pages.json') if x['doc']==a.document and x['page'] in wanted]
  print(json.dumps(rows,ensure_ascii=False,indent=2))
 else:
  allowed={'manifest.json','e5-progress.json','e5-query-times.json','results.json','per-query.json','bge-progress.json','runtime-read.json','fts-scale.json','bge-current-shape.json'}
  if a.file not in allowed:raise ValueError('unsupported bounded audit report')
  data=load(lab/a.file)
  if a.file=='manifest.json':data={k:data[k] for k in ('created_at','source_sha256','shape','scope')}
  print(json.dumps(data,ensure_ascii=False,indent=2))
if __name__=='__main__':main()
