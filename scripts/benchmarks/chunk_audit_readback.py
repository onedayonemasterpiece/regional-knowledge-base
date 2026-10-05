"""Read only this task's retained evidence; never reads credentials."""
from __future__ import annotations
import argparse,json,shutil
from pathlib import Path

def main():
    p=argparse.ArgumentParser();p.add_argument('--lab');p.add_argument('--section',default='summary');a=p.parse_args()
    root=Path('/home/dev/artifacts/regional-knowledge-base')
    labs=sorted(root.glob('*-vector-only-chunk-audit-20261005'))
    if not a.lab:
        print(json.dumps({'disk_free':shutil.disk_usage(root).free,'labs':[str(x) for x in labs]},indent=2));return
    lab=Path(a.lab)
    if lab not in labs:raise ValueError('not this managed audit')
    def load(name):return json.loads((lab/name).read_text())
    print('LAB',str(lab))
    if a.section in ('summary','status'):
        print('FILES',json.dumps({x.name:x.stat().st_size for x in lab.iterdir() if x.is_file()},sort_keys=True))
        names=('manifest.json','gold.json','e5-progress.json','e5-query-times.json') if a.section=='summary' else ('e5-progress.json','bge-progress.json','selection.json')
        for name in names:
            if not (lab/name).exists():continue
            obj=load(name)
            if name=='manifest.json':obj={k:obj[k] for k in ('documents','shape')}
            print(name,json.dumps(obj,ensure_ascii=False,indent=2))
    elif a.section=='scores':
        for split in ('dev','test','all'):
            file='score-'+split+'.json'
            if not (lab/file).exists():continue
            print(split,json.dumps({k:v['aggregate'] for k,v in load(file)['results'].items()},indent=2))
    elif a.section=='review':
        gold={c['id']:c for c in load('gold.json')['cases'] if c['split']=='dev'}
        ranked=load('details-dev.json')['current:e5'];items={x['id']:x for x in load('current.json')}
        for row in ranked:
            if row['id'] not in ('B01-ru','B09-ru','G01-ru','G03-ru'):continue
            case=gold[row['id']]
            print(json.dumps({'case':case,'known_rank':row['rank'],'top3':[{'id':ident,'doc':items[ident]['doc'],'pages':items[ident]['pages'],'text':items[ident]['text']} for ident in row['top10_ids'][:3]]},ensure_ascii=False,indent=2))
    else:
        names={'gold':'gold.json','results':'results.json','per-query':'per-query.json','runtime':'runtime-read.json','fts':'fts-scale.json','bge':'bge-progress.json','consistency':'encoder-consistency.json','parity':'installed-parity.json'}
        print(json.dumps(load(names[a.section]),ensure_ascii=False,indent=2))
if __name__=='__main__':main()
