"""Store explicit model-reviewed diagnostic qrels separately from frozen seed labels."""
import argparse,json,time
from pathlib import Path
from chunk_size_audit import load,dump,sha

def main():
    p=argparse.ArgumentParser();p.add_argument('--lab',required=True);p.add_argument('--row',action='append',default=[]);a=p.parse_args();lab=Path(a.lab)
    cases={c['id']:c for c in load(lab/'gold.json')['cases']};items={x['id']:x for x in load(lab/'current.json')}
    path=lab/'diagnostic-judgments.json';data=load(path) if path.exists() else {'method':'Post-ranking ChatGPT semantic review, not independent/blinded ground truth. Separate from frozen seed metrics. Grade 0 does not answer, 1 useful background, 2 sufficient answer evidence at requested granularity. No unjudged pair is assigned zero.','rows':[]}
    existing={(r['case'],r['chunk']):r for r in data['rows']}
    for raw in a.row:
        r=json.loads(raw);c=cases[r['case']];item=items[r['chunk']]
        if c['split']!='dev' and not (lab/'selection.json').exists():raise ValueError('holdout not open')
        if r['grade'] not in (0,1,2) or not r.get('rationale'):raise ValueError('explicit semantic judgment required')
        r.update(source_sha256=sha(item['text'].encode()),reviewed_at=time.time());existing[r['case'],r['chunk']]=r
    data['rows']=list(existing.values());dump(path,data)
    print(json.dumps({'judged_pairs':len(data['rows']),'cases':len({r['case'] for r in data['rows']}),'grade2':sum(r['grade']==2 for r in data['rows'])}))
if __name__=='__main__':main()
