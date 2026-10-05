"""Export only allowlisted anonymous aggregates, never corpus or question text."""
from __future__ import annotations
import argparse,collections,hashlib,json,re
from pathlib import Path
from chunk_size_audit import load,dump


def anonymized(lab: Path) -> dict:
    m=load(lab/'manifest.json');gold=load(lab/'gold.json');docs=m['documents']
    names={d['id']:f'source_{chr(65+i)}' for i,d in enumerate(docs)}
    result={'fixture':{'version':1,'queries':len(gold['cases']),'fact_families':len({c['family'] for c in gold['cases']}),'by_split':dict(collections.Counter(c['split'] for c in gold['cases'])),'labels':'Pre-ranking source-verified exact seed spans; not exhaustive relevance or generated-answer accuracy.'},'corpus':[{ 'label':names[d['id']],'language':d['language'],'physical_pages':d['page_count'],'sampled_revision_was_active':d['sampled_revision']==d['active_revision']} for d in docs],'shape':{},'scores':{}}
    for key,s in m['shape'].items():
        result['shape'][key]={k:v for k,v in s.items() if k!='by_document'}
        if s.get('by_document'):result['shape'][key]['by_source']={names[d]:v for d,v in s['by_document'].items()}
    for split in ('dev','test','all'):
        file=lab/('score-'+split+'.json')
        if not file.exists():continue
        methods={}
        for key,v in load(file)['results'].items():
            by={names[d]+':query_'+lang:val for doclang,val in v['by_doc_lang'].items() for d,lang in [doclang.rsplit(':',1)]}
            methods[key]={'aggregate':v['aggregate'],'by_source_and_query_language':by,'ranking_seconds':v['ranking_seconds'],'chunks':v['chunks']}
        result['scores'][split]=methods
    if (lab/'selection.json').exists():
        s=load(lab/'selection.json');result['selection']={k:s[k] for k in ('variant','reason','selected_at')}
    if (lab/'fts-scale.json').exists():result['fts_scale']=load(lab/'fts-scale.json')
    if (lab/'runtime-read.json').exists():
        r=load(lab/'runtime-read.json');result['runtime']={k:r[k] for k in ('all_candidate_count','all_candidate_uuid_json_bytes','installed_e5_rows','timings','vector_plan','errors') if k in r}
        result['runtime']['dense_queries']=[{k:v for k,v in q.items() if k!='id'} for q in r.get('queries',[])]
        result['runtime']['lexical']=r.get('lexical',[])
    if (lab/'e5-query-times.json').exists():result['e5_query_timings']=load(lab/'e5-query-times.json')
    if (lab/'bge-progress.json').exists():
        r=load(lab/'bge-progress.json');result['bge']={k:r[k] for k in ('space','variants','query_encoder_seconds','document_encoder_seconds') if k in r}
    if (lab/'encoder-consistency.json').exists():
        r=load(lab/'encoder-consistency.json');result['consistency']={'matrix_order_check':r['matrix_order_check'],'local_deployed_agreement':r['local_deployed_agreement'],'sampled_cases':len(r['rows'])}
    if (lab/'installed-parity.json').exists():
        r=load(lab/'installed-parity.json');result['installed_parity']={k:r[k] for k in ('installed_rows','cosine_installed_vs_lab','under_0999','under_099','note')}
        result['installed_parity']['query_top10_overlaps']=[r['top10_overlap'] for r in r['dev_queries_only']]
    if (lab/'diagnostic-judgments.json').exists():
        r=load(lab/'diagnostic-judgments.json');result['diagnostic_judgments']={'pairs':len(r['rows']),'cases':len({x['case'] for x in r['rows']}),'grade_counts':dict(collections.Counter(x['grade'] for x in r['rows'])),'method':r['method']}
    for key,name in [('bootstrap','bootstrap.json'),('hybrid_ablation','hybrid-ablation.json')]:
        if (lab/name).exists():result[key]=load(lab/name)
    matrix_files=[f'{v}-e5.npy' for v in m['variants']]+['queries-e5.npy','current-bge.npy','queries-bge.npy']
    result['matrix_files']={name:{'bytes':(lab/name).stat().st_size,'sha256':hashlib.sha256((lab/name).read_bytes()).hexdigest()} for name in matrix_files if (lab/name).exists()}
    result['complete_selected_experiment']=all((lab/name).exists() for name in matrix_files) and all(s in result['scores'] for s in ('dev','test','all'))
    result['bge_size_grid_tested']=False
    encoded=json.dumps(result,ensure_ascii=False)
    if re.search(r'\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b',encoded):raise ValueError('private UUID in export')
    for d in docs:
        if d['title'] in encoded or d['source_sha256'] in encoded:raise ValueError('private source identity in export')
    for c in gold['cases']:
        if c['query'] in encoded or c['proof'] in encoded:raise ValueError('private query/proof in export')
    return result


def main():
    p=argparse.ArgumentParser();p.add_argument('--lab',required=True);p.add_argument('--output',required=True);a=p.parse_args()
    target=Path(a.output).resolve();allowed=(Path.cwd()/'docs/reports').resolve()
    if not target.is_relative_to(allowed) or target.suffix!='.json':raise ValueError('aggregate output must be docs/reports/*.json')
    target.parent.mkdir(parents=True,exist_ok=True);result=anonymized(Path(a.lab));dump(target,result)
    print(json.dumps({'path':str(target),'bytes':target.stat().st_size,'splits':list(result['scores']),'variants':list(result['shape']),'private_values_excluded':True,'complete':result['complete_selected_experiment']}))
if __name__=='__main__':main()
