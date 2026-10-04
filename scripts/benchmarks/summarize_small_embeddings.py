"""Consolidate measured candidate files; never create or estimate missing numbers."""
import argparse
import hashlib
import json
import importlib.metadata
import shutil
import subprocess
import numpy as np
from pathlib import Path
from small_embedding_metrics import metrics, rrf


def quantile(values, p):
    values=sorted(values);index=(len(values)-1)*p
    lo=int(index);hi=min(lo+1,len(values)-1)
    return values[lo]+(values[hi]-values[lo])*(index-lo)


def main():
    parser=argparse.ArgumentParser();parser.add_argument('root',type=Path);args=parser.parse_args();root=args.root
    fixture=json.loads(Path('scripts/benchmarks/small_embedding_questions.json').read_text());questions=fixture['questions']
    lexical=json.loads((root/'lexical.json').read_text())
    result={'schema_version':1,'host':json.loads((root/'host.json').read_text()),'corpus':{'document_id':fixture['document_id'],'revision':fixture['revision'],'chunks':712,'physical_pages':174,'sha256':fixture['corpus_sha256'],'private_snapshot':str(root/'corpus.jsonl')},'questions':{'count':len(questions),'natural_answerable':sum(not q['unanswerable'] and 'keyword_control' not in q['classes'] for q in questions),'keyword_controls':sum('keyword_control' in q['classes'] for q in questions),'negative_controls':sum(q['unanswerable'] for q in questions),'fixture_sha256':hashlib.sha256(Path('scripts/benchmarks/small_embedding_questions.json').read_bytes()).hexdigest()},'fusion':{'method':'RRF','k':60,'branch_depth':100,'weights':[1,1],'tie_break':'chunk_id ascending'},'candidates':{},'lexical':{},'artifact_retention':'retained, pending review'}
    groups={'all':questions,'natural':[q for q in questions if 'keyword_control' not in q['classes']],'keyword_controls':[q for q in questions if 'keyword_control' in q['classes']]}
    result['lexical']['quality']={key:metrics(subset,lexical['rankings']) for key,subset in groups.items()}
    times=lexical['query_seconds'];result['lexical']['latency']={'n':len(times),'p50_seconds':quantile(times,.5),'p95_seconds':quantile(times,.95),'max_seconds':max(times)}
    result['lexical']['empty_count']=sum(not r for r in lexical['rankings'].values())
    for name in ['e5','gemma','potion']:
        data=json.loads((root/f'{name}-result.json').read_text())
        assert data['status']=='success',f'{name} incomplete: {data["status"]}'
        doc_vectors=np.load(root/f'{name}-document-vectors.npy')
        query_vectors=np.load(root/f'{name}-query-vectors.npy')
        corpus=[json.loads(line) for line in (root/'corpus.jsonl').read_text().splitlines()]
        ids=[row['id'] for row in corpus]
        scores=query_vectors @ doc_vectors.T
        vr={q['id']:[ids[j] for j in sorted(range(len(ids)),key=lambda j:(-float(scores[i,j]),ids[j]))] for i,q in enumerate(questions)}
        fr={qid:rrf(ranking,lexical['rankings'][qid]) for qid,ranking in vr.items()}
        data['quality_groups']={mode:{key:metrics(subset,ranks) for key,subset in groups.items()} for mode,ranks in [('vector',vr),('fused',fr)]}
        for mode in ['vector','fused']:
            # CPU reductions on a matrix batch can differ in final ULPs; retain
            # the worker's original scalar-query metric and expose revalidation.
            assert abs(data['quality_groups'][mode]['all']['mrr']-data['quality'][mode]['mrr'])<1e-6
        if (root/f'{name}-live-result.json').exists():
            extra=json.loads((root/f'{name}-live-result.json').read_text());assert extra['status']=='success'
            data['live_run']={k:extra[k] for k in ['load_seconds','loaded_memory','first_embedding_seconds','process_start_to_first_embedding_seconds','launch_to_first_embedding_seconds','query_latency','local_retrieval_latency','full_retrieval_latency','concurrent_4_latency','cgroup','memory','ru_maxrss_bytes']}
            data['compatibility']=extra['compatibility']
        data['runtime']['package_versions']={package:importlib.metadata.version(package) for package in ['numpy','tokenizers','onnxruntime','model2vec','psutil','psycopg']}
        data['best_safe_batch_size']=min(data['throughput'],key=lambda run:run['seconds'])['batch_size']
        data['quality_document_batch_size']=data['throughput'][-1]['batch_size']
        manifest=data['model'];data['model_files_bytes']=sum(f['bytes'] for f in manifest['files'])
        data['disk_bytes']=sum(p.stat().st_size for p in (root/'models'/name).rglob('*') if p.is_file())
        data['token_lengths']=json.loads((root/'token-lengths.json').read_text())[name]
        rss_peaks=[data['ru_maxrss_bytes']]+[v['peak_rss_bytes'] for v in data['memory'].values()]
        cgroup_peaks=[data['cgroup']['memory_peak_bytes']]
        if 'live_run' in data:
            rss_peaks.append(data['live_run']['ru_maxrss_bytes']);cgroup_peaks.append(data['live_run']['cgroup']['memory_peak_bytes'])
        data['observed_peak_rss_bytes']=max(rss_peaks);data['observed_cgroup_peak_bytes']=max(cgroup_peaks)
        latency=data.get('live_run',data)['query_latency'];full=data.get('live_run',data).get('full_retrieval_latency')
        data['gates']={'hard_memory_1GiB':data['observed_cgroup_peak_bytes']<2**30,'observed_rss_1GiB':data['observed_peak_rss_bytes']<2**30,'loaded_rss_under_800MiB':data['loaded_memory']['rss_bytes']<800*2**20,'warm_query_p95_under_1s':latency['p95_seconds']<=1,'full_retrieval_p95_under_2s':None if full is None else full['p95_seconds']<=2,'crash_free_measured_successful_loader':True}
        result['candidates'][name]=data
    result['candidates']['potion']['default_loader_failure']={'systemd_unit':'rkb-bench-potion-1791073460','result':'oom-kill','exit_signal':9,'journal_memory_peak':'1G','memory_peak_bytes':1073741824,'cpu_seconds':17.080359,'journal_evidence':str(root/'potion-default-oom.log'),'status':'failed; mmap variant measured separately'}
    result['disk']={'shared_runtime_files_bytes':sum(p.stat().st_size for p in (root/'venv').rglob('*') if p.is_file() and not p.is_symlink()),'shared_runtime_du_bytes':int(subprocess.check_output(['du','-sb',str(root/'venv')],text=True).split()[0]),'model_cache_files_bytes':sum(c['disk_bytes'] for c in result['candidates'].values()),'free_bytes':shutil.disk_usage(root).free,'cache_directory':str(root/'models')}
    output=root/'devcoveer-small-embeddings-20261004.json';output.write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n');print(output)

if __name__=='__main__':main()
