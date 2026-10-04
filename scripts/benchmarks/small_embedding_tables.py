"""Render tables directly from reviewed measurements (missing is never zero)."""
import json
import sys
from pathlib import Path


def number(value,scale=1,digits=2):
    return '—' if value is None else f'{value*scale:.{digits}f}'


def main():
    root=Path(sys.argv[1]);d=json.loads((root/'devcoveer-small-embeddings-20261004.json').read_text())
    lines=[]
    def table(header,rows):
        lines.append('| '+' | '.join(header)+' |')
        lines.append('| '+' | '.join('---' for _ in header)+' |')
        lines.extend('| '+' | '.join(str(x) for x in row)+' |' for row in rows);lines.append('')
    lines.append('## Model files and startup\n')
    rows=[]
    for name,c in d['candidates'].items():
        p=c.get('live_run',c)
        rows.append([name,c['contract']['dimension'],number(c['model_files_bytes'],1/2**20),number(c['model']['download_seconds']),number(p['load_seconds']),number(p['first_embedding_seconds'],1000),number(p['launch_to_first_embedding_seconds'])])
    table(['Candidate','Dimensions','Files MiB','Download s','Load s','First embed ms','Launch → first embed s'],rows)
    lines.append('## Memory\n');rows=[]
    for name,c in d['candidates'].items():
        one=max(c['memory'].get('first_query',{}).get('peak_rss_bytes',0),c['memory'].get('warm_query',{}).get('peak_rss_bytes',0))
        corpus=max(t['memory']['peak_rss_bytes'] for t in c['throughput'])
        rows.append([name,number(c['loaded_memory']['rss_bytes'],1/2**20),number(c['loaded_memory']['pss_bytes'],1/2**20),number(one,1/2**20),number(corpus,1/2**20),number(c['observed_peak_rss_bytes'],1/2**20),number(c['observed_cgroup_peak_bytes'],1/2**20)])
    table(['Candidate','Loaded RSS MiB','Loaded PSS MiB','One-query sampled peak RSS MiB','Corpus sampled peak RSS MiB','Lifetime RSS high-water MiB','Cgroup peak MiB'],rows)
    lines.append('## Query latency\n');rows=[]
    for name,c in d['candidates'].items():
        p=c.get('live_run',c);q=p['query_latency'];full=p.get('full_retrieval_latency',{});four=p['concurrent_4_latency']
        rows.append([name,q['n'],number(q['p50_seconds'],1000),number(q['p95_seconds'],1000),number(q['max_seconds'],1000),number(full.get('p95_seconds'),1000),number(four['p95_seconds'],1000),number(four['max_seconds'],1000)])
    table(['Candidate','Requests','Warm p50 ms','Warm p95 ms','Warm max ms','Full retrieval p95 ms','4-client queued embedding p95 ms','4-client queued embedding max ms'],rows)
    lines.append('## Full-corpus throughput\n');rows=[]
    for name,c in d['candidates'].items():
        for t in c['throughput']:rows.append([name,t['batch_size'],t['chunks'],number(t['seconds']),number(t['chunks_per_second']),number(t['memory']['peak_rss_bytes'],1/2**20)])
    table(['Candidate','Batch size','Chunks','Encoding s','Chunks/s','Sampled RSS peak MiB'],rows)
    for group,title in [('natural','Quality: 24 answerable natural questions'),('keyword_controls','Quality: 4 known-evidence keyword controls'),('all','Quality: all 28 answerable questions')]:
        lines.append('## '+title+'\n');rows=[]
        def add(name,mode,m):rows.append([name,mode,m['answerable_count'],number(m['recall_at_1'],100),number(m['recall_at_5'],100),number(m['recall_at_10'],100),number(m['mrr'],digits=4),number(m['all_evidence_at_10'],100)])
        add('baseline','lexical',d['lexical']['quality'][group])
        for name,c in d['candidates'].items():
            for mode in ['vector','fused']:add(name,mode,c['quality_groups'][mode][group])
        table(['Candidate','Mode','n','Recall@1 %','Recall@5 %','Recall@10 %','MRR','All evidence@10 %'],rows)
    lines.append('## Answerability controls\n');rows=[]
    for name,c in d['candidates'].items():
        positives=[c['top_scores'][f'q{i:02}'] for i in range(1,25)]
        rows.append([name,number(c['top_scores']['q25'],digits=4),number(c['top_scores']['q26'],digits=4),number(min(positives),digits=4),number(max(positives),digits=4)])
    table(['Candidate','Population 2026: top cosine','Tram fare 2026: top cosine','Min top cosine on answerable natural questions','Max top cosine on answerable natural questions'],rows)
    output=root/'measured-tables.md';output.write_text('\n'.join(lines));print(output)

if __name__=='__main__':main()
