"""One isolated candidate, local weights only; run under systemd resource limits."""
import os
import time
PROCESS_START = time.monotonic()
# Must precede native imports, including numpy.
for key in ('OMP_NUM_THREADS','OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','NUMEXPR_NUM_THREADS'):
    os.environ[key] = '1'
os.environ['TOKENIZERS_PARALLELISM'] = 'false'
os.environ['CUDA_VISIBLE_DEVICES'] = ''
os.environ['HF_HUB_OFFLINE'] = '1'
import argparse
import concurrent.futures
import hashlib
import importlib.metadata
import json
import resource
import threading
from pathlib import Path
import numpy as np
import psutil
from small_embedding_metrics import preprocess, rrf, metrics, validate_fixture

STRINGS = ['Кёнигсберг и Калининград', 'Почему объединили три города?', 'The university opened in 1544.', 'Königsberg liegt am Pregel.', 'La ville et son histoire.', 'El puerto permanece abierto en invierno.', 'Miasto nad rzeką.', '城市的历史与大学', '都市の歴史', 'تاريخ المدينة والجامعة']

class Memory:
    def __init__(self, trace):
        self.trace=trace
        self.phase = 'imports'
        self.samples = {}
        self.done = threading.Event()
        self.process = psutil.Process()
        self.thread = threading.Thread(target=self.poll,daemon=True)
        self.thread.start()
    def sample(self):
        rss=pss=0
        for proc in [self.process, *self.process.children(recursive=True)]:
            try:
                info=proc.memory_full_info();rss+=info.rss;pss+=getattr(info,'pss',0)
            except (psutil.NoSuchProcess,psutil.AccessDenied):pass
        point={'rss_bytes':rss,'pss_bytes':pss}
        old=self.samples.setdefault(self.phase,{'peak_rss_bytes':0,'peak_pss_bytes':0,'sample_count':0})
        old['peak_rss_bytes']=max(old['peak_rss_bytes'],rss);old['peak_pss_bytes']=max(old['peak_pss_bytes'],pss);old['sample_count']+=1
        return point
    def poll(self):
        while not self.done.wait(.02):
            point=self.sample()
            with self.trace.open('a') as fh: fh.write(json.dumps({'t':time.monotonic(),'phase':self.phase,**point})+'\n')
    def set(self,phase):self.phase=phase;self.sample()

def percentiles(values):
    return {'n':len(values),'p50_seconds':float(np.percentile(values,50)),'p95_seconds':float(np.percentile(values,95)),'max_seconds':float(max(values))}

class Encoder:
    def __init__(self,name,root,potion_mmap=False):
        self.name=name;self.path=root/'models'/name
        self.max_tokens = 2048 if name=='gemma' else 512
        if name=='potion':
            from model2vec import StaticModel
            if potion_mmap:
                # Preserve the exact FP32 table; avoid safetensors numpy's eager copy.
                import struct
                from tokenizers import Tokenizer
                with (self.path/'model.safetensors').open('rb') as fh:
                    header_len=struct.unpack('<Q',fh.read(8))[0]
                    header=json.loads(fh.read(header_len))
                tensor=header['embeddings']
                assert tensor['dtype']=='F32'
                table=np.memmap(self.path/'model.safetensors',dtype='<f4',mode='r',offset=8+header_len+tensor['data_offsets'][0],shape=tuple(tensor['shape']))
                self.model=StaticModel(vectors=table,tokenizer=Tokenizer.from_file(str(self.path/'tokenizer.json')),config=json.loads((self.path/'config.json').read_text()),normalize=True)
            else:
                self.model=StaticModel.from_pretrained(str(self.path),normalize=True)
            self.dimension=self.model.dim
            self.contract={'prefix_query':'','prefix_document':'','pooling':'official Model2Vec StaticModel.encode, normalize=True','max_tokens':512,'dimension':int(self.dimension),'normalization':'L2 float32','loader':'FP32 numpy.memmap + official StaticModel constructor' if potion_mmap else 'official StaticModel.from_pretrained'}
        else:
            import onnxruntime as ort
            from tokenizers import Tokenizer
            options=ort.SessionOptions();options.intra_op_num_threads=1;options.inter_op_num_threads=1
            options.execution_mode=ort.ExecutionMode.ORT_SEQUENTIAL
            options.add_session_config_entry('session.intra_op.allow_spinning','0')
            options.add_session_config_entry('session.inter_op.allow_spinning','0')
            self.session=ort.InferenceSession(str(self.path/'onnx'/('model_quantized.onnx' if name=='e5' else 'model_q4.onnx')),sess_options=options,providers=['CPUExecutionProvider'])
            self.tokenizer=Tokenizer.from_file(str(self.path/'tokenizer.json'))
            self.tokenizer.enable_truncation(max_length=self.max_tokens)
            self.tokenizer.enable_padding(pad_id=1 if name=='e5' else 0,pad_token='<pad>')
            self.dimension=384 if name=='e5' else 768
            self.contract={'prefix_query':'query: ' if name=='e5' else 'task: search result | query: ','prefix_document':'passage: ' if name=='e5' else 'title: {title or none} | text: ','pooling':'attention-mask mean over last_hidden_state' if name=='e5' else 'export sentence_embedding','normalization':'L2 float32 after pooling','max_tokens':self.max_tokens,'dimension':self.dimension,'tokenizer_special_tokens':'tokenizer.json post_processor (BOS/EOS enabled)','padding':'right; masked pad ID 1' if name=='e5' else 'right; masked pad ID 0','inputs':[{ 'name':x.name,'type':x.type,'shape':x.shape} for x in self.session.get_inputs()],'outputs':[{'name':x.name,'shape':x.shape} for x in self.session.get_outputs()]}
    def encode(self,texts,query=True,titles=None):
        prepared=[preprocess(self.name,t,query=query,title=titles[i] if titles else 'none') for i,t in enumerate(texts)]
        if self.name=='potion':
            vector=self.model.encode(prepared,max_length=512,use_multiprocessing=False,batch_size=len(prepared))
        else:
            encoded=self.tokenizer.encode_batch(prepared)
            data={'input_ids':np.array([x.ids for x in encoded],dtype=np.int64),'attention_mask':np.array([x.attention_mask for x in encoded],dtype=np.int64),'token_type_ids':np.array([x.type_ids for x in encoded],dtype=np.int64)}
            outputs=self.session.run(None,{x.name:data[x.name] for x in self.session.get_inputs()})
            names=[x.name for x in self.session.get_outputs()]
            if self.name=='gemma':vector=outputs[names.index('sentence_embedding')]
            else:
                hidden=outputs[names.index('last_hidden_state')] if 'last_hidden_state' in names else outputs[0]
                mask=data['attention_mask'][...,None]
                vector=(hidden*mask).sum(axis=1)/np.maximum(mask.sum(axis=1),1)
        vector=np.asarray(vector,dtype=np.float32)
        assert vector.shape==(len(texts),self.dimension),vector.shape
        norm=np.linalg.norm(vector,axis=1,keepdims=True)
        assert np.isfinite(vector).all() and (norm>0).all()
        return vector/norm

def main():
    parser=argparse.ArgumentParser();parser.add_argument('root',type=Path);parser.add_argument('candidate');parser.add_argument('--batches',default='1,4');parser.add_argument('--smoke',action='store_true');parser.add_argument('--potion-mmap',action='store_true');parser.add_argument('--live-only',action='store_true')
    args=parser.parse_args();root=args.root;name=args.candidate
    outfile=root/(name+('-smoke' if args.smoke else '-live' if args.live_only else '')+'-result.json')
    output={'candidate':name,'status':'running','pid':os.getpid(),'cpu_affinity':sorted(os.sched_getaffinity(0)),'runtime':{'python':__import__('sys').version},'model':json.loads((root/'models'/name/'manifest.json').read_text())}
    output['runtime']['packages']={package:importlib.metadata.version(package) for package in ['numpy','tokenizers','onnxruntime','model2vec','psutil','psycopg']}
    memory=Memory(root/f'{name}-memory-samples.jsonl')
    cg=Path('/sys/fs/cgroup')/Path('/proc/self/cgroup').read_text().strip().split('::')[-1].lstrip('/')
    output['cgroup']={key:(cg/key).read_text().strip() for key in ('memory.max','memory.swap.max','cpu.max')}
    output['cgroup']['path']=str(cg)
    def save():
        output['memory']=memory.samples.copy()
        output['cgroup']['memory_peak_bytes']=int((cg/'memory.peak').read_text())
        output['ru_maxrss_bytes']=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024
        outfile.write_text(json.dumps(output,ensure_ascii=False,indent=2)+'\n')
    save()
    try:
        memory.set('load');start=time.perf_counter();encoder=Encoder(name,root,potion_mmap=args.potion_mmap)
        output['load_seconds']=time.perf_counter()-start;output['loaded_memory']=memory.sample();output['contract']=encoder.contract;save()
        memory.set('first_query');start=time.perf_counter();encoder.encode([STRINGS[0]])
        output['first_embedding_seconds']=time.perf_counter()-start
        output['process_start_to_first_embedding_seconds']=time.monotonic()-PROCESS_START
        output['launch_to_first_embedding_seconds']=time.monotonic()-float(os.environ.get('BENCH_LAUNCH_MONOTONIC',PROCESS_START))
        save();print(name,'loaded',output['load_seconds'],'memory',output['loaded_memory'],flush=True)
        memory.set('compatibility');fixture=np.concatenate([encoder.encode([s]) for s in STRINGS])
        output['compatibility']={'strings':STRINGS,'vectors':fixture.tolist(),'float32_sha256':hashlib.sha256(fixture.astype('<f4').tobytes()).hexdigest(),'comparison':{'cosine_min':.999,'absolute_component_tolerance':.002},'contract':encoder.contract}
        output['compatibility']['document_vectors']=np.concatenate([encoder.encode([s],query=False,titles=['none']) for s in STRINGS]).tolist()
        if name!='potion':
            output['compatibility']['query_token_ids']=[encoder.tokenizer.encode(preprocess(name,s,query=True)).ids for s in STRINGS]
            output['compatibility']['document_token_ids']=[encoder.tokenizer.encode(preprocess(name,s,query=False,title='none')).ids for s in STRINGS]
        if args.smoke:output['status']='smoke_success';save();return
        corpus=[json.loads(line) for line in (root/'corpus.jsonl').read_text().splitlines()]
        fixture_info=json.loads(Path('scripts/benchmarks/small_embedding_questions.json').read_text())
        assert hashlib.sha256((root/'corpus.jsonl').read_bytes()).hexdigest()==fixture_info['corpus_sha256']
        questions=fixture_info['questions']
        validate_fixture(questions,corpus)
        lexical=json.loads((root/'lexical.json').read_text())['rankings']
        output['corpus_sha256']=hashlib.sha256((root/'corpus.jsonl').read_bytes()).hexdigest()
        output['throughput']=[]
        for batch_size in ([] if args.live_only else map(int,args.batches.split(','))):
            memory.set(f'corpus_batch_{batch_size}');start=time.perf_counter();vectors=[]
            for offset in range(0,len(corpus),batch_size):
                batch=corpus[offset:offset+batch_size]
                vectors.append(encoder.encode([r['text'] for r in batch],query=False,titles=[r['title'] for r in batch]))
                if offset%100==0:print(name,'batch',batch_size,'chunks',offset,flush=True)
            vectors=np.concatenate(vectors);elapsed=time.perf_counter()-start
            output['throughput'].append({'batch_size':batch_size,'chunks':len(corpus),'seconds':elapsed,'chunks_per_second':len(corpus)/elapsed,'memory':memory.samples[memory.phase].copy()});save()
        if args.live_only: vectors=np.load(root/f'{name}-document-vectors.npy')
        else: np.save(root/f'{name}-document-vectors.npy',vectors)
        ids=[r['id'] for r in corpus]
        def rank(qvec):
            scores=vectors@qvec[0]
            return sorted(ids,key=lambda x:(-float(scores[id_index[x]]),x)),scores
        id_index={id:i for i,id in enumerate(ids)}
        output['quality']={};output['rankings']={};output['top_scores']={}
        qvectors=np.concatenate([encoder.encode([q['query']]) for q in questions])
        np.save(root/f'{name}-query-vectors.npy',qvectors)
        vr={};fr={}
        for i,q in enumerate(questions):
            vr[q['id']],scores=rank(qvectors[i:i+1]);fr[q['id']]=rrf(vr[q['id']],lexical[q['id']])
            output['top_scores'][q['id']]=float(scores.max())
        output['quality']={'vector':metrics(questions,vr),'lexical':metrics(questions,lexical),'fused':metrics(questions,fr)}
        output['rankings']={'vector':{k:v[:20] for k,v in vr.items()},'fused':{k:v[:20] for k,v in fr.items()}};save()
        memory.set('warm_query');[encoder.encode([q['query']]) for q in questions[:5]]
        times=[];retrieval=[]
        for i in range(104):
            q=questions[i%len(questions)];start=time.perf_counter();qvec=encoder.encode([q['query']]);embedded=time.perf_counter()
            ranking,_=rank(qvec);rrf(ranking,lexical[q['id']]);end=time.perf_counter()
            times.append(embedded-start);retrieval.append(end-start)
        output['query_latency']=percentiles(times);output['local_retrieval_latency']=percentiles(retrieval);save()
        # Actual round trips, same SQL baseline, read-only; no embedding API.
        import shlex
        import psycopg
        dsn=None
        for raw in Path('/home/dev/.local/state/regional-knowledge-base/service.env').read_text().splitlines():
            if raw.strip().startswith('KB_SUPABASE_SESSION_CONNECTION='):
                dsn=shlex.split(raw.strip())[0].split('=',1)[1]
        assert dsn
        memory.set('live_retrieval');full=[]
        with psycopg.connect(dsn) as conn:
            conn.execute('set transaction read only')
            for i in range(104):
                q=questions[i%len(questions)];start=time.perf_counter()
                qvec=encoder.encode([q['query']]);ranking,_=rank(qvec)
                live=conn.execute("""
                    select c.id from public.rkb_chunks c
                    join public.rkb_documents d on d.id=c.document_id
                    cross join (select websearch_to_tsquery('simple',%s) tsq) p
                    where c.document_id=%s and c.revision=d.active_revision
                      and p.tsq<>''::tsquery and c.fts@@p.tsq
                    order by ts_rank_cd(c.fts,p.tsq) desc,c.id limit 100
                """,(q['query'],'7ce738b0-d3d3-4fc2-9a61-aa58b537a0e9')).fetchall()
                actual=[str(row[0]) for row in live]
                assert actual==lexical[q['id']]
                rrf(ranking,actual);full.append(time.perf_counter()-start)
        output['full_retrieval_latency']=percentiles(full);save()
        # Four clients enqueue simultaneously; one worker. Includes queue wait.
        memory.set('concurrent_4');latencies=[];service=[]
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            def request(q,submitted):
                start=time.perf_counter();encoder.encode([q['query']]);end=time.perf_counter()
                return end-submitted,end-start
            for group in range(26):
                submitted=time.perf_counter();futures=[pool.submit(request,questions[(group*4+j)%len(questions)],submitted) for j in range(4)]
                for future in futures:
                    total,busy=future.result();latencies.append(total);service.append(busy)
        output['concurrent_4_latency']=percentiles(latencies);output['concurrent_4_service']=percentiles(service)
        output['status']='success';save();print(name,'success',output['query_latency'],flush=True)
    except Exception as exc:
        output['status']='error';output['error']=f'{type(exc).__name__}: {exc}';save();raise
    finally:memory.done.set();memory.thread.join();save()

if __name__=='__main__':main()
