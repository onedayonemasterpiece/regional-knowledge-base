"""Private Kaggle CPU worker template. Controller substitutes only RUN_CONFIG.

Legacy execution encodes bounded jobs only. Explicit prepared target factories
can attach the owned Kaggle PG pool after control; no arbitrary code is accepted.
"""
import os
os.environ['CUDA_VISIBLE_DEVICES']=''
os.environ['TOKENIZERS_PARALLELISM']='false'
os.environ['OMP_NUM_THREADS']='4'
os.environ['MKL_NUM_THREADS']='4'
import json
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

RUN_CONFIG = {}  # Replaced in the private generated notebook, never committed.
REVISION = '5617a9f61b028005a4858fdac845db406aefb181'
SPACE = 'bge-m3:5617a9f:t211-tr5161:cls-l2-512:q1-d1:v1'
_MODEL_LOAD_COUNT = 0

def load_encoder():
    global _MODEL_LOAD_COUNT
    prepared = RUN_CONFIG.get('prepared_model')
    artifacts = None
    if prepared is not None:
        from regional_knowledge.kaggle_bootstrap import PreparedArtifact, verify_artifacts, ResidentEncoder
        from regional_knowledge.kaggle_readiness import ArtifactRequirement
        if not isinstance(prepared, dict) or set(prepared) != {'root', 'entries', 'max_total_bytes'}:
            raise ValueError('explicit prepared model inventory required')
        entries = tuple(PreparedArtifact(ArtifactRequirement(e['name'], e['size'], e['sha256']), e['role'])
                        for e in prepared['entries'])
        artifacts = verify_artifacts(prepared['root'], entries, max_total_bytes=prepared['max_total_bytes'])
    import torch
    import importlib.metadata
    versions={name:importlib.metadata.version(name) for name in ('torch','transformers','tokenizers','huggingface-hub')}
    if versions!={'torch':'2.11.0+cpu','transformers':'5.16.1','tokenizers':'0.23.1','huggingface-hub':'1.29.0'}:
        raise RuntimeError('pinned BGE CPU runtime mismatch')
    from transformers import AutoModel,AutoTokenizer
    torch.set_num_threads(4);torch.set_num_interop_threads(1)
    if artifacts is None:
        tokenizer=AutoTokenizer.from_pretrained('BAAI/bge-m3',revision=REVISION)
        model=AutoModel.from_pretrained('BAAI/bge-m3',revision=REVISION,torch_dtype=torch.float32).to('cpu').eval()
    else:
        model_path = artifacts.model_directory
        tokenizer=AutoTokenizer.from_pretrained(str(model_path),use_fast=True,
            tokenizer_file=str(model_path/'tokenizer.json'),local_files_only=True,trust_remote_code=False)
        model=AutoModel.from_pretrained(str(model_path),torch_dtype=torch.float32,
            local_files_only=True,trust_remote_code=False).to('cpu').eval()
    _MODEL_LOAD_COUNT += 1
    if model.config.hidden_size!=1024:raise RuntimeError('BGE dimension mismatch')
    def encode(text):
        # Same no-prefix CLS/L2 FP32 batch1 contract for query and document.
        tokens=tokenizer([text],padding=True,truncation=False,return_tensors='pt')
        if tokens['input_ids'].shape[1]>512:raise ValueError('BGE exact input exceeds 512 tokens')
        with torch.inference_mode():
            hidden=model(**tokens).last_hidden_state[:,0,:]
            result=torch.nn.functional.normalize(hidden,p=2,dim=1).float()[0].tolist()
        return result
    encode('readiness')
    if artifacts is not None:
        return ResidentEncoder(encode,tokenizer,artifacts,artifacts.model_directory,_MODEL_LOAD_COUNT)
    return encode

def main(*, prepare_runtime=None):
    config=RUN_CONFIG;started=time.monotonic();stop=threading.Event()
    if prepare_runtime is not None and config.get('transport') != 'wss':
        raise ValueError('resident preparation requires the existing WSS channel')
    if prepare_runtime is not None and (config.get('shared_reads') is not True
            or not isinstance(config.get('prepared_model'), dict)
            or set(config['prepared_model']) != {'root', 'entries', 'max_total_bytes'}):
        raise ValueError('resident preparation requires explicit shared model inventory')
    def diagnostics():
        status=Path('/proc/self/status').read_text().splitlines()
        values={line.split(':',1)[0]:int(line.split()[1]) for line in status if line.startswith(('VmRSS:','VmHWM:'))}
        pss=sum(int(line.split()[1]) for line in Path('/proc/self/smaps_rollup').read_text().splitlines() if line.startswith('Pss:'))
        cpu=os.times();return {'rss_kib':values['VmRSS'],'hwm_kib':values['VmHWM'],'pss_kib':pss,'cpu_seconds':cpu.user+cpu.system,'cpu_threads':4,'pid':os.getpid()}
    if config.get('transport')=='wss':
        try:
            run_wss_worker(config,load_encoder,diagnostics,prepare_runtime=prepare_runtime)
        finally:
            Path('/kaggle/working/bge-run-summary.json').write_text(json.dumps({
                'run_id':config['run_id'],'space':SPACE,'transport':'wss',
                'worker_wall_seconds':time.monotonic()-started,
                'cooperative_worker_exit':True,'provider_termination_verified':False}))
        return
    def call(action,payload):
        data=json.dumps({'run_id':config['run_id'],**payload}).encode()
        request=urllib.request.Request(config['broker']+'/'+action,data=data,headers={'Authorization':'Bearer '+config['token'],'Content-Type':'application/json'},method='POST')
        with urllib.request.urlopen(request,timeout=30) as response:return json.load(response)
    def heartbeat():
        while not stop.wait(20):
            try:call('heartbeat',{'ready':False,'space':SPACE,'diagnostics':diagnostics()})
            except urllib.error.HTTPError as error:
                if error.code in (401,403):stop.set()
            except (TimeoutError,OSError):pass
    thread=threading.Thread(target=heartbeat,daemon=True);thread.start()
    encode=load_encoder()
    ready=time.monotonic()-started
    call('heartbeat',{'ready':True,'space':SPACE,'model_revision':REVISION,'diagnostics':{'startup_seconds':ready,**diagnostics()}})
    print(json.dumps({'event':'bge_ready','run_id':config['run_id'],'space':SPACE,'startup_seconds':ready,'cpu_threads':4}),flush=True)
    pending=None
    try:
        while not stop.is_set() and time.monotonic()-started<11*3600:
            try:
                if pending is None:
                    job=call('claim',{'space':SPACE}).get('job')
                    if not job:stop.wait(.1);continue
                    if job['space']!=SPACE or len(job['texts'])!=1:raise RuntimeError('job contract mismatch')
                    before=time.monotonic();vector=encode(job['texts'][0]);seconds=time.monotonic()-before
                    pending={'job_id':job['id'],'claim':job['claim'],'space':SPACE,'vectors':[vector],'timings':{'encoder_seconds':seconds}}
                call('complete',pending);pending=None
            except urllib.error.HTTPError as error:
                if error.code in (401,403):stop.set()
                elif error.code==409:pending=None
                else:stop.wait(1)
            except (TimeoutError,OSError):stop.wait(1)
    finally:
        stop.set()
        open('/kaggle/working/bge-run-summary.json','w').write(json.dumps({'run_id':config['run_id'],'space':SPACE,'startup_seconds':ready,'worker_wall_seconds':time.monotonic()-started,'cpu_only':True}))

# WSS_TRANSPORT_HELPER

if __name__=='__main__':main()
