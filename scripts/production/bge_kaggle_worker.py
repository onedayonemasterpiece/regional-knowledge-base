"""Private Kaggle CPU worker template. Controller substitutes only RUN_CONFIG.

This worker cannot execute arbitrary jobs or access the corpus/DB/object store.
Query/document text is delivered only through authenticated, bounded jobs.
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

def main():
    config=RUN_CONFIG;started=time.monotonic();stop=threading.Event()
    def diagnostics():
        status=Path('/proc/self/status').read_text().splitlines()
        values={line.split(':',1)[0]:int(line.split()[1]) for line in status if line.startswith(('VmRSS:','VmHWM:'))}
        pss=sum(int(line.split()[1]) for line in Path('/proc/self/smaps_rollup').read_text().splitlines() if line.startswith('Pss:'))
        cpu=os.times();return {'rss_kib':values['VmRSS'],'hwm_kib':values['VmHWM'],'pss_kib':pss,'cpu_seconds':cpu.user+cpu.system,'cpu_threads':4,'pid':os.getpid()}
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
    import torch
    import importlib.metadata
    versions={name:importlib.metadata.version(name) for name in ('torch','transformers','tokenizers','huggingface-hub')}
    if versions!={'torch':'2.11.0+cpu','transformers':'5.16.1','tokenizers':'0.23.1','huggingface-hub':'1.29.0'}:
        raise RuntimeError('pinned BGE CPU runtime mismatch')
    from transformers import AutoModel,AutoTokenizer
    torch.set_num_threads(4);torch.set_num_interop_threads(1)
    tokenizer=AutoTokenizer.from_pretrained('BAAI/bge-m3',revision=REVISION)
    model=AutoModel.from_pretrained('BAAI/bge-m3',revision=REVISION,torch_dtype=torch.float32).to('cpu').eval()
    if model.config.hidden_size!=1024:raise RuntimeError('BGE dimension mismatch')
    def encode(text):
        # Same no-prefix CLS/L2 FP32 batch1 contract for query and document.
        tokens=tokenizer([text],padding=True,truncation=True,max_length=512,return_tensors='pt')
        with torch.inference_mode():
            hidden=model(**tokens).last_hidden_state[:,0,:]
            result=torch.nn.functional.normalize(hidden,p=2,dim=1).float()[0].tolist()
        return result
    encode('readiness')
    ready=time.monotonic()-started
    call('heartbeat',{'ready':True,'space':SPACE,'model_revision':REVISION,'diagnostics':{'startup_seconds':ready,**diagnostics()}})
    print(json.dumps({'event':'bge_ready','run_id':config['run_id'],'space':SPACE,'startup_seconds':ready,'cpu_threads':4}),flush=True)
    pending=None
    try:
        while not stop.is_set() and time.monotonic()-started<11*3600:
            try:
                if pending is None:
                    job=call('claim',{'space':SPACE}).get('job')
                    if not job:stop.wait(.2);continue
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

if __name__=='__main__':main()
