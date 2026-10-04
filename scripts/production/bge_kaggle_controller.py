"""One small Kaggle launcher/reconciler; durable queue stays on DevCoveer.

Private launch material lives in the runtime state directory, never run evidence.
After an ambiguous upload it probes the exact notebook before any further action.
"""
import json
import logging
import os
from pathlib import Path
import shlex
import time
from regional_knowledge.bge_queue import BgeQueue

def provider_phase(status):
    phase=status.get('status') if isinstance(status,dict) else getattr(status,'status',None)
    return str(getattr(phase,'name',phase)).rsplit('.',1)[-1].upper()

def main():
    logging.basicConfig(level=logging.INFO)
    for line in Path('/home/dev/.env').read_text().splitlines():
        if line.startswith(('KAGGLE_USERNAME=','KAGGLE_KEY=')):
            key,value=line.split('=',1);os.environ[key]=shlex.split(value)[0]
    from kaggle.api.kaggle_api_extended import KaggleApi
    api=KaggleApi();api.authenticate()
    queue=BgeQueue(os.environ['RKB_BGE_QUEUE_PATH'])
    template=Path(__file__).with_name('bge_kaggle_worker.py').read_text()
    broker=os.environ['RKB_BGE_BROKER_URL']
    if not broker.startswith('https://'):raise RuntimeError('HTTPS broker required')
    state=queue.path.parent/'bge-launches';state.mkdir(mode=0o700,exist_ok=True)
    probed={}
    while True:
        runs=queue.maintenance()
        for run in runs:
            run_id=run['id'];slug='rkb-bge-'+run_id.replace('-','')[:20]
            ref=os.environ['KAGGLE_USERNAME']+'/'+slug
            try:
                if run['status']=='starting' and run['launch_state']=='pending':
                    token=queue.launch_claim(run_id)
                    if token is None:continue
                    folder=state/run_id;folder.mkdir(mode=0o700,exist_ok=True)
                    config={'run_id':run_id,'token':token,'broker':broker}
                    source=template.replace('RUN_CONFIG = {}', 'RUN_CONFIG = '+repr(config),1)
                    (folder/'main.py').write_text(source);(folder/'main.py').chmod(0o600)
                    metadata={'id':ref,'title':slug,'code_file':'main.py','language':'python','kernel_type':'script','is_private':True,'enable_gpu':False,'enable_tpu':False,'enable_internet':True,'dataset_sources':[],'competition_sources':[],'kernel_sources':[]}
                    (folder/'kernel-metadata.json').write_text(json.dumps(metadata));(folder/'kernel-metadata.json').chmod(0o600)
                    # Credential is run-bound and source remains private. SDK
                    # provider request is the only notebook creation boundary.
                    api.kernels_push(str(folder),timeout=11*3600)
                    queue.launch_record(run_id,ref)
                    logging.info(json.dumps({'event':'bge_dispatched','run_id':run_id,'provider_ref':ref,'cpu_only':True}))
                elif run['launch_state']=='dispatching':
                    # Unknown side effect after crash/lost response. Do not
                    # issue another save/version: reconcile the stable slug.
                    status=api.kernels_status(ref)
                    if status:queue.launch_record(run_id,ref)
                elif run['status']=='starting' and run['launch_state']=='dispatched' and time.monotonic()-probed.get(run_id,0)>30:
                    probed[run_id]=time.monotonic();status=api.kernels_status(ref)
                    phase=provider_phase(status)
                    if phase in ('ERROR','FAILED','CANCELED','CANCELLED','COMPLETE'):
                        queue.launch_record(run_id,ref,failed=True)
                        logging.warning(json.dumps({'event':'bge_startup_failed','run_id':run_id,'provider_status':str(phase)}))
                elif run['status']=='expired' and run['launch_state']=='dispatched':
                    # Expired credential makes worker polling stop even when
                    # provider cancellation is unavailable in this SDK.
                    queue.launch_record(run_id,ref)
            except Exception as error:
                logging.warning(json.dumps({'event':'bge_provider_boundary_failed','run_id':run_id,'error_type':type(error).__name__}))
        time.sleep(5)

if __name__=='__main__':main()
