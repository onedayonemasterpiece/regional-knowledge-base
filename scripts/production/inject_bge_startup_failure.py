"""Fixed CPU-only startup failure for owned real-provider acceptance.

Run with the controller stopped and exactly one unlaunched starting current run.
No arbitrary code/job input is accepted; this is an operator CLI, not a route.
No corpus text or run credential is sent to the failing notebook.
"""
import argparse,json,os,shlex
from pathlib import Path
from regional_knowledge.bge_queue import BgeQueue
from bge_kaggle_controller import notebook_ref

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--execute',action='store_true',required=True);parser.parse_args()
    for line in Path('/home/dev/.env').read_text().splitlines():
        if line.startswith(('KAGGLE_USERNAME=','KAGGLE_KEY=')):
            key,value=line.split('=',1);os.environ[key]=shlex.split(value)[0]
    from kaggle.api.kaggle_api_extended import KaggleApi
    api=KaggleApi();api.authenticate();queue=BgeQueue('/home/dev/.local/state/regional-knowledge-base/bge/queue.sqlite')
    with queue.connect() as db:
        run=dict(db.execute('select r.* from runs r join control c on c.current_run=r.id').fetchone())
    if run['status']!='starting' or run['launch_state']!='pending':raise RuntimeError('unlaunched owned starting run required')
    ref=notebook_ref(run,queue.path.parent/'bge-launches');slug=ref.split('/',1)[1]
    if queue.launch_claim(run['id'],provider_ref=ref) is None:raise RuntimeError('dispatch already claimed')
    folder=queue.path.parent/'bge-launches'/run['id'];folder.mkdir(parents=True,exist_ok=True,mode=0o700)
    (folder/'main.py').write_text('RUN_CONFIG = '+repr({'run_id':run['id']})+"\nraise RuntimeError('controlled BGE startup failure before model load')\n")
    (folder/'main.py').chmod(0o600)
    metadata={'id':ref,'title':slug,'code_file':'main.py','language':'python','kernel_type':'script','is_private':True,'enable_gpu':False,'enable_tpu':False,'enable_internet':True,'dataset_sources':[],'competition_sources':[],'kernel_sources':[]}
    (folder/'kernel-metadata.json').write_text(json.dumps(metadata));(folder/'kernel-metadata.json').chmod(0o600)
    saved=api.kernels_push(str(folder),timeout=120)
    if saved.error or saved.ref!=ref or saved.version_number<1:raise RuntimeError('kaggle_save_receipt_invalid')
    queue.launch_record(run['id'],ref,provider_version=saved.version_number)
    print(json.dumps({'run_id':run['id'],'provider_ref':ref,'controlled_startup_failure':True,'cpu_only':True}))

if __name__=='__main__':main()
