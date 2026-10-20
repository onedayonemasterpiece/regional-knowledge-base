"""One small Kaggle launcher/reconciler; durable queue stays on DevCoveer.

Private launch material lives in the runtime state directory, never run evidence.
After an ambiguous upload it probes the exact notebook before any further action.
"""
import json
import ast
import logging
import os
from pathlib import Path
import shlex
import re
import time
from regional_knowledge.bge_queue import BgeQueue

def provider_phase(status):
    phase=status.get('status') if isinstance(status,dict) else getattr(status,'status',None)
    return str(getattr(phase,'name',phase)).rsplit('.',1)[-1].upper()

def version_status(api, ref, version, *, request_factory=None):
    """Read a frozen version; this alone does not prove exact execution Stop."""
    if type(version) is not int or version < 1 or ref.count('/') != 1:
        raise ValueError('exact provider version required')
    owner,slug=ref.split('/')
    if not owner or not slug:raise ValueError('exact notebook ref required')
    if request_factory is None:
        from kagglesdk.kernels.types.kernels_api_service import ApiGetKernelSessionStatusRequest
        request_factory=ApiGetKernelSessionStatusRequest
    request=request_factory()
    if not hasattr(request,'version_label'):
        raise RuntimeError('version_qualified_status_unsupported')
    request.user_name=owner;request.kernel_slug=slug
    # Native API expects v<N>; bare integers give HTTP 404.
    request.version_label='v'+str(version)
    with api.build_kaggle_client() as client:
        return client.kernels.kernels_api_client.get_kernel_session_status(request)

def notebook_ref(run, state):
    if run.get('provider_ref'):return run['provider_ref']
    if run['launch_state']=='dispatching':
        metadata=state/run['id']/'kernel-metadata.json'
        # Preserve pre-migration unknown launches at their original notebook.
        if metadata.exists():return json.loads(metadata.read_text())['id']
        return os.environ['KAGGLE_USERNAME']+'/rkb-bge-'+run['id'].replace('-','')[:20]
    slug=os.environ.get('RKB_BGE_KERNEL_SLUG','rkb-bge-m3-cpu')
    if not 5<=len(slug)<=100 or not re.fullmatch(r'[a-z0-9]+(?:-[a-z0-9]+)*',slug):
        raise ValueError('invalid stable BGE notebook slug')
    return os.environ['KAGGLE_USERNAME']+'/'+slug

def source_run_id(source):
    try:
        for node in ast.parse(source).body:
            if isinstance(node,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='RUN_CONFIG' for t in node.targets):
                value=ast.literal_eval(node.value)
                return value.get('run_id') if isinstance(value,dict) else None
    except (SyntaxError,ValueError,TypeError):pass
    return None

def notebook_snapshot(api, ref):
    # Status addresses the latest version, so bind it to exact run source first.
    # Provider source contains a private run credential: never log or artifact it.
    from kagglesdk.kernels.types.kernels_api_service import ApiGetKernelRequest
    owner,slug=ref.split('/',1);request=ApiGetKernelRequest();request.user_name=owner;request.kernel_slug=slug
    with api.build_kaggle_client() as client:
        response=client.kernels.kernels_api_client.get_kernel(request)
    return {'run_id':source_run_id(response.blob.source),'version':response.metadata.current_version_number}

def reconcile_launch(api, queue, run, ref):
    observed=notebook_snapshot(api,ref)
    if observed['run_id']!=run['id'] or observed['version']<1:
        return False
    if run.get('provider_version') and run['provider_version']!=observed['version']:
        return False
    queue.launch_record(run['id'],ref,provider_version=observed['version'])
    return True

def worker_source(template, queue, run_id, token, broker, *, transport='http', target=None):
    """Build private launch bytes once; no provider call or inferred version."""
    if transport not in ('http','wss'):
        raise ValueError('unsupported BGE transport')
    config={'run_id':run_id,'token':token,'broker':broker}
    if transport=='wss':
        from urllib.parse import urlsplit,urlunsplit
        url=urlsplit(broker)
        if (url.scheme!='https' or not url.hostname or url.port not in (None,443)
                or url.username or url.password or url.query or url.fragment):
            raise ValueError('HTTPS/443 worker authority required')
        config.update(transport='wss',launch_nonce=queue.prepare_channel(run_id),
                      channel=urlunsplit(('wss',url.netloc,url.path.rstrip('/')+'/channel','','')))
        helper=Path(__file__).with_name('bge_wss_transport.py').read_text()
        if template.count('# WSS_TRANSPORT_HELPER')!=1:
            raise ValueError('WSS worker template slot missing')
        template=template.replace('# WSS_TRANSPORT_HELPER',helper,1)
    if template.count('RUN_CONFIG = {}')!=1:
        raise ValueError('worker config slot missing')
    if target is not None:
        from regional_knowledge.kaggle_target_launch import compose_target
        return compose_target(template,config,target,queue=queue,repository=Path(__file__).resolve().parents[2])
    return template.replace('RUN_CONFIG = {}','RUN_CONFIG = '+repr(config),1)

def main(*, target_spec=None, recovery_journal=None, target_config_path=None):
    logging.basicConfig(level=logging.INFO)
    target_config_path=target_config_path or os.getenv('RKB_BGE_TARGET_CONFIG','').strip()
    if target_config_path:
        if target_spec is not None:
            raise ValueError('one explicit target launch configuration required')
        from regional_knowledge.kaggle_launch_config import load_target_launch
        target_spec=load_target_launch(target_config_path)
    if target_spec is not None:
        from regional_knowledge.kaggle_target_runtime import TargetLaunchSpec
        from regional_knowledge.recovery_journal import RecoveryJournal
        if type(target_spec) is not TargetLaunchSpec or os.getenv('RKB_BGE_TRANSPORT','http')!='wss':
            raise ValueError('explicit typed target requires WSS configuration')
        if target_spec.database.mode=='NEW_EMPTY_GENERATION' and not isinstance(recovery_journal,RecoveryJournal):
            raise TypeError('fresh generation requires existing real RecoveryJournal')
        if target_spec.database.mode=='NEW_EMPTY_GENERATION' and target_spec.stage0 is None:
            raise ValueError('fresh launch requires complete pinned Stage0 preparation')
        if target_spec.database.mode=='NEW_EMPTY_GENERATION' and target_spec.native_bundle is None:
            raise ValueError('fresh launch requires complete pinned native bundle')
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
            run_id=run['id'];ref=notebook_ref(run,state);slug=ref.rsplit('/',1)[-1]
            try:
                if run['status']=='starting' and run['launch_state']=='pending':
                    token=queue.launch_claim(run_id,provider_ref=ref)
                    if token is None:continue
                    folder=state/run_id;folder.mkdir(mode=0o700,exist_ok=True)
                    if target_spec is not None and target_spec.database.mode=='NEW_EMPTY_GENERATION':
                        import asyncio
                        from regional_knowledge.kaggle_target_launch import persist_empty_intent
                        from regional_knowledge.kaggle_owned_pg import SQL_ARTIFACTS,digest
                        nonce=queue.prepare_channel(run_id)
                        sql_root=Path(__file__).resolve().parents[2]/'sql/kaggle'
                        asyncio.run(persist_empty_intent(queue,run_id,target_spec,launch_nonce=nonce,
                            recovery_journal=recovery_journal,
                            sql_hashes={name:digest((sql_root/name).read_bytes()) for name in SQL_ARTIFACTS}))
                    source=worker_source(template,queue,run_id,token,broker,
                                         transport=os.getenv('RKB_BGE_TRANSPORT','http'),target=target_spec)
                    (folder/'main.py').write_text(source);(folder/'main.py').chmod(0o600)
                    metadata={'id':ref,'title':slug,'code_file':'main.py','language':'python','kernel_type':'script','is_private':True,'enable_gpu':False,'enable_tpu':False,'enable_internet':True,'dataset_sources':[],'competition_sources':[],'kernel_sources':[]}
                    (folder/'kernel-metadata.json').write_text(json.dumps(metadata));(folder/'kernel-metadata.json').chmod(0o600)
                    # Credential is run-bound and source remains private. SDK
                    # provider request is the only notebook creation boundary.
                    saved=api.kernels_push(str(folder),timeout=11*3600)
                    if saved.error or saved.ref!=ref or saved.version_number<1:
                        raise RuntimeError('kaggle_save_receipt_invalid')
                    queue.launch_record(run_id,ref,provider_version=saved.version_number)
                    logging.info(json.dumps({'event':'bge_dispatched','run_id':run_id,'provider_ref':ref,'provider_version':saved.version_number,'cpu_only':True}))
                elif run['launch_state']=='dispatching':
                    # Unknown side effect after crash/lost response. Do not
                    # issue another save/version: reconcile the stable slug.
                    if not reconcile_launch(api,queue,run,ref):
                        logging.warning(json.dumps({'event':'bge_launch_observation_mismatch','run_id':run_id,'provider_ref':ref}))
                elif run['status']=='starting' and run['launch_state']=='dispatched' and time.monotonic()-probed.get(run_id,0)>30:
                    probed[run_id]=time.monotonic()
                    if run.get('provider_version') and not reconcile_launch(api,queue,run,ref):continue
                    status=version_status(api,ref,run.get('provider_version'))
                    phase=provider_phase(status)
                    logging.info(json.dumps({'event':'bge_version_status_observed','run_id':run_id,'provider_version':run['provider_version'],'phase':phase}))
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
