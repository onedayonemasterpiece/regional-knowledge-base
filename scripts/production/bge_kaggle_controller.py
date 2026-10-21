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

def provider_error_facts(error):
    """Bounded provider diagnostics without URLs, messages or request bodies."""
    result={'error_type':type(error).__name__}
    response=getattr(error,'response',None)
    status=getattr(response,'status_code',None)
    if type(status) is int and 100<=status<=599:result['http_status']=status
    content=getattr(response,'content',None)
    if type(content) is bytes and len(content)<=16384:
        try:
            value=json.loads(content)
            if type(value) is dict:
                code=value.get('code')
                if type(code) is int and 100<=code<=599:result['provider_code']=code
                message=value.get('message')
                if type(message) is str and 'kernel source size exceeds' in message.lower():
                    result['reason']='kernel_source_size_exceeded'
        except (ValueError,TypeError):pass
    return result

_PREPARATION_STEPS=frozenset({'not_started','create_private_folder',
    'verify_empty_journal_intent','verify_writer_binding','compose_notebook_source',
    'write_private_notebook','write_private_metadata','provider_submission'})


def provider_boundary_report(error, run_id, boundary, preparation_step):
    """Only static stage/exception type for operations; never arbitrary text."""
    facts={'event':'bge_provider_boundary_failed','run_id':run_id,
        'boundary':boundary,'submission_state':'not_sent' if boundary=='launch_preparation' else 'unknown',
        **provider_error_facts(error)}
    if boundary=='launch_preparation':
        facts['preparation_step']=(preparation_step if preparation_step in _PREPARATION_STEPS
                                   else 'not_started')
    return facts


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

def reconcile_launch_due(api,queue,run,ref,probed,*,now):
    """Bound exact source reads while preserving the original unknown launch."""
    previous=probed.get(run['id'])
    if previous is not None and now-previous<30:return None
    probed[run['id']]=now  # Reserve before provider I/O, including failures.
    return reconcile_launch(api,queue,run,ref)


def reconcile_dispatched_terminal(api, queue, run, ref):
    """Observe the exact owned version; never infer death from lost heartbeat.

    A READY worker can disappear after its last heartbeat. We retain that
    run's remote ownership until this native version-qualified terminal check;
    after terminal confirmation the durable cold demand can resume via the
    existing queue maintenance, without another user search. Unknown source,
    mismatched version or provider errors leave the original effect fenced.
    """
    if run.get('launch_state')!='dispatched' or not queue.needs_dispatched_terminal_probe(run['id']):
        return None
    version=run.get('provider_version')
    if type(version) is not int or version < 1:
        return None
    if not reconcile_launch(api, queue, run, ref):
        return 'unknown'
    phase=provider_phase(version_status(api,ref,version))
    logging.info(json.dumps({'event':'bge_exact_version_phase','run_id':run['id'],
        'provider_version':version,'phase':phase}))
    if phase in ('ERROR','FAILED','CANCELED','CANCELLED','COMPLETE'):
        # Kaggle's version label is NOT the execution/session identity. A
        # terminal-looking version response, even with matching source, does
        # not prove this exact worker/its descendants stopped. Keep the
        # UNKNOWN run and capacity fenced; request_startup/startup_status
        # report provider_execution_unknown rather than blindly pushing.
        logging.warning(json.dumps({'event':'bge_provider_version_terminal_unverified',
            'run_id':run['id'],'provider_version':version,'phase':phase,
            'remote_compute_terminal_proof':'unavailable'}))
        return 'terminal_unverified'
    return phase


_LEASE_TERMINAL_PROBED={}
LEASE_TERMINAL_PROBE_SECONDS=30


def lease_terminal_probe_due(queue, run, *, now=None):
    """An owned run whose lease elapsed may be retired only by exact proof.

    _expire keeps a dispatched run's status while its provider outcome is
    unknown, so status alone would never request that proof and every dead
    run would need a manual retire. Probe at most every 30 s per run.
    """
    if run['status']=='expired':return True
    if run['status'] not in ('starting','ready','draining') or not queue._run_expired(run,queue.clock()):
        return False
    now=time.monotonic() if now is None else now
    previous=_LEASE_TERMINAL_PROBED.get(run['id'])
    if previous is not None and now-previous<LEASE_TERMINAL_PROBE_SECONDS:return False
    _LEASE_TERMINAL_PROBED[run['id']]=now
    return True


def reconcile_provider_history(api, queue, *, limit=2, current_only=False):
    """One bounded resumable page; unknown submissions never hold up known rows.

    The native adapter performs no writes. Only its sealed exact-session proof
    can retire a queue reservation; provider bodies never enter diagnostics.
    """
    from regional_knowledge.kaggle_provider_terminal import observe_version_terminal
    retired=0
    if current_only:
        if type(limit) is not int or not 1<=limit<=8:raise ValueError('bounded owner page required')
        owners=sorted(queue.provider_owned_run_ids())
        with queue.connect(write=False) as db:
            marks=','.join('?' for _ in owners)
            runs=[dict(r) for r in db.execute(
                f"select rowid as _cursor,* from runs where id in ({marks}) and launch_state in ('dispatching','dispatched') limit ?",
                (*owners,limit))] if owners else []
    else:runs=queue.provider_reconciliation_page(limit=limit)
    for run in runs:
        try:
            if (type(run['provider_version']) is int and run['provider_ref']
                    and (run['status']=='expired' if not current_only else lease_terminal_probe_due(queue,run))):
                proof=observe_version_terminal(api,run,clock=queue.clock)
                if proof is not None and queue.retire_provider_reservation(proof):
                    retired+=1
                    logging.info(json.dumps({'event':'bge_provider_session_terminal',
                        'run_id':run['id'],'provider_version':run['provider_version'],
                        'phase':proof.status,'contract':proof.contract}))
                else:
                    logging.info(json.dumps({'event':'bge_provider_session_unknown',
                        'run_id':run['id'],'provider_version':run['provider_version']}))
        except Exception as error:
            logging.warning(json.dumps({'event':'bge_provider_reconciliation_failed',
                'run_id':run['id'],'error_type':type(error).__name__}))
        finally:
            if not current_only:queue.advance_provider_reconciliation(run['_cursor'])
    return retired


def require_writer_binding(queue, run_id, writer_generation):
    """Known not-sent refusal when the target writer was already used by another run.

    The service moves rkb_writer_control to the new run only after its attach
    (begin_restore), so before push it still names the previous run. A fresh
    restore target therefore carries a writer generation nobody has used yet;
    one recorded for another run can never begin recovery on this run.
    """
    with queue.connect(write=False) as db:
        used=db.execute('SELECT 1 FROM rkb_writer_generations WHERE writer_generation=? AND run_id!=?',
            (writer_generation,run_id)).fetchone()
    if used is not None:
        raise ValueError('restore writer generation belongs to another run')


def target_sql_hashes(target, repository):
    """Pin exactly the migration prefix the native initializer will execute."""
    from regional_knowledge.kaggle_owned_pg import sql_artifacts, digest
    root=Path(repository)/'sql/kaggle'
    return {name:digest((root/name).read_bytes())
            for name in sql_artifacts(target.database.schema_revision)}


def worker_source(template, queue, run_id, token, broker, *, transport='http', target=None, provider_image=None):
    """Build private launch bytes once; no provider call or inferred version."""
    if transport not in ('http','wss'):
        raise ValueError('unsupported BGE transport')
    config={'run_id':run_id,'token':token,'broker':broker}
    if provider_image is not None:
        if re.fullmatch(r'gcr\.io/kaggle-images/python@sha256:[0-9a-f]{64}',provider_image) is None:
            raise ValueError('exact pinned Kaggle provider image required')
        config['provider_image']=provider_image
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
        image=os.getenv('RKB_BGE_DOCKER_IMAGE','').strip()
        if re.fullmatch(r'gcr\.io/kaggle-images/python@sha256:[0-9a-f]{64}',image) is None:
            raise ValueError('target launch requires exact RKB_BGE_DOCKER_IMAGE digest')
    else:image=None
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
        reconcile_provider_history(api,queue,current_only=True)
        runs=queue.maintenance()
        owners=queue.provider_owned_run_ids()
        for run in runs:
            if run['id'] not in owners:continue
            run_id=run['id'];ref=notebook_ref(run,state);slug=ref.rsplit('/',1)[-1]
            boundary='observation'
            preparation_step='not_started'  # Fixed, low-cardinality diagnostic only.
            try:
                if run['status']=='starting' and run['launch_state']=='pending':
                    token=queue.launch_claim(run_id,provider_ref=ref)
                    if token is None:continue
                    boundary='launch_preparation'
                    preparation_step='create_private_folder'
                    folder=state/run_id;folder.mkdir(mode=0o700,exist_ok=True)
                    if target_spec is not None and target_spec.database.mode=='NEW_EMPTY_GENERATION':
                        preparation_step='verify_empty_journal_intent'
                        import asyncio
                        from regional_knowledge.kaggle_target_launch import persist_empty_intent
                        nonce=queue.prepare_channel(run_id)
                        asyncio.run(persist_empty_intent(queue,run_id,target_spec,launch_nonce=nonce,
                            recovery_journal=recovery_journal,
                            sql_hashes=target_sql_hashes(target_spec,Path(__file__).resolve().parents[2])))
                    if target_spec is not None and target_spec.database.mode=='VERIFIED_INITIALIZED':
                        # A restore target pins one writer generation minted for
                        # one run. A successor created by later demand (reboot,
                        # retire) cannot adopt a generation already used by that
                        # run; pushing would only burn a Kaggle session.
                        preparation_step='verify_writer_binding'
                        require_writer_binding(queue,run_id,target_spec.database.writer_generation)
                    preparation_step='compose_notebook_source'
                    source=worker_source(template,queue,run_id,token,broker,
                                         transport=os.getenv('RKB_BGE_TRANSPORT','http'),target=target_spec,provider_image=image)
                    preparation_step='write_private_notebook'
                    (folder/'main.py').write_text(source);(folder/'main.py').chmod(0o600)
                    metadata={'id':ref,'title':slug,'code_file':'main.py','language':'python','kernel_type':'script','is_private':True,'enable_gpu':False,'enable_tpu':False,'enable_internet':True,'dataset_sources':[],'competition_sources':[],'kernel_sources':[]}
                    # Dataset restore uses a version-qualified native download;
                    # provider mount metadata does not attest its exact version.
                    if image is not None:
                        metadata.update(docker_image=image,docker_image_pinning_type='original')
                    preparation_step='write_private_metadata'
                    (folder/'kernel-metadata.json').write_text(json.dumps(metadata));(folder/'kernel-metadata.json').chmod(0o600)
                    # Credential is run-bound and source remains private. SDK
                    # provider request is the only notebook creation boundary.
                    boundary='save_kernel'
                    preparation_step='provider_submission'
                    saved=api.kernels_push(str(folder),timeout=11*3600)
                    if saved.error or saved.ref!=ref or saved.version_number<1:
                        raise RuntimeError('kaggle_save_receipt_invalid')
                    queue.launch_record(run_id,ref,provider_version=saved.version_number)
                    logging.info(json.dumps({'event':'bge_dispatched','run_id':run_id,'provider_ref':ref,'provider_version':saved.version_number,'cpu_only':True}))
                elif run['launch_state']=='dispatching':
                    # Unknown side effect after crash/lost response. Do not
                    # issue another save/version: reconcile the stable slug.
                    if reconcile_launch_due(api,queue,run,ref,probed,now=time.monotonic()) is False:
                        logging.warning(json.dumps({'event':'bge_launch_observation_mismatch','run_id':run_id,'provider_ref':ref}))
                # Known-version terminal observations are performed only by
                # the bounded persisted history cursor above, including expired
                # legacy rows. Do not also perform latest-source probes here.
            except Exception as error:
                if boundary=='launch_preparation':
                    # No provider save was called; this is a known not-sent
                    # operation, not an uncertain remote effect to re-dispatch.
                    queue.launch_record(run_id,ref,failed=True)
                # No error.message, URL, notebook bytes, token, or tracebacks
                # enter the durable journal. The step is a fixed source label.
                logging.warning(json.dumps(provider_boundary_report(error,run_id,boundary,preparation_step)))
        time.sleep(5)

if __name__=='__main__':main()
