"""Publish only explicit aggregate fields from retained private acceptance evidence."""
import argparse,hashlib,json
from pathlib import Path
from regional_knowledge.bge_contract import SPACE,REVISION

def summarize(root,output):
    if not (root/'.artifact.json').is_file():raise RuntimeError('managed evidence directory required')
    def read(name):return json.loads((root/name).read_text())
    ablation=read('ablation-acceptance.json');warm=read('warm-backend.json');http=read('warm-http.json');cold=read('cold-http.json');life=read('lifecycle.json');runtime=read('final-runtime.json');failure=read('actual-startup-failure.json')
    def cold_fast(data):return {key:value for key,value in data.items() if key!='rows'}
    steps=life['steps'];rotation=steps['planned_rotation'];loss=steps['mid_job_loss']
    data={'date':'2026-10-04','runtime_sha':'acfc3d5f2e1a43b1406039dcd344bc405896c147','warm_acceptance_sha':'89daac414ad4a6b2bc9338ce7e3ba5189343c730','selected_warm_mode':'bge_lexical','contract':{'space':SPACE,'revision':REVISION,'dimension':1024,'pooling':'CLS / FP32 / L2','max_tokens':512,'prefix':None,'batch_size':1,'cpu_threads':4},'external_embedding_api_calls':0,'source_verification':{'original_german_chunks':runtime['source_verified_hashes'],'paired_targets_equal':runtime['paired_targets_equal'],'paired_needs':runtime['paired_needs'],'cases':40,'answerable':38,'unsupported':2,'labels':'agent source-verified positive evidence; unjudged remains unknown; no independently judged Precision/nDCG'},'backfill':read('backfill-first.json'),'backfill_replay':read('backfill-idempotent.json'),'query_replay':read('query-replay.json'),'storage_proof':read('production-migration.json'),'sql_fixture_proof':read('bge-migration-proof.json'),'ablation':{key:ablation[key] for key in ('modes','alias_fusion','top10_overlap','paired_language_gap','database_distribution_seconds')},'warm_backend':{users:{key:level[key] for key in ('requests','failures','initial_states','initial_fast','queue_peak','seconds')} for users,level in warm['levels'].items()},'warm_http':{users:{key:value for key,value in level.items() if key!='rows'} for users,level in http['levels'].items()},'cold_http':{users:{key:value for key,value in level.items() if key!='rows'} for users,level in cold['levels'].items()},'cold_first_demand_ready_seconds':read('cold-start.json')['demand_to_ready_seconds'],'last_cold_ready_seconds':runtime['runs'][-1]['cold_seconds'],'rotation':{key:value for key,value in rotation.items() if key not in ('old_run','successor_run')},'mid_job_loss':{key:cold_fast(value) if key=='cold_fast' else value for key,value in loss.items()},'idle_expiry':steps['idle_expiry'],'provider_unavailable':{key:cold_fast(value) if key=='cold_fast' else value for key,value in steps['provider_unavailable_duplicate_start'].items()},'actual_startup_failure':{key:value for key,value in failure.items() if key not in ('run_id','provider_ref','failure_detected_unix')},'credential_boundaries':steps['credential_boundaries'],'user_token_worker_denied':http['user_token_worker_denied'] and cold['user_token_worker_denied'],'temporary_oauth_families_revoked':http['temporary_family_revoked'] and cold['temporary_family_revoked'],'e5_pid_unchanged_during_lifecycle':life['e5_pid_unchanged'],'final_coverage':runtime['counts'],'final_worker_diagnostics':runtime['runs'][-1]['diagnostics'],'final_queue':runtime['queue'],'evidence_sha256':{name:hashlib.sha256((root/name).read_bytes()).hexdigest() for name in ('german-source.json','multilingual-fixture.json','query-vectors.json','ablation-acceptance.json','backfill-first.json','backfill-idempotent.json','warm-backend.json','warm-http.json','cold-http.json','lifecycle.json','actual-startup-failure.json','final-runtime.json')}}
    data['alias_methodology']='Assisted auxiliary experiment with manually case-scoped source-verified alias mappings; not autonomous discovery or one of the seven fixed ablations.'
    if (root/'priority-proof.json').exists():
        data['actual_query_priority']=read('priority-proof.json')
        data['evidence_sha256']['priority-proof.json']=hashlib.sha256((root/'priority-proof.json').read_bytes()).hexdigest()
    if (root/'runtime-readback.json').exists():
        readback=read('runtime-readback.json')
        data['runtime_readback']={key:readback[key] for key in ('runtime_sha','runtime_files_match_final_head','checked_runtime_files','mcp_model_libraries_loaded','public_fast_health')}
        data['evidence_sha256']['runtime-readback.json']=hashlib.sha256((root/'runtime-readback.json').read_bytes()).hexdigest()
    if (root/'provider-readback.json').exists():
        data['provider_readback']=[{key:row[key] for key in ('status','provider_status','cpu_only_private_request')} for row in read('provider-readback.json')]
        data['evidence_sha256']['provider-readback.json']=hashlib.sha256((root/'provider-readback.json').read_bytes()).hexdigest()
    if (root/'final-smoke.json').exists():
        smoke=read('final-smoke.json');data['explicit_alias_http_smoke']=smoke['explicit_alias_smoke'];data['final_smoke_oauth_family_revoked']=smoke['temporary_family_revoked']
        data['evidence_sha256']['final-smoke.json']=hashlib.sha256((root/'final-smoke.json').read_bytes()).hexdigest()
    output.write_text(json.dumps(data,indent=2)+'\n');print('aggregate acceptance saved; private rows/IDs/texts/vectors excluded')

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('root',type=Path);parser.add_argument('output',type=Path);args=parser.parse_args();summarize(args.root,args.output)
