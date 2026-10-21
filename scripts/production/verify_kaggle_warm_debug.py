"""One bounded same-run acceptance scenario over an existing maintenance client.

This is an operator harness, not a transport, SQL executor or public MCP tool.
Its client owns authentication, exact artifact delivery and native readbacks.
No provider launch/stop interface is accepted here. Receipts omit evidence text.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import importlib
import inspect
import json
import os
from pathlib import Path
import re
import stat
import time


IDENTITY = frozenset({'run_id','provider_ref','provider_version','runtime_incarnation',
    'database_generation','pg_system_identifier','pg_started_at','model_context_id',
    'model_sha256','dependency_sha256'})
COUNTERS = frozenset({'notebook_starts','worker_restarts','pg_starts','model_loads',
    'dependency_installs','model_download_bytes'})
HASH = re.compile('[0-9a-f]{64}\\Z')


class AcceptanceFailed(RuntimeError):
    pass


def require(condition, reason):
    if not condition:raise AcceptanceFailed(reason)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def artifact(value):
    require(type(value) is dict and set(value)=={'id','sha256'}, 'artifact_binding_invalid')
    require(type(value['id']) is str and
        re.fullmatch('[A-Za-z0-9][A-Za-z0-9._:/-]{0,255}',value['id']) and
        '://' not in value['id'], 'artifact_binding_invalid')
    require(type(value['sha256']) is str and HASH.fullmatch(value['sha256']), 'artifact_binding_invalid')
    return dict(value)


def plan_from_wire(value):
    fields={'operation_key','query','sql_patch','sql_rollback','sql_definition_after',
            'handler','rejected_handler','expected_evidence','timeout_seconds','expected_identity','min_data_seq',
            'expected_loaded_sha256','sql_definition_before'}
    require(type(value) is dict and set(value)==fields, 'plan_fields_invalid')
    require(type(value['operation_key']) is str and
        re.fullmatch('[A-Za-z0-9][A-Za-z0-9._:-]{0,127}',value['operation_key']), 'operation_key_invalid')
    require(type(value['query']) is str and 1<=len(value['query'].encode())<=4096, 'query_invalid')
    require(type(value['timeout_seconds']) is int and 1<=value['timeout_seconds']<=120, 'budget_invalid')
    selected=value['expected_identity']
    selected_fields=IDENTITY-{'pg_system_identifier','pg_started_at','model_context_id'}
    require(type(selected) is dict and set(selected)==selected_fields, 'selected_runtime_required')
    require(all(type(v) is int and v>=1 if k=='provider_version' else
                type(v) is str and 1<=len(v)<=256 for k,v in selected.items()), 'selected_runtime_invalid')
    require(type(value['min_data_seq']) is int and value['min_data_seq']>=0, 'minimum_cursor_invalid')
    for key in ('sql_definition_after','sql_definition_before','expected_loaded_sha256'):
        require(type(value[key]) is str and HASH.fullmatch(value[key]), 'initial_or_sql_pin_invalid')
    expected=value['expected_evidence']
    require(type(expected) is dict and 1<=len(expected)<=5, 'expected_evidence_required')
    require(all(type(k) is str and 1<=len(k)<=256 and type(v) is str and HASH.fullmatch(v)
                for k,v in expected.items()), 'expected_evidence_invalid')
    result=dict(value,expected_evidence=dict(expected),expected_identity=dict(selected))
    for name in ('sql_patch','sql_rollback','handler','rejected_handler'):
        result[name]=artifact(value[name])
    require(result['handler']['sha256']!=result['rejected_handler']['sha256'], 'distinct_failure_candidate_required')
    return result


def probe_from_wire(value):
    fields={'scope','identity','counters','loaded_sha256','handler_generation','sql_definition_sha256'}
    require(type(value) is dict and set(value)==fields, 'probe_fields_invalid')
    require(value['scope'] in ('offline','live'), 'probe_scope_invalid')
    identity=value['identity'];counters=value['counters']
    require(type(identity) is dict and set(identity)==IDENTITY, 'runtime_identity_incomplete')
    for k,v in identity.items():
        require(type(v) is int and v>=1 if k=='provider_version' else
                type(v) is str and 1<=len(v)<=256, 'runtime_identity_invalid')
    require(all(HASH.fullmatch(identity[k]) for k in ('model_sha256','dependency_sha256')),
            'runtime_model_or_dependency_pin_invalid')
    require(type(counters) is dict and set(counters)==COUNTERS and
        all(type(v) is int and v>=0 for v in counters.values()), 'runtime_counters_invalid')
    require(type(value['handler_generation']) is int and value['handler_generation']>=0,
            'handler_generation_invalid')
    for key in ('loaded_sha256','sql_definition_sha256'):
        require(type(value[key]) is str and HASH.fullmatch(value[key]), 'runtime_hash_invalid')
    return dict(value,identity=dict(identity),counters=dict(counters))


def same_context(before, after):
    require(after['scope']==before['scope'], 'acceptance_scope_changed')
    require(after['identity']==before['identity'], 'run_pg_or_model_identity_changed')
    require(after['counters']==before['counters'], 'start_restart_install_or_model_counter_changed')


def useful(result, plan, baseline):
    from regional_knowledge.kaggle_protocol import EvidenceItem
    require(type(result) is dict and result.get('protocol_version')==2 and
        result.get('status')=='ok' and not result.get('context_gap') and
        not result.get('prior_write_pending'), 'useful_read_unavailable')
    require(result.get('observed_database_generation')==baseline['identity']['database_generation'],
            'query_database_generation_changed')
    for key in ('observed_data_seq','observed_projection_seq'):
        require(type(result.get(key)) is int and result[key]>=0, 'query_freshness_unobserved')
    evidence=result.get('evidence')
    require(type(evidence) in (tuple,list) and 1<=len(evidence)<=5, 'useful_evidence_missing')
    require(result['observed_data_seq']>=plan['min_data_seq'] and
            result['observed_projection_seq']>=result['observed_data_seq'], 'query_freshness_unobserved')
    try:parsed=[EvidenceItem.model_validate_json(json.dumps(item)) for item in evidence]
    except (ValueError,TypeError):raise AcceptanceFailed('evidence_wire_invalid') from None
    observed={item.id:digest(item.text.encode()) for item in parsed}
    require(all(observed.get(k)==v for k,v in plan['expected_evidence'].items()),
            'expected_evidence_changed_or_missing')
    # Neither source text nor the private query is copied into the receipt.
    return {'evidence_count':len(parsed),'evidence_sha256':digest(json.dumps(observed,sort_keys=True).encode()),
            'data_seq':result['observed_data_seq'],'projection_seq':result['observed_projection_seq']}


async def run_acceptance(client, wire_plan, *, trace=None):
    """Stop on UNKNOWN/transport failure; never retry an uncertain effect.

    Client methods are internal adapter callbacks over the existing maintenance
    owner, not new RPC names: probe(), query(text), apply_sql_artifact(),
    update_read_handler(), rollback_read_handler(). Mutations receive a stable
    per-step operation_key and exact artifact/hash. Native SQL and loaded-code
    readbacks are checked again independently through probe().
    """
    plan=plan_from_wire(wire_plan);steps=[];started=time.monotonic()
    trace={} if trace is None else trace
    trace.update(operation_key=plan['operation_key'],completed_steps=steps,effects=[],pending_effect=None)
    async def effect(method, step, **options):
        key=plan['operation_key']+':'+step;trace['pending_effect']=key
        receipt=await getattr(client,method)(operation_key=key,**options)
        require(type(receipt) is dict and receipt.get('status') in ('applied','rejected'),
                'maintenance_outcome_unknown')
        trace['effects'].append({'operation_key':key,'status':receipt['status']})
        trace['pending_effect']=None
        return receipt
    async def check(label, baseline, loaded, generation, sql):
        observed=probe_from_wire(await client.probe());trace['last_probe']=observed
        same_context(baseline,observed)
        require(observed['loaded_sha256']==loaded and observed['handler_generation']==generation,
                'loaded_handler_readback_changed')
        require(observed['sql_definition_sha256']==sql, 'native_sql_definition_readback_changed')
        async with asyncio.timeout(2):
            read=useful(await client.query(plan['query']),plan,baseline)
        final=probe_from_wire(await client.probe())
        trace['last_probe']=final
        require(final==observed,'runtime_changed_during_useful_read')
        steps.append(dict(step=label,elapsed_ms=round((time.monotonic()-started)*1000,3),
                          loaded_sha256=loaded,handler_generation=generation,**read))
        return final
    async with asyncio.timeout(plan['timeout_seconds']):
        baseline=probe_from_wire(await client.probe());initial=baseline['loaded_sha256']
        trace['baseline']=baseline
        require(all(baseline['identity'][k]==v for k,v in plan['expected_identity'].items()),
                'selected_runtime_mismatch')
        generation=baseline['handler_generation'];sql=baseline['sql_definition_sha256']
        require(initial==plan['expected_loaded_sha256'] and sql==plan['sql_definition_before'],
                'initial_handler_or_sql_pin_changed')
        await check('baseline',baseline,initial,generation,sql)
        receipt=await effect('apply_sql_artifact','sql',artifact=plan['sql_patch'],
                             expected_definition_sha256=sql)
        require(receipt['status']=='applied' and receipt.get('artifact_sha256')==plan['sql_patch']['sha256'],
                'sql_apply_not_confirmed')
        await check('sql_applied',baseline,initial,generation,plan['sql_definition_after'])
        receipt=await effect('update_read_handler','handler',artifact=plan['handler'],expected_loaded_sha256=initial)
        require(receipt['status']=='applied', 'handler_apply_not_confirmed')
        loaded=plan['handler']['sha256'];generation+=1
        await check('handler_applied',baseline,loaded,generation,plan['sql_definition_after'])
        receipt=await effect('update_read_handler','rejected-handler',artifact=plan['rejected_handler'],
                             expected_loaded_sha256=loaded)
        require(receipt['status']=='rejected', 'failure_candidate_was_not_rejected')
        await check('failed_candidate_rolled_back',baseline,loaded,generation,plan['sql_definition_after'])
        receipt=await effect('rollback_read_handler','handler-rollback',expected_loaded_sha256=loaded)
        require(receipt['status']=='applied', 'handler_rollback_not_confirmed')
        generation+=1
        await check('handler_rolled_back',baseline,initial,generation,plan['sql_definition_after'])
        receipt=await effect('apply_sql_artifact','sql-rollback',artifact=plan['sql_rollback'],
                             expected_definition_sha256=plan['sql_definition_after'])
        require(receipt['status']=='applied' and receipt.get('artifact_sha256')==plan['sql_rollback']['sha256'],
                'sql_rollback_not_confirmed')
        final=await check('sql_rolled_back',baseline,initial,generation,sql)
    return {'status':'passed','scope':baseline['scope'],'operation_key':plan['operation_key'],
            'identity':baseline['identity'],'counter_deltas':{k:final['counters'][k]-baseline['counters'][k]
                for k in COUNTERS},'steps':steps,'live_acceptance':baseline['scope']=='live'}


def private_plan(path):
    fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    try:
        info=os.fstat(fd)
        require(stat.S_ISREG(info.st_mode) and not info.st_mode&0o077 and 1<=info.st_size<=65536,
                'private_bounded_plan_required')
        data=os.read(fd,65537);require(len(data)==info.st_size,'plan_changed_during_read')
    finally:os.close(fd)
    def unique(pairs):
        result={}
        for key,value in pairs:
            require(key not in result,'duplicate_plan_field')
            result[key]=value
        return result
    return plan_from_wire(json.loads(data,object_pairs_hook=unique))


def main(argv=None, *, artifact_root=Path('/home/dev/artifacts')):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--client-factory',required=True,help='Trusted existing-owner adapter module:factory')
    parser.add_argument('--plan',required=True,type=Path)
    parser.add_argument('--report',required=True,type=Path)
    args=parser.parse_args(argv)
    report=args.report.resolve();root=Path(artifact_root).resolve()
    require(report.is_relative_to(root) and any((p/'.artifact.json').is_file() for p in report.parents
        if p.is_relative_to(root)), 'managed_artifact_report_required')
    trace={}
    async def execute():
        plan=private_plan(args.plan)
        module,name=args.client_factory.split(':',1)
        factory=getattr(importlib.import_module(module),name)
        client=factory()
        if inspect.isawaitable(client):client=await client
        try:return await run_acceptance(client,plan,trace=trace)
        finally:
            close=getattr(client,'aclose',None)
            if close is not None:await close()
    try:result=asyncio.run(execute())
    except Exception as error:
        result={'status':'failed','error_type':type(error).__name__,
                'reason':'acceptance_assertion_failed' if type(error) is AcceptanceFailed
                    else 'no_retry_after_unknown_outcome',
                'live_acceptance':False,'trace':trace}
    fd=os.open(report,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    with os.fdopen(fd,'w') as handle:json.dump(result,handle,indent=2)
    print(json.dumps({'status':result['status'],'live_acceptance':result['live_acceptance'],'report':str(report)}))
    return 0 if result['status']=='passed' else 1


if __name__=='__main__':raise SystemExit(main())
