"""Real handler-slot + SQLite fixture; explicitly NOT live PG/BGE evidence."""
import asyncio
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys

import pytest

from regional_knowledge.kaggle_warm_read import ReadHandlerCompatibility, ReadHandlerSlot, WarmHandlerRejected


ROOT=Path(__file__).resolve().parents[1]
loader=importlib.util.spec_from_file_location('warm_acceptance_harness',ROOT/'scripts/production/verify_kaggle_warm_debug.py')
harness=importlib.util.module_from_spec(loader);loader.loader.exec_module(harness)
BEFORE='CREATE VIEW fixture_view AS SELECT id, body FROM evidence'
AFTER="CREATE VIEW fixture_view AS SELECT id, body || '' AS body FROM evidence"
TEXT='Synthetic fixture evidence'
SQL_PATCH='DROP VIEW fixture_view;'+AFTER+';'
SQL_ROLLBACK='DROP VIEW fixture_view;'+BEFORE+';'


def sha(data):return hashlib.sha256(data.encode() if isinstance(data,str) else data).hexdigest()


def response(rows):
    return {'protocol_version':2,'status':'ok','observed_database_generation':'offline-fixture-db',
        'observed_data_seq':1,'observed_projection_seq':1,'context_gap':False,'prior_write_pending':False,
        'evidence':[{'id':row[0],'kind':'source','text':row[1],
            'dependencies':{'source_ids':['fixture-source'],'variant_refs':[],'grant_refs':[],
                'source_revisions':[{'source_id':'fixture-source','content_revision':1,'eligibility_revision':1}]}}
            for row in rows]}


class ExistingHandler:
    def __init__(self,*,acquire,tokenizer,telemetry,source_provenance,result_builder):
        self.acquire,self.tokenizer,self.telemetry=acquire,tokenizer,telemetry
        self.source_provenance,self.result_builder=source_provenance,result_builder
    async def __call__(self,frame,*,encode):
        return self.result_builder(self.acquire().execute('SELECT id, body FROM fixture_view').fetchall())


class OfflineClient:
    """No network/provider/model: the database is a private synthetic SQLite."""
    def __init__(self,root):
        root.mkdir(mode=0o700,exist_ok=True);root.chmod(0o700)
        self.db=sqlite3.connect(':memory:')
        self.db.executescript('CREATE TABLE evidence(id TEXT PRIMARY KEY,body TEXT);'+BEFORE+';')
        self.db.execute('INSERT INTO evidence VALUES(?,?)',('fixture-evidence',TEXT))
        self.model=object();self.acquire=lambda:self.db;self.telemetry=object()
        self.constructor=dict(acquire=self.acquire,tokenizer=self.model,telemetry=self.telemetry,
                              source_provenance=True,result_builder=response)
        self.compatibility=ReadHandlerCompatibility('offline-fixture-db','schema1','fn1','exec1','b'*64,'c'*64)
        self.slot=ReadHandlerSlot(ExistingHandler(**self.constructor),loaded_sha256='a'*64,
                                 compatibility=self.compatibility)
        self.counters={k:0 for k in harness.COUNTERS}
        self.counters.update(notebook_starts=1,pg_starts=1,model_loads=1)
        self.actions=[];self.drift=None;self.unknown_sql=False
        self.files={}
        for name,broken in [('handler',False),('bad-handler',True)]:
            source=("class KaggleWorkerQuery:\n"
                " def __init__(self, *, acquire, tokenizer, telemetry, source_provenance, result_builder):\n"
                "  self.acquire,self.tokenizer,self.telemetry=acquire,tokenizer,telemetry\n"
                "  self.source_provenance,self.result_builder=source_provenance,result_builder\n"
                " async def __call__(self, frame, *, encode):\n"+
                ("  return self.result_builder([])\n" if broken else
                 "  rows=self.acquire().execute('SELECT t.id,t.body FROM fixture_view AS t').fetchall()\n"
                 "  return self.result_builder(rows)\n"))
            path=root/(name+'.py');path.write_text(source);path.chmod(0o600)
            self.files[name]=(path,sha(source))
        self.root=root
        self.plan={'operation_key':'offline-warm-case','query':'private fixture query',
            'sql_patch':{'id':'sql-forward','sha256':sha(SQL_PATCH)},
            'sql_rollback':{'id':'sql-rollback','sha256':sha(SQL_ROLLBACK)},
            'sql_definition_after':sha(AFTER),
            'sql_definition_before':sha(BEFORE),'expected_loaded_sha256':'a'*64,
            'handler':{'id':'handler','sha256':self.files['handler'][1]},
            'rejected_handler':{'id':'bad-handler','sha256':self.files['bad-handler'][1]},
            'expected_evidence':{'fixture-evidence':sha(TEXT)},'timeout_seconds':10,'min_data_seq':1,
            'expected_identity':dict(run_id='offline-fixture-run',provider_ref='fixture/fixture',provider_version=1,
                runtime_incarnation='offline-fixture-incarnation',database_generation='offline-fixture-db',
                model_sha256='b'*64,dependency_sha256='c'*64)}

    async def probe(self):
        current=self.slot.receipt()
        identity=dict(run_id='offline-fixture-run',provider_ref='fixture/fixture',provider_version=1,
            runtime_incarnation='offline-fixture-incarnation',database_generation='offline-fixture-db',
            pg_system_identifier='OFFLINE_SQLITE_NOT_POSTGRES',pg_started_at='offline-fixture-start',
            model_context_id=str(id(self.model)),model_sha256='b'*64,dependency_sha256='c'*64)
        counters=dict(self.counters)
        if self.actions and self.drift in identity:identity[self.drift]+='-changed'
        if self.actions and self.drift in counters:counters[self.drift]+=1
        return dict(scope='offline',identity=identity,counters=counters,loaded_sha256=current['loaded_sha256'],
                    handler_generation=current['generation'],sql_definition_sha256=sha(
                        self.db.execute("SELECT sql FROM sqlite_master WHERE name='fixture_view'").fetchone()[0]))

    async def query(self,text):return await self.slot({'query':text},encode=None)

    async def apply_sql_artifact(self,*,artifact,operation_key,expected_definition_sha256):
        sql={'sql-forward':SQL_PATCH,'sql-rollback':SQL_ROLLBACK}[artifact['id']]
        assert sha(self.db.execute("SELECT sql FROM sqlite_master WHERE name='fixture_view'").fetchone()[0])==expected_definition_sha256
        assert sha(sql)==artifact['sha256'];self.actions.append(('sql',operation_key))
        self.db.executescript(sql)
        return {'status':'unknown' if self.unknown_sql else 'applied','artifact_sha256':sha(sql)}

    async def _probe_handler(self,handler):
        result=await handler({},encode=None)
        if result['evidence']!=response([('fixture-evidence',TEXT)])['evidence']:
            raise WarmHandlerRejected('fixture_usefulness_probe_failed')

    async def update_read_handler(self,*,artifact,expected_loaded_sha256,operation_key):
        self.actions.append(('handler',operation_key));path,expected=self.files[artifact['id']]
        assert expected==artifact['sha256']
        async def imported(candidate):assert callable(candidate)
        try:
            await self.slot.stage(path=path,trusted_root=self.root,expected_sha256=expected,
                compatibility=self.compatibility,constructor=self.constructor,self_test=imported)
            await self.slot.activate(staged_sha256=expected,expected_loaded_sha256=expected_loaded_sha256,
                                     probe=self._probe_handler)
        except WarmHandlerRejected:return {'status':'rejected'}
        return {'status':'applied'}

    async def rollback_read_handler(self,*,expected_loaded_sha256,operation_key):
        self.actions.append(('rollback',operation_key))
        await self.slot.rollback(expected_loaded_sha256=expected_loaded_sha256,probe=self._probe_handler)
        return {'status':'applied'}

    async def aclose(self):self.slot.close();self.db.close()


@pytest.mark.asyncio
async def test_real_slot_sql_fixture_useful_reads_and_rollback_without_restart(tmp_path):
    client=OfflineClient(tmp_path/'private')
    try:
        report=await harness.run_acceptance(client,client.plan)
        assert report['status']=='passed' and report['scope']=='offline' and not report['live_acceptance']
        assert all(v==0 for v in report['counter_deltas'].values())
        assert [s['step'] for s in report['steps']]==['baseline','sql_applied','handler_applied',
            'failed_candidate_rolled_back','handler_rolled_back','sql_rolled_back']
        assert report['steps'][2]['loaded_sha256']==client.plan['handler']['sha256']
        assert client.slot.receipt()['loaded_sha256']=='a'*64
        assert client.db.execute("SELECT sql FROM sqlite_master WHERE name='fixture_view'").fetchone()[0]==BEFORE
        assert TEXT not in json.dumps(report) and client.plan['query'] not in json.dumps(report)
    finally:await client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize('drift',['run_id','pg_system_identifier','pg_started_at','model_context_id',
                                'notebook_starts','model_loads','dependency_installs','model_download_bytes'])
async def test_identity_or_counter_drift_stops_before_handler_update(tmp_path,drift):
    client=OfflineClient(tmp_path/'private');client.drift=drift
    try:
        with pytest.raises(harness.AcceptanceFailed,match='identity_changed|counter_changed'):
            await harness.run_acceptance(client,client.plan)
        assert [a[0] for a in client.actions]==['sql']
    finally:await client.aclose()


@pytest.mark.asyncio
async def test_uncertain_sql_ack_is_not_retried_or_blindly_rolled_back(tmp_path):
    client=OfflineClient(tmp_path/'private');client.unknown_sql=True
    try:
        trace={}
        with pytest.raises(harness.AcceptanceFailed,match='maintenance_outcome_unknown'):
            await harness.run_acceptance(client,client.plan,trace=trace)
        assert [a[0] for a in client.actions]==['sql']
        assert trace['pending_effect']=='offline-warm-case:sql' and not trace['effects']
        assert client.db.execute("SELECT sql FROM sqlite_master WHERE name='fixture_view'").fetchone()[0]==AFTER
    finally:await client.aclose()


@pytest.mark.asyncio
async def test_native_definition_mismatch_fails_closed(tmp_path):
    client=OfflineClient(tmp_path/'private')
    try:
        wrong=dict(client.plan,sql_definition_after='f'*64)
        with pytest.raises(harness.AcceptanceFailed,match='native_sql_definition'):
            await harness.run_acceptance(client,wrong)
        assert [a[0] for a in client.actions]==['sql']
    finally:await client.aclose()


@pytest.mark.asyncio
async def test_selected_run_and_missing_evidence_fail_before_any_effect(tmp_path):
    client=OfflineClient(tmp_path/'private')
    try:
        wrong=dict(client.plan,expected_identity=dict(client.plan['expected_identity'],run_id='other-run'))
        with pytest.raises(harness.AcceptanceFailed,match='selected_runtime_mismatch'):
            await harness.run_acceptance(client,wrong)
        async def no_evidence(query):return response([])
        client.query=no_evidence
        with pytest.raises(harness.AcceptanceFailed,match='useful_evidence_missing'):
            await harness.run_acceptance(client,client.plan)
        assert client.actions==[]
    finally:await client.aclose()


@pytest.mark.asyncio
async def test_apply_ack_without_loaded_hash_change_is_not_acceptance(tmp_path):
    client=OfflineClient(tmp_path/'private')
    try:
        async def false_ack(**kwargs):return {'status':'applied'}
        client.update_read_handler=false_ack
        with pytest.raises(harness.AcceptanceFailed,match='loaded_handler_readback_changed'):
            await harness.run_acceptance(client,client.plan)
        assert [a[0] for a in client.actions]==['sql']
    finally:await client.aclose()


@pytest.mark.asyncio
async def test_rollback_ack_without_actual_rollback_is_not_acceptance(tmp_path):
    client=OfflineClient(tmp_path/'private')
    try:
        async def false_ack(**kwargs):return {'status':'applied'}
        client.rollback_read_handler=false_ack
        with pytest.raises(harness.AcceptanceFailed,match='loaded_handler_readback_changed'):
            await harness.run_acceptance(client,client.plan)
        assert client.slot.receipt()['loaded_sha256']==client.plan['handler']['sha256']
    finally:await client.aclose()


@pytest.mark.asyncio
async def test_absolute_scenario_deadline_does_not_trigger_effect_retry(tmp_path):
    client=OfflineClient(tmp_path/'private')
    try:
        async def too_slow(query):await asyncio.sleep(2)
        client.query=too_slow
        with pytest.raises(TimeoutError):
            await harness.run_acceptance(client,dict(client.plan,timeout_seconds=1))
        assert client.actions==[]
    finally:await client.aclose()


def test_private_plan_rejects_world_readable_file(tmp_path):
    path=tmp_path/'plan.json';path.write_text('{}');path.chmod(0o644)
    with pytest.raises(harness.AcceptanceFailed,match='private_bounded_plan'):
        harness.private_plan(path)


def offline_client():
    """Executable fixture factory for the operator CLI, never a live adapter."""
    return OfflineClient(Path(os.environ['RKB_WARM_ACCEPTANCE_FIXTURE_ROOT']))


def test_actual_cli_runs_offline_and_writes_private_report(tmp_path):
    client=OfflineClient(tmp_path/'plan-source')
    plan=tmp_path/'private-plan.json';plan.write_text(json.dumps(client.plan));plan.chmod(0o600)
    client.slot.close();client.db.close()
    report=tmp_path/'cli-report.json'
    # CI uses its own repository fixture root; the real CLI still defaults to
    # /home/dev/artifacts. Injection is internal, not a public CLI override.
    (tmp_path/'.artifact.json').write_text(json.dumps({'project':'regional-knowledge-base','scope':'offline-fixture'}))
    environment=dict(os.environ,PYTHONPATH=os.pathsep.join(map(str,[ROOT/'src',ROOT/'tests',ROOT/'scripts/production'])),
                     RKB_WARM_ACCEPTANCE_FIXTURE_ROOT=str(tmp_path/'cli-fixture'))
    entry="import sys; from pathlib import Path; from verify_kaggle_warm_debug import main; root=Path(sys.argv.pop(1)); raise SystemExit(main(artifact_root=root))"
    completed=subprocess.run([sys.executable,'-c',entry,str(tmp_path),
        '--client-factory','test_kaggle_warm_acceptance:offline_client','--plan',str(plan),
        '--report',str(report)],env=environment,capture_output=True,text=True,timeout=10)
    assert completed.returncode==0,completed.stderr
    result=json.loads(report.read_text())
    assert result['status']=='passed' and result['scope']=='offline' and not result['live_acceptance']
    assert report.stat().st_mode&0o077==0
    assert TEXT not in report.read_text() and 'private fixture query' not in report.read_text()


def test_plan_rejects_duplicate_fields_and_signed_url_artifact(tmp_path):
    path=tmp_path/'plan.json';path.write_text('{"query":"a","query":"b"}');path.chmod(0o600)
    with pytest.raises(harness.AcceptanceFailed,match='duplicate_plan_field'):
        harness.private_plan(path)
    with pytest.raises(harness.AcceptanceFailed,match='artifact_binding_invalid'):
        harness.artifact({'id':'https://fixture.invalid/object?signature=private','sha256':'a'*64})


@pytest.mark.asyncio
async def test_blocked_query_over_two_seconds_cannot_pass(tmp_path,monkeypatch):
    client=OfflineClient(tmp_path/'private');now=[0.0]
    monkeypatch.setattr(harness,'time',type('Clock',(),{'monotonic':staticmethod(lambda:now[0])}))
    original=client.query
    async def blocked(query):
        now[0]+=2.1  # No await: event-loop timers cannot intervene.
        return await original(query)
    client.query=blocked
    try:
        with pytest.raises(TimeoutError,match='useful_query_deadline'):
            await harness.run_acceptance(client,client.plan)
        assert client.actions==[]
    finally:await client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize('phase',['initial_probe','effect','final_probe'])
async def test_blocked_callback_exceeding_scenario_budget_never_passes_or_retries(tmp_path,monkeypatch,phase):
    client=OfflineClient(tmp_path/'private');now=[0.0];trace={}
    monkeypatch.setattr(harness,'time',type('Clock',(),{'monotonic':staticmethod(lambda:now[0])}))
    original_probe=client.probe;original_sql=client.apply_sql_artifact;calls=[0]
    async def probe():
        calls[0]+=1
        result=await original_probe()
        # Initial probe + two probes for each of the six successful checks.
        if phase=='initial_probe' or (phase=='final_probe' and calls[0]==13):now[0]+=11
        return result
    async def sql(**kwargs):
        result=await original_sql(**kwargs)
        if phase=='effect':now[0]+=11
        return result
    client.probe=probe;client.apply_sql_artifact=sql
    try:
        with pytest.raises(TimeoutError,match='scenario_deadline'):
            await harness.run_acceptance(client,client.plan,trace=trace)
        if phase=='initial_probe':assert client.actions==[]
        elif phase=='effect':
            assert [x[0] for x in client.actions]==['sql']
            assert trace['pending_effect']=='offline-warm-case:sql' and trace['effects']==[]
        else:assert len(client.actions)==5
    finally:await client.aclose()


@pytest.mark.asyncio
async def test_suppressed_scenario_cancellation_cannot_pass_or_apply(tmp_path):
    client=OfflineClient(tmp_path/'private');original=client.query
    async def suppress(query):
        try:await asyncio.sleep(3)
        except asyncio.CancelledError:pass
        return await original(query)
    client.query=suppress
    try:
        with pytest.raises(TimeoutError):
            await harness.run_acceptance(client,dict(client.plan,timeout_seconds=1))
        assert client.actions==[]
    finally:await client.aclose()
