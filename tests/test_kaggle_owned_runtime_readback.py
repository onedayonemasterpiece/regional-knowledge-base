"""Actual owned PG/ResidentEncoder status, never made-up zero/restart counters."""
import sys
from types import SimpleNamespace
from contextlib import asynccontextmanager

import pytest

from regional_knowledge.bge_contract import REVISION,SPACE,TOKENIZER_SHA256
from regional_knowledge.kaggle_query import PinnedTokenizer
from regional_knowledge.kaggle_worker_query import KaggleWorkerQuery
from regional_knowledge.kaggle_target_runtime import TargetRuntime
from regional_knowledge.kaggle_warm_read import ReadHandlerSlot,ReadHandlerCompatibility
from test_kaggle_target_runtime import spec
from test_bge_wss_worker import CONFIG


class ActualEncoder:
    model_load_count=1
    async def count(self,query):return 5


class PgConnection:
    def __init__(self,statements):self.statements=statements
    async def __aenter__(self):return self
    async def __aexit__(self,*args):return None
    @asynccontextmanager
    async def transaction(self):yield self
    async def execute(self,query,*args):
        self.statements.append(query)
        return self
    async def fetchone(self):
        return ('7399281637401','2026-10-21 14:22:04+00')


def prepared_runtime(monkeypatch):
    statements=[]
    class Owner:
        def __init__(self,database):
            self.spec=database;self.process=SimpleNamespace(pid=1331,returncode=None)
        async def connect(self,**kwargs):
            assert kwargs['user']=='rkb_bootstrap'
            assert kwargs['host']==self.spec.socket
            return PgConnection(statements)
    target=spec()
    runtime=TargetRuntime(dict(CONFIG,target_runtime=dict(target.wire(),sql_root='/unused')),
        executed_source='/fixture',owner_factory=Owner)
    encoder=ActualEncoder()
    tokenizer= PinnedTokenizer(encoder.count,REVISION,TOKENIZER_SHA256,SPACE)
    read=KaggleWorkerQuery(acquire=lambda:None,tokenizer=tokenizer)
    runtime.read_handler_slot=ReadHandlerSlot(read,loaded_sha256='a'*64,
        compatibility=ReadHandlerCompatibility(
            database_generation=runtime.database.database_generation,
            schema_revision=runtime.database.schema_revision,
            function_revision=runtime.database.function_revision,
            executor_revision=runtime.database.executor_revision,
            model_sha256='b'*64,dependency_sha256='c'*64))
    runtime.prepared=SimpleNamespace(readiness=SimpleNamespace(
        hashes=dict(model='b'*64),database_generation=runtime.database.database_generation))
    runtime.identity=(CONFIG['run_id'],CONFIG['launch_nonce'],'incarnation')
    runtime._resident_encoder=encoder
    runtime._resident_model_context_ref='synthetic-context-not-a-capability'
    return runtime,statements




def test_generated_worker_records_actual_stage0_download_and_install_counts(monkeypatch):
    from test_bge_wss_worker import module
    import regional_knowledge.kaggle_stage0 as stage0
    worker=module('bge_kaggle_worker')
    worker.RUN_CONFIG=dict(stage0={'fixture':'only'},prepared_model=None,run_id='synthetic')
    calls=[]
    async_stub=lambda content:[1.]+[0.]*1023
    def verified_stage0(*args,**kwargs):
        calls.append((args,kwargs))
        return dict(downloaded_bytes=4096 if len(calls)==1 else 0,
                    installed=len(calls)==1)
    monkeypatch.setattr(stage0,'prepare_stage0',verified_stage0)
    monkeypatch.setattr(worker,'_load_model',lambda artifacts:async_stub)
    assert worker._RKB_STAGE0_COUNTERS is None
    assert worker.load_encoder() is async_stub
    assert worker._RKB_STAGE0_COUNTERS==dict(
        invocation_count=1,downloaded_bytes=4096,install_count=1)
    assert worker.load_encoder() is async_stub
    assert worker._RKB_STAGE0_COUNTERS==dict(
        invocation_count=2,downloaded_bytes=4096,install_count=1)
    assert len(calls)==2


def test_generated_worker_rejects_missing_stage0_measurement(monkeypatch):
    from test_bge_wss_worker import module
    import regional_knowledge.kaggle_stage0 as stage0
    worker=module('bge_kaggle_worker')
    worker.RUN_CONFIG=dict(stage0={'fixture':'only'},prepared_model=None,run_id='synthetic')
    monkeypatch.setattr(stage0,'prepare_stage0',lambda *args,**kw:dict(installed=True))
    with pytest.raises(ValueError,match='stage0_runtime_counters_missing'):
        worker.load_encoder()
    assert worker._RKB_STAGE0_COUNTERS is None




@pytest.mark.asyncio
async def test_generated_notebook_stage0_location_drives_actual_counter_gate(monkeypatch,tmp_path):
    from dataclasses import replace
    import ast
    from test_kaggle_target_runtime import queue_run,generate
    from test_kaggle_generated_restore import target
    # The real verified-restore layout carries Stage0 without requiring a
    # NEW_EMPTY_GENERATION initialization intent or mutating the queue.
    selected=target()
    queue,run=queue_run(tmp_path)
    source=generate(queue,run,selected)
    tree=ast.parse(source)
    assignments=[node for node in tree.body if isinstance(node,ast.Assign)
        and any(isinstance(item,ast.Name) and item.id=='RUN_CONFIG'
                for item in node.targets)]
    config=ast.literal_eval(assignments[0].value)
    assert config['stage0']==selected.stage0.wire()
    assert 'stage0' not in config['target_runtime']
    runtime,_=prepared_runtime(monkeypatch)
    runtime.config['stage0']=config['stage0']
    assert runtime.target.get('stage0') is None
    monkeypatch.setattr(sys.modules['__main__'],'_MODEL_LOAD_COUNT',1,raising=False)
    monkeypatch.delattr(sys.modules['__main__'],'_RKB_STAGE0_COUNTERS',raising=False)
    denied=await runtime._read_owned_runtime()
    assert denied==dict(status='unavailable',reason='stage0_runtime_counters_unavailable')
    actual=dict(invocation_count=1,install_count=1,downloaded_bytes=123456)
    monkeypatch.setattr(sys.modules['__main__'],'_RKB_STAGE0_COUNTERS',actual,raising=False)
    ready=await runtime._read_owned_runtime()
    assert ready['status']=='ready'
    assert ready['stage0_actual_counters']==actual
    assert ready['resident_model_load_count']==ready['process_model_load_count']


@pytest.mark.asyncio
async def test_actual_pg_superuser_identity_and_model_process_counter(monkeypatch):
    runtime,statements=prepared_runtime(monkeypatch)
    monkeypatch.setattr(sys.modules['__main__'],'_MODEL_LOAD_COUNT',1,raising=False)
    observed=await runtime._read_owned_runtime()
    assert observed['status']=='ready'
    assert observed['pg_system_identifier']=='7399281637401'
    assert observed['pg_postmaster_started_at']=='2026-10-21 14:22:04+00'
    assert observed['pg_process_pid']==1331
    assert observed['resident_model_load_count']==1
    assert observed['process_model_load_count']==1
    assert observed['resident_model_context_ref']=='synthetic-context-not-a-capability'
    assert any('pg_control_system()' in sql for sql in statements)
    assert any('pg_postmaster_start_time()' in sql for sql in statements)
    assert not any('INSERT' in sql.upper() or 'UPDATE' in sql.upper() for sql in statements)


@pytest.mark.asyncio
async def test_live_reloaded_model_or_unavailable_counter_is_not_reported_as_pass(monkeypatch):
    runtime,statements=prepared_runtime(monkeypatch)
    monkeypatch.setattr(sys.modules['__main__'],'_MODEL_LOAD_COUNT',2,raising=False)
    mismatch=await runtime._read_owned_runtime()
    assert mismatch['status']=='inconsistent'
    assert mismatch['resident_model_load_count']==1
    assert mismatch['process_model_load_count']==2
    monkeypatch.delattr(sys.modules['__main__'],'_MODEL_LOAD_COUNT',raising=False)
    unavailable=await runtime._read_owned_runtime()
    assert unavailable['status']=='unavailable'
    assert unavailable['reason']=='resident_model_counter_unavailable'
    assert 'process_model_load_count' not in unavailable




@pytest.mark.asyncio
async def test_actual_stage0_metrics_and_pg_model_identity_survive_readback(monkeypatch):
    runtime,statements=prepared_runtime(monkeypatch)
    # compose_target puts Stage0 at top level, NOT target_runtime; the
    # generated config must exercise the same location used in production.
    runtime.config['stage0']={'configured':True}
    assert runtime.target.get('stage0') is None
    monkeypatch.setattr(sys.modules['__main__'],'_MODEL_LOAD_COUNT',1,raising=False)
    expected=dict(invocation_count=1,downloaded_bytes=65536,install_count=1)
    monkeypatch.setattr(sys.modules['__main__'],'_RKB_STAGE0_COUNTERS',expected,raising=False)
    before=await runtime._read_owned_runtime()
    assert before['status']=='ready'
    assert before['stage0_actual_counters']==expected
    assert before['runtime_process_pid']>0
    assert before['runtime_incarnation']=='incarnation'
    # Actual stage0 counters are not incremented by readback or handler swap.
    after=await runtime._read_owned_runtime()
    assert after['stage0_actual_counters']==before['stage0_actual_counters']
    assert after['pg_system_identifier']==before['pg_system_identifier']
    assert after['pg_postmaster_started_at']==before['pg_postmaster_started_at']
    assert after['resident_model_context_ref']==before['resident_model_context_ref']


@pytest.mark.asyncio
async def test_missing_or_fabricated_stage0_metrics_fail_closed(monkeypatch):
    runtime,_=prepared_runtime(monkeypatch)
    runtime.config['stage0']={'configured':True}
    assert runtime.target.get('stage0') is None
    monkeypatch.setattr(sys.modules['__main__'],'_MODEL_LOAD_COUNT',1,raising=False)
    monkeypatch.delattr(sys.modules['__main__'],'_RKB_STAGE0_COUNTERS',raising=False)
    missing=await runtime._read_owned_runtime()
    assert missing==dict(status='unavailable',reason='stage0_runtime_counters_unavailable')
    monkeypatch.setattr(sys.modules['__main__'],'_RKB_STAGE0_COUNTERS',
                        dict(invocation_count=0,install_count=0,downloaded_bytes=0),raising=False)
    invalid=await runtime._read_owned_runtime()
    assert invalid==dict(status='unavailable',reason='stage0_runtime_counters_unavailable')
    monkeypatch.setattr(sys.modules['__main__'],'_RKB_STAGE0_COUNTERS',
                        dict(invocation_count=1,install_count=2,downloaded_bytes=0),raising=False)
    overreport=await runtime._read_owned_runtime()
    assert overreport==dict(status='unavailable',reason='stage0_runtime_counters_unavailable')


@pytest.mark.asyncio
async def test_owned_pg_observation_failure_does_not_forge_system_or_model_ready(monkeypatch):
    runtime,_=prepared_runtime(monkeypatch)
    monkeypatch.setattr(sys.modules['__main__'],'_MODEL_LOAD_COUNT',1,raising=False)
    async def pg_denied(**kwargs):raise PermissionError('private PG operator details')
    runtime.owner.connect=pg_denied
    result=await runtime._read_owned_runtime()
    assert result=={'status':'unavailable','reason':'owned_pg_observation_unavailable'}
    runtime.closed=True
    result=await runtime._read_owned_runtime()
    assert result['status']=='unavailable'
