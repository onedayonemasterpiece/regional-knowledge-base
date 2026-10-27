"""Cold pending timing uses real OAuth/queue and a gated executor, no live worker."""
import asyncio
import contextvars
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import json
import threading
from types import SimpleNamespace

import pytest
from mcp.server.mcpserver.exceptions import ToolError

from regional_knowledge.bge_queue import BgeQueue
from regional_knowledge.kaggle_public_backend import KaggleSourceBackend
from regional_knowledge.read_telemetry import ReadTelemetry
from regional_knowledge import read_telemetry
from test_kaggle_public_backend import Gate
from test_read_authority import auth
from test_participants import people


def backend_for(auth,tmp_path):
    queue=BgeQueue(tmp_path/'cold.sqlite',clock=lambda:10000.)
    admission=queue.configure_shared_reads(auth.resolver)
    records=[]
    telemetry=ReadTelemetry(sink=lambda record:records.append(json.loads(record)))
    backend=KaggleSourceBackend(worker_queue=queue,read_authority=auth.resolver,
        source_gate=Gate(auth.resolver),destination_ref='synthetic-response',read_telemetry=telemetry)
    return backend,queue,admission,telemetry,records


@pytest.mark.asyncio
async def test_actual_auth_executor_wait_is_separate_from_callback_and_pending_stages(auth,tmp_path,monkeypatch):
    backend,queue,admission,telemetry,records=backend_for(auth,tmp_path)
    # A private fake diagnostic clock leaves the backend's real 2s deadline and
    # security clocks unchanged. Events establish exact queue/callback times.
    clock=[1.]
    monkeypatch.setattr(read_telemetry,'time',SimpleNamespace(monotonic=lambda:clock[0]))
    original_read=auth.provider.store.read
    def token_callback(callback):
        result=original_read(callback)
        clock[0]+=.125
        return result
    monkeypatch.setattr(auth.provider.store,'read',token_callback)
    original_state=auth.resolver._state
    def state_callback(*args,**kwargs):
        result=original_state(*args,**kwargs)
        clock[0]+=.05
        return result
    monkeypatch.setattr(auth.resolver,'_state',state_callback)
    occupied=threading.Event();release=threading.Event();submitted=asyncio.Event()
    pool=ThreadPoolExecutor(max_workers=1)
    def blocker():occupied.set();release.wait()
    blocker_future=pool.submit(blocker)
    original_thread=asyncio.to_thread
    await original_thread(occupied.wait)
    async def queued(function,*args,**kwargs):
        context=contextvars.copy_context()
        future=asyncio.get_running_loop().run_in_executor(pool,
            partial(context.run,function,*args,**kwargs))
        submitted.set()
        return await future
    monkeypatch.setattr(asyncio,'to_thread',queued)
    task=asyncio.create_task(backend.search('PRIVATE_QUERY',SimpleNamespace(access_token=auth.tokens.access_token)))
    try:
        await asyncio.wait_for(submitted.wait(),1)
        clock[0]=2.
        release.set()
        reply=await task
        assert reply.status=='pending'
        await original_thread(telemetry._queue.join)
        terminal=next(r for r in records if r.get('terminal'))
        stages=terminal['controller_stages_ms']
        assert stages['auth_token_executor_wait_ms']==pytest.approx(1000.)
        assert stages['auth_token_callback_ms']==pytest.approx(125.)
        assert stages['auth_token_resume_ms']==pytest.approx(0.)
        assert stages['auth_state_callback_ms']==pytest.approx(50.)
        assert stages['startup_auth_state_callback_ms']==pytest.approx(50.)
        assert all(stages[key]>=0 for key in ('startup_eta_ms','startup_final_auth_ms','auth_ms'))
        assert terminal['dispatch_state']=='not_dispatched' and terminal['executed_branches']==[]
        assert 'PRIVATE_QUERY' not in json.dumps(records)
        assert auth.tokens.access_token not in json.dumps(records)
        assert reply.startup.startup_ref not in json.dumps(records)
        with queue.connect(write=False) as db:
            assert db.execute('select count(*) from runs').fetchone()[0]==1
            assert db.execute('select count(*) from jobs').fetchone()[0]==0
        assert not auth.resolver._contexts
    finally:
        release.set()
        if not task.done():task.cancel()
        await asyncio.gather(task,return_exceptions=True)
        pool.shutdown(wait=True)
        admission.close()


@pytest.mark.asyncio
async def test_cancelled_queued_auth_reports_unknown_not_zero_and_makes_no_demand(auth,tmp_path,monkeypatch):
    backend,queue,admission,telemetry,records=backend_for(auth,tmp_path)
    occupied=threading.Event();release=threading.Event();submitted=asyncio.Event()
    pool=ThreadPoolExecutor(max_workers=1)
    def blocker():occupied.set();release.wait()
    pool.submit(blocker)
    original_thread=asyncio.to_thread
    await original_thread(occupied.wait)
    async def queued(function,*args,**kwargs):
        context=contextvars.copy_context()
        future=asyncio.get_running_loop().run_in_executor(pool,
            partial(context.run,function,*args,**kwargs))
        submitted.set()
        return await future
    monkeypatch.setattr(asyncio,'to_thread',queued)
    task=asyncio.create_task(backend.search('PRIVATE_CANCEL',SimpleNamespace(access_token=auth.tokens.access_token)))
    try:
        await asyncio.wait_for(submitted.wait(),1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):await task
        await original_thread(telemetry._queue.join)
        terminal=next(r for r in records if r.get('terminal'))
        assert terminal['status']=='cancelled'
        assert terminal['reason']=='caller_cancelled'
        assert 'auth_ms' in terminal['controller_stages_ms']
        assert not any(key.startswith('auth_token_') for key in terminal['controller_stages_ms'])
        with queue.connect(write=False) as db:
            assert db.execute('select count(*) from runs').fetchone()[0]==0
        assert not auth.resolver._contexts
    finally:
        release.set()
        pool.shutdown(wait=True)
        admission.close()


@pytest.mark.asyncio
async def test_final_auth_rejection_still_denied_with_pending_stage_timing(auth,tmp_path,monkeypatch):
    backend,queue,admission,telemetry,records=backend_for(auth,tmp_path)
    async def reject(authority):raise PermissionError('PRIVATE_AUTH_DETAIL')
    monkeypatch.setattr(auth.resolver,'recheck',reject)
    try:
        with pytest.raises(ToolError,match='access_denied'):
            await backend.search('PRIVATE_QUERY',SimpleNamespace(access_token=auth.tokens.access_token))
        await asyncio.to_thread(telemetry._queue.join)
        terminal=next(r for r in records if r.get('terminal'))
        assert terminal['status']=='denied'
        assert 'startup_eta_ms' in terminal['controller_stages_ms']
        assert 'startup_final_auth_ms' in terminal['controller_stages_ms']
        assert 'PRIVATE_' not in json.dumps(records)
        with queue.connect(write=False) as db:
            assert db.execute('select count(*) from runs').fetchone()[0]==1
            assert db.execute('select count(*) from jobs').fetchone()[0]==0
    finally:admission.close()


@pytest.mark.asyncio
async def test_slow_optional_eta_still_obeys_original_deadline_and_logs_interrupted_phase(auth,tmp_path,monkeypatch):
    backend,queue,admission,telemetry,records=backend_for(auth,tmp_path)
    async def slow_estimate(ref):await asyncio.sleep(3)
    monkeypatch.setattr(backend,'_startup_wait',slow_estimate)
    try:
        with pytest.raises(ToolError,match='deadline_exceeded'):
            await backend.search('PRIVATE_TIMEOUT',SimpleNamespace(access_token=auth.tokens.access_token))
        await asyncio.to_thread(telemetry._queue.join)
        terminal=next(r for r in records if r.get('terminal'))
        assert terminal['status']=='unavailable' and terminal['reason']=='deadline_exceeded'
        assert terminal['controller_stages_ms']['startup_eta_ms']>0
        assert 'startup_final_auth_ms' not in terminal['controller_stages_ms']
        assert not auth.resolver._contexts
        assert 'PRIVATE_' not in json.dumps(records)
        with queue.connect(write=False) as db:
            assert db.execute('select count(*) from runs').fetchone()[0]==1
            assert db.execute('select count(*) from jobs').fetchone()[0]==0
    finally:admission.close()


@pytest.mark.asyncio
async def test_worker_finishing_after_cancel_cutoff_stays_unknown(auth,monkeypatch):
    # Deterministically model a worker finishing after resumed is sampled but
    # before its timestamps are inspected. A late completion is not clamped.
    stages={};entered=threading.Event();release=threading.Event();worker_done=threading.Event()
    main=threading.get_ident();now=[0.];cancelled=[False]
    def diagnostic_clock():
        if threading.get_ident()==main and cancelled[0]:
            cutoff=2.
            release.set()
            assert worker_done.wait(1)
            return cutoff
        return now[0]
    monkeypatch.setattr(read_telemetry,'time',SimpleNamespace(monotonic=diagnostic_clock))
    original_thread=asyncio.to_thread
    async def observed_thread(function,*args,**kwargs):
        def invoke():
            try:return function(*args,**kwargs)
            finally:worker_done.set()
        return await original_thread(invoke)
    monkeypatch.setattr(asyncio,'to_thread',observed_thread)
    def callback():
        entered.set();release.wait();now[0]=3.
    token=read_telemetry.authority_timing.set((stages,'auth'))
    try:
        task=asyncio.create_task(read_telemetry.authority_thread('token',callback))
    finally:read_telemetry.authority_timing.reset(token)
    await original_thread(entered.wait)
    cancelled[0]=True
    task.cancel()
    with pytest.raises(asyncio.CancelledError):await task
    assert stages=={'auth_token_executor_wait_ms':0.}
