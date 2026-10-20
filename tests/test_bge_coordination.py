"""Committed wakeups and read-only compatibility queue observation."""
import asyncio
import json
import os
from pathlib import Path
import sqlite3
import sys

import pytest
from starlette.applications import Starlette
from starlette.routing import Route
from starlette.testclient import TestClient

from regional_knowledge.bge_broker import register_routes
from regional_knowledge.bge_contract import SPACE
from regional_knowledge.bge_queue import BgeQueue, _WAITERS


def prepared(tmp_path, clock=None):
    queue = BgeQueue(tmp_path / 'queue.sqlite', **({'clock': clock} if clock else {}))
    job_id = queue.enqueue('actor', 'original', ['synthetic query'])
    with queue.connect(write=False) as db:
        run = db.execute('select id from runs').fetchone()[0]
    token = (queue.path.parent / 'bge-worker-credentials' / run).read_text()
    queue.heartbeat(run, token, ready=True)
    job = queue.claim(run, token)
    return queue, run, token, job_id, job


def test_status_and_results_do_not_wait_for_unrelated_sqlite_writer(tmp_path):
    queue, run, token, job_id, _ = prepared(tmp_path)
    with queue.connect() as writer:
        writer.execute('update runs set heartbeat=heartbeat where id=?', (run,))
        assert queue.result('actor', job_id)['state'] == 'claimed'
        assert queue.lookup('actor', 'original')['id'] == job_id
        assert queue.status()['state'] == 'ready'
        assert queue.document_pending() == 0
        # Production creates a new queue object per search; opening that object
        # must not silently reintroduce the same writer lock.
        another_instance = BgeQueue(queue.path)
        assert another_instance.result('actor', job_id)['state'] == 'claimed'
        with pytest.raises(PermissionError):
            queue.result('another-actor', job_id)
    with queue.connect(write=False) as db:
        with pytest.raises(sqlite3.OperationalError, match='readonly'):
            db.execute('delete from jobs')


def test_expired_assignment_returns_conflict_and_run_can_claim_again(tmp_path):
    now = [1000.0]
    queue, run, token, job_id, job = prepared(tmp_path, lambda: now[0])

    class Registrar:
        def __init__(self): self.routes = []
        def custom_route(self, path, methods):
            def register(handler): self.routes.append(Route(path, handler, methods=methods))
            return register

    registrar = Registrar()
    register_routes(registrar, queue)
    with TestClient(Starlette(routes=registrar.routes)) as client:
        headers = {'Authorization': 'Bearer ' + token}
        for _ in range(2):
            now[0] += 90
            queue.heartbeat(run, token)
        now[0] += 1
        payload = {'run_id': run, 'space': SPACE, 'job_id': job_id,
                   'claim': job['claim'], 'vectors': [[1] + [0] * 1023]}
        response = client.post('/bge-worker/complete', json=payload, headers=headers)
        assert response.status_code == 409
        assert response.json() == {'error': 'stale_claim'}
        assert queue.status()['state'] == 'ready'
        reassigned = client.post('/bge-worker/claim', json={'run_id': run, 'space': SPACE}, headers=headers)
        assert reassigned.status_code == 200
        assert reassigned.json()['job']['id'] == job_id
        assert reassigned.json()['job']['claim'] != job['claim']
        with queue.connect() as db:
            db.execute("update runs set status='expired' where id=?", (run,))
        revoked = client.post('/bge-worker/heartbeat', json={'run_id': run, 'space': SPACE}, headers=headers)
        assert revoked.status_code == 403
        assert revoked.json() == {'error': 'revoked_run'}


def test_observation_projects_expired_successor_without_mutating_control(tmp_path):
    now = [1000.0]
    queue, run, _, _, _ = prepared(tmp_path, lambda: now[0])
    with queue.connect() as db:
        successor = queue._new_run(db, now[0])
        db.execute('update control set successor=? where id=1', (successor,))
        now[0] += 901
        db.execute('update runs set heartbeat=?,useful=? where id=?', (now[0], now[0], run))
    status = queue.status()
    assert status['state'] == 'ready'
    assert status['successor_starting'] is False
    with queue.connect(write=False) as db:
        assert db.execute('select successor from control').fetchone()[0] == successor


@pytest.mark.asyncio
async def test_local_completion_wakes_after_commit_without_poll_interval(tmp_path, monkeypatch):
    queue, run, token, job_id, job = prepared(tmp_path)
    another_instance = BgeQueue(queue.path)
    monkeypatch.setattr('regional_knowledge.bge_queue.RECONCILE_SECONDS', 10.0)
    waiter = asyncio.create_task(queue.wait_result('actor', job_id, 1))
    # Let registration and its initial read finish before completion.
    await asyncio.sleep(.02)
    await asyncio.to_thread(another_instance.complete, run, token, job_id, job['claim'], SPACE, [[1] + [0] * 1023], {})
    assert (await asyncio.wait_for(waiter, .5))['state'] == 'done'
    assert (str(queue.path.resolve()), job_id) not in _WAITERS


@pytest.mark.asyncio
async def test_other_process_completion_is_reconciled_without_local_signal(tmp_path):
    queue, run, token, job_id, job = prepared(tmp_path)
    code = '''import json,sys
from regional_knowledge.bge_queue import BgeQueue
from regional_knowledge.bge_contract import SPACE
p=json.loads(sys.stdin.read())
BgeQueue(p['path']).complete(p['run'],p['token'],p['job'],p['claim'],SPACE,[[1]+[0]*1023],{})
'''
    child = await asyncio.create_subprocess_exec(sys.executable, '-c', code,
        stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env={**os.environ, 'PYTHONPATH': str(Path(__file__).resolve().parents[1] / 'src')})
    communicate = asyncio.create_task(child.communicate(json.dumps({
        'path': str(queue.path), 'run': run, 'token': token,
        'job': job_id, 'claim': job['claim']}).encode()))
    try:
        result = await queue.wait_result('actor', job_id, 3)
        await communicate
        assert child.returncode == 0
        assert result and result['state'] == 'done'
    finally:
        if child.returncode is None:
            child.kill()
            await child.wait()
        await communicate


@pytest.mark.asyncio
async def test_wait_denial_and_cancellation_remove_subscriptions(tmp_path):
    queue, _, _, job_id, _ = prepared(tmp_path)
    with pytest.raises(PermissionError):
        await queue.wait_result('another-actor', job_id, 1)
    assert (str(queue.path.resolve()), job_id) not in _WAITERS
    waiter = asyncio.create_task(queue.wait_result('actor', job_id, 5))
    await asyncio.sleep(.02)
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError): await waiter
    assert (str(queue.path.resolve()), job_id) not in _WAITERS
