"""Cold target reads must create one durable startup demand, never a BGE query job."""
import asyncio
import concurrent.futures
import json
from types import SimpleNamespace

import pytest
from mcp.server.mcpserver.exceptions import ToolError

from regional_knowledge.bge_queue import BgeQueue,ROTATE_SECONDS
from regional_knowledge.kaggle_public_backend import KaggleSourceBackend
from test_kaggle_public_backend import Gate,target
from test_read_authority import auth
from test_participants import people


def test_concurrent_startup_requests_use_one_stable_run(tmp_path):
    now = [1000.0]
    queue = BgeQueue(tmp_path / "cold.sqlite", clock=lambda: now[0])
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        demands = list(pool.map(lambda _: queue.request_startup(), range(8)))
    assert len({d["startup_ref"] for d in demands}) == 1
    assert all(d["status"] == "pending" for d in demands)
    run_id = demands[0]["run_id"]
    assert demands[0]["startup_ref"].startswith("rkb:startup:v1:")
    assert queue.startup_status(demands[0]['startup_ref'])['status'] == 'pending'
    with queue.connect(write=False) as db:
        assert db.execute("select count(*) from runs").fetchone()[0] == 1
        assert db.execute("select count(*) from jobs").fetchone()[0] == 0




def test_poll_after_control_detach_keeps_history_diagnostic(tmp_path):
    """A detached diagnostic row does not retain current launch authority."""
    q=BgeQueue(tmp_path/'null-current.sqlite',clock=lambda:1000.)
    first=q.request_startup();old=first['run_id']
    assert q.launch_claim(old,provider_ref='owner/rkb-bge-m3-cpu') is not None
    with q.connect() as db:
        db.execute('update control set current_run=null,successor=null where id=1')
    # No maintenance, no replacement run allocation, and no second search.
    poll=q.startup_status(first['startup_ref'])
    assert poll['status']=='pending' and poll['reason']=='target_warming'
    status=q.status()
    assert status['state']=='stopped' and status['reason'] is None
    with q.connect(write=False) as db:
        assert db.execute('select count(*) from runs').fetchone()[0]==1
        assert db.execute('select launch_state from runs where id=?',(old,)).fetchone()[0]=='dispatching'


def test_detached_historical_unknown_allows_one_current_development_launch(tmp_path):
    """History is retained without blocking the newly nominated current request."""
    now=[1000.]
    queue=BgeQueue(tmp_path/'historical-unknown.sqlite',clock=lambda:now[0])
    first=queue.request_startup()
    old_run=first['run_id'];stable='owner/rkb-bge-m3-cpu'
    assert queue.launch_claim(old_run,provider_ref=stable) is not None
    # Recover a historical/migrated control pointer without destroying the
    # original provider claim or any Accepted/remote evidence.
    with queue.connect() as db:
        db.execute('update control set current_run=null,successor=null where id=1')
    new=[row for row in queue.maintenance() if row['status']=='starting' and row['id']!=old_run]
    assert len(new)==1
    replacement=new[0]['id']
    current=queue.request_startup()
    assert current['status']=='pending'
    assert current['startup_ref']==first['startup_ref']
    assert current['run_id']==replacement
    poll=queue.startup_status(first['startup_ref'])
    assert poll==dict(protocol='rkb.startup.v1',startup_ref=first['startup_ref'],
                      status='pending',reason='target_warming')
    assert queue.status()['state']=='starting'
    assert queue.status()['reason'] is None
    assert queue.launch_claim(replacement,provider_ref=stable)
    assert queue.launch_claim(replacement,provider_ref=stable) is None
    with queue.connect(write=False) as db:
        assert db.execute('select count(*) from runs').fetchone()[0]==2
        assert db.execute('select launch_state from runs where id=?',(old_run,)).fetchone()[0]=='dispatching'
        assert db.execute('select launch_state from runs where id=?',(replacement,)).fetchone()[0]=='dispatching'
        assert db.execute('select count(*) from jobs').fetchone()[0]==0


@pytest.mark.asyncio
async def test_authorized_public_cold_read_starts_despite_archived_history(auth,tmp_path):
    queue=BgeQueue(tmp_path/'mcp-historical-unknown.sqlite',clock=lambda:1000.)
    first=queue.request_startup()
    assert queue.launch_claim(first['run_id'],provider_ref='owner/rkb-bge-m3-cpu')
    with queue.connect() as db:
        db.execute('update control set current_run=null,successor=null where id=1')
    queue.configure_shared_reads(auth.resolver)
    backend=KaggleSourceBackend(worker_queue=queue,read_authority=auth.resolver,
        source_gate=Gate(auth.resolver),destination_ref='synthetic-response')
    principal=SimpleNamespace(access_token=auth.tokens.access_token)
    try:
        reply=await backend.search('synthetic question',principal)
        assert reply.status=='pending' and reply.reason=='target_warming'
        assert reply.startup.startup_ref==first['startup_ref']
        with queue.connect(write=False) as db:
            assert db.execute('select count(*) from runs').fetchone()[0]==2
            assert db.execute('select count(*) from jobs').fetchone()[0]==0
    finally:queue._shared_reads.close()


def test_known_not_sent_failures_have_finite_budget_and_honest_unavailable(tmp_path):
    from regional_knowledge.bge_queue import MAX_STARTUP_FAILURES
    now=[1000.]
    queue=BgeQueue(tmp_path/'finite-retry.sqlite',clock=lambda:now[0])
    initial=queue.request_startup()
    current=initial['run_id']
    for number in range(MAX_STARTUP_FAILURES):
        assert queue.launch_claim(current,provider_ref='owner/stable-slug') is not None
        queue.launch_record(current,'owner/stable-slug',failed=True)
        # Duplicate failure receipts must not double-count.
        queue.launch_record(current,'owner/stable-slug',failed=True)
        with queue.connect(write=False) as db:
            state=db.execute('select startup_state,startup_failures from control where id=1').fetchone()
        assert state['startup_failures']==number+1
        if number<MAX_STARTUP_FAILURES-1:
            assert state['startup_state']=='pending'
            now[0]+=61
            active=[row for row in queue.maintenance() if row['status']=='starting']
            assert len(active)==1 and active[0]['id']!=current
            current=active[0]['id']
        else:
            assert state['startup_state']=='blocked'
    assert queue.startup_status(initial['startup_ref'])['status']=='unavailable'
    assert queue.startup_status(initial['startup_ref'])['reason']=='startup_retry_exhausted'
    assert queue.request_startup()['status']=='unavailable'
    # A separate already-authorized document producer may append work, but
    # must NOT create an unbounded replacement run through queue._ensure.
    queued=queue.enqueue('actor','post-block',['some text'],kind='document')
    assert queue.result('actor',queued)['state']=='pending'
    now[0]+=3600
    assert len(queue.maintenance())==MAX_STARTUP_FAILURES
    assert not any(x['status']=='starting' for x in queue.maintenance())


def test_cold_demand_survives_controller_down_and_known_prelaunch_failure(tmp_path):
    now = [1000.0]
    queue = BgeQueue(tmp_path / 'late-controller.sqlite', clock=lambda: now[0])
    first = queue.request_startup()
    ref = first['startup_ref']
    now[0] += 901
    runs = queue.maintenance()
    live = [row for row in runs if row['status'] == 'starting']
    assert len(live) == 1 and live[0]['id'] != first['run_id']
    assert queue.startup_status(ref)['status'] == 'pending'
    run_id = live[0]['id']
    assert queue.launch_claim(run_id, provider_ref='owner/stable-slug') is not None
    queue.launch_record(run_id, 'owner/stable-slug', failed=True)  # known not-sent
    now[0] += 61
    resumed = queue.maintenance()
    new_runs = [row for row in resumed if row['status'] == 'starting']
    assert len(new_runs) == 1 and new_runs[0]['id'] != run_id
    assert queue.startup_status(ref)['status'] == 'pending'
    assert queue.request_startup()['startup_ref'] == ref


@pytest.mark.asyncio
async def test_same_incarnation_wss_reconnect_after_heartbeat_gap_without_new_launch(target):
    from regional_knowledge.kaggle_public_backend import current_binding,TargetUnavailable
    from test_bge_runtime_readiness import observation
    s=target
    now=[1000.0]
    s.queue.clock=lambda:now[0]
    assert s.queue.detach_channel(s.run,s.token,s.generation)
    now[0]+=121
    with pytest.raises(TargetUnavailable):
        current_binding(s.queue)
    attached=s.queue.attach_channel(s.run,s.token,s.nonce,'incarnation-a')
    assert attached['connection_generation']==s.generation+1
    with pytest.raises(TargetUnavailable):
        current_binding(s.queue)  # no stale proof carried into new generation
    s.queue.heartbeat(s.run,s.token,connection_generation=attached['connection_generation'])
    assert s.queue.accept_search_runtime(
        s.run,s.token,attached['connection_generation'],observation()).ok
    assert current_binding(s.queue).connection_generation==attached['connection_generation']
    assert len(s.queue.maintenance())==1


@pytest.mark.asyncio
async def test_authorized_concurrent_cold_public_search_returns_one_pending_demand(auth, tmp_path):
    queue = BgeQueue(tmp_path / "cold-public.sqlite", clock=lambda: 1000.0)
    admission = queue.configure_shared_reads(auth.resolver)
    backend = KaggleSourceBackend(worker_queue=queue, read_authority=auth.resolver,
        source_gate=Gate(auth.resolver), destination_ref="synthetic-response")
    principal = SimpleNamespace(access_token=auth.tokens.access_token)

    async def cold_read():
        return await backend.search("synthetic query", principal)

    try:
        replies = await asyncio.gather(*(cold_read() for _ in range(6)))
        refs = {reply.startup.startup_ref for reply in replies}
        assert len(refs) == 1
        assert all(reply.status == "pending" and reply.reason == "target_warming"
                   and reply.startup.protocol == 'rkb.startup.v1'
                   and not reply.results and reply.main_state == 'starting'
                   for reply in replies)
        assert all(reply.startup.eta_seconds == 300
                   and reply.startup.message_ru is not None
                   and reply.startup.message_ru.startswith("Запускаем инфраструктуру")
                   and "минут" not in reply.startup.message_ru
                   and "секунд" not in reply.startup.message_ru for reply in replies)
        assert queue.status()["state"] == "starting"
        with queue.connect(write=False) as db:
            assert db.execute("select count(*) from runs").fetchone()[0] == 1
            assert db.execute("select count(*) from jobs").fetchone()[0] == 0
    finally:
        admission.close()


@pytest.mark.asyncio
async def test_startup_status_requires_fresh_public_search_authority(auth,tmp_path):
    queue=BgeQueue(tmp_path/'status-acl.sqlite',clock=lambda:1000.0)
    admission=queue.configure_shared_reads(auth.resolver)
    backend=KaggleSourceBackend(worker_queue=queue,read_authority=auth.resolver,
        source_gate=Gate(auth.resolver),destination_ref='synthetic-response')
    ref=queue.request_startup()['startup_ref']
    try:
        principal=SimpleNamespace(access_token=auth.tokens.access_token)
        observed=await backend.startup_status(ref,principal)
        assert observed.status=='pending' and observed.protocol=='rkb.startup.v1'
        assert observed.eta_seconds == 300
        assert observed.message_ru is not None
        assert observed.message_ru.startswith("Запускаем инфраструктуру")
        assert observed.model_dump(exclude_none=True)['eta_seconds'] == 300
        with pytest.raises(ToolError,match='access_denied'):
            await backend.startup_status(ref,SimpleNamespace(access_token='invalid-token'))
        with pytest.raises(ToolError,match='invalid_input'):
            await backend.startup_status('not-a-startup-ref',principal)
    finally:
        admission.close()


@pytest.mark.asyncio
async def test_unverified_caller_cannot_create_cold_startup_demand(auth, tmp_path):
    queue = BgeQueue(tmp_path / "cold-denied.sqlite", clock=lambda: 1000.0)
    admission = queue.configure_shared_reads(auth.resolver)
    backend = KaggleSourceBackend(worker_queue=queue, read_authority=auth.resolver,
        source_gate=Gate(auth.resolver), destination_ref="synthetic-response")
    try:
        with pytest.raises(ToolError):
            await backend.search("synthetic query", SimpleNamespace(access_token="invalid-token"))
        with queue.connect(write=False) as db:
            assert db.execute("select count(*) from runs").fetchone()[0] == 0
    finally:
        admission.close()
