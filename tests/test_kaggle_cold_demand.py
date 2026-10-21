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


def test_concurrent_startup_requests_use_one_stable_run_even_after_lost_launch_ack(tmp_path):
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

    # The controller persists dispatching BEFORE a remote push. A lost ACK,
    # even past the normal pre-READY timeout, may not allocate another run.
    token = queue.launch_claim(run_id, provider_ref="fixture-owner/stable-slug")
    assert token is not None
    now[0] += 901
    replay = queue.request_startup()
    assert replay["startup_ref"] == demands[0]["startup_ref"]
    assert queue.launch_claim(run_id, provider_ref="fixture-owner/stable-slug") is None
    with queue.connect(write=False) as db:
        assert db.execute("select count(*) from runs").fetchone()[0] == 1
        assert db.execute("select launch_state from runs where id=?", (run_id,)).fetchone()[0] == "dispatching"


def test_expired_ready_run_fences_remote_owner_until_terminal_reconciliation(tmp_path):
    now = [1000.0]
    queue = BgeQueue(tmp_path / 'ready-lost.sqlite', clock=lambda: now[0])
    first = queue.request_startup()
    run_id = first['run_id']
    token = queue.launch_claim(run_id, provider_ref='owner/stable-slug')
    queue.launch_record(run_id, 'owner/stable-slug', provider_version=7)
    queue.heartbeat(run_id, token, ready=True)
    now[0] += 121
    repeated = queue.request_startup()
    assert repeated['run_id'] == run_id
    assert repeated['startup_ref'] == first['startup_ref']
    assert queue.status()['state'] == 'unknown'
    for _ in range(2):
        assert len(queue.maintenance()) == 1
    now[0] += ROTATE_SECONDS
    queue.maintenance()  # rotation must not launch successor during UNKNOWN
    with queue.connect(write=False) as db:
        assert db.execute('select count(*) from runs').fetchone()[0] == 1
        assert db.execute('select launch_state from runs where id=?',(run_id,)).fetchone()[0] == 'dispatched'
        assert db.execute('select successor from control where id=1').fetchone()[0] is None


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
    assert s.queue.status()['state']=='unknown'
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
