"""Cold target reads must create one durable startup demand, never a BGE query job."""
import asyncio
import concurrent.futures
import json
from types import SimpleNamespace

import pytest
from mcp.server.mcpserver.exceptions import ToolError

from regional_knowledge.bge_queue import BgeQueue
from regional_knowledge.kaggle_public_backend import KaggleSourceBackend
from test_kaggle_public_backend import Gate
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
    assert demands[0]["startup_ref"] == "rkb:startup:v1:" + run_id
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


@pytest.mark.asyncio
async def test_authorized_concurrent_cold_public_search_returns_one_pending_demand(auth, tmp_path):
    queue = BgeQueue(tmp_path / "cold-public.sqlite", clock=lambda: 1000.0)
    admission = queue.configure_shared_reads(auth.resolver)
    backend = KaggleSourceBackend(worker_queue=queue, read_authority=auth.resolver,
        source_gate=Gate(auth.resolver), destination_ref="synthetic-response")
    principal = SimpleNamespace(access_token=auth.tokens.access_token)

    async def cold_read():
        with pytest.raises(ToolError) as exc:
            await backend.search("synthetic query", principal)
        return json.loads(str(exc.value))

    try:
        replies = await asyncio.gather(*(cold_read() for _ in range(6)))
        refs = {reply["startup_ref"] for reply in replies}
        assert len(refs) == 1
        assert all(reply["status"] == "pending" and reply["reason"] == "target_warming"
                   for reply in replies)
        assert queue.status()["state"] == "starting"
        with queue.connect(write=False) as db:
            assert db.execute("select count(*) from runs").fetchone()[0] == 1
            assert db.execute("select count(*) from jobs").fetchone()[0] == 0
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
