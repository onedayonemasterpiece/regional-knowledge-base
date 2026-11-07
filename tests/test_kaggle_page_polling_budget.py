"""Bounded polling of a rendered page through the existing recovery channel."""
import asyncio
import json
from contextlib import nullcontext
from types import SimpleNamespace

import pytest

from regional_knowledge.kaggle_recovery_dispatch import RecoveryPhaseDispatch
from regional_knowledge.kaggle_pg_snapshot_native import SnapshotError


@pytest.mark.asyncio
@pytest.mark.parametrize("render_seconds", [16, 35])
async def test_page_render_uses_at_most_two_channel_status_polls(monkeypatch, render_seconds):
    elapsed = 0
    polls = []

    async def sleep(seconds):
        nonlocal elapsed
        elapsed += seconds
        await original_sleep(0)

    original_sleep = asyncio.sleep
    monkeypatch.setattr("regional_knowledge.kaggle_recovery_dispatch.asyncio.sleep", sleep)

    class Queue:
        shared_reads = SimpleNamespace(_event=SimpleNamespace(set=lambda: None))

        def connect(self, **_):
            return nullcontext(SimpleNamespace(
                execute=lambda *_: SimpleNamespace(fetchone=lambda: None)))

        async def wait_result(self, _actor, phase, _timeout):
            if phase == "original_pages":
                value = dict(status="pending", handle="render-handle")
            else:
                polls.append(elapsed)
                value = (dict(status="complete", value={"physical_page_index": 0})
                         if elapsed >= render_seconds else dict(status="pending"))
            return dict(result=dict(value=value))

    bridge = object.__new__(RecoveryPhaseDispatch)
    bridge.queue = Queue()
    bridge.identity = ("run", "nonce", "incarnation")
    bridge.database = SimpleNamespace(database_generation="db", writer_generation="writer")
    bridge.timeout = 40
    bridge._bindings = {}
    bridge.current = lambda _: True
    bridge.bind_attachment = lambda _: None
    bridge._enqueue = lambda identity: (identity["phase"], "maintenance:run")

    page = await bridge.request(object(), "original_pages", {"index": 0}, operation_key="page-0")
    assert page == {"physical_page_index": 0}
    assert 1 <= len(polls) <= 2, polls


@pytest.mark.asyncio
@pytest.mark.parametrize("cached_error,changed", [(False, False), (True, False), (False, True)])
async def test_completed_page_reconciliation_uses_result_without_rewaiting(monkeypatch, cached_error, changed):
    sleeps=[];enqueued=[]
    pending=dict(status="pending",handle="retained-render")
    page=dict(physical_page_index=89)
    original_sleep=asyncio.sleep
    async def sleep(seconds):
        sleeps.append(seconds)
        await original_sleep(0)
    monkeypatch.setattr("regional_knowledge.kaggle_recovery_dispatch.asyncio.sleep",sleep)

    class Queue:
        shared_reads=SimpleNamespace(_event=SimpleNamespace(set=lambda:None))
        def connect(self, **_):
            def execute(_sql, args):
                row=(dict(state="done",result=json.dumps(dict(value=pending)))
                     if args[1]=="recovery:page-0" else None)
                return SimpleNamespace(fetchone=lambda:row)
            return nullcontext(SimpleNamespace(execute=execute))
        async def wait_result(self, _actor, phase, _timeout):
            if phase=="snapshot_status":
                result=dict(error="page-unavailable",retryable=True) if cached_error else page
                value=dict(status="complete",value=result)
            else:
                value=page if cached_error else pending
            return dict(result=dict(value=value))

    bridge=object.__new__(RecoveryPhaseDispatch)
    bridge.queue=Queue();bridge.identity=("run","nonce","incarnation")
    bridge.database=SimpleNamespace(database_generation="db",writer_generation="writer")
    bridge.timeout=40;bridge._bindings={}
    bridge.current=lambda _:True;bridge.bind_attachment=lambda _:None
    def enqueue(identity):
        if identity["phase"]=="original_pages" and identity["arguments"]!={"index":89}:
            raise SnapshotError("recovery_operation_changed")
        enqueued.append((identity["phase"],identity["operation_key"]))
        return identity["phase"],"maintenance:run"
    bridge._enqueue=enqueue
    if changed:
        with pytest.raises(SnapshotError,match="recovery_operation_changed"):
            await bridge.request(object(),"original_pages",{"index":90},operation_key="page-0")
        return
    result=await bridge.request(object(),"original_pages",{"index":89},operation_key="page-0")
    assert result==page
    assert not sleeps
    assert len([x for x in enqueued if x[0]=="snapshot_status"])==1
    assert [key for phase,key in enqueued if phase=="original_pages"]==(
        ["page-0:retry:1"] if cached_error else ["page-0"])
