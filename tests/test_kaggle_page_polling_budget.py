"""Bounded polling of a rendered page through the existing recovery channel."""
import asyncio
from contextlib import nullcontext
from types import SimpleNamespace

import pytest

from regional_knowledge.kaggle_recovery_dispatch import RecoveryPhaseDispatch


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
