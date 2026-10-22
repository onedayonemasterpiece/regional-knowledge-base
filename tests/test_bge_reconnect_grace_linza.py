"""Actual BgeWssTransport reconnect boundaries with fake clock/network only.

PR228 already tests a 70s *failed* outage and 600s expiry, plus earlier
lost-ACK replay. This leaf tests the missing combination: a recovery AFTER
>30s but BEFORE 600s with the SAME pending result and incarnation, and
no false READY during exhausted reconnects. No BGE, live sockets or PG.
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import json

import pytest

from test_bge_wss_worker import CONFIG, module
from regional_knowledge.bge_contract import SPACE


@pytest.mark.asyncio
async def test_recovery_after_eighty_seconds_keeps_exact_pending_job_and_incarnation(monkeypatch):
    transport = module("bge_wss_transport")
    monkeypatch.setattr(transport.random, "uniform", lambda *_: .0001)
    clock = [0.0]
    stop = asyncio.Event()
    attempts, attachments, sent, loads = [], [], [], []

    @asynccontextmanager
    async def connector():
        attempt = len(attempts) + 1
        attempts.append((attempt, clock[0]))
        if 2 <= attempt <= 9:
            clock[0] += 10.0
            raise ConnectionError("synthetic Dev unavailable")
        assert attempt in (1, 10), f"unexpected reconnect attempt {attempt}"
        generation = len(attachments) + 1
        incoming = asyncio.Queue()

        async def enqueue(frame):
            await incoming.put(json.dumps({
                "protocol_version": 2, "connection_generation": generation, **frame}))

        class Socket:
            async def send(self, raw):
                frame = json.loads(raw)
                sent.append(frame)
                kind = frame["type"]
                if kind == "ATTACH":
                    attachments.append((frame["runtime_incarnation"], frame["launch_nonce"]))
                    await enqueue({"type": "WELCOME", "provider_version": "85"})
                elif kind == "READY":
                    await enqueue({"type": "READY_ACK", "status": "ready"})
                    # Repeat the exact claim on reconnect, not a new job.
                    await enqueue({"type": "DISPATCH", "id": "fixture-job",
                        "claim": "fixture-claim", "kind": "document",
                        "texts": ["synthetic-book-evidence"], "space": SPACE})
                elif kind == "HEARTBEAT":
                    await enqueue({"type": "HEARTBEAT_ACK"})
                elif kind == "RESULT":
                    if generation == 1:
                        clock[0] = 10.0
                        raise ConnectionError("synthetic lost result acknowledgment")
                    await enqueue({"type": "RESULT_ACK", "job_id": "fixture-job",
                                   "claim": "fixture-claim", "status": "duplicate"})
                    asyncio.get_running_loop().call_later(.01, stop.set)
                else:
                    raise AssertionError(f"unexpected frame: {kind}")
            async def receive(self):
                return await incoming.get()
        yield Socket()

    def load_once():
        loads.append("load")
        def encode(text):
            loads.append(text)
            return [1.0] + [0.0] * 1023
        return encode

    worker = transport.BgeWssTransport(CONFIG, load_once, lambda: {},
        connector=connector, heartbeat_seconds=.01, maximum_seconds=4,
        reconnect_grace_seconds=600, stop=stop)
    worker._retry_clock = lambda: clock[0]
    await worker.run()
    assert len(attempts) == 10
    assert 30 < attempts[-1][1] - 10 < 600  # fake-clock outage elapsed 80s
    assert loads == ["load", "synthetic-book-evidence"]  # no duplicated work
    assert len(attachments) == 2
    assert attachments[0] == attachments[1]  # SAME incarnation + nonce
    results = [frame for frame in sent if frame["type"] == "RESULT"]
    assert len(results) == 2
    assert results[0]["connection_generation"] == 1
    assert results[1]["connection_generation"] == 2
    assert {k:v for k,v in results[0].items() if k!="connection_generation"} == {
           k:v for k,v in results[1].items() if k!="connection_generation"}
    assert worker.pending is None and worker.acknowledged == (
        "fixture-job", "fixture-claim")


@pytest.mark.asyncio
async def test_unavailable_peer_exhausts_grace_without_new_ready_or_new_runtime(monkeypatch):
    transport = module("bge_wss_transport")
    monkeypatch.setattr(transport.random, "uniform", lambda *_: .0001)
    clock = [0.0]
    attempts, outgoing, loads = [], [], []
    @asynccontextmanager
    async def connector():
        attempt = len(attempts) + 1
        attempts.append(attempt)
        if attempt > 1:
            clock[0] += 100.0
            raise ConnectionError("peer remains unavailable past bounded grace")
        incoming = asyncio.Queue()
        class Socket:
            async def send(self, raw):
                frame=json.loads(raw)
                outgoing.append(frame)
                if frame["type"]=="ATTACH":
                    await incoming.put(json.dumps(dict(
                        type="WELCOME",protocol_version=2,connection_generation=1,
                        provider_version="85")))
                elif frame["type"]=="READY":
                    await incoming.put(json.dumps(dict(
                        type="READY_ACK",protocol_version=2,connection_generation=1,
                        status="ready")))
                    await incoming.put(ConnectionError("synthetic peer disconnect"))
                elif frame["type"]=="HEARTBEAT":
                    await incoming.put(json.dumps(dict(
                        type="HEARTBEAT_ACK",protocol_version=2,connection_generation=1)))
                else:
                    raise AssertionError(frame["type"])
            async def receive(self):
                next_item=await incoming.get()
                if isinstance(next_item,Exception):
                    raise next_item
                return next_item
        yield Socket()
    def load_once():
        loads.append("load")
        return lambda _: [1.0]+[0.0]*1023
    worker=transport.BgeWssTransport(CONFIG,load_once,lambda:{},
        connector=connector,heartbeat_seconds=.01,maximum_seconds=3,
        reconnect_grace_seconds=600)
    worker._retry_clock=lambda:clock[0]
    with pytest.raises(transport.BgeChannelStopped, match="reconnect exhausted"):
        await worker.run()
    assert clock[0] >= 600
    assert attempts[0] == 1 and len(attempts) >= 7
    assert len([item for item in outgoing if item["type"]=="ATTACH"]) == 1
    # The server was not reachable after the first session: no false later READY.
    assert len([item for item in outgoing if item["type"]=="READY"]) == 1
    assert loads == ["load"]  # no artificial model reload during reconnect
    assert worker._outage_started is not None
