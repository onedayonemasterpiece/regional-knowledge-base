"""Independent review-only reconnect regressions; no Kaggle or model runtime."""
import asyncio
from contextlib import asynccontextmanager
import json

import pytest

from test_bge_wss_worker import CONFIG, module
from regional_knowledge.bge_contract import SPACE


@pytest.mark.asyncio
async def test_two_outage_waves_pending_replayed_once_no_encoder_reload(monkeypatch):
    transport = module("bge_wss_transport")
    monkeypatch.setattr(transport.random, "uniform", lambda *_: .0001)
    stop = asyncio.Event()
    clock = [0.0]
    attempts, sessions, sent, calls = [], [], [], []

    @asynccontextmanager
    async def connect():
        attempt = len(attempts) + 1
        attempts.append(attempt)
        if attempt <= 8 or 10 <= attempt <= 17:
            raise ConnectionError("temporary outage")
        generation = len(sessions) + 1
        sessions.append(generation)
        incoming = asyncio.Queue()

        async def enqueue(frame):
            await incoming.put(json.dumps({"protocol_version": 2,
                                           "connection_generation": generation, **frame}))

        class Socket:
            async def send(self, raw):
                frame = json.loads(raw)
                sent.append(frame)
                if frame["type"] == "ATTACH":
                    await enqueue({"type": "WELCOME", "provider_version": "85"})
                elif frame["type"] == "READY":
                    await enqueue({"type": "READY_ACK", "status": "ready", "provider_version": "85"})
                    if generation == 2:
                        # Peer repeats exact claim before its original result is acknowledged.
                        await enqueue({"type": "DISPATCH", "id": "job", "claim": "claim",
                                       "kind": "document", "texts": ["same text"], "space": SPACE})
                elif frame["type"] == "HEARTBEAT":
                    if generation == 1:
                        clock[0] = 31.0
                    await enqueue({"type": "HEARTBEAT_ACK", "provider_version": "85"})
                    if generation == 1:
                        await enqueue({"type": "DISPATCH", "id": "job", "claim": "claim",
                                       "kind": "document", "texts": ["same text"], "space": SPACE})
                elif frame["type"] == "RESULT":
                    if generation == 1:
                        raise ConnectionError("result transmitted but ACK lost")
                    await enqueue({"type": "RESULT_ACK", "job_id": "job", "claim": "claim",
                                   "status": "duplicate"})
                    asyncio.get_running_loop().call_later(.01, stop.set)
                else:
                    raise AssertionError(frame["type"])

            async def receive(self):
                return await incoming.get()

        yield Socket()

    def load():
        calls.append("load")
        def encode(text):
            calls.append(text)
            return [1.0] + [0.0] * 1023
        return encode

    worker = transport.BgeWssTransport(CONFIG, load, lambda: {},
        connector=connect, heartbeat_seconds=.01, maximum_seconds=4, stop=stop)
    worker._retry_clock = lambda: clock[0]
    await worker.run()
    assert len(attempts) == 18 and sessions == [1, 2]
    assert calls == ["load", "same text"]
    results = [frame for frame in sent if frame["type"] == "RESULT"]
    assert len(results) == 2
    assert {k: v for k, v in results[0].items() if k != "connection_generation"} == {
           k: v for k, v in results[1].items() if k != "connection_generation"}
    attaches = [frame for frame in sent if frame["type"] == "ATTACH"]
    assert len(attaches) == 2
    assert attaches[0]["runtime_incarnation"] == attaches[1]["runtime_incarnation"]
    assert worker.pending is None and worker.acknowledged == ("job", "claim")


@pytest.mark.asyncio
async def test_early_heartbeat_ack_does_not_erase_retry_debt(monkeypatch):
    transport = module("bge_wss_transport")
    monkeypatch.setattr(transport.random, "uniform", lambda *_: .0001)
    clock = [0.0]
    attempts = []
    @asynccontextmanager
    async def connect():
        attempt = len(attempts) + 1
        attempts.append(attempt)
        if attempt != 9:
            raise ConnectionError("offline")
        incoming = asyncio.Queue()

        class Socket:
            async def send(self, raw):
                kind = json.loads(raw)["type"]
                if kind == "ATTACH":
                    value = {"type": "WELCOME", "provider_version": "85"}
                elif kind == "READY":
                    value = {"type": "READY_ACK", "status": "ready"}
                elif kind == "HEARTBEAT":
                    clock[0] = 29.99
                    value = {"type": "HEARTBEAT_ACK"}
                else:
                    raise AssertionError(kind)
                await incoming.put(json.dumps({"protocol_version": 2,
                    "connection_generation": 1, **value}))
                if kind == "HEARTBEAT":
                    await incoming.put(ConnectionError("short-lived connection"))
            async def receive(self):
                value = await incoming.get()
                if isinstance(value, Exception):
                    raise value
                return value
        yield Socket()
    worker = transport.BgeWssTransport(CONFIG,
        lambda: lambda _: [1.] + [0.] * 1023,
        lambda: {}, connector=connect, heartbeat_seconds=.01, maximum_seconds=4,
        reconnect_grace_seconds=29)
    worker._retry_clock = lambda: clock[0]
    with pytest.raises(transport.BgeChannelStopped, match="reconnect exhausted"):
        await worker.run()
    # The short session did not reset the outage start: its grace still ran out.
    assert len(attempts) == 9 and worker._outage_started == 0.0


@pytest.mark.asyncio
async def test_wrong_generation_heartbeat_ack_cannot_reset_budget(monkeypatch):
    transport = module("bge_wss_transport")
    monkeypatch.setattr(transport.random, "uniform", lambda *_: .0001)
    attempts = [];clock=[0.]
    @asynccontextmanager
    async def connect():
        attempts.append(1)
        if len(attempts) < 9:
            clock[0]+=1
            raise ConnectionError("offline")
        incoming = asyncio.Queue()
        class Socket:
            async def send(self, raw):
                kind = json.loads(raw)["type"]
                if kind=='HEARTBEAT':clock[0]=40.
                value = {"type": "WELCOME", "provider_version": "85"} if kind == "ATTACH" else (
                    {"type": "READY_ACK", "status": "ready"} if kind == "READY" else {
                    "type": "HEARTBEAT_ACK"})
                await incoming.put(json.dumps({"protocol_version": 2,
                    "connection_generation": 2 if kind == "HEARTBEAT" else 1, **value}))
            async def receive(self):
                return await incoming.get()
        yield Socket()
    worker = transport.BgeWssTransport(CONFIG, lambda: lambda _: [1.] + [0.] * 1023,
        lambda: {}, connector=connect, heartbeat_seconds=.01, maximum_seconds=4,reconnect_grace_seconds=29)
    worker._retry_clock = lambda: clock[0]
    with pytest.raises(transport.BgeChannelStopped, match="reconnect exhausted"):
        await worker.run()
    assert len(attempts) == 9 and worker._reconnect_attempt == 9
    assert worker._outage_started==1.


@pytest.mark.asyncio
async def test_dev_restart_outage_within_grace_is_survived_then_bounded(monkeypatch):
    """A ~70 s Dev reboot no longer ends the warm worker; the 10 min grace still ends it."""
    transport = module("bge_wss_transport")
    monkeypatch.setattr(transport.random, "uniform", lambda *_: .0001)
    clock = [0.0]; attempts = []
    @asynccontextmanager
    async def connect():
        attempts.append(clock[0]); clock[0] += 10.0
        raise ConnectionError("Dev API restarting")
        yield
    worker = transport.BgeWssTransport(CONFIG, lambda: None, lambda: {}, connector=connect, maximum_seconds=30)
    worker._retry_clock = lambda: clock[0]
    with pytest.raises(transport.BgeChannelStopped, match="reconnect exhausted"):
        await worker.run()
    assert worker.reconnect_grace_seconds == 600
    assert attempts[-1] >= 590 and len(attempts) > 8  # old 10-attempt budget would stop at ~30 s
