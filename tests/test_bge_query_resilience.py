"""Sustained, bounded local BGE queue regression tests (no model/IO required)."""
import asyncio
import time

import httpx
import pytest

from regional_knowledge.bge_query_service import QueryOverloaded, QueryQueue
from regional_knowledge.local_bge import LocalBGEOverloaded, LocalBGEQueryEmbedder


async def close_queue(queue):
    if queue.worker is not None:
        queue.worker.cancel()
        await asyncio.gather(queue.worker, return_exceptions=True)
    queue.executor.shutdown(wait=True)


@pytest.mark.asyncio
async def test_same_query_coalesces_then_hits_digest_keyed_cache():
    seen = []
    def encode(text):
        seen.append(text)
        time.sleep(.04)
        return [1.0] + [0.0] * 1023

    queue = QueryQueue(encode, capacity=3, deadline_seconds=.6)
    try:
        first, second = await asyncio.gather(
            queue.submit("historical query"), queue.submit("historical query")
        )
        assert first["vectors"] == second["vectors"]
        assert queue.completed == 1
        assert queue.coalesced == 1
        assert len(seen) == 1
        third = await queue.submit("historical query")
        assert third["cache_hit"] is True
        assert third["encoder_seconds"] == 0.0
        assert queue.cache_hits == 1
        assert len(queue._inflight) == 0
        assert list(queue._cache)[0] != "historical query"
    finally:
        await close_queue(queue)


@pytest.mark.asyncio
async def test_real_overload_expires_explicitly_without_poisoning_worker():
    def encode(text):
        time.sleep(.35)
        return [1.0] + [0.0] * 1023

    queue = QueryQueue(encode, capacity=2, deadline_seconds=.2, cache_capacity=3)
    try:
        outcomes = await asyncio.gather(
            *(queue.submit("query-" + str(i)) for i in range(6)),
            return_exceptions=True,
        )
        assert all(isinstance(result, QueryOverloaded) for result in outcomes)
        assert queue.rejected >= 1
        assert queue.expired >= 1
        assert queue.queue.qsize() <= queue.queue.maxsize
        await asyncio.sleep(.8)
        assert queue.worker is not None and not queue.worker.done()
        # A previously timed-out valid result may still be cached and reused.
        result = await queue.submit("query-0")
        assert result["cache_hit"] is True
        assert len(queue._inflight) == 0
    finally:
        await close_queue(queue)


@pytest.mark.asyncio
async def test_encoder_exception_is_reported_without_caching_failed_vector():
    def encode(text):
        if text == "error":
            raise ValueError("invalid model input")
        return [1.0] + [0.0] * 1023

    queue = QueryQueue(encode, capacity=2, deadline_seconds=.5)
    try:
        with pytest.raises(ValueError, match="invalid model"):
            await queue.submit("error")
        assert len(queue._cache) == 0
        assert (await queue.submit("healthy"))["vectors"][0][0] == 1.0
        assert queue.completed == 1
    finally:
        await close_queue(queue)


@pytest.mark.asyncio
async def test_local_client_maps_http_429_to_bounded_overload_signal():
    embedder = LocalBGEQueryEmbedder()
    original = embedder.client
    embedder.client = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(429, json={"error": "encoder_capacity_or_deadline"})
        ),
        base_url="http://127.0.0.1:8768",
    )
    try:
        with pytest.raises(LocalBGEOverloaded, match="interaction deadline"):
            await embedder.embed("a valid historical source request")
    finally:
        await embedder.aclose()
        await original.aclose()


def test_explicit_sidecar_deadline_maximum_remains_below_search_hard_budget():
    queue = QueryQueue(lambda t: [1.0] + [0.0] * 1023)
    assert 1 < queue.deadline_seconds <= 1.6
    assert queue.queue.maxsize <= 16
    queue.executor.shutdown(wait=True)
    with pytest.raises(ValueError):
        QueryQueue(lambda t: [], deadline_seconds=2.1)
