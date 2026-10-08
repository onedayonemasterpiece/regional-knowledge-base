"""Loopback BGE time budget must tolerate scheduler jitter without unbounded waits."""
import inspect
import pytest

from regional_knowledge.local_bge import LocalBGEQueryEmbedder


@pytest.mark.asyncio
async def test_local_connect_budget_is_separate_from_strict_total_deadline():
    client=LocalBGEQueryEmbedder()
    try:
        assert .3<=client.client.timeout.connect<=.4
        assert client.client.timeout.read<=1.7
        source=inspect.getsource(LocalBGEQueryEmbedder.embed)
        assert "asyncio.timeout(1.7)" in source
        assert "response.status_code==429" in source
    finally:
        await client.aclose()
