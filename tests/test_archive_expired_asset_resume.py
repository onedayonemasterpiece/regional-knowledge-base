"""RKB real durable archive-get admission: re-fetch only proven expired cache bytes."""
from __future__ import annotations

import hashlib
import httpx
import pytest

from regional_knowledge.original_cache import OriginalCache
from regional_knowledge.source_archive import (
    ArchiveSourceCacheExpired, archive_payload,
)


class FakeVibe:
    owner = "rkb-actor"
    issuer = "https://vibe.example.test"

    def __init__(self, body=b"archived Telegram document", status_codes=()):
        self.body = body
        self.status_codes = list(status_codes)
        self.get_requests = []
        self.operation_by_key = {}
        self.asset_attempts = 0
        self.status_receipts = 0

    async def call(self, name, arguments):
        assert name == "vibepublish_media_store"
        assert arguments["command"] == {"kind": "get", "entry_ref": "pub_verified_source"}
        key = arguments["request_key"]
        self.get_requests.append(key)
        op = self.operation_by_key.setdefault(key, "get-" + str(len(self.operation_by_key) + 1))
        return {"operation_id": op}

    async def receipt(self, operation):
        self.status_receipts += 1
        number = operation.rsplit("-", 1)[-1]
        return {
            "operation_complete": True, "state": "verified",
            "items": [{
                "media": [{"source": {"kind": "asset", "id": "asset-" + number}}],
                "media_evidence": [{"media_kind": "document",
                                    "sha256": hashlib.sha256(self.body).hexdigest()}],
            }]
        }

    async def request(self, method, url, **kwargs):
        assert method == "GET"
        assert url.startswith(self.issuer + "/v1/assets/asset-")
        self.asset_attempts += 1
        code = self.status_codes.pop(0) if self.status_codes else 200
        request = httpx.Request("GET", url)
        response = httpx.Response(code, request=request,
                                  content=self.body if code == 200 else b"")
        if code >= 400:
            raise httpx.HTTPStatusError("mock provider status", request=request, response=response)
        return response


@pytest.fixture
def private_cache(tmp_path, monkeypatch):
    cache = OriginalCache(tmp_path / "originals")
    monkeypatch.setattr(OriginalCache, "from_env", classmethod(lambda cls: cache))
    return cache


@pytest.mark.asyncio
async def test_expired_asset_is_refetched_in_one_tool_call_with_one_new_read(private_cache):
    client = FakeVibe(status_codes=[410, 200])
    body, provider_sha = await archive_payload(client, "pub_verified_source")
    assert body == client.body
    assert provider_sha == hashlib.sha256(body).hexdigest()
    assert client.asset_attempts == 2
    assert len(client.get_requests) == 2
    assert len(set(client.get_requests)) == 2  # New GET, never new PUT.
    assert client.status_receipts == 2
    assert len(client.operation_by_key) == 2
    # The newly admitted durable receipt is retained for a later authorized read.
    read_again, _ = await archive_payload(client, "pub_verified_source")
    assert read_again == body
    assert client.get_requests[-1] == client.get_requests[-2]
    assert len(client.operation_by_key) == 2


@pytest.mark.asyncio
async def test_double_expiration_is_bounded_to_two_reads(private_cache):
    client = FakeVibe(status_codes=[404, 410])
    with pytest.raises(ArchiveSourceCacheExpired, match="archive_source_cache_expired"):
        await archive_payload(client, "pub_verified_source")
    assert client.asset_attempts == 2
    assert len(set(client.get_requests)) == 2
    assert client.status_receipts == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [401, 403, 429, 500])
async def test_other_errors_do_not_retire_durable_read_or_retry(private_cache, status):
    client = FakeVibe(status_codes=[status, 200])
    with pytest.raises(httpx.HTTPStatusError):
        await archive_payload(client, "pub_verified_source")
    assert len(client.get_requests) == 1
    assert client.asset_attempts == 1
    # The original request key and operation are reused, no new provider work.
    await archive_payload(client, "pub_verified_source")
    assert len(client.operation_by_key) == 1
    assert client.get_requests[0] == client.get_requests[1]


@pytest.mark.asyncio
async def test_unknown_network_error_does_not_mint_a_second_operation(private_cache):
    class UnknownNetwork(FakeVibe):
        async def request(self, method, url, **kwargs):
            self.asset_attempts += 1
            raise httpx.ReadTimeout("outcome unknown")
    client = UnknownNetwork()
    with pytest.raises(httpx.ReadTimeout):
        await archive_payload(client, "pub_verified_source")
    assert len(client.get_requests) == len(client.operation_by_key) == 1


@pytest.mark.asyncio
async def test_denied_receipt_never_retries(private_cache):
    class Denied(FakeVibe):
        async def receipt(self, op):
            return {"operation_complete": True, "state": "blocked"}
    client = Denied()
    with pytest.raises(RuntimeError, match="archive_source_unavailable"):
        await archive_payload(client, "pub_verified_source")
    assert len(client.operation_by_key) == 1
    assert client.asset_attempts == 0
