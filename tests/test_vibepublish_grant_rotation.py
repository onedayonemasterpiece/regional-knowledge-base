"""Regression coverage for the shared RKB VibePublish OAuth grant."""
import asyncio
import json
import stat
import time
from urllib.parse import parse_qs
from uuid import uuid4

import httpx
import pytest

from regional_knowledge.illustration_mirror import VibePublishClient


def _grant(tmp_path, *, expired=True):
    path = tmp_path / "mirror-grant.json"
    data = {
        "rkb_owner_id": str(uuid4()),
        "client_id": "existing-rkb-client",
        "issuer": "https://vibe.example",
        "access_token": "old_access",
        "refresh_token": "old_refresh",
        "expires_at": time.time() - 1 if expired else time.time() + 300,
    }
    path.write_text(json.dumps(data))
    path.chmod(0o600)
    return path, data


async def _install_http(client, handler):
    await client.http.aclose()
    client.http = httpx.AsyncClient(transport=httpx.MockTransport(handler))


@pytest.mark.asyncio
async def test_two_clients_refresh_shared_grant_once(tmp_path):
    path, original = _grant(tmp_path)
    clients = [VibePublishClient(path), VibePublishClient(path)]
    rotations = []

    async def handler(request):
        if request.url.path == "/token":
            presented = parse_qs(request.content.decode())["refresh_token"][0]
            rotations.append(presented)
            await asyncio.sleep(0.03)
            if presented != "old_refresh" or len(rotations) != 1:
                return httpx.Response(400, json={"error": "invalid_grant"})
            return httpx.Response(200, json={
                "access_token": "new_access",
                "refresh_token": "new_refresh",
                "expires_in": 600,
            })
        if request.url.path == "/protected":
            authorized = request.headers.get("Authorization") == "Bearer new_access"
            return httpx.Response(200 if authorized else 401)
        return httpx.Response(404)

    try:
        for client in clients:
            await _install_http(client, handler)
        responses = await asyncio.gather(*(
            client.request("GET", "https://vibe.example/protected")
            for client in clients
        ))
        assert [response.status_code for response in responses] == [200, 200]
        assert rotations == ["old_refresh"]
        saved = json.loads(path.read_text())
        assert saved["client_id"] == original["client_id"]
        assert saved["rkb_owner_id"] == original["rkb_owner_id"]
        assert saved["refresh_token"] == "new_refresh"
        assert all(client.grant["access_token"] == "new_access" for client in clients)
        lock = path.with_name(path.name + ".refresh.lock")
        assert stat.S_IMODE(lock.stat().st_mode) == 0o600
        assert stat.S_IMODE(path.stat().st_mode) == 0o600
    finally:
        await asyncio.gather(*(client.close() for client in clients))


@pytest.mark.asyncio
async def test_401_from_stale_snapshot_reuses_advanced_disk_grant(tmp_path):
    path, grant = _grant(tmp_path, expired=False)
    client = VibePublishClient(path)
    fresh = {**grant, "access_token": "current_access",
             "refresh_token": "current_refresh", "expires_at": time.time() + 400}
    path.write_text(json.dumps(fresh))
    path.chmod(0o600)
    requests = []

    async def handler(request):
        requests.append((request.url.path, request.headers.get("Authorization")))
        if request.url.path == "/token":
            return httpx.Response(400, json={"error": "unexpected_refresh"})
        return httpx.Response(
            200 if request.headers.get("Authorization") == "Bearer current_access"
            else 401
        )

    try:
        await _install_http(client, handler)
        response = await client.request("GET", "https://vibe.example/protected")
        assert response.status_code == 200
        assert requests == [
            ("/protected", "Bearer old_access"),
            ("/protected", "Bearer current_access"),
        ]
        assert json.loads(path.read_text()) == fresh
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_invalid_grant_does_not_replace_on_disk_credentials(tmp_path):
    path, original = _grant(tmp_path)
    client = VibePublishClient(path)

    async def handler(request):
        assert request.url.path == "/token"
        return httpx.Response(400, json={"error": "invalid_grant"})

    try:
        await _install_http(client, handler)
        with pytest.raises(httpx.HTTPStatusError) as error:
            await client.refresh()
        assert error.value.response.status_code == 400
        assert json.loads(path.read_text()) == original
        assert not path.with_name(path.name + ".new").exists()
    finally:
        await client.close()
