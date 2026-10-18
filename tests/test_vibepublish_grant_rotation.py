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

@pytest.mark.asyncio
async def test_local_service_bearer_uses_only_loopback_with_fixed_host(tmp_path):
    path = tmp_path / "service.json"
    owner = str(uuid4())
    binding = {
        "rkb_owner_id": owner,
        "client_id": "rkb-archive-technical",
        "issuer": "https://mcp-vibepublish.kenigevents.ru",
        "auth_mode": "local_service",
        "local_endpoint": "http://127.0.0.1:18765",
        "service_token": "private_local_machine_token_123456789",
        "destination_alias": "rkb_archive",
        "source_thread_ref": "https://t.me/c/4368830579/2",
        "thread_ref": "https://t.me/c/4368830579/4",
    }
    path.write_text(json.dumps(binding))
    path.chmod(0o600)
    client = VibePublishClient(path)
    requests = []

    async def handler(request):
        requests.append((str(request.url), request.headers["host"],
                         request.headers["authorization"]))
        if request.url.path == "/token" or request.url.scheme != "http":
            raise AssertionError("Unexpected OAuth or external call")
        return httpx.Response(200, json={"operation_id": "op_test"})

    try:
        await _install_http(client, handler)
        response = await client.request(
            "POST", binding["issuer"] + "/v1/assets",
            headers={"Content-Type": "application/pdf"}, content=b"%PDF-1.7 test")
        assert response.json() == {"operation_id": "op_test"}
        assert requests == [(
            "http://127.0.0.1:18765/v1/assets",
            "mcp-vibepublish.kenigevents.ru",
            "Bearer " + binding["service_token"],
        )]
        with pytest.raises(PermissionError):
            await client.request("GET", "https://other.example/v1/assets")
        with pytest.raises(RuntimeError):
            await client.refresh()
        assert len(requests) == 1
        assert json.loads(path.read_text()) == binding
    finally:
        await client.close()


@pytest.mark.parametrize("endpoint", [
    "https://mcp-vibepublish.kenigevents.ru",
    "http://localhost:18765", "http://127.0.0.1:80",
    "http://0.0.0.0:18765", "http://127.0.0.1:18765/other",
    "http://127.0.0.1:18765/?redirect=1",
])
def test_local_service_rejects_untrusted_transports(tmp_path, endpoint):
    path = tmp_path / "service.json"
    data = {
        "rkb_owner_id": str(uuid4()), "client_id": "rkb-archive-technical",
        "issuer": "https://mcp-vibepublish.kenigevents.ru",
        "auth_mode": "local_service",
        "local_endpoint": endpoint,
        "service_token": "private_local_machine_token_123456789",
    }
    path.write_text(json.dumps(data))
    path.chmod(0o600)
    with pytest.raises(ValueError):
        VibePublishClient(path)
