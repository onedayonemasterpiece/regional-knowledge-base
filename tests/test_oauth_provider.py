from __future__ import annotations

from pathlib import Path
from urllib.parse import parse_qs, urlsplit
from uuid import uuid4

import pytest
from mcp.server.auth.provider import AuthorizationParams, AuthorizeError, RegistrationError
from mcp.shared.auth import OAuthClientInformationFull
from pydantic import AnyUrl
from starlette.testclient import TestClient

from regional_knowledge.backend import UnavailableBackend
from regional_knowledge.oauth_provider import (
    KNOWLEDGE_SCOPE,
    RegionalOAuthProvider,
)
from regional_knowledge.server import build_server


def _provider(tmp_path: Path) -> RegionalOAuthProvider:
    return RegionalOAuthProvider(
        issuer="https://knowledge.example.test",
        resource="https://knowledge.example.test/mcp",
        client_id="chatgpt-regional-knowledge",
        client_secret="client-secret-" + "x" * 40,
        redirect_uris=["https://chatgpt.com/connector_platform_oauth_redirect"],
        owner_subject=str(uuid4()),
        owner_login_secret="owner-login-secret-12345",
        state_file=tmp_path / "oauth.state",
        key_file=tmp_path / "oauth.key",
    )


def _params(
    *,
    resource: str = "https://knowledge.example.test/mcp",
    redirect_uri: str = "https://chatgpt.com/connector_platform_oauth_redirect",
) -> AuthorizationParams:
    return AuthorizationParams(
        state="state-1",
        scopes=[KNOWLEDGE_SCOPE],
        code_challenge="challenge-value",
        redirect_uri=AnyUrl(redirect_uri),
        redirect_uri_provided_explicitly=True,
        resource=resource,
    )


def _dynamic_client(*, redirect_uri: str) -> OAuthClientInformationFull:
    return OAuthClientInformationFull(
        client_id=str(uuid4()),
        client_secret="d" * 64,
        redirect_uris=[AnyUrl(redirect_uri)],
        token_endpoint_auth_method="client_secret_post",
        grant_types=["authorization_code", "refresh_token"],
        response_types=["code"],
        scope=KNOWLEDGE_SCOPE,
        application_type="web",
    )


@pytest.mark.asyncio
async def test_oauth_code_refresh_rotation_replay_revokes_family(tmp_path: Path) -> None:
    provider = _provider(tmp_path)
    consent = await provider.authorize(provider.client, _params())
    request_id = parse_qs(urlsplit(consent).query)["id"][0]

    redirect = provider.approve(request_id)
    code = parse_qs(urlsplit(redirect).query)["code"][0]
    loaded = await provider.load_authorization_code(provider.client, code)
    assert loaded is not None
    issued = await provider.exchange_authorization_code(provider.client, loaded)
    assert issued.refresh_token is not None
    assert await provider.load_access_token(issued.access_token) is not None

    refresh = await provider.load_refresh_token(provider.client, issued.refresh_token)
    assert refresh is not None
    rotated = await provider.exchange_refresh_token(
        provider.client,
        refresh,
        [KNOWLEDGE_SCOPE],
    )
    assert await provider.load_access_token(rotated.access_token) is not None

    # Replaying the consumed refresh token invalidates the complete token family.
    assert await provider.load_refresh_token(provider.client, issued.refresh_token) is None
    assert await provider.load_access_token(rotated.access_token) is None


@pytest.mark.asyncio
async def test_oauth_state_survives_restart_and_is_encrypted(tmp_path: Path) -> None:
    provider = _provider(tmp_path)
    consent = await provider.authorize(provider.client, _params())
    request_id = parse_qs(urlsplit(consent).query)["id"][0]
    redirect = provider.approve(request_id)
    code = parse_qs(urlsplit(redirect).query)["code"][0]
    loaded = await provider.load_authorization_code(provider.client, code)
    assert loaded is not None
    issued = await provider.exchange_authorization_code(provider.client, loaded)

    restarted = RegionalOAuthProvider(
        issuer=provider.issuer,
        resource=provider.resource,
        client_id=provider.client.client_id,
        client_secret=str(provider.client.client_secret),
        redirect_uris=list(provider.redirect_uris),
        owner_subject=provider.owner_subject,
        owner_login_secret="owner-login-secret-12345",
        state_file=tmp_path / "oauth.state",
        key_file=tmp_path / "oauth.key",
    )
    restored = await restarted.load_access_token(issued.access_token)
    assert restored is not None
    assert restored.subject == provider.owner_subject
    assert issued.access_token.encode() not in (tmp_path / "oauth.state").read_bytes()
    assert (tmp_path / "oauth.state").stat().st_mode & 0o077 == 0
    assert (tmp_path / "oauth.key").stat().st_mode & 0o077 == 0


@pytest.mark.asyncio
async def test_oauth_rejects_wrong_resource(tmp_path: Path) -> None:
    provider = _provider(tmp_path)
    with pytest.raises(AuthorizeError):
        await provider.authorize(
            provider.client,
            _params(resource="https://other.example.test/mcp"),
        )


@pytest.mark.asyncio
async def test_dynamic_chatgpt_client_survives_restart(tmp_path: Path) -> None:
    provider = _provider(tmp_path)
    redirect = "https://chatgpt.com/connector/oauth/callback-123"
    client = _dynamic_client(redirect_uri=redirect)
    await provider.register_client(client)
    loaded = await provider.get_client(client.client_id)
    assert loaded is not None
    assert loaded.client_secret == client.client_secret

    restarted = _provider(tmp_path)
    restored = await restarted.get_client(client.client_id)
    assert restored is not None
    consent = await restarted.authorize(
        restored,
        _params(redirect_uri=redirect),
    )
    assert parse_qs(urlsplit(consent).query)["id"]


@pytest.mark.asyncio
async def test_dynamic_client_rejects_non_chatgpt_redirect(tmp_path: Path) -> None:
    provider = _provider(tmp_path)
    client = _dynamic_client(redirect_uri="https://evil.example/callback")
    with pytest.raises(RegistrationError):
        await provider.register_client(client)


def test_embedded_oauth_mounts_standard_and_consent_routes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _provider(tmp_path)
    monkeypatch.setenv("RKB_AUTH_MODE", "embedded")
    server = build_server(
        backend=UnavailableBackend(),
        issuer=provider.issuer,
        resource_url=provider.resource,
        oauth_provider=provider,
    )
    app = server.streamable_http_app()
    assert provider.issuer == "https://knowledge.example.test/"
    assert provider.origin == "https://knowledge.example.test"
    with TestClient(app, base_url=provider.origin) as client:
        metadata = client.get("/.well-known/oauth-authorization-server")
        assert metadata.status_code == 200
        assert metadata.json()["issuer"] == provider.issuer
    paths = {getattr(route, "path", None) for route in app.routes}
    assert "/mcp" in paths
    assert "/authorize" in paths
    assert "/token" in paths
    assert "/register" in paths
    assert "/revoke" in paths
    assert "/oauth/consent" in paths
    assert "/oauth/login" in paths
    assert "/health" in paths
