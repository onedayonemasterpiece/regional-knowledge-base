from __future__ import annotations

import time
from typing import Any

import httpx
import jwt
from jwt import PyJWTError
from mcp.server.auth.provider import AccessToken, TokenVerifier


def _audiences(claims: dict[str, Any]) -> set[str]:
    values: set[str] = set()
    aud = claims.get("aud")
    if isinstance(aud, str):
        values.add(aud)
    elif isinstance(aud, list):
        values.update(str(v) for v in aud)
    resource = claims.get("resource")
    if isinstance(resource, str):
        values.add(resource)
    return values


def claims_to_access_token(
    raw_token: str,
    claims: dict[str, Any],
    *,
    expected_resource: str,
    expected_issuer: str,
) -> AccessToken | None:
    sub = claims.get("sub")
    client_id = claims.get("client_id")
    issuer = claims.get("iss")
    if not isinstance(sub, str) or not sub:
        return None
    if not isinstance(client_id, str) or not client_id:
        return None
    if issuer != expected_issuer:
        return None
    if expected_resource.rstrip("/") not in {v.rstrip("/") for v in _audiences(claims)}:
        return None

    exp = claims.get("exp")
    if not isinstance(exp, int) or exp <= int(time.time()):
        return None

    scope_value = claims.get("scope", "")
    scopes = scope_value.split() if isinstance(scope_value, str) else []
    return AccessToken(
        token=raw_token,
        client_id=client_id,
        scopes=scopes,
        expires_at=exp,
        resource=expected_resource,
        subject=sub,
        claims=claims,
    )


class SupabaseJwtVerifier(TokenVerifier):
    """Verify Supabase asymmetric JWTs and bind them to exactly one MCP resource."""

    def __init__(
        self,
        *,
        issuer: str,
        jwks_url: str,
        resource: str,
        timeout_seconds: float = 5.0,
        cache_seconds: float = 900.0,
    ) -> None:
        self.issuer = issuer.rstrip("/")
        self.jwks_url = jwks_url
        self.resource = resource.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.cache_seconds = cache_seconds
        self._keys: dict[str, Any] = {}
        self._loaded_at = 0.0

    async def _refresh_keys(self) -> None:
        async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
            response = await client.get(self.jwks_url)
            response.raise_for_status()
            payload = response.json()
        keys: dict[str, Any] = {}
        for jwk in payload.get("keys", []):
            kid = jwk.get("kid")
            if isinstance(kid, str):
                keys[kid] = jwt.PyJWK.from_dict(jwk).key
        if not keys:
            raise ValueError("JWKS contained no usable signing keys")
        self._keys = keys
        self._loaded_at = time.monotonic()

    async def verify_token(self, token: str) -> AccessToken | None:
        try:
            header = jwt.get_unverified_header(token)
            kid = header.get("kid")
            alg = header.get("alg")
            if not isinstance(kid, str) or alg not in {"RS256", "ES256"}:
                return None
            if (
                kid not in self._keys
                or time.monotonic() - self._loaded_at > self.cache_seconds
            ):
                await self._refresh_keys()
            key = self._keys.get(kid)
            if key is None:
                await self._refresh_keys()
                key = self._keys.get(kid)
            if key is None:
                return None
            claims = jwt.decode(
                token,
                key,
                algorithms=[alg],
                issuer=self.issuer,
                options={"verify_aud": False},
                leeway=30,
            )
        except (PyJWTError, httpx.HTTPError, ValueError):
            return None

        return claims_to_access_token(
            token,
            claims,
            expected_resource=self.resource,
            expected_issuer=self.issuer,
        )
