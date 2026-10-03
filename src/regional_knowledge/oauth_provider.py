from __future__ import annotations

import hashlib
import hmac
import html
import json
import os
import secrets
import threading
import time
from pathlib import Path
from typing import Any, Callable
from urllib.parse import parse_qs, urlsplit
from uuid import UUID

from cryptography.fernet import Fernet
from mcp.server.auth.provider import (
    AccessToken,
    AuthorizationCode,
    AuthorizationParams,
    AuthorizeError,
    RegistrationError,
    RefreshToken,
    TokenError,
    construct_redirect_uri,
)
from mcp.shared.auth import OAuthClientInformationFull, OAuthToken
from pydantic import AnyHttpUrl, AnyUrl
from starlette.requests import Request
from starlette.responses import HTMLResponse, RedirectResponse, Response


KNOWLEDGE_SCOPE = "knowledge:manage"
_ACCESS_SECONDS = 3600
_REFRESH_SECONDS = 30 * 86400
_CODE_SECONDS = 60
_REQUEST_SECONDS = 600
_OWNER_SESSION_SECONDS = 15 * 60
_MAX_PENDING = 4096
_MAX_REGISTERED_CLIENTS = 256
_COOKIE_SESSION = "__Host-rkb_owner"
_COOKIE_CSRF = "__Host-rkb_csrf"


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _random() -> str:
    return secrets.token_urlsafe(32)


class _EncryptedStateStore:
    """Small single-writer encrypted state file for OAuth credentials.

    Opaque credentials are indexed only by SHA-256 digest. The entire state file
    is additionally Fernet-encrypted and both key/state are mode 0600.
    """

    def __init__(self, state_file: Path, key_file: Path) -> None:
        self.state_file = state_file
        self.key_file = key_file
        self._lock = threading.RLock()
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        self.key_file.parent.mkdir(parents=True, exist_ok=True)
        self._fernet = Fernet(self._load_or_create_key())
        if not self.state_file.exists():
            self._write(self._empty())

    @staticmethod
    def _empty() -> dict[str, Any]:
        return {
            "clients": {},
            "pending": {},
            "codes": {},
            "access": {},
            "refresh": {},
            "used_refresh": {},
            "sessions": {},
        }

    def _load_or_create_key(self) -> bytes:
        if self.key_file.exists():
            key = self.key_file.read_bytes().strip()
            Fernet(key)
            return key
        key = Fernet.generate_key()
        fd = os.open(
            self.key_file,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
        )
        try:
            os.write(fd, key + b"\n")
        finally:
            os.close(fd)
        return key

    def _read(self) -> dict[str, Any]:
        if not self.state_file.exists():
            return self._empty()
        encrypted = self.state_file.read_bytes()
        if not encrypted:
            return self._empty()
        value = json.loads(self._fernet.decrypt(encrypted).decode("utf-8"))
        if not isinstance(value, dict):
            raise RuntimeError("invalid OAuth state")
        base = self._empty()
        for key in base:
            item = value.get(key)
            base[key] = item if isinstance(item, dict) else {}
        return base

    def _write(self, value: dict[str, Any]) -> None:
        plaintext = json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        encrypted = self._fernet.encrypt(plaintext)
        temp = self.state_file.with_name(
            self.state_file.name + f".{os.getpid()}.{secrets.token_hex(4)}.tmp"
        )
        fd = os.open(
            temp,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
        )
        try:
            os.write(fd, encrypted)
            os.fsync(fd)
        finally:
            os.close(fd)
        os.replace(temp, self.state_file)
        os.chmod(self.state_file, 0o600)

    def read(self, callback: Callable[[dict[str, Any]], Any]) -> Any:
        with self._lock:
            state = self._read()
            self._prune(state)
            return callback(state)

    def mutate(self, callback: Callable[[dict[str, Any]], Any]) -> Any:
        with self._lock:
            state = self._read()
            self._prune(state)
            result = callback(state)
            self._write(state)
            return result

    @staticmethod
    def _prune(state: dict[str, Any]) -> None:
        now = time.time()
        for bucket in (
            "pending",
            "codes",
            "access",
            "refresh",
            "used_refresh",
            "sessions",
        ):
            for key, record in list(state[bucket].items()):
                if float(record.get("expires_at", 0)) <= now:
                    del state[bucket][key]


class RegionalOAuthProvider:
    """One-owner beta OAuth AS with durable opaque grants and explicit consent."""

    def __init__(
        self,
        *,
        issuer: str,
        resource: str,
        client_id: str,
        client_secret: str,
        redirect_uris: list[str],
        owner_subject: str,
        owner_login_secret: str,
        state_file: Path,
        key_file: Path,
        clock: Callable[[], float] = time.time,
    ) -> None:
        # MCP's AuthSettings serializes a host-only AnyHttpUrl with the
        # canonical trailing slash (RFC 8414 issuer identifier). Keep that exact
        # identifier for OAuth metadata, callback `iss`, and token claims.
        # Browser Origin headers never contain the trailing slash, so retain a
        # separate origin form for CSRF/origin checks and consent URLs.
        self.issuer = str(AnyHttpUrl(issuer))
        self.origin = self.issuer.rstrip("/")
        self.resource = resource.rstrip("/")
        if not self.issuer.startswith("https://"):
            raise ValueError("embedded OAuth issuer must use HTTPS")
        if self.resource != self.origin + "/mcp":
            raise ValueError("embedded OAuth resource must be the issuer /mcp URL")
        if len(client_secret) < 32:
            raise ValueError("OAuth client secret must contain at least 32 characters")
        if len(owner_login_secret) < 12:
            raise ValueError("owner login secret is too short")
        self.owner_subject = str(UUID(owner_subject))
        self.owner_login_hash = _digest(owner_login_secret)
        self.redirect_uris = tuple(redirect_uris)
        if not self.redirect_uris:
            raise ValueError("OAuth redirect allowlist cannot be empty")
        for uri in self.redirect_uris:
            parsed = AnyUrl(uri)
            if parsed.scheme != "https":
                raise ValueError("OAuth redirects must use HTTPS")
            if parsed.fragment:
                raise ValueError("OAuth redirects cannot contain fragments")
        self.client = OAuthClientInformationFull(
            client_id=client_id,
            client_secret=client_secret,
            redirect_uris=[AnyUrl(value) for value in self.redirect_uris],
            token_endpoint_auth_method="client_secret_basic",
            grant_types=["authorization_code", "refresh_token"],
            response_types=["code"],
            scope=KNOWLEDGE_SCOPE,
            application_type="web",
            issuer=self.issuer,
        )
        self.store = _EncryptedStateStore(state_file, key_file)
        self.clock = clock
        self._admission_started = 0.0
        self._admission_count = 0

    def _redirect_allowed(self, value: str) -> bool:
        if value in self.redirect_uris:
            return True
        try:
            parsed = urlsplit(value)
            if (
                parsed.scheme != "https"
                or parsed.hostname != "chatgpt.com"
                or parsed.username
                or parsed.password
                or parsed.query
                or parsed.fragment
                or parsed.port not in (None, 443)
            ):
                return False
        except ValueError:
            return False
        if parsed.path == "/connector_platform_oauth_redirect":
            return True
        prefix = "/connector/oauth/"
        if not parsed.path.startswith(prefix):
            return False
        callback_id = parsed.path[len(prefix):]
        return bool(callback_id) and "/" not in callback_id and len(callback_id) <= 256

    async def get_client(self, client_id: str) -> OAuthClientInformationFull | None:
        if client_id == self.client.client_id:
            return self.client
        record = self.store.read(
            lambda state: state["clients"].get(_digest(client_id))
        )
        if not record:
            return None
        return OAuthClientInformationFull.model_validate(record)

    async def register_client(self, client_info: OAuthClientInformationFull) -> None:
        redirects = [str(value) for value in (client_info.redirect_uris or [])]
        if not redirects or any(not self._redirect_allowed(value) for value in redirects):
            raise RegistrationError(
                error="invalid_redirect_uri",
                error_description="only registered ChatGPT HTTPS callbacks are allowed",
            )
        if client_info.token_endpoint_auth_method not in {
            "client_secret_basic",
            "client_secret_post",
        }:
            raise RegistrationError(
                error="invalid_client_metadata",
                error_description="registered clients must use a supported client secret method",
            )
        if not client_info.client_secret:
            raise RegistrationError(
                error="invalid_client_metadata",
                error_description="registered client secret is required",
            )
        if set(client_info.grant_types) - {"authorization_code", "refresh_token"}:
            raise RegistrationError(
                error="invalid_client_metadata",
                error_description="unsupported OAuth grant type",
            )
        scopes = set((client_info.scope or "").split())
        if scopes and scopes != {KNOWLEDGE_SCOPE}:
            raise RegistrationError(
                error="invalid_client_metadata",
                error_description="unsupported OAuth scope",
            )

        record = client_info.model_dump(mode="json", exclude_none=True)
        key = _digest(client_info.client_id)

        def write(state: dict[str, Any]) -> None:
            existing = state["clients"].get(key)
            if existing is not None:
                if existing != record:
                    raise RegistrationError(
                        error="invalid_client_metadata",
                        error_description="client identifier collision",
                    )
                return
            if len(state["clients"]) >= _MAX_REGISTERED_CLIENTS:
                raise RegistrationError(
                    error="invalid_client_metadata",
                    error_description="client registration capacity reached",
                )
            state["clients"][key] = record

        self.store.mutate(write)

    def _admit(self) -> None:
        now = self.clock()
        if now - self._admission_started >= 60:
            self._admission_started = now
            self._admission_count = 0
        self._admission_count += 1
        if self._admission_count > 120:
            raise AuthorizeError(
                error="temporarily_unavailable",
                error_description="authorization admission limit exceeded",
            )

    async def authorize(
        self,
        client: OAuthClientInformationFull,
        params: AuthorizationParams,
    ) -> str:
        self._admit()
        if client.client_id != self.client.client_id:
            if await self.get_client(client.client_id) is None:
                raise AuthorizeError(error="unauthorized_client")
        redirect = str(params.redirect_uri)
        registered_redirects = {str(value) for value in (client.redirect_uris or [])}
        if redirect not in registered_redirects or not self._redirect_allowed(redirect):
            raise AuthorizeError(error="invalid_request")
        if params.resource != self.resource:
            raise AuthorizeError(error="invalid_target")
        scopes = params.scopes or [KNOWLEDGE_SCOPE]
        if scopes != [KNOWLEDGE_SCOPE]:
            raise AuthorizeError(error="invalid_scope")

        request_id = _random()

        def write(state: dict[str, Any]) -> None:
            if len(state["pending"]) >= _MAX_PENDING:
                raise AuthorizeError(error="temporarily_unavailable")
            state["pending"][_digest(request_id)] = {
                "client_id": client.client_id,
                "redirect_uri": redirect,
                "redirect_uri_provided_explicitly": params.redirect_uri_provided_explicitly,
                "scopes": scopes,
                "state": params.state,
                "code_challenge": params.code_challenge,
                "resource": params.resource,
                "expires_at": self.clock() + _REQUEST_SECONDS,
            }

        self.store.mutate(write)
        return f"{self.origin}/oauth/consent?id={request_id}"

    def _pending(self, request_id: str) -> dict[str, Any] | None:
        return self.store.read(
            lambda state: state["pending"].get(_digest(request_id))
        )

    def _set_csrf(self, request_id: str) -> str:
        csrf = _random()

        def write(state: dict[str, Any]) -> None:
            record = state["pending"].get(_digest(request_id))
            if record is None:
                raise LookupError("authorization request expired")
            record["csrf_hash"] = _digest(csrf)

        self.store.mutate(write)
        return csrf

    def _session_subject(self, raw_session: str | None) -> str | None:
        if not raw_session:
            return None
        record = self.store.read(
            lambda state: state["sessions"].get(_digest(raw_session))
        )
        if not record:
            return None
        return str(record["subject"])

    def _new_owner_session(self) -> str:
        raw = _random()
        self.store.mutate(
            lambda state: state["sessions"].__setitem__(
                _digest(raw),
                {
                    "subject": self.owner_subject,
                    "expires_at": self.clock() + _OWNER_SESSION_SECONDS,
                },
            )
        )
        return raw

    def verify_owner_login(self, supplied: str) -> bool:
        return hmac.compare_digest(_digest(supplied), self.owner_login_hash)

    def _verify_csrf(
        self,
        request_id: str,
        raw_cookie: str | None,
        submitted: str,
    ) -> bool:
        pending = self._pending(request_id)
        expected = pending.get("csrf_hash") if pending else None
        if not expected or not raw_cookie or not submitted:
            return False
        return hmac.compare_digest(expected, _digest(raw_cookie)) and hmac.compare_digest(
            raw_cookie,
            submitted,
        )

    def approve(self, request_id: str) -> str:
        code = _random()

        def write(state: dict[str, Any]) -> str:
            pending = state["pending"].pop(_digest(request_id), None)
            if pending is None:
                raise LookupError("authorization request expired")
            state["codes"][_digest(code)] = {
                **pending,
                "subject": self.owner_subject,
                "expires_at": self.clock() + _CODE_SECONDS,
            }
            return construct_redirect_uri(
                pending["redirect_uri"],
                code=code,
                state=pending.get("state"),
                iss=self.issuer,
            )

        return self.store.mutate(write)

    def deny(self, request_id: str) -> str:
        def write(state: dict[str, Any]) -> str:
            pending = state["pending"].pop(_digest(request_id), None)
            if pending is None:
                raise LookupError("authorization request expired")
            return construct_redirect_uri(
                pending["redirect_uri"],
                error="access_denied",
                state=pending.get("state"),
                iss=self.issuer,
            )

        return self.store.mutate(write)

    async def load_authorization_code(
        self,
        client: OAuthClientInformationFull,
        authorization_code: str,
    ) -> AuthorizationCode | None:
        record = self.store.read(
            lambda state: state["codes"].get(_digest(authorization_code))
        )
        if not record or record["client_id"] != client.client_id:
            return None
        return AuthorizationCode(
            code=authorization_code,
            scopes=list(record["scopes"]),
            expires_at=float(record["expires_at"]),
            client_id=str(record["client_id"]),
            code_challenge=str(record["code_challenge"]),
            redirect_uri=AnyUrl(str(record["redirect_uri"])),
            redirect_uri_provided_explicitly=bool(
                record["redirect_uri_provided_explicitly"]
            ),
            resource=str(record["resource"]),
            subject=str(record["subject"]),
        )

    def _mint_family(
        self,
        state: dict[str, Any],
        *,
        client_id: str,
        scopes: list[str],
        resource: str,
        subject: str,
        family: str | None = None,
    ) -> OAuthToken:
        family = family or _random()
        access = _random()
        refresh = _random()
        common = {
            "family": family,
            "client_id": client_id,
            "scopes": scopes,
            "resource": resource,
            "subject": subject,
        }
        state["access"][_digest(access)] = {
            **common,
            "expires_at": self.clock() + _ACCESS_SECONDS,
        }
        state["refresh"][_digest(refresh)] = {
            **common,
            "expires_at": self.clock() + _REFRESH_SECONDS,
        }
        return OAuthToken(
            access_token=access,
            token_type="Bearer",
            expires_in=_ACCESS_SECONDS,
            scope=" ".join(scopes),
            refresh_token=refresh,
        )

    async def exchange_authorization_code(
        self,
        client: OAuthClientInformationFull,
        authorization_code: AuthorizationCode,
    ) -> OAuthToken:
        def write(state: dict[str, Any]) -> OAuthToken:
            record = state["codes"].pop(_digest(authorization_code.code), None)
            if record is None or record["client_id"] != client.client_id:
                raise TokenError(error="invalid_grant")
            return self._mint_family(
                state,
                client_id=str(record["client_id"]),
                scopes=list(record["scopes"]),
                resource=str(record["resource"]),
                subject=str(record["subject"]),
            )

        return self.store.mutate(write)

    async def load_refresh_token(
        self,
        client: OAuthClientInformationFull,
        refresh_token: str,
    ) -> RefreshToken | None:
        key = _digest(refresh_token)

        def read_or_revoke(state: dict[str, Any]) -> dict[str, Any] | None:
            used = state["used_refresh"].get(key)
            if used:
                family = used["family"]
                self._revoke_family(state, family)
                return None
            record = state["refresh"].get(key)
            if not record or record["client_id"] != client.client_id:
                return None
            return dict(record)

        record = self.store.mutate(read_or_revoke)
        if not record:
            return None
        return RefreshToken(
            token=refresh_token,
            client_id=str(record["client_id"]),
            scopes=list(record["scopes"]),
            expires_at=int(record["expires_at"]),
            resource=str(record["resource"]),
            subject=str(record["subject"]),
        )

    async def exchange_refresh_token(
        self,
        client: OAuthClientInformationFull,
        refresh_token: RefreshToken,
        scopes: list[str],
    ) -> OAuthToken:
        if scopes and scopes != [KNOWLEDGE_SCOPE]:
            raise TokenError(error="invalid_scope")
        key = _digest(refresh_token.token)

        def rotate(state: dict[str, Any]) -> OAuthToken:
            record = state["refresh"].pop(key, None)
            if record is None or record["client_id"] != client.client_id:
                raise TokenError(error="invalid_grant")
            state["used_refresh"][key] = {
                "family": record["family"],
                "expires_at": record["expires_at"],
            }
            return self._mint_family(
                state,
                client_id=str(record["client_id"]),
                scopes=list(record["scopes"]),
                resource=str(record["resource"]),
                subject=str(record["subject"]),
                family=str(record["family"]),
            )

        return self.store.mutate(rotate)

    async def load_access_token(self, token: str) -> AccessToken | None:
        record = self.store.read(
            lambda state: state["access"].get(_digest(token))
        )
        if not record:
            return None
        return AccessToken(
            token=token,
            client_id=str(record["client_id"]),
            scopes=list(record["scopes"]),
            expires_at=int(record["expires_at"]),
            resource=str(record["resource"]),
            subject=str(record["subject"]),
            claims={"iss": self.issuer},
        )

    @staticmethod
    def _revoke_family(state: dict[str, Any], family: str) -> None:
        for bucket in ("access", "refresh"):
            for key, record in list(state[bucket].items()):
                if record.get("family") == family:
                    del state[bucket][key]

    async def revoke_token(self, token: AccessToken | RefreshToken) -> None:
        key = _digest(token.token)

        def revoke(state: dict[str, Any]) -> None:
            record = state["access"].get(key) or state["refresh"].get(key)
            if record:
                self._revoke_family(state, str(record["family"]))

        self.store.mutate(revoke)

    async def exchange_identity_assertion(self, client, params):
        raise TokenError(error="unsupported_grant_type")

    @staticmethod
    async def _form(request: Request) -> dict[str, str]:
        body = await request.body()
        if len(body) > 16_384:
            raise ValueError("form too large")
        parsed = parse_qs(body.decode("utf-8"), keep_blank_values=True)
        if any(len(values) != 1 for values in parsed.values()):
            raise ValueError("duplicate form field")
        return {key: values[0] for key, values in parsed.items()}

    @staticmethod
    def _security_headers() -> dict[str, str]:
        return {
            "Cache-Control": "no-store",
            "Pragma": "no-cache",
            "Referrer-Policy": "same-origin",
            "X-Content-Type-Options": "nosniff",
            "Content-Security-Policy": (
                "default-src 'none'; style-src 'unsafe-inline'; "
                "form-action 'self'; frame-ancestors 'none'; base-uri 'none'"
            ),
        }

    def _html_page(self, body: str) -> HTMLResponse:
        return HTMLResponse(
            "<!doctype html><html lang='ru'><meta charset='utf-8'>"
            "<meta name='viewport' content='width=device-width,initial-scale=1'>"
            "<title>Regional Knowledge · ChatGPT</title>"
            "<style>body{font:17px/1.5 system-ui;max-width:620px;margin:8vh auto;"
            "padding:24px;background:#f6f6fb;color:#171725}form{display:grid;gap:14px}"
            "input,button{font:inherit;padding:12px;border:1px solid #bbb;border-radius:10px}"
            "button{cursor:pointer}button[value=allow]{background:#1820b8;color:white}</style>"
            + body
            + "</html>",
            headers=self._security_headers(),
        )

    def register_routes(self, mcp) -> None:
        @mcp.custom_route("/oauth/consent", methods=["GET"])
        async def consent_page(request: Request) -> Response:
            request_id = request.query_params.get("id", "")
            pending = self._pending(request_id)
            if not pending:
                return Response("Authorization request expired", status_code=400)
            csrf = self._set_csrf(request_id)
            subject = self._session_subject(request.cookies.get(_COOKIE_SESSION))
            if subject != self.owner_subject:
                body = (
                    "<h1>Вход владельца</h1>"
                    "<p>Введите код входа владельца Regional Knowledge. "
                    "После входа доступ ChatGPT подтверждается отдельно.</p>"
                    "<form method='post' action='/oauth/login'>"
                    f"<input type='hidden' name='id' value='{html.escape(request_id)}'>"
                    f"<input type='hidden' name='csrf' value='{html.escape(csrf)}'>"
                    "<input type='password' name='secret' autocomplete='one-time-code' "
                    "required minlength='12'>"
                    "<button type='submit'>Войти</button></form>"
                )
            else:
                body = (
                    "<h1>Подключить ChatGPT</h1>"
                    "<p>Regional Knowledge предоставит этому подключению доступ к "
                    "вашим книгам, поиску и импорту. Доступ можно отозвать.</p>"
                    "<form method='post' action='/oauth/consent'>"
                    f"<input type='hidden' name='id' value='{html.escape(request_id)}'>"
                    f"<input type='hidden' name='csrf' value='{html.escape(csrf)}'>"
                    "<button name='decision' value='allow'>Разрешить доступ</button>"
                    "<button name='decision' value='deny'>Отмена</button></form>"
                )
            response = self._html_page(body)
            response.set_cookie(
                _COOKIE_CSRF,
                csrf,
                secure=True,
                httponly=True,
                samesite="lax",
                path="/",
                max_age=_REQUEST_SECONDS,
            )
            return response

        @mcp.custom_route("/oauth/login", methods=["POST"])
        async def owner_login(request: Request) -> Response:
            try:
                if request.headers.get("origin") != self.origin:
                    raise ValueError("invalid origin")
                form = await self._form(request)
                request_id = form.get("id", "")
                if not self._verify_csrf(
                    request_id,
                    request.cookies.get(_COOKIE_CSRF),
                    form.get("csrf", ""),
                ):
                    raise ValueError("csrf")
                if not self.verify_owner_login(form.get("secret", "")):
                    raise ValueError("login")
                session = self._new_owner_session()
                response = RedirectResponse(
                    f"/oauth/consent?id={request_id}",
                    status_code=303,
                    headers=self._security_headers(),
                )
                response.set_cookie(
                    _COOKIE_SESSION,
                    session,
                    secure=True,
                    httponly=True,
                    samesite="lax",
                    path="/",
                    max_age=_OWNER_SESSION_SECONDS,
                )
                return response
            except (ValueError, LookupError):
                return Response(
                    "Login rejected",
                    status_code=401,
                    headers=self._security_headers(),
                )

        @mcp.custom_route("/oauth/consent", methods=["POST"])
        async def consent_submit(request: Request) -> Response:
            try:
                if request.headers.get("origin") != self.origin:
                    raise ValueError("invalid origin")
                form = await self._form(request)
                request_id = form.get("id", "")
                if self._session_subject(
                    request.cookies.get(_COOKIE_SESSION)
                ) != self.owner_subject:
                    raise ValueError("owner session required")
                if not self._verify_csrf(
                    request_id,
                    request.cookies.get(_COOKIE_CSRF),
                    form.get("csrf", ""),
                ):
                    raise ValueError("csrf")
                decision = form.get("decision")
                if decision == "allow":
                    target = self.approve(request_id)
                elif decision == "deny":
                    target = self.deny(request_id)
                else:
                    raise ValueError("decision")
                response = RedirectResponse(
                    target,
                    status_code=303,
                    headers=self._security_headers(),
                )
                response.delete_cookie(_COOKIE_CSRF, path="/")
                return response
            except (ValueError, LookupError):
                return Response(
                    "Consent rejected",
                    status_code=400,
                    headers=self._security_headers(),
                )

        @mcp.custom_route("/health", methods=["GET"])
        async def health(_: Request) -> Response:
            return Response(
                json.dumps({"status": "ok"}),
                media_type="application/json",
                headers={"Cache-Control": "no-store"},
            )


def oauth_provider_from_env(
    *,
    issuer: str,
    resource: str,
) -> RegionalOAuthProvider:
    def required(name: str) -> str:
        value = os.getenv(name, "").strip()
        if not value:
            raise RuntimeError(f"{name} is required for embedded OAuth")
        return value

    try:
        redirects = json.loads(required("RKB_OAUTH_REDIRECT_URIS"))
    except json.JSONDecodeError as exc:
        raise RuntimeError("RKB_OAUTH_REDIRECT_URIS must be a JSON array") from exc
    if not isinstance(redirects, list) or not all(
        isinstance(item, str) for item in redirects
    ):
        raise RuntimeError("RKB_OAUTH_REDIRECT_URIS must be a JSON string array")

    return RegionalOAuthProvider(
        issuer=issuer,
        resource=resource,
        client_id=required("RKB_OAUTH_CLIENT_ID"),
        client_secret=required("RKB_OAUTH_CLIENT_SECRET"),
        redirect_uris=redirects,
        owner_subject=required("RKB_OWNER_SUBJECT"),
        owner_login_secret=required("RKB_OWNER_LOGIN_SECRET"),
        state_file=Path(required("RKB_OAUTH_STATE_FILE")),
        key_file=Path(required("RKB_OAUTH_KEY_FILE")),
    )
