"""Redacted OAuth failure classification; never retain response bodies."""
import logging

_ALLOWED_ERRORS = frozenset({
    "invalid_request", "invalid_client", "invalid_grant", "unauthorized_client",
    "unsupported_grant_type", "invalid_scope", "invalid_target", "access_denied",
    "server_error", "temporarily_unavailable",
})


def check_refresh_response(response):
    if response.is_success:
        return
    code = "unclassified"
    try:
        value = response.json()
        candidate = value.get("error") if isinstance(value, dict) else None
        if isinstance(candidate, str) and candidate in _ALLOWED_ERRORS:
            code = candidate
    except (ValueError, TypeError):
        pass
    # Neither error_description nor arbitrary error values are safe to log.
    logging.getLogger(__name__).warning(
        "vibepublish_refresh_failed status=%s oauth_error=%s",
        response.status_code, code,
    )
    response.raise_for_status()
