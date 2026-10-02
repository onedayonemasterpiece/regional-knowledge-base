import time

from regional_knowledge.auth import claims_to_access_token


def test_token_must_be_bound_to_exact_mcp_resource():
    claims = {
        "sub": "user-1",
        "client_id": "chatgpt-client",
        "iss": "https://issuer.example/auth/v1",
        "aud": ["https://knowledge.example/mcp"],
        "exp": int(time.time()) + 3600,
        "scope": "openid profile",
    }
    token = claims_to_access_token(
        "raw",
        claims,
        expected_resource="https://knowledge.example/mcp",
        expected_issuer="https://issuer.example/auth/v1",
    )
    assert token is not None
    assert token.subject == "user-1"
    assert token.resource == "https://knowledge.example/mcp"


def test_token_for_other_mcp_is_rejected():
    claims = {
        "sub": "user-1",
        "client_id": "chatgpt-client",
        "iss": "https://issuer.example/auth/v1",
        "aud": ["https://wonderful.example/mcp"],
        "exp": int(time.time()) + 3600,
    }
    assert claims_to_access_token(
        "raw",
        claims,
        expected_resource="https://knowledge.example/mcp",
        expected_issuer="https://issuer.example/auth/v1",
    ) is None
