import httpx
import pytest
from regional_knowledge.oauth_diagnostics import check_refresh_response

@pytest.mark.parametrize("body,expected",[
    ({"error":"invalid_grant","error_description":"private-secret"},"invalid_grant"),
    ({"error":"private-secret"},"unclassified"),
    ({"error":{"nested":"private-secret"}},"unclassified"),
    (["private-secret"],"unclassified"),
])
def test_error_output_is_allowlisted(caplog,body,expected):
    response=httpx.Response(400,json=body,request=httpx.Request("POST","https://example.test/token"))
    with pytest.raises(httpx.HTTPStatusError):check_refresh_response(response)
    assert "oauth_error="+expected in caplog.text
    assert "private-secret" not in caplog.text

def test_non_json_error_is_safe(caplog):
    response=httpx.Response(502,text="private-secret",request=httpx.Request("POST","https://example.test/token"))
    with pytest.raises(httpx.HTTPStatusError):check_refresh_response(response)
    assert "oauth_error=unclassified" in caplog.text
    assert "private-secret" not in caplog.text

def test_success_does_not_log_tokens(caplog):
    check_refresh_response(httpx.Response(200,json={"access_token":"private-secret"}))
    assert not caplog.text
