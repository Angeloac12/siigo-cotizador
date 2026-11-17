import base64
import json
import logging
import time

import httpx
import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from app.upstream_gateway.siigo import SiigoGateway


@pytest.fixture
def gateway(monkeypatch):
    settings = {
        "SIIGO_ENABLED": "true",
        "SIIGO_SIGNIN_URL": "https://services.siigo.com/alliances/api/siigoapi-users/v1/sign-in",
        "SIIGO_BASE_URL": "https://api.siigo.com/v1",
        "SIIGO_PARTNER_ID": "QuoteBuddy",
        "SIIGO_API_USER": "test-user@example.com",
        "SIIGO_ACCESS_KEY": "test-secret-access-key",
    }
    for name, value in settings.items():
        monkeypatch.setenv(name, value)
    return SiigoGateway()


@pytest.fixture
def upstream(monkeypatch):
    clients = []

    def install(handler):
        client = httpx.Client(transport=httpx.MockTransport(handler))
        clients.append(client)
        monkeypatch.setattr("app.upstream_gateway.siigo.httpx.post", client.post)

    yield install
    for client in clients:
        client.close()


def test_quote_authenticates_with_documented_fields_and_reuses_token(gateway, upstream):
    requests = []

    def respond(request):
        requests.append(request)
        if str(request.url) == gateway.signin_url:
            assert json.loads(request.content) == {
                "username": gateway.api_user,
                "access_key": gateway.access_key,
            }
            assert request.headers["Partner-Id"] == gateway.partner_id
            return httpx.Response(201, json={"access_token": "test-token", "expires_in": 86400})
        assert str(request.url) == f"{gateway.base_url}/quotations"
        assert request.headers["Authorization"] == "Bearer test-token"
        return httpx.Response(201, json={"id": "test-quote"})

    upstream(respond)
    assert gateway._get_token() == "test-token"
    assert gateway.create_quote(
        customer_identification="123", branch_office=0, document_id=1,
        seller=1, items=[], date_iso="2026-11-17",
    ) == {"id": "test-quote"}
    assert len(requests) == 2


@pytest.mark.parametrize("status", [400, 401, 403, 500, 503])
def test_auth_rejection_returns_controlled_error_without_creating_quote(
    gateway, upstream, caplog, status,
):
    requests = []

    def respond(request):
        requests.append(request)
        return httpx.Response(status, json={
            "message": f"Rejected {gateway.api_user} {gateway.access_key}",
            "access_token": "sensitive-upstream-token",
        })

    upstream(respond)
    app = FastAPI()

    @app.post("/commit")
    def commit():
        return gateway.create_quote(
            customer_identification="123", branch_office=0, document_id=1,
            seller=1, items=[], date_iso="2026-11-17",
        )

    with caplog.at_level(logging.WARNING), TestClient(app) as client:
        response = client.post("/commit")

    assert response.status_code == 502
    assert response.json()["detail"]["code"] == "SIIGO_AUTH_FAILED"
    assert response.json()["detail"]["upstream_status"] == status
    assert len(requests) == 1
    assert gateway._token is None
    assert f"upstream_status={status}" in caplog.text
    for secret in (gateway.api_user, gateway.access_key, "sensitive-upstream-token"):
        assert secret not in caplog.text + response.text


def test_html_403_is_controlled(gateway, upstream):
    upstream(lambda request: httpx.Response(403, text="<html>Forbidden</html>"))
    with pytest.raises(HTTPException) as exc:
        gateway._get_token()
    assert exc.value.status_code == 502
    assert exc.value.detail["code"] == "SIIGO_AUTH_FAILED"


def test_auth_rate_limit_remains_retryable(gateway, upstream):
    upstream(lambda request: httpx.Response(429))
    with pytest.raises(HTTPException) as exc:
        gateway._get_token()
    assert exc.value.status_code == 503


@pytest.mark.parametrize("error,status,code", [
    (httpx.ReadTimeout, 504, "SIIGO_AUTH_TIMEOUT"),
    (httpx.ConnectError, 502, "SIIGO_AUTH_UNAVAILABLE"),
])
def test_auth_network_errors_are_controlled(gateway, upstream, error, status, code):
    def fail(request):
        raise error("upstream unavailable", request=request)

    upstream(fail)
    with pytest.raises(HTTPException) as exc:
        gateway._get_token()
    assert exc.value.status_code == status
    assert exc.value.detail["code"] == code


@pytest.mark.parametrize("body", [
    b"not-json", b"{}", b"[]", b'{"access_token": 123}',
    b'{"access_token": "test-token", "expires_in": "invalid"}',
    b'{"access_token": "test-token", "expires_in": -1}',
])
def test_invalid_auth_response_is_controlled(gateway, upstream, body):
    upstream(lambda request: httpx.Response(200, content=body))
    with pytest.raises(HTTPException) as exc:
        gateway._get_token()
    assert exc.value.status_code == 502
    assert exc.value.detail["code"] == "SIIGO_AUTH_INVALID_RESPONSE"
    assert gateway._token is None


def test_missing_partner_id_prevents_auth_request(gateway, upstream):
    gateway.partner_id = ""

    def unexpected(request):
        pytest.fail("Authentication must not run without Partner-Id")

    upstream(unexpected)
    with pytest.raises(HTTPException) as exc:
        gateway._get_token()
    assert exc.value.status_code == 500


@pytest.mark.parametrize("setting,status", [
    ("enabled", 501), ("api_user", 500), ("access_key", 500), ("signin_url", 500),
])
def test_invalid_config_prevents_auth_request(gateway, upstream, setting, status):
    setattr(gateway, setting, False if setting == "enabled" else "")

    def unexpected(request):
        pytest.fail("Authentication must not run with missing configuration")

    upstream(unexpected)
    with pytest.raises(HTTPException) as exc:
        gateway._get_token()
    assert exc.value.status_code == status


def test_jwt_expiry_is_used_and_expired_token_is_refreshed(gateway, upstream):
    expiration = int(time.time()) + 86400
    payload = base64.urlsafe_b64encode(json.dumps({"exp": expiration}).encode()).decode().rstrip("=")
    token = f"header.{payload}.signature"
    requests = []

    def respond(request):
        requests.append(request)
        return httpx.Response(201, json={"access_token": token})

    gateway._token = "expired-token"
    gateway._token_exp = int(time.time()) - 1
    upstream(respond)
    assert gateway._headers()["Authorization"] == f"Bearer {token}"
    assert gateway._headers()["Authorization"] == f"Bearer {token}"
    assert len(requests) == 1
    assert gateway._token_exp == expiration
