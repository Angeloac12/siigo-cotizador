import logging

import httpx
import pytest
from fastapi import FastAPI
from starlette.requests import Request
from fastapi.testclient import TestClient

from app.upstream_gateway.siigo import SiigoGateway
from app.services.siigo_timeouts import quote_read_timeout, quote_timeout
from app.api.routes_quote_drafts import _timeout_for_path
from app.api.routes_quote_drafts import _proxy


@pytest.fixture
def quote_endpoint(monkeypatch):
    gateway = SiigoGateway()
    monkeypatch.setattr(gateway, "_headers", lambda: {"Authorization": "Bearer private-token"})
    app = FastAPI()

    @app.post("/commit")
    def commit():
        return gateway.create_quote(
            customer_identification="123", branch_office=0, document_id=1,
            seller=1, items=[], date_iso="2026-11-17",
        )

    return gateway, app


@pytest.mark.parametrize("error,status,code", [
    (httpx.ReadTimeout, 504, "SIIGO_QUOTE_TIMEOUT"),
    (httpx.ConnectTimeout, 504, "SIIGO_QUOTE_TIMEOUT"),
    (httpx.WriteTimeout, 504, "SIIGO_QUOTE_TIMEOUT"),
    (httpx.ConnectError, 502, "SIIGO_QUOTE_UNAVAILABLE"),
    (httpx.ReadError, 502, "SIIGO_QUOTE_UNAVAILABLE"),
    (httpx.RemoteProtocolError, 502, "SIIGO_QUOTE_UNAVAILABLE"),
])
def test_quote_network_failure_is_controlled_without_retry(
    quote_endpoint, monkeypatch, caplog, error, status, code,
):
    gateway, app = quote_endpoint
    requests = []

    def fail(request):
        requests.append(request)
        raise error("private-network-diagnostic", request=request)

    with httpx.Client(transport=httpx.MockTransport(fail)) as upstream:
        monkeypatch.setattr("app.upstream_gateway.siigo.httpx.post", upstream.post)
        with caplog.at_level(logging.WARNING), TestClient(app) as client:
            response = client.post("/commit")

    assert response.status_code == status
    detail = response.json()["detail"]
    assert detail["code"] == code
    assert detail["outcome_unknown"] is True
    assert detail["retryable"] is False
    assert "Siigo" in detail["message"]
    assert len(requests) == 1
    assert str(requests[0].url) == f"{gateway.base_url}/quotations"
    assert f"error_type={error.__name__}" in caplog.text
    for secret in ("private-token", "private-network-diagnostic"):
        assert secret not in caplog.text + response.text


def test_quote_timeout_after_token_refresh_is_not_retried(quote_endpoint, monkeypatch):
    gateway, app = quote_endpoint
    requests = []
    invalidations = []
    monkeypatch.setattr(gateway, "_invalidate_token", lambda: invalidations.append(True))

    def respond(request):
        requests.append(request)
        if len(requests) == 1:
            return httpx.Response(401)
        raise httpx.ReadTimeout("timed out", request=request)

    with httpx.Client(transport=httpx.MockTransport(respond)) as upstream:
        monkeypatch.setattr("app.upstream_gateway.siigo.httpx.post", upstream.post)
        with TestClient(app) as client:
            response = client.post("/commit")

    assert response.status_code == 504
    assert response.json()["detail"]["code"] == "SIIGO_QUOTE_TIMEOUT"
    assert len(requests) == 2
    assert invalidations == [True]


def test_quote_success_is_returned(quote_endpoint, monkeypatch):
    _, app = quote_endpoint
    result = {"id": "quote-123", "name": "C-1-123"}
    with httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(201, json=result))) as upstream:
        monkeypatch.setattr("app.upstream_gateway.siigo.httpx.post", upstream.post)
        with TestClient(app) as client:
            response = client.post("/commit")

    assert response.status_code == 200
    assert response.json() == result


def test_provider_504_is_pending_without_retry_or_exposing_raw_error(quote_endpoint, monkeypatch):
    _, app = quote_endpoint
    calls = []
    def respond(request):
        calls.append(request)
        return httpx.Response(504, headers={"siigoapi-error-code": "timed_out"}, json={
            "Status": 504, "Errors": [{"Code": "timed_out", "Message": "Request timed out."}],
        })
    with httpx.Client(transport=httpx.MockTransport(respond)) as upstream:
        monkeypatch.setattr("app.upstream_gateway.siigo.httpx.post", upstream.post)
        with TestClient(app) as client:
            response = client.post("/commit")
    detail = response.json()["detail"]
    assert response.status_code == 504
    assert detail["code"] == "SIIGO_QUOTE_TIMEOUT"
    assert detail["upstream_status"] == 504
    assert detail["outcome_unknown"] and not detail["retryable"]
    assert "Verificar en Siigo" in detail["message"]
    assert len(calls) == 1


def test_quote_listing_uses_bounded_read_and_filters(quote_endpoint, monkeypatch):
    gateway, _ = quote_endpoint
    def respond(request):
        assert request.method == "GET"
        assert request.url.params["customer_identification"] == "123"
        assert request.url.params["page"] == "2"
        assert request.extensions["timeout"]["read"] == 20
        return httpx.Response(200, json={"results": []})
    with httpx.Client(transport=httpx.MockTransport(respond)) as upstream:
        monkeypatch.setattr("app.upstream_gateway.siigo.httpx.get", upstream.get)
        assert gateway.list_quotes(identification="123", branch_office=0,
            created_start="2026-11-17", created_end="2026-11-17", page=2) == {"results": []}


@pytest.mark.parametrize("body", [b"html", b"{}", b'{"results":null}', b'{"results":[1]}'])
def test_invalid_lookup_is_not_an_empty_success(quote_endpoint, monkeypatch, body):
    gateway, _ = quote_endpoint
    with httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(200, content=body))) as upstream:
        monkeypatch.setattr("app.upstream_gateway.siigo.httpx.get", upstream.get)
        from fastapi import HTTPException
        with pytest.raises(HTTPException) as exc:
            gateway.list_quotes(identification="123", branch_office=0,
                created_start="2026-11-17", created_end="2026-11-17")
    assert exc.value.status_code == 502


@pytest.mark.parametrize("quotation_id", ["../customers", "abc?token=secret", "", "https://example.com"])
def test_quote_detail_rejects_non_uuid_without_network(quote_endpoint, monkeypatch, quotation_id):
    gateway, _ = quote_endpoint
    monkeypatch.setattr("app.upstream_gateway.siigo.httpx.get", lambda *a, **kw: pytest.fail("Unexpected network call"))
    from fastapi import HTTPException
    with pytest.raises(HTTPException) as exc:
        gateway.get_quote(quotation_id)
    assert exc.value.status_code == 422


@pytest.mark.parametrize("setting,expected", [
    ("120", 120), ("180", 180), ("30", 30), ("300", 300),
    ("0", 120), ("nan", 120), ("inf", 120), ("bad", 120), ("301", 120),
])
def test_quote_timeout_configuration_is_bounded(monkeypatch, setting, expected):
    monkeypatch.setenv("SIIGO_QUOTE_READ_TIMEOUT_SECONDS", setting)
    assert quote_read_timeout() == expected
    assert quote_timeout().read == expected
    assert quote_timeout().connect == 10
    assert _timeout_for_path("/v1/drafts/test/quote/commit").read > 2 * expected


def test_quote_post_uses_120_second_read_timeout(quote_endpoint, monkeypatch):
    _, app = quote_endpoint
    monkeypatch.delenv("SIIGO_QUOTE_READ_TIMEOUT_SECONDS", raising=False)

    def respond(request):
        assert request.extensions["timeout"]["read"] == 120
        assert request.extensions["timeout"]["connect"] == 10
        return httpx.Response(201, json={"id": "quote-123"})

    with httpx.Client(transport=httpx.MockTransport(respond)) as upstream:
        monkeypatch.setattr("app.upstream_gateway.siigo.httpx.post", upstream.post)
        with TestClient(app) as client:
            assert client.post("/commit").status_code == 200


def test_quote_rate_limit_is_a_confirmed_rejection_without_immediate_retry(quote_endpoint, monkeypatch):
    _, app = quote_endpoint
    requests = []

    def limited(request):
        requests.append(request)
        return httpx.Response(429, headers={"Retry-After": "3600"})

    with httpx.Client(transport=httpx.MockTransport(limited)) as upstream:
        monkeypatch.setattr("app.upstream_gateway.siigo.httpx.post", upstream.post)
        with TestClient(app) as client:
            response = client.post("/commit")
    assert response.status_code == 503
    assert response.json()["detail"]["outcome_unknown"] is False
    assert response.json()["detail"]["code"] == "SIIGO_QUOTE_RATE_LIMITED"
    assert len(requests) == 1


@pytest.mark.parametrize("body", [b"not-json", b"{}", b"[]", b'{"id": null}'])
def test_unconfirmed_response_is_not_treated_as_success(quote_endpoint, monkeypatch, body):
    _, app = quote_endpoint
    with httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(201, content=body))) as upstream:
        monkeypatch.setattr("app.upstream_gateway.siigo.httpx.post", upstream.post)
        with TestClient(app) as client:
            response = client.post("/commit")
    assert response.status_code == 502
    assert response.json()["detail"]["code"] == "SIIGO_QUOTE_INVALID_RESPONSE"
    assert response.json()["detail"]["outcome_unknown"] is True


@pytest.mark.parametrize("error,status", [(httpx.ReadTimeout, 504), (httpx.ReadError, 502)])
def test_proxy_commit_error_does_not_tell_user_to_retry(monkeypatch, error, status):
    app = FastAPI()

    @app.post("/submit")
    async def submit(request: Request):
        return await _proxy(request, "POST", "/v1/drafts/test/quote/commit", "test-key", json={})

    class UnavailableClient:
        def __init__(self, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def request(self, *args, **kwargs):
            raise error("unavailable")

    monkeypatch.setattr("app.api.routes_quote_drafts.httpx.AsyncClient", UnavailableClient)
    with TestClient(app) as client:
        response = client.post("/submit")
    assert response.status_code == status
    assert response.json()["detail"]["retryable"] is False
    assert response.json()["detail"]["outcome_unknown"] is True
