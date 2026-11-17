"""Run with SIIGO_TEST_DATABASE_URL pointing to an isolated PostgreSQL instance.

Each test creates its own schema. All Siigo calls are simulated.
"""

import importlib
import os
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event, text

from app.services.quote_commit import quote_commit_attempt


@pytest.fixture
def quote_db():
    url = os.getenv("SIIGO_TEST_DATABASE_URL")
    if not url:
        pytest.skip("Set SIIGO_TEST_DATABASE_URL to run PostgreSQL integration tests")
    schema = "quote_test_" + uuid.uuid4().hex
    admin = create_engine(url)
    with admin.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(url, connect_args={"options": f"-csearch_path={schema}"})
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE drafts (
                    id text PRIMARY KEY, status text NOT NULL,
                    warnings_json jsonb, updated_at timestamptz DEFAULT now(),
                    upstream_client_id text, stored_path text, original_filename text
                );
                CREATE TABLE draft_items (
                    id text PRIMARY KEY, draft_id text REFERENCES drafts(id),
                    line_index integer, raw_text text, description text,
                    quantity numeric, uom text, item_code text, item_name text,
                    uom_raw text, confidence numeric, warnings_json jsonb
                );
                CREATE TABLE draft_item_selections (
                    draft_id text, line_index integer, description_override text, sim numeric
                );
                INSERT INTO drafts (id, status, warnings_json)
                VALUES ('test-draft', 'PARSED', '{"global_warnings":["test-warning"]}');
                INSERT INTO draft_items (id, draft_id, line_index, description, quantity, uom, item_code)
                VALUES ('item-1', 'test-draft', 0, 'Cable', 2, 'UND', '123');
            """))
        yield engine
    finally:
        engine.dispose()
        with admin.begin() as conn:
            conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        admin.dispose()


@pytest.fixture
def endpoint(quote_db, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", quote_db.url.render_as_string(hide_password=False))
    monkeypatch.setenv("API_KEYS", "local-test-key")
    monkeypatch.setenv("OPENAI_ENABLED", "false")
    main = importlib.import_module("app.main")
    monkeypatch.setattr(main, "get_engine", lambda: quote_db)
    calls = []

    def success(**payload):
        calls.append(payload)
        return {"id": "siigo-test-quote", "customer": {"id": "test-customer"}}

    gateway = SimpleNamespace(create_quote=success)
    monkeypatch.setattr(main, "get_gateway", lambda: gateway)
    with TestClient(main.app, raise_server_exceptions=False) as client:
        client.headers["X-API-Key"] = "local-test-key"
        yield SimpleNamespace(client=client, gateway=gateway, calls=calls, db=quote_db)


PATH = "/v1/drafts/test-draft/quote/commit"
PAYLOAD = {"customer_identification": "123", "document_id": 1, "seller": 1, "dry_run": False}


def state(engine):
    with engine.connect() as conn:
        return conn.execute(text("SELECT status, warnings_json FROM drafts WHERE id='test-draft'")).mappings().one()


def test_success_is_saved_and_replayed_without_second_post(endpoint):
    first = endpoint.client.post(PATH, json=PAYLOAD)
    assert first.status_code == 200, first.text
    assert endpoint.client.post(PATH, json=PAYLOAD).status_code == 200
    assert len(endpoint.calls) == 1
    saved = state(endpoint.db)
    assert saved["status"] == "COMMITTED"
    assert saved["warnings_json"]["quote_commit"]["state"] == "SUCCEEDED"
    assert saved["warnings_json"]["global_warnings"] == ["test-warning"]


def test_timeout_is_persistent_and_prevents_resubmit_and_edit(endpoint):
    def timeout(**payload):
        endpoint.calls.append(payload)
        raise HTTPException(504, detail={"code": "SIIGO_QUOTE_TIMEOUT", "outcome_unknown": True})

    endpoint.gateway.create_quote = timeout
    assert endpoint.client.post(PATH, json=PAYLOAD).status_code == 504
    assert state(endpoint.db)["warnings_json"]["quote_commit"]["state"] == "UNKNOWN"
    again = endpoint.client.post(PATH, json=PAYLOAD)
    assert again.status_code == 409
    assert again.json()["detail"]["code"] == "SIIGO_QUOTE_REVIEW_REQUIRED"
    assert len(endpoint.calls) == 1
    edit = endpoint.client.put("/v1/drafts/test-draft/items", json={"items": [
        {"line_index": 0, "description": "Other", "quantity": 1, "uom": "UND"},
    ]})
    assert edit.status_code == 409
    assert endpoint.client.post("/v1/drafts/test-draft/parse").status_code == 409
    assert endpoint.client.get("/v1/drafts/test-draft").json()["draft"]["warnings_json"]["quote_commit"]["state"] == "UNKNOWN"
    assert endpoint.client.post(PATH, json={**PAYLOAD, "dry_run": True}).status_code == 200
    assert len(endpoint.calls) == 1


def test_concurrent_requests_and_health_are_not_blocked(endpoint):
    started = threading.Event()
    release = threading.Event()

    def slow(**payload):
        endpoint.calls.append(payload)
        started.set()
        assert release.wait(10)
        return {"id": "siigo-test-quote"}

    endpoint.gateway.create_quote = slow
    with ThreadPoolExecutor(max_workers=3) as executor:
        first = executor.submit(endpoint.client.post, PATH, json=PAYLOAD)
        try:
            assert started.wait(3)
            health = executor.submit(endpoint.client.get, "/health")
            assert health.result(timeout=2).status_code == 200
            duplicate = executor.submit(endpoint.client.post, PATH, json=PAYLOAD).result(timeout=2)
            assert duplicate.status_code == 409
            assert duplicate.json()["detail"]["code"] == "SIIGO_QUOTE_IN_PROGRESS"
            assert len(endpoint.calls) == 1
        finally:
            release.set()
        assert first.result(timeout=3).status_code == 200


def test_confirmed_rejection_allows_correction_and_retry(endpoint):
    original = endpoint.gateway.create_quote

    def rejected(**payload):
        raise HTTPException(400, detail={"code": "INVALID_REFERENCE", "outcome_unknown": False})

    endpoint.gateway.create_quote = rejected
    assert endpoint.client.post(PATH, json=PAYLOAD).status_code == 400
    assert state(endpoint.db)["warnings_json"]["quote_commit"]["state"] == "FAILED"
    endpoint.gateway.create_quote = original
    assert endpoint.client.post(PATH, json=PAYLOAD).status_code == 200


def test_dry_run_does_not_reserve_or_call_siigo(endpoint):
    assert endpoint.client.post(PATH, json={**PAYLOAD, "dry_run": True}).status_code == 200
    assert "quote_commit" not in state(endpoint.db)["warnings_json"]
    assert not endpoint.calls


def test_interrupted_process_reservation_still_blocks_another_instance(endpoint):
    # Simulate persisted IN_PROGRESS left by process shutdown, with no success saved.
    with quote_commit_attempt(endpoint.db, "test-draft", "test-correlation"):
        pass
    response = endpoint.client.post(PATH, json=PAYLOAD)
    assert response.status_code == 409
    assert not endpoint.calls


def test_failure_to_save_success_does_not_allow_a_second_post(endpoint):
    def fail_save(conn, cursor, statement, parameters, context, executemany):
        if "SET status='COMMITTED'" in statement:
            raise RuntimeError("simulated persistence failure")

    event.listen(endpoint.db, "before_cursor_execute", fail_save)
    try:
        assert endpoint.client.post(PATH, json=PAYLOAD).status_code == 500
    finally:
        event.remove(endpoint.db, "before_cursor_execute", fail_save)
    assert state(endpoint.db)["warnings_json"]["quote_commit"]["state"] == "UNKNOWN"
    assert endpoint.client.post(PATH, json=PAYLOAD).status_code == 409
    assert len(endpoint.calls) == 1


def test_reservation_is_atomic_across_independent_workers(quote_db):
    barrier = threading.Barrier(2)
    rejected = threading.Event()

    def worker():
        barrier.wait(timeout=3)
        try:
            with quote_commit_attempt(quote_db, "test-draft", "worker-correlation"):
                assert rejected.wait(3)
                return "reserved"
        except HTTPException as exc:
            rejected.set()
            return exc.detail["code"]

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _: worker(), range(2)))
    assert sorted(results) == ["SIIGO_QUOTE_IN_PROGRESS", "reserved"]


def test_auth_timeout_does_not_lock_a_quote_that_was_never_sent(endpoint):
    original = endpoint.gateway.create_quote

    def auth_failure(**payload):
        raise HTTPException(504, detail={"code": "SIIGO_AUTH_TIMEOUT"})

    endpoint.gateway.create_quote = auth_failure
    assert endpoint.client.post(PATH, json=PAYLOAD).status_code == 504
    assert state(endpoint.db)["warnings_json"]["quote_commit"]["state"] == "FAILED"
    endpoint.gateway.create_quote = original
    assert endpoint.client.post(PATH, json=PAYLOAD).status_code == 200


def test_timeout_after_customer_creation_keeps_reservation(endpoint, monkeypatch):
    def quote(**payload):
        endpoint.calls.append(payload)
        if len(endpoint.calls) == 1:
            raise HTTPException(400, detail={
                "siigoapi_error_code": "invalid_reference",
                "response": {"Errors": [{"Message": "Customer doesn't exist"}]},
            })
        raise HTTPException(504, detail={"code": "SIIGO_QUOTE_TIMEOUT", "outcome_unknown": True})

    endpoint.gateway.create_quote = quote
    endpoint.gateway.create_client = lambda payload: {"id": "new-customer"}
    # Avoid a real sleep while exercising the customer creation branch.
    monkeypatch.setattr("app.main.time.sleep", lambda _: None)
    customer = {
        "person_type": "Person", "id_type": "13", "identification": "123", "name": ["Test"],
        "fiscal_responsibilities": [{"code": "R-99-PN"}],
        "address": {"address": "Test address", "city": {"country_code": "Co", "state_code": "11", "city_code": "11001"}},
        "contacts": [{"first_name": "Test"}],
    }
    payload = {**PAYLOAD, "create_customer_if_missing": True, "customer_create_payload": customer}
    assert endpoint.client.post(PATH, json=payload).status_code == 504
    assert state(endpoint.db)["warnings_json"]["quote_commit"]["state"] == "UNKNOWN"
    assert endpoint.client.post(PATH, json=payload).status_code == 409
    assert len(endpoint.calls) == 2  # One explicit rejection; one unconfirmed creation.
