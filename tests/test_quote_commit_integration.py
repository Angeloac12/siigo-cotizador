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
    monkeypatch.setenv("SIIGO_QUOTE_WORKER_ENABLED", "false")
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


def pending_quote(endpoint):
    def timeout(**payload):
        endpoint.calls.append(payload)
        raise HTTPException(504, detail={"code": "SIIGO_QUOTE_TIMEOUT", "outcome_unknown": True})
    endpoint.gateway.create_quote = timeout
    assert endpoint.client.post(PATH, json=PAYLOAD).status_code == 504
    attempt = state(endpoint.db)["warnings_json"]["quote_commit"]
    quotation = {**attempt["quote_payload"], "id": "71c4c296-4190-4c46-9dc6-8fb65c6f0c3b",
                 "name": "C-1-123", "metadata": {"created": attempt["started_at"]}}
    endpoint.gateway.list_quotes = lambda **params: {"results": [quotation]}
    endpoint.gateway.get_quote = lambda quote_id: quotation
    return attempt, quotation


def test_queued_job_survives_new_engine_and_replays_only_once(endpoint):
    from app.services.quote_queue import run_next_quote
    first = endpoint.client.post(PATH, json={**PAYLOAD, "enqueue": True})
    assert first.status_code == 202
    assert first.json()["submission"]["state"] == "QUEUED"
    assert not endpoint.calls
    repeated = endpoint.client.post(PATH, json={**PAYLOAD, "enqueue": True})
    assert repeated.json()["submission"]["attempt_id"] == first.json()["submission"]["attempt_id"]
    assert endpoint.client.post(PATH, json=PAYLOAD).status_code == 409
    assert endpoint.client.put("/v1/drafts/test-draft/items", json={"items": [
        {"line_index": 0, "description": "Other", "quantity": 1},
    ]}).status_code == 409
    # Preserve the isolated search_path while using an independent engine/pool.
    with endpoint.db.connect() as conn:
        schema = conn.execute(text("SELECT current_schema()")).scalar_one()
    restarted = create_engine(endpoint.db.url, connect_args={"options": f"-csearch_path={schema}"})
    try:
        assert run_next_quote(restarted, endpoint.gateway)
        assert not run_next_quote(restarted, endpoint.gateway)
    finally:
        restarted.dispose()
    assert len(endpoint.calls) == 1
    assert endpoint.client.get("/v1/drafts/test-draft/quote/submission").json()["submission"]["state"] == "SUCCEEDED"
    assert endpoint.client.post(PATH, json=PAYLOAD).status_code == 200
    assert len(endpoint.calls) == 1


def test_queued_timeout_is_never_replayed(endpoint):
    from app.services.quote_queue import run_next_quote
    def timeout(**payload):
        endpoint.calls.append(payload)
        raise HTTPException(504, detail={"code": "SIIGO_QUOTE_TIMEOUT", "outcome_unknown": True})
    endpoint.gateway.create_quote = timeout
    assert endpoint.client.post(PATH, json={**PAYLOAD, "enqueue": True}).status_code == 202
    assert run_next_quote(endpoint.db, endpoint.gateway)
    assert not run_next_quote(endpoint.db, endpoint.gateway)
    status = endpoint.client.get("/v1/drafts/test-draft/quote/submission").json()["submission"]
    assert status["state"] == "UNKNOWN"
    assert not status["can_submit"] and not status["can_edit"]
    assert len(endpoint.calls) == 1


def test_verify_empty_result_never_releases_pending_submission(endpoint):
    pending_quote(endpoint)
    endpoint.gateway.list_quotes = lambda **params: {"results": []}
    response = endpoint.client.post("/v1/drafts/test-draft/quote/verify")
    assert response.status_code == 200, response.text
    assert response.json()["candidates"] == []
    assert state(endpoint.db)["warnings_json"]["quote_commit"]["state"] == "UNKNOWN"
    assert endpoint.client.post(PATH, json=PAYLOAD).status_code == 409
    assert len(endpoint.calls) == 1


def test_verify_requires_explicit_confirmation_then_reuses_result(endpoint):
    attempt, quotation = pending_quote(endpoint)
    verified = endpoint.client.post("/v1/drafts/test-draft/quote/verify")
    assert verified.status_code == 200, verified.text
    assert verified.json()["candidates"][0]["can_confirm"]
    assert state(endpoint.db)["status"] != "COMMITTED"
    confirmation = {"attempt_id": attempt["attempt_id"], "quotation_id": quotation["id"], "confirmed": True}
    path = "/v1/drafts/test-draft/quote/confirm"
    assert endpoint.client.post(path, json={**confirmation, "confirmed": False}).status_code == 422
    confirmed = endpoint.client.post(path, json=confirmation)
    assert confirmed.status_code == 200, confirmed.text
    assert confirmed.json()["submission"]["state"] == "SUCCEEDED"
    assert endpoint.client.post(path, json=confirmation).status_code == 200
    assert endpoint.client.post(PATH, json=PAYLOAD).json()["siigo_quote_response"]["id"] == quotation["id"]
    assert len(endpoint.calls) == 1
    assert state(endpoint.db)["warnings_json"]["global_warnings"] == ["test-warning"]


@pytest.mark.parametrize("change", ["quantity", "price", "description", "customer", "date", "created", "missing_items"])
def test_confirmation_refetches_document_and_rejects_mismatches(endpoint, change):
    import copy
    attempt, quotation = pending_quote(endpoint)
    assert endpoint.client.post("/v1/drafts/test-draft/quote/verify").status_code == 200
    changed = copy.deepcopy(quotation)
    if change in ("quantity", "price", "description"):
        changed["items"][0][change] = "Different" if change == "description" else 999
    elif change == "customer":
        changed["customer"]["identification"] = "other-client"
    elif change == "date":
        changed["date"] = "2000-01-01"
    elif change == "created":
        changed["metadata"]["created"] = "2000-01-01T00:00:00Z"
    else:
        del changed["items"]
    endpoint.gateway.get_quote = lambda quote_id: changed
    response = endpoint.client.post("/v1/drafts/test-draft/quote/confirm", json={
        "attempt_id": attempt["attempt_id"], "quotation_id": quotation["id"], "confirmed": True,
    })
    assert response.status_code == 409
    assert state(endpoint.db)["warnings_json"]["quote_commit"]["state"] == "UNKNOWN"
    assert len(endpoint.calls) == 1


def test_legacy_pending_attempt_keeps_block_and_requests_operational_review(endpoint):
    with quote_commit_attempt(endpoint.db, "test-draft", "legacy"):
        pass
    response = endpoint.client.post("/v1/drafts/test-draft/quote/verify")
    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "SIIGO_QUOTE_LEGACY_REVIEW_REQUIRED"
    assert endpoint.client.post(PATH, json=PAYLOAD).status_code == 409
    assert not endpoint.calls


def test_lifespan_worker_consumes_queue_without_holding_request(endpoint, monkeypatch):
    completed = threading.Event()
    def success(**payload):
        endpoint.calls.append(payload)
        completed.set()
        return {"id": "worker-test-quote"}
    endpoint.gateway.create_quote = success
    # Seed while the worker is stopped, then start a new app lifecycle.
    assert endpoint.client.post(PATH, json={**PAYLOAD, "enqueue": True}).status_code == 202
    monkeypatch.setenv("SIIGO_QUOTE_WORKER_ENABLED", "true")
    main = importlib.import_module("app.main")
    with TestClient(main.app) as client:
        assert completed.wait(5)
        assert client.get("/health").status_code == 200
    assert len(endpoint.calls) == 1


def test_two_queue_workers_cannot_send_one_job_twice(endpoint):
    from app.services.quote_queue import run_next_quote
    started, release = threading.Event(), threading.Event()
    def slow(**payload):
        endpoint.calls.append(payload)
        started.set()
        assert release.wait(5)
        return {"id": "one-quote-only"}
    endpoint.gateway.create_quote = slow
    endpoint.client.post(PATH, json={**PAYLOAD, "enqueue": True})
    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(run_next_quote, endpoint.db, endpoint.gateway)
        try:
            assert started.wait(3)
            assert not run_next_quote(endpoint.db, endpoint.gateway)
        finally:
            release.set()
        assert first.result(timeout=3)
    assert len(endpoint.calls) == 1


def test_verify_filters_other_customers_and_preserves_pagination(endpoint):
    import copy
    _, quotation = pending_quote(endpoint)
    other = copy.deepcopy(quotation)
    other["customer"]["identification"] = "other"
    seen = []
    def listing(**params):
        seen.append(params)
        return {"results": [other, quotation], "_links": {"next": {"href": "https://untrusted.example"}}}
    endpoint.gateway.list_quotes = listing
    response = endpoint.client.post("/v1/drafts/test-draft/quote/verify?page=2")
    assert response.status_code == 200, response.text
    assert len(response.json()["candidates"]) == 1
    assert response.json()["has_more"] and seen[0]["page"] == 2
    assert seen[0]["identification"] == "123"


def test_lookup_failure_keeps_unknown_state_and_no_second_post(endpoint):
    pending_quote(endpoint)
    def unavailable(**params):
        raise HTTPException(502, detail={"code": "SIIGO_QUOTE_LOOKUP_UNAVAILABLE"})
    endpoint.gateway.list_quotes = unavailable
    assert endpoint.client.post("/v1/drafts/test-draft/quote/verify").status_code == 502
    assert state(endpoint.db)["warnings_json"]["quote_commit"]["state"] == "UNKNOWN"
    assert len(endpoint.calls) == 1


def test_active_attempt_cannot_be_confirmed_while_worker_is_running(endpoint):
    attempt, quotation = pending_quote(endpoint)
    with endpoint.db.begin() as conn:
        conn.execute(text("""UPDATE drafts SET warnings_json=jsonb_set(warnings_json,
            '{quote_commit,state}', '"IN_PROGRESS"'::jsonb) WHERE id='test-draft'"""))
    endpoint.gateway.get_quote = lambda quote_id: pytest.fail("Active worker must finish first")
    response = endpoint.client.post("/v1/drafts/test-draft/quote/confirm", json={
        "attempt_id": attempt["attempt_id"], "quotation_id": quotation["id"], "confirmed": True,
    })
    assert response.status_code == 409
    assert len(endpoint.calls) == 1


def test_interrupted_old_attempt_can_be_explicitly_reconciled(endpoint):
    import json
    from datetime import datetime, timedelta, timezone
    attempt, quotation = pending_quote(endpoint)
    attempt["state"] = "IN_PROGRESS"
    attempt["started_at"] = (datetime.now(timezone.utc) - timedelta(minutes=20)).isoformat()
    quotation["metadata"]["created"] = attempt["started_at"]
    with endpoint.db.begin() as conn:
        conn.execute(text("""UPDATE drafts SET warnings_json=jsonb_set(warnings_json,
            '{quote_commit}', CAST(:attempt AS jsonb)) WHERE id='test-draft'"""), {"attempt": json.dumps(attempt)})
    response = endpoint.client.post("/v1/drafts/test-draft/quote/confirm", json={
        "attempt_id": attempt["attempt_id"], "quotation_id": quotation["id"], "confirmed": True,
    })
    assert response.status_code == 200, response.text
    assert state(endpoint.db)["status"] == "COMMITTED"
    assert len(endpoint.calls) == 1


def test_confirmation_does_not_overwrite_a_changed_attempt(endpoint):
    attempt, quotation = pending_quote(endpoint)
    def changed_attempt(quote_id):
        with endpoint.db.begin() as conn:
            conn.execute(text("""UPDATE drafts SET warnings_json=jsonb_set(warnings_json,
                '{quote_commit,attempt_id}', '"different-attempt"'::jsonb) WHERE id='test-draft'"""))
        return quotation
    endpoint.gateway.get_quote = changed_attempt
    response = endpoint.client.post("/v1/drafts/test-draft/quote/confirm", json={
        "attempt_id": attempt["attempt_id"], "quotation_id": quotation["id"], "confirmed": True,
    })
    assert response.status_code == 409
    assert state(endpoint.db)["status"] != "COMMITTED"
    assert state(endpoint.db)["warnings_json"]["quote_commit"]["attempt_id"] == "different-attempt"


def test_pending_attempt_blocks_all_item_mutations(endpoint, monkeypatch):
    import app.api.routes_matching as matching
    import app.api.routes_overrides as overrides
    monkeypatch.setattr(matching, "get_engine", lambda: endpoint.db)
    monkeypatch.setattr(overrides, "get_engine", lambda: endpoint.db)
    monkeypatch.setenv("ENABLE_MATCHING", "true")
    pending_quote(endpoint)
    responses = [
        endpoint.client.put("/v1/drafts/test-draft/items/0/description", json={"description": "Other"}),
        endpoint.client.patch("/v1/drafts/test-draft/items/0/select", json={"org_id": "org", "code": "456"}),
        endpoint.client.post("/v1/drafts/test-draft/match", json={"org_id": "org", "apply": True}),
    ]
    assert [response.status_code for response in responses] == [409, 409, 409]
    assert len(endpoint.calls) == 1


def test_enqueue_of_committed_draft_returns_saved_submission_contract(endpoint):
    assert endpoint.client.post(PATH, json=PAYLOAD).status_code == 200
    repeated = endpoint.client.post(PATH, json={**PAYLOAD, "enqueue": True})
    assert repeated.status_code == 200
    assert repeated.json()["submission"]["state"] == "SUCCEEDED"
    assert repeated.json()["submission"]["siigo_quote_response"]["id"] == "siigo-test-quote"
    assert len(endpoint.calls) == 1
