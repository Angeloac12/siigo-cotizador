"""PostgreSQL backed queue. Only QUEUED jobs may be claimed; never replay a POST."""
import asyncio
import json
import logging
import uuid
from datetime import datetime, timezone
from types import SimpleNamespace

from fastapi import HTTPException
from sqlalchemy import text

from app.services.quote_commit import quote_submission_status
from app.services.quote_submission import execute_quote

logger = logging.getLogger(__name__)


def enqueue_quote(engine, draft_id, correlation_id, quote_payload, options):
    with engine.begin() as conn:
        draft = conn.execute(text("SELECT id, status, warnings_json FROM drafts WHERE id=:id FOR UPDATE"),
                             {"id": draft_id}).mappings().first()
        if not draft:
            raise HTTPException(404, detail="Draft not found")
        status = quote_submission_status(draft)
        if status["state"] in ("QUEUED", "IN_PROGRESS", "UNKNOWN", "SUCCEEDED"):
            return {"draft_id": draft_id, "submission": status}
        attempt = {
            "state": "QUEUED", "attempt_id": uuid.uuid4().hex,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "correlation_id": correlation_id, "quote_payload": quote_payload,
            "job_options": options,
        }
        conn.execute(text("""
            UPDATE drafts SET warnings_json=COALESCE(warnings_json, '{}'::jsonb)
                || jsonb_build_object('quote_commit', CAST(:attempt AS jsonb)), updated_at=now()
            WHERE id=:id
        """), {"id": draft_id, "attempt": json.dumps(attempt)})
        saved = {**dict(draft), "warnings_json": {"quote_commit": attempt}}
    return {"draft_id": draft_id, "submission": quote_submission_status(saved)}


def run_next_quote(engine, gateway) -> bool:
    # No network call while holding a DB transaction. execute_quote atomically
    # claims by queued attempt_id, so multiple workers cannot send the same job.
    with engine.connect() as conn:
        draft = conn.execute(text("""
            SELECT id, warnings_json->'quote_commit' AS attempt FROM drafts
            WHERE status <> 'COMMITTED' AND warnings_json->'quote_commit'->>'state'='QUEUED'
            ORDER BY updated_at, id LIMIT 1
        """)).mappings().first()
    if not draft:
        return False
    attempt = draft["attempt"]
    try:
        execute_quote(engine, gateway, draft["id"], attempt["correlation_id"],
                      attempt["quote_payload"], SimpleNamespace(**attempt["job_options"]),
                      queued_id=attempt["attempt_id"])
    except Exception:
        # execute_quote keeps UNKNOWN/FAILED durably, or IN_PROGRESS if saving
        # failed. Do not log payloads, credentials or retry an uncertain POST.
        logger.warning("quote_job_finished_without_confirmation draft_id=%s correlation_id=%s",
                       draft["id"], attempt["correlation_id"])
    return True


async def quote_worker(engine_factory, gateway_factory, stop: asyncio.Event):
    while not stop.is_set():
        try:
            await asyncio.to_thread(run_next_quote, engine_factory(), gateway_factory())
        except Exception:
            logger.error("quote_worker_poll_failed")
        # Pace the worker even when the provider responds immediately.
        try:
            await asyncio.wait_for(stop.wait(), timeout=2)
        except TimeoutError:
            pass
