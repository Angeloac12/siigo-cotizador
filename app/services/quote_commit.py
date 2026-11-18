"""Durable per-draft reservation; no transaction is held during a Siigo call."""

import json
import logging
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from collections.abc import Iterator, Mapping
from typing import Any

from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.engine import Engine


logger = logging.getLogger(__name__)


def quote_submission_status(draft: Mapping[str, Any]) -> dict[str, Any]:
    warnings = draft.get("warnings_json") or {}
    attempt = warnings.get("quote_commit") or {}
    state = attempt.get("state", "NOT_SENT")
    saved = warnings.get("siigo_quote_response")
    confirmed = isinstance(saved, dict) and isinstance(saved.get("id"), str) and bool(saved["id"].strip())
    if confirmed:
        state = "SUCCEEDED"
    elif draft.get("status") == "COMMITTED" or state == "SUCCEEDED":
        state = "UNKNOWN"
    labels = {
        "NOT_SENT": "Lista para enviar", "QUEUED": "En cola",
        "IN_PROGRESS": "Enviando; resultado aún sin confirmar",
        "UNKNOWN": "Pendiente de confirmación", "SUCCEEDED": "Cotización creada",
        "FAILED": "Envío rechazado; revisa los datos antes de volver a enviar",
    }
    if state == "FAILED":
        labels["FAILED"] = {
            "CLIENT_NOT_FOUND": "El cliente no existe en Siigo. Completa sus datos y activa Crear cliente.",
            "CLIENT_MISSING_FIELDS": "Faltan datos para crear el cliente. Complétalos antes de enviar.",
            "SIIGO_QUOTE_RATE_LIMITED": "Siigo limitó los envíos. Espera antes de volver a enviar.",
        }.get(attempt.get("error_code"), labels["FAILED"])
    return {
        "state": state, "message": labels.get(state, "Pendiente de revisión"),
        "can_submit": state in ("NOT_SENT", "FAILED"),
        "can_edit": state in ("NOT_SENT", "FAILED"),
        "can_verify": state in ("UNKNOWN", "IN_PROGRESS"),
        "outcome_unknown": state in ("UNKNOWN", "IN_PROGRESS"),
        "attempt_id": attempt.get("attempt_id"),
        "started_at": attempt.get("started_at"),
        "error_code": attempt.get("error_code"),
        "siigo_quote_response": warnings.get("siigo_quote_response"),
    }


def ensure_quote_editable(draft: Mapping[str, Any]) -> None:
    warnings = draft.get("warnings_json") or {}
    if not isinstance(warnings, dict):
        return
    state = (warnings.get("quote_commit") or {}).get("state")
    if state in ("QUEUED", "IN_PROGRESS", "UNKNOWN"):
        raise HTTPException(status_code=409, detail={
            "code": "SIIGO_QUOTE_REVIEW_REQUIRED" if state == "UNKNOWN" else "SIIGO_QUOTE_IN_PROGRESS",
            "message": (
                "El envío de esta cotización está pendiente de confirmación. "
                "Revisa el resultado en Siigo antes de editar o volver a enviarla."
            ),
            "outcome_unknown": True,
            "retryable": False,
        })


def lock_quote_editable(conn, draft_id: str) -> None:
    draft = conn.execute(text("SELECT status, warnings_json FROM drafts WHERE id=:id FOR UPDATE"),
                         {"id": draft_id}).mappings().first()
    if not draft:
        raise HTTPException(404, detail="Draft not found")
    ensure_quote_editable(draft)
    if not quote_submission_status(draft)["can_edit"]:
        raise HTTPException(409, detail={"code": "SIIGO_QUOTE_ALREADY_COMMITTED",
            "message": "La cotización ya fue creada y no permite editar el borrador."})


@contextmanager
def quote_commit_attempt(
    engine: Engine, draft_id: str, correlation_id: str,
    quote_payload: dict | None = None, queued_id: str | None = None,
) -> Iterator[str]:
    started = time.monotonic()
    attempt_id = uuid.uuid4().hex
    attempt = {
        "attempt_id": attempt_id,
        "state": "IN_PROGRESS",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "correlation_id": correlation_id,
    }
    if quote_payload is not None:
        attempt["quote_payload"] = quote_payload
    with engine.begin() as conn:
        # The UPDATE serializes concurrent reservations, including across workers.
        claimed = conn.execute(text("""
            UPDATE drafts
            SET warnings_json = COALESCE(warnings_json, '{}'::jsonb)
                    || jsonb_build_object('quote_commit', CAST(:attempt AS jsonb)),
                updated_at = now()
            WHERE id = :id AND status <> 'COMMITTED'
              AND COALESCE(warnings_json->'siigo_quote_response', 'null'::jsonb) = 'null'::jsonb
              AND (
                  (CAST(:queued_id AS text) IS NULL AND COALESCE(warnings_json->'quote_commit'->>'state', '')
                      NOT IN ('QUEUED', 'IN_PROGRESS', 'UNKNOWN'))
                  OR (warnings_json->'quote_commit'->>'state' = 'QUEUED'
                      AND warnings_json->'quote_commit'->>'attempt_id' = :queued_id)
              )
            RETURNING id
        """), {"id": draft_id, "attempt": json.dumps(attempt), "queued_id": queued_id}).first()
        if not claimed:
            draft = conn.execute(text("""
                SELECT id, status, warnings_json FROM drafts WHERE id = :id
            """), {"id": draft_id}).mappings().first()
            if not draft:
                raise HTTPException(status_code=404, detail="Draft not found")
            ensure_quote_editable(draft)
            raise HTTPException(status_code=409, detail={
                "code": "SIIGO_QUOTE_ALREADY_COMMITTED",
                "message": "La cotización ya fue creada. Consulta el borrador para ver el resultado.",
                "retryable": False,
            })

    try:
        yield attempt_id
    except BaseException as exc:
        # Unknown exceptions, cancellation or persistence failure after the POST
        # must not release a reservation and allow an accidental second POST.
        detail = exc.detail if isinstance(exc, HTTPException) and isinstance(exc.detail, dict) else {}
        code = str(detail.get("code") or "SIIGO_QUOTE_UNCONFIRMED")
        known_rejection = isinstance(exc, HTTPException) and (
            exc.status_code < 500 or code.startswith("SIIGO_AUTH_")
        )
        unknown = bool(detail.get("outcome_unknown", not known_rejection))
        attempt.update(state="UNKNOWN" if unknown else "FAILED", error_code=code)
        logger.warning(
            "quote_commit_unconfirmed draft_id=%s correlation_id=%s attempt_id=%s state=%s error_code=%s duration_ms=%s",
            draft_id, correlation_id, attempt_id, attempt["state"], code,
            int((time.monotonic() - started) * 1000),
        )
        try:
            with engine.begin() as conn:
                conn.execute(text("""
                    UPDATE drafts
                    SET warnings_json = COALESCE(warnings_json, '{}'::jsonb)
                            || jsonb_build_object('quote_commit', CAST(:attempt AS jsonb)),
                        updated_at = now()
                    WHERE id = :id
                      AND warnings_json->'quote_commit'->>'attempt_id' = :attempt_id
                      AND warnings_json->'quote_commit'->>'state' = 'IN_PROGRESS'
                """), {"id": draft_id, "attempt_id": attempt_id, "attempt": json.dumps(attempt)})
        except Exception:
            # The original durable IN_PROGRESS marker still blocks resubmission.
            logger.error("quote_commit_state_save_failed draft_id=%s correlation_id=%s", draft_id, correlation_id)
        raise
