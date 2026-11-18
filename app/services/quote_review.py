"""Read-only discovery and explicit reconciliation; absence never releases a draft."""
import json
from collections import Counter
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation

from fastapi import HTTPException
from sqlalchemy import text

from app.services.quote_commit import quote_submission_status


def load_draft(engine, draft_id):
    with engine.connect() as conn:
        draft = conn.execute(text("SELECT id, status, warnings_json FROM drafts WHERE id=:id"),
                             {"id": draft_id}).mappings().first()
    if not draft:
        raise HTTPException(404, detail="Draft not found")
    return dict(draft)


def _datetime(value):
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("Missing timezone")
    return parsed.astimezone(timezone.utc)


def review_attempt(draft):
    attempt = (draft.get("warnings_json") or {}).get("quote_commit") or {}
    if attempt.get("state") not in ("UNKNOWN", "IN_PROGRESS"):
        raise HTTPException(409, detail={"code": "SIIGO_QUOTE_NOT_PENDING",
            "message": "Este borrador no tiene un envío pendiente de verificación."})
    if not attempt.get("quote_payload"):
        raise HTTPException(409, detail={"code": "SIIGO_QUOTE_LEGACY_REVIEW_REQUIRED",
            "message": "Este envío antiguo no guardó los datos enviados. Requiere revisión operativa en Siigo; no lo reenvíes.",
            "outcome_unknown": True, "retryable": False})
    return attempt


def _number(value):
    value = Decimal(str(value))
    if not value.is_finite():
        raise ValueError("Invalid number")
    return value


def _items(items):
    # A multiset preserves repeated lines while allowing a different line order.
    if not isinstance(items, list) or not items:
        raise ValueError("Missing items")
    return Counter((str(i["code"]), str(i["description"]).strip(),
                    _number(i["quantity"]), _number(i["price"])) for i in items)


def comparison(payload, quotation, started_at):
    try:
        checks = {
            "customer": str(quotation["customer"]["identification"]) == str(payload["customer"]["identification"]),
            "branch_office": _number(quotation["customer"].get("branch_office", 0)) == _number(payload["customer"]["branch_office"]),
            "document": _number(quotation["document"]["id"]) == _number(payload["document"]["id"]),
            "seller": _number(quotation["seller"]) == _number(payload["seller"]),
            "date": quotation["date"] == payload["date"],
            "items": _items(quotation["items"]) == _items(payload["items"]),
            "creation_time": _datetime(started_at) - timedelta(minutes=5)
                <= _datetime(quotation["metadata"]["created"])
                <= min(_datetime(started_at) + timedelta(minutes=15), datetime.now(timezone.utc) + timedelta(minutes=5)),
        }
        return checks
    except (KeyError, TypeError, ValueError, InvalidOperation, AttributeError):
        return {"complete_data": False}


def can_confirm(attempt):
    # An active worker must finish before reconciliation. A killed worker leaves
    # IN_PROGRESS; allow explicit recovery only after all bounded calls can finish.
    return attempt["state"] == "UNKNOWN" or (
        datetime.now(timezone.utc) - _datetime(attempt["started_at"]) > timedelta(minutes=15)
    )


def verify_quote(engine, gateway, draft_id, page=1):
    draft = load_draft(engine, draft_id)
    attempt = review_attempt(draft)
    payload = attempt["quote_payload"]
    started = _datetime(attempt["started_at"]) - timedelta(minutes=5)
    ended = min(_datetime(attempt["started_at"]) + timedelta(minutes=15), datetime.now(timezone.utc))
    result = gateway.list_quotes(
        identification=payload["customer"]["identification"],
        branch_office=payload["customer"]["branch_office"],
        created_start=started.date().isoformat(),
        created_end=ended.date().isoformat(), page=page,
    )
    candidates = []
    for quotation in result["results"]:
        # Do not expose other clients even if upstream ignores its filters.
        if str((quotation.get("customer") or {}).get("identification")) != str(payload["customer"]["identification"]):
            continue
        checks = comparison(payload, quotation, attempt["started_at"])
        candidates.append({"quotation": quotation, "checks": checks,
                           "can_confirm": bool(quotation.get("id")) and all(checks.values()) and can_confirm(attempt)})
    # Re-read: a worker or another reviewer may have completed in the meantime.
    current = load_draft(engine, draft_id)
    current_status = quote_submission_status(current)
    if not current_status["can_verify"] or current_status["attempt_id"] != attempt["attempt_id"]:
        for candidate in candidates:
            candidate["can_confirm"] = False
    return {"draft_id": draft_id, "submission": current_status,
        "attempt_id": attempt["attempt_id"], "candidates": candidates, "page": page,
        "has_more": bool((result.get("_links") or {}).get("next")) or len(result["results"]) == 25,
        "message": "Confirma el documento que corresponde al envío. Si no aparece, conserva el estado pendiente y revisa de nuevo más tarde.",
        "retryable": False}


def confirm_quote(engine, gateway, draft_id, attempt_id, quotation_id, correlation_id):
    draft = load_draft(engine, draft_id)
    status = quote_submission_status(draft)
    if status["state"] == "SUCCEEDED":
        if (status["siigo_quote_response"] or {}).get("id") == quotation_id:
            return {"draft_id": draft_id, "submission": status}
        raise HTTPException(409, detail={"code": "SIIGO_QUOTE_ALREADY_COMMITTED"})
    attempt = review_attempt(draft)
    if attempt["attempt_id"] != attempt_id or not can_confirm(attempt):
        raise HTTPException(409, detail={"code": "SIIGO_QUOTE_REVIEW_CHANGED",
            "message": "El envío sigue en curso o cambió. Actualiza su estado antes de confirmar."})
    # Fetch the selected document again; never persist the client's JSON as proof.
    quotation = gateway.get_quote(quotation_id)
    checks = comparison(attempt["quote_payload"], quotation, attempt["started_at"])
    if quotation.get("id") != quotation_id or not all(checks.values()):
        raise HTTPException(409, detail={"code": "SIIGO_QUOTE_REVIEW_MISMATCH",
            "message": "El documento no coincide completamente con los datos enviados. Requiere revisión operativa.",
            "checks": checks, "outcome_unknown": True, "retryable": False})
    reviewed = {**attempt, "state": "SUCCEEDED", "review": {
        "confirmed_at": datetime.now(timezone.utc).isoformat(),
        "correlation_id": correlation_id, "quotation_id": quotation_id, "checks": checks,
    }}
    with engine.begin() as conn:
        saved = conn.execute(text("""
            UPDATE drafts SET status='COMMITTED', updated_at=now(),
                upstream_client_id=COALESCE(NULLIF(upstream_client_id, ''), :client_id),
                warnings_json=COALESCE(warnings_json, '{}'::jsonb) || CAST(:meta AS jsonb)
            WHERE id=:id AND status <> 'COMMITTED'
              AND warnings_json->'quote_commit'->>'attempt_id'=:attempt_id
              AND warnings_json->'quote_commit'->>'state'=:state
            RETURNING id
        """), {"id": draft_id, "attempt_id": attempt_id, "state": attempt["state"],
            "client_id": str((quotation.get("customer") or {}).get("id") or ""),
            "meta": json.dumps({"quote_commit": reviewed, "siigo_quote_response": quotation})}).first()
        if not saved:
            raise HTTPException(409, detail={"code": "SIIGO_QUOTE_REVIEW_CHANGED",
                "message": "El estado cambió durante la revisión. Actualízalo antes de continuar."})
    return {"draft_id": draft_id, "submission": quote_submission_status(load_draft(engine, draft_id))}
