"""Send an already prepared quotation. The persistent reservation owns the POST."""
import json
import logging
import time
from typing import Any, Dict, List, Optional

from fastapi import HTTPException
from sqlalchemy import text

from app.services.quote_commit import quote_commit_attempt

log = logging.getLogger("uvicorn.error")


def execute_quote(engine, gw, draft_id, correlation_id, quote_payload, body, queued_id=None):
    customer_identification = quote_payload["customer"]["identification"]
    document_id = quote_payload["document"]["id"]
    seller_id = quote_payload["seller"]
    items = quote_payload["items"]
    def _validate_customer_payload(payload: Dict[str, Any]) -> List[str]:
        missing: List[str] = []

        def has(path: str) -> bool:
            cur: Any = payload
            for key in path.split("."):
                if not isinstance(cur, dict) or key not in cur:
                    return False
                cur = cur[key]
            return cur is not None and cur != "" and cur != []

        if not has("person_type"): missing.append("person_type")
        if not has("id_type"): missing.append("id_type")
        if not has("identification"): missing.append("identification")
        if not has("name"): missing.append("name")

        fr = payload.get("fiscal_responsibilities")
        if not isinstance(fr, list) or not fr or not isinstance(fr[0], dict) or not fr[0].get("code"):
            missing.append("fiscal_responsibilities[0].code")

        if not has("address.address"): missing.append("address.address")
        if not has("address.city.country_code"): missing.append("address.city.country_code")
        if not has("address.city.state_code"): missing.append("address.city.state_code")
        if not has("address.city.city_code"): missing.append("address.city.city_code")

        contacts = payload.get("contacts")
        if not isinstance(contacts, list) or not contacts or not isinstance(contacts[0], dict) or not contacts[0].get("first_name"):
            missing.append("contacts[0].first_name")

        return missing

    def _normalize_detail(http_exc: HTTPException) -> Dict[str, Any]:
        detail = http_exc.detail
        if isinstance(detail, str):
            try:
                detail = json.loads(detail)
            except Exception:
                return {}
        if isinstance(detail, dict) and "detail" in detail and isinstance(detail["detail"], dict):
            detail = detail["detail"]
        return detail if isinstance(detail, dict) else {}

    def _siigo_code(http_exc: HTTPException) -> str:
        return (_normalize_detail(http_exc).get("siigoapi_error_code") or "").lower()

    def _is_customer_missing_siigo(http_exc: HTTPException) -> bool:
        # Siigo: invalid_reference y en el mensaje dice que el customer no existe
        if _siigo_code(http_exc) != "invalid_reference":
            return False
        resp = _normalize_detail(http_exc).get("response") or {}
        errors = resp.get("Errors") or []
        for e in errors:
            msg = (e.get("Message") or "").lower()
            if "customer doesn't exist" in msg or "customer doesnt exist" in msg:
                return True
        return False

    def _is_duplicated_document_siigo(http_exc: HTTPException) -> bool:
        return _siigo_code(http_exc) == "duplicated_document"

    customer_created: Optional[Dict[str, Any]] = None

    with quote_commit_attempt(engine, draft_id, correlation_id, quote_payload, queued_id) as attempt_id:
        started = time.monotonic()
        log.info("quote_commit_started draft_id=%s correlation_id=%s attempt_id=%s item_count=%s",
                 draft_id, correlation_id, attempt_id, len(items))
        try:
            quote_resp = gw.create_quote(
                customer_identification=str(customer_identification),
                branch_office=int(quote_payload["customer"]["branch_office"]),
                document_id=int(document_id),
                seller=int(seller_id),
                items=items,
                date_iso=quote_payload["date"],
            )

        except HTTPException as e:
            log.warning(
                "quote_commit_upstream_rejected draft_id=%s correlation_id=%s status=%s code=%s",
                draft_id,
                correlation_id,
                e.status_code,
                _normalize_detail(e).get("code") or _siigo_code(e),
            )
            if _is_duplicated_document_siigo(e):
                raise HTTPException(status_code=409, detail={"code": "SIIGO_DUPLICATED_DOCUMENT", **_normalize_detail(e)})

            if not _is_customer_missing_siigo(e):
                raise


            # 3) Cliente no existe -> si no autorizaste autocreate, error controlado
            if not body.create_customer_if_missing:
                raise HTTPException(status_code=409, detail={"code": "CLIENT_NOT_FOUND"})

            if not body.customer_create_payload:
                raise HTTPException(
                    status_code=409,
                    detail={"code": "CLIENT_MISSING_FIELDS", "missing_fields": ["customer_create_payload"]},
                )

            missing_fields = _validate_customer_payload(body.customer_create_payload)
            if missing_fields:
                raise HTTPException(
                    status_code=409,
                    detail={"code": "CLIENT_MISSING_FIELDS", "missing_fields": missing_fields},
                )

            log.info(
                "customer_auto_create_attempt",
                extra={"correlation_id": correlation_id, "customer_identification": str(customer_identification)},
            )
            customer_created = gw.create_client(body.customer_create_payload)
            log.info(
                "customer_auto_create_success",
                extra={"correlation_id": correlation_id, "customer_identification": str(customer_identification)},
            )

            time.sleep(1)

            quote_resp = gw.create_quote(
                customer_identification=str(customer_identification),
                branch_office=int(quote_payload["customer"]["branch_office"]),
                document_id=int(document_id),
                seller=int(seller_id),
                items=items,
                date_iso=quote_payload["date"],
            )

        # -------------------------
        # 7) Guardar resultado y marcar COMMITTED
        # -------------------------
        up_client_id = ""
        if isinstance(customer_created, dict):
            up_client_id = str(customer_created.get("id") or "")
        if not up_client_id:
            up_client_id = str((quote_resp.get("customer") or {}).get("id") or "")

        with engine.begin() as conn:
            saved = conn.execute(
                text("""
                    UPDATE drafts
                    SET status='COMMITTED',
                        updated_at=now(),
                        upstream_client_id = COALESCE(NULLIF(upstream_client_id, ''), :up_client_id),
                        warnings_json = jsonb_set(COALESCE(warnings_json, '{}'::jsonb) || CAST(:meta AS jsonb),
                            '{quote_commit,state}', '"SUCCEEDED"'::jsonb)
                    WHERE id=:id
                      AND warnings_json->'quote_commit'->>'attempt_id' = :attempt_id
                      AND warnings_json->'quote_commit'->>'state' = 'IN_PROGRESS'
                    RETURNING id
                """),
                {
                    "id": draft_id,
                    "attempt_id": attempt_id,
                    "up_client_id": up_client_id,
                    "meta": json.dumps({
                        "siigo_quote_response": quote_resp,
                        "correlation_id": correlation_id,
                    }),
                },
            ).first()
            if not saved:
                raise HTTPException(409, detail={"code": "SIIGO_QUOTE_REVIEW_REQUIRED", "outcome_unknown": True, "retryable": False})

    log.info("quote_commit_succeeded draft_id=%s correlation_id=%s attempt_id=%s duration_ms=%s item_count=%s",
             draft_id, correlation_id, attempt_id, int((time.monotonic() - started) * 1000), len(items))
    return {
        "draft_id": draft_id,
        "status": "COMMITTED",
        "upstream_client_id": up_client_id,
        "siigo_quote_response": quote_resp,
    }
