from fastapi import APIRouter, Request, UploadFile, File, Header, HTTPException, Body, Form
import httpx
from typing import Any, Dict, Optional
import json as _json
import logging
import os

from app.services.local_fallback_parser import fallback_enabled, fallback_txt_lines_to_extraction

logger = logging.getLogger("uvicorn.error")

router = APIRouter(prefix="/v1/quote-drafts", tags=["quote-drafts"])
ERROR_MESSAGES = {
    "CLIENT_NOT_FOUND": "El cliente no existe en Siigo. Activa 'Crear cliente' y completa los datos.",
    "CLIENT_MISSING_FIELDS": "Faltan datos obligatorios para crear el cliente.",
}


def _timeout_for_path(path: str) -> httpx.Timeout:
    # Ajusta según tu realidad:
    # - upload: rápido
    # - parse: puede tardar ~90s+ cuando OpenAI se pone lento
    # - match: puede tardar según catálogo/búsqueda
    # - commit: medio
    if path.endswith("/parse"):
        return httpx.Timeout(180.0, connect=10.0, read=180.0, write=30.0, pool=10.0)
    if path.endswith("/upload"):
        return httpx.Timeout(60.0, connect=10.0, read=60.0, write=30.0, pool=10.0)
    if path.endswith("/match"):
        return httpx.Timeout(90.0, connect=10.0, read=90.0, write=30.0, pool=10.0)
    return httpx.Timeout(90.0, connect=10.0, read=90.0, write=30.0, pool=10.0)


def _base_url(request: Request) -> str:
    return str(request.base_url).rstrip("/")


async def _proxy(
    request: Request,
    method: str,
    path: str,
    x_api_key: str,
    files=None,
    json=None,
):
    url = f"{_base_url(request)}{path}"

    headers = {"X-API-Key": x_api_key}
    corr = request.headers.get("X-Correlation-Id")
    if corr:
        headers["X-Correlation-Id"] = corr

    timeout = _timeout_for_path(path)

    async with httpx.AsyncClient(timeout=timeout) as client:
        try:
            resp = await client.request(
                method,
                url,
                headers=headers,
                files=files,
                json=json,
            )
        except httpx.TimeoutException:
            raise HTTPException(
                status_code=504,
                detail={
                    "code": "UPSTREAM_TIMEOUT",
                    "message": "El backend tardó demasiado procesando (parse lento). Intenta de nuevo.",
                    "debug": {
                        "path": path,
                        "timeout_s": getattr(timeout, "read", timeout) if timeout is not None else "unknown",
                    },
                },
            )
        except httpx.RequestError as e:
            raise HTTPException(
                status_code=502,
                detail={
                    "code": "UPSTREAM_REQUEST_ERROR",
                    "message": "Error de red llamando al backend.",
                    "debug": {"path": path, "error": str(e)[:200]},
                },
            )

    if resp.status_code >= 400:
        try:
            data = resp.json()
        except Exception:
            data = resp.text

        # ✅ si viene {"detail": ...} lo “desenvolvemos” para que el UI reciba limpio
        if isinstance(data, dict) and set(data.keys()) == {"detail"}:
            data = data["detail"]

        # ✅ UX: si viene nuestro error estándar {code: "..."} agregar message friendly
        if isinstance(data, dict) and isinstance(data.get("code"), str):
            code = data["code"]
            if code in ERROR_MESSAGES:
                data = {**data, "message": ERROR_MESSAGES[code]}

        raise HTTPException(status_code=resp.status_code, detail=data)

    return resp.json()


@router.get("/health")
async def health():
    return {"ok": True}


@router.post("/upload")
async def upload_quote_draft(
    request: Request,
    file: UploadFile = File(...),
    x_api_key: str = Header(..., alias="X-API-Key"),
):
    return await _proxy(
        request,
        "POST",
        "/v1/drafts/upload",
        x_api_key,
        files={"file": (file.filename, await file.read(), file.content_type or "application/octet-stream")},
    )


@router.get("/{draft_id}")
async def get_quote_draft(
    request: Request,
    draft_id: str,
    x_api_key: str = Header(..., alias="X-API-Key"),
):
    return await _proxy(request, "GET", f"/v1/drafts/{draft_id}", x_api_key)


@router.post("/{draft_id}/parse")
async def parse_quote_draft(
    request: Request,
    draft_id: str,
    x_api_key: str = Header(..., alias="X-API-Key"),
):
    return await _proxy(request, "POST", f"/v1/drafts/{draft_id}/parse", x_api_key)


@router.put("/{draft_id}/items")
async def replace_quote_draft_items(
    request: Request,
    draft_id: str,
    payload: Dict[str, Any] = Body(...),
    x_api_key: str = Header(..., alias="X-API-Key"),
):
    return await _proxy(request, "PUT", f"/v1/drafts/{draft_id}/items", x_api_key, json=payload)


@router.post("/{draft_id}/submit")
async def submit_quote_draft(
    request: Request,
    draft_id: str,
    payload: Dict[str, Any] = Body(...),
    x_api_key: str = Header(..., alias="X-API-Key"),
):
    return await _proxy(request, "POST", f"/v1/drafts/{draft_id}/quote/commit", x_api_key, json=payload)


@router.post("/process")
async def process_quote_draft(
    request: Request,
    file: UploadFile = File(...),
    customer_identification: str = Form(...),
    document_id: int = Form(...),
    seller: int = Form(...),
    branch_office: int = Form(0),
    default_price: float = Form(0),
    dry_run: bool = Form(True),
    create_customer_if_missing: bool = Form(False),
    customer_create_payload: Optional[str] = Form(None),
    x_api_key: str = Header(..., alias="X-API-Key"),
    x_org_id: Optional[str] = Header(None, alias="X-Org-Id"),  # ✅ NUEVO
):
    # ✅ Lee el archivo UNA sola vez (y valida que no esté vacío)
    file_bytes = await file.read()
    file_len = len(file_bytes or b"")
    ct = file.content_type or "application/octet-stream"

    raw_text = ""
    try:
        raw_text = file_bytes.decode("utf-8", errors="ignore")
    except Exception:
        raw_text = ""

    if file_len == 0:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "EMPTY_FILE",
                "message": "El archivo llegó vacío al backend. Revisa el envío desde la UI/Edge Function.",
                "debug": {"filename": file.filename, "content_type": ct, "bytes": file_len},
            },
        )

    # 1) upload
    upload_json = await _proxy(
        request,
        "POST",
        "/v1/drafts/upload",
        x_api_key,
        files={"file": (file.filename, file_bytes, ct)},
    )
    draft_id = upload_json["draft_id"]

    # 2) parse
    parse_json = await _proxy(request, "POST", f"/v1/drafts/{draft_id}/parse", x_api_key)
    items_created = int(parse_json.get("items_created") or 0)

    if items_created <= 0:
        # 2A) intentar fallback local con el mismo texto
        if fallback_enabled() and raw_text.strip():
            fb = fallback_txt_lines_to_extraction(raw_text)

            if fb.items:
                items_payload = {
                    "items": [
                        {
                            "line_index": i,
                            "raw_text": it.raw_text,
                            "description": it.description,
                            "quantity": it.quantity,
                            "uom": it.uom.value,  # enum -> "UND", "M", etc
                            "uom_raw": it.uom_raw,
                            "confidence": it.confidence,
                            "warnings": it.warnings,
                        }
                        for i, it in enumerate(fb.items)
                    ]
                }

                # 2B) persistir ítems en el draft para que el commit funcione
                await _proxy(
                    request,
                    "PUT",
                    f"/v1/drafts/{draft_id}/items",
                    x_api_key,
                    json=items_payload,
                )

                # 2C) “simular” un parse exitoso para respuesta/telemetría
                parse_json = {
                    "items_created": len(fb.items),
                    "warnings": ["FALLBACK_LOCAL_USED", "OPENAI_EMPTY_ITEMS"],
                    "meta": {"source_type": "txt", "extractor": "local", "model": "local-fallback-v1"},
                    "items": items_payload["items"],
                }
            else:
                logger.warning("NO_ITEMS_EXTRACTED draft_id=%s filename=%s bytes=%s", draft_id, file.filename, file_len)
                raise HTTPException(
                    status_code=400,
                    detail={
                        "code": "NO_ITEMS_EXTRACTED",
                        "message": "No se detectaron ítems con cantidad en el archivo/texto.",
                        "debug": {"draft_id": draft_id, "filename": file.filename, "content_type": ct, "bytes": file_len},
                        "parse": parse_json,
                    },
                )
        else:
            logger.warning("NO_ITEMS_EXTRACTED draft_id=%s filename=%s bytes=%s", draft_id, file.filename, file_len)
            raise HTTPException(
                status_code=400,
                detail={
                    "code": "NO_ITEMS_EXTRACTED",
                    "message": "No se detectaron ítems con cantidad en el archivo/texto.",
                    "debug": {"draft_id": draft_id, "filename": file.filename, "content_type": ct, "bytes": file_len},
                    "parse": parse_json,
                },
            )

    # ✅ 2.5) MATCH (apply=true) ANTES del commit (solo si ENABLE_MATCHING=1)
    match_json = None
    try:
        matching_enabled = str(os.getenv("ENABLE_MATCHING", "0")).lower() in ("1", "true", "yes", "on")
        if matching_enabled:
            if not x_org_id:
                # No rompemos: solo warning
                if isinstance(parse_json, dict):
                    parse_json.setdefault("warnings", [])
                    if "MISSING_X_ORG_ID_FOR_MATCH" not in parse_json["warnings"]:
                        parse_json["warnings"].append("MISSING_X_ORG_ID_FOR_MATCH")
                logger.warning("MATCHING_ENABLED_BUT_NO_X_ORG_ID draft_id=%s", draft_id)
            else:
                match_json = await _proxy(
                    request,
                    "POST",
                    f"/v1/drafts/{draft_id}/match",
                    x_api_key,
                    json={"org_id": x_org_id, "provider": "siigo", "limit": 5, "apply": True},
                )
    except Exception as e:
        logger.exception("MATCH_STEP_FAILED draft_id=%s err=%s", draft_id, str(e))
        if isinstance(parse_json, dict):
            parse_json.setdefault("warnings", [])
            if "MATCH_STEP_FAILED" not in parse_json["warnings"]:
                parse_json["warnings"].append("MATCH_STEP_FAILED")

    # 3) build submit payload
    submit_payload: Dict[str, Any] = {
        "customer_identification": customer_identification,
        "document_id": int(document_id),
        "seller": int(seller),
        "branch_office": int(branch_office),
        "default_price": float(default_price),
        "dry_run": bool(dry_run),
        "create_customer_if_missing": bool(create_customer_if_missing),
    }

    if customer_create_payload:
        try:
            submit_payload["customer_create_payload"] = _json.loads(customer_create_payload)
        except Exception:
            raise HTTPException(status_code=400, detail="customer_create_payload must be valid JSON string")

    # 4) submit
    submit_json = await _proxy(
        request,
        "POST",
        f"/v1/drafts/{draft_id}/quote/commit",
        x_api_key,
        json=submit_payload,
    )

    return {
        "draft_id": draft_id,
        "upload": upload_json,
        "parse": parse_json,
        "match": match_json,  # ✅ NUEVO
        "submit": submit_json,
    }
