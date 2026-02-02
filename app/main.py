from dotenv import load_dotenv
load_dotenv()

import json
import os
import time, base64, json, threading
import logging
from ulid import ULID
from fastapi import FastAPI, Request
from starlette.responses import JSONResponse
import uuid
from pathlib import Path
from sqlalchemy import text
from sqlalchemy import create_engine
from app.upstream_gateway.factory import get_gateway
from app.db import db_ping
from dotenv import load_dotenv
##from app.api.routes_siigo_catalog import router as siigo_catalog_router
from app.api.routes_quote_drafts import router as quote_drafts_router
from app.services.document_extractor import DocumentExtractor



load_dotenv()
_SIIGO_TOKEN_CACHE = {"token": None, "exp": 0}
_SIIGO_TOKEN_LOCK = threading.Lock()

def _sanitize_text(value: str | None) -> str | None:
    if value is None:
        return None
    # Elimina cualquier byte NUL que rompe PostgreSQL
    return value.replace("\x00", "")

def _jwt_exp(token: str) -> int:
    try:
        payload = token.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        data = json.loads(base64.urlsafe_b64decode(payload))
        return int(data.get("exp") or 0)
    except Exception:
        return 0

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)

app = FastAPI(title="SiigoCotizador", version="0.1.0")
##app.include_router(siigo_catalog_router)
app.include_router(quote_drafts_router)

def new_correlation_id() -> str:
    return f"corr_{ULID()}"

def get_allowed_keys() -> set[str]:
    raw = os.getenv("API_KEYS", "")
    return {k.strip() for k in raw.split(",") if k.strip()}

def error_json(code: str, message: str, corr_id: str, details: dict | None = None):
    payload = {"error": {"code": code, "message": message, "correlation_id": corr_id}}
    if details:
        payload["error"]["details"] = details
    return payload

@app.middleware("http")
async def base_middleware(request: Request, call_next):
    start = time.time()
    corr_id = request.headers.get("X-Correlation-Id") or new_correlation_id()

    # Auth: proteger /v1/* (excepto /health que va libre)
    if request.url.path.startswith("/v1/"):
        provided = request.headers.get("X-API-Key")
        allowed = get_allowed_keys()

        if not provided:
            resp = JSONResponse(
                status_code=401,
                content=error_json("AUTH_MISSING_API_KEY", "Missing X-API-Key", corr_id),
            )
            resp.headers["X-Correlation-Id"] = corr_id
            print(json.dumps({
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "level": "INFO",
                "correlation_id": corr_id,
                "method": request.method,
                "path": request.url.path,
                "status_code": 401,
                "latency_ms": int((time.time() - start) * 1000),
            }, ensure_ascii=False))
            return resp

        if provided not in allowed:
            resp = JSONResponse(
                status_code=403,
                content=error_json("AUTH_INVALID_API_KEY", "Invalid X-API-Key", corr_id),
            )
            resp.headers["X-Correlation-Id"] = corr_id
            print(json.dumps({
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "level": "INFO",
                "correlation_id": corr_id,
                "method": request.method,
                "path": request.url.path,
                "status_code": 403,
                "latency_ms": int((time.time() - start) * 1000),
            }, ensure_ascii=False))
            return resp

    response = await call_next(request)
    response.headers["X-Correlation-Id"] = corr_id

    print(json.dumps({
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "level": "INFO",
        "correlation_id": corr_id,
        "method": request.method,
        "path": request.url.path,
        "status_code": response.status_code,
        "latency_ms": int((time.time() - start) * 1000),
    }, ensure_ascii=False))

    return response

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/v1/db/ping")
def ping_db():
    db_ping()
    return {"db": "ok"}


from fastapi import UploadFile, File, HTTPException

UPLOAD_DIR = os.getenv("UPLOAD_DIR", "./data/uploads")

@app.post("/v1/drafts/upload")
def upload_draft(file: UploadFile = File(...)):
    # 1) Validar
    if not file.filename:
        raise HTTPException(status_code=400, detail="Missing filename")

    # 2) Guardar archivo en disco
    Path(UPLOAD_DIR).mkdir(parents=True, exist_ok=True)
    draft_id = str(uuid.uuid4())
    safe_name = file.filename.replace("/", "_").replace("\\", "_")
    saved_path = os.path.join(UPLOAD_DIR, f"{draft_id}__{safe_name}")

    content = file.file.read()
    with open(saved_path, "wb") as f:
        f.write(content)

    # 3) Guardar draft en DB (stub)
    with engine.connect() as conn:
        conn.execute(
            text("""
                INSERT INTO drafts (id, status, original_filename, stored_path)
                VALUES (:id, :status, :original_filename, :stored_path)
            """),
            {
                "id": draft_id,
                "status": "UPLOADED",
                "original_filename": file.filename,
                "stored_path": saved_path,
            },
        )
        conn.commit()

    return {"draft_id": draft_id, "status": "UPLOADED", "filename": file.filename}


import re
MAX_ITEMS = 200

def _parse_line_basic(line: str):
    """
    Intenta extraer cantidad + unidad al inicio.
    Si no puede, qty=1, uom=UND y description=line.
    """
    raw = (line or "").strip()
    if not raw:
        return None

    m = re.match(r"^\s*(\d+(?:[.,]\d+)?)\s*([a-zA-Z]+)?\s*(?:[-–]|x)?\s*(.*)$", raw)

    qty = 1.0
    uom_raw = None
    desc = raw

    if m:
        qty_str, unit, rest = m.group(1), m.group(2), m.group(3)
        try:
            qty = float(qty_str.replace(",", "."))
        except Exception:
            qty = 1.0
        if unit:
            uom_raw = unit.lower()
        desc = (rest or "").strip() or raw

    uom = "UND"
    if uom_raw in {"m", "mt", "mts"}:
        uom = "M"
    elif uom_raw in {"und", "unds", "unidad", "unidades", "u"}:
        uom = "UND"
    elif uom_raw in {"kg"}:
        uom = "KG"
    elif uom_raw in {"rollo", "rollos"}:
        uom = "ROL"

    return {
        "raw_text": raw,
        "description": desc,
        "quantity": qty,
        "uom": uom,
        "uom_raw": uom_raw,
        "confidence": 0.5,
    }


@app.post("/v1/drafts/{draft_id}/parse")
def parse_draft(draft_id: str, request: Request):
    engine = get_engine()

    with engine.connect() as conn:
        draft = conn.execute(
            text("SELECT id, status, stored_path, original_filename FROM drafts WHERE id = :id"),
            {"id": draft_id},
        ).mappings().first()

    if not draft:
        raise HTTPException(status_code=404, detail="Draft not found")

    if (draft.get("status") or "").upper() == "COMMITTED":
        raise HTTPException(status_code=409, detail="Draft is COMMITTED. Create a new draft to parse.")

    with engine.connect() as conn:
        cnt = conn.execute(
            text("SELECT COUNT(1) FROM draft_items WHERE draft_id=:id"),
            {"id": draft_id},
        ).scalar() or 0
    if int(cnt) > 0:
        raise HTTPException(
            status_code=409,
            detail="Draft already has items. Use PUT /v1/quote-drafts/{draft_id}/items or create a new draft.",
        )

    stored_path = draft["stored_path"]
    original_filename = draft.get("original_filename") or ""

    extractor = DocumentExtractor()
    result = extractor.extract(stored_path, original_filename, content_type=None)

    with engine.begin() as conn:
        conn.execute(text("DELETE FROM draft_items WHERE draft_id = :draft_id"), {"draft_id": draft_id})

        for it in result.items:
            item_warnings = it.warnings or []
            conn.execute(
                text("""
                    INSERT INTO draft_items
                        (id, draft_id, line_index, raw_text, description, quantity, uom, uom_raw, confidence, warnings_json)
                    VALUES
                        (:id, :draft_id, :line_index, :raw_text, :description, :quantity, :uom, :uom_raw, :confidence, CAST(:warnings_json AS jsonb))
                """),
                {
                    "id": str(uuid.uuid4()),
                    "draft_id": draft_id,
                    "line_index": int(it.line_index),
                    "raw_text": _sanitize_text(it.raw_text),
                    "description": _sanitize_text(it.description),
                    "quantity": float(it.quantity),
                    "uom": it.uom,
                    "uom_raw": _sanitize_text(it.uom_raw),
                    "confidence": it.confidence,
                    "warnings_json": json.dumps(item_warnings),
                },
            )

        draft_warning_payload = {
            "global_warnings": result.global_warnings or [],
            "meta": result.meta or {},
        }

        conn.execute(
            text("""
                UPDATE drafts
                SET status = CASE WHEN status='COMMITTED' THEN 'COMMITTED' ELSE :status END,
                    warnings_json = CAST(:warnings_json AS jsonb),
                    updated_at = NOW()
                WHERE id = :id
            """),
            {
                "id": draft_id,
                "status": "PARSED",
                "warnings_json": json.dumps(draft_warning_payload),
            },
        )

    return {
        "draft_id": draft_id,
        "status": "PARSED",
        "items_created": len(result.items),
        "warnings": result.global_warnings or [],
        "meta": result.meta or {},
    }


@app.get("/v1/drafts/{draft_id}")
def get_draft(draft_id: str, request: Request):
    engine = get_engine()

    with engine.connect() as conn:
        draft = conn.execute(
            text("SELECT * FROM drafts WHERE id = :id"),
            {"id": draft_id},
        ).mappings().first()

        if not draft:
            raise HTTPException(status_code=404, detail="Draft not found")

        items = conn.execute(
            text("""
                SELECT line_index, raw_text, description, quantity, uom, uom_raw, confidence, warnings_json
                FROM draft_items
                WHERE draft_id = :id
                ORDER BY line_index ASC
                LIMIT 200
            """),
            {"id": draft_id},
        ).mappings().all()

    return {"draft": dict(draft), "items": [dict(x) for x in items]}


# --- DB engine helper (needed by /parse) ---
from sqlalchemy.engine import Engine

_ENGINE: Engine | None = None

def get_engine() -> Engine:
    global _ENGINE
    if _ENGINE is None:
        db_url = os.getenv("DATABASE_URL")
        if not db_url:
            raise RuntimeError("DATABASE_URL not set")
        _ENGINE = create_engine(db_url, pool_pre_ping=True)
    return _ENGINE


from datetime import date
from fastapi import Body

def _engine_or_get_engine():
    fn = globals().get("get_engine")
    if callable(fn):
        return fn()
    eng = globals().get("engine")
    if eng is None:
        raise RuntimeError("DB engine not configured (engine/get_engine missing)")
    return eng


@app.post("/v1/drafts/{draft_id}/quote/preview")
def quote_preview(draft_id: str, body: dict = Body(default=None)):
    """
    Preview del payload para Siigo SIN llamar a Siigo.

    Regla:
    - NO autocompleta customer_identification: debe venir desde UI (popup si falta).
    - document_id y seller deben venir desde UI (dropdowns).
    - price sale de default_price (para pruebas). En real debe ser > 0.
    """
    body = body or {}
    eng = _engine_or_get_engine()

    # 1) Cargar draft (solo columnas que sí existen)
    with eng.connect() as conn:
        draft = conn.execute(
            text("SELECT id, status, warnings_json FROM drafts WHERE id = :id"),
            {"id": draft_id},
        ).mappings().first()

        if not draft:
            raise HTTPException(status_code=404, detail="Draft not found")

        items = conn.execute(
            text("""
                SELECT line_index, description, quantity, uom, raw_text, confidence
                FROM draft_items
                WHERE draft_id = :draft_id
                ORDER BY line_index ASC
            """),
            {"draft_id": draft_id},
        ).mappings().all()

    # 2) Inputs que DEBEN venir del UI
    customer_identification = (body.get("customer_identification") or "").strip()
    document_id = int(body.get("document_id") or 0)
    seller_id = int(body.get("seller") or 0)
    customer_branch_office = int(body.get("customer_branch_office") or 0)

    default_item_code = str(body.get("item_code", "2543"))
    default_price = float(body.get("default_price") or 0)

    warnings = []

    if not customer_identification:
        warnings.append({"code": "MISSING_CUSTOMER_IDENTIFICATION", "message": "Falta NIT/CC del cliente. El UI debe pedirlo (popup)."})
    if not document_id:
        warnings.append({"code": "MISSING_DOCUMENT_ID", "message": "Falta document_id. El UI debe enviarlo desde dropdown (documentos Siigo)."})
    if not seller_id:
        warnings.append({"code": "MISSING_SELLER_ID", "message": "Falta seller. El UI debe enviarlo desde dropdown."})
    if default_price <= 0:
        warnings.append({"code": "PRICE_IS_ZERO", "message": "items.price está en 0. Para enviar real a Siigo necesitas price > 0."})

    # 3) Construir items
    siigo_items = []
    for it in items:
        desc = (it.get("description") or "").strip() or (it.get("raw_text") or "").strip()
        qty = it.get("quantity") or 0
        try:
            qty = float(qty)
        except Exception:
            qty = 0

        if not desc:
            warnings.append({"code": "EMPTY_ITEM_DESCRIPTION", "message": f"Línea {it.get('line_index')} sin descripción."})

        siigo_items.append({
            "code": default_item_code,
            "description": desc,
            "quantity": qty,
            "price": default_price,
        })

    payload = {
        "document": {"id": document_id},
        "date": str(date.today()),
        "customer": {
            "identification": customer_identification,
            "branch_office": customer_branch_office,
        },
        "seller": seller_id,
        "items": siigo_items,
    }

    return {
        "draft_id": draft_id,
        "draft_status": draft.get("status"),
        "siigo_quote_payload": payload,
        "warnings": warnings,
    }


# =========================
# PASO 7 — Draft items edit + Quote commit (dry-run)
# =========================
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field

import httpx


class DraftItemIn(BaseModel):
    line_index: int = Field(..., ge=0)
    description: str = Field(..., min_length=1)
    quantity: float = Field(..., gt=0)
    uom: str = Field("UND", min_length=1)
    raw_text: Optional[str] = None
    confidence: Optional[float] = None


class DraftItemsReplaceRequest(BaseModel):
    items: List[DraftItemIn]


@app.put("/v1/drafts/{draft_id}/items")
def replace_draft_items(draft_id: str, payload: DraftItemsReplaceRequest):
    engine = get_engine()

    with engine.begin() as conn:
        draft_row = conn.execute(
            text("SELECT id, status, warnings_json FROM drafts WHERE id = :id"),
            {"id": draft_id},
        ).mappings().first()

        if not draft_row:
            raise HTTPException(status_code=404, detail="Draft not found")

        w = draft_row.get("warnings_json") or {}
        has_quote = isinstance(w, dict) and w.get("siigo_quote_response")

        if draft_row.get("status") == "COMMITTED" or has_quote:
            raise HTTPException(status_code=409, detail="Draft is COMMITTED. Create a new draft to edit items.")

        conn.execute(text("DELETE FROM draft_items WHERE draft_id = :id"), {"id": draft_id})

        for it in payload.items:
            conn.execute(
                text("""
                    INSERT INTO draft_items
                    (id, draft_id, line_index, raw_text, description, quantity, uom, uom_raw, confidence, warnings_json)
                    VALUES
                    (:id, :draft_id, :line_index, :raw_text, :description, :quantity, :uom, :uom_raw, :confidence, NULL)
                """),
                {
                    "id": str(uuid.uuid4()),
                    "draft_id": draft_id,
                    "line_index": int(it.line_index),
                    "raw_text": it.raw_text,
                    "description": it.description,
                    "quantity": float(it.quantity),
                    "uom": it.uom,
                    "uom_raw": None,
                    "confidence": float(it.confidence) if it.confidence is not None else None,
                },
            )

        conn.execute(
            text("""
                UPDATE drafts
                SET status = CASE WHEN status='COMMITTED' THEN 'COMMITTED' ELSE 'PARSED' END,
                    updated_at=now()
                WHERE id=:id
            """),
            {"id": draft_id},
        )

    return {"draft_id": draft_id, "status": "PARSED", "items_saved": len(payload.items)}



class QuoteCommitRequest(BaseModel):
    customer_identification: Optional[str] = None
    document_id: int | None = None
    seller: Optional[int] = None
    branch_office: int = 0
    default_price: float = 0

    dry_run: bool = True
    create_customer_if_missing: bool = False
    customer_create_payload: Optional[Dict[str, Any]] = None


# ✅✅✅ CAMBIO CLAVE: ahora acepta JSON o FORM
@app.post("/v1/drafts/{draft_id}/quote/commit")
async def commit_quote(draft_id: str, request: Request):
    engine = get_engine()
    log = logging.getLogger("uvicorn.error")
    correlation_id = request.headers.get("x-correlation-id") or request.headers.get("X-Correlation-Id") or ""

    # -------------------------
    # 0) Leer body (JSON o FORM) y validar
    # -------------------------
    content_type = (request.headers.get("content-type") or "").lower()

    try:
        if "application/json" in content_type:
            raw = await request.json()

        elif "multipart/form-data" in content_type or "application/x-www-form-urlencoded" in content_type:
            form = await request.form()
            raw = dict(form)

            # booleans
            for k in ["dry_run", "create_customer_if_missing"]:
                if k in raw and isinstance(raw[k], str):
                    raw[k] = raw[k].lower() in ("1", "true", "yes", "y", "on")

            # ints
            for k in ["document_id", "seller", "branch_office"]:
                if k in raw and isinstance(raw[k], str) and raw[k].strip():
                    try:
                        raw[k] = int(raw[k])
                    except Exception:
                        pass

            # floats
            if "default_price" in raw and isinstance(raw["default_price"], str) and raw["default_price"].strip():
                try:
                    raw["default_price"] = float(raw["default_price"])
                except Exception:
                    pass

            # json string -> dict
            if "customer_create_payload" in raw and raw["customer_create_payload"]:
                if isinstance(raw["customer_create_payload"], str):
                    try:
                        raw["customer_create_payload"] = json.loads(raw["customer_create_payload"])
                    except Exception:
                        pass
        else:
            form = await request.form()
            raw = dict(form)

    except Exception as e:
        raise HTTPException(status_code=400, detail={"code": "INVALID_BODY", "message": str(e)[:200]})

    try:
        body = QuoteCommitRequest.model_validate(raw)
    except Exception as e:
        errs = None
        if hasattr(e, "errors"):
            try:
                errs = e.errors()
            except Exception:
                errs = None
        raise HTTPException(
            status_code=400,
            detail={"code": "COMMIT_VALIDATION_FAILED", "errors": errs or str(e)[:300], "received_keys": list(raw.keys())},
        )

    # -------------------------
    # 1) Leer draft + items (IMPORTANTE: traer client_document_number)
    # -------------------------
    with engine.connect() as conn:
        draft = conn.execute(
            text("""
                SELECT id, status, warnings_json
                FROM drafts
                WHERE id=:id
            """),
            {"id": draft_id},
        ).mappings().first()

        if not draft:
            raise HTTPException(status_code=404, detail="Draft not found")

        rows = conn.execute(
            text("""
                SELECT line_index, raw_text, description, quantity, uom
                FROM draft_items
                WHERE draft_id=:id
                ORDER BY line_index
            """),
            {"id": draft_id},
        ).mappings().all()

    if not rows:
        raise HTTPException(status_code=400, detail="Draft has no items. Parse first or PUT items.")

    # -------------------------
    # 2) Completar defaults SI NO VINIERON del UI
    # -------------------------
    customer_identification = (body.customer_identification or "").strip()
    if not customer_identification:
        raise HTTPException(status_code=400, detail={"code":"MISSING_CUSTOMER_IDENTIFICATION"})

    document_id = int(body.document_id or os.getenv("SIIGO_QUOTE_DOCUMENT_ID", "0") or "0")
    seller_id = int(body.seller or os.getenv("SIIGO_SELLER_ID", "0") or "0")

    missing = []
    if not customer_identification:
        missing.append("customer_identification (or drafts.client_document_number)")
    if not document_id:
        missing.append("document_id (or SIIGO_QUOTE_DOCUMENT_ID)")
    if not seller_id:
        missing.append("seller (or SIIGO_SELLER_ID)")

    if missing:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "MISSING_REQUIRED_FIELDS",
                "missing": missing,
                "hint": "Envía estos campos desde UI o configúralos en drafts/ENV para que el backend complete.",
            },
        )

    # -------------------------
    # 3) Idempotencia (si ya está COMMITTED y no es dry_run)
    # -------------------------
    if (
        (draft.get("status") == "COMMITTED"
         or (isinstance(draft.get("warnings_json"), dict) and draft.get("warnings_json").get("siigo_quote_response")))
        and not body.dry_run
    ):
        w = draft.get("warnings_json") or {}
        siigo_saved = w.get("siigo_quote_response")
        return {
            "draft_id": draft_id,
            "status": "COMMITTED",
            "siigo_quote_response": siigo_saved,
            "note": "Already committed (idempotent). Not calling Siigo again.",
        }

    # -------------------------
    # 4) Armar items (code fijo 2543)
    # -------------------------
    items: List[Dict[str, Any]] = []
    for r in rows:
        items.append(
            {
                "code": "2543",
                "description": r["description"],
                "quantity": float(r["quantity"]),
                "price": float(body.default_price),
            }
        )
    
    if document_id <= 0:
        raise HTTPException(status_code=409, detail={"code": "MISSING_DOCUMENT_ID"})

    quote_payload = {
        "document": {"id": int(document_id)},
        "date": str(date.today()),
        "customer": {"identification": str(customer_identification), "branch_office": int(body.branch_office)},
        "seller": int(seller_id),
        "items": items,
    }

    # -------------------------
    # 5) Dry-run
    # -------------------------
    if body.dry_run:
        return {
            "draft_id": draft_id,
            "draft_status": draft["status"],
            "dry_run": True,
            "siigo_quote_payload": quote_payload,
            "notes": {
                "defaults_used": {
                    "customer_identification": str(customer_identification),
                    "document_id": int(document_id),
                    "seller": int(seller_id),
                }
            },
        }

    # -------------------------
    # 6) Llamada real a Siigo (+ autocreate si aplica)
    # -------------------------
    gw = get_gateway()
    customer_created: Optional[Dict[str, Any]] = None

    # helper functions (los mismos tuyos)
    def _is_customer_missing_siigo(http_exc: HTTPException) -> bool:
        detail = http_exc.detail
        if isinstance(detail, str):
            try:
                detail = json.loads(detail)
            except Exception:
                return False

        if isinstance(detail, dict) and "detail" in detail and isinstance(detail["detail"], dict):
            detail = detail["detail"]

        if not isinstance(detail, dict):
            return False

        if (detail.get("siigoapi_error_code") or "").lower() != "invalid_reference":
            return False

        resp = detail.get("response") or {}
        errors = resp.get("Errors") or []
        for e in errors:
            msg = (e.get("Message") or "").lower()
            if "customer doesn't exist" in msg:
                return True
        return False

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

    try:
        quote_resp = gw.create_quote(
            customer_identification=str(customer_identification),
            branch_office=int(body.branch_office),
            document_id=int(document_id),
            seller=int(seller_id),
            items=items,
            date_iso=str(date.today()),
        )

    except HTTPException as e:
        if not _is_customer_missing_siigo(e):
            raise

        if not body.create_customer_if_missing:
            raise HTTPException(status_code=409, detail={"code": "CLIENT_NOT_FOUND"})

        if not body.customer_create_payload:
            raise HTTPException(status_code=409, detail={"code": "CLIENT_MISSING_FIELDS", "missing_fields": ["customer_create_payload"]})

        missing_fields = _validate_customer_payload(body.customer_create_payload)
        if missing_fields:
            raise HTTPException(status_code=409, detail={"code": "CLIENT_MISSING_FIELDS", "missing_fields": missing_fields})

        log.info("customer_auto_create_attempt", extra={"correlation_id": correlation_id, "customer_identification": str(customer_identification)})
        customer_created = gw.create_client(body.customer_create_payload)
        log.info("customer_auto_create_success", extra={"correlation_id": correlation_id, "customer_identification": str(customer_identification)})

        time.sleep(1)

        quote_resp = gw.create_quote(
            customer_identification=str(customer_identification),
            branch_office=int(body.branch_office),
            document_id=int(document_id),
            seller=int(seller_id),
            items=items,
            date_iso=str(date.today()),
        )

    up_client_id = ""
    if isinstance(customer_created, dict):
        up_client_id = str(customer_created.get("id") or "")
    if not up_client_id:
        up_client_id = str((quote_resp.get("customer") or {}).get("id") or "")

    with engine.begin() as conn:
        conn.execute(
            text("""
                UPDATE drafts
                SET status='COMMITTED',
                    updated_at=now(),
                    upstream_client_id = COALESCE(NULLIF(upstream_client_id, ''), :up_client_id),
                    warnings_json = COALESCE(warnings_json, '{}'::jsonb) || CAST(:meta AS jsonb)
                WHERE id=:id
            """),
            {
                "id": draft_id,
                "up_client_id": up_client_id,
                "meta": json.dumps({"siigo_quote_response": quote_resp, "correlation_id": correlation_id}),
            },
        )

    return {
        "draft_id": draft_id,
        "status": "COMMITTED",
        "upstream_client_id": up_client_id,
        "siigo_quote_response": quote_resp,
    }
