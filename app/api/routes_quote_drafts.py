from fastapi import APIRouter, Request, UploadFile, File, Header, HTTPException, Body, Form
import httpx
from typing import Any, Dict, Optional
import json as _json

router = APIRouter(prefix="/v1/quote-drafts", tags=["quote-drafts"])
ERROR_MESSAGES = {
    "CLIENT_NOT_FOUND": "El cliente no existe en Siigo. Activa 'Crear cliente' y completa los datos.",
    "CLIENT_MISSING_FIELDS": "Faltan datos obligatorios para crear el cliente.",
}


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

    async with httpx.AsyncClient(timeout=90.0) as client:
        resp = await client.request(
            method,
            url,
            headers=headers,
            files=files,
            json=json,
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

    return resp.json(
        
    )



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

    # ✅ soporta payload JSON string
    payload: Optional[str] = Form(None),

    # ✅ soporta campos sueltos (lo actual)
    customer_identification: Optional[str] = Form(None),
    document_id: Optional[int] = Form(None),
    seller: Optional[int] = Form(None),

    branch_office: int = Form(0),
    default_price: float = Form(0),
    dry_run: bool = Form(True),
    create_customer_if_missing: bool = Form(False),
    customer_create_payload: Optional[str] = Form(None),
    x_api_key: str = Header(..., alias="X-API-Key"),
):
    # ---- 0) payload fallback (si viene) ----
    if payload:
        try:
            p = _json.loads(payload)
            if isinstance(p, dict):
                customer_identification = customer_identification or p.get("customer_identification")
                document_id = document_id or p.get("document_id")
                seller = seller or p.get("seller")

                branch_office = p.get("branch_office", branch_office)
                default_price = p.get("default_price", default_price)
                dry_run = p.get("dry_run", dry_run)
                create_customer_if_missing = p.get("create_customer_if_missing", create_customer_if_missing)

                if not customer_create_payload and p.get("customer_create_payload"):
                    customer_create_payload = _json.dumps(p["customer_create_payload"])
        except Exception:
            raise HTTPException(status_code=400, detail="payload must be valid JSON string")

    # ✅ validación dura ANTES de upload/parse (evita tu 400)
    missing = []
    if not customer_identification: missing.append("customer_identification")
    if not document_id: missing.append("document_id")
    if not seller: missing.append("seller")
    if missing:
        raise HTTPException(status_code=422, detail={"code": "MISSING_FIELDS", "missing": missing})

    # 1) upload
    upload_json = await _proxy(
        request,
        "POST",
        "/v1/drafts/upload",
        x_api_key,
        files={"file": (file.filename, await file.read(), file.content_type or "application/octet-stream")},
    )
    draft_id = upload_json["draft_id"]

    # 2) parse
    parse_json = await _proxy(request, "POST", f"/v1/drafts/{draft_id}/parse", x_api_key)

    # 3) submit payload (nunca null)
    submit_payload: Dict[str, Any] = {
        "customer_identification": str(customer_identification),
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

    # 4) commit
    submit_json = await _proxy(
        request,
        "POST",
        f"/v1/drafts/{draft_id}/quote/commit",
        x_api_key,
        json=submit_payload,
    )

    return {"draft_id": draft_id, "upload": upload_json, "parse": parse_json, "submit": submit_json}
