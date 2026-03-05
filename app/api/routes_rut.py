"""
RUT extraction and customer lookup endpoints.

POST /v1/rut/extract     — Extract fields from a DIAN RUT PDF
GET  /v1/customers/{id}/lookup — Check if customer exists in Siigo
"""
from __future__ import annotations

import os
import tempfile
from typing import Optional

from fastapi import APIRouter, File, HTTPException, UploadFile
from pydantic import BaseModel

from app.services.rut_extractor import extract_rut
from app.upstream_gateway.factory import get_gateway

router = APIRouter(prefix="/v1", tags=["rut"])

MAX_FILE_SIZE = 5 * 1024 * 1024  # 5 MB


# ---------------------------------------------------------------------------
# POST /v1/rut/extract
# ---------------------------------------------------------------------------

class RutExtractResponse(BaseModel):
    nit: Optional[str] = None
    dv: Optional[str] = None
    razon_social: Optional[str] = None
    nombre_comercial: Optional[str] = None
    direccion: Optional[str] = None
    municipio: Optional[str] = None
    correo: Optional[str] = None
    telefono: Optional[str] = None
    warnings: list[str] = []


@router.post("/rut/extract", response_model=RutExtractResponse)
async def rut_extract(file: UploadFile = File(...)):
    # Validate file type
    filename = (file.filename or "").lower()
    content_type = (file.content_type or "").lower()
    if not filename.endswith(".pdf") and "pdf" not in content_type:
        raise HTTPException(
            status_code=400,
            detail={"code": "INVALID_FILE_TYPE", "message": "Solo se aceptan archivos PDF del RUT"},
        )

    # Read content and validate size
    content = await file.read()
    if len(content) > MAX_FILE_SIZE:
        raise HTTPException(
            status_code=400,
            detail={"code": "FILE_TOO_LARGE", "message": "El archivo excede 5MB"},
        )

    # Save to temp file, extract, then clean up
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            tmp.write(content)
            tmp_path = tmp.name

        data = extract_rut(tmp_path)
    except Exception as e:
        raise HTTPException(
            status_code=422,
            detail={"code": "RUT_EXTRACTION_FAILED", "message": f"No se pudo leer el PDF: {str(e)[:200]}"},
        )
    finally:
        if tmp_path:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

    if not data.nit:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "NIT_NOT_FOUND",
                "message": "No se encontró el NIT. Verifique que es un RUT válido de la DIAN.",
                "warnings": data.warnings,
            },
        )

    return RutExtractResponse(
        nit=data.nit,
        dv=data.dv,
        razon_social=data.razon_social,
        nombre_comercial=data.nombre_comercial,
        direccion=data.direccion,
        municipio=data.municipio,
        correo=data.correo,
        telefono=data.telefono,
        warnings=data.warnings,
    )


# ---------------------------------------------------------------------------
# GET /v1/customers/{identification}/lookup
# ---------------------------------------------------------------------------

class CustomerLookupResponse(BaseModel):
    exists: bool
    customer: Optional[dict] = None


@router.get("/customers/{identification}/lookup", response_model=CustomerLookupResponse)
def customer_lookup(identification: str):
    identification = identification.strip()
    if not identification:
        raise HTTPException(status_code=400, detail={"code": "MISSING_IDENTIFICATION"})

    gw = get_gateway()
    try:
        customer = gw.find_client(identification)
    except Exception:
        # Silently fail — let the normal commit flow handle it
        return CustomerLookupResponse(exists=False)

    if customer:
        return CustomerLookupResponse(exists=True, customer=customer)

    return CustomerLookupResponse(exists=False)
