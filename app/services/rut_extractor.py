"""
RUT PDF extractor — reads DIAN RUT PDFs and extracts key fields.

Strategy A: AcroForm fields (fillable PDFs from muisca.dian.gov.co)
Strategy B: Full-text extraction + regex patterns by casilla number

Zero OpenAI calls.
"""
from __future__ import annotations

import re
from typing import Optional, List

from pydantic import BaseModel


class RutData(BaseModel):
    nit: Optional[str] = None
    dv: Optional[str] = None
    razon_social: Optional[str] = None
    nombre_comercial: Optional[str] = None
    direccion: Optional[str] = None
    municipio: Optional[str] = None
    correo: Optional[str] = None
    telefono: Optional[str] = None
    warnings: List[str] = []


# ---------------------------------------------------------------------------
# Normalizers
# ---------------------------------------------------------------------------

def _digits_only(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"[^\d]", "", value)


def _clean_str(value: str | None) -> str:
    if not value:
        return ""
    return " ".join(value.split()).strip()


# ---------------------------------------------------------------------------
# Strategy A: AcroForm fields
# ---------------------------------------------------------------------------

_FORM_FIELD_MAP = {
    # Common AcroForm field-name patterns in DIAN RUTs
    "nit": "nit",
    "dv": "dv",
    "5": "nit",
    "6": "dv",
    "35": "razon_social",
    "36": "nombre_comercial",
    "40": "direccion",
    "42": "telefono",
    "44": "correo",
}


def _try_acroform(reader) -> RutData | None:
    """Try to extract from AcroForm fields (fillable PDFs)."""
    try:
        fields = reader.get_form_text_fields()
    except Exception:
        return None

    if not fields:
        return None

    # Normalize keys: lowercase, strip whitespace
    norm: dict[str, str] = {}
    for k, v in fields.items():
        if v is None:
            continue
        key = (k or "").strip().lower()
        val = str(v).strip()
        if not val:
            continue
        norm[key] = val

    if not norm:
        return None

    data = RutData()
    warnings: list[str] = []

    # Try to map known field names
    for form_key, attr in _FORM_FIELD_MAP.items():
        # exact match
        if form_key in norm:
            _set_field(data, attr, norm[form_key])
            continue
        # partial match: field name contains the key
        for nk, nv in norm.items():
            if form_key in nk:
                _set_field(data, attr, nv)
                break

    # Also try matching casilla patterns like "casilla5", "cas_5", "c5"
    for nk, nv in norm.items():
        m = re.search(r"(?:casilla|cas|c)\s*_?\s*(\d+)", nk)
        if m:
            casilla = m.group(1)
            if casilla in _FORM_FIELD_MAP:
                _set_field(data, _FORM_FIELD_MAP[casilla], nv)

    if not data.nit:
        return None

    _normalize_fields(data)
    data.warnings = warnings
    return data


def _set_field(data: RutData, attr: str, value: str):
    current = getattr(data, attr, None)
    if not current:
        setattr(data, attr, value)


# ---------------------------------------------------------------------------
# Strategy B: Full text + regex
# ---------------------------------------------------------------------------

# Regex patterns for casilla-based extraction
_CASILLA_PATTERNS = {
    "nit": [
        # "NIT" or "5." followed by digits
        r"(?:NIT|N\.I\.T\.?)\s*[:\.]?\s*(\d[\d.]+\d)",
        r"(?:^|\s)5\s*[.\-)\s]+\s*(\d[\d.]+\d)",
        # Standalone large number (9+ digits) that looks like a NIT
        r"\b(\d{6,12})\b",
    ],
    "dv": [
        r"(?:DV|D\.V\.?)\s*[:\.]?\s*(\d)",
        r"(?:^|\s)6\s*[.\-)\s]+\s*(\d)\b",
    ],
    "razon_social": [
        r"(?:Raz[oó]n\s+[Ss]ocial|35)\s*[.\-:)\s]+\s*(.+?)(?:\s{3,}|\n|$)",
    ],
    "nombre_comercial": [
        r"(?:Nombre\s+[Cc]omercial|36)\s*[.\-:)\s]+\s*(.+?)(?:\s{3,}|\n|$)",
    ],
    "direccion": [
        r"(?:Direcci[oó]n\s+[Pp]rincipal|40)\s*[.\-:)\s]+\s*(.+?)(?:\s{3,}|\n|$)",
    ],
    "telefono": [
        r"(?:Tel[eé]fono\s*1?|42)\s*[.\-:)\s]+\s*(\d[\d\s\-]+\d)",
    ],
    "correo": [
        r"(?:Correo\s+[Ee]lectr[oó]nico|44)\s*[.\-:)\s]+\s*([^\s@]+@[^\s@]+\.[^\s@]+)",
        # Generic email near the text
        r"\b([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)\b",
    ],
}


def _try_text_extraction(reader) -> RutData | None:
    """Fallback: extract text from first page(s) and apply regex."""
    text_parts: list[str] = []
    try:
        for page in reader.pages[:3]:
            t = page.extract_text()
            if t:
                text_parts.append(t)
    except Exception:
        return None

    if not text_parts:
        return None

    full_text = "\n".join(text_parts)
    data = RutData()
    warnings: list[str] = []

    for field, patterns in _CASILLA_PATTERNS.items():
        for pattern in patterns:
            m = re.search(pattern, full_text, re.IGNORECASE | re.MULTILINE)
            if m:
                value = m.group(1).strip()
                if value:
                    _set_field(data, field, value)
                    break

    if not data.nit:
        return None

    _normalize_fields(data)
    data.warnings = warnings
    return data


# ---------------------------------------------------------------------------
# Normalization
# ---------------------------------------------------------------------------

def _normalize_fields(data: RutData):
    if data.nit:
        data.nit = _digits_only(data.nit)

    if data.dv:
        data.dv = _digits_only(data.dv)[:1]

    if data.telefono:
        data.telefono = _digits_only(data.telefono)

    if data.correo:
        data.correo = data.correo.lower().strip()

    if data.razon_social:
        data.razon_social = _clean_str(data.razon_social)

    if data.nombre_comercial:
        data.nombre_comercial = _clean_str(data.nombre_comercial)

    if data.direccion:
        data.direccion = _clean_str(data.direccion)

    if data.municipio:
        data.municipio = _clean_str(data.municipio)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def extract_rut(file_path: str) -> RutData:
    """
    Extract RUT data from a DIAN PDF.
    Tries AcroForm fields first, then falls back to text+regex.
    """
    from pypdf import PdfReader

    reader = PdfReader(file_path)

    # Strategy A: AcroForm
    result = _try_acroform(reader)
    if result and result.nit:
        return result

    # Strategy B: text extraction
    result = _try_text_extraction(reader)
    if result and result.nit:
        return result

    # Nothing found
    return RutData(warnings=["No se pudo extraer el NIT del documento"])
