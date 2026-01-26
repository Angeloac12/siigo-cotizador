# app/services/local_fallback_parser.py
from __future__ import annotations

import re
from typing import List, Optional, Tuple

from app.schemas.extraction import ExtractionResult, ExtractedItem, Uom

_UOM_ALIASES = {
    # UND
    "und": Uom.UND, "unidad": Uom.UND, "un": Uom.UND, "u": Uom.UND, "pza": Uom.UND, "pzas": Uom.UND,
    # M
    "m": Uom.M, "mt": Uom.M, "mts": Uom.M, "mtr": Uom.M, "mtrs": Uom.M, "metro": Uom.M, "metros": Uom.M,
    # KG
    "kg": Uom.KG, "kilo": Uom.KG, "kilos": Uom.KG, "kilogramo": Uom.KG, "kilogramos": Uom.KG,
    # ROL
    "rol": Uom.ROL, "rollo": Uom.ROL, "rollos": Uom.ROL,
    # EA
    "ea": Uom.EA,
    # BOX
    "box": Uom.BOX, "caja": Uom.BOX, "cajas": Uom.BOX,
    # SET
    "set": Uom.SET, "juego": Uom.SET, "juegos": Uom.SET, "kit": Uom.SET,
    # L
    "l": Uom.L, "lt": Uom.L, "lts": Uom.L, "litro": Uom.L, "litros": Uom.L,
    # GAL
    "gal": Uom.GAL, "galon": Uom.GAL, "galones": Uom.GAL,
    # PACK
    "pack": Uom.PACK, "paquete": Uom.PACK, "paquetes": Uom.PACK, "pkg": Uom.PACK,
}

_BULLET_PREFIX_RE = re.compile(r"^\s*[-*•]+\s*")


def _to_float(s: str) -> Optional[float]:
    s = (s or "").strip()
    if not s:
        return None
    s = s.replace(",", ".")
    try:
        v = float(s)
        return v
    except Exception:
        return None


def _infer_uom(token: Optional[str]) -> Optional[Uom]:
    if not token:
        return None
    t = token.strip().lower()
    # limpiar puntuación
    t = re.sub(r"[^a-zA-Z0-9]+", "", t)
    return _UOM_ALIASES.get(t)


def _extract_qty_uom_desc(raw: str):
    warnings = []
    raw_clean = _BULLET_PREFIX_RE.sub("", raw.strip())

    qty = None
    uom = None
    uom_raw = None
    desc = raw_clean

    # qty al inicio: "10 cable THHN..."
    m = re.match(r"^(\d+(?:[.,]\d+)?)\s*(?:x\s*)?([A-Za-zñÑ\.]+)?\s*(.+)$", raw_clean)
    if m:
        qty = _to_float(m.group(1))
        token_raw = (m.group(2) or "").strip()
        token = token_raw.lower().strip(".")
        rest = (m.group(3) or "").strip()

        maybe_uom = _infer_uom(token)

        if maybe_uom is not None:
            uom = maybe_uom
            uom_raw = token_raw
            desc = rest
        else:
            # NO era unidad -> es parte de la descripción
            desc = f"{token_raw} {rest}".strip() if token_raw else rest

    if qty is None or qty <= 0:
        qty = 1.0
        warnings.append("QTY_INFERRED")

    if uom is None:
        uom = Uom.UND
        warnings.append("UOM_INFERRED")

    desc2 = (desc or "").strip()
    if not desc2:
        desc2 = raw_clean
        warnings.append("DESCRIPTION_FALLBACK")

    conf = 0.4
    if "QTY_INFERRED" not in warnings and "UOM_INFERRED" not in warnings:
        conf = 0.75
    elif "QTY_INFERRED" in warnings and "UOM_INFERRED" in warnings:
        conf = 0.35

    return float(qty), uom, desc2, warnings, conf, uom_raw




def fallback_txt_lines_to_extraction(text: str, max_items: int = 200) -> ExtractionResult:
    items: List[ExtractedItem] = []
    global_warnings: List[str] = []

    lines = [ln.strip() for ln in (text or "").splitlines()]
    lines = [ln for ln in lines if ln]  # drop empties

    for idx, ln in enumerate(lines):
        if len(items) >= max_items:
            global_warnings.append("TRUNCATED_ITEMS_MAX_ITEMS")
            break

        qty, uom, desc, warnings, conf, uom_raw = _extract_qty_uom_desc(ln)

        items.append(
            ExtractedItem(
                line_index=len(items),
                raw_text=ln,
                description=desc,
                quantity=qty,
                uom=uom,
                uom_raw=uom_raw,
                confidence=conf,
                warnings=warnings or None,
            )
        )

    return ExtractionResult(
        items=items,
        global_warnings=global_warnings or None,
        meta={"source_type": "txt", "extractor": "local", "model": "local-fallback-v1"},
    )


def fallback_table_text_to_extraction(table_text: str, source_type: str, max_items: int = 200) -> ExtractionResult:
    """
    table_text es TSV:
    header1\theader2...
    v1\tv2...
    """
    lines = [ln.rstrip("\n") for ln in (table_text or "").splitlines() if ln.strip()]
    if not lines:
        return ExtractionResult(items=[], global_warnings=["EMPTY_TABLE"], meta={"source_type": source_type, "extractor": "local"})

    header = [h.strip() for h in lines[0].split("\t")]
    header_norm = [h.lower() for h in header]

    def find_col(keys: List[str]) -> Optional[int]:
        for i, h in enumerate(header_norm):
            for k in keys:
                if k in h:
                    return i
        return None

    desc_idx = find_col(["descripcion", "descripción", "desc", "producto", "item", "material", "nombre"])
    qty_idx = find_col(["cantidad", "cant", "qty", "quantity"])
    uom_idx = find_col(["uom", "unidad", "unit"])

    items: List[ExtractedItem] = []
    global_warnings: List[str] = []

    for row_i, row in enumerate(lines[1:]):
        if len(items) >= max_items:
            global_warnings.append("TRUNCATED_ITEMS_MAX_ITEMS")
            break

        cols = [c.strip() for c in row.split("\t")]
        # asegurar largo
        while len(cols) < len(header):
            cols.append("")

        raw_text = row

        warnings: List[str] = []
        qty: Optional[float] = None
        uom: Optional[Uom] = None
        uom_raw: Optional[str] = None

        if qty_idx is not None:
            qty = _to_float(cols[qty_idx])

        if uom_idx is not None:
            uom_raw = cols[uom_idx] or None
            uom = _infer_uom(cols[uom_idx])

        if desc_idx is not None:
            desc = cols[desc_idx]
        else:
            # fallback: join columnas no vacías
            desc = " ".join([c for c in cols if c])

        # si qty/uom no claros, intentar inferir desde desc
        if qty is None or qty <= 0 or uom is None:
            qty2, uom2, desc2, infer_warnings, conf, uom_raw2 = _extract_qty_uom_desc(desc or raw_text)
            if qty is None or qty <= 0:
                qty = qty2
                if "QTY_INFERRED" in infer_warnings:
                    warnings.append("QTY_INFERRED")
            if uom is None:
                uom = uom2
                if "UOM_INFERRED" in infer_warnings:
                    warnings.append("UOM_INFERRED")
            if not (desc or "").strip():
                desc = desc2
                warnings.append("DESCRIPTION_FALLBACK")
            if not uom_raw and uom_raw2:
                uom_raw = uom_raw2
            confidence = conf
        else:
            confidence = 0.7

        if not (desc or "").strip():
            desc = raw_text
            warnings.append("DESCRIPTION_FALLBACK")

        if qty is None or qty <= 0:
            qty = 1.0
            warnings.append("QTY_INFERRED")
        if uom is None:
            uom = Uom.UND
            warnings.append("UOM_INFERRED")

        items.append(
            ExtractedItem(
                line_index=len(items),
                raw_text=raw_text,
                description=desc.strip(),
                quantity=float(qty),
                uom=uom,
                uom_raw=uom_raw,
                confidence=confidence,
                warnings=warnings or None,
            )
        )

    return ExtractionResult(
        items=items,
        global_warnings=global_warnings or None,
        meta={"source_type": source_type, "extractor": "local", "model": "local-fallback-v1"},
    )
