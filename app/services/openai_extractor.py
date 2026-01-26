# app/services/openai_extractor.py
from __future__ import annotations

import json
import os
from typing import Any, Dict, Optional, List

from openai import OpenAI
from pydantic import ValidationError

from app.schemas.extraction import ExtractionResult


def _min_tokens(v: int) -> int:
    # responses.create exige >= 16
    return v if v >= 16 else 16


class OpenAIExtractor:
    """
    Normaliza RFQs desde:
    - texto plano
    - tabla en texto (csv/xlsx preconvertido)
    - PDF (input_file)

    Salida: ExtractionResult (items + global_warnings + meta)
    """

    def __init__(self) -> None:
        self.client = OpenAI()
        # Por defecto usamos gpt-4.1-mini para extracción
        self.model = os.getenv("OPENAI_MODEL_EXTRACTOR", "gpt-4.1-mini")
        self.max_output_tokens = _min_tokens(int(os.getenv("OPENAI_MAX_OUTPUT_TOKENS", "900")))
        self.temperature = float(os.getenv("OPENAI_TEMPERATURE", "0"))

    # -------------------------
    # Public API
    # -------------------------
    def normalize_from_text(self, text: str) -> ExtractionResult:
        return self._call_openai(
            user_content=[{"type": "input_text", "text": self._prompt_for_text(text)}],
            source_type="txt",
        )

    def normalize_from_table(self, table_text: str) -> ExtractionResult:
        return self._call_openai(
            user_content=[{"type": "input_text", "text": self._prompt_for_table(table_text)}],
            source_type="table",
        )

    def normalize_from_pdf(self, pdf_path: str) -> ExtractionResult:
        # Subir PDF y pasarlo como input_file (Responses API)
        with open(pdf_path, "rb") as f:
            uploaded = self.client.files.create(file=f, purpose="user_data")

        return self._call_openai(
            user_content=[
                {"type": "input_file", "file_id": uploaded.id},
                {"type": "input_text", "text": self._prompt_for_pdf()},
            ],
            source_type="pdf",
        )

    # Alias para compatibilidad antigua
    def extract_from_pdf(self, pdf_path: str) -> ExtractionResult:
        return self.normalize_from_pdf(pdf_path)

    # -------------------------
    # Internals
    # -------------------------
    def _call_openai(self, user_content: list[dict], source_type: str) -> ExtractionResult:
        resp = self.client.responses.create(
            model=self.model,
            input=[
                {"role": "system", "content": self._system_prompt()},
                {"role": "user", "content": user_content},
            ],
            temperature=self.temperature,
            max_output_tokens=self.max_output_tokens,
            text={
                "format": {
                    "type": "json_schema",
                    "name": "extraction_result",
                    "schema": self._json_schema(),
                    "strict": True,
                }
            },
        )

        out_text = getattr(resp, "output_text", None)
        if not out_text:
            out_text = ""

        data = json.loads(out_text) if out_text else {}

        # 1) Normalización de unidades, etc.
        data = self._normalize_before_validation(data)

        # 2) Intentar validar; si Pydantic se queja, limpiamos items y reintentamos
        try:
            res = self._validate_extraction_result(data)
        except ValidationError as e1:
            # limpieza adicional: eliminar items con descripción vacía o quantity <= 0
            data_clean = self._cleanup_items_for_validation(data)

            try:
                res = self._validate_extraction_result(data_clean)
            except ValidationError as e2:
                # último recurso: no lanzar excepción, devolver resultado vacío
                res = ExtractionResult(
                    items=[],
                    global_warnings=[
                        "OPENAI_PYDANTIC_VALIDATION_FAILED",
                        str(e1)[:200],
                        str(e2)[:200],
                    ],
                    meta={},
                )

        # meta mínima garantizada (se complementa, no se borra)
        res.meta = {
            **(res.meta or {}),
            "extractor": "openai",
            "model": self.model,
            "source_type": source_type,
        }
        res.global_warnings = res.global_warnings or []
        return res

    def _normalize_before_validation(self, data: dict) -> dict:
        """
        Ajusta el JSON crudo de OpenAI para que pase la validación Pydantic.
        - Si uom no es una unidad válida, la mueve a uom_raw y pone uom='UND'.
        """
        if not isinstance(data, dict):
            return data

        items = data.get("items")
        if not isinstance(items, list):
            return data

        allowed_uom = {"UND", "M", "KG", "ROL", "EA", "BOX", "SET", "L", "GAL", "PACK"}

        for it in items:
            if not isinstance(it, dict):
                continue

            uom = it.get("uom")
            if isinstance(uom, str):
                uom_upper = uom.upper()

                if uom_upper not in allowed_uom:
                    # guardar el original en uom_raw si no estaba
                    if not it.get("uom_raw"):
                        it["uom_raw"] = uom
                    # setear uom normalizada
                    it["uom"] = "UND"

        return data

    def _cleanup_items_for_validation(self, data: dict) -> dict:
        """
        Filtro extra:
        - Elimina items con description vacía.
        - Elimina items con quantity <= 0 o no numérica.
        - Asegura line_index y warnings_json.
        """
        if not isinstance(data, dict):
            return {"items": [], "global_warnings": [], "meta": {}}

        raw_items = data.get("items") or []
        if not isinstance(raw_items, list):
            raw_items = []

        clean_items: List[Dict[str, Any]] = []

        for idx, it in enumerate(raw_items):
            if not isinstance(it, dict):
                continue

            desc = str(it.get("description") or "").strip()
            qty_raw = it.get("quantity", 0)

            try:
                qty = float(qty_raw)
            except Exception:
                qty = 0.0

            # desc vacía o qty <= 0 => descartar
            if not desc or qty <= 0:
                continue

            # asegurar line_index secuencial
            if "line_index" not in it or not isinstance(it["line_index"], int):
                it["line_index"] = len(clean_items)

            it["description"] = desc
            it["quantity"] = qty

            # asegurar warnings_json lista
            if "warnings_json" not in it or it["warnings_json"] is None:
                it["warnings_json"] = []

            clean_items.append(it)

        return {
            "items": clean_items,
            "global_warnings": data.get("global_warnings") or [],
            "meta": data.get("meta") or {},
        }

    def _validate_extraction_result(self, data: dict) -> ExtractionResult:
        # Pydantic v2
        if hasattr(ExtractionResult, "model_validate"):
            return ExtractionResult.model_validate(data)

        # Pydantic v1 (por si acaso)
        if hasattr(ExtractionResult, "parse_obj"):
            return ExtractionResult.parse_obj(data)

        # Último recurso
        return ExtractionResult(**data)  # type: ignore[arg-type]

    # -------------------------
    # Prompts
    # -------------------------
    def _system_prompt(self) -> str:
        return (
            "Eres un extractor de ítems de cotización (RFQ) para materiales eléctricos.\n"
            "Tu único objetivo es identificar líneas que correspondan a productos o servicios reales "
            "y devolverlas como items.\n"
            "\n"
            "REGLAS MUY IMPORTANTES:\n"
            "- NO calcules ni sumes valores de dinero.\n"
            "- NO crees items para filas de resumen como:\n"
            "  'Total', 'Total Bruto', 'Subtotal', 'IVA', 'IVA 19%', 'Retefuente', 'ReteFuente',\n"
            "  'Total a Pagar', 'Total a pagar', 'Abono', 'Saldo', 'Saldo a pagar', u otros totales.\n"
            "- Si una línea contiene principalmente un valor monetario (por ejemplo 584,200.00, 680,593.00)\n"
            "  y palabras como 'Total', 'Subtotal', 'IVA', 'Rete...', etc., DEBES ignorarla.\n"
            "- quantity SIEMPRE es la cantidad física del ítem (unidades, metros, rollos, etc.),\n"
            "  NUNCA debe ser un valor de dinero.\n"
            "- uom (unidad) debe representar la unidad física ('UND', 'M', 'KG', 'ROL', etc.).\n"
            "  Si no se entiende la unidad, usa 'UND' y pon la unidad original en uom_raw.\n"
            "- Solo debes extraer ítems que representen materiales o servicios.\n"
            "- global_warnings es una lista de strings (puede ir vacía).\n"
            "- meta es un objeto libre (el sistema lo completará con detalles internos).\n"
            "\n"
            "Formato de salida:\n"
            "- Devuelve SOLO un JSON válido que cumpla exactamente el schema indicado.\n"
            "- Cada item debe incluir: line_index, raw_text, description, quantity, uom, uom_raw, confidence, warnings_json.\n"
            "- line_index empieza en 0 y aumenta de 1 en 1 según el orden de aparición en el texto.\n"
        )

    def _prompt_for_text(self, text: str) -> str:
        return (
            "Tienes un texto con una solicitud de cotización o lista de materiales.\n"
            "- Cada línea suele ser un ítem o una línea de resumen.\n"
            "- Extrae SOLO las líneas que describen productos/servicios con cantidades físicas.\n"
            "- Ignora por completo filas de totales, subtotales, IVA, retenciones, 'Total a Pagar' y similares.\n"
            "- No conviertas precios ni valores de dinero en quantity.\n"
            "- Si una línea no tiene cantidad explícita, asume quantity=1.\n\n"
            f"TEXTO:\n{text}"
        )

    def _prompt_for_table(self, table_text: str) -> str:
        return (
            "Tienes una tabla en texto (por ejemplo convertida desde Excel/CSV) con campos de materiales.\n"
            "- Busca columnas de descripción y cantidad.\n"
            "- Si no hay unidad explícita, usa uom='UND'.\n"
            "- NO crees items para filas de totales, subtotales, IVA, retenciones ni 'Total a Pagar'.\n"
            "- No conviertas precios ni valores monetarios en quantity.\n\n"
            f"TABLA:\n{table_text}"
        )

    def _prompt_for_pdf(self) -> str:
        return (
            "Este archivo es un PDF con una solicitud de cotización o listado de materiales.\n"
            "- Detecta solo las líneas que describen productos o servicios con cantidades físicas.\n"
            "- NO debes crear items para filas de resumen como 'Total Bruto', 'Subtotal', 'IVA',\n"
            "  'Retefuente', 'Total a Pagar', 'Abono', 'Saldo' u otros totales.\n"
            "- No conviertas precios ni valores en COP a quantity.\n"
            "- Si el PDF tiene tabla, úsala para identificar cantidad y descripción.\n"
        )

    # -------------------------
    # JSON schema para Structured Outputs
    # -------------------------
    def _json_schema(self) -> Dict[str, Any]:
        """
        JSON Schema para Structured Outputs.
        """
        return {
            "type": "object",
            "properties": {
                "items": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "line_index": {"type": "integer", "minimum": 0},
                            "raw_text": {"type": "string"},
                            "description": {"type": "string"},
                            "quantity": {"type": "number"},
                            "uom": {"type": "string"},
                            "uom_raw": {"type": ["string", "null"]},
                            "confidence": {"type": "number"},
                            "warnings_json": {
                                "type": ["array", "null"],
                                "items": {"type": "string"},
                            },
                        },
                        "required": [
                            "line_index",
                            "raw_text",
                            "description",
                            "quantity",
                            "uom",
                            "uom_raw",
                            "confidence",
                            "warnings_json",
                        ],
                        "additionalProperties": False,
                    },
                },
                "global_warnings": {
                    "type": "array",
                    "items": {"type": "string"},
                },
                "meta": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "source_type": {"type": "string"},
                        "extractor": {"type": "string"},
                        "model": {"type": "string"},
                    },
                    "required": ["source_type", "extractor", "model"],
                },
            },
            "required": ["items", "global_warnings", "meta"],
            "additionalProperties": False,
        }
