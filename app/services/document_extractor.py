# app/services/document_extractor.py
from __future__ import annotations

import logging
import os
import re
from typing import Optional, Tuple, List

from app.schemas.extraction import ExtractionResult, ExtractedItem, Uom
from app.services.local_fallback_parser import fallback_txt_lines_to_extraction

logger = logging.getLogger(__name__)


class DocumentExtractor:
    def __init__(self) -> None:
        self.max_items = int(os.getenv("OPENAI_MAX_ITEMS", "200"))
        self.max_file_mb = int(os.getenv("OPENAI_MAX_FILE_MB", "10"))
        self.pdf_max_pages = int(os.getenv("OPENAI_PDF_MAX_PAGES", "10"))
        self.model = os.getenv("OPENAI_MODEL_EXTRACTOR", "gpt-4o-mini")

    def _openai_enabled(self) -> bool:
        return os.getenv("OPENAI_ENABLED", "false").lower() == "true"

    def _enrich_enabled(self) -> bool:
        return os.getenv("OPENAI_ENRICH_ENABLED", "false").lower() == "true"

    def _needs_enrichment(self, items: List[ExtractedItem]) -> bool:
        """Check if any items would benefit from OpenAI enrichment."""
        _AWG_RE = re.compile(r"awg|\bthhn\b|\bthwn\b|\btpx\b|\bacsr\b|\bkcmil\b", re.I)
        for it in items:
            # Low confidence items
            if (it.confidence or 0) < 0.5:
                return True
            # Items kept despite low confidence
            if it.warnings and "LOW_CONFIDENCE_KEPT" in it.warnings:
                return True
            # Multi-conductor patterns in description without clear category
            desc = (it.description or "").lower()
            if re.search(r"\d[xX]\d{1,2}", desc) and _AWG_RE.search(desc):
                return True
        return False

    def _enrich_with_openai(self, result: ExtractionResult, raw_text: str) -> ExtractionResult:
        """Post-parse enrichment: send problematic items to OpenAI for correction."""
        if not result.items:
            return result

        try:
            from app.services.openai_extractor import OpenAIExtractor

            # Convert items to dicts for the enrichment API
            items_dicts = []
            for it in result.items:
                d = {
                    "line_index": it.line_index,
                    "description": it.description,
                    "quantity": it.quantity,
                    "uom": it.uom.value if isinstance(it.uom, Uom) else str(it.uom),
                    "confidence": it.confidence,
                    "warnings": it.warnings or [],
                }
                items_dicts.append(d)

            enriched = OpenAIExtractor().enrich_items(items_dicts, raw_text)

            if not enriched or len(enriched) != len(result.items):
                return result

            # Apply enriched data back to ExtractedItem objects
            for orig, enr in zip(result.items, enriched):
                if enr.get("description"):
                    orig.description = str(enr["description"])[:160]
                if "quantity" in enr:
                    try:
                        q = float(enr["quantity"])
                        if q > 0:
                            orig.quantity = q
                    except (ValueError, TypeError):
                        pass
                if "uom" in enr:
                    uom_str = str(enr["uom"]).upper()
                    try:
                        orig.uom = Uom(uom_str)
                    except ValueError:
                        pass
                if "confidence" in enr:
                    try:
                        orig.confidence = float(enr["confidence"])
                    except (ValueError, TypeError):
                        pass
                # Track enrichment in warnings
                ws = list(orig.warnings or [])
                if "OPENAI_ENRICHED" in (enr.get("warnings") or []):
                    if "OPENAI_ENRICHED" not in ws:
                        ws.append("OPENAI_ENRICHED")
                    orig.warnings = ws

            result.global_warnings = (result.global_warnings or []) + ["OPENAI_ENRICHMENT_APPLIED"]
            result.meta = {**(result.meta or {}), "enrichment": "openai"}

        except Exception as e:
            logger.warning("OpenAI enrichment failed: %s", e)
            result.global_warnings = (result.global_warnings or []) + [
                "OPENAI_ENRICHMENT_FAILED",
                f"ENRICH_ERROR_{e.__class__.__name__}",
            ]

        return result

    def detect(self, source_path: str, filename: str | None, content_type: str | None) -> str:
        name = (filename or "").lower()
        ct = (content_type or "").lower()

        # content-type primero (más confiable)
        if ct.startswith("application/pdf"):
            return "pdf"
        if ct.startswith("image/"):
            return "image"
        if ct.startswith("text/plain"):
            return "txt"
        if ct in ("text/csv", "application/csv"):
            return "csv"
        if ct in (
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "application/vnd.ms-excel",
        ):
            return "xlsx"

        # fallback por extensión
        if name.endswith(".pdf"):
            return "pdf"
        if name.endswith((".png", ".jpg", ".jpeg", ".webp")):
            return "image"
        if name.endswith(".csv"):
            return "csv"
        if name.endswith((".xlsx", ".xls")):
            return "xlsx"
        if name.endswith((".txt", ".md", ".log")):
            return "txt"

        return "txt"

    def extract(self, source_path: str, filename: str | None, content_type: str | None) -> ExtractionResult:
        source_type = self.detect(source_path, filename, content_type)

        # Guard simple de tamaño (no revienta el server; deja warning)
        try:
            size_bytes = os.path.getsize(source_path)
            if size_bytes > self.max_file_mb * 1024 * 1024:
                res = fallback_txt_lines_to_extraction(f"[file too large: {filename or 'document'}]")
                res.global_warnings = (res.global_warnings or []) + ["FILE_TOO_LARGE"]
                res.meta = {
                    **(res.meta or {}),
                    "source_type": source_type,
                    "extractor": "local",
                    "model": "local-fallback-v1",
                    "max_file_mb": self.max_file_mb,
                }
                return self._enforce_max_items(res)
        except Exception:
            # si falla getsize, seguimos normal
            pass

        # ---------------- TXT ----------------
        if source_type == "txt":
            text = open(source_path, "r", encoding="utf-8", errors="ignore").read()

            # 0) Siempre correr parser local primero (rápido y estable)
            local_res = fallback_txt_lines_to_extraction(text)
            local_res.meta = {**(local_res.meta or {}), "extractor": "local", "model": "local-fallback-v1", "source_type": "txt"}

            # Heurística simple: si local ya encontró items suficientes -> NO OpenAI full extraction
            min_items = int(os.getenv("LOCAL_FIRST_MIN_ITEMS", "1"))
            if len(local_res.items) >= min_items:
                local_res.global_warnings = (local_res.global_warnings or []) + ["LOCAL_FIRST_USED", "SKIPPED_OPENAI"]

                # Post-parse enrichment: if enabled and items need it, ask OpenAI to fix them
                if self._enrich_enabled() and self._needs_enrichment(local_res.items):
                    local_res = self._enrich_with_openai(local_res, text)

                return self._enforce_max_items(local_res)

            # 1) Si local NO encontró items, ahí sí intenta OpenAI (si está habilitado)
            if self._openai_enabled():
                try:
                    from app.services.openai_extractor import OpenAIExtractor  # lazy import
                    res = OpenAIExtractor().normalize_from_text(text)
                    res.meta = {**(res.meta or {}), "extractor": "openai", "model": self.model, "source_type": "txt"}
                    return self._enforce_max_items(res)
                except Exception as e:
                    # 2) Si OpenAI falla, devolvemos local (aunque sea vacío) con warnings claros
                    local_res.global_warnings = (local_res.global_warnings or []) + [
                        "OPENAI_FAILED",
                        f"OPENAI_ERROR_{e.__class__.__name__}",
                        "FALLBACK_LOCAL_USED",
                    ]
                    local_res.meta = {
                        **(local_res.meta or {}),
                        "openai_error_class": e.__class__.__name__,
                        "openai_error_message": str(e),
                    }
                    return self._enforce_max_items(local_res)

            # OpenAI deshabilitado
            return self._enforce_max_items(local_res)

        # ---------------- CSV ----------------
        if source_type == "csv":
            try:
                from app.services.tabular_loader import csv_to_table_text  # lazy import
                table_text, was_truncated = csv_to_table_text(source_path, max_rows=80)
            except Exception as e:
                # si falla loader, degradar a txt
                table_text = open(source_path, "r", encoding="utf-8", errors="ignore").read()
                was_truncated = False

            if self._openai_enabled():
                try:
                    from app.services.openai_extractor import OpenAIExtractor
                    res = OpenAIExtractor().normalize_from_table(table_text)
                    res.meta = {**(res.meta or {}), "extractor": "openai", "model": self.model}

                except Exception as e:
                    res = fallback_txt_lines_to_extraction(text)
                    res.global_warnings = (res.global_warnings or []) + [
                        "OPENAI_FAILED",
                        f"OPENAI_ERROR_{e.__class__.__name__}",
                        "FALLBACK_LOCAL_USED",
                    ]
                    res.meta = {
                        **(res.meta or {}),
                        "extractor": "local",
                        "model": "local-fallback-v1",
                        "openai_error_class": e.__class__.__name__,
                        "openai_error_message": str(e),
                        "fallback_used": True,
                    }

                    res.meta = {
                        **(res.meta or {}),
                        "extractor": "local",
                        "model": "local-fallback-v1",
                        "openai_error_class": e.__class__.__name__,
                        "openai_error_message": str(e),
                    }
            else:
                res = fallback_txt_lines_to_extraction(table_text)
                res.meta = {**(res.meta or {}), "extractor": "local", "model": "local-fallback-v1"}

            if was_truncated:
                res.global_warnings = (res.global_warnings or []) + ["TRUNCATED_TABLE_ROWS"]

            res.meta = {**(res.meta or {}), "source_type": "csv"}
            return self._enforce_max_items(res)

        # ---------------- XLSX ----------------
        if source_type == "xlsx":
            try:
                from app.services.tabular_loader import xlsx_to_table_text  # lazy import
                table_text, was_truncated = xlsx_to_table_text(source_path, max_rows=80, sheet=0)
            except Exception:
                table_text = f"[uploaded xlsx: {filename or 'document.xlsx'}]"
                was_truncated = False

            if self._openai_enabled():
                try:
                    from app.services.openai_extractor import OpenAIExtractor
                    res = OpenAIExtractor().normalize_from_table(table_text)
                    res.meta = {**(res.meta or {}), "extractor": "openai", "model": self.model}
                
                except Exception as e:
                    res = fallback_txt_lines_to_extraction(text)
                    res.global_warnings = (res.global_warnings or []) + [
                        "OPENAI_FAILED",
                        f"OPENAI_ERROR_{e.__class__.__name__}",
                        "FALLBACK_LOCAL_USED",
                    ]
                    res.meta = {
                        **(res.meta or {}),
                        "extractor": "local",
                        "model": "local-fallback-v1",
                        "openai_error_class": e.__class__.__name__,
                        "openai_error_message": str(e),
                        "fallback_used": True,
                    }

                    res.meta = {
                        **(res.meta or {}),
                        "extractor": "local",
                        "model": "local-fallback-v1",
                        "openai_error_class": e.__class__.__name__,
                        "openai_error_message": str(e),
                    }
            else:
                res = fallback_txt_lines_to_extraction(table_text)
                res.meta = {**(res.meta or {}), "extractor": "local", "model": "local-fallback-v1"}

            if was_truncated:
                res.global_warnings = (res.global_warnings or []) + ["TRUNCATED_TABLE_ROWS"]

            res.meta = {**(res.meta or {}), "source_type": "xlsx"}
            return self._enforce_max_items(res)

        # ---------------- PDF ----------------
        if source_type == "pdf":
            pdf_path = source_path
            truncated = False

            # truncado de páginas (soporta retorno str o (path, truncated_bool))
            try:
                from app.services.pdf_utils import truncate_pdf_pages  # lazy import
                out = truncate_pdf_pages(source_path, max_pages=self.pdf_max_pages)
                if isinstance(out, tuple):
                    pdf_path, truncated = out[0], bool(out[1])
                else:
                    pdf_path = out
                    truncated = True  # si la util no dice, asumimos que truncó si devolvió nuevo path
            except Exception:
                pdf_path = source_path
                truncated = False

            if self._openai_enabled():
                try:
                    from app.services.openai_extractor import OpenAIExtractor
                    res = OpenAIExtractor().extract_from_pdf(pdf_path)
                    res.meta = {**(res.meta or {}), "extractor": "openai", "model": self.model}
                
                except Exception as e:
                    res = fallback_txt_lines_to_extraction(text)
                    res.global_warnings = (res.global_warnings or []) + [
                        "OPENAI_FAILED",
                        f"OPENAI_ERROR_{e.__class__.__name__}",
                        "FALLBACK_LOCAL_USED",
                    ]
                    res.meta = {
                        **(res.meta or {}),
                        "extractor": "local",
                        "model": "local-fallback-v1",
                        "openai_error_class": e.__class__.__name__,
                        "openai_error_message": str(e),
                        "fallback_used": True,
                    }

                    res.meta = {
                        **(res.meta or {}),
                        "extractor": "local",
                        "model": "local-fallback-v1",
                        "openai_error_class": e.__class__.__name__,
                        "openai_error_message": str(e),
                    }
            else:
                res = fallback_txt_lines_to_extraction(f"[uploaded pdf: {filename or 'document.pdf'}]")
                res.meta = {**(res.meta or {}), "extractor": "local", "model": "local-fallback-v1"}

            if truncated:
                res.global_warnings = (res.global_warnings or []) + ["TRUNCATED_PDF_PAGES"]

            res.meta = {**(res.meta or {}), "source_type": "pdf"}
            return self._enforce_max_items(res)

        # ---------------- IMAGE ----------------
        if source_type == "image":
            mime = (content_type or "image/jpeg").lower()
            img_bytes = open(source_path, "rb").read()

            if self._openai_enabled():
                try:
                    from app.services.openai_extractor import OpenAIExtractor
                    res = OpenAIExtractor().extract_from_image(img_bytes, mime=mime)
                    res.meta = {**(res.meta or {}), "extractor": "openai", "model": self.model}
               
                
                except Exception as e:
                    res = fallback_txt_lines_to_extraction(text)
                    res.global_warnings = (res.global_warnings or []) + [
                        "OPENAI_FAILED",
                        f"OPENAI_ERROR_{e.__class__.__name__}",
                        "FALLBACK_LOCAL_USED",
                    ]
                    res.meta = {
                        **(res.meta or {}),
                        "extractor": "local",
                        "model": "local-fallback-v1",
                        "openai_error_class": e.__class__.__name__,
                        "openai_error_message": str(e),
                        "fallback_used": True,
                    }

                    res.meta = {
                        **(res.meta or {}),
                        "extractor": "local",
                        "model": "local-fallback-v1",
                        "openai_error_class": e.__class__.__name__,
                        "openai_error_message": str(e),
                    }
            else:
                res = fallback_txt_lines_to_extraction(f"[uploaded image: {filename or 'image'}]")
                res.meta = {**(res.meta or {}), "extractor": "local", "model": "local-fallback-v1"}

            res.meta = {**(res.meta or {}), "source_type": "image"}
            return self._enforce_max_items(res)

        # fallback final
        text = open(source_path, "r", encoding="utf-8", errors="ignore").read()
        res = fallback_txt_lines_to_extraction(text)
        res.meta = {**(res.meta or {}), "source_type": source_type, "extractor": "local", "model": "local-fallback-v1"}
        return self._enforce_max_items(res)

    def _enforce_max_items(self, res: ExtractionResult) -> ExtractionResult:
        if len(res.items) > self.max_items:
            res.items = res.items[: self.max_items]
            res.global_warnings = (res.global_warnings or []) + ["TRUNCATED_ITEMS"]
        return res
