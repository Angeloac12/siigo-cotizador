# app/services/document_extractor.py
from __future__ import annotations

import os
from typing import Optional, Tuple

from app.schemas.extraction import ExtractionResult
from app.services.local_fallback_parser import fallback_txt_lines_to_extraction


class DocumentExtractor:
    def __init__(self) -> None:
        self.max_items = int(os.getenv("OPENAI_MAX_ITEMS", "200"))
        self.max_file_mb = int(os.getenv("OPENAI_MAX_FILE_MB", "10"))
        self.pdf_max_pages = int(os.getenv("OPENAI_PDF_MAX_PAGES", "10"))
        self.model = os.getenv("OPENAI_MODEL_EXTRACTOR", "gpt-4o-mini")

    def _openai_enabled(self) -> bool:
        return os.getenv("OPENAI_ENABLED", "false").lower() == "true"

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

            if self._openai_enabled():
                try:
                    from app.services.openai_extractor import OpenAIExtractor  # lazy import
                    res = OpenAIExtractor().normalize_from_text(text)
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
                res = fallback_txt_lines_to_extraction(text)
                res.meta = {**(res.meta or {}), "extractor": "local", "model": "local-fallback-v1"}

            res.meta = {**(res.meta or {}), "source_type": "txt"}
            return self._enforce_max_items(res)

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
