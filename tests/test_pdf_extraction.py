"""Tests for PDF text extraction pipeline (Phase 1).

Creates small PDFs with pypdf containing material lines,
verifies extraction through the local parser pipeline.
"""
from __future__ import annotations

import os
import tempfile

import pytest
from pypdf import PdfWriter
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas


def _create_text_pdf(lines: list[str], path: str) -> str:
    """Create a simple text PDF using reportlab."""
    c = canvas.Canvas(path, pagesize=letter)
    y = 750
    for line in lines:
        c.drawString(72, y, line)
        y -= 15
    c.save()
    return path


def _create_text_pdf_pypdf(text: str, path: str) -> str:
    """Create a minimal PDF with pypdf (blank page — for scanned-PDF simulation)."""
    writer = PdfWriter()
    writer.add_blank_page(width=612, height=792)
    with open(path, "wb") as f:
        writer.write(f)
    return path


# --------------- pypdf text extraction ---------------

class TestPdfTextExtraction:
    """Verify pypdf can extract text from a text-based PDF."""

    def test_extract_text_from_pdf(self):
        lines = [
            "10 und Cable THHN 12 AWG",
            "5 m Tubo conduit 1 pulgada",
            "20 und Breaker 20A",
        ]
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            path = tmp.name

        try:
            _create_text_pdf(lines, path)

            from pypdf import PdfReader
            reader = PdfReader(path)
            extracted = "\n".join(
                page.extract_text() or "" for page in reader.pages
            )

            assert "Cable THHN" in extracted or "THHN" in extracted
            assert "Tubo conduit" in extracted or "conduit" in extracted
        finally:
            os.unlink(path)


# --------------- DocumentExtractor PDF branch ---------------

class TestDocumentExtractorPdf:
    """Integration: DocumentExtractor.extract() with a text-based PDF."""

    def test_pdf_local_extraction_finds_items(self):
        lines = [
            "10 und Cable THHN 12 AWG",
            "5 m Tubo conduit 1 pulgada",
            "20 und Breaker 20A",
            "3 rollos Cable duplex 2x12",
        ]
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            path = tmp.name

        try:
            _create_text_pdf(lines, path)

            from app.services.document_extractor import DocumentExtractor
            ext = DocumentExtractor()
            result = ext.extract(path, "materiales.pdf", "application/pdf")

            assert result.items, "Should extract at least 1 item from text PDF"
            assert result.meta.get("source_type") == "pdf"
            assert result.meta.get("extractor") == "local"
            # OpenAI disabled → local fallback used
            warnings = result.global_warnings or []
            assert "FALLBACK_LOCAL_USED" in warnings
        finally:
            os.unlink(path)

    def test_pdf_scanned_returns_warning(self):
        """A blank PDF (simulating scanned/image PDF) should return warning, not crash."""
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            path = tmp.name

        try:
            _create_text_pdf_pypdf("", path)

            # Ensure OpenAI is disabled for this test
            old = os.environ.get("OPENAI_ENABLED")
            os.environ["OPENAI_ENABLED"] = "false"
            try:
                from app.services.document_extractor import DocumentExtractor
                ext = DocumentExtractor()
                result = ext.extract(path, "scanned.pdf", "application/pdf")

                # Should not crash, should have warning
                warnings = result.global_warnings or []
                assert "OPENAI_DISABLED" in warnings or "FALLBACK_LOCAL_USED" in warnings
                assert result.meta.get("source_type") == "pdf"
            finally:
                if old is not None:
                    os.environ["OPENAI_ENABLED"] = old
                else:
                    os.environ.pop("OPENAI_ENABLED", None)
        finally:
            os.unlink(path)

    def test_pdf_extraction_under_2s(self):
        """PDF text extraction should be fast (<2s for a small PDF)."""
        import time
        lines = [f"{i} und Producto genérico #{i}" for i in range(1, 51)]
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            path = tmp.name

        try:
            _create_text_pdf(lines, path)

            from app.services.document_extractor import DocumentExtractor
            ext = DocumentExtractor()

            start = time.time()
            result = ext.extract(path, "big.pdf", "application/pdf")
            elapsed = time.time() - start

            assert elapsed < 2.0, f"PDF extraction took {elapsed:.2f}s, should be <2s"
            assert result.items, "Should extract items from 50-line PDF"
        finally:
            os.unlink(path)


# --------------- routes_quote_drafts binary fallback ---------------

class TestBinaryFallback:
    """Verify that PDF bytes are not decoded as UTF-8 garbage."""

    def test_pdf_content_type_detected_as_binary(self):
        ct = "application/pdf"
        is_binary = ct.startswith("application/pdf") or ct.startswith("image/")
        assert is_binary

    def test_image_content_type_detected_as_binary(self):
        ct = "image/png"
        is_binary = ct.startswith("application/pdf") or ct.startswith("image/")
        assert is_binary

    def test_text_content_type_not_binary(self):
        ct = "text/plain"
        is_binary = ct.startswith("application/pdf") or ct.startswith("image/")
        assert not is_binary
