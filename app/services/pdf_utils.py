from __future__ import annotations

from pathlib import Path
from typing import Tuple

def truncate_pdf_pages(input_path: str, max_pages: int) -> Tuple[str, bool]:
    """
    Retorna (path_pdf_usable, was_truncated).
    Si no puede truncar, retorna el input original.
    """
    try:
        from pypdf import PdfReader, PdfWriter
    except Exception:
        return input_path, False

    p = Path(input_path)
    reader = PdfReader(str(p))
    total = len(reader.pages)
    if total <= max_pages:
        return input_path, False

    writer = PdfWriter()
    for i in range(max_pages):
        writer.add_page(reader.pages[i])

    out_path = str(p.with_suffix(".truncated.pdf"))
    with open(out_path, "wb") as f:
        writer.write(f)

    return out_path, True
