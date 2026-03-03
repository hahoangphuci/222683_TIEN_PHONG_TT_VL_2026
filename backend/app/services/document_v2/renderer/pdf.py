from __future__ import annotations

import os
from typing import Dict, Iterable, Optional, Tuple

try:
    import pymupdf as fitz  # PyMuPDF >= 1.24
except Exception:  # pragma: no cover
    import fitz

from ..types import TextBlock


def _find_unicode_font() -> Optional[str]:
    for fp in (
        "C:/Windows/Fonts/arial.ttf",
        "C:/Windows/Fonts/tahoma.ttf",
        "C:/Windows/Fonts/calibri.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
    ):
        if os.path.exists(fp):
            return fp
    return None


class PdfRenderer:
    """Render translated blocks back into the original PDF.

    Strategy:
      - Keep original page content (images/background) intact.
      - For text-layer PDFs: redact the original block bbox, then insert translated text.
      - For scanned PDFs: just overlay translated text (no redaction).
      - Auto-scale font size so text stays within bbox.
    """

    def __init__(self, *, redact_fill_rgb01: Optional[Tuple[float, float, float]] = None):
        self.redact_fill = redact_fill_rgb01
        self._fontfile = _find_unicode_font()
        self._fontname = "v2-uni"
        self._font_ready = False

    def _ensure_font(self, page: "fitz.Page") -> str:
        """Ensure a Unicode-capable font is available for insertion."""
        if self._font_ready:
            return self._fontname

        if not self._fontfile:
            return "helv"

        try:
            # Page-level font insertion is supported across PyMuPDF versions.
            page.insert_font(fontname=self._fontname, fontfile=self._fontfile)
            self._font_ready = True
            return self._fontname
        except Exception:
            return "helv"

    def render(
        self,
        doc: "fitz.Document",
        *,
        page_index: int,
        blocks: Iterable[TextBlock],
        translated: Dict[int, str],
        is_text_layer: bool,
    ) -> None:
        page = doc[page_index]
        fontname = self._ensure_font(page)

        # For text-layer pages: redact original text per-block bbox.
        if is_text_layer:
            for b in blocks:
                if b.page_index != page_index:
                    continue
                rect = fitz.Rect(*b.bbox)
                try:
                    page.add_redact_annot(rect, fill=self.redact_fill)
                except Exception:
                    # If redaction fails, we still overlay translated text.
                    pass
            try:
                page.apply_redactions()
            except Exception:
                pass

        for b in blocks:
            if b.page_index != page_index:
                continue
            txt = translated.get(b.block_index)
            if txt is None:
                continue
            if not str(txt).strip():
                continue

            rect = fitz.Rect(*b.bbox)
            # Starting font size: prefer extracted size, else bbox height heuristic.
            start_fs = float(b.font_size) if b.font_size else max(6.0, min(18.0, rect.height * 0.70))
            color = b.color_rgb if b.color_rgb else (0, 0, 0)

            # Fit loop using insert_textbox return value (negative => didn't fit)
            fs = start_fs
            for _ in range(20):
                rc = page.insert_textbox(
                    rect,
                    txt,
                    fontsize=fs,
                    fontname=fontname,
                    color=color,
                    overlay=True,
                    align=0,
                )
                if rc >= 0:
                    break
                fs = max(4.0, fs * 0.9)
                if fs <= 4.0 + 1e-6:
                    break
