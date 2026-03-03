from __future__ import annotations

from statistics import median
from typing import Iterator, Optional, Tuple

try:
    import pymupdf as fitz  # PyMuPDF >= 1.24
except Exception:  # pragma: no cover
    import fitz  # older name

from ..types import TextBlock


def _int_color_to_rgb01(color_int: int) -> Tuple[float, float, float]:
    # color_int is usually 0xRRGGBB
    r = (color_int >> 16) & 255
    g = (color_int >> 8) & 255
    b = color_int & 255
    return (r / 255.0, g / 255.0, b / 255.0)


def pdf_has_text_layer(doc: "fitz.Document", *, min_chars: int = 10) -> bool:
    """Heuristic: PDF has text layer if we can find enough text blocks."""
    for page in doc:
        d = page.get_text("dict")
        for b in d.get("blocks", []) or []:
            if b.get("type") != 0:
                continue
            if (b.get("text") or "").strip():
                if len((b.get("text") or "").strip()) >= min_chars:
                    return True
        # fall back: blocks may not have 'text', so check spans
        for b in d.get("blocks", []) or []:
            if b.get("type") != 0:
                continue
            for ln in b.get("lines", []) or []:
                for sp in ln.get("spans", []) or []:
                    if (sp.get("text") or "").strip():
                        return True
    return False


def extract_pdf_text_blocks(
    doc: "fitz.Document",
    *,
    page_index: int,
) -> Iterator[TextBlock]:
    """Extract each text block (no merging across blocks) with bbox.

    Requirement: do NOT extract whole page text at once; do NOT merge blocks.
    """

    page = doc[page_index]
    d = page.get_text("dict")

    block_idx = 0
    for b in d.get("blocks", []) or []:
        if b.get("type") != 0:
            continue

        bbox = b.get("bbox")
        if not bbox or len(bbox) != 4:
            continue
        x0, y0, x1, y1 = [float(v) for v in bbox]

        # Build block text by lines/spans to preserve line breaks.
        lines_out = []
        sizes = []
        fonts = []
        colors = []

        for ln in b.get("lines", []) or []:
            parts = []
            for sp in ln.get("spans", []) or []:
                t = sp.get("text") or ""
                if t:
                    parts.append(t)
                try:
                    if sp.get("size") is not None:
                        sizes.append(float(sp.get("size")))
                except Exception:
                    pass
                if sp.get("font"):
                    fonts.append(str(sp.get("font")))
                if sp.get("color") is not None:
                    try:
                        colors.append(int(sp.get("color")))
                    except Exception:
                        pass
            line_text = "".join(parts)
            # Keep even empty lines to preserve structure inside the block.
            lines_out.append(line_text)

        text = "\n".join(lines_out).rstrip("\n")
        if not text.strip():
            continue

        font_size: Optional[float] = None
        if sizes:
            try:
                font_size = float(median(sizes))
            except Exception:
                font_size = None

        font_name: Optional[str] = fonts[0] if fonts else None
        color_rgb = _int_color_to_rgb01(colors[0]) if colors else None

        yield TextBlock(
            page_index=page_index,
            block_index=block_idx,
            bbox=(x0, y0, x1, y1),
            text=text,
            kind="pdf_text",
            font_size=font_size,
            font_name=font_name,
            color_rgb=color_rgb,
            meta=None,
        )
        block_idx += 1
