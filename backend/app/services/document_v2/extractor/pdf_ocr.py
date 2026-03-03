from __future__ import annotations

from collections import defaultdict
from typing import Dict, Iterator, List, Optional, Tuple

try:
    import pymupdf as fitz  # PyMuPDF >= 1.24
except Exception:  # pragma: no cover
    import fitz

from ..types import TextBlock


def extract_pdf_ocr_blocks(
    doc: "fitz.Document",
    *,
    page_index: int,
    dpi: int = 300,
    ocr_langs: Optional[str] = None,
    psm: int = 6,
    conf_min: int = 45,
    granularity: str = "line",  # 'word' | 'line'
    tesseract_cmd: Optional[str] = None,
) -> Iterator[TextBlock]:
    """Extract OCR blocks using pytesseract.image_to_data.

    Requirement: must use image_to_data to get left/top/width/height/text.

    Output bbox is converted to PDF coordinates.
    """

    try:
        import pytesseract
        from pytesseract import Output
    except Exception as e:  # pragma: no cover
        raise RuntimeError(f"pytesseract is required for OCR: {e}")

    try:
        from PIL import Image
    except Exception as e:  # pragma: no cover
        raise RuntimeError(f"Pillow is required for OCR: {e}")

    if tesseract_cmd:
        try:
            pytesseract.pytesseract.tesseract_cmd = tesseract_cmd
        except Exception:
            pass

    page = doc[page_index]
    zoom = float(dpi) / 72.0
    mat = fitz.Matrix(zoom, zoom)
    pix = page.get_pixmap(matrix=mat, alpha=False)

    img = Image.frombytes("RGB", (pix.width, pix.height), pix.samples)

    lang = (ocr_langs or "eng").strip() or "eng"
    cfg = f"--psm {int(psm)}"

    data: Dict[str, List] = pytesseract.image_to_data(img, lang=lang, config=cfg, output_type=Output.DICT)
    n = int(len(data.get("text") or []))
    if n <= 0:
        return

    page_w_pt = float(page.rect.width)
    page_h_pt = float(page.rect.height)
    img_w = float(pix.width)
    img_h = float(pix.height)

    sx = page_w_pt / img_w
    sy = page_h_pt / img_h

    gran = (granularity or "line").strip().lower()
    if gran not in ("word", "line"):
        gran = "line"

    # Collect items
    items = []
    for i in range(n):
        word = (data.get("text") or [""])[i]
        if not word or not str(word).strip():
            continue

        try:
            conf_raw = (data.get("conf") or [""])[i]
            conf = int(float(conf_raw)) if str(conf_raw).strip() != "" else -1
        except Exception:
            conf = -1
        if conf >= 0 and conf < int(conf_min):
            continue

        try:
            left = int((data.get("left") or [0])[i])
            top = int((data.get("top") or [0])[i])
            width = int((data.get("width") or [0])[i])
            height = int((data.get("height") or [0])[i])
        except Exception:
            continue

        if width <= 0 or height <= 0:
            continue

        # Line grouping key (Tesseract provides these IDs)
        key = (
            int((data.get("block_num") or [0])[i]),
            int((data.get("par_num") or [0])[i]),
            int((data.get("line_num") or [0])[i]),
        )

        items.append((top, left, key, word, (left, top, left + width, top + height)))

    if not items:
        return

    # Sort to keep reading order stable
    items.sort(key=lambda t: (t[0], t[1]))

    if gran == "word":
        block_idx = 0
        for _top, _left, _key, word, (x0, y0, x1, y1) in items:
            yield TextBlock(
                page_index=page_index,
                block_index=block_idx,
                bbox=(x0 * sx, y0 * sy, x1 * sx, y1 * sy),
                text=str(word),
                kind="pdf_ocr",
                meta={"granularity": "word"},
            )
            block_idx += 1
        return

    # Group by line key
    grouped: Dict[Tuple[int, int, int], List] = defaultdict(list)
    for _top, _left, key, word, bbox in items:
        grouped[key].append((_top, _left, word, bbox))

    # Emit in reading order by first item in each group
    groups_sorted = sorted(
        grouped.items(),
        key=lambda kv: (kv[1][0][0], kv[1][0][1]),
    )

    block_idx = 0
    for key, parts in groups_sorted:
        parts.sort(key=lambda t: (t[0], t[1]))
        text = " ".join(str(p[2]) for p in parts).strip()
        if not text:
            continue
        xs0 = min(p[3][0] for p in parts)
        ys0 = min(p[3][1] for p in parts)
        xs1 = max(p[3][2] for p in parts)
        ys1 = max(p[3][3] for p in parts)
        yield TextBlock(
            page_index=page_index,
            block_index=block_idx,
            bbox=(xs0 * sx, ys0 * sy, xs1 * sx, ys1 * sy),
            text=text,
            kind="pdf_ocr",
            meta={"granularity": "line", "tesseract_line": key},
        )
        block_idx += 1
