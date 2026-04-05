"""Clean block-by-block PDF translation pipeline.

Pipeline:
  PDF
  ↓
  Extract blocks (text + bbox + font) via PyMuPDF
  ↓
  Translate each block (using translate_fn / existing prompt)
  ↓
  Render back to PDF at exact bbox positions
    - Redact original text at block bbox
    - Insert translated text at same bbox preserving font/size/color/alignment

This approach preserves:
  - All PDF graphics, images, table borders, backgrounds
  - Exact text positioning via bounding box coordinates
  - Font size, bold, italic, color per block
  - Text alignment (left / center / right)
"""

from __future__ import annotations

import os
import re
import uuid
import io
import unicodedata
import bisect
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Set, Tuple


# ─────────────────────────────────────────────────────────────────────────────
# Data types
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class PdfTextBlock:
    """A logical text block extracted from a PDF page."""
    text: str
    bbox: Tuple[float, float, float, float]   # (x0, y0, x1, y1) in PDF points
    font_name: str = "Helvetica"
    font_size: float = 10.0
    is_bold: bool = False
    is_italic: bool = False
    color: Tuple[float, float, float] = (0.0, 0.0, 0.0)   # RGB 0-1
    alignment: str = "left"                                 # left | center | right
    page_width: float = 595.0
    page_height: float = 842.0
    is_table_cell: bool = False
    table_bbox: Optional[Tuple[float, float, float, float]] = None
    # Exact baseline origin of the first span (from span["origin"])
    # y is the actual text baseline – use this for insert_text(), NOT bbox y1
    origin_x: float = 0.0
    origin_y: float = 0.0
    # Raw PyMuPDF span dicts for rebuilding (used by renderer)
    raw_spans: list = field(default_factory=list)


# ─────────────────────────────────────────────────────────────────────────────
# Phase 1 — Extraction
# ─────────────────────────────────────────────────────────────────────────────

def _int_color_to_rgb(color_int: int) -> Tuple[float, float, float]:
    c = int(color_int or 0)
    return (((c >> 16) & 255) / 255.0, ((c >> 8) & 255) / 255.0, (c & 255) / 255.0)


def _detect_alignment(x0: float, x1: float, page_w: float) -> str:
    """Heuristic alignment detection from bbox."""
    left_margin  = x0
    right_margin = page_w - x1
    width        = x1 - x0
    center_off   = abs((left_margin + width / 2) - page_w / 2)

    # Detect true center lines by balancing left/right margins, even when
    # the left margin is not large (common in headers above tables).
    # Guard: Long paragraph lines at normal indent positions (left < 28%
    # of page width) often span nearly the full width, making their
    # margins *look* balanced.  Require either a generous left margin
    # (text starts well away from the edge) or a short/moderate width
    # to avoid false-positive centre classification.
    if center_off < page_w * 0.06 and abs(left_margin - right_margin) < page_w * 0.12:
        if left_margin > page_w * 0.28 or width < page_w * 0.40:
            return "center"
    if right_margin < page_w * 0.08 and left_margin > page_w * 0.28:
        return "right"
    return "left"


def _detect_alignment_in_cell(
    x0: float,
    x1: float,
    cell_bbox: Tuple[float, float, float, float],
    *,
    fallback: str = "left",
) -> str:
    """Infer alignment using margins inside a table cell.

    Using page-level margins for table text can misclassify centered headers.
    This compares text box margins to the enclosing cell instead.
    """
    cx0, _cy0, cx1, _cy1 = cell_bbox
    cw = max(1.0, cx1 - cx0)
    left_margin = max(0.0, x0 - cx0)
    right_margin = max(0.0, cx1 - x1)

    # Balanced margins => centered text in cell.
    if abs(left_margin - right_margin) <= max(1.1, cw * 0.10):
        return "center"
    # Very small right margin => right aligned.
    if right_margin <= max(0.8, cw * 0.06) and left_margin > right_margin:
        return "right"
    # Very small left margin => left aligned.
    if left_margin <= max(0.8, cw * 0.06) and right_margin > left_margin:
        return "left"

    return fallback


def _dominant_span(spans: list) -> dict:
    """Return the span with the most text (used for font/color metadata)."""
    if not spans:
        return {}
    return max(spans, key=lambda s: len((s.get("text") or "").strip()))


def _rect_intersection_ratio(a: Tuple[float, float, float, float],
                             b: Tuple[float, float, float, float]) -> float:
    ax0, ay0, ax1, ay1 = a
    bx0, by0, bx1, by1 = b
    ix0 = max(ax0, bx0)
    iy0 = max(ay0, by0)
    ix1 = min(ax1, bx1)
    iy1 = min(ay1, by1)
    iw = ix1 - ix0
    ih = iy1 - iy0
    if iw <= 0 or ih <= 0:
        return 0.0
    inter = iw * ih
    a_area = max(1e-6, (ax1 - ax0) * (ay1 - ay0))
    return inter / a_area


def _extract_table_cells(page) -> List[Tuple[float, float, float, float]]:
    """Return all detected table cell rectangles on a page (best effort)."""
    cells: List[Tuple[float, float, float, float]] = []
    try:
        tables = page.find_tables()
    except Exception:
        return cells

    if not tables:
        return cells

    for table in tables:
        for cb in (getattr(table, "cells", None) or []):
            if not cb or len(cb) < 4:
                continue
            x0, y0, x1, y1 = float(cb[0]), float(cb[1]), float(cb[2]), float(cb[3])
            if x1 - x0 < 1 or y1 - y0 < 1:
                continue
            cells.append((x0, y0, x1, y1))
    return cells


def _find_best_table_cell(
    line_bbox: Tuple[float, float, float, float],
    table_cells: List[Tuple[float, float, float, float]],
) -> Optional[Tuple[float, float, float, float]]:
    """Find the most suitable table-cell bbox for a line bbox."""
    if not table_cells:
        return None

    lx0, ly0, lx1, ly1 = line_bbox
    cx = (lx0 + lx1) * 0.5
    cy = (ly0 + ly1) * 0.5

    containing: List[Tuple[float, Tuple[float, float, float, float]]] = []
    for cell in table_cells:
        x0, y0, x1, y1 = cell
        if x0 - 0.8 <= cx <= x1 + 0.8 and y0 - 0.8 <= cy <= y1 + 0.8:
            area = max(1e-6, (x1 - x0) * (y1 - y0))
            containing.append((area, cell))
    if containing:
        containing.sort(key=lambda t: t[0])
        return containing[0][1]

    # Fallback by overlap ratio
    best = None
    best_ratio = 0.0
    for cell in table_cells:
        r = _rect_intersection_ratio(line_bbox, cell)
        if r > best_ratio:
            best_ratio = r
            best = cell
    if best_ratio >= 0.45:
        return best
    return None


def extract_page_blocks(page) -> List[PdfTextBlock]:
    """Extract text blocks from a PyMuPDF page with full metadata.

    Uses page.get_text("dict") which returns blocks → lines → spans with
    per-span bbox, font name, font size, flags (bold/italic), and color.

    Returns a list of PdfTextBlock objects, sorted top-to-bottom / left-to-right.
    """
    page_w = float(page.rect.width)
    page_h = float(page.rect.height)

    page_dict = page.get_text("dict", flags=0)
    blocks_raw = page_dict.get("blocks") or []

    # Collect all font sizes for median calculation (used for title detection).
    all_sizes: List[float] = []
    for blk in blocks_raw:
        if blk.get("type") != 0:
            continue
        for ln in blk.get("lines") or []:
            for sp in ln.get("spans") or []:
                sz = float(sp.get("size") or 10.0)
                if sz > 0:
                    all_sizes.append(sz)

    all_sizes.sort()
    median_size = all_sizes[len(all_sizes) // 2] if all_sizes else 10.0

    result: List[PdfTextBlock] = []
    table_cells = _extract_table_cells(page)

    for blk in blocks_raw:
        if blk.get("type") != 0:          # 0 = text block, 1 = image block
            continue
        for ln in blk.get("lines") or []:
            spans = ln.get("spans") or []
            if not spans:
                continue

            # Join span texts to form line text
            line_text = "".join((sp.get("text") or "") for sp in spans)
            if not line_text.strip():
                continue

            bbox_raw = ln.get("bbox")
            if not bbox_raw or len(bbox_raw) < 4:
                continue
            x0, y0, x1, y1 = float(bbox_raw[0]), float(bbox_raw[1]), float(bbox_raw[2]), float(bbox_raw[3])

            # Skip degenerate bboxes
            if x1 - x0 < 1 or y1 - y0 < 0.5:
                continue

            # Dominant span for style info
            dom = _dominant_span(spans)
            font_name = (dom.get("font") or "Helvetica").strip() or "Helvetica"
            font_size = max(4.0, float(dom.get("size") or 10.0))
            flags     = int(dom.get("flags") or 0)
            color_int = int(dom.get("color") or 0)

            # Exact baseline origin from first span (span["origin"] = (x, y) where y is baseline)
            first_origin = spans[0].get("origin") or (x0, y1)
            origin_x = float(first_origin[0])
            origin_y = float(first_origin[1])

            # Bold / italic from flags (bit 16 = bold, bit 2 = italic)
            if flags:
                is_bold   = bool(flags & 16)
                is_italic = bool(flags & 2)
            else:
                fn_lower  = font_name.lower()
                is_bold   = "bold" in fn_lower
                is_italic = "italic" in fn_lower or "oblique" in fn_lower

            color = _int_color_to_rgb(color_int)
            # Treat near-white as black (invisible text rendered on white bg)
            if color[0] > 0.94 and color[1] > 0.94 and color[2] > 0.94:
                color = (0.0, 0.0, 0.0)

            alignment = _detect_alignment(x0, x1, page_w)
            cell_bbox = _find_best_table_cell((x0, y0, x1, y1), table_cells)
            if cell_bbox:
                alignment = _detect_alignment_in_cell(x0, x1, cell_bbox, fallback=alignment)

            result.append(PdfTextBlock(
                text=line_text,
                bbox=(x0, y0, x1, y1),
                font_name=font_name,
                font_size=font_size,
                is_bold=is_bold,
                is_italic=is_italic,
                color=color,
                alignment=alignment,
                page_width=page_w,
                page_height=page_h,
                is_table_cell=bool(cell_bbox),
                table_bbox=cell_bbox,
                origin_x=origin_x,
                origin_y=origin_y,
                raw_spans=spans,
            ))

    # Sort reading order: top-to-bottom, then left-to-right
    result.sort(key=lambda b: (round(b.bbox[1] / 3) * 3, b.bbox[0]))
    return result


_INLINE_SYMBOL_MAP = {
    "\uf02b": "+",
    "\uf0b7": "•",
    "\uf0a7": "§",
}


def _normalize_inline_symbol_text(text: str) -> str:
    """Normalize list-prefix glyphs to visible Unicode/ASCII for inline mode.

    PyMuPDF often extracts SymbolMT bullets as private-use glyphs like `\uf02b`.
    If we redraw those glyphs using a text font, they become tofu squares. In
    inline mode the line can move vertically, so these prefixes must be redrawn
    with the line rather than preserved at the old coordinates.
    """
    s = str(text or "")
    if not s:
        return s
    for old, new in _INLINE_SYMBOL_MAP.items():
        s = s.replace(old, new)
    s = re.sub(r"^(\s*)[\uE000-\uF8FF]+", r"\1" + "\u2022", s)
    s = re.sub(r"^(\s*[\u2022+\-*◦o])(\S)", r"\1 \2", s)
    return s


def reorder_inline_row_blocks(blocks: List[PdfTextBlock], *, y_tol: float = 4.5) -> List[PdfTextBlock]:
    """Reorder same-row inline blocks left-to-right.

    Some PDFs extract the content span slightly above its bullet prefix span,
    causing the row to be ordered as text first, bullet second. Reordering small
    same-band clusters by x allows prefix merging to work deterministically.
    """
    if len(blocks) < 2:
        return blocks

    ordered: List[PdfTextBlock] = []
    i = 0
    while i < len(blocks):
        cluster = [blocks[i]]
        base_mid = (blocks[i].bbox[1] + blocks[i].bbox[3]) * 0.5
        j = i + 1
        while j < len(blocks):
            mid = (blocks[j].bbox[1] + blocks[j].bbox[3]) * 0.5
            if abs(mid - base_mid) > y_tol:
                break
            cluster.append(blocks[j])
            j += 1
        cluster.sort(key=lambda b: (b.bbox[0], b.bbox[1]))
        ordered.extend(cluster)
        i = j
    return ordered


def merge_inline_prefix_blocks(blocks: List[PdfTextBlock]) -> List[PdfTextBlock]:
    """Merge standalone bullet/prefix blocks with the following text block.

    Some PDFs extract list markers as separate blocks, for example:
      SymbolMT `\uf02b` at x=126, text block at x=144 on the same row.
    In inline bilingual mode later lines can move vertically, so keeping the
    prefix block at its original y-position causes visual overlap and lost list
    structure. We merge the prefix into the following text block so the whole
    line moves together.
    """
    if len(blocks) < 2:
        return blocks

    merged: List[PdfTextBlock] = []
    i = 0
    while i < len(blocks):
        cur = blocks[i]
        if i + 1 >= len(blocks):
            cur.text = _normalize_inline_symbol_text(cur.text)
            merged.append(cur)
            break

        nxt = blocks[i + 1]
        cur_text = str(cur.text or "").strip()
        nxt_text = str(nxt.text or "").strip()
        cur_mid = (cur.bbox[1] + cur.bbox[3]) * 0.5
        nxt_mid = (nxt.bbox[1] + nxt.bbox[3]) * 0.5
        cur_h = max(1.0, cur.bbox[3] - cur.bbox[1])
        nxt_h = max(1.0, nxt.bbox[3] - nxt.bbox[1])
        same_band = abs(cur_mid - nxt_mid) <= max(cur_h, nxt_h) * 0.65
        gap = nxt.bbox[0] - cur.bbox[2]

        is_prefix = False
        if cur_text and nxt_text and not nxt.is_table_cell:
            if _is_symbol_font(cur.font_name) and not _has_letters(cur_text):
                is_prefix = True
            elif bool(_PRIVATE_USE_RE.search(cur_text)):
                is_prefix = True
            elif len(cur_text) <= 2 and cur_text in ("+", "-", "*", "•", "◦", "o"):
                is_prefix = True

        if is_prefix and same_band and -1.0 <= gap <= 24.0 and _has_letters(nxt_text):
            prefix = _normalize_inline_symbol_text(cur_text).strip()
            body = _normalize_inline_symbol_text(nxt.text).lstrip()
            joiner = " " if prefix and body and not body.startswith((":", ")", "]", "}")) else ""
            merged_text = f"{prefix}{joiner}{body}" if prefix else body
            merged.append(PdfTextBlock(
                text=merged_text,
                bbox=(
                    min(cur.bbox[0], nxt.bbox[0]),
                    min(cur.bbox[1], nxt.bbox[1]),
                    max(cur.bbox[2], nxt.bbox[2]),
                    max(cur.bbox[3], nxt.bbox[3]),
                ),
                font_name=nxt.font_name,
                font_size=nxt.font_size,
                is_bold=nxt.is_bold,
                is_italic=nxt.is_italic,
                color=nxt.color,
                alignment=nxt.alignment,
                page_width=nxt.page_width,
                page_height=nxt.page_height,
                is_table_cell=nxt.is_table_cell,
                table_bbox=nxt.table_bbox,
                origin_x=cur.origin_x if cur.origin_x else nxt.origin_x,
                origin_y=nxt.origin_y,
                raw_spans=list(cur.raw_spans or []) + list(nxt.raw_spans or []),
            ))
            i += 2
            continue

        cur.text = _normalize_inline_symbol_text(cur.text)
        merged.append(cur)
        i += 1

    return merged


# ─────────────────────────────────────────────────────────────────────────────
# Phase 1b — Block merging (consecutive lines → paragraph units)
# ─────────────────────────────────────────────────────────────────────────────

def merge_paragraph_blocks(blocks: List[PdfTextBlock]) -> List[PdfTextBlock]:
    """Merge consecutive single-line blocks into paragraph units.

    Lines are merged when:
      - They share a similar left margin (same column)
      - They are vertically close (< 1.6× line height apart)
      - Neither ends with sentence-terminating punctuation
      - Both have the same bold/italic/font-size style

    Merging improves translation quality by giving the model more context.
    The merged block's bbox spans the union of all member lines.
    """
    if len(blocks) < 2:
        return blocks

    _LONE_BULLET_RE = re.compile(
        r"^\s*[o\u2022\u2023\u25E6\u2043\u2219\u25CF\u25CB\u25A0\u25A1·•◦‣⁃]\s*$",
        re.UNICODE,
    )

    def _is_lone_bullet(b: PdfTextBlock) -> bool:
        t = (b.text or "").strip()
        return len(t) <= 2 and bool(_LONE_BULLET_RE.match(t))

    _MAX_MERGE = 12

    _MERGE_LIST_START_RE = re.compile(
        r"^\s*(?:[\u2022\u2023\u25E6\u2043\u2219\u25CF\u25CB\u25A0\u25A1·•◦‣⁃+\-\*]"
        r"|\d{1,3}[.)]\s|[a-zA-Z][.)]\s|[ivxIVX]{1,4}[.)]\s)",
        re.UNICODE,
    )

    def _continues(text: str) -> bool:
        t = (text or "").strip()
        if not t:
            return False
        # Single trailing period = sentence end (not ellipsis like "..." or "…")
        if t.endswith('.') and not t.endswith('..'):
            return False
        # Strip ellipsis / decorative dots for further checks
        t = re.sub(r"[.\s…·]+$", "", t)
        if not t:
            return False
        return t[-1] not in '?!;:)»"\'"'

    def _ends_with_comma(text: str) -> bool:
        t = (text or "").rstrip()
        return bool(t) and t[-1] == ','

    merged: List[PdfTextBlock] = []
    i = 0
    while i < len(blocks):
        base = blocks[i]
        # Standalone bullet blocks stay separate; don't start a merge group.
        if _is_lone_bullet(base):
            merged.append(base)
            i += 1
            continue
        group = [base]
        skipped_bullets: List[PdfTextBlock] = []
        j = i + 1
        while j < len(blocks) and len(group) < _MAX_MERGE:
            nxt = blocks[j]
            # Skip lone bullet blocks — don't break the merge chain.
            if _is_lone_bullet(nxt):
                skipped_bullets.append(nxt)
                j += 1
                continue
            prev = group[-1]

            # Style must match
            if nxt.is_bold != prev.is_bold or nxt.is_italic != prev.is_italic:
                break
            if abs((nxt.font_size or 10) - (prev.font_size or 10)) > 2.0:
                break

            # Vertical proximity — use last content block, skipping bullets.
            v_gap = nxt.bbox[1] - prev.bbox[3]
            line_h = prev.font_size or 10.0
            max_gap = line_h * (2.0 if _continues(prev.text) else 1.4)
            if v_gap < -2 or v_gap > max_gap:
                break

            # Left-margin alignment
            # When previous line ends with comma, skip margin check (indented
            # continuation of attribute/parameter lists).
            # But never merge across list-item boundaries when previous sentence
            # is complete (doesn't continue).
            nxt_text = (nxt.text or "").lstrip()
            if _MERGE_LIST_START_RE.match(nxt_text):
                if not _continues(prev.text):
                    break  # complete sentence + new list item = separate
                if abs(nxt.bbox[0] - base.bbox[0]) > line_h * 2.5:
                    break
            elif not _ends_with_comma(prev.text):
                if abs(nxt.bbox[0] - base.bbox[0]) > line_h * 2.5:
                    break

            group.append(nxt)
            j += 1

        if len(group) == 1:
            merged.append(base)
        else:
            merged_text = " ".join(b.text.strip() for b in group if b.text.strip())
            merged_bbox = (
                min(b.bbox[0] for b in group),
                group[0].bbox[1],
                max(b.bbox[2] for b in group),
                group[-1].bbox[3],
            )
            all_spans: list = []
            for b in group:
                all_spans.extend(b.raw_spans)
            merged.append(PdfTextBlock(
                text=merged_text,
                bbox=merged_bbox,
                font_name=base.font_name,
                font_size=base.font_size,
                is_bold=base.is_bold,
                is_italic=base.is_italic,
                color=base.color,
                alignment=base.alignment,
                page_width=base.page_width,
                page_height=base.page_height,
                is_table_cell=any(g.is_table_cell for g in group),
                table_bbox=base.table_bbox,
                raw_spans=all_spans,
            ))
        # Re-insert any skipped bullet blocks after the merged group
        merged.extend(skipped_bullets)
        i = j

    return merged


# ─────────────────────────────────────────────────────────────────────────────
# Phase 2 — Translation helpers
# ─────────────────────────────────────────────────────────────────────────────

_DOTTED = re.compile(r"\.{3,}|_{3,}|-{5,}|…{2,}")
_LIST_PREFIX = re.compile(
    r"^(\s*(?:"
    r"[\u2022\u2023\u25E6\u2043\u2219\u25CF\u25CB\u25A0\u25A1·•◦‣⁃]"
    r"|\-\s|\*\s"
    r"|\d{1,3}[.)]\s"
    r"|[a-zA-Z][.)]\s"
    r"|[ivxIVX]{1,4}[.)]\s"
    r"))",
    re.UNICODE,
)

_FUNC_CALL_TOKEN_RE = re.compile(r"^\s*[A-Za-z_][A-Za-z0-9_]*\s*\([^()\n]*\)\s*[;.,:]?\s*$")
_CODE_IDENT_TOKEN_RE = re.compile(
    r"^\s*(?:"
    r"[A-Za-z_]*_[A-Za-z0-9_]+"
    r"|[a-z]+[A-Z][A-Za-z0-9_]*"
    r"|[A-Z][a-z0-9]+[A-Z]{2,}[A-Za-z0-9_]*"
    r")\s*$"
)
_SHORT_ABBR_RE = re.compile(r"^\s*[A-Z]{2,5}\.?\s*$")
_NO_TOKEN_RE = re.compile(r"^\s*(?:No\.?|NO\.?|Stt\.?|STT\.?)\s*$")

# ── Code-dominated block detection ──────────────────────────────────────
# Matches camelCase / PascalCase / snake_case identifiers followed by '('
_CODE_IDENT_CALL_RE = re.compile(
    r"(?:"
    r"[a-z]+[A-Z]\w*"                # camelCase: maSach, themSach
    r"|[A-Z][a-z]+(?:[A-Z]\w*)+"     # PascalCase: KhachHang, DangKyTaiKhoan
    r"|[A-Za-z]+_\w+"                # snake_case: ma_sach
    r")\s*\("
)
# camelCase/PascalCase identifier with type annotation in parentheses
_TYPE_ANNOTATED_IDENT_RE = re.compile(
    r"(?:"
    r"[a-z]+[A-Z]\w*"
    r"|[A-Z][a-z]+(?:[A-Z]\w*)+"
    r"|[A-Za-z]+_\w+"
    r")\s*\(\s*(?:PK|FK|string|int|float|boolean|date|datetime"
    r"|nvarchar|varchar|char|text|integer|bigint|smallint)\b",
    re.IGNORECASE,
)
# Lines starting with "+ ClassName:" in a class-listing block
_CLASS_LISTING_LINE_RE = re.compile(
    r"^\s*[+\-\u2022\uf02b\uf0b7o]\s*"
    r"(?:[A-Z][a-z]+(?:[A-Z][a-zA-Z]*)*"   # PascalCase: Sach, DocGia
    r"|[a-z]+[A-Z][a-zA-Z]*"               # camelCase:  docGia
    r")\s*:",
    re.MULTILINE,
)
# PascalCase / camelCase identifier (standalone, no diacritics)
_PASCAL_IDENT_RE = re.compile(
    r"^(?:[A-Z][a-z]+(?:[A-Z][a-zA-Z0-9]*)*"
    r"|[a-z]+[A-Z][a-zA-Z0-9]*"
    r"|[A-Za-z]+_[A-Za-z0-9_]+)$"
)
# Bullet prefix characters to strip when checking identifiers
_BULLET_PREFIX_RE = re.compile(r"^\s*[+\-\u2022\uf02b\uf0b7o]\s*")
# PascalCase/camelCase with 2+ uppercase letters (reliable code identifier)
_MULTI_CAP_IDENT_RE = re.compile(
    r"(?<![a-zA-Z\u00C0-\u1EF9])"
    r"(?:[A-Z][a-z]+(?:[A-Z][a-zA-Z0-9]*)+"    # PascalCase: DocGia, KhachHang
    r"|[a-z]+[A-Z][a-zA-Z0-9]*"                 # camelCase:  maSach, tinhTrang
    r")"
    r"(?![a-zA-Z\u00C0-\u1EF9])")
_SQL_TYPE_RE = re.compile(
    r"^\s*(?:"
    r"N?VARCHAR|CHAR|TEXT|INT|INTEGER|BIGINT|SMALLINT|TINYINT"
    r"|DATE|DATETIME|TIMESTAMP|BOOLEAN|BOOL|FLOAT|DOUBLE|DECIMAL"
    r")\s*(?:\(\s*\d+(?:\s*,\s*\d+)?\s*\))?\s*$",
    re.IGNORECASE,
)
_DB_CONSTRAINT_RE = re.compile(
    r"^\s*(?:PRIMARY\s*KEY|FOREIGN\s*KEY|NOT\s*NULL|NULL|UNIQUE|PK|FK)\s*$",
    re.IGNORECASE,
)
_NONTRANSLATABLE_TERMS = {
    "pk", "fk", "id", "mssv", "msv", "stt", "null", "n/a", "na",
    "sql", "api", "url", "uuid", "dob", "no", "no.",
}


def _canonical_table_constraint(text: str) -> Optional[str]:
    """Return canonical DB-constraint text for short table-cell tokens.

    This avoids unstable model outputs like "No NULL" and keeps technical
    constraints visually consistent.  Also handles common Vietnamese database
    table headers.
    """
    t = re.sub(r"\s+", " ", (text or "").strip())
    if not t:
        return None
    k = t.lower()

    if k == "pk":
        return "PK"
    if k == "fk":
        return "FK"
    if k in ("primary key", "primarykey"):
        return "Primary Key"
    if k in ("foreign key", "foreignkey"):
        return "Foreign Key"
    if k in ("unique",):
        return "Unique"
    if k in ("null",):
        return "NULL"
    if re.fullmatch(r"(?:not|no)\s*null", k):
        return "Not NULL"

    # Common Vietnamese DB table headers
    if k in ("tên trường", "tên trƣờng", "tenruong"):
        return "Field name"
    if k in ("kiểu dữ liệu", "kiểu dữliệu", "kiểu dl"):
        return "Data type"
    if k in ("ràng buộc",):
        return "Constraint"

    return None


def _is_nonsemantic_token(core: str, *, table_cell: bool = False) -> bool:
    t = (core or "").strip()
    if not t:
        return True
    low = t.lower()

    if _FUNC_CALL_TOKEN_RE.fullmatch(t):
        return True
    if _CODE_IDENT_TOKEN_RE.fullmatch(t):
        return True
    if low in _NONTRANSLATABLE_TERMS:
        return True
    if _NO_TOKEN_RE.fullmatch(t):
        return True
    if _SHORT_ABBR_RE.fullmatch(t):
        return True
    if _SQL_TYPE_RE.fullmatch(t):
        return True
    if _DB_CONSTRAINT_RE.fullmatch(t):
        return True

    # After stripping bullet prefix, check if remainder is a code identifier
    stripped = _BULLET_PREFIX_RE.sub("", t).strip()
    if stripped and stripped != t and _PASCAL_IDENT_RE.fullmatch(stripped):
        return True  # e.g. "+ Sach", "+ DocGia", "+ PhieuMuon"

    if table_cell:
        # Alphanumeric code-like tokens inside table cells (e.g. maMon, DH22TIN07, CODE001)
        if re.fullmatch(r"[A-Za-z0-9_]{2,24}", t):
            has_digit = any(ch.isdigit() for ch in t)
            has_upper = any(ch.isupper() for ch in t)
            has_lower = any(ch.islower() for ch in t)
            is_title_case_word = len(t) > 1 and t[0].isupper() and t[1:].islower()
            if has_digit or (has_upper and has_lower and not is_title_case_word):
                return True

    return False


def _is_code_dominated_block(text: str) -> bool:
    """Return True when a multi-token block is dominated by code identifiers.

    Catches blocks like:
      "Hành vi: themSach (), capNhatSach (), xoaSach ()"
      "Thuộc tính: maSach (PK, string), tenSach (string), ..."
      "+ Sach: thực thể chính...\n+ DocGia: người mượn sách...\n..."
    """
    # ≥2 code-style identifier calls → function/method listing
    if len(_CODE_IDENT_CALL_RE.findall(text)) >= 2:
        return True
    # ≥2 identifiers with type annotations → attribute listing
    if len(_TYPE_ANNOTATED_IDENT_RE.findall(text)) >= 2:
        return True
    # ≥2 lines starting with "+ ClassName:" → class listing
    if len(_CLASS_LISTING_LINE_RE.findall(text)) >= 2:
        return True
    # ≥2 PascalCase/camelCase identifiers (2+ caps) anywhere in block
    # e.g. "ThanhToan include XacNhanDonHang", "KhachHang và DonHang: ..."
    if len(_MULTI_CAP_IDENT_RE.findall(text)) >= 2:
        return True
    return False


def _should_translate(text: str) -> bool:
    """Return True when the text block should be sent to the AI for translation."""
    if not text or not text.strip():
        return False
    core = text.strip()
    # Pure numbers / symbols — skip
    if re.fullmatch(r"[\d\W_]+", core, flags=re.UNICODE):
        return False
    # Need at least 2 letter characters
    letters = re.findall(r"[A-Za-zÀ-ỹ]", core)
    return len(letters) >= 2


def _preserve_translate(text: str, translate_fn: Callable[[str], str]) -> str:
    """Translate while preserving structural elements (dots, list prefix, etc.)."""
    # Preserve list bullet / number prefix
    m = _LIST_PREFIX.match(text)
    if m:
        prefix = m.group(1)
        body   = text[m.end():]
        if _should_translate(body):
            return prefix + translate_fn(body.strip())
        return text

    # Preserve dotted placeholders: translate labels, keep dot runs
    if _DOTTED.search(text):
        parts  = _DOTTED.split(text)
        seps   = _DOTTED.findall(text)
        result = []
        for idx, part in enumerate(parts):
            if idx > 0:
                result.append(seps[idx - 1])
            if part.strip() and _should_translate(part):
                result.append(translate_fn(part.strip()))
            else:
                result.append(part)
        return "".join(result)

    return translate_fn(text.strip())


# ─────────────────────────────────────────────────────────────────────────────
# Font resolution for rendering
# ─────────────────────────────────────────────────────────────────────────────

_FONT_DIR_CACHE: Optional[str] = None

def _get_font_dir() -> str:
    global _FONT_DIR_CACHE
    if _FONT_DIR_CACHE is not None:
        return _FONT_DIR_CACHE
    env = os.environ.get("FONT_DIR", "").strip()
    if env and os.path.isdir(env):
        _FONT_DIR_CACHE = env
        return env
    for candidate in (r"C:\Windows\Fonts", "/usr/share/fonts/truetype", "/usr/share/fonts"):
        if os.path.isdir(candidate):
            _FONT_DIR_CACHE = candidate
            return candidate
    _FONT_DIR_CACHE = ""
    return ""


# Map normalized PDF font name fragments → (filename_regular, filename_bold, filename_italic, filename_bolditalic)
_FONT_FAMILY_MAP: List[Tuple[str, str, str, str, str]] = [
    # (key_fragment,       regular,      bold,         italic,       bolditalic)
    ("timesnewroman",      "times.ttf",  "timesbd.ttf","timesi.ttf", "timesbi.ttf"),
    ("couriernew",         "cour.ttf",   "courbd.ttf", "couri.ttf",  "courbi.ttf"),
    ("courier",            "cour.ttf",   "courbd.ttf", "couri.ttf",  "courbi.ttf"),
    ("symbol",             "symbol.ttf", "symbol.ttf", "symbol.ttf", "symbol.ttf"),
    ("calibri",            "calibri.ttf","calibrib.ttf","calibrii.ttf","calibriz.ttf"),
    ("cambria",            "cambria.ttc","cambriab.ttf","cambriai.ttf","cambriaz.ttf"),
    ("georgia",            "georgia.ttf","georgiab.ttf","georgiai.ttf","georgiaz.ttf"),
    ("garamond",           "GARA.TTF",   "GARABD.TTF", "GARAIT.TTF", "GARAIT.TTF"),
    ("palatino",           "pala.ttf",   "palab.ttf",  "palai.ttf",  "palabi.ttf"),
    ("arial",              "arial.ttf",  "arialbd.ttf","ariali.ttf", "arialbi.ttf"),
    ("helvetica",          "arial.ttf",  "arialbd.ttf","ariali.ttf", "arialbi.ttf"),
    ("verdana",            "verdana.ttf","verdanab.ttf","verdanai.ttf","verdanaz.ttf"),
    ("tahoma",             "tahoma.ttf", "tahomabd.ttf","tahoma.ttf","tahomabd.ttf"),
    ("trebuchet",          "trebuc.ttf", "trebucbd.ttf","trebucit.ttf","trebucbi.ttf"),
    ("consolas",           "consola.ttf","consolab.ttf","consolai.ttf","consolaz.ttf"),
    ("dejavusans",         "arial.ttf",  "arialbd.ttf","ariali.ttf", "arialbi.ttf"),
]


def _normalize_font_name(name: str) -> str:
    """Strip variant suffixes and normalize to lowercase for matching."""
    n = name.lower()
    # Strip common PDF font name suffixes
    for suffix in ("-boldmt", "-bolditalicmt", "-italicmt", "-boldobliquemt",
                   "psmt", "-bold", "-italic", "-oblique", "-regular", "mt",
                   "ps", " bold", " italic", " regular", ",bold", ",italic"):
        n = n.replace(suffix, "")
    # Remove non-alphanumeric
    n = re.sub(r"[^a-z0-9]", "", n)
    return n


_FONT_FILE_CACHE: Dict[Tuple[str, bool, bool], Optional[str]] = {}


def _find_ttf_for_font(pdf_font_name: str, is_bold: bool, is_italic: bool) -> Optional[str]:
    """Find the best matching TTF file for a PDF font name and style.

    First tries to match by font family name, then falls back to generic
    bold/italic style lookup (Arial/DejaVu).
    """
    key = (pdf_font_name, is_bold, is_italic)
    if key in _FONT_FILE_CACHE:
        return _FONT_FILE_CACHE[key]

    font_dir = _get_font_dir()
    norm = _normalize_font_name(pdf_font_name)

    result: Optional[str] = None
    for frag, reg, bold, italic, bolditalic in _FONT_FAMILY_MAP:
        if frag in norm:
            if is_bold and is_italic:
                candidates = [bolditalic, bold, italic, reg]
            elif is_bold:
                candidates = [bold, bolditalic, reg]
            elif is_italic:
                candidates = [italic, bolditalic, reg]
            else:
                candidates = [reg]
            for fname in candidates:
                path = os.path.join(font_dir, fname)
                if os.path.isfile(path):
                    result = path
                    break
                # Try case-insensitive in font_dir
                try:
                    for f in os.listdir(font_dir):
                        if f.lower() == fname.lower():
                            result = os.path.join(font_dir, f)
                            break
                except Exception:
                    pass
                if result:
                    break
            if result:
                break

    # Fallback: generic style lookup (Arial / DejaVu)
    if result is None:
        result = _find_ttf(is_bold, is_italic)

    _FONT_FILE_CACHE[key] = result
    return result


_FONT_LOOKUP: Dict[Tuple[bool, bool], Optional[str]] = {}

_FONT_VARIANTS = {
    # Windows
    (False, False): ["arial.ttf"],
    (True,  False): ["arialbd.ttf"],
    (False, True):  ["ariali.ttf"],
    (True,  True):  ["arialbi.ttf"],
}
_FONT_VARIANTS_LINUX = {
    (False, False): ["DejaVuSans.ttf"],
    (True,  False): ["DejaVuSans-Bold.ttf"],
    (False, True):  ["DejaVuSans-Oblique.ttf"],
    (True,  True):  ["DejaVuSans-BoldOblique.ttf"],
}

_LETTER_RE = re.compile(r"[A-Za-zÀ-ỹ]")
_PRIVATE_USE_RE = re.compile(r"[\uE000-\uF8FF]")
_LEADING_SYMBOL_RE = re.compile(r"^\s*[\uE000-\uF8FF□☐☑✓✔▪▫■●○◦◆◇\u2022\+\-\*]+\s*")


def _has_letters(text: str) -> bool:
    return bool(_LETTER_RE.search(text or ""))


def _is_symbol_font(font_name: str) -> bool:
    name = (font_name or "").lower()
    return any(k in name for k in ("symbol", "wingdings", "webdings", "zapfdingbats", "dingbats"))


def _strip_preserved_prefix(translated: str, prefix_text: str) -> str:
    """Remove an already-preserved symbol prefix from translated text."""
    out = str(translated or "")
    pref = str(prefix_text or "").strip()
    if pref:
        lout = out.lstrip()
        if lout.startswith(pref):
            lout = lout[len(pref):]
            return lout.lstrip()
    # If model echoed private-use/symbol prefix in a different form, strip it.
    return _LEADING_SYMBOL_RE.sub("", out, count=1).lstrip()


def _find_ttf(is_bold: bool, is_italic: bool) -> Optional[str]:
    """Find a suitable Unicode-capable TTF font file for the given style."""
    key = (is_bold, is_italic)
    if key in _FONT_LOOKUP:
        return _FONT_LOOKUP[key]

    font_dir = _get_font_dir()
    if not font_dir:
        _FONT_LOOKUP[key] = None
        return None

    candidates: List[str] = []
    for table in (_FONT_VARIANTS, _FONT_VARIANTS_LINUX):
        candidates.extend(table.get(key, []))

    for name in candidates:
        path = os.path.join(font_dir, name)
        if os.path.isfile(path):
            _FONT_LOOKUP[key] = path
            return path
        # Search sub-directories
        for root, _dirs, files in os.walk(font_dir):
            if name in files:
                found = os.path.join(root, name)
                _FONT_LOOKUP[key] = found
                return found

    # Fallback to any regular font
    if key != (False, False):
        reg = _find_ttf(False, False)
        _FONT_LOOKUP[key] = reg
        return reg

    _FONT_LOOKUP[key] = None
    return None


def render_blocks_on_page(
    fitz_module,
    page,
    blocks: List[PdfTextBlock],
    translations: Dict[int, str],
    *,
    inline_mode: bool = False,
    inline_overflow_out: Optional[List] = None,
) -> None:
    """Phase 3: Redact original text and insert translations at the same positions.

    Approach:
      3a. mark redaction rects (white fill) over each block to translate
      3b. apply_redactions — removes original text, keeps images/graphics
      3c. register needed TTF fonts with page.insert_font() (correct embedding)
          then page.insert_text() at exact span baseline origin

    Args:
        fitz_module:  The imported PyMuPDF module.
        page:         PyMuPDF page object (mutated in-place).
        blocks:       Extracted blocks for this page (one per original line).
        translations: Mapping {block_index → translated_text}.
    """
    fitz = fitz_module
    pr   = page.rect
    table_cells_to_clear: Dict[Tuple[float, float, float, float], Tuple[float, float, float, float]] = {}
    table_cells_precise_clear: Set[Tuple[float, float, float, float]] = set()
    table_span_clear_rects: List[Tuple[float, float, float, float]] = []
    redact_count = 0

    # Lines often start with symbol-font bullets (e.g. SymbolMT private-use glyphs).
    # Preserve that prefix on the page and redraw only the textual body.
    preserved_prefix: Dict[int, Dict[str, Tuple[float, float] | str]] = {}
    if not inline_mode:
        for i, block in enumerate(blocks):
            if i not in translations:
                continue
            spans = block.raw_spans or []
            if len(spans) < 2:
                continue
            first = spans[0]
            first_text = (first.get("text") or "")
            first_font = (first.get("font") or "")
            rest_text = "".join((sp.get("text") or "") for sp in spans[1:])
            is_symbol_prefix = (
                bool(first_text.strip())
                and not _has_letters(first_text)
                and _has_letters(rest_text)
                and (_is_symbol_font(first_font) or bool(_PRIVATE_USE_RE.search(first_text)))
            )
            if not is_symbol_prefix:
                continue
            body_origin = spans[1].get("origin")
            if body_origin and len(body_origin) >= 2:
                preserved_prefix[i] = {
                    "prefix": first_text.strip(),
                    "body_origin": (float(body_origin[0]), float(body_origin[1])),
                }

    # ── Step 3a: Add redaction annotations for all blocks to translate ────
    # In inline mode, also redact non-translated non-table blocks so that when
    # the accumulated y-shift pushes content down, the original text at the old
    # position is cleared and can be re-rendered at the shifted position in step 3c.
    _inline_needs_rerender: Set[int] = set()
    for i, block in enumerate(blocks):
        if i not in translations:
            if inline_mode and not block.is_table_cell:
                # Redact non-translated blocks so expanded bilingual text above
                # doesn't overlap them; they'll be re-rendered shifted in step 3c.
                bx0, by0, bx1, by1 = block.bbox
                _redacted_ok = False
                # Per-span redaction preserves accuracy
                spans = block.raw_spans or []
                for sp in spans:
                    sp_text = (sp.get("text") or "")
                    if not sp_text.strip():
                        continue
                    sb = sp.get("bbox")
                    if not sb or len(sb) < 4:
                        continue
                    sx0, sy0, sx1, sy1 = float(sb[0]), float(sb[1]), float(sb[2]), float(sb[3])
                    srect = fitz.Rect(sx0 - 0.2, sy0 - 0.3, sx1 + 0.2, sy1 + 0.3) & pr
                    if srect.is_empty:
                        continue
                    try:
                        page.add_redact_annot(srect, fill=(1, 1, 1))
                        redact_count += 1
                        _redacted_ok = True
                    except Exception:
                        pass
                if _redacted_ok:
                    _inline_needs_rerender.add(i)
            elif inline_mode and block.is_table_cell:
                # Clear non-translated table cell text using precise span rects
                # (same approach as translated cells) so _relocate_table_group
                # doesn't need a big white-fill that would erase shifted text above.
                tb = block.table_bbox or block.bbox
                spans = block.raw_spans or []
                for sp in spans:
                    sp_text = (sp.get("text") or "")
                    if not sp_text.strip():
                        continue
                    sb = sp.get("bbox")
                    if not sb or len(sb) < 4:
                        continue
                    sx0, sy0, sx1, sy1 = float(sb[0]), float(sb[1]), float(sb[2]), float(sb[3])
                    tx0, ty0, tx1, ty1 = tb
                    cell_w = max(1.0, tx1 - tx0)
                    cell_h = max(1.0, ty1 - ty0)
                    border_guard_x = min(2.4, max(0.9, cell_w * 0.055))
                    border_guard_y = min(2.2, max(0.8, cell_h * 0.16))
                    pad_x = min(0.14, max(0.02, (sx1 - sx0) * 0.02))
                    pad_y = min(0.22, max(0.05, (sy1 - sy0) * 0.10))
                    cx0 = max(tx0 + border_guard_x, sx0 + pad_x)
                    cy0 = max(ty0 + border_guard_y, sy0 - pad_y)
                    cx1 = min(tx1 - border_guard_x, sx1 - pad_x)
                    cy1 = min(ty1 - border_guard_y, sy1 + pad_y)
                    if cx1 - cx0 < 0.4 or cy1 - cy0 < 0.3:
                        continue
                    table_span_clear_rects.append((cx0, cy0, cx1, cy1))
            continue
        if block.is_table_cell and block.table_bbox:
            tb = block.table_bbox
            key = (round(tb[0], 2), round(tb[1], 2), round(tb[2], 2), round(tb[3], 2))
            table_cells_to_clear[key] = tb
            # For table text, avoid redaction (can damage borders). Instead, record
            # precise span-area wipes bounded within the cell.
            spans = block.raw_spans or []
            precise = False
            for sp_idx, sp in enumerate(spans):
                sp_text = (sp.get("text") or "")
                if not sp_text.strip():
                    continue
                if i in preserved_prefix and sp_idx == 0:
                    continue
                sb = sp.get("bbox")
                if not sb or len(sb) < 4:
                    continue
                sx0, sy0, sx1, sy1 = float(sb[0]), float(sb[1]), float(sb[2]), float(sb[3])
                tx0, ty0, tx1, ty1 = tb

                # Keep clear rectangles safely away from table borders to avoid
                # visible border gaps ("mất nét") after redraw.
                cell_w = max(1.0, tx1 - tx0)
                cell_h = max(1.0, ty1 - ty0)
                border_guard_x = min(2.4, max(0.9, cell_w * 0.055))
                border_guard_y = min(2.2, max(0.8, cell_h * 0.16))

                # X-axis: keep conservative contraction to avoid touching borders.
                # Y-axis: allow a small expansion to remove diacritic leftovers.
                pad_x = min(0.14, max(0.02, (sx1 - sx0) * 0.02))
                pad_y = min(0.22, max(0.05, (sy1 - sy0) * 0.10))
                cx0 = max(tx0 + border_guard_x, sx0 + pad_x)
                cy0 = max(ty0 + border_guard_y, sy0 - pad_y)
                cx1 = min(tx1 - border_guard_x, sx1 - pad_x)
                cy1 = min(ty1 - border_guard_y, sy1 + pad_y)
                if cx1 - cx0 < 0.4 or cy1 - cy0 < 0.3:
                    continue
                table_span_clear_rects.append((cx0, cy0, cx1, cy1))
                precise = True
            if precise:
                table_cells_precise_clear.add(key)
            # Important: skip redaction on table text to avoid wiping border line-art.
            continue

        if inline_mode:
            bx0, by0, bx1, by1 = block.bbox
            redact_rect = fitz.Rect(bx0 - 0.8, by0 - 0.4, bx1 + 0.8, by1 + 0.4) & pr
            if not redact_rect.is_empty:
                try:
                    page.add_redact_annot(redact_rect, fill=(1, 1, 1))
                    redact_count += 1
                except Exception:
                    pass

        spans = block.raw_spans or []
        has_span_redaction = False
        if spans:
            for sp_idx, sp in enumerate(spans):
                sp_text = (sp.get("text") or "")
                if not sp_text.strip():
                    continue
                if i in preserved_prefix and sp_idx == 0:
                    # Keep symbol-prefix glyph as-is.
                    continue
                # Preserve pure symbol glyph spans to avoid tofu replacement.
                if _is_symbol_font(sp.get("font") or "") and not _has_letters(sp_text):
                    continue
                sb = sp.get("bbox")
                if not sb or len(sb) < 4:
                    continue
                sx0, sy0, sx1, sy1 = float(sb[0]), float(sb[1]), float(sb[2]), float(sb[3])
                pad_x = min(0.15, max(0.0, (sx1 - sx0) * 0.03))
                pad_y = min(0.20, max(0.0, (sy1 - sy0) * 0.08))
                srect = fitz.Rect(sx0 + pad_x, sy0 + pad_y, sx1 - pad_x, sy1 - pad_y)
                # If insetting collapses the box, fall back to the raw span bbox.
                if srect.is_empty:
                    srect = fitz.Rect(sx0, sy0, sx1, sy1)
                srect = srect & pr
                if srect.is_empty:
                    continue
                page.add_redact_annot(srect, fill=(1, 1, 1))
                redact_count += 1
                has_span_redaction = True

        if not has_span_redaction:
            # Fallback for malformed span metadata.
            x0, y0, x1, y1 = block.bbox
            redact_rect = fitz.Rect(x0, y0, x1, y1) & pr
            if not redact_rect.is_empty:
                page.add_redact_annot(redact_rect, fill=(1, 1, 1))
                redact_count += 1

    # ── Step 3b: Apply all redactions in one pass ─────────────────────────
    # Preserve images and vector graphics (table borders, rules, etc.)
    if redact_count > 0:
        try:
            page.apply_redactions(
                images=fitz.PDF_REDACT_IMAGE_NONE,
                graphics=fitz.PDF_REDACT_LINE_ART_NONE,
            )
        except Exception:
            try:
                page.apply_redactions(images=fitz.PDF_REDACT_IMAGE_NONE)
            except Exception:
                try:
                    page.apply_redactions()
                except Exception:
                    pass

    # Clear precise original table text spans first.
    for sx0, sy0, sx1, sy1 in table_span_clear_rects:
        clear_rect = fitz.Rect(sx0, sy0, sx1, sy1) & pr
        if clear_rect.is_empty:
            continue
        try:
            page.draw_rect(clear_rect, color=None, fill=(1, 1, 1), overlay=True, width=0)
        except Exception:
            pass

    # Fallback: clear table-cell interior only when no precise span bboxes were found.
    for key, _tb in table_cells_to_clear.items():
        if key in table_cells_precise_clear:
            continue
        tx0, ty0, tx1, ty1 = _tb
        cw = max(1.0, tx1 - tx0)
        ch = max(1.0, ty1 - ty0)
        inset_x = min(2.6, max(1.0, cw * 0.05))
        inset_y = min(2.8, max(1.0, ch * 0.18))
        clear_rect = fitz.Rect(tx0 + inset_x, ty0 + inset_y, tx1 - inset_x, ty1 - inset_y) & pr
        if clear_rect.is_empty:
            continue
        try:
            page.draw_rect(clear_rect, color=None, fill=(1, 1, 1), overlay=True, width=0)
        except Exception:
            pass

    # ── Step 3c: Register TTF fonts on this page, then insert translated text ──
    # page.insert_text(fontfile=...) silently falls back to Helvetica in PyMuPDF
    # 1.27.x. The correct approach is insert_font() first, then use the alias.
    _page_fonts: Dict[str, str] = {}   # ttf_path → registered alias on this page
    inline_right_limits: Dict[int, float] = {}
    table_tops_sorted: List[float] = []

    if inline_mode:
        table_tops_sorted = sorted({float(b.bbox[1]) for b in blocks if b.is_table_cell})
        import logging as _dbg_log2
        _dbg2 = _dbg_log2.getLogger("pdf_blocks.yshift")
        _dbg2.debug(f"PAGE table_tops_sorted={[round(t,1) for t in table_tops_sorted]}")
        _dbg2.debug(f"PAGE blocks count={len(blocks)}")
        for _bi, _bb in enumerate(blocks):
            _dbg2.debug(f"  blk[{_bi}] y0={_bb.bbox[1]:.1f} y1={_bb.bbox[3]:.1f} tbl={_bb.is_table_cell} txt={_bb.text[:50] if _bb.text else ''!r}")
        _dbg2.debug(f"PAGE translations keys={sorted(translations.keys())}")
        # Use the original document's right text edge (max x1 of translated blocks)
        # so the bilingual text wraps at the same right margin as the source.
        _orig_right_edge = max(
            (b.bbox[2] for b in blocks if not b.is_table_cell),
            default=float(pr.x1) - 8.0,
        )
        page_right = min(float(pr.x1) - 4.0, max(_orig_right_edge + 4.0, float(pr.x1) * 0.90))
        for i, block in enumerate(blocks):
            if i not in translations or block.is_table_cell:
                continue
            x0, y0, x1, y1 = block.bbox
            h = max(1.0, y1 - y0)
            mid = (y0 + y1) * 0.5
            right_limit = page_right

            for j, nxt in enumerate(blocks):
                if j == i:
                    continue
                nx0, ny0, nx1, ny1 = nxt.bbox
                if nx0 <= x1 + 1.0:
                    continue
                nh = max(1.0, ny1 - ny0)
                inter_h = min(y1, ny1) - max(y0, ny0)
                nmid = (ny0 + ny1) * 0.5
                same_band = (
                    inter_h > min(h, nh) * 0.22
                    or abs(nmid - mid) <= max(h, nh) * 0.55
                )
                if not same_band:
                    continue
                cand = nx0 - 4.0
                if cand < right_limit:
                    right_limit = cand

            min_right = max(x1 + 2.0, x0 + 12.0)
            if right_limit < min_right:
                right_limit = min_right
            inline_right_limits[i] = right_limit

    # ── Table group identification & border capture (inline mode) ────────
    # Groups contiguous runs of table cells into table regions.  For each
    # region we record the overall bounding box and the original border line
    # drawings so that the table can be white-filled and redrawn at a
    # shifted position when preceding bilingual text expands.
    _table_groups: List[Dict] = []   # [{first_idx, last_idx, bbox, drawings}]
    _tbl_group_for_block: Dict[int, int] = {}  # block_idx → group index
    _tbl_group_shifted: Set[int] = set()  # groups already relocated

    if inline_mode:
        # 1. Identify contiguous runs of table cells
        cur_group: Optional[Dict] = None
        for bi, blk in enumerate(blocks):
            if blk.is_table_cell:
                tb = blk.table_bbox or blk.bbox
                if cur_group is None:
                    cur_group = {
                        'first_idx': bi,
                        'last_idx': bi,
                        'x0': tb[0], 'y0': tb[1], 'x1': tb[2], 'y1': tb[3],
                    }
                else:
                    cur_group['last_idx'] = bi
                    cur_group['x0'] = min(cur_group['x0'], tb[0])
                    cur_group['y0'] = min(cur_group['y0'], tb[1])
                    cur_group['x1'] = max(cur_group['x1'], tb[2])
                    cur_group['y1'] = max(cur_group['y1'], tb[3])
            else:
                if cur_group is not None:
                    _table_groups.append(cur_group)
                    cur_group = None
        if cur_group is not None:
            _table_groups.append(cur_group)

        # 2. Map block indices → group index
        for gi, tg in enumerate(_table_groups):
            for bi2 in range(tg['first_idx'], tg['last_idx'] + 1):
                _tbl_group_for_block[bi2] = gi

        # 3. Capture border drawings for each group
        all_drawings = page.get_drawings() if _table_groups else []
        for gi, tg in enumerate(_table_groups):
            gx0, gy0, gx1, gy1 = tg['x0'], tg['y0'], tg['x1'], tg['y1']
            margin = 2.0
            group_lines: List[dict] = []
            for d in all_drawings:
                drect = d.get("rect")
                if drect is None:
                    continue
                # Check if drawing overlaps with the table group area
                if (drect.y1 < gy0 - margin or drect.y0 > gy1 + margin or
                        drect.x1 < gx0 - margin or drect.x0 > gx1 + margin):
                    continue
                group_lines.append(d)
            tg['drawings'] = group_lines

    def _register(ttf_path: str) -> Optional[str]:
        """Register ttf_path on the page once; return its alias or None."""
        if ttf_path in _page_fonts:
            return _page_fonts[ttf_path]
        alias = f"F{len(_page_fonts)}"
        try:
            page.insert_font(fontname=alias, fontfile=ttf_path)
            _page_fonts[ttf_path] = alias
            return alias
        except Exception:
            return None

    def _relocate_table_group(gi: int, y_shift: float, page, fitz, pr, groups, shifted_set):
        """White-fill the original table area and re-draw borders at shifted Y."""
        tg = groups[gi]
        gx0, gy0, gx1, gy1 = tg['x0'], tg['y0'], tg['x1'], tg['y1']
        # 1. Erase original border lines individually (NOT a big white-fill
        #    rect — that would erase shifted text already rendered above).
        for d in tg.get('drawings', []):
            _ec = d.get('color', (0, 0, 0))
            _ew = max(d.get('width', 0.48) + 1.0, 2.0)
            for item in d.get('items', []):
                if item[0] == 'l':
                    p1, p2 = item[1], item[2]
                    try:
                        page.draw_line(fitz.Point(p1.x, p1.y), fitz.Point(p2.x, p2.y),
                                       color=(1, 1, 1), width=_ew)
                    except Exception:
                        pass
                elif item[0] == 're':
                    r = item[1]
                    try:
                        page.draw_rect(r, color=None, fill=(1, 1, 1), overlay=True, width=0)
                    except Exception:
                        pass
        # 2. Re-draw table border lines at shifted positions
        for d in tg.get('drawings', []):
            items = d.get('items', [])
            dcolor = d.get('color', (0, 0, 0))
            dwidth = d.get('width', 0.48)
            for item in items:
                if item[0] == 'l':  # line
                    p1, p2 = item[1], item[2]
                    sp = fitz.Point(p1.x, p1.y + y_shift)
                    ep = fitz.Point(p2.x, p2.y + y_shift)
                    # Only draw if shifted line is within page
                    if sp.y <= float(pr.y1) and ep.y <= float(pr.y1):
                        try:
                            page.draw_line(sp, ep, color=dcolor, width=dwidth)
                        except Exception:
                            pass
                elif item[0] == 're':  # rectangle
                    r = item[1]
                    sr = fitz.Rect(r.x0, r.y0 + y_shift, r.x1, r.y1 + y_shift) & pr
                    if not sr.is_empty:
                        try:
                            page.draw_rect(sr, color=dcolor, fill=d.get('fill'), width=dwidth)
                        except Exception:
                            pass
        shifted_set.add(gi)
        _dbg.debug(f"  TABLE_RELOCATE group {gi} by {y_shift:.1f}  orig_y=[{gy0:.1f},{gy1:.1f}] → [{gy0+y_shift:.1f},{gy1+y_shift:.1f}]")

    inline_y_shift = 0.0
    inline_flow_bottom: Optional[float] = None
    prev_source_bottom: Optional[float] = None
    page_full = False
    _last_tbl_group_idx: Optional[int] = None  # track which table group we're in

    import logging as _dbg_log
    _dbg = _dbg_log.getLogger("pdf_blocks.yshift")
    _dbg.setLevel(_dbg_log.DEBUG)
    if not _dbg.handlers:
        _dbg.addHandler(_dbg_log.StreamHandler())

    for i, block in enumerate(blocks):
        # ── Table-group exit bookkeeping ─────────────────────────────
        # When we leave a table group (current block is NOT in the same
        # group as the previous one), update flow tracking so that the
        # gap-absorption for the next non-table block uses the shifted
        # table bottom — not the pre-table text bottom.
        if inline_mode:
            cur_grp = _tbl_group_for_block.get(i)
            if _last_tbl_group_idx is not None and cur_grp != _last_tbl_group_idx:
                _exited_tg = _table_groups[_last_tbl_group_idx]
                _tg_orig_bot = _exited_tg['y1']
                _tg_shift_bot = _tg_orig_bot + inline_y_shift
                prev_source_bottom = _tg_orig_bot
                if inline_flow_bottom is None or _tg_shift_bot > inline_flow_bottom:
                    inline_flow_bottom = _tg_shift_bot
                _dbg.debug(f"  TBL_GROUP_EXIT grp {_last_tbl_group_idx} orig_bot={_tg_orig_bot:.1f} shifted_bot={_tg_shift_bot:.1f} shift={inline_y_shift:.1f}")
            _last_tbl_group_idx = cur_grp
        if i not in translations:
            if inline_mode and not block.is_table_cell:
                _nt_x0, _nt_y0, _nt_x1, _nt_y1 = block.bbox
                _dbg.debug(f"[NT] blk {i} y0={_nt_y0:.1f} y1={_nt_y1:.1f} shift={inline_y_shift:.1f} prev_bot={prev_source_bottom} txt={block.text[:40] if block.text else ''!r}")

                # Absorb y_shift into natural gaps
                if inline_y_shift > 0 and prev_source_bottom is not None:
                    _nt_gap = _nt_y0 - prev_source_bottom
                    if _nt_gap > 0:
                        _nt_absorb = min(inline_y_shift, _nt_gap * 0.85)
                        inline_y_shift = max(0.0, inline_y_shift - _nt_absorb)
                        _dbg.debug(f"  NT absorb gap={_nt_gap:.1f} absorb={_nt_absorb:.1f} new_shift={inline_y_shift:.1f}")

                _nt_shifted_bottom = _nt_y1 + inline_y_shift
                if inline_flow_bottom is None or _nt_shifted_bottom > inline_flow_bottom:
                    inline_flow_bottom = _nt_shifted_bottom
                prev_source_bottom = _nt_y1
                # Re-render span-by-span at shifted Y to preserve Vietnamese spacing
                if i in _inline_needs_rerender and inline_y_shift != 0.0:
                    for sp in (block.raw_spans or []):
                        sp_text = (sp.get("text") or "")
                        if not sp_text.strip():
                            continue
                        sp_origin = sp.get("origin")
                        if not sp_origin or len(sp_origin) < 2:
                            continue
                        sp_fs = max(4.0, float(sp.get("size") or block.font_size))
                        sp_color = block.color
                        sp_ox = float(sp_origin[0])
                        sp_oy = float(sp_origin[1]) + inline_y_shift
                        if sp_oy < float(pr.y0) or sp_oy > float(pr.y1) - 2.0:
                            continue
                        sp_font = (sp.get("font") or block.font_name or "")
                        sp_bold = "bold" in sp_font.lower() or "Bold" in sp_font
                        sp_italic = "italic" in sp_font.lower() or "Italic" in sp_font
                        sp_ttf = _find_ttf_for_font(sp_font, sp_bold, sp_italic)
                        sp_alias = _register(sp_ttf) if sp_ttf else None
                        try:
                            page.insert_text(
                                fitz.Point(sp_ox, sp_oy),
                                sp_text,
                                fontname=(sp_alias or "helv"),
                                fontsize=sp_fs,
                                color=sp_color,
                            )
                        except Exception:
                            try:
                                page.insert_text(
                                    fitz.Point(sp_ox, sp_oy),
                                    sp_text,
                                    fontname="helv",
                                    fontsize=sp_fs,
                                    color=sp_color,
                                )
                            except Exception:
                                pass
            elif inline_mode and block.is_table_cell:
                # ── Non-translated table cell ────────────────────────────
                # Text was cleared in step 3a; re-render at shifted position.
                # If shift is 0, re-render at original position.
                _tc_shift = inline_y_shift
                gi = _tbl_group_for_block.get(i)
                if gi is not None and gi not in _tbl_group_shifted and _tc_shift > 0:
                    _relocate_table_group(gi, _tc_shift, page, fitz, pr, _table_groups, _tbl_group_shifted)
                tb = block.table_bbox or block.bbox
                _tc_x0, _tc_y0, _tc_x1, _tc_y1 = tb
                _tc_text = (block.text or "").strip()
                if _tc_text:
                    _tc_fs = max(4.0, block.font_size)
                    _tc_dr = fitz.Rect(
                        _tc_x0 + 0.75, _tc_y0 + _tc_shift + 0.35,
                        _tc_x1 - 0.75, _tc_y1 + _tc_shift - 0.35,
                    ) & pr
                    if not _tc_dr.is_empty:
                        _tc_ttf = _find_ttf_for_font(block.font_name, block.is_bold, block.is_italic)
                        _tc_alias = _register(_tc_ttf) if _tc_ttf else None
                        try:
                            page.insert_text(
                                fitz.Point(_tc_dr.x0, _tc_dr.y0 + _tc_fs * 0.86),
                                _tc_text,
                                fontname=(_tc_alias or "helv"),
                                fontsize=_tc_fs,
                                color=block.color,
                            )
                        except Exception:
                            pass
            continue

        translated = (translations[i] or "").strip()
        if not translated:
            continue

        _dbg.debug(f"[TR] blk {i} y0={block.bbox[1]:.1f} y1={block.bbox[3]:.1f} shift={inline_y_shift:.1f} tbl={block.is_table_cell} prev_bot={prev_source_bottom} txt={block.text[:40] if block.text else ''!r}")

        # Absorb y_shift into natural gaps (non-table blocks only)
        if inline_mode and not block.is_table_cell and inline_y_shift > 0:
            if prev_source_bottom is not None:
                gap = block.bbox[1] - prev_source_bottom
                if gap > 0 and inline_y_shift > 0:
                    absorb = min(inline_y_shift, gap * 0.85)
                    inline_y_shift = max(0.0, inline_y_shift - absorb)
                    _dbg.debug(f"  TR absorb gap={gap:.1f} absorb={absorb:.1f} new_shift={inline_y_shift:.1f}")

        if i in preserved_prefix:
            translated = _strip_preserved_prefix(
                translated,
                str(preserved_prefix[i].get("prefix") or ""),
            )
            if not translated.strip():
                continue

        # Single-line block renderer: collapse accidental model line-breaks to
        # prevent table cell wrapping artifacts like "No\nNULL".
        translated = re.sub(r"[ \t]*[\r\n]+[ \t]*", " ", translated).strip()
        translated = re.sub(r"[ \t]{2,}", " ", translated)
        if block.is_table_cell:
            translated = re.sub(r"^\s*No\s+NULL\s*$", "Not NULL", translated, flags=re.IGNORECASE)
        if not translated:
            continue

        x0, y0, x1, y1 = block.bbox
        target_bbox = block.table_bbox if (block.is_table_cell and block.table_bbox) else block.bbox
        tx0, ty0, tx1, ty1 = target_bbox
        font_size = max(4.0, block.font_size)
        color     = block.color

        ox = block.origin_x if block.origin_x else x0
        oy = block.origin_y if block.origin_y else y1
        body_origin = preserved_prefix.get(i, {}).get("body_origin") if i in preserved_prefix else None
        if isinstance(body_origin, tuple) and len(body_origin) >= 2:
            ox = float(body_origin[0])
            oy = float(body_origin[1])
        elif inline_mode:
            # In inline bilingual mode, maximize usable width by anchoring at bbox left.
            ox = x0

        if block.is_table_cell:
            bbox_w = max(1.0, tx1 - tx0 - 1.2)
        else:
            bbox_w = max(1.0, (x1 - x0) if inline_mode else (x1 - ox))

        # Resolve TTF for this block's original font name + bold/italic
        if _is_symbol_font(block.font_name) and _has_letters(translated):
            ttf = _find_ttf(block.is_bold, block.is_italic) or _find_ttf(False, False)
        else:
            ttf = _find_ttf_for_font(block.font_name, block.is_bold, block.is_italic)
        alias = _register(ttf) if ttf else None

        # Keep source font size as much as possible; only shrink on overflow.
        # Inline non-table mode preserves original font size (user request).
        if alias and not (inline_mode and not block.is_table_cell):
            try:
                _fobj  = fitz.Font(fontfile=ttf)
                text_w = _fobj.text_length(translated, fontsize=font_size)
                if block.is_table_cell:
                    overflow_threshold = 1.03
                elif inline_mode:
                    overflow_threshold = 1.02
                else:
                    overflow_threshold = 1.20
                if text_w > bbox_w * overflow_threshold:
                    scaled = font_size * bbox_w / text_w
                    try:
                        if block.is_table_cell:
                            ratio_env = "PDF_TABLE_MIN_FONT_RATIO"
                            ratio_default = "0.90"
                        elif inline_mode:
                            ratio_env = "PDF_INLINE_MIN_FONT_RATIO"
                            ratio_default = "0.72"
                        else:
                            ratio_env = "PDF_BLOCK_MIN_FONT_RATIO"
                            ratio_default = "0.96"
                        min_ratio = float(os.getenv(ratio_env, ratio_default))
                    except Exception:
                        min_ratio = 0.90 if block.is_table_cell else (0.72 if inline_mode else 0.96)
                    min_floor = 0.60 if inline_mode and not block.is_table_cell else 0.70
                    if min_ratio < min_floor:
                        min_ratio = min_floor
                    if min_ratio > 1.0:
                        min_ratio = 1.0
                    fs_min = max(4.0, block.font_size * min_ratio)
                    font_size = max(fs_min, scaled)
            except Exception:
                pass

        if block.is_table_cell:
            # ── Shift table cell position in inline mode ─────────────────
            _tbl_yshift = 0.0
            if inline_mode and inline_y_shift > 0:
                _tbl_yshift = inline_y_shift
                gi = _tbl_group_for_block.get(i)
                if gi is not None and gi not in _tbl_group_shifted:
                    _relocate_table_group(gi, inline_y_shift, page, fitz, pr, _table_groups, _tbl_group_shifted)
            # Constrain translated text inside its table cell (shifted if needed).
            draw_rect = fitz.Rect(tx0 + 0.75, ty0 + _tbl_yshift + 0.35, tx1 - 0.75, ty1 + _tbl_yshift - 0.35) & pr
            if draw_rect.is_empty:
                draw_rect = fitz.Rect(tx0, ty0 + _tbl_yshift, tx1, ty1 + _tbl_yshift) & pr
            align_map = {"left": 0, "center": 1, "right": 2}
            align_val = align_map.get(block.alignment, 0)

            inserted = False
            measure_font = None
            if ttf:
                try:
                    measure_font = fitz.Font(fontfile=ttf)
                except Exception:
                    measure_font = None

            def _measure_width(txt: str, fs_val: int) -> float:
                if measure_font is not None:
                    try:
                        return float(measure_font.text_length(txt, fontsize=fs_val))
                    except Exception:
                        pass
                try:
                    return float(fitz.get_text_length(txt, fontname=(alias or block.font_name or "helv"), fontsize=fs_val))
                except Exception:
                    try:
                        return float(fitz.get_text_length(txt, fontname="helv", fontsize=fs_val))
                    except Exception:
                        return max(1.0, len(txt) * fs_val * 0.55)

            # Prefer single-line insertion in table cells (no textbox wrapping)
            # and reduce font only when really needed.
            try:
                single_min_ratio = float(os.getenv("PDF_TABLE_SINGLE_LINE_MIN_RATIO", "0.78"))
            except Exception:
                single_min_ratio = 0.78
            if single_min_ratio < 0.65:
                single_min_ratio = 0.65
            if single_min_ratio > 1.0:
                single_min_ratio = 1.0

            # Short DB constraints must stay one line (e.g. "Not NULL").
            if _DB_CONSTRAINT_RE.fullmatch(translated):
                single_min_ratio = min(single_min_ratio, 0.52)

            fs_start = int(round(font_size))
            fs_min_single = int(max(4, round(block.font_size * single_min_ratio)))
            avail_w = max(1.0, float(draw_rect.width))
            avail_h = max(1.0, float(draw_rect.height))

            for fs in range(fs_start, fs_min_single - 1, -1):
                tw = _measure_width(translated, fs)
                if tw > avail_w * 1.002:
                    continue
                if fs > avail_h * 0.98:
                    continue

                if align_val == 1:
                    px = draw_rect.x0 + max(0.0, (avail_w - tw) * 0.5)
                elif align_val == 2:
                    px = draw_rect.x1 - tw
                else:
                    px = draw_rect.x0
                py = draw_rect.y0 + max(0.0, (avail_h - fs) * 0.5) + fs * 0.86

                try:
                    if alias:
                        page.insert_text(
                            fitz.Point(px, py),
                            translated,
                            fontname=alias,
                            fontsize=fs,
                            color=color,
                        )
                    else:
                        page.insert_text(
                            fitz.Point(px, py),
                            translated,
                            fontname=block.font_name,
                            fontsize=fs,
                            color=color,
                        )
                    inserted = True
                    break
                except Exception:
                    try:
                        page.insert_text(
                            fitz.Point(px, py),
                            translated,
                            fontname="helv",
                            fontsize=fs,
                            color=color,
                        )
                        inserted = True
                        break
                    except Exception:
                        pass

            # Fallback to textbox wrapping only when single-line fit is impossible.
            for fs in range(int(round(font_size)), int(max(4, round(font_size * 0.72))) - 1, -1):
                if inserted:
                    break
                try:
                    if alias:
                        rc = page.insert_textbox(
                            draw_rect,
                            translated,
                            fontname=alias,
                            fontsize=fs,
                            color=color,
                            align=align_val,
                        )
                    else:
                        rc = page.insert_textbox(
                            draw_rect,
                            translated,
                            fontname=block.font_name,
                            fontsize=fs,
                            color=color,
                            align=align_val,
                        )
                    if rc >= 0:
                        inserted = True
                        break
                except Exception:
                    try:
                        rc = page.insert_textbox(
                            draw_rect,
                            translated,
                            fontname="helv",
                            fontsize=fs,
                            color=color,
                            align=align_val,
                        )
                        if rc >= 0:
                            inserted = True
                            break
                    except Exception:
                        pass

            if not inserted:
                # Last resort fallback to baseline insertion.
                point = fitz.Point(draw_rect.x0, draw_rect.y0 + max(4.0, font_size * 0.9))
                try:
                    page.insert_text(point, translated, fontname=(alias or "helv"), fontsize=font_size, color=color)
                except Exception:
                    pass
        else:
            if inline_mode:
                right_limit = inline_right_limits.get(i, page_right if inline_mode else (float(pr.x1) - 4.0))
                y0_shifted = y0 + inline_y_shift
                oy_shifted = oy + inline_y_shift

                _dbg.debug(f"  RENDER blk {i} y0={y0:.1f} shift={inline_y_shift:.1f} -> y0_shifted={y0_shifted:.1f}")

                # Force left alignment in inline bilingual mode
                align_val = 0
                fs_fixed = max(4.0, block.font_size)
                source_only = (block.text or "").strip()
                avail_w = max(1.0, right_limit - ox)

                measure_font = None
                if ttf:
                    try:
                        measure_font = fitz.Font(fontfile=ttf)
                    except Exception:
                        measure_font = None

                def _measure_inline_width(txt: str) -> float:
                    if measure_font is not None:
                        try:
                            return float(measure_font.text_length(txt, fontsize=fs_fixed))
                        except Exception:
                            pass
                    try:
                        return float(fitz.get_text_length(txt, fontname=(alias or block.font_name or "helv"), fontsize=fs_fixed))
                    except Exception:
                        try:
                            return float(fitz.get_text_length(txt, fontname="helv", fontsize=fs_fixed))
                        except Exception:
                            return max(1.0, len(txt) * fs_fixed * 0.55)

                def _wrap_inline_text(txt: str) -> List[str]:
                    s = str(txt or "").strip()
                    if not s:
                        return []
                    tokens = s.split()
                    if not tokens:
                        return [s]
                    lines: List[str] = []
                    cur = ""
                    for token in tokens:
                        cand = token if not cur else f"{cur} {token}"
                        if _measure_inline_width(cand) <= avail_w * 1.001:
                            cur = cand
                            continue
                        if cur:
                            lines.append(cur)
                            cur = ""
                        if _measure_inline_width(token) <= avail_w * 1.001:
                            cur = token
                            continue
                        # Split very long token by character while keeping order.
                        part = ""
                        for ch in token:
                            c2 = f"{part}{ch}"
                            if part and _measure_inline_width(c2) > avail_w * 1.001:
                                lines.append(part)
                                part = ch
                            else:
                                part = c2
                        cur = part
                    if cur:
                        lines.append(cur)
                    return lines

                # Tables shift with text, so max_bottom is page bottom.
                max_bottom = float(pr.y1) - 0.2

                line_h = max(y1 - y0, fs_fixed * 1.16)
                top = max(float(pr.y0) + 0.2, y0_shifted - min(0.35, line_h * 0.08))
                source_gap = 0.0 if prev_source_bottom is None else max(0.0, y0 - prev_source_bottom)
                if inline_flow_bottom is not None:
                    flow_top = inline_flow_bottom + source_gap
                    if flow_top > top:
                        top = flow_top
                        # Sync inline_y_shift so downstream table relocation
                        # uses the actual displacement, not the gap-absorbed one.
                        inline_y_shift = max(inline_y_shift, top - y0 + min(0.35, line_h * 0.08))

                def _queue_overflow_lines(lines_to_queue: List[str]) -> bool:
                    if inline_overflow_out is None:
                        return False
                    queued = [ln for ln in (lines_to_queue or []) if str(ln or "").strip()]
                    if not queued:
                        return False
                    inline_overflow_out.append({
                        'lines': queued,
                        'ttf': ttf,
                        'font_name': block.font_name,
                        'font_size': fs_fixed,
                        'color': color,
                        'ox': ox,
                        'line_h': line_h,
                    })
                    return True

                def _insert_wrapped(txt: str) -> Tuple[bool, float, List[str]]:
                    """Insert wrapped text on current page.

                    Returns (any_drawn, used_h, overflow_lines) where
                    overflow_lines contains the lines that did not fit
                    on this page and should flow to the next page.
                    """
                    lines = _wrap_inline_text(txt)
                    if not lines:
                        return False, 0.0, []
                    line_fudge = max(0.5, fs_fixed * 0.12)
                    # How many lines fit before max_bottom?
                    avail_page_h = max_bottom - top
                    n_fits = max(0, int((avail_page_h - line_fudge) / line_h)) if line_h > 0 else 0
                    n_fits = min(n_fits, len(lines))

                    if n_fits == 0:
                        # Nothing fits on this page: entire block overflows.
                        return False, 0.0, list(lines)

                    lines_to_draw = lines[:n_fits]
                    overflow_lines = list(lines[n_fits:])

                    text_to_draw = "\n".join(lines_to_draw)
                    needed_h = line_h * len(lines_to_draw) + line_fudge
                    bottom = min(max_bottom, top + needed_h)
                    if bottom <= top + max(1.0, line_h * 0.92):
                        return False, 0.0, list(lines)
                    draw_rect = fitz.Rect(ox, top, max(ox + 1.0, right_limit), bottom) & pr
                    if draw_rect.is_empty:
                        return False, 0.0, list(lines)
                    try:
                        if alias:
                            rc = page.insert_textbox(
                                draw_rect,
                                text_to_draw,
                                fontname=alias,
                                fontsize=fs_fixed,
                                color=color,
                                align=align_val,
                            )
                        else:
                            rc = page.insert_textbox(
                                draw_rect,
                                text_to_draw,
                                fontname=block.font_name,
                                fontsize=fs_fixed,
                                color=color,
                                align=align_val,
                            )
                    except Exception:
                        try:
                            rc = page.insert_textbox(
                                draw_rect,
                                text_to_draw,
                                fontname="helv",
                                fontsize=fs_fixed,
                                color=color,
                                align=align_val,
                            )
                        except Exception:
                            return False, 0.0, list(lines)
                    if rc < 0:
                        return False, 0.0, list(lines)
                    used_h = line_h * len(lines_to_draw)
                    return True, used_h, overflow_lines

                if page_full and inline_overflow_out is not None:
                    _queue_overflow_lines(_wrap_inline_text(translated))
                    continue

                if top >= max_bottom - max(1.0, line_h * 0.60):
                    if _queue_overflow_lines(_wrap_inline_text(translated)):
                        page_full = True
                        continue

                inserted, used_h, overflow_lines = _insert_wrapped(translated)

                # Queue any undrawn lines for the next page.
                if overflow_lines:
                    _queue_overflow_lines(overflow_lines)
                    page_full = True

                if inserted:
                    orig_h = max(1.0, y1 - y0)
                    _old_yshift = inline_y_shift
                    inline_y_shift += max(0.0, used_h - orig_h)
                    _dbg.debug(f"  EXPAND blk {i} orig_h={orig_h:.1f} used_h={used_h:.1f} shift {_old_yshift:.1f}->{inline_y_shift:.1f}")
                    inline_flow_bottom = top + max(orig_h, used_h)
                    prev_source_bottom = y1
                elif inline_overflow_out is not None:
                    page_full = True
                    prev_source_bottom = y1
                else:
                    # Legacy fallback when caller does not support overflow:
                    # insert at baseline near page bottom to avoid losing content.
                    fallback_text = source_only or translated
                    point = fitz.Point(ox, min(max(float(pr.y0) + 2.0, oy_shifted), float(pr.y1) - 1.5))
                    try:
                        page.insert_text(
                            point,
                            fallback_text,
                            fontname=(alias or block.font_name),
                            fontsize=fs_fixed,
                            color=color,
                        )
                    except Exception:
                        try:
                            page.insert_text(
                                point,
                                fallback_text,
                                fontname="helv",
                                fontsize=fs_fixed,
                                color=color,
                            )
                        except Exception:
                            pass
                    inline_flow_bottom = max(top, float(point.y) - fs_fixed * 0.86) + line_h
                    prev_source_bottom = y1
            else:
                # Place text at exact baseline origin from the original span.
                # span["origin"] = (x, y) where y is the actual text baseline.
                point = fitz.Point(ox, oy)
                try:
                    if alias:
                        page.insert_text(point, translated,
                                          fontname=alias, fontsize=font_size, color=color)
                    else:
                        page.insert_text(point, translated,
                                          fontname=block.font_name, fontsize=font_size, color=color)
                except Exception:
                    try:
                        page.insert_text(point, translated,
                                          fontname="helv", fontsize=font_size, color=color)
                    except Exception:
                        pass


# ─────────────────────────────────────────────────────────────────────────────
# Overflow page renderer (inline bilingual: lines that did not fit on source page)
# ─────────────────────────────────────────────────────────────────────────────

def _render_overflow_on_page(fitz_module, page, items: list) -> list:
    """Render overflow inline-bilingual items onto a blank overflow page.

    Items are rendered top-to-bottom starting from the top margin.
    Each item is a dict produced by render_blocks_on_page's overflow
    path:  {lines, ttf, font_name, font_size, color, ox, line_h}

    Returns the remaining items that did not fit on this overflow page.
    """
    fitz = fitz_module
    pr = page.rect
    cur_y = float(pr.y0) + 42.0   # top margin on the new overflow page
    _page_fonts: Dict[str, str] = {}

    def _reg(ttf_path: Optional[str]) -> Optional[str]:
        if not ttf_path:
            return None
        if ttf_path in _page_fonts:
            return _page_fonts[ttf_path]
        alias = f"F{len(_page_fonts)}"
        try:
            page.insert_font(fontname=alias, fontfile=ttf_path)
            _page_fonts[ttf_path] = alias
            return alias
        except Exception:
            return None

    remaining_items: list = []

    for item_idx, item in enumerate(items):
        lines    = item.get('lines') or []
        ttf      = item.get('ttf')
        fn_fb    = item.get('font_name') or 'helv'
        fs       = float(item.get('font_size') or 10.0)
        color    = item.get('color') or (0.0, 0.0, 0.0)
        ox       = float(item.get('ox') or 42.0)
        line_h   = float(item.get('line_h') or fs * 1.2)

        if not lines:
            continue

        alias = _reg(ttf)
        for line_idx, line in enumerate(lines):
            if cur_y + fs > float(pr.y1) - 6.0:
                leftover = dict(item)
                leftover['lines'] = list(lines[line_idx:])
                remaining_items.append(leftover)
                for rest in items[item_idx + 1:]:
                    rest_copy = dict(rest)
                    rest_copy['lines'] = list(rest.get('lines') or [])
                    remaining_items.append(rest_copy)
                return remaining_items
            try:
                page.insert_text(
                    fitz.Point(ox, cur_y + fs * 0.86),
                    line,
                    fontname=(alias or fn_fb or 'helv'),
                    fontsize=fs,
                    color=color,
                )
            except Exception:
                try:
                    page.insert_text(
                        fitz.Point(ox, cur_y + fs * 0.86),
                        line,
                        fontname='helv',
                        fontsize=fs,
                        color=color,
                    )
                except Exception:
                    pass
            cur_y += line_h
        cur_y += line_h * 0.4   # small gap between items

    return remaining_items


# ─────────────────────────────────────────────────────────────────────────────
# Main orchestrator
# ─────────────────────────────────────────────────────────────────────────────

class PdfBlockTranslator:
    """Clean PDF translator using the block-by-block pipeline.

    Pipeline per page:
      1. extract_page_blocks(page)      → List[PdfTextBlock]
      2. merge_paragraph_blocks(blocks) → List[PdfTextBlock]   (optional)
      3. translate each block           → Dict[int, str]
      4. render_blocks_on_page(...)     → mutates page in-place

    The translated document is saved to output_path.
    """

    def __init__(
        self,
        translate_fn: Callable[[str], str],
        *,
        download_folder: Optional[str] = None,
        merge_paragraphs: bool = False,
        bilingual_mode: str = "none",
        bilingual_delimiter: str = "|",
        inline_table_mode: str = "keep-source",
    ):
        self.translate_fn    = translate_fn
        self.merge_paragraphs = merge_paragraphs
        mode = str(bilingual_mode or "none").strip().lower()
        self.bilingual_mode = mode if mode in ("none", "inline") else "none"
        delim = str(bilingual_delimiter or "|").strip() or "|"
        if len(delim) > 10:
            delim = delim[:10]
        self.bilingual_delimiter = delim
        tmode = str(inline_table_mode or "keep-source").strip().lower()
        self.inline_table_mode = tmode if tmode in ("keep-source", "translate-only", "inline") else "keep-source"
        self.download_folder = download_folder or os.path.normpath(os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "..", "..", "..", "..", "downloads",
        ))
        os.makedirs(self.download_folder, exist_ok=True)
        self._cache: Dict[str, str] = {}

    def _join_inline_bilingual(self, src_text: str, dst_text: str) -> str:
        src = (src_text or "").strip()
        dst = (dst_text or "").strip()
        if not src:
            return dst
        if not dst:
            return src
        return f"{src} {self.bilingual_delimiter} {dst}"

    @staticmethod
    def _norm_inline_compare(text: str) -> str:
        s = unicodedata.normalize("NFKD", str(text or ""))
        s = "".join(ch for ch in s if not unicodedata.combining(ch))
        s = s.lower()
        s = re.sub(r"[^a-z0-9]+", "", s)
        return s

    def _looks_like_source_echo(self, maybe_src: str, source: str) -> bool:
        a = self._norm_inline_compare(maybe_src)
        b = self._norm_inline_compare(source)
        if not a or not b:
            return False
        if a == b:
            return True
        if len(a) >= 12 and a in b:
            return True
        if len(b) >= 12 and b in a:
            return True
        min_len = min(len(a), len(b))
        if min_len >= 12:
            same_prefix = 0
            for ca, cb in zip(a, b):
                if ca != cb:
                    break
                same_prefix += 1
            if same_prefix >= int(min_len * 0.70):
                return True
        return False

    def _clean_inline_translation(self, src_text: str, translated_text: str) -> str:
        src = str(src_text or "").strip()
        out = str(translated_text or "").strip()
        if not out:
            return out

        # If model returns "source | translation", keep only the RHS.
        delims = [self.bilingual_delimiter, "|", "｜", "¦"]
        for d in delims:
            if not d or d not in out:
                continue
            left, right = out.split(d, 1)
            if self._looks_like_source_echo(left, src):
                out = right.strip()
                break

        # Remove source echo when it appears as a direct prefix.
        if src and out.startswith(src):
            tail = out[len(src):].lstrip(" |-:;,.–—")
            if tail:
                out = tail

        # Remove duplicated source-like prefix chunks (OCR/noise tolerant).
        if src:
            parts = re.split(r"\s*[|｜¦]\s*", out, maxsplit=1)
            if len(parts) == 2 and parts[1].strip() and self._looks_like_source_echo(parts[0], src):
                out = parts[1].strip()

        return out.strip()

    # ── Translation helpers ──────────────────────────────────────────────

    def _translate_cached(self, text: str) -> str:
        """Translate text using cache to avoid duplicate API calls."""
        core = text.strip()
        if not core or not _should_translate(core):
            return text
        if core in self._cache:
            return self._cache[core]
        try:
            result = _preserve_translate(core, self.translate_fn)
            result = (result or "").strip() or core
        except Exception:
            result = core
        self._cache[core] = result
        return result

    # ── Phase 2: Translate all blocks for one page ───────────────────────

    def _translate_blocks(self, blocks: List[PdfTextBlock]) -> Dict[int, str]:
        """Translate each block; return index→translated mapping."""
        translations: Dict[int, str] = {}
        for i, block in enumerate(blocks):
            if not _should_translate(block.text):
                continue
            core = (block.text or "").strip()
            if self.bilingual_mode == "inline":
                # Keep table geometry stable: by default, do not place
                # "source | translation" in table cells.
                if block.is_table_cell and self.inline_table_mode == "keep-source":
                    continue

                if block.is_table_cell and self.inline_table_mode != "inline":
                    canonical = _canonical_table_constraint(core)
                    if canonical:
                        translations[i] = canonical
                        continue
                    if _is_nonsemantic_token(core, table_cell=True):
                        continue
                    translated_tbl = self._translate_cached(block.text)
                    if translated_tbl and translated_tbl.strip():
                        translations[i] = translated_tbl
                    continue

                if _is_nonsemantic_token(core, table_cell=bool(block.is_table_cell)):
                    continue
                translated_core = self._translate_cached(block.text)
                translated_core = self._clean_inline_translation(block.text, translated_core)
                if not translated_core or not translated_core.strip():
                    continue
                translations[i] = self._join_inline_bilingual(block.text, translated_core)
                continue

            if block.is_table_cell:
                canonical = _canonical_table_constraint(core)
                if canonical:
                    translations[i] = canonical
                    continue
            if _is_nonsemantic_token(core, table_cell=bool(block.is_table_cell)):
                continue
            translated = self._translate_cached(block.text)
            if translated and translated.strip():
                translations[i] = translated
        return translations

    # ── Main entry point ─────────────────────────────────────────────────

    def translate_pdf(
        self,
        input_path: str,
        output_path: Optional[str] = None,
        *,
        progress_cb: Optional[Callable[[int, str], None]] = None,
    ) -> str:
        """Translate PDF preserving layout via bbox-level block pipeline.

        Args:
            input_path:  Path to source PDF file.
            output_path: Destination path (auto-generated inside download_folder if None).
            progress_cb: Optional callback(percent: int, message: str).

        Returns:
            Absolute path to the translated PDF file.
        """
        try:
            import fitz
        except ImportError as exc:
            raise RuntimeError("PyMuPDF is required. Install: pip install PyMuPDF") from exc

        if not os.path.exists(input_path):
            raise FileNotFoundError(input_path)

        if output_path is None:
            base = os.path.splitext(os.path.basename(input_path))[0]
            output_path = os.path.join(
                self.download_folder,
                f"{base}_translated_{uuid.uuid4().hex[:8]}.pdf",
            )

        self._cache.clear()

        doc = fitz.open(input_path)
        total_pages = doc.page_count
        original_page_sizes = [
            (float(doc[i].rect.width), float(doc[i].rect.height))
            for i in range(total_pages)
        ]
        is_inline = (self.bilingual_mode == "inline")

        # Collect overflow items per original page index (inline mode only).
        # After processing all pages, insert blank overflow pages in reverse
        # order so that earlier insertions don't shift subsequent indices.
        page_overflows: Dict[int, list] = {}

        try:
            for page_idx in range(total_pages):
                page = doc[page_idx]
                page_w = float(page.rect.width)
                page_h = float(page.rect.height)

                if progress_cb:
                    pct = int(5 + (page_idx / max(1, total_pages)) * 85)
                    progress_cb(pct, f"PDF: page {page_idx + 1}/{total_pages}")

                # ── Phase 1: Extract blocks (text + bbox + font) ─────────
                blocks = extract_page_blocks(page)

                if not blocks:
                    continue   # image-only page — no text to process

                if is_inline:
                    blocks = reorder_inline_row_blocks(blocks)
                    blocks = merge_inline_prefix_blocks(blocks)

                # Optional: merge single lines into paragraph units
                if self.merge_paragraphs:
                    blocks = merge_paragraph_blocks(blocks)

                # ── Phase 2: Translate each block ─────────────────────────
                translations = self._translate_blocks(blocks)

                if not translations:
                    continue

                # ── Phase 3: Render — redact originals + insert at bbox ───
                overflow_items: Optional[list] = [] if is_inline else None
                render_blocks_on_page(
                    fitz,
                    page,
                    blocks,
                    translations,
                    inline_mode=is_inline,
                    inline_overflow_out=overflow_items,
                )
                if overflow_items:
                    page_overflows[page_idx] = overflow_items

            # ── Phase 4 (inline only): Insert overflow pages ──────────────
            # Process in reverse order so inserting page at position N+1
            # does not shift the indices of pages we have not yet processed.
            if page_overflows:
                for orig_idx in sorted(page_overflows.keys(), reverse=True):
                    items = list(page_overflows[orig_idx])
                    ref_w, ref_h = original_page_sizes[orig_idx]
                    insert_after = orig_idx
                    while items:
                        ovf_page = doc.new_page(
                            insert_after + 1,
                            width=ref_w,
                            height=ref_h,
                        )
                        items = _render_overflow_on_page(fitz, ovf_page, items)
                        insert_after += 1

            # Save the modified document
            doc.save(output_path, garbage=4, deflate=True, clean=True)

        finally:
            try:
                doc.close()
            except Exception:
                pass

        if progress_cb:
            progress_cb(100, "PDF: translation completed")

        return output_path
