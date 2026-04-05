"""PDF layout detection and classification.

Analyses PyMuPDF text extraction (dict mode) to classify each text block/line
into a layout element type, enabling the renderer to faithfully reproduce
the original visual structure during translation.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional, Tuple


class LayoutType(Enum):
    """Classification of a text element in the PDF layout."""
    TITLE = "title"
    PARAGRAPH = "paragraph"
    LIST_ITEM = "list_item"
    TABLE_CELL = "table_cell"
    FORM_FIELD = "form_field"
    DOTTED_PLACEHOLDER = "dotted_placeholder"
    HEADER_FOOTER = "header_footer"
    PAGE_NUMBER = "page_number"
    WHITESPACE = "whitespace"
    CAPTION = "caption"


@dataclass
class LayoutSpan:
    """A single span with its style metadata."""
    text: str
    bbox: Tuple[float, float, float, float]
    font: str = ""
    size: float = 10.0
    flags: int = 0
    color: int = 0

    @property
    def is_bold(self) -> bool:
        """True if the span is rendered in bold weight.

        PyMuPDF flag bit 4 (value 16) is the authoritative bold indicator.
        We fall back to the font-name heuristic **only** when all flags are
        zero, which happens with some PDF generators that do not populate the
        font-descriptor flags field.  Relying solely on the font name causes
        false positives for fonts whose family name contains the word "Bold"
        (e.g. "ArialMT,Bold" as a *regular* member of a bold-weight family),
        leading to table cells and body text being rendered in bold when they
        should not be.
        """
        if self.flags:
            return bool(self.flags & 16)
        # flags == 0: fall back to name — search for standalone "bold" or
        # common abbreviations used in PostScript font names.
        fn = (self.font or "").lower()
        return "bold" in fn  # common in PostScript names: "-BoldMT", "Bold", "BoldItalic"

    @property
    def is_italic(self) -> bool:
        """True if the span is rendered in italic / oblique style.

        Same flag-first strategy as ``is_bold``: bit 1 (value 2) in the
        PyMuPDF flags byte is the authoritative italic indicator.
        """
        if self.flags:
            return bool(self.flags & 2)
        fn = (self.font or "").lower()
        return "italic" in fn or "oblique" in fn


@dataclass
class LayoutLine:
    """A single line extracted from the PDF with layout classification."""
    text: str
    bbox: Tuple[float, float, float, float]
    spans: List[LayoutSpan] = field(default_factory=list)
    layout_type: LayoutType = LayoutType.PARAGRAPH
    alignment: str = "left"  # left, center, right
    indent_level: int = 0
    font_size: float = 10.0
    is_bold: bool = False
    is_italic: bool = False
    line_spacing: float = 0.0

    @property
    def x0(self) -> float:
        return self.bbox[0]

    @property
    def y0(self) -> float:
        return self.bbox[1]

    @property
    def x1(self) -> float:
        return self.bbox[2]

    @property
    def y1(self) -> float:
        return self.bbox[3]

    @property
    def width(self) -> float:
        return self.bbox[2] - self.bbox[0]

    @property
    def height(self) -> float:
        return self.bbox[3] - self.bbox[1]


# ── Patterns ──

# Bullet/list patterns: bullets, dashes, roman numerals, numbered items
_LIST_PATTERN = re.compile(
    r"^\s*("
    r"[\u2022\u2023\u25E6\u2043\u2219\u25AA\u25AB\u25CF\u25CB\u25A0\u25A1\u2605\u2606·•◦‣⁃]"  # unicode bullets
    r"|\-\s"            # dash + space
    r"|\*\s"            # asterisk + space 
    r"|\d{1,3}[.)]\s"   # numbered: 1. 2) etc
    r"|[a-zA-Z][.)]\s"  # lettered: a. b) etc
    r"|[ivxIVX]{1,4}[.)]\s"  # roman numerals
    r")",
    re.UNICODE,
)

# Dotted placeholder / fill-in-the-blank patterns
_DOTTED_PATTERN = re.compile(r"\.{3,}|_{3,}|-{5,}|…{2,}")

# Page number patterns
_PAGE_NUM_PATTERN = re.compile(
    r"^\s*(\d{1,4})\s*$"
    r"|^\s*[-–—]\s*\d{1,4}\s*[-–—]\s*$"
    r"|^\s*trang\s+\d{1,4}\s*$"
    r"|^\s*page\s+\d{1,4}\s*$",
    re.IGNORECASE,
)

# Form field labels (Vietnamese common patterns)
_FORM_FIELD_PATTERN = re.compile(
    r"(Họ\s*(và|&)?\s*tên|Ngày\s*sinh|Địa\s*chỉ|Số\s*(điện\s*thoại|CMND|CCCD)"
    r"|Giới\s*tính|Email|Mã\s*số|MSSV|MSV|\bLớp\b|Khóa\s*học|Chức\s*(vụ|danh)|Đơn\s*vị|Phòng|Ngày\s*tháng"
    r"|Số\s*hiệu|Ký\s*tên|Nơi\s*cấp|Quốc\s*tịch|Dân\s*tộc|Tôn\s*giáo|Sinh\s*viên"
    r"|Name|Date\s*of\s*birth|Address|Phone|ID\s*(No|Number)|Student\s*(ID|No\.?)?|Class)",
    re.IGNORECASE | re.UNICODE,
)


def _form_like_compact_line(text: str) -> bool:
    """Ô biểu mẫu ngắn (một cell / một mảnh hàng): có nhãn kiểu form, không phải đoạn văn dài."""
    t = (text or "").strip()
    if not t or len(t) > 240:
        return False
    if _FORM_FIELD_PATTERN.search(t):
        return True
    if re.search(
        r"\b(mssv|msv|email|lớp|khóa|họ\s+tên|sv\b|mã\s*số)\b",
        t,
        re.IGNORECASE,
    ):
        return True
    return False


def merge_form_row_layout_lines(
    lines: List[LayoutLine],
    *,
    y_tol: float = 3.5,
    max_h_gap: float = 36.0,
) -> List[LayoutLine]:
    """Gộp các layout line cùng hàng, kề ngang, giống ô biểu mẫu — tránh PDF tách MSSV/Email/Lớp thành nhiều ô nhưng chỉ dịch được ô đầu."""
    if len(lines) < 2:
        return lines

    sorted_lines = sorted(lines, key=lambda ll: (ll.y0, ll.x0))
    out: List[LayoutLine] = []
    i = 0
    n = len(sorted_lines)
    while i < n:
        base = sorted_lines[i]
        yref = (base.y0 + base.y1) / 2.0
        cluster: List[LayoutLine] = [base]
        j = i + 1
        while j < n:
            nxt = sorted_lines[j]
            ymid = (nxt.y0 + nxt.y1) / 2.0
            if abs(ymid - yref) > y_tol:
                break
            last = cluster[-1]
            if (nxt.x0 - last.x1) <= max_h_gap:
                cluster.append(nxt)
                j += 1
            else:
                break

        if len(cluster) >= 2 and all(_form_like_compact_line(ll.text) for ll in cluster):
            x0 = min(ll.bbox[0] for ll in cluster)
            y0 = min(ll.bbox[1] for ll in cluster)
            x1 = max(ll.bbox[2] for ll in cluster)
            y1 = max(ll.bbox[3] for ll in cluster)
            parts = [ll.text.strip() for ll in cluster if (ll.text or "").strip()]
            text = " ".join(parts)
            spans_flat: list = []
            for ll in cluster:
                spans_flat.extend(ll.spans or [])
            first = cluster[0]
            fs = first.font_size or 10.0
            for ll in cluster[1:]:
                try:
                    fs = max(float(fs), float(ll.font_size or 0))
                except Exception:
                    pass
            out.append(
                LayoutLine(
                    text=text,
                    bbox=(x0, y0, x1, y1),
                    spans=spans_flat,
                    layout_type=LayoutType.FORM_FIELD,
                    alignment=first.alignment,
                    indent_level=max(ll.indent_level for ll in cluster),
                    font_size=fs,
                    is_bold=any(ll.is_bold for ll in cluster),
                    is_italic=any(ll.is_italic for ll in cluster),
                )
            )
        else:
            out.extend(cluster)
        i = j

    out.sort(key=lambda ll: (ll.y0, ll.x0))
    return out


def classify_line(
    line_text: str,
    line_bbox: Tuple[float, float, float, float],
    spans: list,
    page_width: float,
    page_height: float,
    median_font_size: float,
    all_lines_x0: List[float],
) -> Tuple[LayoutType, str, int]:
    """Classify a text line into a layout type.

    Returns (layout_type, alignment, indent_level).
    """
    text = (line_text or "").rstrip()
    if not text.strip():
        return LayoutType.WHITESPACE, "left", 0

    x0, y0, x1, y1 = line_bbox
    line_width = x1 - x0

    # ── Font size from spans ──
    font_sizes = []
    is_bold = False
    is_italic = False
    for sp in (spans or []):
        sz = sp.get("size", 10.0) if isinstance(sp, dict) else getattr(sp, "size", 10.0)
        font_sizes.append(sz)
        fl = sp.get("flags", 0) if isinstance(sp, dict) else getattr(sp, "flags", 0)
        fn = sp.get("font", "") if isinstance(sp, dict) else getattr(sp, "font", "")
        if (fl & 16) or "bold" in str(fn).lower():
            is_bold = True
        if (fl & 2) or "italic" in str(fn).lower() or "oblique" in str(fn).lower():
            is_italic = True

    avg_size = sum(font_sizes) / len(font_sizes) if font_sizes else 10.0

    # ── Alignment detection ──
    left_margin = x0
    right_margin = page_width - x1
    center_offset = abs((left_margin + line_width / 2) - page_width / 2)

    alignment = "left"
    if center_offset < page_width * 0.05 and left_margin > page_width * 0.15:
        alignment = "center"
    elif right_margin < page_width * 0.08 and left_margin > page_width * 0.3:
        alignment = "right"

    # ── Indent detection ──
    typical_x0 = sorted(all_lines_x0)[len(all_lines_x0) // 4] if all_lines_x0 else x0
    indent_level = 0
    if x0 > typical_x0 + 15:
        indent_level = max(1, int((x0 - typical_x0) / 20))

    # ── Page number ──
    if _PAGE_NUM_PATTERN.match(text):
        # Typically at top or bottom of page
        if y0 < page_height * 0.08 or y1 > page_height * 0.92:
            return LayoutType.PAGE_NUMBER, alignment, 0

    # ── Header/Footer zone ──
    if y0 < page_height * 0.06 or y1 > page_height * 0.94:
        return LayoutType.HEADER_FOOTER, alignment, 0

    # ── Dotted placeholder ──
    if _DOTTED_PATTERN.search(text):
        # Check if this is a form field with label + dots
        if _FORM_FIELD_PATTERN.search(text):
            return LayoutType.FORM_FIELD, alignment, indent_level
        return LayoutType.DOTTED_PLACEHOLDER, alignment, indent_level

    # ── Form field (label without dots but has form-like structure) ──
    if _FORM_FIELD_PATTERN.search(text):
        stripped = text.strip()
        n_colon = stripped.count(":")
        if _DOTTED_PATTERN.search(text):
            return LayoutType.FORM_FIELD, alignment, indent_level
        if n_colon >= 2 and len(stripped) <= 320:
            return LayoutType.FORM_FIELD, alignment, indent_level
        if n_colon >= 1 and len(stripped) <= 120:
            return LayoutType.FORM_FIELD, alignment, indent_level
        if len(stripped) < 60:
            return LayoutType.FORM_FIELD, alignment, indent_level

    # ── List item ──
    if _LIST_PATTERN.match(text):
        return LayoutType.LIST_ITEM, alignment, indent_level

    # ── Title detection ──
    # Larger font, bold, or short centered text
    is_title = False
    if avg_size > median_font_size * 1.3 and is_bold:
        is_title = True
    elif avg_size > median_font_size * 1.5:
        is_title = True
    elif is_bold and alignment == "center" and len(text.strip()) < 80:
        is_title = True
    elif alignment == "center" and avg_size > median_font_size * 1.2 and len(text.strip()) < 100:
        is_title = True

    if is_title:
        return LayoutType.TITLE, alignment, 0

    # ── Caption (small text under images/tables) ──
    if avg_size < median_font_size * 0.85 and is_italic and len(text.strip()) < 120:
        return LayoutType.CAPTION, alignment, indent_level

    # ── Default: paragraph ──
    return LayoutType.PARAGRAPH, alignment, indent_level


def detect_page_layout(
    page_text_dict: dict,
    page_width: float,
    page_height: float,
) -> List[LayoutLine]:
    """Analyse a PyMuPDF page text dict and return classified layout lines.

    Args:
        page_text_dict: result of page.get_text("dict")
        page_width: page rect width
        page_height: page rect height

    Returns:
        List of LayoutLine objects in document order.
    """
    # First pass: collect all font sizes and x0 values for statistical analysis
    all_font_sizes: List[float] = []
    all_x0: List[float] = []
    raw_lines: list = []

    for block in (page_text_dict.get("blocks") or []):
        if block.get("type") != 0:
            continue
        for line in (block.get("lines") or []):
            spans = line.get("spans") or []
            if not spans:
                continue
            text = "".join((sp.get("text") or "") for sp in spans)
            if not text.strip():
                continue
            bbox = line.get("bbox")
            if not bbox:
                continue

            for sp in spans:
                sz = sp.get("size", 10.0)
                if sz > 0:
                    all_font_sizes.append(sz)

            all_x0.append(bbox[0])
            raw_lines.append((text, tuple(bbox), spans))

    if not raw_lines:
        return []

    # Compute median font size for classification thresholds
    sorted_sizes = sorted(all_font_sizes) if all_font_sizes else [10.0]
    median_size = sorted_sizes[len(sorted_sizes) // 2]

    # Second pass: classify each line
    result: List[LayoutLine] = []
    for text, bbox, spans in raw_lines:
        layout_type, alignment, indent_level = classify_line(
            text, bbox, spans, page_width, page_height, median_size, all_x0,
        )

        # Build LayoutSpan objects
        layout_spans = []
        for sp in spans:
            layout_spans.append(LayoutSpan(
                text=sp.get("text", ""),
                bbox=tuple(sp.get("bbox", (0, 0, 0, 0))),
                font=sp.get("font", ""),
                size=sp.get("size", 10.0),
                flags=sp.get("flags", 0),
                color=sp.get("color", 0),
            ))

        avg_size = sum(sp.size for sp in layout_spans) / len(layout_spans) if layout_spans else 10.0
        any_bold = any(sp.is_bold for sp in layout_spans)
        any_italic = any(sp.is_italic for sp in layout_spans)

        result.append(LayoutLine(
            text=text,
            bbox=bbox,
            spans=layout_spans,
            layout_type=layout_type,
            alignment=alignment,
            indent_level=indent_level,
            font_size=avg_size,
            is_bold=any_bold,
            is_italic=any_italic,
        ))

    # Compute line spacing for consecutive lines
    for i in range(1, len(result)):
        result[i].line_spacing = result[i].y0 - result[i - 1].y1

    return result


def detect_table_regions(page_text_dict: dict, page_width: float) -> List[Tuple[float, float, float, float]]:
    """Detect likely table regions by analysing column alignment patterns.

    Returns list of bounding boxes (x0, y0, x1, y1) of detected table areas.
    """
    blocks = page_text_dict.get("blocks") or []
    text_blocks = [b for b in blocks if b.get("type") == 0]

    if len(text_blocks) < 3:
        return []

    # Group blocks by vertical proximity (rows)
    sorted_blocks = sorted(text_blocks, key=lambda b: (b.get("bbox", [0])[1], b.get("bbox", [0])[0]))

    rows: List[List[dict]] = []
    current_row: List[dict] = []
    current_y = -1.0

    for block in sorted_blocks:
        bbox = block.get("bbox", [0, 0, 0, 0])
        y_mid = (bbox[1] + bbox[3]) / 2
        if current_y < 0 or abs(y_mid - current_y) < 8:
            current_row.append(block)
            current_y = y_mid
        else:
            if current_row:
                rows.append(current_row)
            current_row = [block]
            current_y = y_mid
    if current_row:
        rows.append(current_row)

    # Rows with 2+ blocks at same y-level suggest tabular layout
    table_regions: List[Tuple[float, float, float, float]] = []
    table_start = -1
    consecutive = 0

    for i, row in enumerate(rows):
        if len(row) >= 2:
            consecutive += 1
            if table_start < 0:
                table_start = i
        else:
            if consecutive >= 2:
                # We found a table region
                start_row = rows[table_start]
                end_row = rows[i - 1]
                x0 = min(b.get("bbox", [0])[0] for r in rows[table_start:i] for b in r)
                y0 = min(b.get("bbox", [0, 0])[1] for b in start_row)
                x1 = max(b.get("bbox", [0, 0, 0])[2] for r in rows[table_start:i] for b in r)
                y1 = max(b.get("bbox", [0, 0, 0, 0])[3] for b in end_row)
                table_regions.append((x0, y0, x1, y1))
            table_start = -1
            consecutive = 0

    if consecutive >= 2:
        start_row = rows[table_start]
        end_row = rows[-1]
        x0 = min(b.get("bbox", [0])[0] for r in rows[table_start:] for b in r)
        y0 = min(b.get("bbox", [0, 0])[1] for b in start_row)
        x1 = max(b.get("bbox", [0, 0, 0])[2] for r in rows[table_start:] for b in r)
        y1 = max(b.get("bbox", [0, 0, 0, 0])[3] for b in end_row)
        table_regions.append((x0, y0, x1, y1))

    return table_regions


def detect_table_regions_from_page(page) -> List[Tuple[float, float, float, float]]:
    """Detect table bounding boxes using PyMuPDF native table finder.

    Requires PyMuPDF >= 1.23.  Falls back to an empty list for older builds or
    pages that contain no detected tables.

    Args:
        page: A ``fitz.Page`` object (PyMuPDF).

    Returns:
        List of ``(x0, y0, x1, y1)`` bounding boxes for every detected table.
    """
    try:
        tabs = page.find_tables()
        return [tuple(t.bbox) for t in (tabs or [])]
    except (AttributeError, TypeError, Exception):
        return []


def has_selectable_text(page) -> bool:
    """Return ``True`` if the page contains extractable (non-scanned) text.

    Scanned / image-only pages return ``False`` and should be handled via OCR.

    Args:
        page: A ``fitz.Page`` object (PyMuPDF).
    """
    try:
        return bool((page.get_text("text") or "").strip())
    except Exception:
        return False
