"""Layout-preserving PDF renderer.

Translates PDF text while preserving the exact visual structure:
- Titles, paragraphs, lists, form fields, dotted placeholders
- Exact spacing, line breaks, alignment, table structure
- Font style (bold/italic/size) and color matching

Uses the layout detector to classify elements and applies type-specific
rendering strategies for faithful reproduction.
"""

from __future__ import annotations

import os
import re
import uuid
import io
from typing import Callable, Optional, Tuple

from ..extractor.pdf_layout import (
    LayoutLine,
    LayoutType,
    detect_page_layout,
    detect_table_regions,
    detect_table_regions_from_page,
    has_selectable_text,
)


class PdfLayoutRenderer:
    """Layout-aware PDF translator that preserves exact visual structure.

    Workflow:
      1. Detect document layout (titles, paragraphs, tables, lists, form fields, dotted placeholders)
      2. Translate text (Vietnamese → target language)
      3. Reconstruct layout preserving exact spacing, line breaks, alignment, table structure
    """

    def __init__(
        self,
        translate_fn: Callable[[str], str],
        *,
        translate_batch_fn: Optional[Callable[[list[str]], list[str]]] = None,
        download_folder: Optional[str] = None,
        skip_vietnamese_source: bool = False,
        target_is_vietnamese: bool = False,
    ):
        self.translate_fn = translate_fn
        self.translate_batch_fn = translate_batch_fn
        self.skip_vietnamese_source = bool(skip_vietnamese_source)
        self.target_is_vietnamese = bool(target_is_vietnamese)
        self.download_folder = download_folder or os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))),
            "downloads",
        )
        os.makedirs(self.download_folder, exist_ok=True)
        self.render_engine = (os.getenv("PDF_RENDER_ENGINE") or "reportlab").strip().lower()

        # Translation cache to avoid duplicate API calls
        self._cache: dict[str, str] = {}
        # Flag: reportlab Unicode fonts registered for this instance
        self._rl_fonts_ready: bool = False

    # ── Font resolution (same logic as FileService) ──

    @staticmethod
    def _find_font_dir() -> str:
        font_dir = os.environ.get("FONT_DIR", "")
        if font_dir and os.path.isdir(font_dir):
            return font_dir
        if os.path.isdir(r"C:\Windows\Fonts"):
            return r"C:\Windows\Fonts"
        if os.path.isdir("/usr/share/fonts/truetype"):
            return "/usr/share/fonts/truetype"
        return ""

    _FONT_FAMILIES = {
        "sans": ("arial.ttf", "arialbd.ttf", "ariali.ttf", "arialbi.ttf"),
        "serif": ("times.ttf", "timesbd.ttf", "timesi.ttf", "timesbi.ttf"),
        "mono": ("cour.ttf", "courbd.ttf", "couri.ttf", "courbi.ttf"),
    }
    _FONT_FAMILIES_LINUX = {
        "sans": ("DejaVuSans.ttf", "DejaVuSans-Bold.ttf", "DejaVuSans-Oblique.ttf", "DejaVuSans-BoldOblique.ttf"),
        "serif": ("DejaVuSerif.ttf", "DejaVuSerif-Bold.ttf", "DejaVuSerif-Italic.ttf", "DejaVuSerif-BoldItalic.ttf"),
        "mono": ("DejaVuSansMono.ttf", "DejaVuSansMono-Bold.ttf", "DejaVuSansMono-Oblique.ttf", "DejaVuSansMono-BoldOblique.ttf"),
    }

    def _find_font_file(self, family_key: str, variant_idx: int) -> Optional[str]:
        font_dir = self._find_font_dir()
        if not font_dir:
            return None
        for families in (self._FONT_FAMILIES, self._FONT_FAMILIES_LINUX):
            names = families.get(family_key, families["sans"])
            fname = names[variant_idx]
            path = os.path.join(font_dir, fname)
            if os.path.isfile(path):
                return path
            for root, _dirs, files in os.walk(font_dir):
                if fname in files:
                    return os.path.join(root, fname)
        return None

    def _resolve_font(self, layout_line: LayoutLine) -> Tuple[str, Optional[str], str]:
        """Resolve font name, file path, and family for a layout line."""
        font_name = ""
        raw_font_name = ""
        if layout_line.spans:
            raw_font_name = layout_line.spans[0].font or ""
            font_name = raw_font_name.lower()

        family = "sans"
        if any(k in font_name for k in ("times", "tiro", "serif", "georgia")):
            family = "serif"
        elif any(k in font_name for k in ("cour", "mono", "consol")):
            family = "mono"

        variant = 0
        if layout_line.is_bold and layout_line.is_italic:
            variant = 3
        elif layout_line.is_bold:
            variant = 1
        elif layout_line.is_italic:
            variant = 2

        fontfile = self._find_font_file(family, variant)
        if fontfile:
            internal = f"F{family[0]}{variant}"
            return internal, fontfile, family

        # Base14 fonts often cannot encode Vietnamese/Unicode text.
        # If the line contains non-ASCII chars, force a Unicode-capable sans font.
        line_text = (layout_line.text or "")
        if any(ord(ch) > 127 for ch in line_text):
            unicode_font = self._find_font_file("sans", variant) or self._find_font_file("sans", 0)
            if unicode_font:
                return f"Fu{variant}", unicode_font, "sans"

        # Try the original embedded font from the PDF before Base14 fallback.
        if raw_font_name:
            return raw_font_name, None, family

        base14_map = {
            ("sans", 0): "Helvetica", ("sans", 1): "Helvetica-Bold",
            ("sans", 2): "Helvetica-Oblique", ("sans", 3): "Helvetica-BoldOblique",
            ("serif", 0): "Times-Roman", ("serif", 1): "Times-Bold",
            ("serif", 2): "Times-Italic", ("serif", 3): "Times-BoldItalic",
            ("mono", 0): "Courier", ("mono", 1): "Courier-Bold",
            ("mono", 2): "Courier-Oblique", ("mono", 3): "Courier-BoldOblique",
        }
        return base14_map.get((family, variant), "Helvetica"), None, family

    # ── ReportLab Unicode font support ──

    def _setup_rl_fonts(self) -> None:
        """Register Unicode-capable TTF fonts with ReportLab (runs once per instance).

        Tries to register Arial/DejaVu variants for regular, bold, italic and
        bold-italic so that Vietnamese and other Unicode text renders correctly.
        Base-14 Helvetica/Times cannot encode multi-byte Unicode glyphs.
        """
        if self._rl_fonts_ready:
            return
        self._rl_fonts_ready = True  # Mark before any early return to avoid loops
        try:
            from reportlab.pdfbase import pdfmetrics
            from reportlab.pdfbase.ttfonts import TTFont
        except Exception:
            return

        # (logical_name, family_key, variant_index)
        # variant_index: 0=regular, 1=bold, 2=italic, 3=bold-italic
        registrations = [
            ("UniSans",             "sans",  0),
            ("UniSansBold",         "sans",  1),
            ("UniSansItalic",       "sans",  2),
            ("UniSansBoldItalic",   "sans",  3),
            ("UniSerif",            "serif", 0),
            ("UniSerifBold",        "serif", 1),
            ("UniSerifItalic",      "serif", 2),
            ("UniSerifBoldItalic",  "serif", 3),
            ("UniMono",             "mono",  0),
            ("UniMonoBold",         "mono",  1),
        ]
        for logical_name, family, variant_idx in registrations:
            # Skip if already registered (e.g., by another renderer instance)
            try:
                pdfmetrics.getFont(logical_name)
                continue
            except Exception:
                pass
            path = self._find_font_file(family, variant_idx)
            if not path:
                continue
            try:
                pdfmetrics.registerFont(TTFont(logical_name, path))
            except Exception:
                pass  # Non-fatal: fall back to Helvetica for that variant

    def _get_rl_font(self, is_bold: bool, is_italic: bool) -> str:
        """Return the best available ReportLab font name for a given style.

        Prefers Unicode-capable TTF fonts registered by ``_setup_rl_fonts()``.
        Falls back to the built-in Base-14 Helvetica variants when TTF is
        unavailable (note: Base-14 cannot render multi-byte Unicode glyphs).

        Args:
            is_bold:   The text should be rendered in bold weight.
            is_italic: The text should be rendered in italic / oblique style.

        Returns:
            A font name string suitable for ``Canvas.setFont()`` or
            ``Canvas.stringWidth()``.
        """
        try:
            from reportlab.pdfbase import pdfmetrics

            if is_bold and is_italic:
                candidates = ["UniSansBoldItalic", "UniSansBold", "Helvetica-BoldOblique"]
            elif is_bold:
                candidates = ["UniSansBold", "UniSans", "Helvetica-Bold"]
            elif is_italic:
                candidates = ["UniSansItalic", "UniSans", "Helvetica-Oblique"]
            else:
                candidates = ["UniSans", "Helvetica"]

            for name in candidates:
                try:
                    pdfmetrics.getFont(name)
                    return name
                except Exception:
                    pass
        except Exception:
            pass

        # Fallback to Base-14 (safe but no Unicode)
        if is_bold and is_italic:
            return "Helvetica-BoldOblique"
        if is_bold:
            return "Helvetica-Bold"
        if is_italic:
            return "Helvetica-Oblique"
        return "Helvetica"

    def _get_rl_font_by_family(
        self, family: str, is_bold: bool, is_italic: bool
    ) -> str:
        """Return the best available ReportLab font matching the original family.

        Unlike ``_get_rl_font`` (which always picks a sans-serif font), this
        method respects the font family detected from the source PDF span so
        that serif documents render with a serif font, monospaced text keeps
        its fixed-width appearance, and sans-serif text uses sans-serif.

        Candidate priority:
        - Registered Unicode TTF (registered by ``_setup_rl_fonts``).
        - Base-14 PDF font (no Unicode, but always available).

        Args:
            family:    ``'serif'``, ``'mono'``, or ``'sans'``.
            is_bold:   Bold weight flag.
            is_italic: Italic style flag.

        Returns:
            Font name suitable for ``Canvas.setFont()`` / ``Canvas.stringWidth()``.
        """
        try:
            from reportlab.pdfbase import pdfmetrics

            if family == "serif":
                if is_bold and is_italic:
                    candidates = ["UniSerifBoldItalic", "UniSerifBold", "UniSerif",
                                  "Times-BoldItalic"]
                elif is_bold:
                    candidates = ["UniSerifBold", "UniSerif", "Times-Bold"]
                elif is_italic:
                    candidates = ["UniSerifItalic", "UniSerif", "Times-Italic"]
                else:
                    candidates = ["UniSerif", "Times-Roman"]
            elif family == "mono":
                if is_bold:
                    candidates = ["UniMonoBold", "UniMono", "Courier-Bold"]
                else:
                    candidates = ["UniMono", "Courier"]
            else:  # sans (default)
                if is_bold and is_italic:
                    candidates = ["UniSansBoldItalic", "UniSansBold",
                                  "Helvetica-BoldOblique"]
                elif is_bold:
                    candidates = ["UniSansBold", "UniSans", "Helvetica-Bold"]
                elif is_italic:
                    candidates = ["UniSansItalic", "UniSans", "Helvetica-Oblique"]
                else:
                    candidates = ["UniSans", "Helvetica"]

            for name in candidates:
                try:
                    pdfmetrics.getFont(name)
                    return name
                except Exception:
                    pass
        except Exception:
            pass

        return self._get_rl_font(is_bold, is_italic)  # sans fallback

    @staticmethod
    def _int_color_to_rgb01(color_int: int) -> Tuple[float, float, float]:
        try:
            c = int(color_int)
        except Exception:
            c = 0
        return (((c >> 16) & 255) / 255.0, ((c >> 8) & 255) / 255.0, (c & 255) / 255.0)

    # ── Translation helpers ──

    @staticmethod
    def _is_probably_vietnamese(text: str) -> bool:
        if not text:
            return False
        core = text.strip().lower()
        if not core:
            return False
        # Reliable Vietnamese signal: dedicated diacritics/letters.
        return bool(re.search(r"[ăâđêôơưáàảãạấầẩẫậắằẳẵặéèẻẽẹếềểễệíìỉĩịóòỏõọốồổỗộớờởỡợúùủũụứừửữựýỳỷỹỵ]", core))

    @staticmethod
    def _table_glossary_translate(text: str) -> Optional[str]:
        core = (text or "").strip()
        if not core:
            return None
        key = re.sub(r"\s+", " ", core).strip().lower()
        glossary = {
            "userid": "Mã người dùng",
            "user id": "Mã người dùng",
            "username": "Tên đăng nhập",
            "password": "Mật khẩu",
            "role": "Vai trò",
            "primary key": "Khóa chính",
            "not null": "Không null",
            "null": "null",
            "unique": "Duy nhất",
            "constraint": "Ràng buộc",
            "data type": "Kiểu dữ liệu",
            "description": "Mô tả",
            "name": "Tên",
            "code": "Mã",
            "school": "Trường",
            "class": "Lớp",
            "email": "Email",
            "code person use": "Mã người dùng",
            "name post input": "Tên đăng nhập",
        }
        return glossary.get(key)

    def _should_translate(self, text: str) -> bool:
        if not text or not text.strip():
            return False
        core = text.strip()
        if re.fullmatch(r"[\d\W_]+", core, flags=re.UNICODE):
            return False
        # Skip tiny noisy fragments (often OCR/extraction artifacts near logos/headers).
        letters = re.findall(r"[A-Za-zÀ-ỹ]", core)
        if len(letters) < 2:
            return False
        if self.skip_vietnamese_source and self._is_probably_vietnamese(core):
            return False
        return True

    def _translate_cached(self, text: str) -> str:
        core = text.strip()
        if not self._should_translate(core):
            return text
        if core in self._cache:
            return self._cache[core]
        try:
            dst = self.translate_fn(core)
            dst = "" if dst is None else str(dst)
        except Exception:
            # Never drop content on translation failure; keep original text.
            dst = core
        if not dst.strip():
            # Empty model output should not erase source content.
            dst = core
        self._cache[core] = dst
        return dst

    def _translate_preserve_ws(self, text: str) -> str:
        src = "" if text is None else str(text)
        m = re.match(r"^(\s*)(.*?)(\s*)$", src, flags=re.DOTALL)
        lead, core, tail = (m.group(1), m.group(2), m.group(3)) if m else ("", src, "")
        if not self._should_translate(core):
            return src
        dst = self._translate_cached(core)
        return f"{lead}{dst}{tail}"

    def _translate_preserve_structure(self, text: str, layout_type: LayoutType) -> str:
        """Translate text while preserving structural elements based on layout type."""
        if layout_type == LayoutType.TABLE_CELL and self.target_is_vietnamese:
            mapped = self._table_glossary_translate(text)
            if mapped:
                m = re.match(r"^(\s*)(.*?)(\s*)$", text or "", flags=re.DOTALL)
                lead, _core, tail = (m.group(1), m.group(2), m.group(3)) if m else ("", text or "", "")
                return f"{lead}{mapped}{tail}"

        if layout_type == LayoutType.DOTTED_PLACEHOLDER:
            # Preserve dotted/underscored placeholders exactly
            parts = re.split(r"(\.{3,}|_{3,}|-{5,}|…{2,})", text)
            result = []
            for i, part in enumerate(parts):
                if i % 2 == 1:
                    result.append(part)  # Keep placeholder as-is
                elif part.strip() and self._should_translate(part):
                    result.append(self._translate_preserve_ws(part))
                else:
                    result.append(part)
            return "".join(result)

        if layout_type == LayoutType.FORM_FIELD:
            # For form fields: translate the label, preserve dots/lines
            parts = re.split(r"(\.{3,}|_{3,}|-{5,}|…{2,}|\s*:\s*)", text)
            result = []
            for i, part in enumerate(parts):
                if i % 2 == 1:
                    result.append(part)  # Keep delimiter/placeholder
                elif part.strip() and self._should_translate(part):
                    result.append(self._translate_preserve_ws(part))
                else:
                    result.append(part)
            return "".join(result)

        if layout_type == LayoutType.LIST_ITEM:
            # Preserve bullet/number prefix
            m = re.match(
                r"^(\s*[\u2022\u2023\u25E6\u2043\u2219\u25AA\u25AB\u25CF\u25CB\u25A0\u25A1·•◦‣⁃\-\*]"
                r"|\s*\d{1,3}[.)]\s"
                r"|\s*[a-zA-Z][.)]\s"
                r"|\s*[ivxIVX]{1,4}[.)]\s)(.*)",
                text,
                re.UNICODE,
            )
            if m:
                prefix = m.group(1)
                body = m.group(2)
                if self._should_translate(body):
                    return prefix + self._translate_cached(body.strip())
                return text

        if layout_type in (LayoutType.PAGE_NUMBER, LayoutType.WHITESPACE):
            return text  # Never translate

        # Default: translate full text
        return self._translate_preserve_ws(text)

    def _extract_layout_lines_from_ocr(self, fitz, page) -> list:
        """Fallback OCR for scanned/image-only PDF pages.

        Uses Tesseract for OCR and layoutparser to normalize reading order.
        """
        try:
            import pytesseract
            from pytesseract import Output
            from PIL import Image
        except Exception:
            return []

        try:
            pix = page.get_pixmap(dpi=220)
            pil = Image.open(io.BytesIO(pix.tobytes("png"))).convert("RGB")
            w_img, h_img = pil.size
            if w_img <= 0 or h_img <= 0:
                return []
        except Exception:
            return []

        langs = (os.getenv("OCR_LANGS_DEFAULT") or "eng+vie").strip() or "eng+vie"
        try:
            d = pytesseract.image_to_data(
                pil,
                lang=langs,
                config="--oem 3 --psm 6 -c preserve_interword_spaces=1",
                output_type=Output.DICT,
            )
        except Exception:
            return []

        n = len(d.get("text", []) or [])
        lines = {}
        for i in range(n):
            word = (d.get("text", [""])[i] or "").strip()
            if not word:
                continue
            try:
                conf = float(d.get("conf", ["-1"])[i])
            except Exception:
                conf = -1.0
            if conf != -1 and conf < 20:
                continue

            left = int(d.get("left", [0])[i] or 0)
            top = int(d.get("top", [0])[i] or 0)
            width = int(d.get("width", [0])[i] or 0)
            height = int(d.get("height", [0])[i] or 0)
            if width <= 1 or height <= 1:
                continue
            key = (
                int(d.get("block_num", [0])[i] or 0),
                int(d.get("par_num", [0])[i] or 0),
                int(d.get("line_num", [0])[i] or 0),
            )
            entry = lines.get(key)
            if not entry:
                lines[key] = {
                    "tokens": [word],
                    "left": left,
                    "top": top,
                    "right": left + width,
                    "bottom": top + height,
                }
            else:
                entry["tokens"].append(word)
                entry["left"] = min(entry["left"], left)
                entry["top"] = min(entry["top"], top)
                entry["right"] = max(entry["right"], left + width)
                entry["bottom"] = max(entry["bottom"], top + height)

        if not lines:
            return []

        page_w = float(page.rect.width)
        page_h = float(page.rect.height)
        ocr_items = []
        for entry in lines.values():
            text = " ".join(entry.get("tokens") or []).strip()
            if not text:
                continue
            x0 = (float(entry["left"]) / float(w_img)) * page_w
            y0 = (float(entry["top"]) / float(h_img)) * page_h
            x1 = (float(entry["right"]) / float(w_img)) * page_w
            y1 = (float(entry["bottom"]) / float(h_img)) * page_h
            if x1 - x0 < 1 or y1 - y0 < 1:
                continue
            ocr_items.append({"text": text, "bbox": (x0, y0, x1, y1)})

        if not ocr_items:
            return []

        try:
            import layoutparser as lp

            lp_blocks = []
            for idx, item in enumerate(ocr_items):
                x0, y0, x1, y1 = item["bbox"]
                block = lp.TextBlock(
                    block=lp.Rectangle(x_1=x0, y_1=y0, x_2=x1, y_2=y1),
                    text=item["text"],
                    id=idx,
                    type="ocr_line",
                )
                lp_blocks.append(block)
            _ = lp.Layout(lp_blocks)
        except Exception:
            pass

        ocr_items.sort(key=lambda x: (x["bbox"][1], x["bbox"][0]))
        out = []
        for item in ocr_items:
            x0, y0, x1, y1 = item["bbox"]
            out.append(LayoutLine(
                text=item["text"],
                bbox=(x0, y0, x1, y1),
                spans=[],
                layout_type=LayoutType.PARAGRAPH,
                alignment="left",
                indent_level=0,
                font_size=max(8.0, min(12.0, (y1 - y0) * 0.85)),
                is_bold=False,
                is_italic=False,
            ))
        return out

    # ── Paragraph merging ──────────────────────────────────────────
    @staticmethod
    def _merge_paragraph_lines(layout_lines: list, should_translate_fn=None) -> list:
        """Merge consecutive PARAGRAPH lines into single entries.

        Lines that share a similar left margin, are closely spaced
        vertically, and are both translatable body text are merged so that
        the whole sentence / paragraph is translated as one unit.

        If the previous line's text ends without sentence-ending punctuation
        (i.e. the sentence continues), the next line is merged even if it
        has a different layout type (e.g. DOTTED_PLACEHOLDER).
        """
        if not layout_lines:
            return layout_lines

        _MAX_MERGE = 5
        _mergeable = {LayoutType.PARAGRAPH, LayoutType.LIST_ITEM}
        # Types that can be pulled into an existing group when the previous
        # line's text clearly continues (no ending punctuation).
        _continuable = _mergeable | {LayoutType.DOTTED_PLACEHOLDER,
                                      LayoutType.FORM_FIELD}

        def _sentence_continues(text: str) -> bool:
            """True when *text* ends mid-sentence (no final punctuation).

            A closing ')' only counts as sentence-ending if the line does
            NOT start with '(' — i.e. the parenthesised block opened on a
            prior line, so this line closes the *whole* expression.
            """
            t = re.sub(r'[.\s…·]+$', '', (text or '').strip())
            if not t:
                return False
            raw = (text or '').strip()
            # If line starts with '(' and ends with ')', the ')' closes
            # an *inner* parenthetical — sentence likely continues.
            if t[-1] == ')' and raw.startswith('('):
                # Check if the closing ')' matches the opening '('
                depth = 0
                for ch in t:
                    if ch == '(':
                        depth += 1
                    elif ch == ')':
                        depth -= 1
                if depth == 0:
                    # All parens balanced — this ')' closes the opening '('
                    # but the outer sentence might still continue. Strip the
                    # balanced block and re-check what's left.
                    inner = re.sub(r'\([^)]*\)\s*$', '', t).strip()
                    inner = re.sub(r'[.\s…·]+$', '', inner)
                    if inner and inner[-1] not in '.?!;:':
                        return True
                else:
                    # More opens than closes — sentence continues
                    return True
            return t[-1] not in '.?!;:)»"\'"""'

        _heading_re = re.compile(
            r'^(?:\d+[\.\)]\s|[a-zA-Z][\.\)]\s|[-–—•+]\s|[IVX]+[\.\)]\s)',
        )

        def _is_heading_or_item(text: str) -> bool:
            """True when line starts with a numbered/bulleted heading."""
            return bool(_heading_re.match((text or '').strip()))

        def _is_label_line(text: str) -> bool:
            """True when line looks like a section label (contains `:` separator).

            Matches patterns like 'Tóm tắt nội dung đề tài:' or
            'Mục tiêu: Xây dựng hệ thống...' but not times (10:30),
            URLs (http://), ratios (1:2), or colons inside parentheses/
            brackets like (string: "value").
            """
            t = (text or '').strip()
            for m in re.finditer(r':', t):
                pos = m.start()
                if pos < 4:
                    continue
                if pos > 0 and t[pos - 1].isdigit():
                    continue
                if pos + 2 < len(t) and t[pos + 1:pos + 3] == '//':
                    continue
                # Ignore colons inside parentheses or brackets
                depth = 0
                for ch in t[:pos]:
                    if ch in '([': depth += 1
                    elif ch in ')]': depth -= 1
                if depth > 0:
                    continue
                if pos == len(t) - 1 or (pos < len(t) - 1 and t[pos + 1] == ' '):
                    return True
            return False

        merged: list = []
        i = 0
        while i < len(layout_lines):
            ll = layout_lines[i]
            if ll.layout_type not in _mergeable:
                merged.append(ll)
                i += 1
                continue
            if should_translate_fn and not should_translate_fn(ll.text):
                merged.append(ll)
                i += 1
                continue
            # Numbered/bulleted headings stay standalone — never start a group
            if _is_heading_or_item((ll.text or '').strip()):
                merged.append(ll)
                i += 1
                continue

            group = [ll]
            j = i + 1
            while j < len(layout_lines) and len(group) < _MAX_MERGE:
                nxt = layout_lines[j]
                if should_translate_fn and not should_translate_fn(nxt.text):
                    # When sentence continues, skip over WHITESPACE lines
                    prev_text_chk = (group[-1].text or "").strip()
                    if (_sentence_continues(prev_text_chk)
                            and nxt.layout_type == LayoutType.WHITESPACE):
                        j += 1
                        continue
                    break

                # Never pull a numbered/bulleted heading into a group
                if _is_heading_or_item((nxt.text or '').strip()):
                    break
                # Never pull a section-label line into an existing group
                if _is_label_line((nxt.text or '').strip()):
                    break

                # Decide whether this next line can join the group.
                prev = group[-1]
                prev_text = (prev.text or "").strip()
                continuing = _sentence_continues(prev_text)

                # Type check: must be in _mergeable normally, but
                # _continuable types are OK when the sentence continues.
                if nxt.layout_type not in _mergeable:
                    if not (continuing and nxt.layout_type in _continuable):
                        break

                # Style checks (skip when sentence clearly continues)
                if not continuing:
                    if nxt.is_italic != ll.is_italic:
                        break
                    if nxt.is_bold != ll.is_bold:
                        break

                prev_fs = prev.font_size or 10.0
                # Vertical gap: next line starts near where previous ends
                # Allow larger gaps when the sentence clearly continues.
                v_gap = nxt.bbox[1] - prev.bbox[3]
                max_vgap = prev_fs * (2.5 if continuing else 1.5)
                if v_gap > max_vgap or v_gap < -2:
                    break
                # Left margin: skip check when sentence clearly continues
                # (centered/indented text has varying x0 per line)
                if not continuing:
                    if abs(nxt.bbox[0] - prev.bbox[0]) > prev_fs * 3:
                        break
                # Font size must be similar
                if abs((nxt.font_size or 10.0) - (prev.font_size or 10.0)) > 2.0:
                    break
                group.append(nxt)
                j += 1

            if len(group) == 1:
                merged.append(ll)
            else:
                # Strip filler dots from DOTTED_PLACEHOLDER lines when merging
                parts: list[str] = []
                for g in group:
                    t = (g.text or "").strip()
                    if g.layout_type in (LayoutType.DOTTED_PLACEHOLDER,
                                         LayoutType.FORM_FIELD):
                        t = re.sub(r'[.\s…·]{4,}', ' ', t).strip()
                    if t:
                        parts.append(t)
                m_text = " ".join(parts)
                m_bbox = (
                    min(g.bbox[0] for g in group),
                    group[0].bbox[1],
                    max(g.bbox[2] for g in group),
                    group[-1].bbox[3],
                )
                m_spans: list = []
                for g in group:
                    m_spans.extend(g.spans)
                merged.append(LayoutLine(
                    text=m_text,
                    bbox=m_bbox,
                    spans=m_spans,
                    layout_type=LayoutType.PARAGRAPH,
                    alignment=group[0].alignment,
                    indent_level=group[0].indent_level,
                    font_size=group[0].font_size,
                    is_bold=group[0].is_bold,
                    is_italic=group[0].is_italic,
                    line_spacing=group[0].line_spacing,
                ))
            i = j

        return merged

    def _render_with_reportlab(
        self,
        fitz,
        doc,
        output_path: str,
        *,
        bilingual_mode: str,
        bilingual_delimiter: str,
        progress_cb,
    ) -> str:
        """Render translated PDF using reportlab with page-image background.

        This mode is robust for scanned PDFs and complex visual layouts.

        Improvements over the original:
        - Unicode-capable TTF fonts (Vietnamese / CJK / Cyrillic etc.)
        - Preserves original font size and bold/italic style per line.
        - Original text colour is preserved where it differs from black.
        - Multi-line word-wrap when translated text overflows the cell width.
        - Tables are translated (no longer silently skipped).
        """
        try:
            from reportlab.pdfgen import canvas
            from reportlab.lib.utils import ImageReader
        except Exception as e:
            raise RuntimeError("reportlab is required for PDF_RENDER_ENGINE=reportlab") from e

        # Register Unicode fonts once before we start drawing.
        self._setup_rl_fonts()

        c = canvas.Canvas(output_path)
        total_pages = doc.page_count
        for page_idx in range(total_pages):
            page = doc.load_page(page_idx)
            page_w = float(page.rect.width)
            page_h = float(page.rect.height)

            pix = page.get_pixmap(dpi=170)
            bg = ImageReader(io.BytesIO(pix.tobytes("png")))

            # ─────────────────────────────────────────────────
            # PHASE 1 — EXTRACT
            # ─────────────────────────────────────────────────
            text_dict = page.get_text("dict")
            layout_lines = detect_page_layout(text_dict, page_w, page_h)
            if not layout_lines:
                layout_lines = self._extract_layout_lines_from_ocr(fitz, page)

            # ─────────────────────────────────────────────────
            # PHASE 2 — CLEAN  (merge paragraphs)
            # ─────────────────────────────────────────────────
            layout_lines = self._merge_paragraph_lines(
                layout_lines, self._should_translate)

            # ─────────────────────────────────────────────────
            # PHASE 3 — TRANSLATE  (all lines upfront)
            # ─────────────────────────────────────────────────
            translations: dict[int, str] = {}
            for _li, _ll in enumerate(layout_lines):
                if not self._should_translate(_ll.text):
                    continue
                if _ll.layout_type in (LayoutType.PAGE_NUMBER, LayoutType.WHITESPACE):
                    continue
                try:
                    _t = self._translate_preserve_structure(_ll.text, _ll.layout_type)
                except Exception:
                    _t = ""
                if str(_t).strip():
                    translations[_li] = str(_t).strip()

            # ─────────────────────────────────────────────────
            # PHASE 4 — REBUILD LAYOUT
            #   4a. Resolve font/color/size for each translated line
            #   4b. Build final text lines & measure needed height
            #   4c. Compute insertion gaps & strips
            #   4d. Draw background → white-out → text
            # ─────────────────────────────────────────────────

            # Helper: resolve font info for a layout line
            def _resolve_font(ll):
                if ll.spans:
                    dom = max(ll.spans, key=lambda s: len(s.text.strip()))
                    _bold   = dom.is_bold
                    _italic = dom.is_italic
                    _size   = max(6.0, float(dom.size or ll.font_size or 10.0))
                    _color  = dom.color
                    _rfn    = (dom.font or "").lower()
                else:
                    _bold   = ll.is_bold
                    _italic = ll.is_italic
                    _size   = max(6.0, float(ll.font_size or 10.0))
                    _color  = 0
                    _rfn    = ""
                if any(k in _rfn for k in ("times", "tiro", "serif", "georgia",
                                           "cambria", "palatino")):
                    _fam = "serif"
                elif any(k in _rfn for k in ("cour", "mono", "consol", "lucida")):
                    _fam = "mono"
                else:
                    _fam = "sans"
                _fn = self._get_rl_font_by_family(_fam, _bold, _italic)
                _r, _g, _b = self._int_color_to_rgb01(_color)
                if _r > 0.95 and _g > 0.95 and _b > 0.95:
                    _r, _g, _b = 0.0, 0.0, 0.0
                return _fn, _size, (_r, _g, _b)

            LINE_H_MULT = 1.1  # single source of truth

            # 4a/4b — Build draw records: each entry has all info to
            # white-out and draw one translated block.
            # Record: (ll_idx, ll, font_name, fs, color, x0, y0, w, h,
            #          draw_w, text_lines, needed_h)
            draw_records: list = []
            for ll_idx, ll in enumerate(layout_lines):
                if ll_idx not in translations:
                    continue
                src_text = (ll.text or "").strip()
                dst_text = translations[ll_idx]

                x0, y0, x1, y1 = ll.bbox
                w = max(1.0, x1 - x0)
                h = max(1.0, y1 - y0)
                font_name, fs, color = _resolve_font(ll)

                # Compute drawing width
                draw_w = w
                is_table = ll.layout_type == LayoutType.TABLE_CELL
                if bilingual_mode == "inline" and not is_table:
                    right_pad = max(12.0, min(x0, 40.0))
                    draw_w = max(w, page_w - x0 - right_pad)

                # Build the final text lines for this block
                text_lines: list[str] = []
                if bilingual_mode == "inline" and not is_table:
                    # TITLE: shrink font to fit on 1 line
                    if ll.layout_type == LayoutType.TITLE:
                        _joined = self._join_inline_bilingual(
                            src_text, dst_text, bilingual_delimiter)
                        c.setFont(font_name, fs)
                        _tw = c.stringWidth(_joined, font_name, fs)
                        _min_fs = max(6.0, fs * 0.65)
                        while _tw > draw_w and fs > _min_fs:
                            fs -= 0.3
                            _tw = c.stringWidth(_joined, font_name, fs)

                    c.setFont(font_name, fs)
                    def _pm(s, _fn=font_name, _fs=fs):
                        return float(c.stringWidth(s or "", _fn, _fs))

                    text_lines, _ = self._build_inline_bilingual_lines(
                        src_text, dst_text, bilingual_delimiter,
                        draw_w, _pm,
                    )
                elif bilingual_mode == "inline" and is_table:
                    # Table cell: try src|dst, fallback to dst-only,
                    # then shrink, then wrap
                    draw_text = self._join_inline_bilingual(
                        src_text, dst_text, bilingual_delimiter)
                    c.setFont(font_name, fs)
                    tw = c.stringWidth(draw_text, font_name, fs)
                    if tw > w:
                        draw_text = dst_text
                        tw = c.stringWidth(draw_text, font_name, fs)
                    if tw > w:
                        min_fs = max(5.0, fs * 0.6)
                        while tw > w and fs > min_fs:
                            fs -= 0.3
                            tw = c.stringWidth(draw_text, font_name, fs)
                    text_lines = self._wrap_words_for_width(
                        draw_text, w,
                        lambda s, _fn=font_name, _fs=fs: float(
                            c.stringWidth(s or "", _fn, _fs)),
                    )
                elif ll.layout_type in (LayoutType.DOTTED_PLACEHOLDER,
                                        LayoutType.FORM_FIELD):
                    draw_text = dst_text
                    if bilingual_mode == "inline":
                        draw_text = self._join_inline_bilingual(
                            src_text, dst_text, bilingual_delimiter)
                    draw_text = self._normalize_dot_fill_rl(
                        c, draw_w, draw_text, font_name, fs)
                    c.setFont(font_name, fs)
                    text_lines = self._wrap_words_for_width(
                        draw_text, draw_w,
                        lambda s, _fn=font_name, _fs=fs: float(
                            c.stringWidth(s or "", _fn, _fs)),
                    )
                else:
                    # replace mode (translation only)
                    draw_text = dst_text
                    c.setFont(font_name, fs)
                    if ll.layout_type == LayoutType.TITLE:
                        tw = c.stringWidth(draw_text, font_name, fs)
                        min_fs = max(6.0, fs * 0.92)
                        while tw > w and fs > min_fs:
                            fs -= 0.2
                            tw = c.stringWidth(draw_text, font_name, fs)
                    text_lines = self._wrap_words_for_width(
                        draw_text, draw_w,
                        lambda s, _fn=font_name, _fs=fs: float(
                            c.stringWidth(s or "", _fn, _fs)),
                    )

                needed_h = max(h, len(text_lines) * fs * LINE_H_MULT)

                draw_records.append({
                    "idx": ll_idx, "ll": ll,
                    "font": font_name, "fs": fs, "color": color,
                    "x0": x0, "y0": y0, "w": w, "h": h,
                    "draw_w": draw_w, "lines": text_lines,
                    "needed_h": needed_h,
                })

            # 4c — Compute insertion gaps
            insertions: list[tuple[float, float]] = []
            if bilingual_mode == "inline":
                for rec in draw_records:
                    if rec["ll"].layout_type == LayoutType.TABLE_CELL:
                        continue
                    _extra_need = rec["needed_h"] - rec["h"]
                    if _extra_need <= 0:
                        continue
                    _y0 = rec["y0"]
                    _y1 = _y0 + rec["h"]
                    # Find next line's y0 — must start BELOW the
                    # current block's bottom edge (_y1).
                    _ny = page_h
                    for _j in range(rec["idx"] + 1, len(layout_lines)):
                        _nj_y0 = layout_lines[_j].bbox[1]
                        if _nj_y0 >= _y1 - 1:
                            _ny = _nj_y0
                            break
                    _ny = max(_ny, _y1)
                    _gap = _ny - _y0
                    _extra = rec["needed_h"] - _gap
                    if _extra > 0:
                        insertions.append((_ny, _extra))

            insertions.sort(key=lambda x: x[0])
            merged_ins: list[tuple[float, float]] = []
            for ins_y, extra in insertions:
                if merged_ins and abs(merged_ins[-1][0] - ins_y) < 5:
                    merged_ins[-1] = (merged_ins[-1][0],
                                      max(merged_ins[-1][1], extra))
                else:
                    merged_ins.append((ins_y, extra))

            strips: list[tuple[float, float, float]] = []
            _cum = 0.0
            _prev = 0.0
            for ins_y, extra in merged_ins:
                if ins_y > _prev:
                    strips.append((_prev, ins_y, _cum))
                _cum += extra
                _prev = ins_y
            if _prev < page_h:
                strips.append((_prev, page_h, _cum))
            total_shift = _cum

            def _cs_at(py: float) -> float:
                cs = 0.0
                for ins_y, extra in merged_ins:
                    if ins_y <= py:
                        cs += extra
                    else:
                        break
                return cs

            eff_h = page_h + total_shift

            # 4d-i — Draw background image as strips
            c.setPageSize((page_w, eff_h))
            if not strips or total_shift < 0.5:
                c.drawImage(bg, 0, 0, width=page_w, height=page_h,
                            preserveAspectRatio=False, mask="auto")
            else:
                for s_y0, s_y1, s_cs in strips:
                    img_offset = total_shift - s_cs
                    rl_top = eff_h - (s_y0 + s_cs)
                    rl_bot = eff_h - (s_y1 + s_cs)
                    s_h = rl_top - rl_bot
                    if s_h < 0.5:
                        continue
                    c.saveState()
                    p = c.beginPath()
                    p.rect(0, rl_bot, page_w, s_h)
                    c.clipPath(p, stroke=0, fill=0)
                    c.drawImage(bg, 0, img_offset, width=page_w,
                                height=page_h,
                                preserveAspectRatio=False, mask="auto")
                    c.restoreState()

            # 4d-ii — White-out ALL blocks first (with small padding)
            _WO_PAD = 2.0  # pixels padding on each side
            c.setFillColorRGB(1, 1, 1)
            for rec in draw_records:
                _cs = _cs_at(rec["y0"])
                _yb = eff_h - (rec["y0"] + rec["h"] + _cs)
                _overflow = max(0.0, rec["needed_h"] - rec["h"])
                c.rect(rec["x0"] - _WO_PAD,
                       _yb - _overflow - _WO_PAD,
                       rec["draw_w"] + 2 * _WO_PAD,
                       rec["needed_h"] + 2 * _WO_PAD,
                       fill=1, stroke=0)

            # 4d-iii — Draw ALL text (no white-out can interfere now)
            for rec in draw_records:
                ll = rec["ll"]
                x0 = rec["x0"]
                fs = rec["fs"]
                w = rec["w"]
                h = rec["h"]
                draw_w = rec["draw_w"]
                font_name = rec["font"]
                r_col, g_col, b_col = rec["color"]
                text_lines = rec["lines"]

                _cs = _cs_at(rec["y0"])
                y_bottom = eff_h - (rec["y0"] + rec["h"] + _cs)

                c.setFont(font_name, fs)
                c.setFillColorRGB(r_col, g_col, b_col)

                line_h = fs * LINE_H_MULT
                y_cur = y_bottom + h - fs  # first line at top of bbox

                for i, line in enumerate(text_lines):
                    tw = c.stringWidth(line, font_name, fs)
                    use_w = w if ll.layout_type == LayoutType.TABLE_CELL else draw_w
                    draw_x = x0
                    if ll.alignment == "center" and tw < use_w:
                        draw_x = x0 + (use_w - tw) / 2.0
                    elif ll.alignment == "right" and tw < use_w:
                        draw_x = x0 + use_w - tw
                    c.drawString(draw_x, y_cur, line)
                    y_cur -= line_h

            c.showPage()
            if progress_cb:
                pct = int(5 + ((page_idx + 1) / max(1, total_pages)) * 90)
                progress_cb(min(99, pct), f"PDF(reportlab): page {page_idx + 1}/{total_pages}")

        c.save()
        return output_path

    # ── PDF → DOCX → Translate → PDF pipeline ──

    def translate_pdf_via_docx(
        self,
        input_path: str,
        output_path: Optional[str] = None,
        *,
        bilingual_mode: str = "inline",
        bilingual_delimiter: str = "|",
        progress_cb: Optional[Callable[[int, str], None]] = None,
    ) -> str:
        """Translate PDF by converting to DOCX first, then translating, then
        exporting back to PDF.

        Pipeline: PDF → DOCX (pdf2docx) → Translate DOCX → PDF (LibreOffice headless)

        This approach preserves layout ~80-90% because DOCX has proper text
        structure (paragraphs, tables, styles) vs PDF's flat coordinate model.
        """
        import tempfile

        if not os.path.exists(input_path):
            raise FileNotFoundError(input_path)

        if output_path is None:
            base = os.path.splitext(os.path.basename(input_path))[0]
            out_name = f"{base}_translated_{uuid.uuid4().hex[:8]}.pdf"
            output_path = os.path.join(self.download_folder, out_name)

        bi_mode = (bilingual_mode or "inline").strip().lower()
        if bi_mode not in ("none", "inline", "newline"):
            bi_mode = "inline"

        # Step 1: PDF → DOCX
        if progress_cb:
            progress_cb(5, "PDF→DOCX: converting...")
        try:
            from pdf2docx import Converter
        except ImportError as e:
            raise RuntimeError("pdf2docx is required. Install: pip install pdf2docx") from e

        tmp_dir = tempfile.mkdtemp(prefix="pdf_via_docx_")
        docx_path = os.path.join(tmp_dir, "converted.docx")
        translated_docx_path = os.path.join(tmp_dir, "translated.docx")

        try:
            cv = Converter(input_path)
            cv.convert(docx_path)
            cv.close()
        except Exception as e:
            raise RuntimeError(f"PDF to DOCX conversion failed: {e}") from e

        if progress_cb:
            progress_cb(15, "PDF→DOCX: conversion done")

        # Step 2: Translate DOCX using existing DocxRenderer
        if progress_cb:
            progress_cb(20, "Translating DOCX...")

        from .docx import DocxRenderer
        docx_renderer = DocxRenderer(
            self.translate_fn,
            translate_batch_fn=self.translate_batch_fn,
        )

        def _docx_progress(pct, msg):
            # Map DOCX progress (0-100) to overall (20-85)
            mapped = 20 + int(pct * 0.65)
            if progress_cb:
                progress_cb(min(85, mapped), msg)

        try:
            if bi_mode == "inline":
                # Use per-line method for pdf2docx output where multiple
                # logical lines are merged into one paragraph with \n runs.
                docx_renderer.translate_docx_bilingual_inline_per_line(
                    docx_path,
                    translated_docx_path,
                    delimiter=bilingual_delimiter or "|",
                    progress_cb=_docx_progress,
                )
            elif bi_mode == "newline":
                docx_renderer.translate_docx_bilingual_newline_paragraph(
                    docx_path,
                    translated_docx_path,
                    progress_cb=_docx_progress,
                )
            else:
                docx_renderer.translate_docx(
                    docx_path,
                    translated_docx_path,
                    progress_cb=_docx_progress,
                )
        except Exception as e:
            raise RuntimeError(f"DOCX translation failed: {e}") from e

        if progress_cb:
            progress_cb(85, "DOCX→PDF: LibreOffice headless...")

        # Step 3: DOCX → PDF
        # Engine selection:
        # - PDF_DOCX_EXPORT_ENGINE=auto (default):
        #     Windows -> docx2pdf first (better font/glyph fidelity), then LibreOffice fallback
        #     Others  -> LibreOffice first, then docx2pdf fallback
        # - PDF_DOCX_EXPORT_ENGINE=docx2pdf|libreoffice to force one engine
        try:
            import shutil
            import subprocess

            engine = (os.getenv("PDF_DOCX_EXPORT_ENGINE") or "auto").strip().lower()
            if engine not in ("auto", "docx2pdf", "libreoffice"):
                engine = "auto"

            def _resolve_lo_bin() -> Optional[str]:
                lo_paths = [
                    r"C:\Program Files\LibreOffice\program\soffice.exe",
                    r"C:\Program Files (x86)\LibreOffice\program\soffice.exe",
                ]
                for p in lo_paths:
                    if os.path.isfile(p):
                        return p
                return shutil.which("soffice") or shutil.which("libreoffice")

            def _export_with_libreoffice() -> bool:
                lo_bin = _resolve_lo_bin()
                if not lo_bin:
                    return False

                out_dir = os.path.dirname(output_path)
                subprocess.run(
                    [
                        lo_bin,
                        "--headless",
                        "--convert-to",
                        "pdf",
                        "--outdir",
                        out_dir,
                        translated_docx_path,
                    ],
                    check=True,
                    timeout=240,
                )
                # LibreOffice names output from input filename.
                lo_out = os.path.join(
                    out_dir,
                    os.path.splitext(os.path.basename(translated_docx_path))[0] + ".pdf",
                )
                if lo_out != output_path and os.path.exists(lo_out):
                    shutil.move(lo_out, output_path)
                return os.path.exists(output_path)

            def _export_with_docx2pdf() -> bool:
                from docx2pdf import convert as docx2pdf_convert

                docx2pdf_convert(translated_docx_path, output_path)
                return os.path.exists(output_path)

            attempts: list[str] = []
            errors: list[str] = []

            if engine == "docx2pdf":
                attempts = ["docx2pdf"]
            elif engine == "libreoffice":
                attempts = ["libreoffice"]
            else:
                attempts = ["docx2pdf", "libreoffice"] if os.name == "nt" else ["libreoffice", "docx2pdf"]

            exported = False
            for name in attempts:
                try:
                    if name == "docx2pdf":
                        exported = _export_with_docx2pdf()
                    else:
                        exported = _export_with_libreoffice()
                    if exported:
                        break
                    errors.append(f"{name}: output PDF was not created")
                except Exception as e:
                    errors.append(f"{name}: {e}")

            if not exported:
                raise RuntimeError("; ".join(errors) if errors else "no DOCX->PDF engine available")
        except Exception as e:
            raise RuntimeError(f"DOCX to PDF export failed: {e}") from e

        # Cleanup temp files
        try:
            import shutil as _shutil
            _shutil.rmtree(tmp_dir, ignore_errors=True)
        except Exception:
            pass

        if progress_cb:
            progress_cb(100, "PDF: completed (via DOCX)")

        return output_path

    # ── Core rendering ──

    def translate_pdf(
        self,
        input_path: str,
        output_path: Optional[str] = None,
        *,
        bilingual_mode: str = "none",
        bilingual_delimiter: str = "|",
        progress_cb: Optional[Callable[[int, str], None]] = None,
    ) -> str:
        """Translate a PDF preserving exact visual layout.

        Args:
            input_path: Path to source PDF
            output_path: Path for translated output (auto-generated if None)
            bilingual_mode: 'none', 'inline', or 'newline'
            bilingual_delimiter: Delimiter for inline bilingual mode
            progress_cb: Optional callback(percent, message)

        Returns:
            Path to the translated PDF file.
        """
        try:
            import fitz  # PyMuPDF
        except ImportError as e:
            raise RuntimeError("PyMuPDF is required. Install 'PyMuPDF'.") from e

        if not os.path.exists(input_path):
            raise FileNotFoundError(input_path)

        if output_path is None:
            base = os.path.splitext(os.path.basename(input_path))[0]
            out_name = f"{base}_translated_{uuid.uuid4().hex[:8]}.pdf"
            output_path = os.path.join(self.download_folder, out_name)

        bi_mode = (bilingual_mode or "none").strip().lower()
        if bi_mode not in ("none", "inline", "newline"):
            bi_mode = "none"

        self._cache.clear()

        doc = fitz.open(input_path)
        try:
            total_pages = doc.page_count
            if total_pages <= 0:
                raise RuntimeError("Empty PDF")

            if self.render_engine == "reportlab" and bi_mode != "newline":
                out = self._render_with_reportlab(
                    fitz,
                    doc,
                    output_path,
                    bilingual_mode=bi_mode,
                    bilingual_delimiter=bilingual_delimiter,
                    progress_cb=progress_cb,
                )
                if progress_cb:
                    progress_cb(100, "PDF: completed")
                return out

            # For bilingual newline mode, build a new document with image backgrounds
            bi_out_doc = None
            if bi_mode == "newline":
                bi_out_doc = fitz.open()

            for page_idx in range(total_pages):
                page = doc.load_page(page_idx)
                text_dict = page.get_text("dict")
                page_w = page.rect.width
                page_h = page.rect.height

                if progress_cb:
                    pct = int(5 + (page_idx / max(1, total_pages)) * 90)
                    progress_cb(pct, f"Layout analysis: page {page_idx + 1}/{total_pages}")

                # Step 1: Detect layout
                layout_lines = detect_page_layout(text_dict, page_w, page_h)

                # Prefer PyMuPDF native table finder (>= 1.23) which uses line
                # art detection for accurate cell boundaries.  Fall back to the
                # heuristic x-alignment grouper for older builds.
                table_regions = detect_table_regions_from_page(page)
                if not table_regions:
                    table_regions = detect_table_regions(text_dict, page_w)

                # OCR fallback for scanned/image-only pages.
                if not layout_lines:
                    layout_lines = self._extract_layout_lines_from_ocr(fitz, page)

                # Mark lines inside detected table regions
                for ll in layout_lines:
                    if ll.layout_type not in (LayoutType.TITLE, LayoutType.HEADER_FOOTER):
                        for tr in table_regions:
                            if (ll.x0 >= tr[0] - 5 and ll.y0 >= tr[1] - 5 and
                                    ll.x1 <= tr[2] + 5 and ll.y1 <= tr[3] + 5):
                                ll.layout_type = LayoutType.TABLE_CELL
                                break

                if not layout_lines:
                    if bi_mode == "newline" and bi_out_doc is not None:
                        pix = page.get_pixmap(dpi=200)
                        new_page = bi_out_doc.new_page(width=page_w, height=page_h)
                        new_page.insert_image(fitz.Rect(0, 0, page_w, page_h), pixmap=pix)
                    continue

                # ── Bilingual newline: render as image + overlay ──
                if bi_mode == "newline" and bi_out_doc is not None:
                    self._render_bilingual_newline(
                        fitz, page, bi_out_doc, layout_lines, page_w, page_h, page_idx, total_pages, progress_cb,
                    )
                    continue

                # ── Normal / inline: redact original text, re-insert translation ──
                layout_lines = self._merge_paragraph_lines(
                    layout_lines, self._should_translate)
                self._render_replace(
                    fitz, page, layout_lines, page_w, page_h,
                    bi_mode, bilingual_delimiter, page_idx, total_pages, progress_cb,
                )

            # Save output
            if bi_out_doc is not None:
                bi_out_doc.save(output_path, garbage=4, deflate=True)
                bi_out_doc.close()
            else:
                doc.save(output_path, garbage=4, deflate=True)

        finally:
            try:
                doc.close()
            except Exception:
                pass

        if progress_cb:
            progress_cb(100, "PDF: completed")

        return output_path

    def _normalize_dot_fill(
        self,
        fitz,
        rect,
        text: str,
        fontname: str,
        fontfile: Optional[str],
        fontsize: float,
    ) -> str:
        """Normalise dot / underscore placeholder sections to fill the bbox width.

        Form-field lines typically look like::

            Label: value ........................ Next Label:

        After translation the *label* may be wider or narrower than the
        original Vietnamese text, so the dots no longer reach the right edge
        of the row.  This method recalculates the number of fill characters
        for each dot section so that the whole translated line fits snugly
        inside ``rect.width``, keeping all rows visually aligned on the right.

        Args:
            fitz:      The PyMuPDF module (passed as a parameter since it is
                       imported lazily inside ``translate_pdf``).
            rect:      The target bounding box for the line.
            text:      The translated line text (dots preserved from the
                       translation step).
            fontname:  PyMuPDF font name for width measurement.
            fontfile:  Optional TTF path for width measurement.
            fontsize:  Font size in points.

        Returns:
            A new string where every dot / underscore run is sized to make the
            full line exactly fill ``rect.width``.  Returns the original
            *text* unchanged if anything goes wrong.
        """
        _DOT_RE = re.compile(r"(\.{3,}|_{3,}|-{5,}|…{2,})")
        parts = _DOT_RE.split(text)
        if len(parts) <= 1:
            return text  # No fill sections — nothing to adjust.

        try:
            def _measure(s: str) -> float:
                if not s:
                    return 0.0
                try:
                    kw: dict = {"fontname": fontname, "fontsize": fontsize}
                    if fontfile:
                        kw["fontfile"] = fontfile
                    return float(fitz.get_text_length(s, **kw))
                except Exception:
                    try:
                        return float(fitz.get_text_length(s, fontname="Helvetica", fontsize=fontsize))
                    except Exception:
                        return len(s) * fontsize * 0.55  # rough fallback

            # Measure width of all non-dot parts.
            non_dot_width = sum(_measure(parts[i]) for i in range(0, len(parts), 2))
            n_dot_sections = sum(1 for i in range(1, len(parts), 2))
            if n_dot_sections == 0:
                return text

            available = max(0.0, float(rect.width) - non_dot_width)
            dot_char = "." if "." in text else ("_" if "_" in text else ".")
            char_w = _measure(dot_char)
            if char_w <= 0:
                return text

            # All dot sections share the available space equally.
            dots_per_section = max(3, int(available / (n_dot_sections * char_w)))
            fill = dot_char * dots_per_section

            result: list[str] = []
            for i, part in enumerate(parts):
                result.append(fill if (i % 2 == 1) else part)
            return "".join(result)
        except Exception:
            return text

    def _normalize_dot_fill_rl(
        self,
        canvas,
        box_width: float,
        text: str,
        font_name: str,
        fontsize: float,
    ) -> str:
        """ReportLab version of :meth:`_normalize_dot_fill`.

        Uses ``canvas.stringWidth()`` for glyph metrics instead of PyMuPDF's
        ``get_text_length()``.  Adjusts every dot / underscore fill section so
        that the full translated line fills ``box_width`` exactly, keeping all
        form rows visually right-aligned.

        Args:
            canvas:     Active ``reportlab.pdfgen.canvas.Canvas`` object.
            box_width:  Available width of the bounding box in points.
            text:       Translated line text (dots already preserved).
            font_name:  Font currently set on the canvas.
            fontsize:   Font size currently set on the canvas.

        Returns:
            Text with dot sections resized to fill ``box_width``.
        """
        _DOT_RE = re.compile(r"(\.{3,}|_{3,}|-{5,}|…{2,})")
        parts = _DOT_RE.split(text)
        if len(parts) <= 1:
            return text

        try:
            non_dot_width = sum(
                canvas.stringWidth(parts[i], font_name, fontsize)
                for i in range(0, len(parts), 2)
            )
            n_dot_sections = sum(1 for i in range(1, len(parts), 2))
            if n_dot_sections == 0:
                return text

            available = max(0.0, box_width - non_dot_width)
            dot_char = "." if "." in text else ("_" if "_" in text else ".")
            char_w = canvas.stringWidth(dot_char, font_name, fontsize)
            if char_w <= 0:
                return text

            dots_per_section = max(3, int(available / (n_dot_sections * char_w)))
            fill = dot_char * dots_per_section
            result: list[str] = []
            for i, part in enumerate(parts):
                result.append(fill if (i % 2 == 1) else part)
            return "".join(result)
        except Exception:
            return text

    @staticmethod
    def _join_inline_bilingual(src_text: str, dst_text: str, delimiter: str) -> str:
        """Join source and translation into one adjacent bilingual line."""
        src = (src_text or "").strip()
        dst = (dst_text or "").strip()
        d = (delimiter or "|").strip() or "|"
        if src and dst:
            return f"{src} {d} {dst}"
        if src:
            return src
        return dst

    @staticmethod
    def _take_words_for_width(
        text: str,
        max_width: float,
        measure_fn: Callable[[str], float],
    ) -> Tuple[str, str]:
        """Take as many leading words as possible without exceeding max_width."""
        tokens = re.findall(r"\S+", text or "")
        if not tokens:
            return "", ""

        taken: list[str] = []
        for tok in tokens:
            candidate = " ".join(taken + [tok]).strip()
            if not taken or measure_fn(candidate) <= max_width:
                taken.append(tok)
            else:
                break

        if not taken:
            taken = [tokens[0]]

        head = " ".join(taken).strip()
        tail = " ".join(tokens[len(taken):]).strip()
        return head, tail

    @classmethod
    def _wrap_words_for_width(
        cls,
        text: str,
        max_width: float,
        measure_fn: Callable[[str], float],
    ) -> list[str]:
        """Wrap plain text by width using word boundaries."""
        remaining = " ".join(re.findall(r"\S+", text or "")).strip()
        if not remaining:
            return []

        out: list[str] = []
        guard = 0
        while remaining and guard < 4096:
            guard += 1
            head, tail = cls._take_words_for_width(remaining, max_width, measure_fn)
            if not head:
                break
            out.append(head)
            if tail == remaining:
                break
            remaining = tail

        if not out:
            out = [remaining]
        return out

    @classmethod
    def _build_inline_bilingual_lines(
        cls,
        src_text: str,
        dst_text: str,
        delimiter: str,
        box_width: float,
        measure_fn: Callable[[str], float],
    ) -> Tuple[list[str], float]:
        """Build lines for adjacent bilingual text with hanging indent for wrapped translation."""
        src = (src_text or "").strip()
        dst = (dst_text or "").strip()
        joined = cls._join_inline_bilingual(src, dst, delimiter)
        if not joined:
            return [], 0.0
        if not src or not dst:
            return cls._wrap_words_for_width(joined, max(24.0, box_width), measure_fn), 0.0

        d = (delimiter or "|").strip() or "|"
        prefix = f"{src} {d}".strip()
        prefix_with_gap = f"{prefix} "
        prefix_w = measure_fn(prefix_with_gap)

        # No hanging indent — continuation lines start from the left edge.
        hanging_indent = 0.0

        # If the source prefix itself is wider than the box, wrap the
        # source+delimiter and translation as separate blocks so they
        # don't interleave on continuation lines.
        if prefix_w > box_width:
            src_lines = cls._wrap_words_for_width(
                f"{src} {d}", max(24.0, box_width), measure_fn)
            dst_lines = cls._wrap_words_for_width(
                dst, max(24.0, box_width), measure_fn)
            return src_lines + dst_lines, 0.0

        first_width = box_width - prefix_w
        lines: list[str] = []
        remaining = dst
        if first_width >= 18.0:
            first_chunk, tail = cls._take_words_for_width(dst, first_width, measure_fn)
            if first_chunk:
                lines.append(f"{prefix_with_gap}{first_chunk}".strip())
                remaining = tail
            else:
                lines.append(prefix)
        else:
            lines.append(prefix)

        if remaining:
            cont_width = max(24.0, box_width)
            lines.extend(cls._wrap_words_for_width(remaining, cont_width, measure_fn))

        return lines or [joined], hanging_indent

    def _draw_inline_bilingual_rl(
        self,
        canvas,
        *,
        x0: float,
        y_bottom: float,
        width: float,
        height: float,
        src_text: str,
        dst_text: str,
        delimiter: str,
        font_name: str,
        fontsize: float,
        alignment: str,
    ) -> bool:
        """Draw adjacent bilingual line in ReportLab with hanging-indent wrap."""

        def _measure(s: str) -> float:
            return float(canvas.stringWidth(s or "", font_name, fontsize))

        lines, hanging_indent = self._build_inline_bilingual_lines(
            src_text,
            dst_text,
            delimiter,
            width,
            _measure,
        )
        if not lines:
            return False

        line_h = max(1.0, fontsize * 1.1)
        # Draw ALL lines (no n_fit clipping); white-out was pre-computed
        # to accommodate the exact number of lines needed.
        y_cur = y_bottom + height - fontsize  # top-aligned

        for i, line in enumerate(lines):
            inset = hanging_indent if i > 0 else 0.0
            avail = max(1.0, width - inset)
            tw = _measure(line)

            draw_x = x0 + inset
            if alignment == "center" and tw < avail:
                draw_x = x0 + inset + (avail - tw) / 2.0
            elif alignment == "right" and tw < avail:
                draw_x = x0 + inset + (avail - tw)

            canvas.drawString(draw_x, y_cur, line)
            y_cur -= line_h

        return True

    def _insert_inline_bilingual_at(
        self,
        fitz,
        page,
        rect,
        *,
        src_text: str,
        dst_text: str,
        delimiter: str,
        fontname: str,
        fontfile: Optional[str],
        fontsize: float,
        color: Tuple[float, float, float],
        alignment: str = "left",
        min_font_ratio: float = 0.90,
    ) -> bool:
        """Insert adjacent bilingual text with hanging-indent wrap in a PyMuPDF rect."""
        fs = int(max(4, round(float(fontsize))))

        def _measure(s: str) -> float:
            try:
                kwargs = {"fontname": fontname, "fontsize": fs}
                if fontfile:
                    kwargs["fontfile"] = fontfile
                return float(fitz.get_text_length(s or "", **kwargs))
            except Exception:
                return float(fitz.get_text_length(s or "", fontname="Helvetica", fontsize=fs))

        lines, hanging_indent = self._build_inline_bilingual_lines(
            src_text,
            dst_text,
            delimiter,
            float(rect.width),
            _measure,
        )
        if not lines:
            # fallback
            fallback_text = self._join_inline_bilingual(src_text, dst_text, delimiter)
            return self._insert_text_at(
                fitz, page, rect, fallback_text,
                fontname=fontname, fontfile=fontfile,
                fontsize=fontsize, color=color,
                alignment=alignment, min_font_ratio=min_font_ratio,
                no_wrap=False,
            )

        line_h = max(1.0, fs * 1.1)
        # Draw ALL lines — rect was already extended to fit them.
        base_y = rect.y0 + fs  # top-aligned

        for i, line in enumerate(lines):
            inset = hanging_indent if i > 0 else 0.0
            avail = max(1.0, rect.width - inset)
            tw = _measure(line)

            x = rect.x0 + inset
            if alignment == "center" and tw < avail:
                x = rect.x0 + inset + (avail - tw) / 2.0
            elif alignment == "right" and tw < avail:
                x = rect.x0 + inset + (avail - tw)
            y = base_y + i * line_h

            # Stop drawing if we'd go below the available rect
            if y > rect.y1:
                break
            try:
                kwargs = {"fontsize": fs, "fontname": fontname, "color": color}
                if fontfile:
                    kwargs["fontfile"] = fontfile
                page.insert_text(fitz.Point(x, y), line, **kwargs)
            except Exception:
                try:
                    page.insert_text(
                        fitz.Point(x, y),
                        line,
                        fontsize=fs,
                        fontname="Helvetica",
                        color=color,
                    )
                except Exception:
                    pass

        return True

    @staticmethod
    def _distribute_translation(
        orig_fragments: list,
        translated: str,
    ) -> list:
        """Distribute *translated* text proportionally across *orig_fragments*.

        Used when a line has multiple spans with different fonts: after
        translating the full line we split the result so each span receives
        a slice of the translation that matches its original character-count
        proportion, breaking on word boundaries where possible.

        Args:
            orig_fragments: Non-empty list of original span text strings.
            translated:     The fully translated line string.

        Returns:
            List of translated fragment strings, same length as
            *orig_fragments*.  The last element always receives any remainder.
        """
        n = len(orig_fragments)
        if n == 0:
            return []
        if n == 1:
            return [translated]

        total_orig = sum(len(f) for f in orig_fragments)
        trans_len = len(translated)

        if total_orig == 0 or trans_len == 0:
            # Degenerate case: spread evenly.
            chunk = max(1, trans_len // n) if trans_len else 0
            result, pos = [], 0
            for i in range(n):
                end = trans_len if i == n - 1 else pos + chunk
                result.append(translated[pos:end])
                pos = end
            return result

        result: list[str] = []
        pos = 0
        for i, frag in enumerate(orig_fragments):
            if i == n - 1:
                result.append(translated[pos:])
                break
            prop = len(frag) / total_orig
            ideal_end = pos + round(trans_len * prop)
            end = min(ideal_end, trans_len)
            # Prefer breaking after a space (search up to 12 chars ahead).
            probe = end
            while probe < min(trans_len, end + 12) and translated[probe] not in (" ", "\n"):
                probe += 1
            if probe < trans_len and translated[probe] in (" ", "\n"):
                end = probe + 1
            result.append(translated[pos:end])
            pos = end

        return result

    def _render_replace(
        self,
        fitz,
        page,
        layout_lines: list,
        page_w: float,
        page_h: float,
        bi_mode: str,
        bi_delimiter: str,
        page_idx: int,
        total_pages: int,
        progress_cb,
    ):
        """Redact original text and re-insert translated text at span level.

        Works span-by-span to preserve each span's exact font name, size,
        weight, style, and colour.  For lines with multiple spans the
        translated text is distributed proportionally so mixed formatting
        (e.g. a bold title word followed by regular text) is maintained.
        Table cells are fully translated using near-zero redaction expansion
        to avoid erasing the vector border lines.
        """
        # Collect span-level rects for redaction.
        # TABLE_CELL lines are now included but use minimal expansion so table
        # borders (drawn as vector graphics) are not accidentally erased.
        redact_items = []
        for ll in layout_lines:
            if not self._should_translate(ll.text):
                continue
            if ll.layout_type in (LayoutType.PAGE_NUMBER, LayoutType.WHITESPACE):
                continue
            for sp in ll.spans:
                if not sp.text.strip():
                    continue
                try:
                    r = fitz.Rect(sp.bbox)
                except Exception:
                    continue
                if r.width < 1 or r.height < 1:
                    continue
                # Expansion amounts per layout type:
                # - TABLE_CELL: near-zero to preserve cell borders.
                # - FORM_FIELD / DOTTED: small to avoid erasing nearby dots.
                # - Everything else: moderate to clean up glyph fragments.
                if ll.layout_type == LayoutType.TABLE_CELL:
                    px, py = 0.2, 0.1
                elif ll.layout_type in (LayoutType.FORM_FIELD, LayoutType.DOTTED_PLACEHOLDER):
                    px, py = 0.9, 0.8
                else:
                    px, py = 0.7, 0.6
                rr = fitz.Rect(r.x0 - px, r.y0 - py, r.x1 + px, r.y1 + py)
                try:
                    pr = page.rect
                    rr = fitz.Rect(
                        max(pr.x0, rr.x0),
                        max(pr.y0, rr.y0),
                        min(pr.x1, rr.x1),
                        min(pr.y1, rr.y1),
                    )
                except Exception:
                    pass
                redact_items.append(rr)

        # Apply redactions (preserves table borders/lines)
        for r in redact_items:
            try:
                page.add_redact_annot(r, fill=False)
            except Exception:
                pass

        try:
            page.apply_redactions(
                images=fitz.PDF_REDACT_IMAGE_NONE,
                graphics=fitz.PDF_REDACT_LINE_ART_NONE,
            )
        except Exception:
            for r in redact_items:
                try:
                    page.draw_rect(r, color=None, fill=(1, 1, 1), overlay=True, width=0)
                except Exception:
                    pass

        # ── Phase 3: Re-insert translated text at span level ──
        # Single pass: translate, compute rects, white-out, draw.
        # For inline bilingual, rects are extended and text is clipped
        # if it would overflow into the next line's area.

        for idx, ll in enumerate(layout_lines):
            if not self._should_translate(ll.text):
                continue
            if ll.layout_type in (LayoutType.PAGE_NUMBER, LayoutType.WHITESPACE):
                continue

            # Translate the full line so the model has sufficient context.
            try:
                dst = self._translate_preserve_structure(ll.text, ll.layout_type)
            except Exception:
                dst = ll.text
            if not str(dst).strip():
                dst = ll.text

            src_text = (ll.text or "").strip()
            dst_text = str(dst).strip()
            render_text = dst_text
            if bi_mode == "inline":
                render_text = self._join_inline_bilingual(src_text, dst_text, bi_delimiter)

            no_wrap = ll.layout_type in (
                LayoutType.TABLE_CELL, LayoutType.FORM_FIELD, LayoutType.DOTTED_PLACEHOLDER
            )

            fontname, fontfile, _ = self._resolve_font(ll)
            color = self._int_color_to_rgb01(ll.spans[0].color if ll.spans else 0)

            # Compute drawing rect (extend for bilingual wrapping)
            draw_rect = fitz.Rect(ll.bbox)
            if bi_mode == "inline" and ll.layout_type != LayoutType.TABLE_CELL:
                right_pad = max(12.0, min(draw_rect.x0, 40.0))
                draw_rect.x1 = max(draw_rect.x1, page_w - right_pad)

                # Pre-measure exact line count needed
                fs_m = ll.font_size
                _fn_local = fontname
                _ff_local = fontfile
                def _pm(s: str, _fz=fitz, _fn=_fn_local, _ff=_ff_local, _fs=fs_m) -> float:
                    try:
                        kw = {"fontname": _fn, "fontsize": _fs}
                        if _ff:
                            kw["fontfile"] = _ff
                        return float(_fz.get_text_length(s or "", **kw))
                    except Exception:
                        return float(_fz.get_text_length(s or "", fontname="Helvetica", fontsize=_fs))
                pre_lines, _ = self._build_inline_bilingual_lines(
                    src_text, dst_text, bi_delimiter, float(draw_rect.width), _pm,
                )
                needed_lines = max(1, len(pre_lines))
                needed_h = needed_lines * fs_m * 1.1 + 2
                draw_rect.y1 = max(draw_rect.y1, draw_rect.y0 + needed_h)

                # White-out the extended area
                try:
                    page.draw_rect(draw_rect, color=None, fill=(1, 1, 1), overlay=True, width=0)
                except Exception:
                    pass

            if progress_cb and idx % 20 == 0:
                pct = int(5 + ((page_idx + (idx / max(1, len(layout_lines)))) / max(1, total_pages)) * 90)
                progress_cb(min(98, pct), f"PDF: page {page_idx + 1}/{total_pages} ({idx}/{len(layout_lines)})")

            # ── Form fields and dotted placeholders ──
            if ll.layout_type in (LayoutType.DOTTED_PLACEHOLDER, LayoutType.FORM_FIELD):
                norm_text = self._normalize_dot_fill(
                    fitz, draw_rect, render_text.strip(),
                    fontname, fontfile, ll.font_size,
                )
                self._insert_text_at(
                    fitz, page, draw_rect, norm_text,
                    fontname=fontname, fontfile=fontfile,
                    fontsize=ll.font_size, color=color,
                    alignment=ll.alignment, min_font_ratio=0.85,
                    no_wrap=(bi_mode != "inline"),
                )
                continue

            # ── Adjacent bilingual (non-table) ──
            if bi_mode == "inline" and ll.layout_type != LayoutType.TABLE_CELL:
                inserted = self._insert_inline_bilingual_at(
                    fitz,
                    page,
                    draw_rect,
                    src_text=src_text,
                    dst_text=dst_text,
                    delimiter=bi_delimiter,
                    fontname=fontname,
                    fontfile=fontfile,
                    fontsize=ll.font_size,
                    color=color,
                    alignment=ll.alignment,
                    min_font_ratio=0.90,
                )
                if not inserted:
                    self._insert_text_at(
                        fitz,
                        page,
                        draw_rect,
                        render_text,
                        fontname=fontname,
                        fontfile=fontfile,
                        fontsize=ll.font_size,
                        color=color,
                        alignment=ll.alignment,
                        min_font_ratio=0.88,
                        no_wrap=False,
                    )
                continue

            # ── Regular lines (non-bilingual or table cells) ──
            # Collect spans that actually carry translatable text.
            translatable_spans = [
                sp for sp in (ll.spans or [])
                if sp.text.strip() and self._should_translate(sp.text)
            ]

            if not translatable_spans:
                self._insert_text_at(
                    fitz, page, fitz.Rect(ll.bbox), render_text.strip(),
                    fontname=fontname, fontfile=fontfile,
                    fontsize=ll.font_size, color=color,
                    alignment=ll.alignment, min_font_ratio=0.88,
                    no_wrap=no_wrap,
                )

            elif len(translatable_spans) == 1:
                sp = translatable_spans[0]
                sp_ll = LayoutLine(
                    text=sp.text, bbox=sp.bbox, spans=[sp],
                    layout_type=ll.layout_type,
                    alignment=ll.alignment, indent_level=ll.indent_level,
                    font_size=sp.size, is_bold=sp.is_bold, is_italic=sp.is_italic,
                )
                sp_fontname, sp_fontfile, _ = self._resolve_font(sp_ll)
                sp_color = self._int_color_to_rgb01(sp.color)
                ratio = 0.88 if ll.layout_type == LayoutType.TABLE_CELL else 0.92
                inserted = self._insert_text_at(
                    fitz, page, fitz.Rect(sp.bbox), render_text.strip(),
                    fontname=sp_fontname, fontfile=sp_fontfile,
                    fontsize=sp.size, color=sp_color,
                    alignment=ll.alignment, min_font_ratio=ratio,
                    no_wrap=no_wrap,
                )
                if not inserted and ll.text.strip():
                    self._insert_text_at(
                        fitz, page, fitz.Rect(sp.bbox), ll.text.strip(),
                        fontname=sp_fontname, fontfile=sp_fontfile,
                        fontsize=sp.size, color=sp_color,
                        alignment=ll.alignment, min_font_ratio=0.82,
                        no_wrap=no_wrap,
                    )

            else:
                frags = self._distribute_translation(
                    [sp.text for sp in translatable_spans], render_text.strip()
                )
                for sp, frag in zip(translatable_spans, frags):
                    if not frag.strip():
                        continue
                    sp_ll = LayoutLine(
                        text=sp.text, bbox=sp.bbox, spans=[sp],
                        layout_type=ll.layout_type,
                        alignment=ll.alignment, indent_level=ll.indent_level,
                        font_size=sp.size, is_bold=sp.is_bold, is_italic=sp.is_italic,
                    )
                    sp_fontname, sp_fontfile, _ = self._resolve_font(sp_ll)
                    sp_color = self._int_color_to_rgb01(sp.color)
                    self._insert_text_at(
                        fitz, page, fitz.Rect(sp.bbox), frag.strip(),
                        fontname=sp_fontname, fontfile=sp_fontfile,
                        fontsize=sp.size, color=sp_color,
                        alignment=ll.alignment, min_font_ratio=0.80,
                        no_wrap=no_wrap,
                    )

    def _render_bilingual_newline(
        self,
        fitz,
        src_page,
        out_doc,
        layout_lines: list,
        page_w: float,
        page_h: float,
        page_idx: int,
        total_pages: int,
        progress_cb,
    ):
        """Render bilingual (newline) mode: original + translation below each line."""
        RIGHT_MARGIN = 30

        pix = src_page.get_pixmap(dpi=200)
        new_page = out_doc.new_page(width=page_w, height=page_h)
        new_page.insert_image(fitz.Rect(0, 0, page_w, page_h), pixmap=pix)

        for idx, ll in enumerate(layout_lines):
            if not self._should_translate(ll.text):
                continue
            if ll.layout_type in (LayoutType.PAGE_NUMBER, LayoutType.WHITESPACE):
                continue

            try:
                dst = self._translate_preserve_structure(ll.text, ll.layout_type)
            except Exception:
                dst = ""

            fontname, fontfile, _family = self._resolve_font(ll)
            fontsize = ll.font_size
            color = self._int_color_to_rgb01(ll.spans[0].color if ll.spans else 0)
            rect = fitz.Rect(ll.bbox)

            # White-out span areas
            for sp in ll.spans:
                try:
                    sr = fitz.Rect(sp.bbox)
                    new_page.draw_rect(sr, color=None, fill=(1, 1, 1), overlay=True, width=0)
                except Exception:
                    pass

            # Re-insert original at exact position
            self._insert_text_at(fitz, new_page, rect, ll.text.strip(),
                                 fontname=fontname, fontfile=fontfile,
                                 fontsize=fontsize, color=color,
                                 alignment=ll.alignment)

            # Insert translation below in blue italic
            if str(dst).strip():
                fn_i, ff_i, _ = self._resolve_font(LayoutLine(
                    text="", bbox=ll.bbox, is_italic=True,
                    spans=ll.spans,
                ))
                trans_fs = max(5, fontsize - 2)
                trans_color = (0.0, 0.10, 0.65)
                trans_h = max(trans_fs + 2, ll.height * 0.85)
                trans_rect = fitz.Rect(
                    rect.x0,
                    rect.y1 + 1,
                    max(rect.x1, page_w - RIGHT_MARGIN),
                    rect.y1 + 1 + trans_h,
                )
                new_page.draw_rect(trans_rect, color=None, fill=(1, 1, 1), overlay=True, width=0)
                self._insert_text_at(fitz, new_page, trans_rect, str(dst).strip(),
                                     fontname=fn_i, fontfile=ff_i,
                                     fontsize=trans_fs, color=trans_color,
                                     alignment=ll.alignment)

            if progress_cb and idx % 10 == 0:
                pct = int(5 + ((page_idx + (idx / max(1, len(layout_lines)))) / max(1, total_pages)) * 90)
                progress_cb(min(98, pct), f"PDF bilingual: page {page_idx + 1}/{total_pages}")

    def _insert_text_at(
        self,
        fitz,
        page,
        rect,
        text: str,
        *,
        fontname: str,
        fontfile: Optional[str],
        fontsize: float,
        color: Tuple[float, float, float],
        alignment: str = "left",
        min_font_ratio: float = 0.95,
        no_wrap: bool = False,
    ):
        """Insert text at the given rect, respecting alignment.

        Alignment mapping:
          left=0, center=1, right=2
        """
        align_map = {"left": 0, "center": 1, "right": 2}
        align_val = align_map.get(alignment, 0)

        fs0 = int(max(4, round(float(fontsize))))
        # Keep visual size close to original; aggressive shrinking breaks layout fidelity.
        fs_min = int(max(4, round(fs0 * max(0.85, min(1.0, float(min_font_ratio))))))

        if no_wrap:
            # For tight table cells, keep a stable size and avoid multiline wrapping.
            for fs in range(fs0, fs_min - 1, -1):
                try:
                    tw = fitz.get_text_length(text, fontname=fontname, fontsize=fs)
                except Exception:
                    tw = fitz.get_text_length(text, fontname="Helvetica", fontsize=fs)

                x = rect.x0
                if align_val == 1:
                    x = rect.x0 + max(0, (rect.width - tw) / 2)
                elif align_val == 2:
                    x = rect.x1 - tw
                if x < rect.x0:
                    x = rect.x0

                y = rect.y1 - max(1, int(fs * 0.15))
                try:
                    kwargs = dict(fontsize=fs, fontname=fontname, color=color)
                    if fontfile:
                        kwargs["fontfile"] = fontfile
                    page.insert_text(fitz.Point(x, y), text, **kwargs)
                    return True
                except Exception:
                    try:
                        page.insert_text(
                            fitz.Point(x, y), text,
                            fontsize=fs, fontname="Helvetica", color=color,
                        )
                        return True
                    except Exception:
                        continue

            # Hard fallback for no-wrap mode: trim to fit, never overflow outside cell.
            t = text
            fs = max(4, fs_min)
            while t:
                try:
                    tw = fitz.get_text_length(t, fontname=fontname, fontsize=fs)
                except Exception:
                    tw = fitz.get_text_length(t, fontname="Helvetica", fontsize=fs)
                if tw <= rect.width:
                    break
                t = t[:-1]

            if t:
                y = rect.y1 - max(1, int(fs * 0.15))
                try:
                    kwargs = dict(fontsize=fs, fontname=fontname, color=color)
                    if fontfile:
                        kwargs["fontfile"] = fontfile
                    page.insert_text(fitz.Point(rect.x0, y), t, **kwargs)
                    return True
                except Exception:
                    try:
                        page.insert_text(
                            fitz.Point(rect.x0, y), t,
                            fontsize=fs, fontname="Helvetica", color=color,
                        )
                        return True
                    except Exception:
                        pass

        # Try textbox first (wraps within bounds)
        for fs in range(fs0, fs_min - 1, -1):
            try:
                kwargs = dict(fontsize=fs, fontname=fontname, color=color, align=align_val)
                if fontfile:
                    kwargs["fontfile"] = fontfile
                rc = page.insert_textbox(rect, text, **kwargs)
                if rc >= 0:
                    return True
            except Exception:
                try:
                    rc = page.insert_textbox(
                        rect, text, fontsize=fs, fontname="Helvetica",
                        color=color, align=align_val,
                    )
                    if rc >= 0:
                        return True
                except Exception:
                    continue

        # Last safe attempt: do not shrink below fs_min.
        for fs in range(fs_min, fs_min - 1, -1):
            try:
                kwargs = dict(fontsize=fs, fontname=fontname, color=color, align=align_val)
                if fontfile:
                    kwargs["fontfile"] = fontfile
                rc = page.insert_textbox(rect, text, **kwargs)
                if rc >= 0:
                    return True
            except Exception:
                try:
                    rc = page.insert_textbox(
                        rect, text, fontsize=fs, fontname="Helvetica",
                        color=color, align=align_val,
                    )
                    if rc >= 0:
                        return True
                except Exception:
                    continue

        # Do not point-insert for normal paragraphs; that can overflow out of page.
        return False
