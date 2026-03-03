import os
import re
import unicodedata
import time
import uuid
import io
import zipfile
import shutil
import PyPDF2
import docx
import openpyxl
# fpdf is optional; if missing we fallback to text output for PDFs
try:
    from fpdf import FPDF
    HAS_FPDF = True
except Exception:
    FPDF = None
    HAS_FPDF = False
from werkzeug.utils import secure_filename
from docx.enum.text import WD_PARAGRAPH_ALIGNMENT


class ProviderRateLimitError(Exception):
    """Raised when the upstream AI provider indicates a hard rate limit (429 or insufficient credits)."""
    pass

class FileService:
    def __init__(self, translator=None, ocr_image_to_text=None, ocr_translate_overlay=None, ocr_image_to_bboxes=None):
        """translator: callable(text, source_lang, target_lang) -> translated_text

        ocr_image_to_text: optional callable(image_path, ocr_langs=None) -> text
        """
        from concurrent.futures import ThreadPoolExecutor

        self.upload_folder = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'uploads')
        self.download_folder = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'downloads')
        os.makedirs(self.upload_folder, exist_ok=True)
        os.makedirs(self.download_folder, exist_ok=True)
        self.translator = translator
        self.ocr_image_to_text = ocr_image_to_text
        self.ocr_translate_overlay = ocr_translate_overlay
        self.ocr_image_to_bboxes = ocr_image_to_bboxes
        self.has_tesseract = False  # Will be set by TranslationService
        # Performance tuning
        try:
            from app import config as app_config
            self.concurrency = getattr(app_config.Config, 'TRANSLATION_CONCURRENCY', 4)
            self.retries = getattr(app_config.Config, 'TRANSLATION_RETRIES', 3)
            self.backoff = getattr(app_config.Config, 'TRANSLATION_BACKOFF', 1.5)
        except Exception:
            self.concurrency = int(os.getenv('TRANSLATION_CONCURRENCY', '4'))
            self.retries = int(os.getenv('TRANSLATION_RETRIES', '3'))
            self.backoff = float(os.getenv('TRANSLATION_BACKOFF', '1.5'))
        self._executor_cls = ThreadPoolExecutor

    def _translate_with_retry(self, text, target_lang):
        """Translate a piece of text with retry/backoff on transient errors.

        IMPORTANT: If a provider rate-limit or "insufficient credits" error is encountered,
        fail fast by raising ProviderRateLimitError so the calling job can abort immediately
        instead of continuing and wasting quota/retries.
        """
        if not self.translator:
            raise RuntimeError('Translator not configured')
        last = None
        attempt = 0
        max_attempts = max(1, self.retries)
        while attempt < max_attempts:
            try:
                out = self.translator(text, 'auto', target_lang)
                return out
            except Exception as e:
                last = e
                err = str(e).lower()
                # Fail fast for provider rate limits or insufficient credits
                if any(k in err for k in ('429', 'too many requests', 'rate', 'insufficient', 'free-models', '402', 'credit', 'requires more credits')):
                    try:
                        print(f"Provider rate limit or insufficient credits encountered: {e}")
                    except UnicodeEncodeError:
                        print("Provider rate limit encountered: ", repr(e))
                    raise ProviderRateLimitError(str(e))
                # Retry on transient network errors
                if any(k in err for k in ('temporarily', 'timed out', 'timeout', 'connection')):
                    sleep_time = (self.backoff ** attempt)
                    print(f"Translate retry {attempt+1}/{self.retries} after {sleep_time}s due to: {e}")
                    time.sleep(sleep_time)
                    attempt += 1
                    continue
                # Non-retryable errors: break
                break
        # Final attempt to raise helpful error
        raise last

    
    def process_document(self, file_path, target_lang, progress_callback=None, *, ocr_images=False, ocr_langs=None, ocr_mode=None, bilingual_mode=None, bilingual_delimiter=None, pdf_docx_pipeline=None):
        filename = os.path.basename(file_path)
        name, ext = os.path.splitext(filename)
        
        if ext.lower() == '.pdf':
            # PDF handling modes:
            # - Default: in-place PDF text replacement (keeps PDF output)
            # - Optional: PDF->DOCX pipeline for best format preservation (outputs DOCX)
            env_pdf_docx_pipeline = str(os.getenv('PDF_DOCX_PIPELINE', '0')).strip().lower() in ('1', 'true', 'yes', 'on')
            use_docx_pipeline = env_pdf_docx_pipeline if pdf_docx_pipeline is None else bool(pdf_docx_pipeline)
            if use_docx_pipeline:
                return self._process_pdf_via_docx(
                    file_path,
                    target_lang,
                    progress_callback,
                    ocr_images=ocr_images,
                    ocr_langs=ocr_langs,
                    ocr_mode=ocr_mode,
                    bilingual_mode=bilingual_mode,
                    bilingual_delimiter=bilingual_delimiter,
                )

            # Always keep PDF input -> PDF output.
            return self._process_pdf(file_path, target_lang, progress_callback, ocr_images=ocr_images, ocr_langs=ocr_langs, ocr_mode=ocr_mode, bilingual_mode=bilingual_mode, bilingual_delimiter=bilingual_delimiter)
        elif ext.lower() == '.docx':
            return self._process_docx(file_path, target_lang, progress_callback, ocr_images=ocr_images, ocr_langs=ocr_langs, ocr_mode=ocr_mode, bilingual_mode=bilingual_mode, bilingual_delimiter=bilingual_delimiter)
        elif ext.lower() == '.xlsx':
            return self._process_xlsx(file_path, target_lang, progress_callback)
        elif ext.lower() == '.txt':
            return self._process_txt(file_path, target_lang, progress_callback)
        else:
            raise ValueError("Unsupported file type")

    @staticmethod
    def _normalize_bilingual_delimiter(delimiter):
        """Normalize a user-provided delimiter for inline bilingual output.

        Returns a short string (no surrounding spaces). Defaults to '|'.
        """
        d = (delimiter or '').strip()
        if not d:
            return '|'
        # Prevent pathological inputs from breaking layout.
        if len(d) > 10:
            d = d[:10]
        return d

    def _join_inline_bilingual(self, src, dst, delimiter):
        d = self._normalize_bilingual_delimiter(delimiter)
        s = (src or '').strip()
        t = (dst or '').strip()
        # If translation is empty, do not append a dangling delimiter.
        if not t:
            return src or ''
        if not s:
            return dst or ''
        return f"{src} {d} {dst}"

    def _process_pdf_via_docx(self, file_path, target_lang, progress_callback=None, *, ocr_images=False, ocr_langs=None, ocr_mode=None, bilingual_mode=None, bilingual_delimiter=None):
        """Convert PDF to DOCX, translate using DOCX engine, then return translated DOCX.

        This path preserves layout far better for form-like / table-heavy PDFs compared to
        replacing PDF text in-place.
        """
        try:
            from pdf2docx import Converter
        except Exception as e:
            raise RuntimeError(f"pdf2docx is required for PDF_DOCX_PIPELINE: {e}")

        input_name = os.path.splitext(os.path.basename(file_path))[0]
        tmp_docx_path = os.path.join(self.upload_folder, f"pdf_to_docx_{uuid.uuid4().hex}.docx")

        if progress_callback:
            progress_callback(5, "PDF->DOCX: converting layout...")

        cv = Converter(file_path)
        try:
            cv.convert(tmp_docx_path, start=0, end=None)
        finally:
            cv.close()

        if progress_callback:
            progress_callback(20, "PDF->DOCX: translating as Word...")

        translated_tmp = self._process_docx(
            tmp_docx_path,
            target_lang,
            progress_callback,
            ocr_images=ocr_images,
            ocr_langs=ocr_langs,
            ocr_mode=ocr_mode,
            bilingual_mode=bilingual_mode,
            bilingual_delimiter=bilingual_delimiter,
            cleanup_pdf_layout=True,
        )

        final_docx = os.path.join(self.download_folder, f"translated_{input_name}.docx")
        try:
            if os.path.exists(final_docx):
                os.remove(final_docx)
            shutil.move(translated_tmp, final_docx)
        except Exception:
            final_docx = translated_tmp
        finally:
            try:
                if os.path.exists(tmp_docx_path):
                    os.remove(tmp_docx_path)
            except Exception:
                pass

        if progress_callback:
            progress_callback(100, "Completed (output DOCX for best format preservation)")
        return final_docx
    
    # ------------------------------------------------------------------ #
    #  PDF helpers                                                         #
    # ------------------------------------------------------------------ #
    @staticmethod
    def _pdf_find_font():
        """Return path to a Unicode-capable TTF font, or None."""
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

    @staticmethod
    def _pdf_fit_fontsize(rect_w, rect_h, text, base_size):
        """Return a fontsize <= *base_size* estimated to fit *text* inside the rectangle."""
        if rect_w <= 0 or rect_h <= 0 or not text:
            return max(6, base_size)
        n = len(text)
        for fs in (base_size, base_size * 0.85, base_size * 0.72, base_size * 0.6, base_size * 0.5, base_size * 0.42, base_size * 0.35, base_size * 0.30):
            char_w = fs * 0.52
            line_h = fs * 1.35
            cpl = max(1, int(rect_w / char_w))
            lines_needed = max(1, -(-n // cpl))       # ceil division
            if lines_needed * line_h <= rect_h * 1.05: # 5 % tolerance
                return max(4.5, round(fs, 1))
        return max(4.0, round(base_size * 0.28, 1))

    def _pdf_insert_text(self, page, rect, text, fontsize, font_path, font_name,
                         color=(0, 0, 0), align=0, preferred_base_font=None):
        """Insert *text* into *rect* on *page* with best-effort fit.

        Strategy:
        - Choose a font (base-14 when ASCII; Unicode TTF when needed)
        - Wrap text to the rect width using font metrics
        - Iteratively reduce fontsize until wrapped text fits rect height

        This is intentionally conservative to preserve layout (avoid overflow/overlap).
        """

        if not text or rect.width <= 0 or rect.height <= 0:
            return

        try:
            import pymupdf as _mu
        except Exception:
            try:
                import fitz as _mu
            except Exception:
                # As a last resort, fall back to old heuristic insertion.
                fontsize = self._pdf_fit_fontsize(rect.width, rect.height, text, fontsize)
                page.insert_textbox(rect=rect, buffer=text, fontsize=fontsize, align=align, color=color, fontname="helv")
                return

        needs_unicode = any(ord(ch) > 127 for ch in (text or ""))

        # Select font and build a font metrics object
        fontname = "helv"
        fontfile = None
        if preferred_base_font and not needs_unicode:
            fontname = preferred_base_font
        elif font_path:
            fontname = font_name
            fontfile = font_path

        try:
            if fontfile:
                font_obj = _mu.Font(fontfile=fontfile)
            else:
                font_obj = _mu.Font(fontname=fontname)
        except Exception:
            font_obj = None

        def _text_width(s, fs):
            try:
                if font_obj is not None:
                    return float(font_obj.text_length(s, fontsize=fs))
            except Exception:
                pass
            try:
                return float(_mu.get_text_length(s, fontname=fontname, fontsize=fs))
            except Exception:
                # Fallback: rough estimate
                return len(s) * fs * 0.52

        def _wrap_to_width(raw, width, fs):
            # Keep explicit newlines, wrap each paragraph separately
            paras = str(raw).splitlines() if raw is not None else []
            if not paras:
                return ""

            wrapped_lines = []
            for para in paras:
                p = (para or "").strip()
                if not p:
                    wrapped_lines.append("")
                    continue

                words = [w for w in re.split(r"\s+", p) if w]
                line = ""
                for w in words:
                    cand = (w if not line else f"{line} {w}")
                    if _text_width(cand, fs) <= width * 0.985:
                        line = cand
                        continue

                    if line:
                        wrapped_lines.append(line)
                        line = w
                    else:
                        # Single long token: hard-break by characters
                        chunk = ""
                        for ch in w:
                            cand2 = chunk + ch
                            if _text_width(cand2, fs) <= width * 0.985 or not chunk:
                                chunk = cand2
                            else:
                                wrapped_lines.append(chunk)
                                chunk = ch
                        line = chunk

                if line:
                    wrapped_lines.append(line)

            return "\n".join(wrapped_lines)

        # Candidate sizes: start near requested size, then shrink.
        base = float(fontsize or 11)
        candidates = [base, base * 0.92, base * 0.86, base * 0.80, base * 0.74, base * 0.68, base * 0.62, base * 0.56, base * 0.50, base * 0.44, base * 0.38]
        candidates = [round(max(3.6, fs), 2) for fs in candidates]
        # Unique, descending
        candidates = sorted(set(candidates), reverse=True)

        # Try to fit; use a modest line height multiplier.
        for fs in candidates:
            wrapped = _wrap_to_width(text, rect.width, fs)
            line_count = max(1, len(wrapped.splitlines()))
            line_h = fs * 1.18
            if (line_count * line_h) <= rect.height * 1.04:
                page.insert_textbox(
                    rect=rect,
                    buffer=wrapped,
                    fontsize=fs,
                    align=align,
                    color=color,
                    fontname=fontname,
                    fontfile=fontfile,
                )
                return

        # If nothing fits, insert at minimum size with wrapping (may clip, but preserves layout).
        fs = min(candidates) if candidates else 3.6
        wrapped = _wrap_to_width(text, rect.width, fs)
        page.insert_textbox(
            rect=rect,
            buffer=wrapped,
            fontsize=fs,
            align=align,
            color=color,
            fontname=fontname,
            fontfile=fontfile,
        )

    @staticmethod
    def _pdf_decode_color_int(color_int):
        """Convert PyMuPDF span color int to RGB tuple in range 0..1."""
        try:
            if isinstance(color_int, int):
                r = (color_int >> 16) & 255
                g = (color_int >> 8) & 255
                b = color_int & 255
                return (r / 255.0, g / 255.0, b / 255.0)
        except Exception:
            pass
        return (0, 0, 0)

    @staticmethod
    def _pdf_should_translate_text(text):
        """Filter out form-like lines (dot leaders, separators) to avoid layout damage."""
        if not text:
            return False
        s = text.strip()
        if len(s) < 2:
            return False
        if re.fullmatch(r"[\._\-=:;\s]+", s):
            return False
        if re.search(r"\.{5,}|_{4,}|-{4,}", s):
            return False
        alpha_count = sum(ch.isalpha() for ch in s)
        if alpha_count == 0:
            return False
        punctuation_count = sum(1 for ch in s if not ch.isalnum() and not ch.isspace())
        if punctuation_count > 0 and punctuation_count >= int(len(s) * 0.6):
            return False
        return True

    @staticmethod
    def _pdf_is_dot_leader_span(text: str) -> bool:
        """Return True for dot/underscore leader spans (form fill lines) we should never translate/redact."""
        if not text:
            return False
        s = text.strip()
        if len(s) < 4:
            return False
        # Pure leader-like characters
        if re.fullmatch(r"[\._\-=:;\s]+", s):
            # Require it to be mostly non-space leader chars
            non_space = sum(1 for ch in s if not ch.isspace())
            return non_space >= 4
        # Mostly dots/underscores/dashes (common in forms)
        leader = sum(1 for ch in s if ch in ('.', '_', '-'))
        return leader >= max(4, int(len(s) * 0.75))

    @staticmethod
    def _pdf_is_prose_candidate(text, rect):
        """Return True only for likely prose lines (safer for preserving PDF layout)."""
        s = (text or "").strip()
        if not s:
            return False

        words = [w for w in re.split(r"\s+", s) if w]
        if len(words) < 6:
            return False
        if len(s) < 35:
            return False

        # Narrow / tiny boxes are commonly table cells or form fields.
        if rect.width < 180 or rect.height < 11:
            return False

        # Many short words typically indicate labels / table content.
        short_ratio = sum(1 for w in words if len(w) <= 3) / max(1, len(words))
        if short_ratio >= 0.55:
            return False

        # Prefer sentence-like lines.
        has_sentence_punct = any(p in s for p in ('.', '?', '!', ';', ':', ','))
        if not has_sentence_punct and len(words) < 10:
            return False

        return True

    @staticmethod
    def _pdf_has_columnar_layout(span_bboxes, base_fontsize):
        """Detect table/column-like lines by large horizontal gaps between spans."""
        if not span_bboxes or len(span_bboxes) < 3:
            return False
        boxes = sorted(span_bboxes, key=lambda b: b[0])
        big_gaps = 0
        gap_threshold = max(16.0, base_fontsize * 2.8)
        for i in range(len(boxes) - 1):
            gap = boxes[i + 1][0] - boxes[i][2]
            if gap > gap_threshold:
                big_gaps += 1
        return big_gaps >= 2

    @staticmethod
    def _pdf_is_relaxed_candidate(text, rect):
        """Relaxed prose heuristic used when strict filtering leaves too little translatable text."""
        s = (text or "").strip()
        if not s:
            return False
        words = [w for w in re.split(r"\s+", s) if w]
        if len(words) < 4:
            return False
        if len(s) < 18:
            return False
        if rect.width < 120 or rect.height < 9:
            return False
        return True

    @staticmethod
    def _pdf_is_table_like_line(text, rect, is_columnar=False):
        """Heuristic: identify table/schema cell lines that should not be replaced in-place."""
        s = (text or "").strip()
        if not s:
            return False

        if is_columnar:
            return True

        # Common DB/schema terms usually appear in narrow table cells.
        if re.search(r"\b(primary\s+key|foreign\s+key|not\s+null|nullable|nvarchar|varchar|char|int|bigint|tinyint|decimal|datetime)\b", s, re.IGNORECASE):
            return True

        words = [w for w in re.split(r"\s+", s) if w]
        if rect.width < 165 and rect.height < 16 and len(words) <= 6:
            return True

        # Many short tokens in a compact box usually indicate column values / labels.
        short_ratio = sum(1 for w in words if len(w) <= 3) / max(1, len(words))
        if rect.width < 190 and len(words) >= 3 and short_ratio >= 0.6:
            return True

        return False

    @staticmethod
    def _pdf_group_table_cell_lines(lines):
        """Group consecutive table-like lines into a single cell-level item.

        PDFs often split table cell content into multiple lines/spans. Translating those
        pieces independently produces nonsense and breaks layout. We merge vertically
        adjacent lines that appear to belong to the same cell, then translate once.
        """
        if not lines:
            return []

        # Sort visually: top-to-bottom then left-to-right
        sorted_lines = sorted(lines, key=lambda it: (float(it['rect'].y0), float(it['rect'].x0)))
        out = []

        def _h_overlap(a, b):
            inter = max(0.0, min(a.x1, b.x1) - max(a.x0, b.x0))
            denom = max(1.0, min(a.width, b.width))
            return inter / denom

        for info in sorted_lines:
            if not info.get('is_table_like'):
                out.append(info)
                continue

            # Try to merge into the last table-like group when it looks like same cell.
            merged = False
            if out and out[-1].get('is_table_like'):
                prev = out[-1]
                prev_rect = prev['rect']
                rect = info['rect']
                y_gap = rect.y0 - prev_rect.y1
                fs = max(6.0, float(prev.get('fontsize') or info.get('fontsize') or 11))
                # Same column/cell: strong horizontal overlap, small vertical gap.
                if _h_overlap(prev_rect, rect) >= 0.82 and -2.0 <= y_gap <= (fs * 0.95 + 2.0):
                    prev['rect'] = type(rect)(
                        min(prev_rect.x0, rect.x0),
                        min(prev_rect.y0, rect.y0),
                        max(prev_rect.x1, rect.x1),
                        max(prev_rect.y1, rect.y1),
                    )
                    prev['text'] = (str(prev.get('text') or '').rstrip() + "\n" + str(info.get('text') or '').lstrip()).strip()
                    prev['fontsize'] = min(float(prev.get('fontsize') or fs), float(info.get('fontsize') or fs))
                    merged = True

            if not merged:
                out.append(info)

        return out

    @staticmethod
    def _pdf_map_base_font(raw_font_name):
        """Map source PDF font names to PyMuPDF base fonts.

        Notes:
        - PyMuPDF accepts PostScript-like base font names such as:
          Helvetica, Helvetica-Bold, Helvetica-Oblique, Helvetica-BoldOblique,
          Times-Roman, Times-Bold, Times-Italic, Times-BoldItalic,
          Courier, Courier-Bold, Courier-Oblique, Courier-BoldOblique.

        This is a best-effort mapping. For Unicode output we may still need a TTF font,
        in which case bold/italic may not be perfectly preserved.
        """
        name = (raw_font_name or "").lower()
        if not name:
            return "Helvetica"

        is_bold = any(k in name for k in ("bold", "black", "heavy", "demi", "semibold"))
        is_italic = any(k in name for k in ("italic", "oblique", "slanted"))

        family = "Helvetica"
        if "courier" in name or name.startswith("cour"):
            family = "Courier"
        elif "times" in name or "roman" in name or "serif" in name:
            family = "Times-Roman"
        elif "helvetica" in name or "arial" in name or "sans" in name:
            family = "Helvetica"

        # Compose variant name
        if family == "Times-Roman":
            if is_bold and is_italic:
                return "Times-BoldItalic"
            if is_bold:
                return "Times-Bold"
            if is_italic:
                return "Times-Italic"
            return "Times-Roman"

        if family == "Courier":
            if is_bold and is_italic:
                return "Courier-BoldOblique"
            if is_bold:
                return "Courier-Bold"
            if is_italic:
                return "Courier-Oblique"
            return "Courier"

        # Helvetica
        if is_bold and is_italic:
            return "Helvetica-BoldOblique"
        if is_bold:
            return "Helvetica-Bold"
        if is_italic:
            return "Helvetica-Oblique"
        return "Helvetica"

    def _pdf_should_keep_source_for_layout(self, source_text, translated_text, rect, base_fontsize):
        """Return True when replacing text likely breaks layout (tiny fallback / narrow cell)."""
        src = (source_text or "").strip()
        dst = (translated_text or "").strip()
        if not src or not dst:
            return True
        if rect.width <= 0 or rect.height <= 0:
            return True

        # Form-like patterns: keep source to preserve exact visual layout.
        if re.search(r"\.{4,}|_{3,}|-{4,}|:{1}\s*$", src):
            return True

        # Extremely tiny boxes are still risky even with wrapping.
        if rect.width < 38 or rect.height < 8:
            return True

        # If translation is long, we prefer auto-fit (shrink/wrap) rather than keeping source.
        # The actual fitting is handled by _pdf_insert_text.
        return False

    # ------------------------------------------------------------------ #
    #  PDF — main processor (PyMuPDF in-place text replacement)            #
    # ------------------------------------------------------------------ #
    def _process_pdf(self, file_path, target_lang, progress_callback=None, *,
                     ocr_images=False, ocr_langs=None, ocr_mode=None, bilingual_mode=None, bilingual_delimiter=None):
        """Translate PDF preserving layout / images using PyMuPDF text redaction+insertion."""

        # If enabled, do NOT silently fall back to source text on translation errors.
        # This makes it obvious when the API is not being used / cannot be used.
        api_only = str(os.getenv('AI_DISABLE_FALLBACK', '0')).strip().lower() in ('1', 'true', 'yes', 'on')

        bi_mode = (bilingual_mode or 'none').strip().lower()
        if bi_mode not in ('none', 'inline', 'newline'):
            bi_mode = 'none'
        # Newline bilingual mode on PDFs often causes text overlap because PDF pages don't reflow.
        # Keep it disabled by default; allow opt-in via PDF_ALLOW_NEWLINE_MODE=1.
        allow_pdf_newline = str(os.getenv('PDF_ALLOW_NEWLINE_MODE', '0')).strip().lower() in ('1', 'true', 'yes', 'on')
        if bi_mode == 'newline' and not allow_pdf_newline:
            bi_mode = 'none'
        ocr_mode_norm = (str(ocr_mode).strip().lower() if ocr_mode else 'auto')
        if ocr_mode_norm not in ('image', 'text', 'both', 'auto'):
            ocr_mode_norm = 'auto'
        strict_pdf_preserve = str(os.getenv('PDF_STRICT_PRESERVE', '0')).strip().lower() in ('1', 'true', 'yes', 'on')
        # Table handling:
        # - skip  : never replace table-like cell text (max layout preservation)
        # - safe  : attempt table replacement only when it fits without aggressive shrinking (default)
        # - force : treat tables like normal lines (highest risk of layout damage)
        # Default to 'safe' to avoid layout corruption on form-like PDFs.
        pdf_table_mode = (os.getenv('PDF_TABLE_MODE') or 'safe').strip().lower()
        if pdf_table_mode not in ('skip', 'safe', 'force'):
            pdf_table_mode = 'safe'

        # Coverage fallback:
        # When enabled, we relax layout guards to translate more lines.
        # This can cause overlaps on form-like PDFs, so default is OFF.
        pdf_coverage_fallback = str(os.getenv('PDF_COVERAGE_FALLBACK', '0')).strip().lower() in ('1', 'true', 'yes', 'on')

        # --- require PyMuPDF ---
        try:
            import pymupdf as _mu
        except ImportError:
            try:
                import fitz as _mu          # older PyMuPDF
            except ImportError:
                return self._process_pdf_text_fallback(
                    file_path, target_lang, progress_callback,
                    ocr_images=ocr_images, ocr_langs=ocr_langs, bi_mode=bi_mode, bilingual_delimiter=bilingual_delimiter)

        doc = _mu.open(file_path)
        total_pages = len(doc)

        _font_path = self._pdf_find_font()
        FONT_NAME = "tj-uni"
        # OCR collected entries
        # - image OCR: OCR extracted embedded images (existing behavior)
        # - page OCR : OCR rendered whole page when the page is scanned (no selectable text)
        ocr_image_entries = []  # (page_num, ocr_text_raw)
        ocr_page_entries = []   # (page_num, ocr_text_raw)
        # For scanned pages where we already have translated OCR text (e.g. from overlay pipeline)
        ocr_page_translated_entries = []  # (page_num, ocr_text_raw, translated_text)

        # Full-page overlay mode for PDFs (highest visual layout fidelity):
        # We ONLY apply it to scanned pages (little / no selectable text), otherwise we'd
        # rasterize normal PDFs and the user would perceive it as "mất định dạng".
        # Visual-exact PDF mode:
        # Render the whole page to an image, translate via OCR bboxes, then place the rendered
        # translated image back onto the PDF page.
        # This is the only practical way to achieve near-100% layout fidelity on complex forms.
        # Trade-off: output pages become rasterized (text may no longer be selectable/copyable).
        pdf_full_page_overlay_enabled = bool(
            self.ocr_translate_overlay
            and ocr_mode_norm in ('image', 'both', 'auto')
        )

        # Force full-page overlay even when selectable text exists.
        # WARNING: this will rasterize the page visually (text may no longer be selectable/copyable).
        # Force full-page overlay even when selectable text exists.
        # Default ON to satisfy strict "same as original" visual requirement.
        # Set PDF_FULL_PAGE_OVERLAY_FORCE=0 to revert to text-based replacement where possible.
        pdf_full_page_overlay_force = (
            str(os.getenv('PDF_FULL_PAGE_OVERLAY_FORCE', '1')).strip().lower() in ('1', 'true', 'yes', 'on')
        )

        # Selectable-text PDFs: translate by text *blocks* (bbox) rather than per-line.
        # This matches the required workflow: extract block + coords -> translate -> fit -> reinsert.
        # Default OFF: form-like PDFs frequently mix styles/colors within a block, and block-union
        # insertion can distort layout. Enable explicitly if desired.
        pdf_translate_by_block = str(os.getenv('PDF_TRANSLATE_BY_BLOCK', '0')).strip().lower() in ('1', 'true', 'yes', 'on')

        # Heuristic: treat page as scanned if extracted selectable text is below this threshold.
        try:
            pdf_scan_text_threshold = int(os.getenv('PDF_SCAN_TEXT_THRESHOLD', '40'))
        except Exception:
            pdf_scan_text_threshold = 40
        if pdf_scan_text_threshold < 5:
            pdf_scan_text_threshold = 5

        def _pdf_render_dpi():
            try:
                default_dpi = '300' if pdf_full_page_overlay_force else '200'
                dpi = int(os.getenv('PDF_OCR_DPI', default_dpi))
            except Exception:
                dpi = 200
            if dpi < 120:
                dpi = 120
            if dpi > 350:
                dpi = 350
            return dpi

        def _maybe_ocr_scanned_page(_page, _pg_idx, _raw_text_lines):
            """Best-effort OCR for scanned PDF pages.

            Only runs when:
            - ocr_images is enabled
            - ocr_mode is text/both/auto
            - page appears to have no/very little selectable text
            """
            if not (ocr_images and self.ocr_image_to_text):
                return
            if ocr_mode_norm not in ('text', 'both', 'auto'):
                return

            # Coordinate overlay (vector PDF output) for scanned pages.
            # - When enabled, we OCR the rendered page image, translate per line, and insert
            #   translated text back into the PDF using the original OCR line bounding boxes.
            # - This keeps the original page background (scanned image) intact.
            pdf_ocr_coordinate_overlay_env = str(os.getenv('PDF_OCR_COORDINATE_OVERLAY', '0')).strip().lower() in ('1', 'true', 'yes', 'on')
            pdf_ocr_coordinate_overlay = bool(pdf_ocr_coordinate_overlay_env)
            # Auto-enable coordinate overlay when:
            # - user enabled OCR for PDFs
            # - output mode wants image preservation (image/both/auto)
            # - and we have an API bbox OCR hook available
            if (not pdf_ocr_coordinate_overlay) and ocr_images and self.ocr_image_to_bboxes and ocr_mode_norm in ('image', 'both', 'auto'):
                pdf_ocr_coordinate_overlay = True

            pdf_ocr_erase_behind = str(os.getenv('PDF_OCR_ERASE_BEHIND', '0')).strip().lower() in ('1', 'true', 'yes', 'on')
            try:
                pdf_ocr_conf_min = int(os.getenv('PDF_OCR_CONF_MIN', '45'))
            except Exception:
                pdf_ocr_conf_min = 45
            try:
                pdf_ocr_line_min_chars = int(os.getenv('PDF_OCR_LINE_MIN_CHARS', '3'))
            except Exception:
                pdf_ocr_line_min_chars = 3
            try:
                pdf_ocr_psm = int(os.getenv('PDF_OCR_PSM', '6'))
            except Exception:
                pdf_ocr_psm = 6

            def _ocr_coordinate_overlay_from_image(tmp_img_path, dpi):
                """OCR -> get line bounding boxes -> translate per line -> insert back into PDF.

                This keeps the page as a vector PDF (no full-page raster), but cannot perfectly
                remove the original scanned text unless PDF_OCR_ERASE_BEHIND=1 is enabled.
                """
                # Choose OCR provider for bbox extraction:
                # 1) API bbox OCR (preferred when available + requested)
                # 2) Tesseract bbox OCR (fallback)
                use_api = str(os.getenv('PDF_OCR_COORDINATE_OVERLAY_USE_API', '0')).strip().lower() in ('1', 'true', 'yes', 'on')
                line_items = []
                used_normalized = False

                if (use_api or not self.has_tesseract) and self.ocr_image_to_bboxes:
                    try:
                        items = self.ocr_image_to_bboxes(tmp_img_path, ocr_langs=ocr_langs)
                    except ProviderRateLimitError:
                        raise
                    except Exception as e:
                        print(f"PDF page {_pg_idx} API bbox OCR failed: {e}")
                        items = None

                    if items:
                        # Expect items: list of {'text': str, 'bbox': [x0,y0,x1,y1]} with bbox normalized [0..1]
                        used_normalized = True
                        for it in items:
                            try:
                                text = (it.get('text') or '').strip()
                                bb = it.get('bbox')
                                if not text or not bb or len(bb) != 4:
                                    continue
                                x0, y0, x1, y1 = [float(v) for v in bb]
                                # Clamp
                                x0 = max(0.0, min(1.0, x0))
                                y0 = max(0.0, min(1.0, y0))
                                x1 = max(0.0, min(1.0, x1))
                                y1 = max(0.0, min(1.0, y1))
                                if x1 <= x0 or y1 <= y0:
                                    continue
                                if len(text) < pdf_ocr_line_min_chars:
                                    continue
                                line_items.append((y0, x0, (x0, y0, x1, y1), text))
                            except Exception:
                                continue

                if not line_items:
                    # --- Tesseract coordinate OCR fallback ---
                    try:
                        import pytesseract
                        from pytesseract import Output
                    except Exception as e:
                        print(f"PDF page {_pg_idx} coordinate OCR unavailable (pytesseract): {e}")
                        return False
                    try:
                        from PIL import Image
                    except Exception as e:
                        print(f"PDF page {_pg_idx} coordinate OCR unavailable (PIL): {e}")
                        return False

                    # Ensure tesseract command is configured (Windows PATH issues)
                    try:
                        tcmd = (os.getenv('TESSERACT_CMD') or '').strip()
                        if tcmd:
                            pytesseract.pytesseract.tesseract_cmd = tcmd
                    except Exception:
                        pass

                    try:
                        img = Image.open(tmp_img_path)
                    except Exception as e:
                        print(f"PDF page {_pg_idx} coordinate OCR image open error: {e}")
                        return False

                    lang = (ocr_langs or os.getenv('OCR_LANGS_DEFAULT') or 'eng').strip()
                    cfg = f"--psm {pdf_ocr_psm}"

                    try:
                        data = pytesseract.image_to_data(img, lang=lang, config=cfg, output_type=Output.DICT)
                    except Exception as e:
                        print(f"PDF page {_pg_idx} coordinate OCR failed: {e}")
                        return False

                    n = 0
                    try:
                        n = int(len(data.get('text') or []))
                    except Exception:
                        n = 0
                    if n <= 0:
                        return False

                    # Group words into lines using Tesseract's block/par/line numbers.
                    lines = {}
                    for i in range(n):
                        w = (data.get('text') or [''])[i]
                        if not w or not str(w).strip():
                            continue

                        try:
                            conf_raw = (data.get('conf') or [''])[i]
                            conf = int(float(conf_raw)) if str(conf_raw).strip() != '' else -1
                        except Exception:
                            conf = -1
                        if conf >= 0 and conf < pdf_ocr_conf_min:
                            continue

                        try:
                            left = int((data.get('left') or [0])[i])
                            top = int((data.get('top') or [0])[i])
                            width = int((data.get('width') or [0])[i])
                            height = int((data.get('height') or [0])[i])
                        except Exception:
                            continue

                        try:
                            key = (
                                int((data.get('block_num') or [0])[i]),
                                int((data.get('par_num') or [0])[i]),
                                int((data.get('line_num') or [0])[i]),
                            )
                        except Exception:
                            key = (0, 0, i)

                        entry = lines.get(key)
                        if entry is None:
                            entry = {
                                'words': [],
                                'bbox': [left, top, left + width, top + height],
                            }
                            lines[key] = entry

                        entry['words'].append(str(w))
                        bb = entry['bbox']
                        bb[0] = min(bb[0], left)
                        bb[1] = min(bb[1], top)
                        bb[2] = max(bb[2], left + width)
                        bb[3] = max(bb[3], top + height)

                    if not lines:
                        return False

                    # Sort visually by y then x
                    for _k, v in lines.items():
                        text = ' '.join(v['words']).strip()
                        if len(text) < pdf_ocr_line_min_chars:
                            continue
                        x0, y0, x1, y1 = v['bbox']
                        if (x1 - x0) <= 1 or (y1 - y0) <= 1:
                            continue
                        line_items.append((y0, x0, (x0, y0, x1, y1), text))
                    if not line_items:
                        return False

                line_items.sort(key=lambda it: (it[0], it[1]))

                # Translate per line (parallel)
                src_texts = [it[3] for it in line_items]
                trans_texts = [None] * len(src_texts)
                with self._executor_cls(max_workers=self.concurrency) as ex:
                    futs = {}
                    for idx, t in enumerate(src_texts):
                        if t.strip():
                            futs[ex.submit(self._translate_with_retry, t, target_lang)] = idx
                    for fut in futs:
                        idx = futs[fut]
                        try:
                            trans_texts[idx] = fut.result()
                        except ProviderRateLimitError:
                            raise
                        except Exception as e:
                            print(f"PDF page {_pg_idx} OCR-line translation error: {e}")
                            if api_only:
                                raise
                            trans_texts[idx] = src_texts[idx]

                # Map image coords -> PDF coords
                page_w = float(_page.rect.width)
                page_h = float(_page.rect.height)
                if used_normalized:
                    sx = page_w
                    sy = page_h
                else:
                    # pixel -> PDF scaling factors
                    try:
                        pix_w = float(img.size[0])
                        pix_h = float(img.size[1])
                        sx = (page_w / pix_w) if pix_w else 1.0
                        sy = (page_h / pix_h) if pix_h else 1.0
                    except Exception:
                        sx = 1.0
                        sy = 1.0

                # Optional erase behind (white boxes)
                if pdf_ocr_erase_behind:
                    for (_y, _x, (x0, y0, x1, y1), _src), trans in zip(line_items, trans_texts):
                        if not trans or not str(trans).strip():
                            continue
                        r = _mu.Rect(x0 * sx, y0 * sy, x1 * sx, y1 * sy)
                        # Slightly pad to cover glyph ascenders/descenders
                        pad = max(0.5, float(r.height) * 0.15)
                        r = _mu.Rect(r.x0, max(0, r.y0 - pad), r.x1, min(_page.rect.height, r.y1 + pad))
                        _page.add_redact_annot(r, fill=(1, 1, 1))
                    try:
                        _page.apply_redactions(images=0)
                    except TypeError:
                        _page.apply_redactions()

                # Insert translated text
                for (_y, _x, (x0, y0, x1, y1), _src), trans in zip(line_items, trans_texts):
                    if not trans or not str(trans).strip():
                        continue
                    r = _mu.Rect(x0 * sx, y0 * sy, x1 * sx, y1 * sy)
                    # Estimate fontsize from bbox height
                    base_fs = max(6.0, float(r.height) * 0.78)
                    self._pdf_insert_text(
                        _page,
                        r,
                        str(trans).strip(),
                        base_fs,
                        _font_path,
                        FONT_NAME,
                        color=(0, 0, 0),
                        preferred_base_font=None,
                    )

                return True

            try:
                # Heuristic: if extracted selectable text is very small, treat as scanned.
                extracted_chars = 0
                try:
                    extracted_chars = sum(len((ln.get('text') or '').strip()) for ln in (_raw_text_lines or []))
                except Exception:
                    extracted_chars = 0
                if extracted_chars >= 40 and len((_raw_text_lines or [])) >= 2:
                    return

                # Render page to image using PyMuPDF and OCR that image.
                try:
                    dpi = int(os.getenv('PDF_OCR_DPI', '200'))
                except Exception:
                    dpi = 200
                if dpi < 120:
                    dpi = 120
                if dpi > 350:
                    dpi = 350

                pix = _page.get_pixmap(dpi=dpi)
                tmp = os.path.join(self.upload_folder, f"pdf_page_ocr_{uuid.uuid4().hex}.png")
                pix.save(tmp)
                try:
                    # Optional: coordinate overlay mode (non-raster PDF output)
                    if pdf_ocr_coordinate_overlay:
                        try:
                            ok = _ocr_coordinate_overlay_from_image(tmp, dpi)
                        except ProviderRateLimitError:
                            raise
                        except Exception as e:
                            ok = False
                            print(f"PDF page {_pg_idx} coordinate overlay error: {e}")
                        if ok:
                            # Coordinate overlay already placed translated text on-page.
                            # Do NOT append OCR pages; return early.
                            return

                    # Prefer overlay translation when requested so we preserve the original visual layout.
                    if self.ocr_translate_overlay and ocr_mode_norm in ('image', 'both', 'auto'):
                        try:
                            ocr_text, translated_text, png_bytes, rec_mode = self.ocr_translate_overlay(
                                tmp,
                                source_lang='auto',
                                target_lang=target_lang,
                                ocr_langs=ocr_langs,
                            )
                        except Exception as e:
                            ocr_text = ""
                            translated_text = ""
                            png_bytes = None
                            rec_mode = 'text'
                            print(f"PDF page {_pg_idx} overlay OCR error: {e}")

                        rec_mode = (rec_mode or 'text').strip().lower()
                        if rec_mode not in ('text', 'image', 'both'):
                            rec_mode = 'text'

                        # Decide whether to overlay image back onto the scanned page
                        do_overlay = False
                        if ocr_mode_norm == 'image':
                            do_overlay = True
                        elif ocr_mode_norm == 'both':
                            do_overlay = True
                        elif ocr_mode_norm == 'auto':
                            do_overlay = rec_mode in ('image', 'both')

                        if do_overlay and png_bytes:
                            try:
                                _page.insert_image(_page.rect, stream=png_bytes)
                            except Exception as e:
                                print(f"PDF page {_pg_idx} insert overlay error: {e}")

                        # Decide whether to append translated OCR text pages
                        do_append_text = False
                        if ocr_mode_norm == 'both':
                            do_append_text = True
                        elif ocr_mode_norm == 'auto':
                            do_append_text = rec_mode in ('text', 'both')
                        elif ocr_mode_norm == 'text':
                            do_append_text = True

                        if do_append_text:
                            if translated_text and str(translated_text).strip():
                                ocr_page_translated_entries.append((_pg_idx, (ocr_text or '').strip(), str(translated_text).strip()))
                            else:
                                # Fallback to plain OCR text later translation
                                if ocr_text and str(ocr_text).strip():
                                    ocr_page_entries.append((_pg_idx, str(ocr_text).strip()))

                        return

                    # Fallback: text-only OCR (append translated text pages later)
                    txt = self.ocr_image_to_text(tmp, ocr_langs=ocr_langs)
                    if txt and txt.strip():
                        ocr_page_entries.append((_pg_idx, txt.strip()))
                finally:
                    try:
                        os.remove(tmp)
                    except Exception:
                        pass
            except Exception as e:
                print(f"PDF page {_pg_idx} render OCR error: {e}")

        # =================== PAGE LOOP ===================
        for pg_idx in range(total_pages):
            if progress_callback:
                pct = int(5 + (pg_idx / max(1, total_pages)) * 75)
                progress_callback(pct, f"Translating page {pg_idx + 1}/{total_pages}")

            page = doc[pg_idx]

            # --- Full-page overlay path (best layout preservation) ---
            if pdf_full_page_overlay_enabled:
                # IMPORTANT: Only overlay on scanned pages (no/low selectable text)
                try:
                    selectable = (page.get_text('text') or '').strip()
                    if len(selectable) >= pdf_scan_text_threshold:
                        selectable = None
                except Exception:
                    selectable = None
                if selectable is not None:
                    # scanned/low-text page -> overlay
                    pass
                else:
                    # normal text page -> keep original text-based pipeline
                    selectable = None
                    # fall through to normal processing
                if selectable is None:
                    # Not a scanned page
                    pass
            if pdf_full_page_overlay_enabled:
                try:
                    selectable = (page.get_text('text') or '').strip()
                    is_scanned = len(selectable) < pdf_scan_text_threshold
                except Exception:
                    is_scanned = True

            if pdf_full_page_overlay_enabled and (is_scanned or pdf_full_page_overlay_force):
                try:
                    dpi = _pdf_render_dpi()
                    pix = page.get_pixmap(dpi=dpi)
                    tmp = os.path.join(self.upload_folder, f"pdf_page_full_overlay_{uuid.uuid4().hex}.png")
                    pix.save(tmp)
                    try:
                        ocr_text, translated_text, png_bytes, _rec_mode = self.ocr_translate_overlay(
                            tmp,
                            source_lang='auto',
                            target_lang=target_lang,
                            ocr_langs=ocr_langs,
                        )
                    finally:
                        try:
                            os.remove(tmp)
                        except Exception:
                            pass

                    if png_bytes:
                        # Place translated rendered page image on top of original page.
                        try:
                            page.insert_image(page.rect, stream=png_bytes)
                        except Exception as e:
                            print(f"PDF page {pg_idx} insert full overlay error: {e}")

                    if ocr_mode_norm == 'both':
                        if translated_text and str(translated_text).strip():
                            ocr_page_translated_entries.append((pg_idx, (ocr_text or '').strip(), str(translated_text).strip()))
                        elif ocr_text and str(ocr_text).strip():
                            ocr_page_entries.append((pg_idx, str(ocr_text).strip()))

                    # Skip normal text replacement for this page.
                    continue
                except Exception as e:
                    # If overlay fails for any reason, fall back to the normal PDF text pipeline.
                    # But if full-page overlay is forced, do not silently degrade fidelity.
                    msg = f"PDF page {pg_idx} full overlay failed: {e}"
                    if pdf_full_page_overlay_force:
                        raise RuntimeError(msg) from e
                    print(f"{msg}; falling back to text mode")

            # --- extract selectable text units with bbox + style metadata ---
            td = page.get_text("dict", flags=_mu.TEXT_PRESERVE_WHITESPACE)
            raw_blocks = td.get("blocks", [])

            raw_text_lines = []
            if pdf_translate_by_block:
                # BLOCK mode: one translate call per PyMuPDF text block
                for blk in raw_blocks:
                    if blk.get("type") != 0:   # 0 = text
                        continue
                    lines = blk.get("lines", [])
                    if not lines:
                        continue

                    block_lines = []
                    sizes = []
                    colors = []
                    span_fonts = []
                    span_bboxes = []
                    union_rect = None
                    for ln in lines:
                        ln_text = ""
                        for sp in ln.get("spans", []):
                            seg_text = sp.get("text", "")
                            if seg_text:
                                ln_text += seg_text
                                sizes.append(sp.get("size", 11))
                                colors.append(sp.get("color", 0))
                                span_fonts.append(sp.get("font", ""))
                                span_bboxes.append(sp.get("bbox", (0, 0, 0, 0)))

                        ln_text = ln_text.rstrip()
                        if ln_text:
                            block_lines.append(ln_text)

                        try:
                            r_ln = _mu.Rect(ln.get("bbox", blk.get("bbox")))
                            if union_rect is None:
                                union_rect = r_ln
                            else:
                                union_rect |= r_ln
                        except Exception:
                            pass

                    if not block_lines:
                        continue

                    block_text = "\n".join(block_lines).rstrip()
                    if not self._pdf_should_translate_text(block_text):
                        continue

                    block_rect = union_rect or _mu.Rect(blk.get("bbox"))
                    avg_sz = sum(sizes) / len(sizes) if sizes else 11
                    color_val = colors[0] if colors else 0
                    source_font = span_fonts[0] if span_fonts else ""
                    raw_text_lines.append({
                        "rect": block_rect,
                        "text": block_text,
                        "fontsize": max(6.0, round(avg_sz, 1)),
                        "color": self._pdf_decode_color_int(color_val),
                        "base_font": self._pdf_map_base_font(source_font),
                        "is_columnar": self._pdf_has_columnar_layout(span_bboxes, avg_sz),
                    })
            else:
                # LINE mode: legacy behavior
                for blk in raw_blocks:
                    if blk.get("type") != 0:   # 0 = text
                        continue
                    lines = blk.get("lines", [])
                    if not lines:
                        continue
                    for ln in lines:
                        ln_text = ""
                        sizes = []
                        colors = []
                        span_fonts = []
                        span_bboxes = []
                        trans_span_bboxes = []
                        for sp in ln.get("spans", []):
                            seg_text = sp.get("text", "")
                            if seg_text:
                                # Always track for layout heuristics
                                sizes.append(sp.get("size", 11))
                                colors.append(sp.get("color", 0))
                                span_fonts.append(sp.get("font", ""))
                                span_bboxes.append(sp.get("bbox", (0, 0, 0, 0)))

                                # Exclude dotted/underscore leaders from translation/redaction.
                                if not self._pdf_is_dot_leader_span(seg_text):
                                    bb = sp.get("bbox")
                                    # If a span mixes text and dot-leaders, translate only the prefix
                                    # and use an approximated sub-bbox that ends before the leader run.
                                    try:
                                        m = re.search(r"[\._\-]{5,}", seg_text)
                                    except Exception:
                                        m = None

                                    if m and bb and len(bb) == 4:
                                        prefix = (seg_text[: m.start()] or "").rstrip()
                                        if prefix and self._pdf_should_translate_text(prefix):
                                            ln_text += prefix
                                            try:
                                                x0, y0, x1, y1 = [float(v) for v in bb]
                                                w = max(1.0, x1 - x0)
                                                frac = float(m.start()) / float(max(1, len(seg_text)))
                                                sub_x1 = x0 + (w * max(0.05, min(0.98, frac)))
                                                if sub_x1 > x0 + 1:
                                                    trans_span_bboxes.append((x0, y0, sub_x1, y1))
                                            except Exception:
                                                trans_span_bboxes.append(bb)
                                    else:
                                        ln_text += seg_text
                                        if bb and len(bb) == 4:
                                            trans_span_bboxes.append(bb)

                        ln_text = ln_text.rstrip()
                        if not self._pdf_should_translate_text(ln_text):
                            continue

                        # Redact/insert only within the union of translatable spans.
                        # This prevents erasing form leader dots/lines that typically live in separate spans.
                        if trans_span_bboxes:
                            line_rect = _mu.Rect(trans_span_bboxes[0])
                            for bb in trans_span_bboxes[1:]:
                                try:
                                    line_rect |= _mu.Rect(bb)
                                except Exception:
                                    continue
                        else:
                            line_rect = _mu.Rect(ln.get("bbox", blk["bbox"]))
                        avg_sz = sum(sizes) / len(sizes) if sizes else 11
                        color_val = colors[0] if colors else 0
                        source_font = span_fonts[0] if span_fonts else ""
                        raw_text_lines.append({
                            "rect": line_rect,
                            "text": ln_text,
                            "fontsize": max(6.0, round(avg_sz, 1)),
                            "color": self._pdf_decode_color_int(color_val),
                            "base_font": self._pdf_map_base_font(source_font),
                            "is_columnar": self._pdf_has_columnar_layout(span_bboxes, avg_sz),
                        })

            for info in raw_text_lines:
                info["is_table_like"] = self._pdf_is_table_like_line(
                    info.get("text", ""),
                    info.get("rect"),
                    bool(info.get("is_columnar")),
                )

            # Merge multi-line cell content before translation (only meaningful in LINE mode).
            if not pdf_translate_by_block:
                raw_text_lines = self._pdf_group_table_cell_lines(raw_text_lines)

            # Table mode: skip/safe/force
            if pdf_table_mode == 'skip':
                raw_text_lines = [info for info in raw_text_lines if not info.get('is_table_like')]

            # Strict preserve first, then relaxed fallback if too few lines remain.
            text_lines = raw_text_lines
            if strict_pdf_preserve and bi_mode == 'none' and raw_text_lines:
                strict_lines = [
                    info for info in raw_text_lines
                    if (not info.get("is_columnar")) and self._pdf_is_prose_candidate(info["text"], info["rect"])
                ]
                if len(strict_lines) >= max(2, int(len(raw_text_lines) * 0.20)):
                    text_lines = strict_lines
                else:
                    relaxed_lines = [
                        info for info in raw_text_lines
                        if (not info.get("is_columnar")) and self._pdf_is_relaxed_candidate(info["text"], info["rect"])
                    ]
                    if relaxed_lines:
                        text_lines = relaxed_lines

                # Emergency fallback: if strict+relaxed still leaves no translatable lines,
                # allow medium-length non-column lines so requested language is still applied.
                if not text_lines:
                    fallback_lines = [
                        info for info in raw_text_lines
                        if (not info.get("is_columnar"))
                        and len((info.get("text") or "").strip()) >= 20
                        and info["rect"].width >= 140
                    ]
                    if fallback_lines:
                        text_lines = fallback_lines

            # --- OCR images on this page ---
            if ocr_images and (self.ocr_image_to_text or self.ocr_translate_overlay):
                try:
                    for img_info in page.get_images(full=True):
                        xref = img_info[0]
                        try:
                            # Where does this image appear on the page?
                            try:
                                img_rects = list(page.get_image_rects(xref) or [])
                            except Exception:
                                img_rects = []

                            base = doc.extract_image(xref)
                            if not base:
                                continue
                            data = base["image"]
                            if len(data) < 2000:
                                continue
                            ext = base.get("ext", "png")
                            tmp = os.path.join(self.upload_folder, f"pdf_img_{uuid.uuid4().hex}.{ext}")
                            with open(tmp, "wb") as f:
                                f.write(data)
                            try:
                                # If requested, overlay translated text onto the image area (keeps PDF layout).
                                overlay_ocr_text = None
                                if self.ocr_translate_overlay and img_rects and ocr_mode_norm in ('image', 'both', 'auto'):
                                    try:
                                        ocr_text, _translated_text, png_bytes, rec_mode = self.ocr_translate_overlay(
                                            tmp,
                                            source_lang='auto',
                                            target_lang=target_lang,
                                            ocr_langs=ocr_langs,
                                        )
                                        overlay_ocr_text = (ocr_text or '').strip()
                                    except Exception as e:
                                        png_bytes = None
                                        rec_mode = 'text'
                                        print(f"PDF page {pg_idx} image overlay OCR error: {e}")

                                    rec_mode = (rec_mode or 'text').strip().lower()
                                    if rec_mode not in ('text', 'image', 'both'):
                                        rec_mode = 'text'

                                    do_overlay = False
                                    if ocr_mode_norm in ('image', 'both'):
                                        do_overlay = True
                                    elif ocr_mode_norm == 'auto':
                                        do_overlay = rec_mode in ('image', 'both')

                                    if do_overlay and png_bytes:
                                        try:
                                            try:
                                                min_area = float(os.getenv('PDF_IMAGE_OVERLAY_MIN_AREA', '2500'))
                                            except Exception:
                                                min_area = 2500.0
                                            for r in img_rects:
                                                try:
                                                    if (float(r.width) * float(r.height)) < min_area:
                                                        continue
                                                except Exception:
                                                    pass
                                                page.insert_image(r, stream=png_bytes, overlay=True)
                                        except Exception as e:
                                            print(f"PDF page {pg_idx} insert image overlay error: {e}")

                                # Collect OCR text from images (append pages only in ocr_mode=both).
                                if self.ocr_image_to_text and ocr_mode_norm in ('text', 'both', 'auto'):
                                    txt = overlay_ocr_text
                                    if not txt:
                                        txt = self.ocr_image_to_text(tmp, ocr_langs=ocr_langs)
                                    if txt and txt.strip():
                                        ocr_image_entries.append((pg_idx, txt.strip()))
                            finally:
                                try:
                                    os.remove(tmp)
                                except Exception:
                                    pass
                        except Exception:
                            continue
                except Exception as e:
                    print(f"PDF page {pg_idx} image extraction error: {e}")

            # If the page likely contains only scanned content (no selectable text),
            # optionally OCR the whole rendered page.
            if not text_lines:
                _maybe_ocr_scanned_page(page, pg_idx, raw_text_lines)
                continue

            # Also consider OCRing the page when selectable text is very sparse.
            _maybe_ocr_scanned_page(page, pg_idx, raw_text_lines)

            # --- translate lines in parallel ---
            src_texts   = [line["text"] for line in text_lines]
            trans_texts = [None] * len(src_texts)

            with self._executor_cls(max_workers=self.concurrency) as ex:
                futs = {}
                for i, t in enumerate(src_texts):
                    if t.strip():
                        futs[ex.submit(self._translate_with_retry, t, target_lang)] = i
                for fut in futs:
                    idx = futs[fut]
                    try:
                        trans_texts[idx] = fut.result()
                    except ProviderRateLimitError:
                        doc.close()
                        raise
                    except Exception as e:
                        print(f"PDF line translation error: {e}")
                        if api_only:
                            doc.close()
                            raise
                        trans_texts[idx] = src_texts[idx]

            # --- apply translations to the page ---
            if bi_mode in ('none', 'inline'):
                render_items = []
                skipped_due_layout = 0
                skipped_candidates = []
                for info, trans in zip(text_lines, trans_texts):
                    if not trans or not trans.strip():
                        continue
                    if bi_mode == 'inline':
                        display = self._join_inline_bilingual(info.get('text') or '', trans or '', bilingual_delimiter)
                        fs = info["fontsize"] * 0.78
                    else:
                        # Table-safe mode: be extra conservative for table/cell content.
                        if pdf_table_mode == 'safe' and info.get('is_table_like'):
                            src = (info.get('text') or '').strip()
                            dst = (trans or '').strip()
                            if not src or not dst:
                                continue
                            # Avoid big expansion in tight cells.
                            if len(dst) > int(len(src) * 1.60):
                                continue
                            fitted = self._pdf_fit_fontsize(info['rect'].width, info['rect'].height, dst, info['fontsize'])
                            if fitted < max(4.6, info['fontsize'] * 0.60):
                                continue
                        if self._pdf_should_keep_source_for_layout(
                            info["text"], trans, info["rect"], info["fontsize"]
                        ):
                            skipped_due_layout += 1
                            skipped_candidates.append((info, trans))
                            continue
                        display = trans
                        fs = info["fontsize"]

                    render_items.append({
                        "rect": info["rect"],
                        "display": display,
                        "fontsize": fs,
                        "color": info["color"],
                        "base_font": info.get("base_font", "helv"),
                    })

                if pdf_coverage_fallback:
                    # Coverage fallback: if too few lines are translated on this page,
                    # relax layout guard for additional safe candidates so target language appears clearly.
                    if bi_mode == 'none' and text_lines:
                        min_expected = max(1, int(len(text_lines) * 0.35))
                        if len(render_items) < min_expected and skipped_candidates:
                            for info, trans in skipped_candidates:
                                src = (info.get("text") or "").strip()
                                if len(src) < 24:
                                    continue
                                if info["rect"].width < 140 or info["rect"].height < 10:
                                    continue
                                if re.search(r"\.{4,}|_{3,}|-{4,}", src):
                                    continue
                                render_items.append({
                                    "rect": info["rect"],
                                    "display": trans,
                                    "fontsize": info["fontsize"] * 0.90,
                                    "color": info["color"],
                                    "base_font": info.get("base_font", "helv"),
                                })
                                if len(render_items) >= min_expected:
                                    break

                    # Adaptive fallback: if strict preservation skipped almost everything,
                    # still replace safe prose lines so output reflects requested target language.
                    if bi_mode == 'none' and not render_items and text_lines and skipped_due_layout > 0:
                        for info, trans in zip(text_lines, trans_texts):
                            if not trans or not trans.strip():
                                continue
                            src = (info.get("text") or "").strip()
                            if len(src) < 28:
                                continue
                            if info["rect"].width < 160 or info["rect"].height < 10:
                                continue
                            if re.search(r"\.{4,}|_{3,}|-{4,}", src):
                                continue

                            render_items.append({
                                "rect": info["rect"],
                                "display": trans,
                                "fontsize": info["fontsize"] * 0.92,
                                "color": info["color"],
                                "base_font": info.get("base_font", "helv"),
                            })

                    # Last fallback: use first translated prose line if page still has no replacements.
                    if bi_mode == 'none' and not render_items and text_lines:
                        for info, trans in zip(text_lines, trans_texts):
                            if trans and trans.strip() and len((info.get("text") or "").strip()) >= 35:
                                render_items.append({
                                    "rect": info["rect"],
                                    "display": trans,
                                    "fontsize": info["fontsize"] * 0.90,
                                    "color": info["color"],
                                    "base_font": info.get("base_font", "helv"),
                                })
                                break

                # Replace visually without destructive redaction:
                # - Draw a filled rectangle to cover the old text area
                # - Insert translated text on top
                # This prevents "blanked" regions when insertion cannot fit.
                for item in render_items:
                    try:
                        page.draw_rect(item["rect"], color=None, fill=(1, 1, 1), overlay=True)
                    except Exception:
                        pass
                    self._pdf_insert_text(
                        page,
                        item["rect"],
                        item["display"],
                        item["fontsize"],
                        _font_path,
                        FONT_NAME,
                        color=item["color"],
                        preferred_base_font=item["base_font"],
                    )

            elif bi_mode == 'newline':
                # Keep original text, add translated text below each line
                for info, trans in zip(text_lines, trans_texts):
                    if not trans or not trans.strip():
                        continue
                    r = info["rect"]
                    h = max(r.height, 14)
                    new_r = _mu.Rect(r.x0, r.y1 + 2, r.x1, r.y1 + h + 12)
                    self._pdf_insert_text(page, new_r, trans,
                                          info["fontsize"] * 0.85,
                                          _font_path, FONT_NAME,
                                          color=(0.25, 0.25, 0.25))

        # =================== OCR image text translation ===================
        if ocr_image_entries:
            if progress_callback:
                progress_callback(85, f"Translating {len(ocr_image_entries)} image texts…")

            trans_ocr = []
            with self._executor_cls(max_workers=self.concurrency) as ex:
                fmap = {}
                for pn, txt in ocr_image_entries:
                    fmap[ex.submit(self._translate_with_retry, txt, target_lang)] = (pn, txt)
                for fut in fmap:
                    pn, txt = fmap[fut]
                    try:
                        trans_ocr.append((pn, txt, fut.result()))
                    except ProviderRateLimitError:
                        doc.close()
                        raise
                    except Exception:
                        if api_only:
                            doc.close()
                            raise
                        trans_ocr.append((pn, txt, txt))

            # Append OCR results as extra page(s) only when explicitly requested.
            # This avoids surprising "TEXT EXTRACTED FROM IMAGES" pages in normal PDF translation flow.
            if trans_ocr and ocr_mode_norm == 'both':
                kw = {"fontname": FONT_NAME, "fontfile": _font_path} if _font_path else {"fontname": "helv"}
                pg_new = doc.new_page(width=595, height=842)
                y = 50
                pg_new.insert_text(_mu.Point(50, y),
                                   "TEXT EXTRACTED FROM IMAGES (OCR):",
                                   fontsize=14, color=(0, 0, 0), **kw)
                y += 30
                for pn, _orig, translated in sorted(trans_ocr, key=lambda x: x[0]):
                    if y > 770:
                        pg_new = doc.new_page(width=595, height=842)
                        y = 50
                    pg_new.insert_text(_mu.Point(50, y),
                                       f"--- Page {pn + 1} Image ---",
                                       fontsize=10, color=(0.3, 0.3, 0.3), **kw)
                    y += 18
                    rect = _mu.Rect(50, y, 545, y + 120)
                    pg_new.insert_textbox(rect, translated, fontsize=10,
                                          color=(0, 0, 0), **kw)
                    y += 125

        # =================== OCR scanned pages translation ===================
        # Append OCR scanned-page translations as extra pages when requested (text/both/auto).
        # NOTE: When ocr_mode=image we prefer overlay onto the scanned page (layout preserved),
        # and do not append text pages.
        if (ocr_page_entries or ocr_page_translated_entries) and ocr_mode_norm in ('text', 'both', 'auto'):
            # Build final list of (page_num, original_ocr, translated)
            trans_pages = []

            # Entries already translated (from overlay pipeline)
            for pn, orig, translated in ocr_page_translated_entries:
                trans_pages.append((pn, orig, translated))

            # Entries requiring translation
            if ocr_page_entries:
                if progress_callback:
                    progress_callback(88, f"Translating {len(ocr_page_entries)} scanned page(s) via OCR…")

                with self._executor_cls(max_workers=self.concurrency) as ex:
                    fmap = {}
                    for pn, txt in ocr_page_entries:
                        fmap[ex.submit(self._translate_with_retry, txt, target_lang)] = (pn, txt)
                    for fut in fmap:
                        pn, txt = fmap[fut]
                        try:
                            trans_pages.append((pn, txt, fut.result()))
                        except ProviderRateLimitError:
                            doc.close()
                            raise
                        except Exception:
                            if api_only:
                                doc.close()
                                raise
                            trans_pages.append((pn, txt, txt))

            # For scanned pages, we cannot safely replace text in-place (there is none).
            # So we append translated text pages.
            if trans_pages:
                kw = {"fontname": FONT_NAME, "fontfile": _font_path} if _font_path else {"fontname": "helv"}
                pg_new = doc.new_page(width=595, height=842)
                y = 50
                pg_new.insert_text(
                    _mu.Point(50, y),
                    "OCR SCANNED PAGES (TRANSLATION):",
                    fontsize=14,
                    color=(0, 0, 0),
                    **kw,
                )
                y += 28

                for pn, _orig, translated in sorted(trans_pages, key=lambda x: x[0]):
                    if y > 770:
                        pg_new = doc.new_page(width=595, height=842)
                        y = 50
                    pg_new.insert_text(
                        _mu.Point(50, y),
                        f"--- Page {pn + 1} (OCR) ---",
                        fontsize=10,
                        color=(0.3, 0.3, 0.3),
                        **kw,
                    )
                    y += 16
                    rect = _mu.Rect(50, y, 545, y + 220)
                    pg_new.insert_textbox(
                        rect,
                        translated,
                        fontsize=10,
                        color=(0, 0, 0),
                        **kw,
                    )
                    y += 225

        # =================== SAVE ===================
        out_name = f"translated_{os.path.basename(file_path)}"
        if not out_name.lower().endswith('.pdf'):
            out_name = os.path.splitext(out_name)[0] + '.pdf'
        output_path = os.path.join(self.download_folder, out_name)
        doc.save(output_path, garbage=3, deflate=True)
        doc.close()

        if progress_callback:
            progress_callback(100, "Completed")
        return output_path

    # ------------------------------------------------------------------ #
    #  PDF — text-only fallback (no PyMuPDF available)                     #
    # ------------------------------------------------------------------ #
    def _process_pdf_text_fallback(self, file_path, target_lang, progress_callback=None,
                                    *, ocr_images=False, ocr_langs=None, bi_mode='none', bilingual_delimiter=None):
        """Fallback when PyMuPDF is not installed: extract text → translate → write .txt."""
        api_only = str(os.getenv('AI_DISABLE_FALLBACK', '0')).strip().lower() in ('1', 'true', 'yes', 'on')
        text = ""
        with open(file_path, 'rb') as f:
            pdf_reader = PyPDF2.PdfReader(f)
            total_pages = len(pdf_reader.pages)
            for i, page in enumerate(pdf_reader.pages):
                text += (page.extract_text() or "") + "\n"
                if progress_callback:
                    progress_callback(int(5 + (i / max(1, total_pages)) * 20),
                                      f"Extracting page {i+1}/{total_pages}")

        if progress_callback:
            progress_callback(25, "Translating PDF text…")

        paragraphs = [p for p in re.split(r'\n{2,}', text) if p.strip()]
        with self._executor_cls(max_workers=self.concurrency) as ex:
            futures = [ex.submit(self._translate_with_retry, p, target_lang) for p in paragraphs]
            translated = []
            for idx, fut in enumerate(futures):
                try:
                    t = fut.result()
                except ProviderRateLimitError:
                    raise
                except Exception:
                    if api_only:
                        raise
                    t = paragraphs[idx]
                if bi_mode == 'inline':
                    translated.append(self._join_inline_bilingual(paragraphs[idx], t, bilingual_delimiter))
                elif bi_mode == 'newline':
                    translated.append(f"{paragraphs[idx]}\n  → {t}")
                else:
                    translated.append(t)
        translated_text = '\n\n'.join(translated)

        output_filename = f"translated_{os.path.basename(file_path)}.txt"
        output_path = os.path.join(self.download_folder, output_filename)
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(translated_text)

        if progress_callback:
            progress_callback(100, "Completed")
        return output_path
    
    def _process_docx(self, file_path, target_lang, progress_callback=None, *, ocr_images=False, ocr_langs=None, ocr_mode=None, bilingual_mode=None, bilingual_delimiter=None, cleanup_pdf_layout=False):
        # Modify original document in-place so styles/images/relationships are preserved
        doc = docx.Document(file_path)

        api_only = str(os.getenv('AI_DISABLE_FALLBACK', '0')).strip().lower() in ('1', 'true', 'yes', 'on')

        # Bilingual mode:
        # - None / 'none': normal (replace original with translation)
        # - 'inline':  song ngữ liền kề — "Original | Translated" in same paragraph
        # - 'newline': song ngữ xuống dòng — keep original, add translated paragraph below
        bi_mode = (str(bilingual_mode).strip().lower() if bilingual_mode else 'none')
        if bi_mode not in ('none', 'inline', 'newline'):
            bi_mode = 'none'

        # OCR mode for embedded images in DOCX:
        # - image: replace embedded image bytes with overlay-rendered translation (keep design)
        # - text:  replace current image with translated text at that image location
        # - both:  keep translated image + also add translated text paragraph
        # - auto:  smart pick per image (prefers 'text' when Tesseract not available)
        mode = (str(ocr_mode).strip().lower() if ocr_mode else 'image')
        if mode not in ('image', 'text', 'both', 'auto'):
            mode = 'auto'

        def _auto_pick_mode(ocr_text, translated_text, ai_recommended_mode=None):
            """Pick OCR output mode per embedded image (AUTO mode).

            Returns 'text' or 'image':
              'text'  -> giữ ảnh gốc + chèn bản dịch bên dưới
              'image' -> chồng bản dịch lên ảnh (overlay)

            Logic đơn giản:
              - Ảnh có nhiều chữ (bài đọc, văn bản) -> 'text'
              - Ảnh kiểu banner / poster / quảng cáo  -> 'image'
            """
            try:
                raw = (ocr_text or '').strip()
                if not raw:
                    return 'text'

                char_count = len(raw)
                words = re.findall(r'\w+', raw, flags=re.UNICODE)
                word_count = len(words)

                # ── Prose: nhiều chữ -> luôn 'text' (giữ ảnh gốc) ──
                if char_count >= 120 or word_count >= 25:
                    print(f"  [MODE] Prose detected (chars={char_count}, words={word_count}), AI={ai_recommended_mode} -> text")
                    return 'text'

                # ── Banner detection ──
                lines = [ln.strip() for ln in raw.splitlines() if ln.strip()]
                low_raw = raw.lower()
                low_trans = (translated_text or '').lower()
                promo_keywords = (
                    'sale', 'discount', 'offer', 'book now', 'vacation',
                    'summer', 'up to', '% off', 'promo', 'hotline',
                    'free', 'limited', 'special', 'deal', 'subscribe',
                )
                has_promo = any(k in low_raw or k in low_trans for k in promo_keywords)

                alpha_chars = [ch for ch in raw if ch.isalpha()]
                upper_ratio = (
                    sum(1 for ch in alpha_chars if ch == ch.upper()) / max(1, len(alpha_chars))
                    if alpha_chars else 0.0
                )
                line_word_counts = [len(re.findall(r'\w+', ln, flags=re.UNICODE)) for ln in lines] if lines else [0]
                avg_wpl = (sum(line_word_counts) / len(line_word_counts)) if line_word_counts else 0.0
                short_lines = sum(1 for c in line_word_counts if c <= 3)

                looks_banner = (
                    has_promo or
                    (upper_ratio >= 0.50 and avg_wpl <= 4) or
                    (short_lines >= 3 and avg_wpl <= 3)
                )

                ai_mode = (ai_recommended_mode or '').lower()

                if looks_banner or ai_mode == 'image':
                    final = 'image'
                else:
                    final = 'text'

                print(
                    f"  [MODE] AI={ai_mode}, banner={looks_banner}, "
                    f"chars={char_count}, words={word_count}, upper={upper_ratio:.2f} -> {final}"
                )
                return final
            except Exception:
                return 'text'

        def iter_all_paragraphs(document):
            paras = []
            try:
                paras.extend(list(document.paragraphs))
            except Exception:
                pass
            try:
                for table in document.tables:
                    for row in table.rows:
                        for cell in row.cells:
                            paras.extend(list(cell.paragraphs))
            except Exception:
                pass
            try:
                for section in document.sections:
                    paras.extend(list(section.header.paragraphs))
                    paras.extend(list(section.footer.paragraphs))
            except Exception:
                pass
            return paras

        def paragraph_image_rids(paragraph):
            # Return relationship ids (rIdX) for images embedded in this paragraph.
            rids = []
            try:
                runs = list(paragraph.runs)
            except Exception:
                runs = []
            if not runs:
                return rids

            # Use a namespace-agnostic xpath for blips (image references)
            rel_attr = '{http://schemas.openxmlformats.org/officeDocument/2006/relationships}embed'
            for run in runs:
                try:
                    blips = run._element.xpath('.//*[local-name()="blip"]')
                except Exception:
                    blips = []
                for blip in blips:
                    try:
                        rid = blip.get(rel_attr)
                    except Exception:
                        rid = None
                    if rid:
                        rids.append(rid)
            # Preserve order but de-dup
            seen = set()
            out = []
            for rid in rids:
                if rid in seen:
                    continue
                seen.add(rid)
                out.append(rid)
            return out

        def rid_to_image_part(paragraph, rid):
            try:
                part = paragraph.part
                # python-docx keeps a mapping of related parts by rId
                related = getattr(part, 'related_parts', None)
                if isinstance(related, dict) and rid in related:
                    return related[rid]
            except Exception:
                pass
            try:
                # Fallback: via relationship object
                rels = getattr(paragraph.part, 'rels', None)
                if rels and rid in rels:
                    return rels[rid].target_part
            except Exception:
                pass
            return None

        def replace_image_with_text(paragraph, rid, translated_text):
            """Replace a specific embedded image (by rid) in a paragraph with translated text."""
            txt = (translated_text or '').strip()
            if not txt:
                return False

            rel_attr = '{http://schemas.openxmlformats.org/officeDocument/2006/relationships}embed'
            replaced = False
            try:
                runs = list(paragraph.runs)
            except Exception:
                runs = []

            for run in runs:
                try:
                    blips = run._element.xpath('.//*[local-name()="blip"]')
                except Exception:
                    blips = []
                if not blips:
                    continue

                has_target = False
                for blip in blips:
                    try:
                        if blip.get(rel_attr) == rid:
                            has_target = True
                            break
                    except Exception:
                        continue
                if not has_target:
                    continue

                try:
                    drawings = run._element.xpath('./*[local-name()="drawing"]')
                    for dr in drawings:
                        parent = dr.getparent()
                        if parent is not None:
                            parent.remove(dr)
                except Exception:
                    pass
                # Remove residual text from the image run; we add a clean run below.
                run.text = ""
                replaced = True
                break

            # Insert a clean run with normalized paragraph settings to avoid stretched spacing.
            try:
                paragraph.alignment = WD_PARAGRAPH_ALIGNMENT.LEFT
            except Exception:
                pass

            try:
                new_run = paragraph.add_run(txt)
                _ = new_run
                replaced = True
            except Exception:
                pass

            if not replaced:
                try:
                    paragraph.add_run(txt)
                    replaced = True
                except Exception:
                    replaced = False
            return replaced

        def _normalize_ocr_text_for_docx(text):
            """Normalize OCR translated text for readable DOCX insertion.

            - remove noisy tiny lines
            - collapse excessive whitespace
            - reflow short broken lines into normal prose
            """
            raw = (text or '').replace('\r\n', '\n').replace('\r', '\n')
            if not raw.strip():
                return ''

            cleaned_lines = []
            for ln in raw.split('\n'):
                ln2 = re.sub(r'\s+', ' ', (ln or '').strip())
                if not ln2:
                    continue
                if len(ln2) <= 1 and not re.search(r'[0-9]', ln2):
                    continue
                cleaned_lines.append(ln2)

            if not cleaned_lines:
                return ''

            out_parts = []
            cur = ''
            for ln in cleaned_lines:
                if not cur:
                    cur = ln
                    continue

                end_punct = cur.endswith(('.', '!', '?', ':', ';'))
                starts_bullet = bool(re.match(r'^(\-|\*|\d+[\.)])\s+', ln))
                if end_punct or starts_bullet:
                    out_parts.append(cur)
                    cur = ln
                else:
                    cur = f"{cur} {ln}".strip()

            if cur:
                out_parts.append(cur)

            normalized = '\n'.join(out_parts)
            return normalized.strip()

        def image_part_ext(image_part):
            # Try best-effort extension resolution
            try:
                partname = str(getattr(image_part, 'partname', '') or '')
                base = os.path.basename(partname)
                ext = os.path.splitext(base)[1].lower()
                if ext:
                    return ext
            except Exception:
                pass
            try:
                ct = str(getattr(image_part, 'content_type', '') or '').lower()
                mapping = {
                    'image/png': '.png',
                    'image/jpeg': '.jpg',
                    'image/jpg': '.jpg',
                    'image/gif': '.gif',
                    'image/bmp': '.bmp',
                    'image/tiff': '.tif',
                    'image/webp': '.webp',
                }
                return mapping.get(ct, '.png')
            except Exception:
                return '.png'

        def _overlay_bytes_to_original_format(png_bytes: bytes, desired_ext: str) -> bytes:
            """Convert PNG bytes (rendered overlay) to match the original image extension."""
            desired_ext = (desired_ext or '.png').lower()
            try:
                from PIL import Image
            except Exception:
                # If Pillow is not available, return PNG bytes as-is.
                return png_bytes

            fmt_map = {
                '.png': 'PNG',
                '.jpg': 'JPEG',
                '.jpeg': 'JPEG',
                '.bmp': 'BMP',
                '.tif': 'TIFF',
                '.tiff': 'TIFF',
                '.webp': 'WEBP',
                '.gif': 'PNG',  # avoid animated GIF issues
            }
            out_fmt = fmt_map.get(desired_ext, 'PNG')
            try:
                img = Image.open(io.BytesIO(png_bytes))
                if out_fmt in ('JPEG', 'BMP', 'TIFF'):
                    if img.mode not in ('RGB', 'L'):
                        img = img.convert('RGB')
                buf = io.BytesIO()
                img.save(buf, format=out_fmt)
                return buf.getvalue()
            except Exception:
                return png_bytes

        def _apply_translation_to_runs(paragraph, translated_text):
            """Apply translated paragraph text with minimal layout drift.

            Instead of distributing text across all runs (which often causes
            severe spacing/kerning artifacts), write into a single primary run
            and clear textual content of the others.
            Also strips forced-caps XML attributes (w:caps, w:smallCaps) so
            the translated text displays in its natural casing.
            """
            from docx.oxml.ns import qn as _qn
            runs = list(paragraph.runs)
            if not runs:
                paragraph.add_run(translated_text or "")
                return

            primary_idx = 0
            for i, run in enumerate(runs):
                if (run.text or '').strip():
                    primary_idx = i
                    break

            for i, run in enumerate(runs):
                try:
                    # Remove forced-caps attributes that turn lowercase into uppercase
                    rPr = run._element.find(_qn('w:rPr'))
                    if rPr is not None:
                        for caps_tag in ('w:caps', 'w:smallCaps'):
                            el = rPr.find(_qn(caps_tag))
                            if el is not None:
                                rPr.remove(el)
                    if i == primary_idx:
                        run.text = translated_text or ""
                    else:
                        # Only clear textual content; keep run object/style in place.
                        run.text = ""
                except Exception:
                    continue

        # ── Helper: insert a new paragraph right after `ref_para` in the document body ──
        def _insert_paragraph_after(ref_para, text, italic=True):
            """Insert a clean translated paragraph immediately after ref_para.

            Only copies alignment and indentation from the source paragraph.
            Uses a consistent style: italic, dark gray color, same font size.
            """
            from docx.oxml import OxmlElement
            from docx.oxml.ns import qn as _qn
            import copy as _copy

            new_p = OxmlElement('w:p')

            # Copy paragraph properties that influence layout/appearance.
            # NOTE: we avoid copying numbering (numPr) to prevent duplicating list bullets.
            try:
                pPr_src = ref_para._element.find(_qn('w:pPr'))
                if pPr_src is not None:
                    new_pPr = OxmlElement('w:pPr')
                    # Copy paragraph style (pStyle)
                    pStyle = pPr_src.find(_qn('w:pStyle'))
                    if pStyle is not None:
                        new_pPr.append(_copy.deepcopy(pStyle))
                    # Copy alignment (jc)
                    jc = pPr_src.find(_qn('w:jc'))
                    if jc is not None:
                        new_pPr.append(_copy.deepcopy(jc))
                    # Copy indentation (ind)
                    ind = pPr_src.find(_qn('w:ind'))
                    if ind is not None:
                        new_pPr.append(_copy.deepcopy(ind))
                    # Copy spacing (spacing)
                    spacing = pPr_src.find(_qn('w:spacing'))
                    if spacing is not None:
                        new_pPr.append(_copy.deepcopy(spacing))
                    # Copy tab stops (tabs)
                    tabs = pPr_src.find(_qn('w:tabs'))
                    if tabs is not None:
                        new_pPr.append(_copy.deepcopy(tabs))
                    new_p.insert(0, new_pPr)
            except Exception:
                pass

            run_el = OxmlElement('w:r')
            rPr = OxmlElement('w:rPr')

            # Inherit run formatting from the first non-empty run of source.
            # This preserves font name, size, and color so the translation matches the original.
            # Optionally apply italic, but do NOT force a different color.
            try:
                src_runs = ref_para._element.findall('.//' + _qn('w:r'))
                chosen_rPr = None
                # 1) Prefer a run that has explicit color formatting
                for r in src_runs:
                    try:
                        t = r.find(_qn('w:t'))
                        if t is None or not (t.text or '').strip():
                            continue
                        rpr = r.find(_qn('w:rPr'))
                        if rpr is None:
                            continue
                        if rpr.find(_qn('w:color')) is not None:
                            chosen_rPr = rpr
                            break
                    except Exception:
                        continue
                # 2) Fallback: first non-empty run
                if chosen_rPr is None:
                    for r in src_runs:
                        try:
                            t = r.find(_qn('w:t'))
                            if t is not None and (t.text or '').strip():
                                chosen_rPr = r.find(_qn('w:rPr'))
                                break
                        except Exception:
                            continue
                if chosen_rPr is None:
                    chosen_rPr = ref_para._element.find('.//' + _qn('w:rPr'))
                if chosen_rPr is not None:
                    rPr = _copy.deepcopy(chosen_rPr)
            except Exception:
                pass

            if italic:
                try:
                    rPr.append(OxmlElement('w:i'))
                except Exception:
                    pass

            run_el.insert(0, rPr)
            t_el = OxmlElement('w:t')
            t_el.set(_qn('xml:space'), 'preserve')
            t_el.text = text
            run_el.append(t_el)
            new_p.append(run_el)
            ref_para._element.addnext(new_p)
            return new_p

        def _append_translation_linebreak(paragraph, text, italic=True):
            """Append translation as a new line within the same paragraph.

            This preserves original paragraph/table/cell structure better than inserting
            a brand new paragraph node (which can alter spacing/indent and break form layout).
            """
            txt = (text or '').strip()
            if not txt:
                return False

            try:
                # Add a soft line break within the same paragraph.
                # IMPORTANT: create a NEW run for the break so we don't mutate the last
                # original run (which might be part of complex field/hyperlink runs).
                try:
                    paragraph.add_run('').add_break()
                except Exception:
                    try:
                        br = paragraph.add_run('')
                        br.add_break()
                    except Exception:
                        return False

                tr = paragraph.add_run(txt)
                if italic:
                    try:
                        tr.italic = True
                    except Exception:
                        pass
                # Match the visual cue used by _insert_paragraph_after: dark gray.
                try:
                    from docx.shared import RGBColor
                    tr.font.color.rgb = RGBColor(0x40, 0x40, 0x40)
                except Exception:
                    pass
                # Copy only font size from the first run, if available.
                try:
                    if paragraph.runs and paragraph.runs[0].font and paragraph.runs[0].font.size:
                        tr.font.size = paragraph.runs[0].font.size
                except Exception:
                    pass
                return True
            except Exception:
                return False

        leader_re = re.compile(r"(\.{5,}|_{4,}|-{4,})")

        def _cleanup_pdf2docx_spacing(paragraph):
            """Best-effort cleanup for PDF->DOCX artifacts.

            pdf2docx often uses many TABs/spaces to emulate absolute positioning.
            Some viewers render this as 'giãn chữ' (words spread far apart).

            Rules (conservative):
            - Never touch dot/underscore/dash leader lines.
            - Keep single-tab form field alignment (label \t value).
            - Collapse multi-tab headings (word\tword\tword) into normal spaces.
            - Collapse 3+ consecutive spaces into a single space for non-leader prose.
            """
            try:
                raw = paragraph.text or ''
            except Exception:
                return
            if not raw.strip():
                return
            if leader_re.search(raw):
                return

            tab_count = raw.count('\t')
            needs = False
            fixed = raw

            # Multi-tab lines: likely headings/labels broken into tab-separated words.
            if tab_count >= 2:
                fixed = re.sub(r"\t+", " ", fixed)
                fixed = re.sub(r" {2,}", " ", fixed)
                needs = (fixed != raw)
            # Many spaces: typical pdf2docx artifact
            elif re.search(r" {3,}", raw):
                fixed = re.sub(r" {3,}", " ", fixed)
                needs = (fixed != raw)

            # Keep single-tab alignment lines intact.
            if tab_count == 1 and not needs:
                return

            if needs and fixed.strip():
                _apply_translation_to_runs(paragraph, fixed)

        if cleanup_pdf_layout:
            try:
                paras = iter_all_paragraphs(doc)
                for p in paras:
                    _cleanup_pdf2docx_spacing(p)
            except Exception:
                pass

        def _translate_preserve_form_leaders(text):
            """Translate text while preserving dot/underscore/dash leader runs.

            This avoids breaking fill-in-the-blank lines produced by PDF->DOCX conversion
            or form templates.
            """
            raw = text or ""
            if not raw.strip():
                return raw
            if not leader_re.search(raw):
                return self._translate_with_retry(raw, target_lang)

            parts = leader_re.split(raw)
            out_parts = []
            for i, part in enumerate(parts):
                if i % 2 == 1:
                    out_parts.append(part)
                    continue

                seg = part or ""
                if not seg.strip():
                    out_parts.append(seg)
                    continue
                # Skip translating segments that are effectively just punctuation.
                if not re.search(r"[\w\u00C0-\u1EF9]", seg, flags=re.UNICODE):
                    out_parts.append(seg)
                    continue

                try:
                    out_parts.append(self._translate_with_retry(seg, target_lang))
                except ProviderRateLimitError:
                    raise
                except Exception:
                    if api_only:
                        raise
                    out_parts.append(seg)

            return "".join(out_parts)

        def translate_paragraph_runs(paragraph, idx=None, total=None):
            # Translate at paragraph-level for better quality, then map back to runs to keep formatting.
            runs = list(paragraph.runs)
            if not runs:
                return

            original_texts = [r.text or "" for r in runs]
            paragraph_text = "".join(original_texts)
            if not paragraph_text.strip():
                return

            try:
                translated_para = _translate_preserve_form_leaders(paragraph_text)
            except ProviderRateLimitError:
                # Critical: if provider is rate-limited stop the entire document job
                print("Provider rate limit detected during paragraph translation, raising to abort job.")
                raise
            except Exception as e:
                print(f"Translator failed for paragraph: {e}")
                if api_only:
                    raise
                translated_para = paragraph_text

            if bi_mode == 'inline':
                joined = self._join_inline_bilingual(paragraph_text, translated_para, bilingual_delimiter)
                _apply_translation_to_runs(paragraph, joined)
            elif bi_mode == 'newline':
                # Avoid duplicating lines that are effectively untranslatable (e.g., dot leaders).
                if (translated_para or '').strip() != paragraph_text.strip():
                    # Do NOT force italic; inherit formatting from the source paragraph/run.
                    new_p = _insert_paragraph_after(paragraph, translated_para, italic=False)
                    try:
                        _seen_para_elems.add(id(new_p))
                    except Exception:
                        pass
            else:
                _apply_translation_to_runs(paragraph, translated_para)
            if progress_callback and idx is not None and total is not None:
                progress_callback(10 + int((idx / total) * 70), f"Translating paragraph {idx+1}/{total}")

        # Avoid processing the same paragraph multiple times.
        # This can happen in tables with merged cells, where python-docx exposes the same
        # underlying paragraph through multiple cell references.
        _seen_para_elems = set()

        def _seen_or_mark(paragraph):
            try:
                key = id(paragraph._element)
            except Exception:
                key = id(paragraph)
            if key in _seen_para_elems:
                return True
            _seen_para_elems.add(key)
            return False

        # Body paragraphs
        paragraphs = [p for p in doc.paragraphs]
        # Translate in parallel using ThreadPoolExecutor
        from concurrent.futures import as_completed
        with self._executor_cls(max_workers=self.concurrency) as ex:
            futures = {}
            for idx, para in enumerate(paragraphs):
                if _seen_or_mark(para):
                    continue
                paragraph_text = "".join([r.text or "" for r in para.runs])
                if not paragraph_text.strip():
                    continue
                futures[ex.submit(_translate_preserve_form_leaders, paragraph_text)] = (idx, para, paragraph_text)

            total_work = max(1, len(futures))
            completed = 0
            for fut in as_completed(list(futures.keys())):
                try:
                    translated = fut.result()
                    idx, para, original_text = futures[fut]
                    if bi_mode == 'inline':
                        # Song ngữ liền kề: "Original  |  Translated"
                        joined = self._join_inline_bilingual(original_text, translated, bilingual_delimiter)
                        _apply_translation_to_runs(para, joined)
                    elif bi_mode == 'newline':
                        # Song ngữ xuống dòng: keep original, insert translated below
                        if (translated or '').strip() != (original_text or '').strip():
                            # Do NOT force italic; inherit formatting from the source paragraph/run.
                            new_p = _insert_paragraph_after(para, translated, italic=False)
                            try:
                                _seen_para_elems.add(id(new_p))
                            except Exception:
                                pass
                    else:
                        # Normal: replace with translation only
                        _apply_translation_to_runs(para, translated)
                except ProviderRateLimitError:
                    print("Provider rate limit detected during paragraph processing, aborting job.")
                    raise
                except Exception as e:
                    print(f"Paragraph translation failed: {e}")
                    if api_only:
                        raise
                completed += 1
                if progress_callback:
                    progress_callback(
                        10 + int((completed / total_work) * 70),
                        f"Translating paragraph {completed}/{total_work}",
                    )

        # Tables: translate cell-by-cell, preserve cell formatting
        for table in doc.tables:
            for r in range(len(table.rows)):
                for c in range(len(table.columns)):
                    cell = table.rows[r].cells[c]
                    for p_idx, p in enumerate(cell.paragraphs):
                        if _seen_or_mark(p):
                            continue
                        translate_paragraph_runs(p, p_idx, len(cell.paragraphs))

        # Headers and footers
        try:
            for section in doc.sections:
                header = section.header
                for p_idx, p in enumerate(header.paragraphs):
                    if _seen_or_mark(p):
                        continue
                    translate_paragraph_runs(p, p_idx, len(header.paragraphs))
                footer = section.footer
                for p_idx, p in enumerate(footer.paragraphs):
                    if _seen_or_mark(p):
                        continue
                    translate_paragraph_runs(p, p_idx, len(footer.paragraphs))
        except Exception:
            # ignore headers/footers issues
            pass

        # Optional: OCR embedded images in DOCX.
        # When mode includes 'image', we replace embedded image bytes.
        # When mode includes 'text', we export OCR+translated text to a sidecar .txt.
        if ocr_images and self.ocr_translate_overlay:
            if progress_callback:
                progress_callback(82, "OCR images in DOCX...")

            paras_to_scan = iter_all_paragraphs(doc)
            total_paras = len(paras_to_scan) or 1
            images_found = 0
            ocr_attempted = 0
            ocr_success = 0
            ocr_disabled = False

            # Collect replacements: zip internal path -> bytes
            image_replacements = {}
            # Collect OCR text export entries
            ocr_export_entries = []
            # Collect entries for text insertion into DOCX paragraphs (used by mode='both')
            text_insert_entries = []  # list of (paragraph, translated_text)
            # Collect in-place replacements for mode='text': (paragraph, rid, translated_text)
            text_replace_entries = []

            for idx, para in enumerate(paras_to_scan):
                if ocr_disabled:
                    break
                rids = paragraph_image_rids(para)
                if not rids:
                    continue
                for rid in rids:
                    img_part = rid_to_image_part(para, rid)
                    if not img_part:
                        continue
                    try:
                        blob = getattr(img_part, 'blob', None)
                        if not blob:
                            continue

                        images_found += 1

                        ext = image_part_ext(img_part)
                        tmp_name = f"docx_img_{uuid.uuid4().hex}{ext}"
                        tmp_path = os.path.join(self.upload_folder, tmp_name)
                        with open(tmp_path, 'wb') as f:
                            f.write(blob)

                        ocr_attempted += 1
                        try:
                            # Render translated text back onto the image.
                            # Returns (ocr_text, translated_text, png_bytes, recommended_mode)
                            ocr_text, translated_text, png_bytes, ai_recommended_mode = self.ocr_translate_overlay(
                                tmp_path,
                                'auto',
                                target_lang,
                                ocr_langs,
                            )
                        finally:
                            try:
                                os.remove(tmp_path)
                            except Exception:
                                pass

                        if not ocr_text or not str(ocr_text).strip():
                            continue

                        per_mode = mode
                        if mode == 'auto':
                            per_mode = _auto_pick_mode(ocr_text, translated_text, ai_recommended_mode)

                        print(f"  [IMAGE #{images_found}] OCR={len(ocr_text)}chars, AI_class={ai_recommended_mode}, per_mode={per_mode}")

                        # Export OCR text + translation for editing when requested
                        try:
                            if per_mode in ('text', 'both'):
                                partname = str(getattr(img_part, 'partname', '') or '').lstrip('/')
                                ocr_export_entries.append({
                                    'image': partname or '(embedded image)',
                                    'ocr_text': (ocr_text or '').strip(),
                                    'translated_text': (translated_text or '').strip(),
                                })
                                normalized_translated = _normalize_ocr_text_for_docx((translated_text or '').strip())

                                if per_mode == 'text' and mode == 'text':
                                    # EXPLICIT text mode: replace image with text in-place
                                    text_replace_entries.append((para, rid, normalized_translated))
                                else:
                                    # AUTO or 'both': keep original image + insert text below
                                    text_insert_entries.append((para, normalized_translated))
                        except Exception:
                            pass

                        if per_mode in ('image', 'both') and png_bytes and len(png_bytes) > 100:
                            # Replace the original embedded image bytes in the resulting docx.
                            try:
                                partname = str(getattr(img_part, 'partname', '') or '').lstrip('/')
                                if partname:
                                    new_bytes = _overlay_bytes_to_original_format(png_bytes, ext)
                                    image_replacements[partname] = new_bytes
                                    ocr_success += 1
                            except Exception:
                                continue
                    except Exception as e:
                        # If Tesseract is missing/unavailable AND AI Vision is also failing,
                        # stop trying further images to avoid repeated failures.
                        msg = str(e).lower()
                        if ('tesseract' in msg and ('not installed' in msg or 'path' in msg)) or \
                           ('ocr unavailable' in msg):
                            ocr_disabled = True
                            if progress_callback:
                                progress_callback(85, "Skipping DOCX image OCR (OCR not available)")
                            break
                        if 'ai provider' in msg and ('not configured' in msg or 'rate' in msg):
                            ocr_disabled = True
                            if progress_callback:
                                progress_callback(85, f"Skipping DOCX image OCR: {e}")
                            break
                        # Otherwise, continue best-effort.
                        print(f"DOCX image OCR error (continuing): {e}")
                        continue

                if progress_callback and (idx % 10 == 0):
                    # Keep progress moving while OCR runs
                    progress_callback(82 + int((idx / total_paras) * 10), f"OCR scanning {idx+1}/{total_paras}")

            if progress_callback:
                if images_found <= 0:
                    progress_callback(92, "DOCX OCR: no embedded images found")
                else:
                    progress_callback(
                        92,
                        f"DOCX OCR: found={images_found}, attempted={ocr_attempted}, replaced={ocr_success}",
                    )

            # Case 1: replace image by text at the same location
            try:
                if text_replace_entries:
                    for para, rid, trans_text in text_replace_entries:
                        if not trans_text or not trans_text.strip():
                            continue
                        try:
                            replace_image_with_text(para, rid, trans_text)
                        except Exception:
                            continue
            except Exception:
                pass

        # Insert translated text as paragraphs after image paragraphs in the DOCX
        try:
            if ocr_images and 'text_insert_entries' in locals() and text_insert_entries:
                from docx.oxml import OxmlElement
                from docx.oxml.ns import qn as _qn

                # Insert in REVERSE order so addnext doesn't shift positions
                for para, trans_text in reversed(text_insert_entries):
                    if not trans_text or not trans_text.strip():
                        continue
                    try:
                        # Split long text into multiple paragraphs for readability
                        text_paragraphs = [p.strip() for p in trans_text.split('\n') if p.strip()]
                        if not text_paragraphs:
                            text_paragraphs = [trans_text.strip()]

                        # Insert paragraphs in reverse so they appear in correct order after the image
                        for t_idx, t_para in enumerate(reversed(text_paragraphs)):
                            new_p = OxmlElement('w:p')

                            # Run with styled text
                            run = OxmlElement('w:r')

                            t_el = OxmlElement('w:t')
                            t_el.set(_qn('xml:space'), 'preserve')
                            t_el.text = t_para
                            run.append(t_el)
                            new_p.append(run)

                            para._element.addnext(new_p)
                    except Exception:
                        continue
        except Exception:
            pass

        # Ensure output filename has .docx extension
        output_filename = f"translated_{os.path.basename(file_path)}"
        if not output_filename.lower().endswith('.docx'):
            output_filename += '.docx'
        output_path = os.path.join(self.download_folder, output_filename)

        # Save and validate
        doc.save(output_path)

        # NOTE: Sidecar OCR text export removed — translated text is now
        # inserted directly into the DOCX as paragraphs after each image.

        # If we rendered overlays, patch the embedded image bytes inside the saved DOCX (zip container)
        try:
            if ocr_images and mode in ('image', 'both', 'auto') and 'image_replacements' in locals() and image_replacements:
                if progress_callback:
                    progress_callback(93, "Applying translated overlays to DOCX images...")
                tmp_out = output_path + ".tmp"
                with zipfile.ZipFile(output_path, 'r') as zin, zipfile.ZipFile(tmp_out, 'w') as zout:
                    for item in zin.infolist():
                        data = zin.read(item.filename)
                        repl = image_replacements.get(item.filename)
                        if repl is not None:
                            data = repl
                        zout.writestr(item, data)
                # Replace original
                try:
                    os.replace(tmp_out, output_path)
                except Exception:
                    # Best-effort fallback
                    try:
                        os.remove(output_path)
                    except Exception:
                        pass
                    os.rename(tmp_out, output_path)
        except Exception as e:
            # If patching fails, keep the DOCX as saved (text translations still present)
            if progress_callback:
                progress_callback(94, f"DOCX image overlay patch failed: {e}")


        # Validate produced DOCX — if invalid, write a plain text fallback to avoid corrupt file being returned
        try:
            # Try opening the saved file with python-docx to validate
            docx.Document(output_path)
        except Exception as e:
            # Create a text fallback containing translated paragraphs and table text
            if progress_callback:
                progress_callback(95, "DOCX validation failed, writing fallback text file")
            fallback_filename = output_filename
            if not fallback_filename.lower().endswith('.txt'):
                fallback_filename += '.txt'
            fallback_path = os.path.join(self.download_folder, fallback_filename)
            # Collect text from doc
            lines = []
            for p in doc.paragraphs:
                lines.append(p.text)
            for t in doc.tables:
                for row in t.rows:
                    for cell in row.cells:
                        lines.append(cell.text)
            with open(fallback_path, 'w', encoding='utf-8') as f:
                f.write("NOTE: DOCX creation failed on server. Showing plain text fallback below.\n\n")
                f.write('\n'.join(lines))
            output_path = fallback_path

        if progress_callback:
            progress_callback(100, "Completed")
        return output_path
    
    def _process_xlsx(self, file_path, target_lang, progress_callback=None):
        # Translate in-place to preserve styles, merged cells, formulas, column widths, etc.
        wb = openpyxl.load_workbook(file_path)

        # Count total cells (rough progress)
        total_cells = 0
        for sheet_name in wb.sheetnames:
            ws = wb[sheet_name]
            for _ in ws.iter_rows():
                total_cells += 1
        total_cells = total_cells or 1

        # Collect cells to translate
        to_translate = []
        for sheet_name in wb.sheetnames:
            ws = wb[sheet_name]
            for row in ws.iter_rows():
                for cell in row:
                    try:
                        is_formula = (cell.data_type == 'f') or (
                            isinstance(cell.value, str) and cell.value.startswith("=")
                        )
                    except Exception:
                        is_formula = False

                    if (not is_formula) and isinstance(cell.value, str) and cell.value.strip():
                        to_translate.append(cell)

        total = len(to_translate) or 1
        processed = 0
        # Translate cells in parallel
        with self._executor_cls(max_workers=self.concurrency) as ex:
            futures = {ex.submit(self._translate_with_retry, cell.value, target_lang): cell for cell in to_translate}
            for fut in futures:
                try:
                    translated = fut.result()
                    cell = futures[fut]
                    cell.value = translated
                except ProviderRateLimitError:
                    print("Provider rate limit detected during cell translation, aborting job.")
                    raise
                except Exception as e:
                    print(f"Cell translation failed: {e}")
                processed += 1
                if progress_callback:
                    progress_callback(10 + int((processed / total) * 80), f"Translating cells {processed}/{total}")

        # Ensure output filename has .xlsx extension
        output_filename = f"translated_{os.path.basename(file_path)}"
        if not output_filename.lower().endswith('.xlsx'):
            output_filename += '.xlsx'
        output_path = os.path.join(self.download_folder, output_filename)
        wb.save(output_path)
        if progress_callback:
            progress_callback(100, "Completed")
        return output_path
    
    def _process_txt(self, file_path, target_lang, progress_callback=None):
        with open(file_path, 'r', encoding='utf-8') as f:
            text = f.read()
        if progress_callback:
            progress_callback(25, "Translating text file...")

        # Split into paragraphs then chunk long paragraphs to avoid provider length limits
        paras = [p.strip() for p in re.split(r'\n{2,}', text) if p.strip()]
        max_chars = 3000
        chunks = []
        for p in paras:
            if len(p) <= max_chars:
                chunks.append(p)
            else:
                parts = re.split(r'(?<=[.!?])\s+', p)
                cur = ''
                for part in parts:
                    if len(cur) + len(part) + 1 <= max_chars:
                        cur = (cur + ' ' + part).strip() if cur else part
                    else:
                        if cur:
                            chunks.append(cur)
                        cur = part
                if cur:
                    # If still too long, slice it
                    while len(cur) > max_chars:
                        chunks.append(cur[:max_chars])
                        cur = cur[max_chars:]
                    if cur:
                        chunks.append(cur)

        # Translate chunks in parallel
        translated_parts = []
        with self._executor_cls(max_workers=self.concurrency) as ex:
            futures = [ex.submit(self._translate_with_retry, c, target_lang) for c in chunks]
            total = len(futures) or 1
            for i, fut in enumerate(futures, start=1):
                try:
                    res = fut.result()
                    translated_parts.append(res)
                except ProviderRateLimitError as e:
                    try:
                        print("Provider rate limit detected during text file translation, aborting job.")
                    except UnicodeEncodeError:
                        print("Provider rate limit detected during text file translation, aborting job.")
                    raise
                except Exception as e:
                    print(f"Chunk translation failed: {e}")
                    translated_parts.append('')
                if progress_callback:
                    progress_callback(25 + int((i / total) * 70), f"Translating chunk {i}/{total}")

        translated_text = '\n\n'.join(translated_parts)

        output_filename = f"translated_{os.path.basename(file_path)}"
        if not output_filename.lower().endswith('.txt'):
            output_filename += '.txt'
        output_path = os.path.join(self.download_folder, output_filename)
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(translated_text)
        if progress_callback:
            progress_callback(100, "Completed")
        return output_path
    
    def _sanitize_text(self, text: str) -> str:
        if not isinstance(text, str):
            try:
                text = str(text)
            except Exception:
                return ''
        # Normalize unicode and remove control characters that break XML/docx
        text = unicodedata.normalize('NFC', text)
        # Remove C0 control characters except tab/newline/carriage return
        text = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F]', '', text)
        # Collapse weird zero-width/formatting if any
        text = re.sub(r'[\u200B-\u200F\u2028\u2029]', ' ', text)
        return text

    def _translate_text(self, text, target_lang):
        # Use injected translator with retry/backoff
        if self.translator:
            out = self._translate_with_retry(text, target_lang)
            return self._sanitize_text(out)
        raise RuntimeError("Translator is not configured")