import os
import re
import unicodedata
import time
import uuid
import io
import zipfile
import shutil
import docx
import openpyxl
from werkzeug.utils import secure_filename
from docx.enum.text import WD_PARAGRAPH_ALIGNMENT


class ProviderRateLimitError(Exception):
    """Raised when the upstream AI provider indicates a hard rate limit (429 or insufficient credits)."""


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

    def process_document(self, file_path, target_lang, progress_callback=None, *, ocr_images=False, ocr_langs=None, ocr_mode=None, bilingual_mode=None, bilingual_delimiter=None):
        filename = os.path.basename(file_path)
        name, ext = os.path.splitext(filename)

        if ext.lower() == '.pdf':
            return self._process_pdf(file_path, target_lang, progress_callback)
        elif ext.lower() == '.docx':
            return self._process_docx(file_path, target_lang, progress_callback, ocr_images=ocr_images, ocr_langs=ocr_langs, ocr_mode=ocr_mode, bilingual_mode=bilingual_mode, bilingual_delimiter=bilingual_delimiter)
        elif ext.lower() == '.xlsx':
            return self._process_xlsx(file_path, target_lang, progress_callback)
        elif ext.lower() == '.txt':
            return self._process_txt(file_path, target_lang, progress_callback)
        else:
            raise ValueError("Unsupported file type")

    def _process_pdf(self, file_path, target_lang, progress_callback=None):
        """Translate a text-based PDF while preserving original layout.

        Strategy:
          - Extract text lines with bounding boxes.
          - Remove (redact) the original text for those boxes.
          - Insert translated text back into the same boxes via insert_textbox.

        Notes / limitations:
          - Works best for selectable-text PDFs.
          - If text length expands, font size is reduced to fit the original box.
          - Complex typography (mixed fonts within a line, curved/rotated text) may not be perfect.
        """

        try:
            import fitz  # PyMuPDF
        except Exception as e:
            raise RuntimeError("PyMuPDF is required for PDF translation. Install 'PyMuPDF'.") from e

        if not os.path.exists(file_path):
            raise FileNotFoundError(file_path)

        # Output path in downloads (consistent with other document outputs)
        base = os.path.splitext(os.path.basename(file_path))[0]
        out_name = f"{base}_translated_{uuid.uuid4().hex[:8]}.pdf"
        out_path = os.path.join(self.download_folder, out_name)

        # Simple cache to avoid repeated API calls for identical lines.
        cache = {}

        def _should_translate(s: str) -> bool:
            if s is None:
                return False
            if not str(s).strip():
                return False
            # Skip pure punctuation / symbols / numbers.
            core = str(s).strip()
            if re.fullmatch(r"[\d\W_]+", core, flags=re.UNICODE):
                return False
            return True

        def _translate_preserve_ws(s: str) -> str:
            src = "" if s is None else str(s)
            m = re.match(r"^(\s*)(.*?)(\s*)$", src, flags=re.DOTALL)
            lead, core, tail = (m.group(1), m.group(2), m.group(3)) if m else ("", src, "")
            if not _should_translate(core):
                return src
            if core in cache:
                return f"{lead}{cache[core]}{tail}"
            dst = self._translate_with_retry(core, target_lang)
            dst = "" if dst is None else str(dst)
            cache[core] = dst
            return f"{lead}{dst}{tail}"

        def _int_color_to_rgb01(color_int: int):
            try:
                c = int(color_int)
            except Exception:
                c = 0
            r = ((c >> 16) & 255) / 255.0
            g = ((c >> 8) & 255) / 255.0
            b = (c & 255) / 255.0
            return (r, g, b)

        def _choose_builtin_font(style_span: dict) -> str:
            """Pick a built-in PDF font name that approximates the original span.

            Why: Using the extracted font name often fails for insert_textbox() unless the font
            is one of the built-in Base-14 or explicitly registered. Built-ins reliably support
            bold/italic variants.
            """
            name = str((style_span or {}).get("font") or "").lower()
            flags = 0
            try:
                flags = int((style_span or {}).get("flags") or 0)
            except Exception:
                flags = 0

            is_bold = ("bold" in name) or bool(flags & 16)
            is_italic = ("italic" in name) or ("oblique" in name) or bool(flags & 2)

            # Choose family
            family = "Helvetica"
            if "times" in name or "tiro" in name or "serif" in name:
                family = "Times"
            elif "cour" in name or "mono" in name or "consol" in name:
                family = "Courier"
            elif "helv" in name or "arial" in name or "sans" in name:
                family = "Helvetica"

            # Use Base-14 font names which PyMuPDF accepts without external font files.
            if family == "Helvetica":
                if is_bold and is_italic:
                    return "Helvetica-BoldOblique"
                if is_bold:
                    return "Helvetica-Bold"
                if is_italic:
                    return "Helvetica-Oblique"
                return "Helvetica"

            if family == "Times":
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

            return "Helvetica"

        def _insert_text_fit(page, rect, text, *, fontname, fontsize, color):
            # insert_textbox() returns a non-negative value if text fits, negative if it doesn't.
            # We reduce font size until it fits (down to 4pt).
            fs0 = int(max(4, round(float(fontsize))))
            for fs in range(fs0, 3, -1):
                try:
                    rc = page.insert_textbox(
                        rect,
                        text,
                        fontsize=fs,
                        fontname=fontname,
                        color=color,
                        align=0,  # left
                    )
                    if rc >= 0:
                        return True
                except Exception:
                    # Fallback to built-in Helvetica if the font isn't usable.
                    try:
                        rc = page.insert_textbox(
                            rect,
                            text,
                            fontsize=fs,
                            fontname="Helvetica",
                            color=color,
                            align=0,
                        )
                        if rc >= 0:
                            return True
                    except Exception:
                        continue
            return False

        doc = fitz.open(file_path)
        try:
            total_pages = int(getattr(doc, "page_count", 0) or len(doc))
            if total_pages <= 0:
                raise RuntimeError("Empty PDF")

            for page_index in range(total_pages):
                page = doc.load_page(page_index)
                text_dict = page.get_text("dict")

                # Collect line items first to allow a deterministic processing order.
                items = []  # (line_rect, span_rects, src_text, style_span)
                for block in (text_dict.get("blocks") or []):
                    if block.get("type") != 0:
                        continue
                    for line in (block.get("lines") or []):
                        spans = line.get("spans") or []
                        if not spans:
                            continue
                        src = "".join((sp.get("text") or "") for sp in spans)
                        if not _should_translate(src):
                            continue
                        # Style: take the first non-empty span in this line.
                        style = None
                        for sp in spans:
                            if (sp.get("text") or "").strip():
                                style = sp
                                break
                        if not style:
                            continue
                        try:
                            line_rect = fitz.Rect(line.get("bbox"))
                        except Exception:
                            continue

                        # Ignore tiny boxes (often artifacts)
                        if line_rect.width < 2 or line_rect.height < 2:
                            continue

                        span_rects = []
                        for sp in spans:
                            if not (sp.get("text") or "").strip():
                                continue
                            try:
                                r = fitz.Rect(sp.get("bbox"))
                            except Exception:
                                continue
                            if r.width < 1 or r.height < 1:
                                continue
                            span_rects.append(r)

                        if not span_rects:
                            continue

                        items.append((line_rect, span_rects, src, style))

                if progress_callback:
                    base_pct = int(5 + (page_index / max(1, total_pages)) * 90)
                    progress_callback(base_pct, f"PDF: scanning page {page_index+1}/{total_pages}")

                if not items:
                    continue

                # IMPORTANT: Do NOT use PDF redactions here.
                # Redaction permanently removes ANY content under the rectangle (including table/grid lines),
                # which is the main reason for "bảng bị mất nét".
                # Instead, we overlay small white rectangles only over the original glyph span boxes.
                pad_x = 0.6  # points; keep tight to avoid covering nearby strokes
                pad_y = 0.2  # smaller Y padding helps preserve table/grid lines
                for _line_rect, span_rects, _src, _style in items:
                    for r in span_rects:
                        rr = fitz.Rect(r.x0 - pad_x, r.y0 - pad_y, r.x1 + pad_x, r.y1 + pad_y)
                        try:
                            # fill-only to avoid drawing a white stroke that can cut through thin lines
                            page.draw_rect(rr, color=None, fill=(1, 1, 1), overlay=True, width=0)
                        except Exception:
                            pass

                # Insert translated text back into the same boxes.
                for idx, (rect, _span_rects, src, style) in enumerate(items, start=1):
                    try:
                        dst = _translate_preserve_ws(src)
                    except ProviderRateLimitError:
                        # bubble up for the outer job handler
                        raise
                    except Exception:
                        # If one line fails, keep original box empty rather than failing the whole PDF.
                        dst = ""

                    if not str(dst).strip():
                        continue

                    fontname = _choose_builtin_font(style)
                    fontsize = style.get("size") or 10
                    color = _int_color_to_rgb01(style.get("color") or 0)

                    _insert_text_fit(page, rect, str(dst), fontname=fontname, fontsize=fontsize, color=color)

                    if progress_callback and idx % 40 == 0:
                        pct = int(5 + ((page_index + (idx / max(1, len(items)))) / max(1, total_pages)) * 90)
                        progress_callback(min(98, pct), f"PDF: translating page {page_index+1}/{total_pages} ({idx}/{len(items)} lines)")

            doc.save(out_path, garbage=4, deflate=True)
        finally:
            try:
                doc.close()
            except Exception:
                pass

        if progress_callback:
            progress_callback(100, "PDF: completed")
        return out_path

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

    def _process_docx(self, file_path, target_lang, progress_callback=None, *, ocr_images=False, ocr_langs=None, ocr_mode=None, bilingual_mode=None, bilingual_delimiter=None):
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

        def _translate_preserve_form_leaders(text):
            """Translate text while preserving dot/underscore/dash leader runs.

            This avoids breaking fill-in-the-blank lines in form-like DOCX templates.
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

            # Fallback coverage: some DOCX images are floating/textbox/header objects
            # that python-docx doesn't expose via paragraph runs. For image/both/auto
            # modes, we can still translate by replacing the image parts at the package level.
            try:
                if not ocr_disabled and mode in ('image', 'both', 'auto'):
                    # Collect all image parts from the package
                    pkg = getattr(getattr(doc, 'part', None), 'package', None)
                    parts = list(getattr(pkg, 'parts', []) or [])

                    extra_attempted = 0
                    extra_replaced = 0
                    for part in parts:
                        try:
                            ct = str(getattr(part, 'content_type', '') or '').lower()
                            if not ct.startswith('image/'):
                                continue
                            partname = str(getattr(part, 'partname', '') or '').lstrip('/')
                            if not partname:
                                continue
                            # Skip already processed via paragraph scan
                            if partname in image_replacements:
                                continue
                            blob = getattr(part, 'blob', None)
                            if not blob:
                                continue

                            ext = image_part_ext(part)
                            tmp_name = f"docx_img_pkg_{uuid.uuid4().hex}{ext}"
                            tmp_path = os.path.join(self.upload_folder, tmp_name)
                            with open(tmp_path, 'wb') as f:
                                f.write(blob)

                            extra_attempted += 1
                            try:
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

                            # For package-level scan we only apply overlay replacements.
                            if per_mode in ('image', 'both') and png_bytes and len(png_bytes) > 100:
                                try:
                                    new_bytes = _overlay_bytes_to_original_format(png_bytes, ext)
                                    image_replacements[partname] = new_bytes
                                    extra_replaced += 1
                                except Exception:
                                    continue
                        except Exception as e:
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
                            continue

                    if progress_callback and (extra_attempted or extra_replaced):
                        progress_callback(
                            92,
                            f"DOCX OCR (package scan): attempted={extra_attempted}, replaced={extra_replaced}",
                        )
            except Exception:
                pass

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
