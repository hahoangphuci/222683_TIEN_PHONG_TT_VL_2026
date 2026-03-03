from __future__ import annotations

import os
import re
import uuid
from typing import Callable, Dict, Optional

try:
    import pymupdf as fitz  # PyMuPDF >= 1.24
except Exception:  # pragma: no cover
    import fitz

from .extractor import (
    extract_pdf_ocr_blocks,
    extract_pdf_text_blocks,
)
from .renderer import DocxRenderer, PdfRenderer
from .translator import OpenRouterTranslator, translate_with_retry


class DocumentPipelineV2:
    """Strict pipeline that matches the spec (block-by-block, coordinate preserving)."""

    def __init__(self):
        self.translator = OpenRouterTranslator()

        # Minimal ISO-code to language-name mapping for better model compliance.
        # The REST API typically passes codes like "en", "vi".
        self._lang_code_to_name = {
            "en": "English",
            "en-us": "English (US)",
            "en-gb": "English (UK)",
            "vi": "Vietnamese",
            "ja": "Japanese",
            "ko": "Korean",
            "zh": "Chinese (Simplified)",
            "zh-cn": "Chinese (Simplified)",
            "zh-tw": "Chinese (Traditional)",
            "fr": "French",
            "de": "German",
            "es": "Spanish",
            "it": "Italian",
            "ru": "Russian",
            "th": "Thai",
        }

    def _normalize_target_lang(self, target_lang: str) -> str:
        t = (target_lang or "").strip()
        if not t:
            return "English"
        key = t.lower()
        return self._lang_code_to_name.get(key, t)

    def _translate_one(self, text: str, *, target_lang: str) -> str:
        tgt = self._normalize_target_lang(target_lang)
        return self.translator.translate_text(text, source_lang="auto", target_lang=tgt)

    def process(
        self,
        file_path: str,
        target_lang: str,
        *,
        progress_cb: Optional[Callable[[int, str], None]] = None,
        ocr_langs: Optional[str] = None,
        bilingual_mode: Optional[str] = None,
        bilingual_delimiter: Optional[str] = None,
    ) -> str:
        ext = os.path.splitext(file_path)[1].lower()
        if ext == ".pdf":
            return self._process_pdf(
                file_path,
                target_lang,
                progress_cb=progress_cb,
                ocr_langs=ocr_langs,
                bilingual_mode=bilingual_mode,
                bilingual_delimiter=bilingual_delimiter,
            )
        if ext == ".docx":
            return self._process_docx(
                file_path,
                target_lang,
                progress_cb=progress_cb,
                bilingual_mode=bilingual_mode,
                bilingual_delimiter=bilingual_delimiter,
            )
        raise ValueError("Unsupported file type for pipeline v2")

    @staticmethod
    def _normalize_bilingual_delimiter(delimiter: Optional[str]) -> str:
        d = (delimiter or "").strip()
        if not d:
            return "|"
        return d[:10]

    @staticmethod
    def _split_leading_marker(text: str) -> tuple[str, str]:
        """Split common leading markers (numbering/bullets) from a line.

        Examples:
          "1. Text" -> ("1. ", "Text")
          "a) Text" -> ("a) ", "Text")
          "- Text"  -> ("- ", "Text")
          "• Text"  -> ("• ", "Text")
        """
        if text is None:
            return ("", "")
        s = str(text)
        if not s:
            return ("", "")

        # Keep original leading whitespace inside the prefix.
        m = re.match(
            r"^(?P<prefix>\s*(?:\(?\d{1,3}\)?[\.)]|[A-Za-z][\.)]|[-–—•\u2022])\s+)(?P<body>.*)$",
            s,
        )
        if m:
            return (m.group("prefix"), m.group("body"))
        return ("", s)

    @staticmethod
    def _strip_repeated_prefix(prefix: str, translated: str) -> str:
        """If the model repeats the leading marker, drop it from the translation."""
        if not prefix:
            return translated
        if translated is None:
            return ""
        p = str(prefix).strip()
        t = str(translated)
        t_strip = t.lstrip()
        if p and t_strip.lower().startswith(p.lower()):
            # Remove the prefix once and any following whitespace.
            out = t_strip[len(p) :]
            return out.lstrip()
        return t

    def _translate_docx_form_like_text(self, text: str, base_translate: Callable[[str], str]) -> str:
        """Translate DOCX paragraph content while preserving form structure.

        - Preserves tabs (\t) used for columns / dot leaders.
        - Translates label parts only for common label/value patterns.
        - Keeps values (names, IDs, numbers) unchanged.
        """
        if text is None:
            return ""
        s = str(text)
        if not s.strip():
            return ""

        # Split by tabs but keep them so column layout remains.
        parts = re.split(r"(\t+)", s)
        out: list[str] = []

        for part in parts:
            if not part:
                continue
            if part.startswith("\t"):
                out.append(part)
                continue

            leading_ws = re.match(r"^\s*", part).group(0)
            trailing_ws = re.search(r"\s*$", part).group(0)
            core = part.strip()

            if not core:
                out.append(part)
                continue

            # Skip obvious non-language tokens.
            if re.search(r"https?://|www\.|\S+@\S+", core, flags=re.IGNORECASE):
                out.append(part)
                continue
            if not any(ch.isalpha() for ch in core):
                out.append(part)
                continue

            # If core is just a name, do not translate.
            if self._looks_like_person_name(core):
                out.append(part)
                continue

            # Label/value patterns: translate label only.
            lv = self._split_label_value(core)
            if lv is not None:
                label, sep, val = lv
                label_core = label.strip()
                if label_core and not self._looks_like_person_name(label_core):
                    dst_label = base_translate(label_core)
                    dst_label_core = (str(dst_label) if dst_label is not None else "").strip()
                    if dst_label_core:
                        out.append(f"{leading_ws}{dst_label_core}{sep}{val}{trailing_ws}")
                        continue
                out.append(part)
                continue

            # Label-only ending with ':'
            m = re.match(r"^(?P<label>[^\n]{2,120}?)(?P<sep>\s*[:：]\s*)$", core)
            if m:
                label_core = (m.group("label") or "").strip()
                sep = m.group("sep") or ":"
                if label_core and not self._looks_like_person_name(label_core):
                    dst_label = base_translate(label_core)
                    dst_label_core = (str(dst_label) if dst_label is not None else "").strip()
                    if dst_label_core:
                        out.append(f"{leading_ws}{dst_label_core}{sep.strip()}{trailing_ws}")
                        continue
                out.append(part)
                continue

            # Fallback: translate the whole chunk.
            dst = base_translate(core)
            dst_core = (str(dst) if dst is not None else "").strip()
            if dst_core:
                out.append(f"{leading_ws}{dst_core}{trailing_ws}")
            else:
                out.append(part)

        return "".join(out)

    def _apply_bilingual(
        self,
        src: str,
        dst: str,
        *,
        bilingual_mode: Optional[str],
        bilingual_delimiter: Optional[str],
        is_pdf: bool,
    ) -> str:
        mode = (bilingual_mode or "none").strip().lower()
        if mode not in ("none", "inline", "newline"):
            mode = "none"

        if mode == "newline" and is_pdf:
            # Default: don't do newline bilingual in PDF to avoid overlaps.
            allow_pdf_newline = (os.getenv("PDF_ALLOW_NEWLINE_MODE", "0").strip().lower() in ("1", "true", "yes", "on"))
            if not allow_pdf_newline:
                mode = "none"

        if mode == "none":
            return dst

        s = src or ""
        t = dst or ""
        if not t.strip():
            return s
        if not s.strip():
            return t

        # Avoid duplicated numbering/bullets if the model repeats them.
        prefix, _body = self._split_leading_marker(s)
        if prefix:
            t = self._strip_repeated_prefix(prefix, t)

        if mode == "newline":
            if is_pdf:
                # In PDF we want to preserve original layout; caller will skip redaction.
                # Prefix with newline so translation starts on the next line.
                return f"\n{t}"
            return f"{s}\n{t}"

        d = self._normalize_bilingual_delimiter(bilingual_delimiter)
        return f"{s} {d} {t}"

    @staticmethod
    def _split_sentences_with_separators(text: str) -> list[tuple[str, str]]:
        """Return list of (sentence, separator_after) preserving order.

        Separator includes trailing whitespace/newlines after a sentence.
        """

        if text is None:
            return []
        s = str(text)
        if not s:
            return []

        # Split on sentence-ending punctuation followed by whitespace or end.
        # Keep punctuation with the sentence.
        pattern = re.compile(r"(.+?[\.!\?…。！？])(?=\s+|$)", flags=re.DOTALL)
        out: list[tuple[str, str]] = []
        idx = 0

        for m in pattern.finditer(s):
            sent = m.group(1)
            start, end = m.span(1)
            if start > idx:
                # Leading fragment without terminal punctuation.
                frag = s[idx:start]
                if frag:
                    out.append((frag, ""))
            # Capture trailing whitespace after the sentence.
            sep_start = end
            sep_end = sep_start
            while sep_end < len(s) and s[sep_end].isspace():
                sep_end += 1
            sep = s[sep_start:sep_end]
            out.append((sent, sep))
            idx = sep_end

        if idx < len(s):
            out.append((s[idx:], ""))
        return out

    @staticmethod
    def _should_pair_sentence(core: str) -> bool:
        """Return True if we should show bilingual pair for this unit.

        Goal: avoid visual clutter for fragments like IDs, emails, URLs, short labels.
        """
        if core is None:
            return False
        s = str(core).strip()
        if not s:
            return False

        # Skip obvious non-natural-language tokens
        if re.search(r"https?://|www\.|\S+@\S+", s, flags=re.IGNORECASE):
            return False
        if re.fullmatch(r"[\d\W_]+", s):
            return False

        # If it's mostly digits/symbols, don't pair.
        letters = sum(1 for ch in s if ch.isalpha())
        digits = sum(1 for ch in s if ch.isdigit())
        if letters == 0:
            return False
        if digits > 0 and digits >= letters:
            return False

        # Very short fragments (single word / label) become noisy when paired.
        words = [w for w in re.split(r"\s+", s) if w]
        if len(words) < 2 and len(s) < 18:
            return False

        # Prefer pairing for sentence-like endings.
        if s[-1] in ".!?…。！？":
            return True
        # Or if long enough text with letters.
        return len(s) >= 28

    @staticmethod
    def _should_pair_paren_unit(core: str) -> bool:
        """Less aggressive pairing for paren mode to reduce clutter.

        Pairs for natural-language labels/sentences, skips for IDs/emails/URLs.
        """
        if core is None:
            return False
        s = str(core).strip()
        if not s:
            return False

        if re.search(r"https?://|www\.|\S+@\S+", s, flags=re.IGNORECASE):
            return False
        if re.fullmatch(r"[\d\W_]+", s):
            return False

        letters = sum(1 for ch in s if ch.isalpha())
        if letters == 0:
            return False

        # Labels usually end with ':' or are short phrases.
        if s.endswith(":"):
            return True

        # Pair if it looks like a sentence or a short label.
        words = [w for w in re.split(r"\s+", s) if w]
        if s[-1] in ".!?…。！？":
            return True
        if len(words) <= 6 and len(s) <= 40:
            return True
        return len(s) >= 60

    @staticmethod
    def _looks_like_person_name(text: str) -> bool:
        """Heuristic to detect person names (avoid translating them).

        Examples: "Hà Hoàng Phúc", "Nguyen Van A".
        """
        if text is None:
            return False
        s = str(text).strip()
        if not s:
            return False
        if any(ch.isdigit() for ch in s):
            return False
        if any(ch in s for ch in ("@", "://", "www.")):
            return False

        # 2-5 words, mostly Title Case, no sentence punctuation
        if any(p in s for p in (".", "!", "?", ",", ";", ":")):
            return False
        words = [w for w in re.split(r"\s+", s) if w]
        if not (2 <= len(words) <= 5):
            return False
        titled = sum(1 for w in words if w[:1].isupper())
        if titled >= max(2, len(words) - 1):
            return True
        return False

    @staticmethod
    def _split_label_value(core: str) -> tuple[str, str, str] | None:
        """Split common label/value patterns.

        Returns (label, sep, value) or None.
        """
        s = str(core)
        # Colon-separated labels
        m = re.match(r"^(?P<label>[^\n]{2,80}?)(?P<sep>\s*[:：]\s*)(?P<val>.+)$", s)
        if m:
            return (m.group("label"), m.group("sep"), m.group("val"))
        # Dotted leader labels: "Label .... value"
        m = re.match(r"^(?P<label>[^\n]{2,80}?)(?P<sep>\s*\.{4,}\s*)(?P<val>.+)$", s)
        if m:
            return (m.group("label"), m.group("sep"), m.group("val"))
        return None

    def _bilingual_sentence_inline_text(
        self,
        src_text: str,
        *,
        translate_sentence: Callable[[str], str],
        delimiter: Optional[str],
    ) -> str:
        d = self._normalize_bilingual_delimiter(delimiter)
        parts = self._split_sentences_with_separators(src_text)
        if not parts:
            return src_text or ""

        out_chunks: list[str] = []
        for sent, sep in parts:
            if sent is None:
                continue
            raw = str(sent)
            if not raw.strip():
                out_chunks.append(raw + (sep or ""))
                continue

            # Only translate if it contains letters; otherwise keep as-is.
            core = raw.strip()
            if not any(ch.isalpha() for ch in core):
                out_chunks.append(raw + (sep or ""))
                continue

            dst = translate_sentence(core)
            if self._should_pair_sentence(core):
                out_chunks.append(f"{core} {d} {str(dst).strip()}{sep or ''}")
            else:
                out_chunks.append(f"{str(dst).strip()}{sep or ''}")

        return "".join(out_chunks)

    def _bilingual_sentence_newline_text(
        self,
        src_text: str,
        *,
        translate_sentence: Callable[[str], str],
    ) -> str:
        parts = self._split_sentences_with_separators(src_text)
        if not parts:
            return src_text or ""

        def _norm_sep(sep: str) -> str:
            # Preserve existing newlines; otherwise ensure a newline between sentence blocks.
            if sep and ("\n" in sep or "\r" in sep):
                return sep
            return "\n" if sep is not None else ""

        out_chunks: list[str] = []
        for sent, sep in parts:
            if sent is None:
                continue
            raw = str(sent)
            if not raw.strip():
                out_chunks.append(raw + (sep or ""))
                continue

            core = raw.strip()
            if not any(ch.isalpha() for ch in core):
                out_chunks.append(raw + (sep or ""))
                continue

            dst = translate_sentence(core)
            if self._should_pair_sentence(core):
                out_chunks.append(f"{core}\n{str(dst).strip()}{_norm_sep(sep or '')}")
            else:
                out_chunks.append(f"{str(dst).strip()}{_norm_sep(sep or '')}")

        return "".join(out_chunks)

    def _process_pdf(
        self,
        file_path: str,
        target_lang: str,
        *,
        progress_cb: Optional[Callable[[int, str], None]] = None,
        ocr_langs: Optional[str] = None,
        bilingual_mode: Optional[str] = None,
        bilingual_delimiter: Optional[str] = None,
    ) -> str:
        doc = fitz.open(file_path)
        total_pages = len(doc)
        renderer = PdfRenderer()

        # OCR config
        try:
            dpi = int(os.getenv("PDF_OCR_DPI", "300"))
        except Exception:
            dpi = 300
        gran = (os.getenv("PDF_OCR_GRANULARITY", "line") or "line").strip().lower()
        tcmd = (os.getenv("TESSERACT_CMD") or "").strip() or None

        # Per-document cache to reduce repeat calls (headers, page numbers, etc.)
        cache: Dict[str, str] = {}

        def _translate_cached(src: str) -> str:
            if src in cache:
                return cache[src]
            out = translate_with_retry(lambda t: self._translate_one(t, target_lang=target_lang), src, max_attempts=3)
            cache[src] = out
            return out

        mode = (bilingual_mode or 'none').strip().lower()
        allow_pdf_newline = (os.getenv("PDF_ALLOW_NEWLINE_MODE", "0").strip().lower() in ("1", "true", "yes", "on"))

        for page_index in range(total_pages):
            if progress_cb:
                pct = int(2 + 90 * (page_index / max(1, total_pages)))
                progress_cb(pct, f"PDF: processing page {page_index + 1}/{total_pages}")

            text_blocks = list(extract_pdf_text_blocks(doc, page_index=page_index))
            if text_blocks:
                blocks = text_blocks
                is_text_layer = True
            else:
                blocks = list(
                    extract_pdf_ocr_blocks(
                        doc,
                        page_index=page_index,
                        dpi=dpi,
                        ocr_langs=ocr_langs,
                        granularity=gran,
                        tesseract_cmd=tcmd,
                    )
                )
                is_text_layer = False

            translated: Dict[int, str] = {}
            for b in blocks:
                if mode == 'paren':
                    src = b.text or ""
                    core = src.strip()
                    if not core:
                        translated[b.block_index] = src
                        continue
                    # If this is likely a person's name, keep as-is.
                    if self._looks_like_person_name(core):
                        translated[b.block_index] = src
                        continue

                    # Label/value pattern: translate label only; keep value unchanged.
                    lv = self._split_label_value(core)
                    if lv is not None:
                        label, sep, val = lv
                        label_core = label.strip()
                        val_core = val.strip()
                        if "(" in label_core and ")" in label_core:
                            translated[b.block_index] = src
                        else:
                            dst_label = _translate_cached(label_core)
                            translated[b.block_index] = f"{label}{' ' if label.endswith(' ') else ''}({str(dst_label).strip()}){sep}{val}"
                        continue

                    dst = _translate_cached(core)
                    if self._should_pair_paren_unit(core):
                        translated[b.block_index] = f"{src} ({str(dst).strip()})"
                    else:
                        translated[b.block_index] = str(dst)
                    continue
                if mode == 'sentence':
                    translated[b.block_index] = self._bilingual_sentence_inline_text(
                        b.text,
                        translate_sentence=_translate_cached,
                        delimiter='|',
                    )
                elif mode == 'sentence_newline':
                    translated[b.block_index] = self._bilingual_sentence_newline_text(
                        b.text,
                        translate_sentence=_translate_cached,
                    )
                else:
                    dst = _translate_cached(b.text)
                    translated[b.block_index] = self._apply_bilingual(
                        b.text,
                        dst,
                        bilingual_mode=bilingual_mode,
                        bilingual_delimiter=bilingual_delimiter,
                        is_pdf=True,
                    )

            renderer.render(
                doc,
                page_index=page_index,
                blocks=blocks,
                translated=translated,
                is_text_layer=(is_text_layer and not (mode == 'newline' and allow_pdf_newline)),
            )

        backend_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
        out_dir = os.path.join(backend_dir, "downloads")
        os.makedirs(out_dir, exist_ok=True)
        base = os.path.splitext(os.path.basename(file_path))[0]
        out_path = os.path.join(out_dir, f"translated_{base}_{uuid.uuid4().hex[:8]}.pdf")
        doc.save(out_path)
        doc.close()
        if progress_cb:
            progress_cb(100, "PDF: completed")
        return out_path

    def _process_docx(
        self,
        file_path: str,
        target_lang: str,
        *,
        progress_cb: Optional[Callable[[int, str], None]] = None,
        bilingual_mode: Optional[str] = None,
        bilingual_delimiter: Optional[str] = None,
    ) -> str:
        backend_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
        out_dir = os.path.join(backend_dir, "downloads")
        os.makedirs(out_dir, exist_ok=True)
        base = os.path.splitext(os.path.basename(file_path))[0]
        out_path = os.path.join(out_dir, f"translated_{base}_{uuid.uuid4().hex[:8]}.docx")

        def _translate_cached_factory():
            cache: Dict[str, str] = {}

            def _fn(src: str) -> str:
                if src in cache:
                    return cache[src]
                out = translate_with_retry(lambda t: self._translate_one(t, target_lang=target_lang), src, max_attempts=3)
                cache[src] = out
                return out

            return _fn

        base_translate = _translate_cached_factory()

        mode = (bilingual_mode or 'none').strip().lower()
        if mode == 'inline':
            # Render inline bilingual at paragraph-scope to avoid per-run separator clutter.
            def _split(src_para: str) -> tuple[str, str]:
                return self._split_leading_marker(src_para)

            # We translate only the body; prefix stays on the VN side and won't be repeated.
            def _translate_body(body: str) -> str:
                b = (body or "").strip()
                if not b:
                    return ""
                # Prefer form-aware translation to keep columns/leaders stable.
                return self._translate_docx_form_like_text(body, base_translate)

            renderer = DocxRenderer(_translate_body)
            return renderer.translate_docx_bilingual_inline_paragraph(
                file_path,
                out_path,
                delimiter='|',
                progress_cb=progress_cb,
                split_prefix=_split,
            )
        if mode == 'newline':
            # Render newline bilingual at paragraph-scope to avoid weird per-run breaks.
            renderer = DocxRenderer(lambda src_para: self._translate_docx_form_like_text(src_para, base_translate))
            return renderer.translate_docx_bilingual_newline_paragraph(
                file_path,
                out_path,
                progress_cb=progress_cb,
            )

        if mode == 'sentence':
            renderer = DocxRenderer(lambda one_sentence: base_translate(one_sentence))
            return renderer.translate_docx_bilingual_sentence_inline_paragraph(
                file_path,
                out_path,
                delimiter='|',
                progress_cb=progress_cb,
                sentence_splitter=self._split_sentences_with_separators,
                should_pair=self._should_pair_sentence,
            )

        if mode == 'sentence_newline':
            renderer = DocxRenderer(lambda one_sentence: base_translate(one_sentence))
            return renderer.translate_docx_bilingual_sentence_newline_paragraph(
                file_path,
                out_path,
                progress_cb=progress_cb,
                sentence_splitter=self._split_sentences_with_separators,
                should_pair=self._should_pair_sentence,
            )

        if mode == 'paren':
            renderer = DocxRenderer(lambda src_para: base_translate(src_para))
            return renderer.translate_docx_bilingual_paren_paragraph(
                file_path,
                out_path,
                progress_cb=progress_cb,
                should_pair=self._should_pair_paren_unit,
                formatter=lambda core: self._format_paren_docx(core, base_translate),
            )

        def _translate_run_text(src: str) -> str:
            dst = base_translate(src)
            return self._apply_bilingual(
                src,
                dst,
                bilingual_mode=bilingual_mode,
                bilingual_delimiter=bilingual_delimiter,
                is_pdf=False,
            )

        renderer = DocxRenderer(_translate_run_text)
        return renderer.translate_docx(file_path, out_path, progress_cb=progress_cb)

    def _format_paren_docx(self, core: str, base_translate: Callable[[str], str]) -> tuple[str, str]:
        """Return ('append'|'replace', payload) for DOCX paren mode."""
        s = (core or "").strip()
        if not s:
            return ("replace", core or "")

        if self._looks_like_person_name(s):
            return ("replace", core)

        lv = self._split_label_value(s)
        if lv is not None:
            label, sep, val = lv
            label_core = label.strip()
            if "(" in label_core and ")" in label_core:
                return ("replace", core)
            dst_label = base_translate(label_core)
            return ("replace", f"{label} ({str(dst_label).strip()}){sep}{val}")

        # Label-only line ending with ':' (no value). Keep ':' at the end: "Label (EN):"
        m = re.match(r"^(?P<label>[^\n]{2,120}?)(?P<sep>\s*[:：]\s*)$", s)
        if m:
            label = m.group("label")
            sep = m.group("sep")
            label_core = label.strip()
            if label_core and "(" not in label_core and ")" not in label_core and not self._looks_like_person_name(label_core):
                dst_label = base_translate(label_core)
                dst_label_core = (str(dst_label) if dst_label is not None else "").strip()
                if dst_label_core:
                    return ("replace", f"{label} ({dst_label_core}){sep.strip()}")

        dst = base_translate(s)
        if self._should_pair_paren_unit(s):
            return ("append", str(dst).strip())
        return ("replace", str(dst).strip())
