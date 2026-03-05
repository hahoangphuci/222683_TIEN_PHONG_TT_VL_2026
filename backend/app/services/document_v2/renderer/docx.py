from __future__ import annotations

from typing import Callable, Optional

import docx

from ..extractor.docx_runs import (
    iter_docx_header_footer_paragraphs,
    iter_docx_header_footer_runs,
    iter_docx_paragraphs,
    iter_docx_text_runs,
)


class DocxRenderer:
    """In-place DOCX translation by replacing run.text only.

    Guarantees:
      - Does NOT delete paragraphs.
      - Does NOT rebuild the document.
      - Preserves run-level formatting (font, size, bold/italic, etc.) because we only change text.
      - Table structure and header/footer are preserved.
    """

    def __init__(self, translate_fn: Callable[[str], str]):
        self.translate_fn = translate_fn

    def translate_docx(
        self,
        input_path: str,
        output_path: str,
        *,
        progress_cb: Optional[Callable[[int, str], None]] = None,
    ) -> str:
        doc = docx.Document(input_path)

        done = 0
        # Stream body runs first (document order), then headers/footers.
        for run in iter_docx_text_runs(doc):
            src = run.text
            if src is None or not str(src).strip():
                done += 1
                continue
            dst = self.translate_fn(str(src))
            run.text = dst
            done += 1

            if progress_cb and done % 80 == 0:
                # We don't know total without pre-scanning; report a coarse progress.
                progress_cb(min(95, 5 + done // 80), f"DOCX: translated {done} runs")

        for run in iter_docx_header_footer_runs(doc):
            src = run.text
            if src is None or not str(src).strip():
                done += 1
                continue
            dst = self.translate_fn(str(src))
            run.text = dst
            done += 1
            if progress_cb and done % 80 == 0:
                progress_cb(min(98, 6 + done // 80), f"DOCX: translated {done} runs (incl. header/footer)")

        doc.save(output_path)
        if progress_cb:
            progress_cb(100, "DOCX: completed")
        return output_path

    def translate_docx_bilingual_newline_paragraph(
        self,
        input_path: str,
        output_path: str,
        *,
        progress_cb: Optional[Callable[[int, str], None]] = None,
    ) -> str:
        """Bilingual mode (newline) rendered per paragraph.

        Why: DOCX content often splits one sentence into many runs; doing "src\ntrg"
        per-run creates many unexpected line breaks.
        Strategy:
          - Keep source paragraph runs unchanged.
          - Translate the paragraph text once.
          - Append "\n<translation>" to the last non-empty run in that paragraph.
        """

        doc = docx.Document(input_path)
        done = 0

        def _process_paragraph(p):
            nonlocal done
            runs = list(p.runs)
            if not runs:
                return
            src_para = "".join((r.text or "") for r in runs)
            if not src_para.strip():
                return
            dst_para = self.translate_fn(src_para)
            if not str(dst_para).strip():
                return

            # Find last run that has some text to attach translation.
            last = None
            for r in reversed(runs):
                if (r.text or "").strip():
                    last = r
                    break
            if last is None:
                last = runs[-1]

            last.text = f"{last.text or ''}\n{dst_para}"
            done += 1
            if progress_cb and done % 30 == 0:
                progress_cb(min(98, 5 + done // 10), f"DOCX: bilingual-newline {done} paragraphs")

        for p in iter_docx_paragraphs(doc):
            _process_paragraph(p)

        for p in iter_docx_header_footer_paragraphs(doc):
            _process_paragraph(p)

        doc.save(output_path)
        if progress_cb:
            progress_cb(100, "DOCX: completed (bilingual newline by paragraph)")
        return output_path

    def translate_docx_bilingual_sentence_inline_paragraph(
        self,
        input_path: str,
        output_path: str,
        *,
        delimiter: str = "|",
        progress_cb: Optional[Callable[[int, str], None]] = None,
        sentence_splitter: Optional[Callable[[str], list[tuple[str, str]]]] = None,
        should_pair: Optional[Callable[[str], bool]] = None,
    ) -> str:
        """Bilingual mode (sentence inline) rendered per paragraph.

        Output format per sentence: "<src> <delimiter> <dst>".
        Keeps paragraph boundaries (no merging paragraphs).
        """

        doc = docx.Document(input_path)
        done = 0

        def _default_splitter(text: str) -> list[tuple[str, str]]:
            # Fallback: treat whole paragraph as one unit.
            return [(text, "")]

        splitter = sentence_splitter or _default_splitter
        pair_decider = should_pair or (lambda _s: True)
        d = (delimiter or "|").strip() or "|"
        if len(d) > 10:
            d = d[:10]

        def _process_paragraph(p):
            nonlocal done
            runs = list(p.runs)
            if not runs:
                return
            src_para = "".join((r.text or "") for r in runs)
            if not src_para.strip():
                return

            parts = splitter(src_para)
            out_chunks: list[str] = []
            for sentence, sep in parts:
                s = sentence or ""
                if not s.strip():
                    out_chunks.append(s + (sep or ""))
                    continue
                core = s.strip()
                dst = self.translate_fn(core)
                if pair_decider(core):
                    out_chunks.append(f"{core} {d} {str(dst).strip()}{sep or ''}")
                else:
                    out_chunks.append(f"{str(dst).strip()}{sep or ''}")

            out_text = "".join(out_chunks)
            if not out_text.strip():
                return

            # Preserve formatting by writing into first run; clear others.
            runs[0].text = out_text
            for r in runs[1:]:
                r.text = ""

            done += 1
            if progress_cb and done % 25 == 0:
                progress_cb(min(98, 5 + done // 8), f"DOCX: bilingual-sentence {done} paragraphs")

        for p in iter_docx_paragraphs(doc):
            _process_paragraph(p)

        for p in iter_docx_header_footer_paragraphs(doc):
            _process_paragraph(p)

        doc.save(output_path)
        if progress_cb:
            progress_cb(100, "DOCX: completed (bilingual sentence inline by paragraph)")
        return output_path

    def translate_docx_bilingual_inline_paragraph(
        self,
        input_path: str,
        output_path: str,
        *,
        delimiter: str = "|",
        progress_cb: Optional[Callable[[int, str], None]] = None,
        split_prefix: Optional[Callable[[str], tuple[str, str]]] = None,
    ) -> str:
        """Bilingual mode (inline) rendered per paragraph with a SINGLE delimiter.

        Output format per paragraph: "<src_para> <delimiter> <dst_para>".
        This avoids multiple separators caused by run splitting.
        """

        doc = docx.Document(input_path)
        done = 0

        d = (delimiter or "|").strip() or "|"
        if len(d) > 10:
            d = d[:10]

        def _process_paragraph(p):
            nonlocal done
            runs = list(p.runs)
            if not runs:
                return

            src_para = "".join((r.text or "") for r in runs)
            if not src_para.strip():
                return

            # Avoid double-appending if the paragraph already contains a bilingual delimiter.
            if f" {d} " in src_para:
                return

            prefix = ""
            body = src_para
            if split_prefix is not None:
                try:
                    prefix, body = split_prefix(src_para)
                except Exception:
                    prefix, body = "", src_para

            dst_para = self.translate_fn(str(body))
            dst_core = (str(dst_para) if dst_para is not None else "").strip()
            if not dst_core:
                return

            # Append to the last non-empty run to preserve existing run formatting,
            # tab stops / leaders, and other Word layout behaviors.
            last = None
            for r in reversed(runs):
                if (r.text or "").strip():
                    last = r
                    break
            if last is None:
                last = runs[-1]

            spacer = "" if (last.text or "").endswith((" ", "\t")) else " "
            last.text = f"{last.text or ''}{spacer}{d} {dst_core}"

            done += 1
            if progress_cb and done % 25 == 0:
                progress_cb(min(98, 5 + done // 8), f"DOCX: bilingual-inline {done} paragraphs")

        for p in iter_docx_paragraphs(doc):
            _process_paragraph(p)

        for p in iter_docx_header_footer_paragraphs(doc):
            _process_paragraph(p)

        doc.save(output_path)
        if progress_cb:
            progress_cb(100, "DOCX: completed (bilingual inline by paragraph)")
        return output_path

    def translate_docx_bilingual_sentence_newline_paragraph(
        self,
        input_path: str,
        output_path: str,
        *,
        progress_cb: Optional[Callable[[int, str], None]] = None,
        sentence_splitter: Optional[Callable[[str], list[tuple[str, str]]]] = None,
        should_pair: Optional[Callable[[str], bool]] = None,
    ) -> str:
        """Bilingual mode (sentence newline) rendered per paragraph.

        Required format per sentence:
          Line 1: source sentence
          Line 2: translation

        Notes:
          - Keeps paragraph boundaries (does not merge/split paragraphs).
          - Preserves original newlines that exist between sentences.
        """

        doc = docx.Document(input_path)
        done = 0

        def _default_splitter(text: str) -> list[tuple[str, str]]:
            return [(text, "")]

        splitter = sentence_splitter or _default_splitter
        pair_decider = should_pair or (lambda _s: True)

        def _norm_sep(sep: str) -> str:
            if sep and ("\n" in sep or "\r" in sep):
                return sep
            return "\n" if sep is not None else ""

        def _process_paragraph(p):
            nonlocal done
            runs = list(p.runs)
            if not runs:
                return
            src_para = "".join((r.text or "") for r in runs)
            if not src_para.strip():
                return

            parts = splitter(src_para)
            out_chunks: list[str] = []
            for sentence, sep in parts:
                raw = sentence or ""
                if not raw.strip():
                    # Keep blank fragments as-is
                    out_chunks.append(raw + (sep or ""))
                    continue
                core = raw.strip()
                dst = self.translate_fn(core)
                if pair_decider(core):
                    out_chunks.append(f"{core}\n{str(dst).strip()}{_norm_sep(sep or '')}")
                else:
                    out_chunks.append(f"{str(dst).strip()}{_norm_sep(sep or '')}")

            out_text = "".join(out_chunks)
            if not out_text.strip():
                return

            runs[0].text = out_text
            for r in runs[1:]:
                r.text = ""

            done += 1
            if progress_cb and done % 25 == 0:
                progress_cb(min(98, 5 + done // 8), f"DOCX: bilingual-sentence-newline {done} paragraphs")

        for p in iter_docx_paragraphs(doc):
            _process_paragraph(p)

        for p in iter_docx_header_footer_paragraphs(doc):
            _process_paragraph(p)

        doc.save(output_path)
        if progress_cb:
            progress_cb(100, "DOCX: completed (bilingual sentence newline by paragraph)")
        return output_path

    def translate_docx_bilingual_paren_paragraph(
        self,
        input_path: str,
        output_path: str,
        *,
        progress_cb: Optional[Callable[[int, str], None]] = None,
        should_pair: Optional[Callable[[str], bool]] = None,
        formatter: Optional[Callable[[str], tuple[str, str]]] = None,
    ) -> str:
        """Bilingual mode using parentheses: "<src> (<dst>)".

        Keeps paragraph boundaries and preserves run formatting by only appending
        the translation to the last non-empty run.
        """

        doc = docx.Document(input_path)
        done = 0
        pair_decider = should_pair or (lambda _s: True)

        def _process_paragraph(p):
            nonlocal done
            runs = list(p.runs)
            if not runs:
                return

            src_para = "".join((r.text or "") for r in runs)
            if not src_para.strip():
                return

            core = src_para.strip()
            action = None
            payload = None

            if formatter is not None:
                try:
                    action, payload = formatter(core)
                except Exception:
                    action, payload = None, None

            if not action:
                dst = self.translate_fn(core)
                dst_core = (str(dst) if dst is not None else "").strip()
                if not dst_core:
                    return

                if pair_decider(core):
                    action, payload = "append", dst_core
                else:
                    action, payload = "replace", dst_core

            last = None
            for r in reversed(runs):
                if (r.text or "").strip():
                    last = r
                    break
            if last is None:
                last = runs[-1]

            if action == "append":
                last.text = f"{last.text or ''} ({str(payload or '').strip()})"
            else:
                runs[0].text = str(payload or "")
                for r in runs[1:]:
                    r.text = ""

            done += 1
            if progress_cb and done % 40 == 0:
                progress_cb(min(98, 5 + done // 12), f"DOCX: paren bilingual {done} paragraphs")

        for p in iter_docx_paragraphs(doc):
            _process_paragraph(p)
        for p in iter_docx_header_footer_paragraphs(doc):
            _process_paragraph(p)

        doc.save(output_path)
        if progress_cb:
            progress_cb(100, "DOCX: completed (bilingual paren by paragraph)")
        return output_path
