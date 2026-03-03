import argparse
import os
import re
import sys
import tempfile


def _parse_pages(pages_spec: str | None, total_pages: int):
    if not pages_spec:
        return list(range(total_pages))

    spec = pages_spec.strip().lower()
    if spec in ("all", "*"):
        return list(range(total_pages))

    pages: set[int] = set()
    for part in re.split(r"\s*,\s*", spec):
        if not part:
            continue
        if "-" in part:
            a, b = part.split("-", 1)
            a_i = int(a)
            b_i = int(b)
            if a_i < 1:
                a_i = 1
            if b_i > total_pages:
                b_i = total_pages
            for p in range(a_i, b_i + 1):
                pages.add(p - 1)
        else:
            p = int(part)
            if 1 <= p <= total_pages:
                pages.add(p - 1)

    return sorted(pages)


def main():
    ap = argparse.ArgumentParser(description="OCR scanned PDF pages using Tesseract (via pytesseract).")
    ap.add_argument("pdf", help="Path to PDF (inside container: /app/uploads/xxx.pdf)")
    ap.add_argument("--langs", default=os.getenv("OCR_LANGS_DEFAULT", "vie+eng"), help="Tesseract languages, e.g. vie+eng")
    ap.add_argument("--dpi", type=int, default=int(os.getenv("PDF_OCR_DPI", "200")), help="Render DPI (120-350 recommended)")
    ap.add_argument("--pages", default=None, help="Pages to OCR, 1-based. Examples: 1-2, 3,5, all")
    ap.add_argument("--out", default=None, help="Output text file path (default: /app/downloads/ocr_<name>.txt)")
    args = ap.parse_args()

    try:
        import fitz  # PyMuPDF
    except Exception as e:
        print(f"[ERROR] PyMuPDF not available: {e}")
        return 2

    try:
        import pytesseract
        from PIL import Image
    except Exception as e:
        print(f"[ERROR] OCR deps not available (pytesseract/Pillow): {e}")
        return 2

    # Configure tesseract_cmd (especially on Windows where PATH may not be set)
    try:
        import shutil

        def _resolve_tesseract_cmd():
            env_cmd = os.getenv("TESSERACT_CMD")
            if env_cmd and str(env_cmd).strip():
                cand = str(env_cmd).strip().strip('"')
                if os.path.exists(cand):
                    return cand
                found = shutil.which(cand)
                if found:
                    return found

            found = shutil.which("tesseract")
            if found:
                return found

            if os.name == 'nt':
                for p in (
                    r"C:\\Program Files\\Tesseract-OCR\\tesseract.exe",
                    r"C:\\Program Files (x86)\\Tesseract-OCR\\tesseract.exe",
                    r"C:\\Tesseract-OCR\\tesseract.exe",
                ):
                    if os.path.exists(p):
                        return p
            return None

        resolved = _resolve_tesseract_cmd()
        if resolved:
            pytesseract.pytesseract.tesseract_cmd = resolved
        else:
            print("[WARN] Could not resolve tesseract.exe. OCR may fail unless Tesseract is installed.")
    except Exception:
        pass

    pdf_path = args.pdf
    if not os.path.exists(pdf_path):
        print(f"[ERROR] PDF not found: {pdf_path}")
        return 2

    dpi = max(120, min(350, int(args.dpi)))
    doc = fitz.open(pdf_path)
    total = len(doc)

    page_indices = _parse_pages(args.pages, total)
    if not page_indices:
        print("[WARN] No pages selected.")
        return 0

    base = os.path.splitext(os.path.basename(pdf_path))[0]
    out_path = args.out or os.path.join("/app/downloads", f"ocr_{base}.txt")

    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    config = os.getenv("TESSERACT_CONFIG") or "--oem 3 --psm 6"
    langs = (args.langs or "vie+eng").strip()

    with open(out_path, "w", encoding="utf-8") as f_out:
        f_out.write(f"PDF: {pdf_path}\n")
        f_out.write(f"DPI: {dpi}\n")
        f_out.write(f"LANGS: {langs}\n")
        f_out.write(f"PAGES: {', '.join(str(i+1) for i in page_indices)}\n")
        f_out.write("\n")

        for idx in page_indices:
            page = doc[idx]
            pix = page.get_pixmap(dpi=dpi)

            with tempfile.TemporaryDirectory() as td:
                png_path = os.path.join(td, f"page_{idx+1}.png")
                pix.save(png_path)

                try:
                    img = Image.open(png_path)
                    text = pytesseract.image_to_string(img, lang=langs, config=config) or ""
                except Exception as e:
                    text = ""
                    print(f"[WARN] OCR failed on page {idx+1}: {e}")

            text = text.strip()
            f_out.write(f"===== PAGE {idx+1} =====\n")
            f_out.write(text)
            f_out.write("\n\n")

    print(f"[OK] OCR written to: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
