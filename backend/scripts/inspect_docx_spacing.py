import os
import sys


def inspect_docx(path: str, limit: int = 40) -> None:
    try:
        from docx import Document
    except Exception as e:
        print("IMPORT_ERROR", repr(e))
        return

    try:
        from docx.oxml.ns import qn
    except Exception:
        qn = None

    print("==", os.path.basename(path))
    try:
        doc = Document(path)
    except Exception as e:
        print("OPEN_ERROR", repr(e))
        return

    shown = 0
    for p in doc.paragraphs:
        text = p.text
        if not text or not text.strip():
            continue
        has_tab = "\t" in text
        has_multi_space = "  " in text
        jc_val = None
        try:
            if qn is not None:
                pPr = p._element.find(qn('w:pPr'))
                if pPr is not None:
                    jc = pPr.find(qn('w:jc'))
                    if jc is not None:
                        jc_val = jc.get(qn('w:val'))
        except Exception:
            jc_val = None
        print("TEXT", repr(text[:240]))
        print("HAS_TAB", has_tab, "MULTI_SPACE", has_multi_space, "JC", jc_val)
        shown += 1
        if shown >= limit:
            break


def main() -> int:
    files = sys.argv[1:] or [
        r"backend\downloads\translated_testfile - Copy.docx",
        r"backend\downloads\translated_testfile_-_Copy.docx",
        r"backend\downloads\translated_testfile_scanned.docx",
        r"backend\downloads\translated_translated_testfile_3.docx",
    ]

    for p in files:
        if os.path.exists(p):
            inspect_docx(p)
        else:
            print("MISSING", p)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
