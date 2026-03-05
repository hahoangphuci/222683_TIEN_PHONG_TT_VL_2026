"""Quick check: verify _is_dot_leader_or_filler fix and extraction of page 0."""
import sys, os, re
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
os.environ.setdefault("OPENROUTER_API_KEY", "x")
os.environ.setdefault("DATABASE_URL", "sqlite:///:memory:")

import pymupdf as fitz
from app.services.document_v2.extractor.pdf_text_layer import extract_pdf_text_blocks
from app.services.document_v2.pipeline import _is_dot_leader_or_filler

OUTPUT = os.path.join(os.path.dirname(__file__), "check_filler_output.txt")

pdf_path = sys.argv[1] if len(sys.argv) > 1 else "uploads/testfile.pdf"
max_pages = int(sys.argv[2]) if len(sys.argv) > 2 else 3

lines = []

# --- Test _is_dot_leader_or_filler directly ---
test_cases = [
    ("Họ Tên TV1: Hà Hoàng Phúc .......MSSV: 222683.......", False),
    ("Tên đề tài: Website học lập trình - CodequestAI.......", False),
    ("Tóm tắt nội dung đề tài: ...", False),
    ("Phân tích thiết kế hệ thống: Có ......", False),
    ("............................................", True),
    ("____________________________", True),
    ("", True),
    ("1", False),
    ("AB", False),
]

lines.append("=== _is_dot_leader_or_filler unit tests ===")
all_pass = True
for text, expected in test_cases:
    result = _is_dot_leader_or_filler(text)
    status = "PASS" if result == expected else "FAIL"
    if status == "FAIL":
        all_pass = False
    lines.append(f"  [{status}] expected={expected} got={result} | {repr(text[:60])}")

lines.append(f"\nAll tests: {'PASS' if all_pass else 'FAIL'}\n")

# --- Extract blocks from PDF and classify ---
doc = fitz.open(pdf_path)
n_ok = 0
n_skip = 0

for page_idx in range(min(max_pages, len(doc))):
    blocks = list(extract_pdf_text_blocks(doc, page_index=page_idx))
    lines.append(f"=== Page {page_idx+1}: {len(blocks)} blocks ===")
    for b in blocks:
        txt = b.text.strip()
        filler = _is_dot_leader_or_filler(txt)
        alpha = sum(1 for ch in txt if ch.isalpha())
        label = "[SKIP-filler]" if filler else "[OK]"
        if filler:
            n_skip += 1
        else:
            n_ok += 1
        snippet = repr(txt[:70])
        lines.append(f"  {label} alpha={alpha} blk#{b.block_index} | {snippet}")

lines.append(f"\nSUMMARY: {n_ok} will translate, {n_skip} skipped as filler")
doc.close()

with open(OUTPUT, "w", encoding="utf-8") as f:
    f.write("\n".join(lines))

# Also print to stdout
print("\n".join(lines))
print(f"\n[Written to {OUTPUT}]")
