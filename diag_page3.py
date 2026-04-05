"""Diagnose page 3 block extraction + table_tops to understand y_shift flow."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "backend"))

import fitz
from app.services.document_v2.renderer.pdf_blocks import extract_page_blocks

pdf_path = r"backend\app\uploads\HoangPhuc222683.pdf"
doc = fitz.open(pdf_path)

for pg_idx in range(len(doc)):
    page = doc[pg_idx]
    blocks = extract_page_blocks(page)
    table_tops = sorted({float(b.bbox[1]) for b in blocks if b.is_table_cell})
    
    print(f"\n{'='*80}")
    print(f"PAGE {pg_idx} — {len(blocks)} blocks, {len(table_tops)} table tops")
    print(f"table_tops = {[round(t,1) for t in table_tops]}")
    print(f"{'='*80}")
    
    for i, b in enumerate(blocks):
        tbl_marker = " [TABLE]" if b.is_table_cell else ""
        txt = (b.text or "").strip()[:60]
        print(f"  [{i:2d}] y0={b.bbox[1]:7.1f} y1={b.bbox[3]:7.1f} x0={b.bbox[0]:6.1f} fs={b.font_size:4.1f}{tbl_marker} | {txt!r}")

doc.close()
