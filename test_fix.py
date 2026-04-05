import sys, os
root = os.path.dirname(os.path.abspath(__file__))
backend = os.path.join(root, 'backend')
sys.path.insert(0, backend)
os.chdir(backend)
from app.services.document_v2.renderer.pdf_blocks import (
    PdfBlockTranslator, extract_page_blocks, merge_paragraph_blocks,
    _is_nonsemantic_token
)
import fitz

pdf_path = 'app/uploads/HoangPhuc222683.pdf'
if os.path.exists(pdf_path):
    doc = fitz.open(pdf_path)
    for pno in range(len(doc)):
        page = doc[pno]
        blocks = extract_page_blocks(page)
        print(f"\n=== Page {pno+1} ({len(blocks)} blocks) ===")
        if pno == 2:  # page 3
            for i, b in enumerate(blocks):
                t = (b.text or "").strip()
                tag = "TBL" if b.is_table_cell else "TXT"
                print(f"  [{i:2d}] {tag} y={b.bbox[1]:6.1f}-{b.bbox[3]:6.1f} x={b.bbox[0]:5.1f} fs={b.font_size:.1f} | {t[:70]}")
    doc.close()
else:
    print(f"PDF not found: {pdf_path}")
