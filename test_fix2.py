import sys, os
root = os.path.dirname(os.path.abspath(__file__))
backend = os.path.join(root, 'backend')
sys.path.insert(0, backend)
os.chdir(backend)
from app.services.document_v2.renderer.pdf_blocks import (
    PdfBlockTranslator, extract_page_blocks, _is_nonsemantic_token,
    _canonical_table_constraint
)
import fitz, bisect

# Test canonical table header
print("=== Canonical table header tests ===")
for text in ["Tên trường", "Kiểu dữliệu", "Ràng buộc"]:
    result = _canonical_table_constraint(text)
    print(f"  '{text}' -> '{result}'")

# Simulate y_shift flow on page 3
pdf_path = 'app/uploads/HoangPhuc222683.pdf'
if not os.path.exists(pdf_path):
    print(f"PDF not found: {pdf_path}")
    sys.exit(1)

doc = fitz.open(pdf_path)
page = doc[2]  # page 3
blocks = extract_page_blocks(page)

call_count = 0
def mock_translate(text):
    global call_count
    call_count += 1
    return f'[EN] {text}'

translator = PdfBlockTranslator(
    mock_translate,
    bilingual_mode='inline',
    inline_table_mode='translate-only',
)
translator._translate_cached = mock_translate
translations = translator._translate_blocks(blocks)

print(f"\n=== Page 3 y_shift simulation ===")
print(f"Total blocks: {len(blocks)}, Translated: {len(translations)}")

table_tops = sorted(set(
    round(b.table_bbox[1], 2) for b in blocks
    if b.is_table_cell and b.table_bbox
))
print(f"Table tops: {table_tops}")

inline_y_shift = 0.0
prev_source_bottom = None
for i, block in enumerate(blocks):
    if block.is_table_cell:
        continue
    t = (block.text or "").strip()[:50]
    y0 = block.bbox[1]
    y1 = block.bbox[3]

    # Gap absorption
    if inline_y_shift > 0 and prev_source_bottom is not None:
        gap = y0 - prev_source_bottom
        if gap > 0:
            absorb = min(inline_y_shift, gap * 0.85)
            inline_y_shift = max(0.0, inline_y_shift - absorb)

    # Table cross reset
    if prev_source_bottom is not None:
        for tt in table_tops:
            if prev_source_bottom < tt < y0:
                inline_y_shift = 0.0
                break

    # Clamp before table
    idx_tbl = bisect.bisect_right(table_tops, y0 + 0.2)
    if idx_tbl < len(table_tops):
        next_tbl = table_tops[idx_tbl]
        max_shift_y0 = next_tbl - (y1 - y0) - 1.0
        if y0 + inline_y_shift > max_shift_y0:
            inline_y_shift = max(0.0, max_shift_y0 - y0)

    shifted_y0 = y0 + inline_y_shift
    is_translated = i in translations

    # Simulate expansion
    orig_h = y1 - y0
    if is_translated:
        used_h = orig_h * 2.0  # bilingual roughly doubles height
        inline_y_shift += max(0.0, used_h - orig_h)

    marker = ">>>" if any(kw in t for kw in ['Bài 3', 'MonHoc', 'KhoaHoc', 'Bảng']) else "   "
    tr_mark = "TR" if is_translated else "  "
    print(f"  {marker}[{i:2d}] {tr_mark} y0={y0:6.1f} shifted={shifted_y0:6.1f} shift={inline_y_shift:6.1f} | {t}")

    prev_source_bottom = y1

doc.close()
