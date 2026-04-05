"""Examine table border drawings on page 2."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "backend"))
import fitz

pdf_path = r"backend\app\uploads\HoangPhuc222683.pdf"
doc = fitz.open(pdf_path)
page = doc[2]  # Page 3 (0-indexed = 2)

# Table area: y=284 to y=394 (MonHoc), y=427 to y=507 (KhoaHoc)
drawings = page.get_drawings()
print(f"Total drawings on page 2: {len(drawings)}")
for di, d in enumerate(drawings):
    # Check if drawing is in table area
    rect = d.get("rect")
    if rect:
        ry0, ry1 = rect.y0, rect.y1
        if (280 < ry0 < 510) or (280 < ry1 < 510):
            items = d.get("items", [])
            item_strs = []
            for item in items[:5]:
                item_strs.append(str(item))
            print(f"  draw[{di}] rect={rect} color={d.get('color')} fill={d.get('fill')} width={d.get('width')}")
            for s in item_strs:
                print(f"    item: {s}")

doc.close()
