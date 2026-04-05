"""Document translation pipeline (V2).

This package implements a strict, layout-preserving pipeline for:
- DOCX (python-docx, translate runs in-place without rebuilding the document)
- PDF  (PyMuPDF, layout detection + redact/re-insert preserving exact visual structure)

PDF workflow:
  1. Detect document layout (titles, paragraphs, tables, lists, form fields, dotted placeholders)
  2. Translate text preserving structure per element type
  3. Reconstruct layout preserving exact spacing, line breaks, alignment, table structure

Architecture: extractor -> translator -> renderer

PDF song ngữ liền kề: `renderer/pdf.py` với `bilingual_mode='inline'`.
"""
