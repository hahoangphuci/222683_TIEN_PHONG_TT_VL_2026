"""Document translation pipeline (V2).

This package implements a strict, layout-preserving pipeline for:
- PDF (text-layer via PyMuPDF blocks; scanned via Tesseract image_to_data)
- DOCX (python-docx, translate runs in-place without rebuilding the document)

Architecture: extractor -> translator -> renderer
"""
