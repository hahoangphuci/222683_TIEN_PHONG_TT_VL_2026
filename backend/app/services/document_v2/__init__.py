"""Document translation pipeline (V2).

This package implements a strict, layout-preserving pipeline for:
- DOCX (python-docx, translate runs in-place without rebuilding the document)

Architecture: extractor -> translator -> renderer
"""
