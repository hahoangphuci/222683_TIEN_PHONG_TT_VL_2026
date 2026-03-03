import inspect

try:
    import pymupdf as fitz  # PyMuPDF >= 1.24
    print('import pymupdf ok')
except Exception as e:
    print('import pymupdf failed:', e)
    import fitz
    print('import fitz ok')

print('fitz version:', getattr(fitz, '__doc__', '')[:80].replace('\n',' '))
print('Document has insert_font:', hasattr(fitz.Document, 'insert_font'))
print('Page has insert_font:', hasattr(fitz.Page, 'insert_font'))
print('Page.insert_textbox:', inspect.signature(fitz.Page.insert_textbox))
