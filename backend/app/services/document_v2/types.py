from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

Bbox = Tuple[float, float, float, float]  # (x0, y0, x1, y1) in PDF coordinates


@dataclass(frozen=True)
class TextBlock:
    """A single translatable text block with a bounding box.

    Notes:
      - Order matters. The pipeline preserves input order (page -> block).
      - bbox is in PDF coordinate space (points).
    """

    page_index: int
    block_index: int
    bbox: Bbox
    text: str
    kind: str  # 'pdf_text' | 'pdf_ocr'

    # Optional styling hints (best-effort)
    font_size: Optional[float] = None
    font_name: Optional[str] = None
    color_rgb: Optional[Tuple[float, float, float]] = None

    meta: Optional[Dict[str, Any]] = None


class ProviderRateLimitError(RuntimeError):
    """Hard rate-limit / insufficient credits (fail fast)."""


class TranslationTimeoutError(RuntimeError):
    """Timeout talking to translation API."""
