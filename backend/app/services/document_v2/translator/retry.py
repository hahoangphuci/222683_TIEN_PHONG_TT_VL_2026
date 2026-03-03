from __future__ import annotations

import time
from typing import Callable, Optional

from ..types import ProviderRateLimitError, TranslationTimeoutError


def translate_with_retry(
    translate_fn: Callable[[str], str],
    text: str,
    *,
    max_attempts: int = 3,
    backoff_base: float = 1.5,
    on_progress: Optional[Callable[[int, str], None]] = None,
) -> str:
    """Translate with retry/backoff on transient failures.

    Fail-fast on provider rate limit / insufficient credits.
    """

    attempt = 0
    last_exc: Exception | None = None
    while attempt < max(1, int(max_attempts)):
        try:
            return translate_fn(text)
        except ProviderRateLimitError:
            raise
        except TranslationTimeoutError as e:
            last_exc = e
        except Exception as e:
            # Best-effort: treat obvious network-ish errors as retryable.
            msg = str(e).lower()
            if any(k in msg for k in ("timeout", "timed out", "temporar", "connection", "reset", "gateway")):
                last_exc = e
            else:
                raise

        sleep_s = backoff_base ** attempt
        if on_progress:
            on_progress(0, f"Retry {attempt + 1}/{max_attempts} after {sleep_s:.1f}s")
        time.sleep(sleep_s)
        attempt += 1

    if last_exc:
        raise last_exc
    raise RuntimeError("Translation failed")
