from __future__ import annotations

import os
import re
from typing import Optional

import requests

from ..types import ProviderRateLimitError, TranslationTimeoutError


class OpenRouterTranslator:
    """Minimal OpenRouter Chat Completions client.

    Uses env vars:
      - OPENROUTER_API_KEY (required)
      - AI_MODEL (optional)
      - AI_HTTP_TIMEOUT (optional, seconds)
      - AI_HEADER_HTTP_REFERER / AI_HEADER_X_TITLE (optional)
    """

    def __init__(
        self,
        *,
        api_key: Optional[str] = None,
        model: Optional[str] = None,
        timeout_s: Optional[float] = None,
    ):
        self.api_key = (api_key or os.getenv("OPENROUTER_API_KEY") or "").strip().strip('"').strip("'")
        if not self.api_key:
            raise RuntimeError("OPENROUTER_API_KEY is required")

        self.model = (model or os.getenv("AI_MODEL") or "gpt-4o-mini").strip()
        if not self.model:
            self.model = "gpt-4o-mini"

        if timeout_s is None:
            try:
                timeout_s = float(os.getenv("AI_HTTP_TIMEOUT", "60"))
            except Exception:
                timeout_s = 60.0
        self.timeout_s = max(5.0, float(timeout_s))

        self.base_url = "https://openrouter.ai/api/v1"
        self.session = requests.Session()

        ref = (os.getenv("AI_HEADER_HTTP_REFERER") or os.getenv("HTTP_REFERER") or "").strip()
        title = (os.getenv("AI_HEADER_X_TITLE") or "").strip()

        self.default_headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        if ref:
            self.default_headers["HTTP-Referer"] = ref
        if title:
            self.default_headers["X-Title"] = title

    def translate_text(self, text: str, *, source_lang: str = "auto", target_lang: str) -> str:
        if text is None:
            return ""
        src = (text or "")
        if not src.strip():
            return src

        # Keep whitespace around the core text unchanged.
        m = re.match(r"^(\s*)(.*?)(\s*)$", src, flags=re.DOTALL)
        lead, core, tail = (m.group(1), m.group(2), m.group(3)) if m else ("", src, "")
        if not core.strip():
            return src

        system_prompt = (
            "You are a professional translator. "
            f"Translate the following text to {target_lang}.\n"
            "RULES:\n"
            "- Only return the translated text.\n"
            "- Preserve line breaks exactly.\n"
            "- Do NOT translate people's names; keep them exactly as in the source.\n"
            "- Do NOT introduce '/' or '\\' as separators.\n"
            "- Do not add explanations or quotes."
        )
        if source_lang and source_lang != "auto":
            system_prompt = (
                "You are a professional translator. "
                f"Translate the following text from {source_lang} to {target_lang}.\n"
                "RULES:\n"
                "- Only return the translated text.\n"
                "- Preserve line breaks exactly.\n"
                "- Do NOT translate people's names; keep them exactly as in the source.\n"
                "- Do NOT introduce '/' or '\\' as separators.\n"
                "- Do not add explanations or quotes."
            )

        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": core},
            ],
            "temperature": 0,
        }

        try:
            r = self.session.post(
                f"{self.base_url}/chat/completions",
                headers=self.default_headers,
                json=payload,
                timeout=self.timeout_s,
            )
        except requests.Timeout as e:
            raise TranslationTimeoutError(str(e)) from e
        except requests.RequestException as e:
            raise TranslationTimeoutError(str(e)) from e

        # OpenRouter uses HTTP status codes (429/402) when rate limited / insufficient credits.
        if r.status_code in (402, 429):
            raise ProviderRateLimitError(f"OpenRouter error {r.status_code}: {r.text}")
        if r.status_code >= 400:
            raise RuntimeError(f"OpenRouter error {r.status_code}: {r.text}")

        data = r.json()
        try:
            out = data["choices"][0]["message"]["content"]
        except Exception as e:
            raise RuntimeError(f"Unexpected OpenRouter response: {data}") from e

        out = (out or "").strip()
        return f"{lead}{out}{tail}"
