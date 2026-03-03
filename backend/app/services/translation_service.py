import openai
import deepl
import os
import threading
import uuid
import time
import re
import shutil
from dotenv import load_dotenv
from app.services.file_service import FileService, ProviderRateLimitError
from app.services.document_v2.pipeline import DocumentPipelineV2
from app.services.document_v2.types import ProviderRateLimitError as ProviderRateLimitErrorV2
from deep_translator import MyMemoryTranslator, GoogleTranslator
import requests
import urllib.parse

# Load .env từ thư mục backend (app/services -> app -> backend)
_backend_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# IMPORTANT: In Docker, environment variables should win (DATABASE_URL, API keys, etc).
# Set DOTENV_OVERRIDE=1 only if you explicitly want backend/.env to override existing env vars.
_override = (os.getenv('DOTENV_OVERRIDE') or '').strip().lower() in ('1', 'true', 'yes', 'on')
load_dotenv(os.path.join(_backend_dir, '.env'), override=_override)

# Bản đồ mã ISO 639-1 sang mã ngôn ngữ đích của DeepL (chỉ các ngôn ngữ DeepL hỗ trợ)
# Nguồn: https://developers.deepl.com/docs/resources/supported-languages
DEEPL_TARGET_MAP = {
    'ar': 'AR', 'bg': 'BG', 'cs': 'CS', 'da': 'DA', 'de': 'DE', 'el': 'EL',
    'en': 'EN-US', 'en-us': 'EN-US', 'en-gb': 'EN-GB',
    'es': 'ES', 'et': 'ET', 'fi': 'FI', 'fr': 'FR',
    'he': 'HE', 'iw': 'HE',  # iw là mã cũ của Hebrew
    'hu': 'HU', 'id': 'ID', 'it': 'IT', 'ja': 'JA', 'ko': 'KO',
    'lt': 'LT', 'lv': 'LV', 'nb': 'NB', 'no': 'NB',  # Norwegian -> Bokmål
    'nl': 'NL', 'pl': 'PL',
    'pt': 'PT-BR', 'pt-br': 'PT-BR', 'pt-pt': 'PT-PT',
    'ro': 'RO', 'ru': 'RU', 'sk': 'SK', 'sl': 'SL', 'sv': 'SV',
    'th': 'TH', 'tr': 'TR', 'uk': 'UK', 'vi': 'VI',
    'zh': 'ZH', 'zh-cn': 'ZH', 'zh-hans': 'ZH', 'zh-tw': 'ZH-HANT', 'zh-hant': 'ZH-HANT',
}

# Tên ngôn ngữ cho prompt OpenAI (các ngôn ngữ DeepL không hỗ trợ dùng OpenAI)
# Giúp model hiểu rõ hơn so với chỉ dùng mã (vd: "Thai" thay vì "th")
CODE_TO_NAME = {
    'af': 'Afrikaans', 'sq': 'Albanian', 'am': 'Amharic', 'ar': 'Arabic',
    'hy': 'Armenian', 'az': 'Azerbaijani', 'eu': 'Basque', 'be': 'Belarusian',
    'bn': 'Bengali', 'bs': 'Bosnian', 'bg': 'Bulgarian', 'ca': 'Catalan',
    'zh': 'Chinese (Simplified)', 'zh-cn': 'Chinese (Simplified)', 'zh-hans': 'Chinese (Simplified)',
    'zh-tw': 'Chinese (Traditional)', 'zh-hant': 'Chinese (Traditional)',
    'hr': 'Croatian', 'cs': 'Czech', 'da': 'Danish', 'nl': 'Dutch',
    'en': 'English', 'et': 'Estonian', 'fi': 'Finnish', 'fr': 'French',
    'de': 'German', 'el': 'Greek', 'he': 'Hebrew', 'iw': 'Hebrew',
    'hi': 'Hindi', 'hu': 'Hungarian', 'id': 'Indonesian', 'it': 'Italian',
    'ja': 'Japanese', 'ko': 'Korean', 'lv': 'Latvian', 'lt': 'Lithuanian',
    'ms': 'Malay', 'no': 'Norwegian', 'nb': 'Norwegian Bokmål',
    'fa': 'Persian', 'pl': 'Polish', 'pt': 'Portuguese', 'ro': 'Romanian',
    'ru': 'Russian', 'sr': 'Serbian', 'sk': 'Slovak', 'sl': 'Slovenian',
    'es': 'Spanish', 'sw': 'Swahili', 'sv': 'Swedish', 'th': 'Thai',
    'tr': 'Turkish', 'uk': 'Ukrainian', 'ur': 'Urdu', 'vi': 'Vietnamese',
    'gu': 'Gujarati', 'kn': 'Kannada', 'ml': 'Malayalam',
    'mr': 'Marathi', 'ta': 'Tamil', 'te': 'Telugu', 'pa': 'Punjabi', 'ne': 'Nepali',
    'my': 'Myanmar (Burmese)', 'km': 'Khmer', 'lo': 'Lao', 'tl': 'Filipino',
}

class TranslationService:
    def __init__(self):
        def _sanitize_key(val):
            if not val:
                return None
            v = val.strip()
            # Strip wrapping quotes if user put OPENAI_API_KEY="..." in .env
            if (v.startswith('"') and v.endswith('"')) or (v.startswith("'") and v.endswith("'")):
                v = v[1:-1].strip()
            # Treat obvious placeholders or very-short values as absent
            if v.lower().startswith('your-') or v.lower() in ('changeme', 'replace-me', '') or len(v) < 20:
                return None
            return v

        openai_key = _sanitize_key(os.getenv('OPENAI_API_KEY'))
        deepl_key = _sanitize_key(os.getenv('DEEPL_API_KEY'))
        openrouter_key = _sanitize_key(os.getenv('OPENROUTER_API_KEY'))
        # HTTP timeout (seconds) to avoid hanging forever when provider stalls
        try:
            http_timeout = float(os.getenv('AI_HTTP_TIMEOUT', '60'))
        except Exception:
            http_timeout = 60.0
        # Debug: show which keys are present (do not print values). placeholders are ignored.
        print(f"TranslationService keys - DEEPL: {bool(deepl_key)}, OPENAI: {bool(openai_key)}, OPENROUTER: {bool(openrouter_key)}")
        self.deepl_translator = deepl.Translator(deepl_key) if deepl_key else None
        
        # Initialize OpenAI client for direct OpenAI or OpenRouter
        if openrouter_key:
            extra = {}
            ref = os.getenv('AI_HEADER_HTTP_REFERER') or os.getenv('HTTP_REFERER')
            if ref:
                extra["default_headers"] = {"Referer": ref.strip()}
            self.openai_client = openai.OpenAI(
                api_key=openrouter_key,
                base_url="https://openrouter.ai/api/v1",
                timeout=http_timeout,
                **extra
            )
        elif openai_key:
            self.openai_client = openai.OpenAI(api_key=openai_key, timeout=http_timeout)
        else:
            self.openai_client = None
            
        # Pass translator callback into FileService so document processing can call
        # Provide FileService both translation and OCR hooks (used for DOCX embedded images)
        self.file_service = FileService(
            translator=self.translate_text,
            ocr_image_to_text=self.ocr_image_to_text,
            ocr_translate_overlay=self.ocr_translate_overlay,
            ocr_image_to_bboxes=self.ocr_image_to_bboxes,
        )
        self.file_service.has_tesseract = self._is_tesseract_available()
        # Simple in-memory job store for background document processing
        self.jobs = {}  # job_id -> {status, progress, message, download_path, error}
    
    def _openai_translate(self, text, source_lang, target_lang, target_code):
        """Dịch bằng OpenAI/OpenRouter. Dùng cho mọi ngôn ngữ (kể cả DeepL không hỗ trợ)."""
        if not self.openai_client:
            return None
        target_name = CODE_TO_NAME.get(target_code, target_lang)
        model = os.getenv('AI_MODEL', 'gpt-3.5-turbo')
        system_prompt = (
            f"You are a professional translator. Translate the following text to {target_name}.\n"
            f"IMPORTANT RULES:\n"
            f"- Only return the translated text, nothing else.\n"
            f"- Preserve the EXACT original casing: if the source is lowercase, keep lowercase; if uppercase, keep uppercase.\n"
            f"- Do NOT capitalize words that were not capitalized in the original.\n"
            f"- Preserve line breaks and paragraph structure."
        )
        if source_lang and source_lang != 'auto':
            src_name = CODE_TO_NAME.get(source_lang.lower(), source_lang)
            system_prompt = (
                f"You are a professional translator. Translate the following text from {src_name} to {target_name}.\n"
                f"IMPORTANT RULES:\n"
                f"- Only return the translated text, nothing else.\n"
                f"- Preserve the EXACT original casing: if the source is lowercase, keep lowercase; if uppercase, keep uppercase.\n"
                f"- Do NOT capitalize words that were not capitalized in the original.\n"
                f"- Preserve line breaks and paragraph structure."
            )
        try:
            try:
                max_tokens = int(os.getenv('AI_MAX_TOKENS', '900'))
            except Exception:
                max_tokens = 900
            if max_tokens < 64:
                max_tokens = 64
            if max_tokens > 2048:
                max_tokens = 2048

            attempt_tokens = [max_tokens]
            last_error = None

            for tok in attempt_tokens:
                try:
                    response = self.openai_client.chat.completions.create(
                        model=model,
                        messages=[
                            {"role": "system", "content": system_prompt},
                            {"role": "user", "content": text}
                        ],
                        max_tokens=tok,
                        temperature=0
                    )
                    content = response.choices[0].message.content
                    return (content or "").strip()
                except Exception as inner_e:
                    last_error = inner_e
                    msg = str(inner_e)
                    m = re.search(r"can only afford\s+(\d+)", msg, flags=re.IGNORECASE)
                    if m:
                        affordable = int(m.group(1))
                        fallback_tok = max(64, min(affordable - 32, tok - 64))
                        if fallback_tok >= 64 and fallback_tok < tok and fallback_tok not in attempt_tokens:
                            attempt_tokens.append(fallback_tok)
                            continue
                    break

            if last_error:
                raise last_error
            raise RuntimeError("AI translation failed with unknown error")
        except Exception as e:
            # Surface API errors with their message so the caller can detect credit or rate issues
            raise RuntimeError(f"AI translation failed: {e}") from e

    def translate_text(self, text, source_lang, target_lang):
        if target_lang is None or not str(target_lang).strip():
            raise ValueError("target_lang is required")
        if text is None:
            return ""
        target_lang = str(target_lang).strip()
        source = (str(source_lang).strip() if source_lang is not None else 'auto') or 'auto'
        t = target_lang.lower()
        s = source.lower()

        # API-only mode:
        # If AI_DISABLE_FALLBACK=1, we will NOT use GoogleTranslator/MyMemory.
        # Instead, the job fails fast on provider credit/rate errors so the user knows
        # the API is required (and tokens/credits should be consumed).
        disable_fallback = (os.getenv('AI_DISABLE_FALLBACK') or '').strip().lower() in ('1', 'true', 'yes', 'on')

        if not self.openai_client:
            raise RuntimeError("AI provider not configured: set OPENAI_API_KEY or OPENROUTER_API_KEY in backend/.env")
        try:
            out = self._openai_translate(text, source, target_lang, t)
            if out is not None and out != "":
                return out
            else:
                raise RuntimeError("AI translation returned empty result")
        except Exception as e:
            err = str(e).lower()
            # If fallback is disabled, propagate the provider error.
            if disable_fallback:
                raise

            # Graceful fallback for provider credit/rate issues so document jobs don't fail entirely.
            if any(k in err for k in ('429', 'too many requests', '402', 'credit', 'insufficient', 'rate', 'free-models', 'requires more credits')):
                try:
                    fallback_out = GoogleTranslator(source='auto', target=t).translate(text)
                    if fallback_out and str(fallback_out).strip():
                        print("AI unavailable (credits/rate). Fallback to GoogleTranslator succeeded.")
                        return str(fallback_out)
                except Exception as g_err:
                    print(f"GoogleTranslator fallback failed: {g_err}")
                try:
                    fallback_out = MyMemoryTranslator(source='auto', target=t).translate(text)
                    if fallback_out and str(fallback_out).strip():
                        print("AI unavailable (credits/rate). Fallback to MyMemoryTranslator succeeded.")
                        return str(fallback_out)
                except Exception as m_err:
                    print(f"MyMemory fallback failed: {m_err}")

            import traceback
            traceback.print_exc()
            raise RuntimeError(f"AI translation failed: {e}")
    
    def translate_document(self, file_path, target_lang):
        # Synchronous translation (kept for compatibility)
        return self.file_service.process_document(file_path, target_lang)

    def translate_html(self, html, source_lang, target_lang):
        """Translate an HTML string while preserving tags. We translate text nodes only."""
        try:
            from bs4 import BeautifulSoup
            from bs4.element import NavigableString
        except Exception as e:
            raise RuntimeError("BeautifulSoup is required for HTML translation. Install 'beautifulsoup4'.") from e

        soup = BeautifulSoup(html, "html.parser")
        # Collect text nodes to translate
        text_nodes = []
        for element in soup.find_all(string=True):
            parent_name = element.parent.name if element.parent else ''
            # Skip non-visible or script/style/code/pre content
            if parent_name in ('script', 'style', 'code', 'pre', 'noscript'):
                continue
            text = str(element).strip()
            if text:
                text_nodes.append(element)

        # Translate each text node individually (simple; can be optimized by batching)
        # IMPORTANT: preserve leading/trailing whitespace exactly to keep original formatting.
        for node in text_nodes:
            original = str(node)
            try:
                # Keep surrounding whitespace exactly as-is (including newlines)
                leading_ws = original[: len(original) - len(original.lstrip())]
                trailing_ws = original[len(original.rstrip()):]
                core = original[len(leading_ws): len(original) - len(trailing_ws)]

                # If core is empty after trimming, skip translation
                if not core or not core.strip():
                    continue

                translated_core = self.translate_text(core, source_lang, target_lang)
                # Do NOT collapse whitespace. Only trim accidental outer spaces/newlines.
                translated_core = (translated_core or "").strip()

                # Replace the node content with translated text (preserving tags)
                node.replace_with(f"{leading_ws}{translated_core}{trailing_ws}")
            except Exception as e:
                # On failure, keep original text to avoid corrupting HTML
                print(f"HTML node translation failed, keeping original: {e}")
                continue

        return str(soup)

    # ── AI Vision OCR (no Tesseract needed) ──────────────────────────────

    def _ai_vision_ocr(self, image_path):
        """Extract text from an image using the AI Vision model (GPT-4o / OpenRouter vision model).

        This does NOT require Tesseract — it sends the image to the configured AI provider.
        Set AI_VISION_MODEL in .env to pick a vision-capable model (default: auto-detect).
        """
        if not self.openai_client:
            raise RuntimeError("AI provider not configured: set OPENAI_API_KEY or OPENROUTER_API_KEY in backend/.env")

        import base64
        try:
            from PIL import Image as _PILImage
        except Exception:
            _PILImage = None

        vision_model = (os.getenv('AI_VISION_MODEL') or '').strip()
        if not vision_model:
            # Fallback: use main model if it's known to support vision, else pick a sensible default
            main_model = os.getenv('AI_MODEL', '')
            _KNOWN_VISION = ('gpt-4o', 'gpt-4-turbo', 'gpt-4-vision', 'gemini', 'claude-3',
                             'claude-4', 'llama-4', 'qwen2.5-vl', 'qwen-2.5-vl',
                             'pixtral', 'internvl', 'mistral-small', 'gemma-3')
            if any(kw in main_model.lower() for kw in _KNOWN_VISION):
                vision_model = main_model
            else:
                # Default to a free vision model on OpenRouter
                vision_model = 'google/gemini-2.0-flash-001'

        # Read & encode image as base64 data-URI
        with open(image_path, 'rb') as f:
            raw = f.read()

        # Detect MIME
        ext = os.path.splitext(image_path)[1].lower()
        mime_map = {'.png': 'image/png', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg',
                    '.gif': 'image/gif', '.bmp': 'image/bmp', '.webp': 'image/webp', '.tif': 'image/tiff', '.tiff': 'image/tiff'}
        mime = mime_map.get(ext, 'image/png')

        # Resize very large images to save tokens/bandwidth
        try:
            if _PILImage:
                img = _PILImage.open(image_path)
                w, h = img.size
                if max(w, h) > 2048:
                    ratio = 2048.0 / max(w, h)
                    img = img.resize((int(w * ratio), int(h * ratio)), resample=_PILImage.BICUBIC)
                    import io as _io
                    buf = _io.BytesIO()
                    fmt = 'JPEG' if ext in ('.jpg', '.jpeg') else 'PNG'
                    if fmt == 'JPEG' and img.mode not in ('RGB', 'L'):
                        img = img.convert('RGB')
                    img.save(buf, format=fmt, quality=85)
                    raw = buf.getvalue()
                    mime = 'image/jpeg' if fmt == 'JPEG' else 'image/png'
        except Exception:
            pass

        b64 = base64.b64encode(raw).decode('ascii')
        data_uri = f"data:{mime};base64,{b64}"

        try:
            response = self.openai_client.chat.completions.create(
                model=vision_model,
                messages=[
                    {"role": "system", "content": "You are an OCR assistant. Extract ALL visible text from the image exactly as it appears. Return only the extracted text, nothing else. Preserve line breaks."},
                    {"role": "user", "content": [
                        {"type": "image_url", "image_url": {"url": data_uri}},
                        {"type": "text", "text": "Extract all text from this image."},
                    ]},
                ],
                max_tokens=2048,
                temperature=0,
            )
            content = response.choices[0].message.content
            return (content or "").strip()
        except Exception as e:
            raise RuntimeError(f"AI Vision OCR failed: {e}") from e

    def _ai_vision_ocr_and_translate(self, image_path, target_lang):
        """OCR + translate an image in a single AI Vision call (efficient, no Tesseract).

        Also classifies the image type to recommend the best output mode:
        - 'text': image is a document/scan/reading passage → extract as plain text
        - 'image': image is a poster/banner/design/photo with text → overlay translation on image
        - 'both': image has mixed characteristics → return both text output and image overlay

        Returns: (ocr_text, translated_text, recommended_mode)
        """
        if not self.openai_client:
            raise RuntimeError("AI provider not configured")

        import base64
        try:
            from PIL import Image as _PILImage
        except Exception:
            _PILImage = None

        vision_model = (os.getenv('AI_VISION_MODEL') or '').strip()
        if not vision_model:
            main_model = os.getenv('AI_MODEL', '')
            _KNOWN_VISION = ('gpt-4o', 'gpt-4-turbo', 'gpt-4-vision', 'gemini', 'claude-3',
                             'claude-4', 'llama-4', 'qwen2.5-vl', 'qwen-2.5-vl',
                             'pixtral', 'internvl', 'mistral-small', 'gemma-3')
            if any(kw in main_model.lower() for kw in _KNOWN_VISION):
                vision_model = main_model
            else:
                vision_model = 'google/gemini-2.0-flash-001'

        with open(image_path, 'rb') as f:
            raw = f.read()

        ext = os.path.splitext(image_path)[1].lower()
        mime_map = {'.png': 'image/png', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg',
                    '.gif': 'image/gif', '.bmp': 'image/bmp', '.webp': 'image/webp'}
        mime = mime_map.get(ext, 'image/png')

        try:
            if _PILImage:
                img = _PILImage.open(image_path)
                w, h = img.size
                if max(w, h) > 2048:
                    ratio = 2048.0 / max(w, h)
                    img = img.resize((int(w * ratio), int(h * ratio)), resample=_PILImage.BICUBIC)
                    import io as _io
                    buf = _io.BytesIO()
                    fmt = 'JPEG' if ext in ('.jpg', '.jpeg') else 'PNG'
                    if fmt == 'JPEG' and img.mode not in ('RGB', 'L'):
                        img = img.convert('RGB')
                    img.save(buf, format=fmt, quality=85)
                    raw = buf.getvalue()
                    mime = 'image/jpeg' if fmt == 'JPEG' else 'image/png'
        except Exception:
            pass

        b64 = base64.b64encode(raw).decode('ascii')
        data_uri = f"data:{mime};base64,{b64}"

        target_name = CODE_TO_NAME.get(target_lang.lower(), target_lang) if target_lang else target_lang

        try:
            response = self.openai_client.chat.completions.create(
                model=vision_model,
                messages=[
                    {"role": "system", "content": (
                        f"You are an image classifier, OCR, and translation assistant.\n\n"
                        f"STEP 1 - CLASSIFY the image into exactly one of three categories:\n"
                        f"  'text' = The image is primarily a TEXT DOCUMENT. Examples:\n"
                        f"    - A scanned document, article, book page, essay, letter\n"
                        f"    - A screenshot of text, chat messages, code\n"
                        f"    - A reading passage written on any background (even on a photo)\n"
                        f"    - Any image where the MAIN PURPOSE is to convey text/paragraphs to read\n"
                        f"  'image' = The image is primarily a VISUAL DESIGN with some text. Examples:\n"
                        f"    - A poster, banner, advertisement, flyer\n"
                        f"    - An infographic, diagram, chart with labels\n"
                        f"    - A photo with captions or watermarks\n"
                        f"    - A logo, title card, social media graphic\n"
                        f"    - Any image where the MAIN PURPOSE is visual/graphical and text is decorative\n\n"
                        f"  'both' = MIXED content where both outputs are useful. Examples:\n"
                        f"    - A visual design that also contains long readable paragraphs\n"
                        f"    - A mixed document where you should keep design and still provide editable text\n"
                        f"    - Cases where classification between text/image is ambiguous\n\n"
                        f"STEP 2 - Extract ALL visible text exactly as it appears.\n"
                        f"STEP 3 - Translate the extracted text to {target_name}.\n\n"
                        f"Return in this EXACT format (markers must be on their own lines):\n"
                        f"IMAGE_TYPE_START\n<text or image or both>\nIMAGE_TYPE_END\n"
                        f"OCR_TEXT_START\n<extracted original text>\nOCR_TEXT_END\n"
                        f"TRANSLATED_START\n<translated text>\nTRANSLATED_END"
                    )},
                    {"role": "user", "content": [
                        {"type": "image_url", "image_url": {"url": data_uri}},
                        {"type": "text", "text": f"Classify this image, extract all text, and translate to {target_name}."},
                    ]},
                ],
                max_tokens=3000,
                temperature=0,
            )
            content = (response.choices[0].message.content or "").strip()

            # Parse structured output
            ocr_text = ""
            translated_text = ""
            recommended_mode = "text"  # safe default
            import re as _re

            # Parse image type classification
            type_match = _re.search(r'IMAGE_TYPE_START\s*\n\s*(text|image|both)\s*\n\s*IMAGE_TYPE_END', content, _re.IGNORECASE)
            if type_match:
                recommended_mode = type_match.group(1).strip().lower()
                if recommended_mode not in ('text', 'image', 'both'):
                    recommended_mode = 'text'

            ocr_match = _re.search(r'OCR_TEXT_START\s*\n(.*?)\nOCR_TEXT_END', content, _re.DOTALL)
            trans_match = _re.search(r'TRANSLATED_START\s*\n(.*?)\nTRANSLATED_END', content, _re.DOTALL)
            if ocr_match:
                ocr_text = ocr_match.group(1).strip()
            if trans_match:
                translated_text = trans_match.group(1).strip()

            # Fallback: if markers weren't followed, try to split by common patterns
            if not ocr_text and not translated_text:
                # Maybe model returned plain text — treat entire response as translated
                ocr_text = content
                try:
                    translated_text = self.translate_text(content, 'auto', target_lang)
                except Exception:
                    translated_text = content

            print(f"[AI Vision] Image classified as '{recommended_mode}': {os.path.basename(image_path)}")
            return (ocr_text, translated_text, recommended_mode)
        except Exception as e:
            raise RuntimeError(f"AI Vision OCR+translate failed: {e}") from e

    def _ai_vision_ocr_bboxes(self, image_path, ocr_langs=None):
        """OCR an image and return line-level bounding boxes using the AI Vision model.

        Output format (Python):
            [{'text': '...', 'bbox': [x0, y0, x1, y1]}, ...]

        Where bbox coordinates are NORMALIZED floats in [0..1] relative to the image size.
        This makes the result robust even if the provider resizes the image internally.
        """
        if not self.openai_client:
            raise RuntimeError("AI provider not configured")

        import base64
        import json
        import re as _re

        vision_model = (os.getenv('AI_VISION_MODEL') or '').strip()
        if not vision_model:
            main_model = os.getenv('AI_MODEL', '')
            _KNOWN_VISION = (
                'gpt-4o', 'gpt-4-turbo', 'gpt-4-vision', 'gemini', 'claude-3',
                'claude-4', 'llama-4', 'qwen2.5-vl', 'qwen-2.5-vl',
                'pixtral', 'internvl', 'mistral-small', 'gemma-3'
            )
            if any(kw in main_model.lower() for kw in _KNOWN_VISION):
                vision_model = main_model
            else:
                vision_model = 'google/gemini-2.0-flash-001'

        with open(image_path, 'rb') as f:
            raw = f.read()

        ext = os.path.splitext(image_path)[1].lower()
        mime_map = {
            '.png': 'image/png', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg',
            '.gif': 'image/gif', '.bmp': 'image/bmp', '.webp': 'image/webp'
        }
        mime = mime_map.get(ext, 'image/png')
        b64 = base64.b64encode(raw).decode('ascii')
        data_uri = f"data:{mime};base64,{b64}"

        langs_hint = (ocr_langs or os.getenv('OCR_LANGS_DEFAULT') or '').strip()
        if langs_hint:
            langs_hint = f"OCR languages hint: {langs_hint}. "

        system_prompt = (
            "You are an OCR system that returns line-level bounding boxes. "
            "Extract ALL visible text lines from the image and return JSON only. "
            "For each line, return: text and bbox. "
            "bbox MUST be normalized floats [x0,y0,x1,y1] in range [0,1], "
            "where (0,0) is the top-left of the image and (1,1) is the bottom-right. "
            "Return tight boxes that cover the rendered glyphs for that line. "
            "Do not include empty lines. Do not include any commentary or markdown. "
            f"{langs_hint}"
            "Return this exact JSON structure: {\"lines\": [{\"text\": \"...\", \"bbox\": [0,0,0,0]}]}"
        )

        try:
            response = self.openai_client.chat.completions.create(
                model=vision_model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": [
                        {"type": "image_url", "image_url": {"url": data_uri}},
                        {"type": "text", "text": "Extract text lines with normalized bounding boxes."},
                    ]},
                ],
                max_tokens=3000,
                temperature=0,
            )
            content = (response.choices[0].message.content or '').strip()

            # Extract JSON payload even if wrapped.
            m = _re.search(r'\{.*\}', content, _re.DOTALL)
            if not m:
                raise ValueError("No JSON object found in vision OCR bbox response")
            payload = json.loads(m.group(0))
            lines = payload.get('lines') if isinstance(payload, dict) else None
            if not isinstance(lines, list):
                raise ValueError("Invalid JSON: missing 'lines' array")

            out = []
            for ln in lines:
                if not isinstance(ln, dict):
                    continue
                text = (ln.get('text') or '').strip()
                bb = ln.get('bbox')
                if not text or not isinstance(bb, list) or len(bb) != 4:
                    continue
                try:
                    x0, y0, x1, y1 = [float(v) for v in bb]
                except Exception:
                    continue
                out.append({"text": text, "bbox": [x0, y0, x1, y1]})
            return out
        except Exception as e:
            raise RuntimeError(f"AI Vision OCR bbox failed: {e}") from e

    def ocr_image_to_bboxes(self, image_path, ocr_langs=None):
        """Public hook for FileService: OCR image -> line bboxes (API-based)."""
        return self._ai_vision_ocr_bboxes(image_path, ocr_langs=ocr_langs)

    # ── Tesseract check helper ───────────────────────────────────────────

    def _is_tesseract_available(self):
        """Return True if Tesseract OCR is installed and reachable."""
        try:
            import pytesseract
        except ImportError:
            return False
        import shutil

        env_cmd = os.getenv('TESSERACT_CMD')
        if env_cmd and str(env_cmd).strip():
            cand = str(env_cmd).strip().strip('"')
            if os.path.exists(cand):
                return True
            if shutil.which(cand):
                return True

        try:
            existing = getattr(pytesseract.pytesseract, 'tesseract_cmd', None)
            if existing and str(existing).strip():
                if os.path.exists(str(existing).strip()) or shutil.which(str(existing).strip()):
                    return True
        except Exception:
            pass

        if shutil.which('tesseract'):
            return True

        if os.name == 'nt':
            for p in [
                r"C:\Program Files\Tesseract-OCR\tesseract.exe",
                r"C:\Program Files (x86)\Tesseract-OCR\tesseract.exe",
                r"C:\Tesseract-OCR\tesseract.exe",
            ]:
                if os.path.exists(p):
                    return True
        return False

    # ── Public OCR methods (auto-fallback: Tesseract → AI Vision) ────────

    def ocr_image_to_text(self, image_path, ocr_langs=None):
        """Extract text from an image.

        Strategy:
        1. Try Tesseract OCR if installed (fast, free, offline).
        2. If Tesseract is not available, fall back to AI Vision model
           (uses the configured OpenAI/OpenRouter API — no install needed).
        """
        # ── Attempt Tesseract first ──
        if self._is_tesseract_available():
            try:
                return self._tesseract_ocr(image_path, ocr_langs)
            except Exception as e:
                print(f"Tesseract OCR failed, falling back to AI Vision: {e}")

        # ── Fallback: AI Vision ──
        if self.openai_client:
            print("Using AI Vision for OCR (Tesseract not available)")
            return self._ai_vision_ocr(image_path)

        raise RuntimeError(
            "OCR unavailable: Tesseract is not installed AND no AI provider configured. "
            "Either install Tesseract OCR, or set OPENAI_API_KEY / OPENROUTER_API_KEY in backend/.env."
        )

    def _tesseract_ocr(self, image_path, ocr_langs=None):
        """Extract text from an image using Tesseract OCR (original implementation)."""
        try:
            from PIL import Image
        except Exception as e:
            raise RuntimeError("Pillow is required for OCR. Install 'Pillow'.") from e

        try:
            import pytesseract
        except Exception as e:
            raise RuntimeError("pytesseract is required for OCR. Install 'pytesseract'.") from e

        import shutil

        def _resolve_tesseract_cmd():
            env_cmd = os.getenv('TESSERACT_CMD')
            if env_cmd and str(env_cmd).strip():
                env_cmd = str(env_cmd).strip().strip('"')

            candidates = []
            if env_cmd:
                candidates.append(env_cmd)

            try:
                existing = getattr(pytesseract.pytesseract, 'tesseract_cmd', None)
                if existing and str(existing).strip():
                    candidates.append(str(existing).strip())
            except Exception:
                pass

            found_on_path = shutil.which('tesseract')
            if found_on_path:
                candidates.append(found_on_path)

            if os.name == 'nt':
                candidates.extend([
                    r"C:\\Program Files\\Tesseract-OCR\\tesseract.exe",
                    r"C:\\Program Files (x86)\\Tesseract-OCR\\tesseract.exe",
                    r"C:\\Tesseract-OCR\\tesseract.exe",
                ])

            for cand in candidates:
                if not cand:
                    continue
                cand = str(cand).strip().strip('"')
                if os.path.isabs(cand) or cand.lower().endswith('.exe'):
                    if os.path.exists(cand):
                        return cand
                else:
                    resolved = shutil.which(cand)
                    if resolved:
                        return resolved
            return None

        resolved_cmd = _resolve_tesseract_cmd()
        if resolved_cmd:
            pytesseract.pytesseract.tesseract_cmd = resolved_cmd
        else:
            raise RuntimeError(
                "OCR failed: tesseract is not installed or it's not in your PATH. "
                "Install Tesseract OCR, then either add it to PATH or set TESSERACT_CMD in backend/.env. "
                "Windows example: TESSERACT_CMD=C:\\Program Files\\Tesseract-OCR\\tesseract.exe"
            )

        langs = (ocr_langs or os.getenv('OCR_LANGS_DEFAULT') or 'eng').strip()
        if not langs:
            langs = 'eng'

        try:
            img = Image.open(image_path)
            # Normalize to a mode that OCR handles well
            if img.mode not in ('RGB', 'L'):
                img = img.convert('RGB')
            text = pytesseract.image_to_string(img, lang=langs)
            return text or ""
        except Exception as e:
            raise RuntimeError(f"OCR failed: {e}") from e

    def ocr_translate_overlay(self, image_path, source_lang, target_lang, ocr_langs=None):
        """OCR an image, translate detected text, and render translated text back onto the image.

        Returns: (ocr_text, translated_text, png_bytes, recommended_mode)
              recommended_mode: 'text' or 'image' or 'both'.

                Strategy:
                - Prefer API Vision line-bboxes (when available) to render translated text back
                    into the original image while preserving layout.
                - Otherwise, if Tesseract is available: use bounding-box overlay.
                - If neither is available: fall back to AI Vision banner overlay.
        """
        try:
            from PIL import Image, ImageDraw, ImageFont
        except Exception as e:
            raise RuntimeError("Pillow is required for OCR. Install 'Pillow'.") from e

        target_code = (str(target_lang or '').strip().lower() or 'auto')
        target_base = target_code.split('-')[0]
        # For RTL targets, force AI-vision banner overlay to avoid broken glyph shaping
        # and unstable per-box placement from Tesseract line rendering.
        if target_base in ('ar', 'fa', 'ur', 'he', 'iw'):
            return self._ai_vision_overlay_fallback(image_path, source_lang, target_lang)

        # Prefer API-based bbox OCR for overlay (more portable than Tesseract).
        prefer_api_bbox = str(os.getenv('OCR_OVERLAY_USE_API', '1')).strip().lower() in ('1', 'true', 'yes', 'on')

        img = Image.open(image_path)
        if img.mode not in ('RGB', 'RGBA'):
            img = img.convert('RGB')
        original_rgba = img.convert('RGBA')
        base_img = original_rgba.copy()

        w_img, h_img = base_img.size

        # ---------------- API bbox OCR path (preferred) ----------------
        line_items = None
        if prefer_api_bbox and self.openai_client:
            try:
                bboxes = self._ai_vision_ocr_bboxes(image_path, ocr_langs=ocr_langs)
            except Exception as e:
                bboxes = None
                print(f"[OCR Overlay] API bbox OCR failed, falling back: {e}")

            if bboxes:
                items = []
                for it in bboxes:
                    try:
                        text = (it.get('text') or '').strip()
                        bb = it.get('bbox')
                        if not text or not bb or len(bb) != 4:
                            continue
                        x0, y0, x1, y1 = [float(v) for v in bb]
                        # Clamp
                        x0 = max(0.0, min(1.0, x0))
                        y0 = max(0.0, min(1.0, y0))
                        x1 = max(0.0, min(1.0, x1))
                        y1 = max(0.0, min(1.0, y1))
                        if x1 <= x0 or y1 <= y0:
                            continue

                        l = int(x0 * w_img)
                        t = int(y0 * h_img)
                        r = int(x1 * w_img)
                        b = int(y1 * h_img)

                        # Basic sanity filter
                        bw = max(1, r - l)
                        bh = max(1, b - t)
                        if bh > int(h_img * 0.25):
                            continue
                        if len(text) > 260:
                            continue

                        items.append({
                            'text': text,
                            'left': max(0, l),
                            'top': max(0, t),
                            'right': min(w_img, r),
                            'bottom': min(h_img, b),
                            'mean_conf': 100.0,
                        })
                    except Exception:
                        continue

                # Sort top-to-bottom then left-to-right
                if items:
                    line_items = sorted(items, key=lambda x: (x['top'], x['left']))

        # ---------------- Tesseract bbox OCR path (fallback) ----------------
        if line_items is None:
            # ── Check if Tesseract is available ──
            if not self._is_tesseract_available():
                # Use AI Vision banner fallback
                return self._ai_vision_overlay_fallback(image_path, source_lang, target_lang)

            try:
                import pytesseract
                from pytesseract import Output
            except Exception:
                return self._ai_vision_overlay_fallback(image_path, source_lang, target_lang)

            # Reuse existing resolver logic by calling _tesseract_ocr once to validate tesseract availability.
            # This also sets pytesseract.pytesseract.tesseract_cmd if needed.
            try:
                _ = self._tesseract_ocr(image_path, ocr_langs=ocr_langs)
            except Exception:
                return self._ai_vision_overlay_fallback(image_path, source_lang, target_lang)

            langs = (ocr_langs or os.getenv('OCR_LANGS_DEFAULT') or 'eng').strip() or 'eng'

        if line_items is None:
            # OCR pass: use multi-PSM + upscale to catch small/header text,
            # but keep Tesseract's own line grouping to avoid merging many lines into one.
            def _run_ocr_lines(psm, pil_rgb, scale):
                config = f"--oem 3 --psm {int(psm)} -c preserve_interword_spaces=1"
                d = pytesseract.image_to_data(
                    pil_rgb,
                    lang=langs,
                    config=config,
                    output_type=Output.DICT,
                )
                n_ = len(d.get('text', []) or [])
                lines = {}
                for i in range(n_):
                    word = (d['text'][i] or '').strip()
                    if not word:
                        continue
                    try:
                        conf = float(d.get('conf', [])[i]) if d.get('conf') else -1
                    except Exception:
                        conf = -1

                    left = int(d.get('left', [0])[i] or 0)
                    top = int(d.get('top', [0])[i] or 0)
                    width = int(d.get('width', [0])[i] or 0)
                    height = int(d.get('height', [0])[i] or 0)
                    right = left + width
                    bottom = top + height

                    if scale and scale != 1.0:
                        left = int(left / scale)
                        top = int(top / scale)
                        right = int(right / scale)
                        bottom = int(bottom / scale)

                    key = (
                        int(d.get('block_num', [0])[i] or 0),
                        int(d.get('par_num', [0])[i] or 0),
                        int(d.get('line_num', [0])[i] or 0),
                    )
                    entry = lines.get(key)
                    if not entry:
                        lines[key] = {
                            'tokens': [(word, conf)],
                            'left': left,
                            'top': top,
                            'right': right,
                            'bottom': bottom,
                        }
                    else:
                        entry['tokens'].append((word, conf))
                        entry['left'] = min(entry['left'], left)
                        entry['top'] = min(entry['top'], top)
                        entry['right'] = max(entry['right'], right)
                        entry['bottom'] = max(entry['bottom'], bottom)

                out_lines = []
                for _, entry in lines.items():
                    tokens = entry.get('tokens') or []
                    # Prefer higher-confidence tokens for translation text, but keep bbox regardless.
                    hi = [w for (w, c) in tokens if c == -1 or c >= 30]
                    mid = [w for (w, c) in tokens if c == -1 or c >= 15]
                    words_for_text = hi or mid
                    text_line = ' '.join(words_for_text).strip()
                    if not text_line:
                        continue
                    confs = [c for (_, c) in tokens if c != -1]
                    mean_conf = (sum(confs) / len(confs)) if confs else 100.0
                    out_lines.append({
                        'text': text_line,
                        'left': int(entry['left']),
                        'top': int(entry['top']),
                        'right': int(entry['right']),
                        'bottom': int(entry['bottom']),
                        'mean_conf': float(mean_conf),
                    })
                return out_lines

            # Upscale for OCR if the image is not huge (helps catch small text)
            ocr_rgb = original_rgba.convert('RGB')
            w0, h0 = ocr_rgb.size
            scale = 1.0
            try:
                if max(w0, h0) < 1600:
                    scale = 2.0
                    ocr_rgb = ocr_rgb.resize((int(w0 * scale), int(h0 * scale)), resample=Image.BICUBIC)
            except Exception:
                scale = 1.0

            candidates = []
            for psm in (6, 11):
                try:
                    candidates.extend(_run_ocr_lines(psm, ocr_rgb, scale))
                except Exception:
                    continue

            # Deduplicate overlapping lines between PSM passes (keep higher mean_conf)
            def _iou(a, b):
                ax1, ay1, ax2, ay2 = a
                bx1, by1, bx2, by2 = b
                ix1 = max(ax1, bx1)
                iy1 = max(ay1, by1)
                ix2 = min(ax2, bx2)
                iy2 = min(ay2, by2)
                iw = max(0, ix2 - ix1)
                ih = max(0, iy2 - iy1)
                inter = iw * ih
                if inter <= 0:
                    return 0.0
                area_a = max(1, (ax2 - ax1) * (ay2 - ay1))
                area_b = max(1, (bx2 - bx1) * (by2 - by1))
                return inter / float(area_a + area_b - inter)

            filtered = []
            candidates.sort(key=lambda x: (-(x.get('mean_conf') or 0.0), x['top'], x['left']))
            for it in candidates:
                l, t, r, b = it['left'], it['top'], it['right'], it['bottom']
                # Quality gates: avoid "giant merged blocks" that will destroy the image.
                bw = max(1, r - l)
                bh = max(1, b - t)
                if bh > int(h_img * 0.22):
                    continue
                if len(it.get('text', '')) > 220:
                    continue
                text_trim = (it.get('text', '') or '').strip()
                # Ignore tiny noisy OCR fragments (e.g., random single letters on banners)
                if len(text_trim) <= 2 and not re.search(r'[0-9%$€£¥]', text_trim):
                    continue
                if len(text_trim) <= 4 and (it.get('mean_conf') or 0.0) < 45.0:
                    continue
                if (it.get('mean_conf') or 0.0) < 18.0 and len(it.get('text', '')) > 18:
                    continue

                bbox = (l, t, r, b)
                dup = False
                for keep in filtered:
                    kb = (keep['left'], keep['top'], keep['right'], keep['bottom'])
                    if _iou(bbox, kb) >= 0.72:
                        dup = True
                        break
                if not dup:
                    filtered.append(it)

            # Sort top-to-bottom, then left-to-right
            line_items = sorted(filtered, key=lambda x: (x['top'], x['left']))

        def _pick_font(font_size):
            font_path = (os.getenv('OCR_FONT_PATH') or '').strip() or None
            candidates = []
            if font_path:
                candidates.append(font_path)
            if os.name == 'nt':
                candidates.extend([
                    r"C:\\Windows\\Fonts\\arialbd.ttf",
                    r"C:\\Windows\\Fonts\\arial.ttf",
                    r"C:\\Windows\\Fonts\\segoeui.ttf",
                    r"C:\\Windows\\Fonts\\tahoma.ttf",
                    r"C:\\Windows\\Fonts\\msyh.ttc",
                    r"C:\\Windows\\Fonts\\msyhbd.ttc",
                    r"C:\\Windows\\Fonts\\simhei.ttf",
                    r"C:\\Windows\\Fonts\\simsun.ttc",
                    r"C:\\Windows\\Fonts\\meiryo.ttc",
                ])
            candidates.extend([
                "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
                "/usr/share/fonts/truetype/noto/NotoSansCJK-Bold.ttc",
                "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
                "/usr/share/fonts/truetype/freefont/FreeSans.ttf",
            ])
            for p in candidates:
                try:
                    if p and os.path.exists(p):
                        return ImageFont.truetype(p, font_size)
                except Exception:
                    continue
            return ImageFont.load_default()

        def _measure(draw, font, text):
            try:
                box = draw.textbbox((0, 0), text, font=font)
                return (box[2] - box[0], box[3] - box[1])
            except Exception:
                try:
                    return draw.textsize(text, font=font)
                except Exception:
                    return (len(text) * font.size, font.size)

        def _fit_font(draw, text, max_w, max_h):
            size = max(10, int(max_h * 0.85))
            for s in range(size, 9, -2):
                font = _pick_font(s)
                w, h = _measure(draw, font, text)
                if w <= max_w and h <= max_h:
                    return font
            return _pick_font(10)

        draw = ImageDraw.Draw(base_img)
        ocr_lines = []
        translated_lines = []

        leader_re = re.compile(r"(\.{5,}|_{4,}|-{4,})")

        def _translate_preserve_form_leaders(text_line: str) -> str:
            raw = (text_line or '')
            if not raw.strip():
                return raw
            # If the whole line is just leaders/punct/whitespace, keep as-is.
            if leader_re.fullmatch(raw.strip()):
                return raw
            if not leader_re.search(raw):
                return self.translate_text(raw, source_lang, target_lang)

            parts = leader_re.split(raw)
            out_parts = []
            for i, part in enumerate(parts):
                if i % 2 == 1:
                    out_parts.append(part)
                    continue

                seg = part or ''
                if not seg.strip():
                    out_parts.append(seg)
                    continue
                # Skip translating segments that have no letters/numbers (avoid punctuation-only noise)
                if not re.search(r"[\w\u00C0-\u1EF9]", seg, flags=re.UNICODE):
                    out_parts.append(seg)
                    continue
                try:
                    out_parts.append(self.translate_text(seg, source_lang, target_lang))
                except Exception:
                    out_parts.append(seg)
            return ''.join(out_parts)

        def _clamp(v, lo, hi):
            return max(lo, min(hi, v))

        def _sample_bg_color(img_rgba, l, t, r, b):
            """Sample background color around a text box.

            We sample a few pixels just outside the OCR box corners to approximate
            the surrounding background. This avoids painting white blocks on dark
            backgrounds.
            """
            try:
                w, h = img_rgba.size
                # sample points just outside the box
                pts = [
                    (_clamp(l - 2, 0, w - 1), _clamp(t - 2, 0, h - 1)),
                    (_clamp(r + 2, 0, w - 1), _clamp(t - 2, 0, h - 1)),
                    (_clamp(l - 2, 0, w - 1), _clamp(b + 2, 0, h - 1)),
                    (_clamp(r + 2, 0, w - 1), _clamp(b + 2, 0, h - 1)),
                ]
                cols = []
                for x, y in pts:
                    px = img_rgba.getpixel((x, y))
                    if isinstance(px, int):
                        cols.append((px, px, px))
                    else:
                        cols.append((int(px[0]), int(px[1]), int(px[2])))
                if not cols:
                    return (255, 255, 255)
                rr = sum(c[0] for c in cols) // len(cols)
                gg = sum(c[1] for c in cols) // len(cols)
                bb = sum(c[2] for c in cols) // len(cols)
                return (rr, gg, bb)
            except Exception:
                return (255, 255, 255)

        def _pick_text_colors(bg_rgb):
            # Choose text/stroke colors based on background luminance
            try:
                lum = (0.2126 * bg_rgb[0] + 0.7152 * bg_rgb[1] + 0.0722 * bg_rgb[2])
            except Exception:
                lum = 255
            if lum < 128:
                # dark bg -> light text with dark stroke
                return ((255, 255, 255, 255), (0, 0, 0, 255))
            # light bg -> dark text with light stroke
            return ((0, 0, 0, 255), (255, 255, 255, 255))
        def _wrap_text_to_width(draw_obj, font_obj, text, max_w):
            # Greedy wrap by spaces; if a single word is too long, keep it as-is.
            words = (text or '').split()
            if not words:
                return ['']
            lines_out = []
            cur = words[0]
            for w in words[1:]:
                cand = cur + ' ' + w
                cw, _ = _measure(draw_obj, font_obj, cand)
                if cw <= max_w:
                    cur = cand
                else:
                    lines_out.append(cur)
                    cur = w
            lines_out.append(cur)
            return lines_out

        def _fit_font_wrapped(draw_obj, text, max_w, max_h):
            # Find largest font where wrapped text fits within (max_w,max_h)
            size = max(10, int(max_h * 0.85))
            for s in range(size, 9, -2):
                font = _pick_font(s)
                lines_ = _wrap_text_to_width(draw_obj, font, text, max_w)
                # Measure total height
                widths = []
                heights = []
                for ln in lines_:
                    w, h = _measure(draw_obj, font, ln)
                    widths.append(w)
                    heights.append(h)
                total_h = (sum(heights) + max(0, (len(heights) - 1) * int(s * 0.18)))
                max_line_w = max(widths) if widths else 0
                if max_line_w <= max_w and total_h <= max_h:
                    return font, lines_, total_h
            font = _pick_font(10)
            lines_ = _wrap_text_to_width(draw_obj, font, text, max_w)
            # best-effort height
            heights = [_measure(draw_obj, font, ln)[1] for ln in lines_]
            total_h = (sum(heights) + max(0, (len(heights) - 1) * int(font.size * 0.18)))
            return font, lines_, total_h

        # 1) Remove original text as naturally as possible.
        # Prefer OpenCV inpainting (keeps background texture), fallback to soft rectangle fill.
        use_inpaint = False
        inpainted_rgb = None
        try:
            import numpy as np  # type: ignore
            import cv2  # type: ignore

            if line_items:
                w_img, h_img = base_img.size
                mask = np.zeros((h_img, w_img), dtype=np.uint8)
                # Larger pad helps avoid leaving edge artifacts (especially for the first/header line)
                for item in line_items:
                    ih = max(1, int(item['bottom']) - int(item['top']))
                    pad = max(8, int(ih * 0.35))
                    l = max(0, int(item['left']) - pad)
                    t = max(0, int(item['top']) - pad)
                    r = min(w_img, int(item['right']) + pad)
                    b = min(h_img, int(item['bottom']) + pad)
                    if r > l and b > t:
                        mask[t:b, l:r] = 255

                # Dilate mask slightly to cover anti-aliased edges
                try:
                    kernel = np.ones((3, 3), np.uint8)
                    mask = cv2.dilate(mask, kernel, iterations=1)
                except Exception:
                    pass

                # Convert to BGR for OpenCV
                rgb = np.array(original_rgba.convert('RGB'))
                bgr = cv2.cvtColor(rgb, cv2.COLOR_RGB2BGR)
                # Inpaint to fill text regions from surrounding pixels
                inpainted = cv2.inpaint(bgr, mask, 4, cv2.INPAINT_TELEA)
                inpainted_rgb = cv2.cvtColor(inpainted, cv2.COLOR_BGR2RGB)
                use_inpaint = True
        except Exception:
            use_inpaint = False

        if use_inpaint and inpainted_rgb is not None:
            try:
                base_img = Image.fromarray(inpainted_rgb).convert('RGBA')
                draw = ImageDraw.Draw(base_img)
            except Exception:
                base_img = original_rgba.copy()
                draw = ImageDraw.Draw(base_img)

        for item in line_items:
            src_text = item['text']
            # Translate per line to keep layout stable
            try:
                dst_text = _translate_preserve_form_leaders(src_text)
            except Exception:
                dst_text = src_text

            ocr_lines.append(src_text)
            translated_lines.append(dst_text)

            l = max(0, item['left'])
            t = max(0, item['top'])
            r = min(base_img.size[0], item['right'])
            b = min(base_img.size[1], item['bottom'])
            box_w = max(1, r - l)
            box_h = max(1, b - t)

            # 2) If OpenCV isn't available, fallback: softly cover region with sampled bg
            if not use_inpaint:
                pad = 6
                rect = (
                    max(0, l - pad),
                    max(0, t - pad),
                    min(base_img.size[0], r + pad),
                    min(base_img.size[1], b + pad),
                )
                bg = _sample_bg_color(base_img, l, t, r, b)
                draw.rectangle(rect, fill=(bg[0], bg[1], bg[2], 245))

            bg = _sample_bg_color(base_img, l, t, r, b)
            fill, stroke = _pick_text_colors(bg)

            # 3) Fit + wrap translated text to the detected box
            font, lines_wrapped, total_h = _fit_font_wrapped(draw, dst_text, max_w=box_w, max_h=box_h)
            line_gap = int(max(1, font.size * 0.18))
            y = t + max(0, int((box_h - total_h) / 2))
            for ln in lines_wrapped:
                try:
                    draw.text((l, y), ln, fill=fill, font=font, stroke_width=2, stroke_fill=stroke)
                except TypeError:
                    draw.text((l, y), ln, fill=fill, font=font)
                y += _measure(draw, font, ln)[1] + line_gap

        out = base_img.convert('RGB')
        import io
        buf = io.BytesIO()
        out.save(buf, format='PNG')
        png_bytes = buf.getvalue()
        ocr_full_text = "\n".join(ocr_lines).strip()
        non_empty_lines = [ln for ln in ocr_full_text.splitlines() if (ln or '').strip()]
        char_count = len(ocr_full_text)
        # Heuristic mode recommendation for Tesseract path (no AI classifier here)
        # NOTE: auto should prefer a single mode per image (text or image),
        # not "both" on the same image.
        try:
            img_area = max(1, base_img.size[0] * base_img.size[1])
            text_area = 0
            for item in line_items:
                text_area += max(1, (int(item['right']) - int(item['left'])) * (int(item['bottom']) - int(item['top'])))
            coverage = min(1.0, float(text_area) / float(img_area))
        except Exception:
            coverage = 0.0

        words_per_line = []
        for ln in non_empty_lines:
            try:
                words_per_line.append(len(re.findall(r'\w+', ln, flags=re.UNICODE)))
            except Exception:
                words_per_line.append(len((ln or '').split()))
        avg_words_per_line = (sum(words_per_line) / len(words_per_line)) if words_per_line else 0.0

        if not ocr_full_text:
            recommended_mode = 'text'
        elif coverage >= 0.42 and (char_count >= 120 or avg_words_per_line >= 5.0):
            recommended_mode = 'text'
        elif coverage <= 0.28:
            recommended_mode = 'image'
        else:
            # Ambiguous/mixed image blocks (photo + text) -> keep visual design
            recommended_mode = 'image'
        return (ocr_full_text, "\n".join(translated_lines).strip(), png_bytes, recommended_mode)

    def _ai_vision_overlay_fallback(self, image_path, source_lang, target_lang):
        """Fallback OCR+translate+overlay using AI Vision (no Tesseract).

        Since we don't have bounding boxes, we render translated text as a
        clean semi-transparent banner overlay at the bottom of the original image.
        Returns: (ocr_text, translated_text, png_bytes, recommended_mode)
        """
        from PIL import Image, ImageDraw, ImageFont
        import io as _io

        # Do OCR + translate + classify in one AI call
        ocr_text, translated_text, recommended_mode = self._ai_vision_ocr_and_translate(image_path, target_lang)

        if not ocr_text and not translated_text:
            with open(image_path, 'rb') as f:
                raw = f.read()
            return ("", "", raw, recommended_mode)

        # Load original image and keep same dimensions
        img = Image.open(image_path).convert('RGBA')
        w, h = img.size

        # ── Font selection (supports Vietnamese/CJK) ──
        def _pick_font(size):
            # Prefer fonts with broad Unicode coverage (Vietnamese diacritics, CJK, etc.)
            candidates = []
            if os.name == 'nt':
                candidates.extend([
                    r"C:\Windows\Fonts\arialbd.ttf",    # Arial Bold
                    r"C:\Windows\Fonts\arial.ttf",
                    r"C:\Windows\Fonts\segoeui.ttf",
                    r"C:\Windows\Fonts\tahoma.ttf",
                    r"C:\Windows\Fonts\msyh.ttc",       # Microsoft YaHei (CJK)
                    r"C:\Windows\Fonts\meiryo.ttc",
                ])
            candidates.extend([
                "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
                "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
                "/usr/share/fonts/truetype/freefont/FreeSansBold.ttf",
                "/usr/share/fonts/truetype/noto/NotoSans-Regular.ttf",
            ])
            for p in candidates:
                try:
                    if p and os.path.exists(p):
                        return ImageFont.truetype(p, size)
                except Exception:
                    continue
            try:
                return ImageFont.load_default()
            except Exception:
                return ImageFont.load_default()

        # ── Adaptive font size based on image dimensions ──
        # Larger images get larger fonts; minimum 14px, maximum 28px
        font_size = max(14, min(28, int(min(w, h) / 18)))
        font = _pick_font(font_size)
        padding_x = max(12, int(w * 0.03))
        padding_y = max(8, int(h * 0.015))
        max_text_w = w - 2 * padding_x

        # ── Text wrapping ──
        overlay = Image.new('RGBA', (w, h), (0, 0, 0, 0))
        draw = ImageDraw.Draw(overlay)

        def _measure_text(txt, fnt):
            try:
                bx = draw.textbbox((0, 0), txt, font=fnt)
                return bx[2] - bx[0], bx[3] - bx[1]
            except Exception:
                return int(len(txt) * font_size * 0.55), font_size

        def _wrap_text(text, fnt, max_w):
            """Word-wrap with fallback to character-level wrap for long words."""
            result_lines = []
            for paragraph in (text or '').split('\n'):
                if not paragraph.strip():
                    result_lines.append('')
                    continue
                words = paragraph.split()
                if not words:
                    result_lines.append('')
                    continue
                cur = ''
                for wd in words:
                    test = (cur + ' ' + wd).strip() if cur else wd
                    tw, _ = _measure_text(test, fnt)
                    if tw <= max_w:
                        cur = test
                    else:
                        if cur:
                            result_lines.append(cur)
                        # If a single word is wider than max, do char-level break
                        ww, _ = _measure_text(wd, fnt)
                        if ww > max_w:
                            temp = ''
                            for ch in wd:
                                cw, _ = _measure_text(temp + ch, fnt)
                                if cw > max_w and temp:
                                    result_lines.append(temp)
                                    temp = ch
                                else:
                                    temp += ch
                            cur = temp
                        else:
                            cur = wd
                if cur:
                    result_lines.append(cur)
            return result_lines if result_lines else ['']

        wrapped = _wrap_text(translated_text, font, max_text_w)
        line_h = int(font_size * 1.4)

        # ── Calculate banner height ──
        banner_h = len(wrapped) * line_h + 2 * padding_y
        max_banner = int(h * 0.50)  # max 50% of image
        if banner_h > max_banner:
            banner_h = max_banner
            # Truncate lines to fit
            max_lines = max(1, (banner_h - 2 * padding_y) // line_h)
            if len(wrapped) > max_lines:
                wrapped = wrapped[:max_lines - 1] + [wrapped[max_lines - 1] + '...']

        # ── Draw clean banner at bottom ──
        banner_top = h - banner_h
        # Gradient-like effect: two rectangles for depth
        draw.rectangle([(0, banner_top - 4), (w, banner_top)], fill=(0, 0, 0, 60))
        draw.rectangle([(0, banner_top), (w, h)], fill=(0, 0, 0, 190))

        # ── Draw text lines ──
        y = banner_top + padding_y
        for ln in wrapped:
            if y + line_h > h - 4:
                break
            # Subtle text shadow for readability
            try:
                draw.text((padding_x + 1, y + 1), ln, fill=(0, 0, 0, 150), font=font)
                draw.text((padding_x, y), ln, fill=(255, 255, 255, 245), font=font)
            except Exception:
                draw.text((padding_x, y), ln, fill=(255, 255, 255, 245))
            y += line_h

        # ── Composite ──
        result = Image.alpha_composite(img, overlay).convert('RGB')
        buf = _io.BytesIO()
        result.save(buf, format='PNG', quality=95)
        png_bytes = buf.getvalue()
        return (ocr_text.strip(), translated_text.strip(), png_bytes, recommended_mode)

    def _check_provider_available(self):
        """Lightweight preflight check to see if the configured AI provider is available.

        Returns (True, None) if OK, otherwise (False, message) if rate-limited or clearly unavailable.
        Only blocks on CLEAR rate-limit / payment errors. Other errors are allowed through
        since the actual translation call may still succeed.
        """
        if not self.openai_client:
            return (False, 'No AI provider configured: set OPENAI_API_KEY or OPENROUTER_API_KEY')
        try:
            # Call models.list to surface rate-limit errors quickly
            _ = self.openai_client.models.list()
            return (True, None)
        except Exception as e:
            err = str(e).lower()
            if '429' in err or '402' in err or 'rate' in err or 'insufficient' in err or 'free-models' in err or 'credit' in err:
                # Rate-limited or insufficient credits — abort early so background job fails fast
                print(f"AI provider preflight check indicates rate limit/insufficient credits: {e}.")
                return (False, str(e))
            # Non-rate errors: DON'T block — models.list() can fail on some providers
            # but actual chat completions may still work fine.
            print(f"AI provider preflight warning (proceeding anyway): {e}")
            return (True, None)
    def translate_document_background(self, file_path, target_lang, user_id=None, *, ocr_images=False, ocr_langs=None, ocr_mode=None, bilingual_mode=None, bilingual_delimiter=None, pdf_docx_pipeline=None, pipeline=None):
        job_id = str(uuid.uuid4())
        disable_fallback = (os.getenv('AI_DISABLE_FALLBACK') or '').strip().lower() in ('1', 'true', 'yes', 'on')
        self.jobs[job_id] = {
            'status': 'pending',
            'progress': 0,
            'message': 'Queued',
            'download_path': None,
            'error': None,
            'user_id': user_id
        }
        # Preflight provider availability: fail early for rate limit/insufficient credits
        available, message = self._check_provider_available()
        if not available:
            if disable_fallback:
                # API-only mode: fail fast and do not start a background worker.
                self.jobs[job_id]['status'] = 'failed'
                self.jobs[job_id]['progress'] = 0
                self.jobs[job_id]['error'] = str(message)
                err_low = str(message).lower()
                if '402' in err_low or 'insufficient' in err_low or 'credit' in err_low:
                    self.jobs[job_id]['message'] = 'Failed - Insufficient credits'
                elif '429' in err_low or 'rate' in err_low or 'too many requests' in err_low:
                    self.jobs[job_id]['message'] = 'Failed - Rate limited'
                else:
                    self.jobs[job_id]['message'] = 'Failed - AI provider unavailable'
                return job_id

            # Fallback-enabled mode: proceed and allow Google/MyMemory to complete.
            self.jobs[job_id]['message'] = 'Starting with fallback translators (AI limited)'
            self.jobs[job_id]['error'] = str(message)

        def _worker(job_id, file_path, target_lang, ocr_images, ocr_langs, ocr_mode, bilingual_mode, bilingual_delimiter, pdf_docx_pipeline, pipeline):
            try:
                self.jobs[job_id]['status'] = 'in_progress'
                self.jobs[job_id]['progress'] = 5
                self.jobs[job_id]['message'] = 'Starting'

                ocr_done_message = None
                ocr_skip_message = None

                def progress_cb(percent, msg=''):
                    self.jobs[job_id]['progress'] = max(0, min(100, int(percent)))
                    self.jobs[job_id]['message'] = msg

                    nonlocal ocr_done_message, ocr_skip_message
                    try:
                        s = str(msg or '')
                    except Exception:
                        s = ''
                    if s.startswith('DOCX OCR'):
                        ocr_done_message = s
                    if s.startswith('Skipping DOCX image OCR'):
                        ocr_skip_message = s

                # Pipeline selection
                chosen = (pipeline or os.getenv('DOCUMENT_PIPELINE', 'v2')).strip().lower()
                if chosen not in ('v2', 'legacy'):
                    chosen = 'v2'

                if chosen == 'v2':
                    v2 = DocumentPipelineV2()
                    output_path = v2.process(
                        file_path,
                        target_lang,
                        progress_cb=progress_cb,
                        ocr_langs=ocr_langs,
                        bilingual_mode=bilingual_mode,
                        bilingual_delimiter=bilingual_delimiter,
                    )
                else:
                    # Legacy pipeline (kept for compatibility)
                    output_path = self.file_service.process_document(
                        file_path,
                        target_lang,
                        progress_callback=progress_cb,
                        ocr_images=bool(ocr_images),
                        ocr_langs=ocr_langs,
                        ocr_mode=ocr_mode,
                        bilingual_mode=bilingual_mode,
                        bilingual_delimiter=bilingual_delimiter,
                        pdf_docx_pipeline=pdf_docx_pipeline,
                    )

                # Validate and normalize output to backend/downloads so the /downloads route can serve it.
                if not output_path or not str(output_path).strip():
                    raise RuntimeError("Document pipeline returned empty output path")

                output_path = str(output_path)
                if not os.path.exists(output_path):
                    raise RuntimeError(f"Output file was not created: {output_path}")

                backend_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
                downloads_dir = os.path.join(backend_dir, 'downloads')
                os.makedirs(downloads_dir, exist_ok=True)

                # If the pipeline produced a file outside downloads, copy it in so downloading works.
                try:
                    out_abs = os.path.abspath(output_path)
                    dl_abs = os.path.abspath(downloads_dir)
                    in_downloads = os.path.commonpath([out_abs, dl_abs]) == dl_abs
                except Exception:
                    in_downloads = False

                if not in_downloads:
                    base_name = os.path.basename(output_path)
                    dest_path = os.path.join(downloads_dir, base_name)
                    if os.path.exists(dest_path):
                        root, ext = os.path.splitext(base_name)
                        dest_path = os.path.join(downloads_dir, f"{root}_{uuid.uuid4().hex[:8]}{ext}")
                    shutil.copy2(output_path, dest_path)
                    output_path = dest_path

                self.jobs[job_id]['download_path'] = output_path

                # Sidecar OCR text export is no longer generated (text inserted into DOCX directly).

                # Persist OCR summary for status endpoint / debugging
                if ocr_images:
                    if ocr_done_message:
                        self.jobs[job_id]['ocr_summary'] = ocr_done_message
                    if ocr_skip_message:
                        self.jobs[job_id]['ocr_skipped'] = ocr_skip_message

                # Detect fallback: if output extension != original extension -> it's a fallback
                try:
                    orig_ext = os.path.splitext(file_path)[1].lower()
                    out_ext = os.path.splitext(output_path)[1].lower()
                    if out_ext and orig_ext and out_ext != orig_ext:
                        # Expected output change when explicitly using PDF->DOCX pipeline.
                        if bool(pdf_docx_pipeline) and orig_ext == '.pdf' and out_ext == '.docx':
                            self.jobs[job_id]['fallback'] = False
                            self.jobs[job_id]['output_format'] = 'docx'
                            self.jobs[job_id]['message'] = 'Completed (DOCX for best format preservation)'
                        else:
                            self.jobs[job_id]['fallback'] = True
                            self.jobs[job_id]['fallback_reason'] = f"Output changed from {orig_ext} to {out_ext}"
                            # Keep OCR summary visible if available
                            if ocr_images and self.jobs[job_id].get('ocr_summary'):
                                self.jobs[job_id]['message'] = f"Completed with fallback — {self.jobs[job_id]['ocr_summary']}"
                            elif ocr_images and self.jobs[job_id].get('ocr_skipped'):
                                self.jobs[job_id]['message'] = f"Completed with fallback — {self.jobs[job_id]['ocr_skipped']}"
                            else:
                                self.jobs[job_id]['message'] = 'Completed with fallback'
                    else:
                        self.jobs[job_id]['fallback'] = False
                        if ocr_images and self.jobs[job_id].get('ocr_summary'):
                            self.jobs[job_id]['message'] = f"Completed — {self.jobs[job_id]['ocr_summary']}"
                        elif ocr_images and self.jobs[job_id].get('ocr_skipped'):
                            self.jobs[job_id]['message'] = f"Completed — {self.jobs[job_id]['ocr_skipped']}"
                        else:
                            self.jobs[job_id]['message'] = 'Completed'
                except Exception:
                    self.jobs[job_id]['fallback'] = False
                    if ocr_images and self.jobs[job_id].get('ocr_summary'):
                        self.jobs[job_id]['message'] = f"Completed — {self.jobs[job_id]['ocr_summary']}"
                    elif ocr_images and self.jobs[job_id].get('ocr_skipped'):
                        self.jobs[job_id]['message'] = f"Completed — {self.jobs[job_id]['ocr_skipped']}"
                    else:
                        self.jobs[job_id]['message'] = 'Completed'

                self.jobs[job_id]['progress'] = 100
                self.jobs[job_id]['status'] = 'completed'
            except (ProviderRateLimitError, ProviderRateLimitErrorV2) as e:
                self.jobs[job_id]['status'] = 'failed'
                self.jobs[job_id]['error'] = str(e)
                err_low = str(e).lower()
                if '402' in err_low or 'insufficient' in err_low or 'credit' in err_low:
                    self.jobs[job_id]['message'] = 'Failed - Insufficient credits'
                elif '429' in err_low or 'rate' in err_low or 'too many requests' in err_low:
                    self.jobs[job_id]['message'] = 'Failed - Rate limited'
                else:
                    self.jobs[job_id]['message'] = 'Failed - AI provider unavailable'
            except Exception as e:
                import traceback
                traceback.print_exc()
                print(f"[DOCUMENT TRANSLATION ERROR] job={job_id}: {e}")
                self.jobs[job_id]['status'] = 'failed'
                self.jobs[job_id]['error'] = str(e)
                self.jobs[job_id]['message'] = 'Failed'

        thread = threading.Thread(target=_worker, args=(job_id, file_path, target_lang, ocr_images, ocr_langs, ocr_mode, bilingual_mode, bilingual_delimiter, pdf_docx_pipeline, pipeline), daemon=True)
        thread.start()
        return job_id

    def get_job(self, job_id):
        return self.jobs.get(job_id)