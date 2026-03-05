"""Shared types/exceptions for the document_v2 translation pipeline."""

from __future__ import annotations


class ProviderRateLimitError(Exception):
    """Upstream provider indicates a hard rate limit or insufficient credits."""


class TranslationTimeoutError(TimeoutError):
    """Network/provider timeout while translating text."""
