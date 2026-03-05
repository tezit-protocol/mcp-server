"""In-memory token store for ephemeral URL exchange.

Tokens are opaque hex strings that map to a JSON-serialisable payload
(typically pre-signed URL dicts). They are single-use and short-lived.

This module is intentionally simple -- no persistence, no distribution.
Suitable for a single-process deployment. For production, swap to Redis
or DynamoDB TTL items.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from secrets import token_hex
from typing import Any

_DEFAULT_TTL_SECONDS = 900  # 15 minutes


@dataclass
class _TokenEntry:
    """A stored token with its payload and expiry time."""

    payload: dict[str, Any]
    expires_at: float


class TokenStore:
    """Thread-safe in-memory token store with TTL.

    Args:
        default_ttl: Default time-to-live in seconds for new tokens.
    """

    def __init__(self, *, default_ttl: int = _DEFAULT_TTL_SECONDS) -> None:
        self._store: dict[str, _TokenEntry] = {}
        self._lock = threading.Lock()
        self._default_ttl = default_ttl

    def create(self, payload: dict[str, Any], *, ttl: int | None = None) -> str:
        """Store a payload and return an opaque token string.

        Args:
            payload: The data to store (must be JSON-serialisable).
            ttl: Time-to-live in seconds. Uses default if not provided.

        Returns:
            A 32-character hex token string.
        """
        token = token_hex(16)
        ttl_seconds = ttl if ttl is not None else self._default_ttl
        entry = _TokenEntry(
            payload=payload,
            expires_at=time.monotonic() + ttl_seconds,
        )
        with self._lock:
            self._purge_expired()
            self._store[token] = entry
        return token

    def exchange(self, token: str) -> dict[str, Any] | None:
        """Exchange a token for its payload (single-use).

        Retrieves and deletes the token atomically. Returns None if the
        token does not exist or has expired.

        Args:
            token: The token string to exchange.

        Returns:
            The stored payload dict, or None.
        """
        with self._lock:
            self._purge_expired()
            entry = self._store.pop(token, None)
        if entry is None:
            return None
        if time.monotonic() > entry.expires_at:
            return None
        return entry.payload

    def _purge_expired(self) -> None:
        """Remove all expired entries. Must be called under lock."""
        now = time.monotonic()
        expired = [k for k, v in self._store.items() if now > v.expires_at]
        for k in expired:
            del self._store[k]
