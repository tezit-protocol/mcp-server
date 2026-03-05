"""Tests for the in-memory token store."""

import time
from unittest.mock import patch

from tez_server.token_store import TokenStore


class TestTokenStoreCreate:
    def test_returns_hex_string(self) -> None:
        store = TokenStore()
        token = store.create({"key": "value"})

        assert isinstance(token, str)
        assert len(token) == 32
        int(token, 16)  # must be valid hex

    def test_returns_unique_tokens(self) -> None:
        store = TokenStore()
        tokens = {store.create({"i": i}) for i in range(100)}
        assert len(tokens) == 100


class TestTokenStoreExchange:
    def test_returns_payload(self) -> None:
        store = TokenStore()
        payload = {"upload_urls": {"file.md": "https://example.com"}}
        token = store.create(payload)

        result = store.exchange(token)

        assert result == payload

    def test_single_use(self) -> None:
        store = TokenStore()
        token = store.create({"key": "value"})

        first = store.exchange(token)
        second = store.exchange(token)

        assert first is not None
        assert second is None

    def test_unknown_token_returns_none(self) -> None:
        store = TokenStore()
        assert store.exchange("nonexistent") is None

    def test_expired_token_returns_none(self) -> None:
        store = TokenStore(default_ttl=1)
        now = time.monotonic()

        with patch("tez_server.token_store.time") as mock_time:
            mock_time.monotonic.return_value = now
            token = store.create({"key": "value"})

        with patch("tez_server.token_store.time") as mock_time:
            mock_time.monotonic.return_value = now + 2
            result = store.exchange(token)

        assert result is None

    def test_custom_ttl_per_token(self) -> None:
        store = TokenStore(default_ttl=900)
        now = time.monotonic()

        with patch("tez_server.token_store.time") as mock_time:
            mock_time.monotonic.return_value = now
            token = store.create({"key": "value"}, ttl=1)

        with patch("tez_server.token_store.time") as mock_time:
            mock_time.monotonic.return_value = now + 2
            result = store.exchange(token)

        assert result is None

    def test_valid_token_before_expiry(self) -> None:
        store = TokenStore(default_ttl=900)
        now = time.monotonic()

        with patch("tez_server.token_store.time") as mock_time:
            mock_time.monotonic.return_value = now
            token = store.create({"key": "value"})

        with patch("tez_server.token_store.time") as mock_time:
            mock_time.monotonic.return_value = now + 100
            result = store.exchange(token)

        assert result == {"key": "value"}


class TestTokenStorePurge:
    def test_expired_tokens_purged_on_create(self) -> None:
        store = TokenStore(default_ttl=1)
        now = time.monotonic()

        with patch("tez_server.token_store.time") as mock_time:
            mock_time.monotonic.return_value = now
            store.create({"old": True})

        with patch("tez_server.token_store.time") as mock_time:
            mock_time.monotonic.return_value = now + 2
            store.create({"new": True})

        assert len(store._store) == 1

    def test_expired_tokens_purged_on_exchange(self) -> None:
        store = TokenStore(default_ttl=1)
        now = time.monotonic()

        with patch("tez_server.token_store.time") as mock_time:
            mock_time.monotonic.return_value = now
            store.create({"old": True})
            token = store.create({"keep": True})

        with patch("tez_server.token_store.time") as mock_time:
            mock_time.monotonic.return_value = now + 2
            store.exchange(token)

        assert len(store._store) == 0


class TestTokenStoreDefaults:
    def test_default_ttl_is_900(self) -> None:
        store = TokenStore()
        assert store._default_ttl == 900

    def test_custom_default_ttl(self) -> None:
        store = TokenStore(default_ttl=60)
        assert store._default_ttl == 60
