"""Unit tests for MinIOStorageProvider.

Tests the MinIOStorageProvider in isolation using a mocked minio.Minio client.
No real MinIO instance or network connection is required.

Expected module: tez_server.services.minio_provider
Expected class:  MinIOStorageProvider(client, bucket: str)
"""

from __future__ import annotations

import os
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from minio.error import S3Error

from tez_server.services.minio_provider import MinIOStorageProvider
from tez_server.services.storage import StorageProviderError

TEST_BUCKET = "tez-packages"
TEZ_ID = "abc123"


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _make_provider(mock_client: MagicMock) -> MinIOStorageProvider:
    return MinIOStorageProvider(client=mock_client, bucket=TEST_BUCKET)


def _s3_error(code: str, message: str = "test error") -> S3Error:
    return S3Error(
        code=code,
        message=message,
        resource="/test",
        request_id="req123",
        host_id="host123",
        response=MagicMock(status=400, headers={}, data=b""),
    )


SAMPLE_FILES = [
    {"name": "transcript.md", "size": 1024, "content_type": "text/markdown"},
    {"name": "slides.pdf", "size": 2048, "content_type": "application/pdf"},
]


# ---------------------------------------------------------------------------
# 1. Upload URL generation
# ---------------------------------------------------------------------------
class TestGenerateUploadUrls:
    def test_returns_url_per_file_plus_manifests(self) -> None:
        client = MagicMock()
        client.presigned_put_object.return_value = "http://minio/presigned-put"
        svc = _make_provider(client)

        urls = svc.generate_upload_urls(tez_id=TEZ_ID, files=SAMPLE_FILES)

        expected_keys = {f["name"] for f in SAMPLE_FILES} | {"manifest.json", "tez.md"}
        assert set(urls.keys()) == expected_keys

    def test_urls_are_strings(self) -> None:
        client = MagicMock()
        client.presigned_put_object.return_value = "http://minio/presigned-put"
        svc = _make_provider(client)

        urls = svc.generate_upload_urls(tez_id=TEZ_ID, files=SAMPLE_FILES)

        for url in urls.values():
            assert isinstance(url, str)

    def test_context_files_use_context_prefix(self) -> None:
        client = MagicMock()
        client.presigned_put_object.side_effect = lambda bucket, key, expires: (
            f"http://minio/{key}"
        )
        svc = _make_provider(client)

        urls = svc.generate_upload_urls(tez_id=TEZ_ID, files=SAMPLE_FILES)

        for f in SAMPLE_FILES:
            assert f"{TEZ_ID}/context/{f['name']}" in urls[f["name"]]

    def test_manifest_files_at_tez_root(self) -> None:
        client = MagicMock()
        client.presigned_put_object.side_effect = lambda bucket, key, expires: (
            f"http://minio/{key}"
        )
        svc = _make_provider(client)

        urls = svc.generate_upload_urls(tez_id=TEZ_ID, files=[])

        assert f"{TEZ_ID}/manifest.json" in urls["manifest.json"]
        assert f"{TEZ_ID}/tez.md" in urls["tez.md"]


# ---------------------------------------------------------------------------
# 2. Download URL generation
# ---------------------------------------------------------------------------
class TestGenerateDownloadUrls:
    def test_returns_url_per_file_plus_manifests(self) -> None:
        client = MagicMock()
        client.presigned_get_object.return_value = "http://minio/presigned-get"
        svc = _make_provider(client)

        urls = svc.generate_download_urls(tez_id=TEZ_ID, files=SAMPLE_FILES)

        expected_keys = {f["name"] for f in SAMPLE_FILES} | {"manifest.json", "tez.md"}
        assert set(urls.keys()) == expected_keys

    def test_urls_are_strings(self) -> None:
        client = MagicMock()
        client.presigned_get_object.return_value = "http://minio/presigned-get"
        svc = _make_provider(client)

        urls = svc.generate_download_urls(tez_id=TEZ_ID, files=SAMPLE_FILES)

        for url in urls.values():
            assert isinstance(url, str)


# ---------------------------------------------------------------------------
# 3. validate_uploads — file found
# ---------------------------------------------------------------------------
class TestValidateUploadsFound:
    def test_all_files_present_returns_success(self) -> None:
        client = MagicMock()
        client.stat_object.return_value = MagicMock(etag="abc123etag")
        svc = _make_provider(client)

        result = svc.validate_uploads(tez_id=TEZ_ID, files=SAMPLE_FILES)

        assert result.success is True
        assert result.missing == []
        assert len(result.verified_files) == len(SAMPLE_FILES)
        assert all("etag" in f for f in result.verified_files)


# ---------------------------------------------------------------------------
# 4. validate_uploads — file not found
# ---------------------------------------------------------------------------
class TestValidateUploadsNotFound:
    def test_missing_files_returns_failure(self) -> None:
        client = MagicMock()
        client.stat_object.side_effect = _s3_error("NoSuchKey")
        svc = _make_provider(client)

        result = svc.validate_uploads(tez_id=TEZ_ID, files=SAMPLE_FILES)

        assert result.success is False
        missing_names = [f["name"] for f in result.missing]
        assert "transcript.md" in missing_names
        assert "slides.pdf" in missing_names
        assert "manifest.json" in missing_names
        assert "tez.md" in missing_names

    def test_partial_missing_reflects_correctly(self) -> None:
        client = MagicMock()

        def stat_side_effect(bucket: str, key: str) -> Any:
            if "transcript.md" in key:
                return MagicMock()
            raise _s3_error("NoSuchKey")

        client.stat_object.side_effect = stat_side_effect
        svc = _make_provider(client)

        result = svc.validate_uploads(tez_id=TEZ_ID, files=SAMPLE_FILES)

        assert result.success is False
        missing_names = [f["name"] for f in result.missing]
        assert "transcript.md" not in missing_names
        assert "slides.pdf" in missing_names


# ---------------------------------------------------------------------------
# 5. delete_tez
# ---------------------------------------------------------------------------
class TestDeleteTez:
    def test_deletes_all_objects(self) -> None:
        client = MagicMock()
        obj1 = MagicMock(object_name=f"{TEZ_ID}/manifest.json")
        obj2 = MagicMock(object_name=f"{TEZ_ID}/context/a.md")
        client.list_objects.return_value = iter([obj1, obj2])
        client.remove_objects.return_value = iter([])  # no errors
        svc = _make_provider(client)

        svc.delete_tez(tez_id=TEZ_ID)

        client.remove_objects.assert_called_once()

    def test_no_objects_does_not_call_remove(self) -> None:
        client = MagicMock()
        client.list_objects.return_value = iter([])
        svc = _make_provider(client)

        svc.delete_tez(tez_id=TEZ_ID)

        client.remove_objects.assert_not_called()

    def test_deletion_errors_raise_storage_provider_error(self) -> None:
        client = MagicMock()
        obj = MagicMock(object_name=f"{TEZ_ID}/manifest.json")
        client.list_objects.return_value = iter([obj])
        delete_error = MagicMock()
        delete_error.__str__ = lambda self: "delete failed"
        client.remove_objects.return_value = iter([delete_error])
        svc = _make_provider(client)

        with pytest.raises(StorageProviderError, match="deletion errors"):
            svc.delete_tez(tez_id=TEZ_ID)


# ---------------------------------------------------------------------------
# 6. Misconfiguration — factory raises on missing env vars
# ---------------------------------------------------------------------------
class TestMisconfiguration:
    def test_missing_minio_env_vars_raise_storage_provider_error(self) -> None:
        from tez_server.services.storage_factory import get_storage_provider

        env = {"STORAGE_BACKEND": "minio"}  # all MINIO_* vars absent
        with patch.dict(os.environ, env, clear=False):
            # Remove any MINIO_* vars that might be set in the environment
            minio_keys = [
                "MINIO_ENDPOINT",
                "MINIO_ACCESS_KEY",
                "MINIO_SECRET_KEY",
                "MINIO_BUCKET",
            ]
            for key in minio_keys:
                os.environ.pop(key, None)
            with pytest.raises(StorageProviderError, match="Missing required env vars"):
                get_storage_provider()

    def test_unknown_backend_raises_storage_provider_error(self) -> None:
        from tez_server.services.storage_factory import get_storage_provider

        with (
            patch.dict(os.environ, {"STORAGE_BACKEND": "gcs"}),
            pytest.raises(StorageProviderError, match="Unknown STORAGE_BACKEND"),
        ):
            get_storage_provider()


# ---------------------------------------------------------------------------
# 7. Error handling — all MinIO errors wrap as StorageProviderError
# ---------------------------------------------------------------------------
class TestErrorHandling:
    """Every S3Error and network error must surface as StorageProviderError."""

    def test_invalid_credentials_raises_storage_provider_error(self) -> None:
        client = MagicMock()
        client.stat_object.side_effect = _s3_error("InvalidAccessKeyId")
        svc = _make_provider(client)

        with pytest.raises(StorageProviderError, match="Invalid or unauthorized"):
            svc._stat_object("abc123/context/file.md")

    def test_access_denied_raises_storage_provider_error(self) -> None:
        client = MagicMock()
        client.stat_object.side_effect = _s3_error("AccessDenied")
        svc = _make_provider(client)

        with pytest.raises(StorageProviderError, match="Invalid or unauthorized"):
            svc._stat_object("abc123/context/file.md")

    def test_no_such_bucket_raises_storage_provider_error(self) -> None:
        client = MagicMock()
        client.stat_object.side_effect = _s3_error("NoSuchBucket")
        svc = _make_provider(client)

        with pytest.raises(StorageProviderError, match="does not exist"):
            svc._stat_object("abc123/context/file.md")

    def test_unknown_s3_error_raises_storage_provider_error(self) -> None:
        client = MagicMock()
        client.stat_object.side_effect = _s3_error("InternalError", "server exploded")
        svc = _make_provider(client)

        with pytest.raises(StorageProviderError, match="MinIO error"):
            svc._stat_object("abc123/context/file.md")

    def test_network_error_in_stat_raises_storage_provider_error(self) -> None:
        client = MagicMock()
        client.stat_object.side_effect = ConnectionError("unreachable")
        svc = _make_provider(client)

        with pytest.raises(StorageProviderError, match="Network error"):
            svc._stat_object("abc123/context/file.md")

    def test_network_error_in_upload_url_raises_storage_provider_error(self) -> None:
        client = MagicMock()
        client.presigned_put_object.side_effect = ConnectionError("unreachable")
        svc = _make_provider(client)

        with pytest.raises(StorageProviderError, match="Network error"):
            svc.generate_upload_urls(tez_id=TEZ_ID, files=SAMPLE_FILES)

    def test_s3_error_in_upload_url_raises_storage_provider_error(self) -> None:
        client = MagicMock()
        client.presigned_put_object.side_effect = _s3_error("AccessDenied")
        svc = _make_provider(client)

        with pytest.raises(StorageProviderError):
            svc.generate_upload_urls(tez_id=TEZ_ID, files=SAMPLE_FILES)

    def test_network_error_in_download_url_raises_storage_provider_error(self) -> None:
        client = MagicMock()
        client.presigned_get_object.side_effect = ConnectionError("unreachable")
        svc = _make_provider(client)

        with pytest.raises(StorageProviderError, match="Network error"):
            svc.generate_download_urls(tez_id=TEZ_ID, files=SAMPLE_FILES)

    def test_s3_error_in_download_url_raises_storage_provider_error(self) -> None:
        client = MagicMock()
        client.presigned_get_object.side_effect = _s3_error("AccessDenied")
        svc = _make_provider(client)

        with pytest.raises(StorageProviderError):
            svc.generate_download_urls(tez_id=TEZ_ID, files=SAMPLE_FILES)

    def test_s3_error_in_delete_raises_storage_provider_error(self) -> None:
        client = MagicMock()
        client.list_objects.side_effect = _s3_error("AccessDenied")
        svc = _make_provider(client)

        with pytest.raises(StorageProviderError):
            svc.delete_tez(tez_id=TEZ_ID)

    def test_network_error_in_delete_raises_storage_provider_error(self) -> None:
        client = MagicMock()
        client.list_objects.side_effect = ConnectionError("unreachable")
        svc = _make_provider(client)

        with pytest.raises(StorageProviderError, match="Network error"):
            svc.delete_tez(tez_id=TEZ_ID)
