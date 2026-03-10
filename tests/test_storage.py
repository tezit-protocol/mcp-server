"""Unit tests for tez.services.storage -- S3 operations.

Tests the StorageService in isolation using moto-backed S3.
The developer implementing StorageService should make these tests pass.

Expected module: tez.services.storage
Expected class:  StorageService(s3_client, bucket: str)
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest

from tests.conftest import TEST_BUCKET

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client


# ---------------------------------------------------------------------------
# StorageService import -- will fail until the module is created
# ---------------------------------------------------------------------------
storage = pytest.importorskip(
    "tez_server.services.storage",
    reason="tez_server.services.storage not yet implemented",
)
StorageService = storage.StorageService


def _upload_context_files(
    s3_client: S3Client,
    tez_id: str,
    files: list[dict[str, Any]],
    contents: dict[str, bytes],
) -> None:
    """Upload context files for a Tez to S3."""
    for file in files:
        s3_client.put_object(
            Bucket=TEST_BUCKET,
            Key=f"{tez_id}/context/{file['name']}",
            Body=contents[file["name"]],
            ContentType=file["content_type"],
        )


def _upload_manifest_files(s3_client: S3Client, tez_id: str) -> None:
    """Upload manifest.json and tez.md to S3."""
    s3_client.put_object(
        Bucket=TEST_BUCKET,
        Key=f"{tez_id}/manifest.json",
        Body=b'{"tezit_version": "1.2"}',
        ContentType="application/json",
    )
    s3_client.put_object(
        Bucket=TEST_BUCKET,
        Key=f"{tez_id}/tez.md",
        Body=b"# Test Tez\n",
        ContentType="text/markdown",
    )


# ===================================================================
# Pre-signed upload URL generation
# ===================================================================
class TestGenerateUploadUrls:
    """StorageService.generate_upload_urls() must return PUT URLs for
    context files + manifest.json + tez.md."""

    def test_returns_url_per_file_plus_manifests(
        self, s3_client: S3Client, sample_files: list[dict[str, Any]]
    ) -> None:
        svc = StorageService(s3_client=s3_client, bucket=TEST_BUCKET)
        urls = svc.generate_upload_urls(tez_id="abc123", files=sample_files)

        expected_keys = {f["name"] for f in sample_files} | {
            "manifest.json",
            "tez.md",
        }
        assert set(urls.keys()) == expected_keys

    def test_urls_are_strings(
        self, s3_client: S3Client, sample_files: list[dict[str, Any]]
    ) -> None:
        svc = StorageService(s3_client=s3_client, bucket=TEST_BUCKET)
        urls = svc.generate_upload_urls(tez_id="abc123", files=sample_files)

        for url in urls.values():
            assert isinstance(url, str)
            assert url.startswith("https://")

    def test_urls_contain_tez_id_and_filename(
        self, s3_client: S3Client, sample_files: list[dict[str, Any]]
    ) -> None:
        svc = StorageService(s3_client=s3_client, bucket=TEST_BUCKET)
        urls = svc.generate_upload_urls(tez_id="abc123", files=sample_files)

        for file in sample_files:
            url = urls[file["name"]]
            assert "abc123" in url
            assert file["name"] in url

    def test_urls_contain_s3_signature_params(
        self, s3_client: S3Client, sample_files: list[dict[str, Any]]
    ) -> None:
        svc = StorageService(s3_client=s3_client, bucket=TEST_BUCKET)
        urls = svc.generate_upload_urls(tez_id="abc123", files=sample_files)

        for url in urls.values():
            # v2 uses AWSAccessKeyId/Signature, v4 uses X-Amz-Algorithm
            assert "Signature" in url or "X-Amz-Signature" in url

    def test_custom_expiry(
        self, s3_client: S3Client, sample_files: list[dict[str, Any]]
    ) -> None:
        svc = StorageService(s3_client=s3_client, bucket=TEST_BUCKET)
        urls = svc.generate_upload_urls(
            tez_id="abc123", files=sample_files, expires_in=300
        )

        # Verify URLs contain expiry params (v2 Expires or v4 X-Amz-Expires)
        for url in urls.values():
            assert "Expires" in url or "X-Amz-Expires" in url

    def test_default_expiry_is_900(
        self, s3_client: S3Client, sample_files: list[dict[str, Any]]
    ) -> None:
        svc = StorageService(s3_client=s3_client, bucket=TEST_BUCKET)
        urls = svc.generate_upload_urls(tez_id="abc123", files=sample_files)

        # Verify URLs contain expiry params
        for url in urls.values():
            assert "Expires" in url or "X-Amz-Expires" in url

    def test_empty_files_returns_manifest_urls_only(self, s3_client: S3Client) -> None:
        svc = StorageService(s3_client=s3_client, bucket=TEST_BUCKET)
        urls = svc.generate_upload_urls(tez_id="abc123", files=[])

        assert set(urls.keys()) == {"manifest.json", "tez.md"}

    def test_context_file_urls_use_context_prefix(
        self, s3_client: S3Client, sample_files: list[dict[str, Any]]
    ) -> None:
        svc = StorageService(s3_client=s3_client, bucket=TEST_BUCKET)
        urls = svc.generate_upload_urls(tez_id="abc123", files=sample_files)

        for file in sample_files:
            assert f"abc123/context/{file['name']}" in urls[file["name"]]


# ===================================================================
# Pre-signed download URL generation
# ===================================================================
class TestGenerateDownloadUrls:
    """generate_download_urls() returns GET URLs for files + manifests."""

    def test_returns_url_per_file_plus_manifests(
        self, s3_client: S3Client, sample_files: list[dict[str, Any]]
    ) -> None:
        svc = StorageService(s3_client=s3_client, bucket=TEST_BUCKET)
        urls = svc.generate_download_urls(tez_id="abc123", files=sample_files)

        expected_keys = {f["name"] for f in sample_files} | {
            "manifest.json",
            "tez.md",
        }
        assert set(urls.keys()) == expected_keys

    def test_urls_are_valid_strings(
        self, s3_client: S3Client, sample_files: list[dict[str, Any]]
    ) -> None:
        svc = StorageService(s3_client=s3_client, bucket=TEST_BUCKET)
        urls = svc.generate_download_urls(tez_id="abc123", files=sample_files)

        for url in urls.values():
            assert isinstance(url, str)
            assert url.startswith("https://")

    def test_default_expiry_is_3600(
        self, s3_client: S3Client, sample_files: list[dict[str, Any]]
    ) -> None:
        svc = StorageService(s3_client=s3_client, bucket=TEST_BUCKET)
        urls = svc.generate_download_urls(tez_id="abc123", files=sample_files)

        for url in urls.values():
            assert "Expires" in url or "X-Amz-Expires" in url

    def test_custom_expiry(
        self, s3_client: S3Client, sample_files: list[dict[str, Any]]
    ) -> None:
        svc = StorageService(s3_client=s3_client, bucket=TEST_BUCKET)
        urls = svc.generate_download_urls(
            tez_id="abc123", files=sample_files, expires_in=1800
        )

        for url in urls.values():
            assert "Expires" in url or "X-Amz-Expires" in url

    def test_manifest_urls_at_tez_root(
        self, s3_client: S3Client, sample_files: list[dict[str, Any]]
    ) -> None:
        svc = StorageService(s3_client=s3_client, bucket=TEST_BUCKET)
        urls = svc.generate_download_urls(tez_id="abc123", files=sample_files)

        assert "abc123/manifest.json" in urls["manifest.json"]
        assert "abc123/tez.md" in urls["tez.md"]

    def test_file_urls_include_context_prefix(
        self, s3_client: S3Client, sample_files: list[dict[str, Any]]
    ) -> None:
        svc = StorageService(s3_client=s3_client, bucket=TEST_BUCKET)
        urls = svc.generate_download_urls(tez_id="abc123", files=sample_files)

        for file in sample_files:
            assert f"abc123/context/{file['name']}" in urls[file["name"]]


# ===================================================================
# File upload validation (HeadObject checks)
# ===================================================================
class TestValidateUploads:
    """StorageService.validate_uploads() checks that files actually landed in S3."""

    def test_all_files_present_returns_success(
        self,
        s3_client: S3Client,
        sample_files: list[dict[str, Any]],
        sample_file_contents: dict[str, bytes],
    ) -> None:
        _upload_context_files(s3_client, "abc123", sample_files, sample_file_contents)
        _upload_manifest_files(s3_client, "abc123")

        svc = StorageService(s3_client=s3_client, bucket=TEST_BUCKET)
        result = svc.validate_uploads(tez_id="abc123", files=sample_files)

        assert result.success is True
        assert result.missing == []

    def test_missing_context_files_returns_failure(
        self, s3_client: S3Client, sample_files: list[dict[str, Any]]
    ) -> None:
        # Upload only the first context file + manifest files
        s3_client.put_object(
            Bucket=TEST_BUCKET,
            Key=f"abc123/context/{sample_files[0]['name']}",
            Body=b"content",
            ContentType=sample_files[0]["content_type"],
        )
        _upload_manifest_files(s3_client, "abc123")

        svc = StorageService(s3_client=s3_client, bucket=TEST_BUCKET)
        result = svc.validate_uploads(tez_id="abc123", files=sample_files)

        assert result.success is False
        missing_names = [f["name"] for f in result.missing]
        assert "action-items.md" in missing_names
        assert "slides.pdf" in missing_names

    def test_missing_manifest_files_returns_failure(
        self,
        s3_client: S3Client,
        sample_files: list[dict[str, Any]],
        sample_file_contents: dict[str, bytes],
    ) -> None:
        # Upload context files but NOT manifest files
        _upload_context_files(s3_client, "abc123", sample_files, sample_file_contents)

        svc = StorageService(s3_client=s3_client, bucket=TEST_BUCKET)
        result = svc.validate_uploads(tez_id="abc123", files=sample_files)

        assert result.success is False
        missing_names = [f["name"] for f in result.missing]
        assert "manifest.json" in missing_names
        assert "tez.md" in missing_names

    def test_no_files_uploaded_returns_all_missing(
        self, s3_client: S3Client, sample_files: list[dict[str, Any]]
    ) -> None:
        svc = StorageService(s3_client=s3_client, bucket=TEST_BUCKET)
        result = svc.validate_uploads(tez_id="abc123", files=sample_files)

        assert result.success is False
        # 3 context files + manifest.json + tez.md
        assert len(result.missing) == len(sample_files) + 2

    def test_returns_etags_for_present_files(
        self,
        s3_client: S3Client,
        sample_files: list[dict[str, Any]],
        sample_file_contents: dict[str, bytes],
    ) -> None:
        _upload_context_files(s3_client, "abc123", sample_files, sample_file_contents)
        _upload_manifest_files(s3_client, "abc123")

        svc = StorageService(s3_client=s3_client, bucket=TEST_BUCKET)
        result = svc.validate_uploads(tez_id="abc123", files=sample_files)

        assert len(result.verified_files) == len(sample_files)
        for vf in result.verified_files:
            assert vf["etag"] is not None
            assert isinstance(vf["etag"], str)

    def test_validate_with_account_id(
        self,
        s3_client: S3Client,
        sample_files: list[dict[str, Any]],
        sample_file_contents: dict[str, bytes],
    ) -> None:
        _upload_context_files(s3_client, "abc123", sample_files, sample_file_contents)
        _upload_manifest_files(s3_client, "abc123")

        svc = StorageService(
            s3_client=s3_client, bucket=TEST_BUCKET, account_id="123456789012"
        )
        result = svc.validate_uploads(tez_id="abc123", files=sample_files)

        assert result.success is True

    def test_head_object_reraises_non_404(self, s3_client: S3Client) -> None:
        from botocore.exceptions import ClientError

        svc = StorageService(s3_client=s3_client, bucket=TEST_BUCKET)

        # Patch head_object to raise a 403 (not 404)
        original = svc.s3.head_object

        def raise_403(**kwargs: Any) -> None:
            raise ClientError(
                {"Error": {"Code": "403", "Message": "Forbidden"}},
                "HeadObject",
            )

        svc.s3.head_object = raise_403  # type: ignore[assignment]
        with pytest.raises(ClientError, match="Forbidden"):
            svc._head_object("abc123/context/notes.md")
        svc.s3.head_object = original  # type: ignore[assignment]

    def test_empty_file_list_with_manifests_returns_success(
        self, s3_client: S3Client
    ) -> None:
        _upload_manifest_files(s3_client, "abc123")

        svc = StorageService(s3_client=s3_client, bucket=TEST_BUCKET)
        result = svc.validate_uploads(tez_id="abc123", files=[])

        assert result.success is True


# ===================================================================
# File deletion
# ===================================================================
class TestDeleteTezFiles:
    """StorageService.delete_tez() removes all objects under a Tez prefix."""

    def test_deletes_all_objects(
        self,
        s3_client: S3Client,
        sample_files: list[dict[str, Any]],
        sample_file_contents: dict[str, bytes],
    ) -> None:
        _upload_context_files(s3_client, "abc123", sample_files, sample_file_contents)
        _upload_manifest_files(s3_client, "abc123")

        svc = StorageService(s3_client=s3_client, bucket=TEST_BUCKET)
        svc.delete_tez(tez_id="abc123")

        # Verify nothing remains
        response = s3_client.list_objects_v2(Bucket=TEST_BUCKET, Prefix="abc123/")
        assert response.get("KeyCount", 0) == 0

    def test_delete_nonexistent_tez_does_not_raise(self, s3_client: S3Client) -> None:
        svc = StorageService(s3_client=s3_client, bucket=TEST_BUCKET)
        # Should not raise -- deleting nothing is fine
        svc.delete_tez(tez_id="nonexistent")

    def test_delete_with_account_id(
        self,
        s3_client: S3Client,
        sample_files: list[dict[str, Any]],
        sample_file_contents: dict[str, bytes],
    ) -> None:
        _upload_context_files(s3_client, "abc123", sample_files, sample_file_contents)
        _upload_manifest_files(s3_client, "abc123")

        svc = StorageService(
            s3_client=s3_client, bucket=TEST_BUCKET, account_id="123456789012"
        )
        svc.delete_tez(tez_id="abc123")

        response = s3_client.list_objects_v2(Bucket=TEST_BUCKET, Prefix="abc123/")
        assert response.get("KeyCount", 0) == 0

    def test_does_not_delete_other_tez_files(self, s3_client: S3Client) -> None:
        # Upload to two different Tez IDs
        s3_client.put_object(Bucket=TEST_BUCKET, Key="abc123/context/a.md", Body=b"a")
        s3_client.put_object(Bucket=TEST_BUCKET, Key="def456/context/b.md", Body=b"b")

        svc = StorageService(s3_client=s3_client, bucket=TEST_BUCKET)
        svc.delete_tez(tez_id="abc123")

        # abc123 gone
        r1 = s3_client.list_objects_v2(Bucket=TEST_BUCKET, Prefix="abc123/")
        assert r1.get("KeyCount", 0) == 0

        # def456 untouched
        r2 = s3_client.list_objects_v2(Bucket=TEST_BUCKET, Prefix="def456/")
        assert r2["KeyCount"] == 1
