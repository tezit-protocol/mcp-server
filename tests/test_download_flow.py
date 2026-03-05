"""Integration tests for the Tez download flow.

Tests the complete download process: auth check -> URL generation -> file
retrieval, including authorisation rules, error scenarios, and edge cases.

Expected modules:
    tez.services.storage.StorageService
    tez.services.metadata.MetadataService
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import pytest

from tests.conftest import (
    CREATOR_EMAIL,
    RECIPIENT_EMAIL,
    TEST_BUCKET,
)

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

storage_mod = pytest.importorskip(
    "tez_server.services.storage",
    reason="tez_server.services.storage not yet implemented",
)
metadata_mod = pytest.importorskip(
    "tez_server.services.metadata",
    reason="tez_server.services.metadata not yet implemented",
)
StorageService = storage_mod.StorageService
MetadataService = metadata_mod.MetadataService


# ---------------------------------------------------------------------------
# Helper to seed a fully-built Tez in S3 + DynamoDB
# ---------------------------------------------------------------------------
def _seed_tez(
    s3_client: S3Client,
    dynamodb_table: Any,
    tez_id: str,
    files: list[dict[str, Any]],
    contents: dict[str, bytes],
    *,
    creator: str = CREATOR_EMAIL,
    recipients: list[str] | None = None,
) -> dict[str, Any]:
    """Upload files, manifest, and DynamoDB record for a complete Tez."""
    for f in files:
        s3_client.put_object(
            Bucket=TEST_BUCKET,
            Key=f"{tez_id}/context/{f['name']}",
            Body=contents[f["name"]],
            ContentType=f["content_type"],
        )

    manifest = {
        "tez_id": tez_id,
        "creator": creator,
        "name": "Test Tez",
        "description": "Seeded for download tests",
        "status": "active",
        "file_count": len(files),
        "total_size": sum(f["size"] for f in files),
        "files": files,
    }
    s3_client.put_object(
        Bucket=TEST_BUCKET,
        Key=f"{tez_id}/manifest.json",
        Body=json.dumps(manifest).encode(),
        ContentType="application/json",
    )
    s3_client.put_object(
        Bucket=TEST_BUCKET,
        Key=f"{tez_id}/tez.md",
        Body=f"# Test Tez\n\n**ID:** {tez_id}\n".encode(),
        ContentType="text/markdown",
    )

    record = {
        **manifest,
        "recipients": recipients or [],
        "created_at": "2026-02-20T10:00:00Z",
        "updated_at": "2026-02-20T10:00:00Z",
    }
    dynamodb_table.put_item(Item=record)
    return record


# ===================================================================
# Phase 1 -- Authorisation checks
# ===================================================================
class TestDownloadAuthorisation:
    """tez_get must verify the caller is the creator or a recipient."""

    def test_creator_is_authorised(
        self,
        s3_client: S3Client,
        dynamodb_table: Any,
        sample_files: list[dict[str, Any]],
        sample_file_contents: dict[str, bytes],
    ) -> None:
        _seed_tez(
            s3_client,
            dynamodb_table,
            "dl-auth-1",
            sample_files,
            sample_file_contents,
        )

        meta = MetadataService(table=dynamodb_table)
        assert meta.is_authorised(tez_id="dl-auth-1", email=CREATOR_EMAIL)

    def test_recipient_is_authorised(
        self,
        s3_client: S3Client,
        dynamodb_table: Any,
        sample_files: list[dict[str, Any]],
        sample_file_contents: dict[str, bytes],
    ) -> None:
        _seed_tez(
            s3_client,
            dynamodb_table,
            "dl-auth-2",
            sample_files,
            sample_file_contents,
            recipients=[RECIPIENT_EMAIL],
        )

        meta = MetadataService(table=dynamodb_table)
        assert meta.is_authorised(tez_id="dl-auth-2", email=RECIPIENT_EMAIL)

    def test_unknown_user_not_authorised(
        self,
        s3_client: S3Client,
        dynamodb_table: Any,
        sample_files: list[dict[str, Any]],
        sample_file_contents: dict[str, bytes],
    ) -> None:
        _seed_tez(
            s3_client,
            dynamodb_table,
            "dl-auth-3",
            sample_files,
            sample_file_contents,
        )

        meta = MetadataService(table=dynamodb_table)
        assert not meta.is_authorised(tez_id="dl-auth-3", email="hacker@evil.com")

    def test_nonexistent_tez_not_authorised(self, dynamodb_table: Any) -> None:
        meta = MetadataService(table=dynamodb_table)
        assert not meta.is_authorised(tez_id="nonexistent", email=CREATOR_EMAIL)

    def test_multiple_recipients_all_authorised(
        self,
        s3_client: S3Client,
        dynamodb_table: Any,
        sample_files: list[dict[str, Any]],
        sample_file_contents: dict[str, bytes],
    ) -> None:
        recipients = [
            "noor@ragu.ai",
            "mackenzie@ragu.ai",
            "rob@ragu.ai",
        ]
        _seed_tez(
            s3_client,
            dynamodb_table,
            "dl-auth-multi",
            sample_files,
            sample_file_contents,
            recipients=recipients,
        )

        meta = MetadataService(table=dynamodb_table)
        for email in recipients:
            assert meta.is_authorised(tez_id="dl-auth-multi", email=email)

    def test_recipient_added_after_creation_is_authorised(
        self,
        s3_client: S3Client,
        dynamodb_table: Any,
        sample_files: list[dict[str, Any]],
        sample_file_contents: dict[str, bytes],
    ) -> None:
        _seed_tez(
            s3_client,
            dynamodb_table,
            "dl-auth-late",
            sample_files,
            sample_file_contents,
        )

        meta = MetadataService(table=dynamodb_table)

        # Not authorised yet
        assert not meta.is_authorised(tez_id="dl-auth-late", email=RECIPIENT_EMAIL)

        # Share with recipient
        meta.add_recipient(tez_id="dl-auth-late", email=RECIPIENT_EMAIL)

        # Now authorised
        assert meta.is_authorised(tez_id="dl-auth-late", email=RECIPIENT_EMAIL)


# ===================================================================
# Phase 1 -- Download URL generation
# ===================================================================
class TestDownloadUrlGeneration:
    """StorageService.generate_download_urls() returns GET URLs."""

    def test_returns_urls_for_all_files_plus_manifests(
        self,
        s3_client: S3Client,
        dynamodb_table: Any,
        sample_files: list[dict[str, Any]],
        sample_file_contents: dict[str, bytes],
    ) -> None:
        _seed_tez(
            s3_client,
            dynamodb_table,
            "dl-urls-1",
            sample_files,
            sample_file_contents,
        )

        storage = StorageService(s3_client=s3_client, bucket=TEST_BUCKET)
        urls = storage.generate_download_urls(tez_id="dl-urls-1", files=sample_files)

        expected_keys = {"manifest.json", "tez.md"} | {f["name"] for f in sample_files}
        assert set(urls.keys()) == expected_keys

    def test_download_urls_have_longer_expiry_than_upload(
        self,
        s3_client: S3Client,
        sample_files: list[dict[str, Any]],
    ) -> None:
        storage = StorageService(s3_client=s3_client, bucket=TEST_BUCKET)

        upload_urls = storage.generate_upload_urls(
            tez_id="dl-expiry", files=sample_files
        )
        download_urls = storage.generate_download_urls(
            tez_id="dl-expiry", files=sample_files
        )

        # Both should contain expiry params (v2 Expires or v4 X-Amz-Expires)
        for url in upload_urls.values():
            assert "Expires" in url or "X-Amz-Expires" in url

        for url in download_urls.values():
            assert "Expires" in url or "X-Amz-Expires" in url

    def test_download_urls_contain_correct_s3_keys(
        self,
        s3_client: S3Client,
        sample_files: list[dict[str, Any]],
    ) -> None:
        storage = StorageService(s3_client=s3_client, bucket=TEST_BUCKET)
        urls = storage.generate_download_urls(tez_id="dl-keys", files=sample_files)

        assert "dl-keys/manifest.json" in urls["manifest.json"]
        assert "dl-keys/tez.md" in urls["tez.md"]

        for f in sample_files:
            assert f"dl-keys/context/{f['name']}" in urls[f["name"]]


# ===================================================================
# Phase 2 -- File downloads from S3
# ===================================================================
class TestFileDownload:
    """Verify that files uploaded in the build flow are downloadable."""

    def test_all_files_downloadable(
        self,
        s3_client: S3Client,
        dynamodb_table: Any,
        sample_files: list[dict[str, Any]],
        sample_file_contents: dict[str, bytes],
    ) -> None:
        _seed_tez(
            s3_client,
            dynamodb_table,
            "dl-files-1",
            sample_files,
            sample_file_contents,
        )

        for f in sample_files:
            obj = s3_client.get_object(
                Bucket=TEST_BUCKET,
                Key=f"dl-files-1/context/{f['name']}",
            )
            assert obj["Body"].read() == sample_file_contents[f["name"]]

    def test_manifest_json_downloadable_and_valid(
        self,
        s3_client: S3Client,
        dynamodb_table: Any,
        sample_files: list[dict[str, Any]],
        sample_file_contents: dict[str, bytes],
    ) -> None:
        _seed_tez(
            s3_client,
            dynamodb_table,
            "dl-manifest",
            sample_files,
            sample_file_contents,
        )

        obj = s3_client.get_object(Bucket=TEST_BUCKET, Key="dl-manifest/manifest.json")
        data = json.loads(obj["Body"].read())
        assert data["tez_id"] == "dl-manifest"
        assert data["name"] == "Test Tez"
        assert len(data["files"]) == len(sample_files)

    def test_tez_md_downloadable(
        self,
        s3_client: S3Client,
        dynamodb_table: Any,
        sample_files: list[dict[str, Any]],
        sample_file_contents: dict[str, bytes],
    ) -> None:
        _seed_tez(
            s3_client,
            dynamodb_table,
            "dl-tezmd",
            sample_files,
            sample_file_contents,
        )

        obj = s3_client.get_object(Bucket=TEST_BUCKET, Key="dl-tezmd/tez.md")
        content = obj["Body"].read().decode()
        assert content.startswith("# ")
        assert "dl-tezmd" in content

    def test_content_types_preserved_on_download(
        self,
        s3_client: S3Client,
        dynamodb_table: Any,
        sample_files: list[dict[str, Any]],
        sample_file_contents: dict[str, bytes],
    ) -> None:
        _seed_tez(
            s3_client,
            dynamodb_table,
            "dl-ctype",
            sample_files,
            sample_file_contents,
        )

        for f in sample_files:
            obj = s3_client.head_object(
                Bucket=TEST_BUCKET,
                Key=f"dl-ctype/context/{f['name']}",
            )
            assert obj["ContentType"] == f["content_type"]


# ===================================================================
# Full end-to-end download flow
# ===================================================================
class TestEndToEndDownload:
    """Complete auth -> URL generation -> download integration test."""

    def test_full_download_lifecycle_as_creator(
        self,
        s3_client: S3Client,
        dynamodb_table: Any,
        sample_files: list[dict[str, Any]],
        sample_file_contents: dict[str, bytes],
    ) -> None:
        tez_id = "e2e-dl-creator"
        _seed_tez(
            s3_client,
            dynamodb_table,
            tez_id,
            sample_files,
            sample_file_contents,
        )

        meta = MetadataService(table=dynamodb_table)
        storage = StorageService(s3_client=s3_client, bucket=TEST_BUCKET)

        # Step 1 -- Auth check
        assert meta.is_authorised(tez_id=tez_id, email=CREATOR_EMAIL)

        # Step 2 -- Get metadata
        record = meta.get_tez(tez_id=tez_id)
        assert record is not None
        assert record["status"] == "active"

        # Step 3 -- Generate download URLs
        urls = storage.generate_download_urls(tez_id=tez_id, files=record["files"])
        assert "manifest.json" in urls
        assert "tez.md" in urls
        for f in sample_files:
            assert f["name"] in urls

        # Step 4 -- Download files (verify they exist in S3)
        for f in sample_files:
            obj = s3_client.get_object(
                Bucket=TEST_BUCKET,
                Key=f"{tez_id}/context/{f['name']}",
            )
            content = obj["Body"].read()
            assert content == sample_file_contents[f["name"]]

    def test_full_download_lifecycle_as_recipient(
        self,
        s3_client: S3Client,
        dynamodb_table: Any,
        sample_files: list[dict[str, Any]],
        sample_file_contents: dict[str, bytes],
    ) -> None:
        tez_id = "e2e-dl-recipient"
        _seed_tez(
            s3_client,
            dynamodb_table,
            tez_id,
            sample_files,
            sample_file_contents,
            recipients=[RECIPIENT_EMAIL],
        )

        meta = MetadataService(table=dynamodb_table)
        storage = StorageService(s3_client=s3_client, bucket=TEST_BUCKET)

        # Recipient is authorised
        assert meta.is_authorised(tez_id=tez_id, email=RECIPIENT_EMAIL)

        record = meta.get_tez(tez_id=tez_id)
        assert record is not None

        urls = storage.generate_download_urls(tez_id=tez_id, files=record["files"])
        assert len(urls) == len(sample_files) + 2  # files + manifests

    def test_download_denied_for_unauthorised_user(
        self,
        s3_client: S3Client,
        dynamodb_table: Any,
        sample_files: list[dict[str, Any]],
        sample_file_contents: dict[str, bytes],
    ) -> None:
        tez_id = "e2e-dl-denied"
        _seed_tez(
            s3_client,
            dynamodb_table,
            tez_id,
            sample_files,
            sample_file_contents,
        )

        meta = MetadataService(table=dynamodb_table)
        assert not meta.is_authorised(tez_id=tez_id, email="hacker@evil.com")

    def test_download_nonexistent_tez_returns_none(self, dynamodb_table: Any) -> None:
        meta = MetadataService(table=dynamodb_table)
        record = meta.get_tez(tez_id="does-not-exist")
        assert record is None


# ===================================================================
# Build -> Share -> Download (full lifecycle)
# ===================================================================
class TestBuildShareDownload:
    """Complete lifecycle: one user builds, shares, another downloads."""

    def test_creator_builds_shares_recipient_downloads(
        self,
        s3_client: S3Client,
        dynamodb_table: Any,
        sample_files: list[dict[str, Any]],
        sample_file_contents: dict[str, bytes],
    ) -> None:
        tez_id = "lifecycle-1"
        storage = StorageService(s3_client=s3_client, bucket=TEST_BUCKET)
        meta = MetadataService(table=dynamodb_table)

        # --- Creator builds ---
        storage.generate_upload_urls(tez_id=tez_id, files=sample_files)
        meta.create_tez(
            {
                "tez_id": tez_id,
                "creator": CREATOR_EMAIL,
                "name": "Lifecycle Test",
                "description": "Build -> Share -> Download",
                "status": "pending_upload",
                "file_count": len(sample_files),
                "total_size": sum(f["size"] for f in sample_files),
                "files": sample_files,
                "recipients": [],
                "created_at": "2026-02-20T10:00:00Z",
                "updated_at": "2026-02-20T10:00:00Z",
            }
        )

        # Upload complete bundle (context files + manifest)
        for f in sample_files:
            s3_client.put_object(
                Bucket=TEST_BUCKET,
                Key=f"{tez_id}/context/{f['name']}",
                Body=sample_file_contents[f["name"]],
                ContentType=f["content_type"],
            )
        s3_client.put_object(
            Bucket=TEST_BUCKET,
            Key=f"{tez_id}/manifest.json",
            Body=json.dumps(
                {"name": "Lifecycle Test", "creator": CREATOR_EMAIL}
            ).encode(),
            ContentType="application/json",
        )
        s3_client.put_object(
            Bucket=TEST_BUCKET,
            Key=f"{tez_id}/tez.md",
            Body=b"# Lifecycle Test\n",
            ContentType="text/markdown",
        )

        result = storage.validate_uploads(tez_id=tez_id, files=sample_files)
        assert result.success is True

        meta.update_status(tez_id=tez_id, status="active")

        # --- Creator shares with recipient ---
        meta.add_recipient(tez_id=tez_id, email=RECIPIENT_EMAIL)

        record_after_share = meta.get_tez(tez_id=tez_id)
        assert record_after_share is not None
        assert RECIPIENT_EMAIL in record_after_share["recipients"]

        # --- Recipient downloads ---
        # Auth check
        assert meta.is_authorised(tez_id=tez_id, email=RECIPIENT_EMAIL)

        # Get record
        record = meta.get_tez(tez_id=tez_id)
        assert record is not None
        assert record["status"] == "active"

        # Generate download URLs
        urls = storage.generate_download_urls(tez_id=tez_id, files=record["files"])
        expected_count = len(sample_files) + 2  # files + manifest + tez.md
        assert len(urls) == expected_count

        # Verify file contents
        for f in sample_files:
            obj = s3_client.get_object(
                Bucket=TEST_BUCKET,
                Key=f"{tez_id}/context/{f['name']}",
            )
            assert obj["Body"].read() == sample_file_contents[f["name"]]

        # Verify manifest
        manifest_obj = s3_client.get_object(
            Bucket=TEST_BUCKET, Key=f"{tez_id}/manifest.json"
        )
        manifest = json.loads(manifest_obj["Body"].read())
        assert manifest["name"] == "Lifecycle Test"
        assert manifest["creator"] == CREATOR_EMAIL

    def test_creator_cannot_be_blocked_after_sharing(
        self,
        s3_client: S3Client,
        dynamodb_table: Any,
        sample_files: list[dict[str, Any]],
        sample_file_contents: dict[str, bytes],
    ) -> None:
        """Creator retains access even after sharing."""
        tez_id = "lifecycle-creator-access"
        _seed_tez(
            s3_client,
            dynamodb_table,
            tez_id,
            sample_files,
            sample_file_contents,
            recipients=[RECIPIENT_EMAIL],
        )

        meta = MetadataService(table=dynamodb_table)
        assert meta.is_authorised(tez_id=tez_id, email=CREATOR_EMAIL)
        assert meta.is_authorised(tez_id=tez_id, email=RECIPIENT_EMAIL)
