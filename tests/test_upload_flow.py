"""Integration tests for the Tez upload flow.

Tests the complete 3-phase upload process end-to-end using moto-backed
AWS services. These tests verify the interaction between StorageService
and MetadataService during the build -> upload -> confirm lifecycle.

With ADR-004, the CLI now builds and uploads the complete bundle
(including manifest.json + tez.md) via pre-signed URLs. The server's
confirm step just validates and activates -- no manifest writing.

Expected modules:
    tez.services.storage.StorageService
    tez.services.metadata.MetadataService
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest

from tests.conftest import (
    CREATOR_EMAIL,
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


def _upload_context_files(
    s3_client: S3Client,
    tez_id: str,
    files: list[dict[str, Any]],
    contents: dict[str, bytes],
) -> None:
    """Upload context files for a Tez to S3."""
    for f in files:
        s3_client.put_object(
            Bucket=TEST_BUCKET,
            Key=f"{tez_id}/context/{f['name']}",
            Body=contents[f["name"]],
            ContentType=f["content_type"],
        )


def _upload_manifest_files(s3_client: S3Client, tez_id: str) -> None:
    """Upload manifest.json and tez.md to S3 (simulates CLI upload)."""
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


def _upload_complete_bundle(
    s3_client: S3Client,
    tez_id: str,
    files: list[dict[str, Any]],
    contents: dict[str, bytes],
) -> None:
    """Upload context files + manifest files (complete bundle)."""
    _upload_context_files(s3_client, tez_id, files, contents)
    _upload_manifest_files(s3_client, tez_id)


# ===================================================================
# Phase 1 -- tez_build: Initialise and generate upload URLs
# ===================================================================
class TestPhase1Initialise:
    """Phase 1 creates the Tez ID and returns pre-signed PUT URLs."""

    def test_generates_upload_urls_for_all_files_plus_manifests(
        self,
        s3_client: S3Client,
        sample_files: list[dict[str, Any]],
    ) -> None:
        storage = StorageService(s3_client=s3_client, bucket=TEST_BUCKET)

        urls = storage.generate_upload_urls(tez_id="int-test-1", files=sample_files)

        expected_keys = {f["name"] for f in sample_files} | {
            "manifest.json",
            "tez.md",
        }
        assert set(urls.keys()) == expected_keys

    def test_urls_are_usable_for_put(
        self,
        s3_client: S3Client,
        sample_files: list[dict[str, Any]],
    ) -> None:
        """Verify URLs point to correct S3 keys (structural check)."""
        storage = StorageService(s3_client=s3_client, bucket=TEST_BUCKET)

        urls = storage.generate_upload_urls(tez_id="int-test-1", files=sample_files)

        for f in sample_files:
            url = urls[f["name"]]
            expected_key = f"int-test-1/context/{f['name']}"
            assert expected_key in url

    def test_metadata_record_created_with_pending_status(
        self,
        s3_client: S3Client,
        dynamodb_table: Any,
        sample_files: list[dict[str, Any]],
    ) -> None:
        """After phase 1, a pending_upload record should exist in DynamoDB."""
        meta = MetadataService(table=dynamodb_table)
        meta.create_tez(
            {
                "tez_id": "int-test-1",
                "creator": CREATOR_EMAIL,
                "name": "Upload Test",
                "description": "Integration test",
                "status": "pending_upload",
                "file_count": len(sample_files),
                "total_size": sum(f["size"] for f in sample_files),
                "files": sample_files,
                "recipients": [],
                "created_at": "2026-02-20T10:00:00Z",
                "updated_at": "2026-02-20T10:00:00Z",
            }
        )

        record = meta.get_tez(tez_id="int-test-1")
        assert record is not None
        assert record["status"] == "pending_upload"


# ===================================================================
# Phase 2 -- Client uploads files directly to S3
# ===================================================================
class TestPhase2Upload:
    """Phase 2 simulates the client uploading files to S3 via PUT."""

    def test_files_land_in_correct_s3_keys(
        self,
        s3_client: S3Client,
        sample_files: list[dict[str, Any]],
        sample_file_contents: dict[str, bytes],
    ) -> None:
        tez_id = "int-test-2"
        _upload_context_files(s3_client, tez_id, sample_files, sample_file_contents)

        for f in sample_files:
            response = s3_client.head_object(
                Bucket=TEST_BUCKET,
                Key=f"{tez_id}/context/{f['name']}",
            )
            assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_content_type_preserved(
        self,
        s3_client: S3Client,
        sample_files: list[dict[str, Any]],
        sample_file_contents: dict[str, bytes],
    ) -> None:
        tez_id = "int-test-2"
        _upload_context_files(s3_client, tez_id, sample_files, sample_file_contents)

        for f in sample_files:
            response = s3_client.head_object(
                Bucket=TEST_BUCKET,
                Key=f"{tez_id}/context/{f['name']}",
            )
            assert response["ContentType"] == f["content_type"]

    def test_parallel_upload_all_files_present(
        self,
        s3_client: S3Client,
        sample_files: list[dict[str, Any]],
        sample_file_contents: dict[str, bytes],
    ) -> None:
        """Simulates parallel upload -- all files should be findable after."""
        tez_id = "int-test-2"
        _upload_context_files(s3_client, tez_id, sample_files, sample_file_contents)

        # List all objects under the context prefix
        response = s3_client.list_objects_v2(
            Bucket=TEST_BUCKET, Prefix=f"{tez_id}/context/"
        )
        keys = [obj["Key"] for obj in response.get("Contents", [])]

        assert len(keys) == len(sample_files)
        for f in sample_files:
            assert f"{tez_id}/context/{f['name']}" in keys

    def test_reupload_overwrites_safely(
        self,
        s3_client: S3Client,
    ) -> None:
        """PUT is idempotent -- re-uploading overwrites without error."""
        tez_id = "int-test-2"
        key = f"{tez_id}/context/readme.md"

        s3_client.put_object(Bucket=TEST_BUCKET, Key=key, Body=b"version 1")
        s3_client.put_object(Bucket=TEST_BUCKET, Key=key, Body=b"version 2")

        obj = s3_client.get_object(Bucket=TEST_BUCKET, Key=key)
        assert obj["Body"].read() == b"version 2"


# ===================================================================
# Phase 3 -- tez_build_confirm: Validate and activate
# ===================================================================
class TestPhase3Confirm:
    """Phase 3 validates uploads and updates DynamoDB status to active.
    The CLI has already built and uploaded the complete bundle (including
    manifest.json and tez.md). Confirm just validates and activates."""

    def test_validation_passes_when_all_files_present(
        self,
        s3_client: S3Client,
        sample_files: list[dict[str, Any]],
        sample_file_contents: dict[str, bytes],
    ) -> None:
        tez_id = "int-test-3"
        _upload_complete_bundle(s3_client, tez_id, sample_files, sample_file_contents)

        storage = StorageService(s3_client=s3_client, bucket=TEST_BUCKET)
        result = storage.validate_uploads(tez_id=tez_id, files=sample_files)

        assert result.success is True

    def test_validation_fails_when_files_missing(
        self,
        s3_client: S3Client,
        sample_files: list[dict[str, Any]],
        sample_file_contents: dict[str, bytes],
    ) -> None:
        tez_id = "int-test-3"
        # Only upload first context file + manifest files
        first = sample_files[0]
        s3_client.put_object(
            Bucket=TEST_BUCKET,
            Key=f"{tez_id}/context/{first['name']}",
            Body=sample_file_contents[first["name"]],
            ContentType=first["content_type"],
        )
        _upload_manifest_files(s3_client, tez_id)

        storage = StorageService(s3_client=s3_client, bucket=TEST_BUCKET)
        result = storage.validate_uploads(tez_id=tez_id, files=sample_files)

        assert result.success is False
        assert len(result.missing) == 2

    def test_dynamodb_status_updated_to_active(
        self,
        s3_client: S3Client,
        dynamodb_table: Any,
        sample_files: list[dict[str, Any]],
        sample_file_contents: dict[str, bytes],
        sample_tez_record: dict[str, Any],
    ) -> None:
        tez_id = sample_tez_record["tez_id"]

        # Phase 1 -- create pending record
        sample_tez_record["status"] = "pending_upload"
        dynamodb_table.put_item(Item=sample_tez_record)

        # Phase 2 -- upload complete bundle
        _upload_complete_bundle(s3_client, tez_id, sample_files, sample_file_contents)

        # Phase 3 -- validate + update status (no manifest writing needed)
        storage = StorageService(s3_client=s3_client, bucket=TEST_BUCKET)
        result = storage.validate_uploads(tez_id=tez_id, files=sample_files)
        assert result.success is True

        meta = MetadataService(table=dynamodb_table)
        meta.update_status(tez_id=tez_id, status="active")

        record = meta.get_tez(tez_id=tez_id)
        assert record is not None
        assert record["status"] == "active"

    def test_validation_returns_etags(
        self,
        s3_client: S3Client,
        sample_files: list[dict[str, Any]],
        sample_file_contents: dict[str, bytes],
        sample_tez_record: dict[str, Any],
    ) -> None:
        tez_id = sample_tez_record["tez_id"]
        _upload_complete_bundle(s3_client, tez_id, sample_files, sample_file_contents)

        storage = StorageService(s3_client=s3_client, bucket=TEST_BUCKET)
        validation = storage.validate_uploads(tez_id=tez_id, files=sample_files)

        assert len(validation.verified_files) == len(sample_files)
        for vf in validation.verified_files:
            assert "etag" in vf


# ===================================================================
# Full end-to-end upload flow
# ===================================================================
class TestEndToEndUpload:
    """Complete phase 1 -> 2 -> 3 integration test."""

    def test_full_upload_lifecycle(
        self,
        s3_client: S3Client,
        dynamodb_table: Any,
        sample_files: list[dict[str, Any]],
        sample_file_contents: dict[str, bytes],
    ) -> None:
        tez_id = "e2e-upload-1"
        storage = StorageService(s3_client=s3_client, bucket=TEST_BUCKET)
        meta = MetadataService(table=dynamodb_table)

        # Phase 1 -- Generate upload URLs and create pending record
        urls = storage.generate_upload_urls(tez_id=tez_id, files=sample_files)
        expected_keys = {f["name"] for f in sample_files} | {
            "manifest.json",
            "tez.md",
        }
        assert set(urls.keys()) == expected_keys

        pending_record = {
            "tez_id": tez_id,
            "creator": CREATOR_EMAIL,
            "name": "E2E Upload Test",
            "description": "Full lifecycle test",
            "status": "pending_upload",
            "file_count": len(sample_files),
            "total_size": sum(f["size"] for f in sample_files),
            "files": sample_files,
            "recipients": [],
            "created_at": "2026-02-20T10:00:00Z",
            "updated_at": "2026-02-20T10:00:00Z",
        }
        meta.create_tez(pending_record)

        record = meta.get_tez(tez_id=tez_id)
        assert record is not None
        assert record["status"] == "pending_upload"

        # Phase 2 -- Upload complete bundle to S3
        _upload_complete_bundle(s3_client, tez_id, sample_files, sample_file_contents)

        # Phase 3 -- Validate and update status
        validation = storage.validate_uploads(tez_id=tez_id, files=sample_files)
        assert validation.success is True
        assert len(validation.missing) == 0

        meta.update_status(tez_id=tez_id, status="active")

        # Final state checks
        final_record = meta.get_tez(tez_id=tez_id)
        assert final_record is not None
        assert final_record["status"] == "active"

        # All context files in S3
        for f in sample_files:
            obj = s3_client.get_object(
                Bucket=TEST_BUCKET,
                Key=f"{tez_id}/context/{f['name']}",
            )
            assert obj["Body"].read() == sample_file_contents[f["name"]]

    def test_upload_flow_with_single_file(
        self,
        s3_client: S3Client,
        dynamodb_table: Any,
    ) -> None:
        """Edge case: building a Tez with just one file."""
        tez_id = "e2e-single"
        files = [{"name": "notes.md", "size": 100, "content_type": "text/markdown"}]
        contents = {"notes.md": b"# Just one file\n"}

        storage = StorageService(s3_client=s3_client, bucket=TEST_BUCKET)
        meta = MetadataService(table=dynamodb_table)

        urls = storage.generate_upload_urls(tez_id=tez_id, files=files)
        # 1 context file + manifest.json + tez.md
        assert len(urls) == 3

        meta.create_tez(
            {
                "tez_id": tez_id,
                "creator": CREATOR_EMAIL,
                "name": "Single File Tez",
                "description": "One file only",
                "status": "pending_upload",
                "file_count": 1,
                "total_size": 100,
                "files": files,
                "recipients": [],
                "created_at": "2026-02-20T10:00:00Z",
                "updated_at": "2026-02-20T10:00:00Z",
            }
        )

        _upload_complete_bundle(s3_client, tez_id, files, contents)

        validation = storage.validate_uploads(tez_id=tez_id, files=files)
        assert validation.success is True

        meta.update_status(tez_id=tez_id, status="active")
        record = meta.get_tez(tez_id=tez_id)
        assert record is not None
        assert record["status"] == "active"

    def test_upload_flow_with_failed_validation_then_retry(
        self,
        s3_client: S3Client,
        dynamodb_table: Any,
        sample_files: list[dict[str, Any]],
        sample_file_contents: dict[str, bytes],
    ) -> None:
        """Simulate partial upload, failed confirm, then successful retry."""
        tez_id = "e2e-retry"
        storage = StorageService(s3_client=s3_client, bucket=TEST_BUCKET)
        meta = MetadataService(table=dynamodb_table)

        # Phase 1
        storage.generate_upload_urls(tez_id=tez_id, files=sample_files)
        meta.create_tez(
            {
                "tez_id": tez_id,
                "creator": CREATOR_EMAIL,
                "name": "Retry Test",
                "description": "",
                "status": "pending_upload",
                "file_count": len(sample_files),
                "total_size": sum(f["size"] for f in sample_files),
                "files": sample_files,
                "recipients": [],
                "created_at": "2026-02-20T10:00:00Z",
                "updated_at": "2026-02-20T10:00:00Z",
            }
        )

        # Phase 2 -- partial upload (only first context file, no manifests)
        first = sample_files[0]
        s3_client.put_object(
            Bucket=TEST_BUCKET,
            Key=f"{tez_id}/context/{first['name']}",
            Body=sample_file_contents[first["name"]],
            ContentType=first["content_type"],
        )

        # Phase 3 -- validation fails
        validation = storage.validate_uploads(tez_id=tez_id, files=sample_files)
        assert validation.success is False

        # Retry -- upload remaining context files + manifest files
        for f in sample_files[1:]:
            s3_client.put_object(
                Bucket=TEST_BUCKET,
                Key=f"{tez_id}/context/{f['name']}",
                Body=sample_file_contents[f["name"]],
                ContentType=f["content_type"],
            )
        _upload_manifest_files(s3_client, tez_id)

        # Re-validate -- should pass now
        validation_retry = storage.validate_uploads(tez_id=tez_id, files=sample_files)
        assert validation_retry.success is True

        meta.update_status(tez_id=tez_id, status="active")
        record = meta.get_tez(tez_id=tez_id)
        assert record is not None
        assert record["status"] == "active"

    def test_upload_flow_large_file_count(
        self,
        s3_client: S3Client,
        dynamodb_table: Any,
    ) -> None:
        """Stress test: upload a Tez with 20 files."""
        tez_id = "e2e-large"
        files = [
            {
                "name": f"file-{i:02d}.md",
                "size": 1024,
                "content_type": "text/markdown",
            }
            for i in range(20)
        ]
        contents = {f["name"]: f"# File {f['name']}\n".encode() for f in files}

        storage = StorageService(s3_client=s3_client, bucket=TEST_BUCKET)
        meta = MetadataService(table=dynamodb_table)

        urls = storage.generate_upload_urls(tez_id=tez_id, files=files)
        # 20 context files + manifest.json + tez.md
        assert len(urls) == 22

        meta.create_tez(
            {
                "tez_id": tez_id,
                "creator": CREATOR_EMAIL,
                "name": "Large Tez",
                "description": "20 files",
                "status": "pending_upload",
                "file_count": 20,
                "total_size": 20 * 1024,
                "files": files,
                "recipients": [],
                "created_at": "2026-02-20T10:00:00Z",
                "updated_at": "2026-02-20T10:00:00Z",
            }
        )

        _upload_complete_bundle(s3_client, tez_id, files, contents)

        validation = storage.validate_uploads(tez_id=tez_id, files=files)
        assert validation.success is True
        assert len(validation.verified_files) == 20

    def test_s3_versioning_creates_new_version_on_reupload(
        self,
        s3_client: S3Client,
    ) -> None:
        """Verify S3 versioning tracks reuploads."""
        tez_id = "e2e-version"
        key = f"{tez_id}/context/readme.md"

        # First upload
        r1 = s3_client.put_object(Bucket=TEST_BUCKET, Key=key, Body=b"version 1")
        version_1 = r1.get("VersionId")

        # Re-upload
        r2 = s3_client.put_object(Bucket=TEST_BUCKET, Key=key, Body=b"version 2")
        version_2 = r2.get("VersionId")

        # Versions should differ (versioning is enabled)
        if version_1 and version_2:
            assert version_1 != version_2

        # Current version should be the latest
        obj = s3_client.get_object(Bucket=TEST_BUCKET, Key=key)
        assert obj["Body"].read() == b"version 2"
