"""S3 storage operations for Tez packages.

Handles pre-signed URL generation (upload/download), file validation,
and Tez deletion. All file bytes flow directly between client and S3 --
this service only generates URLs and validates uploads.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from botocore.exceptions import ClientError

MANIFEST_FILENAME = "manifest.json"
TEZ_MD_FILENAME = "tez.md"
BUNDLE_FILES: tuple[str, ...] = (MANIFEST_FILENAME, TEZ_MD_FILENAME)

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client


@dataclass
class ValidationResult:
    """Result of validating file uploads against expected manifest.

    Attributes:
        success: True if all expected files are present in S3.
        missing: List of file entries that were not found.
        verified_files: List of file entries with etag/checksum from S3.
    """

    success: bool
    missing: list[dict[str, Any]] = field(default_factory=list)
    verified_files: list[dict[str, Any]] = field(default_factory=list)


class StorageService:
    """S3 operations for Tez file storage.

    Args:
        s3_client: A boto3 S3 client (or moto mock).
        bucket: The S3 bucket name (default: "tez-packages").
        account_id: AWS account ID for ExpectedBucketOwner verification.
            When set, all direct S3 calls include this parameter to guard
            against confused-deputy attacks. Optional for tests/local dev.
    """

    def __init__(
        self, s3_client: S3Client, bucket: str, account_id: str | None = None
    ) -> None:
        self.s3 = s3_client
        self.bucket = bucket
        self.account_id = account_id

    def generate_upload_urls(
        self,
        tez_id: str,
        files: list[dict[str, Any]],
        expires_in: int = 900,
    ) -> dict[str, str]:
        """Generate pre-signed PUT URLs for context files + manifest files.

        Args:
            tez_id: The Tez identifier.
            files: List of file dicts with "name" and "content_type" keys.
            expires_in: URL expiry in seconds (default 15 min).

        Returns:
            Dict mapping filename -> pre-signed PUT URL.
        """
        urls: dict[str, str] = {}
        for f in files:
            key = f"{tez_id}/context/{f['name']}"
            url = self.s3.generate_presigned_url(
                "put_object",
                Params={
                    "Bucket": self.bucket,
                    "Key": key,
                    "ContentType": f["content_type"],
                },
                ExpiresIn=expires_in,
            )
            urls[f["name"]] = url
        for name, content_type in (
            (MANIFEST_FILENAME, "application/json"),
            (TEZ_MD_FILENAME, "text/markdown"),
        ):
            key = f"{tez_id}/{name}"
            url = self.s3.generate_presigned_url(
                "put_object",
                Params={
                    "Bucket": self.bucket,
                    "Key": key,
                    "ContentType": content_type,
                },
                ExpiresIn=expires_in,
            )
            urls[name] = url
        return urls

    def generate_download_urls(
        self,
        tez_id: str,
        files: list[dict[str, Any]],
        expires_in: int = 3600,
    ) -> dict[str, str]:
        """Generate pre-signed GET URLs for all files + manifest files.

        Args:
            tez_id: The Tez identifier.
            files: List of file dicts with "name" key.
            expires_in: URL expiry in seconds.

        Returns:
            Dict mapping filename -> pre-signed GET URL.
        """
        urls: dict[str, str] = {}
        for f in files:
            key = f"{tez_id}/context/{f['name']}"
            urls[f["name"]] = self.s3.generate_presigned_url(
                "get_object",
                Params={"Bucket": self.bucket, "Key": key},
                ExpiresIn=expires_in,
            )
        for name in (MANIFEST_FILENAME, TEZ_MD_FILENAME):
            key = f"{tez_id}/{name}"
            urls[name] = self.s3.generate_presigned_url(
                "get_object",
                Params={"Bucket": self.bucket, "Key": key},
                ExpiresIn=expires_in,
            )
        return urls

    def _head_object(self, key: str) -> Any | None:
        """HeadObject with optional bucket-owner check. Returns None on 404."""
        head_args: dict[str, Any] = {"Bucket": self.bucket, "Key": key}
        if self.account_id:
            head_args["ExpectedBucketOwner"] = self.account_id
        try:
            return self.s3.head_object(**head_args)
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return None
            raise

    def validate_uploads(
        self,
        tez_id: str,
        files: list[dict[str, Any]],
    ) -> ValidationResult:
        """Validate that all expected files landed in S3.

        Args:
            tez_id: The Tez identifier.
            files: List of expected context file dicts with "name" key.

        Returns:
            ValidationResult with success flag, missing files, and
            verified files (with etag).
        """
        missing: list[dict[str, Any]] = []
        verified: list[dict[str, Any]] = []

        for f in files:
            key = f"{tez_id}/context/{f['name']}"
            resp = self._head_object(key)
            if resp:
                verified.append({**f, "etag": resp["ETag"]})
            else:
                missing.append(f)

        for name in (MANIFEST_FILENAME, TEZ_MD_FILENAME):
            key = f"{tez_id}/{name}"
            if not self._head_object(key):
                missing.append({"name": name})

        return ValidationResult(
            success=len(missing) == 0,
            missing=missing,
            verified_files=verified,
        )

    def delete_tez(self, tez_id: str) -> None:
        """Delete all S3 objects under the Tez prefix.

        Args:
            tez_id: The Tez identifier.
        """
        prefix = f"{tez_id}/"
        list_args: dict[str, Any] = {"Bucket": self.bucket, "Prefix": prefix}
        if self.account_id:
            list_args["ExpectedBucketOwner"] = self.account_id
        response = self.s3.list_objects_v2(**list_args)
        objects = response.get("Contents", [])
        if not objects:
            return
        del_args: dict[str, Any] = {
            "Bucket": self.bucket,
            "Delete": {"Objects": [{"Key": o["Key"]} for o in objects]},
        }
        if self.account_id:
            del_args["ExpectedBucketOwner"] = self.account_id
        self.s3.delete_objects(**del_args)
