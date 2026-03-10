"""S3 storage operations for Tez packages.

Handles pre-signed URL generation (upload/download), file validation,
and Tez deletion. All file bytes flow directly between client and S3 --
this service only generates URLs and validates uploads.
"""

from __future__ import annotations

import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, NoReturn

from botocore.exceptions import (
    ClientError,
    ConnectTimeoutError,
    EndpointResolutionError,
    ReadTimeoutError,
)

MANIFEST_FILENAME = "manifest.json"
TEZ_MD_FILENAME = "tez.md"
BUNDLE_FILES: tuple[str, ...] = (MANIFEST_FILENAME, TEZ_MD_FILENAME)

DEFAULT_URL_EXPIRY: int = int(os.environ.get("STORAGE_URL_EXPIRY_SECONDS", "900"))

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client


class StorageProviderError(Exception):
    """Raised when a storage operation fails.

    Wraps underlying provider errors (boto3, minio, etc.) so callers
    never need to import provider-specific exception types.
    """


@dataclass
class ValidationResult:
    """Result of validating file uploads against expected manifest.

    Attributes:
        success: True if all expected files are present in storage.
        missing: List of file entries that were not found.
        verified_files: List of file entries with etag/checksum from storage.
    """

    success: bool
    missing: list[dict[str, Any]] = field(default_factory=list)
    verified_files: list[dict[str, Any]] = field(default_factory=list)


class StorageProvider(ABC):
    """Abstract interface for Tez file storage backends.

    Implement this class to add a new storage adapter (e.g. MinIO, GCS).
    All methods must raise :exc:`StorageProviderError` on failure --
    never let provider-specific exceptions propagate to the caller.
    """

    @abstractmethod
    def generate_upload_urls(
        self,
        tez_id: str,
        files: list[dict[str, Any]],
        expires_in: int = DEFAULT_URL_EXPIRY,
    ) -> dict[str, str]:
        """Generate pre-signed PUT URLs for uploading Tez files.

        Args:
            tez_id: The Tez identifier.
            files: List of file dicts with ``"name"`` and ``"content_type"`` keys.
            expires_in: URL expiry in seconds. Defaults to the
                ``STORAGE_URL_EXPIRY_SECONDS`` env var (900 if not set).

        Returns:
            Dict mapping filename -> pre-signed PUT URL. Includes URLs for
            every entry in ``files`` plus ``manifest.json`` and ``tez.md``.

        Raises:
            StorageProviderError: If credentials are invalid, the bucket does
                not exist, access is denied, or the backend is unreachable.
        """

    @abstractmethod
    def generate_download_urls(
        self,
        tez_id: str,
        files: list[dict[str, Any]],
        expires_in: int = DEFAULT_URL_EXPIRY,
    ) -> dict[str, str]:
        """Generate pre-signed GET URLs for downloading Tez files.

        Args:
            tez_id: The Tez identifier.
            files: List of file dicts with a ``"name"`` key.
            expires_in: URL expiry in seconds. Defaults to the
                ``STORAGE_URL_EXPIRY_SECONDS`` env var (900 if not set).

        Returns:
            Dict mapping filename -> pre-signed GET URL. Includes URLs for
            every entry in ``files`` plus ``manifest.json`` and ``tez.md``.

        Raises:
            StorageProviderError: If credentials are invalid, the bucket does
                not exist, access is denied, or the backend is unreachable.
        """

    @abstractmethod
    def validate_uploads(
        self,
        tez_id: str,
        files: list[dict[str, Any]],
    ) -> ValidationResult:
        """Check that all expected files are present in storage.

        Issues a HEAD request per file. Does not download file content.

        Args:
            tez_id: The Tez identifier.
            files: List of expected context file dicts with a ``"name"`` key.

        Returns:
            :class:`ValidationResult` with:

            - ``success``: ``True`` if every file is present.
            - ``missing``: File dicts not found in storage.
            - ``verified_files``: Found file dicts enriched with an ``"etag"`` field.

        Raises:
            StorageProviderError: If a storage error other than 404 occurs
                (e.g. permission denied, credentials expired, network timeout).
        """

    @abstractmethod
    def delete_tez(self, tez_id: str) -> None:
        """Delete all storage objects under the Tez prefix.

        Lists all objects under ``tez_id/`` and deletes them in a single
        batch request. No-ops silently if the Tez has no objects.

        Args:
            tez_id: The Tez identifier.

        Raises:
            StorageProviderError: If the deletion fails due to a storage
                error (e.g. permission denied, credentials expired).
        """


class StorageService(StorageProvider):
    """S3 implementation of :class:`StorageProvider`.

    Args:
        s3_client: A boto3 S3 client (or moto mock).
        bucket: The S3 bucket name.
        account_id: AWS account ID for ``ExpectedBucketOwner`` verification.
            When set, all direct S3 calls include this parameter to guard
            against confused-deputy attacks. Optional for tests/local dev.
    """

    def __init__(
        self, s3_client: S3Client, bucket: str, account_id: str | None = None
    ) -> None:
        self.s3 = s3_client
        self.bucket = bucket
        self.account_id = account_id

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _raise_for_client_error(self, e: ClientError) -> NoReturn:
        """Convert a :class:`ClientError` into a :class:`StorageProviderError`."""
        code = e.response["Error"]["Code"]
        msg = e.response["Error"]["Message"]
        if code == "InvalidClientTokenId":
            raise StorageProviderError(
                f"Invalid or expired AWS credentials: {msg}"
            ) from e
        if code == "NoSuchBucket":
            raise StorageProviderError(
                f"S3 bucket '{self.bucket}' does not exist: {msg}"
            ) from e
        if code in ("AccessDenied", "403"):
            raise StorageProviderError(
                f"Permission denied on bucket '{self.bucket}': {msg}"
            ) from e
        raise StorageProviderError(f"S3 error ({code}): {msg}") from e

    def _raise_for_network_error(
        self,
        e: ConnectTimeoutError | ReadTimeoutError | EndpointResolutionError,
    ) -> NoReturn:
        """Convert a network-level botocore error into a StorageProviderError."""
        raise StorageProviderError(
            f"Network error reaching S3 bucket '{self.bucket}': {e}"
        ) from e

    def _head_object(self, key: str) -> Any | None:
        """Issue HeadObject with optional bucket-owner check.

        Returns:
            The HeadObject response dict, or ``None`` if the object does
            not exist (404).

        Raises:
            StorageProviderError: For any error other than 404.
        """
        head_args: dict[str, Any] = {"Bucket": self.bucket, "Key": key}
        if self.account_id:
            head_args["ExpectedBucketOwner"] = self.account_id
        try:
            return self.s3.head_object(**head_args)
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return None
            self._raise_for_client_error(e)
        except (ConnectTimeoutError, ReadTimeoutError, EndpointResolutionError) as e:
            self._raise_for_network_error(e)

    # ------------------------------------------------------------------
    # StorageProvider interface
    # ------------------------------------------------------------------

    def generate_upload_urls(
        self,
        tez_id: str,
        files: list[dict[str, Any]],
        expires_in: int = DEFAULT_URL_EXPIRY,
    ) -> dict[str, str]:
        """Generate pre-signed PUT URLs for context files + manifest files.

        Args:
            tez_id: The Tez identifier.
            files: List of file dicts with ``"name"`` and ``"content_type"`` keys.
            expires_in: URL expiry in seconds. Defaults to
                ``STORAGE_URL_EXPIRY_SECONDS`` env var (900 if not set).

        Returns:
            Dict mapping filename -> pre-signed PUT URL.

        Raises:
            StorageProviderError: If credentials are invalid, the bucket does
                not exist, access is denied, or the backend is unreachable.
        """
        try:
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
        except ClientError as e:
            self._raise_for_client_error(e)
        except (ConnectTimeoutError, ReadTimeoutError, EndpointResolutionError) as e:
            self._raise_for_network_error(e)

    def generate_download_urls(
        self,
        tez_id: str,
        files: list[dict[str, Any]],
        expires_in: int = DEFAULT_URL_EXPIRY,
    ) -> dict[str, str]:
        """Generate pre-signed GET URLs for all files + manifest files.

        Args:
            tez_id: The Tez identifier.
            files: List of file dicts with a ``"name"`` key.
            expires_in: URL expiry in seconds. Defaults to
                ``STORAGE_URL_EXPIRY_SECONDS`` env var (900 if not set).

        Returns:
            Dict mapping filename -> pre-signed GET URL.

        Raises:
            StorageProviderError: If credentials are invalid, the bucket does
                not exist, access is denied, or the backend is unreachable.
        """
        try:
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
        except ClientError as e:
            self._raise_for_client_error(e)
        except (ConnectTimeoutError, ReadTimeoutError, EndpointResolutionError) as e:
            self._raise_for_network_error(e)

    def validate_uploads(
        self,
        tez_id: str,
        files: list[dict[str, Any]],
    ) -> ValidationResult:
        """Validate that all expected files landed in S3.

        Args:
            tez_id: The Tez identifier.
            files: List of expected context file dicts with a ``"name"`` key.

        Returns:
            :class:`ValidationResult` with success flag, missing files,
            and verified files (with etag).

        Raises:
            StorageProviderError: If a storage error other than 404 occurs.
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

        Raises:
            StorageProviderError: If the deletion fails due to a storage error.
        """
        try:
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
        except ClientError as e:
            self._raise_for_client_error(e)
        except (ConnectTimeoutError, ReadTimeoutError, EndpointResolutionError) as e:
            self._raise_for_network_error(e)
