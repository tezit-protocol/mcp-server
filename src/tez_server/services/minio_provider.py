"""MinIO storage implementation of StorageProvider.

Handles pre-signed URL generation (upload/download), file validation,
and Tez deletion using the MinIO Python SDK. Functionally equivalent to
StorageService (S3) but targets a MinIO endpoint instead of AWS S3.

All file bytes flow directly between client and MinIO --
this service only generates URLs and validates uploads.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any, NoReturn

from minio import Minio
from minio.deleteobjects import DeleteObject
from minio.error import S3Error

from tez_server.services.storage import (
    DEFAULT_URL_EXPIRY,
    MANIFEST_FILENAME,
    TEZ_MD_FILENAME,
    StorageProvider,
    StorageProviderError,
    ValidationResult,
)


class MinIOStorageProvider(StorageProvider):
    """MinIO implementation of :class:`StorageProvider`.

    Args:
        client: A ``minio.Minio`` client instance.
        bucket: The MinIO bucket name.
    """

    def __init__(self, client: Minio, bucket: str) -> None:
        self._client = client
        self.bucket = bucket

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _raise_for_s3_error(self, e: S3Error) -> NoReturn:
        """Convert a :class:`S3Error` into a :class:`StorageProviderError`."""
        code = e.code or "Unknown"
        if code in ("InvalidAccessKeyId", "SignatureDoesNotMatch", "AccessDenied"):
            raise StorageProviderError(
                f"Invalid or unauthorized MinIO credentials: {e.message}"
            ) from e
        if code == "NoSuchBucket":
            raise StorageProviderError(
                f"MinIO bucket '{self.bucket}' does not exist: {e.message}"
            ) from e
        raise StorageProviderError(f"MinIO error ({code}): {e.message}") from e

    def _raise_for_network_error(self, e: Exception) -> NoReturn:
        """Convert a network-level error into a :class:`StorageProviderError`."""
        raise StorageProviderError(
            f"Network error reaching MinIO bucket '{self.bucket}': {e}"
        ) from e

    def _stat_object(self, key: str) -> bool:
        """Return True if the object exists, False if not found.

        Raises:
            StorageProviderError: For any error other than NoSuchKey/NoSuchObject.
        """
        try:
            self._client.stat_object(self.bucket, key)
            return True
        except S3Error as e:
            if e.code in ("NoSuchKey", "NoSuchObject"):
                return False
            self._raise_for_s3_error(e)
        except Exception as e:
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
                not exist, or the backend is unreachable.
        """
        try:
            expiry = timedelta(seconds=expires_in)
            urls: dict[str, str] = {}
            for f in files:
                key = f"{tez_id}/context/{f['name']}"
                urls[f["name"]] = self._client.presigned_put_object(
                    self.bucket, key, expires=expiry
                )
            for name in (MANIFEST_FILENAME, TEZ_MD_FILENAME):
                key = f"{tez_id}/{name}"
                urls[name] = self._client.presigned_put_object(
                    self.bucket, key, expires=expiry
                )
            return urls
        except S3Error as e:
            self._raise_for_s3_error(e)
        except Exception as e:
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
                not exist, or the backend is unreachable.
        """
        try:
            expiry = timedelta(seconds=expires_in)
            urls: dict[str, str] = {}
            for f in files:
                key = f"{tez_id}/context/{f['name']}"
                urls[f["name"]] = self._client.presigned_get_object(
                    self.bucket, key, expires=expiry
                )
            for name in (MANIFEST_FILENAME, TEZ_MD_FILENAME):
                key = f"{tez_id}/{name}"
                urls[name] = self._client.presigned_get_object(
                    self.bucket, key, expires=expiry
                )
            return urls
        except S3Error as e:
            self._raise_for_s3_error(e)
        except Exception as e:
            self._raise_for_network_error(e)

    def validate_uploads(
        self,
        tez_id: str,
        files: list[dict[str, Any]],
    ) -> ValidationResult:
        """Check that all expected files are present in MinIO.

        Issues a stat_object request per file. Does not download file content.

        Args:
            tez_id: The Tez identifier.
            files: List of expected context file dicts with a ``"name"`` key.

        Returns:
            :class:`ValidationResult` with success flag, missing files,
            and verified files.

        Raises:
            StorageProviderError: If a storage error other than NoSuchKey occurs.
        """
        missing: list[dict[str, Any]] = []
        verified: list[dict[str, Any]] = []

        for f in files:
            key = f"{tez_id}/context/{f['name']}"
            if self._stat_object(key):
                verified.append(f)
            else:
                missing.append(f)

        for name in (MANIFEST_FILENAME, TEZ_MD_FILENAME):
            key = f"{tez_id}/{name}"
            if not self._stat_object(key):
                missing.append({"name": name})

        return ValidationResult(
            success=len(missing) == 0,
            missing=missing,
            verified_files=verified,
        )

    def delete_tez(self, tez_id: str) -> None:
        """Delete all MinIO objects under the Tez prefix.

        Lists all objects under ``tez_id/`` and deletes them in one call.
        No-ops silently if the Tez has no objects.

        Args:
            tez_id: The Tez identifier.

        Raises:
            StorageProviderError: If the deletion fails due to a storage error.
        """
        try:
            prefix = f"{tez_id}/"
            objects = self._client.list_objects(
                self.bucket, prefix=prefix, recursive=True
            )
            delete_list = [DeleteObject(obj.object_name) for obj in objects]
            if not delete_list:
                return
            errors = list(self._client.remove_objects(self.bucket, iter(delete_list)))
            if errors:
                raise StorageProviderError(
                    f"MinIO deletion errors for tez '{tez_id}': "
                    + ", ".join(str(e) for e in errors)
                )
        except StorageProviderError:
            raise
        except S3Error as e:
            self._raise_for_s3_error(e)
        except Exception as e:
            self._raise_for_network_error(e)
