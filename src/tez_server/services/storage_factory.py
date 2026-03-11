"""Factory for selecting a StorageProvider based on environment configuration.

Set ``STORAGE_BACKEND`` to ``"s3"`` (default) or ``"minio"`` to choose the
active storage adapter. Missing required env vars raise StorageProviderError
immediately at startup rather than at first use.
"""

from __future__ import annotations

import os
from functools import cache
from typing import Any

import boto3

from tez_server.services.storage import (
    StorageProvider,
    StorageProviderError,
    StorageService,
)


def get_storage_provider() -> StorageProvider:
    """Return the configured storage provider.

    Reads ``STORAGE_BACKEND`` env var (default: ``"s3"``):

    - ``"s3"``: Uses AWS S3 via boto3. Reads ``TEZ_S3_BUCKET``,
      ``TEZ_AWS_REGION``, and optionally ``TEZ_AWS_ACCOUNT_ID``.
    - ``"minio"``: Uses MinIO via the minio SDK. Reads ``MINIO_ENDPOINT``,
      ``MINIO_ACCESS_KEY``, ``MINIO_SECRET_KEY``, ``MINIO_BUCKET``,
      and optionally ``MINIO_SECURE`` (default ``"true"``).

    Raises:
        StorageProviderError: If a required env var is missing or
            ``STORAGE_BACKEND`` is set to an unknown value.
    """
    backend = os.environ.get("STORAGE_BACKEND", "s3").lower()

    if backend == "s3":
        return _build_s3_provider()
    if backend == "minio":
        return _build_minio_provider()

    raise StorageProviderError(
        f"Unknown STORAGE_BACKEND '{backend}'. Valid values: 's3', 'minio'."
    )


@cache
def _get_s3_client(region: str) -> Any:
    """Return a cached boto3 S3 client for the given region."""
    return boto3.client("s3", region_name=region)


def _build_s3_provider() -> StorageService:
    bucket = os.environ.get("TEZ_S3_BUCKET", "tez-packages")
    region = os.environ.get("TEZ_AWS_REGION", "eu-west-2")
    account_id = os.environ.get("TEZ_AWS_ACCOUNT_ID")
    s3_client = _get_s3_client(region)
    return StorageService(s3_client=s3_client, bucket=bucket, account_id=account_id)


def _build_minio_provider() -> StorageProvider:
    from minio import Minio  # noqa: PLC0415
    from tez_server.services.minio_provider import MinIOStorageProvider  # noqa: PLC0415

    endpoint = os.environ.get("MINIO_ENDPOINT")
    access_key = os.environ.get("MINIO_ACCESS_KEY")
    secret_key = os.environ.get("MINIO_SECRET_KEY")
    bucket = os.environ.get("MINIO_BUCKET")
    secure = os.environ.get("MINIO_SECURE", "true").lower() != "false"

    missing = [
        name
        for name, val in [
            ("MINIO_ENDPOINT", endpoint),
            ("MINIO_ACCESS_KEY", access_key),
            ("MINIO_SECRET_KEY", secret_key),
            ("MINIO_BUCKET", bucket),
        ]
        if not val
    ]
    if missing:
        raise StorageProviderError(
            f"Missing required env vars for MinIO backend: {', '.join(missing)}"
        )

    assert endpoint is not None
    assert access_key is not None
    assert secret_key is not None
    assert bucket is not None
    client = Minio(
        endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure,
    )
    return MinIOStorageProvider(client=client, bucket=bucket)
