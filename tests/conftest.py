"""Shared fixtures for Tez test suite.

Provides moto-backed S3 and DynamoDB resources, sample data factories,
and reusable service instances. All fixtures are function-scoped by default
so each test gets a clean environment.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import boto3
import pytest
from moto import mock_aws

if TYPE_CHECKING:
    from collections.abc import Generator

    from mypy_boto3_dynamodb import DynamoDBServiceResource
    from mypy_boto3_s3 import S3Client

# ---------------------------------------------------------------------------
# Constants -- must match production config
# ---------------------------------------------------------------------------
TEST_BUCKET = "tez-packages"
TEST_REGION = os.environ.get("TEZ_TEST_REGION", "ap-southeast-2")
TEST_TABLE = "tez-metadata"
TEST_ACCOUNT_ID = "123456789012"
CREATOR_EMAIL = "adam@ragu.ai"
RECIPIENT_EMAIL = "noor@ragu.ai"


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------
def create_test_resources() -> None:
    """Create S3 bucket and DynamoDB table inside an active mock_aws context."""
    s3 = boto3.client("s3", region_name=TEST_REGION)
    s3.create_bucket(
        Bucket=TEST_BUCKET,
        CreateBucketConfiguration={"LocationConstraint": TEST_REGION},
    )
    dynamo = boto3.resource("dynamodb", region_name=TEST_REGION)
    dynamo.create_table(
        TableName=TEST_TABLE,
        KeySchema=[{"AttributeName": "tez_id", "KeyType": "HASH"}],
        AttributeDefinitions=[
            {"AttributeName": "tez_id", "AttributeType": "S"},
            {"AttributeName": "creator", "AttributeType": "S"},
            {"AttributeName": "created_at", "AttributeType": "S"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "creator-index",
                "KeySchema": [
                    {"AttributeName": "creator", "KeyType": "HASH"},
                    {"AttributeName": "created_at", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
        ],
        BillingMode="PAY_PER_REQUEST",
    )


# ---------------------------------------------------------------------------
# AWS mocks
# ---------------------------------------------------------------------------
@pytest.fixture()
def aws_credentials() -> None:
    """Set dummy AWS credentials for moto."""
    import os

    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = TEST_REGION


@pytest.fixture()
def s3_client(aws_credentials: None) -> Generator[S3Client, None, None]:
    """Provide a moto-backed S3 client with the Tez bucket pre-created."""
    with mock_aws():
        client = boto3.client("s3", region_name=TEST_REGION)
        client.create_bucket(
            Bucket=TEST_BUCKET,
            CreateBucketConfiguration={"LocationConstraint": TEST_REGION},
        )
        # Enable versioning (matches production)
        client.put_bucket_versioning(
            Bucket=TEST_BUCKET,
            VersioningConfiguration={"Status": "Enabled"},
            ExpectedBucketOwner=TEST_ACCOUNT_ID,
        )
        yield client


@pytest.fixture()
def dynamodb_resource(
    aws_credentials: None,
) -> Generator[DynamoDBServiceResource, None, None]:
    """Provide a moto-backed DynamoDB resource with the metadata table."""
    with mock_aws():
        resource = boto3.resource("dynamodb", region_name=TEST_REGION)
        resource.create_table(
            TableName=TEST_TABLE,
            KeySchema=[
                {"AttributeName": "tez_id", "KeyType": "HASH"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "tez_id", "AttributeType": "S"},
                {"AttributeName": "creator", "AttributeType": "S"},
                {"AttributeName": "created_at", "AttributeType": "S"},
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "creator-index",
                    "KeySchema": [
                        {"AttributeName": "creator", "KeyType": "HASH"},
                        {"AttributeName": "created_at", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                },
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        yield resource


@pytest.fixture()
def dynamodb_table(
    dynamodb_resource: DynamoDBServiceResource,
) -> Any:
    """Return the DynamoDB Table object directly."""
    return dynamodb_resource.Table(TEST_TABLE)


# ---------------------------------------------------------------------------
# Sample data factories
# ---------------------------------------------------------------------------
@pytest.fixture()
def sample_files() -> list[dict[str, Any]]:
    """Return a list of sample file entries for testing."""
    return [
        {
            "name": "transcript.md",
            "size": 24576,
            "content_type": "text/markdown",
        },
        {
            "name": "action-items.md",
            "size": 4096,
            "content_type": "text/markdown",
        },
        {
            "name": "slides.pdf",
            "size": 1048576,
            "content_type": "application/pdf",
        },
    ]


@pytest.fixture()
def sample_tez_record(sample_files: list[dict[str, Any]]) -> dict[str, Any]:
    """Return a complete DynamoDB Tez record."""
    return {
        "tez_id": "a3f8b2c1",
        "creator": CREATOR_EMAIL,
        "creator_name": "Adam Cross",
        "name": "Q1 Standup Notes",
        "description": "Notes from Q1 standup -- Jan 2026",
        "status": "active",
        "file_count": len(sample_files),
        "total_size": sum(f["size"] for f in sample_files),
        "files": sample_files,
        "recipients": [],
        "created_at": datetime.now(tz=UTC).isoformat(),
        "updated_at": datetime.now(tz=UTC).isoformat(),
    }


@pytest.fixture()
def sample_file_contents() -> dict[str, bytes]:
    """Return sample file contents for upload testing."""
    return {
        "transcript.md": b"# Meeting Transcript\n\nAttendees: Adam, Noor\n",
        "action-items.md": b"# Action Items\n\n- [ ] Ship Tez POC\n",
        "slides.pdf": b"%PDF-1.4 fake pdf content for testing purposes",
    }


@pytest.fixture()
def mock_auth() -> MagicMock:
    """Return a mock auth object that resolves to a known email."""
    auth = MagicMock()
    auth.get_caller_email.return_value = CREATOR_EMAIL
    return auth
