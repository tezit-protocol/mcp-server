"""Infrastructure tests for S3 and DynamoDB configuration.

These tests verify that the AWS resources are configured correctly --
bucket policies, versioning, table schemas, GSIs, etc. They use moto
to simulate the infrastructure and assert expected behaviour.

These tests do NOT depend on StorageService or MetadataService -- they
test the raw AWS resource configuration directly.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import boto3
import pytest
from botocore.exceptions import ClientError

from tests.conftest import (
    CREATOR_EMAIL,
    RECIPIENT_EMAIL,
    TEST_BUCKET,
    TEST_REGION,
    TEST_TABLE,
)

if TYPE_CHECKING:
    from mypy_boto3_dynamodb import DynamoDBServiceResource
    from mypy_boto3_s3 import S3Client


# ===================================================================
# S3 Bucket Configuration
# ===================================================================
class TestS3BucketConfig:
    """Verify the Tez S3 bucket is configured correctly."""

    def test_bucket_exists(self, s3_client: S3Client) -> None:
        response = s3_client.list_buckets()
        bucket_names = [b["Name"] for b in response["Buckets"]]
        assert TEST_BUCKET in bucket_names

    def test_bucket_region(self, s3_client: S3Client) -> None:
        response = s3_client.get_bucket_location(Bucket=TEST_BUCKET)
        assert response["LocationConstraint"] == TEST_REGION

    def test_versioning_enabled(self, s3_client: S3Client) -> None:
        response = s3_client.get_bucket_versioning(Bucket=TEST_BUCKET)
        assert response["Status"] == "Enabled"

    def test_versioning_creates_versions(self, s3_client: S3Client) -> None:
        key = "version-test/file.txt"

        s3_client.put_object(Bucket=TEST_BUCKET, Key=key, Body=b"v1")
        s3_client.put_object(Bucket=TEST_BUCKET, Key=key, Body=b"v2")

        versions = s3_client.list_object_versions(Bucket=TEST_BUCKET, Prefix=key)
        assert len(versions.get("Versions", [])) == 2


# ===================================================================
# S3 Key Structure
# ===================================================================
class TestS3KeyStructure:
    """Verify that S3 key naming follows the Tezit Protocol convention."""

    def test_tez_files_under_context_prefix(self, s3_client: S3Client) -> None:
        tez_id = "key-struct-1"
        s3_client.put_object(
            Bucket=TEST_BUCKET,
            Key=f"{tez_id}/context/readme.md",
            Body=b"hello",
        )

        response = s3_client.list_objects_v2(
            Bucket=TEST_BUCKET, Prefix=f"{tez_id}/context/"
        )
        assert response["KeyCount"] == 1
        assert response["Contents"][0]["Key"] == f"{tez_id}/context/readme.md"

    def test_manifest_at_tez_root(self, s3_client: S3Client) -> None:
        tez_id = "key-struct-2"
        s3_client.put_object(
            Bucket=TEST_BUCKET,
            Key=f"{tez_id}/manifest.json",
            Body=b"{}",
        )

        response = s3_client.head_object(
            Bucket=TEST_BUCKET, Key=f"{tez_id}/manifest.json"
        )
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_tez_md_at_tez_root(self, s3_client: S3Client) -> None:
        tez_id = "key-struct-3"
        s3_client.put_object(
            Bucket=TEST_BUCKET,
            Key=f"{tez_id}/tez.md",
            Body=b"# Tez",
        )

        response = s3_client.head_object(Bucket=TEST_BUCKET, Key=f"{tez_id}/tez.md")
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_prefix_isolation_between_tez(self, s3_client: S3Client) -> None:
        """Files from different Tez IDs do not interfere."""
        s3_client.put_object(Bucket=TEST_BUCKET, Key="tez-aaa/context/a.md", Body=b"a")
        s3_client.put_object(Bucket=TEST_BUCKET, Key="tez-bbb/context/b.md", Body=b"b")

        r_a = s3_client.list_objects_v2(Bucket=TEST_BUCKET, Prefix="tez-aaa/")
        r_b = s3_client.list_objects_v2(Bucket=TEST_BUCKET, Prefix="tez-bbb/")

        assert r_a["KeyCount"] == 1
        assert r_b["KeyCount"] == 1
        assert r_a["Contents"][0]["Key"] == "tez-aaa/context/a.md"
        assert r_b["Contents"][0]["Key"] == "tez-bbb/context/b.md"

    def test_head_object_returns_404_for_missing_key(self, s3_client: S3Client) -> None:
        with pytest.raises(ClientError) as exc_info:
            s3_client.head_object(Bucket=TEST_BUCKET, Key="nonexistent/file.txt")
        assert exc_info.value.response["Error"]["Code"] == "404"


# ===================================================================
# S3 Content Type Handling
# ===================================================================
class TestS3ContentTypes:
    """Verify content types are stored and retrievable correctly."""

    @pytest.mark.parametrize(
        "filename,content_type",
        [
            ("notes.md", "text/markdown"),
            ("report.pdf", "application/pdf"),
            (
                "data.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            ),
            ("photo.png", "image/png"),
            ("recording.mp4", "video/mp4"),
            ("config.json", "application/json"),
        ],
    )
    def test_content_type_roundtrip(
        self,
        s3_client: S3Client,
        filename: str,
        content_type: str,
    ) -> None:
        key = f"ct-test/context/{filename}"
        s3_client.put_object(
            Bucket=TEST_BUCKET,
            Key=key,
            Body=b"test content",
            ContentType=content_type,
        )

        response = s3_client.head_object(Bucket=TEST_BUCKET, Key=key)
        assert response["ContentType"] == content_type


# ===================================================================
# S3 Pre-signed URL Behaviour
# ===================================================================
class TestS3PresignedUrls:
    """Verify pre-signed URL generation works as expected."""

    def test_put_presigned_url_contains_signature(self, s3_client: S3Client) -> None:
        url = s3_client.generate_presigned_url(
            "put_object",
            Params={"Bucket": TEST_BUCKET, "Key": "test/file.md"},
            ExpiresIn=900,
        )
        # Moto may use v2 (AWSAccessKeyId/Signature) or v4 (X-Amz-Algorithm)
        assert "Signature" in url or "X-Amz-Signature" in url

    def test_get_presigned_url_contains_signature(self, s3_client: S3Client) -> None:
        url = s3_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": TEST_BUCKET, "Key": "test/file.md"},
            ExpiresIn=3600,
        )
        assert "Signature" in url or "X-Amz-Signature" in url

    def test_presigned_url_includes_expiry(self, s3_client: S3Client) -> None:
        url = s3_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": TEST_BUCKET, "Key": "test/file.md"},
            ExpiresIn=1800,
        )
        # v2 uses Expires (unix timestamp), v4 uses X-Amz-Expires (seconds)
        assert "Expires" in url or "X-Amz-Expires" in url

    def test_presigned_url_scoped_to_key(self, s3_client: S3Client) -> None:
        url = s3_client.generate_presigned_url(
            "get_object",
            Params={
                "Bucket": TEST_BUCKET,
                "Key": "abc123/context/notes.md",
            },
            ExpiresIn=3600,
        )
        assert "abc123" in url
        assert "notes.md" in url


# ===================================================================
# S3 Delete Operations
# ===================================================================
class TestS3DeleteOperations:
    """Verify bulk delete behaviour for Tez cleanup."""

    def test_delete_objects_removes_all(self, s3_client: S3Client) -> None:
        tez_id = "del-test"
        keys = [
            f"{tez_id}/manifest.json",
            f"{tez_id}/tez.md",
            f"{tez_id}/context/a.md",
            f"{tez_id}/context/b.md",
        ]

        for key in keys:
            s3_client.put_object(Bucket=TEST_BUCKET, Key=key, Body=b"content")

        # Delete all objects
        s3_client.delete_objects(
            Bucket=TEST_BUCKET,
            Delete={"Objects": [{"Key": k} for k in keys]},
        )

        response = s3_client.list_objects_v2(Bucket=TEST_BUCKET, Prefix=f"{tez_id}/")
        assert response.get("KeyCount", 0) == 0

    def test_delete_nonexistent_key_does_not_error(self, s3_client: S3Client) -> None:
        # Deleting a nonexistent key should succeed silently
        response = s3_client.delete_objects(
            Bucket=TEST_BUCKET,
            Delete={"Objects": [{"Key": "ghost/file.txt"}]},
        )
        assert "Errors" not in response or len(response["Errors"]) == 0


# ===================================================================
# DynamoDB Table Configuration
# ===================================================================
class TestDynamoDBTableConfig:
    """Verify the metadata table schema and indexes."""

    def test_table_exists(self, dynamodb_resource: DynamoDBServiceResource) -> None:
        table_names = [t.name for t in dynamodb_resource.tables.all()]
        assert TEST_TABLE in table_names

    def test_partition_key_is_tez_id(
        self, dynamodb_resource: DynamoDBServiceResource
    ) -> None:
        table = dynamodb_resource.Table(TEST_TABLE)
        key_schema = table.key_schema
        assert len(key_schema) == 1
        assert key_schema[0]["AttributeName"] == "tez_id"
        assert key_schema[0]["KeyType"] == "HASH"

    def test_creator_index_exists(
        self, dynamodb_resource: DynamoDBServiceResource
    ) -> None:
        table = dynamodb_resource.Table(TEST_TABLE)
        gsis = table.global_secondary_indexes or []
        gsi_names = [gsi["IndexName"] for gsi in gsis]
        assert "creator-index" in gsi_names

    def test_creator_index_key_schema(
        self, dynamodb_resource: DynamoDBServiceResource
    ) -> None:
        table = dynamodb_resource.Table(TEST_TABLE)
        gsis = table.global_secondary_indexes or []
        creator_gsi = next(gsi for gsi in gsis if gsi["IndexName"] == "creator-index")

        key_schema = creator_gsi["KeySchema"]
        pk = next(k for k in key_schema if k["KeyType"] == "HASH")
        sk = next(k for k in key_schema if k["KeyType"] == "RANGE")

        assert pk["AttributeName"] == "creator"
        assert sk["AttributeName"] == "created_at"

    def test_creator_index_projects_all(
        self, dynamodb_resource: DynamoDBServiceResource
    ) -> None:
        table = dynamodb_resource.Table(TEST_TABLE)
        gsis = table.global_secondary_indexes or []
        creator_gsi = next(gsi for gsi in gsis if gsi["IndexName"] == "creator-index")
        assert creator_gsi["Projection"]["ProjectionType"] == "ALL"

    def test_billing_mode_pay_per_request(
        self, dynamodb_resource: DynamoDBServiceResource
    ) -> None:
        dynamodb_resource.Table(TEST_TABLE)
        # moto may not expose billing mode directly; verify via describe
        client = boto3.client("dynamodb", region_name=TEST_REGION)
        desc = client.describe_table(TableName=TEST_TABLE)
        assert desc["Table"]["BillingModeSummary"]["BillingMode"] == "PAY_PER_REQUEST"


# ===================================================================
# DynamoDB CRUD Operations
# ===================================================================
class TestDynamoDBCrudOperations:
    """Verify raw DynamoDB operations work as expected for Tez records."""

    def test_put_and_get_item(self, dynamodb_table: Any) -> None:
        item = {
            "tez_id": "crud-1",
            "creator": CREATOR_EMAIL,
            "name": "CRUD Test",
            "status": "active",
        }
        dynamodb_table.put_item(Item=item)

        response = dynamodb_table.get_item(Key={"tez_id": "crud-1"})
        assert response["Item"]["name"] == "CRUD Test"

    def test_get_missing_item_returns_no_item_key(self, dynamodb_table: Any) -> None:
        response = dynamodb_table.get_item(Key={"tez_id": "missing"})
        assert "Item" not in response

    def test_update_item_modifies_field(self, dynamodb_table: Any) -> None:
        dynamodb_table.put_item(
            Item={
                "tez_id": "crud-2",
                "status": "pending_upload",
                "creator": CREATOR_EMAIL,
            }
        )

        dynamodb_table.update_item(
            Key={"tez_id": "crud-2"},
            UpdateExpression="SET #s = :s",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":s": "active"},
        )

        item = dynamodb_table.get_item(Key={"tez_id": "crud-2"})["Item"]
        assert item["status"] == "active"

    def test_delete_item(self, dynamodb_table: Any) -> None:
        dynamodb_table.put_item(Item={"tez_id": "crud-3", "creator": CREATOR_EMAIL})
        dynamodb_table.delete_item(Key={"tez_id": "crud-3"})

        response = dynamodb_table.get_item(Key={"tez_id": "crud-3"})
        assert "Item" not in response

    def test_query_creator_index(self, dynamodb_table: Any) -> None:
        for i in range(3):
            dynamodb_table.put_item(
                Item={
                    "tez_id": f"gsi-{i}",
                    "creator": CREATOR_EMAIL,
                    "created_at": f"2026-02-{20 + i}T10:00:00Z",
                    "name": f"GSI Test {i}",
                }
            )

        # Also add one from a different creator
        dynamodb_table.put_item(
            Item={
                "tez_id": "gsi-other",
                "creator": "other@ragu.ai",
                "created_at": "2026-02-20T10:00:00Z",
                "name": "Other",
            }
        )

        from boto3.dynamodb.conditions import Key

        response = dynamodb_table.query(
            IndexName="creator-index",
            KeyConditionExpression=Key("creator").eq(CREATOR_EMAIL),
        )

        assert response["Count"] == 3
        for item in response["Items"]:
            assert item["creator"] == CREATOR_EMAIL

    def test_conditional_put_prevents_overwrite(self, dynamodb_table: Any) -> None:
        """Verify conditional put can prevent duplicate Tez IDs."""
        dynamodb_table.put_item(Item={"tez_id": "dup-1", "creator": CREATOR_EMAIL})

        with pytest.raises(ClientError) as exc_info:
            dynamodb_table.put_item(
                Item={"tez_id": "dup-1", "creator": "other@ragu.ai"},
                ConditionExpression="attribute_not_exists(tez_id)",
            )
        assert (
            exc_info.value.response["Error"]["Code"]
            == "ConditionalCheckFailedException"
        )

    def test_update_recipients_list(self, dynamodb_table: Any) -> None:
        dynamodb_table.put_item(
            Item={
                "tez_id": "recip-1",
                "creator": CREATOR_EMAIL,
                "recipients": [],
            }
        )

        dynamodb_table.update_item(
            Key={"tez_id": "recip-1"},
            UpdateExpression="SET recipients = list_append(recipients, :r)",
            ExpressionAttributeValues={":r": [RECIPIENT_EMAIL]},
        )

        item = dynamodb_table.get_item(Key={"tez_id": "recip-1"})["Item"]
        assert RECIPIENT_EMAIL in item["recipients"]

    def test_scan_filter_by_recipients_contains(self, dynamodb_table: Any) -> None:
        """Verify scan can find Tez shared with a specific email."""
        dynamodb_table.put_item(
            Item={
                "tez_id": "scan-1",
                "creator": CREATOR_EMAIL,
                "recipients": [RECIPIENT_EMAIL],
                "created_at": "2026-02-20T10:00:00Z",
            }
        )
        dynamodb_table.put_item(
            Item={
                "tez_id": "scan-2",
                "creator": CREATOR_EMAIL,
                "recipients": [],
                "created_at": "2026-02-20T11:00:00Z",
            }
        )

        from boto3.dynamodb.conditions import Attr

        response = dynamodb_table.scan(
            FilterExpression=Attr("recipients").contains(RECIPIENT_EMAIL)
        )

        assert response["Count"] == 1
        assert response["Items"][0]["tez_id"] == "scan-1"
