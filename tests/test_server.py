"""Tests for the MCP server tools and health endpoint."""

from __future__ import annotations

import os
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any
from unittest.mock import patch

import boto3
import pytest
from moto import mock_aws
from mypy_boto3_s3 import S3Client

from tests.conftest import (
    TEST_ACCOUNT_ID,
    TEST_BUCKET,
    TEST_REGION,
    TEST_TABLE,
    create_test_resources,
)


@contextmanager
def _mock_aws_env() -> Generator[tuple[S3Client, Any], None, None]:
    """Set up moto-backed S3 + DynamoDB with server module patches."""
    with mock_aws():
        create_test_resources()
        s3 = boto3.client("s3", region_name=TEST_REGION)
        dynamo = boto3.resource("dynamodb", region_name=TEST_REGION)
        with (
            patch.dict(
                os.environ,
                {
                    "STORAGE_BACKEND": "s3",
                    "TEZ_S3_BUCKET": TEST_BUCKET,
                    "TEZ_AWS_REGION": TEST_REGION,
                    "TEZ_AWS_ACCOUNT_ID": TEST_ACCOUNT_ID,
                },
            ),
            patch("tez_server.server.DYNAMO_TABLE", TEST_TABLE),
            patch("tez_server.server.AWS_REGION", TEST_REGION),
        ):
            yield s3, dynamo


def _upload_bundle(s3: S3Client, tez_id: str, files: list[dict[str, Any]]) -> None:
    """Upload context files + manifest files to S3 (simulates CLI upload)."""
    for f in files:
        s3.put_object(
            Bucket=TEST_BUCKET,
            Key=f"{tez_id}/context/{f['name']}",
            Body=f"# {f['name']}".encode(),
            ContentType=f["content_type"],
            ExpectedBucketOwner=TEST_ACCOUNT_ID,
        )
    s3.put_object(
        Bucket=TEST_BUCKET,
        Key=f"{tez_id}/manifest.json",
        Body=b'{"tezit_version": "1.2"}',
        ContentType="application/json",
        ExpectedBucketOwner=TEST_ACCOUNT_ID,
    )
    s3.put_object(
        Bucket=TEST_BUCKET,
        Key=f"{tez_id}/tez.md",
        Body=b"# Test Tez\n",
        ContentType="text/markdown",
        ExpectedBucketOwner=TEST_ACCOUNT_ID,
    )


class TestAddTool:
    """Tests for the add MCP tool."""

    def test_add_positive_numbers(self) -> None:
        from tez_server.server import add

        assert add(7, 35) == 42

    def test_add_negative_numbers(self) -> None:
        from tez_server.server import add

        assert add(-10, 10) == 0

    def test_add_zeros(self) -> None:
        from tez_server.server import add

        assert add(0, 0) == 0

    def test_add_large_numbers(self) -> None:
        from tez_server.server import add

        assert add(1_000_000, 2_000_000) == 3_000_000


class TestCheckS3Tool:
    """Tests for the check_s3 MCP tool."""

    def test_check_s3_success(self, aws_credentials: None) -> None:
        with mock_aws():
            client = boto3.client("s3", region_name=TEST_REGION)
            client.create_bucket(
                Bucket=TEST_BUCKET,
                CreateBucketConfiguration={"LocationConstraint": TEST_REGION},
            )

            with (
                patch.dict(
                    os.environ,
                    {
                        "TEZ_S3_BUCKET": TEST_BUCKET,
                        "TEZ_AWS_ACCOUNT_ID": TEST_ACCOUNT_ID,
                    },
                ),
                patch("tez_server.server.AWS_REGION", TEST_REGION),
            ):
                from tez_server.server import check_s3

                result = check_s3()
                assert TEST_BUCKET in result
                assert TEST_REGION in result

    def test_check_s3_no_account_id(self, aws_credentials: None) -> None:
        with patch.dict(os.environ, {"TEZ_AWS_ACCOUNT_ID": ""}):
            from tez_server.server import check_s3

            result = check_s3()
            assert "not configured" in result

    def test_check_s3_missing_bucket(self, aws_credentials: None) -> None:
        with mock_aws():
            from botocore.exceptions import ClientError

            with (
                patch.dict(
                    os.environ,
                    {
                        "TEZ_S3_BUCKET": "nonexistent-bucket",
                        "TEZ_AWS_ACCOUNT_ID": TEST_ACCOUNT_ID,
                    },
                ),
                patch("tez_server.server.AWS_REGION", TEST_REGION),
            ):
                from tez_server.server import check_s3

                with pytest.raises(ClientError):
                    check_s3()


class TestCheckDynamoDBTool:
    """Tests for the check_dynamodb MCP tool."""

    def test_check_dynamodb_success(self, aws_credentials: None) -> None:
        with mock_aws():
            resource = boto3.resource("dynamodb", region_name=TEST_REGION)
            resource.create_table(
                TableName=TEST_TABLE,
                KeySchema=[
                    {"AttributeName": "tez_id", "KeyType": "HASH"},
                ],
                AttributeDefinitions=[
                    {"AttributeName": "tez_id", "AttributeType": "S"},
                ],
                BillingMode="PAY_PER_REQUEST",
            )

            with (
                patch("tez_server.server.DYNAMO_TABLE", TEST_TABLE),
                patch("tez_server.server.AWS_REGION", TEST_REGION),
            ):
                from tez_server.server import check_dynamodb

                result = check_dynamodb()
                assert TEST_TABLE in result
                assert TEST_REGION in result
                assert "ACTIVE" in result

    def test_check_dynamodb_missing_table(self, aws_credentials: None) -> None:
        with mock_aws():
            from botocore.exceptions import ClientError

            with (
                patch("tez_server.server.DYNAMO_TABLE", "nonexistent-table"),
                patch("tez_server.server.AWS_REGION", TEST_REGION),
            ):
                from tez_server.server import check_dynamodb

                with pytest.raises(ClientError):
                    check_dynamodb()


class TestTezBuildTool:
    """Tests for the tez_build MCP tool."""

    def test_returns_tez_id_and_urls(self, aws_credentials: None) -> None:
        with _mock_aws_env():
            from tez_server.server import tez_build

            result = tez_build(
                name="Test Tez",
                description="A test",
                creator="adam@ragu.ai",
                creator_name="Adam Cross",
                files=[
                    {
                        "name": "notes.md",
                        "size": 100,
                        "content_type": "text/markdown",
                    }
                ],
            )

            assert "tez_id" in result
            assert result["status"] == "pending_upload"
            assert result["expires_in"] == 900
            assert "upload_token" in result
            assert isinstance(result["upload_token"], str)
            assert len(result["upload_token"]) == 32
            assert "server" in result
            assert isinstance(result["server"], str)

    def test_creates_dynamo_record(self, aws_credentials: None) -> None:
        with _mock_aws_env() as (_s3, dynamo):
            table = dynamo.Table(TEST_TABLE)
            from tez_server.server import tez_build

            result = tez_build(
                name="Test Tez",
                description="A test",
                creator="adam@ragu.ai",
                creator_name="Adam Cross",
                files=[
                    {
                        "name": "notes.md",
                        "size": 100,
                        "content_type": "text/markdown",
                    }
                ],
            )

            record = table.get_item(Key={"tez_id": result["tez_id"]}).get("Item")
            assert record is not None
            assert record["status"] == "pending_upload"
            assert record["name"] == "Test Tez"
            assert record["creator"] == "adam@ragu.ai"


class TestTezBuildConfirmTool:
    """Tests for the tez_build_confirm MCP tool."""

    def test_confirms_successful_upload(self, aws_credentials: None) -> None:
        with _mock_aws_env() as (s3, _dynamo):
            from tez_server.server import tez_build, tez_build_confirm

            # Phase 1: build
            build_result = tez_build(
                name="Confirm Test",
                description="Testing confirm",
                creator="adam@ragu.ai",
                creator_name="Adam Cross",
                files=[
                    {
                        "name": "notes.md",
                        "size": 100,
                        "content_type": "text/markdown",
                    }
                ],
            )
            tez_id = build_result["tez_id"]

            # Phase 2: simulate CLI upload (context + manifest files)
            _upload_bundle(
                s3,
                tez_id,
                [{"name": "notes.md", "content_type": "text/markdown"}],
            )

            # Phase 3: confirm
            confirm_result = tez_build_confirm(tez_id=tez_id)

            assert confirm_result["status"] == "active"
            assert confirm_result["tez_id"] == tez_id
            assert confirm_result["file_count"] == 1

    def test_confirm_missing_files_returns_error(self, aws_credentials: None) -> None:
        with _mock_aws_env():
            from tez_server.server import tez_build, tez_build_confirm

            build_result = tez_build(
                name="Missing Files",
                description="Test",
                creator="adam@ragu.ai",
                creator_name="Adam Cross",
                files=[
                    {
                        "name": "a.md",
                        "size": 50,
                        "content_type": "text/markdown",
                    },
                    {
                        "name": "b.md",
                        "size": 50,
                        "content_type": "text/markdown",
                    },
                ],
            )
            tez_id = build_result["tez_id"]

            # Don't upload any files -- confirm should fail
            result = tez_build_confirm(tez_id=tez_id)

            assert "error" in result
            assert "a.md" in result["missing_files"]
            assert "b.md" in result["missing_files"]
            assert "manifest.json" in result["missing_files"]
            assert "tez.md" in result["missing_files"]

    def test_confirm_nonexistent_tez_returns_error(self, aws_credentials: None) -> None:
        with _mock_aws_env():
            from tez_server.server import tez_build_confirm

            result = tez_build_confirm(tez_id="nonexistent")
            assert "error" in result


def _build_and_confirm(s3: S3Client) -> str:
    """Build a Tez, simulate upload, confirm -- return the tez_id."""
    from tez_server.server import tez_build, tez_build_confirm

    build_result = tez_build(
        name="Download Test",
        description="For download tests",
        creator="adam@ragu.ai",
        creator_name="Adam Cross",
        files=[
            {"name": "notes.md", "size": 100, "content_type": "text/markdown"},
        ],
    )
    tez_id = build_result["tez_id"]
    _upload_bundle(
        s3,
        tez_id,
        [{"name": "notes.md", "content_type": "text/markdown"}],
    )
    tez_build_confirm(tez_id=tez_id)
    return tez_id


class TestTezDownloadTool:
    """Tests for the tez_download MCP tool."""

    def test_returns_download_urls(self, aws_credentials: None) -> None:
        with _mock_aws_env() as (s3, _dynamo):
            tez_id = _build_and_confirm(s3)

            from tez_server.server import tez_download

            result = tez_download(tez_id=tez_id, caller="adam@ragu.ai")

            assert result["tez_id"] == tez_id
            assert result["status"] == "active"
            assert result["name"] == "Download Test"
            assert result["expires_in"] == 3600
            assert "download_token" in result
            assert isinstance(result["download_token"], str)
            assert len(result["download_token"]) == 32
            assert "server" in result
            assert isinstance(result["server"], str)

    def test_tez_not_found(self, aws_credentials: None) -> None:
        with _mock_aws_env():
            from tez_server.server import tez_download

            result = tez_download(tez_id="nonexistent", caller="adam@ragu.ai")
            assert "error" in result
            assert "not found" in result["error"]

    def test_not_authorised(self, aws_credentials: None) -> None:
        with _mock_aws_env() as (s3, _dynamo):
            tez_id = _build_and_confirm(s3)

            from tez_server.server import tez_download

            result = tez_download(tez_id=tez_id, caller="hacker@evil.com")
            assert "error" in result
            assert "Access denied" in result["error"]

    def test_download_token_exchangeable_for_urls(self, aws_credentials: None) -> None:
        with _mock_aws_env() as (s3, _dynamo):
            tez_id = _build_and_confirm(s3)

            from tez_server.server import tez_download, token_store

            result = tez_download(tez_id=tez_id, caller="adam@ragu.ai")

            payload = token_store.exchange(result["download_token"])
            assert payload is not None
            urls = payload["download_urls"]
            assert f"{tez_id}/manifest.json" in urls["manifest.json"]
            assert f"{tez_id}/tez.md" in urls["tez.md"]


class TestTezShareTool:
    """Tests for the tez_share MCP tool."""

    def test_share_adds_recipient(self, aws_credentials: None) -> None:
        with _mock_aws_env() as (s3, dynamo):
            tez_id = _build_and_confirm(s3)

            from tez_server.server import tez_share

            result = tez_share(
                tez_id=tez_id,
                recipient_email="noor@ragu.ai",
                caller="adam@ragu.ai",
            )

            assert result["tez_id"] == tez_id
            assert result["shared_with"] == "noor@ragu.ai"

            # Verify DynamoDB was updated
            table = dynamo.Table(TEST_TABLE)
            record = table.get_item(Key={"tez_id": tez_id})["Item"]
            assert "noor@ragu.ai" in record["recipients"]

    def test_share_not_found(self, aws_credentials: None) -> None:
        with _mock_aws_env():
            from tez_server.server import tez_share

            result = tez_share(
                tez_id="nonexistent",
                recipient_email="noor@ragu.ai",
                caller="adam@ragu.ai",
            )
            assert "error" in result
            assert "not found" in result["error"]

    def test_share_sends_email_when_api_key_set(self, aws_credentials: None) -> None:
        with _mock_aws_env() as (s3, _dynamo):
            tez_id = _build_and_confirm(s3)

            from tez_server.server import tez_share

            with (
                patch.dict("os.environ", {"TEZ_SENDGRID_API_KEY": "fake-key"}),
                patch("tez_server.server.EmailService") as mock_email_cls,
            ):
                mock_svc = mock_email_cls.from_api_key.return_value
                mock_svc.send_share_notification.return_value = 202

                result = tez_share(
                    tez_id=tez_id,
                    recipient_email="noor@ragu.ai",
                    caller="adam@ragu.ai",
                    message="Check this out",
                )

            assert result["email_sent"] is True
            mock_svc.send_share_notification.assert_called_once()

    def test_share_not_creator(self, aws_credentials: None) -> None:
        with _mock_aws_env() as (s3, _dynamo):
            tez_id = _build_and_confirm(s3)

            from tez_server.server import tez_share

            result = tez_share(
                tez_id=tez_id,
                recipient_email="noor@ragu.ai",
                caller="hacker@evil.com",
            )
            assert "error" in result
            assert "Only the creator" in result["error"]


class TestTezListTool:
    """Tests for the tez_list MCP tool."""

    def test_list_created_tez(self, aws_credentials: None) -> None:
        with _mock_aws_env() as (s3, _dynamo):
            _build_and_confirm(s3)

            from tez_server.server import tez_list

            result = tez_list(caller="adam@ragu.ai")

            assert len(result["created"]) == 1
            assert result["created"][0]["shared_by"] == "me"

    def test_list_shared_tez(self, aws_credentials: None) -> None:
        with _mock_aws_env() as (s3, dynamo):
            tez_id = _build_and_confirm(s3)

            # Share with noor
            table = dynamo.Table(TEST_TABLE)
            table.update_item(
                Key={"tez_id": tez_id},
                UpdateExpression="SET recipients = list_append(recipients, :r)",
                ExpressionAttributeValues={":r": ["noor@ragu.ai"]},
            )

            from tez_server.server import tez_list

            result = tez_list(caller="noor@ragu.ai")

            assert len(result["shared_with_me"]) == 1
            assert result["shared_with_me"][0]["shared_by"] == "adam@ragu.ai"

    def test_list_empty(self, aws_credentials: None) -> None:
        with _mock_aws_env():
            from tez_server.server import tez_list

            result = tez_list(caller="nobody@ragu.ai")
            assert result["created"] == []
            assert result["shared_with_me"] == []


class TestTezInfoTool:
    """Tests for the tez_info MCP tool."""

    def test_info_returns_metadata(self, aws_credentials: None) -> None:
        with _mock_aws_env() as (s3, _dynamo):
            tez_id = _build_and_confirm(s3)

            from tez_server.server import tez_info

            result = tez_info(tez_id=tez_id, caller="adam@ragu.ai")

            assert result["tez_id"] == tez_id
            assert result["name"] == "Download Test"
            assert result["creator"] == "adam@ragu.ai"
            assert result["status"] == "active"
            assert "files" in result
            assert "recipients" in result
            assert "download_urls" not in result

    def test_info_hides_recipients_from_non_creator(
        self, aws_credentials: None
    ) -> None:
        with _mock_aws_env() as (s3, dynamo):
            tez_id = _build_and_confirm(s3)

            # Share with noor so she can access info
            table = dynamo.Table(TEST_TABLE)
            table.update_item(
                Key={"tez_id": tez_id},
                UpdateExpression=("SET recipients = list_append(recipients, :r)"),
                ExpressionAttributeValues={":r": ["noor@ragu.ai"]},
            )

            from tez_server.server import tez_info

            result = tez_info(tez_id=tez_id, caller="noor@ragu.ai")

            assert result["tez_id"] == tez_id
            assert "recipients" not in result

    def test_info_not_found(self, aws_credentials: None) -> None:
        with _mock_aws_env():
            from tez_server.server import tez_info

            result = tez_info(tez_id="nonexistent", caller="adam@ragu.ai")
            assert "error" in result
            assert "not found" in result["error"]

    def test_info_access_denied(self, aws_credentials: None) -> None:
        with _mock_aws_env() as (s3, _dynamo):
            tez_id = _build_and_confirm(s3)

            from tez_server.server import tez_info

            result = tez_info(tez_id=tez_id, caller="hacker@evil.com")
            assert "error" in result
            assert "Access denied" in result["error"]


class TestTezDeleteTool:
    """Tests for the tez_delete MCP tool."""

    def test_delete_removes_tez(self, aws_credentials: None) -> None:
        with _mock_aws_env() as (s3, dynamo):
            tez_id = _build_and_confirm(s3)

            from tez_server.server import tez_delete

            result = tez_delete(tez_id=tez_id, caller="adam@ragu.ai")

            assert result["status"] == "deleted"
            assert result["tez_id"] == tez_id

            # Verify DynamoDB record is gone
            table = dynamo.Table(TEST_TABLE)
            record = table.get_item(Key={"tez_id": tez_id}).get("Item")
            assert record is None

    def test_delete_not_found(self, aws_credentials: None) -> None:
        with _mock_aws_env():
            from tez_server.server import tez_delete

            result = tez_delete(tez_id="nonexistent", caller="adam@ragu.ai")
            assert "error" in result
            assert "not found" in result["error"]

    def test_delete_not_creator(self, aws_credentials: None) -> None:
        with _mock_aws_env() as (s3, _dynamo):
            tez_id = _build_and_confirm(s3)

            from tez_server.server import tez_delete

            result = tez_delete(tez_id=tez_id, caller="hacker@evil.com")
            assert "error" in result
            assert "Only the creator" in result["error"]


class TestHealthEndpoint:
    """Tests for the /health HTTP endpoint."""

    def test_health_returns_200(self) -> None:
        from starlette.testclient import TestClient

        from tez_server.server import app

        client = TestClient(app)
        response = client.get("/health")
        assert response.status_code == 200

    def test_health_returns_json(self) -> None:
        from starlette.testclient import TestClient

        from tez_server.server import app

        client = TestClient(app)
        response = client.get("/health")
        data = response.json()
        assert data["status"] == "healthy"
        assert data["service"] == "tez-server"


class TestTokenExchangeEndpoint:
    """Tests for the GET /api/tokens/{token} endpoint."""

    def test_exchange_valid_token(self) -> None:
        from starlette.testclient import TestClient

        from tez_server.server import app, token_store

        token = token_store.create(
            {"upload_urls": {"file.md": "https://s3.example.com"}}
        )

        client = TestClient(app)
        response = client.get(f"/api/tokens/{token}")

        assert response.status_code == 200
        data = response.json()
        assert data["upload_urls"]["file.md"] == "https://s3.example.com"

    def test_exchange_is_single_use(self) -> None:
        from starlette.testclient import TestClient

        from tez_server.server import app, token_store

        token = token_store.create({"key": "value"})

        client = TestClient(app)
        first = client.get(f"/api/tokens/{token}")
        second = client.get(f"/api/tokens/{token}")

        assert first.status_code == 200
        assert second.status_code == 404

    def test_exchange_unknown_token_returns_404(self) -> None:
        from starlette.testclient import TestClient

        from tez_server.server import app

        client = TestClient(app)
        response = client.get("/api/tokens/nonexistent")

        assert response.status_code == 404
        assert "error" in response.json()

    def test_exchange_returns_error_message(self) -> None:
        from starlette.testclient import TestClient

        from tez_server.server import app

        client = TestClient(app)
        response = client.get("/api/tokens/nonexistent")
        data = response.json()
        assert data["error"] == "Token not found or expired"


class TestServerEntryPoint:
    def test_main_starts_server(self) -> None:
        from tez_server.server import main

        with patch("tez_server.server.mcp") as mock_mcp:
            main()
            mock_mcp.run.assert_called_once()

    def test_main_reads_port_from_env(self) -> None:
        from tez_server.server import main

        with (
            patch("tez_server.server.mcp") as mock_mcp,
            patch.dict("os.environ", {"PORT": "9999"}),
        ):
            main()
            call_kwargs = mock_mcp.run.call_args[1]
            assert call_kwargs["port"] == 9999
