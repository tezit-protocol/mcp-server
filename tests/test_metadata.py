"""Unit tests for tez.services.metadata -- DynamoDB operations.

Tests the MetadataService in isolation using moto-backed DynamoDB.
The developer implementing MetadataService should make these tests pass.

Expected module: tez.services.metadata
Expected class:  MetadataService(table)
"""

from __future__ import annotations

from typing import Any

import pytest

from tests.conftest import CREATOR_EMAIL, RECIPIENT_EMAIL

metadata = pytest.importorskip(
    "tez_server.services.metadata",
    reason="tez_server.services.metadata not yet implemented",
)
MetadataService = metadata.MetadataService


# ===================================================================
# Create Tez record
# ===================================================================
class TestCreateTez:
    """MetadataService.create_tez() writes a new record to DynamoDB."""

    def test_creates_record(
        self,
        dynamodb_table: Any,
        sample_tez_record: dict[str, Any],
    ) -> None:
        svc = MetadataService(table=dynamodb_table)
        svc.create_tez(sample_tez_record)

        item = dynamodb_table.get_item(Key={"tez_id": sample_tez_record["tez_id"]})
        assert "Item" in item
        assert item["Item"]["tez_id"] == sample_tez_record["tez_id"]

    def test_stores_all_fields(
        self,
        dynamodb_table: Any,
        sample_tez_record: dict[str, Any],
    ) -> None:
        svc = MetadataService(table=dynamodb_table)
        svc.create_tez(sample_tez_record)

        item = dynamodb_table.get_item(Key={"tez_id": sample_tez_record["tez_id"]})[
            "Item"
        ]

        assert item["creator"] == CREATOR_EMAIL
        assert item["name"] == "Q1 Standup Notes"
        assert item["status"] == "active"
        assert item["file_count"] == 3
        assert len(item["files"]) == 3

    def test_stores_timestamps(
        self,
        dynamodb_table: Any,
        sample_tez_record: dict[str, Any],
    ) -> None:
        svc = MetadataService(table=dynamodb_table)
        svc.create_tez(sample_tez_record)

        item = dynamodb_table.get_item(Key={"tez_id": sample_tez_record["tez_id"]})[
            "Item"
        ]

        assert "created_at" in item
        assert "updated_at" in item

    def test_initial_recipients_empty(
        self,
        dynamodb_table: Any,
        sample_tez_record: dict[str, Any],
    ) -> None:
        svc = MetadataService(table=dynamodb_table)
        svc.create_tez(sample_tez_record)

        item = dynamodb_table.get_item(Key={"tez_id": sample_tez_record["tez_id"]})[
            "Item"
        ]

        assert item["recipients"] == []

    def test_duplicate_tez_id_raises(
        self,
        dynamodb_table: Any,
        sample_tez_record: dict[str, Any],
    ) -> None:
        svc = MetadataService(table=dynamodb_table)
        svc.create_tez(sample_tez_record)

        with pytest.raises(Exception):  # noqa: B017
            svc.create_tez(sample_tez_record)


# ===================================================================
# Get Tez record
# ===================================================================
class TestGetTez:
    """MetadataService.get_tez() retrieves a record by ID."""

    def test_returns_existing_record(
        self,
        dynamodb_table: Any,
        sample_tez_record: dict[str, Any],
    ) -> None:
        dynamodb_table.put_item(Item=sample_tez_record)

        svc = MetadataService(table=dynamodb_table)
        result = svc.get_tez(tez_id=sample_tez_record["tez_id"])

        assert result is not None
        assert result["tez_id"] == sample_tez_record["tez_id"]
        assert result["name"] == "Q1 Standup Notes"

    def test_returns_none_for_missing(self, dynamodb_table: Any) -> None:
        svc = MetadataService(table=dynamodb_table)
        result = svc.get_tez(tez_id="nonexistent")

        assert result is None

    def test_returns_full_file_list(
        self,
        dynamodb_table: Any,
        sample_tez_record: dict[str, Any],
    ) -> None:
        dynamodb_table.put_item(Item=sample_tez_record)

        svc = MetadataService(table=dynamodb_table)
        result = svc.get_tez(tez_id=sample_tez_record["tez_id"])

        assert result is not None
        assert len(result["files"]) == 3


# ===================================================================
# Update status
# ===================================================================
class TestUpdateStatus:
    """MetadataService.update_status() changes the Tez status field."""

    def test_updates_status_to_active(
        self,
        dynamodb_table: Any,
        sample_tez_record: dict[str, Any],
    ) -> None:
        sample_tez_record["status"] = "pending_upload"
        dynamodb_table.put_item(Item=sample_tez_record)

        svc = MetadataService(table=dynamodb_table)
        svc.update_status(tez_id=sample_tez_record["tez_id"], status="active")

        item = dynamodb_table.get_item(Key={"tez_id": sample_tez_record["tez_id"]})[
            "Item"
        ]
        assert item["status"] == "active"

    def test_updates_updated_at_timestamp(
        self,
        dynamodb_table: Any,
        sample_tez_record: dict[str, Any],
    ) -> None:
        original_updated = sample_tez_record["updated_at"]
        dynamodb_table.put_item(Item=sample_tez_record)

        svc = MetadataService(table=dynamodb_table)
        svc.update_status(tez_id=sample_tez_record["tez_id"], status="active")

        item = dynamodb_table.get_item(Key={"tez_id": sample_tez_record["tez_id"]})[
            "Item"
        ]
        assert item["updated_at"] != original_updated

    def test_update_nonexistent_raises(self, dynamodb_table: Any) -> None:
        svc = MetadataService(table=dynamodb_table)

        with pytest.raises(Exception):  # noqa: B017
            svc.update_status(tez_id="nonexistent", status="active")


# ===================================================================
# Add recipient (share)
# ===================================================================
class TestAddRecipient:
    """MetadataService.add_recipient() appends an email to recipients[]."""

    def test_adds_recipient(
        self,
        dynamodb_table: Any,
        sample_tez_record: dict[str, Any],
    ) -> None:
        dynamodb_table.put_item(Item=sample_tez_record)

        svc = MetadataService(table=dynamodb_table)
        svc.add_recipient(
            tez_id=sample_tez_record["tez_id"],
            email=RECIPIENT_EMAIL,
        )

        item = dynamodb_table.get_item(Key={"tez_id": sample_tez_record["tez_id"]})[
            "Item"
        ]
        assert RECIPIENT_EMAIL in item["recipients"]

    def test_add_multiple_recipients(
        self,
        dynamodb_table: Any,
        sample_tez_record: dict[str, Any],
    ) -> None:
        dynamodb_table.put_item(Item=sample_tez_record)

        svc = MetadataService(table=dynamodb_table)
        svc.add_recipient(tez_id=sample_tez_record["tez_id"], email="noor@ragu.ai")
        svc.add_recipient(
            tez_id=sample_tez_record["tez_id"],
            email="mackenzie@ragu.ai",
        )

        item = dynamodb_table.get_item(Key={"tez_id": sample_tez_record["tez_id"]})[
            "Item"
        ]
        assert len(item["recipients"]) == 2
        assert "noor@ragu.ai" in item["recipients"]
        assert "mackenzie@ragu.ai" in item["recipients"]

    def test_duplicate_recipient_not_added_twice(
        self,
        dynamodb_table: Any,
        sample_tez_record: dict[str, Any],
    ) -> None:
        dynamodb_table.put_item(Item=sample_tez_record)

        svc = MetadataService(table=dynamodb_table)
        svc.add_recipient(tez_id=sample_tez_record["tez_id"], email=RECIPIENT_EMAIL)
        svc.add_recipient(tez_id=sample_tez_record["tez_id"], email=RECIPIENT_EMAIL)

        item = dynamodb_table.get_item(Key={"tez_id": sample_tez_record["tez_id"]})[
            "Item"
        ]
        count = item["recipients"].count(RECIPIENT_EMAIL)
        assert count == 1

    def test_add_recipient_to_nonexistent_raises(self, dynamodb_table: Any) -> None:
        svc = MetadataService(table=dynamodb_table)

        with pytest.raises(Exception):  # noqa: B017
            svc.add_recipient(tez_id="nonexistent", email=RECIPIENT_EMAIL)


# ===================================================================
# List Tez by creator
# ===================================================================
class TestListByCreator:
    """MetadataService.list_by_creator() queries the creator-index GSI."""

    def test_returns_created_tez(
        self,
        dynamodb_table: Any,
        sample_tez_record: dict[str, Any],
    ) -> None:
        dynamodb_table.put_item(Item=sample_tez_record)

        svc = MetadataService(table=dynamodb_table)
        results = svc.list_by_creator(email=CREATOR_EMAIL)

        assert len(results) == 1
        assert results[0]["tez_id"] == sample_tez_record["tez_id"]

    def test_returns_empty_for_unknown_creator(self, dynamodb_table: Any) -> None:
        svc = MetadataService(table=dynamodb_table)
        results = svc.list_by_creator(email="unknown@ragu.ai")

        assert results == []

    def test_returns_multiple_tez_sorted_by_created_at(
        self, dynamodb_table: Any
    ) -> None:
        for i, name in enumerate(["First", "Second", "Third"]):
            dynamodb_table.put_item(
                Item={
                    "tez_id": f"tez-{i}",
                    "creator": CREATOR_EMAIL,
                    "name": name,
                    "description": f"Tez {name}",
                    "status": "active",
                    "file_count": 1,
                    "total_size": 100,
                    "files": [],
                    "recipients": [],
                    "created_at": f"2026-02-{18 + i}T10:00:00Z",
                    "updated_at": f"2026-02-{18 + i}T10:00:00Z",
                }
            )

        svc = MetadataService(table=dynamodb_table)
        results = svc.list_by_creator(email=CREATOR_EMAIL)

        assert len(results) == 3

    def test_does_not_return_other_creators_tez(
        self,
        dynamodb_table: Any,
        sample_tez_record: dict[str, Any],
    ) -> None:
        dynamodb_table.put_item(Item=sample_tez_record)
        dynamodb_table.put_item(
            Item={
                **sample_tez_record,
                "tez_id": "other-tez",
                "creator": "other@ragu.ai",
            }
        )

        svc = MetadataService(table=dynamodb_table)
        results = svc.list_by_creator(email=CREATOR_EMAIL)

        assert len(results) == 1
        assert results[0]["creator"] == CREATOR_EMAIL


# ===================================================================
# List Tez shared with user
# ===================================================================
class TestListSharedWith:
    """MetadataService.list_shared_with() finds Tez where email is in recipients."""

    def test_returns_shared_tez(
        self,
        dynamodb_table: Any,
        sample_tez_record: dict[str, Any],
    ) -> None:
        sample_tez_record["recipients"] = [RECIPIENT_EMAIL]
        dynamodb_table.put_item(Item=sample_tez_record)

        svc = MetadataService(table=dynamodb_table)
        results = svc.list_shared_with(email=RECIPIENT_EMAIL)

        assert len(results) == 1
        assert results[0]["tez_id"] == sample_tez_record["tez_id"]

    def test_returns_empty_when_not_shared(
        self,
        dynamodb_table: Any,
        sample_tez_record: dict[str, Any],
    ) -> None:
        dynamodb_table.put_item(Item=sample_tez_record)

        svc = MetadataService(table=dynamodb_table)
        results = svc.list_shared_with(email=RECIPIENT_EMAIL)

        assert results == []

    def test_returns_multiple_shared_tez(self, dynamodb_table: Any) -> None:
        for i in range(3):
            dynamodb_table.put_item(
                Item={
                    "tez_id": f"shared-{i}",
                    "creator": CREATOR_EMAIL,
                    "name": f"Shared Tez {i}",
                    "description": "",
                    "status": "active",
                    "file_count": 1,
                    "total_size": 100,
                    "files": [],
                    "recipients": [RECIPIENT_EMAIL],
                    "created_at": f"2026-02-{18 + i}T10:00:00Z",
                    "updated_at": f"2026-02-{18 + i}T10:00:00Z",
                }
            )

        svc = MetadataService(table=dynamodb_table)
        results = svc.list_shared_with(email=RECIPIENT_EMAIL)

        assert len(results) == 3


# ===================================================================
# Authorisation check
# ===================================================================
class TestIsAuthorised:
    """MetadataService.is_authorised() checks if a user can access a Tez."""

    def test_creator_is_authorised(
        self,
        dynamodb_table: Any,
        sample_tez_record: dict[str, Any],
    ) -> None:
        dynamodb_table.put_item(Item=sample_tez_record)

        svc = MetadataService(table=dynamodb_table)
        assert svc.is_authorised(
            tez_id=sample_tez_record["tez_id"],
            email=CREATOR_EMAIL,
        )

    def test_recipient_is_authorised(
        self,
        dynamodb_table: Any,
        sample_tez_record: dict[str, Any],
    ) -> None:
        sample_tez_record["recipients"] = [RECIPIENT_EMAIL]
        dynamodb_table.put_item(Item=sample_tez_record)

        svc = MetadataService(table=dynamodb_table)
        assert svc.is_authorised(
            tez_id=sample_tez_record["tez_id"],
            email=RECIPIENT_EMAIL,
        )

    def test_random_user_not_authorised(
        self,
        dynamodb_table: Any,
        sample_tez_record: dict[str, Any],
    ) -> None:
        dynamodb_table.put_item(Item=sample_tez_record)

        svc = MetadataService(table=dynamodb_table)
        assert not svc.is_authorised(
            tez_id=sample_tez_record["tez_id"],
            email="random@example.com",
        )

    def test_nonexistent_tez_not_authorised(self, dynamodb_table: Any) -> None:
        svc = MetadataService(table=dynamodb_table)
        assert not svc.is_authorised(tez_id="nonexistent", email=CREATOR_EMAIL)


# ===================================================================
# Delete Tez record
# ===================================================================
class TestDeleteTezRecord:
    """MetadataService.delete_tez() removes the DynamoDB record."""

    def test_deletes_record(
        self,
        dynamodb_table: Any,
        sample_tez_record: dict[str, Any],
    ) -> None:
        dynamodb_table.put_item(Item=sample_tez_record)

        svc = MetadataService(table=dynamodb_table)
        svc.delete_tez(tez_id=sample_tez_record["tez_id"])

        item = dynamodb_table.get_item(Key={"tez_id": sample_tez_record["tez_id"]})
        assert "Item" not in item

    def test_delete_nonexistent_does_not_raise(self, dynamodb_table: Any) -> None:
        svc = MetadataService(table=dynamodb_table)
        # Should not raise
        svc.delete_tez(tez_id="nonexistent")
