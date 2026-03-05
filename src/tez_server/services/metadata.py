"""DynamoDB metadata operations for Tez packages.

Handles ownership, sharing, status tracking, and queries against the
tez-metadata table. Uses a GSI (creator-index) for efficient list queries.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from boto3.dynamodb.conditions import Attr, Key


class MetadataService:
    """DynamoDB operations for Tez metadata.

    Args:
        table: A boto3 DynamoDB Table resource (or moto mock).

    Table schema:
        PK: tez_id (String)
        GSI creator-index: PK=creator (String), SK=created_at (String)
    """

    def __init__(self, table: Any) -> None:
        self.table = table

    def create_tez(self, record: dict[str, Any]) -> None:
        """Write a new Tez record to DynamoDB.

        Args:
            record: Complete Tez metadata dict.

        Raises:
            ClientError: If tez_id already exists (ConditionalCheckFailed).
        """
        self.table.put_item(
            Item=record,
            ConditionExpression="attribute_not_exists(tez_id)",
        )

    def get_tez(self, tez_id: str) -> dict[str, Any] | None:
        """Retrieve a Tez record by ID.

        Args:
            tez_id: The Tez identifier.

        Returns:
            The full record dict, or None if not found.
        """
        response = self.table.get_item(Key={"tez_id": tez_id})
        item: dict[str, Any] | None = response.get("Item")
        return item

    def update_status(self, tez_id: str, status: str) -> None:
        """Update the status field of a Tez.

        Args:
            tez_id: The Tez identifier.
            status: New status value (e.g. "active", "pending_upload").
        """
        self.table.update_item(
            Key={"tez_id": tez_id},
            UpdateExpression="SET #s = :s, updated_at = :u",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":s": status,
                ":u": datetime.now(tz=UTC).isoformat(),
            },
            ConditionExpression="attribute_exists(tez_id)",
        )

    def add_recipient(self, tez_id: str, email: str) -> None:
        """Add a recipient email to the Tez's recipients list.

        Idempotent -- no-op if email is already in recipients[].

        Args:
            tez_id: The Tez identifier.
            email: Recipient email address to add.
        """
        record = self.get_tez(tez_id)
        if record is None:
            msg = f"Tez {tez_id} not found"
            raise ValueError(msg)
        if email in record.get("recipients", []):
            return
        self.table.update_item(
            Key={"tez_id": tez_id},
            UpdateExpression="SET recipients = list_append(recipients, :r)",
            ExpressionAttributeValues={":r": [email]},
            ConditionExpression="attribute_exists(tez_id)",
        )

    def list_by_creator(self, email: str) -> list[dict[str, Any]]:
        """List all Tez created by a specific user.

        Args:
            email: Creator's email address.

        Returns:
            List of Tez records (may be empty).
        """
        response = self.table.query(
            IndexName="creator-index",
            KeyConditionExpression=Key("creator").eq(email),
        )
        items: list[dict[str, Any]] = response.get("Items", [])
        return items

    def list_shared_with(self, email: str) -> list[dict[str, Any]]:
        """List all Tez shared with a specific user.

        Args:
            email: Recipient's email address.

        Returns:
            List of Tez records (may be empty).
        """
        response = self.table.scan(
            FilterExpression=Attr("recipients").contains(email),
        )
        items: list[dict[str, Any]] = response.get("Items", [])
        return items

    def is_authorised(self, tez_id: str, email: str) -> bool:
        """Check if a user is authorised to access a Tez.

        A user is authorised if they are the creator or in recipients[].
        Returns False for nonexistent Tez IDs.

        Args:
            tez_id: The Tez identifier.
            email: User's email address.
        """
        record = self.get_tez(tez_id)
        if record is None:
            return False
        if record.get("creator") == email:
            return True
        return email in record.get("recipients", [])

    def delete_tez(self, tez_id: str) -> None:
        """Delete a Tez record from DynamoDB.

        Args:
            tez_id: The Tez identifier.
        """
        self.table.delete_item(Key={"tez_id": tez_id})
