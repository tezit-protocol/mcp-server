import os
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import boto3
from fastmcp import FastMCP

from tez_server.services.email import EmailService
from tez_server.services.metadata import MetadataService
from tez_server.services.storage import StorageService
from tez_server.token_store import TokenStore

S3_BUCKET = os.environ.get("TEZ_S3_BUCKET", "tez-packages")
DYNAMO_TABLE = os.environ.get("TEZ_DYNAMO_TABLE", "tez-metadata")
AWS_REGION = os.environ.get("TEZ_AWS_REGION", "eu-west-2")
AWS_ACCOUNT_ID = os.environ.get("TEZ_AWS_ACCOUNT_ID")

SERVER_URL = os.environ.get("TEZ_SERVER_URL", "")

_INSTRUCTIONS = """\
Tez is a system for creating, sharing, and downloading scoped context packages.
You orchestrate all flows by calling MCP tools and the `tez` CLI together.

## Identity

Run `tez auth whoami` to get the current user's name and email.
If not logged in, run: tez auth login --email <email> --name "<name>"
Use the email as `creator`/`caller` and the name as `creator_name` in all tool calls.

## Build flow (create + upload + confirm)

1. Collect the files the user wants to package. For each file, determine its
   name, size (bytes), and content_type (MIME type).
   The `name` field should be a relative path preserving directory structure
   (e.g. "2026-02-05_Onboarding/context.md"), or a simple basename for flat
   files (e.g. "notes.md"). Use forward slashes as path separators.
2. Call MCP tool `tez_build` with name, description, creator, creator_name,
   and the files list. The response contains: tez_id, upload_token, and server.
3. Run the CLI using the `server` and `upload_token` values from the response:
   tez build <tez_id> \\
     --name "<name>" --desc "<description>" \\
     --server <server> \\
     --token <upload_token> \\
     <file1> <file2> ...
4. Call MCP tool `tez_build_confirm` with the tez_id to activate the Tez.

## Download flow

1. Call MCP tool `tez_download` with tez_id and caller.
   The response contains: download_token and server.
2. Run the CLI using the `server` and `download_token` values from the response:
   tez download <tez_id> \\
     --server <server> \\
     --token <download_token>
   Files are saved to the OS temp directory under tez/<tez_id>/.

## Other operations (MCP only -- no CLI needed)

- tez_share: share a Tez with someone via email
- tez_list: list all Tez the user can access
- tez_info: get metadata about a Tez (no download)
- tez_delete: permanently delete a Tez (creator only)

## CLI-only operations

- tez cache clean <tez_id>: remove locally cached files
- tez auth login / whoami / logout: manage local identity
"""

mcp = FastMCP("tez-server", instructions=_INSTRUCTIONS)
token_store = TokenStore()


@mcp.tool()
def add(a: int, b: int) -> int:
    """Add two numbers together.

    Args:
        a: The first number.
        b: The second number.
    """
    return a + b


@mcp.tool()
def check_s3() -> str:
    """Check connectivity to the S3 bucket used for Tez package storage.

    Returns a status message confirming whether the connection is working.
    """
    if not AWS_ACCOUNT_ID:
        return "TEZ_AWS_ACCOUNT_ID not configured -- cannot verify bucket ownership"
    s3 = boto3.client("s3", region_name=AWS_REGION)
    s3.head_bucket(Bucket=S3_BUCKET, ExpectedBucketOwner=AWS_ACCOUNT_ID)
    return f"Connected to S3 bucket '{S3_BUCKET}' in {AWS_REGION}"


@mcp.tool()
def check_dynamodb() -> str:
    """Check connectivity to the DynamoDB table used for Tez metadata.

    Returns a status message confirming whether the connection is working.
    """
    dynamo = boto3.client("dynamodb", region_name=AWS_REGION)
    resp = dynamo.describe_table(TableName=DYNAMO_TABLE)
    status = resp["Table"]["TableStatus"]
    return (
        f"Connected to DynamoDB table '{DYNAMO_TABLE}' "
        f"in {AWS_REGION} (status: {status})"
    )


@mcp.tool()
def tez_build(
    name: str,
    description: str,
    creator: str,
    creator_name: str,
    files: list[dict[str, Any]],
) -> dict[str, Any]:
    """Create a new Tez and generate pre-signed upload URLs.

    Phase 1 of the upload flow: generates a unique Tez ID, creates a
    pending metadata record in DynamoDB, and returns a single-use upload
    token that can be exchanged for pre-signed PUT URLs via the REST API.

    Args:
        name: Name of the Tez.
        description: Description of the Tez.
        creator: Creator's email address.
        creator_name: Creator's display name.
        files: List of file dicts with "name", "size", and "content_type".
    """
    tez_id = uuid4().hex[:8]
    now = datetime.now(tz=UTC).isoformat()

    s3_client = boto3.client("s3", region_name=AWS_REGION)
    dynamo = boto3.resource("dynamodb", region_name=AWS_REGION)
    table = dynamo.Table(DYNAMO_TABLE)

    storage = StorageService(
        s3_client=s3_client, bucket=S3_BUCKET, account_id=AWS_ACCOUNT_ID
    )
    metadata = MetadataService(table=table)

    expires_in = 900
    upload_urls = storage.generate_upload_urls(
        tez_id=tez_id, files=files, expires_in=expires_in
    )

    record = {
        "tez_id": tez_id,
        "creator": creator,
        "creator_name": creator_name,
        "name": name,
        "description": description,
        "status": "pending_upload",
        "file_count": len(files),
        "total_size": sum(f["size"] for f in files),
        "files": files,
        "recipients": [],
        "created_at": now,
        "updated_at": now,
    }
    metadata.create_tez(record)

    upload_token = token_store.create(
        {"tez_id": tez_id, "upload_urls": upload_urls},
        ttl=expires_in,
    )

    return {
        "tez_id": tez_id,
        "status": "pending_upload",
        "expires_in": expires_in,
        "upload_token": upload_token,
        "server": SERVER_URL,
    }


@mcp.tool()
def tez_build_confirm(tez_id: str) -> dict[str, Any]:
    """Confirm file uploads and finalise a Tez.

    Phase 3 of the upload flow: validates that all expected files have
    been uploaded to S3, writes the manifest files, and updates the
    DynamoDB record status from "pending_upload" to "active".

    Args:
        tez_id: The Tez identifier returned by tez_build.
    """
    s3_client = boto3.client("s3", region_name=AWS_REGION)
    dynamo = boto3.resource("dynamodb", region_name=AWS_REGION)
    table = dynamo.Table(DYNAMO_TABLE)

    storage = StorageService(
        s3_client=s3_client, bucket=S3_BUCKET, account_id=AWS_ACCOUNT_ID
    )
    metadata = MetadataService(table=table)

    record = metadata.get_tez(tez_id)
    if record is None:
        return {"error": f"Tez {tez_id} not found"}

    validation = storage.validate_uploads(tez_id=tez_id, files=record["files"])

    if not validation.success:
        missing_names = [f["name"] for f in validation.missing]
        return {
            "error": "Upload validation failed",
            "missing_files": missing_names,
        }

    metadata.update_status(tez_id=tez_id, status="active")

    return {
        "tez_id": tez_id,
        "status": "active",
        "file_count": len(validation.verified_files),
    }


@mcp.tool()
def tez_download(tez_id: str, caller: str) -> dict[str, Any]:
    """Generate pre-signed download URLs for a Tez.

    Checks authorization (caller must be creator or recipient),
    then returns metadata + a single-use download token that can be
    exchanged for pre-signed GET URLs via the REST API.

    Args:
        tez_id: The Tez identifier.
        caller: Email of the requesting user.
    """
    s3_client = boto3.client("s3", region_name=AWS_REGION)
    dynamo = boto3.resource("dynamodb", region_name=AWS_REGION)
    table = dynamo.Table(DYNAMO_TABLE)

    storage = StorageService(
        s3_client=s3_client, bucket=S3_BUCKET, account_id=AWS_ACCOUNT_ID
    )
    metadata = MetadataService(table=table)

    record = metadata.get_tez(tez_id)
    if record is None:
        return {"error": f"Tez {tez_id} not found"}

    if not metadata.is_authorised(tez_id, caller):
        return {"error": "Access denied"}

    expires_in = 3600
    download_urls = storage.generate_download_urls(
        tez_id=tez_id, files=record["files"], expires_in=expires_in
    )

    download_token = token_store.create(
        {"tez_id": tez_id, "download_urls": download_urls},
        ttl=expires_in,
    )

    return {
        "tez_id": tez_id,
        "name": record["name"],
        "creator": record["creator"],
        "description": record.get("description", ""),
        "status": record["status"],
        "files": record["files"],
        "download_token": download_token,
        "expires_in": expires_in,
        "server": SERVER_URL,
    }


@mcp.tool()
def tez_share(
    tez_id: str,
    recipient_email: str,
    caller: str,
    message: str | None = None,
) -> dict[str, Any]:
    """Share a Tez with someone via email.

    Adds the recipient to the Tez's authorised recipients list in DynamoDB
    and sends a notification email via SendGrid.

    Args:
        tez_id: The Tez identifier.
        recipient_email: Email address of the person to share with.
        caller: Email of the user performing the share (must be the creator).
        message: Optional personal message to include in the email.
    """
    dynamo = boto3.resource("dynamodb", region_name=AWS_REGION)
    table = dynamo.Table(DYNAMO_TABLE)
    metadata = MetadataService(table=table)

    record = metadata.get_tez(tez_id)
    if record is None:
        return {"error": f"Tez {tez_id} not found"}

    if record["creator"] != caller:
        return {"error": "Only the creator can share a Tez"}

    metadata.add_recipient(tez_id=tez_id, email=recipient_email)

    email_status = None
    api_key = os.environ.get("TEZ_SENDGRID_API_KEY")
    if api_key:
        svc = EmailService.from_api_key(api_key)
        email_status = svc.send_share_notification(
            recipient_email=recipient_email,
            sharer_name=record.get("creator_name", caller),
            tez_name=record["name"],
            tez_id=tez_id,
            message=message,
        )

    return {
        "tez_id": tez_id,
        "shared_with": recipient_email,
        "email_sent": email_status == 202,
    }


@mcp.tool()
def tez_list(caller: str) -> dict[str, Any]:
    """List all Tez accessible to a user.

    Returns Tez created by the caller and Tez shared with the caller,
    annotated with who shared what.

    Args:
        caller: Email of the requesting user.
    """
    dynamo = boto3.resource("dynamodb", region_name=AWS_REGION)
    table = dynamo.Table(DYNAMO_TABLE)
    metadata = MetadataService(table=table)

    created = metadata.list_by_creator(caller)
    shared = metadata.list_shared_with(caller)

    created_items = [
        {
            "tez_id": r["tez_id"],
            "name": r["name"],
            "file_count": r["file_count"],
            "shared_by": "me",
            "created_at": r["created_at"],
        }
        for r in created
    ]
    shared_items = [
        {
            "tez_id": r["tez_id"],
            "name": r["name"],
            "file_count": r["file_count"],
            "shared_by": r["creator"],
            "created_at": r["created_at"],
        }
        for r in shared
    ]

    return {"created": created_items, "shared_with_me": shared_items}


@mcp.tool()
def tez_info(tez_id: str, caller: str) -> dict[str, Any]:
    """Get metadata about a Tez without generating download URLs.

    Returns the Tez record metadata -- name, creator, description,
    files, and status. Does not return pre-signed URLs.
    Recipients are only visible to the creator.

    Args:
        tez_id: The Tez identifier.
        caller: Email of the requesting user.
    """
    dynamo = boto3.resource("dynamodb", region_name=AWS_REGION)
    table = dynamo.Table(DYNAMO_TABLE)
    metadata = MetadataService(table=table)

    record = metadata.get_tez(tez_id)
    if record is None:
        return {"error": f"Tez {tez_id} not found"}

    if not metadata.is_authorised(tez_id, caller):
        return {"error": "Access denied"}

    result = {
        "tez_id": record["tez_id"],
        "name": record["name"],
        "creator": record["creator"],
        "description": record.get("description", ""),
        "status": record["status"],
        "file_count": record["file_count"],
        "total_size": record.get("total_size", 0),
        "files": record["files"],
        "created_at": record["created_at"],
        "updated_at": record.get("updated_at", ""),
    }

    if record["creator"] == caller:
        result["recipients"] = record.get("recipients", [])

    return result


@mcp.tool()
def tez_delete(tez_id: str, caller: str) -> dict[str, Any]:
    """Delete a Tez -- removes S3 files and DynamoDB record.

    Only the creator can delete a Tez. Removes all S3 objects
    under the Tez prefix and the DynamoDB metadata record.

    Args:
        tez_id: The Tez identifier.
        caller: Email of the requesting user (must be the creator).
    """
    s3_client = boto3.client("s3", region_name=AWS_REGION)
    dynamo = boto3.resource("dynamodb", region_name=AWS_REGION)
    table = dynamo.Table(DYNAMO_TABLE)

    storage = StorageService(
        s3_client=s3_client, bucket=S3_BUCKET, account_id=AWS_ACCOUNT_ID
    )
    metadata = MetadataService(table=table)

    record = metadata.get_tez(tez_id)
    if record is None:
        return {"error": f"Tez {tez_id} not found"}

    if record["creator"] != caller:
        return {"error": "Only the creator can delete a Tez"}

    storage.delete_tez(tez_id=tez_id)
    metadata.delete_tez(tez_id=tez_id)

    return {"tez_id": tez_id, "status": "deleted"}


app = mcp.http_app()


def main() -> None:
    port = int(os.environ.get("PORT", "8000"))
    mcp.run(transport="http", host="0.0.0.0", port=port)


if __name__ == "__main__":
    main()
