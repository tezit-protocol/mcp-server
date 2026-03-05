import os

import boto3
from fastmcp import FastMCP

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


app = mcp.http_app()


def main() -> None:
    port = int(os.environ.get("PORT", "8000"))
    mcp.run(transport="http", host="0.0.0.0", port=port)


if __name__ == "__main__":
    main()
