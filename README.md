# Tez MCP Server

Reference MCP server for the [Tezit Protocol](https://github.com/tezit-protocol/spec) -- metadata, access control, storage orchestration, and token-based URL exchange.

See [Proposal #8](https://github.com/tezit-protocol/spec/issues/8) for the full architecture rationale.

## What it does

The MCP server is the brain of the Tez system. It handles everything that requires cloud credentials, authorisation decisions, or persistent state -- so the [CLI](https://github.com/tezit-protocol/cli) doesn't have to.

Any MCP-compatible LLM (Claude, GPT, etc.) discovers these tools automatically and orchestrates the full Tez lifecycle.

### MCP Tools

| Tool | Purpose | Flow |
|------|---------|------|
| `tez_build` | Reserve a tez_id, generate upload URLs, return upload token | Build phase 1 |
| `tez_build_confirm` | Validate uploaded files, activate the Tez | Build phase 3 |
| `tez_download` | Authorise access, generate download URLs, return download token | Download phase 1 |
| `tez_share` | Grant access to a recipient, send notification email | Share (MCP only) |
| `tez_list` | List all Tez accessible to a user | List (MCP only) |
| `tez_info` | Get metadata without generating download URLs | Info (MCP only) |
| `tez_delete` | Remove all files from storage and delete metadata | Delete (MCP only) |

### HTTP Endpoints

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/tokens/{token}` | GET | Exchange a single-use token for pre-signed URLs |
| `/health` | GET | Health check |

## Architecture

```
LLM Orchestrator
    |
    +-- MCP Tools --> MCP Server (this repo)
    |                     +-- Metadata store (DynamoDB)
    |                     +-- Object storage (S3 pre-signed URLs)
    |                     +-- Token store (in-memory)
    |                     +-- Access control
    |                     +-- Email notifications (SendGrid)
    |
    +-- Shell -----> CLI (tezit-protocol/cli)
```

The server never touches file bytes. It generates pre-signed URLs so clients upload/download directly to/from object storage.

See [flows.md](flows.md) for detailed sequence diagrams of every operation.

## Operational Flows

### Build (3-phase)

```
Phase 1 -- Reserve (MCP)
  LLM calls tez_build(name, description, creator, files[])
  Server: generates tez_id, creates pending metadata record
  Server: generates pre-signed PUT URLs, stores against token
  Returns: {tez_id, upload_token, server}

Phase 2 -- Upload (CLI)
  CLI exchanges token via GET /api/tokens/{token}
  CLI uploads files directly to storage via pre-signed URLs

Phase 3 -- Confirm (MCP)
  LLM calls tez_build_confirm(tez_id)
  Server: validates all files exist in storage (HeadObject per file)
  Server: updates status from "pending_upload" to "active"
```

### Download (2-phase)

```
Phase 1 -- Authorise (MCP)
  LLM calls tez_download(tez_id, caller)
  Server: verifies caller is creator or recipient
  Server: generates pre-signed GET URLs, stores against token
  Returns: {tez_id, download_token, server}

Phase 2 -- Fetch (CLI)
  CLI exchanges token via GET /api/tokens/{token}
  CLI downloads files directly from storage via pre-signed URLs
```

### Share (MCP only)

```
  LLM calls tez_share(tez_id, recipient_email, caller)
  Server: verifies caller is creator
  Server: adds recipient to access list
  Server: sends notification email (if configured)
```

### List / Info / Delete (MCP only)

```
  tez_list(caller)           -- all Tez created by or shared with caller
  tez_info(tez_id, caller)   -- metadata without download URLs
  tez_delete(tez_id, caller) -- removes storage objects + metadata record
```

## Token Exchange

The server never returns pre-signed URLs directly to the LLM. Instead:

1. Server generates URLs and stores them in an in-memory token store
2. Server returns a short-lived, opaque token to the LLM
3. LLM passes the token to the CLI as a command-line argument
4. CLI exchanges the token for URLs via `GET /api/tokens/{token}`
5. Token is consumed on exchange (single-use)

**Why:** Pre-signed URLs are 500+ characters each, contain storage path information, and waste LLM context tokens. A 32-character hex token is shorter, safer, and cheaper.

## Services

The server is composed of four services behind abstract interfaces:

### StorageService

Generates pre-signed URLs (PUT for upload, GET for download), validates uploads via HeadObject, and handles deletion. All file bytes flow directly between client and storage -- this service never sees them.

- `generate_upload_urls(tez_id, files, expires_in)` -- PUT URLs for context files + manifest
- `generate_download_urls(tez_id, files, expires_in)` -- GET URLs for all files
- `validate_uploads(tez_id, files)` -- HeadObject per file, returns missing list
- `delete_tez(tez_id)` -- ListObjects + DeleteObjects under prefix

### MetadataService

CRUD operations against the metadata store. Source of truth for ownership, access control, and Tez status.

- `create_tez(record)` -- conditional write (prevents ID collision)
- `get_tez(tez_id)` -- single record lookup
- `update_status(tez_id, status)` -- status transitions (pending_upload -> active)
- `add_recipient(tez_id, email)` -- append to recipients list (idempotent)
- `list_by_creator(email)` -- GSI query
- `list_shared_with(email)` -- scan with filter
- `is_authorised(tez_id, email)` -- creator or in recipients[]
- `delete_tez(tez_id)` -- remove record

### TokenStore

In-memory, thread-safe, single-use token store with TTL. Tokens are 32-character hex strings that map to a payload (pre-signed URL dict).

- `create(payload, ttl)` -- store and return token
- `exchange(token)` -- retrieve and delete atomically, returns None if expired/missing
- Auto-purges expired entries on each operation

### EmailService

Sends sharing notification emails via SendGrid. Includes both plain text and branded HTML templates.

- `send_share_notification(recipient, sharer_name, tez_name, tez_id, message)`
- Optional -- server works without email configured

## Metadata Record Schema

```json
{
  "tez_id": "a1b2c3d4",
  "creator": "adam@example.com",
  "creator_name": "Adam Cross",
  "name": "Q4 Board Meeting",
  "description": "Context package for the Q4 board meeting",
  "status": "active",
  "file_count": 3,
  "total_size": 45200,
  "files": [
    {"name": "transcript.md", "size": 12000, "content_type": "text/markdown"},
    {"name": "slides.pdf", "size": 30000, "content_type": "application/pdf"},
    {"name": "actions.md", "size": 3200, "content_type": "text/markdown"}
  ],
  "recipients": ["bob@example.com"],
  "created_at": "2026-03-05T12:00:00+00:00",
  "updated_at": "2026-03-05T12:01:00+00:00"
}
```

## Configuration

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `STORAGE_BACKEND` | `s3` | Storage adapter: `s3` or `minio` |
| `STORAGE_URL_EXPIRY_SECONDS` | `900` | Pre-signed URL expiry in seconds |
| `TEZ_S3_BUCKET` | `tez-packages` | S3 bucket name (S3 backend only) |
| `TEZ_DYNAMO_TABLE` | `tez-metadata` | DynamoDB table name |
| `TEZ_AWS_REGION` | `eu-west-2` | AWS region |
| `TEZ_AWS_ACCOUNT_ID` | (none) | AWS account ID for bucket owner verification (S3 backend only) |
| `TEZ_SERVER_URL` | (none) | Public URL of this server (returned to clients) |
| `TEZ_SENDGRID_API_KEY` | (none) | SendGrid API key for email notifications |
| `PORT` | `8000` | HTTP port |
| `MINIO_ENDPOINT` | (none) | MinIO endpoint e.g. `localhost:9000` (MinIO backend only) |
| `MINIO_ACCESS_KEY` | (none) | MinIO access key (MinIO backend only) |
| `MINIO_SECRET_KEY` | (none) | MinIO secret key (MinIO backend only) |
| `MINIO_BUCKET` | (none) | MinIO bucket name (MinIO backend only) |
| `MINIO_SECURE` | `true` | Use TLS; set `false` for local dev (MinIO backend only) |

## Local Development with MinIO

MinIO is an S3-compatible object store you can run locally without AWS credentials.

### 1. Start MinIO

```bash
docker compose up -d
```

- **API:** `http://localhost:9000`
- **Console:** `http://localhost:9001` — login: `minioadmin` / `minioadmin`

### 2. Create a bucket

Log in to the console at `http://localhost:9001` and create a bucket named `tez-packages`, or use the CLI:

```bash
docker run --rm --network host minio/mc alias set local http://localhost:9000 minioadmin minioadmin
docker run --rm --network host minio/mc mb local/tez-packages
```

### 3. Run the server against MinIO

```bash
export STORAGE_BACKEND=minio
export MINIO_ENDPOINT=localhost:9000
export MINIO_ACCESS_KEY=minioadmin
export MINIO_SECRET_KEY=minioadmin
export MINIO_BUCKET=tez-packages
export MINIO_SECURE=false

uv run python -m tez_server
```

### Switch back to S3

```bash
export STORAGE_BACKEND=s3
```

## Project Structure

```
mcp-server/
  src/
    server.py              -- MCP server + HTTP routes (entry point)
    token_store.py         -- In-memory token store with TTL
    services/
      storage.py           -- S3 pre-signed URL generation + validation
      metadata.py          -- DynamoDB CRUD + access control
      email.py             -- SendGrid email notifications
  tests/
  Dockerfile
  pyproject.toml
```

## Development

```bash
# Install dependencies
uv sync --dev

# Run tests
uv run pytest --cov --cov-report=term-missing

# Lint + format
uv run ruff check .
uv run ruff format --check .

# Type check
uv run mypy src/
```

## Related

- [Tezit Protocol Spec](https://github.com/tezit-protocol/spec) -- protocol specification
- [Tez CLI](https://github.com/tezit-protocol/cli) -- companion CLI for local file operations
- [Proposal #8](https://github.com/tezit-protocol/spec/issues/8) -- architecture proposal
