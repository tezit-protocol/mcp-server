# Tez MCP Server

Reference MCP server for the [Tezit Protocol](https://github.com/tezit-protocol/spec) -- metadata, access control, and storage orchestration.

## What it does

The MCP server handles all server-side operations for Tez lifecycle management:

- **Build** -- reserve tez IDs, generate upload URLs, validate and activate bundles
- **Share** -- grant access to recipients, send notifications
- **Download** -- authorise access, generate download URLs
- **List / Info / Delete** -- metadata queries and cleanup

The server exposes MCP tools that any compatible LLM can discover and orchestrate. It also provides an HTTP endpoint for token-based URL exchange with the [Tez CLI](https://github.com/tezit-protocol/cli).

## Architecture

```
LLM Orchestrator
    |
    +-- MCP Tools --> MCP Server (this repo)
    |                     +-- Metadata store
    |                     +-- Object storage (pre-signed URLs)
    |                     +-- Access control
    |                     +-- Notifications
    |
    +-- Shell -----> CLI (tezit-protocol/cli)
```

See [Proposal #8](https://github.com/tezit-protocol/spec/issues/8) for the full architecture description.

## Development

```bash
# Install dependencies
uv sync --dev

# Run tests
uv run pytest --cov --cov-report=term-missing

# Lint
uv run ruff check .
uv run ruff format --check .

# Type check
uv run mypy src/
```

## Related

- [Tezit Protocol Spec](https://github.com/tezit-protocol/spec) -- protocol specification
- [Tez CLI](https://github.com/tezit-protocol/cli) -- companion CLI for local file operations
