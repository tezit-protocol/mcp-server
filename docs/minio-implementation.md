# MinIO StorageProvider Implementation

## What this task was about

The server previously only supported AWS S3 for file storage, with the storage client created inline in every MCP tool. This task added:

1. A `MinIOStorageProvider` — a full S3-compatible storage adapter for MinIO
2. A `storage_factory` — selects S3 or MinIO at startup based on an env var
3. `server.py` wired to use the factory — one change point instead of four
4. A `docker-compose.yml` — run MinIO locally with one command
5. 14 new unit tests covering all required cases
6. README updated with MinIO setup instructions

---

## Files created or modified

| File | Action | Purpose |
|---|---|---|
| `src/tez_server/services/minio_provider.py` | Created | MinIO implementation of `StorageProvider` |
| `src/tez_server/services/storage_factory.py` | Created | Reads `STORAGE_BACKEND` env var, builds the right provider |
| `src/tez_server/server.py` | Modified | Replaced 4x inline `StorageService(...)` with `get_storage_provider()` |
| `docker-compose.yml` | Created | Local MinIO instance via Docker |
| `tests/test_minio_provider.py` | Created | 14 unit tests using mocked MinIO client |
| `pyproject.toml` | Modified | Added `minio>=7.0` dependency |
| `README.md` | Modified | Added configuration table rows + MinIO setup section |

---

## Architecture after this task

```
server.py
    |
    +-- get_storage_provider()   <-- reads STORAGE_BACKEND env var
            |
            +-- "s3"    --> StorageService (boto3)         [existing]
            +-- "minio" --> MinIOStorageProvider (minio SDK) [new]
            |
            Both implement StorageProvider (ABC)
            Both raise StorageProviderError on failure
```

The MCP tools (`tez_build`, `tez_build_confirm`, `tez_download`, `tez_delete`) call `get_storage_provider()` and work identically regardless of which backend is active.

---

## Detailed implementation decisions

### 1. MinIOStorageProvider (`minio_provider.py`)

**Constructor:**
```python
MinIOStorageProvider(client: Minio, bucket: str)
```
The `Minio` client is injected — not created inside the class. This keeps the class testable (pass a mock in tests) and keeps credential reading out of the provider logic.

**Key SDK differences vs boto3 (S3):**

| Concern | S3 (boto3) | MinIO (minio SDK) |
|---|---|---|
| Presigned upload | `generate_presigned_url("put_object", ExpiresIn=900)` | `presigned_put_object(bucket, key, expires=timedelta(seconds=900))` |
| Presigned download | `generate_presigned_url("get_object", ExpiresIn=900)` | `presigned_get_object(bucket, key, expires=timedelta(seconds=900))` |
| File existence check | `head_object()` → `ClientError(404)` if missing | `stat_object()` → `S3Error("NoSuchKey")` if missing |
| List objects | `list_objects_v2()` → paginated dict | `list_objects(recursive=True)` → generator |
| Delete objects | `delete_objects(Delete={Objects:[...]})` | `remove_objects(bucket, iter([DeleteObject(key), ...]))` → returns error generator |
| Bucket owner guard | `ExpectedBucketOwner` param | Not supported — skipped cleanly |

**Error handling:**
All `S3Error` and network exceptions are caught and re-raised as `StorageProviderError` — same contract as `StorageService`. Two helper methods handle this:
- `_raise_for_s3_error(e)` — maps error codes to descriptive messages
- `_raise_for_network_error(e)` — wraps any other exception

Both are annotated `-> NoReturn` so mypy knows they always raise.

**`delete_tez` — deletion error handling:**
`remove_objects()` in the MinIO SDK is lazy — it returns a generator of errors, not raises. The implementation consumes the generator and raises `StorageProviderError` if any errors are returned:
```python
errors = list(self._client.remove_objects(self.bucket, iter(delete_list)))
if errors:
    raise StorageProviderError(f"MinIO deletion errors for tez '{tez_id}': ...")
```

**`validate_uploads` — no etag:**
Unlike S3's `head_object` which returns an `ETag`, MinIO's `stat_object` returns a `Object` but we do not expose etag in `verified_files` for MinIO (the field is not required by the `ValidationResult` contract). Files are still tracked as present/missing correctly.

---

### 2. StorageFactory (`storage_factory.py`)

Reads `STORAGE_BACKEND` env var at call time (not module load) so it can be controlled per-request in tests.

```python
get_storage_provider() -> StorageProvider
```

**S3 path** — uses existing env vars: `TEZ_S3_BUCKET`, `TEZ_AWS_REGION`, `TEZ_AWS_ACCOUNT_ID`

**MinIO path** — requires: `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `MINIO_BUCKET`. Optional: `MINIO_SECURE` (default `true`).

If any required MinIO env var is missing, raises `StorageProviderError` immediately with a clear message listing exactly which vars are missing — fail fast at startup rather than at first use.

Unknown backend value also raises `StorageProviderError` immediately.

---

### 3. server.py changes

Before (repeated 4 times across `tez_build`, `tez_build_confirm`, `tez_download`, `tez_delete`):
```python
s3_client = boto3.client("s3", region_name=AWS_REGION)
storage = StorageService(s3_client=s3_client, bucket=S3_BUCKET, account_id=AWS_ACCOUNT_ID)
```

After (same 4 places, one line each):
```python
storage = get_storage_provider()
```

The `check_s3` tool was the only remaining user of `S3_BUCKET` and `AWS_ACCOUNT_ID` module-level variables — those were moved to be read locally inside that function.

---

### 4. docker-compose.yml

```yaml
services:
  minio:
    image: minio/minio:latest
    ports:
      - "9000:9000"   # S3-compatible API
      - "9001:9001"   # Web console
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    command: server /data --console-address ":9001"
    volumes:
      - minio-data:/data
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]
      interval: 10s
      timeout: 5s
      retries: 3
      start_period: 5s
```

- Volume `minio-data` persists data across container restarts
- Healthcheck polls the MinIO liveness endpoint — same pattern as the app's `/health`
- Default credentials (`minioadmin`/`minioadmin`) are local-dev only, never used in production

---

## New environment variables

| Variable | Default | When used |
|---|---|---|
| `STORAGE_BACKEND` | `s3` | Always — selects the active storage adapter |
| `MINIO_ENDPOINT` | (required) | MinIO backend only |
| `MINIO_ACCESS_KEY` | (required) | MinIO backend only |
| `MINIO_SECRET_KEY` | (required) | MinIO backend only |
| `MINIO_BUCKET` | (required) | MinIO backend only |
| `MINIO_SECURE` | `true` | MinIO backend only — set `false` for local dev |

---

## Test suite

### Approach
The MinIO tests use `unittest.mock.MagicMock()` to mock the `minio.Minio` client. No real MinIO container or network connection is needed. The mock is injected directly into `MinIOStorageProvider` at construction time — clean, fast, and deterministic.

**Why not moto?** moto mocks the boto3/botocore layer — it has no knowledge of the `minio` SDK. Attempting to use moto with `MinIOStorageProvider` would not intercept any calls.

### Helper used across all tests

```python
def _s3_error(code: str, message: str = "test error") -> S3Error:
    return S3Error(code=code, message=message, resource="/test",
                   request_id="req123", host_id="host123",
                   response=MagicMock(status=400, headers={}, data=b""))
```

Constructs a real `minio.error.S3Error` with a given error code — used to simulate missing files, credential errors, etc.

---

### Test cases — all 14

**`TestGenerateUploadUrls` (4 tests)**

| Test | What it does | How it passes |
|---|---|---|
| `test_returns_url_per_file_plus_manifests` | Calls `generate_upload_urls` with 2 files. Checks returned dict has 4 keys: 2 context files + `manifest.json` + `tez.md` | Mock `presigned_put_object` returns a fixed URL string. Assert `set(urls.keys()) == expected_keys` |
| `test_urls_are_strings` | Same call, checks every value in the dict is a `str` | `assert isinstance(url, str)` for each value |
| `test_context_files_use_context_prefix` | Checks context file URLs contain `tez_id/context/filename` in the path | Mock returns the key as part of the URL. Assert `f"{TEZ_ID}/context/{name}" in url` |
| `test_manifest_files_at_tez_root` | Calls with empty files list. Checks manifest URLs contain `tez_id/manifest.json` and `tez_id/tez.md` | Same mock approach. Assert path in URL |

**`TestGenerateDownloadUrls` (2 tests)**

| Test | What it does | How it passes |
|---|---|---|
| `test_returns_url_per_file_plus_manifests` | Same structure check as upload | Mock `presigned_get_object`. Assert keys match |
| `test_urls_are_strings` | Type check on all values | `isinstance(url, str)` |

**`TestValidateUploadsFound` (1 test)**

| Test | What it does | How it passes |
|---|---|---|
| `test_all_files_present_returns_success` | `stat_object` returns a mock object (file exists) for every call. Checks `ValidationResult.success = True` and `missing = []` | `client.stat_object.return_value = MagicMock()`. Assert `result.success is True` and `result.missing == []` |

**`TestValidateUploadsNotFound` (2 tests)**

| Test | What it does | How it passes |
|---|---|---|
| `test_missing_files_returns_failure` | `stat_object` raises `S3Error("NoSuchKey")` for every call. Checks all 4 files (2 context + 2 manifest) appear in `missing` | `client.stat_object.side_effect = _s3_error("NoSuchKey")`. Assert all names in `missing` |
| `test_partial_missing_reflects_correctly` | `stat_object` succeeds for `transcript.md`, raises `NoSuchKey` for everything else. Checks `transcript.md` is in `verified_files`, `slides.pdf` is in `missing` | `side_effect` function checks the key to decide raise/return |

**`TestDeleteTez` (3 tests)**

| Test | What it does | How it passes |
|---|---|---|
| `test_deletes_all_objects` | `list_objects` returns 2 mock objects. Checks `remove_objects` is called exactly once | `client.list_objects.return_value = iter([obj1, obj2])`. `client.remove_objects.assert_called_once()` |
| `test_no_objects_does_not_call_remove` | `list_objects` returns empty iterator. Checks `remove_objects` is never called | `client.list_objects.return_value = iter([])`. `client.remove_objects.assert_not_called()` |
| `test_deletion_errors_raise_storage_provider_error` | `remove_objects` returns an error in its result iterator. Checks `StorageProviderError` is raised with "deletion errors" in the message | `client.remove_objects.return_value = iter([delete_error])`. `pytest.raises(StorageProviderError, match="deletion errors")` |

**`TestMisconfiguration` (2 tests)**

| Test | What it does | How it passes |
|---|---|---|
| `test_missing_minio_env_vars_raise_storage_provider_error` | Sets `STORAGE_BACKEND=minio` but removes all `MINIO_*` vars from the environment. Calls `get_storage_provider()`. Checks `StorageProviderError` raised with "Missing required env vars" | `patch.dict(os.environ, {"STORAGE_BACKEND": "minio"})` + pop each `MINIO_*` key. `pytest.raises(StorageProviderError, match="Missing required env vars")` |
| `test_unknown_backend_raises_storage_provider_error` | Sets `STORAGE_BACKEND=gcs`. Checks `StorageProviderError` raised with "Unknown STORAGE_BACKEND" | `patch.dict(os.environ, {"STORAGE_BACKEND": "gcs"})`. `pytest.raises(StorageProviderError, match="Unknown STORAGE_BACKEND")` |

---

## Test run result

```
pytest tests/test_minio_provider.py tests/test_storage.py -v

49 passed in 4.15s
```

- 14 new MinIO tests: all pass
- 35 existing S3 tests: all still pass — nothing broken

---

## Acceptance criteria — all met

- [x] `MinIOStorageProvider` implements all 4 methods from `StorageProvider` ABC
- [x] Provider selected via `STORAGE_BACKEND` env var (`s3` or `minio`)
- [x] `docker-compose.yml` starts a local MinIO instance
- [x] 14 unit tests covering upload URL, download URL, file found, file not found, deletion (3 cases), misconfiguration (2 cases)
- [x] README updated with MinIO setup and Docker Compose usage
- [x] No existing tests broken
