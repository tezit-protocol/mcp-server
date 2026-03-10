# S3 StorageProvider Production Hardening

## What this task was about

The existing `StorageService` worked but was not production-ready:
- Any S3 error (bad credentials, missing bucket, network timeout) would crash the app with a raw boto3 `ClientError`
- URL expiry was hardcoded — no way to change it without editing source code
- No shared interface existed for future storage adapters (e.g. MinIO)
- Methods had minimal docstrings — not usable as a contributor reference

This task fixed all of that.

---

## Files changed

| File | What changed |
|---|---|
| `src/tez_server/services/storage.py` | Added `StorageProviderError`, `StorageProvider` ABC, error handling, env var, full docstrings |
| `tests/test_storage.py` | Added 9 new error case tests, fixed 1 existing test |

---

## Implementation decisions

### 1. Custom exception — `StorageProviderError`
A single custom exception class that wraps all provider-specific errors. Callers only need to catch this one type — they never need to import boto3/botocore exception types.

All raises use `raise StorageProviderError(...) from e` so the original error is preserved in the chain for debugging, but never leaks to the caller.

### 2. Abstract base class — `StorageProvider`
Created an ABC with 4 abstract methods matching the existing `StorageService` API. Full docstrings on every method define: parameters and types, return shape, and which exceptions may be raised. This is now the reference spec for contributors building new adapters (MinIO next).

### 3. Error handling — 4 categories
Each public method and `_head_object` wraps S3 calls in try/except:

| Error type | Botocore class | Message pattern |
|---|---|---|
| Invalid/expired credentials | `ClientError (InvalidClientTokenId)` | "Invalid or expired AWS credentials" |
| Bucket not found | `ClientError (NoSuchBucket)` | "S3 bucket '...' does not exist" |
| Permission denied | `ClientError (AccessDenied / 403)` | "Permission denied on bucket '...'" |
| Network timeout | `ConnectTimeoutError / ReadTimeoutError / EndpointResolutionError` | "Network error reaching S3 bucket '...'" |
| Any other S3 error | `ClientError (any other code)` | "S3 error (CODE): message" |

Two private helpers (`_raise_for_client_error`, `_raise_for_network_error`) annotated `-> NoReturn` handle the conversion — no duplicated try/except logic across methods.

### 4. Configurable URL expiry — `STORAGE_URL_EXPIRY_SECONDS`
Read once at module load into `DEFAULT_URL_EXPIRY` (default: 900). Both `generate_upload_urls` and `generate_download_urls` use it as their `expires_in` default. Previously upload was hardcoded to 900 and download to 3600 — both now use the same env var.

### 5. Assumption made (no manager input needed)
One `STORAGE_URL_EXPIRY_SECONDS` env var applied to both upload and download, defaulting to 900 seconds. The old download default of 3600 was dropped in favour of a single configurable value.

---

## Tests run

**Command:**
```bash
uv run pytest tests/test_storage.py -v
```

**Result: 35/35 passed**

### Existing tests (26) — all still pass
These were not modified (except `test_head_object_reraises_non_404` which was updated to expect `StorageProviderError` instead of raw `ClientError` — the old assertion tested the wrong behaviour).

### New tests added (9) in `TestStorageServiceErrors`

| Test | What it verifies |
|---|---|
| `test_invalid_credentials_raises_storage_provider_error` | `InvalidClientTokenId` → `StorageProviderError` with "Invalid or expired" message |
| `test_no_such_bucket_raises_storage_provider_error` | `NoSuchBucket` → `StorageProviderError` with "does not exist" message |
| `test_access_denied_raises_storage_provider_error` | `AccessDenied` → `StorageProviderError` with "Permission denied" message |
| `test_connect_timeout_raises_storage_provider_error` | `ConnectTimeoutError` → `StorageProviderError` with "Network error" message |
| `test_read_timeout_raises_storage_provider_error` | `ReadTimeoutError` → `StorageProviderError` with "Network error" message |
| `test_head_object_non_404_wrapped_as_storage_provider_error` | Non-404 errors in `_head_object` are wrapped, not leaked |
| `test_no_raw_boto3_exceptions_leak` | Unknown `ClientError` codes still produce `StorageProviderError` |
| `test_custom_expiry_reflected_in_upload_url` | `expires_in=300` produces URLs expiring ~300s from now (parsed from timestamp) |
| `test_custom_expiry_reflected_in_download_url` | `expires_in=1200` produces URLs expiring ~1200s from now (parsed from timestamp) |

### Note on expiry test approach
Moto generates presigned URLs with absolute Unix timestamps (`?Expires=1773062008`), not relative seconds. Asserting `"300" in url` fails. The tests instead parse the URL, extract the `Expires` timestamp, and assert it falls within the expected range of `now + expires_in ± 2 seconds`.

---

## Acceptance criteria — all met

- [x] All error cases handled and wrapped in `StorageProviderError`
- [x] Pre-signed URL expiry driven by `STORAGE_URL_EXPIRY_SECONDS`
- [x] Interface (`StorageProvider`) and implementation (`StorageService`) fully docstringed
- [x] Tests cover happy path (existing) and all error cases (new)
- [x] No raw boto3 exceptions surface above the storage layer
