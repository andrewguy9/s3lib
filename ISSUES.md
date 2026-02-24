# s3lib Logic Issues

A living document tracking known logic problems, misleading code, and bugs in s3lib.

---

## 1. Misleading docstrings on `_s3_request` / `_s3_request_inner`

**File:** `s3lib/__init__.py`

`_s3_request` actually runs two distinct retry loops:

1. **Outer loop** (`for attempt in range(max_redirects)`): retries after 301/307 redirects for region discovery.
2. **Inner loop** (`for retry in range(max_retries)`): retries after connection-level errors (`ConnectionResetError`, `BrokenPipeError`, `ConnectionAbortedError`, `ValueError`).

The docstring on `_s3_request` only mentions redirect handling. The docstring on `_s3_request_inner` says it is "called by `_s3_request` which handles redirect retries" — again only mentioning redirects. Both docstrings are incomplete and mislead the reader about the retry behavior.

---

## 3. Connection error retries are unsafe for conditional PUT operations

**File:** `s3lib/__init__.py`

The inner retry loop in `_s3_request` retries unconditionally on connection errors, regardless of what the caller was trying to accomplish. This is unsafe for conditional PUT operations.

When a connection drops mid-request, the outcome is ambiguous: S3 may have received and processed the request before the connection broke, or it may not have. Retrying blindly can silently violate the caller's intent:

| Operation | `If-` header | Connection error means | Safe to retry? |
|---|---|---|---|
| Unconditional PUT | none | request may not have reached S3 | Yes |
| Create-only PUT | `If-None-Match: *` | object may already have been written | No — retry could overwrite a concurrent write |
| Optimistic PUT | `If-Match: "etag"` | object may have been written after a concurrent change | No — retry could clobber a concurrent update |
| Conditional skip | `If-None-Match: "etag"` | object may have been written | No — retry could overwrite the value we intended to skip |

A 412 response (precondition failed) is not retried because it is a complete HTTP response — S3 understood and answered. But a connection error *before* the response arrives is ambiguous, and the retry logic has no awareness of the conditional headers that were sent. The fix requires threading retry policy (or the presence of conditional headers) down into `_s3_request` so it can decide whether a connection error is safe to retry.

---

## 4. ~~All connections use plain HTTP, not HTTPS~~ ✓ Fixed

HTTPS is now the default (`use_ssl=True`, port 443). HTTP is available via `use_ssl=False` / `--http` CLI flag.

---

## 2. Dead code: `last_error` reset condition is never true

**File:** `s3lib/__init__.py`, lines ~585–587

```python
if last_error and retry < max_retries - 1:
    last_error = None
```

After the inner retry loop exits (via `break` on success or `raise` on exhaustion), `retry` is either:
- `0..max_retries-2` only if the loop `break`ed on success — in which case `last_error` is `None` (no error occurred on that attempt), so the condition is false.
- `max_retries-1` if all retries were exhausted and the exception was re-raised — but then we never reach this line.

The condition `last_error and retry < max_retries - 1` is never true in practice. The `last_error` variable and this reset block are dead code.

---

## 5. Missing type annotations

Several signatures lack type annotations, making the code harder to verify statically:

- **`_s3_request` / `_s3_request_inner`** (`s3lib/__init__.py`): No return type (`-> HTTPResponse`). The `content` parameter accepts `str | bytes | BinaryIO` but is untyped.
- **`put_object`** (`s3lib/__init__.py`): The `data` parameter has a `# TODO` comment acknowledging it lacks an annotation.
- **`sign_request_v4`** (`s3lib/sigv4.py:226`): `secret_key` parameter is untyped; it should be `bytes`.
- **`ConnectionPool.__init__`** (`s3lib/pool.py`): No type annotations on any parameters.

---

## 6. `ValueError` used as a generic S3 error type

**File:** `s3lib/utils.py:92`

`raise_http_resp_error` raises `ValueError` for S3 HTTP failures. There is even a `# TODO` comment in the code acknowledging this is wrong. `ValueError` semantically means a bad argument was passed to a function — not that a network request failed. Two concrete problems follow from this:

1. The inner retry loop in `_s3_request` catches `ValueError` alongside connection errors (`ConnectionResetError`, `BrokenPipeError`, etc.) to trigger retries. This means an S3 error response (e.g. 403 Forbidden, 500 Internal Server Error) that surfaces as a `ValueError` could accidentally trigger a connection retry rather than being propagated immediately.
2. Callers cannot distinguish an S3 error from a programming error without inspecting the exception message string.

The fix is a dedicated exception class (e.g. `S3Error`) that carries the status code and response body as structured attributes.

---

## 7. `_bucket_regions` initialized lazily outside `__init__`

**File:** `s3lib/__init__.py`, lines ~545–546

```python
if not hasattr(self, "_bucket_regions"):
    self._bucket_regions = {}
```

All other instance state is initialized in `__init__`, but `_bucket_regions` is created on first use inside `_s3_request`. This is inconsistent and means the attribute is invisible to static analysis tools and anyone reading `__init__` to understand the object's shape. It should be initialized in `__init__` like all other attributes.

---

## 8. `put_main` `--no-checksum` flag has no effect

**File:** `s3lib/ui.py`, lines ~300–307

```python
if args.get('--no-checksum'):
    checksum_algorithm = None
else:
    checksum_algorithm = None
```

Both branches assign `None` to `checksum_algorithm`. The `--no-checksum` flag is accepted by the CLI but silently does nothing. The `else` branch was presumably meant to set a default algorithm (e.g. `"SHA256"`).

---

## 9. `ConnectionLease._entered` guard is dead code

**File:** `s3lib/pool.py:58`

`ConnectionLease.__exit__` only returns the connection to the pool if `self._entered` is `True`. But `__exit__` can only be called by the `with` statement after `__enter__` has already run and set `_entered = True`. The guard can never be `False` in normal usage and provides no protection.

The more important unguarded question: if the connection has an unconsumed `_outstanding_response` when it is returned, the pool will accept it back and hand it to the next caller, who will get a `ConnectionLifecycleError` on their first request. There is no check for this.

---

## 10. `ConnectionPool._is_connection_valid` does not check for unconsumed responses

**File:** `s3lib/pool.py:265–283`

A connection is considered valid if `conn.conn is not None` and its socket is open. But a connection with `_outstanding_response` set (i.e. a response body not yet fully read) will raise `ConnectionLifecycleError` on the next request. The pool can return such a connection as "valid" and the error surfaces at the caller's next operation rather than at return time, making it hard to diagnose.
