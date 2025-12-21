# S3Lib Future Architecture

This document outlines known issues with the current architecture and a roadmap for incremental, backward-compatible improvements.

## Background

The current s3lib architecture was developed as an early Python project and has some fundamental design issues that limit its ability to support modern patterns like connection pooling, retries, and concurrent requests.

## Status: Connection Lifecycle Bugs - FIXED ✓

**Date:** 2025-12-20

All four connection lifecycle bugs have been fixed with backward-compatible changes:

### Implemented Fixes

1. **Bug #1 - Uninitialized `self.conn`:** ✓ FIXED
   - Added `self.conn = None` and `self._outstanding_response = None` to `__init__()`
   - Connection object now safe to create without immediate connection

2. **Bug #2 - Missing Null Check in `_disconnect()`:** ✓ FIXED
   - Restored `if self.conn is not None:` check before closing
   - Added `self.conn = None` reset after closing
   - Safe to call `_disconnect()` multiple times

3. **Bug #3 - Connection Leak in `_connect()`:** ✓ FIXED
   - Restored `if self.conn is None:` check before creating connection
   - Prevents connection leaks from multiple `_connect()` calls

4. **Bug #4 - Fragile Response Object Lifecycle:** ✓ FIXED
   - Added `ConnectionLifecycleError` exception class
   - Implemented response consumption tracking via `self._outstanding_response`
   - Added `_validate_connection_ready()` to check if previous response was consumed
   - Validation called before each request in `_s3_request()`
   - Methods that consume responses internally now clear the tracker
   - **Behavior:** Raises explicit error if caller tries to make a new request before consuming the previous response

### Tests Added

- `test_connection_lifecycle_error()` - Verifies lifecycle error is raised for unconsumed responses
- `test_connection_initialized()` - Verifies proper initialization
- `test_disconnect_safety()` - Verifies safe disconnect even when never connected

### Backward Compatibility

These fixes are **fully backward compatible**:
- Existing code that properly consumes responses continues to work
- Only code with bugs (making requests without consuming responses) will now raise `ConnectionLifecycleError`
- This is a **good thing** - it surfaces bugs that were previously silent connection corruption

### What Callers Must Do

When using `get_object()`, callers **must** fully consume the response before making another request:

```python
# CORRECT - Response is consumed before next operation
with Connection(...) as s3:
    resp = s3.get_object(bucket, key)
    data = resp.read()  # Consume the response
    # Now safe to make another request
    s3.list_buckets()

# INCORRECT - Will raise ConnectionLifecycleError
with Connection(...) as s3:
    resp1 = s3.get_object(bucket, key1)
    resp2 = s3.get_object(bucket, key2)  # ERROR: resp1 not consumed!
    data1 = resp1.read()
    data2 = resp2.read()
```

The error message clearly explains what's wrong:
```
ConnectionLifecycleError: Previous response not fully consumed.
You must read the entire response body before making another request.
Call response.read() to consume the data.
```

## Known Issues (Architectural)

### Historical Context: Connection Lifecycle Bugs (Now Fixed)

**Note:** These bugs have been fixed as of 2025-12-20. This section is kept for historical reference.

These bugs were introduced in the commit range `a03b601..2316a68` when refactoring the get_object connection handling:

#### 1. Uninitialized `self.conn` (Commit 2a8e09a)

**Location:** `s3lib/__init__.py:20-33`

**Problem:** `self.conn` is not initialized in `__init__()`. It only gets created when `_connect()` is called.

**Impact:**
- If `_disconnect()` is called before `_connect()`, raises `AttributeError`
- If `__exit__` is called without `__enter__` (e.g., exception during setup), cleanup fails
- Fragile error handling around context manager usage

**Fix:** Add `self.conn = None` to `__init__()` and add null checks to `_disconnect()`

#### 2. Missing Null Check in `_disconnect()` (Commit a3bcf87)

**Location:** `s3lib/__init__.py:243-244`

**Problem:** Before commit a3bcf87, `_disconnect()` checked `if self.conn:` before closing. Now it unconditionally calls `self.conn.close()`.

**Previous (safe) implementation:**
```python
def _disconnect(self):
    if self.conn:
        self.conn.close()
        self.conn = None
```

**Current (unsafe) implementation:**
```python
def _disconnect(self):
    self.conn.close()
```

**Impact:**
- Crashes if called when `self.conn` is None or doesn't exist
- Doesn't reset `self.conn` to None, making it harder to detect connection state
- Can't safely call `_disconnect()` multiple times

**Fix:** Restore the null check and reset logic

#### 3. Connection Leak in `_connect()` (Commit a3bcf87)

**Location:** `s3lib/__init__.py:240-241`

**Problem:** Before commit a3bcf87, `_connect()` only created a connection if `self.conn is None`. Now it unconditionally creates a new connection.

**Previous (safe) implementation:**
```python
def _connect(self):
    if self.conn is None:
        self.conn = http.client.HTTPConnection(self.host, self.port, timeout=self.conn_timeout)
```

**Current (leaky) implementation:**
```python
def _connect(self):
    self.conn = http.client.HTTPConnection(self.host, self.port, timeout=self.conn_timeout)
```

**Impact:**
- If `_connect()` is called multiple times without `_disconnect()`, creates new connections and leaks the old ones
- Wastes file descriptors and network resources
- Can hit ulimit on file descriptors in long-running processes

**Fix:** Restore the null check before creating a new connection

#### 4. Fragile Response Object Lifecycle (Commit a3bcf87)

**Location:** `s3lib/__init__.py:68-71`, `s3lib/ui.py:107-110`

**Problem:** The `get_object()` method returns a raw `HTTPResponse` object that's tied to `self.conn`:

```python
def get_object(self, bucket, key):
    """ pull down bucket object by key """
    return self._s3_get_request(bucket, key)  # Returns raw HTTPResponse
```

**Impact:**

1. **HTTP/1.1 Keep-Alive constraint violation:**
   - The response body must be fully consumed before making the next request on the same connection
   - If multiple `get_object()` calls are made and responses aren't read in order, the connection becomes corrupted
   - Other methods like `_s3_head_request()` and `_s3_put_request()` call `resp.read()` to reset the connection, but `get_object()` doesn't

2. **Response invalidation on disconnect:**
   - The returned `HTTPResponse` object is only valid while `self.conn` is alive
   - If you exit the `with Connection()` block before reading the response, reads will fail
   - Example fragile code:
     ```python
     with Connection(...) as s3:
         response = s3.get_object(bucket, key)
     # Connection is now closed, but response object still exists
     data = response.read()  # FAILS - connection is closed
     ```

3. **Leaky abstraction:**
   - Callers must understand HTTP connection internals
   - Breaking encapsulation of the Connection class
   - Comment on line 70 acknowledges this: `#TODO Want to replace with some enter, exit struct.`

**Historical context:** The old `get_object_fd()` function (removed in commit a3bcf87) created a **new connection per fetch**, which avoided these issues at the cost of connection overhead:

```python
def get_object_fd(access_id, secret, bucket, key, host=None, port=None, conn_timeout=None):
    """ Fetch the object with a new connection to S3. Returns the raw handle for closing."""
    new_conn = Connection(access_id, secret, host, port, conn_timeout, connect=True)
    fd = new_conn.get_object(bucket, key)
    return fd
```

**Potential fixes (choose based on backward compatibility needs):**

1. **Minimal fix:** Document the constraint and add connection reset logic
2. **Better fix:** Make `get_object()` read the response and return bytes
3. **Best fix:** Separate response handling from connection management (see architecture section)

## Architectural Issues

The fundamental problem is that **user-facing operations are bound to the Connection class**, which ties the lifecycle of TCP connections to business logic.

### Current Architecture

```
Connection class:
├─ Connection management (connect, disconnect, timeout)
├─ AWS request signing (HMAC-SHA1, headers)
├─ HTTP operations (request, getresponse)
└─ Business operations (list_buckets, get_object, put_object, etc.)
```

### Problems with Current Design

#### 1. Tight Coupling Between TCP Connection and Business Logic

- The lifecycle of HTTP connections (which should be pooled/reused) is tied to user operations
- Each `with Connection()` block manages one TCP connection for one logical operation
- Can't implement connection pooling across multiple operations
- Can't have multiple concurrent requests
- Can't implement retry strategies without exposing users to connection management

#### 2. Leaky Abstractions

- `get_object()` returns an `HTTPResponse` object, exposing internal implementation details
- Callers must understand HTTP/1.1 Keep-Alive semantics
- Callers must know when it's safe to read response bodies
- Breaking the abstraction barrier between network layer and application layer

#### 3. No Separation of Concerns

All responsibilities mixed into one class:
- **Connection management:** TCP connections, Keep-Alive, timeouts, socket handling
- **Protocol implementation:** HTTP request/response, headers, status codes
- **AWS authentication:** Request signing, HMAC-SHA1, canonical string construction
- **Business operations:** List buckets, get objects, put objects, delete objects

This violates the Single Responsibility Principle and makes the code hard to:
- Test (can't mock individual layers)
- Extend (adding features requires modifying the monolithic class)
- Debug (network issues mixed with business logic issues)
- Optimize (can't optimize connection handling independently)

#### 4. Context Manager Misuse

- The `with Connection()` pattern suggests resource management
- But it's really managing a logical session, not just a TCP connection
- Creates confusion about what's being managed
- Makes it impossible to implement proper connection pooling (which requires connections to outlive individual operations)

#### 5. Can't Implement Modern Patterns

The current architecture prevents:
- **Connection pooling:** Connections are tied to the context manager lifecycle
- **Retry logic:** No separation between transient network errors and business errors
- **Circuit breakers:** Can't track connection health across operations
- **Request pipelining:** Can only have one request in flight per Connection object
- **Concurrent requests:** Would need multiple Connection objects, each with its own TCP connection
- **Proper timeout handling:** Timeouts are connection-level, not request-level

## Recommended Future Architecture

### Layered Design

Separate the concerns into distinct layers:

```
┌─────────────────────────────────────┐
│  High-Level API (S3)                │  User-facing operations
│  - get_object() -> bytes            │  Returns data, not HTTP objects
│  - list_buckets() -> Iterator[str]  │  Clean, Pythonic interface
│  - put_object(data)                 │  Hides implementation details
└─────────────────────────────────────┘
              ↓ uses
┌─────────────────────────────────────┐
│  Mid-Level Client (S3Client)        │  Protocol + Auth
│  - request(method, bucket, key)     │  Handles AWS signing
│  - Returns Response objects          │  Manages request/response
└─────────────────────────────────────┘
              ↓ uses
┌─────────────────────────────────────┐
│  Low-Level Pool (ConnectionPool)    │  Connection management
│  - get_connection()                 │  Pooling, reuse, health checks
│  - return_connection()              │  Lifecycle management
└─────────────────────────────────────┘
```

### Example Implementation

```python
# Low-level: Connection pool
class ConnectionPool:
    """Manages a pool of HTTP connections to S3."""

    def __init__(self, host, port, timeout=None, pool_size=10):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.pool_size = pool_size
        self._available = queue.Queue(maxsize=pool_size)
        self._in_use = set()

    def get_connection(self) -> http.client.HTTPConnection:
        """Get a connection from the pool."""
        try:
            conn = self._available.get_nowait()
        except queue.Empty:
            conn = http.client.HTTPConnection(self.host, self.port, timeout=self.timeout)
        self._in_use.add(conn)
        return conn

    def return_connection(self, conn):
        """Return a connection to the pool."""
        self._in_use.discard(conn)
        try:
            self._available.put_nowait(conn)
        except queue.Full:
            conn.close()

    def close_all(self):
        """Close all connections in the pool."""
        while not self._available.empty():
            conn = self._available.get_nowait()
            conn.close()
        for conn in self._in_use:
            conn.close()

# Mid-level: HTTP client with signing
class S3Client:
    """Low-level S3 client that handles HTTP and AWS signing."""

    def __init__(self, access_id, secret, pool):
        self.access_id = access_id
        self.secret = secret
        self.pool = pool

    def request(self, method, bucket, key, args=None, headers=None, content=''):
        """Make a signed S3 request and return the response."""
        # Sign the request (reuse existing signing logic)
        signed_headers = self._sign_request(method, bucket, key, args, headers, content)

        # Get a connection from the pool
        conn = self.pool.get_connection()
        try:
            # Make the request
            conn.request(method, self._build_path(bucket, key, args), content, signed_headers)
            resp = conn.getresponse()

            # Read the response body to reset the connection
            data = resp.read()

            # Return connection to pool
            self.pool.return_connection(conn)

            return Response(resp.status, resp.getheaders(), data)
        except Exception as e:
            # Don't return broken connections to the pool
            conn.close()
            raise

    def _sign_request(self, method, bucket, key, args, headers, content):
        """Sign the request using existing signing logic."""
        # Reuse existing sign() and get_string_to_sign() functions
        pass

# High-level: User operations
class S3:
    """High-level S3 API with clean, Pythonic interface."""

    def __init__(self, access_id, secret, host=None, port=None, timeout=None):
        pool = ConnectionPool(
            host or "s3.amazonaws.com",
            port or 80,
            timeout
        )
        self.client = S3Client(access_id, secret, pool)

    def get_object(self, bucket, key) -> bytes:
        """Get an object from S3. Returns the object data as bytes."""
        resp = self.client.request("GET", bucket, key)
        if resp.status != http.client.OK:
            raise S3Error(resp.status, resp.headers, resp.data)
        return resp.data

    def list_buckets(self) -> Iterator[str]:
        """List all buckets in the account."""
        resp = self.client.request("GET", None, None)
        if resp.status != http.client.OK:
            raise S3Error(resp.status, resp.headers, resp.data)
        # Parse XML and yield bucket names
        buckets = _parse_get_service_response(resp.data)
        for bucket in buckets:
            yield bucket

    def put_object(self, bucket, key, data, headers=None):
        """Upload an object to S3."""
        resp = self.client.request("PUT", bucket, key, headers=headers, content=data)
        if resp.status != http.client.OK:
            raise S3Error(resp.status, resp.headers, resp.data)
        return resp.status

    def close(self):
        """Close all connections."""
        self.client.pool.close_all()
```

### Benefits of New Architecture

1. **Connection pooling:** Connections can be reused across multiple operations
2. **Concurrent requests:** Multiple operations can use different connections from the pool
3. **Clean abstractions:** Users work with bytes and strings, not HTTP objects
4. **Retry logic:** Client layer can retry transient failures without exposing connection details
5. **Testability:** Each layer can be tested independently with mocks
6. **Circuit breakers:** Pool can track connection health and implement backoff
7. **Request-level timeouts:** Can implement timeouts per request, not just per connection
8. **Better resource management:** Pool can limit total connections and reuse them efficiently

## Migration Path

To maintain backward compatibility while moving toward the new architecture:

### Phase 1: Fix Immediate Bugs (Backward Compatible) ✓ COMPLETED 2025-12-20

1. ✓ Add `self.conn = None` to `__init__()`
2. ✓ Restore null check in `_disconnect()`
3. ✓ Restore null check in `_connect()`
4. ✓ Add response consumption tracking and validation
   - Implemented `ConnectionLifecycleError` exception
   - Track outstanding responses via `self._outstanding_response`
   - Validate responses are consumed before next request
   - Clear error messages to help developers fix their code

**Impact:** Fixes crashes and leaks, no API changes. Only surfaces existing bugs with clear error messages.

### Phase 2: Separate Connection Pool (New API, Old API Maintained)

1. Create `ConnectionPool` class
2. Create `S3Client` class
3. Keep existing `Connection` class but reimplement it as a wrapper around `S3Client`
4. Add deprecation warnings to `Connection` class

**Impact:** New API available, old API still works

### Phase 3: Migrate CLI Tools

1. Update `s3lib/ui.py` to use new `S3` API
2. Add tests for new API
3. Keep backward compatibility for library users

**Impact:** CLI tools benefit from new architecture, library API unchanged

### Phase 4: Deprecation (Major Version Bump)

1. Mark `Connection` class as deprecated
2. Encourage users to migrate to new `S3` API
3. Provide migration guide

**Impact:** Clear path for users to upgrade

### Phase 5: Remove Old API (Major Version Bump)

1. Remove `Connection` class
2. New architecture is the only API

**Impact:** Clean codebase with modern architecture

## Design Principles for Future Development

1. **Separation of Concerns:** Each class should have one responsibility
2. **Don't Leak Abstractions:** High-level APIs should not expose low-level implementation details
3. **Resource Management:** Use context managers for resources (connections, files), not for logical sessions
4. **Fail-Safe Defaults:** Null checks, defensive programming, graceful degradation
5. **Backward Compatibility:** New features should not break existing code
6. **Test Coverage:** Each layer should be independently testable
7. **Documentation:** Clear contracts about object lifecycles and thread safety

## References

- Problematic commit range: `a03b601..2316a68`
- Key commits:
  - `2316a68` - Added `get_object_url()` (highlighted the architecture issues)
  - `2a8e09a` - Removed `self.conn = None` initialization
  - `48b2cbc` - Reuse connection in `get_object`
  - `a3bcf87` - Reworked to use context manager, removed `get_object_fd()`
  - `2b97770` - Created `get_object_conn` (attempt to fix, later removed)

## Notes

- Current tests pass, but many are skipped (including `test_s3get`)
- The architecture issues don't cause immediate failures but limit scalability
- Incremental migration allows users to upgrade at their own pace
- The new architecture is inspired by modern Python HTTP clients (requests, httpx, boto3)
