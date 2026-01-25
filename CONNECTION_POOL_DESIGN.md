# S3Lib Connection Pool Design

## Overview

This document describes the design of the `ConnectionPool` and `ConnectionLease` classes for s3lib. These classes enable efficient connection reuse for S3 operations while maintaining thread-safety and proper resource management.

## Motivation

### Problems with Current Design

The existing `Connection` class uses a context manager pattern that creates and destroys connections for each operation:

```python
# Current approach - inefficient for multiple operations
for item in items:
    with Connection(access_id, secret) as conn:
        conn.put_object(bucket, key, data)
    # Connection closed and destroyed
```

**Issues:**
1. **High overhead**: Each operation creates a new TCP connection (handshake, TLS negotiation)
2. **Resource waste**: Connections are destroyed immediately after use
3. **NAT table pressure**: Many short-lived connections exhaust home router NAT tables
4. **Poor performance**: Connection setup dominates operation time for small files

### Goals

1. **Connection reuse**: Share connections across multiple operations
2. **Thread-safety**: Safe for concurrent access from multiple threads
3. **Resource management**: Automatic cleanup, no leaks
4. **Performance**: Minimize connection overhead through pooling
5. **MRU strategy**: Maximize "hot" connection usage for better cache locality
6. **Backward compatibility**: Existing low-level API still works

## Architecture

### Component Overview

```
┌─────────────────────────────────────────────────────────────┐
│ ConnectionPool (Thread-Safe Resource Manager)               │
│                                                              │
│  ┌────────────────────────────────────────────────────┐    │
│  │ Available Connections (MRU Stack)                   │    │
│  │ [Connection_3] ← Most recently used (hot)          │    │
│  │ [Connection_2]                                      │    │
│  │ [Connection_1] ← Least recently used (cold)        │    │
│  └────────────────────────────────────────────────────┘    │
│                                                              │
│  ┌────────────────────────────────────────────────────┐    │
│  │ In-Use Connections                                  │    │
│  │ {Connection_4, Connection_5} ← Currently leased    │    │
│  └────────────────────────────────────────────────────┘    │
│                                                              │
│  Thread Synchronization: RLock + Condition Variable         │
└─────────────────────────────────────────────────────────────┘
                            │
                            │ lease()
                            ↓
                ┌─────────────────────────┐
                │ ConnectionLease         │
                │ (Context Manager)       │
                │                         │
                │  wraps → Connection     │
                └─────────────────────────┘
                            │
                            │ __exit__
                            ↓
                    Returns to Pool
```

### Design Principles

1. **Separation of Concerns**
   - `ConnectionPool`: Resource management, thread-safety, lifecycle
   - `ConnectionLease`: Scoped access, automatic return
   - `Connection`: Low-level HTTP/S3 operations

2. **MRU (Most Recently Used) Strategy**
   - Available connections stored in a stack (LIFO)
   - Most recently returned connections reused first
   - Benefits: Hot connections, warm TCP state, TLS session resumption

3. **Thread-Safety**
   - All pool operations protected by reentrant lock
   - Condition variable for efficient waiting
   - Atomic operations for state transitions

4. **Resource Lifecycle**
   - Pool manages connection creation and destruction
   - Leases manage scoped access and automatic return
   - Context managers ensure cleanup even on exceptions

## Class Specifications

### ConnectionPool

**Responsibilities:**
- Maintain pool of reusable connections
- Create connections on demand up to max limit
- Track available vs in-use connections
- Provide thread-safe lease mechanism
- Implement MRU allocation strategy
- Handle connection validation and cleanup
- Manage pool lifecycle (open/closed states)

**Key Attributes:**

```python
class ConnectionPool:
    # Configuration
    access_id: str              # AWS access key ID
    secret: bytes               # AWS secret key
    host: str                   # S3 hostname
    port: int                   # S3 port
    max_connections: int        # Max total connections
    conn_timeout: int           # Connection timeout (seconds)
    wait_timeout: int           # Max wait for available connection

    # Internal state (protected by _lock)
    _available: deque           # Available connections (MRU stack)
    _in_use: set                # Connections currently leased
    _all_connections: set       # All connections ever created
    _closed: bool               # Pool closed flag

    # Thread synchronization
    _lock: threading.RLock      # Reentrant lock
    _condition: threading.Condition  # For wait/notify
```

**Key Methods:**

```python
def __init__(self, access_id, secret, host=None, port=None,
             max_connections=10, conn_timeout=60, wait_timeout=30):
    """Initialize connection pool with configuration."""

def lease(self) -> ConnectionLease:
    """
    Lease a connection from the pool.
    Thread-safe. Blocks if pool exhausted, up to wait_timeout.

    Returns:
        ConnectionLease: Context manager wrapping a connection

    Raises:
        RuntimeError: If pool is closed
        TimeoutError: If no connection available within wait_timeout
    """

def close(self):
    """
    Close all connections and shut down pool.
    Thread-safe. Idempotent (safe to call multiple times).
    """

@property
def closed(self) -> bool:
    """Check if pool is closed (thread-safe)."""

def stats(self) -> dict:
    """
    Get pool statistics (thread-safe).

    Returns:
        dict with keys:
            - total_connections: Total connections created
            - available: Connections available for lease
            - in_use: Connections currently leased
            - max_connections: Configured maximum
            - closed: Pool closed state
    """

def __enter__(self):
    """Context manager entry: return self."""

def __exit__(self, exc_type, exc_val, exc_tb):
    """Context manager exit: close pool."""
```

**Internal Methods (Protected by Lock):**

```python
def _get_available_connection(self) -> Connection or None:
    """
    Get connection from available pool (MRU).
    Validates connection before returning.
    Returns None if connection is invalid.
    """

def _create_new_connection(self) -> Connection:
    """
    Create and connect a new connection.
    Adds to tracking sets.
    """

def _is_connection_valid(self, conn) -> bool:
    """
    Check if connection is still valid.
    Checks for open socket.
    """

def _return_connection(self, conn):
    """
    Return connection to pool (called by ConnectionLease).
    Validates before returning to available pool.
    Discards invalid connections.
    Notifies waiting threads.
    """
```

### ConnectionLease

**Responsibilities:**
- Provide scoped access to a connection
- Automatically return connection to pool on exit
- Prevent manual connection closure by user
- Handle exceptions gracefully
- Ensure connection is always returned

**Key Attributes:**

```python
class ConnectionLease:
    _connection: Connection     # Wrapped connection
    _pool: ConnectionPool       # Pool to return to
    _entered: bool              # Track if __enter__ was called
```

**Key Methods:**

```python
def __init__(self, connection, pool):
    """
    Create a connection lease.

    Args:
        connection: The Connection object to wrap
        pool: The ConnectionPool to return to
    """

def __enter__(self) -> Connection:
    """
    Enter context: return the wrapped connection.

    Returns:
        Connection: The leased connection for use
    """

def __exit__(self, exc_type, exc_val, exc_tb):
    """
    Exit context: return connection to pool.
    Always returns connection, even on exception.

    Returns:
        False: Don't suppress exceptions
    """
```

**Usage Pattern:**

```python
# Lease wraps connection and ensures return
with pool.lease() as conn:
    conn.put_object(bucket, key, data)
# Connection automatically returned to pool
```

## Thread-Safety Design

### Synchronization Primitives

**RLock (Reentrant Lock):**
- Allows same thread to acquire lock multiple times
- Protects all shared state modifications
- Released only when outermost acquisition releases

**Condition Variable:**
- Built on top of RLock
- Enables efficient waiting for available connections
- Threads sleep instead of spinning (CPU-efficient)
- `wait()`: Release lock, sleep until notified, reacquire lock
- `notify()`: Wake one waiting thread
- `notify_all()`: Wake all waiting threads (used in close)

### Critical Sections

All operations that access shared state are protected:

```python
# Example: Leasing a connection
def lease(self):
    with self._lock:  # Acquire lock for entire operation
        # Check state, get/create connection, track usage
        # All atomic under lock protection
        ...
        return ConnectionLease(conn, self)
```

**Protected Operations:**
1. Checking pool state (`_closed`, connection counts)
2. Getting connection from `_available`
3. Creating new connection and adding to `_all_connections`
4. Returning connection to `_available`
5. Closing pool and clearing all collections

### Wait/Notify Pattern

When pool is exhausted (all connections in use), threads wait efficiently:

```python
# Thread A: Needs connection but pool is full
with self._lock:
    while not (available or can_create):
        # Wait releases lock, sleeps, reacquires on wake
        self._condition.wait(timeout=remaining)
    # Got notified or timed out, check again

# Thread B: Returns connection
with self._lock:
    self._available.append(conn)
    self._condition.notify()  # Wake one waiting thread
```

### Race Conditions Prevented

| Race Condition | Prevention Mechanism |
|----------------|---------------------|
| Two threads get same connection | Lock protects `_available.pop()` |
| Exceed max_connections | Lock protects atomic check-and-create |
| Use after pool closed | Lock protects `_closed` check |
| Return to closed pool | Lock protects return logic, discards if closed |
| Concurrent close + lease | Lock ensures atomic state transitions |
| Lost notifications | Condition variable ensures reliable wake-up |

### Deadlock Prevention

1. **Lock timeout in wait**: `wait(timeout)` prevents indefinite blocking
2. **No nested pool operations**: Lease doesn't acquire pool lock while user has it
3. **Lock ordering**: Only one lock (pool lock), no ordering issues
4. **Exception safety**: Lock released via context manager even on exceptions

## MRU (Most Recently Used) Strategy

### Data Structure

Available connections stored in a **deque** used as a **stack (LIFO)**:

```python
_available = deque()

# Return connection (push to right)
_available.append(conn)  # O(1)

# Get connection (pop from right)
conn = _available.pop()  # O(1)
```

### Why MRU?

**Hot Connections Get Reused:**
- Recently used connections likely have:
  - Warm TCP congestion control state
  - Cached DNS entries
  - TLS session resumption
  - Kernel socket buffers allocated
  - Better chance of S3 keep-alive still active

**Natural Aging:**
- Connections at bottom of stack age out
- Unused connections naturally become stale
- Can implement periodic cleanup of old connections

**Performance:**
- O(1) push and pop operations
- Simple implementation (deque)
- Cache-friendly (reuses same connections)

### Comparison with Other Strategies

| Strategy | Pro | Con |
|----------|-----|-----|
| **MRU (LIFO)** | Hot connections, simple | Older connections may never be used |
| FIFO | Even distribution | All connections get cold |
| Round-robin | Balanced usage | More complex, all connections get cold |
| Random | Statistically even | No locality, unpredictable |

**Chosen:** MRU (LIFO) for performance and simplicity.

## Connection Validation

### When to Validate

1. **Before returning from pool**: Check if connection is still alive
2. **Before returning to pool**: Check if connection is still valid
3. **On lease**: Validate before giving to user

### Validation Logic

```python
def _is_connection_valid(self, conn) -> bool:
    """
    Check if connection is still valid.

    A connection is valid if:
    - Connection object exists
    - Has an HTTPConnection object (conn.conn)
    - HTTPConnection has a socket (conn.conn.sock)
    - Socket is not None
    """
    return (conn.conn is not None and
            hasattr(conn.conn, 'sock') and
            conn.conn.sock is not None)
```

### Invalid Connection Handling

```python
# Getting connection from pool
conn = self._available.pop()
if not self._is_connection_valid(conn):
    # Discard invalid connection
    self._all_connections.discard(conn)
    try:
        conn._disconnect()
    except Exception:
        pass  # Best effort
    return None  # Caller will try another or create new

# Returning connection to pool
if self._is_connection_valid(conn):
    self._available.append(conn)  # Return to pool
else:
    self._all_connections.discard(conn)  # Discard
    try:
        conn._disconnect()
    except Exception:
        pass  # Best effort
```

## Lifecycle Management

### Pool States

```
CREATED → ACTIVE → CLOSED
   │         │         │
   │         │         └─> No operations allowed
   │         │
   │         └─> Connections being leased/returned
   │
   └─> Pool instantiated, no connections yet
```

**State Transitions:**

```python
# CREATED → ACTIVE
def lease(self):
    # First lease() call transitions to ACTIVE
    if self._available or len(self._all_connections) < self.max_connections:
        return ConnectionLease(conn, self)

# ACTIVE → CLOSED
def close(self):
    self._closed = True  # Atomic state change
    # Clean up all connections
    # Notify all waiting threads
```

### Pool Lifecycle Patterns

**Pattern 1: Function-Scoped (Context Manager)**

```python
# Pool lifetime = function lifetime
def process_batch(items):
    with ConnectionPool(access_id, secret) as pool:
        for item in items:
            with pool.lease() as conn:
                conn.put_object(bucket, item.key, item.data)
    # Pool automatically closed
```

**Pattern 2: Application-Scoped (Manual)**

```python
# Pool lifetime = application lifetime
pool = ConnectionPool(access_id, secret, max_connections=20)
atexit.register(pool.close)

try:
    # Application runs
    while running:
        with pool.lease() as conn:
            ...
finally:
    pool.close()
```

**Pattern 3: Module-Scoped (Singleton)**

```python
# Pool lifetime = process lifetime
_pool = None

def get_pool():
    global _pool
    if _pool is None:
        _pool = ConnectionPool(...)
        atexit.register(_pool.close)
    return _pool
```

### Closed Pool Behavior

```python
# Attempting to use closed pool
pool = ConnectionPool(...)
pool.close()

# Lease from closed pool → RuntimeError
try:
    with pool.lease() as conn:
        conn.put_object(...)
except RuntimeError as e:
    print(e)  # "Cannot lease from closed ConnectionPool"

# Close idempotent (safe to call multiple times)
pool.close()  # Does nothing
pool.close()  # Still safe
```

### ConnectionLease Lifecycle

```python
# Lease lifecycle tied to 'with' block
with pool.lease() as conn:  # __enter__: return connection
    conn.put_object(...)     # Use connection
    # If exception, still continues to __exit__
# __exit__: return connection to pool (always)
```

## Usage Examples

### Example 1: Simple Batch Upload

```python
from s3lib import ConnectionPool

# Create pool
pool = ConnectionPool(
    access_id="AKIAIOSFODNN7EXAMPLE",
    secret=b"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    max_connections=5
)

# Upload files
files = ['report.pdf', 'data.csv', 'image.jpg']
for filename in files:
    with pool.lease() as conn:
        with open(filename, 'rb') as f:
            conn.put_object('my-bucket', filename, f)
        print(f"Uploaded {filename}")

# Cleanup
pool.close()
```

**What happens:**
- First iteration: Creates connection, uploads, returns to pool
- Second iteration: Reuses same connection (MRU), uploads, returns
- Third iteration: Reuses same connection again
- Result: 3 uploads, 1 TCP connection

### Example 2: Concurrent Downloads

```python
from s3lib import ConnectionPool
from concurrent.futures import ThreadPoolExecutor

pool = ConnectionPool(
    access_id="...",
    secret=b"...",
    max_connections=5
)

def download_file(key):
    # Thread-safe lease from pool
    with pool.lease() as conn:
        resp = conn.get_object('my-bucket', key)
        data = resp.read()
        with open(key, 'wb') as f:
            f.write(data)
    return key

# 100 files, 10 threads, 5 max connections
files = [f'file_{i}.dat' for i in range(100)]

with ThreadPoolExecutor(max_workers=10) as executor:
    results = list(executor.map(download_file, files))

print(f"Downloaded {len(results)} files")
pool.close()
```

**What happens:**
- 10 threads start, but only 5 connections created (max_connections=5)
- First 5 threads get connections immediately
- Next 5 threads wait for connections to be returned
- As connections are returned, waiting threads are notified
- 100 downloads completed with 5 reused connections

### Example 3: Context Manager Pattern

```python
from s3lib import ConnectionPool

# Pool automatically cleaned up
with ConnectionPool(access_id="...", secret=b"...") as pool:
    # Upload
    with pool.lease() as conn:
        conn.put_object('bucket', 'data.txt', b'content')

    # Download
    with pool.lease() as conn:
        resp = conn.get_object('bucket', 'data.txt')
        data = resp.read()

    # Verify
    assert data == b'content'
# Pool.close() called automatically
```

### Example 4: Error Handling

```python
from s3lib import ConnectionPool

pool = ConnectionPool(access_id="...", secret=b"...", max_connections=5)

def upload_with_retry(bucket, key, data, max_retries=3):
    for attempt in range(max_retries):
        try:
            with pool.lease() as conn:
                status, headers = conn.put_object(bucket, key, data)
                return status, headers
        except (ConnectionResetError, ConnectionAbortedError) as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            if attempt == max_retries - 1:
                raise
            # Pool will provide fresh connection on retry

try:
    upload_with_retry('my-bucket', 'important.dat', b'critical data')
    print("Upload succeeded")
except Exception as e:
    print(f"Upload failed: {e}")
finally:
    pool.close()
```

### Example 5: Monitoring Pool

```python
from s3lib import ConnectionPool
import threading
import time

pool = ConnectionPool(access_id="...", secret=b"...", max_connections=10)

def monitor():
    while not pool.closed:
        stats = pool.stats()
        print(f"Pool: {stats['in_use']}/{stats['total_connections']} in use, "
              f"{stats['available']} available")
        time.sleep(1)

# Start monitor
monitor_thread = threading.Thread(target=monitor, daemon=True)
monitor_thread.start()

# Do work
for i in range(20):
    with pool.lease() as conn:
        conn.put_object('bucket', f'file_{i}', b'data')
        time.sleep(0.5)

pool.close()
```

## API Reference

### ConnectionPool

#### Constructor

```python
ConnectionPool(
    access_id: str,
    secret: bytes,
    host: str = None,
    port: int = None,
    max_connections: int = 10,
    conn_timeout: int = 60,
    wait_timeout: int = 30
)
```

**Parameters:**
- `access_id`: AWS access key ID
- `secret`: AWS secret access key (must be bytes)
- `host`: S3 hostname (default: "s3.amazonaws.com")
- `port`: S3 port (default: 80)
- `max_connections`: Maximum total connections (available + in-use)
- `conn_timeout`: Timeout for individual connections in seconds
- `wait_timeout`: Maximum time to wait for available connection in seconds

#### Methods

**`lease() -> ConnectionLease`**

Lease a connection from the pool. Thread-safe. Blocks if pool is exhausted.

**Raises:**
- `RuntimeError`: If pool is closed
- `TimeoutError`: If no connection available within `wait_timeout`

**`close()`**

Close all connections and shut down pool. Thread-safe and idempotent.

**`stats() -> dict`**

Get pool statistics. Thread-safe.

**Returns:** Dictionary with keys:
- `total_connections`: Total connections created
- `available`: Connections available for lease
- `in_use`: Connections currently leased
- `max_connections`: Configured maximum
- `closed`: Pool closed state (bool)

**`closed -> bool` (property)**

Check if pool is closed. Thread-safe.

**`__enter__() -> ConnectionPool`**

Context manager entry. Returns self.

**`__exit__(exc_type, exc_val, exc_tb)`**

Context manager exit. Calls `close()`.

### ConnectionLease

#### Constructor

```python
ConnectionLease(connection: Connection, pool: ConnectionPool)
```

**Parameters:**
- `connection`: The Connection object to wrap
- `pool`: The ConnectionPool to return to

**Note:** Users don't construct ConnectionLease directly. Use `pool.lease()`.

#### Methods

**`__enter__() -> Connection`**

Context manager entry. Returns the wrapped connection.

**`__exit__(exc_type, exc_val, exc_tb)`**

Context manager exit. Returns connection to pool. Always returns False (doesn't suppress exceptions).

## Implementation Notes

### Dependencies

```python
import threading      # RLock, Condition
from collections import deque  # MRU stack
import time          # Timeout calculations
```

### Performance Characteristics

| Operation | Time Complexity | Notes |
|-----------|----------------|-------|
| `lease()` (available) | O(1) | Pop from deque |
| `lease()` (create) | O(1) | Add to set |
| `lease()` (wait) | O(timeout) | Sleep until notified |
| `_return_connection()` | O(1) | Append to deque |
| `close()` | O(n) | n = total connections |
| `stats()` | O(1) | Just read sizes |

### Memory Usage

```
Per Pool:
- Deque: ~200 bytes base + 8 bytes per available connection
- Sets: ~200 bytes base + 8 bytes per connection
- Lock/Condition: ~100 bytes
- Total: ~500 bytes + 24 bytes per connection

Per ConnectionLease:
- ~100 bytes (2 references + bookkeeping)

Per Connection:
- Existing Connection object (~1KB)
- HTTPConnection object (~2KB)
- Total: ~3KB per connection
```

For a pool with `max_connections=10`:
- Pool overhead: ~500 + 240 = ~740 bytes
- Connections: 10 × 3KB = 30KB
- **Total: ~31KB**

### Error Handling

**Pool Level:**
- `TimeoutError`: No connection available within `wait_timeout`
- `RuntimeError`: Attempting to use closed pool

**Connection Level:**
- `ConnectionResetError`: S3 closed connection
- `ConnectionAbortedError`: Connection aborted
- All handled by returning invalid connection to pool (discarded)

**Lease Level:**
- Always returns connection to pool, even on exception
- Exceptions propagate to caller after return

### Logging (Future Enhancement)

```python
import logging

logger = logging.getLogger('s3lib.pool')

# In lease()
logger.debug(f"Leasing connection (available={len(self._available)}, "
             f"in_use={len(self._in_use)})")

# In _return_connection()
logger.debug(f"Returning connection to pool (valid={valid})")

# In close()
logger.info(f"Closing pool (total_connections={len(self._all_connections)})")
```

## Testing Considerations

### Unit Tests

1. **Basic operations**: lease, return, close
2. **Thread-safety**: Concurrent lease/return
3. **Pool exhaustion**: Wait/timeout behavior
4. **Connection validation**: Invalid connections discarded
5. **MRU strategy**: Most recent connection reused
6. **Idempotency**: Multiple close() calls safe
7. **State transitions**: CREATED → ACTIVE → CLOSED

### Integration Tests

1. **Real S3 operations**: Upload/download through pool
2. **Concurrent operations**: Multiple threads sharing pool
3. **Error recovery**: ConnectionResetError handling
4. **Long-running**: Pool operates for extended time
5. **Resource cleanup**: No leaks after close

### Performance Tests

1. **Connection reuse**: Verify same connection reused
2. **Overhead**: Measure lease/return time
3. **Concurrency**: Throughput with multiple threads
4. **Pool sizing**: Optimal max_connections for workload

## Future Enhancements

### Potential Improvements

1. **Connection TTL**: Expire connections after time limit
2. **Health checks**: Periodic validation of idle connections
3. **Metrics**: Detailed statistics (lease time, wait time, etc.)
4. **Adaptive sizing**: Dynamically adjust pool size
5. **Connection affinity**: Pin threads to connections
6. **Graceful shutdown**: Drain in-use connections before close
7. **Circuit breaker**: Disable pool temporarily on errors
8. **Configuration**: Load pool config from file/environment

### Backward Compatibility

All enhancements must maintain:
- Existing `Connection` class works unchanged
- ConnectionPool is opt-in (not required)
- Low-level API available for advanced users

## Conclusion

The ConnectionPool and ConnectionLease design provides:

✓ **Efficient connection reuse** via MRU pooling
✓ **Thread-safety** for concurrent operations
✓ **Resource management** with automatic cleanup
✓ **Simple API** with context manager pattern
✓ **Performance** through connection reuse
✓ **Robustness** with validation and error handling

This design enables high-throughput S3 operations while maintaining safety and simplicity.
