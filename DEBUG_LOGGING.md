# S3Lib Debug Logging

## Enabling Debug Logging

Set the `S3LIB_DEBUG` environment variable to enable detailed connection and request logging:

```bash
export S3LIB_DEBUG=1
farmfs push origin snapshot
```

Or inline:

```bash
S3LIB_DEBUG=1 farmfs push origin snapshot
```

For farmfs-specific retry logging, also set `FARMFS_DEBUG=1`.

## Output Format

All debug logs are written to **stderr** (not stdout) to avoid interfering with data output.

### Request Start
```
[2026-01-13 10:15:30] DEBUG: Starting PUT request to /bucket/key, content: file (150000000 bytes), socket: fd=8 local_port=51234 remote=('16.182.97.184', 80)
```

Socket info shows:
- `fd` = File descriptor number (OS-level socket ID)
- `local_port` = Ephemeral port on client (unique per connection)
- `remote` = S3 server IP and port
- First request shows `socket: not connected` (socket created during request)

### Request Timing
```
[2026-01-13 10:15:30] WARNING: conn.request() took 15.23s
[2026-01-13 10:15:46] DEBUG: Request completed in 16.45s (request: 15.23s, getresponse: 1.22s), status: 200, socket: fd=8 local_port=51234
```

- `request:` = Time spent in `HTTPConnection.request()` (includes sending data)
- `getresponse:` = Time waiting for S3 response
- WARNING if `conn.request()` takes > 1 second

### Request Failures
```
[2026-01-13 10:15:46] DEBUG: Request failed with ValueError: S3 request failed with: 400 Bad Request ..., socket: fd=8 local_port=51234
```

Shows exception type and socket identity when requests fail, helping correlate errors with specific connections.

### Retry Attempts (FARMFS_DEBUG)
```
DEBUG: retryFdIo2 attempt 2/3 failed with ValueError: RequestTimeout...
```

### Connection Close
```
[2026-01-13 10:15:46] DEBUG: Closing connection to s3.amazonaws.com - fd=8 local_port=51234
```

## What to Look For

### 1. Connection Reuse
**Good** - Same socket across requests:
```
DEBUG: Starting PUT request ... socket: fd=8 local_port=51234
DEBUG: Starting PUT request ... socket: fd=8 local_port=51234  # Same fd & port
DEBUG: Starting PUT request ... socket: fd=8 local_port=51234  # Same fd & port
```

**Bad** - Socket changes without explicit close:
```
DEBUG: Starting PUT request ... socket: fd=8 local_port=51234
DEBUG: Starting PUT request ... socket: fd=9 local_port=51235  # Different! No close logged
```

### 2. Slow Uploads
If `conn.request()` takes many seconds for large files:
```
WARNING: conn.request() took 15.23s
```
This indicates HTTPConnection is buffering the entire file in memory before sending.

### 3. Request Timeouts
If S3 reports "socket not read from or written to":
- Check if there's a long gap between "Starting request" and "Request completed"
- Check if `conn.request()` is taking abnormally long
- Check if the socket is changing unexpectedly

### 4. Socket Identity Tracking
The combination of `fd` + `local_port` is the kernel-level connection identity:
- If these match across requests, it's the same physical socket
- If these change, the underlying connection changed (even if Python thinks it's reusing)
- `local_port` is especially useful on macOS where socket inodes are always 0

## Example: Successful Upload Session

```bash
$ S3LIB_DEBUG=1 farmfs push origin snapshot 2>&1 | grep DEBUG
[2026-01-13 10:15:30] DEBUG: Starting PUT request to /bucket/file1.dat, content: file (5000000 bytes), socket: not connected
[2026-01-13 10:15:31] DEBUG: Request completed in 0.85s (request: 0.42s, getresponse: 0.43s), status: 200, socket: fd=8 local_port=51234 remote=('16.182.97.184', 80)
[2026-01-13 10:15:31] DEBUG: Starting PUT request to /bucket/file2.dat, content: file (5000000 bytes), socket: fd=8 local_port=51234 remote=('16.182.97.184', 80)
[2026-01-13 10:15:32] DEBUG: Request completed in 0.83s (request: 0.41s, getresponse: 0.42s), status: 200, socket: fd=8 local_port=51234 remote=('16.182.97.184', 80)
[2026-01-13 10:15:32] DEBUG: Starting PUT request to /bucket/file3.dat, content: file (5000000 bytes), socket: fd=8 local_port=51234 remote=('16.182.97.184', 80)
[2026-01-13 10:15:33] DEBUG: Request completed in 0.84s (request: 0.40s, getresponse: 0.44s), status: 200, socket: fd=8 local_port=51234 remote=('16.182.97.184', 80)
[2026-01-13 10:15:33] DEBUG: Closing connection to s3.amazonaws.com - fd=8 local_port=51234
```

Notice:
- First request starts with `socket: not connected`, completes with full socket identity
- Subsequent requests: Same `fd=8 local_port=51234` on both start and completion (good connection reuse)
- Request times are fast and consistent
- Clean close at the end with matching socket identity

## Example: Request Timeout with Retry

```bash
$ S3LIB_DEBUG=1 FARMFS_DEBUG=1 farmfs push origin snapshot 2>&1 | grep -E "(DEBUG|WARNING)"
[10:32:04] DEBUG: Starting PUT request to /blobs/4aa1..., content: file (163734 bytes), socket: not connected
[10:32:25] DEBUG: Request completed in 20.74s (request: 0.10s, getresponse: 20.64s), status: 400, socket: unknown
[10:32:25] DEBUG: Closing connection to s3.amazonaws.com (socket already closed)

DEBUG: retryFdIo2 attempt 1/3 failed with ValueError: S3 request failed with: 400 Bad Request RequestTimeout...

[10:32:25] DEBUG: Starting PUT request to /blobs/4aa1..., content: file (163734 bytes), socket: not connected
[10:32:26] DEBUG: Request completed in 1.07s (request: 0.40s, getresponse: 0.67s), status: 200, socket: fd=4 local_port=49568 remote=('16.182.97.184', 80)
[10:32:26] DEBUG: Closing connection to s3.amazonaws.com - fd=4 local_port=49568
```

Notice:
- First attempt: `socket: unknown` at completion (connection became invalid during request)
- Close log shows "socket already closed"
- Retry attempt succeeds with new connection
- Socket identity tracked throughout retry cycle

## Disabling Debug Logging

```bash
unset S3LIB_DEBUG
```

Or simply don't set it - debug logging is off by default.
