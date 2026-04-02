# S3Lib

Python library and collection of command line programs for interfacing with AWS S3.
Uses buffering and fixed memory usage, where possible, so that operations on large buckets and objects are safe and easy.

## Features

- Memory-efficient streaming for large objects
- Batch operations for large buckets
- Support for custom S3-compatible endpoints
- Simple credential management
- Both library and CLI interfaces

## Installation

```bash
pip install s3lib
```

## Configuration

S3Lib supports multiple authentication methods (in order of precedence):

1. **Command-line argument**: Use `--creds <path>` to specify a credentials file
2. **Environment variables**: Set `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
3. **Credentials file**: Create `~/.s3` with your credentials (default)

### Credentials File Format

Create a file at `~/.s3` (or any path you specify) with:

```
<AWS_ACCESS_KEY_ID>
<AWS_SECRET_ACCESS_KEY>
```

Example:

```
AKIAIOSFODNN7EXAMPLE
wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```

## Command Line Utilities

### s3ls - List buckets or objects

List all buckets:

```bash
s3ls
```

List objects in a bucket:

```bash
s3ls mybucket
```

List with prefix filter:

```bash
s3ls mybucket --prefix logs/2024/
```

List with custom fields:

```bash
s3ls mybucket --fields Key Size LastModified
```

Available fields: Key, LastModified, ETag, Size, StorageClass

Options:

- `--host HOST` - Custom S3 endpoint hostname
- `--port PORT` - Custom port
- `--output FILE` - Write output to file
- `--creds FILE` - Path to credentials file
- `--mark MARKER` - Start listing from this key
- `--prefix PREFIX` - Filter by prefix
- `--batch SIZE` - Batch size for API calls (default: 1000)

### s3get - Download objects

Download an object:

```bash
s3get mybucket myfile.txt --output local-file.txt
```

Download to stdout:

```bash
s3get mybucket logs/app.log | grep ERROR
```

Download multiple objects:

```bash
s3get mybucket file1.txt file2.txt --output combined.txt
```

Options:

- `--host HOST` - Custom S3 endpoint hostname
- `--port PORT` - Custom port
- `--output FILE` - Write output to file (default: stdout)
- `--creds FILE` - Path to credentials file
- `--range START-END` - Fetch only a byte range (e.g. `0-499`, `500-`, `-999`)

### s3put - Upload objects

Upload a file:

```bash
s3put mybucket remote-file.txt local-file.txt
```

Upload from stdin:

```bash
echo "Hello World" | s3put mybucket hello.txt
```

Upload with custom headers:

```bash
s3put mybucket file.txt local.txt --header "Content-Type:text/plain" --header "Cache-Control:max-age=3600"
```

Options:

- `--host HOST` - Custom S3 endpoint hostname
- `--port PORT` - Custom port
- `--creds FILE` - Path to credentials file
- `--header KEY:VALUE` - Add custom HTTP headers (repeatable)

### s3head - Get object metadata

Get metadata for objects:

```bash
s3head mybucket file1.txt file2.txt
```

Get metadata in JSON format:

```bash
s3head mybucket file.txt --json
```

Options:

- `--host HOST` - Custom S3 endpoint hostname
- `--port PORT` - Custom port
- `--creds FILE` - Path to credentials file
- `--json` - Output in JSON format

### s3cp - Copy objects

Copy object within or between buckets:

```bash
s3cp source-bucket source-key dest-bucket dest-key
```

Copy with custom metadata:

```bash
s3cp mybucket old.txt mybucket new.txt --header "Content-Type:application/json"
```

Options:

- `--host HOST` - Custom S3 endpoint hostname
- `--port PORT` - Custom port
- `--creds FILE` - Path to credentials file
- `--header KEY:VALUE` - Set metadata headers (repeatable)

### s3rm - Delete objects

Delete objects:

```bash
s3rm mybucket file1.txt file2.txt
```

Delete with verbose output:

```bash
s3rm mybucket file.txt --verbose
```

Batch delete with custom batch size:

```bash
s3rm mybucket file*.txt --batch 100
```

Options:

- `--host HOST` - Custom S3 endpoint hostname
- `--port PORT` - Custom port
- `--creds FILE` - Path to credentials file
- `-v, --verbose` - Show files as they are deleted
- `--batch SIZE` - Batch size for delete operations (default: 500)

### s3sign - Sign S3 forms

Sign a policy document for browser-based uploads:

```bash
s3sign policy.json
```

This outputs the base64-encoded policy and signature.

Options:

- `--creds FILE` - Path to credentials file

## Python Library API

### Connection Lifecycle

`Connection` must be used as a context manager. Calling methods outside of a `with` block raises `ConnectionLifecycleError`. The connection is established lazily on first use and closed when the `with` block exits.

```python
from s3lib import Connection

access_id = "AKIAIOSFODNN7EXAMPLE"
secret = b"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

with Connection(access_id, secret) as s3:
    for bucket in s3.list_buckets():
        print(bucket)
```

**Unconsumed responses**: If `get_object2` returns a stream and the `Connection` context exits before that stream is consumed, `ConnectionLifecycleError` is raised. Always close the stream before letting the connection exit:

```python
with Connection(access_id, secret) as s3:
    stream, headers = s3.get_object2("mybucket", "file.txt")
    with stream:               # stream must be closed inside the connection block
        data = stream.read()
```

### Downloading Objects

#### get_object2 (recommended)

`get_object2` returns `(S3ByteStream, headers)` on success, or `(None, headers)` when a conditional request produces no body (304 Not Modified or 412 Precondition Failed).

```python
with Connection(access_id, secret) as s3:
    # Simple download
    stream, headers = s3.get_object2("mybucket", "file.txt")
    with stream:
        data = stream.read()

    # Conditional download — skip if unchanged (caching)
    stream, headers = s3.get_object2("mybucket", "file.txt", if_none_match=cached_etag)
    if stream is None:
        pass  # 304 Not Modified — use cached copy
    else:
        with stream:
            data = stream.read()

    # Conditional download — only if ETag still matches
    stream, headers = s3.get_object2("mybucket", "file.txt", if_match=expected_etag)
    if stream is None:
        pass  # 412 Precondition Failed — object has changed
    else:
        with stream:
            data = stream.read()
```

#### get_object (low-level)

`get_object` returns the raw `HTTPResponse`. Conditional responses (304, 412) are returned as status codes — no exception is raised.

```python
with Connection(access_id, secret) as s3:
    # Conditional download — check status to detect unchanged object
    response = s3.get_object("mybucket", "file.txt", if_none_match=cached_etag)
    if response.status == 304:
        pass  # Not Modified — use cached copy
    else:
        data = response.read()

    # Conditional download — check status to detect changed object
    response = s3.get_object("mybucket", "file.txt", if_match=expected_etag)
    if response.status == 412:
        pass  # Precondition Failed — object has changed
    else:
        data = response.read()
```

### S3ByteStream

`get_object2` returns an `S3ByteStream` context manager. It must always be used with `with`:

- **Full consumption**: when `.read()` returns `b""` (EOF), the underlying HTTP connection is kept alive and returned to a healthy state for reuse.
- **Early exit**: when the `with` block exits before the stream is exhausted, the underlying socket is closed.

```python
# Incremental read — stream a large object to disk
with Connection(access_id, secret) as s3:
    stream, headers = s3.get_object2("mybucket", "largefile.bin")
    with stream, open("local-large.bin", "wb") as f:
        while chunk := stream.read(65536):
            f.write(chunk)
```

### Byte Range Fetching

Request only a portion of an object using `byte_range=(start, end)`. Both positions are inclusive, 0-based byte offsets. Either can be `None`:

```python
with Connection(access_id, secret) as s3:
    # First 500 bytes
    stream, headers = s3.get_object2("mybucket", "file.bin", byte_range=(0, 499))
    with stream:
        data = stream.read()

    # From byte 4096 to end of object
    stream, headers = s3.get_object2("mybucket", "file.bin", byte_range=(4096, None))
    with stream:
        tail = stream.read()
```

### Uploading Objects

#### put_object2 (recommended)

`put_object2` returns a `PutResult` TypedDict on success, or `None` when a conditional check fails — no exception to catch.

| Field        | Type           | Description                                             |
|--------------|----------------|---------------------------------------------------------|
| `etag`       | `str`          | ETag of the stored object; use with `if_match` for future consistency checks |
| `version_id` | `str \| None`  | Version ID if bucket versioning is enabled              |
| `checksum`   | `str \| None`  | Server-confirmed checksum if one was requested          |

```python
with Connection(access_id, secret) as s3:
    # Upload bytes
    result = s3.put_object2("mybucket", "file.txt", b"Hello World")
    print(result['etag'])

    # Upload from an open file or BytesIO
    with open("local.bin", "rb") as f:
        result = s3.put_object2("mybucket", "remote.bin", f)

    # Create-only — None means the object already existed, upload was skipped
    result = s3.put_object2("mybucket", "file.txt", b"data", if_none_match=True)
    if result is None:
        pass  # object already exists

    # Optimistic locking — None means a concurrent write changed the object
    result = s3.put_object2("mybucket", "file.txt", b"updated", if_match=old_etag)
    if result is None:
        pass  # ETag changed, retry with a fresh read
```

#### put_object (low-level)

`put_object` returns a raw `(status, headers)` tuple. Conditional failures raise `PreconditionFailed`.

```python
from s3lib import Connection, PreconditionFailed

with Connection(access_id, secret) as s3:
    # Create-only upload
    try:
        s3.put_object("mybucket", "file.txt", b"data", if_none_match=True)
    except PreconditionFailed:
        pass  # object already exists

    # Optimistic locking
    try:
        s3.put_object("mybucket", "file.txt", b"updated", if_match=old_etag)
    except PreconditionFailed:
        pass  # ETag changed, retry with a fresh read
```

### Other Operations

```python
with Connection(access_id, secret) as s3:
    # List buckets
    for bucket in s3.list_buckets():
        print(bucket)

    # List objects (keys only)
    for key in s3.list_bucket("mybucket"):
        print(key)

    # List objects with metadata
    for obj in s3.list_bucket2("mybucket"):
        print(obj['Key'], obj['Size'], obj['LastModified'])

    # Object metadata
    headers = s3.head_object("mybucket", "file.txt")

    # Copy object within or between buckets
    s3.copy_object("bucket1", "src.txt", "bucket2", "dst.txt")

    # Delete one object
    s3.delete_object("mybucket", "file.txt")

    # Bulk delete
    for key, ok in s3.delete_objects("mybucket", ["a.txt", "b.txt"]):
        print(key, ok)
```

### Connection Options

```python
# Custom endpoint (e.g. MinIO or a specific AWS region)
with Connection(access_id, secret, host="s3.us-west-2.amazonaws.com") as s3:
    pass

# Custom port
with Connection(access_id, secret, port=9000) as s3:
    pass

# Connection timeout (seconds)
with Connection(access_id, secret, conn_timeout=60) as s3:
    pass
```

## Development

See [MAINTAINING.md](MAINTAINING.md) for development and maintenance instructions.

### Running Tests

```bash
# Install development dependencies
make dev

# Run tests, type checking, and linting
make check

# Run tests with coverage report
make test

# Type check only
make typecheck

# Lint only
make lint
```

## License

MIT License - See setup.py for details.

## Author

Andrew Thomson (<athomsonguy@gmail.com>)
