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

### Basic Usage

```python
from s3lib import Connection

# Create connection
access_id = "AKIAIOSFODNN7EXAMPLE"
secret = b"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

with Connection(access_id, secret) as s3:
    # List buckets
    for bucket in s3.list_buckets():
        print(bucket)

    # List objects in a bucket
    for key in s3.list_bucket("mybucket"):
        print(key)

    # List with metadata
    for obj in s3.list_bucket2("mybucket"):
        print(obj['Key'], obj['Size'], obj['LastModified'])

    # Get object
    response = s3.get_object("mybucket", "myfile.txt")
    data = response.read()

    # Upload object
    s3.put_object("mybucket", "newfile.txt", b"Hello World")

    # Upload from file
    with open("local.txt", "rb") as f:
        s3.put_object("mybucket", "remote.txt", f)

    # Copy object
    s3.copy_object("bucket1", "file.txt", "bucket2", "file.txt")

    # Delete object
    s3.delete_object("mybucket", "oldfile.txt")

    # Bulk delete
    keys = ["file1.txt", "file2.txt", "file3.txt"]
    for key, result in s3.delete_objects("mybucket", keys):
        print(f"{key}: {result}")

    # Get object metadata
    headers = s3.head_object("mybucket", "file.txt")
    print(dict(headers))

    # Get object URL
    url = s3.get_object_url("mybucket", "file.txt")
    print(url)
```

### Connection Options

```python
# Custom endpoint
with Connection(access_id, secret, host="s3.us-west-2.amazonaws.com") as s3:
    pass

# Custom port
with Connection(access_id, secret, port=9000) as s3:
    pass

# Connection timeout
with Connection(access_id, secret, conn_timeout=60) as s3:
    pass
```

### Streaming Large Objects

The library is designed for memory efficiency with large files:

```python
# Download large file
with Connection(access_id, secret) as s3:
    response = s3.get_object("mybucket", "largefile.bin")
    with open("local-large.bin", "wb") as f:
        chunk = response.read(65536)  # 64KB chunks
        while chunk:
            f.write(chunk)
            chunk = response.read(65536)

# Upload large file
with Connection(access_id, secret) as s3:
    with open("large-local.bin", "rb") as f:
        s3.put_object("mybucket", "large-remote.bin", f)
```

## Development

See [MAINTAINING.md](MAINTAINING.md) for development and maintenance instructions.

### Running Tests

```bash
# Install development dependencies
pip install tox

# Run tests
tox

# Run tests for specific Python version
tox -e py39

# Run linting
tox -e lint
```

## License

MIT License - See setup.py for details.

## Author

Andrew Thomson (<athomsonguy@gmail.com>)
