# Modern S3 Checksum Implementation Plan (SigV4 Foundation)

## Overview

This plan builds on the completed SigV4 migration to add modern S3 checksum and conditional headers support as described in `s3_checksums_and_conditionals.md`.

## Current State After SigV4 Merge

### Signature Already Uses SHA256

**Location**: `s3lib/__init__.py:312-318`

```python
# Calculate payload hash for SigV4
if isinstance(content, (str, bytes)):
    payload_hash = hash_payload(content)  # Returns hex-encoded SHA256
else:
    payload_hash = "UNSIGNED-PAYLOAD"
```

**Key insight**: We already calculate SHA256 for signature authentication!

### Format Difference

- **Signature**: Uses `hash_payload()` → hex-encoded (lowercase hex string)
- **Integrity checksums**: Need base64-encoded SHA256
- **Same hash, different encoding!**

### Current Usage

```python
headers["x-amz-content-sha256"] = payload_hash  # hex for signature
```

## Optimization Opportunity

**For SHA256 checksums, we can reuse the signature hash:**

1. Calculate SHA256 once (as bytes)
2. Encode as hex for signature (`x-amz-content-sha256`)
3. Encode as base64 for integrity (`x-amz-checksum-sha256`)
4. Farmfs provides SHA256 once, we derive both formats

## Design

### Principle: Separation of Concerns

**Signature concern** (managed by `_s3_request_inner`):
- `x-amz-content-sha256` header (hex-encoded)
- Required for AWS SigV4 authentication
- Can be `UNSIGNED-PAYLOAD` for streaming

**Integrity concern** (managed by high-level API):
- `x-amz-checksum-*` headers (base64-encoded)
- Optional data validation
- User-controlled algorithm choice
- Passed via headers dict

**Conditional concern** (managed by high-level API):
- `If-Match`, `If-None-Match` headers
- Concurrency control
- Passed via headers dict

### Low-Level Changes: `_s3_request_inner`

**Current signature**:
```python
def _s3_request_inner(self, method, bucket, key, args, headers, content):
```

**New signature**:
```python
def _s3_request_inner(self, method, bucket, key, args, headers, content,
                      payload_sha256_bytes=None):
    """
    Inner request method that performs a single S3 request.

    Args:
        ...existing args...
        payload_sha256_bytes: Optional pre-calculated SHA256 digest as bytes.
                             If provided, skips hash calculation and uses this for
                             both signature (hex) and can be used by caller for
                             integrity checksums (base64).
    """
```

**Implementation**:
```python
# Calculate payload hash for SigV4
if payload_sha256_bytes is not None:
    # Use provided hash
    payload_hash = payload_sha256_bytes.hex()
elif isinstance(content, (str, bytes)):
    # Calculate hash and store as bytes for potential reuse
    if isinstance(content, str):
        content_bytes = content.encode('utf-8')
    else:
        content_bytes = content
    payload_sha256_bytes = sha256(content_bytes).digest()
    payload_hash = payload_sha256_bytes.hex()
else:
    # For file-like objects, use UNSIGNED-PAYLOAD
    payload_hash = "UNSIGNED-PAYLOAD"
    payload_sha256_bytes = None

headers["x-amz-content-sha256"] = payload_hash
```

**Why?**
- Allows caller to provide pre-calculated hash (farmfs use case)
- Low-level stays focused on signature
- Doesn't need to know about modern checksums

### High-Level Changes: Checksum Utilities

**New functions in `s3lib/__init__.py`:**

```python
def calculate_checksum(content, algorithm):
    """
    Calculate checksum for content.

    Args:
        content: str or bytes to hash
        algorithm: 'SHA256', 'SHA1', 'MD5'

    Returns:
        Base64-encoded checksum string

    Raises:
        ValueError: If algorithm not supported
        TypeError: If content is not str/bytes
    """
    if not isinstance(content, (str, bytes)):
        raise TypeError("Content must be str or bytes for checksum calculation")

    if isinstance(content, str):
        content = content.encode('utf-8')

    if algorithm == 'SHA256':
        from hashlib import sha256
        digest = sha256(content).digest()
    elif algorithm == 'SHA1':
        digest = sha1(content).digest()
    elif algorithm == 'MD5':
        digest = md5(content).digest()
    else:
        raise ValueError(f"Unsupported algorithm: {algorithm}. Use SHA256, SHA1, or MD5")

    return binascii.b2a_base64(digest).strip().decode('ascii')


def calculate_checksum_if_possible(content, algorithm):
    """
    Calculate checksum if content is str/bytes, otherwise return empty string.

    Args:
        content: Data to hash
        algorithm: 'SHA256', 'SHA1', 'MD5'

    Returns:
        Base64-encoded checksum, or '' if content is not str/bytes
    """
    if content != '' and isinstance(content, (str, bytes)):
        return calculate_checksum(content, algorithm)
    return ""


def sha256_hex_to_base64(hex_string):
    """
    Convert hex-encoded SHA256 to base64-encoded.

    Utility for converting signature hash to integrity checksum format.

    Args:
        hex_string: Hex-encoded SHA256 (64 chars, lowercase)

    Returns:
        Base64-encoded SHA256 (44 chars)
    """
    digest_bytes = bytes.fromhex(hex_string)
    return binascii.b2a_base64(digest_bytes).strip().decode('ascii')
```

### High-Level Changes: `put_object`

**Current signature**:
```python
def put_object(self, bucket, key, data, headers=None):
```

**New signature**:
```python
def put_object(self, bucket, key, data, headers=None,
               checksum_algorithm=None, checksum_value=None,
               if_none_match=False, if_match=None):
    """
    Push object from local to bucket with optional integrity and conditional checks.

    Args:
        bucket: S3 bucket name
        key: Object key
        data: Data to upload (str, bytes, or file object)
        headers: Optional dict of additional headers
        checksum_algorithm: Algorithm for integrity check: 'SHA256', 'SHA1', 'MD5', or None
        checksum_value: Base64-encoded checksum. If None and algorithm specified, auto-calculate.
        if_none_match: If True, upload only succeeds if object doesn't exist (create-only)
        if_match: ETag value for optimistic concurrency control

    Returns:
        (status, headers) tuple
        Response headers include x-amz-checksum-{algorithm} if used

    Raises:
        ValueError: If checksum auto-calculation requested but not possible (streaming data)
        HTTP 412: If if_none_match=True and object already exists
        HTTP 412: If if_match provided and ETag doesn't match
        HTTP 400: If checksum_value doesn't match uploaded data

    Examples:
        # Basic upload (legacy behavior)
        conn.put_object(bucket, key, data)

        # Create-only (prevent overwrites)
        conn.put_object(bucket, key, data, if_none_match=True)

        # With SHA256 integrity (auto-calculated, reuses signature hash)
        conn.put_object(bucket, key, data, checksum_algorithm='SHA256')

        # User-provided checksum (farmfs: pre-computed SHA256)
        checksum_b64 = calculate_checksum(blob_data, 'SHA256')
        conn.put_object(bucket, key, blob_data,
                       checksum_algorithm='SHA256',
                       checksum_value=checksum_b64)

        # Safe overwrite with optimistic locking
        headers = conn.head_object(bucket, key)
        etag = dict(headers)['etag']
        conn.put_object(bucket, key, new_data, if_match=etag)

        # Combined: create-only + integrity
        conn.put_object(bucket, key, data,
                       checksum_algorithm='SHA256',
                       if_none_match=True)
    """
    if headers is None:
        headers = dict()
    else:
        headers = dict(headers)

    # Handle modern checksum headers
    payload_sha256_bytes = None
    if checksum_algorithm:
        if checksum_value is None:
            # Auto-calculate checksum
            if not isinstance(data, (str, bytes)):
                raise ValueError(
                    f"Cannot auto-calculate {checksum_algorithm} for streaming data. "
                    "Provide checksum_value explicitly."
                )

            # For SHA256, we'll calculate once and reuse for signature
            if checksum_algorithm == 'SHA256':
                if isinstance(data, str):
                    data_bytes = data.encode('utf-8')
                else:
                    data_bytes = data
                payload_sha256_bytes = sha256(data_bytes).digest()
                checksum_value = binascii.b2a_base64(payload_sha256_bytes).strip().decode('ascii')
            else:
                # For other algorithms, calculate separately
                checksum_value = calculate_checksum(data, checksum_algorithm)

        # Add modern checksum headers
        headers['x-amz-checksum-algorithm'] = checksum_algorithm
        headers[f'x-amz-checksum-{checksum_algorithm.lower()}'] = checksum_value

    # Handle conditional headers
    if if_none_match:
        headers['If-None-Match'] = '*'
    if if_match:
        headers['If-Match'] = if_match

    # Call existing _s3_put_request (which calls _s3_request)
    # Pass payload_sha256_bytes for SHA256 optimization
    (status, resp_headers) = self._s3_put_request(bucket, key, data, headers,
                                                   payload_sha256_bytes=payload_sha256_bytes)
    return (status, resp_headers)
```

### Mid-Level Changes: `_s3_put_request`

**Need to thread through `payload_sha256_bytes`:**

```python
def _s3_put_request(self, bucket, key, data, headers, payload_sha256_bytes=None):
    # ... existing content-length logic ...

    resp = self._s3_request("PUT", bucket, key, args, headers, data,
                           payload_sha256_bytes=payload_sha256_bytes)
    # ... rest unchanged ...
```

### Outer Request Wrapper: `_s3_request`

**Need to thread through to `_s3_request_inner`:**

```python
def _s3_request(self, method, bucket, key, args, headers, content,
                payload_sha256_bytes=None):
    """
    Make an S3 request using AWS Signature Version 4.
    Automatically handles region discovery from 307 redirects.
    """
    # ... region discovery logic ...

    for attempt in range(max_redirects):
        resp = self._s3_request_inner(method, bucket, key, args, headers.copy(), content,
                                      payload_sha256_bytes=payload_sha256_bytes)
        # ... redirect handling ...
```

## Implementation Steps

### Step 1: Add Checksum Utilities (Bottom Layer)
**Files**: `s3lib/__init__.py`

1. Add `calculate_checksum()` function
2. Add `calculate_checksum_if_possible()` function
3. Add `sha256_hex_to_base64()` utility
4. **Test**: Unit tests for checksum calculations

### Step 2: Refactor `_s3_request_inner` for Optional Hash
**Files**: `s3lib/__init__.py`

1. Add `payload_sha256_bytes` parameter
2. Use provided hash or calculate
3. Store bytes for potential reuse
4. **Test**: Verify signature still works, backward compatible

### Step 3: Thread Through Mid-Level
**Files**: `s3lib/__init__.py`

1. Update `_s3_request` signature
2. Update `_s3_put_request` signature
3. Pass `payload_sha256_bytes` through layers
4. **Test**: Existing tests still pass

### Step 4: Enhance `put_object`
**Files**: `s3lib/__init__.py`

1. Add checksum parameters
2. Add conditional parameters
3. Implement auto-calculation with SHA256 optimization
4. Add headers to request
5. **Test**: New tests for all features

### Step 5: Verify Other Operations
**Files**: `s3lib/__init__.py`

1. Review `head_object` - should work as-is
2. Review `get_object` - should work as-is
3. Review `list_bucket2` - investigate if checksums included
4. **Test**: Read checksums from responses

### Step 6: Documentation and Polish
**Files**: `README.md`, `FUTURE.md`

1. Update README with checksum examples
2. Mark checksum support as complete in FUTURE.md
3. Add migration notes
4. Export utilities for user convenience

## Testing Strategy

### Unit Tests

**File**: `tests/test_checksums.py` (new)

```python
def test_calculate_checksum_sha256():
    """Test SHA256 checksum calculation."""
    data = b"test data"
    checksum = calculate_checksum(data, 'SHA256')
    # Verify it's base64-encoded
    assert len(checksum) == 44
    # Verify it decodes correctly
    import base64
    decoded = base64.b64decode(checksum)
    assert len(decoded) == 32  # SHA256 is 32 bytes

def test_calculate_checksum_sha1():
    """Test SHA1 checksum calculation."""
    # Similar structure

def test_calculate_checksum_md5():
    """Test MD5 checksum calculation."""
    # Similar structure

def test_sha256_hex_to_base64():
    """Test conversion between formats."""
    data = b"test data"
    hex_hash = sha256(data).hexdigest()
    b64_hash = sha256_hex_to_base64(hex_hash)

    # Should match direct base64 encoding
    direct_b64 = binascii.b2a_base64(sha256(data).digest()).strip().decode('ascii')
    assert b64_hash == direct_b64

def test_put_object_with_sha256_checksum():
    """Test put_object with SHA256 checksum (auto-calculated)."""
    # Mock S3 connection
    # Verify x-amz-checksum-algorithm and x-amz-checksum-sha256 headers

def test_put_object_with_user_provided_checksum():
    """Test put_object with user-provided checksum."""
    # Farmfs use case

def test_put_object_with_if_none_match():
    """Test create-only semantics."""
    # Verify If-None-Match: * header

def test_put_object_with_if_match():
    """Test optimistic locking."""
    # Verify If-Match header

def test_put_object_checksum_and_conditional():
    """Test combined checksum + conditional."""
    # Both headers should be present
```

### Integration Tests (Manual with Real S3)

1. Upload with SHA256, verify S3 accepts and returns checksum
2. Upload with wrong checksum, verify S3 rejects (400 BadDigest)
3. Upload with `if_none_match=True` to new key → succeeds
4. Upload with `if_none_match=True` to existing key → fails (412)
5. Upload with `if_match` + correct ETag → succeeds
6. Upload with `if_match` + wrong ETag → fails (412)
7. Verify `head_object` returns `x-amz-checksum-sha256`
8. Verify `get_object` response includes checksum headers

## Farmfs Use Case

### Current (without checksums):
```python
# Farmfs calculates SHA256 for blob content addressing
blob_hash = sha256(blob_data).hexdigest()  # hex

# Upload to S3 (recalculates SHA256 for signature)
conn.put_object(bucket, blob_hash, blob_data)
```

**Problem**: SHA256 calculated twice!

### With checksums (optimized):
```python
# Farmfs calculates SHA256 once
blob_hash_bytes = sha256(blob_data).digest()
blob_hash_hex = blob_hash_bytes.hex()
blob_hash_b64 = binascii.b2a_base64(blob_hash_bytes).strip().decode('ascii')

# Upload to S3 with integrity check (reuses hash for signature)
conn.put_object(bucket, blob_hash_hex, blob_data,
               checksum_algorithm='SHA256',
               checksum_value=blob_hash_b64,
               if_none_match=True)  # Create-only
```

**Benefits**:
- SHA256 calculated once (farmfs already has it)
- Integrity verified by S3
- Create-only prevents overwrites
- Can detect duplicate uploads from 412 response

## Backward Compatibility

### Guaranteed
- All existing code works unchanged
- Default parameters maintain current behavior
- No breaking changes to return values
- SigV4 signature unaffected

### No Deprecations
- Keep all existing functions

## Success Criteria

1. ✅ All existing tests pass
2. ✅ Can upload with SHA256 checksum (auto-calc)
3. ✅ Can upload with user-provided checksum
4. ✅ SHA256 calculated only once for signature + integrity
5. ✅ Can use `if_none_match` for create-only
6. ✅ Can use `if_match` for optimistic locking
7. ✅ HEAD/GET return checksum headers
8. ✅ Farmfs can optimize by providing pre-calculated SHA256

## Open Questions

1. **Other algorithms (CRC32, CRC32C)**: Defer to future?
   - **Decision**: Start with SHA256, SHA1, MD5 (no external deps)
   - CRC can be added later if users request

2. **List bucket checksums**: Are they included in responses?
   - **Decision**: Needs investigation, likely not in list responses
   - Document if/when available

3. **GET checksum verification**: Auto-verify on download?
   - **Decision**: Phase 2 feature, not in initial implementation
   - For now, checksums available in response headers

## Timeline Estimate

- Step 1 (utilities): 1 hour
- Step 2 (refactor _s3_request_inner): 1-2 hours
- Step 3 (thread through): 1 hour
- Step 4 (put_object): 2-3 hours
- Step 5 (verify other ops): 1 hour
- Step 6 (docs/tests): 2-3 hours

**Total**: 8-11 hours of work

## Next Actions

1. Implement Step 1 (checksum utilities)
2. Implement Step 2 (refactor _s3_request_inner)
3. Test backward compatibility
4. Continue with Steps 3-6
