# S3 Checksum and Conditional Headers Implementation Plan

## Overview

This document outlines the implementation plan for adding modern S3 checksum headers and conditional request support to s3lib, based on the features described in `s3_checksums_and_conditionals.md`.

## Current Architecture

### Three-Layer Design

1. **High-level API** (`put_object`, `get_object`, etc.)
   - User-facing interface
   - Returns `(status, headers)` tuple
   - Currently: Simple pass-through to mid-level

2. **Mid-level** (`_s3_put_request`, `_s3_get_request`, etc.)
   - Handles operation-specific logic
   - `_s3_put_request`: Calculates `Content-Length`
   - Calls low-level `_s3_request`

3. **Low-level** (`_s3_request`)
   - AWS v2 signature calculation
   - HTTP connection management
   - Error handling
   - **Currently auto-calculates MD5 for signature**

### Current Content-MD5 Behavior

**Location**: `s3lib/__init__.py:234`

```python
content_md5 = sign_content_if_possible(content)
```

- Auto-calculates MD5 if content is str/bytes
- Used in AWS v2 signature string (line 236)
- Added to `Content-MD5` header (line 247-248)
- **No way for caller to provide pre-calculated MD5**
- TODOs at lines 186, 213 mention this limitation

### Problem Statement

1. **Content-MD5 is conflated**: Used for both signature and integrity
2. **No user control**: Can't provide pre-calculated MD5 (wastes CPU for farmfs)
3. **No modern checksums**: No support for SHA256, x-amz-checksum-* headers
4. **No conditionals**: No support for If-Match, If-None-Match

## Design Principles

### Separation of Concerns

**Signature MD5** (AWS v2 authentication):
- Part of AWS signature calculation
- Legacy behavior
- Managed by `_s3_request` (low-level)
- Should be explicit parameter

**Integrity Checksums** (data validation):
- Modern x-amz-checksum-* headers
- User-controlled (algorithm choice)
- Managed by high-level API (`put_object`)
- Passed via headers dict to `_s3_request`

**Conditional Headers** (concurrency control):
- If-Match, If-None-Match
- Managed by high-level API
- Passed via headers dict to `_s3_request`

### Key Insight

`_s3_request` should:
- Accept explicit `content_md5` parameter for signature
- Accept headers dict that may contain modern checksums
- NOT know about modern checksum semantics
- Set `Content-MD5` header itself (signature concern)

Higher layers (`put_object`) should:
- Calculate or accept user-provided modern checksums
- Add x-amz-checksum-* headers to headers dict
- Add conditional headers to headers dict
- Optionally provide `content_md5` to `_s3_request`

## Phase 1: Refactor `_s3_request` (Bottom Layer)

### Current Signature

```python
def _s3_request(self, method, bucket, key, args, headers, content):
```

### New Signature

```python
def _s3_request(self, method, bucket, key, args, headers, content, content_md5=None):
    """
    Low-level S3 request with AWS v2 signature.

    Args:
        method: HTTP method
        bucket: S3 bucket name
        key: Object key
        args: Query arguments dict
        headers: Request headers dict (may include x-amz-checksum-*, If-Match, If-None-Match)
        content: Request body
        content_md5: Optional Content-MD5 for signature. If None, auto-calculated if possible.
                     This is the legacy MD5 used for AWS v2 signature authentication.
                     Modern integrity checksums (x-amz-checksum-*) should be in headers dict.

    Returns:
        HTTPResponse object
    """
```

### Changes

**Before (line 234):**
```python
content_md5 = sign_content_if_possible(content)
```

**After:**
```python
# Handle Content-MD5 for signature (AWS v2 authentication)
# If not provided explicitly, try to auto-calculate
if content_md5 is None:
    content_md5 = sign_content_if_possible(content)
```

### Benefits

1. **Explicit contract**: Content-MD5 for signature is now explicit parameter
2. **User control**: Callers can provide pre-calculated MD5 (saves CPU)
3. **Backward compatible**: Default None maintains current auto-calc behavior
4. **Separation**: Modern checksums stay in headers dict (passed through)
5. **Clear ownership**: `_s3_request` owns signature MD5, higher layers own integrity checksums

### Update All Callers

Need to update all methods that call `_s3_request`:

1. `_s3_get_service_request()` - line 124
2. `_s3_list_request()` - line 139
3. `_s3_get_request()` - line 147
4. `_s3_head_request()` - line 153
5. `_s3_delete_request()` - line 161
6. `_s3_delete_bulk_request()` - line 170
7. `_s3_copy_request()` - line 180
8. `_s3_put_request()` - line 205

**Most don't need content_md5** (no body or empty body):
- GET, HEAD, DELETE → no content_md5 needed (use default None)
- POST (delete bulk) → has XML content, could auto-calculate or pass explicitly
- PUT → needs content_md5 for data uploads

### Signature Compatibility

**Question**: Does AWS v2 signature require Content-MD5 to match actual content?

**Answer**: Yes, if Content-MD5 is included in signature, it must match the actual header sent and the content.

**Implications**:
- Auto-calculation is safe (calculates once, uses everywhere)
- User-provided must be correct
- Modern checksums (x-amz-checksum-*) are NOT part of v2 signature

## Phase 2: Add Checksum Utilities

### Checksum Calculation Functions

```python
def calculate_checksum(content, algorithm):
    """
    Calculate checksum for content.

    Args:
        content: str or bytes to hash
        algorithm: 'MD5', 'SHA256', 'SHA1'

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

    if algorithm == 'MD5':
        hash_obj = md5(content)
    elif algorithm == 'SHA256':
        from hashlib import sha256
        hash_obj = sha256(content)
    elif algorithm == 'SHA1':
        hash_obj = sha1(content)
    else:
        raise ValueError(f"Unsupported algorithm: {algorithm}. Use MD5, SHA256, or SHA1")

    return binascii.b2a_base64(hash_obj.digest()).strip().decode('ascii')


def calculate_checksum_if_possible(content, algorithm):
    """
    Calculate checksum if content is str/bytes, otherwise return empty string.

    Args:
        content: Data to hash
        algorithm: 'MD5', 'SHA256', 'SHA1'

    Returns:
        Base64-encoded checksum, or '' if content is not str/bytes
    """
    if content != '' and isinstance(content, (str, bytes)):
        return calculate_checksum(content, algorithm)
    return ""
```

### Refactor Existing MD5 Functions

Keep `sign_content` and `sign_content_if_possible` for backward compatibility:

```python
def sign_content(content):
    """Calculate MD5 checksum (legacy name)."""
    return calculate_checksum(content, 'MD5')

def sign_content_if_possible(content):
    """Calculate MD5 if possible (legacy name)."""
    return calculate_checksum_if_possible(content, 'MD5')
```

## Phase 3: Update High-Level APIs (Vertical Slices)

### 3.1: `put_object` - Most Complex

Current signature:
```python
def put_object(self, bucket, key, data, headers=None):
```

New signature:
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
        checksum_value: Base64-encoded checksum. If None and algorithm specified, auto-calculate if possible.
        if_none_match: If True, upload only succeeds if object doesn't exist (create-only)
        if_match: ETag value for optimistic concurrency control (overwrite specific version)

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

        # With SHA256 integrity (auto-calculated)
        conn.put_object(bucket, key, data, checksum_algorithm='SHA256')

        # User-provided checksum (farmfs use case)
        checksum = calculate_sha256(blob_data)  # Pre-computed
        conn.put_object(bucket, key, blob_data,
                       checksum_algorithm='SHA256',
                       checksum_value=checksum)

        # Safe overwrite (optimistic locking)
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
    if checksum_algorithm:
        if checksum_value is None:
            # Auto-calculate if possible
            checksum_value = calculate_checksum_if_possible(data, checksum_algorithm)
            if not checksum_value:
                raise ValueError(
                    f"Cannot auto-calculate {checksum_algorithm} for streaming data. "
                    "Provide checksum_value explicitly."
                )

        # Add modern checksum headers (not part of signature)
        headers['x-amz-checksum-algorithm'] = checksum_algorithm
        headers[f'x-amz-checksum-{checksum_algorithm.lower()}'] = checksum_value

    # Handle conditional headers
    if if_none_match:
        headers['If-None-Match'] = '*'
    if if_match:
        headers['If-Match'] = if_match

    # Call existing implementation (which calls _s3_put_request)
    (status, resp_headers) = self._s3_put_request(bucket, key, data, headers)
    return (status, resp_headers)
```

**Note**: `_s3_put_request` doesn't need changes - it passes headers through to `_s3_request`.

### 3.2: `head_object` - Simple

Already returns headers tuple. Modern checksums will be in response headers if object was uploaded with them.

**No changes needed** - works as-is.

Users can:
```python
headers = conn.head_object(bucket, key)
headers_dict = dict(headers)
sha256 = headers_dict.get('x-amz-checksum-sha256')
etag = headers_dict.get('etag')
```

### 3.3: `get_object` - Future Enhancement

Currently returns HTTPResponse directly. Checksums are in response headers.

**Phase 3 - No changes needed** - users can read headers from response.

**Future enhancement**: Add optional checksum verification:
```python
def get_object(self, bucket, key, verify_checksum=False, expected_checksum=None):
    # Download object
    # If verify_checksum, read response headers and validate
    # Return (response, checksum_metadata)
```

**Not implementing now** - out of scope.

### 3.4: `list_bucket2` - Investigate

Need to check: Does S3 list response include x-amz-checksum-* metadata for objects?

**Research needed** - might not be in list responses.

**Phase 3 - No changes** - leave as-is for now.

## Implementation Order

### Step 1: Bottom Layer (Plumbing)
1. Add checksum utility functions
2. Refactor `_s3_request` signature
3. Update all callers to pass content_md5 (mostly None)
4. **Test**: Verify backward compatibility - existing tests should pass

### Step 2: Vertical Slice - put_object
1. Add parameters to `put_object`
2. Implement checksum handling
3. Implement conditional headers
4. **Test**: New tests for checksums and conditionals

### Step 3: Vertical Slice - head_object
1. Verify it works as-is
2. **Test**: Read checksums from response

### Step 4: Vertical Slice - get_object
1. Verify it works as-is
2. Document how to read checksums
3. **Test**: Verify response headers

### Step 5: Documentation and Polish
1. Update FUTURE.md with checksum support status
2. Export checksum utilities for user convenience
3. Usage examples

## Testing Strategy

### Unit Tests

1. **Checksum utilities**:
   - Test MD5, SHA256, SHA1 calculation
   - Test base64 encoding
   - Test error handling (unsupported algorithm, wrong types)

2. **_s3_request refactor**:
   - Test with content_md5=None (auto-calculate)
   - Test with content_md5='...' (user-provided)
   - Verify signature includes correct MD5
   - Verify Content-MD5 header set correctly

3. **put_object**:
   - Test with checksum_algorithm + auto-calculate
   - Test with checksum_algorithm + checksum_value
   - Test with if_none_match
   - Test with if_match
   - Test combined scenarios
   - Test error cases (auto-calc on streaming data)

### Integration Tests (Manual with Real S3)

1. Upload with SHA256, verify S3 accepts
2. Upload with wrong checksum, verify S3 rejects (400)
3. Upload with if_none_match to new key, verify succeeds
4. Upload with if_none_match to existing key, verify fails (412)
5. Upload with if_match + correct ETag, verify succeeds
6. Upload with if_match + wrong ETag, verify fails (412)
7. Download object, verify x-amz-checksum-* in response headers

## Open Questions

1. **AWS Signature v4**: Does s3lib use v2 or v4 signing? Are there implications?
   - **Current**: v2 (based on `sign()` function and string_to_sign)
   - **Implication**: Content-MD5 remains relevant for v2 signature

2. **Content-MD5 vs x-amz-checksum-md5**: What's the relationship?
   - **Content-MD5**: Legacy, part of v2 signature
   - **x-amz-checksum-md5**: Modern, explicit MD5, not part of signature
   - **Decision**: Keep Content-MD5 for signature, allow x-amz-checksum-md5 separately

3. **List bucket checksums**: Are checksums included in list responses?
   - **Need to test**: May need S3 API research or live testing

4. **Multipart upload**: How do checksums work with multipart?
   - **Per document**: S3 validates per-part and computes final checksum
   - **Out of scope**: s3lib doesn't support multipart yet
   - **Future**: When adding multipart, checksums will "just work"

## Backward Compatibility

### Guaranteed

1. All existing code continues to work
2. Default parameters maintain current behavior
3. No breaking changes to return values
4. Legacy MD5 behavior unchanged

### Deprecations

None - keeping all existing functions and behavior.

## Success Criteria

1. All existing tests pass
2. Can upload with SHA256 checksum
3. Can use if_none_match for create-only
4. Can use if_match for optimistic locking
5. Farmfs can provide pre-calculated checksums
6. Documentation clear and complete

## Timeline Estimate

- Step 1 (bottom layer): 1-2 hours
- Step 2 (put_object): 2-3 hours
- Step 3-4 (head/get): 1 hour
- Step 5 (docs/tests): 2-3 hours

**Total**: 6-9 hours of work
