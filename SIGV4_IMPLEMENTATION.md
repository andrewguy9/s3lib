# AWS Signature Version 4 Implementation

This document describes the AWS Signature Version 4 implementation added to S3Lib.

## Overview

AWS Signature Version 4 (SigV4) is the current standard for signing requests to AWS services. This implementation provides a complete, tested signing process based on official AWS documentation and test cases.

## Implementation Status

**Status**: ✅ Complete and Tested

All functions are fully implemented and tested against official AWS test cases from:
https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html

## Files Added

### `/s3lib/sigv4.py`
New module containing the Signature Version 4 implementation:

**Core Functions**:
- `create_canonical_request()` - Creates the canonical request string
- `create_string_to_sign()` - Creates the string to sign from canonical request
- `derive_signing_key()` - Derives the signing key using HMAC chain
- `calculate_signature()` - Calculates the final signature
- `sign_request_v4()` - Complete signing process (convenience wrapper)

**Helper Functions**:
- `hash_payload()` - SHA256 hash of request payload
- `get_timestamp()` - Current UTC timestamp in ISO 8601 format
- `get_date()` - Current UTC date in YYYYMMDD format

### `/tests/test_sigv4.py`
Comprehensive test suite with 12 test cases:

**Test Classes**:
1. `TestCanonicalRequest` (4 tests)
   - GET Object with Range header
   - PUT Object with special characters
   - GET Bucket Lifecycle (subresource parameters)
   - GET Bucket List Objects (query parameters)

2. `TestStringToSign` (1 test)
   - Verifies string-to-sign creation and hashing

3. `TestSigningKey` (1 test)
   - Verifies HMAC chain for key derivation

4. `TestSignatureCalculation` (4 tests)
   - Complete signature for each of the 4 canonical request examples

5. `TestCompleteSigningProcess` (2 tests)
   - End-to-end Authorization header generation

**Test Data**: All tests use official AWS example credentials and expected signatures

## Test Results

```
========================= 12 passed in 0.03s =========================
```

All tests pass, confirming the implementation matches AWS specifications exactly.

## Key Features

### 1. Canonical Request Creation
- Proper URI encoding (preserves slashes, encodes special chars like $)
- Header normalization (lowercase, alphabetical sorting, whitespace trimming)
- Query string handling (subresources and parameters)
- Signed headers list generation

### 2. Signing Key Derivation
Implements the HMAC chain specified by AWS:
```
kDate    = HMAC-SHA256("AWS4" + SecretKey, Date)
kRegion  = HMAC-SHA256(kDate, Region)
kService = HMAC-SHA256(kRegion, Service)
kSigning = HMAC-SHA256(kService, "aws4_request")
```

### 3. String to Sign
Builds the canonical string containing:
- Algorithm identifier (AWS4-HMAC-SHA256)
- Request timestamp (ISO 8601)
- Credential scope (date/region/service/aws4_request)
- Hashed canonical request (SHA256)

### 4. Authorization Header Generation
Creates properly formatted Authorization header:
```
AWS4-HMAC-SHA256 Credential=<access-key>/<scope>, SignedHeaders=<headers>, Signature=<sig>
```

## Usage Example

```python
from s3lib.sigv4 import sign_request_v4, hash_payload

# Request details
method = "GET"
uri = "/my-bucket/my-object.txt"
query_string = ""
headers = {
    "host": "s3.amazonaws.com",
    "x-amz-date": "20240101T120000Z",
    "x-amz-content-sha256": hash_payload(b""),
}
payload_hash = hash_payload(b"")

# Sign the request
auth_header = sign_request_v4(
    method=method,
    uri=uri,
    query_string=query_string,
    headers=headers,
    payload_hash=payload_hash,
    access_key_id="AKIAIOSFODNN7EXAMPLE",
    secret_key=b"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    region="us-east-1",
    service="s3",
    timestamp="20240101T120000Z"
)

# Use the authorization header in your request
headers["Authorization"] = auth_header
```

## Next Steps

The SigV4 implementation is complete and ready for integration into the main S3Lib Connection class:

1. **Add SigV4 support to Connection class**
   - Add optional `use_sigv4` parameter to Connection.__init__()
   - Update `_s3_request()` to use SigV4 when enabled
   - Add required headers (x-amz-date, x-amz-content-sha256)

2. **Handle region parameter**
   - Add optional `region` parameter to Connection
   - Default to 'us-east-1' for compatibility
   - Allow custom regions for other S3 endpoints

3. **HTTPS support**
   - Update Connection to use HTTPSConnection when using SigV4
   - SigV4 is typically used with HTTPS for security

4. **Backward compatibility**
   - Keep SigV2 as default initially
   - Provide clear migration path in documentation
   - Consider deprecation timeline for SigV2

5. **Update CLI tools**
   - Add `--sigv4` flag to CLI utilities
   - Update documentation with SigV4 examples

6. **Testing**
   - Add integration tests with real S3 (optional)
   - Test with S3-compatible services (MinIO, etc.)

## References

- [AWS Signature V4 Signing Process](https://docs.aws.amazon.com/general/latest/gr/signature-version-4.html)
- [S3 Request Authentication](https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-authenticating-requests.html)
- [Canonical Request Creation](https://docs.aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html)
- [String to Sign](https://docs.aws.amazon.com/general/latest/gr/sigv4-create-string-to-sign.html)
- [Signing Key Derivation](https://docs.aws.amazon.com/general/latest/gr/sigv4-calculate-signature.html)

## Version History

- **2024-12-19**: Initial implementation completed
  - All core functions implemented
  - 12 tests passing against AWS examples
  - Ready for integration
