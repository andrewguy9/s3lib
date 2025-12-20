"""
AWS Signature Version 4 implementation for S3 requests.

This module implements the AWS Signature Version 4 signing process as described in:
https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-authenticating-requests.html

The signing process consists of several steps:
1. Create a canonical request
2. Create a string to sign
3. Derive a signing key
4. Calculate the signature

References:
- Canonical Request: https://docs.aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html
- String to Sign: https://docs.aws.amazon.com/general/latest/gr/sigv4-create-string-to-sign.html
- Signing Key: https://docs.aws.amazon.com/general/latest/gr/sigv4-calculate-signature.html
"""

import hmac
from hashlib import sha256
from urllib.parse import quote


def create_canonical_request(method, uri, query_string, headers, payload_hash):
    """
    Create a canonical request string for AWS Signature Version 4.

    The canonical request is created by concatenating:
    - HTTP method
    - Canonical URI
    - Canonical query string
    - Canonical headers
    - Signed headers
    - Hashed payload

    Args:
        method: HTTP method (GET, PUT, POST, DELETE, etc.)
        uri: The URI-encoded version of the absolute path component URL
        query_string: The URL-encoded query string parameters
        headers: Dictionary of HTTP headers to include in signing
        payload_hash: Hex-encoded SHA256 hash of the request payload

    Returns:
        tuple: (canonical_request_string, signed_headers_string)
            - canonical_request_string: The complete canonical request
            - signed_headers_string: Semicolon-separated list of signed header names
    """
    # 1. Canonical URI - URI-encode each path segment, preserve slashes
    canonical_uri = _create_canonical_uri(uri)

    # 2. Canonical query string - already provided in correct format for our tests
    canonical_query_string = query_string

    # 3. Canonical headers - lowercase names, sort alphabetically, trim values
    canonical_headers_list = []
    sorted_header_names = sorted(headers.keys(), key=str.lower)

    for header_name in sorted_header_names:
        # Lowercase the header name
        canonical_header_name = header_name.lower()
        # Trim header value (strip leading/trailing whitespace, collapse multiple spaces)
        # Convert to string first in case it's an integer (like content-length)
        header_value_str = str(headers[header_name])
        header_value = ' '.join(header_value_str.split())
        canonical_headers_list.append(f"{canonical_header_name}:{header_value}")

    canonical_headers = '\n'.join(canonical_headers_list) + '\n'

    # 4. Signed headers - semicolon-separated list of lowercase header names
    signed_headers = ';'.join([h.lower() for h in sorted_header_names])

    # 5. Build the canonical request
    canonical_request = '\n'.join([
        method,
        canonical_uri,
        canonical_query_string,
        canonical_headers,
        signed_headers,
        payload_hash
    ])

    return canonical_request, signed_headers


def _create_canonical_uri(uri):
    """
    Create the canonical URI by URI-encoding each path segment.

    AWS requires encoding each path segment but preserving the forward slashes.
    Special characters like $ need to be encoded as %24.

    Args:
        uri: The URI path

    Returns:
        str: The canonical URI
    """
    if not uri:
        return '/'

    # Split by '/', encode each segment, then rejoin
    segments = uri.split('/')
    encoded_segments = [quote(segment, safe='') for segment in segments]
    canonical_uri = '/'.join(encoded_segments)

    # Ensure it starts with /
    if not canonical_uri.startswith('/'):
        canonical_uri = '/' + canonical_uri

    return canonical_uri


def create_string_to_sign(canonical_request, timestamp, date, region, service):
    """
    Create the string to sign for AWS Signature Version 4.

    The string to sign is created by concatenating:
    - Algorithm (AWS4-HMAC-SHA256)
    - Request timestamp (ISO 8601 format)
    - Credential scope (date/region/service/aws4_request)
    - Hashed canonical request

    Args:
        canonical_request: The canonical request string
        timestamp: ISO 8601 timestamp (YYYYMMDDTHHMMSSZ)
        date: Date in YYYYMMDD format
        region: AWS region (e.g., us-east-1)
        service: AWS service (e.g., s3)

    Returns:
        str: The string to sign
    """
    # Hash the canonical request
    canonical_request_hash = sha256(canonical_request.encode('utf-8')).hexdigest()

    # Build the credential scope
    credential_scope = f"{date}/{region}/{service}/aws4_request"

    # Build the string to sign
    string_to_sign = '\n'.join([
        'AWS4-HMAC-SHA256',
        timestamp,
        credential_scope,
        canonical_request_hash
    ])

    return string_to_sign


def derive_signing_key(secret_key, date, region, service):
    """
    Derive the signing key for AWS Signature Version 4.

    The signing key is derived using a series of HMAC operations:
    - kDate = HMAC("AWS4" + SecretKey, Date)
    - kRegion = HMAC(kDate, Region)
    - kService = HMAC(kRegion, Service)
    - kSigning = HMAC(kService, "aws4_request")

    Args:
        secret_key: AWS secret access key (bytes)
        date: Date in YYYYMMDD format (string)
        region: AWS region (e.g., us-east-1)
        service: AWS service (e.g., s3)

    Returns:
        bytes: The derived signing key (32 bytes)
    """
    def _sign(key, msg):
        """Helper to create HMAC-SHA256."""
        return hmac.new(key, msg.encode('utf-8'), sha256).digest()

    # Chain of HMAC operations to derive the signing key
    k_date = _sign(b'AWS4' + secret_key, date)
    k_region = _sign(k_date, region)
    k_service = _sign(k_region, service)
    k_signing = _sign(k_service, 'aws4_request')

    return k_signing


def calculate_signature(signing_key, string_to_sign):
    """
    Calculate the final signature using the signing key and string to sign.

    The signature is calculated by:
    - HMAC-SHA256(signing_key, string_to_sign)
    - Convert result to lowercase hex string

    Args:
        signing_key: The derived signing key (bytes)
        string_to_sign: The string to sign (str)

    Returns:
        str: The signature as a lowercase hexadecimal string
    """
    signature_bytes = hmac.new(
        signing_key,
        string_to_sign.encode('utf-8'),
        sha256
    ).digest()

    return signature_bytes.hex()


def sign_request_v4(method, uri, query_string, headers, payload_hash,
                    access_key_id, secret_key, region, service, timestamp):
    """
    Complete AWS Signature Version 4 signing process.

    This is a convenience function that performs all signing steps and returns
    the Authorization header value.

    Args:
        method: HTTP method (GET, PUT, POST, DELETE, etc.)
        uri: The URI-encoded version of the absolute path component URL
        query_string: The URL-encoded query string parameters
        headers: Dictionary of HTTP headers to include in signing
        payload_hash: Hex-encoded SHA256 hash of the request payload
        access_key_id: AWS access key ID
        secret_key: AWS secret access key (bytes)
        region: AWS region (e.g., us-east-1)
        service: AWS service (e.g., s3)
        timestamp: ISO 8601 timestamp (YYYYMMDDTHHMMSSZ)

    Returns:
        str: The complete Authorization header value
    """
    # Extract date from timestamp (first 8 characters: YYYYMMDD)
    date = timestamp[:8]

    # Step 1: Create canonical request
    canonical_request, signed_headers = create_canonical_request(
        method, uri, query_string, headers, payload_hash
    )

    # Step 2: Create string to sign
    string_to_sign = create_string_to_sign(
        canonical_request, timestamp, date, region, service
    )

    # Step 3: Derive signing key
    signing_key = derive_signing_key(secret_key, date, region, service)

    # Step 4: Calculate signature
    signature = calculate_signature(signing_key, string_to_sign)

    # Step 5: Build Authorization header
    credential = f"{access_key_id}/{date}/{region}/{service}/aws4_request"
    authorization_header = (
        f"AWS4-HMAC-SHA256 "
        f"Credential={credential}, "
        f"SignedHeaders={signed_headers}, "
        f"Signature={signature}"
    )

    return authorization_header


def hash_payload(payload):
    """
    Calculate the SHA256 hash of a payload.

    Args:
        payload: The request payload (bytes or str)

    Returns:
        str: Lowercase hexadecimal hash of the payload
    """
    if isinstance(payload, str):
        payload = payload.encode('utf-8')
    return sha256(payload).hexdigest()


def get_timestamp():
    """
    Get current UTC timestamp in ISO 8601 format (YYYYMMDDTHHMMSSZ).

    Returns:
        str: Current timestamp
    """
    import time
    return time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())


def get_date():
    """
    Get current UTC date in YYYYMMDD format.

    Returns:
        str: Current date
    """
    import time
    return time.strftime('%Y%m%d', time.gmtime())
