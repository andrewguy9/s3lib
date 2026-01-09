"""
Tests for modern checksum utilities.
"""

import pytest
import binascii
from hashlib import sha256, sha1, md5
from s3lib import calculate_checksum, calculate_checksum_if_possible, sha256_hex_to_base64


def test_calculate_checksum_sha256_bytes():
    """Test SHA256 checksum calculation with bytes input."""
    data = b"test data"
    checksum = calculate_checksum(data, 'SHA256')

    # Verify it's base64-encoded
    assert isinstance(checksum, str)
    assert len(checksum) == 44  # SHA256 base64 is 44 chars

    # Verify it decodes correctly to 32 bytes
    import base64
    decoded = base64.b64decode(checksum)
    assert len(decoded) == 32  # SHA256 is 32 bytes

    # Verify it matches direct calculation
    expected = binascii.b2a_base64(sha256(data).digest()).strip().decode('ascii')
    assert checksum == expected


def test_calculate_checksum_sha256_string():
    """Test SHA256 checksum calculation with string input."""
    data = "test data"
    checksum = calculate_checksum(data, 'SHA256')

    # Should match bytes version
    expected = calculate_checksum(data.encode('utf-8'), 'SHA256')
    assert checksum == expected


def test_calculate_checksum_sha1():
    """Test SHA1 checksum calculation."""
    data = b"test data"
    checksum = calculate_checksum(data, 'SHA1')

    # Verify it's base64-encoded SHA1 (28 chars)
    assert isinstance(checksum, str)
    assert len(checksum) == 28  # SHA1 base64 is 28 chars

    # Verify it matches direct calculation
    expected = binascii.b2a_base64(sha1(data).digest()).strip().decode('ascii')
    assert checksum == expected


def test_calculate_checksum_md5():
    """Test MD5 checksum calculation."""
    data = b"test data"
    checksum = calculate_checksum(data, 'MD5')

    # Verify it's base64-encoded MD5 (24 chars)
    assert isinstance(checksum, str)
    assert len(checksum) == 24  # MD5 base64 is 24 chars

    # Verify it matches direct calculation
    expected = binascii.b2a_base64(md5(data).digest()).strip().decode('ascii')
    assert checksum == expected


def test_calculate_checksum_unsupported_algorithm():
    """Test that unsupported algorithm raises ValueError."""
    data = b"test data"

    with pytest.raises(ValueError, match="Unsupported algorithm"):
        calculate_checksum(data, 'CRC32')


def test_calculate_checksum_invalid_content_type():
    """Test that non-str/bytes content raises TypeError."""
    with pytest.raises(TypeError, match="must be str or bytes"):
        calculate_checksum(123, 'SHA256')

    with pytest.raises(TypeError, match="must be str or bytes"):
        calculate_checksum(['not', 'valid'], 'SHA256')


def test_calculate_checksum_if_possible_bytes():
    """Test conditional checksum calculation with bytes."""
    data = b"test data"
    checksum = calculate_checksum_if_possible(data, 'SHA256')

    # Should match unconditional version
    expected = calculate_checksum(data, 'SHA256')
    assert checksum == expected


def test_calculate_checksum_if_possible_string():
    """Test conditional checksum calculation with string."""
    data = "test data"
    checksum = calculate_checksum_if_possible(data, 'SHA256')

    # Should match unconditional version
    expected = calculate_checksum(data, 'SHA256')
    assert checksum == expected


def test_calculate_checksum_if_possible_empty_string():
    """Test conditional checksum with empty string returns empty."""
    checksum = calculate_checksum_if_possible('', 'SHA256')
    assert checksum == ""


def test_calculate_checksum_if_possible_file_object():
    """Test conditional checksum with file object returns empty."""
    import io
    file_obj = io.BytesIO(b"test data")
    checksum = calculate_checksum_if_possible(file_obj, 'SHA256')
    assert checksum == ""


def test_calculate_checksum_if_possible_other_types():
    """Test conditional checksum with other types returns empty."""
    assert calculate_checksum_if_possible(123, 'SHA256') == ""
    assert calculate_checksum_if_possible(None, 'SHA256') == ""
    assert calculate_checksum_if_possible(['list'], 'SHA256') == ""


def test_sha256_hex_to_base64():
    """Test conversion from hex to base64 encoding."""
    # Test with known value (empty string hash)
    # SHA256("") = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
    hex_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    b64_hash = sha256_hex_to_base64(hex_hash)

    # Should match direct base64 encoding of the same bytes
    import base64
    expected = base64.b64encode(bytes.fromhex(hex_hash)).decode('ascii')
    assert b64_hash == expected

    # Verify it's 44 characters (SHA256 base64)
    assert len(b64_hash) == 44


def test_sha256_hex_to_base64_roundtrip():
    """Test that hex->base64 conversion matches direct calculation."""
    data = b"test data"

    # Calculate SHA256 in hex format (like signature uses)
    hex_hash = sha256(data).hexdigest()

    # Convert to base64
    b64_from_hex = sha256_hex_to_base64(hex_hash)

    # Calculate SHA256 directly in base64 format (like checksums use)
    b64_direct = binascii.b2a_base64(sha256(data).digest()).strip().decode('ascii')

    # Should be identical
    assert b64_from_hex == b64_direct


def test_sha256_hex_to_base64_with_uppercase():
    """Test conversion works with uppercase hex (should work)."""
    # Same empty string hash but uppercase
    hex_hash = "E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855"
    b64_hash = sha256_hex_to_base64(hex_hash)

    # Should still produce valid base64
    assert len(b64_hash) == 44

    # Should match lowercase version
    hex_lower = hex_hash.lower()
    b64_lower = sha256_hex_to_base64(hex_lower)
    assert b64_hash == b64_lower


def test_checksum_algorithms_consistency():
    """Test that all algorithms produce consistent results."""
    data = b"The quick brown fox jumps over the lazy dog"

    # SHA256
    sha256_checksum = calculate_checksum(data, 'SHA256')
    assert len(sha256_checksum) == 44

    # SHA1
    sha1_checksum = calculate_checksum(data, 'SHA1')
    assert len(sha1_checksum) == 28

    # MD5
    md5_checksum = calculate_checksum(data, 'MD5')
    assert len(md5_checksum) == 24

    # All should be different
    assert sha256_checksum != sha1_checksum
    assert sha256_checksum != md5_checksum
    assert sha1_checksum != md5_checksum

    # All should be valid base64
    import base64
    base64.b64decode(sha256_checksum)
    base64.b64decode(sha1_checksum)
    base64.b64decode(md5_checksum)


def test_s3_request_inner_with_provided_sha256():
    """Test _s3_request_inner uses provided sha256_hint."""
    import unittest.mock as mock
    import s3lib

    # Create connection
    conn = s3lib.Connection("test_key", b"test_secret")
    conn._connect()

    # Pre-calculate SHA256 digest as bytes
    test_data = b"test payload data"
    sha256_hint = sha256(test_data).digest()
    expected_hex = sha256_hint.hex()

    # Mock the HTTP connection to intercept the request
    mock_http_conn = mock.Mock()
    mock_response = mock.Mock()
    mock_response.status = 200
    mock_response.getheaders.return_value = []
    mock_response.read.return_value = b""
    mock_response.isclosed.return_value = True
    mock_http_conn.getresponse.return_value = mock_response
    conn.conn = mock_http_conn

    # Make request with provided hint (bytes)
    try:
        conn._s3_request_inner("PUT", "bucket", "key", {}, {}, test_data,
                              sha256_hint=sha256_hint)
    except Exception:
        pass  # We expect this might fail due to mocking, but we can check the call

    # Verify request was made with correct hash in headers
    assert mock_http_conn.request.called
    call_args = mock_http_conn.request.call_args
    headers = call_args[0][3]  # Fourth positional arg is headers

    # Check that x-amz-content-sha256 header matches our hint (converted to hex)
    assert headers.get("x-amz-content-sha256") == expected_hex


def test_s3_request_inner_calculates_sha256_when_not_provided():
    """Test _s3_request_inner calculates SHA256 when not provided."""
    import unittest.mock as mock
    import s3lib

    # Create connection
    conn = s3lib.Connection("test_key", b"test_secret")
    conn._connect()

    # Test data
    test_data = b"test payload data"
    expected_hash_hex = sha256(test_data).hexdigest()

    # Mock the HTTP connection
    mock_http_conn = mock.Mock()
    mock_response = mock.Mock()
    mock_response.status = 200
    mock_response.getheaders.return_value = []
    mock_response.read.return_value = b""
    mock_response.isclosed.return_value = True
    mock_http_conn.getresponse.return_value = mock_response
    conn.conn = mock_http_conn

    # Make request WITHOUT providing hash
    try:
        conn._s3_request_inner("PUT", "bucket", "key", {}, {}, test_data)
    except Exception:
        pass

    # Verify hash was calculated correctly
    assert mock_http_conn.request.called
    call_args = mock_http_conn.request.call_args
    headers = call_args[0][3]

    assert headers.get("x-amz-content-sha256") == expected_hash_hex


def test_s3_request_inner_unsigned_payload_for_streams():
    """Test _s3_request_inner uses UNSIGNED-PAYLOAD for file objects."""
    import unittest.mock as mock
    import s3lib
    import io

    # Create connection
    conn = s3lib.Connection("test_key", b"test_secret")
    conn._connect()

    # File-like object
    stream = io.BytesIO(b"stream data")

    # Mock the HTTP connection
    mock_http_conn = mock.Mock()
    mock_response = mock.Mock()
    mock_response.status = 200
    mock_response.getheaders.return_value = []
    mock_response.read.return_value = b""
    mock_response.isclosed.return_value = True
    mock_http_conn.getresponse.return_value = mock_response
    conn.conn = mock_http_conn

    # Make request with stream
    try:
        conn._s3_request_inner("PUT", "bucket", "key", {}, {}, stream)
    except Exception:
        pass

    # Verify UNSIGNED-PAYLOAD is used
    assert mock_http_conn.request.called
    call_args = mock_http_conn.request.call_args
    headers = call_args[0][3]

    assert headers.get("x-amz-content-sha256") == "UNSIGNED-PAYLOAD"


def test_s3_request_inner_with_provided_md5():
    """Test _s3_request_inner uses provided md5_hint."""
    import unittest.mock as mock
    import s3lib

    # Create connection
    conn = s3lib.Connection("test_key", b"test_secret")
    conn._connect()

    # Pre-calculate MD5 digest as bytes
    test_data = b"test payload data"
    md5_hint = md5(test_data).digest()
    expected_b64 = binascii.b2a_base64(md5_hint).strip().decode('ascii')

    # Mock the HTTP connection
    mock_http_conn = mock.Mock()
    mock_response = mock.Mock()
    mock_response.status = 200
    mock_response.getheaders.return_value = []
    mock_response.read.return_value = b""
    mock_response.isclosed.return_value = True
    mock_http_conn.getresponse.return_value = mock_response
    conn.conn = mock_http_conn

    # Make request with provided MD5 hint (bytes)
    try:
        conn._s3_request_inner("PUT", "bucket", "key", {}, {}, test_data,
                              md5_hint=md5_hint)
    except Exception:
        pass

    # Verify Content-MD5 header was set correctly
    assert mock_http_conn.request.called
    call_args = mock_http_conn.request.call_args
    headers = call_args[0][3]

    # Check that Content-MD5 header matches our hint (converted to base64)
    assert headers.get("Content-MD5") == expected_b64


def test_put_object_with_sha256_checksum_auto_calc():
    """Test put_object with SHA256 checksum auto-calculation."""
    import unittest.mock as mock
    import s3lib

    # Create connection
    conn = s3lib.Connection("test_key", b"test_secret")
    conn._connect()

    test_data = b"test data"
    expected_checksum = calculate_checksum(test_data, 'SHA256')

    # Mock the HTTP connection
    mock_http_conn = mock.Mock()
    mock_response = mock.Mock()
    mock_response.status = 200
    mock_response.getheaders.return_value = []
    mock_response.read.return_value = b""
    mock_response.isclosed.return_value = True
    mock_http_conn.getresponse.return_value = mock_response
    conn.conn = mock_http_conn

    # Call put_object with checksum_algorithm
    try:
        conn.put_object("bucket", "key", test_data, checksum_algorithm='SHA256')
    except Exception:
        pass

    # Verify checksum headers were added
    assert mock_http_conn.request.called
    call_args = mock_http_conn.request.call_args
    headers = call_args[0][3]

    assert headers.get('x-amz-checksum-algorithm') == 'SHA256'
    assert headers.get('x-amz-checksum-sha256') == expected_checksum


def test_put_object_with_provided_sha256_hint():
    """Test put_object with user-provided SHA256 hint (reuses for signature)."""
    import unittest.mock as mock
    import s3lib

    # Create connection
    conn = s3lib.Connection("test_key", b"test_secret")
    conn._connect()

    test_data = b"test data"
    sha256_hint = sha256(test_data).digest()
    expected_checksum = binascii.b2a_base64(sha256_hint).strip().decode('ascii')
    expected_hash = sha256_hint.hex()

    # Mock the HTTP connection
    mock_http_conn = mock.Mock()
    mock_response = mock.Mock()
    mock_response.status = 200
    mock_response.getheaders.return_value = []
    mock_response.read.return_value = b""
    mock_response.isclosed.return_value = True
    mock_http_conn.getresponse.return_value = mock_response
    conn.conn = mock_http_conn

    # Call put_object with hint and checksum_algorithm
    try:
        conn.put_object("bucket", "key", test_data,
                       sha256_hint=sha256_hint,
                       checksum_algorithm='SHA256')
    except Exception:
        pass

    # Verify both checksum and signature headers
    assert mock_http_conn.request.called
    call_args = mock_http_conn.request.call_args
    headers = call_args[0][3]

    # Checksum headers (base64)
    assert headers.get('x-amz-checksum-algorithm') == 'SHA256'
    assert headers.get('x-amz-checksum-sha256') == expected_checksum
    # Signature header (hex)
    assert headers.get('x-amz-content-sha256') == expected_hash


def test_put_object_with_sha1_checksum():
    """Test put_object with SHA1 checksum."""
    import unittest.mock as mock
    import s3lib

    # Create connection
    conn = s3lib.Connection("test_key", b"test_secret")
    conn._connect()

    test_data = b"test data"
    expected_checksum = calculate_checksum(test_data, 'SHA1')

    # Mock the HTTP connection
    mock_http_conn = mock.Mock()
    mock_response = mock.Mock()
    mock_response.status = 200
    mock_response.getheaders.return_value = []
    mock_response.read.return_value = b""
    mock_response.isclosed.return_value = True
    mock_http_conn.getresponse.return_value = mock_response
    conn.conn = mock_http_conn

    # Call put_object with SHA1
    try:
        conn.put_object("bucket", "key", test_data, checksum_algorithm='SHA1')
    except Exception:
        pass

    # Verify SHA1 checksum headers
    assert mock_http_conn.request.called
    call_args = mock_http_conn.request.call_args
    headers = call_args[0][3]

    assert headers.get('x-amz-checksum-algorithm') == 'SHA1'
    assert headers.get('x-amz-checksum-sha1') == expected_checksum


def test_put_object_with_md5_checksum():
    """Test put_object with MD5 checksum."""
    import unittest.mock as mock
    import s3lib

    # Create connection
    conn = s3lib.Connection("test_key", b"test_secret")
    conn._connect()

    test_data = b"test data"
    expected_checksum = calculate_checksum(test_data, 'MD5')

    # Mock the HTTP connection
    mock_http_conn = mock.Mock()
    mock_response = mock.Mock()
    mock_response.status = 200
    mock_response.getheaders.return_value = []
    mock_response.read.return_value = b""
    mock_response.isclosed.return_value = True
    mock_http_conn.getresponse.return_value = mock_response
    conn.conn = mock_http_conn

    # Call put_object with MD5
    try:
        conn.put_object("bucket", "key", test_data, checksum_algorithm='MD5')
    except Exception:
        pass

    # Verify MD5 checksum headers
    assert mock_http_conn.request.called
    call_args = mock_http_conn.request.call_args
    headers = call_args[0][3]

    assert headers.get('x-amz-checksum-algorithm') == 'MD5'
    assert headers.get('x-amz-checksum-md5') == expected_checksum


def test_put_object_with_if_none_match():
    """Test put_object with If-None-Match (create-only)."""
    import unittest.mock as mock
    import s3lib

    # Create connection
    conn = s3lib.Connection("test_key", b"test_secret")
    conn._connect()

    # Mock the HTTP connection
    mock_http_conn = mock.Mock()
    mock_response = mock.Mock()
    mock_response.status = 200
    mock_response.getheaders.return_value = []
    mock_response.read.return_value = b""
    mock_response.isclosed.return_value = True
    mock_http_conn.getresponse.return_value = mock_response
    conn.conn = mock_http_conn

    # Call put_object with if_none_match
    try:
        conn.put_object("bucket", "key", b"test data", if_none_match=True)
    except Exception:
        pass

    # Verify If-None-Match header
    assert mock_http_conn.request.called
    call_args = mock_http_conn.request.call_args
    headers = call_args[0][3]

    assert headers.get('If-None-Match') == '*'


def test_put_object_with_if_match():
    """Test put_object with If-Match (optimistic locking)."""
    import unittest.mock as mock
    import s3lib

    # Create connection
    conn = s3lib.Connection("test_key", b"test_secret")
    conn._connect()

    # Mock the HTTP connection
    mock_http_conn = mock.Mock()
    mock_response = mock.Mock()
    mock_response.status = 200
    mock_response.getheaders.return_value = []
    mock_response.read.return_value = b""
    mock_response.isclosed.return_value = True
    mock_http_conn.getresponse.return_value = mock_response
    conn.conn = mock_http_conn

    # Call put_object with if_match
    test_etag = '"abc123"'
    try:
        conn.put_object("bucket", "key", b"test data", if_match=test_etag)
    except Exception:
        pass

    # Verify If-Match header
    assert mock_http_conn.request.called
    call_args = mock_http_conn.request.call_args
    headers = call_args[0][3]

    assert headers.get('If-Match') == test_etag


def test_put_object_combined_checksum_and_conditional():
    """Test put_object with both checksum and conditional headers."""
    import unittest.mock as mock
    import s3lib

    # Create connection
    conn = s3lib.Connection("test_key", b"test_secret")
    conn._connect()

    test_data = b"test data"
    expected_checksum = calculate_checksum(test_data, 'SHA256')

    # Mock the HTTP connection
    mock_http_conn = mock.Mock()
    mock_response = mock.Mock()
    mock_response.status = 200
    mock_response.getheaders.return_value = []
    mock_response.read.return_value = b""
    mock_response.isclosed.return_value = True
    mock_http_conn.getresponse.return_value = mock_response
    conn.conn = mock_http_conn

    # Call put_object with both checksum and if_none_match
    try:
        conn.put_object("bucket", "key", test_data,
                       checksum_algorithm='SHA256',
                       if_none_match=True)
    except Exception:
        pass

    # Verify both headers
    assert mock_http_conn.request.called
    call_args = mock_http_conn.request.call_args
    headers = call_args[0][3]

    assert headers.get('x-amz-checksum-algorithm') == 'SHA256'
    assert headers.get('x-amz-checksum-sha256') == expected_checksum
    assert headers.get('If-None-Match') == '*'


def test_put_object_checksum_error_for_streaming():
    """Test put_object raises error for checksum auto-calc on streaming data."""
    import unittest.mock as mock
    import s3lib
    import io

    # Create connection
    conn = s3lib.Connection("test_key", b"test_secret")
    conn._connect()

    # File-like object
    stream = io.BytesIO(b"stream data")

    # Should raise ValueError
    with pytest.raises(ValueError, match="Cannot auto-calculate"):
        conn.put_object("bucket", "key", stream, checksum_algorithm='SHA256')
