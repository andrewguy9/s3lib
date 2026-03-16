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

    # Pre-calculate SHA256 digest as bytes
    test_data = b"test payload data"
    sha256_hint = sha256(test_data).digest()
    expected_hex = sha256_hint.hex()

    with s3lib.Connection("test_key", b"test_secret") as conn:
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

    # Test data
    test_data = b"test payload data"
    expected_hash_hex = sha256(test_data).hexdigest()

    with s3lib.Connection("test_key", b"test_secret") as conn:
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

    # File-like object
    stream = io.BytesIO(b"stream data")

    with s3lib.Connection("test_key", b"test_secret") as conn:
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

    # Pre-calculate MD5 digest as bytes
    test_data = b"test payload data"
    md5_hint = md5(test_data).digest()
    expected_b64 = binascii.b2a_base64(md5_hint).strip().decode('ascii')

    with s3lib.Connection("test_key", b"test_secret") as conn:
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


def _creds():
    """Load AWS credentials or skip test."""
    try:
        from s3lib.ui import load_creds
        return load_creds(None)
    except Exception:
        pytest.skip("No AWS credentials available")


BUCKET = 's3libtestbucket'


def test_put_object_with_sha256_checksum_auto_calc():
    """PUT with SHA256 checksum is accepted by S3."""
    import s3lib
    access_id, secret_key = _creds()
    test_data = b"checksum test sha256"
    key = 'test_checksum_sha256'
    with s3lib.Connection(access_id, secret_key) as conn:
        conn.put_object(BUCKET, key, test_data, checksum_algorithm='SHA256')
        resp = conn.get_object(BUCKET, key)
        assert resp.read() == test_data
        conn.delete_object(BUCKET, key)


def test_put_object_with_sha1_checksum():
    """PUT with SHA1 checksum is accepted by S3."""
    import s3lib
    access_id, secret_key = _creds()
    test_data = b"checksum test sha1"
    key = 'test_checksum_sha1'
    with s3lib.Connection(access_id, secret_key) as conn:
        conn.put_object(BUCKET, key, test_data, checksum_algorithm='SHA1')
        resp = conn.get_object(BUCKET, key)
        assert resp.read() == test_data
        conn.delete_object(BUCKET, key)


def test_put_object_with_md5_hint():
    """PUT with pre-calculated MD5 hint sends Content-MD5 header and succeeds."""
    import s3lib
    access_id, secret_key = _creds()
    test_data = b"checksum test md5"
    key = 'test_checksum_md5'
    md5_hint = md5(test_data).digest()
    with s3lib.Connection(access_id, secret_key) as conn:
        conn._s3_put_request(BUCKET, key, test_data, md5_hint=md5_hint)
        resp = conn.get_object(BUCKET, key)
        assert resp.read() == test_data
        conn.delete_object(BUCKET, key)


def test_put_object_with_provided_sha256_hint():
    """PUT with pre-calculated SHA256 hint succeeds."""
    import s3lib
    access_id, secret_key = _creds()
    test_data = b"checksum test hint"
    key = 'test_checksum_hint'
    sha256_hint = sha256(test_data).digest()
    with s3lib.Connection(access_id, secret_key) as conn:
        conn.put_object(BUCKET, key, test_data,
                        sha256_hint=sha256_hint,
                        checksum_algorithm='SHA256')
        resp = conn.get_object(BUCKET, key)
        assert resp.read() == test_data
        conn.delete_object(BUCKET, key)


def test_put_object_with_if_none_match():
    """PUT with If-None-Match=* creates object; second PUT raises on 412."""
    import s3lib
    access_id, secret_key = _creds()
    key = 'test_if_none_match'
    with s3lib.Connection(access_id, secret_key) as conn:
        # Ensure object doesn't exist
        try:
            conn.delete_object(BUCKET, key)
        except Exception:
            pass

        # First PUT should succeed
        conn.put_object(BUCKET, key, b"first", if_none_match=True)

        # Second PUT with if_none_match raises since object already exists (412)
        with pytest.raises(ValueError, match="412"):
            conn.put_object(BUCKET, key, b"second", if_none_match=True)

        conn.delete_object(BUCKET, key)


def test_put_object_with_if_match():
    """PUT with If-Match on correct ETag succeeds; wrong ETag raises on 412."""
    import s3lib
    access_id, secret_key = _creds()
    key = 'test_if_match'
    with s3lib.Connection(access_id, secret_key) as conn:
        conn.put_object(BUCKET, key, b"original")

        # Get the ETag
        resp = conn.get_object(BUCKET, key)
        etag = dict(resp.getheaders()).get('ETag', '').strip('"')
        resp.read()

        # PUT with correct ETag should succeed
        conn.put_object(BUCKET, key, b"updated", if_match=etag)

        # PUT with wrong ETag raises (412)
        with pytest.raises(ValueError, match="412"):
            conn.put_object(BUCKET, key, b"rejected", if_match='wrongetag')

        conn.delete_object(BUCKET, key)


def test_put_object_combined_checksum_and_conditional():
    """PUT with both SHA256 checksum and If-None-Match works end-to-end."""
    import s3lib
    access_id, secret_key = _creds()
    key = 'test_checksum_and_conditional'
    test_data = b"combined test"
    with s3lib.Connection(access_id, secret_key) as conn:
        try:
            conn.delete_object(BUCKET, key)
        except Exception:
            pass

        conn.put_object(BUCKET, key, test_data,
                        checksum_algorithm='SHA256',
                        if_none_match=True)
        resp = conn.get_object(BUCKET, key)
        assert resp.read() == test_data
        conn.delete_object(BUCKET, key)


def test_get_object_with_if_match():
    """GET with If-Match on correct ETag returns 200; wrong ETag returns 412."""
    import s3lib
    access_id, secret_key = _creds()
    key = 'test_get_if_match'
    test_data = b"if match test data"
    with s3lib.Connection(access_id, secret_key) as conn:
        conn.put_object(BUCKET, key, test_data)

        # Get ETag from a plain GET
        resp = conn.get_object(BUCKET, key)
        etag = dict(resp.getheaders()).get('ETag', '').strip('"')
        resp.read()

        # GET with correct ETag should return data
        resp = conn.get_object(BUCKET, key, if_match=etag)
        assert resp.read() == test_data

        # GET with wrong ETag returns None (412)
        stream, headers = conn.get_object2(BUCKET, key, if_match='wrongetag')
        assert stream is None

        conn.delete_object(BUCKET, key)


def test_get_object_with_if_none_match():
    """GET with If-None-Match on matching ETag returns None (304)."""
    import s3lib
    access_id, secret_key = _creds()
    key = 'test_get_if_none_match'
    test_data = b"if none match test"
    with s3lib.Connection(access_id, secret_key) as conn:
        conn.put_object(BUCKET, key, test_data)

        # Get ETag
        resp = conn.get_object(BUCKET, key)
        etag = dict(resp.getheaders()).get('ETag', '').strip('"')
        resp.read()

        # GET with matching ETag returns None (304 not modified)
        stream, headers = conn.get_object2(BUCKET, key, if_none_match=etag)
        assert stream is None

        # GET with non-matching ETag returns data
        resp = conn.get_object(BUCKET, key, if_none_match='different')
        assert resp.read() == test_data

        conn.delete_object(BUCKET, key)


def test_put_object_checksum_error_for_streaming():
    """Test put_object raises error for checksum calc on streaming data when explicitly requested."""
    import unittest.mock as mock
    import s3lib
    import io

    # File-like object
    stream = io.BytesIO(b"stream data")

    with s3lib.Connection("test_key", b"test_secret") as conn:
        # Should raise ValueError when user explicitly requests checksum
        with pytest.raises(ValueError, match="Cannot calculate"):
            conn.put_object("bucket", "key", stream, checksum_algorithm='SHA256')
