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
