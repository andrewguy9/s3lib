import s3lib
from s3lib import *
import pytest
import re
import os

def validate_signature(string, expected_string, expected_signature):
  assert(string == expected_string)
  secret = b'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
  signature = sign(secret, string)
  assert(signature == expected_signature)

def test_sign_get():
  string = get_string_to_sign("GET","", "", "Tue, 27 Mar 2007 19:36:42 +0000", {}, "/johnsmith/photos/puppy.jpg")
  expected_string = "GET\n\n\nTue, 27 Mar 2007 19:36:42 +0000\n/johnsmith/photos/puppy.jpg".encode('utf-8')
  expected_signature = b"bWq2s1WEIj+Ydj0vQ697zp+IXMU="
  validate_signature(string, expected_string, expected_signature)

def test_sign_put():
  string = get_string_to_sign("PUT", "", "image/jpeg", "Tue, 27 Mar 2007 21:15:45 +0000", {}, "/johnsmith/photos/puppy.jpg" )
  expected_string = "PUT\n\nimage/jpeg\nTue, 27 Mar 2007 21:15:45 +0000\n/johnsmith/photos/puppy.jpg".encode('utf-8')
  expected_signature = b"MyyxeRY7whkBe+bq8fHCL/2kKUg="
  validate_signature(string, expected_string, expected_signature)

def test_sign_list():
  string = get_string_to_sign("GET","", "", "Tue, 27 Mar 2007 19:42:41 +0000", {}, "/johnsmith/")
  expected_string = "GET\n\n\nTue, 27 Mar 2007 19:42:41 +0000\n/johnsmith/".encode('utf-8')
  expected_signature = b"htDYFYduRNen8P9ZfE/s9SuKy0U="
  validate_signature(string, expected_string, expected_signature)

@pytest.mark.skip()
def test_sign_copy():
  string = get_string_to_sign("PUT", "", "", "Wed, 20 Feb 2008 22:12:21 +0000", {"x-amz-copy-source":"/pacific/flotsam"}, "/atlantic/jetsam")
  expected_string = "PUT\n\n\nWed, 20 Feb 2008 22:12:21 +0000\nx-amz-copy-source:/pacific/flotsam\n/atlantic/jetsam".encode('utf-8')
  expected_signature = b"ENoSbxYByFA0UGLZUqJN5EUnLDg="
  validate_signature(string, expected_string, expected_signature)

def test_s3_request_arg():
  assert s3lib._calculate_query_arg_str({}) == ""
  assert s3lib._calculate_query_arg_str({'k':None}) == "?k"
  assert s3lib._calculate_query_arg_str({'k':'v'}) == "?k=v"
  assert s3lib._calculate_query_arg_str({'k':'v', 'f':None}) == "?f&k=v"
  two_args = s3lib._calculate_query_arg_str({'k1':'v1', 'k2':'v2'}) 
  assert re.findall("k2=v2", two_args) and re.findall("k1=v1", two_args)
  # Test url-encoding.
  assert s3lib._calculate_query_arg_str({"b@dkey": "b@dvalue$$"}) == "?b%40dkey=b%40dvalue%24%24"
  assert s3lib._calculate_query_arg_str({"b@dkey": None}) == "?b%40dkey"


def test_s3_get_object_url():
    """
    Example from https://aws.amazon.com/blogs/aws/amazon-s3-path-deprecation-plan-the-rest-of-the-story/
    """
    #TODO Upgrade to new style URLs.
    expected = "https://s3.amazonaws.com/jbarr-public/images/ritchie_and_thompson_pdp11.jpeg"
    bucket = "jbarr-public"
    key = "images/ritchie_and_thompson_pdp11.jpeg"
    conn = s3lib.Connection("someaccess", b"somesecret")
    url = conn.get_object_url(bucket, key)
    assert url == expected

def test_connection_lifecycle_error():
    """
    Test that ConnectionLifecycleError is raised when trying to make
    a second request before consuming the first response.
    """
    import unittest.mock as mock
    from http.client import HTTPResponse

    # Create a connection and mock the underlying HTTP connection
    conn = s3lib.Connection("someaccess", b"somesecret")
    conn._connect()

    # Create a mock response that is not consumed (isclosed() returns False)
    mock_resp1 = mock.Mock(spec=HTTPResponse)
    mock_resp1.isclosed.return_value = False
    mock_resp1.status = 200

    # Manually set outstanding response to simulate get_object() call
    conn._outstanding_response = mock_resp1

    # Try to make another request - should raise ConnectionLifecycleError
    with pytest.raises(s3lib.ConnectionLifecycleError) as exc_info:
        conn._validate_connection_ready()

    assert "not fully consumed" in str(exc_info.value)

    # Now test that if response is consumed, no error is raised
    mock_resp1.isclosed.return_value = True
    conn._validate_connection_ready()  # Should not raise
    assert conn._outstanding_response is None  # Should be cleared

def test_connection_initialized():
    """Test that connection attributes are properly initialized."""
    conn = s3lib.Connection("someaccess", b"somesecret")
    assert conn.conn is None
    assert conn._outstanding_response is None

def test_disconnect_safety():
    """Test that disconnect is safe to call even if never connected."""
    conn = s3lib.Connection("someaccess", b"somesecret")
    # Should not raise even though conn is None
    conn._disconnect()
    assert conn.conn is None
    assert conn._outstanding_response is None

def test_connection_reset_recovery():
    """Test that connection errors are handled gracefully with automatic retry."""
    import unittest.mock as mock

    conn = s3lib.Connection("someaccess", b"somesecret")

    # Mock _connect to always set up a failing connection
    original_connect = conn._connect
    call_count = [0]

    def mock_connect():
        call_count[0] += 1
        mock_http_conn = mock.Mock()
        mock_http_conn.request.side_effect = ConnectionResetError("Connection reset by peer")
        conn.conn = mock_http_conn
        conn._current_endpoint = conn.host

    conn._connect = mock_connect

    # Try to make a request - should retry and then re-raise after exhausting retries
    with pytest.raises(ConnectionResetError):
        conn._s3_request("GET", "bucket", "key", {}, {}, '')

    # Should have tried 3 times (initial + 2 retries)
    assert call_count[0] == 3

    # Connection should have been cleaned up
    assert conn.conn is None
    assert conn._outstanding_response is None

def test_disconnect_broken_connection():
    """Test that disconnect handles already-broken connections gracefully."""
    import unittest.mock as mock

    conn = s3lib.Connection("someaccess", b"somesecret")
    conn._connect()

    # Mock connection that raises error when closing
    mock_http_conn = mock.Mock()
    mock_http_conn.close.side_effect = BrokenPipeError("Broken pipe")
    conn.conn = mock_http_conn

    # Should not raise - should handle the error gracefully
    conn._disconnect()
    assert conn.conn is None
    assert conn._outstanding_response is None

def test_get_object_byte_range_headers():
    """Test that byte_range correctly sets the Range header."""
    import unittest.mock as mock

    conn = s3lib.Connection("someaccess", b"somesecret")

    cases = [
        ((0, None),   "bytes=0-"),
        ((None, None), "bytes=-"),
        ((0, 499),    "bytes=0-499"),
        ((500, 999),  "bytes=500-999"),
        ((500, None), "bytes=500-"),
    ]

    for byte_range, expected_range in cases:
        captured_headers = {}

        def fake_s3_get_request(bucket, key, headers):
            captured_headers.update(headers)
            mock_resp = mock.Mock()
            mock_resp.status = 206
            return mock_resp

        conn._s3_get_request = fake_s3_get_request
        conn.get_object("bucket", "key", byte_range=byte_range)
        assert captured_headers.get("Range") == expected_range, \
            f"byte_range={byte_range}: expected {expected_range!r}, got {captured_headers.get('Range')!r}"


def test_get_object_no_range_header_when_omitted():
    """Test that omitting byte_range does not add a Range header."""
    import unittest.mock as mock

    conn = s3lib.Connection("someaccess", b"somesecret")
    captured_headers = {}

    def fake_s3_get_request(bucket, key, headers):
        captured_headers.update(headers)
        mock_resp = mock.Mock()
        mock_resp.status = 200
        return mock_resp

    conn._s3_get_request = fake_s3_get_request
    conn.get_object("bucket", "key")
    assert "Range" not in captured_headers


def test_automatic_region_discovery():
    """
    Test that regions are automatically discovered from redirects.
    Tests both us-east-1 (default) and us-west-1 (requires discovery).
    """
    # Note: This test requires valid AWS credentials in environment or ~/.s3
    # It tests against real S3 buckets
    try:
        from s3lib.ui import load_creds
        (access_id, secret_key) = load_creds(None)
    except:
        pytest.skip("No AWS credentials available")

    # Create connection without specifying region (defaults to us-east-1)
    with s3lib.Connection(access_id, secret_key) as conn:
        # Test us-east-1 bucket (should work with default region)
        buckets_east = list(conn.list_bucket('s3libtestbucket'))
        assert len(buckets_east) > 0
        # Verify region is still us-east-1
        assert conn.region == 'us-east-1'

        # Test us-west-1 bucket (should auto-discover region from redirect)
        buckets_west = list(conn.list_bucket('s3libtestbucket2'))
        assert len(buckets_west) > 0
        # Verify region was discovered and updated
        assert conn.region == 'us-west-1'
        # Verify region was cached for this bucket
        assert conn._bucket_regions['s3libtestbucket2'] == 'us-west-1'

        # Test that subsequent requests to the same bucket use cached region
        buckets_west_2 = list(conn.list_bucket('s3libtestbucket2'))
        assert len(buckets_west_2) > 0
        assert conn.region == 'us-west-1'

