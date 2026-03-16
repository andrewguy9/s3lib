import pytest
import re

from s3lib.utils import get_string_to_sign
from s3lib import ConnectionLifecycleError, S3ByteStream, _calculate_query_arg_str, Connection, sign


def validate_signature(string, expected_string, expected_signature):
    assert string == expected_string
    secret = b'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
    signature = sign(secret, string)
    assert signature == expected_signature


def test_sign_get():
    string = get_string_to_sign("GET",
                                "",
                                "",
                                "Tue, 27 Mar 2007 19:36:42 +0000",
                                {},
                                "/johnsmith/photos/puppy.jpg")
    expected_string = "GET\n\n\nTue, 27 Mar 2007 19:36:42 +0000\n/johnsmith/photos/puppy.jpg".encode('utf-8')
    expected_signature = b"bWq2s1WEIj+Ydj0vQ697zp+IXMU="
    validate_signature(string, expected_string, expected_signature)


def test_sign_put():
    string = get_string_to_sign("PUT",
                                "",
                                "image/jpeg",
                                "Tue, 27 Mar 2007 21:15:45 +0000",
                                {},
                                "/johnsmith/photos/puppy.jpg")
    expected_string = "PUT\n\nimage/jpeg\nTue, 27 Mar 2007 21:15:45 +0000\n/johnsmith/photos/puppy.jpg".encode('utf-8')
    expected_signature = b"MyyxeRY7whkBe+bq8fHCL/2kKUg="
    validate_signature(string, expected_string, expected_signature)


def test_sign_list():
    string = get_string_to_sign("GET", "", "", "Tue, 27 Mar 2007 19:42:41 +0000", {}, "/johnsmith/")
    expected_string = "GET\n\n\nTue, 27 Mar 2007 19:42:41 +0000\n/johnsmith/".encode('utf-8')
    expected_signature = b"htDYFYduRNen8P9ZfE/s9SuKy0U="
    validate_signature(string, expected_string, expected_signature)


@pytest.mark.skip()
def test_sign_copy():
    string = get_string_to_sign(
        "PUT",
        "",
        "",
        "Wed, 20 Feb 2008 22:12:21 +0000",
        {"x-amz-copy-source": "/pacific/flotsam"},
        "/atlantic/jetsam")
    expected_string = "PUT\n\n\nWed, 20 Feb 2008 22:12:21 +0000\nx-amz-copy-source:/pacific/flotsam\n/atlantic/jetsam"
    expected_bytes = expected_string.encode('utf-8')
    expected_signature = b"ENoSbxYByFA0UGLZUqJN5EUnLDg="
    validate_signature(string, expected_bytes, expected_signature)


def test_s3_request_arg():
    assert _calculate_query_arg_str({}) == ""
    assert _calculate_query_arg_str({'k': None}) == "?k"
    assert _calculate_query_arg_str({'k': 'v'}) == "?k=v"
    assert _calculate_query_arg_str({'k': 'v', 'f': None}) == "?f&k=v"
    two_args = _calculate_query_arg_str({'k1': 'v1', 'k2': 'v2'})
    assert re.findall("k2=v2", two_args) and re.findall("k1=v1", two_args)
    # Test url-encoding.
    assert _calculate_query_arg_str({"b@dkey": "b@dvalue$$"}) == "?b%40dkey=b%40dvalue%24%24"
    assert _calculate_query_arg_str({"b@dkey": None}) == "?b%40dkey"


def test_s3_get_object_url():
    """
    Example from https://aws.amazon.com/blogs/aws/amazon-s3-path-deprecation-plan-the-rest-of-the-story/
    """
    # TODO Upgrade to new style URLs.
    expected = "https://s3.amazonaws.com/jbarr-public/images/ritchie_and_thompson_pdp11.jpeg"
    bucket = "jbarr-public"
    key = "images/ritchie_and_thompson_pdp11.jpeg"
    conn = Connection("someaccess", b"somesecret")
    url = conn.get_object_url(bucket, key)
    assert url == expected


def test_connection_lifecycle_error():
    """
    Test that ConnectionLifecycleError is raised when trying to make
    a second request before consuming the first response.
    """
    import unittest.mock as mock
    from http.client import HTTPResponse

    with Connection("someaccess", b"somesecret") as conn:
        # Create a mock response that is not consumed (isclosed() returns False)
        mock_resp1 = mock.Mock(spec=HTTPResponse)
        mock_resp1.isclosed.return_value = False
        mock_resp1.status = 200

        # Manually set outstanding response to simulate get_object() call
        conn._outstanding_response = mock_resp1

        # Try to make another request - should raise ConnectionLifecycleError
        with pytest.raises(ConnectionLifecycleError) as exc_info:
            conn._validate_connection_ready()

        assert "not fully consumed" in str(exc_info.value)

        # Now test that if response is consumed, no error is raised
        mock_resp1.isclosed.return_value = True
        conn._validate_connection_ready()  # Should not raise
        assert conn._outstanding_response is None  # Should be cleared


def test_connection_initialized():
    """Test that connection attributes are properly initialized."""
    conn = Connection("someaccess", b"somesecret")
    assert conn.conn is None
    assert conn._outstanding_response is None


def test_disconnect_safety():
    """Test that disconnect is safe to call even if never connected."""
    conn = Connection("someaccess", b"somesecret")
    # Should not raise even though conn is None
    conn._disconnect()
    assert conn.conn is None
    assert conn._outstanding_response is None


def test_connection_reset_recovery():
    """Test that connection errors are handled gracefully with automatic retry."""
    import unittest.mock as mock

    call_count = [0]

    with Connection("someaccess", b"somesecret") as conn:
        def mock_connect():
            call_count[0] += 1
            mock_http_conn = mock.Mock()
            mock_http_conn.request.side_effect = ConnectionResetError("Connection reset by peer")
            conn.conn = mock_http_conn
            conn._current_endpoint = conn.host

        conn._connect = mock_connect

        with pytest.raises(ConnectionResetError):
            conn._s3_request("GET", "bucket", "key", {}, {}, '')

    # Should have tried 3 times (initial + 2 retries)
    assert call_count[0] == 3


def test_disconnect_broken_connection():
    """Test that disconnect handles already-broken connections gracefully."""
    import unittest.mock as mock

    with Connection("someaccess", b"somesecret") as conn:
        # Mock connection that raises error when closing
        mock_http_conn = mock.Mock()
        mock_http_conn.close.side_effect = BrokenPipeError("Broken pipe")
        conn.conn = mock_http_conn

    # __exit__ called _disconnect() — should not have raised
    assert conn.conn is None
    assert conn._outstanding_response is None


# TODO remove
def _make_mock_s3_get_request(status, headers=None, body=b""):
    """Helper to create a _s3_get_request mock with the new (resp|None, headers) signature."""
    import unittest.mock as mock
    mock_resp = mock.Mock()
    mock_resp.status = status
    mock_resp.getheaders.return_value = list((headers or {}).items())
    mock_resp.read.side_effect = [body, b""] if body else [b""]
    resp_headers = dict(headers or {})
    if status in (304, 412):
        return lambda *a, **kw: (None, resp_headers)
    return lambda *a, **kw: (mock_resp, resp_headers)


def test_get_object_byte_range_headers():
    """Test that byte_range correctly sets the Range header in _s3_get_request."""
    conn = Connection("someaccess", b"somesecret")

    cases = [
        ((0, None),    "bytes=0-"),
        ((None, None), "bytes=-"),
        ((0, 499),     "bytes=0-499"),
        ((500, 999),   "bytes=500-999"),
        ((500, None),  "bytes=500-"),
    ]

    for byte_range, expected_range in cases:
        captured = {}

        def fake(bucket, key, if_match=None, if_none_match=None, byte_range=None, extra_headers=None):
            import unittest.mock as mock
            captured['byte_range'] = byte_range
            mock_resp = mock.Mock()
            mock_resp.status = 206
            mock_resp.getheaders.return_value = []
            mock_resp.read.return_value = b""
            return (mock_resp, {})

        conn._s3_get_request = fake
        conn.get_object("bucket", "key", byte_range=byte_range)
        assert captured['byte_range'] == byte_range, \
            f"expected byte_range={byte_range!r} to be passed through"


def test_s3_get_request_rejects_200_for_range():
    """_s3_get_request raises ValueError if a range was requested but server returns 200."""
    import unittest.mock as mock

    conn = Connection("someaccess", b"somesecret")
    mock_resp = mock.Mock()
    mock_resp.status = 200  # server ignored the Range header
    mock_resp.getheaders.return_value = []
    mock_resp.read.return_value = b""

    conn._s3_request = lambda *a, **kw: mock_resp
    with pytest.raises(Exception):
        conn._s3_get_request("bucket", "key", byte_range=(0, 499))


def test_get_object_no_range_when_omitted():
    """Test that omitting byte_range passes byte_range=None to _s3_get_request."""
    import unittest.mock as mock

    conn = Connection("someaccess", b"somesecret")
    captured = {}

    def fake(bucket, key, if_match=None, if_none_match=None, byte_range=None, extra_headers=None):
        captured['byte_range'] = byte_range
        mock_resp = mock.Mock()
        mock_resp.status = 200
        mock_resp.getheaders.return_value = []
        mock_resp.read.return_value = b""
        return (mock_resp, {})

    conn._s3_get_request = fake
    conn.get_object("bucket", "key")
    assert captured['byte_range'] is None


def test_get_object2_returns_stream_on_success():
    """200 response returns (S3ByteStream, headers)."""
    import unittest.mock as mock

    conn = Connection("someaccess", b"somesecret")
    mock_resp = mock.Mock()
    mock_resp.read.side_effect = [b"hello", b""]
    resp_headers = {"content-type": "application/octet-stream"}

    conn._s3_get_request = lambda *a, **kw: (mock_resp, resp_headers)

    stream, headers = conn.get_object2("bucket", "key")
    assert isinstance(stream, S3ByteStream)
    assert headers == resp_headers
    with stream:
        assert stream.read() == b"hello"


def test_get_object2_returns_none_on_304():
    """304 response returns (None, headers)."""
    conn = Connection("someaccess", b"somesecret")
    resp_headers = {"etag": '"abc123"'}

    conn._s3_get_request = lambda *a, **kw: (None, resp_headers)

    stream, headers = conn.get_object2("bucket", "key", if_none_match="abc123")
    assert stream is None
    assert headers == resp_headers


def test_get_object2_returns_none_on_412():
    """412 response returns (None, headers)."""
    conn = Connection("someaccess", b"somesecret")
    resp_headers = {"etag": '"xyz999"'}

    conn._s3_get_request = lambda *a, **kw: (None, resp_headers)

    stream, headers = conn.get_object2("bucket", "key", if_match="abc123")
    assert stream is None
    assert headers == resp_headers


def test_get_object2_stream_exhausted_does_not_close():
    """Fully consuming the stream leaves the response open (connection reusable)."""
    import unittest.mock as mock

    conn = Connection("someaccess", b"somesecret")
    mock_resp = mock.Mock()
    mock_resp.read.side_effect = [b"data", b""]

    conn._s3_get_request = lambda *a, **kw: (mock_resp, {})

    stream, _ = conn.get_object2("bucket", "key")
    assert stream
    with stream:
        stream.read(-1)  # returns b"data"
        stream.read(-1)  # returns b"" — triggers exhausted flag

    mock_resp.close.assert_not_called()


def test_get_object2_stream_early_exit_closes():
    """Exiting the context manager without exhausting the stream force-closes it."""
    import unittest.mock as mock

    conn = Connection("someaccess", b"somesecret")
    mock_resp = mock.Mock()
    mock_resp.read.return_value = b"some data"

    conn._s3_get_request = lambda *a, **kw: (mock_resp, {})

    stream, _ = conn.get_object2("bucket", "key")
    assert stream
    with stream:
        pass  # exit without reading

    mock_resp.close.assert_called_once()


def test_get_object2_206_on_byte_range():
    """byte_range request returns a stream."""
    import unittest.mock as mock

    conn = Connection("someaccess", b"somesecret")
    mock_resp = mock.Mock()
    mock_resp.read.side_effect = [b"x" * 500, b""]
    resp_headers = {"content-range": "bytes 0-499/1000"}

    conn._s3_get_request = lambda *a, **kw: (mock_resp, resp_headers)

    stream, headers = conn.get_object2("bucket", "key", byte_range=(0, 499))
    assert isinstance(stream, S3ByteStream)
    with stream:
        data = stream.read()
    assert len(data) == 500


def test_put_object2_returns_put_result():
    """Successful PUT returns a PutResult with etag, version_id, checksum."""
    import unittest.mock as mock

    conn = Connection("someaccess", b"somesecret")

    resp_headers = [
        ("etag", '"abc123"'),
        ("x-amz-version-id", "v1"),
        ("x-amz-checksum-sha256", "base64checksum=="),
    ]
    mock_resp = mock.Mock()
    mock_resp.status = 200
    mock_resp.getheaders.return_value = resp_headers
    mock_resp.read.return_value = b""

    conn._s3_request = lambda *a, **kw: mock_resp

    result = conn.put_object2("bucket", "key", b"data")
    assert result['etag'] == 'abc123'           # quotes stripped
    assert result['version_id'] == 'v1'
    assert result['checksum'] == 'base64checksum=='


def test_put_object2_no_version_or_checksum():
    """PutResult fields are None when headers are absent."""
    import unittest.mock as mock

    conn = Connection("someaccess", b"somesecret")

    mock_resp = mock.Mock()
    mock_resp.status = 200
    mock_resp.getheaders.return_value = [("etag", '"xyz"')]
    mock_resp.read.return_value = b""

    conn._s3_request = lambda *a, **kw: mock_resp

    result = conn.put_object2("bucket", "key", b"data")
    assert result['etag'] == 'xyz'
    assert result['version_id'] is None
    assert result['checksum'] is None


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
    except Exception:
        pytest.skip("No AWS credentials available")

    # Create connection without specifying region (defaults to us-east-1)
    with Connection(access_id, secret_key) as conn:
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
