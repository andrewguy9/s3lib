from binascii import b2a_base64
from hashlib import md5
from hashlib import sha1
from hmac import new as hmac_new
from http.client import HTTPConnection, HTTPSConnection, HTTPException, HTTPResponse, NO_CONTENT, OK, RemoteDisconnected
import ssl
from logging import basicConfig as logging_basicConfig, DEBUG, getLogger
from typing import Generator, Iterable, Optional, Tuple, TypedDict, Union
from .utils import batchify, raise_http_resp_error
from .sigv4 import sign_request_v4, hash_payload, get_timestamp
from os import environ, fstat
from urllib.parse import quote
from stat import S_ISREG, ST_SIZE
from sys import stderr
from time import time
from xml.etree.ElementTree import fromstring as parse
from xml.etree.ElementTree import Element, SubElement, tostring

from .pool import ConnectionPool, ConnectionLease  # noqa: F401

# Configure module-level logger
logger = getLogger(__name__)

# Enable debug logging via S3LIB_DEBUG environment variable
if environ.get('S3LIB_DEBUG'):
    logging_basicConfig(
        level=DEBUG,
        format='[%(asctime)s] %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        stream=stderr
    )
    logger.setLevel(DEBUG)


class PutResult(TypedDict):
    """
    Result of a successful put_object2() call.

    etag:       ETag of the stored object (use with if_match for consistency checks)
    version_id: Version ID if bucket versioning is enabled, otherwise None
    checksum:   Server-confirmed checksum value if one was sent, otherwise None
    """
    etag: str
    version_id: str | None
    checksum: str | None


class ConnectionLifecycleError(Exception):
    """Raised when attempting to reuse a connection with an unconsumed response."""
    pass


class PreconditionFailed(Exception):
    """Raised when a conditional request fails (HTTP 412 Precondition Failed).

    This is raised by put_object() when if_none_match=True and the object
    already exists, or when if_match is provided and the ETag doesn't match.
    """
    pass


class S3ByteStream:
    """
    A streaming byte source wrapping an S3 HTTP response.

    Returned by get_object2() when data is available. Supports incremental
    reading via read() and MUST be used as a context manager to ensure the
    underlying connection is cleaned up correctly.

    Connection hygiene on __exit__:
      - If the stream was fully exhausted by read(), the HTTP connection is
        already clean and remains open for reuse (keep-alive).
      - If the stream was not fully consumed (early exit), the underlying
        response is closed, triggering a reconnect on the next request.
        This avoids draining potentially large amounts of unwanted data.

    An optional on_close callback fires in __exit__ after cleanup, allowing
    callers to track stream lifetime and enforce handle discipline.

    Usage:
        stream, headers = conn.get_object2(bucket, key)
        if stream is not None:
            with stream:
                while chunk := stream.read(65536):
                    process(chunk)
    """

    def __init__(self, response: HTTPResponse, on_close=None):
        self._response = response
        self._exhausted = False
        self._on_close = on_close

    def read(self, size: int = -1) -> bytes:
        buf = self._response.read(size)
        if not buf:
            self._exhausted = True
        return buf

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self._exhausted:
            # Not fully consumed — close to avoid draining a potentially large body.
            # The connection will reconnect on the next request.
            self._response.close()
        if self._on_close:
            self._on_close()
        return False


class Connection:
    """
    An S3 connection for making requests against a single endpoint.

    Accepts connection configuration and exposes high-level Pythonic methods
    for S3 operations — no HTTP knowledge required.

    Connection is a context manager. On entry, the connection is prepared for
    use. On exit, the underlying socket is closed and all resources are
    released. Always use Connection as a context manager to ensure clean
    resource cleanup:

        with Connection(access_id, secret) as conn:
            result = conn.put_object2(bucket, key, data)
            stream, headers = conn.get_object2(bucket, key)
            with stream:
                data = stream.read()

    Connections can be reused across multiple requests. Once a request's
    response payload has been fully consumed, the connection is ready for
    the next request. Use is_ready() to check before reusing.
    """

    def __init__(
        self,
        access_id: str,
        secret: bytes,
        host: str | None = None,
        port: int | None = None,
        conn_timeout: float | None = None,
        region: str | None = None,
        use_ssl: bool = True
    ):
        """
        Initialize a new S3 connection.

        Args:
            access_id: AWS access key ID
            secret: AWS secret access key
            host: S3 endpoint hostname (optional)
            port: Port number (optional)
            conn_timeout: Connection timeout in seconds (optional, default 10)
            region: AWS region (optional)
            use_ssl: Use HTTPS if True (default), HTTP if False
        """
        assert isinstance(secret, bytes)
        self.access_id = access_id
        self.secret = secret
        self.use_ssl = use_ssl
        self.port = port or (443 if use_ssl else 80)
        self.host = host or "s3.amazonaws.com"
        self.conn_timeout = conn_timeout if conn_timeout is not None else 4
        self.conn: Optional[Union[HTTPConnection, HTTPSConnection]] = None
        self._entered = False
        self._outstanding_response: Optional[HTTPResponse] = None
        self._server_requested_close = False
        # Default region: env var > us-east-1 (matches boto3 behavior)
        self.region = region or environ.get("AWS_DEFAULT_REGION") or "us-east-1"
        self._current_endpoint: Optional[str] = None
        self._bucket_regions: dict[str, str] = {}
        # Track socket identity even after socket closes
        self._socket_identity: Optional[str] = None  # Will be set to "fd=X local_port=Y" when connected
        # Statistics counters for monitoring performance
        self._connects = 0    # Number of TCP connections established
        self._requests = 0    # Number of HTTP requests made
        self._redirects = 0   # Number of S3 redirects (301/307) encountered

    def __enter__(self):
        """Activate the connection for use. Must be called before making requests."""
        self._entered = True
        return self

    def __exit__(self, type, value, traceback):
        """
        Close the connection and release all resources.

        Raises ConnectionLifecycleError if the connection exits with an
        unconsumed response and no exception is already in flight. This catches
        leaked handles — e.g. a get_object2 stream that was never closed.
        """
        self._entered = False
        if value is None and self._outstanding_response is not None:
            if not self._is_response_consumed(self._outstanding_response):
                self._disconnect()
                raise ConnectionLifecycleError(
                    "Connection exited with unconsumed response. "
                    "Close the stream with 'with stream:' before exiting the connection."
                )
        self._disconnect()

    def stats(self):
        """
        Get connection statistics.

        Useful for monitoring performance and detecting unexpected redirects.
        S3 uses redirects for region discovery; frequent redirects indicate
        suboptimal configuration that hurts performance.

        Returns:
            dict: Connection statistics with keys:
                - connects: Number of TCP connections established
                - requests: Number of HTTP requests made
                - redirects: Number of S3 redirects (301/307) encountered
        """
        return {
            'connects': self._connects,
            'requests': self._requests,
            'redirects': self._redirects,
        }

    def is_ready(self) -> bool:
        """
        Returns True if this connection is ready to accept a new request.

        A connection is ready when it has no outstanding response payload
        waiting to be consumed. It does not need an open socket — the
        connection will be established automatically on the next request.

        Use this to check whether a connection can be reused:

            if conn.is_ready():
                stream, headers = conn.get_object2(bucket, key)
        """
        if self._outstanding_response is None:
            return True
        return self._is_response_consumed(self._outstanding_response)

    #######################
    # Interface Functions #
    #######################
    def list_buckets(self) -> Generator[str, None, None]:
        """list all buckets in account"""
        xml = self._s3_get_service_request()
        buckets = _parse_get_service_response(xml)
        yield from buckets

    def list_bucket2(
            self,
            bucket: str,
            start: str | None = None,
            prefix: str | None = None,
            batch_size: int | None = None):
        """List contents of individual bucket returning dict of all attributes."""
        continuation_token = None
        more = True
        while more:
            xml = self._s3_list_request(
                bucket, continuation_token, start, prefix, batch_size
            )
            objects, next_token = _parse_list_response(xml)
            yield from objects
            # For v2 API, use continuation token from response (not last key)
            continuation_token = next_token
            more = next_token is not None
            # After first request, start parameter is no longer used (v2 uses continuation token)
            start = None

    def list_bucket(
            self,
            bucket: str,
            start: str | None = None,
            prefix: str | None = None,
            batch_size: int | None = None):
        """List contents of individual bucket."""
        for obj in self.list_bucket2(bucket, start, prefix, batch_size):
            yield obj[LIST_BUCKET_KEY]

    def get_object(
            self,
            bucket: str,
            key: str,
            headers: dict[str, str] | None = None,
            if_match: str | None = None,
            if_none_match: str | None = None,
            byte_range: Tuple[int | None, int | None] | None = None):
        """
        Pull down bucket object by key with optional conditional checks.

        Args:
            bucket: S3 bucket name
            key: Object key
            headers: Optional dict of additional request headers
            if_match: ETag string (without quotes) - only download if current ETag matches
                      Example: 'abc123def456'
            if_none_match: ETag string (without quotes) - skip download if current ETag matches
                           Example: 'abc123def456'
            byte_range: Tuple of (start, end) byte positions (inclusive, 0-based).
                        Use None to mean "from beginning" or "to end".
                        Examples:
                          (0, None)    - entire object (same as no range)
                          (0, 499)     - first 500 bytes
                          (500, 999)   - bytes 500-999
                          (500, None)  - from byte 500 to end of object

        Returns:
            HTTPResponse object with status:
            - 200: Success, full object downloaded
            - 206: Partial Content, byte range returned
            - 304: Not Modified (if_none_match matched, object unchanged)
            - 412: Precondition Failed (if_match didn't match, object changed)

        Examples:
            # Basic download
            response = conn.get_object(bucket, key)
            data = response.read()

            # First 1024 bytes
            response = conn.get_object(bucket, key, byte_range=(0, 1023))
            data = response.read()

            # From byte 4096 to end
            response = conn.get_object(bucket, key, byte_range=(4096, None))
            data = response.read()

            # Only download if changed (caching)
            headers = conn.head_object(bucket, key)
            cached_etag = dict(headers)['etag'].strip('"')  # Remove quotes
            response = conn.get_object(bucket, key, if_none_match=cached_etag)
            if response.status == 304:
                # Use cached version
            else:
                # Download new version
                data = response.read()

            # Ensure object hasn't changed
            response = conn.get_object(bucket, key, if_match=expected_etag)
            if response.status == 412:
                # Object was modified
            else:
                data = response.read()
        """
        # TODO Want to replace with some enter, exit struct.
        response, _ = self._s3_get_request(
            bucket, key,
            if_match=if_match,
            if_none_match=if_none_match,
            byte_range=byte_range,
            extra_headers=headers,
        )
        return response

    def get_object2(
            self,
            bucket: str,
            key: str,
            if_match: str | None = None,
            if_none_match: str | None = None,
            byte_range: Tuple[int | None, int | None] | None = None,
    ) -> Tuple['S3ByteStream', dict[str, str]] | Tuple[None, dict[str, str]]:
        """
        Fetch an S3 object, returning a stream and headers.

        Unlike get_object(), this method hides all HTTP complexity. The caller
        receives either a stream to read from, or None if the server returned
        no body.

        Args:
            bucket: S3 bucket name
            key: Object key
            if_match: ETag (without quotes) — if the object's ETag no longer
                      matches, returns (None, headers) with the server's 412 response
            if_none_match: ETag (without quotes) — if the object is unchanged,
                           returns (None, headers) with the server's 304 response
            byte_range: (start, end) byte positions, inclusive, 0-based. Either
                        can be None for "from start" or "to end".

        Returns:
            (S3ByteStream, headers) — data is available. Caller MUST always use
                the stream as a context manager. If the stream is fully consumed,
                the underlying connection may be reused. If the stream is not fully
                consumed, the connection will be force-closed on exit, preventing reuse.
            (None, headers) — no body. Either:
                - 304: if_none_match matched, object unchanged
                - 412: if_match failed, object has changed

        Raises:
            ValueError: On any unexpected HTTP response status

        Examples:
            # Basic streaming download — always use as context manager
            stream, headers = conn.get_object2(bucket, key)
            with stream:
                while chunk := stream.read(65536):
                    process(chunk)

            # Caching — skip download if unchanged
            stream, headers = conn.get_object2(bucket, key, if_none_match=cached_etag)
            if stream is None:
                use_cache()  # 304 — object unchanged
            else:
                with stream:
                    data = stream.read()

            # Optimistic read — caller decides if 412 is an error
            stream, headers = conn.get_object2(bucket, key, if_match=expected_etag)
            if stream is None:
                handle_changed()  # 412 — object has changed, no data returned
            else:
                with stream:
                    data = stream.read()

            # Byte range fetch
            stream, headers = conn.get_object2(bucket, key, byte_range=(0, 1023))
            with stream:
                header_bytes = stream.read()
        """
        response, resp_headers = self._s3_get_request(
            bucket, key,
            if_match=if_match,
            if_none_match=if_none_match,
            byte_range=byte_range,
        )
        if response is None:
            return (None, resp_headers)

        def _on_close():
            self._outstanding_response = None

        return (S3ByteStream(response, on_close=_on_close), resp_headers)

    def get_object_url(self, bucket: str, key: str, proto="https") -> str:
        """get a public url for the object in the bucket."""
        return proto + "://" + self.host + "/" + bucket + "/" + key

    def head_object(self, bucket: str, key: str) -> dict[str, str]:
        # TODO head is not returning status.
        """get request metadata for key in bucket"""
        status, headers = self._s3_head_request(bucket, key)
        return headers

    def delete_object(self, bucket: str, key: str) -> Tuple[int, dict[str, str]]:
        """delete key from bucket"""
        status, headers = self._s3_delete_request(bucket, key)
        return (status, headers)

    def delete_objects(self, bucket: str, keys: Iterable[str], batch_size=1000, quiet=False):
        """delete keys from bucket"""
        for batch in batchify(batch_size, keys):
            xml = self._s3_delete_bulk_request(bucket, batch, quiet)
            results = _parse_delete_bulk_response(xml)
            for key, result in results:
                yield key, result

    def copy_object(self,
                    src_bucket: str,
                    src_key: str,
                    dst_bucket: str,
                    dst_key: str,
                    headers: dict[str, str] | None = None
                    ) -> Tuple[int, dict[str, str]]:
        """copy key from one bucket to another"""
        if headers is None:
            headers = dict()
        else:
            headers = dict(headers)
        (status, resp_headers) = self._s3_copy_request(
            src_bucket, src_key, dst_bucket, dst_key, headers
        )
        return (status, resp_headers)

    # TODO we need to add a type annotation for data.
    def put_object(
        self,
        bucket: str,
        key: str,
        data,
        headers: dict[str, str] | None = None,
        sha256_hint: bytes | None = None,
        checksum_algorithm: str | None = None,
        if_none_match: bool = False,
        if_match: str | None = None,
    ):
        """
        Push object from local to bucket with optional integrity and conditional checks.

        Args:
            bucket: S3 bucket name
            key: Object key
            data: Data to upload (str, bytes, or file object)
            headers: Optional dict of additional headers
            sha256_hint: Pre-calculated SHA256 digest as bytes (32 bytes) to skip recalculation.
                         Used for signature authentication and reused for integrity if
                         checksum_algorithm='SHA256'.
            checksum_algorithm: Algorithm for integrity check: 'SHA256', 'SHA1', 'MD5', or None.
                               Default is 'SHA256' for str/bytes data. Set to None to disable.
            if_none_match: If True, upload only succeeds if object doesn't exist (create-only).
            if_match: ETag string (without quotes) for optimistic concurrency control.
                      Example: 'abc123def456'

        Returns:
            (status, headers) tuple
            Response headers include x-amz-checksum-{algorithm} if used

        Raises:
            ValueError: If checksum auto-calculation requested but not possible (streaming data)
            PreconditionFailed: If if_none_match=True and object already exists
            PreconditionFailed: If if_match provided and ETag doesn't match
            ValueError: If checksum value doesn't match uploaded data (HTTP 400)

        Examples:
            # Basic upload with default SHA256 integrity (str/bytes only)
            conn.put_object(bucket, key, b"data")

            # Disable checksum
            conn.put_object(bucket, key, data, checksum_algorithm=None)

            # Create-only (prevent overwrites)
            conn.put_object(bucket, key, data, if_none_match=True)

            # User-provided checksum hint (farmfs: pre-computed SHA256)
            digest = sha256(blob_data).digest()
            conn.put_object(bucket, key, blob_data, sha256_hint=digest)

            # Safe overwrite with optimistic locking
            headers_resp = conn.head_object(bucket, key)
            etag = dict(headers_resp)['etag'].strip('"')  # Remove quotes
            conn.put_object(bucket, key, new_data, if_match=etag)
        """
        return self._s3_put_request(
            bucket, key, data,
            sha256_hint=sha256_hint,
            checksum_algorithm=checksum_algorithm,
            if_none_match=if_none_match,
            if_match=if_match,
            extra_headers=headers,
        )

    def put_object2(
        self,
        bucket: str,
        key: str,
        data,
        sha256_hint: bytes | None = None,
        checksum_algorithm: str | None = None,
        if_none_match: bool = False,
        if_match: str | None = None,
    ) -> 'PutResult | None':
        """
        Upload an object, returning a PutResult with fields useful for consistency checks.

        Unlike put_object(), this method extracts the meaningful fields from the
        HTTP response so the caller never has to parse headers. Conditional failures
        (412) are returned as None rather than raised as exceptions.

        Args:
            bucket: S3 bucket name
            key: Object key
            data: Data to upload (str, bytes, or file object)
            sha256_hint: Pre-calculated SHA256 digest as bytes to skip recalculation
            checksum_algorithm: 'SHA256', 'SHA1', 'MD5', or None. Defaults to 'SHA256'
                                for str/bytes data.
            if_none_match: If True, only upload if object does not already exist
            if_match: ETag (without quotes) — only upload if current ETag matches

        Returns:
            PutResult with:
                etag:       ETag of the stored object — use with if_match on future
                            get_object2/put_object2 calls for consistency checks
                version_id: Version ID if bucket versioning is enabled, otherwise None
                checksum:   Server-confirmed checksum if one was sent, otherwise None
            None if the conditional check failed (object exists when if_none_match=True,
                or ETag mismatch when if_match was provided)

        Raises:
            ValueError: If checksum calculation was requested but not possible

        Examples:
            # Basic upload
            result = conn.put_object2(bucket, key, b"data")
            print(result['etag'])

            # Write then read with consistency check
            result = conn.put_object2(bucket, key, b"data")
            stream, headers = conn.get_object2(bucket, key, if_match=result['etag'])
            if stream is None:
                raise RuntimeError("object was modified between write and read")

            # Create-only (prevent overwrites) — None means object already existed
            result = conn.put_object2(bucket, key, data, if_none_match=True)
            if result is None:
                pass  # object already exists, upload skipped

            # Optimistic locking — None means ETag changed (concurrent write)
            result = conn.put_object2(bucket, key, new_data, if_match=old_etag)
            if result is None:
                pass  # conflict, retry with fresh ETag
        """
        try:
            _, resp_headers = self._s3_put_request(
                bucket, key, data,
                sha256_hint=sha256_hint,
                checksum_algorithm=checksum_algorithm,
                if_none_match=if_none_match,
                if_match=if_match,
            )
        except PreconditionFailed:
            return None
        h = dict(resp_headers)

        # Strip surrounding quotes S3 wraps ETags in
        etag = h.get('etag', '').strip('"')

        # checksum confirmation — S3 echoes back whichever algorithm was used
        checksum = (
            h.get('x-amz-checksum-sha256') or
            h.get('x-amz-checksum-sha1') or
            h.get('x-amz-checksum-md5')
        )

        return PutResult(
            etag=etag,
            version_id=h.get('x-amz-version-id'),
            checksum=checksum,
        )

    ##########################
    # Http request Functions #
    ##########################

    def _s3_get_service_request(self) -> str:
        resp = self._s3_request("GET", None, None, {}, {}, "")
        if resp.status != OK:
            raise_http_resp_error(resp)
        data = resp.read()  # TODO HAS A PAYLOAD, MAYBE NOT BEST READ CANDIDATE.
        self._outstanding_response = None  # Response consumed
        return data

    def _s3_list_request(
        self,
        bucket: str,
        continuation_token: str | None = None,
        start_after: str | None = None,
        prefix: str | None = None,
        max_keys: int | None = None,
    ) -> str:
        """List bucket using ListObjectsV2 API."""
        args = {}
        # v2 API requires list-type=2
        args["list-type"] = "2"

        if continuation_token:
            # Subsequent requests use continuation token
            args["continuation-token"] = continuation_token
        elif start_after:
            # First request can use start-after to begin listing after a specific key
            args["start-after"] = start_after

        if prefix:
            args["prefix"] = prefix
        if max_keys:
            args["max-keys"] = str(max_keys)

        for _read_attempt in range(3):
            resp = self._s3_request("GET", bucket, None, args, {}, "")
            if resp.status != OK:
                raise_http_resp_error(resp)
            try:
                data = resp.read()
            except (ssl.SSLError, RemoteDisconnected, EOFError, ConnectionResetError, TimeoutError):
                self._disconnect()
                continue
            self._outstanding_response = None  # Response consumed
            return data
        raise ConnectionError("Failed to read list response after retries")

    def _s3_get_request(
            self,
            bucket: str,
            key: str,
            if_match: str | None = None,
            if_none_match: str | None = None,
            byte_range: Tuple[int | None, int | None] | None = None,
            extra_headers: dict[str, str] | None = None,
    ) -> Tuple[HTTPResponse | None, dict[str, str]]:
        """
        Execute a GET request against S3, handling all header construction and
        status validation.

        Returns:
            (HTTPResponse, headers) on success (200 or 206)
            (None, headers) for conditional responses (304 or 412) — body consumed internally

        Raises:
            ValueError: On unexpected HTTP status
        """
        headers: dict[str, str] = dict(extra_headers) if extra_headers else {}

        # Request checksums in response headers
        headers['x-amz-checksum-mode'] = 'ENABLED'

        # Conditional request headers — quotes required by HTTP protocol
        if if_match:
            headers['If-Match'] = f'"{if_match}"'
        if if_none_match:
            headers['If-None-Match'] = f'"{if_none_match}"'

        # Byte range header
        if byte_range is not None:
            start, end = byte_range
            start_str = "" if start is None else str(start)
            end_str = "" if end is None else str(end)
            headers['Range'] = f"bytes={start_str}-{end_str}"

        # 206 when a range was requested, 200 for a full object fetch.
        expected_success = 206 if byte_range is not None else 200

        resp = self._s3_request("GET", bucket, key, {}, headers, "")
        resp_headers = dict(resp.getheaders())

        if resp.status in (304, 412):
            # 304 Not Modified: if_none_match matched, object unchanged.
            # 412 Precondition Failed: if_match failed, object has changed.
            # Both have no body — consume to keep the connection clean.
            resp.read()
            return (None, resp_headers)

        if resp.status != expected_success:
            raise_http_resp_error(resp)

        return (resp, resp_headers)

    def _s3_head_request(self, bucket: str, key: str) -> Tuple[int, dict[str, str]]:
        # Request checksums in response headers
        headers = {'x-amz-checksum-mode': 'ENABLED'}
        resp = self._s3_request("HEAD", bucket, key, {}, headers, "")
        if resp.status != OK:
            raise_http_resp_error(resp)
        resp.read()  # NOTE: Should be zero size response. Required to reset the connection.
        self._outstanding_response = None  # Response consumed
        return (resp.status, resp.getheaders())

    def _s3_delete_request(self, bucket: str, key: str) -> Tuple[int, dict[str, str]]:
        resp = self._s3_request("DELETE", bucket, key, {}, {}, "")
        if resp.status != NO_CONTENT:
            raise_http_resp_error(resp)
        resp.read()  # NOTE: Should be zero size response. Required to reset the connection
        self._outstanding_response = None  # Response consumed
        return (resp.status, resp.getheaders())

    def _s3_delete_bulk_request(self, bucket, keys, quiet):
        content = _render_delete_bulk_content(keys, quiet)
        resp = self._s3_request("POST", bucket, None, {"delete": None}, {}, content)
        if resp.status != OK:
            raise_http_resp_error(resp)
        results = resp.read()  # TODO HAS A PAYLOAD, MAYBE NOT BEST READ CANDIDATE.
        self._outstanding_response = None  # Response consumed
        return results

    def _s3_copy_request(self, src_bucket, src_key, dst_bucket, dst_key, headers):
        headers["x-amz-copy-source"] = "/%s/%s" % (src_bucket, src_key)
        headers["x-amz-metadata-directive"] = "REPLACE"
        resp = self._s3_request("PUT", dst_bucket, dst_key, {}, headers, "")
        if resp.status != OK:
            raise_http_resp_error(resp)
        resp.read()  # NOTE: Should be zero size response. Required to reset the connection.
        self._outstanding_response = None  # Response consumed
        return (resp.status, resp.getheaders())

    def _s3_put_request(
        self,
        bucket: str,
        key: str,
        data,
        sha256_hint: bytes | None = None,
        checksum_algorithm: str | None = None,
        if_none_match: bool = False,
        if_match: str | None = None,
        extra_headers: dict[str, str] | None = None,
        md5_hint=None,
    ) -> Tuple[int, list]:
        """
        Execute a PUT request against S3, handling all header construction and
        checksum logic.

        Returns:
            (status, resp_headers)

        Raises:
            ValueError: If checksum calculation requested but not possible for streaming data
        """
        headers: dict[str, str] = dict(extra_headers) if extra_headers else {}

        # Determine which checksum algorithm to use.
        # Default to SHA256 for str/bytes — best effort, no error if not possible.
        user_requested_algo = checksum_algorithm is not None
        if checksum_algorithm is None and isinstance(data, (str, bytes)):
            checksum_algorithm = "SHA256"

        if checksum_algorithm:
            if checksum_algorithm == "SHA256" and sha256_hint is not None:
                # Reuse pre-computed digest — convert bytes to base64
                checksum_value = b2a_base64(sha256_hint).strip().decode("ascii")
            else:
                checksum_value = calculate_checksum_if_possible(data, checksum_algorithm)
                if not checksum_value:
                    if user_requested_algo:
                        raise ValueError(
                            f"Cannot calculate {checksum_algorithm} checksum for streaming data. "
                            "Provide sha256_hint or use str/bytes data."
                        )
                    else:
                        checksum_algorithm = None

                # Derive sha256_hint from data so _s3_request can reuse it for signing
                if checksum_value and checksum_algorithm == "SHA256" and sha256_hint is None:
                    if isinstance(data, (str, bytes)):
                        from hashlib import sha256 as _sha256
                        data_bytes = data.encode("utf-8") if isinstance(data, str) else data
                        sha256_hint = _sha256(data_bytes).digest()

            if checksum_algorithm and checksum_value:
                headers["x-amz-checksum-algorithm"] = checksum_algorithm
                headers[f"x-amz-checksum-{checksum_algorithm.lower()}"] = checksum_value

        # Conditional request headers
        if if_none_match:
            headers["If-None-Match"] = "*"
        if if_match:
            headers["If-Match"] = f'"{if_match}"'

        # Determine content-length
        if isinstance(data, (str, bytes)):
            content_length = len(data)
        elif hasattr(data, "seek") and hasattr(data, "tell"):
            # Seekable stream (e.g. BytesIO, open file) — measure without reading
            pos = data.tell()
            data.seek(0, 2)  # seek to end
            content_length = data.tell() - pos
            data.seek(pos)   # seek back to original position
        elif hasattr(data, "fileno"):
            try:
                fileno = data.fileno()
                filestat = fstat(fileno)
                if S_ISREG(filestat.st_mode):
                    content_length = filestat[ST_SIZE]
                else:
                    # Special file (e.g. stdin/pipe) — read into buffer to get length.
                    # TODO we are reading the ENTIRE STREAM we should not do that.
                    #      If we have to have a content length, we can stream into a
                    #      temp file and use that for buffering/checksumming.
                    data = data.read()
                    content_length = len(data)
            except OSError:
                # fileno() exists but raised (e.g. non-blocking pipe) — read into buffer
                data = data.read()
                content_length = len(data)
        else:
            raise TypeError(f"Cannot determine content-length of type {type(data)}")
        headers["content-length"] = str(content_length)

        resp = self._s3_request(
            "PUT", bucket, key, {}, headers, data,
            sha256_hint=sha256_hint,
            md5_hint=md5_hint,
        )
        if resp.status == 412:
            resp.read()  # consume response body before raising
            self._outstanding_response = None
            raise PreconditionFailed(
                "Precondition failed: object exists (if_none_match) or ETag mismatch (if_match)"
            )
        if resp.status != OK:
            raise_http_resp_error(resp)
        resp.read()  # NOTE: Should be zero length response. Required to reset the connection.
        self._outstanding_response = None  # Response consumed
        return (resp.status, resp.getheaders())

    def _s3_request(
        self,
        method,
        bucket,
        key,
        args,
        headers,
        content,
        sha256_hint=None,
        md5_hint=None,
    ):
        """
        Make an S3 request using AWS Signature Version 4.

        Automatically handles region discovery from 307 redirects.

        Args:
            method: HTTP method
            bucket: S3 bucket name
            key: Object key
            args: Query arguments dict
            headers: Request headers dict
            content: Request body (str, bytes, or file-like object)
            sha256_hint: Pre-calculated SHA256 digest (bytes) to skip recalculation
            md5_hint: Pre-calculated MD5 digest (bytes) to skip recalculation
        """

        # Validate that previous response was consumed before making a new request
        self._validate_connection_ready()

        # Use cached region for this bucket if available
        if bucket and bucket in self._bucket_regions:
            self.region = self._bucket_regions[bucket]

        # Try the request, handling redirects for region discovery and connection errors
        max_redirects = 2
        max_retries = 3  # Retry on connection errors
        last_error = None

        for attempt in range(max_redirects):
            for retry in range(max_retries):
                try:
                    # Ensure connection is established before attempting request
                    self._connect()

                    resp = self._s3_request_inner(
                        method,
                        bucket,
                        key,
                        args,
                        headers.copy(),
                        content,
                        sha256_hint=sha256_hint,
                        md5_hint=md5_hint,
                    )
                    break  # Success, exit retry loop
                except (ConnectionResetError, BrokenPipeError, ConnectionAbortedError, ValueError,
                        RemoteDisconnected, ssl.SSLError, EOFError, TimeoutError) as e:
                    last_error = e
                    # Connection may be in bad state, disconnect before retry
                    self._disconnect()
                    if retry == max_retries - 1:
                        # Exhausted retries, re-raise the error
                        raise
                    # Otherwise, retry the request
                    continue

            # If we got here, we either succeeded or re-raised, so we have a response
            if last_error and retry < max_retries - 1:
                # Reset error flag since we recovered
                last_error = None

            # Check for redirect responses (301 or 307)
            if resp.status in (301, 307):
                self._redirects += 1
                # Read the response body to reset the connection
                resp.read()

                # Extract the correct region from response headers
                resp_headers = dict(resp.getheaders())
                discovered_region = resp_headers.get("x-amz-bucket-region")

                if discovered_region and bucket:
                    # Cache the discovered region for this bucket
                    self._bucket_regions[bucket] = discovered_region
                    self.region = discovered_region
                    # Disconnect so we reconnect with the new region
                    self._disconnect()
                    # Retry the request with the correct region
                    continue

            # Not a redirect, return the response
            return resp

        # If we exhausted retries, return the last response
        return resp

    def _s3_request_inner(
        self,
        method,
        bucket,
        key,
        args,
        headers,
        content,
        sha256_hint=None,
        md5_hint=None,
    ):
        """
        Inner request method that performs a single S3 request.
        Called by _s3_request which handles redirect retries.

        Args:
            method: HTTP method
            bucket: S3 bucket name
            key: Object key
            args: Query arguments dict
            headers: Request headers dict
            content: Request body (str, bytes, or file-like object)
            sha256_hint: Pre-calculated SHA256 digest as bytes (32 bytes) to skip recalculation.
                         Converts to hex for x-amz-content-sha256 header (signature).
            md5_hint: Pre-calculated MD5 digest as bytes (16 bytes) to skip recalculation.
                      Converts to base64 for Content-MD5 header.
        """
        # Build the URI path and host for the request
        # Use regional endpoints for non us-east-1 to avoid redirects
        if bucket:
            # Build regional endpoint if not using default host
            if self.host == "s3.amazonaws.com" and self.region != "us-east-1":
                # Use regional endpoint format: bucket.s3-REGION.amazonaws.com
                # This format works for all regions and avoids 301 redirects
                host = f"{bucket}.s3-{self.region}.amazonaws.com"
                # Only reconnect if we're not already connected to this endpoint
                if self._current_endpoint != host:
                    self._disconnect()
                    cls = HTTPSConnection if self.use_ssl else HTTPConnection
                    self.conn = cls(
                        host, self.port, timeout=self.conn_timeout
                    )
                    self._current_endpoint = host
            else:
                host = bucket + "." + self.host
            uri = "/"
            if key:
                uri += key
        else:
            host = self.host
            uri = "/"

        # URI-encode the path (already done by sign_request_v4, but we need it for the HTTP request)
        resource = quote(uri, safe="/")

        # Build query string from args
        # For SigV4, subresources (like ?delete) must have = even with no value
        if args:
            query_parts = []
            for arg, value in sorted(args.items()):  # Sort for consistent ordering
                if value is not None:
                    query_parts.append(f"{quote(arg, safe='')}={quote(value, safe='')}")
                else:
                    query_parts.append(f"{quote(arg, safe='')}=")  # Subresource with =
            query_string = "&".join(query_parts)
            resource += "?" + query_string
        else:
            query_string = ""

        # Calculate payload hash for SigV4
        if sha256_hint is not None:
            # Use provided pre-calculated digest (bytes)
            # Convert to hex for x-amz-content-sha256 header
            payload_hash = sha256_hint.hex()
        elif isinstance(content, (str, bytes)):
            # Calculate hash (returns hex-encoded string)
            payload_hash = hash_payload(content)
        else:
            # For file-like objects, we can't hash without reading the entire content
            # S3 allows UNSIGNED-PAYLOAD for this case
            payload_hash = "UNSIGNED-PAYLOAD"

        # Get current timestamp in ISO 8601 format for SigV4
        timestamp = get_timestamp()

        # Add required SigV4 headers
        headers["Host"] = host
        headers["x-amz-date"] = timestamp
        headers["x-amz-content-sha256"] = payload_hash
        headers["Connection"] = "keep-alive"

        # Optionally add Content-MD5 header
        if md5_hint is not None:
            # Use provided pre-calculated MD5 digest (bytes)
            # Convert to base64 for Content-MD5 header
            content_md5 = b2a_base64(md5_hint).strip().decode("ascii")
            headers["Content-MD5"] = content_md5
        else:
            # Try to auto-calculate if content can be signed
            content_md5 = sign_content_if_possible(content)
            if content_md5 != "":
                headers["Content-MD5"] = content_md5

        # Sign the request using SigV4
        authorization_header = sign_request_v4(
            method=method,
            uri=uri,
            query_string=query_string,
            headers=headers,
            payload_hash=payload_hash,
            access_key_id=self.access_id,
            secret_key=self.secret,
            region=self.region,
            service="s3",
            timestamp=timestamp,
        )

        headers["Authorization"] = authorization_header

        if self.conn is None:
            raise RuntimeError("Attempted to make request without opening connection.")

        # Make the HTTP request
        request_start = time()

        # Helper to get socket info
        _conn = self.conn

        def get_sock_info():
            if hasattr(_conn, 'sock') and _conn.sock is not None:
                try:
                    fd = _conn.sock.fileno()
                    local_port = _conn.sock.getsockname()[1]
                    remote_addr = _conn.sock.getpeername()
                    return f"fd={fd} local_port={local_port} remote={remote_addr}"
                except Exception as e:
                    # Socket is broken, use cached identity if available
                    if self._socket_identity:
                        return f"{self._socket_identity} (disconnected: {type(e).__name__})"
                    return f"error: {e}"
            # Socket doesn't exist, check if we had one before
            if self._socket_identity:
                return f"{self._socket_identity} (closed)"
            return "not connected"

        # Track file position for debug logging
        file_pos_before = None

        # Debug logging
        if logger.isEnabledFor(DEBUG):
            sock_info = get_sock_info()

            # Log content info
            content_info = "empty"
            if content:
                if hasattr(content, 'fileno'):
                    try:
                        filestat = fstat(content.fileno())
                        content_info = f"file ({filestat.st_size} bytes)"
                    except Exception:
                        content_info = "file (size unknown)"
                elif isinstance(content, bytes):
                    content_info = f"bytes ({len(content)} bytes)"
                elif isinstance(content, str):
                    content_info = f"string ({len(content)} chars)"

            logger.debug(
                "Starting %s request to %s, content: %s, socket: %s",
                method, resource, content_info, sock_info)
            logger.debug("Request headers: %s", dict(headers))

            # Log file position before request if it's a file
            if hasattr(content, 'tell'):
                try:
                    file_pos_before = content.tell()
                    file_id = id(content)  # Python object ID
                    logger.debug("File object id=%s, position before request: %s", file_id, file_pos_before)
                except Exception:
                    pass

        try:
            # Seek file-like objects back to the beginning for retry safety.
            # This handles the case where s3lib's internal retry loop (for connection errors)
            # is retrying with a file handle that was partially/fully consumed by a previous attempt.
            # Note: For non-seekable streams (pipes), caller should handle retries by providing
            # fresh file handles (e.g., farmfs's retryFdIo2 calls getSrcHandle() for each retry).
            if hasattr(content, 'seek') and hasattr(content, 'tell'):
                try:
                    current_pos = content.tell()
                    if current_pos != 0:
                        logger.debug("File was at position %s, seeking back to 0", current_pos)
                        content.seek(0)
                except Exception:
                    pass  # If seek fails (non-seekable stream), proceed anyway

            request_call_start = time()
            self.conn.request(method, resource, content, headers, encode_chunked=False)
            self._requests += 1
            request_call_duration = time() - request_call_start

            # Log file position after request
            if logger.isEnabledFor(DEBUG) and file_pos_before is not None and hasattr(content, 'tell'):
                try:
                    file_pos_after = content.tell()
                    logger.debug(
                        "File position after request: %s (read %s bytes)",
                        file_pos_after, file_pos_after - file_pos_before
                    )
                except Exception:
                    pass

            if logger.isEnabledFor(DEBUG):
                # Check socket status AFTER conn.request() to see if it connected
                sock_after_request = get_sock_info()
                if sock_after_request == "not connected":
                    logger.warning("Socket still not connected after conn.request()!")
                if request_call_duration > 1.0:
                    logger.warning("conn.request() took %.2fs", request_call_duration)

            getresponse_start = time()
            resp = self.conn.getresponse()
            getresponse_duration = time() - getresponse_start

            if logger.isEnabledFor(DEBUG):
                total_duration = time() - request_start
                sock_info_after = get_sock_info()
                logger.debug(
                    "Request completed in %.2fs (request: %.2fs, getresponse: %.2fs), status: %s, socket: %s",
                    total_duration, request_call_duration, getresponse_duration, resp.status, sock_info_after
                )
        except (
            ConnectionResetError,
            BrokenPipeError,
            ConnectionAbortedError,
            OSError,
            HTTPException,
        ) as e:
            # Log socket info before disconnect for debugging
            logger.debug("Request failed with %s: %s, socket: %s", type(e).__name__, e, get_sock_info())

            # Connection is broken, clean it up so next call will reconnect
            self._disconnect()
            raise  # Re-raise for caller to handle/retry
        except Exception as e:
            # Log socket info before disconnect for debugging
            logger.debug("Request failed with %s: %s, socket: %s", type(e).__name__, e, get_sock_info())

            # For any other exception, also disconnect to ensure clean state
            self._disconnect()
            raise

        # Check if server wants us to close the connection
        connection_header = resp.getheader('Connection', '').lower()
        if connection_header == 'close':
            logger.debug("Server sent 'Connection: close', will disconnect after response consumed")
            # Mark that we need to disconnect after consuming this response
            # We can't disconnect now because caller needs to read the response body
            self._server_requested_close = True
        else:
            self._server_requested_close = False

        # Track this response so we can validate it's consumed before the next request
        self._outstanding_response = resp

        return resp

    ###########################
    # S3 Connection Functions #
    ###########################
    def _is_response_consumed(self, resp):
        """Check if an HTTPResponse has been fully consumed."""
        return resp.isclosed()

    def _validate_connection_ready(self):
        """
        Ensure connection is in a valid state for a new request.
        Raises ConnectionLifecycleError if used outside a context manager or
        if a previous response hasn't been consumed.
        """
        if not self._entered:
            raise ConnectionLifecycleError(
                "Connection must be used as a context manager. "
                "Use 'with Connection(...) as conn:' before making requests."
            )
        if self._outstanding_response is not None:
            if not self._is_response_consumed(self._outstanding_response):
                raise ConnectionLifecycleError(
                    "Previous response not fully consumed. "
                    "You must read the entire response body before making another request. "
                    "Call response.read() to consume the data."
                )
            self._outstanding_response = None

            # If server requested close on the last response, disconnect now
            if self._server_requested_close:
                logger.debug("Disconnecting as requested by server in previous response")
                self._disconnect()
                self._server_requested_close = False

    def _connect(self):
        if self.conn is None:
            _transient = (ConnectionResetError, ConnectionRefusedError,
                          ssl.SSLError, TimeoutError, OSError)
            for attempt in range(3):
                cls = HTTPSConnection if self.use_ssl else HTTPConnection
                self.conn = cls(
                    self.host, self.port, timeout=self.conn_timeout
                )
                self._current_endpoint = self.host
                try:
                    self.conn.connect()
                    self._connects += 1
                    # Capture socket identity now while it's valid
                    if hasattr(self.conn, 'sock') and self.conn.sock is not None:
                        try:
                            fd = self.conn.sock.fileno()
                            local_port = self.conn.sock.getsockname()[1]
                            self._socket_identity = f"fd={fd} local_port={local_port}"
                        except Exception:
                            self._socket_identity = "connected (info unavailable)"
                    return  # Connected successfully
                except _transient as e:
                    logger.debug("conn.connect() failed (attempt %d): %s: %s", attempt + 1, type(e).__name__, e)
                    self.conn = None
                    self._socket_identity = None
                    if attempt == 2:
                        raise
        else:
            assert self._current_endpoint is not None

    def _disconnect(self):
        if self.conn is not None:
            # Debug logging
            if logger.isEnabledFor(DEBUG):
                endpoint = self._current_endpoint if hasattr(self, '_current_endpoint') else 'unknown'

                # Get socket identity before closing
                if hasattr(self.conn, 'sock') and self.conn.sock is not None:
                    try:
                        fd = self.conn.sock.fileno()
                        local_port = self.conn.sock.getsockname()[1]
                        logger.debug("Closing connection to %s - fd=%s local_port=%s", endpoint, fd, local_port)
                    except Exception as e:
                        logger.debug("Closing connection to %s (socket info: %s)", endpoint, e)
                else:
                    logger.debug("Closing connection to %s (socket already closed)", endpoint)

            try:
                self.conn.close()
            except (ConnectionResetError, BrokenPipeError, ConnectionAbortedError):
                # Connection already broken by remote, nothing to close
                pass
            except AttributeError:
                # conn object is in an invalid state (shouldn't happen, but be defensive)
                pass
            self.conn = None
        self._outstanding_response = None
        self._socket_identity = None  # Clear cached identity
        self._server_requested_close = False  # Reset flag


def sign(secret, string_to_sign):
    """
    secret is a str?
    string_to_sign is a str.
    return bytes signature.
    """
    hashed = hmac_new(secret, string_to_sign, sha1)
    return b2a_base64(hashed.digest()).strip()


def sign_content_if_possible(content):
    # TODO if the content is a proper file, it would also be possible.
    if content != "" and isinstance(content, (str, bytes)):
        return sign_content(content)
    else:
        return ""


def sign_content(content):
    return b2a_base64(md5(content).digest()).strip().decode("ascii")


###############################
# Modern Checksum Utilities   #
###############################


def calculate_checksum(content, algorithm):
    """
    Calculate checksum for content using specified algorithm.

    Args:
        content: str or bytes to hash
        algorithm: 'SHA256', 'SHA1', or 'MD5'

    Returns:
        Base64-encoded checksum string

    Raises:
        ValueError: If algorithm not supported
        TypeError: If content is not str/bytes
    """
    if not isinstance(content, (str, bytes)):
        raise TypeError("Content must be str or bytes for checksum calculation")

    if isinstance(content, str):
        content = content.encode("utf-8")

    if algorithm == "SHA256":
        from hashlib import sha256

        digest = sha256(content).digest()
    elif algorithm == "SHA1":
        digest = sha1(content).digest()
    elif algorithm == "MD5":
        digest = md5(content).digest()
    else:
        raise ValueError(
            f"Unsupported algorithm: {algorithm}. Use SHA256, SHA1, or MD5"
        )

    return b2a_base64(digest).strip().decode("ascii")


def calculate_checksum_if_possible(content, algorithm):
    """
    Calculate checksum if content is str/bytes, otherwise return empty string.

    Args:
        content: Data to hash
        algorithm: 'SHA256', 'SHA1', or 'MD5'

    Returns:
        Base64-encoded checksum, or '' if content is not str/bytes
    """
    if content != "" and isinstance(content, (str, bytes)):
        return calculate_checksum(content, algorithm)
    return ""


def sha256_hex_to_base64(hex_string):
    """
    Convert hex-encoded SHA256 to base64-encoded.

    Utility for converting signature hash format (hex) to integrity
    checksum format (base64).

    Args:
        hex_string: Hex-encoded SHA256 (64 chars, lowercase)

    Returns:
        Base64-encoded SHA256 (44 chars)

    Example:
        hex_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        b64_hash = sha256_hex_to_base64(hex_hash)
        # Returns: "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU="
    """
    digest_bytes = bytes.fromhex(hex_string)
    return b2a_base64(digest_bytes).strip().decode("ascii")


#################################
# XML Render Handling Functions #
#################################


def _render_delete_bulk_content(keys, quiet):
    delete = Element("Delete")
    if quiet:
        quiet_element = SubElement(delete, "Quiet")
        quiet_element.text = "true"
    objects = []
    for name in keys:
        obj = Element("Object")
        key = SubElement(obj, "Key")
        key.text = name
        objects.append(obj)
    delete.extend(objects)
    return tostring(delete)


####################################
# Http Response Handling Functions #
####################################

LIST_BUCKET_KEY = "Key"
# Standard attributes always present in ListObjectsV2
LIST_BUCKET_ATTRIBUTES = ["Key", "LastModified", "ETag", "Size", "StorageClass"]
# Additional checksum attributes (present if object was uploaded with checksums)
LIST_BUCKET_CHECKSUM_ATTRIBUTES = [
    "ChecksumAlgorithm",  # Which algorithm was used (only field actually returned by ListObjectsV2)
    "ChecksumType",       # Whether checksum is for full object or composite
    # Note: ListObjectsV2 does NOT return actual checksum values (SHA256, CRC32, etc)
    # Use s3head or s3get to retrieve actual checksum values
]
# All possible attributes
LIST_BUCKET_ALL_ATTRIBUTES = LIST_BUCKET_ATTRIBUTES + LIST_BUCKET_CHECKSUM_ATTRIBUTES


def _parse_list_response(xml: str) -> Tuple[list[dict[str, str | None]], str | None]:
    """Parse ListObjectsV2 response."""
    ns = {"ListBucketResult": "http://s3.amazonaws.com/doc/2006-03-01/"}
    ns_str = "{http://s3.amazonaws.com/doc/2006-03-01/}"
    next_token_path = "ListBucketResult:NextContinuationToken"
    contents_path = "ListBucketResult:Contents"
    tree = parse(xml)

    # v2 API returns NextContinuationToken when there are more results
    next_token_elem = tree.find(next_token_path, ns)
    next_token = next_token_elem.text if next_token_elem is not None else None

    contents = tree.findall(contents_path, ns)
    items = [
        {child.tag.replace(ns_str, ""): child.text for child in iter(obj)}
        for obj in contents
    ]
    return (items, next_token)


API_VERSION = "http://s3.amazonaws.com/doc/2006-03-01/"


def _parse_get_service_response(xml: str) -> list[str]:
    bucket_path = f"{{{API_VERSION}}}Buckets/{{{API_VERSION}}}Bucket/{{{API_VERSION}}}Name"
    tree = parse(xml)
    buckets = tree.findall(bucket_path)
    names = [str(b.text) for b in buckets]
    return names


KEY_PATH = f"{{{API_VERSION}}}Key"


def _tag_normalize(name: str) -> str:
    if name[0] == "{":
        _, tag = name[1:].split("}")
        return tag
    else:
        return name


def _parse_delete_bulk_response(xml: str) -> Generator[Tuple[str, str], None, None]:
    actions = parse(xml)
    for action in actions:
        if action is None:
            raise ValueError("Action element is None")
        key_elem = action.find(KEY_PATH)
        if key_elem is None:
            raise ValueError("Key element is None")
        key_text = key_elem.text
        if key_text is None:
            raise ValueError("Key element text is None")
        tag = _tag_normalize(action.tag)
        yield (key_text, tag)


def _calculate_query_arg_str(args: dict[str, str | None]) -> str:
    """
    Produces a query arg string like "/?flag_name&argName=argValue..."
    always returns a string. If no args are present produces the empty string.
    """
    value_args = [
        "%s=%s" % (quote(arg), quote(value))
        for (arg, value) in list(args.items())
        if value is not None
    ]
    flag_args = [
        "%s" % quote(arg) for (arg, value) in list(args.items()) if value is None
    ]
    args_str = "&".join(flag_args + value_args)
    if args_str:
        args_str = "?" + args_str
    return args_str
