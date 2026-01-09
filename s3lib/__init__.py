import hmac
from hashlib import sha1
from hashlib import md5
import binascii
import http.client
import time
from xml.etree.ElementTree import fromstring as parse
from xml.etree.ElementTree import Element, SubElement, tostring
from .utils import split_headers, split_args, batchify, take, get_string_to_sign, raise_http_resp_error
from .sigv4 import sign_request_v4, hash_payload, get_timestamp
from urllib.parse import quote
import sys
import stat
import os

class ConnectionLifecycleError(Exception):
  """Raised when attempting to reuse a connection with an unconsumed response."""
  pass

class Connection:

  ############################
  # Python special functions #
  ############################
  def __init__(self, access_id, secret, host=None, port=None, conn_timeout=None, region=None):
    """
    access_id is str - AWS access key ID
    secret is bytes - AWS secret access key
    host is maybe str - S3 endpoint hostname
    port is maybe int - port number
    conn_timeout is maybe int seconds - connection timeout
    region is maybe str - AWS region (defaults to us-east-1 or AWS_DEFAULT_REGION)
    """
    assert isinstance(secret, bytes)
    self.access_id = access_id
    self.secret = secret
    self.port = port or 80
    self.host = host or "s3.amazonaws.com"
    self.conn_timeout = conn_timeout
    self.conn = None
    self._outstanding_response = None
    # Default region: env var > us-east-1 (matches boto3 behavior)
    self.region = region or os.environ.get('AWS_DEFAULT_REGION') or 'us-east-1'

  def __enter__(self):
    self._connect()
    return self

  def __exit__(self, type, value, traceback):
    self._disconnect()

  #######################
  # Interface Functions #
  #######################
  def list_buckets(self):
    """ list all buckets in account """
    xml = self._s3_get_service_request()
    buckets = _parse_get_service_response(xml)
    for bucket in buckets:
      yield bucket

  def list_bucket2(self, bucket, start=None, prefix=None, batch_size=None):
    """List contents of individual bucket returning dict of all attributes."""
    more = True
    while more:
      xml = self._s3_list_request(bucket, start, prefix, batch_size)
      objects, truncated = _parse_list_response(xml)
      for object in objects:
        yield object
        start = object[LIST_BUCKET_KEY] # Next request should start from last request's last item.
      more = truncated

  def list_bucket(self, bucket, start=None, prefix=None, batch_size=None):
    """List contents of individual bucket."""
    for obj in self.list_bucket2(bucket, start, prefix, batch_size):
        yield obj[LIST_BUCKET_KEY]

  def get_object(self, bucket, key):
    """ pull down bucket object by key """
    #TODO Want to replace with some enter, exit struct.
    return self._s3_get_request(bucket, key)

  def get_object_url(self, bucket, key, proto="https"):
    """get a public url for the object in the bucket."""
    return proto + "://" + self.host + "/" + bucket + "/" + key

  def head_object(self, bucket, key):
    """ get request metadata for key in bucket """
    status, headers = self._s3_head_request(bucket, key)
    return headers

  def delete_object(self, bucket, key):
    """ delete key from bucket """
    status, headers = self._s3_delete_request(bucket, key)
    return (status, headers)

  def delete_objects(self, bucket, keys, batch_size=1000, quiet=False):
    """ delete keys from bucket """
    for batch in batchify(batch_size, keys):
      xml = self._s3_delete_bulk_request(bucket, batch, quiet)
      results = _parse_delete_bulk_response(xml)
      for (key, result) in results:
        yield key, result

  def copy_object(self, src_bucket, src_key, dst_bucket, dst_key, headers=None):
    """ copy key from one bucket to another """
    if headers is None:
        headers = dict()
    else:
        headers = dict(headers)
    (status, resp_headers) = self._s3_copy_request(src_bucket, src_key, dst_bucket, dst_key, headers)
    return (status, resp_headers)

  def put_object(self, bucket, key, data, headers=None,
                 sha256_hint=None,
                 checksum_algorithm=None,
                 if_none_match=False,
                 if_match=None):
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
        if_match: ETag value for optimistic concurrency control (overwrite specific version).

    Returns:
        (status, headers) tuple
        Response headers include x-amz-checksum-{algorithm} if used

    Raises:
        ValueError: If checksum auto-calculation requested but not possible (streaming data)
        HTTP 412: If if_none_match=True and object already exists
        HTTP 412: If if_match provided and ETag doesn't match
        HTTP 400: If checksum value doesn't match uploaded data

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
        etag = dict(headers_resp)['etag']
        conn.put_object(bucket, key, new_data, if_match=etag)
    """
    if headers is None:
        headers = dict()
    else:
        headers = dict(headers)

    # Determine which algorithm to use
    # Default to SHA256 for str/bytes if no algorithm specified (best effort, no error)
    user_requested_algo = checksum_algorithm is not None
    if checksum_algorithm is None and isinstance(data, (str, bytes)):
        checksum_algorithm = 'SHA256'

    # Handle modern checksum headers
    if checksum_algorithm:
        # If SHA256 and we already have the hint, reuse it
        if checksum_algorithm == 'SHA256' and sha256_hint is not None:
            # We have the digest as bytes, convert to base64 for checksum header
            checksum_value = binascii.b2a_base64(sha256_hint).strip().decode('ascii')
        else:
            # Need to calculate checksum
            checksum_value = calculate_checksum_if_possible(data, checksum_algorithm)

            if not checksum_value:
                # If user explicitly requested algorithm, must succeed or error
                if user_requested_algo:
                    raise ValueError(
                        f"Cannot calculate {checksum_algorithm} checksum for streaming data. "
                        "Provide sha256_hint or use str/bytes data."
                    )
                # Otherwise (default algorithm), silently skip checksumming
                else:
                    checksum_algorithm = None

            # Special case: if calculating SHA256 and we don't have the hint yet,
            # calculate it once and reuse for signature
            if checksum_value and checksum_algorithm == 'SHA256' and sha256_hint is None:
                if isinstance(data, str):
                    data_bytes = data.encode('utf-8')
                elif isinstance(data, bytes):
                    data_bytes = data
                else:
                    data_bytes = None

                if data_bytes is not None:
                    from hashlib import sha256
                    sha256_hint = sha256(data_bytes).digest()

        # Add modern checksum headers only if we successfully calculated
        if checksum_algorithm and checksum_value:
            headers['x-amz-checksum-algorithm'] = checksum_algorithm
            headers[f'x-amz-checksum-{checksum_algorithm.lower()}'] = checksum_value

    # Handle conditional headers
    if if_none_match:
        headers['If-None-Match'] = '*'
    if if_match:
        headers['If-Match'] = if_match

    # Call existing implementation (which calls _s3_put_request)
    # Pass sha256_hint for signature optimization
    (status, resp_headers) = self._s3_put_request(bucket, key, data, headers,
                                                   sha256_hint=sha256_hint)
    return (status, resp_headers)

##########################
# Http request Functions #
##########################

  def _s3_get_service_request(self):
    resp = self._s3_request("GET", None, None, {}, {}, '')
    if resp.status != http.client.OK:
      raise_http_resp_error(resp)
    data = resp.read() #TODO HAS A PAYLOAD, MAYBE NOT BEST READ CANDIDATE.
    self._outstanding_response = None  # Response consumed
    return data

  def _s3_list_request(self, bucket, marker=None, prefix=None, max_keys=None):
    args = {}
    if marker:
      args['marker'] = marker
    if prefix:
      args['prefix'] = prefix
    if max_keys:
      args['max-keys'] = str(max_keys)
    resp = self._s3_request("GET", bucket, None, args, {}, '')
    if resp.status != http.client.OK:
      raise_http_resp_error(resp)
    data = resp.read() #TODO HAS A PAYLOAD, MAYBE NOT BEST READ CANDIDATE.
    self._outstanding_response = None  # Response consumed
    return data

  def _s3_get_request(self, bucket, key):
    resp = self._s3_request("GET", bucket, key, {}, {}, '')
    if resp.status != http.client.OK:
      raise_http_resp_error(resp)
    return resp

  def _s3_head_request(self, bucket, key):
    resp = self._s3_request("HEAD", bucket, key, {}, {}, '')
    if resp.status != http.client.OK:
      raise_http_resp_error(resp)
    resp.read() #NOTE: Should be zero size response. Required to reset the connection.
    self._outstanding_response = None  # Response consumed
    return (resp.status, resp.getheaders())

  def _s3_delete_request(self, bucket, key):
    resp = self._s3_request("DELETE", bucket, key, {}, {}, '')
    if resp.status != http.client.NO_CONTENT:
      raise_http_resp_error(resp)
    resp.read() #NOTE: Should be zero size response. Required to reset the connection
    self._outstanding_response = None  # Response consumed
    return (resp.status, resp.getheaders())

  def _s3_delete_bulk_request(self, bucket, keys, quiet):
    content = _render_delete_bulk_content(keys, quiet)
    resp = self._s3_request("POST", bucket, None, {"delete":None}, {}, content)
    if resp.status != http.client.OK:
      raise_http_resp_error(resp)
    results = resp.read() #TODO HAS A PAYLOAD, MAYBE NOT BEST READ CANDIDATE.
    self._outstanding_response = None  # Response consumed
    return results

  def _s3_copy_request(self, src_bucket, src_key, dst_bucket, dst_key, headers):
    headers['x-amz-copy-source'] = "/%s/%s" % (src_bucket, src_key)
    headers['x-amz-metadata-directive'] = 'REPLACE'
    resp = self._s3_request("PUT", dst_bucket, dst_key, {}, headers, '')
    if resp.status != http.client.OK:
      raise_http_resp_error(resp)
    return (resp.status, resp.getheaders())

  def _s3_put_request(self, bucket, key, data, headers,
                      sha256_hint=None, md5_hint=None):
    """
    Mid-level PUT request handler.

    Args:
        bucket: S3 bucket name
        key: Object key
        data: Data to upload (str, bytes, or file-like object)
        headers: Request headers dict
        sha256_hint: Optional pre-calculated SHA256 digest as bytes.
                     Passed through to _s3_request.
        md5_hint: Optional pre-calculated MD5 digest as bytes.
                  Passed through to _s3_request.
    """
    args = {}
    if isinstance(data, (str, bytes)):
        content_length = len(data)
    elif hasattr(data, 'fileno'):
        fileno = data.fileno()
        filestat = os.fstat(fileno)
        if stat.S_ISREG(filestat.st_mode):
            # Regular file
            content_length = filestat[stat.ST_SIZE]
        else:
            # Special file, size won't be valid. Lets read the data to get value.
            # We have to encode to utf-8 for later hashing.
            # TODO This looks totally wrong. What about binary files?
            #      I think data might be stdin which is why we are treating it like a string.
            # TODO we are reading the ENTIRE STREAM we should not do that.
            #      If we have to have a content length, we can stream into a
            #      temp file and use that for buffering/checksumming.
            data = data.read().encode('utf-8')
            content_length = len(data)
    else:
        raise TypeError(f"Cannot determine content-length of type {type(data)}")
    headers['content-length'] = content_length
    resp = self._s3_request("PUT", bucket, key, args, headers, data,
                            sha256_hint=sha256_hint,
                            md5_hint=md5_hint)
    if resp.status != http.client.OK:
      raise_http_resp_error(resp)
    resp.read() #NOTE: Should be zero length response. Required to reset the connection.
    self._outstanding_response = None  # Response consumed
    return (resp.status, resp.getheaders())

  def _s3_request(self, method, bucket, key, args, headers, content,
                  sha256_hint=None, md5_hint=None):
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

    http_now = time.strftime('%a, %d %b %Y %H:%M:%S +0000', time.gmtime())
    # Initialize bucket region cache if not present
    if not hasattr(self, '_bucket_regions'):
      self._bucket_regions = {}

    # Use cached region for this bucket if available
    if bucket and bucket in self._bucket_regions:
      self.region = self._bucket_regions[bucket]

    # Try the request, handling redirects for region discovery
    max_redirects = 2
    for attempt in range(max_redirects):
      resp = self._s3_request_inner(method, bucket, key, args, headers.copy(), content,
                                     sha256_hint=sha256_hint,
                                     md5_hint=md5_hint)

      # Check for redirect responses (301 or 307)
      if resp.status in (301, 307):
        # Read the response body to reset the connection
        resp.read()

        # Extract the correct region from response headers
        resp_headers = dict(resp.getheaders())
        discovered_region = resp_headers.get('x-amz-bucket-region')

        if discovered_region and bucket:
          # Cache the discovered region for this bucket
          self._bucket_regions[bucket] = discovered_region
          self.region = discovered_region
          # Clear the current endpoint so we reconnect with the new region
          if hasattr(self, '_current_endpoint'):
            delattr(self, '_current_endpoint')
          # Retry the request with the correct region
          continue

      # Not a redirect, return the response
      return resp

    # If we exhausted retries, return the last response
    return resp

  def _s3_request_inner(self, method, bucket, key, args, headers, content,
                        sha256_hint=None, md5_hint=None):
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
        if not hasattr(self, '_current_endpoint') or self._current_endpoint != host:
          if hasattr(self, 'conn'):
            self._disconnect()
          self.conn = http.client.HTTPConnection(host, self.port, timeout=self.conn_timeout)
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
    resource = quote(uri, safe='/')

    # Build query string from args
    # For SigV4, subresources (like ?delete) must have = even with no value
    if args:
      query_parts = []
      for arg, value in sorted(args.items()):  # Sort for consistent ordering
        if value is not None:
          query_parts.append(f"{quote(arg, safe='')}={quote(value, safe='')}")
        else:
          query_parts.append(f"{quote(arg, safe='')}=")  # Subresource with =
      query_string = '&'.join(query_parts)
      resource += '?' + query_string
    else:
      query_string = ''

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
      content_md5 = binascii.b2a_base64(md5_hint).strip().decode('ascii')
      headers['Content-MD5'] = content_md5
    else:
      # Try to auto-calculate if content can be signed
      content_md5 = sign_content_if_possible(content)
      if content_md5 != '':
        headers['Content-MD5'] = content_md5

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
      service='s3',
      timestamp=timestamp
    )

    headers["Authorization"] = authorization_header

    # Make the HTTP request
    try:
      if sys.version_info >= (3, 0):
        self.conn.request(method, resource, content, headers, encode_chunked=False)
      else:
        self.conn.request(method, resource, content, headers)

      resp = self.conn.getresponse()
    except (ConnectionResetError, BrokenPipeError, ConnectionAbortedError,
            OSError, http.client.HTTPException) as e:
      # Connection is broken, clean it up so next call will reconnect
      self._disconnect()
      raise  # Re-raise for caller to handle/retry

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
    Raises ConnectionLifecycleError if a previous response hasn't been consumed.
    """
    if self._outstanding_response is not None:
      if not self._is_response_consumed(self._outstanding_response):
        raise ConnectionLifecycleError(
          "Previous response not fully consumed. "
          "You must read the entire response body before making another request. "
          "Call response.read() to consume the data."
        )
      self._outstanding_response = None

  def _connect(self):
    if self.conn is None:
      self.conn = http.client.HTTPConnection(self.host, self.port, timeout=self.conn_timeout)
      self._current_endpoint = self.host
    else:
        assert self._curent_endpoint is not None

  def _disconnect(self):
    if self.conn is not None:
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

def sign(secret, string_to_sign):
  """
  secret is a str?
  string_to_sign is a str.
  return bytes signature.
  """
  hashed = hmac.new(secret, string_to_sign, sha1)
  return binascii.b2a_base64(hashed.digest()).strip()

def sign_content_if_possible(content):
  #TODO if the content is a proper file, it would also be possible.
  if content != '' and isinstance(content, (str, bytes)):
    return sign_content(content)
  else:
    return ""

def sign_content(content):
  return binascii.b2a_base64(md5(content).digest()).strip().decode('ascii')

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
      algorithm: 'SHA256', 'SHA1', or 'MD5'

  Returns:
      Base64-encoded checksum, or '' if content is not str/bytes
  """
  if content != '' and isinstance(content, (str, bytes)):
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
  return binascii.b2a_base64(digest_bytes).strip().decode('ascii')

#################################
# XML Render Handling Functions #
#################################

def _render_delete_bulk_content(keys, quiet):
  delete = Element('Delete')
  if quiet:
    quiet_element = SubElement(delete, 'Quiet')
    quiet_element.text = 'true'
  objects = []
  for name in keys:
    obj = Element('Object')
    key = SubElement(obj, 'Key')
    key.text = name
    objects.append(obj)
  delete.extend(objects)
  return tostring(delete)

####################################
# Http Response Handling Functions #
####################################

LIST_BUCKET_KEY = 'Key'
LIST_BUCKET_ATTRIBUTES = ['Key', 'LastModified', 'ETag', 'Size', 'StorageClass']
def _parse_list_response(xml):
  ns = {'ListBucketResult': 'http://s3.amazonaws.com/doc/2006-03-01/'}
  ns_str = '{http://s3.amazonaws.com/doc/2006-03-01/}'
  is_truncated_path = 'ListBucketResult:IsTruncated'
  contents_path = 'ListBucketResult:Contents'
  tree = parse(xml)
  is_truncated = tree.find(is_truncated_path, ns).text == 'true'
  contents = tree.findall(contents_path, ns)
  items = [{child.tag.replace(ns_str, ''): child.text for child in iter(obj)} for obj in contents]
  return (items, is_truncated)

def _parse_get_service_response(xml):
  bucket_path = '{http://s3.amazonaws.com/doc/2006-03-01/}Buckets/{http://s3.amazonaws.com/doc/2006-03-01/}Bucket/{http://s3.amazonaws.com/doc/2006-03-01/}Name'
  tree = parse(xml)
  buckets = tree.findall(bucket_path)
  names = []
  for bucket in buckets:
    names.append(bucket.text)
  return (names)

KEY_PATH='{http://s3.amazonaws.com/doc/2006-03-01/}Key'
def _tag_normalize(name):
    if name[0] == "{":
        _, tag = name[1:].split("}")
        return tag
    else:
        return name
def _parse_delete_bulk_response(xml):
  actions = parse(xml)
  return [ (action.find(KEY_PATH).text, _tag_normalize(action.tag)) for action in actions]

def _calculate_query_arg_str(args):
  """
  args is dict of str-> Maybe str.
  Produces a query arg string like "/?flag_name&argName=argValue..."
  always returns a string. If no args are present produces the empty string.
  """
  value_args = ["%s=%s"%(quote(arg), quote(value)) for (arg, value) in list(args.items()) if value is not None]
  flag_args = ["%s"%quote(arg) for (arg, value) in list(args.items()) if value is None]
  args_str = "&".join(flag_args+value_args)
  if args_str:
    args_str = "?" + args_str
  return args_str

# Import connection pooling classes
from s3lib.pool import ConnectionPool, ConnectionLease

