import hmac
from hashlib import sha1
from hashlib import md5
import binascii
import http.client
import time
from xml.etree.ElementTree import fromstring as parse
from xml.etree.ElementTree import Element, SubElement, tostring
from s3lib.utils import split_headers, split_args, batchify, take, get_string_to_sign, raise_http_resp_error
from s3lib.sigv4 import sign_request_v4, hash_payload, get_timestamp
from urllib.parse import quote
import sys
import stat
import os

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

  def put_object(self, bucket, key, data, headers=None):
    """ push object from local to bucket """
    if headers is None:
        headers = dict()
    else:
        headers = dict(headers)
    (status, resp_headers) = self._s3_put_request(bucket, key, data, headers)
    return (status, resp_headers)

##########################
# Http request Functions #
##########################

  def _s3_get_service_request(self):
    resp = self._s3_request("GET", None, None, {}, {}, '')
    if resp.status != http.client.OK:
      raise_http_resp_error(resp)
    return resp.read() #TODO HAS A PAYLOAD, MAYBE NOT BEST READ CANDIDATE.

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
    return resp.read() #TODO HAS A PAYLOAD, MAYBE NOT BEST READ CANDIDATE.

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
    return (resp.status, resp.getheaders())

  def _s3_delete_request(self, bucket, key):
    resp = self._s3_request("DELETE", bucket, key, {}, {}, '')
    if resp.status != http.client.NO_CONTENT:
      raise_http_resp_error(resp)
    resp.read() #NOTE: Should be zero size response. Required to reset the connection
    return (resp.status, resp.getheaders())

  def _s3_delete_bulk_request(self, bucket, keys, quiet):
    content = _render_delete_bulk_content(keys, quiet)
    resp = self._s3_request("POST", bucket, None, {"delete":None}, {}, content)
    if resp.status != http.client.OK:
      raise_http_resp_error(resp)
    results = resp.read() #TODO HAS A PAYLOAD, MAYBE NOT BEST READ CANDIDATE.
    return results

  def _s3_copy_request(self, src_bucket, src_key, dst_bucket, dst_key, headers):
    headers['x-amz-copy-source'] = "/%s/%s" % (src_bucket, src_key)
    headers['x-amz-metadata-directive'] = 'REPLACE'
    resp = self._s3_request("PUT", dst_bucket, dst_key, {}, headers, '')
    if resp.status != http.client.OK:
      raise_http_resp_error(resp)
    return (resp.status, resp.getheaders())

  def _s3_put_request(self, bucket, key, data, headers):
    #TODO add abilityo to pass optional Content-MD5 value.
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
            data = data.read().encode('utf-8')
            content_length = len(data)
    headers['content-length'] = content_length
    resp = self._s3_request("PUT", bucket, key, args, headers, data)
    if resp.status != http.client.OK:
      raise_http_resp_error(resp)
    resp.read() #NOTE: Should be zero length response. Required to reset the connection.
    return (resp.status, resp.getheaders())

  def _s3_request(self, method, bucket, key, args, headers, content):
    """
    Make an S3 request using AWS Signature Version 4.
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
    if isinstance(content, (str, bytes)):
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

    # Optionally add Content-MD5 if content can be signed
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
    if sys.version_info >= (3, 0):
      self.conn.request(method, resource, content, headers, encode_chunked=False)
    else:
      self.conn.request(method, resource, content, headers)

    resp = self.conn.getresponse()
    return resp

###########################
# S3 Connection Functions #
###########################
  def _connect(self):
    self.conn = http.client.HTTPConnection(self.host, self.port, timeout=self.conn_timeout)
    self._current_endpoint = self.host

  def _disconnect(self):
    self.conn.close()

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

